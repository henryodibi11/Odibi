"""
Recipe System
=============

Reusable node-level templates with variable substitution.

Recipes allow users to define common pipeline patterns once and reuse them
across many nodes, reducing boilerplate and enforcing consistency.

Usage in YAML:
    recipes:
      my_recipe:
        description: "My reusable pattern"
        required_vars: [key_column]
        template:
          transformer: deduplicate
          params:
            keys: ["${recipe.key_column}"]

    pipelines:
      - pipeline: my_pipeline
        nodes:
          - name: dedup_users
            recipe: my_recipe
            recipe_vars:
              key_column: user_id
            write:
              connection: output
              format: parquet
              path: users.parquet
"""

import copy
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, model_validator

logger = logging.getLogger(__name__)

# Pattern matches ${recipe.var_name} - uses "recipe." prefix to avoid collision
# with existing ${ENV_VAR}, ${vars.xxx}, ${date:xxx} syntax
RECIPE_VAR_PATTERN = re.compile(r"\$\{recipe\.([a-zA-Z_][a-zA-Z0-9_]*)\}")

# Fields that must NOT appear in recipe templates (they belong to the node)
RESERVED_NODE_FIELDS = frozenset(
    {
        "name",
        "recipe",
        "recipe_vars",
        "_source_yaml",
    }
)


class RecipeConfig(BaseModel):
    """Definition of a reusable recipe.

    A recipe contains a partial node configuration (the 'template') plus
    metadata about what variables are required.
    """

    description: Optional[str] = Field(
        default=None, description="Human-readable description of what this recipe does"
    )
    extends: Optional[str] = Field(
        default=None, description="Name of another recipe to inherit from (base recipe)"
    )
    required_vars: List[str] = Field(
        default_factory=list, description="Variable names that must be provided in recipe_vars"
    )
    optional_vars: Dict[str, Any] = Field(
        default_factory=dict, description="Optional variables with default values"
    )
    template: Dict[str, Any] = Field(description="Partial NodeConfig fields to merge into the node")

    @model_validator(mode="after")
    def validate_template_keys(self):
        """Ensure template doesn't contain reserved node fields."""
        bad_keys = RESERVED_NODE_FIELDS & set(self.template.keys())
        if bad_keys:
            raise ValueError(
                f"Recipe template cannot contain reserved fields: {bad_keys}. "
                f"These are set by the node, not the recipe."
            )
        return self

    @model_validator(mode="after")
    def validate_vars_referenced(self):
        """Warn if required_vars are declared but not used in template."""
        template_str = str(self.template)
        for var in self.required_vars:
            if f"${{recipe.{var}}}" not in template_str:
                logger.warning(
                    f"Recipe declares required_var '{var}' but it's not referenced "
                    f"in the template as ${{recipe.{var}}}"
                )
        return self


class RecipeRegistry:
    """Registry for loading and resolving recipes."""

    def __init__(self):
        self._recipes: Dict[str, RecipeConfig] = {}

    def register(self, name: str, recipe: RecipeConfig) -> None:
        """Register a recipe."""
        self._recipes[name] = recipe

    def get(self, name: str) -> Optional[RecipeConfig]:
        """Get a recipe by name."""
        return self._recipes.get(name)

    def list_recipes(self) -> Dict[str, RecipeConfig]:
        """Return all registered recipes."""
        return dict(self._recipes)

    def load_builtins(self) -> None:
        """Load built-in recipes from odibi/recipes/builtins/ directory."""
        import yaml

        builtins_dir = Path(__file__).parent / "builtins"
        if not builtins_dir.exists():
            return

        for yaml_file in sorted(builtins_dir.glob("*.yaml")):
            try:
                with open(yaml_file, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f)

                if not isinstance(data, dict):
                    continue

                for recipe_name, recipe_data in data.items():
                    if recipe_name.startswith("_"):
                        continue
                    try:
                        recipe = RecipeConfig(**recipe_data)
                        self._recipes[recipe_name] = recipe
                        logger.debug(f"Loaded built-in recipe: {recipe_name}")
                    except Exception as e:
                        logger.warning(
                            f"Failed to load built-in recipe '{recipe_name}' "
                            f"from {yaml_file.name}: {e}"
                        )
            except Exception as e:
                logger.warning(f"Failed to read recipe file {yaml_file}: {e}")

    def load_inline(self, recipes_dict: Dict[str, Any]) -> None:
        """Load user-defined recipes from inline YAML config.

        Inline recipes override built-ins with the same name.
        """
        for name, recipe_data in recipes_dict.items():
            try:
                recipe = RecipeConfig(**recipe_data)
                if name in self._recipes:
                    logger.debug(f"Inline recipe '{name}' overrides built-in")
                self._recipes[name] = recipe
            except Exception as e:
                raise ValueError(f"Invalid recipe definition '{name}': {e}") from e

    def _resolve_inheritance(self) -> None:
        """Resolve recipe inheritance chains.

        For each recipe with 'extends', deep merges the parent template
        into the child (child wins). Inherits required_vars and optional_vars
        from parent (child additions/overrides win). Detects circular
        inheritance and supports multi-level chains.
        """
        resolved: Dict[str, RecipeConfig] = {}

        def _resolve(name: str, chain: List[str]) -> RecipeConfig:
            if name in resolved:
                return resolved[name]

            if name not in self._recipes:
                raise ValueError(f"Recipe '{chain[-1]}' extends unknown recipe '{name}'")

            if name in chain:
                raise ValueError(
                    f"Circular recipe inheritance detected: {' -> '.join(chain)} -> {name}"
                )

            recipe = self._recipes[name]
            if recipe.extends is None:
                resolved[name] = recipe
                return recipe

            parent = _resolve(recipe.extends, chain + [name])

            # Deep merge templates: parent is base, child wins
            merged_template = _deep_merge_recipe(parent.template, recipe.template)

            # Merge required_vars: parent + child (deduplicated, child order last)
            parent_req = [v for v in parent.required_vars if v not in recipe.required_vars]
            merged_required = parent_req + recipe.required_vars

            # Merge optional_vars: parent defaults, child overrides
            merged_optional = dict(parent.optional_vars)
            merged_optional.update(recipe.optional_vars)

            merged_recipe = RecipeConfig(
                description=recipe.description or parent.description,
                extends=None,  # Inheritance resolved
                required_vars=merged_required,
                optional_vars=merged_optional,
                template=merged_template,
            )
            resolved[name] = merged_recipe
            return merged_recipe

        for name in list(self._recipes.keys()):
            _resolve(name, [])

        self._recipes = resolved


def _substitute_recipe_vars(
    obj: Any,
    variables: Dict[str, Any],
    recipe_name: str,
) -> Any:
    """Recursively substitute ${recipe.var} placeholders in a data structure.

    If the entire scalar value is a single placeholder (e.g., "${recipe.keys}"),
    the original type from recipe_vars is preserved (list, int, etc.).
    If the placeholder is part of a larger string, it's stringified.
    """
    if isinstance(obj, str):
        # Check if the entire string is a single placeholder
        match = RECIPE_VAR_PATTERN.fullmatch(obj)
        if match:
            var_name = match.group(1)
            if var_name in variables:
                return variables[var_name]
            raise ValueError(
                f"Recipe '{recipe_name}': Variable '${{recipe.{var_name}}}' "
                f"not found in recipe_vars. Available: {list(variables.keys())}"
            )

        # Partial substitution (embedded in string)
        def replace_match(m):
            var_name = m.group(1)
            if var_name in variables:
                return str(variables[var_name])
            raise ValueError(
                f"Recipe '{recipe_name}': Variable '${{recipe.{var_name}}}' "
                f"not found in recipe_vars. Available: {list(variables.keys())}"
            )

        return RECIPE_VAR_PATTERN.sub(replace_match, obj)

    elif isinstance(obj, dict):
        return {k: _substitute_recipe_vars(v, variables, recipe_name) for k, v in obj.items()}

    elif isinstance(obj, list):
        return [_substitute_recipe_vars(item, variables, recipe_name) for item in obj]

    return obj


def _deep_merge_recipe(
    recipe_template: Dict[str, Any], node_overrides: Dict[str, Any]
) -> Dict[str, Any]:
    """Deep merge recipe template with node overrides.

    Rules:
    - dict + dict → recursive merge (node wins on conflicts)
    - list → node replaces recipe list entirely (no appending)
    - scalar → node wins
    """
    result = copy.deepcopy(recipe_template)

    for key, node_value in node_overrides.items():
        if key in result and isinstance(result[key], dict) and isinstance(node_value, dict):
            result[key] = _deep_merge_recipe(result[key], node_value)
        else:
            result[key] = copy.deepcopy(node_value)

    return result


def resolve_recipes(config_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Resolve all recipe references in a project config dict.

    This is the main entry point. Call AFTER load_yaml_with_env() and
    BEFORE ProjectConfig(**config_dict).

    Steps:
    1. Load built-in recipes
    2. Load inline recipes from config_dict['recipes']
    3. For each node with 'recipe', expand it
    4. Remove 'recipes' key from config_dict (consumed)

    Returns:
        Modified config_dict with recipes resolved
    """
    config_dict = copy.deepcopy(config_dict)

    # Build registry
    registry = RecipeRegistry()
    registry.load_builtins()

    # Load inline recipes
    inline_recipes = config_dict.pop("recipes", None)
    if inline_recipes and isinstance(inline_recipes, dict):
        registry.load_inline(inline_recipes)

    # Resolve recipe inheritance chains
    registry._resolve_inheritance()

    # If no recipes defined or referenced, return early
    if not registry.list_recipes():
        return config_dict

    # Resolve recipe references in all pipeline nodes
    pipelines = config_dict.get("pipelines", [])
    for pipeline in pipelines:
        nodes = pipeline.get("nodes", [])
        resolved_nodes = []
        for node in nodes:
            if "recipe" in node:
                node = _expand_recipe_node(node, registry)
            resolved_nodes.append(node)
        pipeline["nodes"] = resolved_nodes

    return config_dict


def _expand_recipe_node(node: Dict[str, Any], registry: RecipeRegistry) -> Dict[str, Any]:
    """Expand a single node that references a recipe."""
    recipe_name = node.get("recipe")
    recipe_vars = node.get("recipe_vars", {})
    node_name = node.get("name", "<unnamed>")

    # Look up recipe
    recipe = registry.get(recipe_name)
    if recipe is None:
        available = list(registry.list_recipes().keys())
        raise ValueError(
            f"Node '{node_name}': Unknown recipe '{recipe_name}'. Available recipes: {available}"
        )

    # Validate required vars
    all_vars = dict(recipe.optional_vars)  # Start with defaults
    all_vars.update(recipe_vars)  # User overrides

    missing = [v for v in recipe.required_vars if v not in all_vars]
    if missing:
        raise ValueError(
            f"Node '{node_name}': Recipe '{recipe_name}' requires variables {missing}. "
            f"Provided: {list(recipe_vars.keys())}. "
            f"Add the missing variables to recipe_vars."
        )

    # Substitute variables in template
    template = copy.deepcopy(recipe.template)
    template = _substitute_recipe_vars(template, all_vars, recipe_name)

    # Prepare node overrides (everything except recipe/recipe_vars)
    node_overrides = {k: v for k, v in node.items() if k not in ("recipe", "recipe_vars")}

    # Merge: recipe template is the base, node overrides win
    merged = _deep_merge_recipe(template, node_overrides)

    logger.debug(
        f"Expanded recipe '{recipe_name}' for node '{node_name}'",
        extra={"recipe": recipe_name, "node": node_name},
    )

    return merged


# Convenience function for CLI
def get_recipe_registry() -> RecipeRegistry:
    """Get a fully loaded recipe registry (built-ins only)."""
    registry = RecipeRegistry()
    registry.load_builtins()
    return registry
