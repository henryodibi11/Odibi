"""Tests for the recipe system (odibi.recipes)."""

import copy

import pytest


class TestRecipeConfig:
    """Tests for RecipeConfig Pydantic model validation."""

    def test_valid_recipe_config(self):
        from odibi.recipes import RecipeConfig

        config = RecipeConfig(
            description="Dedup recipe",
            required_vars=["keys"],
            optional_vars={"order_by": None},
            template={
                "transformer": "deduplicate",
                "params": {"keys": "${recipe.keys}", "order_by": "${recipe.order_by}"},
            },
        )
        assert config.description == "Dedup recipe"
        assert config.required_vars == ["keys"]
        assert config.optional_vars == {"order_by": None}
        assert config.template["transformer"] == "deduplicate"

    def test_template_with_reserved_field_name_raises(self):
        from odibi.recipes import RecipeConfig

        with pytest.raises(ValueError, match="reserved fields"):
            RecipeConfig(
                template={"name": "bad", "transformer": "deduplicate"},
            )

    def test_template_with_reserved_field_recipe_raises(self):
        from odibi.recipes import RecipeConfig

        with pytest.raises(ValueError, match="reserved fields"):
            RecipeConfig(template={"recipe": "bad"})

    def test_template_with_reserved_field_recipe_vars_raises(self):
        from odibi.recipes import RecipeConfig

        with pytest.raises(ValueError, match="reserved fields"):
            RecipeConfig(template={"recipe_vars": {"a": 1}})

    def test_empty_template_is_valid(self):
        from odibi.recipes import RecipeConfig

        config = RecipeConfig(template={})
        assert config.template == {}

    def test_required_vars_default_empty(self):
        from odibi.recipes import RecipeConfig

        config = RecipeConfig(template={"transformer": "noop"})
        assert config.required_vars == []

    def test_optional_vars_default_empty(self):
        from odibi.recipes import RecipeConfig

        config = RecipeConfig(template={"transformer": "noop"})
        assert config.optional_vars == {}

    def test_description_defaults_to_none(self):
        from odibi.recipes import RecipeConfig

        config = RecipeConfig(template={})
        assert config.description is None


class TestSubstituteRecipeVars:
    """Tests for _substitute_recipe_vars."""

    def test_string_substitution(self):
        from odibi.recipes import _substitute_recipe_vars

        result = _substitute_recipe_vars("${recipe.name}", {"name": "users"}, "test_recipe")
        assert result == "users"

    def test_type_preservation_list(self):
        from odibi.recipes import _substitute_recipe_vars

        result = _substitute_recipe_vars(
            "${recipe.keys}", {"keys": ["col_a", "col_b"]}, "test_recipe"
        )
        assert result == ["col_a", "col_b"]
        assert isinstance(result, list)

    def test_type_preservation_int(self):
        from odibi.recipes import _substitute_recipe_vars

        result = _substitute_recipe_vars("${recipe.count}", {"count": 42}, "test_recipe")
        assert result == 42
        assert isinstance(result, int)

    def test_partial_substitution(self):
        from odibi.recipes import _substitute_recipe_vars

        result = _substitute_recipe_vars("prefix_${recipe.x}_suffix", {"x": "val"}, "test_recipe")
        assert result == "prefix_val_suffix"

    def test_partial_substitution_stringifies(self):
        from odibi.recipes import _substitute_recipe_vars

        result = _substitute_recipe_vars("count=${recipe.n}", {"n": 5}, "test_recipe")
        assert result == "count=5"
        assert isinstance(result, str)

    def test_nested_dict_substitution(self):
        from odibi.recipes import _substitute_recipe_vars

        obj = {
            "a": "${recipe.x}",
            "nested": {"b": "${recipe.y}"},
        }
        result = _substitute_recipe_vars(obj, {"x": "X", "y": "Y"}, "r")
        assert result == {"a": "X", "nested": {"b": "Y"}}

    def test_nested_list_substitution(self):
        from odibi.recipes import _substitute_recipe_vars

        obj = ["${recipe.a}", "${recipe.b}"]
        result = _substitute_recipe_vars(obj, {"a": "1", "b": "2"}, "r")
        assert result == ["1", "2"]

    def test_list_inside_dict_substitution(self):
        from odibi.recipes import _substitute_recipe_vars

        obj = {"items": ["${recipe.x}", "static"]}
        result = _substitute_recipe_vars(obj, {"x": "dynamic"}, "r")
        assert result == {"items": ["dynamic", "static"]}

    def test_missing_variable_raises(self):
        from odibi.recipes import _substitute_recipe_vars

        with pytest.raises(ValueError, match="not found in recipe_vars"):
            _substitute_recipe_vars("${recipe.missing}", {}, "my_recipe")

    def test_missing_variable_message_includes_recipe_name(self):
        from odibi.recipes import _substitute_recipe_vars

        with pytest.raises(ValueError, match="my_recipe"):
            _substitute_recipe_vars("${recipe.x}", {}, "my_recipe")

    def test_missing_variable_in_partial_raises(self):
        from odibi.recipes import _substitute_recipe_vars

        with pytest.raises(ValueError, match="not found in recipe_vars"):
            _substitute_recipe_vars("prefix_${recipe.x}_suffix", {}, "r")

    def test_non_string_passthrough_int(self):
        from odibi.recipes import _substitute_recipe_vars

        assert _substitute_recipe_vars(42, {}, "r") == 42

    def test_non_string_passthrough_none(self):
        from odibi.recipes import _substitute_recipe_vars

        assert _substitute_recipe_vars(None, {}, "r") is None

    def test_non_string_passthrough_bool(self):
        from odibi.recipes import _substitute_recipe_vars

        assert _substitute_recipe_vars(True, {}, "r") is True

    def test_non_string_passthrough_float(self):
        from odibi.recipes import _substitute_recipe_vars

        assert _substitute_recipe_vars(3.14, {}, "r") == 3.14

    def test_no_placeholders_returns_string_unchanged(self):
        from odibi.recipes import _substitute_recipe_vars

        assert _substitute_recipe_vars("hello world", {}, "r") == "hello world"


class TestDeepMergeRecipe:
    """Tests for _deep_merge_recipe."""

    def test_flat_dict_merge_node_wins(self):
        from odibi.recipes import _deep_merge_recipe

        recipe = {"transformer": "dedup", "mode": "first"}
        node = {"mode": "last"}
        result = _deep_merge_recipe(recipe, node)
        assert result == {"transformer": "dedup", "mode": "last"}

    def test_nested_dict_merge_recursive(self):
        from odibi.recipes import _deep_merge_recipe

        recipe = {"params": {"keys": ["a"], "order": "asc"}}
        node = {"params": {"order": "desc"}}
        result = _deep_merge_recipe(recipe, node)
        assert result == {"params": {"keys": ["a"], "order": "desc"}}

    def test_list_override_node_replaces_entirely(self):
        from odibi.recipes import _deep_merge_recipe

        recipe = {"steps": [1, 2, 3]}
        node = {"steps": [4, 5]}
        result = _deep_merge_recipe(recipe, node)
        assert result == {"steps": [4, 5]}

    def test_node_adds_new_keys(self):
        from odibi.recipes import _deep_merge_recipe

        recipe = {"transformer": "dedup"}
        node = {"extra": "value"}
        result = _deep_merge_recipe(recipe, node)
        assert result == {"transformer": "dedup", "extra": "value"}

    def test_recipe_provides_defaults(self):
        from odibi.recipes import _deep_merge_recipe

        recipe = {"transformer": "dedup", "params": {"keys": ["id"]}}
        node = {}
        result = _deep_merge_recipe(recipe, node)
        assert result == {"transformer": "dedup", "params": {"keys": ["id"]}}

    def test_does_not_mutate_inputs(self):
        from odibi.recipes import _deep_merge_recipe

        recipe = {"params": {"keys": ["a"]}}
        node = {"params": {"keys": ["b"]}}
        recipe_copy = copy.deepcopy(recipe)
        node_copy = copy.deepcopy(node)
        _deep_merge_recipe(recipe, node)
        assert recipe == recipe_copy
        assert node == node_copy

    def test_deeply_nested_merge(self):
        from odibi.recipes import _deep_merge_recipe

        recipe = {"read": {"options": {"retry": {"max": 3, "backoff": 1.0}}}}
        node = {"read": {"options": {"retry": {"max": 5}}}}
        result = _deep_merge_recipe(recipe, node)
        assert result == {"read": {"options": {"retry": {"max": 5, "backoff": 1.0}}}}


class TestResolveRecipes:
    """Tests for resolve_recipes - the main entry point."""

    def test_no_recipes_no_references_passthrough(self):
        from odibi.recipes import resolve_recipes

        config = {
            "pipelines": [
                {
                    "pipeline": "p1",
                    "nodes": [
                        {"name": "n1", "transformer": "noop"},
                    ],
                }
            ]
        }
        result = resolve_recipes(config)
        assert result["pipelines"][0]["nodes"][0]["name"] == "n1"
        assert result["pipelines"][0]["nodes"][0]["transformer"] == "noop"

    def test_builtin_recipes_loaded(self):
        from odibi.recipes import RecipeRegistry

        registry = RecipeRegistry()
        registry.load_builtins()
        recipes = registry.list_recipes()
        assert len(recipes) > 0
        assert "silver_dedup" in recipes

    def test_inline_recipes_loaded(self):
        from odibi.recipes import resolve_recipes

        config = {
            "recipes": {
                "my_noop": {
                    "description": "Does nothing",
                    "template": {"transformer": "noop"},
                }
            },
            "pipelines": [
                {
                    "pipeline": "p1",
                    "nodes": [
                        {"name": "n1", "recipe": "my_noop"},
                    ],
                }
            ],
        }
        result = resolve_recipes(config)
        assert result["pipelines"][0]["nodes"][0]["transformer"] == "noop"

    def test_inline_recipes_override_builtins(self):
        from odibi.recipes import resolve_recipes

        config = {
            "recipes": {
                "silver_dedup": {
                    "description": "Custom override",
                    "template": {"transformer": "custom_dedup"},
                }
            },
            "pipelines": [
                {
                    "pipeline": "p1",
                    "nodes": [
                        {"name": "n1", "recipe": "silver_dedup"},
                    ],
                }
            ],
        }
        result = resolve_recipes(config)
        assert result["pipelines"][0]["nodes"][0]["transformer"] == "custom_dedup"

    def test_recipes_key_removed_from_output(self):
        from odibi.recipes import resolve_recipes

        config = {
            "recipes": {
                "my_noop": {"template": {"transformer": "noop"}},
            },
            "pipelines": [
                {"pipeline": "p1", "nodes": [{"name": "n1", "recipe": "my_noop"}]},
            ],
        }
        result = resolve_recipes(config)
        assert "recipes" not in result

    def test_node_with_recipe_gets_expanded(self):
        from odibi.recipes import resolve_recipes

        config = {
            "recipes": {
                "my_dedup": {
                    "required_vars": ["keys"],
                    "template": {
                        "transformer": "deduplicate",
                        "params": {"keys": "${recipe.keys}"},
                    },
                }
            },
            "pipelines": [
                {
                    "pipeline": "p1",
                    "nodes": [
                        {
                            "name": "dedup_users",
                            "recipe": "my_dedup",
                            "recipe_vars": {"keys": ["user_id"]},
                        }
                    ],
                }
            ],
        }
        result = resolve_recipes(config)
        node = result["pipelines"][0]["nodes"][0]
        assert node["name"] == "dedup_users"
        assert node["transformer"] == "deduplicate"
        assert node["params"]["keys"] == ["user_id"]
        assert "recipe" not in node
        assert "recipe_vars" not in node

    def test_node_overrides_recipe_fields(self):
        from odibi.recipes import resolve_recipes

        config = {
            "recipes": {
                "base": {
                    "template": {
                        "transformer": "dedup",
                        "write": {"mode": "overwrite", "format": "parquet"},
                    },
                }
            },
            "pipelines": [
                {
                    "pipeline": "p1",
                    "nodes": [
                        {
                            "name": "n1",
                            "recipe": "base",
                            "write": {"mode": "append"},
                        },
                    ],
                }
            ],
        }
        result = resolve_recipes(config)
        node = result["pipelines"][0]["nodes"][0]
        assert node["write"]["mode"] == "append"
        assert node["write"]["format"] == "parquet"

    def test_deep_merge_of_nested_configs(self):
        from odibi.recipes import resolve_recipes

        config = {
            "recipes": {
                "api_load": {
                    "template": {
                        "read": {
                            "format": "api",
                            "options": {
                                "retry": {"max_retries": 3, "backoff": 2.0},
                                "timeout": 30,
                            },
                        }
                    },
                }
            },
            "pipelines": [
                {
                    "pipeline": "p1",
                    "nodes": [
                        {
                            "name": "n1",
                            "recipe": "api_load",
                            "read": {"options": {"retry": {"max_retries": 5}}},
                        },
                    ],
                }
            ],
        }
        result = resolve_recipes(config)
        node = result["pipelines"][0]["nodes"][0]
        assert node["read"]["format"] == "api"
        assert node["read"]["options"]["retry"]["max_retries"] == 5
        assert node["read"]["options"]["retry"]["backoff"] == 2.0
        assert node["read"]["options"]["timeout"] == 30

    def test_missing_required_var_raises(self):
        from odibi.recipes import resolve_recipes

        config = {
            "recipes": {
                "needs_keys": {
                    "required_vars": ["keys"],
                    "template": {"params": {"keys": "${recipe.keys}"}},
                }
            },
            "pipelines": [
                {
                    "pipeline": "p1",
                    "nodes": [
                        {"name": "n1", "recipe": "needs_keys", "recipe_vars": {}},
                    ],
                }
            ],
        }
        with pytest.raises(ValueError, match="requires variables"):
            resolve_recipes(config)

    def test_unknown_recipe_raises_with_available_list(self):
        from odibi.recipes import resolve_recipes

        config = {
            "recipes": {
                "known_recipe": {"template": {"transformer": "noop"}},
            },
            "pipelines": [
                {
                    "pipeline": "p1",
                    "nodes": [
                        {"name": "n1", "recipe": "nonexistent"},
                    ],
                }
            ],
        }
        with pytest.raises(ValueError, match="Unknown recipe 'nonexistent'"):
            resolve_recipes(config)

    def test_unknown_recipe_error_lists_available(self):
        from odibi.recipes import resolve_recipes

        config = {
            "recipes": {
                "my_recipe": {"template": {"transformer": "noop"}},
            },
            "pipelines": [
                {
                    "pipeline": "p1",
                    "nodes": [{"name": "n1", "recipe": "bad_name"}],
                }
            ],
        }
        with pytest.raises(ValueError, match="my_recipe"):
            resolve_recipes(config)

    def test_multiple_nodes_different_recipes(self):
        from odibi.recipes import resolve_recipes

        config = {
            "recipes": {
                "recipe_a": {"template": {"transformer": "a_transform"}},
                "recipe_b": {
                    "required_vars": ["x"],
                    "template": {"transformer": "b_transform", "params": {"x": "${recipe.x}"}},
                },
            },
            "pipelines": [
                {
                    "pipeline": "p1",
                    "nodes": [
                        {"name": "n1", "recipe": "recipe_a"},
                        {
                            "name": "n2",
                            "recipe": "recipe_b",
                            "recipe_vars": {"x": "hello"},
                        },
                    ],
                }
            ],
        }
        result = resolve_recipes(config)
        nodes = result["pipelines"][0]["nodes"]
        assert nodes[0]["transformer"] == "a_transform"
        assert nodes[1]["transformer"] == "b_transform"
        assert nodes[1]["params"]["x"] == "hello"

    def test_node_without_recipe_left_unchanged(self):
        from odibi.recipes import resolve_recipes

        config = {
            "recipes": {
                "some_recipe": {"template": {"transformer": "noop"}},
            },
            "pipelines": [
                {
                    "pipeline": "p1",
                    "nodes": [
                        {"name": "plain_node", "transformer": "custom", "params": {"a": 1}},
                        {"name": "recipe_node", "recipe": "some_recipe"},
                    ],
                }
            ],
        }
        result = resolve_recipes(config)
        nodes = result["pipelines"][0]["nodes"]
        assert nodes[0] == {"name": "plain_node", "transformer": "custom", "params": {"a": 1}}
        assert nodes[1]["transformer"] == "noop"

    def test_optional_vars_defaults_used_when_not_provided(self):
        from odibi.recipes import resolve_recipes

        config = {
            "recipes": {
                "with_defaults": {
                    "required_vars": ["key"],
                    "optional_vars": {"order": "asc", "limit": 100},
                    "template": {
                        "params": {
                            "key": "${recipe.key}",
                            "order": "${recipe.order}",
                            "limit": "${recipe.limit}",
                        }
                    },
                }
            },
            "pipelines": [
                {
                    "pipeline": "p1",
                    "nodes": [
                        {
                            "name": "n1",
                            "recipe": "with_defaults",
                            "recipe_vars": {"key": "id"},
                        },
                    ],
                }
            ],
        }
        result = resolve_recipes(config)
        node = result["pipelines"][0]["nodes"][0]
        assert node["params"]["key"] == "id"
        assert node["params"]["order"] == "asc"
        assert node["params"]["limit"] == 100

    def test_optional_vars_overridden_by_user(self):
        from odibi.recipes import resolve_recipes

        config = {
            "recipes": {
                "with_defaults": {
                    "optional_vars": {"mode": "overwrite"},
                    "template": {"write": {"mode": "${recipe.mode}"}},
                }
            },
            "pipelines": [
                {
                    "pipeline": "p1",
                    "nodes": [
                        {
                            "name": "n1",
                            "recipe": "with_defaults",
                            "recipe_vars": {"mode": "append"},
                        },
                    ],
                }
            ],
        }
        result = resolve_recipes(config)
        assert result["pipelines"][0]["nodes"][0]["write"]["mode"] == "append"

    def test_does_not_mutate_original_config(self):
        from odibi.recipes import resolve_recipes

        config = {
            "recipes": {
                "r": {"template": {"transformer": "noop"}},
            },
            "pipelines": [
                {"pipeline": "p1", "nodes": [{"name": "n1", "recipe": "r"}]},
            ],
        }
        config_copy = copy.deepcopy(config)
        resolve_recipes(config)
        assert config == config_copy

    def test_empty_pipelines_list(self):
        from odibi.recipes import resolve_recipes

        config = {"pipelines": []}
        result = resolve_recipes(config)
        assert result == {"pipelines": []}

    def test_no_pipelines_key(self):
        from odibi.recipes import resolve_recipes

        config = {"project": "test"}
        result = resolve_recipes(config)
        assert result == {"project": "test"}


class TestRecipeRegistry:
    """Tests for RecipeRegistry class."""

    def test_register_and_get(self):
        from odibi.recipes import RecipeConfig, RecipeRegistry

        registry = RecipeRegistry()
        recipe = RecipeConfig(template={"transformer": "noop"})
        registry.register("my_recipe", recipe)
        assert registry.get("my_recipe") is recipe

    def test_get_unknown_returns_none(self):
        from odibi.recipes import RecipeRegistry

        registry = RecipeRegistry()
        assert registry.get("nonexistent") is None

    def test_list_recipes_empty(self):
        from odibi.recipes import RecipeRegistry

        registry = RecipeRegistry()
        assert registry.list_recipes() == {}

    def test_list_recipes_returns_copy(self):
        from odibi.recipes import RecipeConfig, RecipeRegistry

        registry = RecipeRegistry()
        recipe = RecipeConfig(template={})
        registry.register("r", recipe)
        listing = registry.list_recipes()
        listing["extra"] = "bad"
        assert "extra" not in registry.list_recipes()

    def test_load_inline_valid(self):
        from odibi.recipes import RecipeRegistry

        registry = RecipeRegistry()
        registry.load_inline(
            {
                "my_recipe": {
                    "description": "test",
                    "template": {"transformer": "noop"},
                }
            }
        )
        assert registry.get("my_recipe") is not None
        assert registry.get("my_recipe").description == "test"

    def test_load_inline_invalid_raises(self):
        from odibi.recipes import RecipeRegistry

        registry = RecipeRegistry()
        with pytest.raises(ValueError, match="Invalid recipe definition"):
            registry.load_inline(
                {
                    "bad_recipe": {"template": {"name": "reserved_field"}},
                }
            )

    def test_load_inline_overrides_existing(self):
        from odibi.recipes import RecipeConfig, RecipeRegistry

        registry = RecipeRegistry()
        original = RecipeConfig(template={"transformer": "original"})
        registry.register("r", original)

        registry.load_inline(
            {
                "r": {"template": {"transformer": "override"}},
            }
        )
        assert registry.get("r").template["transformer"] == "override"


class TestBuiltinRecipes:
    """Tests for built-in recipe loading and structure."""

    def test_get_recipe_registry_loads_builtins(self):
        from odibi.recipes import get_recipe_registry

        registry = get_recipe_registry()
        recipes = registry.list_recipes()
        assert len(recipes) > 0

    def test_all_builtins_have_valid_config(self):
        from odibi.recipes import RecipeConfig, get_recipe_registry

        registry = get_recipe_registry()
        for name, recipe in registry.list_recipes().items():
            assert isinstance(recipe, RecipeConfig), f"{name} is not a RecipeConfig"
            assert isinstance(recipe.template, dict), f"{name} has non-dict template"

    def test_silver_dedup_resolves(self):
        from odibi.recipes import resolve_recipes

        config = {
            "pipelines": [
                {
                    "pipeline": "p1",
                    "nodes": [
                        {
                            "name": "dedup_node",
                            "recipe": "silver_dedup",
                            "recipe_vars": {
                                "keys": ["user_id"],
                                "order_by": "updated_at DESC",
                            },
                        },
                    ],
                }
            ],
        }
        result = resolve_recipes(config)
        node = result["pipelines"][0]["nodes"][0]
        assert node["transformer"] == "deduplicate"
        assert node["params"]["keys"] == ["user_id"]
        assert node["params"]["order_by"] == "updated_at DESC"

    def test_api_bronze_load_deep_merge(self):
        from odibi.recipes import resolve_recipes

        config = {
            "pipelines": [
                {
                    "pipeline": "p1",
                    "nodes": [
                        {
                            "name": "api_node",
                            "recipe": "api_bronze_load",
                            "recipe_vars": {"source_name": "weather_api"},
                            "read": {
                                "connection": "my_api",
                                "options": {
                                    "http": {"timeout_s": 60},
                                },
                            },
                        },
                    ],
                }
            ],
        }
        result = resolve_recipes(config)
        node = result["pipelines"][0]["nodes"][0]
        assert node["read"]["format"] == "api"
        assert node["read"]["connection"] == "my_api"
        assert node["read"]["options"]["http"]["timeout_s"] == 60
        assert "retry" in node["read"]["options"]
        assert node["write"]["format"] == "parquet"

    def test_builtin_expected_recipes_present(self):
        from odibi.recipes import get_recipe_registry

        registry = get_recipe_registry()
        recipes = registry.list_recipes()
        expected = [
            "silver_dedup",
            "silver_clean",
            "silver_merge",
            "silver_scd2",
            "api_bronze_load",
            "csv_bronze_load",
            "gold_aggregate",
        ]
        for name in expected:
            assert name in recipes, f"Missing built-in recipe: {name}"

    def test_silver_dedup_missing_required_var_raises(self):
        from odibi.recipes import resolve_recipes

        config = {
            "pipelines": [
                {
                    "pipeline": "p1",
                    "nodes": [
                        {
                            "name": "n1",
                            "recipe": "silver_dedup",
                            "recipe_vars": {"keys": ["id"]},
                            # missing order_by
                        },
                    ],
                }
            ],
        }
        with pytest.raises(ValueError, match="requires variables"):
            resolve_recipes(config)


class TestRecipeInheritance:
    """Tests for recipe inheritance via the 'extends' field."""

    def test_simple_extends(self):
        """Child recipe inherits parent template fields."""
        from odibi.recipes import resolve_recipes

        config = {
            "recipes": {
                "base": {"template": {"transformer": "noop", "write": {"mode": "overwrite"}}},
                "child": {"extends": "base", "template": {"write": {"mode": "append"}}},
            },
            "pipelines": [{"pipeline": "p1", "nodes": [{"name": "n1", "recipe": "child"}]}],
        }
        result = resolve_recipes(config)
        node = result["pipelines"][0]["nodes"][0]
        assert node["transformer"] == "noop"  # inherited
        assert node["write"]["mode"] == "append"  # overridden

    def test_extends_inherits_vars(self):
        """Child inherits required_vars and optional_vars from parent."""
        from odibi.recipes import resolve_recipes

        config = {
            "recipes": {
                "base": {
                    "required_vars": ["keys"],
                    "optional_vars": {"mode": "overwrite"},
                    "template": {
                        "params": {"keys": "${recipe.keys}"},
                        "write": {"mode": "${recipe.mode}"},
                    },
                },
                "child": {
                    "extends": "base",
                    "required_vars": ["target"],
                    "template": {"params": {"target": "${recipe.target}"}},
                },
            },
            "pipelines": [
                {
                    "pipeline": "p1",
                    "nodes": [
                        {
                            "name": "n1",
                            "recipe": "child",
                            "recipe_vars": {"keys": ["id"], "target": "table1"},
                        }
                    ],
                }
            ],
        }
        result = resolve_recipes(config)
        node = result["pipelines"][0]["nodes"][0]
        assert node["params"]["keys"] == ["id"]
        assert node["params"]["target"] == "table1"
        assert node["write"]["mode"] == "overwrite"

    def test_circular_extends_raises(self):
        """Circular inheritance raises ValueError."""
        from odibi.recipes import resolve_recipes

        config = {
            "recipes": {
                "a": {"extends": "b", "template": {}},
                "b": {"extends": "a", "template": {}},
            },
            "pipelines": [{"pipeline": "p1", "nodes": [{"name": "n1", "recipe": "a"}]}],
        }
        with pytest.raises(ValueError, match="[Cc]ircular"):
            resolve_recipes(config)

    def test_multi_level_extends(self):
        """A extends B extends C works correctly."""
        from odibi.recipes import resolve_recipes

        config = {
            "recipes": {
                "grandparent": {
                    "template": {"read": {"format": "csv"}},
                    "optional_vars": {"fmt": "csv"},
                },
                "parent": {"extends": "grandparent", "template": {"write": {"mode": "overwrite"}}},
                "child": {"extends": "parent", "template": {"transformer": "noop"}},
            },
            "pipelines": [{"pipeline": "p1", "nodes": [{"name": "n1", "recipe": "child"}]}],
        }
        result = resolve_recipes(config)
        node = result["pipelines"][0]["nodes"][0]
        assert node["read"]["format"] == "csv"
        assert node["write"]["mode"] == "overwrite"
        assert node["transformer"] == "noop"

    def test_extends_unknown_parent_raises(self):
        """Extending an unknown recipe raises ValueError."""
        from odibi.recipes import resolve_recipes

        config = {
            "recipes": {
                "child": {"extends": "nonexistent", "template": {}},
            },
            "pipelines": [{"pipeline": "p1", "nodes": [{"name": "n1", "recipe": "child"}]}],
        }
        with pytest.raises(ValueError, match="nonexistent"):
            resolve_recipes(config)


class TestValidateRecipeIntegration:
    """Tests for recipe validation integration in odibi validate."""

    def test_validate_catches_unknown_recipe(self):
        from odibi.validate.pipeline import validate_yaml
        import yaml

        config = {
            "pipelines": [
                {
                    "pipeline": "p1",
                    "nodes": [{"name": "n1", "recipe": "nonexistent_recipe", "recipe_vars": {}}],
                }
            ],
        }
        result = validate_yaml(yaml.dump(config))
        assert not result["valid"]
        assert any("RECIPE_ERROR" in e.get("code", "") for e in result["errors"])

    def test_validate_catches_missing_required_var(self):
        from odibi.validate.pipeline import validate_yaml
        import yaml

        config = {
            "pipelines": [
                {
                    "pipeline": "p1",
                    "nodes": [
                        {
                            "name": "n1",
                            "recipe": "silver_dedup",
                            "recipe_vars": {"keys": ["id"]},
                            # missing order_by
                        }
                    ],
                }
            ],
        }
        result = validate_yaml(yaml.dump(config))
        assert not result["valid"]
        assert any("RECIPE_ERROR" in e.get("code", "") for e in result["errors"])
