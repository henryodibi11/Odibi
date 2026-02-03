from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from typing import Any, Dict

import yaml

from odibi.utils.logging import logger

# Pattern to match ${VAR} or ${env:VAR}
# Captures the variable name in group 1
ENV_PATTERN = re.compile(r"\$\{(?:env:)?([A-Za-z0-9_]+)\}")

# Pattern to match ${vars.xxx}
VARS_PATTERN = re.compile(r"\$\{vars\.([A-Za-z0-9_]+)\}")

# Pattern to match ${date:expression} or ${date:expression:format}
# Examples: ${date:today}, ${date:-7d}, ${date:now:%Y-%m-%d %H:%M:%S}
DATE_PATTERN = re.compile(r"\$\{date:([^}:]+)(?::([^}]+))?\}")


def _resolve_date_expression(expression: str, fmt: str | None = None) -> str:
    """Resolve a date expression to a formatted string.

    Supported expressions:
    - now: Current datetime
    - today: Today at midnight
    - yesterday: Yesterday at midnight
    - start_of_month: First day of current month
    - end_of_month: Last day of current month
    - start_of_year: First day of current year
    - Relative: -7d, +30d, -1w, +2w, -1m, +3m, -1y, +1y

    Args:
        expression: The date expression (e.g., "today", "-7d", "start_of_month")
        fmt: Optional strftime format string (default: "%Y-%m-%d")

    Returns:
        Formatted date string
    """
    now = datetime.now()
    default_fmt = "%Y-%m-%d"

    # Named expressions
    named_dates = {
        "now": now,
        "today": now.replace(hour=0, minute=0, second=0, microsecond=0),
        "yesterday": now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1),
        "start_of_month": now.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
        "start_of_year": now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0),
    }

    # Handle end_of_month specially
    if expression == "end_of_month":
        # Get first day of next month, then subtract 1 day
        if now.month == 12:
            next_month = now.replace(year=now.year + 1, month=1, day=1)
        else:
            next_month = now.replace(month=now.month + 1, day=1)
        result = next_month - timedelta(days=1)
        result = result.replace(hour=0, minute=0, second=0, microsecond=0)
        return result.strftime(fmt or default_fmt)

    if expression in named_dates:
        result = named_dates[expression]
        # For "now", use datetime format by default
        if expression == "now" and fmt is None:
            return result.strftime("%Y-%m-%d %H:%M:%S")
        return result.strftime(fmt or default_fmt)

    # Relative expressions: -7d, +30d, -1w, +2w, -1m, +3m, -1y, +1y
    relative_match = re.match(r"^([+-]?\d+)([dwmy])$", expression)
    if relative_match:
        amount = int(relative_match.group(1))
        unit = relative_match.group(2)
        base = now.replace(hour=0, minute=0, second=0, microsecond=0)

        if unit == "d":
            result = base + timedelta(days=amount)
        elif unit == "w":
            result = base + timedelta(weeks=amount)
        elif unit == "m":
            # Approximate month calculation
            new_month = base.month + amount
            new_year = base.year + (new_month - 1) // 12
            new_month = ((new_month - 1) % 12) + 1
            # Handle day overflow (e.g., Jan 31 + 1 month)
            try:
                result = base.replace(year=new_year, month=new_month)
            except ValueError:
                # Day doesn't exist in target month, use last day
                if new_month == 12:
                    next_m = base.replace(year=new_year + 1, month=1, day=1)
                else:
                    next_m = base.replace(year=new_year, month=new_month + 1, day=1)
                result = next_m - timedelta(days=1)
        elif unit == "y":
            try:
                result = base.replace(year=base.year + amount)
            except ValueError:
                # Handle Feb 29 on non-leap year
                result = base.replace(year=base.year + amount, day=28)
        else:
            result = base

        return result.strftime(fmt or default_fmt)

    # Unknown expression, return as-is with warning
    logger.warning("Unknown date expression", expression=expression)
    return expression


def _substitute_dates(data: Any) -> Any:
    """Recursively substitute ${date:...} placeholders with resolved dates.

    Args:
        data: The data structure to process (dict, list, or scalar)

    Returns:
        Data with all ${date:...} placeholders replaced
    """
    if isinstance(data, dict):
        return {k: _substitute_dates(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [_substitute_dates(item) for item in data]
    elif isinstance(data, str):

        def replace_date(match):
            expression = match.group(1)
            fmt = match.group(2)  # May be None
            resolved = _resolve_date_expression(expression, fmt)
            logger.debug(
                "Date variable substituted",
                expression=expression,
                format=fmt,
                result=resolved,
            )
            return resolved

        return DATE_PATTERN.sub(replace_date, data)
    else:
        return data


def _substitute_vars(data: Any, vars_dict: Dict[str, Any]) -> Any:
    """Recursively substitute ${vars.xxx} placeholders with values from vars dict.

    Args:
        data: The data structure to process (dict, list, or scalar)
        vars_dict: Dictionary of variable names to values

    Returns:
        Data with all ${vars.xxx} placeholders replaced
    """
    if isinstance(data, dict):
        return {k: _substitute_vars(v, vars_dict) for k, v in data.items()}
    elif isinstance(data, list):
        return [_substitute_vars(item, vars_dict) for item in data]
    elif isinstance(data, str):

        def replace_var(match):
            var_name = match.group(1)
            if var_name in vars_dict:
                value = vars_dict[var_name]
                # If the entire string is just the variable, return the raw value (preserves type)
                if match.group(0) == data:
                    return value
                return str(value)
            else:
                logger.warning(
                    "Undefined variable referenced",
                    variable=f"vars.{var_name}",
                    available_vars=list(vars_dict.keys()),
                )
                return ""  # Return empty string for undefined vars

        return VARS_PATTERN.sub(replace_var, data)
    else:
        return data


def _normalize_pattern_to_transformer(data: Dict[str, Any]) -> None:
    """Normalize 'pattern:' block to 'transformer:' + 'params:' fields.

    The documentation uses `pattern: type: X` syntax, but the node executor
    reads `transformer:` and `params:`. This function converts the user-friendly
    syntax to the internal representation.

    Example:
        pattern:
          type: dimension
          params:
            natural_key: customer_id

        Becomes:
          transformer: dimension
          params:
            natural_key: customer_id
    """
    pipelines = data.get("pipelines", [])
    for pipeline in pipelines:
        if isinstance(pipeline, dict):
            nodes = pipeline.get("nodes", [])
            for node in nodes:
                if isinstance(node, dict) and "pattern" in node:
                    pattern_block = node.pop("pattern")
                    if isinstance(pattern_block, dict):
                        pattern_type = pattern_block.get("type")
                        pattern_params = pattern_block.get("params", {})
                        if pattern_type:
                            node["transformer"] = pattern_type
                        if pattern_params:
                            existing_params = node.get("params", {})
                            node["params"] = {**existing_params, **pattern_params}


def _tag_nodes_with_source(data: Dict[str, Any], source_path: str) -> None:
    """Tag all nodes in pipelines with their source YAML file path.

    This enables sql_file resolution to work correctly when pipelines are imported
    from different directories.
    """
    pipelines = data.get("pipelines", [])
    for pipeline in pipelines:
        if isinstance(pipeline, dict):
            nodes = pipeline.get("nodes", [])
            for node in nodes:
                if isinstance(node, dict) and "_source_yaml" not in node:
                    node["_source_yaml"] = source_path


def _merge_semantic_config(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Merge semantic layer configs by appending lists.

    Semantic configs have list fields (metrics, dimensions, views, materializations)
    that should be accumulated from multiple imports.
    """
    result = base.copy()
    list_keys = ["metrics", "dimensions", "views", "materializations"]

    for key, value in override.items():
        if key in list_keys and isinstance(value, list):
            if key in result and isinstance(result[key], list):
                logger.debug(
                    "Appending semantic list",
                    key=key,
                    existing_count=len(result[key]),
                    new_count=len(value),
                )
                result[key] = result[key] + value
            else:
                result[key] = value
        elif isinstance(value, dict) and key in result and isinstance(result[key], dict):
            result[key] = _merge_semantic_config(result[key], value)
        else:
            result[key] = value

    return result


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Merge override dictionary into base dictionary.

    Rules:
    1. Dicts are merged recursively.
    2. List 'pipelines' are appended.
    3. Semantic config lists (metrics, dimensions, views, materializations) are appended.
    4. Other types (and other lists) are overwritten by the override.
    """
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            if key == "semantic":
                logger.debug(
                    "Merging semantic config",
                    base_keys=list(result[key].keys()) if result[key] else [],
                    override_keys=list(value.keys()),
                )
                result[key] = _merge_semantic_config(result[key], value)
            else:
                logger.debug(
                    "Deep merging nested dictionary",
                    key=key,
                    base_keys=list(result[key].keys()),
                    override_keys=list(value.keys()),
                )
                result[key] = _deep_merge(result[key], value)
        elif key == "pipelines" and isinstance(value, list) and isinstance(result.get(key), list):
            logger.debug(
                "Appending pipelines list",
                existing_count=len(result[key]),
                new_count=len(value),
            )
            result[key] = result[key] + value
        else:
            if key in result:
                logger.debug("Overwriting key during merge", key=key)
            result[key] = value
    return result


def load_yaml_with_env(path: str, env: str = None) -> Dict[str, Any]:
    """Load YAML file with environment variable substitution and imports.

    Supports:
    - ${VAR_NAME} substitution
    - 'imports' list of relative paths
    - 'environments' overrides based on env param

    Args:
        path: Path to YAML file
        env: Environment name (e.g., 'prod', 'dev') to apply overrides

    Returns:
        Parsed dictionary (merged with imports and env overrides)

    Raises:
        FileNotFoundError: If file does not exist
        ValueError: If environment variable is missing
        yaml.YAMLError: If YAML parsing fails
    """
    logger.debug("Loading YAML configuration", path=path, env=env)

    if not os.path.exists(path):
        logger.error("Configuration file not found", path=path)
        raise FileNotFoundError(f"YAML file not found: {path}")

    # Get absolute path for relative import resolution
    abs_path = os.path.abspath(path)
    base_dir = os.path.dirname(abs_path)

    logger.debug("Reading configuration file", absolute_path=abs_path)

    with open(abs_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Debug: Log first 100 chars to detect encoding/BOM issues
    logger.debug(
        "File content loaded",
        file_size=len(content),
        first_100_repr=repr(content[:100]),
    )

    env_vars_found = []

    def replace_env(match):
        var_name = match.group(1)
        env_vars_found.append(var_name)
        value = os.environ.get(var_name)
        if value is None:
            logger.error(
                "Missing required environment variable",
                variable=var_name,
                file=abs_path,
            )
            raise ValueError(
                f"Missing environment variable: {var_name}\n"
                "Tip: Check your .env file or environment setup. See docs/ODIBI_DEEP_CONTEXT.md#env for help."
            )
        # Check for problematic characters that could break YAML
        # Note: Colons are NOT checked because URLs (https://) are common and safe
        # when the value is substituted into a quoted YAML string
        if any(c in value for c in ["\n", "\r"]):
            logger.warning(
                "Environment variable contains newline characters",
                variable=var_name,
            )
        elif "#" in value:
            logger.debug(
                "Environment variable contains # (handled correctly)",
                variable=var_name,
            )
        logger.debug("Environment variable substituted", variable=var_name, length=len(value))
        return value

    # Substitute variables
    substituted_content = ENV_PATTERN.sub(replace_env, content)

    if env_vars_found:
        logger.debug(
            "Environment variable substitution complete",
            variables_substituted=env_vars_found,
            count=len(env_vars_found),
        )

    # Parse YAML
    logger.debug("Parsing YAML content", path=abs_path)
    try:
        data = yaml.safe_load(substituted_content) or {}
    except yaml.YAMLError as e:
        logger.error("YAML parsing failed", path=abs_path, error=str(e))
        raise ValueError(
            f"YAML parsing failed for file '{abs_path}': {e}\n"
            "Common causes: indentation errors, missing colons, tabs instead of spaces, or invalid YAML structure.\n"
            "Tip: Validate your YAML with an online linter or see docs/ODIBI_DEEP_CONTEXT.md#yaml for examples."
        ) from e

    logger.debug("YAML parsed successfully", top_level_keys=list(data.keys()))

    # Normalize pattern: blocks to transformer: + params: (user-friendly -> internal)
    _normalize_pattern_to_transformer(data)

    # Tag all nodes in this file with their source YAML path (for sql_file resolution)
    _tag_nodes_with_source(data, abs_path)

    # Handle imports
    imports = data.pop("imports", [])
    if imports:
        if isinstance(imports, str):
            imports = [imports]

        logger.debug("Processing imports", import_count=len(imports), imports=imports)

        # Start with current data as the base
        merged_data = data.copy()

        for import_path in imports:
            # Resolve relative to current file
            if not os.path.isabs(import_path):
                full_import_path = os.path.join(base_dir, import_path)
            else:
                full_import_path = import_path

            logger.debug(
                "Resolving import",
                import_path=import_path,
                resolved_path=full_import_path,
            )

            if not os.path.exists(full_import_path):
                logger.error(
                    "Imported configuration file not found",
                    import_path=import_path,
                    resolved_path=full_import_path,
                    parent_file=abs_path,
                )
                raise FileNotFoundError(f"Imported YAML file not found: {full_import_path}")

            # Recursive load
            # Note: We pass env down to imported files too
            logger.debug("Loading imported configuration", path=full_import_path)
            try:
                imported_data = load_yaml_with_env(full_import_path, env=env)
            except Exception as e:
                logger.error(
                    "Failed to load imported configuration",
                    import_path=import_path,
                    resolved_path=full_import_path,
                    parent_file=abs_path,
                    error=str(e),
                )
                raise ValueError(
                    f"Failed to load import '{import_path}' (resolved: {full_import_path}): {e}"
                ) from e

            # Merge imported data INTO the current data
            # This way, the main file acts as the "master" that accumulates imports
            logger.debug(
                "Merging imported configuration",
                import_path=full_import_path,
                imported_keys=list(imported_data.keys()),
            )
            merged_data = _deep_merge(merged_data, imported_data)

        data = merged_data
        logger.debug("All imports processed and merged", import_count=len(imports))

    # Apply Environment Overrides from "environments" block in main file
    if env:
        environments = data.get("environments", {})
        if env in environments:
            logger.debug(
                "Applying environment overrides from environments block",
                env=env,
                override_keys=list(environments[env].keys()),
            )
            override = environments[env]
            data = _deep_merge(data, override)
        else:
            logger.debug(
                "No environment override found in environments block",
                env=env,
                available_environments=list(environments.keys()),
            )

    # Apply Environment Overrides from external env.{env}.yaml file
    if env:
        env_file_name = f"env.{env}.yaml"
        env_file_path = os.path.join(base_dir, env_file_name)
        if os.path.exists(env_file_path):
            logger.debug(
                "Loading external environment override file",
                env=env,
                env_file=env_file_path,
            )
            # Load the env file (recursively, so it can have imports too)
            # We pass env=None to avoid infinite recursion if it somehow references itself,
            # though strictly it shouldn't matter as we look for env.{env}.yaml based on the passed env.
            # But logically, an env specific file shouldn't load other env specific files for the same env.
            env_data = load_yaml_with_env(env_file_path, env=None)
            logger.debug(
                "Merging external environment overrides",
                env_file=env_file_path,
                override_keys=list(env_data.keys()),
            )
            data = _deep_merge(data, env_data)
        else:
            logger.debug(
                "No external environment override file found",
                env=env,
                expected_path=env_file_path,
            )

    # Apply vars substitution after all merges are complete
    vars_dict = data.get("vars", {})
    if vars_dict:
        logger.debug(
            "Applying vars substitution",
            vars_keys=list(vars_dict.keys()),
        )
        data = _substitute_vars(data, vars_dict)

    # Apply date substitution (${date:...} syntax)
    data = _substitute_dates(data)

    logger.debug(
        "Configuration loading complete",
        path=path,
        env=env,
        final_keys=list(data.keys()),
    )

    return data
