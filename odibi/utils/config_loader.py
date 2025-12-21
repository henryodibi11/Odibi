import os
import re
from typing import Any, Dict

import yaml

from odibi.utils.logging import logger

# Pattern to match ${VAR} or ${env:VAR}
# Captures the variable name in group 1
ENV_PATTERN = re.compile(r"\$\{(?:env:)?([A-Za-z0-9_]+)\}")


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Merge override dictionary into base dictionary.

    Rules:
    1. Dicts are merged recursively.
    2. List 'pipelines' are appended.
    3. Other types (and other lists) are overwritten by the override.
    """
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
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
            raise ValueError(f"Missing environment variable: {var_name}")
        # Check for problematic characters that could break YAML
        if any(c in value for c in ["\n", "\r", ":", "#"]):
            logger.warning(
                "Environment variable contains YAML-sensitive characters",
                variable=var_name,
                has_newline="\n" in value or "\r" in value,
                has_colon=":" in value,
                has_hash="#" in value,
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
        raise

    logger.debug("YAML parsed successfully", top_level_keys=list(data.keys()))

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

    logger.debug(
        "Configuration loading complete",
        path=path,
        env=env,
        final_keys=list(data.keys()),
    )

    return data
