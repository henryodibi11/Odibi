import os
import re
from typing import Any, Dict

import yaml

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
            result[key] = _deep_merge(result[key], value)
        elif key == "pipelines" and isinstance(value, list) and isinstance(result.get(key), list):
            # Special case: Append pipelines instead of overwriting
            result[key] = result[key] + value
        else:
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
    if not os.path.exists(path):
        raise FileNotFoundError(f"YAML file not found: {path}")

    # Get absolute path for relative import resolution
    abs_path = os.path.abspath(path)
    base_dir = os.path.dirname(abs_path)

    with open(abs_path, "r") as f:
        content = f.read()

    def replace_env(match):
        var_name = match.group(1)
        value = os.environ.get(var_name)
        if value is None:
            raise ValueError(f"Missing environment variable: {var_name}")
        return value

    # Substitute variables
    substituted_content = ENV_PATTERN.sub(replace_env, content)

    # Parse YAML
    data = yaml.safe_load(substituted_content) or {}

    # Handle imports
    imports = data.pop("imports", [])
    if imports:
        if isinstance(imports, str):
            imports = [imports]

        # Start with current data as the base
        merged_data = data.copy()

        for import_path in imports:
            # Resolve relative to current file
            if not os.path.isabs(import_path):
                full_import_path = os.path.join(base_dir, import_path)
            else:
                full_import_path = import_path

            # Recursive load
            # Note: We pass env down to imported files too
            imported_data = load_yaml_with_env(full_import_path, env=env)

            # Merge imported data INTO the current data
            # This way, the main file acts as the "master" that accumulates imports
            merged_data = _deep_merge(merged_data, imported_data)

        data = merged_data

    # Apply Environment Overrides from "environments" block in main file
    if env:
        environments = data.get("environments", {})
        if env in environments:
            override = environments[env]
            data = _deep_merge(data, override)

    # Apply Environment Overrides from external env.{env}.yaml file
    if env:
        env_file_name = f"env.{env}.yaml"
        env_file_path = os.path.join(base_dir, env_file_name)
        if os.path.exists(env_file_path):
            # Load the env file (recursively, so it can have imports too)
            # We pass env=None to avoid infinite recursion if it somehow references itself,
            # though strictly it shouldn't matter as we look for env.{env}.yaml based on the passed env.
            # But logically, an env specific file shouldn't load other env specific files for the same env.
            env_data = load_yaml_with_env(env_file_path, env=None)
            data = _deep_merge(data, env_data)

    return data
