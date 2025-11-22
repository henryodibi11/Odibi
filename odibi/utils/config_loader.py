import os
import re
import yaml
from typing import Dict, Any

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

        merged_data = {}
        for import_path in imports:
            # Resolve relative to current file
            if not os.path.isabs(import_path):
                full_import_path = os.path.join(base_dir, import_path)
            else:
                full_import_path = import_path

            # Recursive load
            # Note: We pass env down to imported files too
            imported_data = load_yaml_with_env(full_import_path, env=env)
            merged_data = _deep_merge(merged_data, imported_data)

        # Merge current data on top of imported data
        data = _deep_merge(merged_data, data)

    # Apply Environment Overrides
    if env:
        environments = data.get("environments", {})
        if env in environments:
            override = environments[env]
            data = _deep_merge(data, override)
            # Remove environments block after merging to keep config clean
            # but keep it if needed for reference? Usually we consume it.
            # Let's keep it in data["environments"] but the root is merged.

    return data
