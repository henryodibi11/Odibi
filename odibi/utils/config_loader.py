import os
import re
import yaml
from typing import Dict, Any

# Pattern to match ${VAR} or ${env:VAR}
# Captures the variable name in group 1
ENV_PATTERN = re.compile(r"\$\{(?:env:)?([A-Za-z0-9_]+)\}")


def load_yaml_with_env(path: str) -> Dict[str, Any]:
    """Load YAML file with environment variable substitution.

    Supports patterns:
    - ${VAR_NAME}
    - ${env:VAR_NAME}

    Args:
        path: Path to YAML file

    Returns:
        Parsed dictionary

    Raises:
        FileNotFoundError: If file does not exist
        ValueError: If environment variable is missing
        yaml.YAMLError: If YAML parsing fails
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"YAML file not found: {path}")

    with open(path, "r") as f:
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
    return yaml.safe_load(substituted_content)
