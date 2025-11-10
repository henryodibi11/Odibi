"""Validate command implementation."""

import yaml
from odibi.config import ProjectConfig


def validate_command(args):
    """Validate config file."""
    try:
        # Load and validate YAML
        with open(args.config, "r") as f:
            config_data = yaml.safe_load(f)

        # Validate against ProjectConfig
        ProjectConfig(**config_data)
        print("Config is valid")
        return 0
    except Exception as e:
        print(f"Config validation failed: {e}")
        return 1
