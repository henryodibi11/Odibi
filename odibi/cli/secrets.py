"""Secrets management CLI commands."""

import os
import re
from typing import Set

from dotenv import load_dotenv

from odibi.utils.config_loader import load_yaml_with_env
from odibi.utils.setup_helpers import fetch_keyvault_secrets_parallel

# Pattern to match ${VAR} or ${env:VAR}
# Captures the variable name in group 1
# Same as in odibi/utils/config_loader.py
ENV_PATTERN = re.compile(r"\$\{(?:env:)?([A-Za-z0-9_]+)\}")


def extract_env_vars(config_path: str) -> Set[str]:
    """Extract environment variables used in config file.

    Args:
        config_path: Path to YAML config file

    Returns:
        Set of variable names
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as f:
        content = f.read()

    return set(ENV_PATTERN.findall(content))


def init_command(args) -> int:
    """Create .env.template from config file usage.

    Args:
        args: Parsed arguments

    Returns:
        Exit code
    """
    try:
        # Extract variables
        vars_found = extract_env_vars(args.config)

        if not vars_found:
            print(f"No environment variables found in {args.config}")
            return 0

        # Create template content
        template_lines = [
            "# Odibi Environment Variables Template",
            f"# Generated from {args.config}",
            "",
        ]

        for var in sorted(vars_found):
            template_lines.append(f"{var}=")

        output_path = args.output

        # Check if file exists
        if os.path.exists(output_path) and not args.force:
            print(f"Error: {output_path} already exists. Use --force to overwrite.")
            return 1

        # Write file
        with open(output_path, "w") as f:
            f.write("\n".join(template_lines) + "\n")

        print(f"Created {output_path} with {len(vars_found)} variables.")
        return 0

    except Exception as e:
        print(f"Error creating secrets template: {e}")
        return 1


class SimpleConnection:
    """Simple connection wrapper for Key Vault checking."""

    def __init__(self, name: str, data: dict):
        self.name = name
        # Support flat or auth dict structure
        self.key_vault_name = data.get("key_vault_name") or data.get("auth", {}).get(
            "key_vault_name"
        )
        self.secret_name = data.get("secret_name") or data.get("auth", {}).get("secret_name")
        self.account = data.get("account_name") or data.get("account") or "unknown"


def check_keyvault_access(config_path: str) -> bool:
    """Check if Key Vault secrets are accessible.

    Args:
        config_path: Path to config file

    Returns:
        True if all checks pass
    """
    print("\nChecking Azure Key Vault access...")
    try:
        # Load config
        config_dict = load_yaml_with_env(config_path)
        connections_data = config_dict.get("connections", {})

        connections_to_check = {}
        for name, data in connections_data.items():
            if not isinstance(data, dict):
                continue
            conn = SimpleConnection(name, data)
            if conn.key_vault_name and conn.secret_name:
                connections_to_check[name] = conn

        if not connections_to_check:
            print("  [OK] No Key Vault connections found.")
            return True

        # Run parallel fetch
        results = fetch_keyvault_secrets_parallel(connections_to_check, verbose=True)

        failures = [r for r in results.values() if not r.success]
        if failures:
            print(f"\nFailed to access {len(failures)} Key Vault secrets:")
            for f in failures:
                print(f"  [FAIL] Connection '{f.connection_name}': {f.error}")
            return False

        return True

    except Exception as e:
        print(f"Error checking Key Vaults: {e}")
        return False


def validate_command(args) -> int:
    """Check if required environment variables and Key Vault secrets are accessible.

    Args:
        args: Parsed arguments

    Returns:
        Exit code
    """
    exit_code = 0

    print("Checking Environment Variables...")
    try:
        # Load .env if it exists
        load_dotenv()

        # Extract variables from config
        vars_required = extract_env_vars(args.config)

        if not vars_required:
            print(f"  [OK] No environment variables found in {args.config}")
        else:
            # Check against environment
            missing_vars = []
            for var in vars_required:
                if var not in os.environ:
                    missing_vars.append(var)

            if missing_vars:
                print(f"  [MISSING] Missing {len(missing_vars)} environment variables:")
                for var in sorted(missing_vars):
                    print(f"    - {var}")
                print("    Please set these variables in your environment or .env file.")
                exit_code = 1
            else:
                print("  [OK] All required environment variables are set.")

    except Exception as e:
        print(f"Error validating secrets: {e}")
        exit_code = 1

    # Check Key Vaults
    # We only check if env vars are fine, otherwise config load might fail
    if exit_code == 0:
        if not check_keyvault_access(args.config):
            exit_code = 1
    else:
        print("\nSkipping Key Vault check due to missing environment variables.")

    return exit_code


def add_secrets_parser(subparsers):
    """Add secrets subparser to main parser.

    Args:
        subparsers: Main subparsers object
    """
    secrets_parser = subparsers.add_parser(
        "secrets", help="Manage secrets and environment variables"
    )
    secrets_subparsers = secrets_parser.add_subparsers(
        dest="secrets_command", help="Secrets commands"
    )

    # init command
    init_parser = secrets_subparsers.add_parser("init", help="Create .env.template from config")
    init_parser.add_argument("config", help="Path to YAML config file")
    init_parser.add_argument(
        "-o", "--output", default=".env.template", help="Output file path (default: .env.template)"
    )
    init_parser.add_argument("-f", "--force", action="store_true", help="Overwrite existing file")

    # validate command
    validate_parser = secrets_subparsers.add_parser("validate", help="Check required variables")
    validate_parser.add_argument("config", help="Path to YAML config file")


def secrets_command(args) -> int:
    """Dispatcher for secrets commands.

    Args:
        args: Parsed arguments

    Returns:
        Exit code
    """
    if args.secrets_command == "init":
        return init_command(args)
    elif args.secrets_command == "validate":
        return validate_command(args)
    else:
        print("Error: No secrets command specified")
        return 1
