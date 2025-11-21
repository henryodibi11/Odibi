import os
import json
from odibi.config import ProjectConfig


def init_vscode_command(args):
    """Initialize VS Code configuration for Odibi."""
    cwd = os.getcwd()
    vscode_dir = os.path.join(cwd, ".vscode")
    settings_path = os.path.join(vscode_dir, "settings.json")
    schema_path = os.path.join(cwd, "odibi.schema.json")

    print(f"Initializing VS Code support in: {cwd}")

    # 1. Generate Schema File
    try:
        schema = ProjectConfig.model_json_schema()
        with open(schema_path, "w") as f:
            json.dump(schema, f, indent=2)
        print("✅ Generated schema: odibi.schema.json")
    except Exception as e:
        print(f"❌ Failed to generate schema: {e}")
        return 1

    # 2. Create .vscode directory
    if not os.path.exists(vscode_dir):
        os.makedirs(vscode_dir)
        print("✅ Created directory: .vscode/")

    # 3. Update/Create settings.json
    settings = {}
    if os.path.exists(settings_path):
        try:
            with open(settings_path, "r") as f:
                settings = json.load(f)
        except json.JSONDecodeError:
            print("⚠️  Existing settings.json was invalid, starting fresh.")

    # Ensure yaml.schemas section exists
    if "yaml.schemas" not in settings:
        settings["yaml.schemas"] = {}

    # Add our schema mapping
    settings["yaml.schemas"]["./odibi.schema.json"] = [
        "odibi.yaml",
        "*_pipeline.yaml",
        "*.odibi.yaml",
    ]

    # Write back settings
    try:
        with open(settings_path, "w") as f:
            json.dump(settings, f, indent=2)
        print("✅ Configured settings: .vscode/settings.json")
    except Exception as e:
        print(f"❌ Failed to write settings: {e}")
        return 1

    print("\nSuccess! VS Code is now configured with auto-completion for Odibi YAML files.")
    return 0


def add_ide_parser(subparsers):
    """Add ide subcommands to argument parser."""
    parser = subparsers.add_parser("init-vscode", help="Initialize VS Code configuration")
    return parser
