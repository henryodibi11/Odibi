"""Doctor command implementation."""

import yaml
from pathlib import Path
from odibi.config import ProjectConfig
from odibi.pipeline import PipelineManager


def doctor_command(args):
    """Diagnose configuration and environment issues."""
    print("🩺 Running Odibi Doctor...\n")

    issues_found = 0

    # 1. Check File Existence
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"❌ Config file not found: {args.config}")
        return 1
    print(f"✅ Config file found: {args.config}")

    # 2. Validate YAML Syntax & Schema
    try:
        with open(config_path, "r") as f:
            config_data = yaml.safe_load(f)

        # This validates against Pydantic schema
        project_config = ProjectConfig(**config_data)
        print("✅ YAML schema is valid")
    except Exception as e:
        print(f"❌ YAML validation failed: {e}")
        return 1

    # 3. Check Engine Dependencies
    engine = project_config.engine
    print(f"ℹ️  Engine: {engine}")
    if engine == "spark":
        try:
            import pyspark

            print(f"✅ pyspark installed ({pyspark.__version__})")
        except ImportError:
            print("❌ pyspark not installed. Run: pip install odibi[spark]")
            issues_found += 1

    # 4. Check Connections
    print("\nTesting Connections:")
    try:
        # We use PipelineManager to build connections because it handles the factory logic
        # We wrap this in try/except because from_yaml might fail if imports are missing
        try:
            manager = PipelineManager.from_yaml(str(config_path))

            for name, connection in manager.connections.items():
                try:
                    connection.validate()
                    print(f"  ✅ {name} ({type(connection).__name__}): OK")
                except Exception as e:
                    print(f"  ❌ {name}: Failed validation - {e}")
                    issues_found += 1
        except ImportError as e:
            print(f"❌ Failed to load connections due to missing dependencies: {e}")
            issues_found += 1
        except Exception as e:
            print(f"❌ Failed to initialize connections: {e}")
            issues_found += 1

    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        issues_found += 1

    print("\n" + "=" * 30)
    if issues_found == 0:
        print("✨ All systems go! Configuration looks good.")
        return 0
    else:
        print(f"⚠️  Found {issues_found} issue(s).")
        return 1
