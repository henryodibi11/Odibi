"""Doctor command implementation."""

import yaml
from pathlib import Path
from odibi.config import ProjectConfig
from odibi.pipeline import PipelineManager


def doctor_command(args):
    """Diagnose configuration and environment issues."""
    try:
        print("🩺 Running Odibi Doctor...\n")
    except UnicodeEncodeError:
        print("[DOCTOR] Running Odibi Doctor...\n")

    issues_found = 0

    # 1. Check File Existence
    config_path = Path(args.config)
    if not config_path.exists():
        try:
            print(f"❌ Config file not found: {args.config}")
        except UnicodeEncodeError:
            print(f"[FAIL] Config file not found: {args.config}")
        return 1

    try:
        print(f"✅ Config file found: {args.config}")
    except UnicodeEncodeError:
        print(f"[OK] Config file found: {args.config}")

    # 2. Validate YAML Syntax & Schema
    try:
        with open(config_path, "r") as f:
            config_data = yaml.safe_load(f)

        # This validates against Pydantic schema
        project_config = ProjectConfig(**config_data)

        try:
            print("✅ YAML schema is valid")
        except UnicodeEncodeError:
            print("[OK] YAML schema is valid")
    except Exception as e:
        try:
            print(f"❌ YAML validation failed: {e}")
        except UnicodeEncodeError:
            print(f"[FAIL] YAML validation failed: {e}")
        return 1

    # 3. Check Engine Dependencies
    engine = project_config.engine
    try:
        print(f"ℹ️  Engine: {engine}")
    except UnicodeEncodeError:
        print(f"[INFO] Engine: {engine}")

    if engine == "spark":
        try:
            import pyspark

            try:
                print(f"✅ pyspark installed ({pyspark.__version__})")
            except UnicodeEncodeError:
                print(f"[OK] pyspark installed ({pyspark.__version__})")
        except ImportError:
            try:
                print("❌ pyspark not installed. Run: pip install odibi[spark]")
            except UnicodeEncodeError:
                print("[FAIL] pyspark not installed. Run: pip install odibi[spark]")
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
                    try:
                        print(f"  ✅ {name} ({type(connection).__name__}): OK")
                    except UnicodeEncodeError:
                        print(f"  [OK] {name} ({type(connection).__name__}): OK")
                except Exception as e:
                    try:
                        print(f"  ❌ {name}: Failed validation - {e}")
                    except UnicodeEncodeError:
                        print(f"  [FAIL] {name}: Failed validation - {e}")
                    issues_found += 1
        except ImportError as e:
            try:
                print(f"❌ Failed to load connections due to missing dependencies: {e}")
            except UnicodeEncodeError:
                print(f"[FAIL] Failed to load connections due to missing dependencies: {e}")
            issues_found += 1
        except Exception as e:
            try:
                print(f"❌ Failed to initialize connections: {e}")
            except UnicodeEncodeError:
                print(f"[FAIL] Failed to initialize connections: {e}")
            issues_found += 1

    except Exception as e:
        try:
            print(f"❌ Unexpected error: {e}")
        except UnicodeEncodeError:
            print(f"[FAIL] Unexpected error: {e}")
        issues_found += 1

    print("\n" + "=" * 30)
    if issues_found == 0:
        try:
            print("✨ All systems go! Configuration looks good.")
        except UnicodeEncodeError:
            print("[SUCCESS] All systems go! Configuration looks good.")
        return 0
    else:
        try:
            print(f"⚠️  Found {issues_found} issue(s).")
        except UnicodeEncodeError:
            print(f"[WARN] Found {issues_found} issue(s).")
        return 1
