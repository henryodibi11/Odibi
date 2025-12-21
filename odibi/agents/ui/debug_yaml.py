"""Debug script to test YAML loading in different contexts.

Run this in Databricks to compare how the same file loads:
1. Directly with load_yaml_with_env
2. Through PipelineManager.from_yaml
3. Through load_config_from_file
"""


def debug_yaml_loading(yaml_path: str):
    """Test all loading methods and compare results."""
    import os

    print(f"\n{'=' * 60}")
    print(f"DEBUG: Testing YAML loading for: {yaml_path}")
    print(f"{'=' * 60}")

    # Check file exists
    print("\n1. File existence check:")
    print(f"   os.path.exists: {os.path.exists(yaml_path)}")
    print(f"   Absolute path: {os.path.abspath(yaml_path)}")

    # Try reading raw content
    print("\n2. Raw file read:")
    try:
        with open(yaml_path, "r", encoding="utf-8") as f:
            raw = f.read()
        print(f"   Size: {len(raw)} bytes")
        print(f"   First 200 chars (repr): {repr(raw[:200])}")
        print(f"   Line count: {raw.count(chr(10))}")
        # Check for BOM
        if raw.startswith("\ufeff"):
            print("   WARNING: File has BOM!")
    except Exception as e:
        print(f"   ERROR: {e}")

    # Try load_yaml_with_env directly
    print("\n3. load_yaml_with_env (direct):")
    try:
        from odibi.utils.config_loader import load_yaml_with_env

        result = load_yaml_with_env(yaml_path)
        print(f"   SUCCESS: Loaded {len(result)} top-level keys")
        print(f"   Keys: {list(result.keys())}")
    except Exception as e:
        print(f"   FAILED: {type(e).__name__}: {e}")

    # Try load_config_from_file
    print("\n4. load_config_from_file:")
    try:
        from odibi.config import load_config_from_file

        config = load_config_from_file(yaml_path)
        print(f"   SUCCESS: Project={config.project}")
        print(f"   Connections: {list(config.connections.keys())}")
    except Exception as e:
        print(f"   FAILED: {type(e).__name__}: {e}")

    # Try PipelineManager
    print("\n5. PipelineManager.from_yaml:")
    try:
        from odibi.pipeline import PipelineManager

        manager = PipelineManager.from_yaml(yaml_path)
        print(f"   SUCCESS: Project={manager.project_config.project}")
    except Exception as e:
        print(f"   FAILED: {type(e).__name__}: {e}")

    print(f"\n{'=' * 60}\n")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        debug_yaml_loading(sys.argv[1])
    else:
        print("Usage: python debug_yaml.py <path_to_yaml>")
