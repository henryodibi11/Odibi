import sys
import os

sys.path.append(os.getcwd())

from odibi.pipeline import PipelineManager


def test_env_override():
    config_path = "env_test.odibi.yaml"

    # 1. Test Default (No Env) -> ./data
    print("Testing Default Environment...")
    manager = PipelineManager.from_yaml(config_path)
    conn = manager.connections["test_conn"]
    print(f"  Base Path: {conn.base_path}")
    # Normalize paths for comparison (handle ./data vs data)
    if (
        not str(conn.base_path).endswith("data")
        or str(conn.base_path).endswith("data/prod")
        or str(conn.base_path).endswith("data/dev")
    ):
        # Wait, if it is 'data', it matches endswith('data'). We need to ensure it's NOT prod/dev.
        # But 'data/prod' also ends with 'data'? No.
        pass

    # Better check
    if "prod" in str(conn.base_path) or "dev" in str(conn.base_path):
        print(f"  [FAIL] Default env shouldn't have prod/dev in path: {conn.base_path}")
        exit(1)

    # 2. Test Prod Env -> ./data/prod
    print("Testing Prod Environment...")
    manager_prod = PipelineManager.from_yaml(config_path, env="prod")
    conn_prod = manager_prod.connections["test_conn"]
    print(f"  Base Path: {conn_prod.base_path}")
    if not str(conn_prod.base_path).replace("\\", "/").endswith("data/prod"):
        print("  [FAIL] Expected .../data/prod")
        exit(1)

    # 3. Test Dev Env -> ./data/dev
    print("Testing Dev Environment...")
    manager_dev = PipelineManager.from_yaml(config_path, env="dev")
    conn_dev = manager_dev.connections["test_conn"]
    print(f"  Base Path: {conn_dev.base_path}")
    if not str(conn_dev.base_path).replace("\\", "/").endswith("data/dev"):
        print("  [FAIL] Expected .../data/dev")
        exit(1)

    print("\n[OK] Environment overrides verified successfully.")


if __name__ == "__main__":
    test_env_override()
