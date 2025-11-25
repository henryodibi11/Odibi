import sys
import os
import pytest
import yaml
from pathlib import Path

sys.path.append(os.getcwd())

from odibi.pipeline import PipelineManager


@pytest.fixture
def config_file(tmp_path):
    """Create a temporary config file."""
    config_data = {
        "project": "env_test",
        "engine": "pandas",
        "story": {"path": "stories", "connection": "test_conn"},
        "connections": {
            "test_conn": {
                "type": "local",
                "base_path": "./data"
            }
        },
        "pipelines": [],
        "environments": {
            "prod": {
                "connections": {
                    "test_conn": {"base_path": "./data/prod"}
                }
            },
            "dev": {
                "connections": {
                    "test_conn": {"base_path": "./data/dev"}
                }
            }
        }
    }
    
    path = tmp_path / "env_test.odibi.yaml"
    with open(path, "w") as f:
        yaml.dump(config_data, f)
        
    return str(path)


def test_env_override(config_file):
    """Test environment overrides."""
    
    # 1. Test Default (No Env) -> ./data
    print("Testing Default Environment...")
    manager = PipelineManager.from_yaml(config_file)
    conn = manager.connections["test_conn"]
    print(f"  Base Path: {conn.base_path}")
    
    # Ensure base path ends with data and NOT prod/dev
    assert str(conn.base_path).endswith("data")
    assert "prod" not in str(conn.base_path)
    assert "dev" not in str(conn.base_path)

    # 2. Test Prod Env -> ./data/prod
    print("Testing Prod Environment...")
    manager_prod = PipelineManager.from_yaml(config_file, env="prod")
    conn_prod = manager_prod.connections["test_conn"]
    print(f"  Base Path: {conn_prod.base_path}")
    assert str(conn_prod.base_path).replace("\\", "/").endswith("data/prod")

    # 3. Test Dev Env -> ./data/dev
    print("Testing Dev Environment...")
    manager_dev = PipelineManager.from_yaml(config_file, env="dev")
    conn_dev = manager_dev.connections["test_conn"]
    print(f"  Base Path: {conn_dev.base_path}")
    assert str(conn_dev.base_path).replace("\\", "/").endswith("data/dev")

    print("\n[OK] Environment overrides verified successfully.")
