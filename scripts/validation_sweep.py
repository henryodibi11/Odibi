import os
import shutil
import sys
from pathlib import Path

import pandas as pd

# Add workspace root to path
sys.path.append(os.getcwd())

from odibi.config import (
    ConnectionType,
    EngineType,
    LocalConnectionConfig,
    NodeConfig,
    PrivacyConfig,
    ProjectConfig,
    StoryConfig,
)
from odibi.engine.pandas_engine import PandasEngine

try:
    from odibi.engine.spark_engine import SparkEngine

    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

from odibi.context import PandasContext, SparkContext
from odibi.node import Node

# Setup Directories
DATA_DIR = Path("test_data")
OUTPUT_DIR = Path("test_output")
SYSTEM_DIR = Path("test_system")


def setup_environment():
    if DATA_DIR.exists():
        shutil.rmtree(DATA_DIR)
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    if SYSTEM_DIR.exists():
        shutil.rmtree(SYSTEM_DIR)

    DATA_DIR.mkdir()
    OUTPUT_DIR.mkdir()
    SYSTEM_DIR.mkdir()

    # Create dummy data
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "amount": [100.0, 200.0, 150.0, 300.0, 250.0],
            "signup_date": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"],
        }
    )
    df.to_csv(DATA_DIR / "users.csv", index=False)
    df.to_parquet(DATA_DIR / "users.parquet", index=False)


def test_pandas_instantiation():
    print("\n--- Testing PandasEngine Instantiation ---")
    try:
        engine = PandasEngine()
        print("[PASS] PandasEngine instantiated successfully.")

        # Check for missing methods
        missing = []
        if not hasattr(engine, "harmonize_schema"):
            missing.append("harmonize_schema")
        if not hasattr(engine, "anonymize"):
            missing.append("anonymize")

        if missing:
            print(f"[FAIL] PandasEngine is missing methods: {missing}")
            return False

    except TypeError as e:
        print(f"[FAIL] PandasEngine instantiation failed: {e}")
        return False
    except Exception as e:
        print(f"[FAIL] PandasEngine instantiation failed with unexpected error: {e}")
        return False
    return True


def instantiate_connections(connection_configs):
    """Helper to instantiate connection objects from configs."""
    from odibi.connections.local import LocalConnection

    connections = {}
    for name, config in connection_configs.items():
        if config.type == ConnectionType.LOCAL:
            connections[name] = LocalConnection(base_path=config.base_path)
        # Add other types if needed for tests
    return connections


def test_pandas_pipeline():
    print("\n--- Testing Pandas Pipeline Execution ---")

    # Config
    project_config = ProjectConfig(
        project="test_project",
        engine=EngineType.PANDAS,
        connections={
            "local": LocalConnectionConfig(type=ConnectionType.LOCAL, base_path=str(DATA_DIR)),
            "output": LocalConnectionConfig(type=ConnectionType.LOCAL, base_path=str(OUTPUT_DIR)),
        },
        story=StoryConfig(connection="output", path="stories"),
        pipelines=[],
        system={"connection": "output", "path": "_system"},
    )

    context = PandasContext()
    # Instantiate connections!
    connections = instantiate_connections(project_config.connections)
    engine = PandasEngine(connections=connections)

    # Node 1: Read
    node1_config = NodeConfig(
        name="read_users", read={"connection": "local", "format": "csv", "path": "users.csv"}
    )

    # Node 2: Transform & Anonymize (Test missing method)
    node2_config = NodeConfig(
        name="process_users",
        depends_on=["read_users"],
        privacy=PrivacyConfig(method="mask"),
        columns={"name": {"pii": True}},  # Tag 'name' as PII
        write={"connection": "output", "format": "parquet", "path": "users_processed.parquet"},
    )

    try:
        # Run Node 1
        print("Running Node 1 (Read)...")
        node1 = Node(node1_config, context, engine, connections)
        res1 = node1.execute()
        if not res1.success:
            print(f"[FAIL] Node 1 Failed: {res1.error}")
            return

        # Run Node 2
        print("Running Node 2 (Privacy/Anonymize)...")
        node2 = Node(node2_config, context, engine, connections)
        res2 = node2.execute()
        if not res2.success:
            print(f"[FAIL] Node 2 Failed: {res2.error}")
        else:
            print("[PASS] Node 2 Success")

    except Exception as e:
        print(f"[FAIL] Runtime Exception: {e}")


def test_spark_pipeline():
    print("\n--- Testing Spark Pipeline Execution ---")
    if not SPARK_AVAILABLE:
        print("[WARN] Spark not available, skipping.")
        return

    try:
        # Config
        project_config = ProjectConfig(
            project="test_project",
            engine=EngineType.SPARK,
            connections={
                "local": LocalConnectionConfig(
                    type=ConnectionType.LOCAL, base_path=str(DATA_DIR.absolute())
                ),
                "output": LocalConnectionConfig(
                    type=ConnectionType.LOCAL, base_path=str(OUTPUT_DIR.absolute())
                ),
            },
            story=StoryConfig(connection="output", path="stories"),
            pipelines=[],
            system={"connection": "output", "path": "_system"},
        )

        # Instantiate Connections
        connections = instantiate_connections(project_config.connections)

        # Initialize Spark Engine
        engine = SparkEngine(connections=connections)
        context = SparkContext(engine.spark)

        # Node 1: Read
        node1_config = NodeConfig(
            name="read_users_spark",
            read={
                "connection": "local",
                "format": "csv",
                "path": "users.csv",
                "options": {"header": "true", "inferSchema": "true"},
            },
        )

        # Node 2: Transform
        node2_config = NodeConfig(
            name="process_users_spark",
            depends_on=["read_users_spark"],
            transform={"steps": [{"sql": "SELECT * FROM df WHERE amount > 150"}]},
            write={"connection": "output", "format": "parquet", "path": "spark_output"},
        )

        # Run Node 1
        print("Running Spark Node 1...")
        node1 = Node(node1_config, context, engine, connections)
        res1 = node1.execute()
        if not res1.success:
            print(f"[FAIL] Spark Node 1 Failed: {res1.error}")
            return

        # Run Node 2
        print("Running Spark Node 2...")
        node2 = Node(node2_config, context, engine, connections)
        res2 = node2.execute()
        if not res2.success:
            print(f"[FAIL] Spark Node 2 Failed: {res2.error}")
        else:
            print("[PASS] Spark Node 2 Success")

    except Exception as e:
        print(f"[FAIL] Spark Execution Error: {e}")


if __name__ == "__main__":
    setup_environment()
    if test_pandas_instantiation():
        test_pandas_pipeline()
    test_spark_pipeline()
