import os
import shutil
import tempfile
import logging
import yaml
import sys
import pandas as pd

# Ensure we can import odibi
sys.path.append(os.getcwd())

from odibi.pipeline import PipelineManager

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SparkDeltaVerification")

# --- Configuration ---
if os.path.exists("/dbfs"):
    PYTHON_ROOT = "/dbfs/tmp/odibi_delta_verification"
    SPARK_ROOT = "dbfs:/tmp/odibi_delta_verification"
    IS_DATABRICKS = True
else:
    PYTHON_ROOT = tempfile.mkdtemp(prefix="odibi_delta_test_")
    SPARK_ROOT = f"file://{PYTHON_ROOT}"
    IS_DATABRICKS = False

DATA_DIR = os.path.join(PYTHON_ROOT, "data")
CONFIG_PATH = os.path.join(PYTHON_ROOT, "project.yaml")


def setup_environment():
    """Setup environment for tests."""
    print("[INFO] Setting up Delta environment")
    print(f"Python Path: {PYTHON_ROOT}")
    print(f"Spark Path:  {SPARK_ROOT}")

    if os.path.exists(PYTHON_ROOT):
        shutil.rmtree(PYTHON_ROOT, ignore_errors=True)

    os.makedirs(DATA_DIR, exist_ok=True)

    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        print(f"[OK] Spark Session Active: {spark.version}")
        return True
    except ImportError:
        print("[FAIL] PySpark not found.")
        return False


def create_test_data():
    """Create initial and update data."""
    print("\n--- Creating Test Data ---")

    # Initial Data (Version 0)
    # id=1, value=A
    df_v0 = pd.DataFrame({"id": [1], "value": ["A"], "batch": [0]})
    df_v0.to_csv(os.path.join(DATA_DIR, "delta_v0.csv"), index=False)

    # Update Data (Version 1)
    # id=1, value=B (Update)
    # id=2, value=C (Insert)
    df_v1 = pd.DataFrame({"id": [1, 2], "value": ["B", "C"], "batch": [1, 1]})
    df_v1.to_csv(os.path.join(DATA_DIR, "delta_v1.csv"), index=False)

    print(f"[OK] Test data created in {DATA_DIR}")


def run_delta_verification():
    """Run pipelines to verify Upsert and Time Travel."""
    print("\n--- 2. Running Delta Verification ---")

    config = {
        "project": "Spark Delta Verification",
        "engine": "spark",
        "connections": {
            "input_data": {"type": "local", "base_path": f"{SPARK_ROOT}/data"},
            "output_data": {"type": "local", "base_path": f"{SPARK_ROOT}/delta_output"},
            "reporting": {"type": "local", "base_path": f"{PYTHON_ROOT}/stories"},
        },
        "story": {"connection": "reporting", "path": ".", "auto_generate": False},
        "pipelines": [
            # 1. Initialize Table (Overwrite)
            {
                "pipeline": "delta_init",
                "nodes": [
                    {
                        "name": "write_v0",
                        "read": {
                            "connection": "input_data",
                            "path": "delta_v0.csv",
                            "format": "csv",
                            "options": {"header": True, "inferSchema": True},
                        },
                        "write": {
                            "connection": "output_data",
                            "path": "main_table",
                            "format": "delta",
                            "mode": "overwrite",
                        },
                    }
                ],
            },
            # 2. Upsert Data (Merge)
            {
                "pipeline": "delta_upsert",
                "nodes": [
                    {
                        "name": "write_v1",
                        "read": {
                            "connection": "input_data",
                            "path": "delta_v1.csv",
                            "format": "csv",
                            "options": {"header": True, "inferSchema": True},
                        },
                        "write": {
                            "connection": "output_data",
                            "path": "main_table",
                            "format": "delta",
                            "mode": "upsert",
                            "options": {"keys": ["id"]},  # Merge on ID
                        },
                    }
                ],
            },
            # 3. Time Travel Read (Read Version 0)
            {
                "pipeline": "delta_time_travel",
                "nodes": [
                    {
                        "name": "read_v0",
                        "read": {
                            "connection": "output_data",
                            "path": "main_table",
                            "format": "delta",
                            "options": {"versionAsOf": 0},  # Should see id=1, val=A only
                        },
                    }
                ],
            },
        ],
    }

    with open(CONFIG_PATH, "w") as f:
        yaml.dump(config, f)

    manager = PipelineManager.from_yaml(CONFIG_PATH)

    # 1. Init
    print("\n[INFO] Initializing Delta Table (Version 0)...")
    manager.run("delta_init")

    # 2. Upsert
    print("\n[INFO] Running Upsert (Version 1)...")
    manager.run("delta_upsert")

    # Verify Current State (Should be V1)
    # id=1, value=B
    # id=2, value=C
    print("\n[INFO] Verifying Current State (Upsert Result)...")
    # We verify manually using the engine to read
    engine = manager._pipelines["delta_upsert"].engine
    conn = manager.connections["output_data"]

    df_current = engine.read(conn, "delta", path="main_table")
    rows = df_current.collect()
    # Sort for consistency
    rows = sorted(rows, key=lambda r: r["id"])

    valid_upsert = (
        len(rows) == 2
        and rows[0]["id"] == 1
        and rows[0]["value"] == "B"
        and rows[1]["id"] == 2
        and rows[1]["value"] == "C"
    )

    if valid_upsert:
        print("[OK] Upsert Verified: Records updated and inserted correctly.")
    else:
        print(f"[FAIL] Upsert Failed. Current rows: {rows}")

    # 3. Time Travel
    print("\n[INFO] Verifying Time Travel (Reading Version 0)...")
    res_tt = manager.run("delta_time_travel")
    node_tt = res_tt.get_node_result("read_v0")

    # Check the data in the result (it's registered in context)
    # Note: In pipeline run, result df is not directly in NodeResult,
    # but we can check row count in metadata or use manager context if exposed.
    # Better: use the engine to read manually with versionAsOf to verify logic

    df_v0_read = engine.read(conn, "delta", path="main_table", options={"versionAsOf": 0})
    rows_v0 = df_v0_read.collect()

    valid_tt = len(rows_v0) == 1 and rows_v0[0]["id"] == 1 and rows_v0[0]["value"] == "A"

    if valid_tt:
        print("[OK] Time Travel Verified: Successfully read Version 0.")
    else:
        print(f"[FAIL] Time Travel Failed. Rows v0: {rows_v0}")

    # 4. History & Vacuum
    print("\n[INFO] Verifying Delta Maintenance (History & Vacuum)...")

    # History
    history = engine.get_delta_history(conn, "main_table")
    print(f"   History length: {len(history)} versions")
    if len(history) >= 2:
        print("[OK] History Verified")
    else:
        print("[FAIL] History Failed (Expected at least 2 versions)")

    # Vacuum
    print("   Running Vacuum...")
    try:
        # Vacuum with 0 retention usually requires a spark conf check override,
        # but we just want to see if the method call works.
        # We use standard retention (default) or just call it.
        # Note: To vacuum with 0 retention, we need: spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        # But usually vacuuming recent data is dangerous. Let's just call it with default to ensure API works.
        engine.vacuum_delta(conn, "main_table")
        print("[OK] Vacuum command executed successfully")
    except Exception as e:
        print(f"[FAIL] Vacuum Failed: {e}")


if __name__ == "__main__":
    if setup_environment():
        try:
            create_test_data()
            run_delta_verification()
        finally:
            if not IS_DATABRICKS:
                shutil.rmtree(PYTHON_ROOT, ignore_errors=True)
            print("\n[OK] Delta Verification Script Complete")
