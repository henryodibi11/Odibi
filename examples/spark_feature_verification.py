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
from odibi.connections import AzureADLS

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SparkVerification")

# --- Configuration ---
# Databricks E2E Setup logic
if os.path.exists("/dbfs"):
    # Databricks environment with DBFS mount
    # We use /dbfs/tmp/... for Python I/O
    # We use dbfs:/tmp/... for Spark I/O
    PYTHON_ROOT = "/dbfs/tmp/odibi_verification"
    SPARK_ROOT = "dbfs:/tmp/odibi_verification"
    IS_DATABRICKS = True
else:
    # Local fallback
    PYTHON_ROOT = tempfile.mkdtemp(prefix="odibi_spark_test_")
    SPARK_ROOT = f"file://{PYTHON_ROOT}"
    IS_DATABRICKS = False

DATA_DIR = os.path.join(PYTHON_ROOT, "data")
CONFIG_PATH = os.path.join(PYTHON_ROOT, "project.yaml")

def setup_environment():
    """Setup environment for tests."""
    print(f"🚀 Setting up environment")
    print(f"📂 Python Path: {PYTHON_ROOT}")
    print(f"⚡ Spark Path:  {SPARK_ROOT}")
    
    if os.path.exists(PYTHON_ROOT):
        shutil.rmtree(PYTHON_ROOT, ignore_errors=True)
    
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Check for Spark availability
    try:
        from pyspark.sql import SparkSession
        # Ensure SparkSession exists
        spark = SparkSession.builder.getOrCreate()
        print(f"✅ Spark Session Active: {spark.version}")
        return True
    except ImportError:
        print("❌ PySpark not found. This script requires a Spark environment.")
        return False
    except Exception as e:
        print(f"⚠️ Error checking Spark: {e}")
        return False

def create_test_data():
    """Create test data using Pandas (Python I/O)."""
    print("\n--- Creating Test Data ---")
    
    # 1. Data for Validation Test
    # Contains:
    # - nulls in 'id' (should fail no_nulls)
    # - age < 0 (should fail range)
    # - status 'INVALID' (should fail allowed_values)
    validation_data = {
        "id": [1, 2, None, 4],
        "age": [25, -5, 30, 40],
        "status": ["active", "active", "inactive", "INVALID"],
        "score": [80.0, 90.0, 70.0, 85.0]
    }
    df_val = pd.DataFrame(validation_data)
    df_val.to_csv(os.path.join(DATA_DIR, "validation_input.csv"), index=False)
    
    # 2. Data for Redaction Test
    sensitive_data = {
        "user_id": [101, 102, 103],
        "name": ["Alice", "Bob", "Charlie"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
        "ssn": ["123-45-6789", "987-65-4321", "000-00-0000"],
        "public_info": ["A", "B", "C"]
    }
    df_sens = pd.DataFrame(sensitive_data)
    df_sens.to_csv(os.path.join(DATA_DIR, "sensitive_input.csv"), index=False)

    # 3. Data for Error Handling Test
    simple_data = {"col1": [1, 2, 3]}
    df_simple = pd.DataFrame(simple_data)
    df_simple.to_csv(os.path.join(DATA_DIR, "simple.csv"), index=False)

    print(f"✅ Test data created in {DATA_DIR}")

def verify_adls_connectivity():
    """Verify ADLS configuration translation (Mock or Real)."""
    print("\n--- 1. Verifying ADLS Configuration ---")
    
    # Note: In a real run without credentials, this might fail connection check if we try to read.
    # But we just want to verify the Spark Configuration matches the Odibi Configuration.
    
    from odibi.engine.spark_engine import SparkEngine
    
    # Dummy credentials
    test_account = "odibitest"
    test_key = "TEST_KEY_12345"
    
    # Create a config with ADLS
    # We use a separate PipelineManager or Engine instance just for this check
    try:
        adls_conn = AzureADLS(
            account=test_account,
            container="test",
            auth_mode="direct_key",
            account_key=test_key
        )
        
        connections = {"adls_verify": adls_conn}
        
        # Initialize Engine (this configures Spark)
        engine = SparkEngine(connections=connections)
        
        # Check Spark Conf
        conf_key = f"fs.azure.account.key.{test_account}.dfs.core.windows.net"
        conf_val = engine.spark.conf.get(conf_key, None)
        
        if conf_val == test_key:
            print(f"✅ Spark Configuration Verified: {conf_key} is set correctly.")
        else:
            print(f"❌ Spark Configuration Failed: Expected '{test_key}', got '{conf_val}'")
            
    except Exception as e:
        print(f"⚠️ ADLS Verification skipped or failed: {e}")

def run_verification_pipelines():
    """Run pipelines to test Validation, Redaction, and Error Handling."""
    print("\n--- 2. Running Feature Verification Pipelines ---")
    
    # We define TWO connections:
    # 1. 'input_data': Uses SPARK_ROOT for reading (Spark I/O)
    # 2. 'reporting': Uses PYTHON_ROOT for Stories (Python I/O)
    
    # Note on CSV path:
    # If we wrote with Pandas to PYTHON_ROOT/data/validation_input.csv
    # Spark needs to read from SPARK_ROOT/data/validation_input.csv
    
    config = {
        "project": "Spark Deep Dive",
        "engine": "spark",
        "connections": {
            "input_data": {"type": "local", "base_path": f"{SPARK_ROOT}/data"},
            "output_data": {"type": "local", "base_path": f"{SPARK_ROOT}/output"},
            "reporting": {"type": "local", "base_path": f"{PYTHON_ROOT}/stories"}
        },
        "story": {
            "connection": "reporting",
            "path": ".",
            "auto_generate": True
        },
        "pipelines": [
            # Pipeline 1: Validation Failure
            {
                "pipeline": "validation_test",
                "nodes": [
                    {
                        "name": "read_val",
                        "read": {"connection": "input_data", "path": "validation_input.csv", "format": "csv", "options": {"header": True, "inferSchema": True}}
                    },
                    {
                        "name": "validate_fail",
                        "depends_on": ["read_val"],
                        "transform": {"steps": [{"operation": "drop", "params": {"columns": []}}]}, # no-op
                        "validation": {
                            "no_nulls": ["id"],
                            "ranges": {"age": {"min": 0, "max": 120}},
                            "allowed_values": {"status": ["active", "inactive"]}
                        },
                        "on_error": "fail_later"
                    }
                ]
            },
            # Pipeline 2: Redaction
            {
                "pipeline": "redaction_test",
                "nodes": [
                    {
                        "name": "read_sens",
                        "read": {"connection": "input_data", "path": "sensitive_input.csv", "format": "csv", "options": {"header": True, "inferSchema": True}}
                    },
                    {
                        "name": "redact_specific",
                        "depends_on": ["read_sens"],
                        "transform": {"steps": [{"operation": "drop", "params": {"columns": []}}]},
                        "sensitive": ["ssn", "email"],
                        "write": {"connection": "output_data", "path": "redacted_1", "format": "csv"}
                    },
                    {
                        "name": "redact_all",
                        "depends_on": ["read_sens"],
                        "transform": {"steps": [{"operation": "drop", "params": {"columns": []}}]},
                        "sensitive": True,
                        "write": {"connection": "output_data", "path": "redacted_2", "format": "csv"}
                    }
                ]
            },
            # Pipeline 3: Error Handling (Fail Fast)
            {
                "pipeline": "fail_fast_test",
                "nodes": [
                    {
                        "name": "node_success",
                        "read": {"connection": "input_data", "path": "simple.csv", "format": "csv", "options": {"header": True}}
                    },
                    {
                        "name": "node_fail",
                        "depends_on": ["node_success"],
                        "transform": {"steps": [{"sql": "SELECT * FROM non_existent_table"}]}, # Should fail
                        "on_error": "fail_fast"
                    },
                    {
                        "name": "node_skipped",
                        "depends_on": ["node_fail"],
                        "transform": {"steps": [{"operation": "drop", "params": {"columns": []}}]}
                    }
                ]
            }
        ]
    }
    
    with open(CONFIG_PATH, "w") as f:
        yaml.dump(config, f)
        
    print(f"✅ Config generated at {CONFIG_PATH}")
    
    manager = PipelineManager.from_yaml(CONFIG_PATH)
    
    # --- Test 1: Validation ---
    print("\n🔹 Running Validation Test...")
    results_val = manager.run("validation_test")
    
    node_val = results_val.get_node_result("validate_fail")
    if node_val and not node_val.success:
        print(f"✅ Validation Correctly Failed: {node_val.error}")
    else:
        print("❌ Validation Failed: Node succeeded but should have failed.")

    # --- Test 2: Redaction ---
    print("\n🔹 Running Redaction Test...")
    results_red = manager.run("redaction_test")
    
    # Check Specific Redaction
    node_spec = results_red.get_node_result("redact_specific")
    if node_spec and node_spec.metadata.get("sample_data"):
        row = node_spec.metadata["sample_data"][0]
        # Note: Spark rows might be dicts or Rows depending on implementation of get_sample
        if row.get("ssn") == "[REDACTED]" and row.get("email") == "[REDACTED]" and row.get("name") != "[REDACTED]":
             print("✅ Specific Redaction Verified")
        else:
             print(f"❌ Specific Redaction Failed: {row}")
    else:
        print("⚠️ No sample data for redact_specific")

    # Check Full Redaction
    node_all = results_red.get_node_result("redact_all")
    if node_all and node_all.metadata.get("sample_data"):
        sample = node_all.metadata["sample_data"]
        if len(sample) == 1 and "Sensitive Data" in str(sample[0].get("message")):
             print("✅ Full Redaction Verified")
        else:
             print(f"❌ Full Redaction Failed: {sample}")
    else:
        print("⚠️ No sample data for redact_all")

    # --- Test 3: Error Handling ---
    print("\n🔹 Running Error Handling Test...")
    results_fail = manager.run("fail_fast_test")
    
    if "node_fail" in results_fail.failed:
        print("✅ Node correctly failed")
    else:
        print("❌ Expected node_fail to fail")
        
    if "node_skipped" not in results_fail.completed and "node_skipped" not in results_fail.failed:
        print("✅ Dependent node stopped (Fail Fast works)")
    elif "node_skipped" in results_fail.skipped:
        print("ℹ️ Dependent node skipped")
    else:
        print(f"❌ dependent node executed? {results_fail.node_results.get('node_skipped')}")

if __name__ == "__main__":
    if setup_environment():
        try:
            create_test_data()
            verify_adls_connectivity()
            run_verification_pipelines()
        finally:
            # Cleanup only if local
            if not IS_DATABRICKS:
                shutil.rmtree(PYTHON_ROOT, ignore_errors=True)
            print("\n✅ Verification Script Complete")
