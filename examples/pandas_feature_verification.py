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
logger = logging.getLogger("PandasVerification")

# Local Setup
PYTHON_ROOT = tempfile.mkdtemp(prefix="odibi_pandas_test_")
DATA_DIR = os.path.join(PYTHON_ROOT, "data")
CONFIG_PATH = os.path.join(PYTHON_ROOT, "project.yaml")


def setup_environment():
    """Setup environment for tests."""
    print("[INFO] Setting up Pandas environment")
    print(f"Root: {PYTHON_ROOT}")

    if os.path.exists(PYTHON_ROOT):
        shutil.rmtree(PYTHON_ROOT, ignore_errors=True)

    os.makedirs(DATA_DIR, exist_ok=True)
    return True


def create_test_data():
    """Create test data using Pandas."""
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
        "score": [80.0, 90.0, 70.0, 85.0],
    }
    df_val = pd.DataFrame(validation_data)
    df_val.to_csv(os.path.join(DATA_DIR, "validation_input.csv"), index=False)

    # 2. Data for Redaction Test
    sensitive_data = {
        "user_id": [101, 102, 103],
        "name": ["Alice", "Bob", "Charlie"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
        "ssn": ["123-45-6789", "987-65-4321", "000-00-0000"],
        "public_info": ["A", "B", "C"],
    }
    df_sens = pd.DataFrame(sensitive_data)
    df_sens.to_csv(os.path.join(DATA_DIR, "sensitive_input.csv"), index=False)

    # 3. Data for Error Handling Test
    simple_data = {"col1": [1, 2, 3]}
    df_simple = pd.DataFrame(simple_data)
    df_simple.to_csv(os.path.join(DATA_DIR, "simple.csv"), index=False)

    print(f"[OK] Test data created in {DATA_DIR}")


def run_verification_pipelines():
    """Run pipelines to test Validation, Redaction, and Error Handling."""
    print("\n--- 2. Running Feature Verification Pipelines (Pandas) ---")

    config = {
        "project": "Pandas Deep Dive",
        "engine": "pandas",  # Using Pandas Engine
        "connections": {
            "local_data": {"type": "local", "base_path": DATA_DIR},
            "reporting": {"type": "local", "base_path": f"{PYTHON_ROOT}/stories"},
        },
        "story": {"connection": "reporting", "path": ".", "auto_generate": True},
        "pipelines": [
            # Pipeline 1: Validation Failure
            {
                "pipeline": "validation_test",
                "nodes": [
                    {
                        "name": "read_val",
                        "read": {
                            "connection": "local_data",
                            "path": "validation_input.csv",
                            "format": "csv",
                        },
                    },
                    {
                        "name": "validate_fail",
                        "depends_on": ["read_val"],
                        "transform": {
                            "steps": [{"operation": "drop", "params": {"columns": []}}]
                        },  # no-op
                        "validation": {
                            "no_nulls": ["id"],
                            "ranges": {"age": {"min": 0, "max": 120}},
                            "allowed_values": {"status": ["active", "inactive"]},
                        },
                        "on_error": "fail_later",
                    },
                ],
            },
            # Pipeline 2: Redaction
            {
                "pipeline": "redaction_test",
                "nodes": [
                    {
                        "name": "read_sens",
                        "read": {
                            "connection": "local_data",
                            "path": "sensitive_input.csv",
                            "format": "csv",
                        },
                    },
                    {
                        "name": "redact_specific",
                        "depends_on": ["read_sens"],
                        "transform": {"steps": [{"operation": "drop", "params": {"columns": []}}]},
                        "sensitive": ["ssn", "email"],
                        "write": {
                            "connection": "local_data",
                            "path": "redacted_1.csv",
                            "format": "csv",
                        },
                    },
                    {
                        "name": "redact_all",
                        "depends_on": ["read_sens"],
                        "transform": {"steps": [{"operation": "drop", "params": {"columns": []}}]},
                        "sensitive": True,
                        "write": {
                            "connection": "local_data",
                            "path": "redacted_2.csv",
                            "format": "csv",
                        },
                    },
                ],
            },
            # Pipeline 3: Error Handling (Fail Fast)
            {
                "pipeline": "fail_fast_test",
                "nodes": [
                    {
                        "name": "node_success",
                        "read": {"connection": "local_data", "path": "simple.csv", "format": "csv"},
                    },
                    {
                        "name": "node_fail",
                        "depends_on": ["node_success"],
                        # Use a SQL step that fails in Pandas (e.g. referencing missing table)
                        "transform": {"steps": [{"sql": "SELECT * FROM non_existent_table"}]},
                        "on_error": "fail_fast",
                    },
                    {
                        "name": "node_skipped",
                        "depends_on": ["node_fail"],
                        "transform": {"steps": [{"operation": "drop", "params": {"columns": []}}]},
                    },
                ],
            },
        ],
    }

    with open(CONFIG_PATH, "w") as f:
        yaml.dump(config, f)

    print(f"[OK] Config generated at {CONFIG_PATH}")

    manager = PipelineManager.from_yaml(CONFIG_PATH)

    # --- Test 1: Validation ---
    print("\n[INFO] Running Validation Test...")
    results_val = manager.run("validation_test")

    node_val = results_val.get_node_result("validate_fail")
    if node_val and not node_val.success:
        # Convert error to string and encode/decode to remove problematic chars for Windows console
        err_str = str(node_val.error).encode("ascii", "replace").decode("ascii")
        print(f"[OK] Validation Correctly Failed: {err_str}")
    else:
        print("[FAIL] Validation Failed: Node succeeded but should have failed.")

    # --- Test 2: Redaction ---
    print("\n[INFO] Running Redaction Test...")
    results_red = manager.run("redaction_test")

    # Check Specific Redaction
    node_spec = results_red.get_node_result("redact_specific")
    if node_spec and node_spec.metadata.get("sample_data"):
        row = node_spec.metadata["sample_data"][0]
        if (
            row.get("ssn") == "[REDACTED]"
            and row.get("email") == "[REDACTED]"
            and row.get("name") != "[REDACTED]"
        ):
            print("[OK] Specific Redaction Verified")
        else:
            print(f"[FAIL] Specific Redaction Failed: {row}")
    else:
        print("[WARN] No sample data for redact_specific")

    # Check Full Redaction
    node_all = results_red.get_node_result("redact_all")
    if node_all and node_all.metadata.get("sample_data"):
        sample = node_all.metadata["sample_data"]
        if len(sample) == 1 and "Sensitive Data" in str(sample[0].get("message")):
            print("[OK] Full Redaction Verified")
        else:
            print(f"[FAIL] Full Redaction Failed: {sample}")
    else:
        print("[WARN] No sample data for redact_all")

    # --- Test 3: Error Handling ---
    print("\n[INFO] Running Error Handling Test...")
    results_fail = manager.run("fail_fast_test")

    if "node_fail" in results_fail.failed:
        print("[OK] Node correctly failed")
    else:
        print("[FAIL] Expected node_fail to fail")

    if "node_skipped" not in results_fail.completed and "node_skipped" not in results_fail.failed:
        print("[OK] Dependent node stopped (Fail Fast works)")
    elif "node_skipped" in results_fail.skipped:
        print("[INFO] Dependent node skipped")
    else:
        print(f"[FAIL] dependent node executed? {results_fail.node_results.get('node_skipped')}")


if __name__ == "__main__":
    if setup_environment():
        try:
            create_test_data()
            run_verification_pipelines()
        finally:
            shutil.rmtree(PYTHON_ROOT, ignore_errors=True)
            print("\n[OK] Pandas Verification Script Complete")
