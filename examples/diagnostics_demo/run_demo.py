"""
Diagnostics API Demo
====================

This script demonstrates how to use ODIBI's diagnostics API programmatically
(similar to how you would use it in a Jupyter Notebook).

It simulates a "Day 1 vs Day 2" scenario where data and logic change.
"""

import os
import sys
import shutil
import pandas as pd
import yaml
import json

# Ensure we can import odibi
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from odibi.pipeline import PipelineManager
from odibi.diagnostics.manager import HistoryManager
from odibi.diagnostics.diff import diff_runs
from odibi.diagnostics.delta import get_delta_diff
from odibi.transformations.registry import get_registry

# --- 1. Setup Environment ---
import time

timestamp = int(time.time())
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
# Use unique output dir to avoid Windows file locking/permission issues
OUTPUT_DIR = os.path.join(BASE_DIR, f"output_{timestamp}")
STORY_DIR = os.path.join(BASE_DIR, f"stories_{timestamp}")

# Clean previous runs (best effort)
# ...
if os.path.exists(OUTPUT_DIR):
    try:
        shutil.rmtree(OUTPUT_DIR)
    except Exception as e:
        print(f"Warning: Could not clean output dir: {e}")

if os.path.exists(STORY_DIR):
    try:
        shutil.rmtree(STORY_DIR)
    except Exception as e:
        print(f"Warning: Could not clean story dir: {e}")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(STORY_DIR, exist_ok=True)


# --- 2. Define Logic ---
# A simple transformation function
def enrich_customers(context, current, segment_prefix="SEG-"):
    """Enrich customer data."""
    df = current
    # Logic: Add prefix to segment
    df["segment_code"] = segment_prefix + df["segment"]
    return df


def join_transactions(context, current):
    """Join customer data with transactions."""
    customers = current  # from enrich_data
    transactions = context.get("load_transactions")

    # Join on customer_id/id
    merged = customers.merge(transactions, left_on="id", right_on="customer_id", how="left")
    return merged


def summarize_customer(context, current):
    """Summarize sales by customer."""
    df = current
    summary = df.groupby(["id", "name"])["amount"].sum().reset_index()
    return summary


# Register it
get_registry().register("enrich_customers", enrich_customers)
get_registry().register("join_transactions", join_transactions)
get_registry().register("summarize_customer", summarize_customer)

# --- 3. Define Pipeline Config ---
pipeline_config = {
    "project": "diagnostics_demo",
    "version": "1.0.0",
    "engine": "pandas",
    "story": {"connection": "local", "path": STORY_DIR},  # Save stories here
    "pipelines": [
        {
            "pipeline": "customer_360",
            "nodes": [
                {
                    "name": "load_customers",
                    "read": {
                        "connection": "local",
                        "path": os.path.join(DATA_DIR, "customers.csv"),
                        "format": "csv",
                    },
                    "write": {
                        "connection": "local",
                        "path": os.path.join(OUTPUT_DIR, "raw_customers"),
                        "format": "delta",
                        "mode": "overwrite",
                        "options": {
                            "deep_diagnostics": True,
                            "diff_keys": ["id"],
                            "schema_mode": "overwrite",
                        },
                    },
                },
                {
                    "name": "load_transactions",
                    "read": {
                        "connection": "local",
                        "path": os.path.join(DATA_DIR, "transactions.csv"),
                        "format": "csv",
                    },
                    "write": {
                        "connection": "local",
                        "path": os.path.join(OUTPUT_DIR, "raw_transactions"),
                        "format": "delta",
                        "mode": "overwrite",
                    },
                },
                {
                    "name": "enrich_data",
                    "depends_on": ["load_customers"],
                    "transformer": "enrich_customers",
                    "params": {"segment_prefix": "SEG-"},  # Initial param
                    "write": {
                        "connection": "local",
                        "path": os.path.join(OUTPUT_DIR, "enriched_customers"),
                        "format": "delta",
                        "mode": "overwrite",
                        "options": {
                            "deep_diagnostics": True,
                            "diff_keys": ["id"],  # Identify updates by ID
                            "schema_mode": "overwrite",
                        },
                    },
                },
                {
                    "name": "join_data",
                    "depends_on": ["enrich_data", "load_transactions"],
                    "transformer": "join_transactions",
                    "params": {},
                },
                {
                    "name": "summarize_stats",
                    "depends_on": ["join_data"],
                    "transformer": "summarize_customer",
                    "params": {},
                    "write": {
                        "connection": "local",
                        "path": os.path.join(OUTPUT_DIR, "customer_stats"),
                        "format": "delta",
                        "mode": "overwrite",
                    },
                },
            ],
        }
    ],
    "connections": {"local": {"type": "local", "base_path": "."}},
}

CONFIG_PATH = os.path.join(BASE_DIR, "pipeline.odibi.yaml")


def save_config(cfg):
    with open(CONFIG_PATH, "w") as f:
        yaml.dump(cfg, f)


# --- 4. Run 1: Baseline ---
print("\n>>> STARTING RUN 1 (Baseline)...")

# Create Data Day 1
df_day1 = pd.DataFrame(
    {"id": [101, 102, 103], "name": ["Alice", "Bob", "Charlie"], "segment": ["A", "B", "A"]}
)
df_day1.to_csv(os.path.join(DATA_DIR, "customers.csv"), index=False)

# Create Transactions
df_trans = pd.DataFrame(
    {
        "trans_id": [1, 2, 3, 4],
        "customer_id": [101, 101, 102, 103],
        "amount": [100, 50, 200, 150],
        "date": ["2023-01-01", "2023-01-02", "2023-01-01", "2023-01-03"],
    }
)
df_trans.to_csv(os.path.join(DATA_DIR, "transactions.csv"), index=False)

# Save Config & Run
save_config(pipeline_config)
manager = PipelineManager.from_yaml(CONFIG_PATH)
result_1 = manager.run()

if result_1.failed:
    print("Run 1 FAILED!")
    for node in result_1.failed:
        res = result_1.node_results[node]
        try:
            print(f"Node {node} error: {str(res.error).encode('ascii', 'replace').decode()}")
        except Exception:
            print(f"Node {node} error: <Printing Failed>")
else:
    print("Run 1 SUCCESS")


# --- 5. Run 2: Changes Introduced ---
print("\n>>> MAKING CHANGES...")

# Change 1: Data Drift (New rows + Update + Schema Change + Nulls)
print("   - Adding new data (David, Eve) and new column (email)")
df_day2 = pd.DataFrame(
    {
        "id": [101, 102, 103, 104, 105],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "segment": ["A", "B", "A", "C", "C"],
        "email": [
            "alice@example.com",
            "bob@example.com",
            "charlie@example.com",
            None,
            "eve@example.com",
        ],
    }
)
df_day2.to_csv(os.path.join(DATA_DIR, "customers.csv"), index=False)

# Add more transactions for new users
df_trans_2 = pd.DataFrame(
    {
        "trans_id": [1, 2, 3, 4, 5, 6],
        "customer_id": [101, 101, 102, 103, 104, 105],
        "amount": [100, 50, 200, 150, 300, 250],
        "date": [
            "2023-01-01",
            "2023-01-02",
            "2023-01-01",
            "2023-01-03",
            "2023-01-04",
            "2023-01-04",
        ],
    }
)
df_trans_2.to_csv(os.path.join(DATA_DIR, "transactions.csv"), index=False)

# Change 2: Logic Drift (Config change)
print("   - Changing logic param: segment_prefix = 'GLOBAL-'")
pipeline_config["pipelines"][0]["nodes"][2]["params"]["segment_prefix"] = "GLOBAL-"
save_config(pipeline_config)

print("\n>>> STARTING RUN 2 (Changed)...")
manager = PipelineManager.from_yaml(CONFIG_PATH)
result_2 = manager.run()

if result_2.failed:
    print("Run 2 FAILED!")
    for node in result_2.failed:
        res = result_2.node_results[node]
        try:
            print(f"Node {node} error: {str(res.error).encode('ascii', 'replace').decode()}")
        except Exception:
            print(f"Node {node} error: <Printing Failed>")
else:
    print("Run 2 SUCCESS")


# --- 6. API Usage: Run Diff ---
print("\n\n>>> DIAGNOSTICS API DEMO")
print("========================")

# Initialize History Manager
history = HistoryManager(history_path=STORY_DIR)

# Get the two runs (automatically finds the .json files generated by the runs)
# Since we just ran them, they are the latest two.
runs = history.list_runs("customer_360")
print(f"Found {len(runs)} runs in history.")

if len(runs) >= 2:
    run_latest = history.load_run(runs[0]["path"])
    run_prev = history.load_run(runs[1]["path"])

    print(f"Comparing Run {run_prev.run_id} -> {run_latest.run_id}")

    # Perform Diff
    diff_result = diff_runs(run_prev, run_latest)

    # Analyze Results
    print("\n1. Logic Drift Analysis:")
    if diff_result.drift_source_nodes:
        print(f"   [!] Logic Changed in: {diff_result.drift_source_nodes}")
        for node in diff_result.drift_source_nodes:
            d = diff_result.node_diffs[node]
            if d.config_changed:
                print(f"      - {node}: Configuration parameters changed")
            if d.sql_changed:
                print(f"      - {node}: SQL logic changed")
    else:
        print("   [OK] No logic drift detected.")

    print("\n2. Data Impact Analysis:")
    for node, d in diff_result.node_diffs.items():
        if d.rows_diff != 0:
            print(f"   [Data] {node}: {d.rows_diff:+} rows (v{d.delta_version_change})")

    # Show HTML Story path
    html_path = runs[0]["path"].replace(".json", ".html")
    print(f"\n   > View HTML Story: {html_path}")


# --- 7. API Usage: Delta Diff ---
print("\n3. Delta Deep Dive (Spark Powered):")
# Direct path to the Delta table
delta_path = os.path.join(OUTPUT_DIR, "enriched_customers")

# Try to initialize Spark for advanced diff
spark = None
try:
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    print("   [INFO] Initializing Spark for Deep Diagnostics...")
    builder = (
        SparkSession.builder.appName("odibi_demo")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
except ImportError:
    print("   [WARN] PySpark/Delta not found. Falling back to Pandas implementation.")
except Exception as e:
    print(f"   [WARN] Spark initialization failed: {e}. Falling back to Pandas.")

# Compare latest version (v1) vs previous (v0)
delta_diff = get_delta_diff(
    table_path=delta_path,
    version_a=0,
    version_b=1,
    deep=True,  # Enable row-level diff
    keys=["id"],  # Use keys to detect updates
    spark=spark,  # Use Spark if available
)

print(f"   Table: {delta_path}")
print(f"   Rows Changed: {delta_diff.rows_change}")
print(f"   Rows Added: {delta_diff.rows_added}")
print(f"   Rows Removed: {delta_diff.rows_removed}")
print(f"   Rows Updated: {delta_diff.rows_updated}")

print(f"   Files Added/Removed: {delta_diff.files_change}")

if delta_diff.sample_updated:
    print("\n   >>> Sample Updated Rows:")
    print(json.dumps(delta_diff.sample_updated[:2], default=str, indent=6))

if delta_diff.sample_added:
    print("\n   >>> Sample Added Rows:")
    # Just print first 2 rows for brevity
    print(json.dumps(delta_diff.sample_added[:2], default=str, indent=6))

print("\nDone.")
