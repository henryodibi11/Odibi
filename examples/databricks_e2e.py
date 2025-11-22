import os
import pandas as pd
import yaml
import logging
from pyspark.sql import SparkSession
from odibi.pipeline import PipelineManager

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("OdibiE2E")

# --- Configuration ---
# Databricks E2E Setup
# We need two paths:
# 1. PYTHON_ROOT: Local path for Python file IO (Data Gen, Stories) -> /dbfs/...
# 2. SPARK_ROOT: Distributed path for Spark IO -> dbfs:/...

if os.path.exists("/dbfs"):
    # Databricks environment with DBFS mount
    PYTHON_ROOT = "/dbfs/tmp/odibi_e2e"
    SPARK_ROOT = "dbfs:/tmp/odibi_e2e"
else:
    # Local fallback (Single Node / Laptop)
    import tempfile

    PYTHON_ROOT = tempfile.mkdtemp(prefix="odibi_e2e_")
    SPARK_ROOT = f"file://{PYTHON_ROOT}"

CONFIG_PATH = f"{PYTHON_ROOT}/project.yaml"
DATA_DIR = f"{PYTHON_ROOT}/data"

# Clean start
try:
    import shutil

    shutil.rmtree(PYTHON_ROOT, ignore_errors=True)
except Exception:
    pass

print(f"🚀 Running E2E Test")
print(f"📂 Python Path: {PYTHON_ROOT}")
print(f"⚡ Spark Path:  {SPARK_ROOT}")


# --- 1. Data Generation ---
def generate_data():
    os.makedirs(DATA_DIR, exist_ok=True)

    # 1. Dirty Sales Data (CSV) - Latin1 encoding
    sales_data = {
        "transaction_id": [101, 102, 102, 103, 104, 105],
        "customer_id": [1, 2, 2, 1, 3, 2],
        "product": ["Widget A", "Widget B", "Widget B", "Widget A", "Widget C", "Widget A"],
        "category": ["Hardware", "Software", "Software", "Hardware", "Services", "Hardware"],
        "amount": [100.50, 50.00, 50.00, None, 200.00, 100.50],
        "date": [
            "2023-01-01",
            "2023-01-02",
            "2023-01-02",
            "2023-01-03",
            "2023-01-04",
            "2023-01-05",
        ],
    }
    df_sales = pd.DataFrame(sales_data)
    df_sales.to_csv(f"{DATA_DIR}/sales_dirty.csv", index=False, encoding="latin1")
    print(f"✅ Generated sales_dirty.csv")

    # 2. Customers Data (JSON)
    cust_data = {
        "customer_id": [1, 2, 3],
        "name": ["Alice Corp", "Bob Inc", "Charlie Ltd"],
        "region": ["North", "South", "East"],
    }
    df_cust = pd.DataFrame(cust_data)
    df_cust.to_json(f"{DATA_DIR}/customers.json", orient="records", lines=True)
    print(f"✅ Generated customers.json")


# --- 2. Odibi Configuration ---
def create_config():
    # We define TWO connections:
    # 1. 'data_lake': Uses SPARK_ROOT (dbfs:/) for Spark operations
    # 2. 'reporting': Uses PYTHON_ROOT (/dbfs/) for Story generation (Python IO)

    config = {
        "project": "Spark E2E Verification",
        "engine": "spark",
        "connections": {
            "data_lake": {"type": "local", "base_path": f"{SPARK_ROOT}/data"},
            "reporting": {"type": "local", "base_path": f"{PYTHON_ROOT}/data"},
        },
        "story": {"connection": "reporting", "path": "stories", "auto_generate": True},
        "pipelines": [
            {
                "pipeline": "bronze_to_silver",
                "description": "Ingest and clean data",
                "nodes": [
                    {
                        "name": "load_sales",
                        "read": {
                            "connection": "data_lake",
                            "path": "sales_dirty.csv",
                            "format": "csv",
                            "options": {"header": True, "inferSchema": True, "auto_encoding": True},
                        },
                    },
                    {
                        "name": "clean_sales",
                        "depends_on": ["load_sales"],
                        "transform": {
                            "steps": [
                                {"operation": "drop_duplicates"},
                                {
                                    "operation": "fillna",
                                    "params": {"value": 0.0, "subset": ["amount"]},
                                },
                                {
                                    "operation": "rename",
                                    "params": {"columns": {"transaction_id": "id"}},
                                },
                            ]
                        },
                        "validation": {"no_nulls": ["id", "amount"]},
                        "write": {
                            "connection": "data_lake",
                            "path": "silver_sales",
                            "format": "delta",
                            "mode": "overwrite",
                        },
                    },
                ],
            },
            {
                "pipeline": "silver_to_gold",
                "description": "Aggregate and Reporting",
                "nodes": [
                    {
                        "name": "read_silver",
                        "read": {
                            "connection": "data_lake",
                            "path": "silver_sales",
                            "format": "delta",
                        },
                    },
                    {
                        "name": "read_customers",
                        "read": {
                            "connection": "data_lake",
                            "path": "customers.json",
                            "format": "json",
                        },
                    },
                    {
                        "name": "create_report",
                        "depends_on": ["read_silver", "read_customers"],
                        "transform": {
                            "steps": [
                                {
                                    "operation": "pivot",
                                    "params": {
                                        "group_by": ["customer_id"],
                                        "pivot_column": "category",
                                        "value_column": "amount",
                                        "agg_func": "sum",
                                    },
                                },
                                {
                                    # Note: SparkContext sanitizes names.
                                    # 'read_customers' -> 'read_customers' (valid)
                                    # 'current_df' is the pivoted result of 'read_silver'
                                    "sql": """
                                        SELECT 
                                            c.name,
                                            c.region,
                                            p.* 
                                        FROM current_df p
                                        JOIN read_customers c ON p.customer_id = c.customer_id
                                    """
                                },
                            ]
                        },
                        "write": {
                            "connection": "data_lake",
                            "path": "gold_report",
                            "format": "delta",
                            "mode": "overwrite",
                        },
                    },
                ],
            },
        ],
    }

    with open(CONFIG_PATH, "w") as f:
        yaml.dump(config, f)
    print(f"✅ Generated Config: {CONFIG_PATH}")


# --- 3. Execution ---
def run_pipeline():
    print("\n🚀 Starting Odibi Pipeline Execution...")

    manager = PipelineManager.from_yaml(CONFIG_PATH)

    print("\n--- Running Bronze to Silver ---")
    res1 = manager.run("bronze_to_silver")
    if res1.failed:
        print(f"❌ Pipeline Failed: {res1.failed}")
        # Don't exit, try to print story path if available
        if res1.story_path:
            print(f"Story: {res1.story_path}")
        exit(1)

    print("\n--- Running Silver to Gold ---")
    res2 = manager.run("silver_to_gold")
    if res2.failed:
        print(f"❌ Pipeline Failed: {res2.failed}")
        if res2.story_path:
            print(f"Story: {res2.story_path}")
        exit(1)

    print("\n✅ E2E Verification Successful!")
    print(f"Stories generated at: {DATA_DIR}/stories/")

    try:
        spark = SparkSession.builder.getOrCreate()
        print("\n📊 Gold Report Preview:")
        # Read using SPARK_ROOT
        df = spark.read.format("delta").load(f"{SPARK_ROOT}/data/gold_report")
        df.show()
    except Exception as e:
        print(f"⚠️ Could not preview result: {e}")


if __name__ == "__main__":
    generate_data()
    create_config()
    run_pipeline()
