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
# Use local temporary directory for reliability (avoiding DBFS mount issues)
# We will force Spark to use file:// scheme
BASE_DIR = "/tmp/odibi_e2e"
CONFIG_PATH = f"{BASE_DIR}/project.yaml"
DATA_DIR = f"{BASE_DIR}/data"

# Clean start
try:
    import shutil

    shutil.rmtree(BASE_DIR, ignore_errors=True)
except Exception:
    pass

print(f"🚀 Running E2E Test in: {BASE_DIR}")


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
    print(f"✅ Generated {DATA_DIR}/sales_dirty.csv (Latin1)")

    # 2. Customers Data (JSON)
    cust_data = {
        "customer_id": [1, 2, 3],
        "name": ["Alice Corp", "Bob Inc", "Charlie Ltd"],
        "region": ["North", "South", "East"],
    }
    df_cust = pd.DataFrame(cust_data)
    df_cust.to_json(f"{DATA_DIR}/customers.json", orient="records", lines=True)
    print(f"✅ Generated {DATA_DIR}/customers.json")


# --- 2. Odibi Configuration ---
def create_config():
    # IMPORTANT: We use file:// prefix for base_path so Spark knows it's local file system
    # regardless of where it's running (Driver node).
    config = {
        "project": "Spark E2E Verification",
        "engine": "spark",
        "connections": {"local_data": {"type": "local", "base_path": f"file://{DATA_DIR}"}},
        "story": {"connection": "local_data", "path": "stories", "auto_generate": True},
        "pipelines": [
            {
                "pipeline": "bronze_to_silver",
                "description": "Ingest and clean data",
                "nodes": [
                    {
                        "name": "load_sales",
                        "read": {
                            "connection": "local_data",
                            "path": "sales_dirty.csv",
                            "format": "csv",
                            "options": {
                                "header": True,
                                "inferSchema": True,
                                "auto_encoding": True,  # Feature Test
                            },
                        },
                    },
                    {
                        "name": "clean_sales",
                        "depends_on": ["load_sales"],
                        "transform": {
                            "steps": [
                                {"operation": "drop_duplicates"},  # Feature Test: Default subset
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
                            "connection": "local_data",
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
                            "connection": "local_data",
                            "path": "silver_sales",
                            "format": "delta",
                        },
                    },
                    {
                        "name": "read_customers",
                        "read": {
                            "connection": "local_data",
                            "path": "customers.json",
                            "format": "json",
                        },
                    },
                    {
                        "name": "create_report",
                        "depends_on": ["read_silver", "read_customers"],
                        "transform": {
                            "steps": [
                                # Feature Test: Pivot
                                {
                                    "operation": "pivot",
                                    "params": {
                                        "group_by": ["customer_id"],
                                        "pivot_column": "category",
                                        "value_column": "amount",
                                        "agg_func": "sum",
                                    },
                                },
                                # Feature Test: SQL Context (joining with registered customers)
                                # Note: 'read_customers' registered a view named 'read_customers'
                                {
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
                            "connection": "local_data",
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

    # Load Project
    manager = PipelineManager.from_yaml(CONFIG_PATH)

    # Run Bronze -> Silver
    print("\n--- Running Bronze to Silver ---")
    res1 = manager.run("bronze_to_silver")
    if res1.failed:
        print(f"❌ Pipeline Failed: {res1.failed}")
        exit(1)

    # Run Silver -> Gold
    print("\n--- Running Silver to Gold ---")
    res2 = manager.run("silver_to_gold")
    if res2.failed:
        print(f"❌ Pipeline Failed: {res2.failed}")
        exit(1)

    print("\n✅ E2E Verification Successful!")
    print(f"Stories generated at: {DATA_DIR}/stories/")

    # Optional: Peek at result
    try:
        spark = SparkSession.builder.getOrCreate()
        print("\n📊 Gold Report Preview:")
        df = spark.read.format("delta").load(f"{DATA_DIR}/gold_report")
        df.show()
    except Exception as e:
        print(f"⚠️ Could not preview result: {e}")


if __name__ == "__main__":
    generate_data()
    create_config()
    run_pipeline()
