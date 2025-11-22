import os
import sys
import shutil
from pyspark.sql import SparkSession

# Ensure odibi is in path if running from repo
sys.path.append(os.getcwd())

from odibi.pipeline import PipelineManager  # noqa: E402

# Setup Paths
BASE_PATH = "/tmp/odibi_databricks_test"
TARGET_PATH = f"{BASE_PATH}/silver/orders"
SOURCE_PATH = f"{BASE_PATH}/bronze/orders_updates"
CONFIG_PATH = f"{BASE_PATH}/project.yaml"

# Cleanup previous run
if os.path.exists(BASE_PATH):
    try:
        shutil.rmtree(BASE_PATH)
    except Exception as e:
        print(f"Warning: Could not delete {BASE_PATH} locally (might be DBFS/Cloud path): {e}")


def main():
    # Get Spark Session
    spark = SparkSession.builder.appName("OdibiSmokeTest").getOrCreate()

    print(">>> Setting up Test Data...")

    # 1. Create Initial Target Table (Delta)
    # --------------------------------------
    initial_data = [
        (1, "Order_1", "OPEN", "2024-01-01"),
        (2, "Order_2", "OPEN", "2024-01-01"),
    ]
    df_initial = spark.createDataFrame(
        initial_data, ["order_id", "customer", "status", "created_date"]
    )
    df_initial.write.format("delta").mode("overwrite").save(TARGET_PATH)

    print(f"Created Target Delta Table at: {TARGET_PATH}")
    spark.read.format("delta").load(TARGET_PATH).show()

    # 2. Create Source Updates (Parquet)
    # ----------------------------------
    # Update Order_1 -> CLOSED
    # Insert Order_3 -> OPEN
    update_data = [
        (1, "Order_1", "CLOSED", "2024-01-02"),
        (3, "Order_3", "OPEN", "2024-01-02"),
    ]
    df_updates = spark.createDataFrame(
        update_data, ["order_id", "customer", "status", "created_date"]
    )
    df_updates.write.mode("overwrite").parquet(SOURCE_PATH)

    print(f"Created Source Updates at: {SOURCE_PATH}")

    # 3. Create Odibi Config
    # ----------------------
    config_yaml = f"""
project: Databricks_Merge_Test
engine: spark

connections:
  local_tmp:
    type: local
    base_path: {BASE_PATH}

  # Dummy connection for story output (required by validation)
  story_conn:
    type: local
    base_path: {BASE_PATH}/stories

story:
  connection: story_conn
  path: /

pipelines:
  - pipeline: merge_orders_pipeline
    nodes:
      - name: apply_merge
        read:
          connection: local_tmp
          format: parquet
          path: bronze/orders_updates

        transformer: merge
        params:
          target: {TARGET_PATH}  # Using absolute path for simplicity in test
          keys: [order_id]
          strategy: upsert
          audit_cols:
            created_col: record_created_at
            updated_col: record_updated_at
"""

    # Ensure directory exists before writing config
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)

    with open(CONFIG_PATH, "w") as f:
        f.write(config_yaml)

    print(f"Created Config at: {CONFIG_PATH}")

    # 4. Run Pipeline
    # ---------------
    print("\n>>> Running Odibi Pipeline...")

    # Initialize Manager
    manager = PipelineManager.from_yaml(CONFIG_PATH)

    # Run
    results = manager.run()

    print("\n>>> Pipeline Results:")
    print(f"Success: {not bool(results.failed)}")
    print(f"Failed Nodes: {results.failed}")

    # 5. Verify Results
    # -----------------
    print("\n>>> Verifying Delta Table State...")

    df_final = spark.read.format("delta").load(TARGET_PATH).orderBy("order_id")
    df_final.show(truncate=False)

    rows = df_final.collect()

    # Assertions
    assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"

    # Check Order 1 (Updated)
    row_1 = [r for r in rows if r.order_id == 1][0]
    assert row_1.status == "CLOSED", f"Order 1 should be CLOSED, got {row_1.status}"
    assert row_1.record_updated_at is not None, "Order 1 should have updated_at"

    # Check Order 2 (Untouched)
    row_2 = [r for r in rows if r.order_id == 2][0]
    assert row_2.status == "OPEN", "Order 2 should remain OPEN"
    # Note: record_created_at/updated_at might be null for existing rows
    # if schema evolution didn't backfill,
    # or if merge added columns they will be null for non-matched rows.
    # The merge transformer adds audit cols to source. Target schema evolves.

    # Check Order 3 (Inserted)
    row_3 = [r for r in rows if r.order_id == 3][0]
    assert row_3.status == "OPEN", "Order 3 should be OPEN"
    assert row_3.record_created_at is not None, "Order 3 should have created_at"
    assert row_3.record_updated_at is not None, "Order 3 should have updated_at"

    print("\n>>> TEST PASSED SUCCESSFULLY! <<<")


if __name__ == "__main__":
    main()
