import os
import sys
import shutil
from pyspark.sql import SparkSession

# Ensure odibi is in path if running from repo
sys.path.append(os.getcwd())

from odibi.pipeline import PipelineManager  # noqa: E402

# Setup Paths
BASE_PATH = "/tmp/odibi_databricks_test"
SILVER_PATH = f"{BASE_PATH}/silver/orders"
BRONZE_PATH = f"{BASE_PATH}/bronze/orders_updates"
CONFIG_PATH = f"{BASE_PATH}/project.yaml"

# Cleanup previous run
if os.path.exists(BASE_PATH):
    try:
        shutil.rmtree(BASE_PATH)
    except Exception as e:
        print(f"Warning: Could not delete {BASE_PATH} locally (might be DBFS/Cloud path): {e}")


def main():
    # Get Spark Session
    try:
        from delta import configure_spark_with_delta_pip

        builder = (
            SparkSession.builder.appName("OdibiSmokeTest")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    except ImportError:
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
    df_initial.write.format("delta").mode("overwrite").save(SILVER_PATH)

    print(f"Created Target Delta Table at: {SILVER_PATH}")

    # 2. Config Template (Parameterized for reuse)
    # -------------------------------------------
    def create_config(pipeline_name, strategy, source_path):
        config_yaml = f"""
project: Databricks_Merge_Test
engine: spark

connections:
  local_tmp:
    type: local
    base_path: {BASE_PATH}

  story_conn:
    type: local
    base_path: {BASE_PATH}/stories

story:
  connection: story_conn
  path: /

pipelines:
  - pipeline: {pipeline_name}
    nodes:
      - name: apply_merge
        read:
          connection: local_tmp
          format: parquet
          path: {source_path}

        transformer: merge
        params:
          target: {SILVER_PATH}
          keys: [order_id]
          strategy: {strategy}
          audit_cols:
            created_col: record_created_at
            updated_col: record_updated_at
"""
        os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
        with open(CONFIG_PATH, "w") as f:
            f.write(config_yaml)

    # ==========================================
    # TEST 1: UPSERT STRATEGY
    # ==========================================
    print("\n=== TEST 1: UPSERT STRATEGY ===")

    # Source: Update 1, Insert 3
    upsert_data = [
        (1, "Order_1", "CLOSED", "2024-01-02"),
        (3, "Order_3", "OPEN", "2024-01-02"),
    ]
    spark.createDataFrame(
        upsert_data, ["order_id", "customer", "status", "created_date"]
    ).write.mode("overwrite").parquet(f"{BRONZE_PATH}_upsert")

    create_config("pipeline_upsert", "upsert", "bronze/orders_updates_upsert")
    PipelineManager.from_yaml(CONFIG_PATH).run()

    # Verify Upsert
    rows = spark.read.format("delta").load(SILVER_PATH).orderBy("order_id").collect()
    assert len(rows) == 3

    r1 = [r for r in rows if r.order_id == 1][0]
    assert r1.status == "CLOSED", "Order 1 should be updated"
    assert r1.record_updated_at is not None

    r2 = [r for r in rows if r.order_id == 2][0]
    assert r2.status == "OPEN", "Order 2 should be untouched"

    r3 = [r for r in rows if r.order_id == 3][0]
    assert r3.status == "OPEN", "Order 3 should be inserted"
    assert r3.record_created_at is not None

    print("✅ Upsert Verified")

    # ==========================================
    # TEST 2: APPEND_ONLY STRATEGY
    # ==========================================
    print("\n=== TEST 2: APPEND_ONLY STRATEGY ===")

    # Source: Update 3 (Should Ignore), Insert 4
    append_data = [
        (3, "Order_3", "CLOSED", "2024-01-03"),  # Should be ignored (it exists)
        (4, "Order_4", "OPEN", "2024-01-03"),  # Should be inserted
    ]
    spark.createDataFrame(
        append_data, ["order_id", "customer", "status", "created_date"]
    ).write.mode("overwrite").parquet(f"{BRONZE_PATH}_append")

    create_config("pipeline_append", "append_only", "bronze/orders_updates_append")
    PipelineManager.from_yaml(CONFIG_PATH).run()

    # Verify Append
    rows = spark.read.format("delta").load(SILVER_PATH).orderBy("order_id").collect()
    assert len(rows) == 4

    r3 = [r for r in rows if r.order_id == 3][0]
    assert r3.status == "OPEN", "Order 3 update should be ignored in append_only"

    r4 = [r for r in rows if r.order_id == 4][0]
    assert r4.status == "OPEN", "Order 4 should be inserted"

    print("✅ Append-Only Verified")

    # ==========================================
    # TEST 3: DELETE_MATCH STRATEGY
    # ==========================================
    print("\n=== TEST 3: DELETE_MATCH STRATEGY ===")

    # Source: Match 2, 4 (Should Delete)
    delete_data = [
        (2, "Order_2", "ANY", "2024-01-01"),
        (4, "Order_4", "ANY", "2024-01-03"),
    ]
    spark.createDataFrame(
        delete_data, ["order_id", "customer", "status", "created_date"]
    ).write.mode("overwrite").parquet(f"{BRONZE_PATH}_delete")

    create_config("pipeline_delete", "delete_match", "bronze/orders_updates_delete")
    PipelineManager.from_yaml(CONFIG_PATH).run()

    # Verify Delete
    rows = spark.read.format("delta").load(SILVER_PATH).orderBy("order_id").collect()
    assert len(rows) == 2  # Should have 1 and 3 left

    ids = [r.order_id for r in rows]
    assert 2 not in ids, "Order 2 should be deleted"
    assert 4 not in ids, "Order 4 should be deleted"
    assert 1 in ids and 3 in ids, "Orders 1 and 3 should remain"

    print("✅ Delete-Match Verified")

    print("\n>>> ALL TESTS PASSED SUCCESSFULLY! <<<")


if __name__ == "__main__":
    main()
