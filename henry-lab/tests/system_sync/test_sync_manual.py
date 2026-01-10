"""
Manual test script for system sync functionality.

This script creates sample data and tests the sync flow end-to-end.
Outputs go to henry-lab/tests/system_sync/output/ for inspection.

Usage:
    cd odibi
    python henry-lab/tests/system_sync/test_sync_manual.py
"""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add odibi to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pandas as pd
from deltalake import DeltaTable, write_deltalake

from odibi.state import (
    CatalogStateBackend,
    SqlServerSystemBackend,
    create_sync_source_backend,
    sync_system_data,
)
from odibi.config import SyncFromConfig


# Output directory
OUTPUT_DIR = Path(__file__).parent / "output"
SOURCE_DIR = OUTPUT_DIR / "source_system"
TARGET_DIR = OUTPUT_DIR / "target_system"


def setup_directories():
    """Create output directories."""
    print("=" * 60)
    print("SYSTEM SYNC MANUAL TEST")
    print("=" * 60)
    print(f"\nOutput directory: {OUTPUT_DIR}")

    # Clean and recreate
    import shutil

    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)

    SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    TARGET_DIR.mkdir(parents=True, exist_ok=True)

    print(f"  Source: {SOURCE_DIR}")
    print(f"  Target: {TARGET_DIR}")


def create_sample_runs_data() -> pd.DataFrame:
    """Create realistic sample run data."""
    now = datetime.now(timezone.utc)

    runs = [
        {
            "run_id": "run_20240615_001",
            "pipeline_name": "bronze_ingestion",
            "node_name": "ingest_customers",
            "status": "SUCCESS",
            "rows_processed": 15234,
            "duration_ms": 4521,
            "metrics_json": '{"source": "salesforce", "api_calls": 3}',
            "environment": "dev",
            "timestamp": now,
            "date": now.date(),
        },
        {
            "run_id": "run_20240615_002",
            "pipeline_name": "bronze_ingestion",
            "node_name": "ingest_orders",
            "status": "SUCCESS",
            "rows_processed": 89432,
            "duration_ms": 12543,
            "metrics_json": '{"source": "erp", "batch_size": 10000}',
            "environment": "dev",
            "timestamp": now,
            "date": now.date(),
        },
        {
            "run_id": "run_20240615_003",
            "pipeline_name": "bronze_ingestion",
            "node_name": "ingest_products",
            "status": "FAILED",
            "rows_processed": 0,
            "duration_ms": 543,
            "metrics_json": '{"error": "Connection timeout", "retry_count": 3}',
            "environment": "dev",
            "timestamp": now,
            "date": now.date(),
        },
        {
            "run_id": "run_20240615_004",
            "pipeline_name": "silver_transform",
            "node_name": "dedupe_customers",
            "status": "SUCCESS",
            "rows_processed": 14892,
            "duration_ms": 8932,
            "metrics_json": '{"duplicates_removed": 342}',
            "environment": "dev",
            "timestamp": now,
            "date": now.date(),
        },
        {
            "run_id": "run_20240615_005",
            "pipeline_name": "gold_aggregation",
            "node_name": "daily_sales_summary",
            "status": "SUCCESS",
            "rows_processed": 1,
            "duration_ms": 23421,
            "metrics_json": '{"total_sales": 1543210.50, "order_count": 4521}',
            "environment": "dev",
            "timestamp": now,
            "date": now.date(),
        },
    ]

    return pd.DataFrame(runs)


def create_sample_state_data() -> pd.DataFrame:
    """Create realistic sample HWM state data."""
    now = datetime.now(timezone.utc)

    state = [
        {
            "key": "bronze_ingestion.ingest_customers.hwm",
            "value": '"2024-06-15T10:30:00Z"',
            "environment": "dev",
            "updated_at": now,
        },
        {
            "key": "bronze_ingestion.ingest_orders.hwm",
            "value": '{"last_order_id": 9999999, "last_modified": "2024-06-15T11:00:00Z"}',
            "environment": "dev",
            "updated_at": now,
        },
        {
            "key": "bronze_ingestion.ingest_products.hwm",
            "value": '"2024-06-14T23:59:59Z"',
            "environment": "dev",
            "updated_at": now,
        },
        {
            "key": "silver_transform.dedupe_customers.version",
            "value": '{"schema_version": "2.1", "last_full_refresh": "2024-06-01"}',
            "environment": "dev",
            "updated_at": now,
        },
    ]

    return pd.DataFrame(state)


def write_source_data():
    """Write sample data to source Delta tables."""
    print("\n" + "-" * 40)
    print("STEP 1: Creating source data")
    print("-" * 40)

    # Write runs
    runs_df = create_sample_runs_data()
    runs_path = str(SOURCE_DIR / "meta_runs")
    write_deltalake(runs_path, runs_df, mode="overwrite")
    print(f"  ✓ Wrote {len(runs_df)} runs to {runs_path}")

    # Write state
    state_df = create_sample_state_data()
    state_path = str(SOURCE_DIR / "meta_state")
    write_deltalake(state_path, state_df, mode="overwrite")
    print(f"  ✓ Wrote {len(state_df)} state records to {state_path}")

    return runs_df, state_df


def test_sync_delta_to_delta():
    """Test syncing from Delta source to Delta target."""
    print("\n" + "-" * 40)
    print("STEP 2: Sync Delta → Delta")
    print("-" * 40)

    source_backend = CatalogStateBackend(
        meta_runs_path=str(SOURCE_DIR / "meta_runs"),
        meta_state_path=str(SOURCE_DIR / "meta_state"),
        environment="dev",
    )

    target_backend = CatalogStateBackend(
        meta_runs_path=str(TARGET_DIR / "meta_runs"),
        meta_state_path=str(TARGET_DIR / "meta_state"),
        environment="prod",  # Note: target has different environment
    )

    print("  Source: CatalogStateBackend (Delta, env=dev)")
    print("  Target: CatalogStateBackend (Delta, env=prod)")
    print("\n  Syncing...")

    result = sync_system_data(source_backend, target_backend)

    print(f"\n  Results:")
    print(f"    Runs synced:  {result['runs']}")
    print(f"    State synced: {result['state']}")

    return result


def verify_target_data():
    """Verify the synced data in target."""
    print("\n" + "-" * 40)
    print("STEP 3: Verify target data")
    print("-" * 40)

    # Read target runs
    runs_dt = DeltaTable(str(TARGET_DIR / "meta_runs"))
    runs_df = runs_dt.to_pandas()

    print(f"\n  Target meta_runs ({len(runs_df)} records):")
    print(f"    Columns: {list(runs_df.columns)}")
    print(f"    Pipelines: {runs_df['pipeline_name'].unique().tolist()}")
    print(f"    Statuses: {runs_df['status'].value_counts().to_dict()}")
    print(f"    Environment: {runs_df['environment'].unique().tolist()}")

    # Read target state
    state_dt = DeltaTable(str(TARGET_DIR / "meta_state"))
    state_df = state_dt.to_pandas()

    print(f"\n  Target meta_state ({len(state_df)} records):")
    print(f"    Keys: {state_df['key'].tolist()}")
    print(f"    Environment: {state_df['environment'].unique().tolist()}")

    return runs_df, state_df


def test_create_sync_source_backend():
    """Test the create_sync_source_backend factory function."""
    print("\n" + "-" * 40)
    print("STEP 4: Test create_sync_source_backend")
    print("-" * 40)

    sync_from = SyncFromConfig(
        connection="local_dev",
        path="output/source_system",
    )

    connections = {
        "local_dev": {
            "type": "local",
            "base_path": str(Path(__file__).parent),
        }
    }

    backend = create_sync_source_backend(
        sync_from_config=sync_from,
        connections=connections,
        project_root=str(Path(__file__).parent),
    )

    print(f"  Created backend: {type(backend).__name__}")
    print(f"  meta_runs_path: {backend.meta_runs_path}")
    print(f"  meta_state_path: {backend.meta_state_path}")

    # Try to read from it
    hwm = backend.get_hwm("bronze_ingestion.ingest_customers.hwm")
    print(f"\n  Test get_hwm: {hwm}")

    return backend


def print_summary():
    """Print summary of output files."""
    print("\n" + "=" * 60)
    print("TEST COMPLETE - Output files for inspection:")
    print("=" * 60)

    for root, dirs, files in os.walk(OUTPUT_DIR):
        level = root.replace(str(OUTPUT_DIR), "").count(os.sep)
        indent = "  " * level
        print(f"{indent}{os.path.basename(root)}/")

        sub_indent = "  " * (level + 1)
        for file in files:
            file_path = Path(root) / file
            size = file_path.stat().st_size
            print(f"{sub_indent}{file} ({size:,} bytes)")


def main():
    """Run all manual tests."""
    try:
        setup_directories()
        write_source_data()
        result = test_sync_delta_to_delta()
        runs_df, state_df = verify_target_data()
        test_create_sync_source_backend()
        print_summary()

        print("\n✓ All manual tests passed!")
        return 0

    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
