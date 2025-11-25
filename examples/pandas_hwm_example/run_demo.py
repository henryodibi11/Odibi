import os
import subprocess
import sys

import pandas as pd

# Setup paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
PROJECT_FILE = os.path.join(BASE_DIR, "project.yaml")

SOURCE_FILE = os.path.join(DATA_DIR, "source_orders.csv")
TARGET_FILE = os.path.join(DATA_DIR, "bronze_orders.parquet")


def setup_data_day1():
    """Create Day 1 data (Jan 1st)."""
    df = pd.DataFrame(
        {
            "order_id": [1, 2],
            "amount": [100.0, 200.0],
            "created_at": ["2025-01-01", "2025-01-01"],
            "updated_at": ["2025-01-01", "2025-01-01"],
        }
    )
    df.to_csv(SOURCE_FILE, index=False)
    print(f"[Setup] Day 1 Data Created: {len(df)} rows")


def setup_data_day2():
    """Create Day 2 data (Day 1 + Updates + New)."""
    df = pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "amount": [100.0, 250.0, 300.0],  # Order 2 updated
            "created_at": ["2025-01-01", "2025-01-01", "2025-01-02"],
            "updated_at": ["2025-01-01", "2025-01-02", "2025-01-02"],
        }
    )
    df.to_csv(SOURCE_FILE, index=False)
    print(f"[Setup] Day 2 Data Created: {len(df)} rows (Order 2 updated, Order 3 new)")


def run_odibi():
    """Run Odibi pipeline."""
    print("[Odibi] Running Pipeline...")
    cmd = [sys.executable, "-m", "odibi.cli.main", "run", "project.yaml"]
    result = subprocess.run(cmd, cwd=BASE_DIR, capture_output=True, text=True)
    if result.returncode != 0:
        print("[Odibi] Run Failed:")
        print(result.stderr)
        print(result.stdout)
        sys.exit(1)
    else:
        print("[Odibi] Run Success")
        # print(result.stdout)


def verify_output(expected_count, label):
    """Verify the output parquet file."""
    if not os.path.exists(TARGET_FILE):
        print(f"[{label}] TARGET FILE NOT FOUND: {TARGET_FILE}")
        return

    df = pd.read_parquet(TARGET_FILE)
    print(f"[{label}] Output Rows: {len(df)}")
    print(df)

    if len(df) != expected_count:
        print(f"[{label}] FAIL: Expected {expected_count} rows, got {len(df)}")
    else:
        print(f"[{label}] PASS: Count matches expected")


def main():
    # Clean slate
    if os.path.exists(TARGET_FILE):
        os.remove(TARGET_FILE)

    print("\n=== DAY 1 (FIRST RUN) ===")
    print(
        "Scenario: Target table does not exist. HWM logic should use 'first_run_query' (load all)."
    )
    setup_data_day1()
    run_odibi()
    # Expected: 2 rows
    verify_output(2, "Day 1")

    print("\n=== DAY 2 (INCREMENTAL) ===")
    print("Scenario: Target table exists. HWM logic should use 'query' (updated_at >= 2025-01-02).")
    setup_data_day2()
    run_odibi()

    # Expected:
    # Day 1 data (2 rows) is already in target.
    # Day 2 run loads records where updated_at >= 2025-01-02.
    # From setup_data_day2:
    # Order 1: updated_at 2025-01-01 (Skipped)
    # Order 2: updated_at 2025-01-02 (Loaded - update)
    # Order 3: updated_at 2025-01-02 (Loaded - new)
    # So 2 new rows appended. Total = 2 + 2 = 4.
    verify_output(4, "Day 2")


if __name__ == "__main__":
    main()
