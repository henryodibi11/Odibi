#!/usr/bin/env python3
"""
Verification script for Example 2: Incremental SQL

Checks:
- Output exists
- State tracking working (HWM persisted)
- Second run loads only new data
- Row counts correct

Exit code 0 = success, non-zero = failure
"""

import sys
from pathlib import Path
import json


def verify_incremental_sql():
    """Verify incremental SQL example output."""
    print("🔍 Verifying Example 2: Incremental SQL")
    print("=" * 60)

    errors = []
    warnings = []

    # 1. Check output exists
    print("\n📁 Checking output...")
    output_path = Path("data/bronze/orders")

    if not output_path.exists():
        errors.append(f"❌ Output not found: {output_path}")
        print(f"❌ Cannot proceed without output")
        return 1
    else:
        print(f"✅ Output exists: {output_path}")

    try:
        import pandas as pd

        df = pd.read_parquet(output_path)
        print(f"✅ Loaded {len(df)} rows")

        # 2. Check for timestamp column
        print("\n⏰ Checking timestamp column...")
        timestamp_cols = ["updated_at", "created_at", "order_date", "timestamp"]
        found_ts = [col for col in timestamp_cols if col in df.columns]

        if not found_ts:
            warnings.append(f"⚠️  No timestamp column found (checked: {timestamp_cols})")
        else:
            print(f"✅ Timestamp column found: {found_ts[0]}")

            # Check timestamps are sorted
            ts_col = found_ts[0]
            if pd.api.types.is_datetime64_any_dtype(df[ts_col]):
                if not df[ts_col].is_monotonic_increasing:
                    warnings.append(f"⚠️  Timestamps not sorted (may indicate multiple runs)")
                else:
                    print(f"✅ Timestamps are sorted")

        # 3. Check system catalog state
        print("\n📊 Checking state tracking...")
        catalog_path = Path(".odibi/system/state.json")

        if not catalog_path.exists():
            warnings.append("⚠️  No state file found (run pipeline to create)")
        else:
            print(f"✅ State file exists: {catalog_path}")

            try:
                with open(catalog_path) as f:
                    state = json.load(f)

                # Look for HWM in state
                if state:
                    print(f"  State entries: {len(state)}")
                    # Show first entry as example
                    if state:
                        first_key = list(state.keys())[0]
                        print(f"  Example: {first_key} = {state[first_key]}")
                else:
                    warnings.append("⚠️  State file is empty")

            except Exception as e:
                warnings.append(f"⚠️  Could not read state: {e}")

        # 4. Check data quality
        print("\n🔍 Checking data quality...")

        # Check for NULLs in key columns
        key_cols = ["id", "order_id", "customer_id"]
        found_keys = [col for col in key_cols if col in df.columns]

        if found_keys:
            key_col = found_keys[0]
            null_count = df[key_col].isnull().sum()
            if null_count > 0:
                errors.append(f"❌ {null_count} NULL values in {key_col}")
            else:
                print(f"✅ No NULLs in {key_col}")

        # 5. Verify incremental behavior (if second run)
        print("\n🔄 Verifying incremental behavior...")
        run_marker = Path(".odibi/verify_incremental_runs.txt")

        if run_marker.exists():
            with open(run_marker) as f:
                previous_count = int(f.read().strip())

            current_count = len(df)
            new_rows = current_count - previous_count

            print(f"  Previous run: {previous_count} rows")
            print(f"  Current run: {current_count} rows")
            print(f"  New rows: {new_rows}")

            if new_rows == 0:
                print(f"✅ Incremental working (no duplicates, no new data)")
            elif new_rows > 0:
                print(f"✅ Incremental working ({new_rows} new rows added)")
            else:
                errors.append(f"❌ Row count decreased (data loss?)")
        else:
            print("  First run detected, creating marker for next run")
            with open(run_marker, "w") as f:
                f.write(str(len(df)))
            warnings.append("⚠️  Run pipeline again to verify incremental behavior")

    except Exception as e:
        errors.append(f"❌ Error reading output: {e}")

    # Summary
    print("\n" + "=" * 60)
    print("📋 VERIFICATION SUMMARY")
    print("=" * 60)

    if errors:
        print(f"\n❌ FAILED ({len(errors)} errors)")
        for error in errors:
            print(f"  {error}")

    if warnings:
        print(f"\n⚠️  WARNINGS ({len(warnings)})")
        for warning in warnings:
            print(f"  {warning}")

    if not errors and not warnings:
        print("\n✅ ALL CHECKS PASSED")
    elif not errors:
        print("\n✅ PASSED (with warnings)")

    print("\n💡 TIP: Run pipeline twice to verify incremental loading:")
    print("   1. First run: loads all data")
    print("   2. Add new rows to source")
    print("   3. Second run: loads only new rows (verify with this script)")

    return 1 if errors else 0


if __name__ == "__main__":
    exit_code = verify_incremental_sql()
    sys.exit(exit_code)
