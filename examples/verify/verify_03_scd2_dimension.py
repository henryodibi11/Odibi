#!/usr/bin/env python3
"""
Verification script for Example 3: SCD2 Dimension

Checks:
- Dimension table exists
- SCD2 columns present (is_current, valid_from, valid_to)
- Surrogate keys generated
- Unknown member exists (SK=0)
- Version history working

Exit code 0 = success, non-zero = failure
"""

import sys
from pathlib import Path


def verify_scd2():
    """Verify SCD2 dimension output."""
    print("🔍 Verifying Example 3: SCD2 Dimension")
    print("=" * 60)
    
    errors = []
    warnings = []
    
    # 1. Check output exists
    print("\n📁 Checking dim_customer...")
    dim_path = Path("data/gold/dim_customer")
    
    if not dim_path.exists():
        errors.append(f"❌ Dimension table not found: {dim_path}")
        print("\n❌ Cannot proceed without output file")
        return 1
    else:
        print(f"✅ Dimension table exists")
    
    try:
        import pandas as pd
        
        # Read parquet
        df = pd.read_parquet(dim_path)
        print(f"✅ Loaded {len(df)} rows")
        
        # 2. Check SCD2 columns
        print("\n🔍 Checking SCD2 columns...")
        required_scd2_cols = [
            'customer_sk',      # Surrogate key
            'customer_id',      # Natural key
            'is_current',       # Current flag
            'valid_from',       # Effective from
            'valid_to',         # Effective to
        ]
        
        missing_cols = [col for col in required_scd2_cols if col not in df.columns]
        if missing_cols:
            errors.append(f"❌ Missing SCD2 columns: {missing_cols}")
        else:
            print(f"✅ All SCD2 columns present")
        
        # 3. Check surrogate keys
        print("\n🔑 Checking surrogate keys...")
        if 'customer_sk' in df.columns:
            if df['customer_sk'].isnull().any():
                errors.append("❌ NULL surrogate keys found")
            else:
                print(f"✅ No NULL surrogate keys")
            
            if not df['customer_sk'].is_unique:
                errors.append("❌ Surrogate keys are not unique")
            else:
                print(f"✅ Surrogate keys are unique")
        
        # 4. Check unknown member (SK=0)
        print("\n👻 Checking unknown member...")
        if 'customer_sk' in df.columns:
            unknown = df[df['customer_sk'] == 0]
            if len(unknown) == 0:
                warnings.append("⚠️  No unknown member (SK=0) found")
            else:
                print(f"✅ Unknown member exists (SK=0)")
        
        # 5. Check is_current flag
        print("\n🚩 Checking is_current flag...")
        if 'is_current' in df.columns:
            current_rows = df[df['is_current'] == True]
            print(f"  Current rows: {len(current_rows)}")
            print(f"  Historical rows: {len(df) - len(current_rows)}")
            
            if len(current_rows) == 0:
                errors.append("❌ No current rows found")
        
        # 6. Check for version history
        print("\n📜 Checking version history...")
        if 'customer_id' in df.columns:
            version_counts = df.groupby('customer_id').size()
            versioned = version_counts[version_counts > 1]
            
            if len(versioned) > 0:
                print(f"✅ Version history found for {len(versioned)} customers")
                print(f"  Example: customer_id {versioned.index[0]} has {versioned.iloc[0]} versions")
            else:
                warnings.append("⚠️  No version history found (run pipeline twice with changed data)")
        
        # 7. Check date ranges
        print("\n📅 Checking date ranges...")
        if 'valid_from' in df.columns and 'valid_to' in df.columns:
            invalid_ranges = df[df['valid_from'] > df['valid_to']]
            if len(invalid_ranges) > 0:
                errors.append(f"❌ {len(invalid_ranges)} rows have invalid date ranges (valid_from > valid_to)")
            else:
                print(f"✅ All date ranges are valid")
        
    except Exception as e:
        errors.append(f"❌ Error reading dimension: {e}")
    
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
    
    return 1 if errors else 0


if __name__ == "__main__":
    exit_code = verify_scd2()
    sys.exit(exit_code)
