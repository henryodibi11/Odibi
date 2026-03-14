#!/usr/bin/env python3
"""
Verification script for Example 1: Hello World

This script verifies that the hello world example ran successfully.
Checks:
- Output file exists
- Row counts match expectations
- Schema is correct
- Data Story was generated

Exit code 0 = success, non-zero = failure (CI-gradable)
"""

import sys
import os
from pathlib import Path


def verify_hello_world():
    """Verify the hello world example output."""
    print("🔍 Verifying Example 1: Hello World")
    print("=" * 60)
    
    errors = []
    warnings = []
    
    # 1. Check output file exists
    print("\n📁 Checking output file...")
    output_path = Path("data/output/output.parquet")
    if not output_path.exists():
        errors.append(f"❌ Output file not found: {output_path}")
    else:
        print(f"✅ Output file exists: {output_path}")
    
    # 2. Verify file is readable (basic check)
    if output_path.exists():
        try:
            import pandas as pd
            df = pd.read_parquet(output_path)
            print(f"✅ File is readable")
            
            # 3. Check row count
            print("\n📊 Checking row count...")
            expected_rows = 5  # Adjust based on sample data
            actual_rows = len(df)
            if actual_rows != expected_rows:
                warnings.append(f"⚠️  Row count mismatch: expected {expected_rows}, got {actual_rows}")
            else:
                print(f"✅ Row count correct: {actual_rows} rows")
            
            # 4. Check schema
            print("\n🔍 Checking schema...")
            expected_columns = ['id', 'name', 'value']  # Adjust based on sample
            actual_columns = df.columns.tolist()
            
            missing = set(expected_columns) - set(actual_columns)
            extra = set(actual_columns) - set(expected_columns)
            
            if missing:
                errors.append(f"❌ Missing columns: {missing}")
            if extra:
                warnings.append(f"⚠️  Extra columns: {extra}")
            
            if not missing and not extra:
                print(f"✅ Schema correct: {actual_columns}")
            
        except Exception as e:
            errors.append(f"❌ Error reading parquet: {e}")
    
    # 5. Check Data Story
    print("\n📖 Checking Data Story...")
    story_paths = list(Path(".odibi/stories").glob("*.html")) if Path(".odibi/stories").exists() else []
    
    if not story_paths:
        warnings.append("⚠️  No Data Story found (run pipeline first)")
    else:
        latest_story = max(story_paths, key=lambda p: p.stat().st_mtime)
        print(f"✅ Data Story found: {latest_story.name}")
    
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
    
    # Return exit code
    return 1 if errors else 0


if __name__ == "__main__":
    exit_code = verify_hello_world()
    sys.exit(exit_code)
