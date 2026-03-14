#!/usr/bin/env python3
"""
Verification script for Example 4: Fact Table

Checks:
- Fact table exists
- Surrogate key lookups worked
- Grain is correct (no duplicates)
- Measures are present
- FK relationships valid
- Orphan handling working

Exit code 0 = success, non-zero = failure
"""

import sys
from pathlib import Path


def verify_fact_table():
    """Verify fact table output."""
    print("🔍 Verifying Example 4: Fact Table")
    print("=" * 60)
    
    errors = []
    warnings = []
    
    # 1. Check fact table exists
    print("\n📁 Checking fact_sales...")
    fact_path = Path("data/gold/fact_sales")
    
    if not fact_path.exists():
        errors.append(f"❌ Fact table not found: {fact_path}")
        print(f"❌ Cannot proceed")
        return 1
    else:
        print(f"✅ Fact table exists")
    
    try:
        import pandas as pd
        
        # Read fact table
        fact = pd.read_parquet(fact_path)
        print(f"✅ Loaded {len(fact)} rows")
        
        # 2. Check surrogate keys
        print("\n🔑 Checking surrogate keys...")
        sk_columns = [col for col in fact.columns if col.endswith('_sk')]
        
        if not sk_columns:
            errors.append("❌ No surrogate key columns found (expected *_sk)")
        else:
            print(f"✅ Found {len(sk_columns)} surrogate keys: {sk_columns}")
            
            for sk in sk_columns:
                null_count = fact[sk].isnull().sum()
                if null_count > 0:
                    warnings.append(f"⚠️  {null_count} NULLs in {sk} (orphans?)")
                else:
                    print(f"  ✓ {sk}: no NULLs")
        
        # 3. Check grain (no duplicates on grain columns)
        print("\n📊 Checking grain...")
        grain_candidates = [
            ['order_id', 'line_item_id'],
            ['order_id', 'product_id'],
            ['transaction_id'],
            ['sale_id'],
        ]
        
        grain_found = False
        for grain in grain_candidates:
            if all(col in fact.columns for col in grain):
                print(f"  Testing grain: {grain}")
                dupes = fact.duplicated(subset=grain, keep=False)
                dupe_count = dupes.sum()
                
                if dupe_count > 0:
                    errors.append(f"❌ {dupe_count} duplicate rows on grain {grain}")
                else:
                    print(f"✅ Grain correct: {grain} (no duplicates)")
                    grain_found = True
                break
        
        if not grain_found:
            warnings.append(f"⚠️  Could not verify grain (checked: {grain_candidates})")
        
        # 4. Check measures
        print("\n💰 Checking measures...")
        measure_candidates = ['amount', 'quantity', 'revenue', 'sales', 'total', 'value']
        measures = [col for col in fact.columns if any(m in col.lower() for m in measure_candidates)]
        
        if not measures:
            warnings.append("⚠️  No measure columns found")
        else:
            print(f"✅ Found {len(measures)} measures: {measures}")
            
            for measure in measures:
                if pd.api.types.is_numeric_dtype(fact[measure]):
                    print(f"  ✓ {measure}: numeric type")
                else:
                    warnings.append(f"⚠️  {measure} is not numeric")
        
        # 5. Check for unknown members (SK=0)
        print("\n👻 Checking orphan handling...")
        sk_with_zeros = {sk: (fact[sk] == 0).sum() for sk in sk_columns}
        orphans_found = {sk: count for sk, count in sk_with_zeros.items() if count > 0}
        
        if orphans_found:
            print(f"✅ Orphan handling working:")
            for sk, count in orphans_found.items():
                print(f"  {count} rows with {sk} = 0 (unknown member)")
        else:
            print(f"  No orphans (SK=0) found")
        
        # 6. Check FK relationships (if dimensions exist)
        print("\n🔗 Checking FK relationships...")
        gold_dir = Path("data/gold")
        
        if gold_dir.exists():
            dims = list(gold_dir.glob("dim_*"))
            print(f"  Found {len(dims)} dimension tables")
            
            for dim_path in dims:
                dim_name = dim_path.name
                sk_name = dim_name.replace('dim_', '') + '_sk'
                
                if sk_name in fact.columns:
                    try:
                        dim = pd.read_parquet(dim_path)
                        
                        # Check if all fact SKs exist in dimension
                        fact_sks = set(fact[sk_name].dropna().unique())
                        dim_sks = set(dim[sk_name].unique())
                        
                        orphans = fact_sks - dim_sks
                        orphans_filtered = orphans - {0}  # Exclude unknown member
                        
                        if orphans_filtered:
                            errors.append(f"❌ {len(orphans_filtered)} invalid {sk_name} values (not in {dim_name})")
                        else:
                            print(f"  ✓ {sk_name}: all FKs valid")
                    
                    except Exception as e:
                        warnings.append(f"⚠️  Could not validate {dim_name}: {e}")
        
        # 7. Check degenerate dimensions
        print("\n📝 Checking degenerate dimensions...")
        degenerate_candidates = [col for col in fact.columns 
                                if any(x in col.lower() for x in ['number', 'id', 'code'])
                                and not col.endswith('_sk')]
        
        if degenerate_candidates:
            print(f"✅ Found degenerate dimensions: {degenerate_candidates}")
        else:
            print(f"  No degenerate dimensions (optional)")
        
    except Exception as e:
        errors.append(f"❌ Error reading fact table: {e}")
    
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
    
    print("\n💡 FACT TABLE CHECKLIST:")
    print("  ✓ Surrogate keys (_sk) for all dimensions")
    print("  ✓ Grain enforced (no duplicates)")
    print("  ✓ Measures are numeric")
    print("  ✓ Orphans handled (SK=0 for missing dimension members)")
    print("  ✓ FK relationships valid")
    
    return 1 if errors else 0


if __name__ == "__main__":
    exit_code = verify_fact_table()
    sys.exit(exit_code)
