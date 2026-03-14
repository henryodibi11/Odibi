#!/usr/bin/env python3
"""
Verification script for Senior DE Capstone Project

Requirements:
- Bronze layer (append-only, incremental)
- Silver layer (SCD2 dimension, cleaned)
- Gold layer (fact with SK lookups, aggregation)
- Validation gates with quarantine
- Alerts configured
- Data Story with explanations

This script grades the capstone project.
Exit code 0 = pass, non-zero = fail
"""

import sys
from pathlib import Path


def verify_sr_de_capstone():
    """Verify Senior DE capstone project."""
    print("🚀 Verifying Senior DE Capstone Project")
    print("=" * 60)
    
    errors = []
    warnings = []
    points_earned = 0
    total_points = 100
    
    # 1. Bronze layer exists (15 points)
    print("\n🥉 [15 pts] Checking Bronze layer...")
    bronze_dirs = [Path("data/bronze"), Path("bronze")]
    bronze_found = None
    
    for bronze_dir in bronze_dirs:
        if bronze_dir.exists() and list(bronze_dir.iterdir()):
            bronze_found = bronze_dir
            break
    
    if bronze_found:
        bronze_tables = [d.name for d in bronze_found.iterdir() if d.is_dir()]
        print(f"✅ Bronze layer found: {len(bronze_tables)} tables")
        print(f"   Tables: {bronze_tables}")
        points_earned += 15
    else:
        errors.append("❌ No Bronze layer found")
    
    # 2. Silver layer with SCD2 (20 points)
    print("\n🥈 [20 pts] Checking Silver layer (SCD2)...")
    silver_dirs = [Path("data/silver"), Path("silver")]
    silver_found = None
    
    for silver_dir in silver_dirs:
        if silver_dir.exists():
            silver_found = silver_dir
            break
    
    scd2_verified = False
    if silver_found:
        dims = list(silver_found.glob("dim_*"))
        if dims:
            print(f"  Found {len(dims)} dimension tables")
            
            # Check first dimension for SCD2 columns
            try:
                import pandas as pd
                dim = pd.read_parquet(dims[0])
                
                scd2_cols = ['is_current', 'valid_from', 'valid_to']
                has_scd2 = all(col in dim.columns for col in scd2_cols)
                
                if has_scd2:
                    print(f"✅ SCD2 dimension verified: {dims[0].name}")
                    print(f"   Columns: is_current, valid_from, valid_to")
                    points_earned += 20
                    scd2_verified = True
                else:
                    warnings.append(f"⚠️  Dimension found but missing SCD2 columns: {dims[0].name}")
            
            except Exception as e:
                warnings.append(f"⚠️  Could not read dimension: {e}")
        else:
            warnings.append("⚠️  No dim_* tables in Silver")
    else:
        errors.append("❌ No Silver layer found")
    
    # 3. Gold layer with fact table (20 points)
    print("\n🥇 [20 pts] Checking Gold layer (Fact)...")
    gold_dirs = [Path("data/gold"), Path("gold")]
    gold_found = None
    
    for gold_dir in gold_dirs:
        if gold_dir.exists():
            gold_found = gold_dir
            break
    
    fact_verified = False
    if gold_found:
        facts = list(gold_found.glob("fact_*"))
        if facts:
            try:
                import pandas as pd
                fact = pd.read_parquet(facts[0])
                
                # Check for surrogate keys
                sk_cols = [col for col in fact.columns if col.endswith('_sk')]
                
                if len(sk_cols) >= 2:
                    print(f"✅ Fact table verified: {facts[0].name}")
                    print(f"   Surrogate keys: {sk_cols}")
                    points_earned += 20
                    fact_verified = True
                else:
                    warnings.append(f"⚠️  Fact table needs ≥2 surrogate keys, found {len(sk_cols)}")
            
            except Exception as e:
                warnings.append(f"⚠️  Could not read fact: {e}")
        else:
            errors.append("❌ No fact_* tables in Gold")
    else:
        errors.append("❌ No Gold layer found")
    
    # 4. Incremental loading (10 points)
    print("\n🔄 [10 pts] Checking incremental loading...")
    state_file = Path(".odibi/system/state.json")
    
    if state_file.exists():
        import json
        try:
            with open(state_file) as f:
                state = json.load(f)
            
            if state:
                print(f"✅ State tracking found: {len(state)} entries")
                points_earned += 10
            else:
                warnings.append("⚠️  State file empty (run pipeline twice)")
        except:
            warnings.append("⚠️  Could not read state file")
    else:
        warnings.append("⚠️  No state tracking (incremental not configured?)")
    
    # 5. Validation gates (15 points)
    print("\n✅ [15 pts] Checking validation gates...")
    
    # Check YAML for validation
    yaml_files = list(Path(".").glob("*.yaml"))
    validation_found = False
    quarantine_found = False
    
    for yaml_file in yaml_files:
        try:
            with open(yaml_file) as f:
                content = f.read()
                if 'validation:' in content:
                    validation_found = True
                if 'quarantine' in content:
                    quarantine_found = True
        except:
            pass
    
    if validation_found:
        print(f"✅ Validation configured")
        points_earned += 10
        
        if quarantine_found:
            print(f"✅ Quarantine configured (bonus!)")
            points_earned += 5
    else:
        errors.append("❌ No validation gates found in YAML")
    
    # 6. Alerts configured (10 points)
    print("\n📢 [10 pts] Checking alerts...")
    
    alerts_found = False
    for yaml_file in yaml_files:
        try:
            with open(yaml_file) as f:
                content = f.read()
                if 'alerts:' in content or 'slack' in content.lower():
                    alerts_found = True
                    break
        except:
            pass
    
    if alerts_found:
        print(f"✅ Alerts configured")
        points_earned += 10
    else:
        warnings.append("⚠️  No alerts configured (Slack/email)")
    
    # 7. Data Story with explanations (10 points)
    print("\n📖 [10 pts] Checking Data Story...")
    
    story_dir = Path(".odibi/stories")
    explanation_found = False
    
    if story_dir.exists():
        stories = list(story_dir.glob("*.html"))
        if stories:
            print(f"✅ Data Story generated")
            points_earned += 5
            
            # Check for explanations in YAML
            for yaml_file in yaml_files:
                try:
                    with open(yaml_file) as f:
                        content = f.read()
                        if 'explanation:' in content or 'explanation_file:' in content:
                            explanation_found = True
                            break
                except:
                    pass
            
            if explanation_found:
                print(f"✅ Explanation sections found")
                points_earned += 5
            else:
                warnings.append("⚠️  Add explanation: sections to document business logic")
        else:
            warnings.append("⚠️  No Story HTML found")
    else:
        warnings.append("⚠️  No Story directory")
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 CAPSTONE GRADING")
    print("=" * 60)
    
    print(f"\n🎯 SCORE: {points_earned}/{total_points} points")
    
    if points_earned >= 90:
        grade = "A (Production-ready!)"
    elif points_earned >= 80:
        grade = "B (Solid work)"
    elif points_earned >= 70:
        grade = "C (Passing)"
    else:
        grade = "F (Needs improvement)"
    
    print(f"📊 GRADE: {grade}")
    
    # Detailed feedback
    print("\n📊 COMPONENT BREAKDOWN:")
    print(f"  Bronze Layer:      {'✅' if bronze_found else '❌'}")
    print(f"  Silver SCD2:       {'✅' if scd2_verified else '❌'}")
    print(f"  Gold Fact:         {'✅' if fact_verified else '❌'}")
    print(f"  Incremental:       {'✅' if state_file.exists() else '❌'}")
    print(f"  Validation:        {'✅' if validation_found else '❌'}")
    print(f"  Alerts:            {'✅' if alerts_found else '⚠️'}")
    print(f"  Documentation:     {'✅' if explanation_found else '⚠️'}")
    
    if errors:
        print(f"\n❌ CRITICAL ISSUES ({len(errors)}):")
        for error in errors:
            print(f"  {error}")
    
    if warnings:
        print(f"\n⚠️  SUGGESTIONS ({len(warnings)}):")
        for warning in warnings:
            print(f"  {warning}")
    
    if points_earned >= 70:
        print("\n🎉 CONGRATULATIONS! You passed the Senior DE capstone!")
        print("   You're ready for production data engineering.")
        print("\n🏆 ACHIEVEMENTS UNLOCKED:")
        print("   ✓ Built full medallion architecture")
        print("   ✓ Implemented SCD2 dimensions")
        print("   ✓ Created fact tables with FK validation")
        print("   ✓ Configured quality gates")
        print("\n📚 WHAT'S NEXT:")
        print("   - Add your pipeline to GitHub")
        print("   - Write a blog post about what you learned")
        print("   - Apply for Odibi Professional Certification (coming soon)")
        print("   - Share your Data Story in Discussions!")
    else:
        print("\n📖 KEEP WORKING:")
        print("   - Review Sr DE Journey modules")
        print("   - Check the implementation checklist")
        print("   - Run verify script again after fixes")
    
    return 0 if points_earned >= 70 else 1


if __name__ == "__main__":
    exit_code = verify_sr_de_capstone()
    sys.exit(exit_code)
