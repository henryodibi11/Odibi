#!/usr/bin/env python3
"""
Verification script for Junior DE Capstone Project

Requirements:
- Pipeline runs without errors
- Filters bad rows (negative amounts)
- Validation gates configured
- Data Story generated
- Explanation documented

This script grades the capstone project.
Exit code 0 = pass, non-zero = fail
"""

import sys
from pathlib import Path


def verify_jr_de_capstone():
    """Verify Junior DE capstone project."""
    print("🎓 Verifying Junior DE Capstone Project")
    print("=" * 60)

    errors = []
    warnings = []
    points_earned = 0
    total_points = 100

    # 1. Output file exists (20 points)
    print("\n📁 [20 pts] Checking output file...")
    expected_outputs = [
        Path("data/silver/sales_cleaned"),
        Path("data/silver/orders_cleaned"),
        Path("data/output/sales"),
    ]

    output_found = None
    for path in expected_outputs:
        if path.exists():
            output_found = path
            break

    if output_found:
        print(f"✅ Output found: {output_found}")
        points_earned += 20
    else:
        errors.append(f"❌ No output found (checked: {[str(p) for p in expected_outputs]})")

    if output_found:
        try:
            import pandas as pd

            df = pd.read_parquet(output_found)

            # 2. Filtered negative amounts (20 points)
            print("\n🔍 [20 pts] Checking filter logic...")
            amount_cols = [col for col in df.columns if "amount" in col.lower()]

            if amount_cols:
                amount_col = amount_cols[0]
                negative_count = (df[amount_col] < 0).sum()

                if negative_count > 0:
                    errors.append(
                        f"❌ {negative_count} negative amounts found (should be filtered)"
                    )
                else:
                    print(f"✅ No negative amounts (filter working)")
                    points_earned += 20
            else:
                warnings.append("⚠️  No 'amount' column found, cannot verify filter")

            # 3. Row count correct (15 points)
            print(f"\n📊 [15 pts] Checking row count...")
            row_count = len(df)
            print(f"  Rows: {row_count}")

            if row_count >= 1:
                print(f"✅ Output has data ({row_count} rows)")
                points_earned += 15
            else:
                errors.append("❌ Empty output")

            # 4. No NULL in required columns (15 points)
            print("\n🔍 [15 pts] Checking for NULLs...")
            required_cols = ["customer_id", "order_id", "id"]
            found_required = [col for col in required_cols if col in df.columns]

            if found_required:
                key_col = found_required[0]
                null_count = df[key_col].isnull().sum()

                if null_count == 0:
                    print(f"✅ No NULLs in {key_col}")
                    points_earned += 15
                else:
                    errors.append(f"❌ {null_count} NULLs in {key_col}")
            else:
                warnings.append(f"⚠️  No key column found (checked: {required_cols})")

        except Exception as e:
            errors.append(f"❌ Error reading output: {e}")

    # 5. Data Story exists (15 points)
    print("\n📖 [15 pts] Checking Data Story...")
    story_dir = Path(".odibi/stories")

    if story_dir.exists():
        stories = list(story_dir.glob("*.html"))
        if stories:
            latest_story = max(stories, key=lambda p: p.stat().st_mtime)
            print(f"✅ Data Story found: {latest_story.name}")
            points_earned += 15
        else:
            errors.append("❌ No Data Story HTML found")
    else:
        errors.append("❌ Story directory not found")

    # 6. Validation section in YAML (15 points)
    print("\n✓ [15 pts] Checking validation configuration...")
    yaml_files = list(Path(".").glob("*.yaml")) + list(Path(".").glob("odibi.yaml"))

    validation_found = False
    for yaml_file in yaml_files:
        try:
            with open(yaml_file) as f:
                content = f.read()
                if "validation:" in content or "contracts:" in content:
                    print(f"✅ Validation found in {yaml_file.name}")
                    validation_found = True
                    points_earned += 15
                    break
        except:
            pass

    if not validation_found:
        errors.append("❌ No validation/contracts in YAML")

    # 7. Comments explaining choices (bonus, not required)
    print("\n💬 [BONUS] Checking for comments...")
    has_comments = False
    for yaml_file in yaml_files:
        try:
            with open(yaml_file) as f:
                lines = f.readlines()
                comment_count = sum(1 for line in lines if line.strip().startswith("#"))
                if comment_count >= 3:
                    print(f"✅ Found {comment_count} comments in {yaml_file.name}")
                    has_comments = True
                    break
        except:
            pass

    if not has_comments:
        warnings.append("⚠️  Consider adding comments explaining your choices")

    # Summary
    print("\n" + "=" * 60)
    print("📋 CAPSTONE GRADING")
    print("=" * 60)

    print(f"\n🎯 SCORE: {points_earned}/{total_points} points")

    if points_earned >= 90:
        grade = "A (Excellent!)"
    elif points_earned >= 80:
        grade = "B (Good work)"
    elif points_earned >= 70:
        grade = "C (Passing)"
    else:
        grade = "F (Needs improvement)"

    print(f"📊 GRADE: {grade}")

    if errors:
        print(f"\n❌ ISSUES ({len(errors)}):")
        for error in errors:
            print(f"  {error}")

    if warnings:
        print(f"\n⚠️  SUGGESTIONS ({len(warnings)}):")
        for warning in warnings:
            print(f"  {warning}")

    if points_earned >= 70:
        print("\n🎉 CONGRATULATIONS! You passed the Junior DE capstone!")
        print("   You're ready to build real pipelines.")
        print("\n📚 NEXT STEPS:")
        print("   - Try the Sr DE Journey for production patterns")
        print("   - Add incremental loading to your pipeline")
        print("   - Share your Data Story in GitHub Discussions!")
    else:
        print("\n📖 KEEP WORKING:")
        print("   - Review the errors above")
        print("   - Check the Jr DE Journey modules you missed")
        print("   - Run the verify script again after fixes")

    return 0 if points_earned >= 70 else 1


if __name__ == "__main__":
    exit_code = verify_jr_de_capstone()
    sys.exit(exit_code)
