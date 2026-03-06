"""
Example 2: Employee History with SCD Type 2

Real-world scenario: Track employee changes over time (promotions, dept changes, salary).
- Source: SQL Server table dbo.Employee
- Target: Delta Lake silver/dim_employee
- Pattern: SCD2 for full history tracking
"""

from odibi_mcp.tools.construction import apply_pattern_template
from odibi_mcp.tools.validation import validate_pipeline


def build_employee_scd2():
    """Build an employee SCD2 pipeline."""

    print("=" * 80)
    print("EXAMPLE 2: Employee History (SCD Type 2)")
    print("=" * 80)

    print("\n[SCENARIO]")
    print("HR system tracks employee data. When an employee gets promoted or")
    print("changes departments, we need to keep full history of changes.")

    # Generate pipeline
    print("\n[GENERATING PIPELINE]")
    result = apply_pattern_template(
        pattern="scd2",
        pipeline_name="track_employee_history",
        layer="silver",
        # Source
        source_connection="hr_db",
        source_table="dbo.Employee",
        source_format="sql",
        # Target
        target_connection="warehouse",
        target_path="silver/dim_employee",
        target_format="delta",
        # SCD2-specific params
        keys=["employee_id"],
        tracked_columns=[
            "first_name",
            "last_name",
            "email",
            "department",
            "job_title",
            "salary",
            "manager_id",
            "hire_date",
        ],
    )

    if not result["valid"]:
        print(f"ERROR: {result['errors']}")
        return None

    print("  ✓ YAML generated")

    # Validate
    validation = validate_pipeline(result["yaml"])
    print(f"  ✓ {validation['summary']}")

    # Save
    output_file = "examples/phase1/employee_scd2.yaml"
    with open(output_file, "w") as f:
        f.write(result["yaml"])

    print(f"\n✓ Saved to: {output_file}")

    print("\n[WHAT THIS DOES]")
    print("1. Reads current employee data from HR database")
    print("2. Compares with existing history in silver/dim_employee")
    print("3. For each changed employee:")
    print("   - Closes old record (sets end_date, is_current=false)")
    print("   - Inserts new record (sets start_date, is_current=true)")
    print("4. Keeps full history of all changes")

    print("\n[EXAMPLE HISTORY]")
    print("employee_id | name      | department | salary | start_date | end_date   | is_current")
    print("1001       | John Doe  | Sales      | 50000  | 2020-01-01 | 2021-06-30 | false")
    print("1001       | John Doe  | Sales      | 55000  | 2021-07-01 | 2022-12-31 | false")
    print("1001       | John Doe  | Marketing  | 60000  | 2023-01-01 | NULL       | true")

    return result["yaml"]


if __name__ == "__main__":
    build_employee_scd2()
