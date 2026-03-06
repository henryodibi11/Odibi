"""
Example 4: Date Dimension Table

Real-world scenario: Generate a complete date dimension for reporting.
- No source (pattern generates dates)
- Target: Delta Lake gold/dim_date
- Pattern: Date dimension with fiscal year
"""

from odibi_mcp.tools.construction import apply_pattern_template
from odibi_mcp.tools.validation import validate_pipeline


def build_date_dimension():
    """Build a date dimension table."""

    print("=" * 80)
    print("EXAMPLE 4: Date Dimension Table")
    print("=" * 80)

    print("\n[SCENARIO]")
    print("Every data warehouse needs a date dimension for time-based analysis.")
    print("This generates all dates from 2020-2030 with calendar attributes.")

    # Generate pipeline
    print("\n[GENERATING PIPELINE]")
    result = apply_pattern_template(
        pattern="date_dimension",
        pipeline_name="generate_date_dim",
        layer="gold",
        # Date range
        start_date="2020-01-01",
        end_date="2030-12-31",
        # Target (source not needed - pattern generates dates)
        source_connection="local",  # Required but not used
        target_connection="warehouse",
        target_path="gold/dim_date",
        target_format="delta",
    )

    if not result["valid"]:
        print(f"ERROR: {result['errors']}")
        return None

    print("  ✓ YAML generated")

    # Validate
    validation = validate_pipeline(result["yaml"])
    print(f"  ✓ {validation['summary']}")

    # Save
    output_file = "examples/phase1/date_dimension.yaml"
    with open(output_file, "w") as f:
        f.write(result["yaml"])

    print(f"\n✓ Saved to: {output_file}")

    print("\n[WHAT THIS GENERATES]")
    print("Every date from 2020-01-01 to 2030-12-31 with:")
    print("  - date_sk (surrogate key)")
    print("  - full_date")
    print("  - day_of_week, day_of_month, day_of_year")
    print("  - week_of_year, month, month_name")
    print("  - quarter, quarter_name, year")
    print("  - fiscal_year, fiscal_quarter")
    print("  - is_weekend, is_month_start, is_month_end")

    print("\n[EXAMPLE ROWS]")
    print("date_sk  | full_date  | day_of_week | month | quarter | fiscal_year | is_weekend")
    print("20200101 | 2020-01-01 | Wednesday   | 1     | Q1      | 2020        | false")
    print("20200104 | 2020-01-04 | Saturday    | 1     | Q1      | 2020        | true")
    print("20201231 | 2020-12-31 | Thursday    | 12    | Q4      | 2020        | false")

    print("\n[USE CASE]")
    print("Join fact tables to date dimension for time-based reporting:")
    print("  SELECT d.month_name, SUM(f.amount)")
    print("  FROM fact_orders f")
    print("  JOIN dim_date d ON f.order_date = d.full_date")
    print("  GROUP BY d.month_name")

    return result["yaml"]


if __name__ == "__main__":
    build_date_dimension()
