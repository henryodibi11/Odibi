"""
Example 3: Orders Fact Table

Real-world scenario: Daily sales transaction loading.
- Source: SQL Server table dbo.Orders
- Target: Delta Lake gold/fact_orders
- Pattern: Fact table with append mode
- Incremental: Only load today's orders
"""

from odibi_mcp.tools.construction import apply_pattern_template
from odibi_mcp.tools.validation import validate_pipeline


def build_orders_fact():
    """Build an orders fact table pipeline."""

    print("=" * 80)
    print("EXAMPLE 3: Orders Fact Table (Daily Load)")
    print("=" * 80)

    print("\n[SCENARIO]")
    print("Sales system generates thousands of orders daily.")
    print("We need to load them incrementally into our data warehouse.")

    # Generate pipeline
    print("\n[GENERATING PIPELINE]")
    result = apply_pattern_template(
        pattern="fact",
        pipeline_name="load_daily_orders",
        layer="gold",
        # Source with incremental query
        source_connection="sales_db",
        source_query="""
            SELECT
                order_id,
                customer_id,
                product_id,
                order_date,
                quantity,
                unit_price,
                total_amount,
                status
            FROM dbo.Orders
            WHERE order_date >= CAST(GETDATE() AS DATE)
        """,
        source_format="sql",
        # Target
        target_connection="warehouse",
        target_path="gold/fact_orders",
        target_format="delta",
        # Fact-specific params
        keys=["order_id"],  # Use upsert mode to handle late updates
    )

    if not result["valid"]:
        print(f"ERROR: {result['errors']}")
        return None

    print("  ✓ YAML generated")

    # Validate
    validation = validate_pipeline(result["yaml"])
    print(f"  ✓ {validation['summary']}")

    # Save
    output_file = "examples/phase1/fact_orders.yaml"
    with open(output_file, "w") as f:
        f.write(result["yaml"])

    print(f"\n✓ Saved to: {output_file}")

    print("\n[WHAT THIS DOES]")
    print("1. Queries today's orders from sales database")
    print("2. Uses UPSERT mode (updates existing, inserts new)")
    print("3. Handles late-arriving updates (e.g., status changes)")
    print("4. Appends to fact_orders table")

    print("\n[TYPICAL USAGE]")
    print("Run this pipeline daily via scheduler:")
    print("  cron: 0 1 * * * python -m odibi run fact_orders.yaml")
    print("\nProcesses:")
    print("  - New orders → INSERT")
    print("  - Updated orders (status change) → UPDATE")
    print("  - Already processed orders → SKIP")

    return result["yaml"]


if __name__ == "__main__":
    build_orders_fact()
