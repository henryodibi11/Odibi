"""
Example 5: Monthly Sales Aggregation

Real-world scenario: Pre-compute monthly sales by customer for fast reporting.
- Source: gold/fact_orders
- Target: gold/agg_monthly_sales
- Pattern: Aggregation with time grain
"""

from odibi_mcp.tools.construction import apply_pattern_template
from odibi_mcp.tools.validation import validate_pipeline


def build_monthly_sales_agg():
    """Build a monthly sales aggregation pipeline."""

    print("=" * 80)
    print("EXAMPLE 5: Monthly Sales Aggregation")
    print("=" * 80)

    print("\n[SCENARIO]")
    print("Business users need monthly sales reports by customer.")
    print("Instead of aggregating millions of orders every time,")
    print("we pre-compute monthly totals.")

    # Generate pipeline
    print("\n[GENERATING PIPELINE]")
    result = apply_pattern_template(
        pattern="aggregation",
        pipeline_name="aggregate_monthly_sales",
        layer="gold",
        # Source: fact table
        source_connection="warehouse",
        source_table="gold.fact_orders",
        source_format="sql",
        # Target: aggregation table
        target_connection="warehouse",
        target_path="gold/agg_monthly_sales",
        target_format="delta",
        # Aggregation-specific params
        grain=["customer_id", "year_month"],
        measures=[
            "COUNT(*) as order_count",
            "SUM(total_amount) as total_revenue",
            "SUM(quantity) as total_quantity",
            "AVG(total_amount) as avg_order_value",
            "MIN(order_date) as first_order_date",
            "MAX(order_date) as last_order_date",
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
    output_file = "examples/phase1/monthly_sales_agg.yaml"
    with open(output_file, "w") as f:
        f.write(result["yaml"])

    print(f"\n✓ Saved to: {output_file}")

    print("\n[WHAT THIS DOES]")
    print("1. Reads all orders from fact_orders")
    print("2. Groups by: customer_id, year_month")
    print("3. Calculates:")
    print("   - Total orders")
    print("   - Total revenue")
    print("   - Total quantity")
    print("   - Average order value")
    print("   - First/last order dates")
    print("4. Overwrites agg_monthly_sales table")

    print("\n[EXAMPLE OUTPUT]")
    print("customer_id | year_month | order_count | total_revenue | avg_order_value")
    print("1001        | 2024-01    | 15          | 12500.00      | 833.33")
    print("1001        | 2024-02    | 18          | 15000.00      | 833.33")
    print("1002        | 2024-01    | 8           | 5000.00       | 625.00")

    print("\n[BENEFIT]")
    print("Query performance:")
    print("  Before: Scan millions of order rows → 30 seconds")
    print("  After:  Scan pre-aggregated rows → 0.1 seconds")

    return result["yaml"]


if __name__ == "__main__":
    build_monthly_sales_agg()
