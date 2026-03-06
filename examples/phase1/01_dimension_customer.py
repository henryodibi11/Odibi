"""
Example 1: Customer Dimension with SCD Type 1

Real-world scenario: Building a customer dimension table from a CRM database.
- Source: SQL Server table dbo.Customer
- Target: Delta Lake gold/dim_customer
- Pattern: Dimension with surrogate key generation
- SCD Type 1: Overwrite changes (no history tracking)
"""

from odibi_mcp.tools.construction import list_patterns, apply_pattern_template
from odibi_mcp.tools.validation import validate_pipeline


def build_customer_dimension():
    """Build a customer dimension pipeline using Phase 1 tools."""

    print("=" * 80)
    print("EXAMPLE 1: Customer Dimension (SCD Type 1)")
    print("=" * 80)

    # Step 1: List available patterns
    print("\n[STEP 1] Discovering available patterns...")
    patterns = list_patterns()

    # Find dimension pattern
    dim_pattern = [p for p in patterns["patterns"] if p["name"] == "dimension"][0]
    print(f"  Pattern: {dim_pattern['name']}")
    print(f"  Required params: {dim_pattern['required_params']}")
    print(f"  Use when: {dim_pattern['use_when']}")

    # Step 2: Generate pipeline YAML
    print("\n[STEP 2] Generating pipeline YAML...")
    result = apply_pattern_template(
        pattern="dimension",
        pipeline_name="ingest_customer_dim",
        layer="gold",
        # Source: CRM database
        source_connection="crm_db",
        source_table="dbo.Customer",
        source_format="sql",
        # Target: Data warehouse
        target_connection="warehouse",
        target_path="gold/dim_customer",
        target_format="delta",
        # Dimension-specific params
        natural_key="customer_id",
        surrogate_key="customer_sk",
        tracked_columns=["customer_name", "email", "phone", "city", "state"],
    )

    if not result["valid"]:
        print(f"  ERROR: {result['errors']}")
        return None

    print("  ✓ YAML generated successfully")

    # Step 3: Validate the YAML
    print("\n[STEP 3] Validating generated YAML...")
    validation = validate_pipeline(result["yaml"])

    if not validation["valid"]:
        print("  Validation errors:")
        for error in validation["errors"]:
            print(f"    - {error['message']}")
        return None

    print(f"  ✓ {validation['summary']}")

    # Step 4: Display the YAML
    print("\n[STEP 4] Generated Pipeline YAML:")
    print("-" * 80)
    print(result["yaml"])
    print("-" * 80)

    # Step 5: Save to file
    output_file = "examples/phase1/customer_dimension.yaml"
    with open(output_file, "w") as f:
        f.write(result["yaml"])

    print(f"\n✓ Saved to: {output_file}")

    # Step 6: Show what this pipeline does
    print("\n[WHAT THIS PIPELINE DOES]")
    print("1. Reads customer data from CRM database (dbo.Customer)")
    print("2. Generates surrogate keys (customer_sk) for each customer_id")
    print("3. Tracks changes in: name, email, phone, city, state")
    print("4. Overwrites the dimension table (SCD Type 1)")
    print("5. Writes to Delta Lake: gold/dim_customer")

    print("\n[HOW TO RUN]")
    print(f"  python -m odibi run {output_file}")

    return result["yaml"]


if __name__ == "__main__":
    build_customer_dimension()
