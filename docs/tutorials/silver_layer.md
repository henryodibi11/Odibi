# Silver Layer Tutorial

The **Silver Layer** is where data gets cleaned, deduplicated, and conformed. This is your trusted, query-ready data.

## Layer Philosophy

> "Clean once, use everywhere."

Silver is your **single source of truth**. All downstream consumers (Gold, BI, ML) should read from Silver, not Bronze.

| Principle | Why |
|-----------|-----|
| Deduplicated | One row per key |
| Validated | Data quality enforced |
| Typed | Consistent data types |
| Conformed | Standard naming, formats |

### The One-Source Test

> "Could this node run if only ONE source system existed?"

- **YES** → Silver ✓
- **NO** → Probably Gold

!!! note "Reference Tables Are Allowed"
    Reference/lookup table joins ARE allowed in Silver. The test refers to **business source systems**, not supporting data.
    
    - ✅ `orders` JOIN `product_codes` (lookup) = Silver
    - ❌ `sap_orders` JOIN `salesforce_customers` = Gold

---

## Quick Start: Merge from Bronze

The most common Silver pattern merges Bronze data into a deduplicated table:

```yaml
# pipelines/silver/orders.yaml
pipelines:
  - pipeline: silver_orders
    layer: silver
    nodes:
      - name: orders
        read:
          connection: bronze
          table: raw_orders
        transformer: merge
        params:
          target: silver.orders
          keys: [order_id]
          strategy: upsert
        write:
          connection: silver
          table: orders
```

---

## Common Problems & Solutions

### 1. "Bronze has duplicates, how do I get one row per key?"

**Problem:** Raw data has multiple versions of the same record.

**Solution:** Use the merge transformer with deduplication.

```yaml
nodes:
  - name: orders
    read:
      connection: bronze
      table: raw_orders
    transformer: deduplicate
    params:
      keys: [order_id]
      order_by: "updated_at DESC"    # Keep most recent
    write:
      connection: silver
      table: orders
```

Or use merge for incremental upsert:

```yaml
nodes:
  - name: orders
    read:
      connection: bronze
      table: raw_orders
    transformer: merge
    params:
      target: silver.orders
      keys: [order_id]
      strategy: upsert
      audit_cols:
        created_col: _sys_created_at
        updated_col: _sys_updated_at
```

**See:** [Merge/Upsert Pattern](../patterns/merge_upsert.md)

---

### 2. "I need to track dimension history (SCD Type 2)"

**Problem:** Customer address changes—need to keep historical versions.

**Solution:** Use the dimension pattern with SCD Type 2.

```yaml
nodes:
  - name: dim_customer
    read:
      connection: bronze
      table: raw_customers
    pattern:
      type: dimension
      params:
        natural_key: customer_id
        surrogate_key: customer_sk
        scd_type: 2
        track_cols: [name, email, address, city, state]
        target: silver.dim_customer
        effective_from_col: valid_from
        effective_to_col: valid_to
        current_flag_col: is_current
    write:
      connection: silver
      table: dim_customer
```

**Result:**
```
customer_sk | customer_id | name     | city      | valid_from | valid_to   | is_current
1           | C001        | Alice    | Chicago   | 2024-01-01 | 2024-06-01 | false
2           | C001        | Alice    | Boston    | 2024-06-01 | 9999-12-31 | true
```

**See:** [SCD2 Pattern](../patterns/scd2.md)

---

### 3. "Just overwrite dimensions, I don't need history"

**Problem:** Reference data that should just reflect current state.

**Solution:** Use SCD Type 1 (no history).

```yaml
nodes:
  - name: dim_product
    read:
      connection: bronze
      table: raw_products
    pattern:
      type: dimension
      params:
        natural_key: product_id
        surrogate_key: product_sk
        scd_type: 1                  # Overwrite changes
        target: silver.dim_product
    write:
      connection: silver
      table: dim_product
```

**See:** [Dimension Pattern](../patterns/dimension.md)

---

### 4. "How do I validate data quality in Silver?"

**Problem:** Want to catch bad data before it reaches Gold.

**Solution:** Add validation tests with quarantine.

```yaml
nodes:
  - name: orders
    read:
      connection: bronze
      table: raw_orders
    validation:
      tests:
        - column: order_id
          test: not_null
        - column: order_total
          test: positive
        - column: customer_id
          test: not_null
        - column: order_date
          test: not_future
      quarantine:
        connection: silver
        table: _quarantine_orders
      gate:
        require_pass_rate: 0.95      # Allow 5% failures
    write:
      connection: silver
      table: orders
```

**Result:**
- Rows passing all tests → `silver.orders`
- Rows failing tests → `silver._quarantine_orders` for review
- Pipeline fails if pass rate < 95%

**See:** [Quality Gates](../features/quality_gates.md), [Quarantine](../features/quarantine.md)

---

### 5. "I need to apply custom SQL transformations"

**Problem:** Need to clean/transform data with custom logic.

**Solution:** Use transform steps.

```yaml
nodes:
  - name: orders
    read:
      connection: bronze
      table: raw_orders
    transform:
      steps:
        - type: sql
          query: |
            SELECT
              order_id,
              UPPER(TRIM(customer_name)) AS customer_name,
              CAST(order_date AS DATE) AS order_date,
              COALESCE(order_total, 0) AS order_total,
              CASE
                WHEN status = 'C' THEN 'Completed'
                WHEN status = 'P' THEN 'Pending'
                ELSE 'Unknown'
              END AS order_status
            FROM {input}
            WHERE order_id IS NOT NULL
    write:
      connection: silver
      table: orders
```

**See:** [Writing Transformations](../guides/writing_transformations.md)

---

### 6. "Records were deleted in source—how do I detect that?"

**Problem:** Source system hard-deletes records, need to flag them.

**Solution:** Use delete detection.

```yaml
nodes:
  - name: orders
    read:
      connection: bronze
      table: raw_orders
    transformer: merge
    params:
      target: silver.orders
      keys: [order_id]
      strategy: upsert
    delete_detection:
      mode: sql_compare              # Compare source to target
      soft_delete_col: is_deleted    # Flag instead of delete
      deleted_at_col: deleted_at     # Timestamp of detection
```

**Result:**
```
order_id | ... | is_deleted | deleted_at
1        | ... | false      | NULL
2        | ... | true       | 2025-01-15 10:30:00  ← Detected as deleted
```

**See:** [Delete Detection Config](../reference/yaml_schema.md#deletedetectionconfig)

---

### 7. "I need to check foreign key relationships"

**Problem:** Orders reference customers that don't exist.

**Solution:** Use the FK validation Python API (not YAML—this is a programmatic feature).

```python
from odibi.validation.fk import FKValidator, RelationshipRegistry, RelationshipConfig

# Define relationships
relationships = [
    RelationshipConfig(
        name="orders_to_customers",
        fact="orders",
        dimension="dim_customer",
        fact_key="customer_id",
        dimension_key="customer_id",
        on_violation="warn"  # or "error", "quarantine"
    )
]

# Validate
registry = RelationshipRegistry(relationships=relationships)
validator = FKValidator(registry)
report = validator.validate_fact(orders_df, "orders", context)

if not report.all_valid:
    print(f"Found {len(report.orphan_records)} orphan records")
```

**See:** [FK Validation](../validation/fk.md)

---

### 8. "I need to join data from multiple Bronze tables"

**Problem:** Order details and order headers in separate tables.

**Solution:** Use multi-read with SQL join.

```yaml
nodes:
  - name: orders_enriched
    read:
      - alias: headers
        connection: bronze
        table: raw_order_headers
      - alias: details
        connection: bronze
        table: raw_order_details
    transform:
      steps:
        - type: sql
          query: |
            SELECT
              h.order_id,
              h.order_date,
              h.customer_id,
              d.product_id,
              d.quantity,
              d.unit_price
            FROM headers h
            JOIN details d ON h.order_id = d.order_id
    write:
      connection: silver
      table: orders_enriched
```

---

## Silver Layer Checklist

Before moving to Gold, verify:

- [ ] **Deduplicated?** One row per natural key
- [ ] **Validated?** Quality tests passing
- [ ] **Typed?** Consistent data types (dates, numbers, etc.)
- [ ] **Complete?** FK relationships valid (or orphans quarantined)
- [ ] **Conformed?** Naming conventions followed

---

## Next Steps

- [Gold Layer Tutorial](gold_layer.md) — Build facts and aggregations
- [Dimension Pattern](../patterns/dimension.md) — SCD1/SCD2 details
- [Merge/Upsert Pattern](../patterns/merge_upsert.md) — Deduplication and upsert
