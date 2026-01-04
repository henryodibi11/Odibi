# Example 4: Fact Table (Star Schema)

Build a fact table with automatic surrogate key lookups from dimensions.

## When to Use

- Building a star schema for BI/reporting
- Need to join business keys to surrogate keys from dimensions
- Want grain validation and orphan handling

## How It Works

1. Reads staging data with business keys (`customer_id`, `product_id`)
2. Looks up surrogate keys from dimension tables (`customer_sk`, `product_sk`)
3. Handles orphan records (missing dimension entries)
4. Validates grain (no duplicate combinations)

---

## Full Config

```yaml
# odibi.yaml
project: sales_warehouse

connections:
  staging:
    type: local
    base_path: ./data/staging
  gold:
    type: local
    base_path: ./data/gold

story:
  connection: gold
  path: stories

system:
  connection: gold
  path: _system

pipelines:
  - pipeline: facts
    layer: gold
    nodes:
      - name: fact_sales
        read:
          connection: staging
          format: parquet
          path: sales_events
        
        pattern:
          type: fact
          params:
            grain:
              - order_id
              - line_item_id
            
            dimensions:
              - source_column: customer_id
                dimension_table: gold.dim_customer
                dimension_key: customer_id
                surrogate_key: customer_sk
              
              - source_column: product_id
                dimension_table: gold.dim_product
                dimension_key: product_id
                surrogate_key: product_sk
              
              - source_column: order_date
                dimension_table: gold.dim_date
                dimension_key: date_key
                surrogate_key: date_sk
            
            orphan_handling: unknown  # Map to SK=0
            # Options: 'unknown' | 'quarantine' | 'fail'
        
        write:
          connection: gold
          format: delta
          path: fact_sales
          mode: overwrite
```

---

## Sample Input (Staging)

Copy from `docs/examples/canonical/sample_data/orders.csv` to `data/staging/sales_events/`:

| order_id | line_item_id | customer_id | product_id | order_date | quantity | amount |
|----------|--------------|-------------|------------|------------|----------|--------|
| O001 | 1 | 1 | P100 | 2025-01-15 | 2 | 49.99 |
| O001 | 2 | 1 | P200 | 2025-01-15 | 1 | 29.99 |
| O005 | 1 | 999 | P100 | 2025-01-19 | 1 | 24.99 |

Note: `999` doesn't exist in `dim_customer` (orphan record).

---

## Expected Output

| order_id | line_item_id | customer_sk | product_sk | date_sk | quantity | amount |
|----------|--------------|-------------|------------|---------|----------|--------|
| O001 | 1 | 1 | 1 | 20250115 | 2 | 49.99 |
| O001 | 2 | 1 | 2 | 20250115 | 1 | 29.99 |
| O005 | 1 | **0** | 1 | 20250119 | 1 | 24.99 |

`999` → `customer_sk = 0` (unknown member row)

---

## Orphan Handling Options

| Option | Behavior | When to Use |
|--------|----------|-------------|
| `unknown` | Map to SK=0 | BI can filter unknowns, no data loss |
| `quarantine` | Route to quarantine table | Review orphans later |
| `fail` | Stop pipeline | Strict referential integrity required |

```yaml
# Quarantine option
orphan_handling: quarantine
orphan_quarantine:
  connection: gold
  path: quarantine/fact_sales_orphans
```

---

## Run

```bash
odibi run odibi.yaml
```

---

## Validate Grain

The fact pattern automatically checks for grain violations:

```
❌ Grain violation: 3 duplicate combinations found for [order_id, line_item_id]
```

This prevents the silent corruption of having duplicate fact rows.

---

## Schema Reference

| Key | Docs |
|-----|------|
| `pattern.type: fact` | [Fact Pattern](../../patterns/fact.md) |
| `params.grain` | [Fact Pattern: Grain Validation](../../patterns/fact.md#grain) |
| `params.orphan_handling` | [Fact Pattern: Orphan Handling](../../patterns/fact.md#orphan-handling) |

---

## See Also

- [Example 3: SCD2 Dimension](03_scd2_dimension.md) — Build dimensions first
- [Pattern: Aggregation](../../patterns/aggregation.md) — Pre-aggregate facts for BI
