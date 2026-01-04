# THE Reference Implementation

> **This is the canonical Odibi pipeline. Copy this. Learn from this. Start here.**

---

## The Pipeline

**[04_fact_table.yaml](runnable/04_fact_table.yaml)** is the complete reference implementation that demonstrates everything Odibi can do in a single, runnable example.

### What It Builds

A complete **star schema** with:

| Table | Pattern | Key Features |
|-------|---------|--------------|
| `dim_customer` | Dimension (SCD1) | Natural→surrogate key, unknown member |
| `dim_product` | Dimension (SCD1) | Track columns, auto SK generation |
| `dim_date` | Date Dimension | Generated 366 rows, fiscal calendar |
| `fact_sales` | Fact | FK lookups, orphan handling, grain validation |

### Run It

```bash
cd docs/examples/canonical/runnable
odibi run 04_fact_table.yaml
```

---

## Why This Example?

1. **Star Schema** — The most common real-world pattern
2. **Multiple Pipelines** — Shows dependency ordering (dimensions before facts)
3. **FK Lookups** — Automatic surrogate key resolution
4. **Orphan Handling** — `customer_id=999` maps to `customer_sk=0` (unknown member)
5. **Production Ready** — Parquet output, proper layering, auditable

---

## The Full Config

```yaml
project: sales_star_schema

connections:
  source:
    type: local
    base_path: ../sample_data
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
  # Pipeline 1: Build all dimensions
  - pipeline: build_dimensions
    layer: gold
    nodes:
      - name: dim_customer
        read:
          connection: source
          format: csv
          path: customers.csv
          options:
            header: true
        pattern:
          type: dimension
          params:
            natural_key: customer_id
            surrogate_key: customer_sk
            scd_type: 1
            track_cols: [name, email, tier, city]
            unknown_member: true
        write:
          connection: gold
          format: parquet
          path: dim_customer
          mode: overwrite

      - name: dim_product
        read:
          connection: source
          format: csv
          path: products.csv
          options:
            header: true
        pattern:
          type: dimension
          params:
            natural_key: product_id
            surrogate_key: product_sk
            scd_type: 1
            track_cols: [name, category, price]
            unknown_member: true
        write:
          connection: gold
          format: parquet
          path: dim_product
          mode: overwrite

      - name: dim_date
        pattern:
          type: date_dimension
          params:
            start_date: "2025-01-01"
            end_date: "2025-12-31"
            fiscal_year_start_month: 1
            unknown_member: true
        write:
          connection: gold
          format: parquet
          path: dim_date
          mode: overwrite

  # Pipeline 2: Build fact table
  - pipeline: build_facts
    layer: gold
    nodes:
      - name: dim_customer
        read:
          connection: gold
          format: parquet
          path: dim_customer

      - name: dim_product
        read:
          connection: gold
          format: parquet
          path: dim_product

      - name: dim_date
        read:
          connection: gold
          format: parquet
          path: dim_date

      - name: fact_sales
        depends_on: [dim_customer, dim_product, dim_date]
        read:
          connection: source
          format: csv
          path: orders.csv
          options:
            header: true
        pattern:
          type: fact
          params:
            grain: [order_id, line_item_id]
            dimensions:
              - source_column: customer_id
                dimension_table: dim_customer
                dimension_key: customer_id
                surrogate_key: customer_sk
              - source_column: product_id
                dimension_table: dim_product
                dimension_key: product_id
                surrogate_key: product_sk
              - source_column: order_date
                dimension_table: dim_date
                dimension_key: full_date
                surrogate_key: date_sk
            orphan_handling: unknown
            measures: [quantity, amount]
            audit:
              load_timestamp: true
              source_system: "orders_csv"
        write:
          connection: gold
          format: parquet
          path: fact_sales
          mode: overwrite
```

---

## Key Concepts Demonstrated

### 1. Dimension Pattern
```yaml
pattern:
  type: dimension
  params:
    natural_key: customer_id      # Your business key
    surrogate_key: customer_sk    # Auto-generated SK
    scd_type: 1                   # Overwrite (use 2 for history)
    unknown_member: true          # Creates SK=0 row for orphans
```

### 2. Fact Pattern with FK Lookups
```yaml
pattern:
  type: fact
  params:
    grain: [order_id, line_item_id]  # Uniqueness check
    dimensions:
      - source_column: customer_id   # What's in your source
        dimension_table: dim_customer
        dimension_key: customer_id   # Match on this
        surrogate_key: customer_sk   # Return this
    orphan_handling: unknown         # Orphans → SK=0
```

### 3. Pipeline Ordering
```yaml
pipelines:
  - pipeline: build_dimensions    # Runs first
  - pipeline: build_facts         # Runs second (depends on dims)
```

---

## Next Steps

| Goal | Link |
|------|------|
| Add validation | [Contracts Reference](../../reference/yaml_schema.md#contractconfig) |
| Add SCD2 history | [SCD2 Pattern](../../patterns/scd2.md) |
| Production deployment | [Decision Guide](../../guides/decision_guide.md) |
| All configuration options | [YAML Schema](../../reference/yaml_schema.md) |

---

*This is the one example to rule them all. When in doubt, copy this.*
