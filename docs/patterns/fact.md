# Fact Pattern

The `fact` pattern builds fact tables with automatic surrogate key lookups from dimension tables, orphan handling, grain validation, and measure calculations.

## Integration with Odibi YAML

The fact pattern looks up dimension tables **from context** - dimensions must be registered (either by running dimension nodes in the same pipeline with `depends_on`, or by reading them from storage).

```yaml
project: sales_warehouse
engine: spark

connections:
  staging:
    type: delta
    path: /mnt/staging
  warehouse:
    type: delta
    path: /mnt/warehouse

story:
  connection: warehouse
  path: stories

pipelines:
  - pipeline: build_star_schema
    nodes:
      # First, build or load dimensions
      - name: dim_customer
        read:
          connection: warehouse
          path: dim_customer
          format: delta
        # Just loading - no transform needed

      - name: dim_product
        read:
          connection: warehouse
          path: dim_product
          format: delta

      - name: dim_date
        read:
          connection: warehouse
          path: dim_date
          format: delta

      # Then build fact table with SK lookups
      - name: fact_orders
        depends_on: [dim_customer, dim_product, dim_date]
        read:
          connection: staging
          path: orders
          format: delta
        
        transformer: fact
        params:
          grain: [order_id, line_item_id]
          dimensions:
            - source_column: customer_id
              dimension_table: dim_customer  # References node name
              dimension_key: customer_id
              surrogate_key: customer_sk
              scd2: true
            - source_column: product_id
              dimension_table: dim_product
              dimension_key: product_id
              surrogate_key: product_sk
            - source_column: order_date
              dimension_table: dim_date
              dimension_key: full_date
              surrogate_key: date_sk
          orphan_handling: unknown
          measures:
            - quantity
            - unit_price
            - line_total: "quantity * unit_price"
          audit:
            load_timestamp: true
            source_system: "pos"
        
        write:
          connection: warehouse
          path: fact_orders
          format: delta
          mode: overwrite
```

---

## Features

- **Automatic SK lookups** from dimension tables
- **Orphan handling** (unknown member, reject, or quarantine)
- **Grain validation** (detect duplicates at PK level)
- **Deduplication** support
- **Measure calculations** and renaming
- **Audit columns** (load_timestamp, source_system)
- **SCD2 dimension support** (filter to is_current=true)

---

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `grain` | list | No | - | Columns defining uniqueness (validates no duplicates) |
| `dimensions` | list | No | [] | Dimension lookup configurations |
| `orphan_handling` | str | No | "unknown" | "unknown", "reject", or "quarantine" |
| `measures` | list | No | [] | Measure definitions (passthrough, rename, or calculated) |
| `deduplicate` | bool | No | false | Remove duplicates before insert |
| `keys` | list | Required if deduplicate | - | Keys for deduplication |
| `audit` | dict | No | {} | Audit column configuration |

### Dimension Lookup Config

```yaml
params:
  dimensions:
    - source_column: customer_id     # Column in source data
      dimension_table: dim_customer  # Node name in context
      dimension_key: customer_id     # Natural key column in dimension
      surrogate_key: customer_sk     # Surrogate key to retrieve
      scd2: true                     # If true, filter is_current=true
```

### Measures Config

```yaml
params:
  measures:
    - quantity                           # Passthrough
    - revenue: total_amount              # Rename
    - line_total: "quantity * unit_price" # Calculate
```

---

## Orphan Handling

Three strategies for handling source records that don't match any dimension:

### 1. Unknown (Default)
Map orphans to the unknown member (SK=0):
```yaml
orphan_handling: unknown
```

### 2. Reject
Fail the pipeline if any orphans exist:
```yaml
orphan_handling: reject
```

### 3. Quarantine
Route orphans to quarantine table:
```yaml
orphan_handling: quarantine
```

---

## Grain Validation

Define the fact table grain to detect duplicate records:

```yaml
params:
  grain: [order_id, line_item_id]
```

If duplicates exist, the pattern raises an error with details.

---

## Full Star Schema Example

Complete pipeline building dimensions AND fact tables:

```yaml
project: sales_star_schema
engine: spark

connections:
  staging:
    type: delta
    path: /mnt/staging
  warehouse:
    type: delta
    path: /mnt/warehouse

story:
  connection: warehouse
  path: stories

system:
  connection: warehouse
  path: _system_catalog

pipelines:
  # Pipeline 1: Build dimensions
  - pipeline: build_dimensions
    nodes:
      - name: dim_customer
        read:
          connection: staging
          path: customers
        transformer: dimension
        params:
          natural_key: customer_id
          surrogate_key: customer_sk
          scd_type: 2
          track_cols: [name, email, region]
          target: warehouse.dim_customer
          unknown_member: true
        write:
          connection: warehouse
          path: dim_customer
          mode: overwrite

      - name: dim_product
        read:
          connection: staging
          path: products
        transformer: dimension
        params:
          natural_key: product_id
          surrogate_key: product_sk
          scd_type: 1
          track_cols: [name, category, price]
          target: warehouse.dim_product
          unknown_member: true
        write:
          connection: warehouse
          path: dim_product
          mode: overwrite

      - name: dim_date
        transformer: date_dimension
        params:
          start_date: "2020-01-01"
          end_date: "2030-12-31"
          unknown_member: true
        write:
          connection: warehouse
          path: dim_date
          mode: overwrite

  # Pipeline 2: Build fact table (depends on dimensions existing)
  - pipeline: build_facts
    nodes:
      # Load dimensions into context
      - name: dim_customer
        read:
          connection: warehouse
          path: dim_customer

      - name: dim_product
        read:
          connection: warehouse
          path: dim_product

      - name: dim_date
        read:
          connection: warehouse
          path: dim_date

      # Build fact table
      - name: fact_orders
        depends_on: [dim_customer, dim_product, dim_date]
        read:
          connection: staging
          path: orders
        transformer: fact
        params:
          grain: [order_id, line_item_id]
          dimensions:
            - source_column: customer_id
              dimension_table: dim_customer
              dimension_key: customer_id
              surrogate_key: customer_sk
              scd2: true
            - source_column: product_id
              dimension_table: dim_product
              dimension_key: product_id
              surrogate_key: product_sk
            - source_column: order_date
              dimension_table: dim_date
              dimension_key: full_date
              surrogate_key: date_sk
          orphan_handling: unknown
          measures:
            - quantity
            - unit_price
            - discount_amount
            - line_total: "quantity * unit_price"
            - net_amount: "quantity * unit_price - discount_amount"
          audit:
            load_timestamp: true
            source_system: "pos"
        write:
          connection: warehouse
          path: fact_orders
          mode: overwrite
```

---

## See Also

- [Dimension Pattern](./dimension.md) - Build dimensions with SCD support
- [Date Dimension Pattern](./date_dimension.md) - Generate date dimensions
- [Aggregation Pattern](./aggregation.md) - Build aggregate tables
- [FK Validation](../validation/fk.md) - Additional FK validation
- [YAML Schema Reference](../reference/yaml_schema.md) - Full configuration reference
