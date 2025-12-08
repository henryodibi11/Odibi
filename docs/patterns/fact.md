# Fact Pattern

The `FactPattern` builds fact tables with automatic surrogate key lookups from dimension tables, orphan handling, grain validation, and measure calculations.

## Features

- **Automatic SK lookups** from dimension tables
- **Orphan handling** (unknown member, reject, or quarantine)
- **Grain validation** (detect duplicates at PK level)
- **Deduplication** support
- **Measure calculations** and renaming
- **Audit columns** (load_timestamp, source_system)
- **SCD2 dimension support** (filter to is_current=true)

## Quick Start

```yaml
nodes:
  - name: fact_orders
    read:
      source: staging.orders
    pattern:
      type: fact
      params:
        grain: [order_id]
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
        orphan_handling: unknown
        audit:
          load_timestamp: true
    write:
      target: gold.fact_orders
```

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
dimensions:
  - source_column: customer_id     # Column in source data
    dimension_table: dim_customer  # Name of dimension in context
    dimension_key: customer_id     # Natural key column in dimension
    surrogate_key: customer_sk     # Surrogate key to retrieve
    scd2: true                     # If true, filter is_current=true
```

### Audit Config

```yaml
audit:
  load_timestamp: true    # Add load_timestamp column
  source_system: "pos"    # Add source_system column
```

---

## Dimension Lookups

The pattern joins source data to dimension tables and retrieves surrogate keys.

```yaml
dimensions:
  - source_column: customer_id
    dimension_table: dim_customer
    dimension_key: customer_id
    surrogate_key: customer_sk
    scd2: true
```

**How it works:**
1. Load dimension table from context
2. If `scd2: true`, filter to `is_current = true`
3. Left join source to dimension on natural key
4. Retrieve surrogate key into fact table
5. Handle orphans per `orphan_handling` setting

---

## Orphan Handling

Three strategies for handling source records that don't match any dimension:

### 1. Unknown (Default)

Map orphans to the unknown member (SK=0):

```yaml
orphan_handling: unknown
```

**Result:** Orphan records get SK=0 (requires dimension to have unknown member)

### 2. Reject

Fail the pipeline if any orphans exist:

```yaml
orphan_handling: reject
```

**Result:** Raises `ValueError` with orphan count and details

### 3. Quarantine

(Future feature) Route orphans to quarantine table:

```yaml
orphan_handling: quarantine
```

---

## Grain Validation

Define the fact table grain to detect duplicate records:

```yaml
grain: [order_id, line_item_id]
```

**Behavior:**
- Counts total rows vs distinct rows at grain level
- Raises `ValueError` if duplicates exist
- Logs duplicate count in error message

**Example error:**
```
FactPattern: Grain validation failed. Found 15 duplicate rows at grain level 
['order_id', 'line_item_id']. Total rows: 10000, Distinct rows: 9985.
```

---

## Measures

Define how to handle measure columns:

```yaml
measures:
  - quantity                           # Passthrough
  - revenue: total_amount              # Rename
  - line_total: "quantity * unit_price" # Calculate
```

### Passthrough
Keep the column as-is:
```yaml
- quantity
```

### Rename
Rename a column:
```yaml
- revenue: total_amount  # Rename total_amount to revenue
```

### Calculate
Create calculated measure (uses expression with operators +, -, *, /):
```yaml
- line_total: "quantity * unit_price"
- margin: "(revenue - cost) / revenue"
```

---

## Full Star Schema Example

Complete star schema with fact and dimension patterns:

```yaml
project:
  name: sales_star_schema

connections:
  staging:
    type: delta
    path: /mnt/staging
  warehouse:
    type: delta
    path: /mnt/warehouse

nodes:
  # Dimensions (run first)
  - name: dim_customer
    read:
      connection: staging
      path: customers
    pattern:
      type: dimension
      params:
        natural_key: customer_id
        surrogate_key: customer_sk
        scd_type: 2
        track_columns: [name, email, region]
        target: warehouse.dim_customer
        unknown_member: true
    write:
      connection: warehouse
      path: dim_customer

  - name: dim_product
    read:
      connection: staging
      path: products
    pattern:
      type: dimension
      params:
        natural_key: product_id
        surrogate_key: product_sk
        scd_type: 1
        track_columns: [name, category, price]
        target: warehouse.dim_product
        unknown_member: true
    write:
      connection: warehouse
      path: dim_product

  - name: dim_date
    pattern:
      type: date_dimension
      params:
        start_date: "2020-01-01"
        end_date: "2030-12-31"
        unknown_member: true
    write:
      connection: warehouse
      path: dim_date

  # Fact table (depends on dimensions)
  - name: fact_orders
    depends_on: [dim_customer, dim_product, dim_date]
    read:
      connection: staging
      path: orders
    pattern:
      type: fact
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
          - line_total: "quantity * unit_price"
          - discount_amount
        audit:
          load_timestamp: true
          source_system: "pos"
    write:
      connection: warehouse
      path: fact_orders
```

---

## Python API

```python
from odibi.patterns.fact import FactPattern
from odibi.context import EngineContext
from odibi.enums import EngineType

# Create pattern instance
pattern = FactPattern(params={
    "grain": ["order_id", "line_item_id"],
    "dimensions": [
        {
            "source_column": "customer_id",
            "dimension_table": "dim_customer",
            "dimension_key": "customer_id",
            "surrogate_key": "customer_sk",
            "scd2": True
        },
        {
            "source_column": "product_id",
            "dimension_table": "dim_product",
            "dimension_key": "product_id",
            "surrogate_key": "product_sk"
        }
    ],
    "orphan_handling": "unknown",
    "measures": [
        "quantity",
        {"line_total": "quantity * unit_price"}
    ],
    "audit": {
        "load_timestamp": True,
        "source_system": "pos"
    }
})

# Validate configuration
pattern.validate()

# Create context with dimension tables registered
context = EngineContext(df=source_df, engine_type=EngineType.SPARK)
context.register("dim_customer", dim_customer_df)
context.register("dim_product", dim_product_df)

# Execute pattern
result_df = pattern.execute(context)
```

---

## FK Validation Integration

For additional FK validation with detailed orphan reporting, integrate with `FKValidator`:

```python
from odibi.validation.fk import validate_fk_on_load, RelationshipConfig

relationships = [
    RelationshipConfig(
        name="orders_to_customers",
        fact="fact_orders",
        dimension="dim_customer",
        fact_key="customer_sk",
        dimension_key="customer_sk"
    )
]

# Validate after pattern execution
validated_df = validate_fk_on_load(
    fact_df=result_df,
    relationships=relationships,
    context=context,
    on_failure="error"  # or "warn" or "filter"
)
```

See [FK Validation](../validation/fk.md) for details.

---

## See Also

- [Dimension Pattern](./dimension.md) - Build dimensions with SCD support
- [Date Dimension Pattern](./date_dimension.md) - Generate date dimensions
- [Aggregation Pattern](./aggregation.md) - Build aggregate tables
- [FK Validation](../validation/fk.md) - Additional FK validation
