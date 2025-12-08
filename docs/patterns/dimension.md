# Dimension Pattern

The `dimension` pattern builds complete dimension tables with automatic surrogate key generation and SCD (Slowly Changing Dimension) support.

## Integration with Odibi YAML

Patterns are used via the `transformer:` field in a node config. The pattern name goes in `transformer:` and configuration goes in `params:`.

```yaml
project: my_warehouse
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
  - pipeline: build_dimensions
    nodes:
      - name: dim_customer
        read:
          connection: staging
          path: customers
          format: delta
        
        # Use dimension pattern via transformer
        transformer: dimension
        params:
          natural_key: customer_id
          surrogate_key: customer_sk
          scd_type: 2
          track_columns: [name, email, address]
          target: warehouse.dim_customer
          unknown_member: true
          audit:
            load_timestamp: true
            source_system: "crm"
        
        write:
          connection: warehouse
          path: dim_customer
          format: delta
          mode: overwrite
```

---

## Features

- **Auto-generate integer surrogate keys** (MAX(existing) + ROW_NUMBER for new rows)
- **SCD Type 0** (static - never update existing records)
- **SCD Type 1** (overwrite - update in place, no history)
- **SCD Type 2** (history tracking - full audit trail)
- **Unknown member row** (SK=0) for orphan FK handling
- **Audit columns** (load_timestamp, source_system)

---

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `natural_key` | str | Yes | - | Natural/business key column name |
| `surrogate_key` | str | Yes | - | Surrogate key column name to generate |
| `scd_type` | int | No | 1 | 0=static, 1=overwrite, 2=history tracking |
| `track_columns` | list | For SCD1/2 | - | Columns to track for changes |
| `target` | str | For SCD2 | - | Target table path (required to read existing history) |
| `unknown_member` | bool | No | false | Insert a row with SK=0 for orphan FK handling |
| `audit` | dict | No | {} | Audit column configuration |

### Audit Config

```yaml
params:
  # ... other params ...
  audit:
    load_timestamp: true      # Add load_timestamp column
    source_system: "pos"      # Add source_system column with this value
```

---

## SCD Type 0 (Static)

Static dimensions never update existing records. Only new records (not matching natural key) are inserted.

**Use case:** Reference data that never changes (ISO country codes, fixed lookup values).

```yaml
nodes:
  - name: dim_country
    read:
      connection: staging
      path: countries
    transformer: dimension
    params:
      natural_key: country_code
      surrogate_key: country_sk
      scd_type: 0
      target: warehouse.dim_country
    write:
      connection: warehouse
      path: dim_country
      mode: overwrite
```

---

## SCD Type 1 (Overwrite)

Overwrite dimensions update existing records in place. No history is kept.

**Use case:** Attributes where you only care about the current value (customer email, product price).

```yaml
nodes:
  - name: dim_customer
    read:
      connection: staging
      path: customers
    transformer: dimension
    params:
      natural_key: customer_id
      surrogate_key: customer_sk
      scd_type: 1
      track_columns: [name, email, address]
      target: warehouse.dim_customer
      audit:
        load_timestamp: true
    write:
      connection: warehouse
      path: dim_customer
      mode: overwrite
```

---

## SCD Type 2 (History Tracking)

History-tracking dimensions preserve full audit trail. Old records are closed, new versions are opened.

**Use case:** Slowly changing attributes where historical accuracy matters (customer address for point-in-time reporting).

```yaml
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
      track_columns: [name, email, address, city, state]
      target: warehouse.dim_customer
      valid_from_col: valid_from     # Optional, default: valid_from
      valid_to_col: valid_to         # Optional, default: valid_to
      is_current_col: is_current     # Optional, default: is_current
      unknown_member: true
      audit:
        load_timestamp: true
        source_system: "crm"
    write:
      connection: warehouse
      path: dim_customer
      mode: overwrite
```

**Generated Columns:**
- `valid_from`: Timestamp when this version became active
- `valid_to`: Timestamp when this version was superseded (NULL for current)
- `is_current`: Boolean flag (true for current version)

---

## Unknown Member Handling

Enable `unknown_member: true` to automatically insert a row with SK=0. This allows fact tables to reference unknown dimensions without FK violations.

**Generated Unknown Member Row:**

| customer_sk | customer_id | name | email | valid_from | is_current |
|-------------|-------------|------|-------|------------|------------|
| 0 | -1 | Unknown | Unknown | 1900-01-01 | true |

---

## Full Star Schema Example

Complete pipeline building dimensions for a star schema:

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

system:
  connection: warehouse
  path: _system_catalog

pipelines:
  - pipeline: build_dimensions
    nodes:
      # Customer dimension with SCD2
      - name: dim_customer
        read:
          connection: staging
          path: customers
          format: delta
        transformer: dimension
        params:
          natural_key: customer_id
          surrogate_key: customer_sk
          scd_type: 2
          track_columns:
            - name
            - email
            - phone
            - address_line_1
            - city
            - state
            - postal_code
          target: warehouse.dim_customer
          unknown_member: true
          audit:
            load_timestamp: true
            source_system: "salesforce"
        write:
          connection: warehouse
          path: dim_customer
          format: delta
          mode: overwrite

      # Product dimension with SCD1 (no history)
      - name: dim_product
        read:
          connection: staging
          path: products
          format: delta
        transformer: dimension
        params:
          natural_key: product_id
          surrogate_key: product_sk
          scd_type: 1
          track_columns: [name, category, price, status]
          target: warehouse.dim_product
          unknown_member: true
        write:
          connection: warehouse
          path: dim_product
          format: delta
          mode: overwrite

      # Date dimension (generated, no source read needed)
      - name: dim_date
        transformer: date_dimension
        params:
          start_date: "2020-01-01"
          end_date: "2030-12-31"
          fiscal_year_start_month: 7
          unknown_member: true
        write:
          connection: warehouse
          path: dim_date
          format: delta
          mode: overwrite
```

---

## Python API

```python
from odibi.patterns.dimension import DimensionPattern
from odibi.context import EngineContext

# Create pattern instance
pattern = DimensionPattern(
    engine=my_engine,
    config=node_config  # NodeConfig with params
)

# Or directly with params dict
from odibi.patterns.dimension import DimensionPattern

pattern = DimensionPattern(params={
    "natural_key": "customer_id",
    "surrogate_key": "customer_sk",
    "scd_type": 2,
    "track_columns": ["name", "email", "address"],
    "target": "gold.dim_customer",
    "unknown_member": True,
    "audit": {
        "load_timestamp": True,
        "source_system": "crm"
    }
})

# Validate configuration
pattern.validate()

# Execute pattern
context = EngineContext(df=source_df, engine_type=EngineType.SPARK)
result_df = pattern.execute(context)
```

---

## See Also

- [Date Dimension Pattern](./date_dimension.md) - Generate date dimensions
- [Fact Pattern](./fact.md) - Build fact tables with SK lookups
- [Aggregation Pattern](./aggregation.md) - Build aggregate tables
- [YAML Schema Reference](../reference/yaml_schema.md) - Full configuration reference
