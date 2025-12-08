# Dimension Pattern

The `DimensionPattern` builds complete dimension tables with automatic surrogate key generation and SCD (Slowly Changing Dimension) support.

## Features

- **Auto-generate integer surrogate keys** (MAX(existing) + ROW_NUMBER for new rows)
- **SCD Type 0** (static - never update existing records)
- **SCD Type 1** (overwrite - update in place, no history)
- **SCD Type 2** (history tracking - full audit trail)
- **Unknown member row** (SK=0) for orphan FK handling
- **Audit columns** (load_timestamp, source_system)

## Quick Start

```yaml
nodes:
  - name: dim_customer
    read:
      source: staging.customers
    pattern:
      type: dimension
      params:
        natural_key: customer_id
        surrogate_key: customer_sk
        scd_type: 1
        track_columns: [name, email, address, phone]
    write:
      target: gold.dim_customer
      mode: overwrite
```

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
audit:
  load_timestamp: true      # Add load_timestamp column
  source_system: "pos"      # Add source_system column with this value
```

---

## SCD Type 0 (Static)

Static dimensions never update existing records. Only new records (not matching natural key) are inserted.

**Use case:** Reference data that never changes (ISO country codes, fixed lookup values).

```yaml
pattern:
  type: dimension
  params:
    natural_key: country_code
    surrogate_key: country_sk
    scd_type: 0
    target: gold.dim_country
```

**Behavior:**
1. Load existing dimension from target
2. Find new records (natural keys not in existing)
3. Generate surrogate keys starting from MAX(existing) + 1
4. Union existing + new records

---

## SCD Type 1 (Overwrite)

Overwrite dimensions update existing records in place. No history is kept.

**Use case:** Attributes where you only care about the current value (customer email, product price).

```yaml
pattern:
  type: dimension
  params:
    natural_key: customer_id
    surrogate_key: customer_sk
    scd_type: 1
    track_columns: [name, email, address]
    target: gold.dim_customer
    audit:
      load_timestamp: true
```

**Behavior:**
1. Load existing dimension from target
2. Match source to existing on natural key
3. Update matched records with new values (preserve SK)
4. Insert new records with generated SKs
5. Preserve unchanged records

---

## SCD Type 2 (History Tracking)

History-tracking dimensions preserve full audit trail. Old records are closed, new versions are opened.

**Use case:** Slowly changing attributes where historical accuracy matters (customer address for point-in-time reporting).

```yaml
pattern:
  type: dimension
  params:
    natural_key: customer_id
    surrogate_key: customer_sk
    scd_type: 2
    track_columns: [name, email, address, city, state]
    target: gold.dim_customer
    valid_from_col: valid_from     # Optional, default: valid_from
    valid_to_col: valid_to         # Optional, default: valid_to
    is_current_col: is_current     # Optional, default: is_current
    unknown_member: true
    audit:
      load_timestamp: true
      source_system: "crm"
```

**Generated Columns:**
- `valid_from`: Timestamp when this version became active
- `valid_to`: Timestamp when this version was superseded (NULL for current)
- `is_current`: Boolean flag (true for current version)

**Behavior:**
1. Load existing dimension history from target
2. Compare source to current records (`is_current=true`)
3. For changed records: close old version, insert new version
4. For new records: insert with new surrogate key
5. Each version gets a unique surrogate key

---

## Unknown Member Handling

Enable `unknown_member: true` to automatically insert a row with SK=0. This allows fact tables to reference unknown dimensions without FK violations.

```yaml
pattern:
  type: dimension
  params:
    natural_key: product_id
    surrogate_key: product_sk
    scd_type: 1
    track_columns: [name, category]
    unknown_member: true
```

**Generated Unknown Member Row:**

| product_sk | product_id | name | category | valid_from | is_current |
|------------|------------|------|----------|------------|------------|
| 0 | -1 | Unknown | Unknown | 1900-01-01 | true |

---

## Full YAML Example

Complete star schema dimension with all features:

```yaml
project:
  name: customer_dimension
  
connections:
  warehouse:
    type: delta
    path: /mnt/warehouse

nodes:
  - name: dim_customer
    read:
      connection: staging
      format: delta
      path: customers
    pattern:
      type: dimension
      params:
        natural_key: customer_id
        surrogate_key: customer_sk
        scd_type: 2
        track_columns:
          - name
          - email
          - phone
          - address_line_1
          - address_line_2
          - city
          - state
          - postal_code
          - country
        target: warehouse.dim_customer
        valid_from_col: effective_date
        valid_to_col: expiration_date
        is_current_col: is_current
        unknown_member: true
        audit:
          load_timestamp: true
          source_system: "salesforce"
    write:
      connection: warehouse
      path: dim_customer
      mode: overwrite
```

---

## Python API

```python
from odibi.patterns.dimension import DimensionPattern
from odibi.context import EngineContext

# Create pattern instance
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
- [SCD2 Transformer](../reference/yaml_schema.md#scd2) - Low-level SCD2 transformer
