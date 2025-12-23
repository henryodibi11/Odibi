# Transformers

Declarative data transformations with SQL-first semantics, dual-engine support (Spark/Pandas), and extensible custom transforms.

## Overview

Odibi's transformer system provides:
- **SQL-First Design**: All core operations leverage SQL for optimal engine performance
- **Dual-Engine Support**: Seamless execution on Spark or Pandas/DuckDB
- **Built-in Library**: 30+ production-ready transformers
- **Extensibility**: Register custom transforms with the `@transform` decorator
- **Chained Operations**: Compose multiple transforms in `transform.steps`

## Configuration

### Basic Transformer Usage

```yaml
nodes:
  - name: clean_orders
    source: raw_orders
    transformer: "filter_rows"
    params:
      condition: "status = 'active'"
```

### Transformer Config Options

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `transformer` | string | Yes | Transformer name (e.g., `filter_rows`, `scd2`) |
| `params` | object | Yes | Transformer-specific parameters |

## Transform Steps

Chain multiple transformations in sequence using `transform.steps`:

```yaml
nodes:
  - name: process_customers
    source: raw_customers
    transform:
      steps:
        - transformer: "clean_text"
          params:
            columns: ["email", "name"]
            trim: true
            case: "lower"

        - transformer: "filter_rows"
          params:
            condition: "email IS NOT NULL"

        - transformer: "derive_columns"
          params:
            derivations:
              full_name: "concat(first_name, ' ', last_name)"

        - transformer: "deduplicate"
          params:
            keys: ["customer_id"]
            order_by: "updated_at DESC"
```

## Built-in Transformers

### SQL Core Transformers

Basic SQL operations that work across all engines.

#### filter_rows

Filter rows using SQL WHERE conditions.

```yaml
transformer: "filter_rows"
params:
  condition: "age > 18 AND status = 'active'"
```

#### derive_columns

Add new columns using SQL expressions.

```yaml
transformer: "derive_columns"
params:
  derivations:
    total_price: "quantity * unit_price"
    full_name: "concat(first_name, ' ', last_name)"
```

#### cast_columns

Cast columns to different types.

```yaml
transformer: "cast_columns"
params:
  casts:
    age: "int"
    salary: "double"
    created_at: "timestamp"
```

#### clean_text

Apply text cleaning operations (trim, case conversion).

```yaml
transformer: "clean_text"
params:
  columns: ["email", "username"]
  trim: true
  case: "lower"  # Options: lower, upper, preserve
```

#### extract_date_parts

Extract year, month, day, hour from timestamps.

```yaml
transformer: "extract_date_parts"
params:
  source_col: "created_at"
  prefix: "created"
  parts: ["year", "month", "day"]
```

#### normalize_schema

Rename, drop, and reorder columns.

```yaml
transformer: "normalize_schema"
params:
  rename:
    old_col: "new_col"
  drop: ["unused_col"]
  select_order: ["id", "new_col", "created_at"]
```

#### sort

Sort data by columns.

```yaml
transformer: "sort"
params:
  by: ["created_at", "id"]
  ascending: false
```

#### limit / sample

Limit or randomly sample rows.

```yaml
# Limit
transformer: "limit"
params:
  n: 100
  offset: 0

# Sample
transformer: "sample"
params:
  fraction: 0.1
  seed: 42
```

#### distinct

Remove duplicate rows.

```yaml
transformer: "distinct"
params:
  columns: ["category", "status"]  # Optional: subset of columns
```

#### fill_nulls

Replace null values with defaults.

```yaml
transformer: "fill_nulls"
params:
  values:
    count: 0
    description: "N/A"
```

#### split_part

Extract parts of strings by delimiter.

```yaml
transformer: "split_part"
params:
  col: "email"
  delimiter: "@"
  index: 2  # Extracts domain
```

#### date_add / date_trunc / date_diff

Date arithmetic operations.

```yaml
# Add interval
transformer: "date_add"
params:
  col: "created_at"
  value: 7
  unit: "day"

# Truncate to precision
transformer: "date_trunc"
params:
  col: "created_at"
  unit: "month"

# Calculate difference
transformer: "date_diff"
params:
  start_col: "created_at"
  end_col: "updated_at"
  unit: "day"
```

#### case_when

Conditional logic.

```yaml
transformer: "case_when"
params:
  output_col: "age_group"
  default: "'Adult'"
  cases:
    - condition: "age < 18"
      value: "'Minor'"
    - condition: "age > 65"
      value: "'Senior'"
```

#### convert_timezone

Convert timestamps between timezones.

```yaml
transformer: "convert_timezone"
params:
  col: "utc_time"
  source_tz: "UTC"
  target_tz: "America/New_York"
```

#### concat_columns

Concatenate multiple columns.

```yaml
transformer: "concat_columns"
params:
  columns: ["first_name", "last_name"]
  separator: " "
  output_col: "full_name"
```

### Relational Transformers

Operations involving multiple datasets.

#### join

Join with another dataset.

```yaml
transformer: "join"
params:
  right_dataset: "customers"  # Must be in depends_on
  on: ["customer_id"]
  how: "left"  # inner, left, right, full, cross
  prefix: "cust"  # Prefix for right columns (avoid collisions)
```

#### union

Union multiple datasets.

```yaml
transformer: "union"
params:
  datasets: ["sales_2023", "sales_2024"]
  by_name: true  # Match columns by name
```

#### pivot

Pivot rows into columns.

```yaml
transformer: "pivot"
params:
  group_by: ["product_id", "region"]
  pivot_col: "month"
  agg_col: "sales"
  agg_func: "sum"
  values: ["Jan", "Feb", "Mar"]  # Optional: explicit pivot values
```

#### unpivot

Unpivot (melt) columns into rows.

```yaml
transformer: "unpivot"
params:
  id_cols: ["product_id"]
  value_vars: ["jan_sales", "feb_sales", "mar_sales"]
  var_name: "month"
  value_name: "sales"
```

#### aggregate

Group and aggregate data.

```yaml
transformer: "aggregate"
params:
  group_by: ["department", "region"]
  aggregations:
    salary: "sum"
    employee_id: "count"
    age: "avg"
```

### Advanced Transformers

Complex data processing operations.

#### deduplicate

Remove duplicates using window functions.

```yaml
transformer: "deduplicate"
params:
  keys: ["customer_id"]
  order_by: "updated_at DESC"  # Keep most recent
```

#### explode_list_column

Flatten array/list columns into rows.

```yaml
transformer: "explode_list_column"
params:
  column: "items"
  outer: true  # Keep rows with empty lists
```

#### dict_based_mapping

Map values using a dictionary.

```yaml
transformer: "dict_based_mapping"
params:
  column: "status_code"
  mapping:
    "1": "Active"
    "0": "Inactive"
  default: "Unknown"
  output_column: "status_desc"
```

#### regex_replace

Replace patterns using regex.

```yaml
transformer: "regex_replace"
params:
  column: "phone"
  pattern: "[^0-9]"
  replacement: ""
```

#### unpack_struct

Flatten struct/dict columns.

```yaml
transformer: "unpack_struct"
params:
  column: "user_info"
```

#### hash_columns

Hash columns for PII anonymization.

```yaml
transformer: "hash_columns"
params:
  columns: ["email", "ssn"]
  algorithm: "sha256"  # or "md5"
```

#### generate_surrogate_key

Create deterministic surrogate keys.

```yaml
transformer: "generate_surrogate_key"
params:
  columns: ["region", "product_id"]
  separator: "-"
  output_col: "unique_id"
```

#### parse_json

Parse JSON strings into structured data.

```yaml
transformer: "parse_json"
params:
  column: "raw_json"
  json_schema: "id INT, name STRING"
  output_col: "parsed_struct"
```

#### validate_and_flag

Flag rows that fail validation rules.

```yaml
transformer: "validate_and_flag"
params:
  flag_col: "data_issues"
  rules:
    age_check: "age >= 0"
    email_format: "email LIKE '%@%'"
```

#### window_calculation

Apply window functions.

```yaml
transformer: "window_calculation"
params:
  target_col: "cumulative_sales"
  function: "sum(sales)"
  partition_by: ["region"]
  order_by: "date ASC"
```

#### normalize_json

Flatten nested JSON/struct into columns.

```yaml
transformer: "normalize_json"
params:
  column: "json_data"
  sep: "_"
```

#### sessionize

Assign session IDs based on inactivity threshold.

```yaml
transformer: "sessionize"
params:
  timestamp_col: "event_time"
  user_col: "user_id"
  threshold_seconds: 1800  # 30 minutes
  session_col: "session_id"
```

### SCD (Slowly Changing Dimensions)

Track historical changes with SCD Type 2.

```yaml
transformer: "scd2"
params:
  target: "gold/customers"        # Path to existing history
  keys: ["customer_id"]           # Entity keys
  track_cols: ["address", "tier"] # Columns to monitor for changes
  effective_time_col: "txn_date"  # When change occurred
  end_time_col: "valid_to"        # End timestamp column
  current_flag_col: "is_current"  # Current record flag
```

**How SCD2 Works:**
1. **Match**: Finds existing records using `keys`
2. **Compare**: Checks `track_cols` to detect changes
3. **Close**: Updates old record's `end_time_col` if changed
4. **Insert**: Adds new record with open-ended validity

### Merge Transformer

Upsert, append, or delete records in target tables.

```yaml
# Upsert (Update + Insert)
transformer: "merge"
params:
  target: "silver.customers"
  keys: ["customer_id"]
  strategy: "upsert"
  audit_cols:
    created_col: "dw_created_at"
    updated_col: "dw_updated_at"
```

**Merge Strategies:**

| Strategy | Description |
|----------|-------------|
| `upsert` | Update existing, insert new (default) |
| `append_only` | Only insert new keys, ignore duplicates |
| `delete_match` | Delete records matching source keys |

**Advanced Merge Options:**

```yaml
transformer: "merge"
params:
  target: "silver.customers"
  keys: ["id"]
  strategy: "upsert"
  update_condition: "source.updated_at > target.updated_at"
  insert_condition: "source.is_deleted = false"
  delete_condition: "source.is_deleted = true"
  optimize_write: true
  zorder_by: ["customer_id"]
  cluster_by: ["region"]
```

**Connection-Based Path (ADLS):**

Use `connection` + `path` instead of `target` to leverage connection-based path resolution:

```yaml
transform:
  steps:
    - function: merge
      params:
        connection: adls_prod           # Connection name
        path: OEE/silver/customers      # Relative path
        register_table: silver.customers  # Register in metastore
        keys: ["customer_id"]
        strategy: "upsert"
        audit_cols:
          created_col: "_created_at"
          updated_col: "_updated_at"
```

### Validation Transformers

Cross-dataset validation checks.

```yaml
transformer: "cross_check"
params:
  type: "row_count_diff"  # or "schema_match"
  inputs: ["node_a", "node_b"]
  threshold: 0.05  # Allow 5% difference
```

### Delete Detection

Detect deleted records for CDC-like behavior.

```yaml
transformer: "detect_deletes"
params:
  mode: "snapshot_diff"  # Compare Delta versions
  keys: ["customer_id"]
  soft_delete_col: "is_deleted"  # Add flag column
  max_delete_percent: 10.0  # Safety threshold
  on_threshold_breach: "error"  # error, warn, skip
```

**Delete Detection Modes:**

| Mode | Description |
|------|-------------|
| `none` | Disabled |
| `snapshot_diff` | Compare current vs previous Delta version |
| `sql_compare` | Compare against live source via JDBC |

## Creating Custom Transformers

Use the `@transform` decorator with `FunctionRegistry` to create custom transformers.

### Basic Custom Transformer

```python
from pydantic import BaseModel, Field
from odibi.context import EngineContext
from odibi.registry import transform


class MyTransformParams(BaseModel):
    """Parameters for my custom transform."""
    column: str = Field(..., description="Column to process")
    multiplier: float = Field(default=1.0, description="Multiplier value")


@transform("my_custom_transform", param_model=MyTransformParams)
def my_custom_transform(context: EngineContext, **params) -> EngineContext:
    """My custom transformation."""
    config = MyTransformParams(**params)

    # Use SQL for cross-engine compatibility
    sql_query = f"""
        SELECT *, {config.column} * {config.multiplier} AS {config.column}_scaled
        FROM df
    """
    return context.sql(sql_query)
```

### Using Custom Transformers in YAML

```yaml
nodes:
  - name: process_data
    source: raw_data
    transformer: "my_custom_transform"
    params:
      column: "price"
      multiplier: 1.1
```

### Engine-Specific Logic

```python
from odibi.enums import EngineType

@transform("dual_engine_transform", param_model=MyParams)
def dual_engine_transform(context: EngineContext, **params) -> EngineContext:
    config = MyParams(**params)

    if context.engine_type == EngineType.SPARK:
        # Spark-specific implementation
        import pyspark.sql.functions as F
        df = context.df.withColumn("new_col", F.lit("spark"))
        return context.with_df(df)

    elif context.engine_type == EngineType.PANDAS:
        # Pandas-specific implementation
        df = context.df.copy()
        df["new_col"] = "pandas"
        return context.with_df(df)
```

## Complete Example

```yaml
project: ECommerceETL
engine: spark

connections:
  bronze:
    type: delta
    path: "dbfs:/bronze"
  silver:
    type: delta
    path: "dbfs:/silver"
  gold:
    type: delta
    path: "dbfs:/gold"

pipelines:
  - pipeline: orders_to_gold
    nodes:
      # Clean raw data
      - name: clean_orders
        source:
          connection: bronze
          path: orders
        transform:
          steps:
            - transformer: "clean_text"
              params:
                columns: ["customer_email"]
                trim: true
                case: "lower"

            - transformer: "cast_columns"
              params:
                casts:
                  order_date: "timestamp"
                  total_amount: "double"

            - transformer: "filter_rows"
              params:
                condition: "total_amount > 0"

      # Deduplicate and enrich
      - name: enriched_orders
        source: clean_orders
        depends_on: [clean_orders, customers]
        transform:
          steps:
            - transformer: "deduplicate"
              params:
                keys: ["order_id"]
                order_by: "updated_at DESC"

            - transformer: "join"
              params:
                right_dataset: "customers"
                on: ["customer_id"]
                how: "left"

            - transformer: "derive_columns"
              params:
                derivations:
                  order_year: "YEAR(order_date)"
                  order_month: "MONTH(order_date)"

      # Final merge to gold
      - name: gold_orders
        source: enriched_orders
        transformer: "merge"
        params:
          target: "gold.orders"
          keys: ["order_id"]
          strategy: "upsert"
          audit_cols:
            created_col: "dw_created_at"
            updated_col: "dw_updated_at"
        destination:
          connection: gold
          path: orders
```

## Best Practices

1. **Use SQL-first transforms** - They push computation to the engine for optimal performance
2. **Chain with transform.steps** - Compose multiple operations declaratively
3. **Prefer built-in transforms** - They're tested for dual-engine compatibility
4. **Use Pydantic models** - Define parameter schemas for custom transforms
5. **Handle nulls explicitly** - Use `fill_nulls` or `COALESCE` in derivations
6. **Document custom transforms** - Include docstrings and param descriptions

## Related

- [Quality Gates](quality_gates.md) - Validate transform outputs
- [Quarantine Tables](quarantine.md) - Handle failed validations
- [YAML Schema Reference](../reference/yaml_schema.md) - Complete configuration options
