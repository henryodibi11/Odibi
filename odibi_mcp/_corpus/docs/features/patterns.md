# Loading Patterns

Pre-built execution patterns for common data warehouse loading scenarios including Dimension, Fact, SCD2, Merge, Aggregation, and Date Dimension operations.

## Overview

Odibi's pattern system provides:
- **Declarative loading**: Configure complex loading logic via YAML
- **Engine agnostic**: Works with Spark, Pandas, and Polars engines
- **Built-in validation**: Patterns validate required parameters before execution
- **Extensible**: Create custom patterns by extending the `Pattern` base class

## Available Patterns

| Pattern | Description | Use Case |
|---------|-------------|----------|
| `dimension` | Builds dimension tables with surrogate keys and SCD support | Star schema dimensions with SK generation |
| `fact` | Builds fact tables with automatic surrogate key lookups | Star schema facts with dimension FK resolution |
| `date_dimension` | Generates a complete date dimension table | Calendar/fiscal date dimensions |
| `aggregation` | Declarative aggregation with time-grain rollups | Summary tables, KPI rollups |
| `scd2` | Slowly Changing Dimension Type 2 | Historical tracking of dimension changes |
| `merge` | Upsert/merge operations | Incremental updates, CDC |

> **Note:** For simple append or overwrite operations, use `write.mode: append` or `write.mode: overwrite` directly—no pattern needed.

## Configuration

Patterns are configured via the `pattern:` block in node configuration:

```yaml
nodes:
  - name: dim_customer
    read:
      connection: source
      path: customers.csv
    pattern:
      type: dimension
      params:
        natural_key: customer_id
        surrogate_key: customer_sk
        scd_type: 1
        track_cols: [name, email, tier]
```

### Config Options

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `pattern.type` | string | Yes | Pattern name: `dimension`, `fact`, `date_dimension`, `aggregation`, `scd2`, `merge` |
| `pattern.params` | object | Yes | Pattern-specific parameters (see below) |

## Pattern Parameters

### Dimension Pattern

Builds complete dimension tables with auto-generated surrogate keys and SCD support (Type 0, 1, or 2). Includes optional unknown member row (SK=0) for orphan FK handling and audit columns.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `natural_key` | string | Yes | - | Natural/business key column name |
| `surrogate_key` | string | Yes | - | Surrogate key column name to generate |
| `scd_type` | int | No | `1` | `0` = static, `1` = overwrite, `2` = history tracking |
| `track_cols` | list | Conditional | - | Columns to monitor for changes (required for SCD Type 1 and 2) |
| `target` | string | Conditional | - | Target table path (required for SCD Type 2 to read existing history) |
| `unknown_member` | bool | No | `false` | If true, insert a row with SK=0 for orphan FK handling |
| `audit` | object | No | `{}` | Audit config: `{load_timestamp: true, source_system: "name"}` |

### Fact Pattern

Builds fact tables with automatic surrogate key lookups from dimension tables, grain validation, orphan handling, and measure calculations.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `grain` | list | No | - | Columns that define uniqueness (validates no duplicates) |
| `dimensions` | list | No | `[]` | Dimension lookup configurations (see below) |
| `orphan_handling` | string | No | `unknown` | `unknown` (SK=0), `reject` (error), or `quarantine` |
| `quarantine` | object | No | - | Quarantine config (required when `orphan_handling: quarantine`) |
| `measures` | list | No | `[]` | Measure definitions (passthrough column names or calculated expressions) |
| `audit` | object | No | `{}` | Audit config: `{load_timestamp: true, source_system: "name"}` |
| `deduplicate` | bool | No | `false` | If true, removes duplicates before insert |
| `keys` | list | Conditional | - | Keys for deduplication (required when `deduplicate: true`) |

#### Dimension Lookup Config

Each entry in the `dimensions` list requires:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source_column` | string | Yes | Column in source data to look up |
| `dimension_table` | string | Yes | Name of the dimension node in context |
| `dimension_key` | string | Yes | Natural key column in the dimension table |
| `surrogate_key` | string | Yes | Surrogate key column to retrieve |
| `scd2` | bool | No | If true, filters `is_current=true` before lookup |

#### Orphan Handling Strategies

| Strategy | Description |
|----------|-------------|
| `unknown` | Map orphans to surrogate key 0 (unknown member row) |
| `reject` | Raise an error if orphan records are found |
| `quarantine` | Write orphan records to a separate quarantine location |

### Date Dimension Pattern

Generates a complete date dimension table with pre-calculated attributes useful for BI/reporting. Does not require a `read:` block—data is generated from the date range parameters.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `start_date` | string | Yes | - | Start date in `YYYY-MM-DD` format |
| `end_date` | string | Yes | - | End date in `YYYY-MM-DD` format |
| `date_key_format` | string | No | `yyyyMMdd` | Format for `date_sk` (e.g., `20240115`) |
| `fiscal_year_start_month` | int | No | `1` | Month when fiscal year starts (1–12) |
| `unknown_member` | bool | No | `false` | If true, add unknown date row with `date_sk=0` |

#### Generated Columns

| Column | Description |
|--------|-------------|
| `date_sk` | Integer surrogate key (YYYYMMDD format) |
| `full_date` | The actual date |
| `day_of_week` | Day name (Monday, Tuesday, etc.) |
| `day_of_week_num` | Day number (1=Monday, 7=Sunday) |
| `day_of_month` | Day of month (1–31) |
| `day_of_year` | Day of year (1–366) |
| `is_weekend` | Boolean flag |
| `week_of_year` | ISO week number (1–53) |
| `month` | Month number (1–12) |
| `month_name` | Month name (January, February, etc.) |
| `quarter` | Calendar quarter (1–4) |
| `quarter_name` | Q1, Q2, Q3, Q4 |
| `year` | Calendar year |
| `fiscal_year` | Fiscal year (based on `fiscal_year_start_month`) |
| `fiscal_quarter` | Fiscal quarter (1–4) |
| `is_month_start` | First day of month |
| `is_month_end` | Last day of month |
| `is_year_start` | First day of year |
| `is_year_end` | Last day of year |

### Aggregation Pattern

Declarative aggregation with SQL expressions, optional HAVING clause, and incremental merge strategies.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `grain` | list | Yes | - | Columns to GROUP BY (defines uniqueness) |
| `measures` | list | Yes | - | Measure definitions with `name` and `expr` (see below) |
| `having` | string | No | - | Optional HAVING clause for filtering aggregates |
| `target` | string | No | - | Target table path (required for incremental mode) |
| `incremental` | object | No | - | Incremental merge config (see below) |
| `audit` | object | No | `{}` | Audit config: `{load_timestamp: true, source_system: "name"}` |

#### Measure Definition

Each entry in the `measures` list is a dict:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Output column name |
| `expr` | string | Yes | SQL aggregation expression (e.g., `SUM(amount)`) |

#### Incremental Config

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `timestamp_column` | string | Yes | - | Column to identify new data |
| `merge_strategy` | string | No | `replace` | `replace`, `sum`, `min`, or `max` |

### SCD2 Pattern

Tracks history by creating new rows for updates. When a tracked column changes, the old record is closed and a new record is inserted.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `target` | string | Yes | - | Target table name or path containing history |
| `keys` | list | Yes | - | Natural keys to identify unique entities |
| `track_cols` | list | No | - | Columns to monitor for changes |
| `time_col` | string | No | - | Timestamp column for versioning (default: current time) |
| `valid_from_col` | string | No | `valid_from` | Name of the start timestamp column |
| `valid_to_col` | string | No | `valid_to` | Name of the end timestamp column |
| `is_current_col` | string | No | `is_current` | Name of the current record flag column |

### Merge Pattern

Upsert/merge logic with support for multiple strategies.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `target` | string | Yes | - | Target table name or path |
| `keys` | list | Yes | - | Join keys for matching records |
| `strategy` | string | No | `upsert` | `upsert`, `append_only`, `delete_match` |
| `audit_cols` | object | No | `null` | `{created_col: "...", updated_col: "..."}` |
| `update_condition` | string | No | `null` | SQL condition for update clause |
| `insert_condition` | string | No | `null` | SQL condition for insert clause |
| `delete_condition` | string | No | `null` | SQL condition for delete clause |
| `optimize_write` | bool | No | `false` | Run OPTIMIZE after write (Spark only) |
| `zorder_by` | list | No | `null` | Columns to Z-Order by |
| `cluster_by` | list | No | `null` | Columns to Liquid Cluster by (Delta) |

#### Merge Strategies

| Strategy | Description |
|----------|-------------|
| `upsert` | Update existing records, insert new ones |
| `append_only` | Ignore duplicates, only insert new keys |
| `delete_match` | Delete records in target that match keys in source |

## Pattern API

All patterns extend the `Pattern` base class:

```python
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Dict, List

from odibi.config import NodeConfig
from odibi.context import EngineContext
from odibi.engine.base import Engine


class Pattern(ABC):
    """Base class for Execution Patterns."""

    required_params: ClassVar[List[str]] = []
    optional_params: ClassVar[List[str]] = []
    param_aliases: ClassVar[Dict[str, List[str]]] = {}

    def __init__(self, engine: Engine, config: NodeConfig):
        self.engine = engine
        self.config = config
        self.params = config.params

    @abstractmethod
    def execute(self, context: EngineContext) -> Any:
        """Execute the pattern logic."""
        pass

    def validate(self) -> None:
        """Validate pattern configuration. Raises ValueError if invalid."""
        pass
```

### Creating Custom Patterns

1. Extend the `Pattern` base class
2. Define `required_params` and `optional_params` class variables
3. Implement `execute()` method
4. Optionally override `validate()` for parameter validation

```python
from odibi.patterns.base import Pattern
from odibi.context import EngineContext

class MyCustomPattern(Pattern):
    required_params = ["required_param"]
    optional_params = ["optional_param"]

    def validate(self) -> None:
        if not self.params.get("required_param"):
            raise ValueError("MyCustomPattern: 'required_param' is required.")

    def execute(self, context: EngineContext):
        df = context.df
        # Custom transformation logic
        return df
```

## Examples

### Dimension: Customer Dimension with SCD1

Build a customer dimension with surrogate keys and unknown member:

```yaml
project: CustomerDW

connections:
  source:
    type: local
    base_path: ./data/source
  gold:
    type: local
    base_path: ./data/gold

pipelines:
  - pipeline: build_dimensions
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
            track_cols:
              - name
              - email
              - tier
              - city
            unknown_member: true

        write:
          connection: gold
          format: parquet
          path: dim_customer.parquet
          mode: overwrite
```

### Fact: Sales Fact with Dimension Lookups

Build a fact table with automatic FK resolution from dimensions:

```yaml
pipelines:
  - pipeline: build_facts
    nodes:
      # Load dimensions into context for FK lookups
      - name: dim_customer
        read:
          connection: gold
          format: parquet
          path: dim_customer.parquet

      - name: dim_product
        read:
          connection: gold
          format: parquet
          path: dim_product.parquet

      - name: dim_date
        read:
          connection: gold
          format: parquet
          path: dim_date.parquet

      # Build fact table
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
            grain:
              - order_id
              - line_item_id
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
            measures:
              - quantity
              - amount
            audit:
              load_timestamp: true
              source_system: "orders_csv"

        write:
          connection: gold
          format: parquet
          path: fact_sales.parquet
          mode: overwrite
```

### Date Dimension: Generated Calendar

Generate a full year of dates with fiscal year support:

```yaml
pipelines:
  - pipeline: build_dimensions
    nodes:
      - name: dim_date
        pattern:
          type: date_dimension
          params:
            start_date: "2025-01-01"
            end_date: "2025-12-31"
            fiscal_year_start_month: 7
            unknown_member: true

        write:
          connection: gold
          format: parquet
          path: dim_date.parquet
          mode: overwrite
```

### Aggregation: Daily Sales Summary

Aggregate sales by date and product with incremental merge:

```yaml
pipelines:
  - pipeline: build_aggregates
    nodes:
      - name: agg_daily_sales
        read:
          connection: gold
          path: fact_sales.parquet
          format: parquet

        pattern:
          type: aggregation
          params:
            grain: [date_sk, product_sk]
            measures:
              - name: total_revenue
                expr: "SUM(amount)"
              - name: order_count
                expr: "COUNT(*)"
              - name: avg_order_value
                expr: "AVG(amount)"
            having: "COUNT(*) > 0"
            audit:
              load_timestamp: true

        write:
          connection: gold
          format: parquet
          path: agg_daily_sales.parquet
          mode: overwrite
```

### SCD2: Customer Dimension with History

Track customer address and tier changes over time:

```yaml
project: CustomerDW
engine: spark

connections:
  gold:
    type: delta
    path: /mnt/gold

pipelines:
  - pipeline: load_customer_dim
    nodes:
      - name: customer_scd2
        read:
          connection: bronze
          path: customers

        pattern:
          type: scd2
          params:
            target: "gold/customers"
            keys: ["customer_id"]
            track_cols: ["address", "city", "state", "tier"]
            valid_from_col: "valid_from"
            valid_to_col: "valid_to"
            is_current_col: "is_active"
```

### Merge: Incremental Customer Updates

Upsert with audit columns:

```yaml
pipelines:
  - pipeline: sync_customers
    nodes:
      - name: customers_merge
        read:
          connection: bronze
          path: customer_updates

        pattern:
          type: merge
          params:
            target: "silver.customers"
            keys: ["customer_id"]
            strategy: upsert
            audit_cols:
              created_col: "dw_created_at"
              updated_col: "dw_updated_at"
```

### Merge: GDPR Delete Request

Delete records matching source keys:

```yaml
pipelines:
  - pipeline: gdpr_delete
    nodes:
      - name: delete_customers
        read:
          connection: compliance
          path: deletion_requests

        pattern:
          type: merge
          params:
            target: "silver.customers"
            keys: ["customer_id"]
            strategy: delete_match
```

### Merge: Conditional Update

Only update if source record is newer:

```yaml
pipelines:
  - pipeline: sync_products
    nodes:
      - name: products_merge
        read:
          connection: bronze
          path: product_updates

        pattern:
          type: merge
          params:
            target: "silver.products"
            keys: ["product_id"]
            strategy: upsert
            update_condition: "source.updated_at > target.updated_at"
            insert_condition: "source.is_deleted = false"
```

### Simple Append (No Pattern Needed)

For event/fact data, just use write mode:

```yaml
pipelines:
  - pipeline: load_events
    nodes:
      - name: events
        read:
          connection: bronze
          path: events
        write:
          connection: gold
          path: events
          mode: append
```

### Simple Overwrite (No Pattern Needed)

For full refresh of small tables:

```yaml
pipelines:
  - pipeline: refresh_products
    nodes:
      - name: products
        read:
          connection: bronze
          path: products
        write:
          connection: gold
          path: products
          mode: overwrite
```

## Best Practices

1. **Choose the right pattern** - Use `dimension` for star schema dimensions with surrogate keys, `fact` for fact tables with FK lookups, `scd2` for standalone history tracking, `merge` for incremental CDC
2. **Use `date_dimension` for calendar tables** - Generates 18+ pre-calculated columns including fiscal year support
3. **Use write.mode for simple cases** - `append` for events, `overwrite` for full refresh
4. **Define business keys** - Ensure `keys` or `natural_key` uniquely identify records in your domain
5. **Monitor tracked columns** - For SCD patterns, only track columns that represent meaningful business changes
6. **Use audit columns** - Track `load_timestamp` and `source_system` for debugging and lineage
7. **Handle orphans** - Use `unknown_member: true` on dimensions and `orphan_handling: unknown` on facts to gracefully handle missing FK references
8. **Optimize large tables** - Use `zorder_by` or `cluster_by` on merge patterns for frequently queried columns

## Related

- [Transformers](transformers.md) - Built-in transformation functions
- [Pipelines](pipelines.md) - Pipeline configuration
- [YAML Schema Reference](../reference/yaml_schema.md) - Full schema documentation
