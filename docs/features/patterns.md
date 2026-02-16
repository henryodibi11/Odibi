# Loading Patterns

Pre-built execution patterns for common data warehouse loading scenarios including SCD2 and Merge operations.

## Overview

Odibi's pattern system provides:
- **Declarative loading**: Configure complex loading logic via YAML
- **Engine agnostic**: Works with Spark and Pandas engines
- **Built-in validation**: Patterns validate required parameters before execution
- **Extensible**: Create custom patterns by extending the `Pattern` base class

## Available Patterns

| Pattern | Description | Use Case |
|---------|-------------|----------|
| `scd2` | Slowly Changing Dimension Type 2 | Historical tracking of dimension changes |
| `merge` | Upsert/merge operations | Incremental updates, CDC |

> **Note:** For simple append or overwrite operations, use `write.mode: append` or `write.mode: overwrite` directly—no pattern needed.

## Configuration

Patterns are configured via the `transformer` field in node configuration:

```yaml
nodes:
  - name: load_customers
    transformer: scd2
    params:
      target: "gold/customers"
      keys: ["customer_id"]
      track_cols: ["address", "tier"]
      effective_time_col: "updated_at"
```

### Config Options

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `transformer` | string | Yes | Pattern name: `scd2`, `merge` |
| `params` | object | Yes | Pattern-specific parameters |

## Pattern Parameters

### SCD2 Pattern

Tracks history by creating new rows for updates. When a tracked column changes, the old record is closed and a new record is inserted.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `target` | string | Yes | - | Target table name or path containing history |
| `keys` | list | Yes | - | Natural keys to identify unique entities |
| `track_cols` | list | Yes | - | Columns to monitor for changes |
| `effective_time_col` | string | Yes | - | Source column indicating when the change occurred |
| `start_time_col` | string | No | `valid_from` | Name of the start timestamp column in target. Renamed from effective_time_col. |
| `end_time_col` | string | No | `valid_to` | Name of the end timestamp column |
| `current_flag_col` | string | No | `is_current` | Name of the current record flag column |
| `delete_col` | string | No | `null` | Column indicating soft deletion (boolean) |

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
from odibi.config import NodeConfig
from odibi.context import EngineContext
from odibi.engine.base import Engine

class Pattern(ABC):
    """Base class for Execution Patterns."""

    def __init__(self, engine: Engine, config: NodeConfig):
        self.engine = engine
        self.config = config
        self.params = config.params

    @abstractmethod
    def execute(self, context: EngineContext) -> Any:
        """
        Execute the pattern logic.

        Args:
            context: EngineContext containing current DataFrame and helpers.

        Returns:
            The transformed DataFrame.
        """
        pass

    def validate(self) -> None:
        """
        Validate pattern configuration.
        Raises ValueError if invalid.
        """
        pass
```

### Creating Custom Patterns

1. Extend the `Pattern` base class
2. Implement `execute()` method
3. Optionally override `validate()` for parameter validation

```python
from odibi.patterns.base import Pattern
from odibi.context import EngineContext

class MyCustomPattern(Pattern):

    def validate(self) -> None:
        if not self.params.get("required_param"):
            raise ValueError("MyCustomPattern: 'required_param' is required.")

    def execute(self, context: EngineContext):
        df = context.df
        # Custom transformation logic
        return df
```

## Examples

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
        transformer: scd2
        params:
          target: "gold/customers"
          keys: ["customer_id"]
          track_cols: ["address", "city", "state", "tier"]
          effective_time_col: "updated_at"
          end_time_col: "valid_to"
          current_flag_col: "is_active"
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
        transformer: merge
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
        transformer: merge
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
        transformer: merge
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

1. **Choose the right pattern** - Use SCD2 for dimensions needing history, Merge for incremental CDC
2. **Use write.mode for simple cases** - `append` for events, `overwrite` for full refresh
3. **Define business keys** - Ensure `keys` uniquely identify records in your domain
4. **Monitor tracked columns** - For SCD2, only track columns that represent meaningful business changes
5. **Use audit columns** - Track `created_at` and `updated_at` for debugging and lineage
6. **Optimize large tables** - Use `zorder_by` or `cluster_by` for frequently queried columns

## Related

- [Transformers](transformers.md) - Built-in transformation functions
- [Pipelines](pipelines.md) - Pipeline configuration
- [YAML Schema Reference](../reference/yaml_schema.md) - Full schema documentation
