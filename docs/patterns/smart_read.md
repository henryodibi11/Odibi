# Smart Read (Rolling Window)

The "Smart Read" feature simplifies incremental data loading by automatically generating the correct SQL query based on time windows.

> **Note:** This page describes the **Rolling Window** mode (Stateless). For exact state tracking (HWM), see [Stateful Incremental Loading](./incremental_stateful.md).

It eliminates the need to write complex SQL with `first_run_query` and dialect-specific date math.

!!! warning "Requirement: Write Configuration"
    Smart Read **requires** a `write` block in the same node.

    It determines whether to run a **Full Load** or **Incremental Load** by checking if the destination defined in `write` already exists.

    *   If you only want to read data (without writing), use the standard `query` option with explicit date filters instead.
    *   Ensure your `write` mode is set correctly (usually `append`) to preserve history.

## Write Modes for Incremental

| Mode | Suitability | Why? |
|---|---|---|
| **`append`** | ✅ **Recommended** | Safely adds new records to the lake. Preserves history. |
| **`upsert`** | ⚠️ **Advanced** | Use only if you are merging directly into a Silver layer table and have defined keys. |
| **`overwrite`** | ❌ **Dangerous** | **Do NOT use.** This would replace your entire historical dataset with just the latest batch (e.g., the last 3 days). |

## How It Works

Odibi checks if your **Write** target exists:

1.  **Target Missing (First Run):**
    *   It assumes this is a historical load.
    *   Generates: `SELECT * FROM source_table`
    *   Result: Loads all history.

2.  **Target Exists (Subsequent Runs):**
    *   It assumes this is an incremental load.
    *   Generates: `SELECT * FROM source_table WHERE column >= [Calculated Date]`
    *   Result: Loads only new/changed data.

## The Standard Pattern: "Ingest to Bronze"

The most common use case for Smart Read is the **Ingestion Node**. This node acts as a bridge between your external source (SQL, API) and your Data Lake (Bronze Layer).

### Why use this pattern?

1.  **State Management**: The node uses the **Write Target** (e.g., `bronze_orders`) as its state.
    *   *Target Empty?* $\rightarrow$ Run `SELECT *` (Full History)
    *   *Target Exists?* $\rightarrow$ Run `SELECT * ... WHERE date > X` (Incremental)
2.  **Efficiency**: Downstream nodes (e.g., "clean_orders") can simply depend on this node. They will receive the dataframe containing *only* the data that was just ingested (the incremental batch), allowing your entire pipeline to process only new data efficiently.

### Example Node

```yaml
- name: "ingest_orders"
  description: "Incrementally load orders from SQL to Delta"

  # 1. READ (Source)
  read:
    connection: "sql_db"
    format: "sql"
    table: "orders"
    incremental:
      column: "updated_at"
      lookback: 3
      unit: "day"

  # 2. WRITE (Target - Required for state tracking)
  write:
    connection: "data_lake"
    format: "delta"
    table: "bronze_orders"
    mode: "append"  # Append new rows from the incremental batch
```

## Configuration

Use the `incremental` block in your `read` configuration.

### Example: Handling Updates & Inserts

This pattern handles both new records (`created_at`) and updates (`updated_at`).

```yaml
nodes:
  - name: "load_orders"
    read:
      connection: "sql_server_prod"
      format: "sql"
      table: "dbo.orders"

      incremental:
        column: "updated_at"         # Primary check
        fallback_column: "created_at" # If updated_at is NULL
        lookback: 1
        unit: "day"

    write:
      connection: "bronze"
      format: "delta"
      table: "orders_raw"
      mode: "append"
```

This generates:
```sql
SELECT * FROM dbo.orders
WHERE COALESCE(updated_at, created_at) >= '2023-10-25 10:00:00'
```

### Example: Simple Append-Only

Perfect for pipelines that run every hour but want a 4-hour safety window for late-arriving data.

```yaml
    read:
      connection: "postgres_db"
      format: "sql"
      table: "public.events"
      incremental:
        column: "event_time"
        lookback: 4
        unit: "hour"

    write:
      connection: "bronze"
      format: "delta"
      table: "events_raw"
      mode: "append"
```

### Advanced: Merging directly to Silver (Upsert)

If you are bypassing Bronze and merging directly into a Silver table, you can use `upsert`.
**Note:** This requires defining the primary `keys` to match on.

```yaml
    read:
      connection: "crm_db"
      format: "sql"
      table: "customers"
      incremental:
        column: "last_modified"
        lookback: 1
        unit: "day"

    write:
      connection: "silver"
      format: "delta"
      table: "dim_customers"
      mode: "upsert"
      options:
        keys: ["customer_id"]
```

## Supported Units

| Unit | Description |
|------|-------------|
| `hour` | Looks back N hours from `now()` |
| `day` | Looks back N days from `now()` |
| `month` | Looks back N * 30 days (approx) |
| `year` | Looks back N * 365 days (approx) |

## Comparison with Legacy Pattern

### ❌ Old Way (Manual)

You had to write two queries and know the SQL dialect.

```yaml
read:
  query: "SELECT * FROM orders WHERE updated_at >= DATEADD(DAY, -1, GETDATE())"
write:
  first_run_query: "SELECT * FROM orders"
```

### ✅ New Way (Smart Read)

Configuration is declarative and dialect-agnostic.

```yaml
read:
  table: "orders"
  incremental:
    column: "updated_at"
    lookback: 1
    unit: "day"
```

## FAQ

**Q: What if I want to reload all history manually?**
A: You can simply delete the target table (or folder) in your data lake. The next run will detect it's missing and trigger the full historical load.

**Q: Does this work with `depends_on`?**
A: This feature is for **Ingestion Nodes** (Node 1) that read from external systems. Downstream nodes automatically benefit because they receive the data frame produced by Node 1.

**Q: Can I mix this with custom SQL?**
A: No. If you provide a `query` in the `read` section, Odibi respects your manual query and ignores the `incremental` block.
