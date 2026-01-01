# Pattern: SQL Server Merge/Upsert

**Status:** Core Pattern (Phase 4 Complete)  
**Target:** Azure SQL Database, SQL Server  
**Engine:** Spark, Pandas, Polars  
**Strategy:** T-SQL MERGE via Staging Table  
**Idempotent:** Yes (by key)

---

## Problem

Syncing data from Databricks/Delta Lake to Azure SQL Server for Power BI or operational systems.
Standard JDBC writes only support basic modes (`overwrite`, `append`), forcing you to either:

1. **Full overwrite** - Inefficient for large tables
2. **Manual staging tables + stored procedures** - Tedious and error-prone

## Solution

Odibi's SQL Server Merge uses a **staging table pattern**:

1. Write source DataFrame to a staging table
2. Execute T-SQL `MERGE` statement against the target
3. Return insert/update/delete counts

This provides **incremental upsert** with full control over conditions.

---

## Quick Start

### Minimal Config

```yaml
write:
  connection: azure_sql
  format: sql_server
  table: silver.orders
  mode: merge
  merge_keys: [order_id]
```

### Full Config

```yaml
write:
  connection: azure_sql
  format: sql_server
  table: oee.oee_fact
  mode: merge
  merge_keys: [DateId, P_ID]
  merge_options:
    update_condition: "source._hash_diff != target._hash_diff"
    delete_condition: "source._is_deleted = 1"
    insert_condition: "source.is_valid = 1"
    exclude_columns: [_hash_diff, _is_deleted]
    staging_schema: staging
    audit_cols:
      created_col: created_ts
      updated_col: updated_ts
    validations:
      check_null_keys: true
      check_duplicate_keys: true
      fail_on_validation_error: true
    # Phase 4 options:
    auto_create_schema: true
    auto_create_table: true
    batch_size: 10000
    schema_evolution:
      mode: evolve
      add_columns: true
```

---

## How It Works

```
┌─────────────────────┐
│   Source DataFrame  │
│   (Spark/Pandas/    │
│    Polars)          │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ 1. Validate Keys    │  Check for NULL/duplicate merge keys
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ 2. Write to Staging │  [staging].[table_staging]
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ 3. Execute T-SQL    │  MERGE target USING staging
│    MERGE            │  ON (keys match)
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ 4. Return Counts    │  inserted: 50, updated: 10, deleted: 2
└─────────────────────┘
```

### Generated T-SQL

```sql
MERGE [oee].[oee_fact] AS target
USING [staging].[oee_fact_staging] AS source
ON target.[DateId] = source.[DateId] AND target.[P_ID] = source.[P_ID]

WHEN MATCHED AND source._hash_diff != target._hash_diff THEN
    UPDATE SET
        [value] = source.[value],
        [updated_ts] = GETUTCDATE()

WHEN MATCHED AND source._is_deleted = 1 THEN
    DELETE

WHEN NOT MATCHED BY TARGET AND source.is_valid = 1 THEN
    INSERT ([DateId], [P_ID], [value], [created_ts], [updated_ts])
    VALUES (source.[DateId], source.[P_ID], source.[value], GETUTCDATE(), GETUTCDATE())

OUTPUT $action INTO @MergeActions;

SELECT
    SUM(CASE WHEN action = 'INSERT' THEN 1 ELSE 0 END) AS inserted,
    SUM(CASE WHEN action = 'UPDATE' THEN 1 ELSE 0 END) AS updated,
    SUM(CASE WHEN action = 'DELETE' THEN 1 ELSE 0 END) AS deleted
FROM @MergeActions;
```

---

## Configuration Reference

### `merge_keys` (Required)

Columns that form the `ON` clause of the MERGE. Can be single or composite.

```yaml
merge_keys: [order_id]                    # Single key
merge_keys: [plant_id, material_id]       # Composite key
merge_keys: [DateId, P_ID, Shift]         # Multi-column
```

### `merge_options`

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `update_condition` | string | None | SQL condition for WHEN MATCHED UPDATE |
| `delete_condition` | string | None | SQL condition for WHEN MATCHED DELETE |
| `insert_condition` | string | None | SQL condition for WHEN NOT MATCHED INSERT |
| `exclude_columns` | list | [] | Columns to exclude from merge |
| `staging_schema` | string | `staging` | Schema for staging table |
| `audit_cols` | object | None | Auto-populate created/updated timestamps |
| `validations` | object | None | Pre-merge validation checks |
| `auto_create_schema` | bool | false | Auto-create schema if missing |
| `auto_create_table` | bool | false | Auto-create target table from DataFrame |
| `schema_evolution` | object | None | Handle schema differences |
| `batch_size` | int | None | Chunk large writes for memory efficiency |

### Conditions

Use `source.` and `target.` prefixes in conditions:

```yaml
update_condition: "source._hash_diff != target._hash_diff"
delete_condition: "source._is_deleted = 1"
insert_condition: "source.is_valid = 1"
```

### Audit Columns

Auto-populate timestamp columns with `GETUTCDATE()`:

```yaml
audit_cols:
  created_col: created_ts   # Set on INSERT only
  updated_col: updated_ts   # Set on INSERT and UPDATE
```

### Validations

Check data quality before merge:

```yaml
validations:
  check_null_keys: true           # Fail if merge keys contain NULL
  check_duplicate_keys: true      # Fail if duplicate key combinations
  fail_on_validation_error: true  # Fail vs. warn
```

---

## Phase 4: Advanced Features

### Auto Schema Creation

Create the schema if it doesn't exist:

```yaml
merge_options:
  auto_create_schema: true   # Runs: CREATE SCHEMA [staging]
```

### Auto Table Creation

Create the target table from DataFrame schema if missing:

```yaml
merge_options:
  auto_create_table: true
```

Type mapping from DataFrame to SQL Server:

| Pandas/Polars Type | SQL Server Type |
|--------------------|-----------------|
| int64 / Int64 | BIGINT |
| int32 / Int32 | INT |
| float64 / Float64 | FLOAT |
| object / Utf8 | NVARCHAR(MAX) |
| datetime64 / Datetime | DATETIME2 |
| bool / Boolean | BIT |

### Schema Evolution

Control how schema differences are handled:

```yaml
merge_options:
  schema_evolution:
    mode: strict    # Default: fail if schemas differ
    # mode: evolve  # Add new columns via ALTER TABLE
    # mode: ignore  # Write only matching columns
    add_columns: true  # With 'evolve', run ALTER TABLE ADD COLUMN
```

| Mode | Behavior |
|------|----------|
| `strict` | Fail if DataFrame has columns not in target table |
| `evolve` | Add new columns (if `add_columns: true`) |
| `ignore` | Write only columns that exist in target table |

### Batch Processing

Chunk large DataFrames for memory efficiency:

```yaml
merge_options:
  batch_size: 10000   # Write 10k rows at a time to staging
```

---

## Overwrite Strategies

For non-merge writes, use enhanced overwrite strategies:

```yaml
write:
  connection: azure_sql
  format: sql_server
  table: fact.summary
  mode: overwrite
  overwrite_options:
    strategy: truncate_insert  # Default
    # strategy: drop_create    # Drop and recreate table
    # strategy: delete_insert  # DELETE FROM then INSERT
```

| Strategy | Behavior | Best For |
|----------|----------|----------|
| `truncate_insert` | TRUNCATE TABLE then INSERT | Fast, needs TRUNCATE permission |
| `drop_create` | DROP TABLE, CREATE, INSERT | Schema refresh |
| `delete_insert` | DELETE FROM then INSERT | Limited permissions |

---

## Engine Parity

All three engines support SQL Server Merge:

| Feature | Spark | Pandas | Polars |
|---------|-------|--------|--------|
| Basic Merge | ✅ | ✅ | ✅ |
| Composite Keys | ✅ | ✅ | ✅ |
| Conditions (update/delete/insert) | ✅ | ✅ | ✅ |
| Audit Columns | ✅ | ✅ | ✅ |
| Validations | ✅ | ✅ | ✅ |
| Auto Schema Creation | ✅ | ✅ | ✅ |
| Auto Table Creation | ✅ | ✅ | ✅ |
| Schema Evolution | ✅ | ✅ | ✅ |
| Batch Processing | ✅ | ✅ | ✅ |
| Enhanced Overwrite | ✅ | ✅ | ✅ |

---

## Examples

### Example 1: OEE Fact Table Sync

Sync OEE (Overall Equipment Effectiveness) data to Azure SQL for Power BI:

```yaml
nodes:
  - id: sync_oee_to_sql
    name: "Sync OEE to Azure SQL"
    read:
      connection: delta_lake
      format: delta
      table: gold.oee_fact
    write:
      connection: azure_sql
      format: sql_server
      table: oee.oee_fact
      mode: merge
      merge_keys: [DateId, P_ID]
      merge_options:
        update_condition: "source._hash_diff != target._hash_diff"
        exclude_columns: [_hash_diff]
        audit_cols:
          created_col: _sys_created_at
          updated_col: _sys_updated_at
```

### Example 2: Dimension with Soft Deletes

Handle soft deletes by flagging deleted records:

```yaml
write:
  connection: azure_sql
  format: sql_server
  table: dim.customers
  mode: merge
  merge_keys: [customer_id]
  merge_options:
    delete_condition: "source._is_deleted = 1"
    exclude_columns: [_is_deleted]
```

### Example 3: First Load with Auto-Create

Auto-create schema and table on first load:

```yaml
write:
  connection: azure_sql
  format: sql_server
  table: new_schema.new_table
  mode: merge
  merge_keys: [id]
  merge_options:
    auto_create_schema: true
    auto_create_table: true
```

### Example 4: Schema Evolution

Add new columns automatically as your source evolves:

```yaml
write:
  connection: azure_sql
  format: sql_server
  table: reporting.metrics
  mode: merge
  merge_keys: [metric_id, date]
  merge_options:
    schema_evolution:
      mode: evolve
      add_columns: true
```

---

## Troubleshooting

### "Target table does not exist"

The table must exist for merge. Either:
- Create it manually first
- Use `auto_create_table: true`
- Use `mode: overwrite` for initial load, then switch to `merge`

### "Merge key validation failed: NULL values"

Your merge keys contain NULL values. Fix your source data or disable:

```yaml
validations:
  check_null_keys: false
```

### "Merge key validation failed: duplicates"

Duplicate key combinations exist in your source. Deduplicate first or disable:

```yaml
validations:
  check_duplicate_keys: false
```

### "Schema evolution mode is 'strict' but DataFrame has new columns"

Your DataFrame has columns not in the target table. Options:
- Add columns to target table manually
- Use `schema_evolution.mode: evolve` with `add_columns: true`
- Use `schema_evolution.mode: ignore` to skip new columns

### Permission errors

Ensure your SQL Server user has permissions for:
- `CREATE TABLE` (for auto_create_table)
- `CREATE SCHEMA` (for auto_create_schema)
- `ALTER TABLE` (for schema evolution)
- `TRUNCATE TABLE` (for truncate_insert strategy)

---

## Connection Setup

### Connection Configuration

```yaml
connections:
  azure_sql:
    type: sql_server
    host: your-server.database.windows.net
    database: your-database
    username: ${SQL_USER}
    password: ${SQL_PASSWORD}
    driver: "ODBC Driver 18 for SQL Server"
```

### Azure AD Authentication

```yaml
connections:
  azure_sql:
    type: sql_server
    host: your-server.database.windows.net
    database: your-database
    authentication: ActiveDirectoryInteractive
```

---

## Related

- [Merge/Upsert Pattern (Delta Lake)](./merge_upsert.md)
- [Connections Guide](../features/connections.md)
- [Azure Setup Guide](../guides/setup_azure.md)
