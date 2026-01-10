# Pattern: SQL Server Merge/Upsert

**Status:** Core Pattern (Phase 4 Complete)  
**Target:** Azure SQL Database, SQL Server  
**Engine:** Spark, Pandas, Polars  
**Strategy:** T-SQL MERGE via Staging Table  
**Idempotent:** Yes (by key)

---

## What is This? (For Beginners)

**The Problem You're Solving:**

You have data in Databricks/Delta Lake (your "data lake") and need to sync it to Azure SQL Server (your "reporting database" for Power BI, apps, etc.).

**Why Not Just Overwrite?**

Imagine you have 1 million customer records. Every day, only 100 customers change their address. With `overwrite`, you'd delete all 1 million rows and re-insert them—slow and wasteful. With `merge`, you only update those 100 changed rows.

**What is a MERGE?**

A MERGE (also called "upsert") does three things in one operation:
- **INSERT** new rows that don't exist in the target
- **UPDATE** existing rows that have changed
- **DELETE** rows that should be removed (optional)

**What is a "Staging Table"?**

A staging table is a temporary holding area. Odibi:
1. Writes your data to `[staging].[your_table_staging]`
2. Runs a MERGE from staging → target table
3. Leaves the staging table for debugging (it gets overwritten next run)

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

### Your First Merge (Complete Example)

If you're new to SQL Server merge, start here. This is a complete, working example:

```yaml
# 1. First, define your SQL Server connection
connections:
  azure_sql:
    type: sql_server
    host: your-server.database.windows.net
    database: your-database
    username: ${SQL_USER}        # Use environment variable
    password: ${SQL_PASSWORD}    # Never hardcode passwords!
    driver: "ODBC Driver 18 for SQL Server"

# 2. Then, create a node that syncs data to SQL Server
nodes:
  - name: sync_orders_to_sql
    read:
      connection: delta_lake     # Read from your data lake
      format: delta
      table: silver.orders
    write:
      connection: azure_sql      # Write to SQL Server
      format: sql_server
      table: dbo.orders          # Target table (schema.table)
      mode: merge                # Use MERGE instead of overwrite
      merge_keys: [order_id]     # Column(s) that identify each row
      merge_options:
        auto_create_table: true  # Create table on first run
        audit_cols:
          created_col: created_at
          updated_col: updated_at
```

**What this does:**
1. Reads orders from your Delta Lake silver layer
2. Creates `dbo.orders` table in SQL Server (first run only)
3. Inserts new orders, updates changed orders
4. Automatically tracks when each row was created/updated

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
  table: sales.fact_orders
  mode: merge
  merge_keys: [DateId, store_id]
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
    primary_key_on_merge_keys: true  # Create PK on merge keys for performance
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
MERGE [sales].[fact_orders] AS target
USING [staging].[fact_orders_staging] AS source
ON target.[DateId] = source.[DateId] AND target.[store_id] = source.[store_id]

WHEN MATCHED AND source._hash_diff != target._hash_diff THEN
    UPDATE SET
        [value] = source.[value],
        [updated_ts] = GETUTCDATE()

WHEN MATCHED AND source._is_deleted = 1 THEN
    DELETE

WHEN NOT MATCHED BY TARGET AND source.is_valid = 1 THEN
    INSERT ([DateId], [store_id], [value], [created_ts], [updated_ts])
    VALUES (source.[DateId], source.[store_id], source.[value], GETUTCDATE(), GETUTCDATE())

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

**What are merge keys?**

Merge keys tell SQL Server how to match rows between your source data and the target table. They answer the question: "Is this row new, or does it already exist?"

**Example:** If your table has customers identified by `customer_id`, then `customer_id` is your merge key. When Odibi sees `customer_id = 123` in the source, it checks if `customer_id = 123` exists in the target:
- If yes → UPDATE that row
- If no → INSERT a new row

**Single vs Composite Keys:**

```yaml
merge_keys: [order_id]                    # Single key - one column identifies a row
merge_keys: [store_id, product_id]       # Composite key - two columns together identify a row
merge_keys: [DateId, store_id, Shift]     # Multi-column - all three must match
```

**How do I know what my merge keys are?**

Ask yourself: "What columns make each row unique?" This is usually your primary key or business key.

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
| `primary_key_on_merge_keys` | bool | false | Create clustered PK on merge keys (with auto_create_table) |
| `index_on_merge_keys` | bool | false | Create nonclustered index on merge keys |
| `schema_evolution` | object | None | Handle schema differences |
| `batch_size` | int | None | Chunk large writes for memory efficiency |
| `incremental` | bool | false | Read target hashes, compare in engine, only write changed rows to staging |
| `hash_column` | string | None | Pre-computed hash column for change detection (auto-detects `_hash_diff`) |
| `change_detection_columns` | list | None | Columns to compute hash from (defaults to all non-key columns) |

### Conditions

**What are conditions?**

Conditions let you control WHEN to update, insert, or delete. They're optional but powerful.

**Why use `update_condition`?**

Without a condition, MERGE updates EVERY matched row—even if nothing changed. This is wasteful. With `update_condition`, you only update rows that actually changed:

```yaml
update_condition: "source._hash_diff != target._hash_diff"
```

This says: "Only update if the hash (a fingerprint of the data) is different."

**Why use `delete_condition`?**

Soft deletes: Instead of removing rows from your source, you flag them with `_is_deleted = 1`. The MERGE then deletes them from the target:

```yaml
delete_condition: "source._is_deleted = 1"
```

**Why use `insert_condition`?**

Skip invalid rows: Only insert rows that meet quality criteria:

```yaml
insert_condition: "source.is_valid = 1"
```

**Important:** Use `source.` and `target.` prefixes to refer to columns in your conditions.

### Audit Columns

**What are audit columns?**

Audit columns automatically track WHEN rows were created or last updated. This is essential for debugging and compliance.

```yaml
audit_cols:
  created_col: created_ts   # Set to current time on INSERT only
  updated_col: updated_ts   # Set to current time on INSERT and UPDATE
```

**What happens:**
- When a NEW row is inserted: both `created_ts` and `updated_ts` are set to now
- When an EXISTING row is updated: only `updated_ts` is set to now (created_ts stays the same)

**Do I need to add these columns to my DataFrame?**

No! Odibi automatically:
1. Adds these columns to the target table (if using `auto_create_table`)
2. Populates them with `GETUTCDATE()` during the MERGE

### Validations

**What are validations?**

Validations check your data BEFORE the merge runs, catching problems early.

**Why use them?**

- NULL keys cause MERGE failures (SQL Server can't match NULL = NULL)
- Duplicate keys cause unpredictable results (which duplicate wins?)

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

#### Primary Key on Merge Keys

Automatically create a clustered primary key on your merge keys when auto-creating the table. This:
- Enforces uniqueness (prevents duplicate key combinations)
- Improves MERGE performance (SQL Server uses the PK for the ON clause)
- Creates a clustered index (physically orders data by these columns)

```yaml
merge_options:
  auto_create_table: true
  primary_key_on_merge_keys: true  # Creates: PK_fact_orders PRIMARY KEY CLUSTERED ([DateId], [store_id])
```

#### Index on Merge Keys

If you already have a primary key elsewhere but want to speed up merges, create a nonclustered index instead:

```yaml
merge_options:
  index_on_merge_keys: true  # Creates: IX_fact_orders_DateId_store_id NONCLUSTERED ([DateId], [store_id])
```

**Note:** Use `primary_key_on_merge_keys` OR `index_on_merge_keys`, not both. Primary key takes precedence if both are set.

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

### Incremental Merge Optimization

**What is incremental merge?**

Without `incremental`, Odibi writes ALL your source rows to the staging table, then runs MERGE. If you have 1 million rows but only 100 changed, you're still writing 1 million rows to staging—wasteful!

With `incremental: true`, Odibi:
1. Reads the existing data from your target table (just the keys and hash)
2. Compares it with your source data IN MEMORY (Spark/Pandas/Polars)
3. Filters to only the rows that are NEW or CHANGED
4. Writes only those rows to staging
5. Runs MERGE on the smaller set

**When to use it:**

- Large tables (100K+ rows)
- Daily syncs where most data doesn't change
- When staging writes are slow

**When NOT to use it:**

- Small tables (just write everything, it's fast)
- Full refreshes where everything changes
- First-time loads (there's nothing to compare against)

```yaml
merge_options:
  incremental: true   # Only write changed rows to staging
```

**How it works:**
1. Reads target table's merge keys and hash column
2. Compares in Spark/Pandas/Polars to determine which rows changed
3. Only writes changed rows to staging table
4. Runs MERGE only on the changed subset

**Performance benefit:** If only 100 of 1M rows changed, staging table has 100 rows instead of 1M—10x faster!

#### Change Detection Options

**Option 1: Use existing hash column**

If your DataFrame already has a hash column (e.g., from SCD2 transformer):

```yaml
merge_options:
  incremental: true
  hash_column: _hash_diff   # Use pre-computed hash
```

**Option 2: Auto-detect `_hash_diff`**

Odibi auto-detects a column named `_hash_diff` if present:

```yaml
merge_options:
  incremental: true   # Auto-uses _hash_diff if present
```

**Option 3: Specify columns for hash computation**

If no hash column exists, specify which columns to use for change detection:

```yaml
merge_options:
  incremental: true
  change_detection_columns: [value, quantity, status]  # Only hash these columns
```

**Option 4: Hash all non-key columns (default)**

If no hash column or change_detection_columns specified, computes hash from all non-key columns:

```yaml
merge_options:
  incremental: true   # Hashes all columns except merge_keys
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

### Example 1: Sales Fact Table Sync

Sync sales metrics data to Azure SQL for Power BI:

```yaml
nodes:
  - id: sync_sales_to_sql
    name: "Sync Sales to Azure SQL"
    read:
      connection: delta_lake
      format: delta
      table: gold.fact_orders
    write:
      connection: azure_sql
      format: sql_server
      table: sales.fact_orders
      mode: merge
      merge_keys: [DateId, store_id]
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

Auto-create schema, table, and primary key on first load:

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
    primary_key_on_merge_keys: true  # Creates PK for better performance
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

### Example 5: Incremental Merge Optimization

Optimize large table syncs when only a small percentage of rows change:

```yaml
write:
  connection: azure_sql
  format: sql_server
  table: gold.fact_orders
  mode: merge
  merge_keys: [DateId, store_id]
  merge_options:
    incremental: true                           # Only write changed rows
    hash_column: _hash_diff                     # Use existing hash column
    update_condition: "source._hash_diff != target._hash_diff"
    exclude_columns: [_hash_diff]
    audit_cols:
      created_col: created_ts
      updated_col: updated_ts
```

**Result:** If syncing 1M rows daily but only 1K changed, staging table contains 1K rows instead of 1M—10x faster writes.

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

## FAQ (Frequently Asked Questions)

### Q: Should I use `merge` or `overwrite`?

**Use `merge` when:**
- You want to keep existing data and only add/update changes
- Your table is large and only a small portion changes
- You need to track created/updated timestamps

**Use `overwrite` when:**
- You want to replace all data every time
- Your table is small (overwrite is simpler)
- You're doing a full refresh/rebuild

### Q: What happens on the first run?

If the table doesn't exist and you have `auto_create_table: true`:
1. Odibi creates the table from your DataFrame schema
2. Adds audit columns if configured
3. Creates primary key/index if configured
4. Inserts all rows (everything is "new")

### Q: How do I know if my merge is working?

Check the logs! You'll see:
```
Starting SQL Server MERGE, target_table=sales.fact_orders, merge_keys=[DateId, store_id]
MERGE completed: inserted=50, updated=10, deleted=0
```

### Q: Why are my audit columns NULL?

This was a bug fixed in v2.2.0. If you created tables before this fix, run:
```sql
UPDATE [schema].[table] 
SET created_ts = GETUTCDATE(), updated_ts = GETUTCDATE() 
WHERE created_ts IS NULL
```

### Q: How do I handle deletes?

Two options:

**Soft delete (recommended):** Add an `_is_deleted` flag to your source, then use:
```yaml
delete_condition: "source._is_deleted = 1"
```

**Hard delete:** Not supported via MERGE. Use a separate DELETE statement after the merge.

### Q: Can I merge to multiple tables?

Yes! Create multiple nodes, each with its own `write` block targeting different tables.

### Q: What's the difference between `primary_key_on_merge_keys` and `index_on_merge_keys`?

- **Primary key**: Enforces uniqueness, creates clustered index, only one per table
- **Index**: Speeds up queries, allows duplicates, can have many per table

Use primary key if your merge keys ARE your primary key. Use index if you already have a different primary key.

---

## Related

- [Merge/Upsert Pattern (Delta Lake)](./merge_upsert.md)
- [Connections Guide](../features/connections.md)
- [Azure Setup Guide](../guides/setup_azure.md)
