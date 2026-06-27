# Catalog Sync Guide

Sync your system catalog to SQL Server for dashboards, cross-environment visibility, and easy SQL queries.

## Why Sync?

The Odibi System Catalog stores metadata in Delta tables, which provide ACID transactions, time travel, and schema evolution. However, querying Delta tables requires Spark or a compatible engine.

**Catalog Sync** replicates this data to SQL Server (or another blob storage), giving you:

- **SQL Access**: Query run history with plain SQL
- **Power BI**: Build dashboards without Spark
- **Cross-Environment Visibility**: See dev/qat/prod in one place
- **Analyst Friendly**: Share observability with the team

## Architecture

```
Pipeline Execution
        │
        ▼
┌───────────────────────────────────────┐
│  PRIMARY: Delta Tables                │
│  • Source of truth                    │
│  • ACID, time travel, schema evolution│
│  • Connection: system.connection      │
└───────────────────────────────────────┘
        │
        │  sync_to (automatic after run)
        ▼
┌───────────────────────────────────────┐
│  SECONDARY: SQL Server                │
│  • Query with SQL                     │
│  • Power BI dashboards                │
│  • Cross-environment views            │
│  • Connection: system.sync_to         │
└───────────────────────────────────────┘
```

## Quick Setup

Add `sync_to` to your system configuration:

```yaml
system:
  connection: adls_prod           # Primary - blob storage (Delta tables)
  environment: prod
  sync_to:
    connection: sql_server_prod   # Secondary - SQL Server
    schema_name: odibi_system
```

That's it! After each pipeline run, the catalog will automatically sync to SQL Server.

## Configuration Options

### Full Example

```yaml
system:
  connection: adls_prod           # Primary - must be blob/local storage
  path: _odibi_system             # Default path (optional)
  environment: prod

  sync_to:
    connection: sql_server_prod   # Target connection
    schema_name: odibi_system     # SQL Server schema
    mode: incremental             # incremental | full
    on: after_run                 # after_run | manual
    async_sync: true              # Don't block pipeline
    tables:                       # Optional: specific tables
      - meta_runs
      - meta_pipeline_runs
      - meta_node_runs
      - meta_failures
    sync_last_days: 90            # For large tables
```

### Options Reference

| Option | Default | Description |
|--------|---------|-------------|
| `connection` | *required* | Target connection name |
| `schema_name` | `odibi_system` | SQL Server schema |
| `path` | `_odibi_system` | Path for blob targets |
| `mode` | `incremental` | `incremental` or `full` |
| `on` | `after_run` | `after_run` or `manual` |
| `async_sync` | `true` | Run sync in background |
| `tables` | *(defaults)* | Tables to sync |
| `sync_last_days` | `null` | Limit to recent data |

### Default Tables

When `tables` is not specified, these high-priority tables are synced:

- `meta_runs` - Node-level run history
- `meta_pipeline_runs` - Pipeline summaries
- `meta_node_runs` - Detailed node runs
- `meta_tables` - Asset registry
- `meta_failures` - Failure records

## Sync to Another Blob Storage

You can also sync Delta tables to another blob storage for cross-region replication or backup:

```yaml
system:
  connection: adls_us_east
  sync_to:
    connection: adls_us_west      # Another blob connection
    path: _odibi_system_replica
    mode: incremental
```

This creates a Delta-to-Delta replica with all original features preserved.

## CLI Commands

### Manual Sync

```bash
# Sync all configured tables
odibi catalog sync config.yaml

# Sync specific tables
odibi catalog sync config.yaml --tables meta_runs,meta_failures

# Full sync (replace all data)
odibi catalog sync config.yaml --mode full

# Dry run - see what would sync
odibi catalog sync config.yaml --dry-run
```

### Check Sync Status

```bash
odibi catalog sync-status config.yaml
```

Output:
```
=== Catalog Sync Status ===

Primary: goat_prod_dm
Path: _odibi_system

Sync Target: goat_qat
Mode: incremental
Trigger: after_run
Async: True
Tables: (default high-priority tables)

--- Last Sync Timestamps ---
  meta_runs: 2026-01-11T16:59:50+00:00
  meta_pipeline_runs: 2026-01-11T16:59:50+00:00
  meta_node_runs: 2026-01-11T16:59:50+00:00
  meta_tables: never synced
  meta_failures: never synced
```

## SQL Server Tables

Odibi automatically creates tables in SQL Server with appropriate schemas:

```sql
-- Query recent runs
SELECT pipeline_name, node_name, status, timestamp
FROM odibi_system.meta_runs
WHERE date >= DATEADD(day, -7, GETDATE())
ORDER BY timestamp DESC;

-- Get pipeline success rates
SELECT
    pipeline_name,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as success_rate
FROM odibi_system.meta_pipeline_runs
WHERE date >= DATEADD(day, -30, GETDATE())
GROUP BY pipeline_name;
```

## Error Handling

Sync failures **never fail your pipeline**. If sync fails:

1. A warning is logged
2. The pipeline continues successfully
3. You can manually retry with `odibi catalog sync`

```
[WARNING] Catalog sync failed (non-fatal): Connection timeout
          Suggestion: Run 'odibi catalog sync' manually to retry
```

## Migrating from SQL Server Primary

If you previously had SQL Server as your primary system connection, migrate to this pattern:

**Before (broken):**
```yaml
system:
  connection: sql_server_prod     # ❌ Causes path error
  schema_name: odibi_system
```

**After (works):**
```yaml
system:
  connection: blob_prod           # ✓ Primary - Delta tables
  environment: prod
  sync_to:
    connection: sql_server_prod   # ✓ Secondary - SQL visibility
    schema_name: odibi_system
```

## Best Practices

1. **Use incremental mode** for regular syncs to minimize load
2. **Set `sync_last_days`** for large tables to avoid syncing years of history
3. **Use `async_sync: true`** (default) to not slow down pipelines
4. **Monitor with `sync-status`** to ensure syncs are working
5. **Run `--mode full`** occasionally to catch any missed records
