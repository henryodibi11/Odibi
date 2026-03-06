# AI Complete Reference - All Odibi Capabilities

**For:** AI agents needing complete odibi knowledge  
**Priority:** Reference for advanced features

---

## Engine-Specific Features

### Spark Engine
- **Temp Views:** Every node output registers as temp view. Query with `spark.sql("SELECT * FROM node_name")`
- **JDBC Pushdown:** Incremental filters pushed to SQL Server/Azure SQL
- **Delta MERGE:** SCD2 uses optimized MERGE when available
- **OPTIMIZE/ZORDER:** Native Delta maintenance commands
- **Liquid Clustering:** `cluster_by` in merge transformer
- **Partition Overwrite:** Auto-configured to `dynamic` mode
- **Can Read:** Tables, views, Delta, Parquet, CSV, JSON, ORC, Avro
- **Views:** YES - Can query database views with `table: SchemaName.ViewName`

### Pandas Engine
- **DuckDB SQL:** `context.sql()` runs SQL over in-memory DataFrames
- **Delta via deltalake:** Read/write Delta format
- **SQL Server MERGE:** Via custom writer with T-SQL
- **Schema Evolution:** Via SQL Server writer
- **Can Read:** CSV, Parquet, JSON, Excel, Feather, Pickle, Delta, SQL tables/views
- **Views:** YES - Can query SQL views via `read_sql_query`

### Polars Engine
- **Lazy Execution:** Supports LazyFrame optimization
- **Delta via scan_delta:** Version-based time travel (not timestamp)
- **Delegates to Pandas:** For Excel, API reads
- **Can Read:** CSV, Parquet, JSON, Delta (via scan_delta)
- **Views:** YES - Via SQL connection delegation
- **NOT Supported:** SCD2 transformer (use Spark/Pandas)

---

## Connection Capabilities

### Azure SQL / SQL Server
- **Can Read:** Tables, views, queries, stored procedures
- **SQL Pushdown:** WHERE filters pushed to database
- **Write Modes:** All (including merge via T-SQL)
- **Schema Evolution:** Supported (EVOLVE mode adds columns)
- **Time Travel:** Via SQL Server temporal tables (if enabled)
- **Discovery:** ✅ list_schemas, list_tables, profile, get_table_info

### Azure ADLS
- **Auth Modes:** key_vault (recommended), direct_key, sas_token, service_principal, managed_identity
- **Formats:** All (CSV, Parquet, JSON, Delta, Excel via delegation)
- **Partition Detection:** Hive-style (key=value) auto-detected
- **Discovery:** ✅ list_files, list_folders, detect_partitions, profile
- **Spark:** Auto-configures with `configure_spark()`
- **Pandas/Polars:** Uses fsspec with storage_options

### Local / DBFS
- **Paths:** Local filesystem, `file://`, `dbfs://`, `/dbfs/`
- **Discovery:** ✅ Full discovery support
- **Use When:** Dev/testing, Databricks mounted volumes

### HTTP / API
- **Auth:** None, Basic, Bearer, API Key
- **Pagination:** offset, page, cursor, link_header
- **Response Parsing:** items_path, add_fields
- **Date Variables:** `$today`, `$yesterday`, `$7_days_ago`, `$start_of_month`, etc.
- **Use When:** REST APIs, third-party data sources

---

## Advanced Pattern Features

### Dimension Pattern - Hidden Features
- **Unknown Member:** `unknown_member: true` → Adds SK=0 row for orphan handling
- **Audit Columns:** `audit.load_timestamp: true`, `audit.source_system: "crm"`
- **SCD Types:** 0 (static), 1 (overwrite), 2 (history)
- **Target Formats:** Works with catalog tables, Delta paths, Parquet, CSV
- **SCD2 Delegation:** `scd_type: 2` uses scd2 transformer under the hood

### Fact Pattern - Advanced
- **Dimension Lookups:** Auto-joins to dimension tables for SK resolution
- **Orphan Handling:** unknown (SK=0), reject (error), quarantine (route to table)
- **Grain Validation:** Detects duplicates at specified grain
- **Measures:** Rename (`revenue: total_amount`) or calculate (`margin: "revenue - cost"`)
- **Deduplication:** `deduplicate: {keys: [...], keep: first/last}`

### SCD2 Transformer - Self-Contained
- **Writes Directly:** No separate `write:` block needed (has own writer)
- **Delta MERGE:** `use_delta_merge: true` (default) uses optimized MERGE on Spark
- **Register Table:** `register_table: "catalog.schema.table"` for Unity Catalog
- **VACUUM:** `vacuum_hours: 168` (7 days) for cleanup
- **NOT on Polars:** Use Spark or Pandas only

### Merge Transformer - Advanced
- **Audit Columns:** `audit_cols: {created_at: true, updated_at: true}`
- **Conditional Logic:** `update_condition: "status <> 'DELETED'"`, `delete_condition: "status = 'DELETED'"`
- **Delete Match:** `delete_match: true` for GDPR/soft deletes
- **OPTIMIZE After:** `optimize_write: true`, `zorder_by: [col1, col2]`
- **Liquid Clustering:** `cluster_by: [col1]` (Databricks Unity Catalog)

---

## Write Mode Decision Matrix

| Scenario | Mode | Keys Required | Options |
|----------|------|---------------|---------|
| Dimension full refresh | `overwrite` | No | - |
| Append-only logs | `append` | No | - |
| Bronze ingestion (idempotent) | `append_once` | **YES** | `options: {keys: [id]}` |
| Silver updates | `upsert` | **YES** | `options: {keys: [id]}` |
| Gold updates | `upsert` | **YES** | `options: {keys: [id]}` |
| SQL Server target | `merge` | **YES** | `merge_keys: [id]` |
| Facts (append new only) | `append` or `upsert` | Optional | - |

**CRITICAL:** `upsert` and `append_once` WILL FAIL without `options.keys` or `merge_keys`!

---

## Incremental Loading - Mode Selection

### Stateful (High Water Mark)
```yaml
read:
  incremental:
    mode: stateful
    column: updated_at
    state_key: orders_hwm  # Optional, auto-generated if omitted
    watermark_lag: "2h"    # Optional, for late-arriving data
```

**Use when:** Exact incremental from SQL, need to track last value processed  
**Engines:** All

### Rolling Window
```yaml
read:
  incremental:
    mode: rolling_window
    column: order_date
    lookback: 7
    unit: day
    date_format: iso  # oracle, sql_server, us, eu, iso
```

**Use when:** Load recent data only, stateless  
**Engines:** All

**Common Mistake:** Expecting exact-once with rolling window (it re-processes overlap)

---

## Validation & Quarantine

### Row-Level Validation
```yaml
validate:
  tests:
    - type: not_null
      columns: [customer_id, order_id]
    - type: range
      column: amount
      min: 0
      max: 1000000
  on_fail: quarantine  # fail, warn, quarantine
  quarantine:
    connection: warehouse
    path: quarantine/bad_orders
```

**Quarantine adds columns:**
- `_rejection_reason`
- `_rejected_at`
- `_failed_tests`
- `_source_batch_id`
- `_original_node`

### Quality Gates (Batch-Level)
```yaml
validate:
  gate:
    require_pass_rate: 0.95  # 95% of rows must pass
    thresholds:
      - type: row_count
        min: 100
        max: 1000000
        on_fail: abort  # abort, warn, quarantine_all
```

---

## Delta Lake Optimization

### Write with Optimization
```yaml
write:
  format: delta
  mode: overwrite
  partition_by: [year, month]
  zorder_by: [customer_id]  # Query performance
  table_properties:
    delta.autoOptimize.optimizeWrite: "true"
  auto_optimize:
    enabled: true
    vacuum_retention_hours: 168  # 7 days
```

### Time Travel
```yaml
read:
  format: delta
  path: gold/orders
  time_travel:
    as_of_version: 5  # OR as_of_timestamp: "2024-01-01 10:00:00"
```

**Use when:** Auditing, reproducing issues, comparing versions

---

## Delete Detection

### SQL Compare Mode (Recommended)
```yaml
delete_detection:
  mode: sql_compare
  keys: [order_id]
  source_connection: sales_db
  source_table: dbo.Orders
  max_delete_percent: 10  # Safety threshold
  on_threshold_breach: error
  soft_delete_col: is_deleted  # Set to true; null for hard delete
```

**Use when:** CDC-style detection from live source

### Snapshot Diff Mode
```yaml
delete_detection:
  mode: snapshot_diff
  keys: [order_id]
  connection: warehouse
  path: silver/orders
```

**Use when:** Full snapshot sources only (compares Delta versions)

---

## Cross-Pipeline References

```yaml
# Pipeline A
nodes:
  - name: bronze_orders
    write:
      path: bronze/orders

# Pipeline B (different file)
nodes:
  - name: silver_orders
    read:
      # References output from Pipeline A
      source: $pipeline_a.bronze_orders
```

**Use when:** Breaking pipelines into modular files

---

## SQL Server Enhanced Write

### Overwrite Strategies
```yaml
write:
  mode: overwrite
  format: sql
  table: dbo.Target
  overwrite_options:
    strategy: truncate_insert  # drop_create, delete_insert, truncate_insert
    audit_cols:
      created_at: true
      updated_at: true
```

**Strategies:**
- `truncate_insert` - Fast, preserves table structure
- `drop_create` - Rebuilds table (schema changes)
- `delete_insert` - Minimal permissions needed

---

## Variable Substitution

```yaml
read:
  query: "SELECT * FROM Orders WHERE created_at >= '${date:-7d}'"

write:
  path: "data/${vars.environment}/${date:YYYY-MM-DD}/output"
```

**Available:**
- `${ENV_VAR}` - Environment variables
- `${vars.custom}` - From YAML vars: section
- `${date:format}` - Current date formatted
- `${date:-7d}` - 7 days ago
- `${date:start_of_month}` - First day of current month

---

## Privacy & PII

```yaml
columns:
  email:
    pii: true

transform:
  steps:
    - function: anonymize
      params:
        columns: [email, ssn]
        method: hash  # hash, redact, mask
        salt: ${PII_SALT}
```

**Use when:** GDPR compliance, data sharing

---

## Common Non-Obvious Gotchas

### 1. Node Names Must Be Valid Identifiers
❌ `my-node`, `my.node`, `my node`  
✅ `my_node`

**Why:** Spark temp views require valid SQL identifiers

### 2. Upsert/Append_Once REQUIRE Keys
```yaml
write:
  mode: upsert
  options:
    keys: [id]  # ← MUST HAVE or runtime error!
```

### 3. SCD2 Needs Target for History
```yaml
transformer: scd2
params:
  keys: [id]
  tracked_columns: [name, email]
  target: warehouse.dim_customer  # ← MUST HAVE for history comparison
```

### 4. First-Run Behavior
- Incremental filters skip if target doesn't exist (full load first run)
- HWM captured even on full load (ready for next run)
- Use `write.first_run_query` if bootstrap query differs

### 5. Quarantine Requires Path
```yaml
validate:
  on_fail: quarantine
  quarantine:
    connection: warehouse
    path: quarantine/bad_data  # ← MUST SPECIFY
```

### 6. Delta Time Travel vs Snapshots
- **Spark:** Both version and timestamp supported
- **Pandas:** Both supported (via deltalake library)
- **Polars:** Only version (via scan_delta)
- **DuckDB:** Not supported

---

## Performance Tips

### 1. Use Delta for Large Datasets
- Faster than Parquet for updates
- Supports ACID transactions
- Enable OPTIMIZE + Z-Order

### 2. Partition Large Tables
```yaml
write:
  partition_by: [year, month, day]  # Time-based
  # OR
  partition_by: [region]            # Dimension-based
```

### 3. Use Append_Once for Bronze
- Idempotent (safe to retry)
- Prevents duplicates
- Faster than checking every row

### 4. Enable SQL Pushdown
- Filters applied at database
- Reduces data transfer
- Works automatically with incremental

### 5. Use Views When Possible
- Spark/SQL can query views directly
- No need to materialize if view is performant
- Reference like a table: `table: dbo.MyView`

---

## Schema Evolution

### Spark Delta
```yaml
write:
  merge_schema: true           # Add new columns
  overwrite_schema: false      # Don't drop columns
```

### SQL Server
```yaml
write:
  schema_evolution:
    mode: EVOLVE  # Add missing columns before MERGE
```

**Use when:** Source schema changes over time

---

## Advanced Validation

### 11 Test Types
1. `not_null` - Column must have values
2. `unique` - Column values must be unique
3. `accepted_values` - Value in allowed list
4. `row_count` - Table-level row count check
5. `custom_sql` - Custom SQL expression
6. `range` - Numeric bounds
7. `regex_match` - String pattern matching
8. `volume_drop` - Detect unexpected drops
9. `schema_contract` - Enforce expected schema
10. `distribution_contract` - Statistical distribution
11. `freshness_contract` - Data staleness check

### Contracts vs Validation vs Gates
- **Contracts:** Pre-transform, always fail (input quality)
- **Validation:** Post-transform, configurable (output quality)
- **Gates:** Batch-level thresholds (pipeline SLOs)

---

## Manufacturing & IoT Features

**Special transformers for manufacturing:**
- `shift_calendar` - Handle shift schedules
- `cycle_time` - Calculate cycle durations
- `downtime_classification` - Categorize machine states
- `oee_calculation` - Overall Equipment Effectiveness
- `reject_analysis` - Quality defect analysis

**Use when:** Manufacturing/OT data pipelines

---

## System Catalog Features

**The catalog tracks:**
- All pipeline runs (success/failure)
- Schema changes over time
- Lineage (data flow)
- State (HWM values)
- Metrics (row counts, durations)
- Health scores

**Query the catalog:**
```python
pm.list_runs()          # Recent runs
pm.list_tables()        # Registered tables
pm.get_state("hwm_key") # Get HWM value
```

---

## Semantic Layer

**Define metrics once, query anywhere:**

```yaml
semantic:
  metrics:
    - name: revenue
      expr: "SUM(total_amount)"
      source: gold.fact_orders
      filters: ["status = 'completed'"]

  dimensions:
    - name: region
      source: gold.dim_customer
      column: region

# Query from Python
project.query("revenue BY region")
```

---

## OpenLineage Integration

**Auto-generates lineage for:**
- Apache Atlas
- Amundsen
- DataHub
- Custom lineage systems

```yaml
lineage:
  enabled: true
  backend: openlineage
  endpoint: http://lineage-server
```

---

## Quick Reference: When to Use What

### Write Modes
- **Dimensions:** overwrite
- **Facts:** append or upsert (with keys)
- **Bronze:** append_once (with keys)
- **Silver/Gold:** upsert (with keys)
- **SQL Server:** merge (with merge_keys)

### Patterns
- **Lookup tables:** dimension
- **Change tracking:** scd2
- **Transactions:** fact
- **Calendar:** date_dimension
- **Summaries:** aggregation
- **Updates:** merge

### Incremental
- **SQL sources:** stateful (HWM)
- **Recent data only:** rolling_window
- **Full refresh:** No incremental config

### Engines
- **Databricks/EMR:** spark
- **Local/single-machine:** pandas
- **Performance-sensitive local:** polars

---

## Critical Reminders

1. **Views work!** Don't materialize if not needed - query views directly
2. **Keys are required** for upsert/append_once/merge
3. **Unknown member** must be enabled in dims when facts use orphan_handling: unknown
4. **Node names** must be valid SQL identifiers (alphanumeric_underscore)
5. **Format** must be specified for both read and write
6. **Connection names** must exist in odibi.yaml
7. **Schema evolution** requires explicit enable (not default)
8. **VACUUM** only works on Delta format
9. **Time travel** capabilities differ by engine
10. **Quarantine** requires path configuration

---

## Non-Obvious Capabilities

- **Variable substitution** works anywhere in YAML
- **Date math** in variables (`${date:-30d}`)
- **Cross-pipeline references** with `$pipeline.node`
- **First-run queries** differ from incremental
- **Soft deletes** via delete_detection + soft_delete_col
- **PII redaction** via anonymize/redact transformers
- **Retry with backoff** configured per node
- **Excel glob patterns** for multiple sheets
- **Remote globbing** for cloud paths (fsspec)
- **Streaming** flag for Spark Structured Streaming
- **View materialization** in semantic layer

---

**This reference covers ALL of odibi's capabilities. Use it when users ask about advanced features!**
