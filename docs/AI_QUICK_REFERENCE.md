# Odibi Quick Reference for AI

**Ultra-concise reference. Each topic = 1-2 sentences.**

---

## 🔥 MOST CRITICAL FACTS (Read These First!)

1. **Upsert/Append_Once FAIL without keys:** `mode: upsert` or `append_once` REQUIRES `options: {keys: [id]}` or you get RUNTIME ERROR
2. **Spark Temp Views:** Every node registers as TEMP VIEW. Reference with: `spark.sql("SELECT * FROM node_name")`
3. **Node Names:** Only alphanumeric_underscore. NO hyphens/dots/spaces
4. **Views Work:** YES - All engines can query database views via `table: schema.ViewName`
5. **Bronze Mode:** Use `append_once` with keys for idempotent ingestion

---

## Engines Can Query Views: YES ✅

**Spark:** `table: dbo.MyView` works. Every node creates temp view: `spark.sql("SELECT * FROM node_name")`  
**Pandas:** `table: dbo.MyView` works. SQL via DuckDB: `context.sql("SELECT ...")`  
**Polars:** Views work via SQL delegation.  

---

## Write Modes - Critical Requirements

- `overwrite` - No keys needed
- `append` - No keys needed
- **`upsert` - REQUIRES `options: {keys: [id]}`** ← **WILL FAIL at runtime without keys!**
- **`append_once` - REQUIRES `options: {keys: [id]}`** ← **WILL FAIL at runtime without keys!**
- **`merge` - REQUIRES `merge_keys: [id]`** ← SQL Server only

⚠️ **CRITICAL:** Using `upsert` or `append_once` without `options.keys` causes **RUNTIME ERROR: "keys required"**

**Bronze ingestion:** Use `append_once` with keys (idempotent, safe for retries)

---

## Node Names: Alphanumeric + Underscore ONLY

❌ `my-node` `my.node` `my node`  
✅ `my_node`

**Why:** Spark temp views require valid identifiers

---

## Incremental Modes

**Stateful (HWM):** Tracks last value in catalog. `mode: stateful, column: updated_at`  
**Rolling Window:** Recent data only. `mode: rolling_window, column: order_date, lookback: 7, unit: day`  
**First run:** Auto-detects missing target, does full load, captures HWM

---

## Validation

**Contracts:** Pre-transform checks, ALWAYS fail  
**Validation:** Post-transform, configurable (fail/warn/quarantine)  
**Gates:** Batch thresholds (pass rate, row counts)  

**Quarantine needs:** `on_fail: quarantine` + `quarantine: {connection: x, path: bad_data/}`

---

## Advanced Pattern Features

**Unknown Member:** `unknown_member: true` → Adds SK=0 row for orphan FK handling  
**Audit Columns:** `audit: {load_timestamp: true, source_system: "crm"}`  
**SCD2 in Dimension:** `scd_type: 2` delegates to scd2 transformer  
**Fact Lookups:** Auto-joins dimensions for SK resolution

---

## Delta Lake

**OPTIMIZE:** `auto_optimize: {enabled: true}`  
**Z-Order:** `zorder_by: [customer_id]` - Query performance  
**VACUUM:** `vacuum_retention_hours: 168` - Cleanup old files  
**Time Travel:** `time_travel: {as_of_version: 5}` or `{as_of_timestamp: "2024-01-01"}`  
**Partition:** `partition_by: [year, month]`

---

## Variables

`${ENV_VAR}` - Environment  
`${date:-7d}` - 7 days ago  
`${date:start_of_month}` - First of month  
`${vars.custom}` - From YAML vars section

---

## Cross-Pipeline

Reference other pipeline: `source: $pipeline_name.node_name`

---

## SCD2 Transformer (Self-Contained)

Writes directly (no separate write: block). **Spark/Pandas only** (not Polars).  
`use_delta_merge: true` - Optimized MERGE on Spark  
`vacuum_hours: 168` - Post-write cleanup

---

## SQL Pushdown

Works automatically. Incremental filters → WHERE clause at database.  
**Supports:** Spark JDBC, Pandas read_sql_query, Polars delegation

---

## Schema Evolution

**Spark:** `merge_schema: true` adds columns  
**SQL Server:** `schema_evolution: {mode: EVOLVE}` adds before MERGE  
**Pandas/Polars:** Limited support

---

## Delete Detection

**sql_compare:** Left anti-join to live source (recommended)  
**snapshot_diff:** Delta time travel comparison (full snapshot sources only)  
Both need `keys` + safety thresholds

---

## Non-Obvious Capabilities

- Views: YES on all engines
- Excel: Supported, delegates to Pandas if needed
- API pagination: cursor, offset, page, link_header all supported
- Soft deletes: `soft_delete_col: is_deleted`
- Retry: Built-in with exponential backoff
- PII: `anonymize()`, `redact()` transformers
- Streaming: Spark only (`streaming: true`)
- Remote globbing: Works with fsspec (abfss://, s3://)

---

**Remember: When in doubt, call discovery tools (list_patterns, list_transformers) - they return exact param requirements!**
