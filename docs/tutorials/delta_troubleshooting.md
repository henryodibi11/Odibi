# Delta Lake Troubleshooting Guide

Common Delta Lake errors you'll hit while building odibi pipelines, with the symptoms, root causes, fixes, and prevention strategies. Each entry has been verified against real Delta tables on Databricks (DBR 17.3 LTS, Spark 4.0.0) or in odibi's local Pandas/Polars test suites.

> **Scope:** This guide is for runtime/operational Delta issues. For odibi-specific patterns (SCD2, Merge, FK validation), see [troubleshooting.md](../troubleshooting.md).

---

## Quick Index

| # | Issue | Symptom keyword |
|---|---|---|
| 1 | [Schema evolution errors](#1-schema-evolution-errors) | `SchemaMismatch`, `column added/removed/type` |
| 2 | [Concurrent write conflicts](#2-concurrent-write-conflicts-merge--merge) | `ConcurrentAppend`, `MetadataChanged`, `commit conflict` |
| 3 | ["Table already exists"](#3-table-already-exists-errors) | `TableAlreadyExistsException`, `Path already exists` |
| 4 | [Null type columns](#4-null-type-columns-reject-on-write) | `Invalid data type for Delta Lake: Null` |
| 5 | [VACUUM and file retention](#5-vacuum-and-file-retention) | `IllegalArgumentException: retentionHours` |
| 6 | [Restore to previous version](#6-restore-to-a-previous-version) | corrupted/bad write recovery |
| 7 | [Time travel queries](#7-time-travel-queries) | `versionAsOf`, `timestampAsOf` |
| 8 | [Z-ORDER optimization](#8-z-order-optimization) | small files, slow filtered reads |
| 9 | [pyarrow / deltalake version conflicts](#9-pyarrow--deltalake-version-conflicts) | `<17.0.0,>=14.0.0`, `schema_mode` vs `overwrite_schema` |
| 10 | [Databricks vs local Delta differences](#10-databricks-vs-local-delta-behavior-differences) | API surface mismatch |

---

## 1. Schema Evolution Errors

**Symptom**

```text
AnalysisException: A schema mismatch detected when writing to the Delta table.
Table schema:
root
 |-- id: long (nullable = true)
 |-- amount: double (nullable = true)

Data schema:
root
 |-- id: long (nullable = true)
 |-- amount: string (nullable = true)
```

Or when adding a column:

```text
AnalysisException: A column or field with name `new_field` cannot be resolved.
```

**Root Cause**

Delta enforces schema by default. Adding a column, dropping a column, or changing a column type triggers a mismatch unless you explicitly enable schema evolution. Common triggers:
- Source CSV/JSON inferred a different type than the target table (e.g., `amount` parsed as `string`).
- A new field was added upstream (Bronze) but not propagated to Silver/Gold.
- Casting was lost during a transformer chain.

**Fix**

For odibi pipelines, enable schema merge in the write step:

```yaml
write:
  format: delta
  mode: append
  options:
    mergeSchema: "true"          # adds new columns
    # autoMerge.enabled: "true"  # alternative for cluster-wide config
```

For type mismatches, normalize in transforms before writing:

```yaml
transform:
  - type: cast
    columns:
      amount: "double"
```

In raw PySpark/deltalake:

```python
# PySpark
df.write.format("delta").option("mergeSchema", "true").mode("append").save(path)

# deltalake (Python, 0.x — odibi-pinned)
write_deltalake(path, df, mode="overwrite", schema_mode="overwrite")
```

**Prevention**

- Use `odibi catalog schema-history <table>` (CLI) to see when schemas changed.
- Pin source schemas in YAML (`read.schema:`) so upstream drift fails fast at Bronze, not silently propagates.
- For breaking changes (drop/rename/retype), use `spark.sql("ALTER TABLE ... ALTER COLUMN ...")` with `delta.columnMapping.mode = 'name'` enabled. Plain overwrite-with-new-schema risks data loss.

---

## 2. Concurrent Write Conflicts (MERGE + MERGE)

**Symptom**

```text
ConcurrentAppendException: Files were added to partition [date=2026-04-30]
by a concurrent update. Please try the operation again.
```

```text
ConcurrentDeleteReadException: This transaction attempted to read one or more
files that were deleted (for example /path/_delta_log/00000.json) by a concurrent update.
```

```text
MetadataChangedException: The metadata of the Delta table has been changed by a concurrent update.
```

**Root Cause**

Delta uses optimistic concurrency. Two writers (e.g., two scheduled jobs both running MERGE on the same target) can pass the validation phase, but only one wins the commit. The loser sees a conflict.

The most common offenders:
- Hourly job + ad-hoc backfill MERGE-ing the same target.
- A job retried via Databricks workflow while the original is still running.
- Two pipelines writing different partitions of the same un-partitioned table.

**Fix**

1. **Partition the target.** Concurrent writes to *different* partitions of a partitioned table do not conflict (post DBR 11.x). Add `partition_by:` to the write step:

```yaml
write:
  format: delta
  mode: merge
  partition_by: [event_date]
  options:
    keys: [order_id]
```

2. **Serialize concurrent writers.** Use a Databricks job dependency or distributed lock (e.g., one queue, one consumer) for the same partition.

3. **Retry with backoff.** odibi already wraps Delta MERGE in retry logic for transient conflicts (see `transformers/merge_transformer.py`). For raw code:

```python
import time
from delta.exceptions import ConcurrentAppendException

for attempt in range(5):
    try:
        target.alias("t").merge(src.alias("s"), "t.id = s.id")\
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        break
    except ConcurrentAppendException:
        time.sleep(2 ** attempt)
else:
    raise
```

**Prevention**

- Always partition large MERGE targets by a high-cardinality time column.
- Avoid running two pipelines that target the same Delta table in overlapping windows.
- In odibi, set `retry: {max_attempts: 5, backoff: exponential}` in pipeline config for tolerant retry on conflicts.

---

## 3. "Table Already Exists" Errors

**Symptom**

```text
AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view
`eaai_dev`.`hardening_scratch`.`my_table` because it already exists.
```

```text
AnalysisException: Cannot create table 'my_table'. The associated location
'/mnt/.../my_table' is not empty.
```

**Root Cause**

Two distinct cases:

1. **Catalog entry exists** but you're calling `CREATE TABLE` (not `CREATE OR REPLACE`).
2. **Path is non-empty** but no table is registered (most often after a manual `DROP TABLE` that didn't delete files).

**Fix**

For case 1 — use idempotent DDL or odibi's first-run gate:

```python
# Idempotent
spark.sql("CREATE TABLE IF NOT EXISTS my_table USING DELTA LOCATION ...")

# Or destructive replace (drops existing data)
spark.sql("CREATE OR REPLACE TABLE my_table USING DELTA AS SELECT ...")
```

For case 2 — clean the path or register the existing data:

```python
# Option A: register existing files as the table
spark.sql(f"CREATE TABLE my_table USING DELTA LOCATION '{path}'")

# Option B: delete the orphaned path (irreversible)
dbutils.fs.rm(path, recurse=True)
```

In odibi YAML, the write step handles both cases when `mode: overwrite`:

```yaml
write:
  format: delta
  mode: overwrite
  connection: adls_silver
  path: silver/my_table
```

**Prevention**

- Always `DROP TABLE ... PURGE` (or follow with `dbutils.fs.rm`) when permanently removing a table — naked `DROP TABLE` in Unity Catalog leaves the path behind.
- Use `spark.catalog.tableExists(name)` for existence checks. **Do NOT use `try: spark.table(name)`** — on Spark Connect it is lazy and will silently report "exists" for missing tables (see [Lessons Learned T-020](../LESSONS_LEARNED.md#t-020-spark-connect-lazy-evaluation--sparktable-and-deltatableforname-dont-throw-for-missing-tables)).

---

## 4. Null Type Columns (Reject on Write)

**Symptom**

```text
SchemaMismatchError: Invalid data type for Delta Lake: Null
```

```text
DeltaInvalidArgumentException: Found columns with NullType: ['environment'].
NullType is not supported in Delta Lake.
```

**Root Cause**

Pandas DataFrames with all-`None` columns produce a `Null`-type Arrow column. Delta Lake refuses to write `Null` types because it cannot reason about the schema. Most common in:
- State backend writes (`environment=None` in HWM rows).
- Test fixtures using `pd.DataFrame({"col": [None, None]})`.
- Optional metadata columns that happen to be all-empty for a small batch.

**Fix**

Use `pyarrow.table` with **explicit types** when seeding Delta data:

```python
import pyarrow as pa
from deltalake import write_deltalake

table = pa.table({
    "id":          pa.array([1, 2, 3], pa.int64()),
    "environment": pa.array([None, None, None], pa.string()),  # typed string, NOT Null
})
write_deltalake(path, table, mode="overwrite")
```

For odibi state backends, **always provide a non-None `environment`**:

```python
backend = CatalogStateBackend(...)
backend.set_hwm("pipeline_a", "node_b", value=42, environment="dev")  # not None
```

**Prevention**

- Use `pa.table(..., schema=pa.schema([...]))` for Delta seeds in tests — never `pd.DataFrame` with all-`None` cols.
- Cast in odibi YAML before writing:

```yaml
transform:
  - type: cast
    columns:
      environment: "string"
```

See [Lessons Learned T-004](../LESSONS_LEARNED.md#t-004-delta-lake-rejects-null-type-columns) and [P-004](../LESSONS_LEARNED.md#p-004-delta-table-seeding-no-null-types).

---

## 5. VACUUM and File Retention

**Symptom**

```text
IllegalArgumentException: requirement failed: Are you sure you would like to vacuum
files with such a low retention period? If you have writers that are currently
writing to this table, there is a risk that you may corrupt the state of your
Delta table. ... Set spark.databricks.delta.retentionDurationCheck.enabled = false
```

Or — files don't actually get deleted after VACUUM:

```text
Found 0 files and directories in 0.05 seconds.
```

**Root Cause**

- Default minimum retention is **168 hours (7 days)** to protect time travel and concurrent readers.
- VACUUM only deletes files **older than retention AND no longer referenced** by the current Delta log.
- If your table was just written to, every file is referenced — VACUUM won't free anything.

**Fix**

For storage cleanup with the default safety:

```sql
VACUUM eaai_dev.hardening_scratch.my_table RETAIN 168 HOURS;
```

For aggressive cleanup (only when no concurrent writers are active and you accept losing time-travel for older versions):

```python
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql("VACUUM my_table RETAIN 1 HOURS")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
```

In odibi, the engine exposes a maintenance helper — prefer it over raw SQL because it handles the safety toggle and logs the file count:

```python
engine.maintain_table("eaai_dev.hardening_scratch.my_table", vacuum_hours=168)
```

**Prevention**

- Set `delta.deletedFileRetentionDuration` table property to align with your time-travel SLA:
  ```sql
  ALTER TABLE my_table SET TBLPROPERTIES (
    'delta.deletedFileRetentionDuration' = 'interval 7 days',
    'delta.logRetentionDuration'         = 'interval 30 days'
  );
  ```
- Schedule VACUUM weekly via Databricks job, not ad-hoc — predictable storage cost, predictable time-travel window.

---

## 6. Restore to a Previous Version

**Symptom**

A bad MERGE corrupted rows, or you accidentally overwrote with the wrong schema. You need to roll back without restoring from backup.

**Fix**

Inspect history:

```sql
DESCRIBE HISTORY eaai_dev.hardening_scratch.my_table;
```

Identify the version *before* the bad operation, then RESTORE:

```sql
RESTORE TABLE eaai_dev.hardening_scratch.my_table TO VERSION AS OF 42;
-- or
RESTORE TABLE my_table TO TIMESTAMP AS OF '2026-04-30 10:00:00';
```

In odibi, the engine exposes this directly:

```python
engine.restore_table("eaai_dev.hardening_scratch.my_table", version=42)
```

**Important constraints**

- RESTORE creates a **new commit** (the previous version's data, written forward). Old versions remain in history.
- You can only restore to a version still inside the `delta.logRetentionDuration` window (default 30 days). Beyond that, the JSON log entries are gone.
- After RESTORE, downstream incremental readers (CDC, structured streaming) will see deletes/inserts and reprocess — plan accordingly.

**Prevention**

- Take a snapshot before risky operations:
  ```sql
  CREATE TABLE my_table_snapshot DEEP CLONE my_table;
  ```
- Run dry-runs of MERGE in a test catalog before production.
- Set `delta.logRetentionDuration = 'interval 30 days'` minimum for production tables — gives a real recovery window.

---

## 7. Time Travel Queries

**Symptom**

```text
DeltaTimeTravelException: The provided timestamp ('2026-04-29 12:00:00')
is before the earliest version available to this table.
```

```text
VersionNotFoundException: Cannot time travel Delta table to version 12.
Available versions: [42, 99].
```

**Root Cause**

Time travel requires the underlying parquet files **and** the Delta log entry for that version. Either can disappear:
- VACUUM removed parquet files older than retention.
- Log was checkpointed and old JSON entries pruned beyond `delta.logRetentionDuration`.

**Fix**

Read by version:

```python
df = spark.read.format("delta").option("versionAsOf", 42).table("my_table")
```

Read by timestamp:

```python
df = spark.read.format("delta")\
    .option("timestampAsOf", "2026-04-30T10:00:00.000Z")\
    .table("my_table")
```

In odibi YAML:

```yaml
read:
  format: delta
  connection: adls_silver
  path: silver/my_table
  options:
    versionAsOf: 42
```

If the version is gone, the only recovery is from upstream re-ingestion or a backup. There is no "extend retention retroactively" — once VACUUM ran, those bytes are gone.

**Prevention**

- Set retention table properties **before** you need them:
  ```sql
  ALTER TABLE my_table SET TBLPROPERTIES (
    'delta.deletedFileRetentionDuration' = 'interval 14 days',
    'delta.logRetentionDuration'         = 'interval 60 days'
  );
  ```
- Never run `VACUUM ... RETAIN 0 HOURS` on a table you intend to time-travel.

---

## 8. Z-ORDER Optimization

**Symptom**

Filtered reads on a large Delta table are slow even though Spark logs report `numFilesAfterFilter`. Or you see `dataFilters: [(customer_id = 12345)]` in the plan but Spark still reads 800 files.

**Root Cause**

Z-ORDER co-locates rows with similar values in the same parquet files. Without it, filtered queries scan many partitions because the filter column's values are scattered. Z-ORDER is most effective on **high-cardinality columns frequently used in filters** (customer_id, sku, machine_id) — not on partition columns.

**Fix**

```sql
OPTIMIZE eaai_dev.hardening_scratch.fact_orders
  ZORDER BY (customer_id);
```

For multiple columns (max 4 — diminishing returns beyond 2):

```sql
OPTIMIZE fact_orders ZORDER BY (customer_id, sku);
```

In odibi pipeline config, enable post-write maintenance:

```yaml
write:
  format: delta
  mode: append
  partition_by: [event_date]
  zorder_by: [customer_id]
  maintenance:
    optimize: true
    zorder_after_write: true
```

**Prevention rules of thumb**

- Z-ORDER columns are ones that appear in `WHERE` clauses, not `GROUP BY` keys (those benefit from partitioning).
- Don't Z-ORDER on the partition column — it's already co-located by definition.
- Re-run OPTIMIZE + ZORDER after large writes (≥ 10% of table). Skipping it leaves new files un-clustered and degrades read performance over time.
- For very small tables (< 1 GB), OPTIMIZE alone is enough; Z-ORDER overhead exceeds its benefit.

---

## 9. pyarrow / deltalake Version Conflicts

**Symptom**

```text
ImportError: deltalake 1.x requires pyarrow >= 18.0.0
```

```text
TypeError: write_deltalake() got an unexpected keyword argument 'overwrite_schema'
```

```text
TypeError: write_deltalake() got an unexpected keyword argument 'schema_mode'
```

**Root Cause**

The `deltalake` Python package has breaking changes between 0.x and 1.x:

| API | deltalake 0.x | deltalake 1.x |
|---|---|---|
| Schema override | `schema_mode="overwrite"` | `overwrite_schema=True` |
| Engine kwarg | `engine="rust"` accepted | removed (Rust is sole engine) |
| pyarrow floor | `>=14.0.0` | `>=18.0.0` |

odibi pins `deltalake>=0.18.0,<0.30.0` (resolves to 0.25.5 with `pyarrow<17.0.0,>=14.0.0`). Databricks clusters often pre-install `deltalake==1.5.1` and `pyarrow>=18`, which conflict with odibi's pins.

**Fix**

In a Databricks notebook running odibi:

```python
%pip install "deltalake>=0.18.0,<0.30.0" "pyarrow<17.0.0,>=14.0.0"
%restart_python
```

**Always use `schema_mode="overwrite"` (the 0.x API)** in odibi-bound code — never `overwrite_schema=True`:

```python
from deltalake import write_deltalake
write_deltalake(path, table, mode="overwrite", schema_mode="overwrite")
```

**Prevention**

- Pin `deltalake` and `pyarrow` in any Databricks notebook used with odibi (see [Lessons Learned V-010](../LESSONS_LEARNED.md#v-010-deltalake-0x-vs-1x-api-differences)).
- Run `pip show deltalake pyarrow` at the top of every notebook session to verify versions before troubleshooting.
- Add a smoke test to your campaign notebook: `from deltalake import write_deltalake; write_deltalake.__module__` — if the signature differs, you're on the wrong version.

---

## 10. Databricks vs Local Delta Behavior Differences

Delta Lake on a Databricks cluster (Spark + Delta Java JARs) and Delta Lake locally (the `deltalake` Python crate via PyO3) implement the same on-disk format, but their behaviors diverge in ways that bite tests:

| Behavior | Databricks (Spark + JVM) | Local (`deltalake` Python) |
|---|---|---|
| `spark.table(name)` / `DeltaTable.forName()` | **Lazy** on Spark Connect — does not throw for missing tables. Use `spark.catalog.tableExists()`. | N/A (uses `DeltaTable(path)`) |
| MERGE statement | Full SQL MERGE, all clauses | No MERGE — must read, diff, overwrite |
| `eqNullSafe()` for NaN comparison | NaN == NaN ✅ (used in odibi SCD2 — see [T-009](../LESSONS_LEARNED.md#t-009-scd2-floatnan-comparison--resolved-on-spark)) | NaN != NaN ❌ — manual handling required |
| Schema evolution | `mergeSchema = true` works for ADD; for type changes use column mapping | `schema_mode='overwrite'` only — must rewrite whole table |
| Z-ORDER | `OPTIMIZE ... ZORDER BY` | Not implemented in 0.25.x |
| Time travel | versionAsOf / timestampAsOf both supported | versionAsOf only (timestampAsOf in 1.x+) |
| Concurrent writers | Optimistic concurrency, automatic retry on append conflicts | No concurrency primitives — use external lock |

**Fix**

- Code paths that must work in **both** environments (e.g., odibi engines) need separate `_spark` and `_pandas`/`_polars` branches. Don't try to share Delta-specific code.
- For odibi tests:
  - Mock-based tests for Spark branches → **CI** (no JVM, fast).
  - Real-Delta integration tests for SCD2/Merge → **Databricks notebooks**, target `eaai_dev.hardening_scratch`.
  - Local Delta tests (`deltalake` crate) → use `pa.table()` + `write_deltalake(path, ...)`. See [P-004](../LESSONS_LEARNED.md#p-004-delta-table-seeding-no-null-types).

**Prevention**

- Document which engine branch a feature uses. odibi maintains engine parity per the table in [AGENTS.md](../../AGENTS.md#current-coverage-status-april-2026).
- When porting a test from Databricks to local, check the API table above and adjust accordingly.
- Never assume a feature that works in `deltalake` Python works the same way on Databricks (and vice versa). Verify both.

---

## See Also

- [troubleshooting.md](../troubleshooting.md) — General odibi runtime troubleshooting (validation, FK, quality gates, Azure, Spark generally).
- [LESSONS_LEARNED.md](../LESSONS_LEARNED.md) — Persistent gotchas across the codebase.
- [Skill 12: Databricks Notebook Protocol](../skills/12_databricks_notebook_protocol.md) — How to write test notebooks that survive cluster restarts and version drift.
- [Delta Lake official docs](https://docs.delta.io/latest/index.html) — Authoritative reference for the Delta protocol and Python/Spark APIs.
