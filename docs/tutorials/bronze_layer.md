# Bronze Layer Tutorial

The **Bronze Layer** is where raw data lands. No transformations, no cleaning—just reliable ingestion with traceability.

## Layer Philosophy

> "Raw is sacred. Preserve everything, trust nothing."

Bronze is your **insurance policy**. If downstream logic has bugs, you can always reprocess from Bronze.

| Principle | Why |
|-----------|-----|
| Append-only | Never lose source data |
| Schema as-is | Don't transform on ingest |
| Full fidelity | Keep all columns, all rows |
| Traceable | Know when each row arrived |

---

## Quick Start: File Ingestion

The simplest Bronze pipeline loads files and appends them:

```yaml
# pipelines/bronze/ingest_orders.yaml
pipelines:
  - pipeline: bronze_orders
    layer: bronze
    nodes:
      - name: raw_orders
        read:
          connection: landing
          format: csv
          path: orders/*.csv
          options:
            header: true
        write:
          connection: bronze
          table: raw_orders
          mode: append
```

---

## Common Problems & Solutions

### 1. "I'm reprocessing files I've already loaded"

**Problem:** Each run loads all files, creating duplicates.

**Solution:** Use stateful incremental tracking with a high-water mark column.

```yaml
nodes:
  - name: raw_orders
    read:
      connection: landing
      format: csv
      path: orders/*.csv
      incremental:
        mode: stateful               # Track HWM (high-water mark)
        column: file_modified_date   # Column to track
    write:
      connection: bronze
      table: raw_orders
      mode: append
```

**How it works:**
- Odibi records the MAX value of `column` after each run
- On next run, only rows with values > stored HWM are processed
- For time-based lookback instead, use `mode: rolling_window`

**See:** [Incremental Loading Pattern](../patterns/incremental_stateful.md)

---

### 2. "Files have inconsistent schemas"

**Problem:** New files have extra/missing columns, breaking the pipeline.

**Solution:** Enable Delta schema evolution on write.

```yaml
nodes:
  - name: raw_orders
    read:
      connection: landing
      format: csv
      path: orders/*.csv
      options:
        header: true
    write:
      connection: bronze
      table: raw_orders
      mode: append
      options:
        mergeSchema: true            # Delta: allow schema evolution
```

**How it works:**
- Spark infers schema from each file
- Delta's `mergeSchema` adds new columns to the target table automatically
- Odibi tracks schema changes in the System Catalog

**See:** [Schema Tracking](../features/schema_tracking.md)

---

### 3. "Malformed records crash the pipeline"

**Problem:** One bad CSV row fails the entire load.

**Solution:** Route bad records to an error path using Spark options.

```yaml
nodes:
  - name: raw_orders
    read:
      connection: landing
      format: csv
      path: orders/*.csv
      options:
        mode: PERMISSIVE             # Don't fail on bad rows
        columnNameOfCorruptRecord: _corrupt_record
        badRecordsPath: /landing/errors/orders/
    write:
      connection: bronze
      table: raw_orders
      mode: append
```

**Result:**
- Valid rows load normally
- Corrupt rows written to `badRecordsPath` for investigation
- Pipeline doesn't fail

---

### 4. "Empty source files break downstream"

**Problem:** Source sends empty files, causing downstream failures.

**Solution:** Add a row count contract.

```yaml
nodes:
  - name: raw_orders
    read:
      connection: landing
      format: csv
      path: orders/*.csv
    contracts:
      - type: row_count
        min: 1                       # Fail if empty
        severity: error
    write:
      connection: bronze
      table: raw_orders
      mode: append
```

**Severity options:**
| Severity | Behavior |
|----------|----------|
| `error` | Fail the node |
| `warn` | Log warning, continue |

**See:** [Contracts](../reference/yaml_schema.md#contracts-data-quality-gates)

---

### 5. "Source volume dropped 90%—something's wrong"

**Problem:** Upstream system broke, sending almost no data.

**Solution:** Add volume drop detection.

```yaml
nodes:
  - name: raw_orders
    read:
      connection: landing
      format: csv
      path: orders/*.csv
    contracts:
      - type: volume_drop
        threshold: 0.5               # Fail if <50% of previous run
        lookback_runs: 3             # Compare to last 3 runs
        severity: error
    write:
      connection: bronze
      table: raw_orders
      mode: append
```

---

### 6. "I need to reprocess a specific date range"

**Problem:** Bug in source data, need to reload specific dates.

**Solution:** Use Delta's partition replacement.

```yaml
nodes:
  - name: raw_orders
    read:
      connection: landing
      format: csv
      path: orders/*.csv
    write:
      connection: bronze
      table: raw_orders
      mode: overwrite
      options:
        replaceWhere: "file_date >= '2025-01-01' AND file_date <= '2025-01-15'"
        partitionBy: [file_date]
```

**How it works:**
- `replaceWhere` only replaces matching partitions
- Rest of the table remains unchanged
- Useful for targeted reloads without full table rebuild

**See:** [Windowed Reprocess Pattern](../patterns/windowed_reprocess.md)

---

### 7. "I'm loading from SQL Server, not files"

**Problem:** Source is a database table, not files.

**Solution:** Use SQL read with stateful incremental.

```yaml
connections:
  source_db:
    type: sql_server
    server: server.database.windows.net
    database: sales
    user: "${DB_USER}"
    password: "${DB_PASSWORD}"

nodes:
  - name: raw_orders
    read:
      connection: source_db
      format: jdbc
      table: dbo.orders
      incremental:
        mode: stateful               # Track HWM
        column: updated_at           # Track by this column
    write:
      connection: bronze
      table: raw_orders
      mode: append
```

**How it works:**
- First run: loads all data, stores MAX(updated_at) as HWM
- Next runs: loads only WHERE updated_at > stored_hwm
- HWM is updated after each successful run

**See:** [Incremental Loading Pattern](../patterns/incremental_stateful.md)

---

## Bronze Layer Checklist

Before moving to Silver, verify:

- [ ] **Append-only?** Raw data is never overwritten (except intentional reprocess)
- [ ] **Incremental?** Only new/changed data is loaded each run
- [ ] **Traceable?** Each row has arrival metadata (`_loaded_at`, source file, etc.)
- [ ] **Contracts?** Row count, schema, or volume checks in place
- [ ] **Error handling?** Bad records routed to error path, not failing pipeline

---

## Next Steps

- [Silver Layer Tutorial](silver_layer.md) — Clean and transform Bronze data
- [Append-Only Raw Pattern](../patterns/append_only_raw.md) — Detailed pattern docs
- [Getting Started](getting_started.md) — End-to-end first pipeline
