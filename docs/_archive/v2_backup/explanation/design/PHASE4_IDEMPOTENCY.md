# Phase 4: Idempotency & Write Modes Design

## 🎯 Goal
Implement reliable, idempotent write operations (`upsert`, `append-once`) to prevent data duplication during pipeline retries or re-runs. This is critical for production pipelines where failures are expected.

## 📝 Concepts

### 1. Write Modes
We will extend the existing `mode` parameter in `write()` operations.

| Mode | Behavior | Supported Formats | Engine Support |
|------|----------|-------------------|----------------|
| `overwrite` | Replace entire target with new data. (Existing) | All | All |
| `append` | Add new data to target. duplicates possible. (Existing) | All | All |
| `upsert` | Update existing records if key matches, insert if new. | Delta (Native), SQL (Native), Parquet/CSV (Simulated) | Pandas, Spark |
| `append_once` | Append only if data doesn't already exist (dedup check). | Delta, Parquet, CSV, SQL | Pandas, Spark |

### 2. Configuration
New `write` configuration options:

```yaml
write:
  connection: my_datalake
  format: delta
  path: "silver/customers"
  mode: upsert  # New mode
  keys: ["customer_id"]  # Required for upsert/append_once

  # Optional
  dedup_strategy: "newest_wins" # For duplicate keys in source data
  timestamp_col: "updated_at"   # Used for newest_wins strategy
```

## 🛠️ Implementation Strategy

### A. Pandas Engine

#### 1. Delta Lake (`format: delta`)
Leverage `deltalake` library's `merge` feature (if available) or read-modify-write pattern.
*   **Upsert:** Use `DeltaTable.merge()`.
*   **Append-Once:** Read target IDs -> Filter source -> Append new.

#### 2. File Formats (`format: csv/parquet`)
*Simulated Upsert* is expensive for files (requires full rewrite), so we focus on **Append-Once**.
*   **Upsert:** ⚠️ **Not recommended** for raw files. Raise warning or require `overwrite` of partition.
*   **Append-Once:**
    1. Read existing files (if small enough) or specific ID columns.
    2. Filter source DataFrame `~source.id.isin(target.id)`.
    3. Append result.

#### 3. SQL (`format: sql` - Future)
*   Use `MERGE INTO` or `INSERT ... ON CONFLICT`.

### B. Spark Engine

#### 1. Delta Lake
*   **Upsert:** Standard `MERGE INTO` syntax.
*   **Append-Once:** `LEFT ANTI JOIN` before append.

#### 2. File Formats
*   **Upsert:** `overwrite` mode on partitions.
*   **Append-Once:** Checkpoint logic handles file-level idempotency, but row-level requires `LEFT ANTI JOIN`.

## ⚠️ Challenges & Edge Cases

1.  **Large Data (Pandas):** Reading entire target to check for duplicates is OOM risk.
    *   *Mitigation:* Only support `upsert` on Delta (optimized) or SQL. For Parquet/CSV, `append_once` is best effort or requires partitioning.
2.  **Concurrent Writes:**
    *   Delta handles this via optimistic concurrency control.
    *   Parquet/CSV: No locking. User must ensure single writer.

## ✅ Acceptance Criteria

1.  `mode: upsert` works for Delta tables (Pandas & Spark).
2.  `mode: append_once` works for simple file formats (Pandas).
3.  Pipeline re-run with `upsert` results in consistent state (no duplicates).
