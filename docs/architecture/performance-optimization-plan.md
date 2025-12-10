# Pipeline Performance Optimization Plan

## Current State Analysis

**Bronze Pipeline**: 55.10s for 35 nodes (as of Dec 2024)
- Write phase: 53.01s (96.2%)
- Read phase: 1.91s (3.5%)
- Transform phase: 1.33s (2.4%)

**Previous State** (before optimization): 75.95s for 35 nodes

**Identified Bottlenecks**:

### 1. Slow SQL Source Reads

**Problem**: Several nodes show extremely slow row rates:
| Node | Duration | Rows | Rate |
|------|----------|------|------|
| `nkcmfgproduction_vwDryerShiftLineProductRunWithDryerOnHours` | 54.98s | 684 | 12 rows/s |
| `nkcmfgproduction_tblGrindDailyProduction` | 40.71s | 59 | 1.4 rows/s |
| `indyProduction_tblDryerDowntime` | 43.68s | 1.6K | 37 rows/s |
| `opsvisdata_vw_ref_annualgoal` | 23.34s | 47 | 2 rows/s |

**Root Cause**: These are SQL Server views with complex underlying joins. The pipeline reads the entire view without incremental filtering, causing full table scans at the source.

**Solution**:
```yaml
# Add incremental config to slow nodes
read:
  connection: sql_source
  format: sql_server
  table: vwDryerShiftLineProductRunWithDryerOnHours
  incremental:
    mode: rolling_window
    column: ModifiedDate  # or appropriate timestamp column
    lookback: 7
    unit: day
```

### 2. Write Phase Dominates Pipeline Time

**Problem**: 80.8% of pipeline time spent in write phase.

**Root Causes**:
1. Many small files creating metadata overhead
2. Delta transaction log operations for each table
3. No coalescing of small DataFrames

**Solutions**:

#### a) Auto-Coalesce Small DataFrames

**Status**: ✅ IMPLEMENTED (Default behavior)

Small DataFrames (< 10K rows) are now automatically coalesced to 1 partition before
Delta writes. This reduces file coordination overhead for incremental loads.

**How it works**:
- Default threshold: 10,000 rows
- Only applies to Delta format writes
- Reduces multiple small files → single file per write

**Override threshold**:
```yaml
write:
  format: delta
  path: bronze/table
  options:
    coalesce_threshold: 5000  # Custom threshold (set to 0 to disable)
```

#### b) Enable Optimized Write
```yaml
write:
  format: delta
  path: bronze/table
  options:
    optimize_write: true  # Let Delta optimize file sizes
```

#### c) Parallel Execution

**Status**: ✅ IN USE

```python
manager.run(pipeline="bronze", parallel=True, max_workers=16)
```

### 3. skip_if_unchanged Hash Computation

**Status**: ✅ IMPLEMENTED

**Problem**: Legacy implementation collected entire DataFrame to driver for hashing.

**Solution**: Distributed hash computation using `xxhash64` (now the default).

**Code Location**: `odibi/utils/content_hash.py:100-122`
```python
def _compute_spark_hash_distributed(df) -> str:
    """Compute hash distributedly using Spark's xxhash64."""
    from pyspark.sql import functions as F

    hash_cols = [F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")) for c in df.columns]
    work_df = df.withColumn("_row_hash", F.xxhash64(*hash_cols))

    result = work_df.agg(
        F.count("*").alias("row_count"),
        F.sum("_row_hash").alias("hash_sum"),
    ).collect()[0]

    row_count = result["row_count"] or 0
    hash_sum = result["hash_sum"] or 0
    combined = f"v2:{row_count}:{hash_sum}:{','.join(sorted(df.columns))}"
    return hashlib.sha256(combined.encode()).hexdigest()
```

**Benefits**:
- No data collection to driver (except 2 scalar values)
- No full sort required (uses commutative sum)
- O(1) memory on driver
- Safe for arbitrarily large DataFrames

### 4. detect_deletes Schema Warnings

**Problem**: Warnings about missing keys in previous version.
```
detect_deletes: Keys ['OEE_EVENT_START', ...] not found in previous version (v3).
Schema may have changed. Skipping delete detection.
```

**Root Cause**: Schema evolution between runs. The previous Delta version has different column names.

**Solution**: Add schema migration handling:
```yaml
transform:
  steps:
    - operation: detect_deletes
      mode: snapshot_diff
      keys: [OEE_EVENT_START, OEE_EVENT_END, Plant, Channel, Asset]
      on_first_run: skip  # Don't error on first run/schema change
      on_schema_change: skip  # Skip if schema incompatible
```

### 5. High Delete Percentage Warnings

**Problem**: 1279% and 3961% deletion thresholds exceeded.

**Root Cause**: Either:
- Wrong keys configured (causing all rows to appear deleted)
- Source data genuinely changed significantly
- First-time comparison against wrong version

**Solution**:
1. Verify key columns are correct primary keys
2. Set reasonable threshold with warning:
```yaml
- operation: detect_deletes
  mode: snapshot_diff
  keys: [correct_key_column]
  max_delete_percent: 50
  on_threshold_breach: warn  # Log warning instead of failing
```

---

## Implementation Priority

| Priority | Fix | Impact | Effort | Status |
|----------|-----|--------|--------|--------|
| 🔴 High | Distributed hash for skip_if_unchanged | Avoid driver OOM, faster | Medium | ✅ Done |
| 🔴 High | Auto-coalesce small DataFrames | Reduce write overhead | Low | ✅ Done |
| 🔴 High | Add incremental to slow SQL sources | 40-50s savings | Low | ✅ Done |
| 🟡 Medium | Fix detect_deletes key configuration | Clean logs, correct behavior | Low | 🔄 Ongoing |
| 🟢 Low | Enable parallel execution | Overlap I/O | Low | ✅ Done |

## Remaining Bottleneck: Delta Append Overhead

The remaining ~30-50s overhead per Delta append to Azure Blob Storage is inherent to the
Delta Lake protocol on cloud storage. Each append requires:
1. Read transaction log from Azure
2. Write new Parquet file(s) to Azure
3. Write new transaction log entry
4. Table registration SQL (if applicable)

**This cannot be significantly reduced further** without architectural changes like:
- Batching multiple tables into fewer writes
- Using a different storage format
- Reducing write frequency

## Quick Wins Applied

1. ✅ **Parallel execution**: `manager.run(pipeline="bronze", parallel=True, max_workers=16)`
2. ✅ **Rolling window incremental** added to slow SQL views
3. ✅ **Distributed hash** now default for skip_if_unchanged
4. ✅ **Auto-coalesce** now default for small DataFrames (< 10K rows)
5. 🔄 **Set on_threshold_breach: warn** for detect_deletes
