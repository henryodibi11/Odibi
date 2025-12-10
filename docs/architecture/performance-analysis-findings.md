# Odibi Delta Write Performance Analysis

## Executive Summary

The write phase accounts for ~96% of pipeline runtime (~50s for slowest nodes). Analysis identified **8 major bottlenecks** with cumulative potential savings of **30-45 seconds per node**.

---

## Write Path Overview

```
_execute_write_phase() [node.py:1585]
    │
    ├─ skip_if_unchanged check (if enabled)
    │   ├─ compute_spark_dataframe_hash() [content_hash.py:57]
    │   │   └─ _compute_spark_hash_distributed() - xxhash64 + agg
    │   └─ get_content_hash_from_state() - Delta read for lookup
    │
    ├─ schema_policy check (if enabled)
    │   └─ get_table_schema() - Delta table read
    │
    ├─ add_metadata columns (if enabled)
    │   └─ withColumn("_extracted_at", current_timestamp())
    │
    └─ engine.write() [spark_engine.py:717]
        ├─ df.rdd.getNumPartitions() - Spark action!
        ├─ [Delta path-based write]
        │   ├─ writer.save(full_path)
        │   ├─ register_table SQL (CREATE TABLE IF NOT EXISTS)
        │   ├─ _apply_table_properties() - ALTER TABLE per property
        │   ├─ _optimize_delta_write() - OPTIMIZE/ZORDER
        │   └─ _get_last_delta_commit_info() - Delta history read
        │
        └─ _register_catalog_entries() [node.py:1806]
            ├─ register_asset() - Delta append
            ├─ track_schema() - Delta merge
            ├─ log_pattern() - Delta merge
            └─ record_lineage() - Delta append
```

---

## Identified Bottlenecks

### 1. **Repeated Table Existence Checks** ⚠️ HIGH IMPACT

**Location:** Multiple locations throughout write path
- `_execute_write_phase` → `get_table_schema()` [line 1635]
- `spark_engine.write()` → `table_exists()` [line 857]
- `_generate_incremental_sql_filter()` → `table_exists()` [line 662]

**Cost:** ~3-5s per redundant check (Delta table open + limit(0).collect())

**Evidence:**
```python
# spark_engine.py:643-648
def table_exists(self, connection, table=None, path=None):
    if table:
        if not self.spark.catalog.tableExists(table):
            return False
        self.spark.table(table).limit(0).collect()  # Expensive verify
```

**Fix:** Cache existence check result at node level
```python
# Add to NodeExecutor.__init__
self._table_exists_cache: Dict[str, bool] = {}

# In table_exists calls
cache_key = f"{connection}:{table or path}"
if cache_key in self._table_exists_cache:
    return self._table_exists_cache[cache_key]
```
**Expected Savings: 5-10s per node**

---

### 2. **Schema Capture on Every Write** ⚠️ MEDIUM-HIGH IMPACT

**Location:** `_execute_write_phase` lines 1634-1642
```python
if config.schema_policy and df is not None:
    target_schema = self.engine.get_table_schema(  # Delta table read!
        connection=connection,
        table=write_config.table,
        path=write_config.path,
        format=write_config.format,
    )
```

**Cost:** ~2-4s (full Delta table schema read, even for small tables)

**Fix:** Skip schema check if table doesn't exist (first run) or use cached schema
```python
# Only check if target exists and schema_policy requires it
if config.schema_policy and config.schema_policy.mode != SchemaMode.EVOLVE:
    if self._table_exists_cache.get(cache_key):
        target_schema = self.engine.get_table_schema(...)
```
**Expected Savings: 2-4s per node**

---

### 3. **Delta Table Registration on EVERY Append** ⚠️ HIGH IMPACT

**Location:** `spark_engine.py` lines 1050-1061
```python
if register_table:
    create_sql = (
        f"CREATE TABLE IF NOT EXISTS {register_table} "
        f"USING DELTA LOCATION '{full_path}'"
    )
    self.spark.sql(create_sql)  # Runs EVERY write, even for appends
```

**Cost:** ~5-15s (catalog operation + metadata update)

This is the **primary bottleneck** for incremental appends. Running `CREATE TABLE IF NOT EXISTS` on every write is extremely expensive.

**Fix:** Check if table already registered before running SQL
```python
if register_table:
    if not self.spark.catalog.tableExists(register_table):
        create_sql = ...
        self.spark.sql(create_sql)
```
**Expected Savings: 10-20s per incremental write**

---

### 4. **Table Properties Applied Per-Property** ⚠️ MEDIUM IMPACT

**Location:** `spark_engine.py:225-252`
```python
def _apply_table_properties(self, target, properties, is_table=False):
    for prop_name, prop_value in properties.items():
        sql = f"ALTER TABLE {table_ref} SET TBLPROPERTIES ('{prop_name}' = '{prop_value}')"
        self.spark.sql(sql)  # One SQL per property!
```

**Cost:** ~1-2s per property (3-4 properties = 5-8s total)

**Fix:** Batch properties into single ALTER TABLE statement
```python
if properties:
    props_str = ", ".join([f"'{k}' = '{v}'" for k, v in properties.items()])
    sql = f"ALTER TABLE {table_ref} SET TBLPROPERTIES ({props_str})"
    self.spark.sql(sql)
```
**Expected Savings: 3-6s per node with table properties**

---

### 5. **Catalog Entries Written Synchronously** ⚠️ MEDIUM IMPACT

**Location:** `_register_catalog_entries()` [node.py:1806-1910]

Each write triggers 4 separate Delta operations:
1. `register_asset()` - Delta append
2. `track_schema()` - Delta merge (upsert)
3. `log_pattern()` - Delta merge (upsert)
4. `record_lineage()` - Delta append

**Cost:** ~3-8s total (each Delta operation has transaction overhead)

**Fix:** Batch catalog writes or make them async
```python
# Option 1: Batch all catalog writes into single transaction
# Option 2: Fire-and-forget async writes for non-critical metadata
# Option 3: Add config to disable catalog writes for performance-critical pipelines
```
**Expected Savings: 3-6s per node**

---

### 6. **skip_if_unchanged State Lookup Overhead** ⚠️ LOW-MEDIUM IMPACT

**Location:** `_check_skip_if_unchanged()` [node.py:1978-2041]

**State Lookup Path:**
```python
state_backend = getattr(self.state_manager, "backend", None)
previous_hash = get_content_hash_from_state(state_backend, config.name, table_name)
```

This triggers `_get_hwm_spark()` which:
```python
df = self.spark.read.format("delta").load(self.meta_state_path)  # Full table read
row = df.filter(F.col("key") == key).select("value").first()
```

**Cost:** ~2-3s (Delta table open + filter + collect)

The hash computation itself is efficient (distributed xxhash64), but the state lookup reads the entire meta_state table.

**Fix:** Add predicate pushdown or use data skipping
```python
# Use partition pruning or data skipping
df = self.spark.read.format("delta").load(self.meta_state_path)
row = df.filter(F.col("key") == key).select("value").limit(1).first()
```

Or cache recent HWM values in memory during pipeline execution.

**Expected Savings: 1-2s per node with skip_if_unchanged**

---

### 7. **df.rdd.getNumPartitions() Triggers Job** ⚠️ LOW IMPACT

**Location:** `spark_engine.write()` line 764
```python
partition_count = df.rdd.getNumPartitions()  # Spark action!
```

For lazy DataFrames, this can trigger computation of the lineage to determine partitions.

**Cost:** ~0.5-2s (variable based on DAG complexity)

**Fix:** Make partition count optional or use lazy evaluation
```python
try:
    # Quick check if already materialized
    if df.isStreaming:
        partition_count = None
    elif hasattr(df, "_jdf") and df._jdf.queryExecution().isInstanceOf(...):
        partition_count = df.rdd.getNumPartitions()
    else:
        partition_count = None
except:
    partition_count = None
```
**Expected Savings: 0.5-2s per node**

---

### 8. **Delta Commit Metadata Fetched After Write** ⚠️ LOW IMPACT

**Location:** `_get_last_delta_commit_info()` [spark_engine.py:299-348]
```python
dt.history(1).collect()  # Opens Delta table again!
```

**Cost:** ~1-2s (Delta table open + history scan)

**Fix:** Extract commit info from write operation directly if possible, or cache DeltaTable reference.

**Expected Savings: 1-2s per node**

---

## Why Small Tables (31-84 rows) Take 30-50s

The overhead is **constant per write**, not proportional to data volume:

| Operation | Estimated Time |
|-----------|----------------|
| Table existence check | 3-5s |
| Schema capture | 2-4s |
| Delta write itself | 2-5s |
| CREATE TABLE IF NOT EXISTS | 5-15s |
| Table properties (3x) | 3-6s |
| Catalog entries (4x) | 3-8s |
| Delta commit metadata | 1-2s |
| **Total** | **19-45s** |

The actual DataFrame write is only ~2-5s. Everything else is metadata overhead.

---

## Specific Node Analysis

### tblGrindDailyProduction (59 rows, 30-45s)
- Likely has: table registration, schema policy, table properties
- All constant overhead applies regardless of 59 rows

### vwDryerShiftLineProductRunWithDryerOnHours (684 rows, 40s)
- Possibly running OPTIMIZE/ZORDER after write
- Check if `auto_optimize` or `zorder_by` is configured

---

## Recommended Priority Fixes

### Phase 1: Quick Wins (Expected: 15-25s savings)
1. **Skip `CREATE TABLE IF NOT EXISTS` for existing tables** (10-20s)
2. **Batch table properties into single ALTER TABLE** (3-6s)
3. **Cache table existence checks** (5-10s)

### Phase 2: Medium Effort (Expected: 5-10s savings)
4. **Skip schema capture when not needed** (2-4s)
5. **Batch catalog entries** (3-6s)

### Phase 3: Architecture Changes (Expected: 5-10s savings)
6. **Async catalog writes**
7. **State lookup optimization**
8. **Lazy partition count**

---

## Implementation Sketch

### Fix #1: Skip Redundant Table Registration

```python
# spark_engine.py, around line 1050
if register_table:
    # Only register if table doesn't exist in catalog
    if not self.spark.catalog.tableExists(register_table):
        create_sql = (
            f"CREATE TABLE IF NOT EXISTS {register_table} "
            f"USING DELTA LOCATION '{full_path}'"
        )
        self.spark.sql(create_sql)
        ctx.info(f"Registered table: {register_table}", path=full_path)
    else:
        ctx.debug(f"Table {register_table} already registered, skipping")
```

### Fix #2: Batch Table Properties

```python
# spark_engine.py, replace _apply_table_properties
def _apply_table_properties(self, target: str, properties: Dict[str, str], is_table: bool = False):
    if not properties:
        return

    table_ref = target if is_table else f"delta.`{target}`"
    props_list = [f"'{k}' = '{v}'" for k, v in properties.items()]
    props_str = ", ".join(props_list)

    sql = f"ALTER TABLE {table_ref} SET TBLPROPERTIES ({props_str})"
    self.spark.sql(sql)
    ctx.debug(f"Set {len(properties)} table properties in single statement")
```

### Fix #3: Add Table Existence Cache

```python
# node.py, NodeExecutor class
def __init__(self, ...):
    ...
    self._table_exists_cache: Dict[str, bool] = {}

def _cached_table_exists(self, connection, table=None, path=None) -> bool:
    cache_key = f"{id(connection)}:{table}:{path}"
    if cache_key not in self._table_exists_cache:
        self._table_exists_cache[cache_key] = self.engine.table_exists(connection, table, path)
    return self._table_exists_cache[cache_key]
```

---

## Next Steps

1. Implement Fix #1 (register_table skip) - highest ROI
2. Add performance timing logs to validate bottleneck locations
3. A/B test with timing before/after each fix
4. Consider adding a `write.performance_mode: fast` option that skips non-essential metadata operations
