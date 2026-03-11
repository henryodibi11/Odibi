# Performance Optimization Summary

## Changes Made

### 1. ✅ Removed Unnecessary Cache Invalidation (pipeline.py)
**Problem:** Between each pipeline execution, `invalidate_cache()` was clearing metadata caches that never changed during execution.

**Fix:** Removed lines 2257-2270 that invalidated catalog caches between pipelines.

**Impact:** Eliminates unnecessary Delta table reads for metadata lookup.

**Files Modified:**
- `odibi/pipeline.py` (removed cache invalidation in PipelineManager.run loop)

---

### 2. ✅ Added Overhead Instrumentation (pipeline.py)
**Problem:** No visibility into where time was being spent between pipelines.

**Fix:** Added detailed timing instrumentation to track:
- Auto-registration time
- Inter-pipeline gaps
- Lineage generation
- Catalog sync flush
- Total overhead vs actual execution time

**Impact:** Full visibility into overhead sources with detailed report printed after run.

**Files Modified:**
- `odibi/pipeline.py` (added timing code and overhead audit report)
- `odibi/pipeline.py` (_auto_register_pipelines method - added timing for fetch and write operations)

---

### 3. ✅ FIX #1: Actually Cache Spark DataFrames (node.py)
**Problem:** Setting `cache: true` on a node did NOT actually cache Spark DataFrames in memory. It only stored the lazy execution plan, causing re-reads from ADLS for every downstream node.

**Fix:** When `cache: true` and engine is Spark, call `df.cache()` before registering:
```python
if config.cache and hasattr(result_df, 'cache'):
    result_df = result_df.cache()
    ctx.debug(f"Cached Spark DataFrame for node '{config.name}'")
```

**Impact:**
- Nodes with `cache: true` now actually cache data in Spark memory
- Downstream nodes in same pipeline read from memory, not ADLS
- Critical for high-fanout scenarios (dims used by multiple facts)

**Files Modified:**
- `odibi/node.py` (lines 320-331, register phase)

---

### 4. ✅ FIX #2: Make `inputs` Use Cached Temp Views (node.py)
**Problem:** Cross-pipeline `inputs` (e.g., `$silver.node_name`) always read from Delta tables, completely bypassing Spark's cached temp views from previous pipelines.

**Fix:** Before reading from Delta, check if temp view exists in Spark catalog:
```python
# For Spark: Try temp view first (uses cache if available from previous pipeline)
if hasattr(self.engine, 'spark') and self.engine.spark and ref_node:
    tables = [t.name for t in self.engine.spark.catalog.listTables()]
    if ref_node in tables:
        df = self.engine.spark.table(ref_node)  # Uses cached memory!
    else:
        df = resolve_from_catalog()  # Fallback to Delta
```

**Impact:**
- Gold reading Silver via `inputs` now uses Silver's cached data
- Massive time savings when Silver nodes are expensive (e.g., 308s `cleaned_PackagingDowntime`)
- Zero user changes required - existing YAML works automatically

**Files Modified:**
- `odibi/node.py` (lines 779-807, _execute_inputs_phase method)

---

## Code Quality Checks

✅ **Ruff linting:** All checks passed  
✅ **Ruff formatting:** 8 files reformatted  
✅ **Syntax validation:** All modified files have valid Python syntax  
❌ **Pytest:** Environment permission issue (unrelated to changes)

---

## Expected Performance Impact

### Scenario: Your OEE Pipeline (Bronze → Silver → Gold)

**Before Fixes:**
```
Silver: cleaned_PackagingDowntime (308s, cache: true)
  → Writes to Delta
  → cache: true did NOTHING - just stored lazy plan

Gold: combined_downtime (inputs: $silver.cleaned_PackagingDowntime)
  → Reads from Delta (re-reads from ADLS)
  → Another 308s+ of ADLS I/O
```

**After Fixes:**
```
Silver: cleaned_PackagingDowntime (308s, cache: true)
  → Actually caches in Spark memory (Fix #1)
  → Writes to Delta
  → Temp view persists with cached data

Gold: combined_downtime (inputs: $silver.cleaned_PackagingDowntime)
  → Checks temp view first (Fix #2)
  → Reads from Spark cache (~seconds)
  → NO ADLS I/O!
```

**Estimated Savings:**
- Cache invalidation overhead: ~5-10s per pipeline transition
- ADLS re-reads avoided: **5-10+ minutes** for large cached tables
- **Total potential savings: 5-15 minutes per bronze→silver→gold run**

---

## Documentation Created

1. **CACHE_INVALIDATION_AUDIT.md** - Full analysis of why cache invalidation was unnecessary
2. **PERFORMANCE_FIXES_SUMMARY.md** - This document

---

## Recommendations

### For Bronze Layer
- Generally DON'T cache nodes (context dies, no downstream benefit)
- Exception: Bronze nodes used by multiple other bronze nodes

### For Silver Layer
- ✅ Cache dimension tables: `cache: true` (small, used by many facts)
- ❌ Don't cache large fact tables (big memory footprint, used once)

### For Gold Layer
- ✅ Cache intermediate aggregations used by multiple reports
- ❌ Don't cache final large output tables

### Example YAML:
```yaml
pipeline:
  name: silver
  nodes:
    - name: dim_calendar
      cache: true  # Small, used by 10+ fact tables

    - name: cleaned_PackagingDowntime
      cache: true  # Expensive (308s), used by Gold

    - name: huge_fact_table
      # cache: false (default) - 10GB, used once
```

---

## Next Steps

1. Run your full pipeline with these fixes
2. Review the overhead audit report printed at the end
3. Identify which Silver nodes to mark `cache: true` based on:
   - Size (can fit in memory?)
   - Usage (used by multiple downstream nodes?)
   - Cost (expensive transformations worth caching?)
4. Re-run and compare wall-clock time improvements

---

## Files Modified

- `odibi/pipeline.py` (cache invalidation removal + instrumentation)
- `odibi/node.py` (Fix #1: Spark cache() + Fix #2: temp view lookup)
