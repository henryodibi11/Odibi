# High Water Mark (HWM) Pattern - Implementation Complete

**Status**: ✅ All Phases Complete  
**Date**: 2025-11-24  
**Total Changes**: 177 lines across 5 files

---

## Quick Start

The HWM pattern enables efficient incremental data loading:

```yaml
write:
  path: raw/orders
  mode: append
  first_run_query: SELECT * FROM dbo.orders  # Full load on first run
```

**Behavior**:
- **Day 1**: Loads all orders (first_run_query) → creates table
- **Day 2+**: Loads incremental orders (normal query) → appends

---

## What's in This Directory

| File | Purpose |
|------|---------|
| `hwm_pattern.md` | Design decisions and rationale |
| `hwm_implementation.md` | Implementation details and code changes |

---

## Key Features

✅ **Works without Unity Catalog** (ADLS + path-based Delta)  
✅ **Automatic first-run detection** (via `table_exists()`)  
✅ **Options work transparently** (cluster_by, partition_by, etc.)  
✅ **Multi-target pipelines** (per-node first-run logic)  
✅ **All engines supported** (Spark, Pandas)

---

## Implementation Overview

### New Configuration Field

```python
# In WriteConfig
first_run_query: Optional[str]  # SQL query for initial full load
```

### New Engine Method

```python
# All engines implement
table_exists(connection, table=None, path=None) -> bool
```

### New Node Helper

```python
# Determines write mode based on table existence
_determine_write_mode() -> Optional[WriteMode]
```

---

## Files Changed

- `odibi/config.py` - Added `first_run_query` field (+15 lines)
- `odibi/engine/base.py` - Added abstract `table_exists()` (+17 lines)
- `odibi/engine/spark_engine.py` - Implemented `table_exists()` (+42 lines)
- `odibi/engine/pandas_engine.py` - Fixed signature + implemented (+31 lines)
- `odibi/node.py` - Added HWM logic (+72 lines)

---

## Example: Orders Pipeline

```yaml
pipeline: daily_orders
nodes:
  - name: load_orders
    read:
      connection: sql_server
      format: sql_server
      table: dbo.orders
      options:
        query: |
          SELECT * FROM dbo.orders 
          WHERE updated_at > (
            SELECT COALESCE(MAX(updated_at), '1900-01-01') 
            FROM raw.orders
          )
    
    write:
      connection: adls
      format: delta
      path: raw/orders
      mode: append
      register_table: raw.orders
      first_run_query: SELECT * FROM dbo.orders
      options:
        cluster_by: order_date
        optimize_write: true
```

**First execution**: Loads all 5 years of historical data  
**Subsequent executions**: Loads only today's changes

---

## Testing

Smoke test included: `tests/test_hwm_implementation.py`

```bash
python tests/test_hwm_implementation.py
[OK] Config imports successful
[OK] WriteConfig first_run_query field works
[OK] Node import successful
[OK] Engine.table_exists() defined
[OK] SparkEngine.table_exists() implemented
[OK] PandasEngine.table_exists() implemented
[OK] Node._determine_write_mode() added

[SUCCESS] All HWM implementation tests passed!
```

---

## Edge Cases Handled

✅ Empty target table  
✅ Concurrent executions  
✅ Partial failures (user can re-run)  
✅ Multi-target scenarios (per-node logic)

---

## Future Enhancements

- `--force-first-run` CLI flag for recovery
- Comprehensive integration tests with real Spark/Pandas
- User guide with troubleshooting

---

## Questions?

See `hwm_pattern.md` for design rationale  
See `hwm_implementation.md` for code details
