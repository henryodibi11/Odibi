# HWM Implementation Details

## Status: COMPLETE ✅

All phases of the HWM implementation have been successfully completed.

---

## What Was Implemented

### Phase 1: Fixed Blocking Bug ✅
**File**: `odibi/node.py` (line 583)

Added missing `register_table` parameter to `engine.write()` call:

```python
delta_info = self.engine.write(
    df=df,
    connection=connection,
    format=write_config.format,
    table=write_config.table,
    path=write_config.path,
    register_table=write_config.register_table,  # NOW PASSED
    mode=write_config.mode,
    options=write_options,
)
```

### Phase 2: Added `table_exists()` Method to All Engines ✅

#### Base Class (`odibi/engine/base.py`)
```python
@abstractmethod
def table_exists(
    self, connection: Any, table: Optional[str] = None, path: Optional[str] = None
) -> bool:
    """Check if table or location exists."""
    pass
```

#### Spark Engine (`odibi/engine/spark_engine.py`)
```python
def table_exists(self, connection, table=None, path=None):
    if table:
        return self.spark.catalog.tableExists(table)
    elif path:
        from delta.tables import DeltaTable
        full_path = connection.get_path(path)
        return DeltaTable.isDeltaTable(self.spark, full_path)
    return False
```

#### Pandas Engine (`odibi/engine/pandas_engine.py`)
```python
def table_exists(self, connection, table=None, path=None):
    if path:
        full_path = connection.get_path(path)
        return os.path.exists(full_path)
    return False
```

### Phase 3: Added HWM Config to WriteConfig ✅
**File**: `odibi/config.py`

```python
first_run_query: Optional[str] = Field(
    default=None,
    description=(
        "SQL query for full-load on first run (High Water Mark pattern). "
        "If set, uses this query when target table doesn't exist, then switches to incremental. "
        "Only applies to SQL reads."
    ),
)
```

### Phase 4: Implemented HWM Logic in Node ✅
**File**: `odibi/node.py`

#### 4.1: Added `_determine_write_mode()` Helper
```python
def _determine_write_mode(self) -> Optional[WriteMode]:
    """Determine write mode considering HWM first-run pattern."""
    if not self.config.write or not self.config.write.first_run_query:
        return None
    
    write_config = self.config.write
    target_connection = self.connections.get(write_config.connection)
    
    if target_connection is None:
        return None
    
    table_exists = self.engine.table_exists(
        target_connection, table=write_config.table, path=write_config.path
    )
    
    if not table_exists:
        return WriteMode.OVERWRITE
    
    return None
```

#### 4.2: Updated `_execute_read()`
Overrides query with `first_run_query` on first run:

```python
options = read_config.options.copy()
if write_config and write_config.first_run_query:
    target_connection = self.connections.get(write_config.connection)
    if target_connection:
        table_exists = self.engine.table_exists(
            target_connection, table=write_config.table, path=write_config.path
        )
        if not table_exists:
            options["query"] = write_config.first_run_query
```

#### 4.3: Updated `_execute_write_phase()`
Passes write mode override:

```python
actual_mode = self._determine_write_mode()
# ...
self._execute_write(df_to_write, actual_mode)
```

#### 4.4: Updated `_execute_write()`
Accepts override mode parameter:

```python
def _execute_write(self, df: Any, override_mode: Optional[WriteMode] = None) -> None:
    # ...
    mode = override_mode if override_mode is not None else write_config.mode
    # ...
```

---

## How It Works

### First Run (Table Doesn't Exist)
1. `_determine_write_mode()` detects missing table
2. Returns `WriteMode.OVERWRITE`
3. `_execute_read()` uses `first_run_query` (full dataset)
4. `_execute_write()` creates table with OVERWRITE mode
5. Options applied at creation

### Subsequent Runs (Table Exists)
1. `_determine_write_mode()` detects existing table
2. Returns `None` (use configured mode)
3. `_execute_read()` uses normal incremental query
4. `_execute_write()` uses configured mode (APPEND)

---

## Files Modified

| File | Lines Changed | Status |
|------|---------------|--------|
| `odibi/config.py` | +15 | ✅ |
| `odibi/engine/base.py` | +17 | ✅ |
| `odibi/engine/spark_engine.py` | +42 | ✅ |
| `odibi/engine/pandas_engine.py` | +31 | ✅ |
| `odibi/node.py` | +72 | ✅ |
| **Total** | **+177** | **✅** |

---

## Design Decisions

1. **`first_run_query` in WriteConfig**: Semantically correct—write config controls table creation
2. **No `first_run_mode`**: Unnecessary—first run always does OVERWRITE
3. **Options independent**: Liquid Clustering, partitioning work normally
4. **Path-based support**: Works with ADLS without Unity Catalog
5. **Dual-check optimization**: Both read and write phases check existence (defensive)

---

## Next Steps

- Write comprehensive unit tests
- Add integration tests with real Spark/Pandas
- Consider adding `--force-first-run` CLI flag for recovery
