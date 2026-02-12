# Engine Parity Report

**Generated:** 2026-02-12  
**Purpose:** Audit public method parity between `pandas_engine.py` and `polars_engine.py`

## Executive Summary

- **Total public methods in pandas_engine:** 25
- **Total public methods in polars_engine:** 21
- **Methods in both engines:** 21
- **Methods only in pandas_engine:** 4
- **Methods only in polars_engine:** 0

**Status:** Polars engine is missing 4 methods that exist in pandas_engine. All methods in polars_engine are also present in pandas_engine.

---

## Methods in Both Engines (21 methods)

These methods have parity across both engines:

1. `anonymize` - Anonymize sensitive data in DataFrame
2. `count_nulls` - Count null values in specified columns
3. `count_rows` - Count total rows in DataFrame
4. `execute_operation` - Execute a parameterized operation on DataFrame
5. `execute_sql` - Execute SQL query on DataFrame
6. `get_delta_history` - Get Delta table version history
7. `get_sample` - Get sample rows from DataFrame
8. `get_schema` - Get schema of DataFrame
9. `get_shape` - Get shape (rows, columns) of DataFrame
10. `get_source_files` - Get source file paths from DataFrame metadata
11. `get_table_schema` - Get schema of external table
12. `harmonize_schema` - Harmonize DataFrame to target schema
13. `maintain_table` - Maintain Delta table (optimize, vacuum, etc.)
14. `materialize` - Materialize DataFrame (e.g., cache, collect)
15. `profile_nulls` - Profile null percentages by column
16. `read` - Read data from various sources
17. `table_exists` - Check if table exists
18. `vacuum_delta` - Vacuum old versions from Delta table
19. `validate_data` - Validate data against rules
20. `validate_schema` - Validate DataFrame schema
21. `write` - Write data to various destinations

---

## Methods Only in pandas_engine (4 methods)

These methods are missing from polars_engine and need to be implemented:

### 1. `add_write_metadata`
**Signature:**
```python
def add_write_metadata(
    self,
    df: pd.DataFrame,
    metadata_config: Any,
    source_connection: Optional[str] = None,
    source_table: Optional[str] = None,
    source_path: Optional[str] = None,
    is_file_source: bool = False,
) -> pd.DataFrame
```

**Purpose:** Add metadata columns to DataFrame before writing (Bronze layer lineage tracking).

**Usage:** Called by `node.py` in `_add_write_metadata()` method for lineage tracking.

**Implementation Status in Other Engines:**
- ✅ Spark: Implemented
- ✅ Base: Abstract method defined
- ❌ Polars: Missing

**Priority:** **HIGH** - Core feature for data lineage and Bronze layer metadata

---

### 2. `filter_coalesce`
**Signature:**
```python
def filter_coalesce(
    self,
    df: pd.DataFrame,
    col1: str,
    col2: str,
    op: str,
    value: Any
) -> pd.DataFrame
```

**Purpose:** Filter using `COALESCE(col1, col2) op value` logic. Automatically casts string columns to datetime for proper comparison.

**Usage:** Used by `node.py` for incremental loading with fallback column logic (e.g., `COALESCE(updated_at, created_at)`).

**Implementation Status in Other Engines:**
- ✅ Spark: Implemented
- ❌ Base: Not defined
- ❌ Polars: Missing

**Priority:** **MEDIUM** - Important for incremental loading with fallback columns

---

### 3. `filter_greater_than`
**Signature:**
```python
def filter_greater_than(
    self,
    df: pd.DataFrame,
    column: str,
    value: Any
) -> pd.DataFrame
```

**Purpose:** Filter DataFrame where `column > value`. Automatically casts string columns to datetime for proper comparison.

**Usage:** Used by `node.py` for incremental loading based on watermark columns.

**Implementation Status in Other Engines:**
- ✅ Spark: Implemented
- ❌ Base: Not defined
- ❌ Polars: Missing

**Priority:** **MEDIUM** - Important for incremental loading functionality

---

### 4. `restore_delta`
**Signature:**
```python
def restore_delta(
    self,
    connection: Any,
    path: str,
    version: int
) -> None
```

**Purpose:** Restore Delta table to a specific version (time travel).

**Usage:** Delta table version restoration for rollback scenarios.

**Implementation Status in Other Engines:**
- ✅ Spark: Implemented
- ❌ Base: Not defined
- ❌ Polars: Missing

**Priority:** **LOW** - Delta time travel feature, less commonly used than read/write operations

---

## Methods Only in polars_engine (0 methods)

No methods exist exclusively in polars_engine. All polars methods are also present in pandas_engine.

---

## Additional Findings

### Spark Engine Unique Methods
The Spark engine has one method not present in either Pandas or Polars:
- `execute_transform` - Executes transformer operations (specific to Spark implementation)

### Pandas/Polars Missing from Spark
The following method exists in Pandas and Polars but not in Spark:
- `materialize` - Spark doesn't need explicit materialization as it handles caching differently

---

## Recommended Implementation Priority

### Priority 1: HIGH (Must Have)
1. **`add_write_metadata`** - Core feature for data lineage, Bronze layer metadata, and write operations
   - Used by `node.py` in production workflows
   - Required for metadata tracking consistency across engines
   - Blocking feature for users who need to switch from pandas to polars engine

### Priority 2: MEDIUM (Should Have)
2. **`filter_coalesce`** - Important for incremental loading with fallback columns
   - Used in incremental loading patterns
   - Enables COALESCE-based watermark logic
   - Can be worked around with custom SQL but reduces usability

3. **`filter_greater_than`** - Important for incremental loading based on watermarks
   - Used in incremental loading patterns
   - Simpler than `filter_coalesce` but still valuable
   - Can be worked around with Polars filter expressions

### Priority 3: LOW (Nice to Have)
4. **`restore_delta`** - Delta time travel feature
   - Used in recovery/rollback scenarios
   - Less commonly used in day-to-day workflows
   - Requires Delta Lake library integration

---

## Implementation Notes

### For `add_write_metadata`:
- Must support WriteMetadataConfig or `True` (defaults)
- Add columns: `_extracted_at`, `_source_file`, `_source_connection`, `_source_table`
- Polars equivalent: Use `pl.lit()` to add constant columns
- Timestamp: Use `pl.lit(datetime.now())`

### For `filter_coalesce`:
- Polars has native `coalesce()` expression
- Datetime casting: Use `pl.col().cast(pl.Datetime)`
- Comparison operators: Use `.gt()`, `.lt()`, `.eq()`, etc.

### For `filter_greater_than`:
- Polars filter: `df.filter(pl.col(column) > value)`
- Datetime casting: Check dtype and cast if needed
- Error handling: Polars raises on invalid casts

### For `restore_delta`:
- Requires `deltalake` Python library
- Similar implementation to pandas (uses DeltaTable.restore())
- Test coverage exists in `tests/test_delta_pandas.py`

---

## Testing Recommendations

When implementing missing methods in polars_engine:

1. **Unit Tests:** Create `tests/unit/test_polars_<method>.py` following the pattern in `tests/unit/test_delete_detection.py`
2. **Integration Tests:** Add polars variants to existing integration tests
3. **Parity Tests:** Create tests that verify identical behavior across pandas and polars engines
4. **Edge Cases:** Test datetime string parsing, null handling, missing columns

---

## Conclusion

The polars_engine is 84% feature-complete compared to pandas_engine (21 of 25 methods). The 4 missing methods fall into two categories:

1. **Critical for production use:** `add_write_metadata` (metadata/lineage tracking)
2. **Important for incremental loading:** `filter_coalesce`, `filter_greater_than`
3. **Nice to have:** `restore_delta` (Delta time travel)

Implementing `add_write_metadata` should be the top priority to ensure polars_engine can serve as a drop-in replacement for pandas_engine in production pipelines with proper lineage tracking.
