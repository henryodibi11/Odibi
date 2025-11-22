# Spark Implementation Verification Plan

This plan outlines the steps to ensure Odibi's Spark support (via `SparkEngine`) is as robust and feature-complete as the Pandas implementation (`PandasEngine`).

## 1. Feature Parity Matrix

We will establish `PandasEngine` as the reference implementation and track Spark parity.

| Feature | Pandas (Reference) | Spark | Status | Notes |
|---------|--------------------|-------|--------|-------|
| **Operations** | | | | |
| `pivot` | ✅ Supported | ❌ Missing | 🚧 TODO | Needs implementation in `execute_operation` |
| `drop_duplicates` | ✅ Supported | ✅ Supported | ✅ Parity | |
| `fillna` | ✅ Supported | ✅ Supported | ✅ Parity | |
| `drop` | ✅ Supported | ✅ Supported | ✅ Parity | |
| `rename` | ✅ Supported | ✅ Supported | ✅ Parity | |
| `sort` | ✅ Supported | ✅ Supported | ✅ Parity | |
| `sample` | ✅ Supported | ✅ Supported | ✅ Parity | |
| **I/O** | | | | |
| CSV Read | ✅ Auto-encoding, header bool | ⚠️ Basic | 🚧 Improved | Added auto-encoding; need header bool fix |
| CSV Write | ✅ Supported | ✅ Supported | ✅ Parity | |
| Parquet | ✅ Supported | ✅ Supported | ✅ Parity | |
| JSON | ✅ Supported | ✅ Supported | ✅ Parity | |
| Delta Lake | ✅ upsert/append_once | ✅ upsert/append_once | ✅ Parity | |
| SQL Server | ✅ Supported | ✅ Supported | ✅ Parity | |

## 2. Immediate Action Items

### A. Implement Missing Operations
1. **Pivot**: Implement `pivot` in `SparkEngine.execute_operation`.
   - Map parameters: `group_by`, `pivot_column`, `value_column`, `agg_func`.
   - Use `df.groupBy(...).pivot(...).agg(...)`.

### B. Improve CSV Robustness
1. **Header Handling**: Spark expects "true"/"false" string for `header` option.
   - Odibi YAML uses boolean `true`/`false`.
   - **Action**: Normalize boolean `header` option to string in `SparkEngine.read`.
2. **Auto-Encoding**: (Implemented)
   - Added `auto_encoding: true` support using `odibi.utils.encoding`.

## 3. Systematic Verification Strategy

### A. Cross-Engine Unit Tests
Create a new test suite `tests/engine/test_parity.py` that runs the same operations on both engines and compares results.

1. **Shared Fixtures**:
   - Create standard Pandas DataFrame fixture.
   - Create equivalent Spark DataFrame fixture.

2. **Operation Tests**:
   - For each operation (`pivot`, `sort`, `fillna`, etc.):
     - Run on Pandas Engine -> Result P
     - Run on Spark Engine -> Result S
     - Assert `Result S.toPandas()` equals `Result P` (allowing for minor dtype differences).

3. **I/O Tests**:
   - Write file using Pandas -> Read using Spark -> Verify equality.
   - Write file using Spark -> Read using Pandas -> Verify equality.
   - Test specific CSV edge cases: Latin1 encoding, missing headers.

### B. Pipeline Integration Tests
Create simple pipelines in `tests/data/pipelines/` that use a mix of operations.

1. **Pipeline A (Transform)**: `read csv` -> `drop` -> `rename` -> `sort` -> `write parquet`.
   - Run with `engine: pandas`.
   - Run with `engine: spark`.
   - Verify output files match.

2. **Pipeline B (Delta)**: `read delta` -> `sample` -> `write delta (upsert)`.
   - Verify Delta log and content match expectations for both engines.

## 4. Implementation Schedule

1. **Phase 1 (Done)**:
   - Auto-encoding support for Spark CSV.
   - Fix Story Generator YAML inclusion.

2. **Phase 2 (Next)**:
   - Implement `pivot` in Spark.
   - Fix `header` boolean handling in Spark CSV.
   - Create `tests/engine/test_parity.py` skeleton.

3. **Phase 3**:
   - Fill out parity tests for all operations.
   - Run full systematic check and fix discovered bugs.
