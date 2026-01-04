# Odibi Bugs Found During Stability Campaign

## Summary

| Category | Count | Status |
|----------|-------|--------|
| Python 3.9 Compatibility | 18 | ✅ Fixed |
| Catalog/Delta Schema Issues | 6 | ✅ Fixed |
| Dimension Pattern Bug | 1 | ✅ Fixed |
| Environment/Config Issues | 17 | ⏭️ Skip (not code bugs) |

**Before:** 41 failed, 871 passed
**After:** 17 failed, 895 passed (+24 tests fixed)

---

## Fixed Bugs

### BUG-001: Python 3.9 Type Hint Syntax in prepare_source_tiers.py ✅ FIXED

**File:** `scripts/prepare_source_tiers.py`
**Error:** `TypeError: unsupported operand type(s) for |: 'type' and 'NoneType'`
**Cause:** Uses `str | None` syntax which requires Python 3.10+. Python 3.9 requires `Optional[str]` from typing.
**Fix:** Replaced all Python 3.10+ union syntax (`X | None`, `list[X]`, `dict[X, Y]`) with typing module equivalents (`Optional[X]`, `List[X]`, `Dict[X, Y]`).
**Tests Fixed:** 18 tests

---

### BUG-002: Dimension Pattern Unknown Member Concat Failure ✅ FIXED

**File:** `odibi/patterns/dimension.py:670`
**Error:** `ValueError: all the input array dimensions except for the concatenation axis must match exactly`
**Cause:** When concatenating unknown_member row with data, datetime columns had mismatched array dimensions.
**Fix:** Cast unknown_df columns to match df column dtypes before concatenation.
**Tests Fixed:** 1 test

---

### BUG-003: Catalog log_run Fails with PyArrow Engine ✅ FIXED

**File:** `odibi/catalog.py:1308`
**Error:** `schema_mode 'merge' is not supported in pyarrow engine. Use engine=rust`
**Cause:** Delta Lake write uses `schema_mode='merge'` but pyarrow engine doesn't support it.
**Fix:** Removed `schema_mode='merge'` option from log_run and log_runs_batch. Schema is fixed at bootstrap time, so schema evolution isn't needed for append operations.
**Tests Fixed:** 5 tests

---

### BUG-004: Catalog log_metrics Schema Mismatch ✅ FIXED

**File:** `odibi/catalog.py:2431`
**Error:** `Schema of data does not match table schema` - dimensions column is `list<string>` but table expects `string`
**Cause:** Dimensions list was passed directly but the table schema expected a string (JSON).
**Fix:** Serialize dimensions list to JSON string before writing.
**Tests Fixed:** 1 test (implicit - part of catalog fixes)

---

## Environment Issues (Not Code Bugs)

### ENV-001: WSL CLI Tests - Python Not Found ⏭️ SKIP

**Tests Affected:** 6 tests in `test_cli_integration.py`
**Error:** `FileNotFoundError: [Errno 2] No such file or directory: 'python'`
**Cause:** WSL uses `python3.9` not `python`. CLI tests invoke `python` directly.
**Resolution:** Environment configuration - install `python-is-python3` package or create symlink.

---

### ENV-002: Spark Python Version Mismatch ⏭️ SKIP

**Tests Affected:** `test_spark_real_session`, `test_spark_validation`
**Error:** `PYTHON_VERSION_MISMATCH: Python in worker has different version (3, 8) than that in driver 3.9`
**Cause:** Spark workers use Python 3.8 while driver uses 3.9.
**Resolution:** Set `PYSPARK_PYTHON=python3.9` and `PYSPARK_DRIVER_PYTHON=python3.9`.

---

### ENV-003: Missing SQLAlchemy in WSL ⏭️ SKIP

**Tests Affected:** 9 tests in `test_azure_sql.py`
**Error:** `ModuleNotFoundError: No module named 'sqlalchemy'`
**Cause:** SQLAlchemy not installed in WSL Python environment.
**Resolution:** Run `pip3.9 install sqlalchemy` in WSL.

---

## Final Test Results

### Windows (Python 3.12)
- **Passed:** 958
- **Skipped:** 24
- **Failed:** 0 ✅

### WSL/Linux (Python 3.9)
- **Passed:** 895 (+24 from fixes)
- **Skipped:** 25
- **Failed:** 17 (all environment issues, not code bugs)
  - 6 CLI (python not found)
  - 9 Azure SQL (SQLAlchemy missing)
  - 2 Spark (Python version mismatch)

---

## Documentation Issues Found

### DOC-001: Incorrect `transformer:` Syntax in Dimension/Fact Pattern Docs ✅ FIXED

**Issue:** Tutorial documentation used incorrect syntax for patterns.

**Fix:** Updated all 50+ occurrences across 15 files to use correct `pattern: type: X` syntax.

**Files Fixed:**
- `docs/tutorials/dimensional_modeling/02_dimension_pattern.md`
- `docs/tutorials/dimensional_modeling/03_date_dimension_pattern.md`
- `docs/tutorials/dimensional_modeling/04_fact_pattern.md`
- `docs/tutorials/dimensional_modeling/05_aggregation_pattern.md`
- `docs/tutorials/dimensional_modeling/06_full_star_schema.md`
- `docs/patterns/dimension.md`
- `docs/patterns/fact.md`
- `docs/patterns/aggregation.md`
- `docs/patterns/date_dimension.md`
- `docs/patterns/README.md`
- `docs/semantics/index.md`
- `docs/tutorials/gold_layer.md`
- `docs/validation/fk.md`
- `docs/guides/dimensional_modeling_guide.md`
- `docs/learning/curriculum.md`

**Reference:** See `docs/examples/EXAMPLES_AUDIT.md` for full details.

---

## Files Modified

1. `scripts/prepare_source_tiers.py` - Python 3.9 type hint compatibility
2. `odibi/patterns/dimension.py` - Unknown member row dtype casting
3. `odibi/catalog.py` - Removed schema_mode='merge', added JSON serialization for dimensions
