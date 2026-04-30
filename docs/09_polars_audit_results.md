# Polars Engine Parity Audit — GitHub Issue #212

**Date:** 2026-04-30  
**Auditor:** Automated deep scan of all odibi/ source files  
**Scope:** Every function in transformers/, patterns/, validation/, engine/ that dispatches on engine_type  
**Method:** Read every .py file, traced every `engine_type ==` and `isinstance(context, ...)` dispatch  

---

## Executive Summary

| Category | Count |
| --- | --- |
| Total functions with engine dispatch | 91 |
| Functions with Polars support (explicit) | 9 |
| Functions with Polars support (implicit via SQL) | 24 |
| **Functions missing Polars branch** | **58** |
| CRITICAL gaps | 23 |
| MEDIUM gaps | 25 |
| LOW gaps | 10 |

**Engine health:** polars_engine.py itself is well-implemented (read/write/SQL/anonymize/harmonize/validate). The gaps are in the **consumer code** (transformers, patterns, validation) that dispatches on engine_type but only branches for Spark and Pandas.

---

## What Works Today

### Polars Engine (polars_engine.py) — 1,336 lines, fully functional
- read() — csv, parquet, json, delta, excel (via Pandas), api, sql, simulation
- write() — csv, parquet, json, delta, sql
- execute_sql(), execute_operation()
- anonymize() — hash, mask, redact, fake methods
- harmonize_schema(), validate_schema(), validate_data()
- count_rows(), count_nulls(), profile_nulls(), get_sample()
- filter_greater_than(), filter_coalesce(), add_write_metadata()
- Delta maintenance: vacuum, history, restore, maintain_table
- Only NotImplementedError: SQL MERGE for non-MSSQL (same as Pandas engine)

### SQL-First Transformers — 24 functions, implicit Polars support via context.sql()
These use `context.sql()` without engine branching. Polars SQL handles them:
- **sql_core.py:** filter_rows, derive_columns, cast_columns, clean_text, extract_date_parts, normalize_schema, sort, limit, distinct, fill_nulls, date_add, date_trunc, case_when, concat_columns, select_columns, add_prefix, add_suffix, trim_whitespace
- **advanced.py:** deduplicate (uses EXCLUDE for Polars), regex_replace, validate_and_flag, window_calculation
- **relational.py:** aggregate, join (SQL fallback for non-Pandas), union (SQL path)

### Explicit Polars Implementations — 9 functions
- **manufacturing.py:** detect_sequential_phases → _detect_phases_polars (native)
- **thermodynamics.py:** fluid_properties → _fluid_properties_polars (via Pandas conversion), psychrometrics → _psychrometrics_polars
- **units.py:** convert_units → _convert_column_polars (native)
- **validation/engine.py:** _validate_polars — all 11 test types (NOT_NULL, UNIQUE, ACCEPTED_VALUES, ROW_COUNT, RANGE, REGEX_MATCH, CUSTOM_SQL, SCHEMA, FRESHNESS)
- **validation/quarantine.py:** _evaluate_test_mask — partial (4 of 6 test types)

### SQL-Dispatched with Implicit Polars Fallback — 9 functions
These branch for Spark vs DuckDB SQL syntax, with `else` using DuckDB-compatible SQL that Polars also understands:
- **sql_core.py:** sample, split_part, date_diff, convert_timezone, drop_columns, rename_columns, normalize_column_names, coalesce_columns, replace_values

---

## CRITICAL Gaps — Core DWH Functionality Missing

| Module | Function | Pd | Sp | Pl | Gap Description |
| --- | --- | --- | --- | --- | --- |
| transformers/scd.py | scd2() | ✅ | ✅ | ❌ | Raises ValueError: "does not support engine type" |
| transformers/merge_transformer.py | merge() | ✅ | ✅ | ❌ | Raises ValueError: isinstance checks SparkContext/PandasContext only |
| patterns/scd2.py | execute() | ✅ | ✅ | ❌ | Checks engine_type=="spark", else falls to Pandas path silently |
| patterns/merge.py | execute() | ✅ | ✅ | ❌ | Checks engine_type=="spark", else falls to Pandas path silently |
| patterns/dimension.py | _get_max_sk() | ✅ | ✅ | ❌ | Spark-specific, else Pandas path |
| patterns/dimension.py | _generate_surrogate_keys() | ✅ | ✅ | ❌ | Spark-specific, else Pandas path |
| patterns/dimension.py | _execute_scd0() | ✅ | ✅ | ❌ | Spark-specific, else Pandas path |
| patterns/dimension.py | _execute_scd1() | ✅ | ✅ | ❌ | Spark-specific with Pandas fallback |
| patterns/dimension.py | _execute_scd2() | ✅ | ✅ | ❌ | Spark-specific, else Pandas path |
| patterns/dimension.py | _ensure_unknown_member() | ✅ | ✅ | ❌ | Spark-specific, else Pandas path |
| patterns/fact.py | _deduplicate() | ✅ | ✅ | ❌ | Spark-specific, no fallback |
| patterns/fact.py | _union_dataframes() | ✅ | ✅ | ❌ | Spark-specific, no fallback |
| patterns/fact.py | _get_dimension_df() | ✅ | ✅ | ❌ | Spark-specific, no fallback |
| patterns/fact.py | _join_dimension() | ✅ | ✅ | ❌ | Spark path + Pandas fallback |
| patterns/fact.py | _add_calculated_measure() | ✅ | ✅ | ❌ | Spark-specific, no fallback |
| patterns/fact.py | _rename_column() | ✅ | ✅ | ❌ | Spark-specific, no fallback |
| patterns/fact.py | _validate_grain() | ✅ | ✅ | ❌ | Spark-specific, no fallback |
| patterns/fact.py | _write_quarantine() | ✅ | ✅ | ❌ | Spark path + Pandas fallback |
| transformers/delete_detection.py | detect_deletes (all 6 fns) | ✅ | ✅ | ❌ | Falls through to Pandas for non-Spark |
| validation/fk.py | validate_relationship() | ✅ | ✅ | ❌ | Falls through to Pandas via _validate_pandas |
| validation/fk.py | get_orphan_records() | ✅ | ✅ | ❌ | Spark/Pandas only |
| validation/fk.py | validate_fk_on_load() | ✅ | ✅ | ❌ | Spark-specific |

**Impact:** Any Polars pipeline using SCD2, Merge, Dimension, Fact, or FK validation will either crash (ValueError) or silently receive Pandas-engine behavior (wrong engine semantics, potential type mismatches).

---

## MEDIUM Gaps — Feature Functions with Explicit Engine Dispatch

| Module | Function | Pd | Sp | Pl | Gap Description |
| --- | --- | --- | --- | --- | --- |
| patterns/aggregation.py | _aggregate() | ✅ | ✅ | ❌ | Falls through to Pandas in else branch |
| patterns/aggregation.py | _merge_replace() | ✅ | ✅ | ❌ | Spark-specific, no fallback |
| patterns/aggregation.py | _merge_sum() | ✅ | ✅ | ❌ | Spark-specific, no fallback |
| patterns/aggregation.py | _merge_min() | ✅ | ✅ | ❌ | Spark-specific, no fallback |
| patterns/aggregation.py | _merge_max() | ✅ | ✅ | ❌ | Spark-specific, no fallback |
| patterns/base.py | _get_row_count() | ✅ | ✅ | ❌ | Spark-specific, else uses .shape |
| patterns/base.py | _load_existing_target() | ✅ | ✅ | ❌ | Spark/Pandas dispatch |
| patterns/base.py | _add_audit_columns() | ✅ | ✅ | ❌ | Spark-specific, else Pandas |
| patterns/date_dimension.py | execute() | ✅ | ✅ | ❌ | Spark path, else Pandas fallback |
| patterns/date_dimension.py | _get_row_count() | ✅ | ✅ | ❌ | Spark-specific |
| patterns/date_dimension.py | _add_unknown_member() | ✅ | ✅ | ❌ | Spark-specific |
| transformers/advanced.py | explode_list_column() | ✅ | ✅ | ❌ | Raises ValueError |
| transformers/advanced.py | dict_based_mapping() | ✅ | ✅ | ❌ | Raises ValueError |
| transformers/advanced.py | unpack_struct() | ✅ | ✅ | ❌ | Raises ValueError |
| transformers/advanced.py | hash_columns() | ✅ | ✅ | ❌ | Raises ValueError |
| transformers/advanced.py | safe_col() (x2) | ✅ | ✅ | ❌ | Spark-specific path only |
| transformers/advanced.py | parse_json() | ❌ | ✅ | ❌ | Spark-only (from_json) |
| transformers/relational.py | pivot() | ✅ | ✅ | ❌ | Raises ValueError |
| transformers/relational.py | unpivot() | ✅ | ✅ | ❌ | Spark/Pandas only, no else branch |
| validation/quarantine.py | _evaluate_test_mask UNIQUE | ✅ | ✅ | ❌ | Polars mask missing UNIQUE test |
| validation/quarantine.py | _evaluate_test_mask CUSTOM_SQL | ✅ | ✅ | ❌ | Polars mask missing CUSTOM_SQL test |

---

## LOW Gaps — Domain-Specific or Rarely Used

| Module | Function | Pd | Sp | Pl | Gap Description |
| --- | --- | --- | --- | --- | --- |
| transformers/advanced.py | normalize_json() | ✅ | ✅ | ❌ | Raises ValueError |
| transformers/advanced.py | sessionize() | ✅ | ✅ | ❌ | Raises ValueError |
| transformers/advanced.py | _split_by_day() | ✅ | ✅ | ❌ | Spark/Pandas only |
| transformers/advanced.py | _split_by_hour() | ✅ | ✅ | ❌ | Spark/Pandas only |
| transformers/advanced.py | _split_by_shift() | ✅ | ✅ | ❌ | Spark/Pandas only |
| semantics/materialize.py | execute_incremental() | ✅ | ✅ | ❌ | Spark-specific |
| semantics/materialize.py | _merge_results() | ✅ | ✅ | ❌ | Spark path + Pandas fallback |
| semantics/query.py | execute() | ✅ | ✅ | ❌ | Spark-specific |
| testing/fixtures.py | generate_sample_data() | ✅ | ✅ | ❌ | No Polars generator |
| node.py | _check_skip_if_unchanged() | ✅ | ✅ | ❌ | Falls through to Pandas |

---

## Gap Patterns

### Pattern 1: Explicit ValueError (8 functions)
These crash immediately on Polars:
```
scd2(), merge(), explode_list_column(), dict_based_mapping(),
hash_columns(), pivot(), normalize_json(), sessionize()
```

### Pattern 2: Silent Pandas Fallback (35 functions)
These check `engine_type == SPARK`, else fall through to Pandas logic. Polars DataFrames hit the Pandas path, which will fail with AttributeError or produce wrong results:
```
All patterns/dimension.py, patterns/fact.py, patterns/scd2.py,
patterns/merge.py, patterns/aggregation.py, patterns/base.py,
delete_detection.py, validation/fk.py
```

### Pattern 3: Missing Mask Branch (2 functions)
Quarantine _evaluate_test_mask returns `pl.lit(True)` for UNIQUE and CUSTOM_SQL, silently passing all rows:
```
validation/quarantine.py: UNIQUE, CUSTOM_SQL
```

---

## Recommended Fix Priority

### Phase 1: Stop the Crashes (Week 1)
Fix the 8 functions that raise ValueError. Minimum: add Polars branches that convert to Pandas and back (shim pattern), then optimize later.

### Phase 2: Fix Silent Fallbacks in Patterns (Week 2-3)
Add explicit Polars branches (or EngineType.POLARS checks) to all 6 patterns. These are the most dangerous because they silently produce wrong results.

### Phase 3: Quarantine + FK Validation (Week 3)
Add UNIQUE and CUSTOM_SQL masks to quarantine. Add Polars branch to FK validation.

### Phase 4: Low-Priority Functions (Week 4+)
Sessionize, split_events, semantics, testing fixtures.

---

## Appendix: Files with Full Polars Parity

These files already handle all three engines correctly:

| File | Polars Method |
| --- | --- |
| engine/polars_engine.py | Native implementation (1,336 lines) |
| transformers/manufacturing.py | _detect_phases_polars (native) |
| transformers/thermodynamics.py | _fluid_properties_polars, _psychrometrics_polars (via Pandas) |
| transformers/units.py | _convert_column_polars (native) |
| validation/engine.py | _validate_polars (all 11 test types) |
| 24 SQL-first functions | Implicit via context.sql() |
