# Engine Parity Table

> **Issue:** [#229](https://github.com/invenergy/odibi/issues/229)
> **Generated:** 2026-04-29 (from source inspection, not documentation)
> **Polars Gap Tracking:** [#212](https://github.com/invenergy/odibi/issues/212)

## Legend

| Symbol | Meaning |
| --- | --- |
| ✅ | Implemented and tested |
| ⚠️ | Implemented but untested or mock-only |
| ❌ | Not implemented (raises `ValueError`/`NotImplementedError` or missing branch) |

---

## 1. Transformers × Engine

### SQL Core (`odibi/transformers/sql_core.py`)

All SQL Core transformers use `context.sql()` which dispatches to the active engine's SQL
layer (DuckDB for Pandas, Spark SQL, Polars SQL). Minor dialect adaptations exist
(backtick vs double-quote quoting, `rand()` vs `random()`, `element_at(split(...))` vs
`split_part(...)`).

| Transformer | Pandas | Spark | Polars | Notes |
| --- | --- | --- | --- | --- |
| `filter_rows` | ✅ | ⚠️ | ⚠️ | SQL-based; Spark/Polars untested |
| `derive_columns` | ✅ | ⚠️ | ⚠️ | SQL-based |
| `cast_columns` | ✅ | ⚠️ | ⚠️ | SQL-based |
| `clean_text` | ✅ | ⚠️ | ⚠️ | SQL-based |
| `extract_date_parts` | ✅ | ⚠️ | ⚠️ | SQL-based |
| `normalize_schema` | ✅ | ⚠️ | ⚠️ | SQL-based |
| `sort` | ✅ | ⚠️ | ⚠️ | SQL-based |
| `limit` | ✅ | ⚠️ | ⚠️ | SQL-based |
| `sample` | ✅ | ⚠️ | ⚠️ | Pandas: `random()`, Spark: `rand()`, Polars: untested |
| `distinct` | ✅ | ⚠️ | ⚠️ | SQL-based |
| `fill_nulls` | ✅ | ⚠️ | ⚠️ | SQL-based |
| `split_part` | ✅ | ⚠️ | ⚠️ | Spark: `element_at(split(...))`, DuckDB/Polars: `split_part(...)` |
| `date_add` | ✅ | ⚠️ | ⚠️ | SQL-based |
| `date_trunc` | ✅ | ⚠️ | ⚠️ | SQL-based |
| `date_diff` | ✅ | ⚠️ | ⚠️ | Spark-specific `DATEDIFF` syntax |
| `case_when` | ✅ | ⚠️ | ⚠️ | SQL-based |
| `convert_timezone` | ✅ | ⚠️ | ⚠️ | Spark-specific `from_utc_timestamp` |
| `concat_columns` | ✅ | ⚠️ | ⚠️ | SQL-based |
| `select_columns` | ✅ | ⚠️ | ⚠️ | SQL-based |
| `drop_columns` | ✅ | ⚠️ | ⚠️ | Pandas: `EXCLUDE`, Spark: column list |
| `rename_columns` | ✅ | ⚠️ | ⚠️ | Quoting differs by engine |
| `add_prefix` | ✅ | ⚠️ | ⚠️ | SQL-based |
| `add_suffix` | ✅ | ⚠️ | ⚠️ | SQL-based |
| `normalize_column_names` | ✅ | ⚠️ | ⚠️ | SQL-based |
| `coalesce_columns` | ✅ | ⚠️ | ⚠️ | Pandas branch for EXCLUDE syntax |
| `replace_values` | ✅ | ⚠️ | ⚠️ | Quoting differs by engine |
| `trim_whitespace` | ✅ | ⚠️ | ⚠️ | SQL-based |

### Relational (`odibi/transformers/relational.py`)

| Transformer | Pandas | Spark | Polars | Notes |
| --- | --- | --- | --- | --- |
| `join` | ✅ | ⚠️ | ⚠️ | Pandas: native `merge()`. Spark/Polars: SQL JOIN fallback |
| `union` | ✅ | ⚠️ | ⚠️ | Spark: explicit column alignment. DuckDB/Polars: `UNION ALL BY NAME` |
| `pivot` | ✅ | ⚠️ | ❌ | Spark: native `groupBy().pivot()`. Polars: raises ValueError (#212) |
| `unpivot` | ✅ | ⚠️ | ❌ | Pandas: `pd.melt()`. Spark: native. Polars: raises ValueError (#212) |
| `aggregate` | ✅ | ⚠️ | ⚠️ | Pure SQL via `context.sql()` |

### Advanced (`odibi/transformers/advanced.py`)

| Transformer | Pandas | Spark | Polars | Notes |
| --- | --- | --- | --- | --- |
| `deduplicate` | ✅ | ⚠️ | ⚠️ | SQL-based; Polars-aware (`EXCLUDE` + `ORDER BY 1`) |
| `explode_list_column` | ✅ | ⚠️ | ❌ | Spark: `explode()`. Polars: raises ValueError (#212) |
| `dict_based_mapping` | ✅ | ⚠️ | ❌ | Spark: broadcast join. Polars: raises ValueError (#212) |
| `regex_replace` | ✅ | ⚠️ | ⚠️ | Pure SQL `REGEXP_REPLACE` |
| `unpack_struct` | ✅ | ⚠️ | ❌ | Spark: `col.*`. Polars: raises ValueError (#212) |
| `hash_columns` | ✅ | ⚠️ | ❌ | Spark: `sha2()`. Pandas: `hash()`. Polars: raises ValueError (#212) |
| `generate_surrogate_key` | ✅ | ⚠️ | ⚠️ | SQL-based; Spark: `md5()`, DuckDB: `md5()`. Polars SQL untested |
| `generate_numeric_key` | ✅ | ⚠️ | ⚠️ | Spark: `CONV(md5,16,10)`, DuckDB: `ABS(hash())`. Polars untested |
| `parse_json` | ✅ | ⚠️ | ⚠️ | Spark: `from_json()`. DuckDB: `CAST AS JSON`. Polars untested |
| `validate_and_flag` | ✅ | ⚠️ | ⚠️ | Pure SQL `concat_ws` + `CASE WHEN` |
| `window_calculation` | ✅ | ⚠️ | ⚠️ | Pure SQL window function wrapper |
| `normalize_json` | ✅ | ⚠️ | ❌ | Spark: `col.*`. Pandas: `json_normalize`. Polars: raises ValueError (#212) |
| `sessionize` | ✅ | ⚠️ | ❌ | Spark: window SQL. Pandas: native. Polars: raises ValueError (#212) |
| `geocode` | ✅ | ✅ | ✅ | Stub (pass-through, no actual geocoding) |
| `split_events_by_period` | ✅ | ⚠️ | ❌ | 3 sub-functions (day/hour/shift). All: Spark+Pandas only (#212) |

### SCD (`odibi/transformers/scd.py`)

| Transformer | Pandas | Spark | Polars | Notes |
| --- | --- | --- | --- | --- |
| `scd2` | ✅ | ⚠️ | ❌ | Spark: Delta MERGE. Pandas: in-memory. Polars: raises ValueError (#212) |

### Merge (`odibi/transformers/merge_transformer.py`)

| Transformer | Pandas | Spark | Polars | Notes |
| --- | --- | --- | --- | --- |
| `merge` | ✅ | ⚠️ | ❌ | Spark: Delta MERGE. Pandas: in-memory upsert. Polars: raises ValueError (#212) |

### Validation (`odibi/transformers/validation.py`)

| Transformer | Pandas | Spark | Polars | Notes |
| --- | --- | --- | --- | --- |
| `cross_check` | ✅ | ⚠️ | ⚠️ | Engine-agnostic (uses `engine.count_rows()`). Untested on Spark/Polars |

### Delete Detection (`odibi/transformers/delete_detection.py`)

| Transformer | Pandas | Spark | Polars | Notes |
| --- | --- | --- | --- | --- |
| `detect_deletes` (snapshot_diff) | ✅ | ⚠️ | ❌ | Spark: Delta time travel. Pandas: Delta Lake Python. No Polars path (#212) |
| `detect_deletes` (sql_compare) | ✅ | ⚠️ | ❌ | Spark: JDBC compare. Pandas: pyodbc. No Polars path (#212) |

### Manufacturing (`odibi/transformers/manufacturing.py`)

| Transformer | Pandas | Spark | Polars | Notes |
| --- | --- | --- | --- | --- |
| `detect_sequential_phases` | ✅ | ⚠️ | ✅ | All 3 engines explicit. Spark has native + UDF modes. Polars tested |

### Thermodynamics (`odibi/transformers/thermodynamics.py`)

| Transformer | Pandas | Spark | Polars | Notes |
| --- | --- | --- | --- | --- |
| `fluid_properties` | ✅ | ⚠️ | ✅ | CoolProp UDF per engine. All 3 explicit branches |
| `saturation_properties` | ✅ | ⚠️ | ✅ | Delegates to `fluid_properties` |
| `psychrometrics` | ✅ | ⚠️ | ✅ | CoolProp HAPropsSI UDF per engine. All 3 explicit |

### Units (`odibi/transformers/units.py`)

| Transformer | Pandas | Spark | Polars | Notes |
| --- | --- | --- | --- | --- |
| `unit_convert` | ✅ | ⚠️ | ✅ | Pint UDF per engine. All 3 explicit branches |

---

## 2. Patterns × Engine

All patterns live in `odibi/patterns/`. They orchestrate transformers and perform
dimension/fact warehouse loading. **No patterns have Polars implementations.**

| Pattern | Pandas | Spark | Polars | Notes |
| --- | --- | --- | --- | --- |
| `AggregationPattern` | ✅ | ⚠️ | ❌ | DuckDB SQL for Pandas. Spark SQL paths implemented, untested (#212) |
| `FactPattern` | ✅ | ⚠️ | ❌ | Dimension lookups, grain validation, quarantine. Spark paths untested |
| `DimensionPattern` | ✅ | ⚠️ | ❌ | SCD0/SCD1/SCD2 modes. `_execute_scd1_spark` exists, untested |
| `DateDimensionPattern` | ✅ | ⚠️ | ❌ | Calendar generation. Spark path via SQL, untested |
| `MergePattern` | ✅ | ⚠️ | ❌ | Wraps `merge` transformer. Spark via Delta MERGE, untested |
| `SCD2Pattern` | ✅ | ⚠️ | ❌ | Wraps `scd2` transformer. Spark via Delta MERGE, untested |

---

## 3. Validation Test Types × Engine

Source: `odibi/validation/engine.py` — methods `_validate_pandas`, `_validate_spark`,
`_validate_polars`.

| Test Type | Pandas | Spark | Polars | Notes |
| --- | --- | --- | --- | --- |
| `SCHEMA` | ✅ | ⚠️ | ✅ | Column existence + type check |
| `ROW_COUNT` | ✅ | ⚠️ | ✅ | Min/max row count bounds |
| `FRESHNESS` | ✅ | ⚠️ | ✅ | Max age of latest timestamp |
| `NOT_NULL` | ✅ | ⚠️ | ✅ | Batched null count aggregation |
| `UNIQUE` | ✅ | ⚠️ | ✅ | Duplicate detection |
| `ACCEPTED_VALUES` | ✅ | ⚠️ | ✅ | Column values in allowed set |
| `RANGE` | ✅ | ⚠️ | ✅ | Min/max bounds for numeric columns |
| `REGEX_MATCH` | ✅ | ⚠️ | ✅ | Pattern matching validation |
| `CUSTOM_SQL` | ✅ | ⚠️ | ⚠️ | Polars: logs warning and skips (partial support) |

### Validation Subsystems

| Subsystem | Pandas | Spark | Polars | Notes |
| --- | --- | --- | --- | --- |
| Quarantine (`validation/quarantine.py`) | ✅ | ⚠️ | ✅ | `_evaluate_test_mask` + `split_valid_invalid` per engine |
| Gate (`validation/gate.py`) | ✅ | ⚠️ | ⚠️ | Spark detection branch (3 lines) untested. Polars: generic path |

---

## 4. Summary Statistics

| Category | Pandas ✅ | Spark ✅/⚠️ | Polars ✅/⚠️/❌ |
| --- | --- | --- | --- |
| Transformers (56 registered) | 56 ✅ | 56 ⚠️ | 17 ❌, 34 ⚠️, 5 ✅ |
| Patterns (6) | 6 ✅ | 6 ⚠️ | 6 ❌ |
| Validation Tests (9) | 9 ✅ | 9 ⚠️ | 8 ✅, 1 ⚠️ |

### Key Gaps (❌)

All Polars gaps are tracked in [#212](https://github.com/invenergy/odibi/issues/212).

| Feature | File | Reason |
| --- | --- | --- |
| `pivot` | `relational.py:485` | No Polars pivot API path |
| `unpivot` | `relational.py:597` | No Polars melt/unpivot path |
| `explode_list_column` | `advanced.py:199` | Needs Polars `explode()` |
| `dict_based_mapping` | `advanced.py:273` | Needs Polars broadcast/join approach |
| `unpack_struct` | `advanced.py:380` | Needs Polars struct unnest |
| `hash_columns` | `advanced.py:461` | Needs Polars hash expression |
| `normalize_json` | `advanced.py:889` | Needs Polars JSON flatten |
| `sessionize` | `advanced.py:993` | Needs Polars window logic |
| `split_events_by_period` | `advanced.py:1261,1381,1509` | 3 sub-modes, all missing Polars |
| `scd2` | `scd.py:205` | No Polars SCD2 strategy |
| `merge` | `merge_transformer.py:365` | No Polars MERGE/upsert |
| `detect_deletes` | `delete_detection.py:76,277` | No Polars snapshot/compare |
| All 6 patterns | `patterns/*.py` | No Polars execute path |

### Why Spark is ⚠️ (not ✅)

All Spark code paths are **implemented** — the code exists and follows correct Spark API
patterns. However, the test suite runs exclusively against `PandasContext` (DuckDB SQL).
Spark paths require a live `SparkSession` which is not available in unit tests. Integration
tests exist but provide limited coverage. Per AGENTS.md:

- `validation/engine.py` — `_validate_spark` (lines 366-570) untested
- `patterns/aggregation.py` — Spark paths untested
- `patterns/fact.py` — Spark paths untested
- `patterns/dimension.py` — `_load_existing_spark`, `_execute_scd1_spark` untested
- `advanced.py` — Spark SQL paths (sessionize, split_events) untested

---

## 5. Engine Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  PandasContext   │     │   SparkContext    │     │  PolarsContext   │
│  (DuckDB SQL)    │     │   (Spark SQL)    │     │  (Polars SQL)    │
└────────┬─────────┘     └────────┬─────────┘     └────────┬─────────┘
         │                         │                         │
         └─────────┬───────────────┴─────────────────────────┘
                   │
         ┌─────────▼─────────┐
         │  context.sql(q)   │  ← SQL-based transformers dispatch here
         └───────────────────┘
```

- **SQL-based transformers** (27 sql_core + aggregate + deduplicate + regex_replace +
  validate_and_flag + window_calculation) work on any engine that supports `context.sql()`.
- **Native transformers** (pivot, unpivot, explode, scd2, merge, etc.) use engine-specific
  APIs and must be ported individually.
- **Domain transformers** (manufacturing, thermodynamics, units) have explicit 3-engine
  dispatch with per-engine UDF implementations.
