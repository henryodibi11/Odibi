# Skill 02 — Odibi-First Lookup

> **Layer:** Reasoning
> **When:** Before writing any new function, class, or utility. Check here first.
> **Rule:** If odibi already has it, use it. Do not reinvent.

---

## Purpose

Odibi has 54 transformers, 6 patterns, 11 validation test types, 5 connection types, and dozens of utility functions. Agents frequently reinvent functionality that already exists. This skill is a lookup table — find what you need before writing new code.

---

## Transformers (54 registered in FunctionRegistry)

### SQL Core (`odibi/transformers/sql_core.py`)
| Need | Use | YAML |
|------|-----|------|
| Filter rows by condition | `filter_rows` | `condition: "age > 18"` |
| Add computed columns | `derive_columns` | `derivations: {total: "qty * price"}` |
| Change column types | `cast_columns` | `casts: {id: "integer"}` |
| Clean text (trim, lower, strip) | `clean_text` | `columns: [name], operations: [trim, lower]` |
| Extract year/month/day | `extract_date_parts` | `column: created_at, parts: [year, month]` |
| Standardize column names | `normalize_column_names` | `style: snake_case` |
| Rename columns | `rename_columns` | `mapping: {old: new}` |
| Select subset of columns | `select_columns` | `columns: [a, b, c]` |
| Drop columns | `drop_columns` | `columns: [temp_col]` |
| Sort rows | `sort` | `columns: [date], ascending: [false]` |
| Limit row count | `limit` | `count: 1000` |
| Random sample | `sample` | `fraction: 0.1` |
| Remove duplicates | `distinct` | `columns: [id]` |
| Fill null values | `fill_nulls` | `fills: {status: "unknown"}` |
| Split string column | `split_part` | `column: name, delimiter: " ", index: 0` |
| Date arithmetic | `date_add` | `column: date, days: 30` |
| Truncate dates | `date_trunc` | `column: ts, unit: month` |
| Date difference | `date_diff` | `start: col1, end: col2, unit: days` |
| Conditional logic | `case_when` | `cases: [{when: "x > 0", then: "pos"}]` |
| Timezone conversion | `convert_timezone` | `column: ts, from: UTC, to: US/Central` |
| Concatenate columns | `concat_columns` | `columns: [first, last], separator: " "` |
| Coalesce columns | `coalesce_columns` | `columns: [a, b], output: result` |
| Replace values | `replace_values` | `column: status, mapping: {0: "inactive"}` |
| Trim whitespace | `trim_whitespace` | `columns: [name, email]` |
| Add prefix to columns | `add_prefix` | `prefix: src_` |
| Add suffix to columns | `add_suffix` | `suffix: _raw` |

### Relational (`odibi/transformers/relational.py`)
| Need | Use | YAML |
|------|-----|------|
| Join two datasets | `join` | `dataset: other, keys: [id], how: left` |
| Union/stack datasets | `union` | `dataset: other, by: name` |
| Pivot (rows → columns) | `pivot` | `index: [id], columns: metric, values: val` |
| Unpivot (columns → rows) | `unpivot` | `id_vars: [id], value_vars: [a, b]` |
| Group and aggregate | `aggregate` | `group_by: [cat], aggs: {val: sum}` |

### Advanced (`odibi/transformers/advanced.py`)
| Need | Use | YAML |
|------|-----|------|
| Remove duplicate rows | `deduplicate` | `keys: [id], order_by: [ts], keep: last` |
| Flatten list column | `explode_list_column` | `column: tags` |
| Map values via dictionary | `dict_based_mapping` | `column: code, mapping: {A: Active}` |
| Sessionize event data | `sessionize` | `timestamp: ts, user: uid, gap: 30m` |
| Split events by time period | `split_events_by_period` | `timestamp: ts, period: day` |
| Fill missing time series | `fill_missing_timestamps` | `timestamp: ts, freq: 1h` |

### SCD & Merge (`odibi/transformers/scd.py`, `merge_transformer.py`)
| Need | Use | YAML |
|------|-----|------|
| SCD Type 2 history tracking | `scd2` | `keys: [id], tracked_columns: [name]` |
| Key-based merge/upsert | `merge` | `keys: [id], target: silver/table` |

### Delete Detection (`odibi/transformers/delete_detection.py`)
| Need | Use | YAML |
|------|-----|------|
| CDC-like snapshot diff | `detect_deletes` | `mode: snapshot_diff, keys: [id]` |
| Compare against live source | `detect_deletes` | `mode: sql_compare, keys: [id]` |

### Domain-Specific
| Need | Module | Use |
|------|--------|-----|
| Unit conversion (m→ft, kg→lb) | `units.py` | `unit_convert` |
| Fluid properties (CoolProp) | `thermodynamics.py` | `fluid_properties`, `psychrometrics` |
| Manufacturing phase detection | `manufacturing.py` | `detect_phases`, `track_status` |

---

## Patterns (6 in `odibi/patterns/`)

| Need | Pattern | Key Params |
|------|---------|------------|
| Build dimension table with surrogate keys | `dimension` | `natural_key`, `surrogate_key`, `scd_type` |
| Build fact table with SK lookups | `fact` | `keys`, `dimension_lookups` |
| SCD Type 2 history tracking | `scd2` | `keys`, `tracked_columns`, `target` |
| Key-based merge/upsert | `merge` | `keys`, `target` |
| Declarative aggregation | `aggregation` | `grain`, `measures` |
| Generate date dimension table | `date_dimension` | `start_date`, `end_date` |

---

## Validation (11 test types in `odibi/validation/engine.py`)

| Need | Test Type | YAML |
|------|-----------|------|
| Column must not be null | `NOT_NULL` | `type: not_null, column: id` |
| Column values must be unique | `UNIQUE` | `type: unique, column: email` |
| Column values in allowed set | `ACCEPTED_VALUES` | `type: accepted_values, column: status, values: [A, B]` |
| Row count in range | `ROW_COUNT` | `type: row_count, min: 1, max: 10000` |
| Numeric range check | `RANGE` | `type: range, column: age, min: 0, max: 150` |
| Regex pattern match | `REGEX_MATCH` | `type: regex_match, column: email, pattern: ".*@.*"` |
| Custom SQL condition | `CUSTOM_SQL` | `type: custom_sql, query: "COUNT(*) > 0"` |
| Schema matches expected | `SCHEMA` | `type: schema, columns: {id: int, name: str}` |
| Data freshness check | `FRESHNESS` | `type: freshness, column: updated_at, max_age: 24h` |
| Foreign key exists in parent | FK validation | `odibi/validation/fk.py` |
| Batch-level quality gate | Quality gate | `odibi/validation/gate.py` |

### Validation Support Functions
| Need | Module | Function |
|------|--------|----------|
| Split valid/invalid rows | `quarantine.py` | `split_valid_invalid()` |
| Add quarantine metadata | `quarantine.py` | `add_quarantine_metadata()` |
| Evaluate quality gate | `gate.py` | `evaluate_gate()` |
| Check foreign keys | `fk.py` | `validate_foreign_keys()` |

---

## Connections (5 types in `odibi/connections/`)

| Need | Type | Config Key |
|------|------|------------|
| Local filesystem | `local` | `base_path` |
| Azure Data Lake (ADLS) | `azure_blob` | `account_name`, `container` |
| Azure SQL / SQL Server | `sql_server` | `host`, `database`, `auth_mode` |
| PostgreSQL | `postgres` | `host`, `database`, `user` |
| HTTP API | `http` | `base_url`, `headers` |

---

## Utilities (Don't Reinvent These)

| Need | Module | Function/Class |
|------|--------|----------------|
| Structured logging | `utils/logging_context.py` | `get_logging_context()` |
| Error suggestions | `utils/error_suggestions.py` | `get_error_suggestions()` |
| Progress tracking | `utils/progress.py` | `ProgressTracker` |
| Content hashing | `utils/content_hash.py` | `content_hash()` |
| Encoding detection | `utils/encoding.py` | `detect_encoding()` |
| Config loading (YAML) | `utils/config_loader.py` | `load_config()` |
| Duration formatting | `utils/duration.py` | `format_duration()` |
| Key Vault secrets | `utils/setup_helpers.py` | `fetch_keyvault_secret()` |
| ADF profiler | `tools/adf_profiler.py` | `AdfProfiler` |
| Template generation | `tools/templates.py` | `TemplateGenerator` |
| Pipeline stories | `story/generator.py` | `StoryGenerator` |
| Lineage tracking | `lineage.py` | `LineageTracker` |
| System catalog | `catalog.py` | `CatalogManager` |

---

## CLI Commands (Don't Rebuild These)

```bash
odibi list transformers             # List all 54 transformers
odibi list patterns                 # List all 6 patterns
odibi list connections              # List all connection types
odibi explain <name>                # Detailed docs for any feature
odibi templates show <type>         # YAML template for any connection/pattern
odibi templates transformer <name>  # Transformer params + example YAML
odibi validate <config.yaml>        # Validate pipeline YAML
odibi init-pipeline                 # Bootstrap new pipeline interactively
odibi doctor                        # Environment diagnostics
odibi test                          # Run pipeline test cases
```

---

## Decision Tree

```
Need to transform data?
├── Filter/derive/cast/clean/rename? → sql_core transformers
├── Join/union/pivot/aggregate? → relational transformers
├── Deduplicate/sessionize/explode? → advanced transformers
├── Track history (SCD2)? → scd transformer or scd2 pattern
├── Merge/upsert? → merge transformer or merge pattern
├── Detect deletes? → delete_detection transformer
├── Domain-specific (units/thermo/mfg)? → domain transformers
└── None of the above? → Write a new transformer (Skill 03)

Need to build a table?
├── Dimension with surrogate keys? → dimension pattern
├── Fact with SK lookups? → fact pattern
├── SCD Type 2? → scd2 pattern
├── Simple merge/upsert? → merge pattern
├── Aggregation/summary? → aggregation pattern
├── Calendar/date table? → date_dimension pattern
└── None of the above? → Write a new pattern (Skill 04)

Need to validate data?
├── Column-level check? → validation engine (11 test types)
├── Row-level split? → quarantine
├── Batch-level threshold? → quality gate
├── FK integrity? → FK validation
└── Custom? → CUSTOM_SQL test type

Need to connect to data?
├── Local files? → local connection
├── Azure blob/ADLS? → azure_blob connection
├── SQL Server/Azure SQL? → sql_server connection
├── PostgreSQL? → postgres connection
├── REST API? → http connection + api_fetcher
└── New source? → Write a connection (Skill 06)
```

---

## Anti-Pattern Examples

❌ **Wrong:** Writing a custom dedup function
```python
def remove_duplicates(df, key_col):
    return df.drop_duplicates(subset=[key_col], keep='last')
```

✅ **Right:** Using the registered transformer
```yaml
transform:
  steps:
    - function: deduplicate
      params:
        keys: [customer_id]
        order_by: [updated_at]
        keep: last
```

❌ **Wrong:** Writing a custom SCD2 implementation
✅ **Right:** Using `scd2` transformer or `scd2` pattern

❌ **Wrong:** Writing a custom SQL query builder for filtering
✅ **Right:** Using `filter_rows` transformer with SQL WHERE clause

❌ **Wrong:** Writing a custom logging wrapper
✅ **Right:** Using `get_logging_context()` from `odibi.utils.logging_context`
