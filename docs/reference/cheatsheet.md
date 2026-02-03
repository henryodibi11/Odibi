# Odibi Cheatsheet

## CLI Commands

| Command | Description |
| :--- | :--- |
| `odibi run odibi.yaml` | Run the pipeline. |
| `odibi run odibi.yaml --dry-run` | Validate connections without moving data. |
| `odibi doctor odibi.yaml` | Check environment and config health. |
| `odibi stress odibi.yaml` | Run fuzz tests (random data) to find bugs. |
| `odibi story view --latest` | Open the latest run report. |
| `odibi secrets init odibi.yaml` | Create .env template for secrets. |
| `odibi graph odibi.yaml` | Visualize pipeline dependencies. |
| `odibi generate-project` | Scaffold a new project from files. |

---

## `odibi.yaml` Structure

```yaml
version: 1
project: My Project
engine: pandas          # or 'spark'

connections:
  raw_data:
    type: local
    base_path: ./data

story:
  connection: raw_data
  path: stories/

pipelines:
  - pipeline: main_etl
    nodes:
      # 1. Read
      - name: load_csv
        read:
          connection: raw_data
          path: input.csv
          format: csv

      # 2. Transform (SQL)
      - name: clean_data
        depends_on: [load_csv]
        transform:
          steps:
            - "SELECT * FROM load_csv WHERE id IS NOT NULL"

      # 3. Transform (Python)
      - name: advanced_clean
        depends_on: [clean_data]
        transform:
          steps:
            - operation: my_custom_func  # defined in python
              params:
                threshold: 10

      # 4. Write
      - name: save_parquet
        depends_on: [advanced_clean]
        write:
          connection: raw_data
          path: output.parquet
          format: parquet
          mode: overwrite
```

---

## Python Transformation

```python
from odibi.transformations import transformation

@transformation("my_custom_func")
def my_func(df, threshold=10):
    """Docstrings are required!"""
    return df[df['val'] > threshold]
```

---

## Cross-Pipeline Dependencies

Reference outputs from other pipelines using `$pipeline.node` syntax:

```yaml
nodes:
  - name: enriched_data
    inputs:
      # Cross-pipeline reference
      events: $read_bronze.shift_events

      # Explicit read config
      calendar:
        connection: prod
        path: "bronze/calendar"
        format: delta
    transform:
      steps:
        - operation: join
          left: events
          right: calendar
          on: [date_id]
```

| Syntax | Example | Description |
| :--- | :--- | :--- |
| `$pipeline.node` | `$read_bronze.orders` | Reference node output from another pipeline |

**Requirements:**
- Referenced pipeline must have run first
- Referenced node must have a `write` block
- Cannot use both `read` and `inputs` in same node

---

## SQL Templates

| Variable | Value |
| :--- | :--- |
| `${source}` | The path of the source file (if reading). |
| `${SELF}` | The name of the current node. |

---

## Variable Substitution

| Syntax | Purpose | Example |
| :--- | :--- | :--- |
| `${VAR}` | Environment variable | `${DB_PASSWORD}` |
| `${env:VAR}` | Environment variable (explicit) | `${env:API_KEY}` |
| `${vars.name}` | Custom variable from `vars:` block | `${vars.env}` |
| `${date:expr}` | Dynamic date | `${date:today}`, `${date:-7d}` |
| `${date:expr:fmt}` | Date with format | `${date:today:%Y%m%d}` |

**Date expressions:** `today`, `yesterday`, `now`, `start_of_month`, `end_of_month`, `-7d`, `+30d`, `-1m`, `-1y`

---

## API Fetching

```yaml
read:
  connection: my_api
  format: api
  path: /v1/data
  options:
    method: GET              # GET (default), POST, PUT, PATCH, DELETE
    params:                  # URL params (GET) or merged into body (POST)
      limit: 100
    request_body:            # JSON body for POST/PUT/PATCH
      filters: {...}
    pagination:
      type: offset_limit     # offset_limit | page_number | cursor | link_header
      start_offset: 0        # Use 1 for 1-indexed APIs
    response:
      items_path: results
      add_fields:
        _fetched_at: "${date:now}"
```

---

## Transformer Quick Reference

### SQL Core (Column & Row Operations)

| Transformer | Description | Key Params |
| :--- | :--- | :--- |
| `filter_rows` | Filter rows with SQL WHERE | `condition` |
| `derive_columns` | Add calculated columns | `derivations: {col: expr}` |
| `cast_columns` | Change column types | `casts: {col: type}` |
| `clean_text` | Trim, lowercase, remove chars | `columns, lowercase, strip` |
| `select_columns` | Keep only listed columns | `columns: [...]` |
| `drop_columns` | Remove columns | `columns: [...]` |
| `rename_columns` | Rename columns | `mapping: {old: new}` |
| `fill_nulls` | Replace nulls | `fills: {col: value}` |
| `case_when` | Conditional logic | `conditions: [{when, then}]` |
| `sort` | Order rows | `order_by: "col DESC"` |
| `limit` | Take first N rows | `n: 100` |
| `distinct` | Remove duplicates | `columns: [...]` (optional) |

### Date/Time Operations

| Transformer | Description | Key Params |
| :--- | :--- | :--- |
| `extract_date_parts` | Extract year, month, day | `column, parts: [year, month]` |
| `date_add` | Add interval to date | `column, interval, unit` |
| `date_trunc` | Truncate to period | `column, unit: "month"` |
| `date_diff` | Days between dates | `start_col, end_col` |
| `convert_timezone` | Convert timezones | `column, from_tz, to_tz` |

### Relational (Joins & Aggregations)

| Transformer | Description | Key Params |
| :--- | :--- | :--- |
| `join` | Join datasets | `right_dataset, on, how` |
| `union` | Stack datasets | `datasets: [...]` |
| `aggregate` | Group and aggregate | `group_by, aggregations` |
| `pivot` | Rows to columns | `index, columns, values` |
| `unpivot` | Columns to rows | `id_vars, value_vars` |

### Advanced (Complex Transformations)

| Transformer | Description | Key Params |
| :--- | :--- | :--- |
| `deduplicate` | Keep first/last per key | `keys, order_by` |
| `hash_columns` | Generate hash | `columns, output_column` |
| `window_calculation` | Window functions | `partition_by, order_by, expr` |
| `explode_list_column` | Flatten arrays | `column, outer: true` |
| `dict_based_mapping` | Value mapping | `column, mapping: {}` |
| `regex_replace` | Regex substitution | `column, pattern, replacement` |
| `parse_json` | Extract from JSON | `column, schema` |
| `generate_surrogate_key` | UUID keys | `columns, output_column` |
| `sessionize` | Session detection | `timestamp_col, gap_minutes` |

### Patterns (SCD, Merge, Delete Detection)

| Transformer | Description | Key Params |
| :--- | :--- | :--- |
| `scd2` | SCD Type 2 history | `target, keys, track_cols` |
| `merge` | Upsert/append/delete | `target, keys, strategy` |
| `detect_deletes` | Find deleted records | `target, keys` |

### Validation

| Transformer | Description | Key Params |
| :--- | :--- | :--- |
| `cross_check` | Compare datasets | `inputs: [a, b], type` |
| `validate_and_flag` | Flag bad records | `rules: [{col, condition}]` |
