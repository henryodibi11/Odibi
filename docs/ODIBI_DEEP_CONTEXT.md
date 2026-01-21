# Odibi Deep Context

> **What is Odibi?** A Python data pipeline framework for building enterprise data warehouses. It orchestrates nodes (read → transform → validate → write) with dependency resolution, supports Pandas/Spark/Polars engines, and provides patterns for common DWH tasks (SCD2, dimension tables, fact tables, merges).

**Target User:** Solo data engineer or small team building data pipelines without dedicated infrastructure support.

**Core Philosophy:** YAML-first configuration, engine parity (same YAML works on Pandas/Spark), patterns for DWH best practices.

> **💡 AI/LLM Tip:** Use CLI introspection to discover features programmatically:
> ```bash
> odibi list transformers --format json   # All 52+ transformers
> odibi list patterns --format json       # All 6 patterns
> odibi explain <name>                    # Detailed docs + example YAML
> ```

---

## Table of Contents

1. [Critical Runtime Behavior](#1-critical-runtime-behavior)
2. [Core Execution Model](#2-core-execution-model)
3. [Patterns Reference](#3-patterns-reference)
4. [Transformers Reference](#4-transformers-reference)
5. [Validation & Quarantine](#5-validation--quarantine)
6. [Connections Reference](#6-connections-reference)
7. [Write Configuration (Delta Lake Features)](#7-write-configuration-delta-lake-features)
8. [Alerts & Notifications](#8-alerts--notifications)
9. [System Catalog (The Brain)](#9-system-catalog-the-brain)
10. [OpenLineage Integration](#10-openlineage-integration)
11. [Foreign Key Validation](#11-foreign-key-validation)
12. [Orchestration Export](#12-orchestration-export)
13. [Manufacturing Transformers](#13-manufacturing-transformers)
14. [Semantic Layer](#14-semantic-layer)
15. [Story Generation](#15-story-generation)
16. [Common Workflows](#16-common-workflows)
17. [CLI Reference](#17-cli-reference)
18. [Anti-Patterns and Gotchas](#18-anti-patterns-and-gotchas)
19. [SQL Server Writer](#19-sql-server-writer)
20. [Incremental Loading (Advanced)](#20-incremental-loading-advanced)
21. [Diagnostics & Diff Tools](#21-diagnostics--diff-tools)
22. [Cross-Check Transformer](#22-cross-check-transformer)
23. [Testing Utilities](#23-testing-utilities)
24. [Derived Updater (Internal)](#24-derived-updater-internal)
25. [Extension Points](#25-extension-points)
26. [Quick Reference Cheat Sheet](#26-quick-reference-cheat-sheet)
27. [Documentation Map](#27-documentation-map)

---

## 1. Critical Runtime Behavior

### 1.1 Spark Temp View Registration (MOST IMPORTANT)

When using PipelineManager with **Spark engine**, each node's output DataFrame is registered as a Spark temp view:

```python
# In odibi/context.py, SparkContext.register() at line 432:
df.createOrReplaceTempView(name)
```

**You can query any node's output with:**
```python
spark.sql("SELECT * FROM node_name")
```

**Constraints:**
- **Node names must be alphanumeric + underscore only** (no spaces, hyphens, dots)
- Validated by `SparkContext._validate_name()` with regex `^[a-zA-Z0-9_]+$`
- Invalid names raise `ValueError`: *"Invalid node name 'X' for Spark engine. Names must contain only alphanumeric characters and underscores"*

**Lifecycle:**
- Views persist for duration of pipeline execution
- Tracked in `SparkContext._registered_views: set[str]`
- Cleaned up via `context.clear()` which calls `spark.catalog.dropTempView(name)`
- Thread-safe: Uses `threading.RLock()` for concurrent access

**How to list registered views:**
```python
# From context
context.list_names()  # Returns list of registered DataFrame names

# From SparkSession
spark.catalog.listTables()
```

**How to get a DataFrame back:**
```python
# Via context
df = context.get("node_name")

# Via Spark (equivalent)
df = spark.table("node_name")
```

### 1.2 Pandas Context Behavior

With **Pandas engine**, DataFrames are stored in an in-memory dictionary:
```python
# odibi/context.py, PandasContext at line 209:
self._data: Dict[str, pd.DataFrame] = {}
```

- **No temp views** - data accessed via `context.get(name)`
- SQL operations use **DuckDB** under the hood (not Spark SQL)
- The `context.sql(query)` method:
  1. Registers current DataFrame as a unique temp view
  2. Replaces `df` references in query with that view name
  3. Executes via DuckDB
  4. Cleans up temp view

**DuckDB vs Spark SQL differences:**
| Feature | Pandas (DuckDB) | Spark |
|---------|-----------------|-------|
| Exclude columns | `SELECT * EXCLUDE (col)` | `SELECT * EXCEPT (col)` |
| Row number | `ROW_NUMBER() OVER (...)` | Same |
| Array explode | `UNNEST(arr)` | `explode(arr)` |

### 1.3 Polars Context Behavior

Similar to Pandas - in-memory dictionary storage:
```python
# odibi/context.py, PolarsContext:
self._data: Dict[str, Any] = {}  # Stores pl.DataFrame or pl.LazyFrame
```

- Supports both eager (`DataFrame`) and lazy (`LazyFrame`) evaluation
- Limited SQL support compared to Pandas/Spark

### 1.4 Data Flow Between Nodes

When Node B depends on Node A:

1. Node A completes → output DataFrame registered via `context.register(name, df)`
2. Node B starts → gets input via `context.get("node_a")`
3. **For Spark:** Node B can also use `spark.sql("SELECT * FROM node_a")`

```yaml
# YAML declaration
nodes:
  - name: node_b
    depends_on: [node_a]  # Explicit dependency (required for execution order)
    inputs:
      source: node_a      # How to reference the data
```

**Cross-Pipeline References:**
```yaml
inputs:
  customers: $other_pipeline.customer_node  # Reference node from another pipeline
```

---

## 2. Core Execution Model

### 2.1 Pipeline Execution Flow

```
PipelineManager.run()
    │
    ├── 1. Load project.yaml + pipeline.yaml
    ├── 2. Build DependencyGraph (topological sort)
    ├── 3. Create Context (Pandas/Spark/Polars)
    ├── 4. Register standard transformers
    │
    └── 5. For each node in execution order:
            │
            ├── PRE-SQL: Execute pre_sql statements
            ├── READ: Load data from source/connection
            │   └── Apply incremental filter if configured
            ├── TRANSFORM: Apply transformers/patterns in order
            ├── VALIDATE: Run quality checks
            │   ├── Tests with on_fail=FAIL → stop pipeline
            │   ├── Tests with on_fail=WARN → log warning
            │   └── Tests with on_fail=QUARANTINE → route to quarantine table
            ├── WRITE: Persist to target
            │   └── Add metadata columns if configured
            └── REGISTER: context.register(node_name, df)
```

**Key Classes:**
| Class | File | Role |
|-------|------|------|
| `Pipeline` | `odibi/pipeline.py` | Orchestrates execution, manages state |
| `PipelineManager` | `odibi/pipeline.py` | Entry point, loads config, creates Pipeline |
| `Node` | `odibi/node.py` | Single unit of work |
| `NodeExecutor` | `odibi/node.py` | Executes read/transform/validate/write |
| `DependencyGraph` | `odibi/graph.py` | Resolves execution order |
| `Context` | `odibi/context.py` | Stores DataFrames between nodes |

### 2.2 Node Execution Phases

Each node goes through these phases (tracked by `PhaseTimer`):

| Phase | What Happens |
|-------|--------------|
| `pre_sql` | Execute SQL statements before read |
| `read` | Load data from connection/input |
| `incremental_filter` | Apply HWM or rolling window filter |
| `transform` | Apply each transformer in order |
| `validate` | Run validation tests |
| `quarantine` | Route failed rows if configured |
| `gate` | Evaluate quality gates |
| `write` | Persist to destination |
| `post_sql` | Execute SQL statements after write |

### 2.3 Error Handling Strategies

```yaml
# In pipeline.yaml or CLI
error_strategy: fail_fast  # Options: fail_fast, fail_later, ignore
```

| Strategy | Behavior | Use Case |
|----------|----------|----------|
| `fail_fast` | Stop immediately on first error | Development, critical pipelines |
| `fail_later` | Continue (dependents skipped), fail at end | Batch processing, partial success OK |
| `ignore` | Log errors, continue (dependents run) | Non-critical, alerting-only |

### 2.4 Retry Configuration

```yaml
retry:
  max_attempts: 3
  delay_seconds: 10
  backoff_multiplier: 2.0  # Exponential backoff
  retry_on:
    - ConnectionError
    - TimeoutError
```

### 2.5 Context Factory

```python
from odibi.context import create_context

# Create appropriate context
context = create_context("pandas")  # Returns PandasContext
context = create_context("spark", spark_session=spark)  # Returns SparkContext
context = create_context("polars")  # Returns PolarsContext
```

---

## 3. Patterns Reference

Patterns are high-level abstractions for common DWH operations. They encapsulate complex logic into declarative configuration.

### 3.1 Dimension Pattern

**Purpose:** Create dimension tables with surrogate keys for star schema.

**Class:** `DimensionPattern` in `odibi/patterns/dimension.py`

**Features:**
- Auto-generate integer surrogate keys (MAX + ROW_NUMBER)
- SCD Type 0 (static), 1 (overwrite), 2 (history tracking)
- Optional unknown member row (SK=0) for orphan FK handling
- Audit columns (load_timestamp, source_system)

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `natural_key` | str/list | Yes | - | Business key column(s) |
| `surrogate_key` | str | Yes | - | Name for generated SK column |
| `scd_type` | int | No | 1 | 0=static, 1=overwrite, 2=history |
| `track_cols` | list | For SCD1/2 | - | Columns to monitor for changes |
| `target` | str | For SCD2 | - | Path to existing dimension |
| `unknown_member` | bool | No | False | Add SK=0 unknown row |
| `audit.load_timestamp` | bool | No | True | Add load timestamp |
| `audit.source_system` | str | No | None | Source system name |

**YAML Example:**
```yaml
pattern:
  type: dimension
  params:
    natural_key: customer_id
    surrogate_key: customer_sk
    scd_type: 1
    track_cols: [name, email, tier]
    unknown_member: true
    audit:
      load_timestamp: true
      source_system: crm
```

**Output Schema Changes:**
- Adds: `{surrogate_key}` column (integer, starts at 1)
- Adds: `_load_timestamp` (if audit.load_timestamp=true)
- Adds: `_source_system` (if audit.source_system set)
- SCD2 adds: `valid_from`, `valid_to`, `is_current`
- Unknown member: Row with SK=0, natural_key=-1 or "UNKNOWN"

---

### 3.2 Fact Pattern

**Purpose:** Build fact tables with foreign key lookups to dimensions.

**Class:** `FactPattern` in `odibi/patterns/fact.py`

**Features:**
- Automatic surrogate key lookups from dimension tables
- Orphan handling (unknown member, reject, quarantine)
- Grain validation (detect duplicates)
- Measure calculations
- Deduplication

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `grain` | list | No | Columns defining uniqueness (validates no duplicates) |
| `dimensions` | list | No | Dimension lookup configurations |
| `orphan_handling` | str | No | `unknown`, `reject`, `quarantine` (default: unknown) |
| `quarantine` | dict | If orphan=quarantine | Quarantine destination config |
| `measures` | list | No | Measure column definitions |
| `deduplicate` | bool | No | Remove duplicates (requires `keys`) |
| `keys` | list | If deduplicate | Deduplication keys |
| `audit` | dict | No | Audit column configuration |

**Dimension Lookup Structure:**
```yaml
dimensions:
  - source_column: customer_id      # FK column in fact
    dimension_table: dim_customer   # Dimension node name in context
    dimension_key: customer_id      # NK column in dimension
    surrogate_key: customer_sk      # SK to retrieve
    scd2: true                      # Filter is_current=true (optional)
```

**YAML Example:**
```yaml
pattern:
  type: fact
  params:
    grain: [order_id]
    dimensions:
      - source_column: customer_id
        dimension_table: dim_customer
        dimension_key: customer_id
        surrogate_key: customer_sk
        scd2: true
      - source_column: product_id
        dimension_table: dim_product
        dimension_key: product_id
        surrogate_key: product_sk
    orphan_handling: unknown  # Orphans get SK=0
    measures:
      - quantity
      - total_amount
      - profit: "total_amount - cost"  # Calculated measure
    audit:
      load_timestamp: true
      source_system: pos
```

**Orphan Handling Options:**
| Mode | Behavior |
|------|----------|
| `unknown` | Assign SK=0 (requires unknown member in dimension) |
| `reject` | Filter out orphan rows |
| `quarantine` | Route orphans to separate table |

---

### 3.3 SCD2 Pattern (Transformer)

**Purpose:** Maintain full history of changes with effective date ranges.

**Class:** `SCD2Params` in `odibi/transformers/scd.py`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `target` | str | Yes* | Table name or path |
| `connection` | str | Yes* | Connection name (with `path`) |
| `path` | str | Yes* | Relative path within connection |
| `keys` | list | Yes | Natural key columns |
| `track_cols` | list | Yes | Columns to monitor for changes |
| `effective_time_col` | str | Yes | Source column with change timestamp |
| `end_time_col` | str | No | Default: `valid_to` |
| `current_flag_col` | str | No | Default: `is_current` |
| `delete_col` | str | No | Column indicating soft deletion |

*Either `target` OR `connection`+`path` required, not both.

**YAML Example (Table Name):**
```yaml
transformer: scd2
params:
  target: silver.dim_customers
  keys: [customer_id]
  track_cols: [address, tier, email]
  effective_time_col: updated_at
```

**YAML Example (Connection + Path):**
```yaml
transformer: scd2
params:
  connection: adls_prod
  path: OEE/silver/dim_customers
  keys: [customer_id]
  track_cols: [address, tier]
  effective_time_col: txn_date
```

**How It Works:**
1. **Match**: Find existing records using `keys`
2. **Compare**: Check `track_cols` for changes (uses `IS DISTINCT FROM` for null-safe comparison)
3. **Close**: Update old record's `end_time_col` to `effective_time_col`, set `is_current=False`
4. **Insert**: Add new record with `is_current=True`, open-ended `end_time_col`

**CRITICAL:** SCD2 returns the FULL history dataset. You MUST use `mode: overwrite`:
```yaml
write:
  connection: silver
  path: dim_customers
  format: delta
  mode: overwrite  # NOT append!
```

---

### 3.4 Merge Pattern (Transformer)

**Purpose:** Upsert, append, or delete records in target table.

**Class:** `MergeTransformer` in `odibi/transformers/merge_transformer.py`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `target` | str | Yes | Target table/path |
| `keys` | list | Yes | Match keys for merge |
| `strategy` | str | No | `upsert`, `append_only`, `delete_match` |
| `created_col` | str | No | Audit column for inserts |
| `updated_col` | str | No | Audit column for updates |
| `soft_delete_col` | str | No | Column for soft delete flag |

**Strategies:**
| Strategy | Behavior |
|----------|----------|
| `upsert` | Insert new, update existing |
| `append_only` | Insert new only, ignore existing |
| `delete_match` | Delete target rows matching source keys |

**YAML Example:**
```yaml
transformer: merge
params:
  target: silver.products
  keys: [product_id]
  strategy: upsert
  updated_col: _updated_at
```

---

### 3.5 Aggregation Pattern

**Purpose:** Group and aggregate data with optional time rollups and incremental merging.

**Class:** `AggregationPattern` in `odibi/patterns/aggregation.py`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `grain` | list | Yes | Group by columns |
| `measures` | list | Yes | SQL aggregation expressions |
| `time_rollup` | str | No | `daily`, `weekly`, `monthly` |
| `merge_strategy` | str | No | `replace`, `sum`, `min`, `max` |
| `having` | str | No | HAVING clause filter |
| `audit_config` | dict | No | Add `load_timestamp`, `source_system` |

**YAML Example:**
```yaml
pattern:
  type: aggregation
  params:
    grain: [date_sk, product_sk]
    measures:
      - "SUM(amount) as total_amount"
      - "COUNT(*) as transaction_count"
      - "AVG(unit_price) as avg_price"
    having: "COUNT(*) > 10"
```

---

### 3.6 Date Dimension Pattern

**Purpose:** Generate a complete date dimension table.

**Class:** `DateDimensionPattern` in `odibi/patterns/date_dimension.py`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `start_date` | str | Yes | Start date (YYYY-MM-DD) |
| `end_date` | str | Yes | End date (YYYY-MM-DD) |
| `fiscal_year_start_month` | int | No | Fiscal year start (1-12) |
| `week_start_day` | int | No | Week start (0=Mon, 6=Sun) |
| `holidays` | list | No | Holiday dates or calendar name |

**YAML Example:**
```yaml
pattern:
  type: date_dimension
  params:
    start_date: "2020-01-01"
    end_date: "2030-12-31"
    fiscal_year_start_month: 7
    week_start_day: 0
```

**Output Columns:**
- `date_sk` (int): Surrogate key (YYYYMMDD format)
- `date_key` (date): Date value
- `full_date` (str): Full date string
- `year`, `quarter`, `month`, `day`
- `day_of_week`, `day_of_week_name`
- `week_of_year`, `month_name`
- `is_weekend`, `is_holiday`
- `fiscal_year`, `fiscal_quarter`, `fiscal_month`

---

## 4. Transformers Reference

Transformers are atomic operations applied to DataFrames. All transformers work on Pandas (via DuckDB) and Spark with engine parity.

### 4.1 SQL Core Transformers

| Transformer | Purpose | Key Parameters |
|-------------|---------|----------------|
| `filter_rows` | WHERE clause | `condition: str` |
| `derive_columns` | Add computed columns | `derivations: {col: expr}` |
| `cast_columns` | Change column types | `casts: {col: type}` |
| `select_columns` | Keep specific columns | `columns: list` |
| `drop_columns` | Remove columns | `columns: list` |
| `rename_columns` | Rename columns | `mapping: {old: new}` |
| `sort` | Order rows | `by: list`, `ascending: bool` |
| `limit` | Limit rows | `n: int` |
| `sample` | Random sample | `fraction: float` or `n: int` |
| `distinct` | Remove duplicates | `columns: list` (optional) |
| `fill_nulls` | Replace nulls | `columns: list`, `value: any` |
| `clean_text` | Trim/case transform | `columns`, `trim`, `case` |
| `case_when` | Conditional logic | `conditions: list`, `output_col` |
| `coalesce_columns` | First non-null | `columns`, `output_col` |
| `concat_columns` | String concatenation | `columns`, `output_col`, `separator` |
| `normalize_column_names` | Clean column names | `style`, `lowercase`, `remove_special` |
| `replace_values` | Bulk value replacement | `columns`, `mapping` |
| `trim_whitespace` | Trim all string columns | `columns` (optional) |
| `add_prefix` | Add column prefix | `prefix`, `columns` |
| `add_suffix` | Add column suffix | `suffix`, `columns` |
| `split_part` | Split string | `column`, `delimiter`, `part` |

**Example:**
```yaml
transform:
  - filter_rows:
      condition: "status = 'active' AND amount > 0"
  - derive_columns:
      derivations:
        total_price: "quantity * unit_price"
        full_name: "concat(first_name, ' ', last_name)"
  - cast_columns:
      casts:
        created_at: timestamp
        amount: double
```

### 4.2 Date Transformers

| Transformer | Purpose | Key Parameters |
|-------------|---------|----------------|
| `extract_date_parts` | Extract year/month/day | `column`, `parts: list` |
| `date_add` | Add interval | `column`, `days`/`months`/`years`, `output_col` |
| `date_diff` | Difference between dates | `start_col`, `end_col`, `unit`, `output_col` |
| `date_trunc` | Truncate to unit | `column`, `unit` |
| `convert_timezone` | TZ conversion | `column`, `from_tz`, `to_tz` |

**Example:**
```yaml
transform:
  - extract_date_parts:
      column: order_date
      parts: [year, month, day_of_week]
  - date_diff:
      start_col: start_date
      end_col: end_date
      unit: days
      output_col: duration_days
```

### 4.3 Relational Transformers

| Transformer | Purpose | Key Parameters |
|-------------|---------|----------------|
| `join` | Join datasets | `right_dataset`, `on`, `how`, `prefix` |
| `union` | Stack datasets | `datasets: list`, `by_name: bool` |
| `aggregate` | Group by | `group_by: list`, `aggregations: dict` |
| `pivot` | Rows to columns | `group_by`, `pivot_col`, `agg_col`, `agg_func` |
| `unpivot` | Columns to rows | `id_cols`, `value_vars`, `var_name`, `value_name` |

**Join Types:** `inner`, `left`, `right`, `full`, `cross`, `anti`, `semi`

**Join Example:**
```yaml
transform:
  - join:
      right_dataset: dim_customer  # Must be in depends_on!
      on: [customer_id]
      how: left
      prefix: cust  # Avoid column collisions → cust_name, cust_email
```

**Aggregate Example:**
```yaml
transform:
  - aggregate:
      group_by: [department, region]
      aggregations:
        salary: sum
        employee_id: count
        age: avg
```

### 4.4 Advanced Transformers

| Transformer | Purpose | Key Parameters |
|-------------|---------|----------------|
| `deduplicate` | Keep first/last per key | `keys`, `order_by` |
| `explode_list_column` | Flatten arrays | `column`, `outer: bool` |
| `dict_based_mapping` | Value replacement | `column`, `mapping`, `default`, `output_column` |
| `hash_columns` | Generate hash | `columns`, `output_col`, `algorithm` |
| `generate_surrogate_key` | UUID/hash key | `columns`, `output_col` |
| `generate_numeric_key` | Integer sequence | `output_col`, `start` |
| `window_calculation` | Window functions | `partition_by`, `order_by`, `calculations` |
| `parse_json` | Extract from JSON | `column`, `schema` |
| `normalize_json` | Flatten nested JSON | `column`, `max_level` |
| `regex_replace` | Regex substitution | `column`, `pattern`, `replacement` |
| `unpack_struct` | Flatten struct columns | `column`, `prefix` |
| `sessionize` | Session detection | `user_col`, `timestamp_col`, `timeout` |
| `geocode` | Geocode addresses | `address_col`, `lat_col`, `lon_col` |
| `validate_and_flag` | Flag invalid rows | `validations`, `flag_col` |
| `split_events_by_period` | Split time-spanning events | `start_col`, `end_col`, `period`, `shifts` |

**Deduplicate Example:**
```yaml
transform:
  - deduplicate:
      keys: [customer_id]
      order_by: "updated_at DESC"  # Keep latest
```

**Window Calculation Example:**
```yaml
transform:
  - window_calculation:
      partition_by: [customer_id]
      order_by: [order_date]
      calculations:
        - function: row_number
          output_col: order_seq
        - function: sum
          column: amount
          output_col: running_total
```

### 4.5 Delete Detection Transformer

**Purpose:** CDC-like behavior for sources without native change capture.

**Class:** `DeleteDetectionConfig` in `odibi/config.py`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `mode` | str | Yes | `none`, `snapshot_diff`, `sql_compare` |
| `keys` | list | Yes | Primary key columns |
| `soft_delete_col` | str | No | Add boolean flag (default: `_is_deleted`) |
| `max_delete_percent` | float | No | Safety threshold (default: 50.0) |
| `on_threshold_breach` | str | No | `warn`, `error`, `skip` |
| `on_first_run` | str | No | `skip`, `error` |

**Modes:**
| Mode | When to Use |
|------|-------------|
| `none` | Append-only facts, no delete tracking needed |
| `snapshot_diff` | Full snapshot sources (compares Delta version N vs N-1) |
| `sql_compare` | HWM incremental ingestion (queries live source) |

**Example (SQL Compare - Recommended):**
```yaml
transformer: detect_deletes
params:
  mode: sql_compare
  keys: [customer_id]
  source_connection: azure_sql
  source_table: dbo.Customers
  soft_delete_col: _is_deleted
  max_delete_percent: 10.0
  on_threshold_breach: error
```

**Example (Snapshot Diff):**
```yaml
transformer: detect_deletes
params:
  mode: snapshot_diff
  keys: [product_id]
  connection: silver_conn
  path: silver/products
```

---

## 5. Validation & Quarantine

### 5.1 Validation Tests

Define data quality tests in the `validate:` block:

```yaml
validate:
  tests:
    - type: not_null
      columns: [customer_id, order_id]
      on_fail: fail  # fail, warn, quarantine

    - type: unique
      columns: [order_id]
      on_fail: fail

    - type: accepted_values
      column: status
      values: [pending, shipped, delivered, cancelled]
      on_fail: warn

    - type: range
      column: quantity
      min: 1
      max: 10000
      on_fail: quarantine

    - type: regex_match
      column: email
      pattern: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
      on_fail: warn

    - type: freshness
      column: updated_at
      max_age: "24h"  # or "7d", "30m"
      on_fail: fail

    - type: row_count
      min: 1
      max: 10000000
      on_fail: fail

    - type: custom_sql
      name: positive_amounts
      condition: "amount > 0"
      on_fail: quarantine
```

**Test Types:**
| Type | Purpose | Parameters |
|------|---------|------------|
| `not_null` | Check for nulls | `columns: list` |
| `unique` | Check uniqueness | `columns: list` |
| `accepted_values` | Check allowed values | `column`, `values: list` |
| `range` | Check numeric range | `column`, `min`, `max` |
| `regex_match` | Check pattern | `column`, `pattern` |
| `freshness` | Check data age | `column`, `max_age` |
| `row_count` | Check row count | `min`, `max` |
| `schema` | Check columns exist | `strict: bool` |
| `custom_sql` | Custom condition | `condition`, `name` |

**on_fail Options:**
| Value | Behavior |
|-------|----------|
| `fail` | Stop pipeline with error |
| `warn` | Log warning, continue |
| `quarantine` | Route failing rows to quarantine table |

### 5.2 Quarantine Configuration

Route failing rows to a separate table:

```yaml
validate:
  tests:
    - type: range
      column: quantity
      min: 1
      on_fail: quarantine

  quarantine:
    connection: silver
    path: quarantine/orders
    # OR: table: silver.orders_quarantine
    add_columns:
      rejection_reason: true      # "_rejection_reason" column
      rejected_at: true           # "_rejected_at" timestamp
      source_batch_id: true       # "_source_batch_id" run ID
      failed_tests: true          # "_failed_tests" list
      original_node: true         # "_original_node" source node
    max_rows: 10000              # Limit quarantine rows (optional)
    sample_fraction: 0.1         # Sample quarantine rows (optional)
```

**Quarantine Output Schema:**
Original columns plus:
- `_rejection_reason`: Text description of failure
- `_rejected_at`: ISO timestamp
- `_source_batch_id`: Pipeline run ID
- `_failed_tests`: Comma-separated test names
- `_original_node`: Node name that failed validation

### 5.3 Quality Gates

Batch-level validation that can block the entire write:

```yaml
validate:
  gate:
    require_pass_rate: 0.95  # 95% of rows must pass
    thresholds:
      - test: not_null
        min_pass_rate: 0.99
      - test: accepted_values
        min_pass_rate: 0.90
    row_count:
      min: 100
      max: 1000000
      change_threshold: 0.5  # Alert if >50% change from previous run
    on_fail: abort  # abort, warn, quarantine_all
```

---

## 6. Connections Reference

### 6.1 Local Connection

**Class:** `LocalConnection` in `odibi/connections/local.py`

```yaml
connections:
  local_data:
    type: local
    base_path: ./data/bronze
```

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `base_path` | str | No | `./data` | Root directory |

**Supports:** Local paths, `file://`, `dbfs:/` URIs

**Path Resolution:**
```python
conn.get_path("silver/customers")
# Local: /absolute/path/to/data/bronze/silver/customers
# URI: dbfs:/mnt/data/silver/customers
```

---

### 6.2 Azure ADLS Connection

**Class:** `AzureADLS` in `odibi/connections/azure_adls.py`

```yaml
connections:
  adls_prod:
    type: azure_adls
    account: mystorageaccount
    container: datalake
    path_prefix: oee/prod
    auth_mode: key_vault
    key_vault_name: my-keyvault
    secret_name: storage-key
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `account` | str | Yes | Storage account name |
| `container` | str | Yes | Container/filesystem name |
| `path_prefix` | str | No | Prefix for all paths |
| `auth_mode` | str | Yes | Authentication mode (see below) |

**Auth Modes:**

| Mode | Required Parameters | Use Case |
|------|---------------------|----------|
| `key_vault` | `key_vault_name`, `secret_name` | Production (recommended) |
| `direct_key` | `account_key` | Development only |
| `sas_token` | `sas_token` | Limited access scenarios |
| `service_principal` | `tenant_id`, `client_id`, `client_secret` | Automation/CI |
| `managed_identity` | (none) | Databricks/Azure VMs |

**Path Resolution:**
```python
conn.get_path("silver/customers")
# Returns: abfss://datalake@mystorageaccount.dfs.core.windows.net/oee/prod/silver/customers
```

**Spark Configuration:**
```python
conn.configure_spark(spark)  # Sets fs.azure.* configs automatically
```

---

### 6.3 Azure SQL Connection

**Class:** `AzureSQL` in `odibi/connections/azure_sql.py`

```yaml
connections:
  sql_prod:
    type: azure_sql
    server: myserver.database.windows.net
    database: analytics
    auth_mode: aad_msi
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `server` | str | Yes | SQL server hostname |
| `database` | str | Yes | Database name |
| `driver` | str | No | Default: `ODBC Driver 18 for SQL Server` |
| `auth_mode` | str | Yes | `aad_msi`, `sql`, `key_vault` |
| `username` | str | For `sql` | SQL username |
| `password` | str | For `sql` | SQL password |
| `port` | int | No | Default: 1433 |

**Methods:**
```python
# Read data
df = conn.read_query("SELECT * FROM dbo.Customers")
df = conn.read_table("Customers", schema="dbo")

# Write data
conn.write_table(df, "Customers", schema="dbo", if_exists="replace")

# Execute statements
conn.execute("TRUNCATE TABLE dbo.Staging")

# Spark JDBC options
options = conn.get_spark_options()
spark.read.format("jdbc").options(**options).option("dbtable", "dbo.Customers").load()
```

---

### 6.4 HTTP Connection

**Class:** `HTTPConnection` in `odibi/connections/http.py`

```yaml
connections:
  api:
    type: http
    base_url: https://api.example.com
    headers:
      User-Agent: odibi-pipeline
    auth:
      mode: bearer
      token: ${API_TOKEN}
```

**Auth Modes:** `none`, `basic`, `bearer`, `api_key`

### 6.5 Local DBFS Connection

For Databricks DBFS paths:

```yaml
connections:
  dbfs:
    type: local_dbfs
    base_path: /dbfs/mnt/datalake
```

Supports: `dbfs:/`, `/dbfs/`, and mounted paths.

---

## 7. Write Configuration (Delta Lake Features)

### 7.1 Partitioning & Z-Ordering

For large tables, use partitioning and Z-ordering to optimize query performance:

```yaml
write:
  connection: gold_lake
  format: delta
  table: fact_sales
  mode: append

  # Partitioning: Physical folders (low-cardinality columns)
  partition_by: [country_code, txn_year_month]

  # Z-Ordering: Data clustering (high-cardinality columns)
  zorder_by: [customer_id, product_id]

  # Delta table properties
  table_properties:
    "delta.autoOptimize.optimizeWrite": "true"
    "delta.autoOptimize.autoCompact": "true"
```

| Feature | When to Use | Cardinality |
|---------|-------------|-------------|
| `partition_by` | Columns in WHERE clauses | Low (country, year_month) |
| `zorder_by` | Columns in JOINs/filters | High (customer_id, product_id) |

### 7.2 Schema Evolution

Allow adding new columns without breaking existing pipelines:

```yaml
write:
  connection: silver
  path: customers
  format: delta
  mode: overwrite
  merge_schema: true  # Enable schema evolution
```

### 7.3 Auto-Optimize (VACUUM & OPTIMIZE)

Automatically run Delta Lake maintenance after writes:

```yaml
write:
  connection: silver
  path: customers
  format: delta
  auto_optimize: true  # Use defaults (168 hours retention)

# OR with custom config:
write:
  connection: silver
  path: customers
  format: delta
  auto_optimize:
    enabled: true
    vacuum_retention_hours: 168  # 7 days (set to 0 to disable VACUUM)
```

### 7.4 Bronze Metadata Columns

Add lineage tracking columns during ingestion:

```yaml
write:
  connection: bronze
  table: customers
  mode: append
  add_metadata: true  # Adds all applicable columns

# OR selective:
write:
  connection: bronze
  table: customers
  mode: append
  add_metadata:
    extracted_at: true        # _extracted_at: pipeline timestamp
    source_file: true         # _source_file: filename (file sources)
    source_connection: false  # _source_connection: connection name
    source_table: false       # _source_table: table name (SQL sources)
```

### 7.5 Skip If Unchanged

Avoid redundant writes for snapshot tables:

```yaml
write:
  connection: silver
  path: reference_data
  format: delta
  mode: overwrite
  skip_if_unchanged: true           # Compares SHA256 hash of content
  skip_hash_columns: [id, name]     # Only hash these columns
  skip_hash_sort_columns: [id]      # Sort before hashing for determinism
```

### 7.6 Time Travel (Read)

Read historical versions of Delta tables:

```yaml
read:
  connection: silver
  path: customers
  format: delta
  time_travel:
    as_of_version: 10
    # OR
    as_of_timestamp: "2023-10-01T12:00:00Z"
```

### 7.7 Streaming Writes (Spark Structured Streaming)

Process data continuously with fault tolerance:

```yaml
write:
  connection: silver_lake
  format: delta
  table: events_stream
  streaming:
    output_mode: append          # append, update, complete
    checkpoint_location: /checkpoints/events_stream
    trigger:
      processing_time: "10 seconds"  # Micro-batch interval
      # OR: available_now: true      # Process all, then stop
    query_name: events_ingestion     # For monitoring
    await_termination: false
```

---

## 8. Alerts & Notifications

### 8.1 Alert Configuration

Send notifications to Slack, Teams, or webhooks on pipeline events:

```yaml
alerts:
  - type: slack
    url: "${SLACK_WEBHOOK_URL}"
    on_events:
      - on_failure
      - on_quarantine
      - on_gate_block
    metadata:
      throttle_minutes: 15  # Don't spam
      max_per_hour: 10
      channel: "#data-alerts"
```

### 8.2 Available Events

| Event | Trigger |
|-------|---------|
| `on_start` | Pipeline started |
| `on_success` | Pipeline completed successfully |
| `on_failure` | Pipeline failed |
| `on_quarantine` | Rows were quarantined |
| `on_gate_block` | Quality gate blocked pipeline |
| `on_threshold_breach` | A threshold was exceeded |

### 8.3 Alert Types

| Type | Use Case |
|------|----------|
| `slack` | Slack webhook |
| `teams` | Microsoft Teams webhook |
| `webhook` | Generic HTTP POST |

---

## 9. System Catalog (The Brain)

The System Catalog stores metadata about pipelines, runs, schemas, and lineage in Delta tables.

### 9.1 Configuration

```yaml
system:
  connection: adls_bronze        # Must be blob storage (supports Delta)
  path: _odibi_system
  environment: dev               # Tag for multi-environment queries

  # Optional: Sync to SQL Server for dashboards
  sync_to:
    connection: sql_server_prod
    schema_name: odibi_system
```

### 9.2 Meta Tables

| Table | Purpose |
|-------|---------|
| `meta_tables` | Table schemas and column info |
| `meta_runs` | Pipeline execution history |
| `meta_patterns` | Pattern configurations used |
| `meta_metrics` | Metric definitions and values |
| `meta_state` | High Water Mark and incremental state |
| `meta_pipelines` | Pipeline definitions |
| `meta_nodes` | Node configurations |
| `meta_schemas` | Schema versions |
| `meta_lineage` | Data lineage relationships |
| `meta_outputs` | Output table metadata |
| `meta_pipeline_runs` | Summary of pipeline runs |
| `meta_node_runs` | Summary of node runs |
| `meta_failures` | Failed runs with error details |
| `meta_daily_stats` | Daily execution statistics |
| `meta_pipeline_health` | Pipeline health scores |
| `meta_sla_status` | SLA compliance tracking |

### 9.3 Cost Tracking

```yaml
system:
  connection: adls_prod
  cost_per_compute_hour: 0.15    # Estimated $/hour
  databricks_billing_enabled: true  # Query actual DBR costs
```

---

## 10. OpenLineage Integration

Track data lineage to external systems (Marquez, Atlan, DataHub):

```yaml
lineage:
  url: "http://localhost:5000"    # OpenLineage API endpoint
  namespace: "my_project"
  api_key: "${OPENLINEAGE_API_KEY}"
```

**Events emitted:**
- Pipeline start/complete/fail
- Node inputs/outputs with schemas
- Parent-child run relationships

---

## 11. Foreign Key Validation

Validate referential integrity between fact and dimension tables:

```yaml
relationships:
  - name: orders_to_customers
    fact: fact_orders
    dimension: dim_customer
    fact_key: customer_sk
    dimension_key: customer_sk
    nullable: false
    on_violation: error  # warn, error, quarantine

  - name: orders_to_products
    fact: fact_orders
    dimension: dim_product
    fact_key: product_sk
    dimension_key: product_sk
```

**Behavior:**
- Validates FK columns exist in dimension tables
- Detects orphan records (FK not in dimension)
- Integrates with FactPattern automatically

---

## 12. Orchestration Export

Generate Airflow or Dagster DAGs from odibi pipelines:

### 12.1 Airflow Export

```bash
odibi export airflow --pipeline my_pipeline --output dags/
```

Generates a Python DAG file with:
- BashOperator tasks for each node
- Task dependencies from `depends_on`
- Retry configuration

### 12.2 Dagster Export

```bash
odibi export dagster --pipeline my_pipeline --output ops/
```

---

## 13. Manufacturing Transformers

Specialized transformers for process/batch manufacturing data:

### 13.1 Detect Sequential Phases

Analyze batch process phases from PLC timer columns:

```yaml
transform:
  - detect_sequential_phases:
      group_by: BatchID           # Or [BatchID, AssetID]
      timestamp_col: ts
      phases:
        - timer_col: LoadTime
        - timer_col: CookTime
        - timer_col: CoolTime
        - timer_col: UnloadTime
      start_threshold: 240        # Max seconds to consider valid start
      status_col: Status
      status_mapping:
        1: idle
        2: active
        3: hold
        4: faulted
      phase_metrics:
        Level: max               # Aggregate column during phase
      metadata:
        ProductCode: first_after_start
        Weight: max
```

**Use cases:**
- Batch reactor cycle analysis
- CIP (Clean-in-Place) phase timing
- Food processing cycle tracking
- Any multi-step batch process with PLC timers

**Output columns:**
- `phase_name`, `phase_start`, `phase_end`, `phase_duration_seconds`
- Status time breakdown: `time_in_idle`, `time_in_active`, etc.
- Aggregated metrics per phase

---

## 14. Semantic Layer

The semantic layer provides a metrics abstraction for BI consumption.

### 14.1 Metric Definitions

```yaml
# metrics.yaml
metrics:
  - name: total_revenue
    description: Total order revenue
    expr: "SUM(total_amount)"
    source: $build_warehouse.fact_orders

  - name: order_count
    description: Number of orders
    expr: "COUNT(*)"
    source: $build_warehouse.fact_orders

  - name: avg_order_value
    description: Average order value
    type: derived
    components: [total_revenue, order_count]
    formula: "total_revenue / order_count"
```

### 14.2 Dimension Definitions

```yaml
dimensions:
  - name: order_date
    source: $build_warehouse.fact_orders
    column: order_date
    grain: month  # day, week, month, quarter, year

  - name: customer_region
    source: $build_warehouse.dim_customer
    column: region
    hierarchy: [country, region, city]  # Drill-down path

  - name: fiscal_year
    source: $build_warehouse.dim_date
    expr: "YEAR(DATEADD(month, 6, date_key))"  # Custom expression
```

### 14.3 Semantic Queries

Query metrics using natural syntax:

```python
from odibi.semantics import SemanticQuery, parse_semantic_config

config = parse_semantic_config(yaml.safe_load(open("metrics.yaml")))
query = SemanticQuery(config)

# Execute query
result = query.execute(
    "total_revenue, order_count BY customer_region, order_date WHERE status = 'completed'",
    context
)

print(result.df)  # DataFrame with aggregated metrics
```

**Query Syntax:**
```
metric1, metric2 BY dimension1, dimension2 WHERE condition
```

### 14.4 Materialization

Pre-compute metrics for performance:

```yaml
materializations:
  - name: daily_sales_by_region
    metrics: [total_revenue, order_count]
    dimensions: [order_date, customer_region]
    output: gold.daily_sales_by_region
    schedule: "0 5 * * *"  # Daily at 5am
```

### 14.5 SQL Server Views

Generate views for analyst consumption:

```yaml
views:
  - name: vw_daily_sales
    description: Daily sales metrics
    metrics: [total_revenue, order_count, avg_order_value]
    dimensions: [order_date]
    db_schema: semantic
```

---

## 15. Story Generation

Stories are execution reports generated after each pipeline run.

### 15.1 Configuration

```yaml
story:
  enabled: true
  output_path: stories/  # Local or abfss:// path
  max_sample_rows: 10
  retention_days: 30
  retention_count: 100
```

### 15.2 Story Contents

Each story includes:
- **Pipeline metadata**: Name, start/end time, duration
- **Node details**: Status, row counts, duration, schema
- **Validation results**: Tests passed/failed
- **Data samples**: First N rows of output
- **Lineage**: Input/output relationships
- **DAG visualization**: Interactive dependency graph

### 15.3 Viewing Stories

```bash
# View last story
odibi story last

# View specific story
odibi story show stories/my_pipeline_2024-01-15_10-30-00.html

# View specific node
odibi story last --node dim_customers
```

### 15.4 Story Metadata

Available in `PipelineResults.story_path`:

```python
results = pm.run("my_pipeline")
print(f"Story: {results.story_path}")
```

### 15.5 Documentation Generation

Generate structured markdown documentation from Story artifacts:

```yaml
story:
  connection: local_data
  path: stories/

  docs:
    enabled: true
    output_path: docs/generated/   # Project-level docs location

    outputs:
      readme: true              # README.md - stakeholder-facing
      technical_details: true   # TECHNICAL_DETAILS.md - engineer-facing
      node_cards: true          # NODE_CARDS/*.md - per-node details
      run_memo: true            # Per-run summary (always generated)

    include:
      sql: true                 # Include executed SQL in node cards
      config_snapshot: true     # Include YAML config snapshots
      schema: true              # Include schema tables
```

> **Note:** For remote storage (ADLS), story paths like `abfss://container@account/...` are shown as-is. They're informational — use Azure Storage Explorer or Portal to browse.

**Generated Artifacts:**

| Artifact | Scope | Update Policy |
|----------|-------|---------------|
| README.md | Project | On successful runs only |
| TECHNICAL_DETAILS.md | Project | On successful runs only |
| NODE_CARDS/*.md | Project | On successful runs only |
| run_*_memo.md | Per-run | Every run (including failures) |

**File Structure:**
```
project/
├── docs/
│   └── generated/
│       ├── README.md              # Pipeline overview
│       ├── TECHNICAL_DETAILS.md   # Full execution details
│       └── node_cards/
│           ├── load_customers.md  # Per-node documentation
│           └── dim_customer.md
└── stories/
    └── my_pipeline/
        └── 2026-01-21/
            ├── run_14-30-00.html   # Interactive story
            ├── run_14-30-00.json   # Machine-readable
            └── run_14-30-00_memo.md  # Run summary
```

**README.md includes:**
- Pipeline name and layer badge
- Last run status and metrics
- Project context (project, plant, asset)
- Node summary table with links to NODE_CARDS
- Links to TECHNICAL_DETAILS and Story HTML

**NODE_CARDS include:**
- Node description (from config)
- Operation type and duration
- Schema in/out with changes highlighted
- Executed SQL (syntax highlighted)
- Transformation stack
- Validation warnings
- Config snapshot (YAML)

**RUN_MEMO includes:**
- Run status and timestamp
- What changed from last successful run
- Anomalies detected
- Failed node details with error messages
- Data quality issues

---

## 16. Common Workflows

### 16.1 Run a Pipeline and Query Intermediate Results (Spark)

```python
from odibi import PipelineManager

# Initialize
pm = PipelineManager("project.yaml")

# Run pipeline
results = pm.run("silver_pipeline")

# Query any node's output via Spark SQL
spark = pm.engine.spark
customers_df = spark.sql("SELECT * FROM dim_customers WHERE tier = 'Gold'")
customers_df.show()

# Or via context
df = pm.context.get("dim_customers")

# List all available nodes
print(pm.context.list_names())
```

### 16.2 Debug a Failing Node

```bash
# 1. Run with verbose logging
odibi run pipeline.yaml --log-level DEBUG

# 2. View the execution story
odibi story last

# 3. Inspect specific node
odibi story last --node failing_node_name

# 4. Run single node in isolation
odibi run pipeline.yaml --node failing_node_name --dry-run
```

**In Python:**
```python
pm = PipelineManager("project.yaml")
result = pm.run("pipeline")

# Get specific node result
node_result = result.get_node_result("failing_node")
print(node_result.error)  # Full error message
print(node_result.metadata.get("input_schema"))  # Schema at input
print(node_result.metadata.get("rows_read"))  # Row count at input

# Debug summary with next steps
print(result.debug_summary())
```

### 16.3 Add a New SCD2 Dimension

**Step 1: Create bronze node (raw data)**
```yaml
nodes:
  - name: bronze_customers
    read:
      connection: source_db
      table: customers
```

**Step 2: Create silver node (SCD2)**
```yaml
  - name: dim_customers
    depends_on: [bronze_customers]
    inputs:
      source: bronze_customers
    transform:
      - scd2:
          connection: adls_prod
          path: silver/dim_customers
          keys: [customer_id]
          track_cols: [name, email, address, tier]
          effective_time_col: updated_at
    write:
      connection: adls_prod
      path: silver/dim_customers
      format: delta
      mode: overwrite  # SCD2 returns full history!
```

**Step 3: Verify**
```python
spark.sql("""
  SELECT customer_id, name, valid_to, is_current
  FROM dim_customers
  WHERE customer_id = 123
  ORDER BY valid_to
""").show()
```

### 16.4 Create a Custom Transformer

**Step 1: Create the function**
```python
# my_transformers.py
from pydantic import BaseModel, Field
from odibi.context import EngineContext

class MyTransformParams(BaseModel):
    input_col: str = Field(..., description="Column to transform")
    output_col: str = Field(..., description="Output column name")

def my_custom_transform(context: EngineContext, params: MyTransformParams) -> EngineContext:
    sql = f"SELECT *, UPPER({params.input_col}) AS {params.output_col} FROM df"
    return context.sql(sql)
```

**Step 2: Register it**
```python
from odibi.registry import FunctionRegistry
from my_transformers import my_custom_transform, MyTransformParams

FunctionRegistry.register(my_custom_transform, "my_custom_transform", MyTransformParams)
```

**Step 3: Use in YAML**
```yaml
transform:
  - my_custom_transform:
      input_col: name
      output_col: name_upper
```

### 16.5 Run in Databricks

```python
# Databricks notebook cell

# Install odibi
%pip install odibi[azure]

# Get SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Configure pipeline with Spark engine
from odibi import PipelineManager

pm = PipelineManager(
    "project.yaml",
    engine="spark",
    spark_session=spark
)

# Run
results = pm.run("my_pipeline")

# Query results as temp views
spark.sql("SELECT * FROM node_name").display()

# Or write to Unity Catalog
spark.sql("CREATE TABLE catalog.schema.table AS SELECT * FROM node_name")
```

**Key Differences from Local:**
- Use `engine="spark"`
- Pass existing `spark_session`
- ADLS connections use `managed_identity` auth
- Node outputs are temp views queryable via `spark.sql()`

### 16.6 Incremental Ingestion with High Water Mark

```yaml
nodes:
  - name: bronze_orders
    read:
      connection: source_db
      table: orders
    incremental:
      mode: hwm              # High Water Mark
      column: updated_at
      state_key: orders_hwm  # Persisted in state store
    write:
      connection: bronze
      path: orders
      mode: append
```

**First Run:** Loads all data, stores max(updated_at) as HWM
**Subsequent Runs:** Filters `WHERE updated_at > {hwm}`, appends new rows

---

## 17. CLI Reference

### 17.1 Core Commands

```bash
# Run a pipeline
odibi run pipeline.yaml

# Run specific nodes
odibi run pipeline.yaml --node node_a --node node_b

# Filter by tag
odibi run pipeline.yaml --tag silver

# Dry run (validate without execution)
odibi run pipeline.yaml --dry-run

# Resume from failure
odibi run pipeline.yaml --resume

# Parallel execution
odibi run pipeline.yaml --parallel --workers 4

# Error handling
odibi run pipeline.yaml --on-error fail_later
```

### 17.2 Story Commands

```bash
odibi story last                    # View last execution
odibi story show path/to/story.html # View specific story
odibi story last --node node_name   # View specific node
```

### 17.3 Validation & Debug

```bash
odibi validate pipeline.yaml  # Validate configuration
odibi graph pipeline.yaml     # Show dependency graph
odibi graph pipeline.yaml --format ascii
odibi doctor                  # Check environment
```

### 17.4 Introspection Commands (AI-Friendly)

These commands help AI tools discover available features programmatically:

```bash
# List all available transformers
odibi list transformers
odibi list transformers --format json   # JSON output for parsing

# List all available patterns
odibi list patterns
odibi list patterns --format json

# List all connection types
odibi list connections
odibi list connections --format json

# Get detailed documentation for any feature
odibi explain fill_nulls      # Explain a transformer
odibi explain dimension       # Explain a pattern
odibi explain azure_sql       # Explain a connection type
```

**AI Workflow Example:**
```bash
# AI checks what's available before generating config
odibi list transformers --format json | jq '.[] | .name'

# AI looks up specific transformer usage
odibi explain derive_columns

# AI validates generated config
odibi validate generated_pipeline.yaml
```

### 17.5 Semantic Layer

```bash
odibi semantic run metrics.yaml     # Execute semantic layer
odibi semantic materialize metrics.yaml
```

---

## 18. Anti-Patterns and Gotchas

### 18.1 Node Naming

❌ **Wrong:**
```yaml
nodes:
  - name: dim-customers      # Hyphens break Spark SQL
  - name: fact sales         # Spaces break Spark SQL
  - name: customers.silver   # Dots break Spark SQL
```

✅ **Correct:**
```yaml
nodes:
  - name: dim_customers
  - name: fact_sales
  - name: customers_silver
```

**Error Message:** *"Invalid node name 'X' for Spark engine. Names must contain only alphanumeric characters and underscores"*

### 18.2 SCD2 Mode Must Be Overwrite

❌ **Wrong:**
```yaml
transformer: scd2
params:
  target: dim_customers
  ...
write:
  mode: append  # WRONG - duplicates history!
```

✅ **Correct:**
```yaml
transformer: scd2
params:
  target: dim_customers
  ...
write:
  mode: overwrite  # SCD2 returns FULL history
```

### 18.3 Missing depends_on for Joins

❌ **Wrong:**
```yaml
- name: fact_sales
  transform:
    - join:
        right_dataset: dim_customer  # Not in depends_on!
```

✅ **Correct:**
```yaml
- name: fact_sales
  depends_on: [dim_customer]  # Required!
  transform:
    - join:
        right_dataset: dim_customer
```

**Error Message:** *"Join failed: dataset 'dim_customer' not found in context"*

### 18.4 Engine-Specific SQL Syntax

| Feature | Pandas (DuckDB) | Spark |
|---------|-----------------|-------|
| Exclude columns | `SELECT * EXCLUDE (col)` | `SELECT * EXCEPT (col)` |
| String contains | `ILIKE '%pattern%'` | `LIKE '%pattern%'` |
| Array explode | `UNNEST(arr)` | `explode(arr)` |
| Null-safe compare | `IS DISTINCT FROM` | `IS DISTINCT FROM` (both support) |

**Transformers handle this automatically** - prefer using transformers over raw SQL.

### 18.5 Performance Gotchas

| Issue | Solution |
|-------|----------|
| Pandas OOM on large data | Use Spark engine or chunked processing |
| Slow Spark startup | Reuse SparkSession, use `--parallel` |
| Repeated Delta reads | Cache with `df.cache()` or persist |
| Large quarantine tables | Use `max_rows` and `sample_fraction` |

### 18.6 Common Error Messages

| Error | Cause | Fix |
|-------|-------|-----|
| *"DataFrame 'X' not found in context"* | Missing `depends_on` | Add to `depends_on` list |
| *"Column 'X' not found"* | Typo or missing column | Check schema, run `--dry-run` |
| *"SCD2: provide either 'target' OR both 'connection' and 'path'"* | Invalid SCD2 config | Use one approach, not both |
| *"Key Vault fetch timed out"* | Network/auth issue | Check VPN, credentials |

---

## 19. SQL Server Writer

### 19.1 Merge Operations

Write to SQL Server with MERGE (upsert) semantics:

```yaml
write:
  connection: azure_sql
  format: sql_server
  table: sales.fact_orders
  mode: merge
  merge_keys: [DateId, store_id]
  merge_options:
    update_condition: "source._hash_diff != target._hash_diff"
    delete_condition: "source._is_deleted = 1"
    insert_condition: "source.is_valid = 1"
    exclude_columns: [_hash_diff]
    staging_schema: staging
    audit_cols:
      created_col: created_ts
      updated_col: updated_ts
    auto_create_table: true
    primary_key_on_merge_keys: true
```

**Merge Options:**
| Option | Description |
|--------|-------------|
| `update_condition` | Only update rows matching this SQL condition |
| `delete_condition` | Delete rows matching this condition (soft delete) |
| `insert_condition` | Only insert rows matching this condition |
| `exclude_columns` | Columns to exclude from target table |
| `staging_schema` | Schema for staging table (default: `staging`) |
| `auto_create_table` | Auto-create target if not exists |
| `auto_create_schema` | Auto-create schema if not exists |
| `batch_size` | Chunk large DataFrames for memory efficiency |

### 19.2 Overwrite Strategies

```yaml
write:
  connection: azure_sql
  format: sql_server
  table: fact.combined_downtime
  mode: overwrite
  overwrite_options:
    strategy: truncate_insert  # or: drop_create, delete_insert
    audit_cols:
      created_col: created_ts
      updated_col: updated_ts
```

| Strategy | Behavior | Use Case |
|----------|----------|----------|
| `truncate_insert` | TRUNCATE then INSERT | Fastest, requires TRUNCATE permission |
| `drop_create` | DROP, CREATE, INSERT | Refreshes schema |
| `delete_insert` | DELETE then INSERT | Works with limited permissions |

---

## 20. Incremental Loading (Advanced)

### 20.1 Stateful High Water Mark

Track the last processed value for exact incremental loading:

```yaml
read:
  connection: source_db
  table: orders
incremental:
  mode: stateful           # Track HWM in system catalog
  column: updated_at
  state_key: orders_hwm    # Unique ID for state tracking
  watermark_lag: "2h"      # Safety buffer for late-arriving data
```

**Watermark Lag:** Subtracts this duration from stored HWM when filtering. Use when source has replication lag or eventual consistency.

### 20.2 Rolling Window

Load recent data without state tracking:

```yaml
incremental:
  mode: rolling_window
  column: updated_at
  lookback: 3
  unit: day  # hour, day, month, year
```

### 20.3 Date Format Handling

For string columns with specific date formats:

```yaml
incremental:
  mode: rolling_window
  column: EVENT_START
  lookback: 3
  unit: day
  date_format: oracle  # DD-MON-YY format
```

**Supported formats:**
| Format | Pattern | Use Case |
|--------|---------|----------|
| `oracle` | DD-MON-YY | Oracle databases |
| `oracle_sqlserver` | DD-MON-YY in SQL Server | Legacy migrations |
| `sql_server` | CONVERT style 120 | SQL Server |
| `us` | MM/DD/YYYY | US format |
| `eu` | DD/MM/YYYY | European format |
| `iso` | YYYY-MM-DDTHH:MM:SS | ISO 8601 |

---

## 21. Diagnostics & Diff Tools

### 21.1 Delta Table Diff

Compare two versions of a Delta table:

```python
from odibi.diagnostics.delta import get_delta_diff

result = get_delta_diff(
    table_path="abfss://lake/silver/customers",
    version_a=10,
    version_b=15,
    spark=spark,
    deep=True,           # Row-by-row comparison
    keys=["customer_id"] # For detecting updates
)

print(f"Rows changed: {result.rows_change}")
print(f"Schema added: {result.schema_added}")
print(f"Schema removed: {result.schema_removed}")
print(f"Operations: {result.operations}")
```

### 21.2 Run Diff

Compare two pipeline runs to identify drift:

```python
from odibi.diagnostics.diff import diff_nodes

result = diff_nodes(node_a_metadata, node_b_metadata)

if result.has_drift:
    print(f"Status change: {result.status_change}")
    print(f"Rows diff: {result.rows_diff}")
    print(f"Columns added: {result.columns_added}")
    print(f"Columns removed: {result.columns_removed}")
```

---

## 22. Cross-Check Transformer

Validate data across multiple nodes:

```yaml
transform:
  - cross_check:
      type: row_count_diff
      inputs: [node_a, node_b]
      threshold: 0.05  # Allow 5% difference

  - cross_check:
      type: schema_match
      inputs: [staging_orders, prod_orders]
```

**Check Types:**
| Type | Behavior |
|------|----------|
| `row_count_diff` | Compare row counts, fail if diff exceeds threshold |
| `schema_match` | Ensure schemas are identical |

---

## 23. Testing Utilities

### 23.1 Test Fixtures

Generate sample data for tests:

```python
from odibi.testing.fixtures import generate_sample_data, temp_directory

# Generate sample DataFrame
df = generate_sample_data(
    rows=100,
    engine_type="pandas",  # or "spark"
    schema={"id": "int", "value": "float", "category": "str", "date": "date"}
)

# Temporary directory for test artifacts
with temp_directory() as temp_dir:
    df.to_parquet(f"{temp_dir}/test.parquet")
```

### 23.2 Assertions

Assert DataFrame equality:

```python
from odibi.testing.assertions import assert_frame_equal, assert_schema_equal

# Compare DataFrames (works with Pandas and Spark)
assert_frame_equal(actual_df, expected_df, check_dtype=True)

# Compare schemas only
assert_schema_equal(actual_df, expected_df)
```

---

## 24. Derived Updater (Internal)

The DerivedUpdater provides exactly-once semantics for system catalog updates:

- **Guard table:** `meta_derived_applied_runs` prevents duplicate processing
- **Retry with backoff:** Handles Delta concurrency conflicts
- **Stale claim recovery:** Reclaims stuck jobs after timeout

**Valid derived tables:**
- `meta_daily_stats` - Daily execution statistics
- `meta_pipeline_health` - Pipeline health scores
- `meta_sla_status` - SLA compliance tracking

---

## 25. Extension Points

### 25.1 Custom Patterns

```python
from odibi.patterns.base import Pattern
from odibi.context import EngineContext

class MyPattern(Pattern):
    def validate(self) -> None:
        if not self.params.get("required_param"):
            raise ValueError("MyPattern: 'required_param' is required")

    def execute(self, context: EngineContext) -> Any:
        # Transform and return DataFrame
        return transformed_df
```

### 25.2 Custom Connections

```python
from odibi.connections.base import BaseConnection

class MyConnection(BaseConnection):
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.api_key = api_key

    def get_path(self, relative_path: str) -> str:
        return f"{self.base_url}/{relative_path}"

    def validate(self) -> None:
        if not self.api_key:
            raise ValueError("API key required")
```

### 25.3 Plugin System

Register via entry points in `pyproject.toml`:
```toml
[project.entry-points."odibi.connections"]
my_connector = "my_package.connections:MyConnectionFactory"
```

Plugins discovered via:
```python
from odibi.plugins import load_plugins
load_plugins()  # Called automatically on startup
```

---

## 26. Quick Reference Cheat Sheet

### YAML Node Structure
```yaml
nodes:
  - name: node_name              # Required, alphanumeric + underscore
    depends_on: [other_node]     # Dependencies for execution order
    tags: [silver, daily]        # For filtering with --tag
    inputs:
      source: other_node         # Input from another node
    read:                        # OR read from connection
      connection: conn_name
      path: relative/path
      format: parquet            # parquet, delta, csv, json
    transform:                   # List of transformations
      - filter_rows:
          condition: "col > 0"
      - derive_columns:
          derivations:
            new_col: "expr"
    pattern:                     # OR use a pattern
      type: dimension
      params: {...}
    validate:
      tests:
        - type: not_null
          columns: [id]
    write:                       # Persist output
      connection: conn_name
      path: output/path
      format: delta
      mode: overwrite            # overwrite, append, merge
      partition_by: [year, month] # Physical partitioning
      zorder_by: [customer_id]   # Z-ordering (Delta only)
      merge_schema: true         # Allow schema evolution
      auto_optimize: true        # Run VACUUM/OPTIMIZE after write
      add_metadata: true         # Add _extracted_at, _source_file
```

### Common Transform Operations

| Task | Transformer |
|------|-------------|
| Filter rows | `filter_rows: {condition: "..."}` |
| Add column | `derive_columns: {derivations: {col: expr}}` |
| Remove duplicates | `deduplicate: {keys: [...], order_by: "..."}` |
| Join tables | `join: {right_dataset: x, on: [...], how: left}` |
| Aggregate | `aggregate: {group_by: [...], aggregations: {...}}` |
| Type casting | `cast_columns: {casts: {col: type}}` |
| Rename columns | `rename_columns: {mapping: {old: new}}` |
| SCD2 history | `scd2: {target: ..., keys: [...], track_cols: [...]}` |
| Detect deletes | `detect_deletes: {mode: sql_compare, keys: [...]}` |

### Query Node Results (Spark)

```python
# After pipeline.run()
spark.sql("SELECT * FROM node_name")
spark.sql("SELECT * FROM node_name WHERE condition")

# Get DataFrame directly
df = context.get("node_name")

# List all available nodes
context.list_names()
```

### Debug Commands

```bash
odibi run x.yaml --dry-run      # Validate only
odibi run x.yaml --log-level DEBUG
odibi story last                # View execution story
odibi graph x.yaml              # View dependencies
odibi doctor                    # Check environment
```

### Validation Quick Reference

```yaml
validate:
  tests:
    - type: not_null
      columns: [id]
      on_fail: fail          # fail, warn, quarantine
    - type: unique
      columns: [id]
    - type: accepted_values
      column: status
      values: [A, B, C]
    - type: range
      column: amount
      min: 0
      max: 1000000
    - type: freshness
      column: updated_at
      max_age: "24h"
```

### Delta Lake Quick Reference

```yaml
write:
  # Performance optimization
  partition_by: [country, year_month]  # Low cardinality columns
  zorder_by: [customer_id, product_id] # High cardinality columns

  # Schema evolution
  merge_schema: true

  # Auto maintenance (VACUUM/OPTIMIZE)
  auto_optimize: true
  # OR with custom retention:
  auto_optimize:
    enabled: true
    vacuum_retention_hours: 168  # 7 days

  # Skip redundant writes
  skip_if_unchanged: true
  skip_hash_columns: [id, name]

  # Bronze metadata
  add_metadata: true  # Adds _extracted_at, _source_file

read:
  # Time travel
  time_travel:
    as_of_version: 10
    # OR: as_of_timestamp: "2023-10-01T12:00:00Z"
```

### Alerts Quick Reference

```yaml
alerts:
  - type: slack              # slack, teams, webhook
    url: "${SLACK_WEBHOOK}"
    on_events: [on_failure, on_quarantine, on_gate_block]
    metadata:
      throttle_minutes: 15
      channel: "#data-alerts"
```

### Connection Quick Reference

```yaml
connections:
  local:
    type: local
    base_path: ./data

  adls:
    type: azure_adls
    account: myaccount
    container: datalake
    auth_mode: managed_identity

  sql:
    type: azure_sql
    server: server.database.windows.net
    database: db
    auth_mode: aad_msi
```

---

## 27. Documentation Map

This section lists all detailed documentation files. Use `get_doc("path")` to retrieve any doc.

### Core Reference
- `docs/reference/yaml_schema.md` - Complete YAML configuration reference
- `docs/reference/cheatsheet.md` - Quick reference card
- `docs/reference/configuration.md` - Configuration options
- `docs/reference/glossary.md` - Term definitions
- `docs/troubleshooting.md` - Common issues and solutions
- `docs/golden_path.md` - Recommended workflow

### Patterns (16 docs)
- `docs/patterns/dimension.md` - Dimension pattern details
- `docs/patterns/fact.md` - Fact table pattern
- `docs/patterns/scd2.md` - SCD Type 2 implementation
- `docs/patterns/aggregation.md` - Aggregation pattern
- `docs/patterns/date_dimension.md` - Date dimension generation
- `docs/patterns/merge_upsert.md` - Merge/upsert pattern
- `docs/patterns/incremental_stateful.md` - Stateful incremental loads
- `docs/patterns/smart_read.md` - Smart read with HWM
- `docs/patterns/sql_server_merge.md` - SQL Server MERGE statement
- `docs/patterns/anti_patterns.md` - What NOT to do

### Guides (21 docs)
- `docs/guides/best_practices.md` - Best practices
- `docs/guides/writing_transformations.md` - How to write transforms
- `docs/guides/dimensional_modeling_guide.md` - Star schema design
- `docs/guides/python_api_guide.md` - Python API usage
- `docs/guides/cli_master_guide.md` - CLI deep dive
- `docs/guides/testing.md` - Testing strategies
- `docs/guides/performance_tuning.md` - Performance optimization
- `docs/guides/production_deployment.md` - Production setup
- `docs/guides/secrets.md` - Secret management
- `docs/guides/environments.md` - Environment configuration

### Features (22 docs)
- `docs/features/transformers.md` - Transformer system
- `docs/features/validation.md` - Data validation
- `docs/features/quarantine.md` - Quarantine system
- `docs/features/quality_gates.md` - Quality gates
- `docs/features/connections.md` - Connection types
- `docs/features/engines.md` - Pandas/Spark/Polars engines
- `docs/features/patterns.md` - Pattern overview
- `docs/features/lineage.md` - Data lineage
- `docs/features/stories.md` - Execution stories
- `docs/features/catalog.md` - System catalog

### Tutorials (19 docs)
- `docs/tutorials/getting_started.md` - Getting started
- `docs/tutorials/bronze_layer.md` - Bronze layer tutorial
- `docs/tutorials/silver_layer.md` - Silver layer tutorial
- `docs/tutorials/gold_layer.md` - Gold layer tutorial
- `docs/tutorials/spark_engine.md` - Using Spark
- `docs/tutorials/dimensional_modeling/` - Dimensional modeling series (12 parts)

### Context Phases (14 docs)
Deep-dive implementation context for each system component:
- `docs/context/PHASE_1_CORE_EXECUTION.md` - Core execution
- `docs/context/PHASE_2_PATTERNS.md` - Pattern system
- `docs/context/PHASE_3_TRANSFORMERS.md` - Transformer implementation
- `docs/context/PHASE_4_CONNECTIONS.md` - Connection system
- `docs/context/PHASE_5_RUNTIME.md` - Runtime behavior
- `docs/context/PHASE_6_WORKFLOWS.md` - Common workflows
- `docs/context/PHASE_7_GOTCHAS.md` - Gotchas and edge cases
- `docs/context/PHASE_8_CLI.md` - CLI implementation
- `docs/context/PHASE_9_EXTENSIONS.md` - Extension points

### Examples (10 docs)
- `docs/examples/canonical/01_hello_world.md` - Hello world
- `docs/examples/canonical/02_incremental_sql.md` - Incremental loading
- `docs/examples/canonical/03_scd2_dimension.md` - SCD2 example
- `docs/examples/canonical/04_fact_table.md` - Fact table example
- `docs/examples/canonical/05_full_pipeline.md` - Full pipeline
- `docs/examples/canonical/THE_REFERENCE.md` - Reference implementation

### Validation (4 docs)
- `docs/validation/README.md` - Validation overview
- `docs/validation/tests.md` - Validation tests
- `docs/validation/contracts.md` - Data contracts
- `docs/validation/fk.md` - Foreign key validation

### Semantic Layer (6 docs)
- `docs/semantics/index.md` - Semantic layer overview
- `docs/semantics/metrics.md` - Metric definitions
- `docs/semantics/query.md` - Querying metrics
- `docs/semantics/materialize.md` - Materialization
- `docs/semantics/runner.md` - Semantic runner

> **MCP Usage:** Call `list_docs(category="patterns")` to list all pattern docs, then `get_doc("docs/patterns/scd2.md")` to get specific content.
