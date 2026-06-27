# Tutorial: Building a Validation Pipeline

> **Level:** Intermediate  
> **Time:** 30 minutes  
> **Prerequisites:** Basic odibi familiarity (read/transform/write pipeline)

This tutorial walks through building a **Bronze → Silver** pipeline with full data quality
enforcement using odibi's validation system. You'll learn to configure all 11 test types,
quarantine bad rows, set quality gate thresholds, and interpret results.

---

## What You'll Build

```
Raw CSV  →  Transform  →  Validate  →  Quality Gate  →  Write Silver
                               │              │
                               ▼              ▼
                          Quarantine     Abort/Warn
```

A pipeline that:
1. Reads raw CSV data with known quality issues
2. Applies basic transformations (clean text, cast types)
3. Validates output against 11 different test types
4. Routes failed rows to a quarantine table
5. Applies a quality gate before writing clean data

---

## Step 1: Understand the Data

We have a `customers.csv` file with intentional quality issues:

| Issue | Column | Example |
|-------|--------|---------|
| Nulls | `customer_id`, `email` | Missing required values |
| Duplicates | `customer_id` | Same ID appears twice |
| Invalid values | `status` | "deleted" not in allowed list |
| Out of range | `age` | Age = -5 or 200 |
| Bad format | `email` | "not-an-email" |
| Stale data | `updated_at` | Timestamp from 2020 |

---

## Step 2: Configure Validation Tests

odibi supports **11 test types** organized into four categories:

### Column-Level Tests

```yaml
validation:
  tests:
    # 1. NOT_NULL — required fields must not be empty
    - type: not_null
      columns: [customer_id, name, email]
      on_fail: quarantine

    # 2. UNIQUE — primary keys must be unique
    - type: unique
      columns: [customer_id]
      on_fail: quarantine

    # 3. ACCEPTED_VALUES — categorical fields must be in allowed set
    - type: accepted_values
      column: status
      values: [active, inactive, pending]
      on_fail: quarantine

    # 4. RANGE — numeric fields must be within bounds
    - type: range
      column: age
      min: 0
      max: 150
      on_fail: quarantine

    # 5. REGEX_MATCH — string fields must match a pattern
    - type: regex_match
      column: email
      pattern: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
      on_fail: quarantine
```

### Batch-Level Tests

```yaml
    # 6. ROW_COUNT — batch must have expected number of rows
    - type: row_count
      min: 1
      max: 1000000

    # 7. FRESHNESS — data must be recent
    - type: freshness
      column: updated_at
      max_age: "48h"

    # 8. VOLUME_DROP — detect sudden drops vs history
    - type: volume_drop
      threshold: 0.5
      lookback_days: 7
      on_fail: warn
```

### Schema & Distribution Tests

```yaml
    # 9. SCHEMA — validate column presence
    - type: schema
      strict: true

    # 10. DISTRIBUTION — statistical monitoring
    - type: distribution
      column: age
      metric: mean
      threshold: ">18"
      on_fail: warn

    # 11. CUSTOM_SQL — any business rule as SQL
    - type: custom_sql
      name: valid_dates
      condition: "created_at <= updated_at"
      threshold: 0.01
```

### Severity Levels (`on_fail`)

| Level | Behavior |
|-------|----------|
| `fail` | Test failure stops the pipeline (default) |
| `warn` | Failure is logged but data passes through |
| `quarantine` | Failed rows are routed to quarantine table |

---

## Step 3: Configure Quarantine

Quarantine captures failed rows with metadata for debugging:

```yaml
validation:
  quarantine:
    connection: quarantine_store
    path: customers_quarantine
    add_columns:
      _rejection_reason: true    # Why the row failed
      _rejected_at: true         # When it was quarantined
      _source_batch_id: true     # Run ID for traceability
      _failed_tests: true        # Which tests failed
    retention_days: 90           # Auto-cleanup after 90 days
    max_rows: 10000              # Cap per run (prevents storage explosion)
```

**Key design choices:**
- Set `on_fail: quarantine` on row-level tests (not_null, unique, range, etc.)
- Batch-level tests (row_count, freshness) use `fail` or `warn` — they can't quarantine individual rows
- `max_rows` prevents a bad batch from filling your quarantine table

---

## Step 4: Configure Quality Gate

The quality gate is the final checkpoint before writing:

```yaml
validation:
  gate:
    require_pass_rate: 0.95      # 95% of rows must pass ALL tests
    on_fail: abort               # Stop pipeline, write nothing

    thresholds:                  # Per-test overrides
      - test: not_null
        min_pass_rate: 0.99      # Stricter for required fields
      - test: unique
        min_pass_rate: 1.0       # Zero tolerance for duplicates

    row_count:                   # Anomaly detection
      min: 100
      max: 1000000
      change_threshold: 0.5      # Alert if ±50% change
```

### Gate Actions

| Action | What Happens |
|--------|-------------|
| `abort` | Pipeline stops — no data written |
| `warn_and_write` | Warning logged, all data written anyway |
| `write_valid_only` | Only rows passing validation are written |

### When the Gate Fails

1. **Investigate:** Check quarantine table for failed row patterns
2. **Decide:** Fix source data, adjust thresholds, or use `warn_and_write`
3. **Rerun:** Pipeline is idempotent — safe to retry after fixing

---

## Step 5: Complete Pipeline YAML

See `examples/validation_pipeline/config.yaml` for the full configuration.
The file is a valid `ProjectConfig` — runnable via `odibi run config.yaml`.

```yaml
project: customer_quality_example
engine: pandas

connections:
  raw:
    type: local
    base_path: ./data
  silver:
    type: local
    base_path: ./output/silver
  quarantine_store:
    type: local
    base_path: ./output/quarantine

story:
  connection: silver
  path: stories/

system:
  connection: silver
  path: _odibi_system

pipelines:
  - pipeline: customer_quality
    layer: silver

    nodes:
      - name: clean_customers
    read:
      connection: raw
      path: bad_data.csv
      format: csv

    transform:
      steps:
        - function: clean_text
          params:
            columns: [name, email]
            trim: true
                case: lower

    validation:
      tests:
        - type: not_null
          columns: [customer_id, name, email]
          on_fail: quarantine
        - type: unique
          columns: [customer_id]
          on_fail: quarantine
        - type: accepted_values
          column: status
          values: [active, inactive, pending]
          on_fail: quarantine
        - type: range
          column: age
          min: 0
          max: 150
          on_fail: quarantine
        - type: regex_match
          column: email
          pattern: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
          on_fail: quarantine
        - type: row_count
          min: 1
          max: 1000000
        - type: freshness
          column: updated_at
          max_age: "720h"
        - type: schema
          strict: false
        - type: distribution
          column: age
          metric: mean
          threshold: ">18"
          on_fail: warn
        - type: custom_sql
          name: valid_dates
          condition: "created_at <= updated_at"
          threshold: 0.01
        - type: volume_drop
          threshold: 0.5
          lookback_days: 7
          on_fail: warn

      quarantine:
        connection: quarantine_store
        path: customers_quarantine
        add_columns:
          _rejection_reason: true
          _rejected_at: true
          _failed_tests: true

      gate:
        require_pass_rate: 0.95
        on_fail: abort
        row_count:
          min: 1

    write:
      connection: silver
      path: customers
      format: parquet
      mode: overwrite
```

---

## Step 6: Run and Interpret Results

### Programmatic Usage

```python
from odibi.validation.engine import Validator
from odibi.validation.quarantine import split_valid_invalid
from odibi.validation.gate import evaluate_gate
from odibi.config import ValidationConfig, GateConfig

# 1. Run validation
validator = Validator()
errors = validator.validate(df, validation_config)
# errors = [] means all passed

# 2. Split valid/invalid rows
quarantine_result = split_valid_invalid(df, tests, engine)
print(f"Valid: {quarantine_result.rows_valid}")
print(f"Quarantined: {quarantine_result.rows_quarantined}")

# 3. Evaluate quality gate
gate_result = evaluate_gate(
    df=df,
    validation_results=per_row_results,
    gate_config=gate_config,
    engine=engine,
)
if not gate_result.passed:
    print(f"GATE FAILED: {gate_result.failure_reasons}")
    print(f"Pass rate: {gate_result.pass_rate:.1%}")
```

### Understanding Validation Output

```
Starting validation: 11 tests, engine=pandas
  ✓ not_null: PASS (0 nulls in customer_id, name, email)
  ✗ unique: FAIL — Column 'customer_id' is not unique
  ✗ accepted_values: FAIL — Found: ['deleted']
  ✗ range: FAIL — 2 values out of range in 'age'
  ✗ regex_match: FAIL — 1 value doesn't match pattern in 'email'
  ✓ row_count: PASS (20 rows, min=1, max=1000000)
  ✓ freshness: PASS (max timestamp within 720h)
  ✓ schema: PASS
  ✓ distribution: PASS (mean age = 35.2 > 18)
  ✗ custom_sql: FAIL — 1 row where created_at > updated_at
  ✓ volume_drop: PASS (skipped — no history)

Validation complete: 11 tests, 6 passed, 5 failed

Quarantine: 5 rows routed to quarantine table
Gate: 75% pass rate < 95% required → ABORT
```

---

## Layer Presets

Use these as starting points for each pipeline layer:

| Layer | Typical Tests | Gate Threshold | Quarantine |
|-------|--------------|----------------|------------|
| **Bronze** (90%) | `not_null` on keys, `row_count` min, `schema` | 90% | Optional |
| **Silver** (95%) | All column tests, `freshness`, `schema`, `regex_match` | 95% | Enabled |
| **Gold** (99%) | All tests + `custom_sql` business rules, `distribution` | 99% | Enabled |

### Bronze Example
```yaml
gate:
  require_pass_rate: 0.90
  on_fail: warn_and_write  # Don't block Bronze ingestion
```

### Silver Example
```yaml
gate:
  require_pass_rate: 0.95
  on_fail: abort
  thresholds:
    - test: not_null
      min_pass_rate: 0.99
```

### Gold Example
```yaml
gate:
  require_pass_rate: 0.99
  on_fail: abort
  thresholds:
    - test: unique
      min_pass_rate: 1.0
    - test: not_null
      min_pass_rate: 1.0
```

---

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| Quarantine defined but no tests use `on_fail: quarantine` | Add `on_fail: quarantine` to row-level tests |
| Gate threshold too low on first setup | Start at 95%, tighten as data quality improves |
| Skipping `freshness` test | Stale data passes all column checks but is still wrong |
| Using `fail` severity for soft checks | Use `warn` for optional fields, `quarantine` for fixable issues |
| Not checking quarantine table after runs | Schedule regular quarantine review — it reveals source issues |

---

## Next Steps

- **Run the example:** See `examples/validation_pipeline/README.md`
- **Databricks test:** See `campaign/10_validation_e2e.py` for cross-engine verification
- **Add FK validation:** Use `fk_checks` for referential integrity
- **Set up alerts:** Configure `alerts` to notify on gate failures
