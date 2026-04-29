# Skill 08 — Validation Workflow

> **Layer:** Quality
> **When:** Adding data quality checks to a pipeline.
> **Key modules:** `odibi/validation/engine.py`, `quarantine.py`, `gate.py`, `fk.py`

---

## Purpose

Odibi provides a complete data quality chain: validate individual rows, quarantine failures, and apply batch-level quality gates. This skill teaches the full workflow.

---

## The Validation Chain

```
Input DataFrame
       │
       ▼
┌─────────────┐
│  Validator   │  11 test types × 3 engines
│  .validate() │  Returns: list of error messages
└──────┬──────┘
       │
       ▼
┌──────────────────┐
│  Quarantine       │  Split valid/invalid rows
│  split_valid_     │  Add metadata (test name, timestamp, reason)
│  invalid()        │  Write quarantine table (optional sampling)
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│  Quality Gate     │  Batch-level threshold check
│  evaluate_gate()  │  Actions: abort | warn | skip
│                   │  Row count anomaly detection
└──────┬───────────┘
       │
       ▼
  Valid DataFrame → Write to target
```

---

## Step 1: Define Validation Tests in YAML

```yaml
validation:
  tests:
    # Column-level checks
    - type: not_null
      column: customer_id
      severity: error           # error (default) | warn

    - type: unique
      column: email

    - type: accepted_values
      column: status
      values: [active, inactive, pending]

    - type: range
      column: age
      min: 0
      max: 150

    - type: regex_match
      column: email
      pattern: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"

    # Batch-level checks
    - type: row_count
      min: 1
      max: 1000000

    - type: freshness
      column: updated_at
      max_age: "24h"

    # Schema check
    - type: schema
      columns:
        customer_id: integer
        name: string
        email: string

    # Custom SQL
    - type: custom_sql
      query: "SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) = 0"
      description: "No negative amounts"

  # Fail-fast mode — stop on first failure
  fail_fast: false
```

### Test Type Reference

| Type | Required Fields | Description |
|------|----------------|-------------|
| `not_null` | `column` | Column must have no nulls |
| `unique` | `column` | Column values must be unique |
| `accepted_values` | `column`, `values` | Column values in allowed set |
| `row_count` | `min` and/or `max` | Row count within bounds |
| `range` | `column`, `min` and/or `max` | Numeric values within bounds |
| `regex_match` | `column`, `pattern` | Values match regex pattern |
| `custom_sql` | `query` | Custom SQL expression evaluates true |
| `schema` | `columns` | Schema matches expected types |
| `freshness` | `column`, `max_age` | Most recent timestamp within age limit |

### Severity Levels
- `error` (default) — test failure is recorded, row may be quarantined
- `warn` — test failure is logged but row passes through

---

## Step 2: Configure Quarantine (Optional)

```yaml
validation:
  quarantine:
    enabled: true
    connection: local           # Where to write quarantine data
    path: quarantine/customers
    format: parquet             # csv | json | parquet

    columns:                    # Customize metadata columns
      test_name: _test_name
      test_timestamp: _quarantine_ts
      failure_reason: _failure_reason

    sampling:                   # For high-volume failures
      max_rows: 10000
      strategy: first           # first | random
```

**What quarantine does:**
1. `split_valid_invalid()` separates the DataFrame into valid and invalid rows
2. `add_quarantine_metadata()` adds columns to invalid rows (test name, timestamp, reason)
3. Invalid rows are written to the quarantine location
4. Valid rows continue to the write step

---

## Step 3: Configure Quality Gate (Optional)

```yaml
validation:
  gate:
    min_pass_rate: 0.95         # 95% of rows must pass all tests
    on_fail: abort              # abort | warn | skip

    row_count_check:
      enabled: true
      max_change_percent: 50    # Alert if row count changes > 50%
```

**Gate actions:**
| Action | Behavior |
|--------|----------|
| `abort` | Pipeline stops — no data written |
| `warn` | Warning logged, data written anyway |
| `skip` | Node skipped, no data written, pipeline continues |

**GateResult fields:**
- `passed: bool` — did the gate pass?
- `pass_rate: float` — percentage of rows passing
- `total_rows: int`, `passed_rows: int`, `failed_rows: int`
- `action: GateOnFail` — what to do on failure
- `failure_reasons: list[str]` — why the gate failed

---

## Step 4: FK Validation (Optional)

```yaml
validation:
  fk_checks:
    - column: customer_sk
      parent_table: gold/dim_customer
      parent_column: customer_sk
      action: reject             # reject | unknown | quarantine
```

**Actions:**
- `reject` — rows with missing FK are excluded
- `unknown` — missing FK mapped to unknown member (SK=0)
- `quarantine` — missing FK rows sent to quarantine

---

## Using Validation in Code

### Running Validation Programmatically

```python
from odibi.validation.engine import Validator
from odibi.config import ValidationConfig, TestConfig, TestType

# Define tests
config = ValidationConfig(
    tests=[
        TestConfig(type=TestType.NOT_NULL, column="id"),
        TestConfig(type=TestType.UNIQUE, column="email"),
        TestConfig(type=TestType.ROW_COUNT, min=1),
    ]
)

# Run validation
validator = Validator()
errors = validator.validate(df, config)
# errors is a list of error message strings (empty = all passed)
```

### Quarantine Split

```python
from odibi.validation.quarantine import split_valid_invalid, add_quarantine_metadata

result = split_valid_invalid(df, test_results, config)
# result.valid_df — rows that passed all tests
# result.invalid_df — rows that failed one or more tests
# result.rows_quarantined — count of quarantined rows
# result.rows_valid — count of valid rows
```

### Quality Gate

```python
from odibi.validation.gate import evaluate_gate

gate_result = evaluate_gate(
    df=df,
    validation_results=test_results,
    gate_config=gate_config,
    engine=engine,
    catalog=catalog,          # Optional — for historical row count
    node_name="my_node",      # Optional — for historical lookup
)

if not gate_result.passed:
    if gate_result.action == GateOnFail.ABORT:
        raise RuntimeError(f"Quality gate failed: {gate_result.failure_reasons}")
```

---

## Complete YAML Example

```yaml
pipeline: customer_quality
layer: silver
engine: pandas

connections:
  raw:
    type: local
    base_path: ./data/bronze
  clean:
    type: local
    base_path: ./data/silver
  quarantine_store:
    type: local
    base_path: ./data/quarantine

nodes:
  - name: clean_customers
    read:
      connection: raw
      path: customers.parquet
      format: parquet

    transform:
      steps:
        - function: clean_text
          params:
            columns: [name, email]
            operations: [trim, lower]

    validation:
      tests:
        - type: not_null
          column: customer_id
        - type: unique
          column: email
        - type: accepted_values
          column: status
          values: [active, inactive]
        - type: range
          column: age
          min: 0
          max: 150
        - type: freshness
          column: updated_at
          max_age: "48h"

      quarantine:
        enabled: true
        connection: quarantine_store
        path: customers
        format: parquet

      gate:
        min_pass_rate: 0.98
        on_fail: abort
        row_count_check:
          enabled: true
          max_change_percent: 30

    write:
      connection: clean
      path: customers
      format: delta
      mode: upsert
      options:
        keys: [customer_id]
```

---

## Layer Presets (Bronze → Silver → Gold)

| Layer | Typical Tests | Gate Threshold | Quarantine |
|-------|--------------|----------------|------------|
| Bronze | `not_null` on keys, `row_count` min | 90% | Optional |
| Silver | All column tests, `schema`, `freshness` | 95% | Enabled |
| Gold | All tests + `custom_sql` business rules | 99% | Enabled |

---

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| Validation without quarantine | Failed rows vanish silently — enable quarantine |
| Gate threshold too low | Start at 95%, tighten over time |
| Not testing freshness | Stale data passes all column checks but is still wrong |
| Missing `severity: warn` for soft checks | Hard errors on optional fields block the pipeline |
| Not checking FK integrity | Facts reference non-existent dimension keys |
