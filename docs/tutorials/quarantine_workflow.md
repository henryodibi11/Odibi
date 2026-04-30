# Quarantine & Orphan Handling Tutorial

> **GitHub Issue:** #222
> **Prerequisite:** Familiarity with odibi validation basics (see [Validation Pipeline Tutorial](validation_pipeline.md))

---

## Overview

When data fails quality checks, you have three choices:
1. **Fail the pipeline** — stop everything, fix upstream, re-run
2. **Warn and continue** — log the issue, write all data anyway
3. **Quarantine** — separate bad rows from good, write both to different locations

Quarantining is the production-grade approach: good data flows to your target while
bad data lands in a quarantine table with metadata explaining *why* each row failed.

This tutorial covers:
- Setting up quarantine in YAML
- How rows are split into valid/invalid
- Quarantine metadata columns
- Sampling for high-volume failures
- FK orphan handling (reject, warn, filter)
- Reviewing and re-processing quarantined data

---

## Scenario

You are building a star schema for order analytics:
- **dim_customer** — 20 customers (IDs 1-20)
- **fact_orders** — 100 orders referencing dim_customer

**Data quality issues in fact_orders:**

| Issue | Rows Affected | Description |
|-------|:---:|-------------|
| Orphan FK | 10 | `customer_id` 21-25 not in dim_customer |
| Null amount | 3 | `amount` column is empty |
| Negative amount | 2 | `amount` is below zero |
| Invalid status | 3 | `status` = "CANCELLED" (not in accepted list) |

---

## Step 1: Define Validation Tests with Quarantine Action

Each test's `on_fail` field controls what happens when rows fail:
- `fail` (default) — pipeline stops
- `warn` — log only, row passes through
- `quarantine` — row routed to quarantine table

```yaml
validation:
  tests:
    - type: not_null
      column: amount
      on_fail: quarantine         # Null amounts → quarantined

    - type: range
      column: amount
      min: 0
      on_fail: quarantine         # Negative amounts → quarantined

    - type: accepted_values
      column: status
      values: [completed, pending, shipped]
      on_fail: quarantine         # Invalid statuses → quarantined
```

**Key insight:** Only tests with `on_fail: quarantine` participate in the split.
Tests with `on_fail: fail` or `on_fail: warn` don't affect quarantine routing.

---

## Step 2: Configure the Quarantine Destination

```yaml
validation:
  quarantine:
    connection: quarantine_store  # Where to write quarantine data
    path: orders_quarantine       # Table/path name
    add_columns:                  # Metadata columns added to quarantined rows
      _rejection_reason: true     # Why the row failed
      _rejected_at: true          # UTC timestamp
      _source_batch_id: true      # Run ID for traceability
      _failed_tests: true         # Comma-separated test names
    retention_days: 90            # Auto-cleanup after 90 days
```

### Quarantine Metadata Columns

| Column | Type | Description |
|--------|------|-------------|
| `_rejection_reason` | string | Human-readable failure description |
| `_rejected_at` | timestamp | UTC timestamp when quarantine occurred |
| `_source_batch_id` | string | Pipeline run ID for traceability |
| `_failed_tests` | string | Comma-separated list of failed test names |
| `_original_node` | string | Source node name (optional, off by default) |

---

## Step 3: Add a Quality Gate

The quality gate evaluates the *batch* after quarantine splitting:

```yaml
validation:
  gate:
    require_pass_rate: 0.95       # 95% of rows must pass
    on_fail: abort                # Stop pipeline if gate fails
    row_count:
      min: 50                     # Minimum expected rows
      change_threshold: 0.5       # Alert if >50% change vs last run
```

### Gate Actions

| Action | Behavior |
|--------|----------|
| `abort` | Pipeline stops — no data written |
| `warn_and_write` | Warning logged, all valid rows written |
| `write_valid_only` | Only rows that passed validation are written |

---

## Step 4: FK Orphan Handling

Foreign key validation catches rows that reference non-existent dimension keys.

### Three Actions for Orphans

```yaml
# Action 1: reject — raise error, stop pipeline
fk_checks:
  - name: customer_fk
    fact: fact_orders
    dimension: dim_customer
    fact_key: customer_id
    dimension_key: customer_id
    on_violation: error

# Action 2: warn — log warning, keep all rows
    on_violation: warn

# Action 3: filter — silently remove orphan rows
    on_violation: quarantine
```

### Using FKValidator Programmatically

```python
from odibi.validation.fk import FKValidator, RelationshipConfig, RelationshipRegistry

rel = RelationshipConfig(
    name="customer_fk",
    fact="fact_orders",
    dimension="dim_customer",
    fact_key="customer_id",
    dimension_key="customer_id",
    on_violation="error",
)

registry = RelationshipRegistry(relationships=[rel])
validator = FKValidator(registry)
result = validator.validate_relationship(fact_df, rel, context)

print(f"Valid: {result.valid}")
print(f"Orphans: {result.orphan_count}")
print(f"Sample values: {result.orphan_values[:5]}")
```

### Convenience Function: validate_fk_on_load

```python
from odibi.validation.fk import validate_fk_on_load

# on_failure="filter" removes orphan rows; "error" raises; "warn" logs
clean_df = validate_fk_on_load(fact_df, [rel], context, on_failure="filter")
```

---

## Step 5: Sampling for High-Volume Failures

When millions of rows fail, quarantining all of them wastes storage:

```yaml
validation:
  quarantine:
    connection: quarantine_store
    path: orders_quarantine
    max_rows: 10000              # Cap at 10k rows
    sample_fraction: 0.1         # Or sample 10% of failures
```

- `max_rows` — hard cap on quarantine output
- `sample_fraction` — random sample (0.0-1.0)
- If both are set, `max_rows` is applied after sampling

---

## Step 6: Re-Processing Quarantined Data

After fixing the upstream issue (e.g., adding missing customers to dim_customer):

1. **Read the quarantine table** — filter by `_source_batch_id` or date range
2. **Strip metadata columns** — remove `_rejection_reason`, `_rejected_at`, etc.
3. **Re-run through the pipeline** — the fixed data should now pass all tests
4. **Verify quarantine is empty** — no new failures

```python
# Read quarantine data
quarantined = pd.read_parquet("data/quarantine/orders_quarantine")

# Strip metadata columns
metadata_cols = [c for c in quarantined.columns if c.startswith("_")]
reprocess_df = quarantined.drop(columns=metadata_cols)

# Re-validate with fixed dimension
# (Now dim_customer has IDs 21-25, so FK passes)
result = validator.validate_relationship(reprocess_df, rel, context_with_fixed_dim)
assert result.valid, "All orphans should be resolved"
```

---

## Complete YAML Example

```yaml
project: quarantine_demo
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
  name: quarantine_workflow
  description: "Demonstrates quarantine split, FK orphan handling, and quality gates"

system:
  connection: silver
  path: _odibi_system

pipelines:
  - pipeline: order_quality
    layer: silver
    nodes:
      - name: validate_orders
        read:
          connection: raw
          path: fact_orders.csv
          format: csv
        transform:
          steps:
            - function: clean_text
              params:
                columns: [status]
                operations: [trim, lower]
        validation:
          tests:
            - type: not_null
              column: amount
              on_fail: quarantine
            - type: range
              column: amount
              min: 0
              on_fail: quarantine
            - type: accepted_values
              column: status
              values: [completed, pending, shipped]
              on_fail: quarantine
          quarantine:
            connection: quarantine_store
            path: orders_quarantine
            add_columns:
              _rejection_reason: true
              _rejected_at: true
              _source_batch_id: true
              _failed_tests: true
          gate:
            require_pass_rate: 0.90
            on_fail: warn_and_write
        write:
          connection: silver
          path: orders
          format: parquet
          mode: overwrite
```

---

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| Quarantine defined but no tests use `on_fail: quarantine` | Add `on_fail: quarantine` to tests that should route failures |
| Gate threshold too strict for dirty sources | Start at 90%, tighten as upstream improves |
| Not checking FK before writing facts | Add FK validation before the write step |
| Quarantining without metadata columns | Always enable `_rejection_reason` + `_rejected_at` |
| Not re-processing quarantine after fixes | Build a re-processing workflow to close the loop |
| Sampling disabled on high-volume sources | Set `max_rows` to avoid quarantine table bloat |
