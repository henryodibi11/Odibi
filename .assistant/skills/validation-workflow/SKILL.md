---
name: validation-workflow
description: "Use when adding data-quality to an Odibi node — the validation tests, quarantine routing, quality gates, and FK checks. Pairs get_validation_rules + validate_yaml."
requires: [odibi]
---

# validation-workflow Skill

Odibi validates data inside a node's `validation:` block, between transform and write.
The chain is: **run tests → quarantine failing rows → apply a batch-level gate.** Use
`get_validation_rules` for the authoritative test catalog and `validate_yaml` to check syntax.

## When to Load This Skill

- Adding quality checks, quarantine, or a quality gate to a node.
- Deciding what to test at bronze vs silver vs gold.
- A node is silently dropping rows and you need to capture/inspect failures.

## Where It Lives

```yaml
nodes:
  - name: clean_customers
    # ... read / transform ...
    validation:
      tests: [ ... ]        # row/column/batch checks
      quarantine: { ... }   # optional: route failing rows somewhere
      gate: { ... }         # optional: batch-level pass-rate threshold
      fk_checks: [ ... ]    # optional: referential integrity
      fail_fast: false      # optional: stop on first failing test
    write: { ... }
```

## Step 1: Tests

```yaml
validation:
  tests:
    - { type: not_null, column: customer_id, severity: error }   # error (default) | warn
    - { type: unique, column: email }
    - { type: accepted_values, column: status, values: [active, inactive, pending] }
    - { type: range, column: age, min: 0, max: 150 }
    - { type: regex_match, column: email, pattern: "^[^@]+@[^@]+\\.[^@]+$" }
    - { type: row_count, min: 1, max: 1000000 }
    - { type: freshness, column: updated_at, max_age: "24h" }
    - { type: schema, columns: { customer_id: integer, name: string } }
    - { type: custom_sql, query: "SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) = 0" }
```

| Type | Required fields |
|---|---|
| `not_null` | `column` |
| `unique` | `column` |
| `accepted_values` | `column`, `values` |
| `range` | `column`, `min` and/or `max` |
| `regex_match` | `column`, `pattern` |
| `row_count` | `min` and/or `max` |
| `freshness` | `column`, `max_age` (e.g. `"24h"`, `"1d"`) |
| `schema` | `columns` (name→type map) |
| `custom_sql` | `query` (SQL expression that must evaluate true) |

`severity: warn` logs the failure but lets the row pass; `error` (default) marks it for
quarantine/gating. Engine note: all 9 tests run on Pandas; `custom_sql` is partial on
Polars (logs a warning and skips) — see the `engine-parity` skill.

## Step 2: Quarantine (capture failing rows)

Without quarantine, failing rows can disappear silently. Enable it to write them out
with metadata (which test failed, when, why).

```yaml
validation:
  quarantine:
    enabled: true
    connection: quarantine_store
    path: customers
    format: parquet            # csv | json | parquet
    sampling: { max_rows: 10000, strategy: first }   # optional, for high-volume failures
```

## Step 3: Quality Gate (batch-level threshold)

```yaml
validation:
  gate:
    min_pass_rate: 0.95        # 95% of rows must pass all tests
    on_fail: abort             # abort (stop, no write) | warn (write anyway) | skip (skip node)
    row_count_check: { enabled: true, max_change_percent: 50 }
```

## Step 4: FK Checks (referential integrity)

```yaml
validation:
  fk_checks:
    - column: customer_sk
      parent_table: gold/dim_customer
      parent_column: customer_sk
      action: reject           # reject (drop) | unknown (map to SK=0) | quarantine
```

## Layer Presets

| Layer | Typical tests | Gate | Quarantine |
|---|---|---|---|
| Bronze | `not_null` on keys, `row_count` min | ~90% | optional |
| Silver | all column tests + `schema` + `freshness` | ~95% | enabled |
| Gold | all + `custom_sql` business rules + `fk_checks` | ~99% | enabled |

## Contracts vs Validation

`validation:` runs on a node's **output** (post-transform). To assert preconditions on the
**input** before transforming, use the node's `contracts:` list (same `TestConfig` shape) —
a circuit-breaker that fails fast on bad input.

## Common Mistakes

| Mistake | Fix |
|---|---|
| Tests but no quarantine | failed rows vanish — set `quarantine.enabled: true` |
| Gate threshold too low | start ~0.95, tighten over time |
| No freshness test | stale data passes every column check but is still wrong |
| Hard `error` on a soft check | use `severity: warn` for non-blocking checks |
| Facts with no FK check | orphan facts reference missing dimension keys — add `fk_checks` |

## Workflow

1. `get_validation_rules` → authoritative test types + required fields.
2. Add `validation:` to the node; start with key `not_null`/`unique` + a `row_count`.
3. `validate_yaml` → fix errors.
4. `test_pipeline`, then inspect the quarantine output with `node_sample` or context-workbench.
