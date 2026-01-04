# Decision Guide

Rules of thumb for common Odibi decisions.

---

## Engine Choice

| Scenario | Engine | Why |
|----------|--------|-----|
| Local dev, files < 1GB | `pandas` | Fast startup, no dependencies |
| Local dev, files 1-10GB | `polars` | Faster than Pandas, lazy eval |
| Production, Delta Lake, > 10GB | `spark` | Distributed, Delta support |
| Databricks | `spark` | Native integration |

**Rule:** Start with `pandas` locally, switch to `spark` for production.

---

## Validation: Contracts vs Tests

| Use... | When... | Behavior |
|--------|---------|----------|
| `contracts:` | Checking **source** data | Runs before read, fail-fast |
| `validation.tests:` | Checking **output** data | Runs after transform, configurable |

**Rule:** Use contracts for freshness/schema/volume. Use tests for row-level quality.

```yaml
# Contracts: Source quality (fail-fast)
contracts:
  - type: freshness
    column: updated_at
    max_age: "24h"

# Tests: Output quality (with gates)
validation:
  tests:
    - type: not_null
      columns: [id, name]
  gate:
    on_failure: warn  # or 'fail'
```

---

## When to Use Quality Gates

| Scenario | Gate Setting |
|----------|--------------|
| Development/testing | `on_failure: warn` |
| Production, non-critical | `on_failure: warn` + alerting |
| Production, critical data | `on_failure: fail` |
| Regulatory/compliance | `on_failure: fail` + quarantine |

**Rule:** Start with `warn`, tighten to `fail` as you trust the data.

---

## When to Enable Alerting

| Scenario | Alert |
|----------|-------|
| Local dev | No alerts |
| Scheduled production jobs | `on_failure` |
| Critical SLA pipelines | `on_failure` + `on_quality_gate_fail` |
| All runs (audit trail) | `on_success` + `on_failure` |

**Rule:** Enable alerting when someone needs to act on failure.

```yaml
alerts:
  - type: slack
    url: ${SLACK_WEBHOOK}
    on_events: [on_failure, on_quality_gate_fail]
```

---

## Bronze vs Silver vs Gold Logic

| Logic Type | Layer | Why |
|------------|-------|-----|
| Ingestion, format conversion | Bronze | Raw preservation |
| Deduplication, cleaning | Silver | Single source of truth |
| Business transforms, aggregation | Gold | Consumption-ready |

**Rule:** If it changes the semantic meaning, it belongs in Gold.

---

## Incremental Mode Selection

```
Has reliable timestamp column?
├─► Yes
│   └─► Need exact row tracking? → mode: stateful
│   └─► OK with overlap? → mode: rolling_window
└─► No
    └─► Data is immutable? → mode: append
    └─► Data can change? → skip_if_unchanged: true
```

| Mode | State | Use Case |
|------|-------|----------|
| `stateful` | Persisted HWM | CDC, database extraction |
| `rolling_window` | Lookback period | Event logs, files with dates |
| `append` | None | Immutable streams |

---

## SCD Type Selection

| Need history? | Changes often? | Recommendation |
|---------------|----------------|----------------|
| No | - | SCD Type 1 (overwrite) |
| Yes | Slowly (< 1/day) | SCD Type 2 (versioned) |
| Yes | Frequently | Daily snapshots instead |

**Rule:** SCD2 is for *slowly* changing dimensions. Fast changes = snapshot approach.

---

## Merge vs Overwrite Write Mode

| Scenario | Mode | Why |
|----------|------|-----|
| First load / full refresh | `overwrite` | Clean slate |
| Incremental updates | `merge` | Preserve existing, update keys |
| Append-only events | `append` | Immutable, no updates |
| SCD2 result | `overwrite` | Transformer returns full history |
| Aggregations | `overwrite` | Idempotent recalculation |

**Rule:** When in doubt, use `overwrite` for transforms and `append` for raw.

---

## Retry Configuration

| Scenario | Retry Config |
|----------|--------------|
| Transient network issues | `max_attempts: 3` |
| Database locks | `backoff: exponential` |
| Stable local files | `enabled: false` |

```yaml
retry:
  enabled: true
  max_attempts: 3
  backoff: exponential
```

---

## When to Use Quarantine

| Scenario | Quarantine? |
|----------|-------------|
| Dev/testing | No (just fail or warn) |
| Data with expected bad rows | Yes (review later) |
| Strict quality required | Yes (fail + quarantine for audit) |
| High-volume, low-quality tolerance | Yes (separate good from bad) |

**Rule:** Quarantine when you can't afford data loss but also can't accept bad data.

---

## File Format Selection

| Format | When to Use |
|--------|-------------|
| `csv` | Source files, human-readable |
| `parquet` | Local analytics, single-machine |
| `delta` | Production, ACID, time travel |
| `json` | API responses, nested data |

**Rule:** Use Delta for anything that needs reliability or history.

---

## Quick Checklist: Production Ready?

```
[ ] Engine set to 'spark' (or appropriate for scale)
[ ] Contracts on source nodes (freshness, row_count)
[ ] Validation tests on transform outputs
[ ] Quality gates configured
[ ] Alerting enabled
[ ] Retry configured
[ ] Stories enabled for audit trail
[ ] Environment variables for secrets
```

---

## See Also

- [THE_REFERENCE.md](../examples/canonical/THE_REFERENCE.md) — All patterns in one runnable example
- [Playbook](../playbook/README.md) — Problem → solution lookup
- [Best Practices](best_practices.md) — Code organization
- [Production Deployment](production_deployment.md) — Going live
