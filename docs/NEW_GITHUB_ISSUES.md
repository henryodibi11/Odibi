# GitHub Issues Tracker

## Completed Issues ✅

These issues have been resolved and can be closed if opened.

---

### Issue: CLI missing `--tag` flag ✅ COMPLETED

**Labels:** `bug`, `cli`

The `--tag` flag is now implemented in `odibi/cli/main.py:79`.

---

### Issue: CLI missing `--node` and `--pipeline` flags ✅ COMPLETED

**Labels:** `bug`, `cli`, `orchestration`

Both flags now exist in `odibi/cli/main.py:83,88`.

---

### Issue: `materialized` config option has no effect ✅ COMPLETED

**Labels:** `bug`, `config`

Implemented in `odibi/node.py:199-1743` - supports `view` and `incremental` strategies.

---

### Issue: Add `POLARS` to `EngineType` enum ✅ COMPLETED

**Labels:** `enhancement`, `engine`

Added in `odibi/config.py:19`.

---

### Issue: PandasEngine missing `as_of_timestamp` time travel ✅ COMPLETED

**Labels:** `bug`, `engine-parity`

Implemented in `odibi/engine/pandas_engine.py:281-283`.

---

### Issue: PolarsEngine missing abstract methods ✅ COMPLETED

**Labels:** `enhancement`, `engine-parity`

`harmonize_schema` and `anonymize` now implemented in `polars_engine.py`.

---

### Issue: Validation engine performance optimizations ✅ COMPLETED

**Labels:** `enhancement`, `performance`

Fixed in validation engine overhaul:
- Pandas UNIQUE double scan
- Pandas ACCEPTED_VALUES memory waste
- Polars eager collection (now lazy)
- Added all missing Polars contract types
- Quarantine memory optimization (removed O(N×tests) lists)
- Added `fail_fast` and `cache_df` config options
- Added quarantine `max_rows` and `sample_fraction`

---

## Open Issues

### Issue: `environments` config not implemented

**Labels:** `enhancement`, `config`, `phase-3`

**Priority:** Low

**Body:**

```
The `environments` field is defined in `ProjectConfig` but does nothing.

## Current State

The validator explicitly notes this is deferred to Phase 3:

```python
@model_validator(mode="after")
def check_environments_not_implemented(self):
    # Implemented in Phase 3
    return self
```

## Expected Behavior

Users should be able to define environment-specific overrides:

```yaml
environments:
  dev:
    connections:
      bronze:
        base_path: ./dev_data
  prod:
    connections:
      bronze:
        base_path: abfss://bronze@prod.dfs.core.windows.net
```

## Acceptance Criteria

- [ ] Environment switching via CLI flag or env var
- [ ] Config merging (base + environment overrides)
- [ ] Tests for environment resolution
```

---

### Issue: Retry logic not wired to execution

**Labels:** `bug`, `reliability`, `priority-high`

**Priority:** High (Quick Win)

**Body:**

```
`RetryConfig` exists in config.py but is not connected to actual node execution.

## Evidence

- `odibi/config.py` defines `RetryConfig` with `max_attempts`, `backoff` strategy
- `odibi/node.py` does not use retry config during execution
- Transient Azure failures (throttling, network) cause immediate pipeline failure

## Expected Behavior

Nodes should retry on transient failures with configurable backoff:

```yaml
retry:
  enabled: true
  max_attempts: 3
  backoff: exponential
```

## Acceptance Criteria

- [ ] Wire RetryConfig to node execution
- [ ] Implement exponential/linear/constant backoff
- [ ] Retry only on transient errors (network, throttling), not data errors
- [ ] Log retry attempts with context
- [ ] Tests for retry behavior
```

---

### Issue: Idempotency for append mode writes

**Labels:** `enhancement`, `reliability`, `priority-high`

**Priority:** High

**Body:**

```
Re-running the same batch in append mode can create duplicate records.

## Problem

If a pipeline fails after node 3 writes but before completion:
1. User re-runs pipeline
2. Nodes 1-3 re-execute and append again
3. Data is duplicated

## Expected Behavior

Append writes should be idempotent - re-running with same data should not duplicate.

## Proposed Solutions

Option A: **Batch ID deduplication**
- Add `_batch_id` column on write
- Delete existing batch before append
- Simple but requires DELETE capability

Option B: **Content hash check**
- Hash input data, skip if already written
- Works with append-only systems

Option C: **Write-ahead log**
- Track which batches completed
- Skip already-completed nodes on re-run

## Acceptance Criteria

- [ ] Design decision on approach
- [ ] Implementation for chosen approach
- [ ] Works with Delta Lake append
- [ ] Tests for idempotent re-runs
```

---

### Issue: Rollback on partial pipeline failure

**Labels:** `enhancement`, `reliability`, `priority-medium`

**Priority:** Medium

**Body:**

```
If node 3/5 fails, nodes 1-2 have already written data but pipeline is marked "failed".

## Problem

No automatic cleanup of partial writes. User must manually identify and fix.

## Expected Behavior

Options:
1. **Transaction-like rollback** - Undo writes from failed run
2. **Checkpoint resume** - Resume from last successful node
3. **Dry-run mode** - Validate all nodes before writing any

## Acceptance Criteria

- [ ] Design decision on approach
- [ ] Implementation
- [ ] Clear error message showing what was written before failure
- [ ] Recovery documentation
```

---

### Issue: Dead letter queue for unparseable records

**Labels:** `enhancement`, `data-quality`, `priority-low`

**Priority:** Low

**Body:**

```
Records that fail parsing (malformed JSON, encoding errors) have no destination.

## Current State

- Quarantine handles validation failures (records that parse but fail contracts)
- Parsing failures cause node failure with no recovery

## Expected Behavior

Unparseable records should go to a dead letter destination for investigation.

## Acceptance Criteria

- [ ] DLQ config option in read config
- [ ] Capture parsing errors with context
- [ ] Continue processing valid records
```

---

### Issue: Metrics export (Prometheus/Azure Monitor)

**Labels:** `enhancement`, `observability`, `priority-medium`

**Priority:** Medium

**Body:**

```
No way to export pipeline metrics to monitoring systems.

## Current State

- Stories capture execution details (great for debugging)
- No push to Prometheus, StatsD, Azure Monitor, etc.

## Expected Behavior

```yaml
observability:
  metrics:
    type: prometheus  # or azure_monitor, statsd
    endpoint: http://pushgateway:9091
    labels:
      environment: prod
```

Metrics to export:
- `odibi_pipeline_duration_seconds`
- `odibi_node_duration_seconds`
- `odibi_rows_processed_total`
- `odibi_validation_failures_total`
- `odibi_quarantine_rows_total`

## Acceptance Criteria

- [ ] Prometheus pushgateway support
- [ ] Azure Monitor support (for your use case)
- [ ] Configurable labels/dimensions
```

---

### Issue: Distributed tracing (OpenTelemetry)

**Labels:** `enhancement`, `observability`, `priority-low`

**Priority:** Low

**Body:**

```
No distributed tracing for cross-node debugging.

## Expected Behavior

```yaml
observability:
  tracing:
    enabled: true
    exporter: otlp
    endpoint: http://jaeger:4317
```

Each node execution creates a span with:
- Parent: pipeline span
- Attributes: node name, row count, duration
- Events: validation results, errors

## Acceptance Criteria

- [ ] OpenTelemetry SDK integration
- [ ] Span per pipeline and node
- [ ] Trace context propagation
```

---

### Issue: Real Spark integration tests

**Labels:** `testing`, `priority-medium`

**Priority:** Medium

**Body:**

```
Most Spark tests use mocks, not real SparkSession.

## Current State

- `test_spark_real.py` exists but limited
- Many Spark code paths untested with real execution

## Acceptance Criteria

- [ ] Integration tests with real SparkSession (WSL/Linux)
- [ ] Test Delta Lake operations end-to-end
- [ ] Test Spark-specific features (partitioning, caching)
- [ ] CI setup for Spark tests
```

---

### Issue: Load/stress testing suite

**Labels:** `testing`, `performance`, `priority-low`

**Priority:** Low

**Body:**

```
No benchmarks for large datasets (10M+ rows).

## Current State

- `tests/benchmarks/` exists with basic benchmarks
- No stress tests for production-scale data

## Acceptance Criteria

- [ ] Benchmark with 1M, 10M, 100M rows
- [ ] Memory profiling
- [ ] Identify performance regressions
```

---

### Issue: Streaming/CDC support

**Labels:** `enhancement`, `future`, `priority-low`

**Priority:** Low (Future)

**Body:**

```
No support for real-time streaming or Change Data Capture.

## Expected Behavior (Future)

```yaml
read:
  connection: eventhub
  stream: true
  watermark_column: event_time
  watermark_delay: "10 minutes"
```

## Note

This is a significant feature. Park for future consideration.
```

---

### Issue: Data profiling / statistics

**Labels:** `enhancement`, `data-quality`, `priority-low`

**Priority:** Low

**Body:**

```
Only basic stats captured. No histograms, distributions, or anomaly detection.

## Expected Behavior

```yaml
profiling:
  enabled: true
  include:
    - histograms
    - null_percentage
    - unique_count
    - min_max
```

## Acceptance Criteria

- [ ] Extended profiling in story metadata
- [ ] Optional (can be expensive for large data)
```

---

## Summary

| Issue | Priority | Status |
|-------|----------|--------|
| `--tag` flag | - | ✅ Completed |
| `--node`/`--pipeline` flags | - | ✅ Completed |
| `materialized` config | - | ✅ Completed |
| POLARS enum | - | ✅ Completed |
| PandasEngine timestamp time travel | - | ✅ Completed |
| PolarsEngine methods | - | ✅ Completed |
| Validation performance | - | ✅ Completed |
| `environments` config | Low | 🔲 Open |
| **Retry wiring** | **High** | 🔲 Open |
| **Idempotency for append** | **High** | 🔲 Open |
| Rollback on failure | Medium | 🔲 Open |
| Dead letter queue | Low | 🔲 Open |
| Metrics export | Medium | 🔲 Open |
| Distributed tracing | Low | 🔲 Open |
| Real Spark tests | Medium | 🔲 Open |
| Load/stress tests | Low | 🔲 Open |
| Streaming/CDC | Low | 🔲 Future |
| Data profiling | Low | 🔲 Open |
