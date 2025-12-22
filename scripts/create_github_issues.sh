#!/bin/bash
# Run from repo root: bash scripts/create_github_issues.sh
# Requires: gh auth login (GitHub CLI authenticated)

echo "Creating GitHub issues for odibi..."

# Issue 1: Retry logic
gh issue create \
  --title "Retry logic not wired to execution" \
  --label "bug,reliability,priority-high" \
  --body "## Problem

\`RetryConfig\` exists in config.py but is not connected to actual node execution.

## Evidence

- \`odibi/config.py\` defines \`RetryConfig\` with \`max_attempts\`, \`backoff\` strategy
- \`odibi/node.py\` does not use retry config during execution
- Transient Azure failures (throttling, network) cause immediate pipeline failure

## Expected Behavior

Nodes should retry on transient failures with configurable backoff:

\`\`\`yaml
retry:
  enabled: true
  max_attempts: 3
  backoff: exponential
\`\`\`

## Acceptance Criteria

- [ ] Wire RetryConfig to node execution
- [ ] Implement exponential/linear/constant backoff
- [ ] Retry only on transient errors (network, throttling), not data errors
- [ ] Log retry attempts with context
- [ ] Tests for retry behavior"

echo "✓ Created: Retry logic"

# Issue 2: Idempotency
gh issue create \
  --title "Idempotency for append mode writes" \
  --label "enhancement,reliability,priority-high" \
  --body "## Problem

Re-running the same batch in append mode can create duplicate records.

If a pipeline fails after node 3 writes but before completion:
1. User re-runs pipeline
2. Nodes 1-3 re-execute and append again
3. Data is duplicated

## Proposed Solution

**Batch ID deduplication:**
- Add \`_batch_id\` column on write (use run_id)
- Delete existing batch before append
- Requires Delta Lake DELETE capability (which we have)

## Acceptance Criteria

- [ ] Design decision on approach
- [ ] Implementation for chosen approach
- [ ] Works with Delta Lake append
- [ ] Tests for idempotent re-runs
- [ ] Opt-in via config (\`idempotent: true\`)"

echo "✓ Created: Idempotency"

# Issue 3: Rollback on failure
gh issue create \
  --title "Rollback on partial pipeline failure" \
  --label "enhancement,reliability,priority-medium" \
  --body "## Problem

If node 3/5 fails, nodes 1-2 have already written data but pipeline is marked 'failed'.
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
- [ ] Recovery documentation"

echo "✓ Created: Rollback"

# Issue 4: Dead letter queue
gh issue create \
  --title "Dead letter queue for unparseable records" \
  --label "enhancement,data-quality,priority-low" \
  --body "## Problem

Records that fail parsing (malformed JSON, encoding errors) have no destination.

## Current State

- Quarantine handles validation failures (records that parse but fail contracts)
- Parsing failures cause node failure with no recovery

## Expected Behavior

Unparseable records should go to a dead letter destination for investigation.

## Acceptance Criteria

- [ ] DLQ config option in read config
- [ ] Capture parsing errors with context
- [ ] Continue processing valid records"

echo "✓ Created: Dead letter queue"

# Issue 5: Metrics export
gh issue create \
  --title "Metrics export (Prometheus/Azure Monitor)" \
  --label "enhancement,observability,priority-medium" \
  --body "## Problem

No way to export pipeline metrics to monitoring systems.

## Current State

- Stories capture execution details (great for debugging)
- No push to Prometheus, StatsD, Azure Monitor, etc.

## Expected Behavior

\`\`\`yaml
observability:
  metrics:
    type: prometheus  # or azure_monitor, statsd
    endpoint: http://pushgateway:9091
\`\`\`

Metrics to export:
- \`odibi_pipeline_duration_seconds\`
- \`odibi_node_duration_seconds\`
- \`odibi_rows_processed_total\`
- \`odibi_validation_failures_total\`
- \`odibi_quarantine_rows_total\`

## Acceptance Criteria

- [ ] Prometheus pushgateway support
- [ ] Azure Monitor support
- [ ] Configurable labels/dimensions"

echo "✓ Created: Metrics export"

# Issue 6: Distributed tracing
gh issue create \
  --title "Distributed tracing (OpenTelemetry)" \
  --label "enhancement,observability,priority-low" \
  --body "## Problem

No distributed tracing for cross-node debugging.

## Expected Behavior

\`\`\`yaml
observability:
  tracing:
    enabled: true
    exporter: otlp
    endpoint: http://jaeger:4317
\`\`\`

Each node execution creates a span with:
- Parent: pipeline span
- Attributes: node name, row count, duration
- Events: validation results, errors

## Acceptance Criteria

- [ ] OpenTelemetry SDK integration
- [ ] Span per pipeline and node
- [ ] Trace context propagation"

echo "✓ Created: Distributed tracing"

# Issue 7: Real Spark tests - REMOVED (already covered in WSL tests)

# Issue 8: Load/stress tests
gh issue create \
  --title "Load/stress testing suite" \
  --label "testing,performance,priority-low" \
  --body "## Problem

No benchmarks for large datasets (10M+ rows).

## Current State

- \`tests/benchmarks/\` exists with basic benchmarks
- No stress tests for production-scale data

## Acceptance Criteria

- [ ] Benchmark with 1M, 10M, 100M rows
- [ ] Memory profiling
- [ ] Identify performance regressions"

echo "✓ Created: Load tests"

# Issue 9: Environments config
gh issue create \
  --title "Implement environments config" \
  --label "enhancement,config,phase-3,priority-low" \
  --body "## Problem

The \`environments\` field is defined in \`ProjectConfig\` but does nothing.

## Current State

The validator explicitly notes this is deferred:

\`\`\`python
@model_validator(mode='after')
def check_environments_not_implemented(self):
    # Implemented in Phase 3
    return self
\`\`\`

## Expected Behavior

\`\`\`yaml
environments:
  dev:
    connections:
      bronze:
        base_path: ./dev_data
  prod:
    connections:
      bronze:
        base_path: abfss://bronze@prod.dfs.core.windows.net
\`\`\`

## Acceptance Criteria

- [ ] Environment switching via CLI flag or env var
- [ ] Config merging (base + environment overrides)
- [ ] Tests for environment resolution"

echo "✓ Created: Environments config"

# Issue 10: Streaming/CDC (future)
gh issue create \
  --title "Streaming/CDC support" \
  --label "enhancement,future,priority-low" \
  --body "## Problem

No support for real-time streaming or Change Data Capture.

## Expected Behavior (Future)

\`\`\`yaml
read:
  connection: eventhub
  stream: true
  watermark_column: event_time
  watermark_delay: '10 minutes'
\`\`\`

## Note

This is a significant feature. Park for future consideration."

echo "✓ Created: Streaming/CDC"

# Issue 11: Data profiling
gh issue create \
  --title "Data profiling / statistics" \
  --label "enhancement,data-quality,priority-low" \
  --body "## Problem

Only basic stats captured. No histograms, distributions, or anomaly detection.

## Expected Behavior

\`\`\`yaml
profiling:
  enabled: true
  include:
    - histograms
    - null_percentage
    - unique_count
    - min_max
\`\`\`

## Acceptance Criteria

- [ ] Extended profiling in story metadata
- [ ] Optional (can be expensive for large data)"

echo "✓ Created: Data profiling"

echo ""
echo "============================================"
echo "Done! Created 10 issues."
echo "View at: https://github.com/henryodibi11/Odibi/issues"
