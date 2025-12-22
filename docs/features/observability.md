# Observability

Odibi integrates with OpenTelemetry for distributed tracing and metrics export.

## Overview

When configured, Odibi automatically emits:

- **Traces**: Spans for pipeline runs, node executions, and operations
- **Metrics**: Execution counts, durations, and error rates

## Configuration

### Environment Variables

Set these environment variables to enable OpenTelemetry export:

| Variable | Description | Example |
|----------|-------------|---------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP endpoint URL | `http://localhost:4317` |
| `OTEL_SERVICE_NAME` | Service name (default: `odibi`) | `my-data-pipeline` |

### Example: Export to Jaeger

```bash
# Start Jaeger
docker run -d --name jaeger \
  -p 16686:16686 \
  -p 4317:4317 \
  jaegertracing/all-in-one:latest

# Run Odibi with tracing
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
odibi run config.yaml

# View traces at http://localhost:16686
```

### Example: Export to Grafana Tempo

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://tempo.monitoring:4317
export OTEL_SERVICE_NAME=odibi-prod
odibi run config.yaml
```

## What Gets Traced

### Pipeline Execution

Each pipeline run creates a root span with:

- Pipeline name
- Start/end timestamps
- Success/failure status
- Node count

### Node Execution

Each node creates a child span with:

- Node name and type
- Duration
- Row counts (input/output)
- Error details (if failed)

### Retry Operations

Retry attempts are recorded as span events:

- Retry count
- Delay duration
- Error that triggered retry

## Telemetry API

For custom instrumentation in transforms:

```python
from odibi.utils.telemetry import get_tracer, get_meter

tracer = get_tracer()
meter = get_meter()

# Create a span
with tracer.start_as_current_span("my_operation") as span:
    span.set_attribute("custom.key", "value")
    # ... your code

# Record a metric
counter = meter.create_counter("my_counter")
counter.add(1, {"dimension": "value"})
```

## Structured Logging

Odibi uses structured logging with context:

```python
from odibi.utils.logging_context import get_logging_context

ctx = get_logging_context()
ctx.info("Processing started", node="my_node", rows=1000)
```

Log output includes:

- Timestamp
- Log level
- Message
- Structured attributes (JSON)

## Disabling Telemetry

Telemetry is disabled by default. It only activates when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

To explicitly disable in an environment where it might be configured:

```bash
unset OTEL_EXPORTER_OTLP_ENDPOINT
```

## Related

- [Lineage](lineage.md) — OpenLineage for data lineage
- [Stories](stories.md) — Built-in execution reports
- [Alerting](alerting.md) — Failure notifications
