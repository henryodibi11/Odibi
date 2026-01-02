# Enhanced Alerting

Real-time notifications for pipeline events with throttling, event-specific payloads, and support for Slack, Teams, and generic webhooks.

## Overview

Odibi's alerting system provides:
- **Multiple channels**: Slack, Teams, generic webhooks
- **Event-specific payloads**: Contextual information for each event type
- **Throttling**: Prevent alert spam with rate limiting
- **Rich formatting**: Adaptive cards for Teams, Block Kit for Slack

## Configuration

### Basic Alert Setup

```yaml
alerts:
  - type: slack
    url: "${SLACK_WEBHOOK_URL}"
    on_events:
      - on_failure
```

### Alert Config Options

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | Alert type: `slack`, `teams`, `webhook` |
| `url` | string | Yes | Webhook URL |
| `on_events` | list | No | Events to trigger on (default: `on_failure`) |
| `metadata` | object | No | Extra settings (throttling, channel, etc.) |

## Event Types

| Event | Description |
|-------|-------------|
| `on_start` | Pipeline started |
| `on_success` | Pipeline completed successfully |
| `on_failure` | Pipeline failed |
| `on_quarantine` | Rows were quarantined |
| `on_gate_block` | Quality gate blocked the pipeline |
| `on_threshold_breach` | A threshold was exceeded |

## Metadata Reference

The `metadata` field accepts the following options:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `throttle_minutes` | int | `15` | Minimum minutes between repeated alerts for the same pipeline+event |
| `max_per_hour` | int | `10` | Maximum alerts of the same type per hour |
| `channel` | string | — | Target channel override (Slack only, e.g., `#data-alerts`) |

!!! note "All metadata keys are optional"
    If not specified, defaults are applied automatically.

### Example with All Metadata Options

```yaml
alerts:
  - type: slack
    url: "${SLACK_WEBHOOK_URL}"
    on_events:
      - on_failure
      - on_quarantine
    metadata:
      throttle_minutes: 15   # Min 15 minutes between same alerts
      max_per_hour: 10       # Max 10 alerts per hour
      channel: "#data-alerts" # Slack channel override
```

## Throttling

Prevent alert spam with time-based and rate-based throttling:

```yaml
alerts:
  - type: slack
    url: "${SLACK_WEBHOOK_URL}"
    on_events:
      - on_failure
      - on_quarantine
    metadata:
      throttle_minutes: 15   # Min 15 minutes between same alerts
      max_per_hour: 10       # Max 10 alerts per hour
```

### Throttle Key

Throttling is applied per unique combination of:
- Pipeline name
- Event type

So a `process_orders` pipeline failing twice in 5 minutes sends one alert, but a different pipeline can still alert.

## Event-Specific Payloads

### Quarantine Events

```yaml
alerts:
  - type: slack
    url: "${SLACK_WEBHOOK_URL}"
    on_events:
      - on_quarantine
```

Payload includes:
- Rows quarantined count
- Quarantine table path
- Failed test names
- Node name

### Gate Block Events

```yaml
alerts:
  - type: teams
    url: "${TEAMS_WEBHOOK_URL}"
    on_events:
      - on_gate_block
```

Payload includes:
- Pass rate achieved
- Required pass rate
- Number of failed rows
- Failure reasons

### Threshold Breach Events

```yaml
alerts:
  - type: webhook
    url: "https://api.example.com/alerts"
    on_events:
      - on_threshold_breach
```

Payload includes:
- Metric name
- Threshold value
- Actual value
- Node name

## Complete Example

```yaml
project: DataPipeline
engine: spark

alerts:
  # Critical failures - immediate notification
  - type: slack
    url: "${SLACK_WEBHOOK_URL}"
    on_events:
      - on_failure
      - on_gate_block
    metadata:
      throttle_minutes: 5
      channel: "#data-critical"

  # Data quality issues - less urgent
  - type: slack
    url: "${SLACK_WEBHOOK_URL}"
    on_events:
      - on_quarantine
      - on_threshold_breach
    metadata:
      throttle_minutes: 30
      max_per_hour: 5
      channel: "#data-quality"

  # Teams for management visibility
  - type: teams
    url: "${TEAMS_WEBHOOK_URL}"
    on_events:
      - on_failure
    metadata:
      throttle_minutes: 60

connections:
  # ...

pipelines:
  - pipeline: process_orders
    nodes:
      - name: validate_orders
        validation:
          tests:
            - type: not_null
              columns: [order_id]
              on_fail: quarantine
          quarantine:
            connection: silver
            path: quarantine/orders
          gate:
            require_pass_rate: 0.95
        # ...
```

## Slack Configuration

### Block Kit Format

Slack alerts use Block Kit for rich formatting:

```
┌──────────────────────────────────────────┐
│ 🚫 ODIBI: process_orders - GATE_BLOCKED │
├──────────────────────────────────────────┤
│ Project:    DataPipeline                 │
│ Status:     GATE_BLOCKED                 │
│ Duration:   45.23s                       │
│ Pass Rate:  92.3%                        │
│ Required:   95.0%                        │
│ Rows Failed: 1,542                       │
│                                          │
│ [📊 View Story]                          │
└──────────────────────────────────────────┘
```

### Custom Channel

```yaml
alerts:
  - type: slack
    url: "${SLACK_WEBHOOK_URL}"
    on_events:
      - on_failure
    metadata:
      channel: "#data-alerts"  # Override default channel
```

## Teams Configuration

### Adaptive Card Format

Teams alerts use Adaptive Cards:

```yaml
alerts:
  - type: teams
    url: "${TEAMS_WEBHOOK_URL}"
    on_events:
      - on_failure
      - on_gate_block
    metadata:
      throttle_minutes: 30
```

Cards include:
- Color-coded header (red for failures, orange for warnings)
- Fact set with key metrics
- Action button to view story

## Generic Webhooks

For custom integrations:

```yaml
alerts:
  - type: webhook
    url: "https://api.example.com/webhooks/odibi"
    on_events:
      - on_failure
      - on_quarantine
```

### Webhook Payload

```json
{
  "pipeline": "process_orders",
  "status": "QUARANTINE",
  "duration": 45.23,
  "message": "150 rows quarantined in validate_orders",
  "timestamp": "2024-01-30T10:15:00Z",
  "event_type": "on_quarantine",
  "quarantine_details": {
    "rows_quarantined": 150,
    "quarantine_path": "silver/quarantine/orders",
    "failed_tests": ["not_null", "email_format"],
    "node_name": "validate_orders"
  }
}
```

## Programmatic Alerts

Send alerts programmatically:

```python
from odibi.utils.alerting import (
    send_quarantine_alert,
    send_gate_block_alert,
    send_threshold_breach_alert,
)
from odibi.config import AlertConfig, AlertType

config = AlertConfig(
    type=AlertType.SLACK,
    url="https://hooks.slack.com/...",
)

# Quarantine alert
send_quarantine_alert(
    config=config,
    pipeline="process_orders",
    node_name="validate_orders",
    rows_quarantined=150,
    quarantine_path="silver/quarantine/orders",
    failed_tests=["not_null", "email_format"],
)

# Gate block alert
send_gate_block_alert(
    config=config,
    pipeline="process_orders",
    node_name="validate_orders",
    pass_rate=0.92,
    required_rate=0.95,
    failed_rows=1542,
    total_rows=20000,
    failure_reasons=["Pass rate 92.0% < required 95.0%"],
)
```

## Best Practices

1. **Use throttling** - Prevent alert fatigue during incidents
2. **Separate channels** - Critical vs informational alerts
3. **Include context** - Story URLs help with debugging
4. **Test webhooks** - Verify connectivity before production
5. **Monitor alert volume** - High volumes indicate systemic issues

## Related

- [Quarantine Tables](quarantine.md) - Quarantine event source
- [Quality Gates](quality_gates.md) - Gate block event source
- [YAML Schema Reference](../reference/yaml_schema.md#alertconfig)
