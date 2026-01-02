# Enhanced Alerting

Real-time notifications for pipeline events with throttling, event-specific payloads, @mentions, and support for Slack, Teams, and generic webhooks.

## Overview

Odibi's alerting system provides:

- **Multiple channels**: Slack, Teams (Power Automate), generic webhooks
- **Event-specific payloads**: Contextual information for each event type
- **Throttling**: Prevent alert spam with rate limiting
- **@Mentions**: Tag users on alerts (Teams)
- **Rich formatting**: Adaptive Cards for Teams, Block Kit for Slack

## Quick Start

```yaml
alerts:
  - type: teams
    url: "${TEAMS_WEBHOOK_URL}"
    on_events:
      - on_failure
    metadata:
      mention_on_failure: "data-team@company.com"
```

## Configuration

### Alert Config Options

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | Alert type: `slack`, `teams`, `webhook` |
| `url` | string | Yes | Webhook URL |
| `on_events` | list | No | Events to trigger on (default: `on_failure`) |
| `metadata` | object | No | Extra settings (throttling, mentions, etc.) |

## Event Types

| Event | Description | Triggered When |
|-------|-------------|----------------|
| `on_start` | Pipeline started | Pipeline execution begins |
| `on_success` | Pipeline completed | Pipeline finishes without errors |
| `on_failure` | Pipeline failed | Pipeline encounters an error |
| `on_quarantine` | Rows quarantined | Validation routes bad rows to quarantine |
| `on_gate_block` | Gate blocked | Quality gate prevents pipeline from continuing |
| `on_threshold_breach` | Threshold exceeded | A metric exceeds its configured threshold |

## Metadata Reference

The `metadata` field controls throttling, mentions, and channel routing.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `throttle_minutes` | int | `15` | Minimum minutes between repeated alerts for the same pipeline+event |
| `max_per_hour` | int | `10` | Maximum alerts of the same type per hour |
| `channel` | string | — | Target channel override (Slack only) |
| `mention` | string or list | — | User email(s) to @mention on **all** events (Teams only) |
| `mention_on_failure` | string or list | — | User email(s) to @mention on **failure events only** (Teams only) |

!!! note "All metadata keys are optional"
    If not specified, defaults are applied automatically.

### @Mentions (Teams)

Tag users directly in Teams alerts to ensure they see critical notifications.

**Single user:**

```yaml
metadata:
  mention_on_failure: "alice@company.com"
```

**Multiple users:**

```yaml
metadata:
  mention_on_failure:
    - "alice@company.com"
    - "bob@company.com"
    - "charlie@company.com"
```

**Mention on all events vs failures only:**

```yaml
metadata:
  mention: "ops-team@company.com"              # @mention on ALL events
  mention_on_failure: "on-call@company.com"    # @mention ONLY on failures
```

When a failure event occurs, both `mention` and `mention_on_failure` users are tagged.

## Throttling

Prevent alert spam with time-based and rate-based throttling:

```yaml
alerts:
  - type: teams
    url: "${TEAMS_WEBHOOK_URL}"
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

Triggered when validation routes bad rows to a quarantine table.

```yaml
alerts:
  - type: teams
    url: "${TEAMS_WEBHOOK_URL}"
    on_events:
      - on_quarantine
```

**Payload includes:**

- Rows quarantined count
- Quarantine table path
- Failed test names
- Node name

### Gate Block Events

Triggered when a quality gate prevents the pipeline from continuing.

```yaml
alerts:
  - type: teams
    url: "${TEAMS_WEBHOOK_URL}"
    on_events:
      - on_gate_block
```

**Payload includes:**

- Pass rate achieved
- Required pass rate
- Number of failed rows
- Failure reasons

### Threshold Breach Events

Triggered when a metric exceeds its configured threshold.

```yaml
alerts:
  - type: webhook
    url: "https://api.example.com/alerts"
    on_events:
      - on_threshold_breach
```

**Payload includes:**

- Metric name
- Threshold value
- Actual value
- Node name

## Complete Example

A comprehensive alerting setup with separate channels for different severity levels:

```yaml
project: SalesAnalytics
engine: spark

alerts:
  # Critical alerts - immediate notification with @mentions
  - type: teams
    url: "${TEAMS_WEBHOOK_URL_CRITICAL}"
    on_events:
      - on_failure
      - on_gate_block
    metadata:
      throttle_minutes: 5
      max_per_hour: 20
      mention_on_failure:
        - "data-lead@company.com"
        - "on-call@company.com"

  # Informational alerts - pipeline lifecycle
  - type: teams
    url: "${TEAMS_WEBHOOK_URL_INFO}"
    on_events:
      - on_start
      - on_success
    metadata:
      throttle_minutes: 1
      max_per_hour: 50
      mention: "data-team@company.com"

  # Data quality alerts - quarantine notifications
  - type: teams
    url: "${TEAMS_WEBHOOK_URL_QUALITY}"
    on_events:
      - on_quarantine
      - on_threshold_breach
    metadata:
      throttle_minutes: 30
      max_per_hour: 10

  # External integration - send to monitoring system
  - type: webhook
    url: "${MONITORING_WEBHOOK_URL}"
    on_events:
      - on_failure
      - on_success
    metadata:
      throttle_minutes: 0  # No throttling for monitoring

connections:
  # ...

pipelines:
  - pipeline: daily_sales
    nodes:
      - name: validate_transactions
        validation:
          tests:
            - type: not_null
              columns: [transaction_id, amount]
              on_fail: quarantine
          quarantine:
            connection: silver
            path: quarantine/transactions
          gate:
            require_pass_rate: 0.95
```

## Teams Configuration

### Power Automate Workflows

Teams alerts use Power Automate workflows (classic Incoming Webhooks were retired December 2025).

**Setup:**

1. In Teams, go to your channel
2. Click `...` → **Workflows** → **Post to a channel when a webhook request is received**
3. Configure the workflow and copy the webhook URL
4. Use the URL in your alert config

!!! warning "Private Channels"
    Power Automate bots may not have access to private channels by default.
    Use a standard (public) channel or explicitly add the Workflows app to your private channel.

### Adaptive Card Format

Teams alerts use Adaptive Cards with:

- Color-coded header (red for failures, green for success, orange for warnings)
- Fact set with key metrics (duration, rows processed, etc.)
- @mentions for tagged users
- Story path for debugging

**Example card contents:**

```
┌──────────────────────────────────────────┐
│ 🚫 Pipeline: daily_sales - FAILED        │
│ Project: SalesAnalytics | Status: FAILED │
├──────────────────────────────────────────┤
│ ⏱ Duration:      45.23s                  │
│ 📅 Time:         2026-01-02T15:30:00Z    │
│ 📊 Rows:         125,000                 │
│ 📂 Story:        stories/daily_sales/... │
│                                          │
│ 🔔 @alice @bob                           │
└──────────────────────────────────────────┘
```

## Slack Configuration

### Block Kit Format

Slack alerts use Block Kit for rich formatting:

```yaml
alerts:
  - type: slack
    url: "${SLACK_WEBHOOK_URL}"
    on_events:
      - on_failure
    metadata:
      channel: "#data-alerts"  # Override default channel
```

**Example card:**

```
┌──────────────────────────────────────────┐
│ 🚫 ODIBI: daily_sales - GATE_BLOCKED     │
├──────────────────────────────────────────┤
│ Project:    SalesAnalytics               │
│ Status:     GATE_BLOCKED                 │
│ Duration:   45.23s                       │
│ Pass Rate:  92.3%                        │
│ Required:   95.0%                        │
│ Rows Failed: 1,542                       │
└──────────────────────────────────────────┘
```

## Generic Webhooks

For custom integrations with monitoring systems, ticketing tools, or other services:

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
  "pipeline": "daily_sales",
  "status": "QUARANTINE",
  "duration": 45.23,
  "message": "150 rows quarantined in validate_transactions",
  "timestamp": "2026-01-02T10:15:00Z",
  "event_type": "on_quarantine",
  "quarantine_details": {
    "rows_quarantined": 150,
    "quarantine_path": "silver/quarantine/transactions",
    "failed_tests": ["not_null", "valid_amount"],
    "node_name": "validate_transactions"
  }
}
```

## Programmatic Alerts

Send alerts programmatically from Python code:

```python
from odibi.utils.alerting import (
    send_alert,
    send_quarantine_alert,
    send_gate_block_alert,
)
from odibi.config import AlertConfig, AlertType

config = AlertConfig(
    type=AlertType.TEAMS,
    url="https://your-webhook-url...",
    metadata={"mention_on_failure": ["alice@company.com"]},
)

# Generic alert
send_alert(
    config=config,
    message="Custom alert message",
    context={
        "pipeline": "my_pipeline",
        "status": "WARNING",
        "event_type": "on_failure",
    },
)

# Quarantine alert
send_quarantine_alert(
    config=config,
    pipeline="daily_sales",
    node_name="validate_transactions",
    rows_quarantined=150,
    quarantine_path="silver/quarantine/transactions",
    failed_tests=["not_null", "valid_amount"],
)

# Gate block alert
send_gate_block_alert(
    config=config,
    pipeline="daily_sales",
    node_name="validate_transactions",
    pass_rate=0.92,
    required_rate=0.95,
    failed_rows=1542,
    total_rows=20000,
    failure_reasons=["Pass rate 92.0% < required 95.0%"],
)
```

## Best Practices

1. **Use throttling** - Prevent alert fatigue during cascading failures
2. **Separate channels** - Critical alerts vs informational notifications
3. **Use @mentions sparingly** - Reserve for truly critical events
4. **Test webhooks** - Verify connectivity before production deployment
5. **Monitor alert volume** - High volumes often indicate systemic issues
6. **Include context** - Story paths help with debugging

## Troubleshooting

### Alerts not being sent

1. Check that the webhook URL is correct and accessible
2. Verify the event type matches your `on_events` configuration
3. Check throttling settings - you may be rate-limited

### Teams @mentions not working

1. Ensure you're using the correct email address format
2. The user must be a member of the Teams channel
3. Power Automate workflow must have permission to mention users

### Power Automate "Bot not in roster" error

The Workflows app doesn't have access to the channel. Solutions:

1. Use a standard (public) channel instead of private
2. Add the Workflows app to your private channel manually

## Related

- [Quarantine Tables](quarantine.md) - Quarantine event source
- [Quality Gates](quality_gates.md) - Gate block event source
- [YAML Schema Reference](../reference/yaml_schema.md#alertconfig)
