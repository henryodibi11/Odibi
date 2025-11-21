# Monitoring and Observability Guide

This guide explains how to monitor Odibi pipelines, interpret logs, and set up alerting for production environments.

## 🔍 Observability Pillars

Odibi provides three pillars of observability:

1.  **Execution Stories:** Rich, human-readable reports of every pipeline run.
2.  **Structured Logging:** Machine-readable JSON logs for ingestion by monitoring tools.
3.  **Alerting Hooks:** Real-time notifications to Slack, Teams, or Webhooks.

---

## 1. Execution Stories

Stories are generated automatically in the configured output path. They provide:
*   **Run Status:** Success/Fail/Skipped summary.
*   **Lineage:** Input/output schema changes.
*   **Data Quality:** Sample data and validation failures.
*   **Performance:** Duration per node.

### Usage
*   **Audit Trail:** Archive stories to S3/ADLS for compliance.
*   **Debugging:** Use the "Error" section in the story to see stack traces and suggestions.

---

## 2. Structured Logging

Enable structured logging in your `project.yaml` to output JSON logs.

```yaml
logging:
  level: INFO
  structured: true
```

### Log Format
Logs follow a consistent JSON schema:

```json
{
  "timestamp": "2023-10-27T10:00:00.123",
  "level": "INFO",
  "message": "Pipeline 'bronze_to_silver' SUCCESS",
  "context": {
    "pipeline": "bronze_to_silver",
    "duration": 45.2,
    "nodes_completed": 5,
    "nodes_failed": 0
  }
}
```

### Integration
*   **Splunk/Datadog/ELK:** Configure your log forwarder (e.g., Fluentd) to parse these JSON lines.
*   **Azure Monitor:** Use the Application Insights SDK (future integration) or file-based collection.

---

## 3. Alerting

Configure alerts to notify your team immediately upon failure.

```yaml
alerts:
  - type: slack
    url: ${SLACK_WEBHOOK_URL}
    on_events: ["on_failure"]
  - type: teams
    url: ${TEAMS_WEBHOOK_URL}
    on_events: ["on_failure", "on_success"]
```

### Alert Payload
Alerts include:
*   Pipeline Name
*   Status (SUCCESS/FAILED)
*   Duration
*   List of failed nodes (if any)

---

## 📊 Key Metrics to Monitor

| Metric | Description | Source |
|--------|-------------|--------|
| **Pipeline Duration** | Total time for end-to-end execution. | Logs / Story |
| **Node Duration** | Time taken by specific heavy transformations. | Logs / Story |
| **Row Counts** | Rows processed (Input vs Output). Detects data loss. | Story (Summary) |
| **Failure Rate** | Percentage of failed runs over time. | Logs (Aggregated) |
| **Data Freshness** | Time since last successful run. | Logs (Timestamp) |

## 🛠️ Troubleshooting

### Common Issues

#### "Node execution failed"
*   Check the **Story** for the specific error message.
*   Look at the **Suggestions** section in the error.
*   Verify **Dependencies** are met (previous nodes succeeded).

#### "Connection failed"
*   Verify credentials in environment variables.
*   Check network access to the data source.

#### Performance Issues
*   Check **Node Duration** in the story.
*   See `docs/performance.md` for tuning tips.
