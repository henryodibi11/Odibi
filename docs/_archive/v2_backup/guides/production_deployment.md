# Production Deployment Guide

This guide outlines best practices for deploying and running ODIBI pipelines in production environments.

## 🚀 Deployment Strategies

### 1. Environment Segregation
Use separate configurations for `dev`, `staging`, and `prod` using environment variables.

```yaml
# project.yaml
connections:
  datalake:
    type: azure_adls
    account_name: ${ADLS_ACCOUNT_NAME}
    container: ${ADLS_CONTAINER}
```

### 2. Containerization
Dockerize your pipelines for consistent execution.

```dockerfile
FROM python:3.11-slim
RUN pip install odibi[azure,spark]
COPY . /app
WORKDIR /app
CMD ["odibi", "run", "project.yaml"]
```

## 🔒 Security & Compliance

### PII Redaction
Enable `sensitive: true` on nodes handling personal data to prevent leaks in execution stories.

```yaml
- name: process_users
  sensitive: true  # Masks all sample data
  # OR specific columns
  sensitive: ["email", "ssn"]
```

### Credential Management
*   **NEVER** commit secrets to Git.
*   Use Azure Key Vault integration for connection strings.
*   Use `${ENV_VAR}` substitution for sensitive values in YAML.

## 🛡️ Reliability & Alerting

### Retries
Configure global or per-node retries for transient failures.

```yaml
retry:
  enabled: true
  max_attempts: 3
  backoff: exponential
```

### Alerting
Receive notifications on Slack, Teams, or Webhooks when pipelines fail.

```yaml
alerts:
  - type: slack
    url: ${SLACK_WEBHOOK_URL}
    on_events: ["on_failure"]
```

### Idempotency
Use `mode: upsert` or `mode: append_once` to ensure re-runs don't duplicate data.

```yaml
write:
  mode: upsert
  keys: ["id"]
```

## 🧹 Maintenance

### Artifact Retention
Prevent storage bloat by configuring story retention policies.

```yaml
story:
  connection: outputs
  path: stories/
  retention_days: 30   # Keep stories for 30 days
  retention_count: 100 # Keep last 100 runs
```

### Log Levels
Control verbosity to manage log volume.

```yaml
logging:
  level: WARNING
  structured: true  # JSON logs for ingestion (Splunk/Datadog)
```

## 📈 Monitoring

### Structured Logging
Enable `structured: true` to output JSON logs containing:
*   Pipeline duration
*   Node status
*   Row counts
*   Error details

### Story Reports
Use generated Markdown stories for audit trails and lineage tracking.
