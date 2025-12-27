# Production Data Pipelines: Monitoring, Retry, and Testing

*Keeping the lights on*

---

## TL;DR

Production pipelines need more than correct code. They need monitoring (know when things fail), retry logic (handle transient failures), alerting (notify the right people), and testing (catch issues before production). This article covers operational patterns for reliable data pipelines.

---

## The Production Checklist

Before going live, verify:

| Category | Check |
|----------|-------|
| **Reliability** | Retry on transient failures |
| **Monitoring** | Metrics collected and dashboarded |
| **Alerting** | Failures notify on-call |
| **Testing** | Unit + integration tests pass |
| **Documentation** | Runbooks exist |
| **Recovery** | Can reprocess from any point |

---

## Monitoring

### What to Monitor

| Metric | Why |
|--------|-----|
| Execution time | Detect slowdowns |
| Row counts | Catch data volume changes |
| Error counts | Track failures |
| Quarantine rate | Data quality trending |
| Late runs | SLA compliance |

### Configuration

```yaml
# Enable metrics collection
system:
  connection: bronze
  path: _system
  
  metrics:
    enabled: true
    table: pipeline_metrics
    retention_days: 90

logging:
  level: INFO
  structured: true  # JSON for log aggregation
```

### Metrics Table

```sql
SELECT 
  run_id,
  pipeline,
  node,
  status,
  rows_read,
  rows_written,
  rows_quarantined,
  execution_seconds,
  started_at,
  completed_at
FROM system.pipeline_metrics
WHERE started_at >= CURRENT_DATE - 7
ORDER BY started_at DESC
```

### Dashboard Queries

```sql
-- Daily success rate
SELECT 
  DATE(started_at) as run_date,
  COUNT(*) as total_runs,
  SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successes,
  ROUND(100.0 * successes / total_runs, 2) as success_rate
FROM system.pipeline_metrics
GROUP BY DATE(started_at)
ORDER BY run_date DESC

-- Slowest nodes this week
SELECT 
  node,
  AVG(execution_seconds) as avg_seconds,
  MAX(execution_seconds) as max_seconds
FROM system.pipeline_metrics
WHERE started_at >= CURRENT_DATE - 7
GROUP BY node
ORDER BY avg_seconds DESC

-- Quarantine trends
SELECT 
  DATE(started_at) as run_date,
  SUM(rows_quarantined) as quarantined,
  SUM(rows_written) as written,
  ROUND(100.0 * quarantined / (written + quarantined), 4) as quarantine_rate
FROM system.pipeline_metrics
GROUP BY DATE(started_at)
ORDER BY run_date DESC
```

---

## Retry Logic

### Transient Failures

Some failures are temporary:
- Network timeout
- Rate limiting
- Service unavailable
- Lock conflicts

These should retry automatically.

### Configuration

```yaml
retry:
  enabled: true
  max_attempts: 3
  backoff: exponential
  initial_delay: 30  # seconds
  max_delay: 300     # 5 minutes
  
  # Which errors to retry
  retryable_errors:
    - "Connection timed out"
    - "Service temporarily unavailable"
    - "Rate limit exceeded"
    - "Lock acquisition failed"
```

### Backoff Strategies

| Strategy | Behavior |
|----------|----------|
| `fixed` | Wait same time each retry (30s, 30s, 30s) |
| `linear` | Increase linearly (30s, 60s, 90s) |
| `exponential` | Double each time (30s, 60s, 120s) |

### Node-Level Retry

Override for specific nodes:

```yaml
- name: load_from_api
  description: "External API - needs extra retries"
  
  retry:
    max_attempts: 5
    backoff: exponential
    initial_delay: 10
```

---

## Alerting

### Alert Configuration

```yaml
alerts:
  # Slack notification
  - type: slack
    url: ${SLACK_WEBHOOK_URL}
    channel: "#data-alerts"
    on_events: [on_failure]
    
  # Email for critical failures
  - type: email
    to: [data-team@company.com, oncall@company.com]
    on_events: [on_failure]
    severity: critical
    
  # PagerDuty for SLA breaches
  - type: pagerduty
    service_key: ${PAGERDUTY_KEY}
    on_events: [on_sla_breach]
```

### Alert Events

| Event | Trigger |
|-------|---------|
| `on_failure` | Pipeline fails |
| `on_warning` | Contract warning |
| `on_success` | Pipeline succeeds (optional) |
| `on_sla_breach` | Runs past deadline |
| `on_quarantine_spike` | High quarantine rate |

### SLA Configuration

```yaml
pipelines:
  - pipeline: silver_ecommerce
    sla:
      deadline: "08:00"  # Must complete by 8 AM
      timezone: "America/Sao_Paulo"
      alert_before: 30   # Alert if not done 30 min before
```

---

## Testing

### Test Levels

| Level | What | When |
|-------|------|------|
| **Unit** | Individual functions | Every commit |
| **Contract** | Data quality rules | Every run |
| **Integration** | End-to-end pipeline | Pre-deploy |
| **Regression** | Known issues | Weekly |

### Unit Tests

```python
# tests/test_transforms.py
import pytest
from odibi.transforms import clean_text, derive_columns

def test_clean_text_uppercase():
    """clean_text should uppercase properly"""
    df = pd.DataFrame({'name': ['john', 'Jane', ' BOB ']})
    result = clean_text(df, columns=['name'], case='upper', trim=True)
    
    assert result['name'].tolist() == ['JOHN', 'JANE', 'BOB']

def test_derive_columns_null_handling():
    """derive_columns should handle NULLs"""
    df = pd.DataFrame({'a': [1, None, 3], 'b': [10, 20, None]})
    result = derive_columns(df, columns={'c': 'a + b'})
    
    assert result['c'].tolist() == [11, None, None]
```

### Contract Tests

```python
# tests/test_contracts.py
def test_not_null_contract_catches_nulls():
    """not_null contract should fail on NULL values"""
    bad_data = pd.DataFrame({
        'order_id': ['ORD-001', 'ORD-002'],
        'customer_id': ['CUST-001', None]
    })
    
    result = run_contract('not_null', column='customer_id', df=bad_data)
    
    assert result.passed == False
    assert result.failure_count == 1

def test_accepted_values_contract():
    """accepted_values should reject invalid values"""
    bad_data = pd.DataFrame({
        'status': ['active', 'inactive', 'bogus']
    })
    
    result = run_contract(
        'accepted_values',
        column='status',
        values=['active', 'inactive'],
        df=bad_data
    )
    
    assert result.passed == False
    assert 'bogus' in result.invalid_values
```

### Integration Tests

```python
# tests/test_integration.py
@pytest.fixture
def test_data():
    """Create test data in Bronze"""
    create_test_tables()
    yield
    cleanup_test_tables()

def test_silver_pipeline_end_to_end(test_data):
    """Full Silver pipeline should complete"""
    result = run_pipeline('silver_ecommerce', test_mode=True)
    
    assert result.status == 'SUCCESS'
    assert result.nodes_completed == 7
    assert result.total_rows > 0

def test_gold_dimensions_populated(test_data):
    """Gold dimensions should have expected rows"""
    run_pipeline('gold_dimensions', test_mode=True)
    
    assert count_rows('dim_customer') > 0
    assert count_rows('dim_product') > 0
    assert count_rows('dim_date') > 0
    
    # Check unknown member exists
    assert exists_row('dim_customer', 'customer_sk = 0')
```

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_contracts.py -v

# Run with coverage
pytest tests/ --cov=odibi --cov-report=html
```

---

## Recovery Patterns

### Reprocessing

```bash
# Rerun failed node
odibi run --node silver_orders --reprocess

# Rerun from specific date
odibi run --node silver_orders --from-date 2023-12-01

# Rerun specific batches
odibi run --node silver_orders --filter "_batch_id = 'batch_20231215'"
```

### Rollback

```bash
# List table versions
odibi history gold.dim_customer

# Rollback to previous version
odibi rollback gold.dim_customer --version 5

# Or restore to timestamp
odibi rollback gold.dim_customer --timestamp "2023-12-14 09:00:00"
```

### Backfill

```yaml
# Historical reprocessing
- name: silver_orders
  read:
    incremental:
      mode: stateful
      column: order_date
      
      # Override for backfill
      first_run_query: |
        SELECT * FROM bronze.orders
        WHERE order_date BETWEEN '2023-01-01' AND '2023-12-31'
```

---

## Runbooks

Document operational procedures:

### Example Runbook

```markdown
# Silver Pipeline Failure Runbook

## Overview
This runbook covers troubleshooting for `silver_ecommerce` pipeline failures.

## Quick Diagnosis

1. Check pipeline status:
   ```bash
   odibi status --last 5
   ```

2. View failed node logs:
   ```bash
   odibi logs $RUN_ID --node $FAILED_NODE
   ```

## Common Issues

### Contract Failure: not_null
**Symptom**: "Contract failed: not_null on customer_id"

**Resolution**:
1. Check Bronze for null values:
   ```sql
   SELECT COUNT(*) FROM bronze.orders WHERE customer_id IS NULL
   ```
2. If source issue: Contact upstream team
3. If temporary: Enable quarantine, rerun

### Schema Change
**Symptom**: "Column 'X' not found"

**Resolution**:
1. Compare schemas:
   ```bash
   odibi schema bronze.orders
   ```
2. Update pipeline config for new schema
3. Rerun

## Escalation
- Level 1: Data team on-call
- Level 2: Platform team
- Level 3: Vendor support
```

---

## Operational Configuration

```yaml
# odibi.yaml - Production configuration

project: "ecommerce_warehouse"
engine: "spark"
version: "1.0.0"

# Reliability
retry:
  enabled: true
  max_attempts: 3
  backoff: exponential

# Observability
logging:
  level: INFO
  structured: true

system:
  connection: bronze
  path: _system
  metrics:
    enabled: true
    retention_days: 90

# Alerting
alerts:
  - type: slack
    url: ${SLACK_WEBHOOK}
    on_events: [on_failure, on_warning]
  
  - type: pagerduty
    service_key: ${PAGERDUTY_KEY}
    on_events: [on_failure]
    severity: critical

# Performance
performance:
  max_concurrent_nodes: 4
  checkpoint_interval: 100000
  spill_to_disk: true

# Quality gates
quality:
  quarantine_threshold: 0.05  # Alert if > 5% quarantined
  row_count_variance: 0.20     # Alert if count varies > 20%
```

---

## Key Takeaways

| Area | Implementation |
|------|----------------|
| **Monitoring** | Collect metrics, build dashboards |
| **Retry** | Exponential backoff for transient failures |
| **Alerting** | Right people, right time, right channel |
| **Testing** | Unit, contract, integration tests |
| **Recovery** | Reprocess, rollback, backfill |
| **Documentation** | Runbooks for common issues |

---

## Next Steps

With production patterns covered, let's reflect on the journey:

- Lessons learned
- Community building
- Open source reflections

Next article: **What I Learned Building an Open Source Data Framework**.

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]
- **Medium:** Follow for more articles in this series
