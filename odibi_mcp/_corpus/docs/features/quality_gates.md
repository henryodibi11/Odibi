# Quality Gates

Batch-level quality validation that evaluates the entire dataset before writing.

## Overview

While validation tests run per-row, quality gates evaluate aggregate metrics:
- **Overall pass rate** - What percentage of rows passed all tests?
- **Per-test thresholds** - Different requirements for different tests
- **Row count anomalies** - Detect unexpected batch sizes

## Configuration

### Basic Gate Setup

```yaml
nodes:
  - name: load_silver_customers
    read:
      connection: bronze
      path: customers

    validation:
      tests:
        - type: not_null
          columns: [customer_id]
        - type: unique
          columns: [customer_id]

      gate:
        require_pass_rate: 0.95  # 95% must pass
        on_fail: abort           # Stop if gate fails
```

### Gate Config Options

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `require_pass_rate` | float | No | 0.95 | Minimum % of rows passing ALL tests |
| `on_fail` | string | No | "abort" | Action on failure |
| `thresholds` | list | No | [] | Per-test thresholds |
| `row_count` | object | No | null | Row count validation |

### On-Fail Actions

| Action | Description |
|--------|-------------|
| `abort` | Stop pipeline, write nothing (default) |
| `warn_and_write` | Log warning, write all rows anyway |
| `write_valid_only` | Write only rows that passed validation |

## Per-Test Thresholds

Set different requirements for specific tests:

```yaml
gate:
  require_pass_rate: 0.95  # Global: 95% must pass all tests

  thresholds:
    - test: not_null
      min_pass_rate: 0.99  # 99% for not_null (stricter)
    - test: unique
      min_pass_rate: 1.0   # 100% unique (no duplicates allowed)
    - test: email_format   # Named test
      min_pass_rate: 0.90  # 90% for email format (more lenient)
```

## Row Count Validation

Detect anomalies in batch size:

```yaml
gate:
  row_count:
    min: 100              # Fail if fewer than 100 rows
    max: 1000000          # Fail if more than 1M rows
    change_threshold: 0.5 # Fail if count changes >50% vs last run
```

### Row Count Options

| Field | Type | Description |
|-------|------|-------------|
| `min` | int | Minimum expected row count |
| `max` | int | Maximum expected row count |
| `change_threshold` | float | Max allowed change vs previous run (0.5 = 50%) |

## Complete Example

```yaml
nodes:
  - name: process_orders
    read:
      connection: bronze
      path: orders_raw

    validation:
      tests:
        # Critical fields
        - type: not_null
          name: required_fields
          columns: [order_id, customer_id, order_date]

        # Uniqueness
        - type: unique
          name: unique_orders
          columns: [order_id]

        # Business rules
        - type: range
          name: valid_amount
          column: amount
          min: 0

        - type: accepted_values
          name: valid_status
          column: status
          values: [pending, completed, cancelled]

      gate:
        # Global threshold
        require_pass_rate: 0.95

        # Per-test overrides
        thresholds:
          - test: required_fields
            min_pass_rate: 0.99
          - test: unique_orders
            min_pass_rate: 1.0

        # Row count checks
        row_count:
          min: 1000
          change_threshold: 0.3

        # What to do on failure
        on_fail: abort

    write:
      connection: silver
      path: orders
      format: delta
```

## Combining Gates with Quarantine

Use both for comprehensive data quality:

```yaml
validation:
  tests:
    - type: not_null
      columns: [customer_id]
      on_fail: quarantine  # Route failures to quarantine

    - type: unique
      columns: [customer_id]
      on_fail: fail        # Critical - must pass

  quarantine:
    connection: silver
    path: quarantine/customers

  gate:
    require_pass_rate: 0.95  # Still need 95% overall
    on_fail: abort
```

**Flow:**
1. Rows failing `not_null` are quarantined
2. Gate evaluates remaining rows
3. If <95% pass, pipeline aborts
4. Otherwise, valid rows are written

## Gate Failure Alerts

Get notified when gates fail:

```yaml
alerts:
  - type: slack
    url: "${SLACK_WEBHOOK_URL}"
    on_events:
      - on_gate_block
    metadata:
      throttle_minutes: 15
      channel: "#data-alerts"
```

Alert payload includes:
- Pass rate achieved vs required
- Number of failed rows
- Failure reasons

## GateFailedError

When a gate fails with `on_fail: abort`, a `GateFailedError` is raised:

```python
from odibi.exceptions import GateFailedError

try:
    pipeline.run()
except GateFailedError as e:
    print(f"Gate failed: {e.pass_rate:.1%} < {e.required_rate:.1%}")
    print(f"Reasons: {e.failure_reasons}")
```

## Best Practices

1. **Start with high thresholds** - Be strict initially, relax as needed
2. **Use per-test thresholds** - Critical tests (uniqueness) should be 100%
3. **Monitor row count changes** - Sudden changes often indicate problems
4. **Combine with quarantine** - Don't lose failed data, route it for analysis
5. **Set up alerts** - Know immediately when gates fail

## Related

- [Quarantine Tables](quarantine.md) - Route failed rows
- [Alerting](alerting.md) - Alert on gate failures
- [YAML Schema Reference](../reference/yaml_schema.md#gateconfig)
