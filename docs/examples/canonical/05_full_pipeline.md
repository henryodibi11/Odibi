# Example 5: Full Pipeline (Validation + Quarantine + Alerting)

A production-ready pipeline with data contracts, quality gates, quarantine routing, and Slack alerts.

## When to Use

- Production workloads
- Data quality is critical
- Need observability and alerting

---

## Full Config

```yaml
# odibi.yaml
project: production_orders

engine: spark

# Resilience
retry:
  enabled: true
  max_attempts: 3
  backoff: exponential

# Alerting
alerts:
  - type: slack
    url: ${SLACK_WEBHOOK_URL}
    on_events: [on_failure, on_quality_gate_fail]

connections:
  source_db:
    type: sql_server
    host: ${SQL_SERVER_HOST}
    database: production
    username: ${SQL_USER}
    password: ${SQL_PASSWORD}

  lake:
    type: local
    base_path: ./data/lake

story:
  connection: lake
  path: stories

system:
  connection: lake
  path: _system

pipelines:
  # ─────────────────────────────────────────────
  # BRONZE: Ingest with contracts
  # ─────────────────────────────────────────────
  - pipeline: bronze
    layer: bronze
    nodes:
      - name: ingest_orders
        read:
          connection: source_db
          format: jdbc
          table: dbo.orders

        # Fail fast if source is broken
        contracts:
          - type: row_count
            min: 1
          - type: not_null
            columns: [order_id, customer_id, amount]
          - type: freshness
            column: created_at
            max_age: "24h"

        incremental:
          mode: stateful
          column: updated_at

        write:
          connection: lake
          format: delta
          path: bronze/orders
          mode: append

  # ─────────────────────────────────────────────
  # SILVER: Clean with validation gates
  # ─────────────────────────────────────────────
  - pipeline: silver
    layer: silver
    nodes:
      - name: clean_orders
        read:
          connection: lake
          format: delta
          path: bronze/orders

        transformer: deduplicate
        params:
          keys: [order_id]
          order_by: "updated_at DESC"

        transform:
          steps:
            - function: filter_rows
              params:
                condition: "amount > 0"
            - function: derive_columns
              params:
                derivations:
                  amount_usd: "amount * 1.0"
                  order_year: "YEAR(order_date)"

        validation:
          tests:
            - type: unique
              columns: [order_id]
            - type: not_null
              columns: [customer_id, amount_usd]
            - type: range
              column: amount_usd
              min: 0
              max: 1000000

          gate:
            require_pass_rate: 0.95
            on_failure: warn  # 'fail' to stop pipeline

          quarantine:
            connection: lake
            path: quarantine/orders

        write:
          connection: lake
          format: delta
          path: silver/orders
          mode: overwrite

  # ─────────────────────────────────────────────
  # GOLD: Aggregate for BI
  # ─────────────────────────────────────────────
  - pipeline: gold
    layer: gold
    nodes:
      - name: agg_daily_sales
        depends_on: [clean_orders]

        pattern:
          type: aggregation
          params:
            grain: [order_date, customer_id]
            measures:
              - name: total_amount
                expr: "SUM(amount_usd)"
              - name: order_count
                expr: "COUNT(*)"
              - name: avg_order_value
                expr: "AVG(amount_usd)"

        write:
          connection: lake
          format: delta
          path: gold/daily_sales
          mode: overwrite
```

---

## What This Config Does

| Stage | Action |
|-------|--------|
| **Bronze** | Ingest from SQL with contracts (fail if empty/stale) |
| **Silver** | Dedupe, filter, validate, quarantine bad rows |
| **Gold** | Aggregate for BI consumption |
| **Alerts** | Slack notification on failure |

---

## Run

```bash
# Set environment variables
export SQL_SERVER_HOST=your-server.database.windows.net
export SQL_USER=reader
export SQL_PASSWORD=your-password
export SLACK_WEBHOOK_URL=https://hooks.slack.com/...

# Run
odibi run odibi.yaml
```

---

## Inspect Quarantine

```bash
# Check quarantined rows
odibi run odibi.yaml --node quarantine_report
```

Or query directly:
```python
spark.read.format("delta").load("data/lake/quarantine/orders").show()
```

---

## Schema Reference

| Key | Docs |
|-----|------|
| `contracts` | [ContractConfig](../../reference/yaml_schema.md#contractconfig) |
| `validation.tests` | [ValidationConfig](../../reference/yaml_schema.md#validationconfig) |
| `validation.gate` | [Quality Gates](../../features/quality_gates.md) |
| `validation.quarantine` | [Quarantine](../../features/quarantine.md) |
| `alerts` | [Alerting](../../features/alerting.md) |
| `retry` | [RetryConfig](../../reference/yaml_schema.md#retryconfig) |

---

## Decision: Contracts vs Validation Tests

| Use Contracts when... | Use Validation when... |
|-----------------------|------------------------|
| Checking **source** data before processing | Checking **output** after transformation |
| Fail-fast is required | Soft warnings are acceptable |
| Freshness, schema, volume checks | Row-level quality (nulls, ranges) |

---

## See Also

- [Validation Overview](../../validation/README.md)
- [Alerting Guide](../../features/alerting.md)
- [Production Deployment](../../guides/production_deployment.md)
