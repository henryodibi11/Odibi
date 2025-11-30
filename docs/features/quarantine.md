# Quarantine Tables

Route failed validation rows to a dedicated quarantine table with rejection metadata for later analysis and reprocessing.

## Overview

When validation tests fail, Odibi provides three options via `on_fail`:
- `fail` - Stop the entire pipeline (default)
- `warn` - Log and continue with all rows
- `quarantine` - Route failed rows to a quarantine table, continue with valid rows

The quarantine option preserves bad data for debugging without blocking production pipelines.

## Configuration

### Basic Quarantine Setup

```yaml
nodes:
  - name: process_customers
    read:
      connection: bronze
      path: customers_raw

    validation:
      tests:
        - type: not_null
          columns: [customer_id, email]
          on_fail: quarantine  # Route failures to quarantine
        - type: regex_match
          column: email
          pattern: "^[^@]+@[^@]+\\.[^@]+$"
          on_fail: quarantine

      quarantine:
        connection: silver
        path: quarantine/customers
        add_columns:
          _rejection_reason: true
          _rejected_at: true
          _source_batch_id: true
          _failed_tests: true
```

### Quarantine Config Options

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `connection` | string | Yes | Connection for quarantine writes |
| `path` | string | No* | Path for quarantine data |
| `table` | string | No* | Table name for quarantine |
| `add_columns` | object | No | Metadata columns to add |
| `retention_days` | int | No | Days to retain (default: 90) |

*Either `path` or `table` is required.

### Metadata Columns

Control which metadata columns are added to quarantined rows:

```yaml
quarantine:
  connection: silver
  path: quarantine/customers
  add_columns:
    _rejection_reason: true    # Description of why row failed
    _rejected_at: true         # UTC timestamp of rejection
    _source_batch_id: true     # Run ID for traceability
    _failed_tests: true        # Comma-separated list of failed tests
    _original_node: false      # Node name (disabled by default)
```

## How It Works

1. **Test Evaluation**: Each test with `on_fail: quarantine` is evaluated per-row
2. **Row Splitting**: DataFrame is split into valid and invalid portions
3. **Metadata Addition**: Failed rows receive metadata columns
4. **Quarantine Write**: Invalid rows are appended to the quarantine table
5. **Pipeline Continues**: Valid rows proceed through the pipeline

```
┌─────────────────┐
│   Input Data    │
│   (100 rows)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Validation    │
│   Tests Run     │
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
┌───────┐  ┌───────────┐
│ Valid │  │  Invalid  │
│(95)   │  │   (5)     │
└───┬───┘  └─────┬─────┘
    │            │
    ▼            ▼
┌───────┐  ┌───────────┐
│Target │  │Quarantine │
│ Table │  │  Table    │
└───────┘  └───────────┘
```

## Example: Complete Quarantine Pipeline

```yaml
project: CustomerData
engine: spark

connections:
  bronze:
    type: local
    base_path: ./data/bronze
  silver:
    type: local
    base_path: ./data/silver

pipelines:
  - pipeline: ingest_customers
    layer: silver
    nodes:
      - name: validate_customers
        read:
          connection: bronze
          path: customers_raw
          format: parquet

        validation:
          tests:
            # Required fields
            - type: not_null
              columns: [customer_id, email, created_at]
              on_fail: quarantine

            # Email format
            - type: regex_match
              column: email
              pattern: "^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$"
              on_fail: quarantine

            # Age validation
            - type: range
              column: age
              min: 0
              max: 150
              on_fail: quarantine

          quarantine:
            connection: silver
            path: quarantine/customers
            add_columns:
              _rejection_reason: true
              _rejected_at: true
              _source_batch_id: true
              _failed_tests: true

        write:
          connection: silver
          path: customers
          format: delta
          mode: append
```

## Querying Quarantine Data

After running the pipeline, query the quarantine table to analyze failures:

```sql
-- View recent quarantined rows
SELECT
    customer_id,
    email,
    _rejection_reason,
    _failed_tests,
    _rejected_at,
    _source_batch_id
FROM quarantine.customers
WHERE _rejected_at >= current_date() - INTERVAL 7 DAYS
ORDER BY _rejected_at DESC;

-- Count failures by test type
SELECT
    _failed_tests,
    COUNT(*) as count
FROM quarantine.customers
GROUP BY _failed_tests
ORDER BY count DESC;
```

## Alerts for Quarantine Events

Configure alerts to notify when rows are quarantined:

```yaml
alerts:
  - type: slack
    url: "${SLACK_WEBHOOK_URL}"
    on_events:
      - on_quarantine
    metadata:
      throttle_minutes: 15
      channel: "#data-quality"
```

## Best Practices

1. **Use meaningful test names** - Helps identify failures in quarantine data
2. **Set appropriate retention** - Balance storage costs vs debugging needs
3. **Monitor quarantine rates** - High rates may indicate upstream data issues
4. **Combine with gates** - Use quality gates to abort if too many rows are quarantined
5. **Automate reprocessing** - Build workflows to reprocess fixed quarantine data

## Related

- [Quality Gates](quality_gates.md) - Batch-level validation
- [Alerting](alerting.md) - Alert on quarantine events
- [YAML Schema Reference](../reference/yaml_schema.md#quarantineconfig)
