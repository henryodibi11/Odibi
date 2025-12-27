# Validation Tests

Row-level data quality checks that run **after** transformation.

## Overview

Validation tests evaluate your **output data** after transformations complete. Unlike contracts (which always fail), validation tests offer flexible responses: fail the pipeline, log a warning, or quarantine bad rows.

```
┌─────────────────────────────────────────────────────────────┐
│  TRANSFORMATION                                             │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  VALIDATION TESTS (You are here)                            │
│  • Runs AFTER transformation                                │
│  • Configurable response (fail/warn/quarantine)             │
│  • Row-level evaluation                                     │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  QUALITY GATES                                              │
│  • Batch-level thresholds                                   │
│  • "Did 95% of rows pass?"                                  │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

```yaml
nodes:
  - name: process_orders
    read:
      connection: bronze
      path: orders_raw

    transform:
      steps:
        - sql: "SELECT *, amount * quantity AS total FROM df"

    # Validation runs after transform
    validation:
      tests:
        - type: not_null
          columns: [order_id, customer_id]
        - type: range
          column: total
          min: 0
        - type: unique
          columns: [order_id]

      on_fail: quarantine  # Route bad rows instead of failing

      quarantine:
        connection: silver
        path: quarantine/orders

    write:
      connection: silver
      path: orders
```

## Test Types Reference

### not_null

Ensures columns contain no NULL values.

```yaml
validation:
  tests:
    - type: not_null
      columns: [order_id, customer_id, email]
      on_fail: quarantine
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `columns` | list | Yes | Columns to check for nulls |
| `on_fail` | string | No | `fail`, `warn`, or `quarantine` |

### unique

Ensures column values (or combinations) are unique.

```yaml
validation:
  tests:
    # Single column
    - type: unique
      columns: [order_id]

    # Composite key
    - type: unique
      columns: [order_id, line_item_id]
      on_fail: fail  # Duplicates are critical
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `columns` | list | Yes | Columns that form the unique key |
| `on_fail` | string | No | `fail`, `warn`, or `quarantine` |

### range

Ensures values fall within min/max bounds.

```yaml
validation:
  tests:
    - type: range
      column: age
      min: 0
      max: 150

    - type: range
      column: price
      min: 0.01
      # max is optional

    - type: range
      column: order_date
      min: "2020-01-01"
      max: "2030-12-31"
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `column` | string | Yes | Column to check |
| `min` | number/string | No | Minimum value (inclusive) |
| `max` | number/string | No | Maximum value (inclusive) |
| `on_fail` | string | No | `fail`, `warn`, or `quarantine` |

### accepted_values

Ensures column only contains allowed values.

```yaml
validation:
  tests:
    - type: accepted_values
      column: status
      values: [pending, approved, rejected, cancelled]
      on_fail: warn  # Log but don't fail
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `column` | string | Yes | Column to check |
| `values` | list | Yes | Allowed values |
| `on_fail` | string | No | `fail`, `warn`, or `quarantine` |

### regex_match

Ensures values match a regex pattern.

```yaml
validation:
  tests:
    - type: regex_match
      column: email
      pattern: "^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$"

    - type: regex_match
      column: phone
      pattern: "^\\+?[1-9]\\d{1,14}$"
      on_fail: quarantine
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `column` | string | Yes | Column to check |
| `pattern` | string | Yes | Regex pattern |
| `on_fail` | string | No | `fail`, `warn`, or `quarantine` |

### row_count

Validates total row count.

```yaml
validation:
  tests:
    - type: row_count
      min: 100
      max: 1000000
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `min` | int | No | Minimum row count |
| `max` | int | No | Maximum row count |
| `on_fail` | string | No | `fail`, `warn`, or `quarantine` |

### custom_sql

Runs a custom SQL condition.

```yaml
validation:
  tests:
    - type: custom_sql
      name: positive_profit  # Named for clarity
      condition: "revenue >= cost"
      threshold: 0.05  # Allow up to 5% failures

    - type: custom_sql
      condition: "end_date >= start_date OR end_date IS NULL"
      threshold: 0.0  # Zero tolerance
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `condition` | string | Yes | SQL WHERE clause (should be true for valid rows) |
| `threshold` | float | No | Allowed failure rate (0.0 = no failures, 0.05 = 5%) |
| `name` | string | No | Name for reporting |
| `on_fail` | string | No | `fail`, `warn`, or `quarantine` |

### volume_drop

Detects unexpected drops in data volume compared to previous runs.

```yaml
validation:
  tests:
    - type: volume_drop
      max_drop_percentage: 50  # Fail if count drops >50%
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `max_drop_percentage` | float | Yes | Maximum allowed drop (0-100) |
| `on_fail` | string | No | `fail`, `warn`, or `quarantine` |

## On-Fail Actions

Each test can specify what happens when it fails:

| Action | Behavior |
|--------|----------|
| `fail` | Stop pipeline immediately (default) |
| `warn` | Log warning, continue with all rows |
| `quarantine` | Route failed rows to quarantine table, continue with valid rows |

```yaml
validation:
  tests:
    # Critical - must pass
    - type: unique
      columns: [order_id]
      on_fail: fail

    # Important but recoverable
    - type: not_null
      columns: [email]
      on_fail: quarantine

    # Nice to have
    - type: regex_match
      column: phone
      pattern: "^\\d{10}$"
      on_fail: warn
```

## Combining with Quality Gates

Quality gates evaluate **aggregate pass rates** after all tests run:

```yaml
validation:
  tests:
    - type: not_null
      columns: [customer_id]
    - type: range
      column: amount
      min: 0

  gate:
    require_pass_rate: 0.95  # 95% must pass ALL tests
    on_fail: abort

    # Per-test thresholds
    thresholds:
      - test: not_null
        min_pass_rate: 0.99  # 99% for not_null
      - test: range
        min_pass_rate: 0.90  # 90% for range
```

See [Quality Gates](../features/quality_gates.md) for full documentation.

## Combining with Quarantine

Route failed rows to a quarantine table for review:

```yaml
validation:
  tests:
    - type: not_null
      columns: [customer_id, email]
      on_fail: quarantine

    - type: regex_match
      column: email
      pattern: "^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$"
      on_fail: quarantine

  quarantine:
    connection: silver
    path: quarantine/customers
    add_columns:
      _rejection_reason: true
      _rejected_at: true
      _failed_tests: true
```

See [Quarantine](../features/quarantine.md) for full documentation.

## Real-World Examples

### Example 1: Customer Data Validation

```yaml
nodes:
  - name: validate_customers
    read:
      connection: bronze
      path: customers_raw

    transform:
      steps:
        - sql: |
            SELECT
              customer_id,
              LOWER(TRIM(email)) AS email,
              phone,
              signup_date,
              country
            FROM df

    validation:
      tests:
        # Required fields
        - type: not_null
          columns: [customer_id, email, signup_date]
          on_fail: quarantine

        # Email format
        - type: regex_match
          column: email
          pattern: "^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$"
          on_fail: quarantine

        # Valid countries
        - type: accepted_values
          column: country
          values: [US, CA, UK, DE, FR, AU]
          on_fail: warn  # Log but allow

        # No duplicates
        - type: unique
          columns: [customer_id]
          on_fail: fail  # Critical

      quarantine:
        connection: silver
        path: quarantine/customers
        add_columns:
          _rejection_reason: true
          _rejected_at: true

      gate:
        require_pass_rate: 0.95
        on_fail: abort

    write:
      connection: silver
      path: customers
      format: delta
```

### Example 2: Financial Transactions

```yaml
nodes:
  - name: validate_transactions
    read:
      connection: bronze
      path: transactions_raw

    validation:
      tests:
        # Business rules
        - type: custom_sql
          name: valid_amount
          condition: "amount > 0"
          on_fail: quarantine

        - type: custom_sql
          name: balanced_transaction
          condition: "debit_amount = credit_amount"
          threshold: 0.0  # Zero tolerance

        # Date sanity
        - type: range
          column: transaction_date
          min: "2020-01-01"
          on_fail: quarantine

        # Volume monitoring
        - type: volume_drop
          max_drop_percentage: 30
          on_fail: fail

      quarantine:
        connection: silver
        path: quarantine/transactions

    write:
      connection: silver
      path: transactions
```

### Example 3: Mixed Severity Levels

```yaml
validation:
  tests:
    # CRITICAL - Pipeline must stop
    - type: unique
      columns: [transaction_id]
      on_fail: fail

    # IMPORTANT - Quarantine for review
    - type: not_null
      columns: [customer_id, amount]
      on_fail: quarantine

    # MODERATE - Track but continue
    - type: accepted_values
      column: category
      values: [retail, wholesale, online]
      on_fail: warn

    # MONITORING - Statistical check
    - type: custom_sql
      name: amount_sanity
      condition: "amount < 100000"
      threshold: 0.01  # Allow 1% outliers
      on_fail: warn
```

## Best Practices

1. **Name your tests** - Use the `name` field for clarity in logs and quarantine tables
2. **Layer your severity** - Use `fail` for critical, `quarantine` for recoverable, `warn` for monitoring
3. **Combine with gates** - Set pass rate thresholds for batch-level control
4. **Use quarantine generously** - Don't lose bad data; capture it for analysis
5. **Keep custom_sql readable** - Break complex conditions into multiple named tests

## See Also

- [Validation Overview](README.md) - The 4-layer validation model
- [Contracts](contracts.md) - Pre-transform fail-fast checks
- [Quality Gates](../features/quality_gates.md) - Batch-level thresholds
- [Quarantine](../features/quarantine.md) - Route bad rows for review
- [YAML Reference](../reference/yaml_schema.md#validationconfig) - Full validation schema
