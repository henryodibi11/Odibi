# Contracts

Pre-transform data quality checks that **fail fast** before any transformation runs.

## Overview

Contracts validate your **input data** before it enters the transformation pipeline. Unlike validation tests (which run after transforms), contracts always halt execution on failure—they're your first line of defense against bad data.

```
┌─────────────────────────────────────────────────────────────┐
│                    INPUT DATA                               │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  CONTRACTS (You are here)                                   │
│  • Runs BEFORE transformation                               │
│  • Always fails on violation                                │
│  • Prevents bad data from entering pipeline                 │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  TRANSFORMATION                                             │
└─────────────────────────────────────────────────────────────┘
```

## When to Use Contracts vs Validation

| Feature | Contracts | Validation Tests |
|---------|-----------|------------------|
| **When it runs** | Before transform | After transform |
| **On failure** | Always fails pipeline | Configurable (fail/warn/quarantine) |
| **Use case** | Input data quality | Output data quality |
| **Example** | "Source must have customer_id" | "Transformed amount must be > 0" |

**Rule of thumb:** Use contracts to validate what you **receive**, use validation to validate what you **produce**.

## Quick Start

```yaml
nodes:
  - name: process_orders
    # Contracts run first - before any transformation
    contracts:
      - type: not_null
        columns: [order_id, customer_id]
      - type: row_count
        min: 100
      - type: freshness
        column: created_at
        max_age: "24h"

    read:
      connection: bronze
      path: orders_raw

    transform:
      steps:
        - sql: "SELECT * FROM df WHERE amount > 0"

    write:
      connection: silver
      path: orders
```

If any contract fails, the pipeline stops immediately with a `ValidationError`—no transformation or write happens.

## Available Contract Types

### not_null

Ensures columns contain no NULL values.

```yaml
contracts:
  - type: not_null
    columns: [order_id, customer_id, created_at]
```

**Use for:** Primary keys, required fields, foreign keys.

### unique

Ensures columns (or combination) contain unique values.

```yaml
# Single column
contracts:
  - type: unique
    columns: [order_id]

# Composite key
contracts:
  - type: unique
    columns: [order_id, line_item_id]
```

**Use for:** Primary keys, natural keys, deduplication verification.

### row_count

Validates row count falls within expected bounds.

```yaml
contracts:
  - type: row_count
    min: 1000      # At least 1000 rows
    max: 100000    # At most 100K rows
```

**Use for:** Detect truncated loads, ensure minimum completeness, cap batch sizes.

### freshness

Validates data is not stale by checking a timestamp column.

```yaml
contracts:
  - type: freshness
    column: updated_at
    max_age: "24h"  # Fail if no data newer than 24 hours
```

**Use for:** SLA monitoring, detecting stale source systems.

### accepted_values

Ensures a column only contains values from an allowed list.

```yaml
contracts:
  - type: accepted_values
    column: status
    values: [pending, approved, rejected, cancelled]
```

**Use for:** Enum fields, status columns, categorical data.

### range

Ensures column values fall within a specified range.

```yaml
contracts:
  - type: range
    column: age
    min: 0
    max: 150

# Date range
contracts:
  - type: range
    column: order_date
    min: "2020-01-01"
    max: "2030-12-31"
```

**Use for:** Numeric bounds (ages, prices, quantities), date ranges.

### regex_match

Ensures column values match a regex pattern.

```yaml
contracts:
  - type: regex_match
    column: email
    pattern: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"

contracts:
  - type: regex_match
    column: phone
    pattern: "^\\+?[1-9]\\d{1,14}$"  # E.164 format
```

**Use for:** Format validation (emails, phone numbers, IDs, codes).

### custom_sql

Runs a custom SQL condition.

```yaml
contracts:
  - type: custom_sql
    condition: "amount > 0 AND quantity > 0"
    threshold: 0.01  # Allow up to 1% failures
```

**Use for:** Complex business rules, multi-column conditions.

### schema

Validates that the DataFrame schema matches expected columns.

```yaml
contracts:
  - type: schema
    strict: true  # Fail if extra columns present

# Works with column definitions
columns:
  - name: order_id
    type: integer
  - name: customer_id
    type: integer
  - name: amount
    type: decimal
```

**Use for:** Schema stability, detecting upstream drift.

### distribution

Checks if a column's statistical distribution is within expected bounds.

```yaml
contracts:
  - type: distribution
    column: price
    metric: mean
    threshold: ">100"  # Mean must be > 100
    on_fail: warn      # Can warn instead of fail

contracts:
  - type: distribution
    column: customer_id
    metric: null_percentage
    threshold: "<0.05"  # Less than 5% nulls
```

**Metrics:** `mean`, `min`, `max`, `null_percentage`

**Use for:** Anomaly detection, data drift monitoring.

## Real-World Examples

### Example 1: Bronze Layer Ingestion

Validate raw data before any processing:

```yaml
nodes:
  - name: ingest_customers
    contracts:
      # Must have data
      - type: row_count
        min: 1

      # Required fields present
      - type: not_null
        columns: [customer_id, email]

      # Data is fresh
      - type: freshness
        column: _extracted_at
        max_age: "48h"

    read:
      connection: source_api
      path: customers
      format: json

    write:
      connection: bronze
      path: customers_raw
      mode: append
      add_metadata: true
```

### Example 2: Silver Layer Processing

Validate before expensive transformations:

```yaml
nodes:
  - name: process_transactions
    contracts:
      # Ensure keys exist for joins
      - type: not_null
        columns: [transaction_id, account_id, merchant_id]

      # No duplicates in source
      - type: unique
        columns: [transaction_id]

      # Amount makes sense
      - type: range
        column: amount
        min: 0.01
        max: 1000000

      # Valid transaction types
      - type: accepted_values
        column: type
        values: [purchase, refund, chargeback, transfer]

    read:
      connection: bronze
      path: transactions_raw

    transform:
      steps:
        - sql: |
            SELECT
              t.*,
              m.merchant_name,
              a.account_type
            FROM df t
            LEFT JOIN merchants m ON t.merchant_id = m.id
            LEFT JOIN accounts a ON t.account_id = a.id

    write:
      connection: silver
      path: transactions
      format: delta
```

### Example 3: Cross-System Data

Validate data from multiple sources before combining:

```yaml
nodes:
  - name: merge_customer_data
    contracts:
      # Schema must match expected structure
      - type: schema
        strict: true

      # Prevent data explosion
      - type: row_count
        max: 10000000

      # Statistical sanity check
      - type: distribution
        column: lifetime_value
        metric: mean
        threshold: ">0"

      # Custom business rule
      - type: custom_sql
        condition: "signup_date <= last_order_date OR last_order_date IS NULL"
        threshold: 0.0  # Zero tolerance

    read:
      - connection: crm
        path: customers
      - connection: ecommerce
        path: users

    transform:
      steps:
        - sql: |
            SELECT * FROM df1
            UNION ALL
            SELECT * FROM df2
```

## Error Handling

When a contract fails, Odibi raises a `ValidationError` with details:

```python
from odibi.exceptions import ValidationError

try:
    pipeline.run()
except ValidationError as e:
    print(f"Contract failed on node: {e.node_name}")
    print(f"Failures: {e.failures}")
    # [{'test': 'not_null', 'column': 'customer_id', 'null_count': 42}]
```

## Best Practices

1. **Start with not_null and unique** - Most contract failures are missing keys or duplicates
2. **Use row_count for safety** - Prevents empty loads and data explosions
3. **Add freshness for SLAs** - Know immediately when source systems are stale
4. **Keep contracts fast** - They run on every execution; avoid expensive checks
5. **Don't over-contract** - Validate what matters; save detailed checks for validation tests

## Troubleshooting

### "ValidationError: Contract Failure" - but which contract?

**Symptom:** Contract fails but error message is unclear.

**Fix:** Check the `failures` list in the error:
```python
except ValidationError as e:
    for failure in e.failures:
        print(f"Test: {failure['test']}, Details: {failure}")
```

Or check the story report for detailed validation results.

### Contract passes locally but fails in production

**Common Causes:**
- Different data volumes (row_count thresholds)
- Timezone differences (freshness checks)
- Case sensitivity differences between engines

**Fixes:**
- Use `--dry-run` in production first to validate
- Set `max_age` with buffer for timezone differences
- Test with production-like data volumes locally

### Freshness contract always fails

**Symptom:** `freshness` check fails even with recent data.

**Causes:**
- Wrong column name for timestamp
- Timestamp column is string, not datetime
- Timezone mismatch

**Fixes:**
```yaml
contracts:
  - type: freshness
    column: updated_at    # Must be datetime type
    max_age: "24h"        # Include buffer for safety
```

### Contract on wrong DataFrame

**Symptom:** Contract checks input data when you wanted to check transformed data.

**Explanation:** Contracts run on **input** data (before transform). For output validation, use `validation.tests` instead:

```yaml
# Contracts = input validation (before transform)
contracts:
  - type: not_null
    columns: [id]

# Validation = output validation (after transform)
validation:
  tests:
    - type: not_null
      columns: [computed_field]
```

### "Column not found" in contract

**Cause:** Column name doesn't exist in input DataFrame.

**Fix:** Verify column names match exactly (case-sensitive):
```bash
# Debug by adding a dry-run first
odibi run config.yaml --dry-run
```

## See Also

- [Validation Overview](README.md) - The 4-layer validation model
- [Validation Tests](tests.md) - Post-transform row-level checks
- [Quality Gates](../features/quality_gates.md) - Batch-level thresholds
- [Quarantine](../features/quarantine.md) - Route bad rows for review
- [YAML Reference](../reference/yaml_schema.md#contracts-data-quality-gates) - Full contract schema
