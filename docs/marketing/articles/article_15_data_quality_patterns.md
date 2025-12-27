# Data Quality Patterns: Contracts, Validation, and Quarantine

*Building trust in your data pipeline*

---

## TL;DR

Data quality isn't one thing-it's a system of checks at every layer. Contracts validate input before processing. Validation checks output after transformation. Quarantine routes bad data for review without stopping the pipeline. This article covers the complete data quality framework: what to check at each layer, severity strategies, quarantine management, and quality metrics.

---

## The Data Quality Framework

Data quality has three components:

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA QUALITY                             │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  CONTRACTS          VALIDATION         QUARANTINE           │
│  (Pre-conditions)   (Post-conditions)  (Bad data routing)  │
│                                                              │
│  "Is input valid?"  "Is output valid?" "What do we do with │
│                                         invalid data?"       │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Contracts

Run **before** transformation on input data:
- "Does the source have data?"
- "Are required columns non-null?"
- "Are values in expected ranges?"

### Validation

Run **after** transformation on output data:
- "Did we produce the right row count?"
- "Are calculated fields reasonable?"
- "Did referential integrity hold?"

### Quarantine

Routes bad records for investigation:
- "What failed the check?"
- "When did it fail?"
- "What was the original data?"

---

## Layer-by-Layer Strategy

Different layers need different checks:

### Bronze Layer

Minimal checks-preserve everything:

```yaml
# Bronze contracts: Just make sure we have data
contracts:
  - type: row_count
    min: 1
    severity: error
    description: "Source file must not be empty"
```

Why minimal?
- Bronze is your safety net
- You can't "fix" data you didn't keep
- Source problems should be visible, not hidden

### Silver Layer

Heavy validation-this is your filter:

```yaml
# Silver: The quality gate
contracts:
  # Structural
  - type: not_null
    column: customer_id
    severity: error
  
  # Domain
  - type: accepted_values
    column: order_status
    values: [created, shipped, delivered, canceled]
    severity: error
  
  # Reasonableness
  - type: range
    column: order_total
    min: 0
    max: 100000
    severity: warn
  
  # Uniqueness
  - type: unique
    columns: [order_id]
    severity: error
```

Why heavy?
- Silver is Single Source of Truth
- Bad data stops here, not in reports
- This is where cleaning happens

### Gold Layer

Business rule validation:

```yaml
# Gold: Business logic checks
contracts:
  # Referential integrity
  - type: custom_sql
    name: "all_orders_have_customers"
    sql: |
      SELECT COUNT(*) = 0
      FROM df f
      WHERE f.customer_sk NOT IN (SELECT customer_sk FROM dim_customer)
    severity: error
  
  # Grain validation
  - type: unique
    columns: [order_id, order_item_id]
    severity: error
```

---

## Contract Types Reference

### Structural Contracts

Check data shape:

| Contract | Purpose | Example |
|----------|---------|---------|
| `not_null` | Column has values | PK columns |
| `unique` | No duplicates | Natural keys |
| `row_count` | Expected volume | Min/max bounds |
| `schema` | Expected columns | Required fields |

```yaml
contracts:
  - type: not_null
    column: order_id
  
  - type: unique
    columns: [order_id]
  
  - type: row_count
    min: 1000
    max: 10000000
  
  - type: schema
    required_columns: [order_id, customer_id, order_date]
```

### Domain Contracts

Check business rules:

| Contract | Purpose | Example |
|----------|---------|---------|
| `accepted_values` | Valid categories | Status codes |
| `range` | Numeric bounds | Prices, quantities |
| `regex_match` | String patterns | Email, phone |
| `custom_sql` | Complex logic | Business rules |

```yaml
contracts:
  - type: accepted_values
    column: payment_type
    values: [credit_card, debit_card, cash, voucher]
  
  - type: range
    column: discount_pct
    min: 0
    max: 100
  
  - type: regex_match
    column: email
    pattern: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
  
  - type: custom_sql
    name: "order_date_not_future"
    sql: "SELECT COUNT(*) = 0 FROM df WHERE order_date > CURRENT_DATE()"
```

### Freshness Contracts

Check data timeliness:

```yaml
contracts:
  - type: freshness
    column: _extracted_at
    max_age: "24 hours"
    severity: warn
```

### Volume Change Contracts

Detect anomalies:

```yaml
contracts:
  - type: volume_drop
    baseline: "7 day average"
    threshold: 0.5  # Alert if < 50% of baseline
    severity: warn
```

---

## Severity Strategies

Choose severity based on impact:

### Error (Pipeline Fails)

Use for:
- Primary key violations
- Null critical fields
- Invalid data that would corrupt downstream

```yaml
- type: not_null
  column: order_id
  severity: error  # Can't proceed without this
```

### Warn (Log and Continue)

Use for:
- Data quality issues to monitor
- Edge cases that don't break processing
- Metrics you want to track

```yaml
- type: range
  column: order_total
  max: 50000
  severity: warn  # Unusual but not impossible
```

### Quarantine (Route Bad Data)

Use for:
- Keep pipeline running
- Isolate problems for investigation
- Process good data, review bad data later

```yaml
- type: not_null
  column: customer_id
  severity: quarantine  # Route to quarantine table
```

---

## Quarantine Pattern

### Configuration

```yaml
- name: silver_orders
  contracts:
    - type: not_null
      column: order_id
      severity: error  # Critical - fail pipeline
    
    - type: not_null
      column: customer_id
      severity: quarantine  # Route to quarantine
    
    - type: accepted_values
      column: order_status
      values: [created, shipped, delivered]
      severity: quarantine  # Route to quarantine
  
  validation:
    quarantine:
      connection: silver
      path: orders_quarantine
      add_columns:
        _rejection_reason: true
        _rejected_at: true
        _source_contract: true
```

### Quarantine Table Schema

| Column | Type | Description |
|--------|------|-------------|
| (all source columns) | varies | Original data |
| `_rejection_reason` | STRING | Which contract failed |
| `_rejected_at` | TIMESTAMP | When it was quarantined |
| `_source_contract` | STRING | Contract name |
| `_batch_id` | STRING | Pipeline run ID |

### Example Quarantine Data

| order_id | customer_id | order_status | _rejection_reason | _rejected_at |
|----------|-------------|--------------|-------------------|--------------|
| ORD-123 | NULL | created | not_null: customer_id | 2023-12-15 09:30:00 |
| ORD-456 | CUST-789 | pending | accepted_values: order_status | 2023-12-15 09:30:00 |

---

## Quarantine Management

### Daily Review Query

```sql
-- Quarantine summary by reason
SELECT 
  DATE(_rejected_at) as rejection_date,
  _rejection_reason,
  COUNT(*) as rejected_count
FROM silver.orders_quarantine
WHERE _rejected_at >= CURRENT_DATE - 7
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC
```

### Trend Analysis

```sql
-- Quarantine rate over time
SELECT 
  DATE(load_timestamp) as load_date,
  COUNT(*) as total_loaded,
  (SELECT COUNT(*) FROM quarantine q 
   WHERE DATE(q._rejected_at) = DATE(m.load_timestamp)) as quarantined,
  ROUND(100.0 * quarantined / total_loaded, 2) as quarantine_rate_pct
FROM silver.orders m
GROUP BY load_date
ORDER BY load_date DESC
```

### Reprocessing Quarantined Data

After fixing upstream issues:

```python
# Read quarantine
quarantined = spark.read.delta("silver/orders_quarantine")

# Filter for specific issue (now fixed)
to_reprocess = quarantined.filter(
    F.col("_rejection_reason") == "not_null: customer_id"
)

# Remove quarantine columns
clean_data = to_reprocess.drop("_rejection_reason", "_rejected_at", "_source_contract")

# Rerun through pipeline
# ...
```

### Retention Policy

Don't keep quarantine data forever:

```yaml
validation:
  quarantine:
    connection: silver
    path: orders_quarantine
    retention_days: 90  # Auto-cleanup after 90 days
```

---

## Quality Metrics

Track data quality over time:

### Core Metrics

| Metric | Formula | Target |
|--------|---------|--------|
| Completeness | (Non-null values / Total values) × 100 | > 99% |
| Uniqueness | (Distinct values / Total values) × 100 | 100% for PKs |
| Validity | (Valid values / Total values) × 100 | > 99% |
| Quarantine Rate | (Quarantined / Total) × 100 | < 1% |

### Metrics Table

```yaml
system:
  connection: bronze
  path: _system
  
  metrics:
    enabled: true
    table: data_quality_metrics
    retention_days: 365
```

Generated metrics:

| metric_date | pipeline | node | metric_name | metric_value |
|-------------|----------|------|-------------|--------------|
| 2023-12-15 | silver | orders | row_count | 99441 |
| 2023-12-15 | silver | orders | null_rate_customer_id | 0.02 |
| 2023-12-15 | silver | orders | quarantine_count | 23 |

### Dashboard Query

```sql
-- Quality dashboard data
SELECT 
  metric_date,
  node,
  SUM(CASE WHEN metric_name = 'row_count' THEN metric_value END) as rows_loaded,
  SUM(CASE WHEN metric_name = 'quarantine_count' THEN metric_value END) as quarantined,
  ROUND(100.0 * quarantine_count / rows_loaded, 2) as quarantine_pct
FROM system.data_quality_metrics
WHERE metric_date >= CURRENT_DATE - 30
GROUP BY metric_date, node
ORDER BY metric_date DESC
```

---

## Testing Contracts

Create test cases for your contracts:

```python
# test_contracts.py
import pytest
from odibi import Pipeline

def test_null_customer_fails():
    """Null customer_id should fail contract"""
    bad_data = pd.DataFrame({
        'order_id': ['ORD-001'],
        'customer_id': [None],
        'order_status': ['created']
    })
    
    result = Pipeline.run_node(
        'silver_orders',
        input_df=bad_data,
        dry_run=True
    )
    
    assert result.contracts_passed == False
    assert 'not_null' in result.failed_contracts[0].type

def test_invalid_status_quarantines():
    """Invalid status should route to quarantine"""
    bad_data = pd.DataFrame({
        'order_id': ['ORD-001'],
        'customer_id': ['CUST-001'],
        'order_status': ['invalid_status']
    })
    
    result = Pipeline.run_node(
        'silver_orders',
        input_df=bad_data
    )
    
    assert result.quarantine_count == 1
    assert result.output_count == 0
```

---

## Complete Example

Here's a comprehensive data quality configuration:

```yaml
- name: silver_orders
  description: "Validated orders with full quality checks"
  
  read:
    connection: bronze
    path: orders
    format: delta
  
  # Pre-processing contracts
  contracts:
    # Structural - fail on violations
    - type: row_count
      min: 1
      severity: error
      description: "Source must have data"
    
    - type: not_null
      column: order_id
      severity: error
      description: "Order ID is required"
    
    - type: unique
      columns: [order_id]
      severity: error
      description: "Order IDs must be unique"
    
    # Domain - quarantine violations
    - type: not_null
      column: customer_id
      severity: quarantine
      description: "Orders need customers"
    
    - type: accepted_values
      column: order_status
      values: [created, approved, shipped, delivered, canceled]
      severity: quarantine
      description: "Status must be valid"
    
    # Reasonableness - warn on violations
    - type: freshness
      column: order_purchase_timestamp
      max_age: "30 days"
      severity: warn
      description: "Data should be recent"
    
    - type: volume_drop
      baseline: "7 day average"
      threshold: 0.5
      severity: warn
      description: "Alert on volume drop"
  
  # Transformations
  transformer: deduplicate
  params:
    keys: [order_id]
    order_by: _extracted_at DESC
  
  transform:
    steps:
      - function: clean_text
        params:
          columns: [order_status]
          case: lower
  
  # Post-processing validation
  validation:
    # Output checks
    checks:
      - type: row_count_comparison
        source: input
        threshold: 0.95  # Output should be >= 95% of input
        severity: warn
    
    # Quarantine configuration
    quarantine:
      connection: silver
      path: orders_quarantine
      add_columns:
        _rejection_reason: true
        _rejected_at: true
        _source_contract: true
        _batch_id: true
      retention_days: 90
  
  write:
    connection: silver
    path: orders
    format: delta
```

---

## Best Practices

### 1. Start Strict, Loosen as Needed

Begin with `error` severity, move to `warn` or `quarantine` for known edge cases.

### 2. Document Every Contract

```yaml
- type: not_null
  column: customer_id
  description: "Orders must have customers for dimension lookups"
  owner: "data-team@company.com"
```

### 3. Alert on Trends, Not Incidents

One bad record isn't news. A 10x spike in quarantine rate is.

### 4. Review Quarantine Weekly

Don't just collect bad data-investigate patterns and fix upstream.

### 5. Track Quality Over Time

Dashboards showing quality trends catch degradation early.

### 6. Test Your Contracts

Write unit tests that verify contracts catch bad data.

---

## Key Takeaways

1. **Contracts validate input** - Before transformation
2. **Validation checks output** - After transformation
3. **Quarantine keeps pipelines running** - Route bad data, process good data
4. **Layer strategy matters** - Light on Bronze, heavy on Silver
5. **Metrics enable improvement** - Track quality over time
6. **Test your quality checks** - Verify they catch real issues

---

## Next Steps

With data quality solid, we'll cover incremental loading:

- Watermarks and high-water marks
- Merge strategies
- Append vs overwrite patterns

Next article: **Incremental Loading: Watermarks, Merge, and Append**.

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]
- **Medium:** Follow for more articles in this series
