# Data Quality Contracts: Catching Problems Before Production

*Stop bad data before it ruins your dashboards*

---

## TL;DR

Data contracts are pre-conditions that run before your pipeline loads data. If they fail, the pipeline stops-or routes bad records to quarantine. This article shows how to configure contracts for null checks, value validation, row counts, uniqueness, and custom SQL. We'll apply them to our Silver layer and set up quarantine for failed records.

---

## The Problem With Late Discovery

You load data at 6 AM. The dashboard updates. At 9 AM, the CFO notices revenue is $0.

What happened? A null crept into `order_status` and filtered out every order. You didn't find out until someone looked at the dashboard.

This is the late discovery problem. By the time you catch bad data, it's already in production. The fix requires:

1. Finding the bad records
2. Understanding how they got in
3. Reloading corrected data
4. Rebuilding downstream tables
5. Apologizing to stakeholders

There's a better way: stop bad data at the gate.

---

## Contracts vs Validation

Before we dive in, let's clarify two related concepts:

| Concept | When It Runs | What It Does |
|---------|--------------|--------------|
| **Contracts** | Before transformation | Pre-conditions on input data |
| **Validation** | After transformation | Post-conditions on output data |

Think of it this way:
- **Contracts** = "I won't process garbage"
- **Validation** = "I didn't produce garbage"

You need both. This article focuses on contracts.

---

## Types of Contracts

Odibi supports several contract types out of the box:

### 1. Not Null

The most common contract. Ensures critical columns have values.

```yaml
contracts:
  - type: not_null
    column: customer_id
    severity: error
```

### 2. Accepted Values

Ensures a column contains only expected values.

```yaml
contracts:
  - type: accepted_values
    column: order_status
    values:
      - created
      - approved
      - shipped
      - delivered
      - canceled
    severity: error
```

### 3. Uniqueness

Ensures no duplicate values in a column or combination of columns.

```yaml
contracts:
  - type: unique
    columns:
      - order_id
    severity: error
```

### 4. Row Count

Ensures the source has enough rows. Catches truncated files.

```yaml
contracts:
  - type: row_count
    min: 1000
    severity: error
```

Or detect suspiciously small files:

```yaml
contracts:
  - type: row_count
    min: 1000
    max: 10000000  # Suspiciously large
    severity: warn
```

### 5. Range

Ensures numeric values fall within bounds.

```yaml
contracts:
  - type: range
    column: price
    min: 0
    max: 100000
    severity: error
```

### 6. Regex Match

Validates string patterns.

```yaml
contracts:
  - type: regex_match
    column: email
    pattern: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
    severity: warn
```

### 7. Custom SQL

For complex business rules.

```yaml
contracts:
  - type: custom_sql
    name: "order_items_have_orders"
    sql: |
      SELECT COUNT(*) = 0 
      FROM df oi
      LEFT JOIN bronze_orders o ON oi.order_id = o.order_id
      WHERE o.order_id IS NULL
    severity: error
```

---

## Severity Levels

Each contract has a severity:

| Severity | Behavior |
|----------|----------|
| `error` | Pipeline fails immediately |
| `warn` | Log warning, continue processing |

Use `error` for critical data issues that would break downstream systems.

Use `warn` for issues you want to monitor but not stop the pipeline.

```yaml
contracts:
  # Critical: can't have orders without IDs
  - type: not_null
    column: order_id
    severity: error
  
  # Nice to have: reviews can be empty
  - type: not_null
    column: review_comment
    severity: warn
```

---

## Full Example: Silver Layer with Contracts

Let's apply contracts to our Brazilian E-Commerce Silver layer:

```yaml
pipelines:
  - pipeline: silver_ecommerce
    layer: silver
    description: "Cleaned and validated e-commerce data"
    
    nodes:
      - name: silver_orders
        description: "Validated order data"
        
        read:
          connection: bronze
          path: orders
          format: delta
        
        # Pre-conditions: validate BEFORE processing
        contracts:
          # Critical: every order needs an ID
          - type: not_null
            column: order_id
            severity: error
          
          # Critical: must have a customer
          - type: not_null
            column: customer_id
            severity: error
          
          # Critical: order IDs should be unique
          - type: unique
            columns: [order_id]
            severity: error
          
          # Business rule: only valid statuses
          - type: accepted_values
            column: order_status
            values:
              - created
              - approved
              - invoiced
              - processing
              - shipped
              - delivered
              - canceled
              - unavailable
            severity: error
          
          # Sanity check: should have data
          - type: row_count
            min: 1000
            severity: warn
        
        # Now transform (runs only if contracts pass)
        transform:
          steps:
            - function: clean_text
              params:
                columns: [order_status]
                case: lower
                trim: true
            
            - function: derive_columns
              params:
                columns:
                  order_purchase_date: "TO_DATE(order_purchase_timestamp)"
                  order_delivered_date: "TO_DATE(order_delivered_customer_date)"
        
        write:
          connection: silver
          path: orders
          format: delta
          mode: overwrite
```

---

## What Happens When Contracts Fail

When a contract with `severity: error` fails:

```
[ERROR] Contract failed: not_null on customer_id
[ERROR]   Found 23 null values
[ERROR]   Sample failing rows:
[ERROR]     Row 4521: order_id=abc123, customer_id=NULL
[ERROR]     Row 8932: order_id=def456, customer_id=NULL
[ERROR] Pipeline stopped. No data written.
```

The pipeline halts. No data is written. You can investigate before bad data spreads.

When `severity: warn`:

```
[WARN] Contract warning: row_count
[WARN]   Expected min 1000, got 847
[WARN] Continuing with pipeline...
```

The pipeline continues, but you're notified.

---

## Quarantine Pattern

Sometimes you don't want to fail the entire pipeline. You want to route bad records somewhere else and process the good ones.

That's the quarantine pattern.

```yaml
- name: silver_orders
  description: "Validated orders with quarantine"
  
  read:
    connection: bronze
    path: orders
    format: delta
  
  contracts:
    - type: not_null
      column: order_id
      severity: error  # Still fail on null IDs
    
    - type: not_null
      column: customer_id
      severity: quarantine  # Route to quarantine
    
    - type: accepted_values
      column: order_status
      values: [created, approved, shipped, delivered, canceled]
      severity: quarantine  # Route to quarantine
  
  # Where to send failed records
  validation:
    quarantine:
      connection: silver
      path: orders_quarantine
      add_columns:
        _rejection_reason: true
        _rejected_at: true
  
  write:
    connection: silver
    path: orders
    format: delta
```

With this config:
- Null `order_id` → Pipeline fails (critical)
- Null `customer_id` → Record goes to quarantine
- Invalid `order_status` → Record goes to quarantine
- Good records → Written to Silver

The quarantine table includes:
- All original columns
- `_rejection_reason`: Which contract failed
- `_rejected_at`: When it was quarantined

---

## Quarantine Review Process

Don't just quarantine and forget. Set up a process:

1. **Daily review**: Check quarantine tables for patterns
2. **Fix upstream**: Work with source systems to fix root causes
3. **Reprocess**: Once fixed, reload from Bronze
4. **Track metrics**: Count quarantine rate over time

```sql
-- Daily quarantine report
SELECT 
  DATE(_rejected_at) as rejection_date,
  _rejection_reason,
  COUNT(*) as rejected_count
FROM silver.orders_quarantine
WHERE _rejected_at >= CURRENT_DATE - 7
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC
```

If quarantine rates spike, something changed upstream.

---

## Contracts for Each Table

Here's a contract checklist for the e-commerce dataset:

### Orders

```yaml
contracts:
  - type: not_null
    column: order_id
  - type: not_null
    column: customer_id
  - type: unique
    columns: [order_id]
  - type: accepted_values
    column: order_status
    values: [created, approved, invoiced, processing, shipped, delivered, canceled, unavailable]
```

### Order Items

```yaml
contracts:
  - type: not_null
    column: order_id
  - type: not_null
    column: product_id
  - type: not_null
    column: seller_id
  - type: range
    column: price
    min: 0
  - type: range
    column: freight_value
    min: 0
```

### Customers

```yaml
contracts:
  - type: not_null
    column: customer_id
  - type: unique
    columns: [customer_id]
  - type: not_null
    column: customer_zip_code_prefix
```

### Products

```yaml
contracts:
  - type: not_null
    column: product_id
  - type: unique
    columns: [product_id]
  - type: range
    column: product_weight_g
    min: 0
    max: 100000  # 100kg max seems reasonable
```

### Payments

```yaml
contracts:
  - type: not_null
    column: order_id
  - type: accepted_values
    column: payment_type
    values: [credit_card, boleto, voucher, debit_card, not_defined]
  - type: range
    column: payment_value
    min: 0
```

---

## Best Practices

### 1. Start Strict, Loosen as Needed

Begin with `severity: error` on everything. When you find legitimate edge cases, loosen specific contracts.

### 2. Document Why

Add descriptions to contracts:

```yaml
contracts:
  - type: not_null
    column: customer_id
    description: "Orders must have customers for dimension lookups"
    severity: error
```

### 3. Layer Your Checks

- **Bronze**: Minimal (just metadata)
- **Silver**: Heavy contracts (this is your validation layer)
- **Gold**: Business rule validation

### 4. Track Contract Failures

Log contract failures to a metrics table:

```yaml
system:
  connection: bronze
  path: _system
  
  # Automatically logs contract results
  metrics:
    enabled: true
```

### 5. Test Your Contracts

Create test files with intentional bad data:

```python
# test_contracts.py
def test_null_customer_rejects():
    """Null customer_id should fail contract"""
    bad_data = pd.DataFrame({
        'order_id': ['123'],
        'customer_id': [None]
    })
    
    result = run_node('silver_orders', input_df=bad_data)
    assert result.status == 'failed'
    assert 'not_null' in result.failure_reason
```

---

## Common Mistakes

### 1. Too Many Warnings

If you have 50 warnings, you'll ignore them all. Keep warnings rare and actionable.

### 2. Contracts Without Context

"23 rows failed" tells you nothing. Include sample data in failure messages.

### 3. Quarantine Without Cleanup

Quarantine tables grow forever. Set up retention:

```yaml
quarantine:
  connection: silver
  path: orders_quarantine
  retention_days: 90
```

### 4. Validating in Bronze

Bronze should be untouched. Move all validation to Silver.

---

## What We Learned

1. **Contracts catch problems early** - Before bad data spreads
2. **Severity levels give control** - Error stops, warn notifies
3. **Quarantine keeps pipelines running** - Process good data, investigate bad
4. **Each table needs its own contracts** - Based on business rules
5. **Track and trend failures** - Spikes indicate upstream changes

---

## Next Steps

With contracts protecting our Silver layer, we're ready to:

1. Build the complete Silver layer configuration
2. Add complex transformations
3. Deduplicate and clean records

That's the next article.

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]
- **LinkedIn:** Follow for more articles in this series
