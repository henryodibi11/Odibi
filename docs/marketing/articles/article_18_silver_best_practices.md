# Silver Layer Best Practices: Centralize, Validate, Deduplicate

*Building the single source of truth*

---

## TL;DR

Silver is where data becomes trustworthy. Three principles guide Silver layer design: centralize (one authoritative version per entity), validate (stop bad data here), and deduplicate (resolve duplicates before downstream processing). This article covers each principle with concrete patterns and configurations.

---

## Silver's Role

Silver sits between raw and refined:

```
Bronze (Raw)          Silver (Clean)         Gold (Modeled)
├── orders_raw   -->  ├── orders        -->  ├── dim_customer
├── customers_raw -->  ├── customers    -->  ├── fact_orders
├── products_raw  -->  ├── products     -->  └── agg_daily_sales
```

Silver's responsibilities:
1. **Clean** - Standardize formats, fix data quality issues
2. **Validate** - Enforce business rules, quarantine violations
3. **Deduplicate** - One record per entity
4. **Centralize** - Single source for all downstream consumers

---

## Principle 1: Centralize

### The Problem: Data Silos

Marketing team creates their own customer table:
```
gold/marketing/customers_marketing
```

Finance creates their own:
```
gold/finance/customers_finance
```

Now you have:
- Different customer counts
- Different attribute values
- Conflicting reports
- No source of truth

### The Solution: Silver as SSOT

All teams consume from one Silver layer:

```
silver/customers  <--  Marketing queries
                  <--  Finance queries
                  <--  Operations queries
```

### Implementation

```yaml
# Central customer table in Silver
- name: silver_customers
  description: "Single Source of Truth for customer data"
  
  # Pull from Bronze
  read:
    connection: bronze
    path: customers
  
  # Clean and validate
  contracts:
    - type: not_null
      column: customer_id
      severity: error
    - type: unique
      columns: [customer_id]
      severity: error
  
  transformer: deduplicate
  params:
    keys: [customer_id]
    order_by: updated_at DESC
  
  transform:
    steps:
      - function: clean_text
        params:
          columns: [customer_name, customer_city]
          case: upper
          trim: true
  
  write:
    connection: silver
    path: customers
    format: delta
```

### Governance

Document who owns Silver tables:

```yaml
- name: silver_customers
  metadata:
    owner: data-team@company.com
    domain: customer
    sla: "Updated daily by 8am"
    consumers:
      - marketing
      - finance
      - operations
```

---

## Principle 2: Validate

### When to Validate

Silver is the validation layer. Bronze preserves; Silver protects.

| Layer | Validation Level |
|-------|-----------------|
| Bronze | Minimal (just verify data exists) |
| **Silver** | Heavy (enforce all business rules) |
| Gold | Light (mostly referential integrity) |

### Validation Categories

**Structural**: Does the data have the right shape?
- Required columns exist
- Primary keys are unique
- Data types are correct

**Domain**: Does the data make business sense?
- Values are in expected ranges
- Categories are valid
- Dates are reasonable

**Freshness**: Is the data current?
- Not stale
- Recent timestamp

### Configuration Pattern

```yaml
- name: silver_orders
  contracts:
    # Structural
    - type: not_null
      column: order_id
      severity: error
    
    - type: unique
      columns: [order_id]
      severity: error
    
    # Domain
    - type: accepted_values
      column: order_status
      values: [created, approved, shipped, delivered, canceled]
      severity: quarantine
    
    - type: range
      column: order_total
      min: 0
      max: 100000
      severity: warn
    
    - type: custom_sql
      name: "order_date_not_future"
      sql: "SELECT COUNT(*) = 0 FROM df WHERE order_date > CURRENT_DATE()"
      severity: error
    
    # Freshness
    - type: freshness
      column: order_timestamp
      max_age: "48 hours"
      severity: warn
```

### Quarantine Strategy

For domain violations, quarantine instead of fail:

```yaml
validation:
  quarantine:
    connection: silver
    path: orders_quarantine
    add_columns:
      _rejection_reason: true
      _rejected_at: true
    retention_days: 90
```

This keeps the pipeline running while tracking issues.

---

## Principle 3: Deduplicate

### Why Duplicates Happen

Source systems send duplicates for many reasons:
- Retry logic creates duplicate events
- Batch files overlap
- Source bugs
- Late-arriving data restatement

### The Deduplication Pattern

```yaml
transformer: deduplicate
params:
  keys: [order_id]           # Natural key for matching
  order_by: updated_at DESC  # Keep the latest version
```

### How It Works

```sql
SELECT * FROM (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id 
      ORDER BY updated_at DESC
    ) as rn
  FROM source_data
)
WHERE rn = 1
```

### Deduplication Strategies

| Strategy | When to Use |
|----------|-------------|
| Keep latest (`order_by: updated_at DESC`) | Source has update timestamp |
| Keep first (`order_by: created_at ASC`) | First occurrence is correct |
| Keep richest (`order_by: field_count DESC`) | Row with most data is best |
| Merge fields | Different rows have different fields |

### Merge Deduplication

When duplicates have complementary data:

```yaml
# Row 1: {id: 1, name: "John", email: null}
# Row 2: {id: 1, name: null, email: "john@email.com"}
# Desired: {id: 1, name: "John", email: "john@email.com"}

transform:
  steps:
    - sql: |
        SELECT 
          id,
          FIRST_VALUE(name IGNORE NULLS) OVER (
            PARTITION BY id ORDER BY updated_at DESC
          ) as name,
          FIRST_VALUE(email IGNORE NULLS) OVER (
            PARTITION BY id ORDER BY updated_at DESC
          ) as email
        FROM source_data
```

### Deduplication Order

Deduplicate **before** other transformations:

```yaml
- name: silver_orders
  # 1. First deduplicate
  transformer: deduplicate
  params:
    keys: [order_id]
    order_by: updated_at DESC
  
  # 2. Then transform clean data
  transform:
    steps:
      - function: clean_text
        params:
          columns: [status]
          case: lower
```

This prevents wasted computation on duplicate rows.

---

## Complete Silver Pattern

Here's a comprehensive Silver layer configuration:

```yaml
- name: silver_orders
  description: "Clean, validated, deduplicated orders"
  
  # Source
  read:
    connection: bronze
    path: orders
    format: delta
  
  # Pre-validation contracts
  contracts:
    # Structural (fail on violation)
    - type: row_count
      min: 1
      severity: error
    
    - type: not_null
      column: order_id
      severity: error
    
    - type: unique
      columns: [order_id]
      severity: error
    
    # Domain (quarantine violations)
    - type: not_null
      column: customer_id
      severity: quarantine
    
    - type: accepted_values
      column: order_status
      values: [created, approved, shipped, delivered, canceled]
      severity: quarantine
    
    - type: range
      column: order_total
      min: 0
      severity: quarantine
    
    # Reasonableness (warn only)
    - type: range
      column: order_total
      max: 50000
      severity: warn
    
    - type: freshness
      column: order_timestamp
      max_age: "24 hours"
      severity: warn
  
  # Deduplicate first
  transformer: deduplicate
  params:
    keys: [order_id]
    order_by: _extracted_at DESC
  
  # Then clean
  transform:
    steps:
      # Standardize text
      - function: clean_text
        params:
          columns: [order_status]
          case: lower
          trim: true
      
      # Parse dates
      - function: derive_columns
        params:
          columns:
            order_date: "TO_DATE(order_timestamp)"
            order_year: "YEAR(order_date)"
            order_month: "MONTH(order_date)"
      
      # Calculate derived fields
      - function: derive_columns
        params:
          columns:
            days_to_delivery: "DATEDIFF(delivered_at, order_timestamp)"
            is_late: "CASE WHEN delivered_at > estimated_delivery THEN 1 ELSE 0 END"
      
      # Select final columns
      - sql: |
          SELECT
            order_id,
            customer_id,
            order_status,
            order_date,
            order_year,
            order_month,
            order_total,
            days_to_delivery,
            is_late,
            _extracted_at
          FROM df
  
  # Quarantine configuration
  validation:
    quarantine:
      connection: silver
      path: orders_quarantine
      add_columns:
        _rejection_reason: true
        _rejected_at: true
        _batch_id: true
      retention_days: 90
  
  # Output
  write:
    connection: silver
    path: orders
    format: delta
    mode: overwrite
    partition_by: [order_year, order_month]
```

---

## Common Silver Mistakes

### Mistake 1: Transforming Too Much

Silver should clean, not model.

❌ Wrong:
```yaml
# Creating dimensions in Silver
transform:
  steps:
    - function: derive_columns
      params:
        columns:
          customer_sk: "HASH(customer_id)"  # Wrong layer!
```

✅ Right: Surrogate keys belong in Gold.

### Mistake 2: Not Deduplicating

❌ Wrong:
```yaml
# Assuming source has no duplicates
- name: silver_orders
  read: ...
  write: ...  # No deduplication!
```

✅ Right: Always deduplicate, even if you think source is clean.

### Mistake 3: Inconsistent Cleaning

❌ Wrong:
```yaml
# Different case rules in different tables
silver_customers:
  clean_text: upper

silver_orders:
  clean_text: lower  # Inconsistent!
```

✅ Right: Establish standards and apply consistently.

---

## Testing Silver Quality

### Row Count Comparison

```sql
SELECT 
  (SELECT COUNT(*) FROM bronze.orders) as bronze_count,
  (SELECT COUNT(*) FROM silver.orders) as silver_count,
  (SELECT COUNT(*) FROM silver.orders_quarantine) as quarantine_count,
  bronze_count - silver_count - quarantine_count as missing
```

`missing` should be 0.

### Uniqueness Check

```sql
SELECT order_id, COUNT(*) as cnt
FROM silver.orders
GROUP BY order_id
HAVING COUNT(*) > 1
```

Should return 0 rows.

### Referential Integrity Preview

```sql
-- Check if customer_ids in orders exist in customers
SELECT COUNT(*)
FROM silver.orders o
WHERE o.customer_id NOT IN (SELECT customer_id FROM silver.customers)
```

Prepare for Gold layer requirements.

---

## Key Takeaways

| Principle | Implementation |
|-----------|----------------|
| **Centralize** | One Silver table per entity, all teams consume from it |
| **Validate** | Heavy contracts in Silver, quarantine violations |
| **Deduplicate** | Dedupe before transforms, use clear ordering |

### The Silver Mantra

**Silver is the single source of truth. Make it clean, make it trusted, make it central.**

---

## Next Steps

With Silver solid, let's examine a specific anti-pattern in detail:

- SCD2 misuse
- History explosion
- When NOT to use SCD2

Next article: **SCD2 Done Wrong: History Explosion and How to Prevent It**.

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]
- **Medium:** Follow for more articles in this series
