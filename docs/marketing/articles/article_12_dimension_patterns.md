# Dimension Table Patterns: Unknown Members, Keys, and Auditing

*Building dimensions that handle real-world messiness*

---

## TL;DR

Dimension tables need more than just surrogate keys. They need unknown member rows for orphan handling, proper key management for consistency, and audit columns for debugging. This article covers the essential patterns: unknown members, natural vs surrogate keys, conformed dimensions, role-playing dimensions, and comprehensive auditing.

---

## The Unknown Member Pattern

### The Problem

Your fact table has `customer_id = CUST-999`. But `dim_customer` has no record for `CUST-999`.

What happens?

| Strategy | Result | Problem |
|----------|--------|---------|
| NULL foreign key | `customer_sk = NULL` | Joins fail, counts wrong |
| Inner join in ETL | Row dropped | Data loss |
| Fail pipeline | Nothing loads | Downstream blocked |

### The Solution: Unknown Member

Add a row with `surrogate_key = 0`:

| customer_sk | customer_id | customer_name | customer_city |
|-------------|-------------|---------------|---------------|
| 0 | UNKNOWN | Unknown Customer | Unknown |
| 1 | CUST-001 | John Smith | São Paulo |
| 2 | CUST-002 | Maria Silva | Rio de Janeiro |

Now orphan facts map to `customer_sk = 0`:

```sql
SELECT 
  d.customer_name,
  SUM(f.amount)
FROM fact_orders f
JOIN dim_customer d ON f.customer_sk = d.customer_sk
GROUP BY d.customer_name
```

Results include "Unknown Customer" for orphan orders. No data loss, no NULL issues.

### Configuration

```yaml
pattern:
  type: dimension
  params:
    natural_key: customer_id
    surrogate_key: customer_sk
    unknown_member: true  # Adds SK=0 row
```

### Customizing Unknown Member

Default unknown member has generic values. Customize with explicit attributes:

```yaml
pattern:
  type: dimension
  params:
    natural_key: customer_id
    surrogate_key: customer_sk
    unknown_member:
      customer_id: "-1"
      customer_name: "Unknown Customer"
      customer_city: "N/A"
      customer_state: "N/A"
      customer_region: "Unassigned"
      is_active: false
```

---

## Key Management

### Natural Keys vs Surrogate Keys

| Concept | Definition | Example |
|---------|------------|---------|
| **Natural Key** | Business identifier from source | `customer_id = "CUST-12345"` |
| **Surrogate Key** | Warehouse-generated integer | `customer_sk = 42` |

### Why Use Both?

**Natural Key** (`customer_id`):
- Match incoming records to existing dimension rows
- Lookup for humans ("find customer CUST-12345")
- Cross-reference with source systems

**Surrogate Key** (`customer_sk`):
- Stable join key (natural keys can change format)
- Enable SCD2 history (same natural key, multiple rows)
- Faster integer joins
- Multi-source integration (two systems, same natural key format)

### Key Generation Strategies

Odibi generates surrogate keys using `MAX(existing) + ROW_NUMBER`:

```sql
-- For new records:
SELECT 
  (SELECT COALESCE(MAX(customer_sk), 0) FROM dim_customer) + ROW_NUMBER() as customer_sk,
  customer_id,
  ...
FROM incoming_data
WHERE customer_id NOT IN (SELECT customer_id FROM dim_customer)
```

Configuration:

```yaml
pattern:
  type: dimension
  params:
    natural_key: customer_id
    surrogate_key: customer_sk
    # SK starts at 1 (0 reserved for unknown member)
```

### Composite Natural Keys

Some dimensions have compound business keys:

```yaml
# Product in category hierarchy
pattern:
  type: dimension
  params:
    natural_key: [category_id, product_id]  # Compound key
    surrogate_key: product_sk
```

This generates one SK per unique combination.

---

## Conformed Dimensions

### The Problem

You have three fact tables:
- `fact_sales` with `customer_sk`
- `fact_returns` with `customer_sk`
- `fact_support_tickets` with `customer_key`  ← Different name!

And two customer dimensions:
- Sales team uses `dim_customer_sales`
- Support team uses `dim_customer_support`

Result: Can't join sales to support tickets. Can't get "360 view" of customer.

### The Solution: Conformed Dimensions

One dimension serves all facts:

```
              ┌──────────────────┐
              │   dim_customer   │
              │  (single source) │
              └────────┬─────────┘
                       │
         ┌─────────────┼─────────────┐
         ▼             ▼             ▼
   fact_sales    fact_returns   fact_support
```

### Implementation

1. **Build once**: Create `dim_customer` in Gold layer
2. **Reference everywhere**: All facts use the same dimension
3. **Consistent naming**: `customer_sk` in all facts

```yaml
# Gold dimension (single source of truth)
- name: dim_customer
  pattern:
    type: dimension
    params:
      natural_key: customer_id
      surrogate_key: customer_sk
  write:
    connection: gold
    path: dim_customer

# All facts reference the same dimension
- name: fact_sales
  pattern:
    type: fact
    params:
      dimensions:
        - dimension_table: dim_customer
          surrogate_key: customer_sk

- name: fact_returns
  pattern:
    type: fact
    params:
      dimensions:
        - dimension_table: dim_customer
          surrogate_key: customer_sk
```

---

## Role-Playing Dimensions

### The Problem

An order has three dates:
- `order_date` - when placed
- `ship_date` - when shipped
- `delivery_date` - when delivered

One `dim_date`, three foreign keys in the fact:

```sql
SELECT 
  -- Which dim_date row?
  d.month_name
FROM fact_orders f
JOIN dim_date d ON f.??? = d.date_sk  -- order_date? ship_date? delivery_date?
```

### The Solution: Role-Playing

The same dimension "plays different roles":

```yaml
- name: fact_orders
  pattern:
    type: fact
    params:
      dimensions:
        # Same dimension, different roles
        - source_column: order_date
          dimension_table: dim_date
          surrogate_key: date_sk
          target_column: order_date_sk  # Role: order date
        
        - source_column: ship_date
          dimension_table: dim_date
          surrogate_key: date_sk
          target_column: ship_date_sk   # Role: ship date
        
        - source_column: delivery_date
          dimension_table: dim_date
          surrogate_key: date_sk
          target_column: delivery_date_sk  # Role: delivery date
```

Result in fact table:

| order_id | order_date_sk | ship_date_sk | delivery_date_sk |
|----------|---------------|--------------|------------------|
| ORD-001 | 20180315 | 20180317 | 20180322 |

Query with explicit role:

```sql
SELECT 
  d_order.month_name as order_month,
  d_ship.month_name as ship_month,
  COUNT(*) as order_count
FROM fact_orders f
JOIN dim_date d_order ON f.order_date_sk = d_order.date_sk
JOIN dim_date d_ship ON f.ship_date_sk = d_ship.date_sk
GROUP BY d_order.month_name, d_ship.month_name
```

---

## Audit Columns

### Why Audit?

When something goes wrong:
- "When was this record loaded?"
- "Where did this data come from?"
- "Why does this row exist?"

Audit columns answer these questions.

### Standard Audit Columns

| Column | Type | Purpose |
|--------|------|---------|
| `load_timestamp` | TIMESTAMP | When record was loaded |
| `source_system` | STRING | Which system provided data |
| `source_file` | STRING | Which file (if file-based) |
| `batch_id` | STRING | Which pipeline run |
| `record_hash` | STRING | Hash for change detection |

### Configuration

```yaml
pattern:
  type: dimension
  params:
    natural_key: customer_id
    surrogate_key: customer_sk
    audit:
      load_timestamp: true
      source_system: "crm"
      batch_id: true
```

Generated columns:

```
customer_sk       INT
customer_id       STRING
customer_name     STRING
...
load_timestamp    TIMESTAMP    (when loaded)
source_system     STRING       ('crm')
batch_id          STRING       (pipeline run ID)
```

### Using Audit Columns

```sql
-- Find records loaded today
SELECT * FROM dim_customer 
WHERE DATE(load_timestamp) = CURRENT_DATE

-- Find records from specific batch
SELECT * FROM dim_customer 
WHERE batch_id = 'run_20231215_093045'

-- Count records by source
SELECT source_system, COUNT(*) 
FROM dim_customer 
GROUP BY source_system
```

---

## Data Dictionary / Column Metadata

Document your dimensions with column metadata:

```yaml
- name: dim_customer
  columns:
    customer_sk:
      description: "Surrogate key for customer dimension"
      tags: [surrogate_key]
    
    customer_id:
      description: "Natural key from CRM system"
      tags: [natural_key, business_key]
    
    customer_email:
      description: "Customer email address"
      pii: true  # Marks as sensitive
    
    customer_segment:
      description: "Customer value segment (Premium, Standard, Basic)"
      tags: [business_attribute]
```

This metadata:
- Documents the schema
- Flags PII for compliance
- Enables automated data catalogs
- Helps new team members understand the model

---

## Junk Dimensions

### The Problem

Fact table has many low-cardinality flags:

```
is_online_order: true/false
is_gift: true/false
is_rush: true/false
payment_status: pending/approved/declined
```

Each flag could be a dimension, but that's excessive.

### The Solution: Junk Dimension

Combine flags into one dimension:

| junk_sk | is_online | is_gift | is_rush | payment_status |
|---------|-----------|---------|---------|----------------|
| 1 | true | false | false | approved |
| 2 | true | true | false | approved |
| 3 | false | false | true | pending |

Fact table stores single `junk_sk` instead of multiple columns.

```yaml
- name: dim_order_attributes
  description: "Junk dimension for order flags"
  
  # Generate all combinations
  transform:
    steps:
      - sql: |
          SELECT DISTINCT
            is_online_order,
            is_gift,
            is_rush_delivery,
            payment_status
          FROM silver.orders
  
  pattern:
    type: dimension
    params:
      natural_key: [is_online_order, is_gift, is_rush_delivery, payment_status]
      surrogate_key: order_attribute_sk
      unknown_member: true
```

---

## Mini-Dimensions

### The Problem

Customer dimension has 10 million rows. A subset of columns changes frequently:

- `customer_segment` - changes quarterly
- `loyalty_tier` - changes monthly
- `credit_score_band` - changes frequently

SCD2 on 10M rows with frequent changes = explosion.

### The Solution: Mini-Dimension

Extract volatile attributes into separate small dimension:

**Main dimension** (SCD1 or SCD2 on stable attributes):
| customer_sk | customer_id | name | address | ... |

**Mini-dimension** (SCD2 on volatile attributes):
| customer_demo_sk | segment | loyalty_tier | credit_band |

Fact table has both keys:
| order_id | customer_sk | customer_demo_sk | ... |

```yaml
# Main customer dimension (stable)
- name: dim_customer
  pattern:
    type: dimension
    params:
      scd_type: 1
      track_cols: [customer_name, customer_address]

# Mini-dimension (volatile)
- name: dim_customer_demographics
  pattern:
    type: dimension
    params:
      scd_type: 2
      track_cols: [customer_segment, loyalty_tier, credit_band]
```

---

## Complete Example

Here's a comprehensive dimension configuration:

```yaml
- name: dim_customer
  description: "Customer dimension with full pattern implementation"
  
  read:
    connection: silver
    path: customers
    format: delta
  
  # Pre-processing
  transformer: deduplicate
  params:
    keys: [customer_id]
    order_by: updated_at DESC
  
  transform:
    steps:
      # Standardize
      - function: clean_text
        params:
          columns: [customer_name, customer_city, customer_state]
          case: upper
          trim: true
      
      # Fill NULLs
      - function: fill_nulls
        params:
          columns:
            customer_segment: "Unknown"
            customer_tier: "Standard"
      
      # Derive attributes
      - function: derive_columns
        params:
          columns:
            customer_region: |
              CASE 
                WHEN customer_state IN ('SP', 'RJ') THEN 'Southeast'
                ELSE 'Other'
              END
            customer_name_length: "LENGTH(customer_name)"
  
  # Column documentation
  columns:
    customer_sk:
      description: "Surrogate key"
      tags: [surrogate_key]
    customer_id:
      description: "Natural key from source"
      tags: [natural_key]
    customer_email:
      description: "Email address"
      pii: true
    customer_segment:
      description: "Value segment"
      tags: [business_attribute]
  
  # Dimension pattern
  pattern:
    type: dimension
    params:
      natural_key: customer_id
      surrogate_key: customer_sk
      scd_type: 2
      track_cols:
        - customer_city
        - customer_state
        - customer_segment
      target: gold.dim_customer
      unknown_member:
        customer_id: "-1"
        customer_name: "Unknown"
        customer_city: "N/A"
        customer_state: "N/A"
        customer_region: "Unknown"
        customer_segment: "Unknown"
      audit:
        load_timestamp: true
        source_system: "crm"
  
  write:
    connection: gold
    path: dim_customer
    format: delta
```

---

## Key Takeaways

1. **Unknown members prevent join failures** - Always add SK=0 row
2. **Natural keys for matching, surrogate keys for joining** - Use both
3. **Conform dimensions across facts** - One dimension, multiple consumers
4. **Role-playing enables multiple relationships** - Same dimension, different contexts
5. **Audit columns enable debugging** - load_timestamp, source_system, batch_id
6. **Document with column metadata** - PII flags, descriptions, tags

---

## Next Steps

With dimension patterns solid, we'll look at fact table design in more depth:

- Grain decisions
- Measure types (additive, semi-additive, non-additive)
- Factless facts

Next article: **Fact Table Design: Grain, Measures, and Validation**.

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]
- **Medium:** Follow for more articles in this series
