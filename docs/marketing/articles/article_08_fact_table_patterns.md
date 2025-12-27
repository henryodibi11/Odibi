# Fact Table Patterns: Lookups, Orphans, and Measures

*Building robust fact tables that handle the real world*

---

## TL;DR

Fact tables need surrogate key lookups from dimensions, strategies for handling orphan records (missing dimension matches), and clear measure definitions. This article covers the complete fact pattern: configuring lookups with SCD2 support, choosing between unknown/reject/quarantine for orphans, defining passthrough and calculated measures, and validating grain. We'll build `fact_order_items` step by step.

---

## Anatomy of a Fact Table

A fact table has three types of columns:

```
┌─────────────────────────────────────────────────────────────┐
│                    FACT_ORDER_ITEMS                          │
├─────────────────────────────────────────────────────────────┤
│ KEYS                                                         │
│   order_id            (degenerate dimension)                 │
│   order_item_id       (degenerate dimension)                 │
│   customer_sk         (FK → dim_customer)                    │
│   product_sk          (FK → dim_product)                     │
│   seller_sk           (FK → dim_seller)                      │
│   order_date_sk       (FK → dim_date)                        │
├─────────────────────────────────────────────────────────────┤
│ MEASURES                                                     │
│   quantity            (additive)                             │
│   price               (additive)                             │
│   freight_value       (additive)                             │
│   line_total          (calculated: price + freight)          │
├─────────────────────────────────────────────────────────────┤
│ AUDIT                                                        │
│   load_timestamp      (when loaded)                          │
│   source_system       (where from)                           │
└─────────────────────────────────────────────────────────────┘
```

Let's break down each part.

---

## Surrogate Key Lookups

The source data has natural keys like `customer_id`. Fact tables need surrogate keys like `customer_sk`.

### The Challenge

```
Source (Silver):          Dimension (Gold):
order_id: ABC123          customer_sk: 42
customer_id: CUST-789 --> customer_id: CUST-789
product_id: PROD-456      
```

We need to look up `customer_sk = 42` using `customer_id = CUST-789`.

### Basic Lookup Configuration

```yaml
pattern:
  type: fact
  params:
    dimensions:
      - source_column: customer_id      # Column in source fact
        dimension_table: dim_customer   # Dimension to look up
        dimension_key: customer_id      # Natural key in dimension
        surrogate_key: customer_sk      # SK to retrieve
```

This generates a LEFT JOIN:

```sql
SELECT 
  f.*,
  d.customer_sk
FROM source_fact f
LEFT JOIN dim_customer d ON f.customer_id = d.customer_id
```

### SCD2 Lookups

If your dimension uses SCD2 (history tracking), you need to join only to current records:

```yaml
dimensions:
  - source_column: customer_id
    dimension_table: dim_customer
    dimension_key: customer_id
    surrogate_key: customer_sk
    scd2: true  # Only join where is_current = true
```

This adds a filter:

```sql
LEFT JOIN dim_customer d 
  ON f.customer_id = d.customer_id 
  AND d.is_current = true
```

### Point-in-Time Lookups

For historical analysis, join based on when the fact occurred:

```yaml
dimensions:
  - source_column: customer_id
    dimension_table: dim_customer
    dimension_key: customer_id
    surrogate_key: customer_sk
    scd2: true
    point_in_time: order_date  # Look up SK valid at order_date
```

This joins to the dimension version that was active at `order_date`:

```sql
LEFT JOIN dim_customer d 
  ON f.customer_id = d.customer_id 
  AND f.order_date >= d.valid_from 
  AND f.order_date < d.valid_to
```

---

## Orphan Handling

What happens when a fact record has a `customer_id` that doesn't exist in `dim_customer`?

This is an **orphan record**. You have three choices:

### Option 1: Unknown (Default to SK=0)

Map orphans to the unknown member row in the dimension:

```yaml
pattern:
  type: fact
  params:
    orphan_handling: unknown
```

Result:
- Orphan records get `customer_sk = 0`
- They join to the "Unknown Customer" row
- Pipeline continues
- No data loss

This is the safest default.

### Option 2: Reject (Fail the Pipeline)

If orphans indicate a serious data quality issue:

```yaml
pattern:
  type: fact
  params:
    orphan_handling: reject
```

Result:
- Pipeline fails immediately
- Error message shows which records failed
- No data written
- Forces upstream fix

Use this when orphans should never happen.

### Option 3: Quarantine (Route to Separate Table)

Process good records, save bad records for later review:

```yaml
pattern:
  type: fact
  params:
    orphan_handling: quarantine
    quarantine:
      connection: silver
      path: fact_order_items_orphans
      add_columns:
        _rejection_reason: true    # Which lookup failed
        _rejected_at: true         # Timestamp
        _source_dimension: true    # Which dimension
```

Result:
- Good records → `fact_order_items`
- Bad records → `fact_order_items_orphans`
- Pipeline succeeds
- Review orphans later

### Choosing Your Strategy

| Scenario | Strategy | Why |
|----------|----------|-----|
| New data sources, still building dimensions | unknown | Keep pipeline running |
| Production, dimensions should be complete | reject | Fail fast on data issues |
| Production, but need resilience | quarantine | Keep pipeline running, track issues |
| Historical loads with known gaps | unknown | Accept that some old data has missing refs |

---

## Defining Measures

Measures are the numbers you aggregate. Three types:

### Passthrough Measures

Copy the column as-is:

```yaml
measures:
  - price
  - freight_value
  - quantity
```

### Calculated Measures

Create new columns with expressions:

```yaml
measures:
  - line_total: "price + freight_value"
  - discount_amount: "list_price - price"
  - unit_price: "price / quantity"
```

### Renamed Measures

Rename for clarity:

```yaml
measures:
  - order_amount: price  # Rename price to order_amount
  - shipping_cost: freight_value
```

### Complex Example

```yaml
measures:
  # Passthrough
  - quantity
  - price
  - freight_value
  
  # Calculated
  - line_total: "price + freight_value"
  - is_free_shipping: "CASE WHEN freight_value = 0 THEN 1 ELSE 0 END"
  - weight_per_item: "product_weight_g / quantity"
  
  # Renamed
  - order_line_count: "1"  # Always 1 for counting lines
```

---

## Grain Validation

The **grain** is the level of detail in your fact table. It defines "one row per ___."

For `fact_order_items`: "One row per order line item" → `[order_id, order_item_id]`

### Why Grain Matters

If you accidentally load duplicates, measures get double-counted. Grain validation catches this.

```yaml
pattern:
  type: fact
  params:
    grain: [order_id, order_item_id]
```

This adds a check:

```sql
SELECT order_id, order_item_id, COUNT(*) as cnt
FROM source_data
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1
```

If duplicates exist:
- With `on_violation: error` → Pipeline fails
- With `on_violation: warn` → Log warning, continue (risky!)

### Common Grain Patterns

| Fact Table | Grain |
|------------|-------|
| fact_orders | [order_id] |
| fact_order_items | [order_id, order_item_id] |
| fact_payments | [order_id, payment_sequential] |
| fact_daily_sales | [date_sk, product_sk, store_sk] |
| fact_inventory | [date_sk, product_sk, warehouse_sk] |

---

## Deduplication

Sometimes source data has duplicates that need removal before processing:

```yaml
pattern:
  type: fact
  params:
    deduplicate: true
    keys: [order_id, order_item_id]
    order_by: _extracted_at DESC  # Keep latest
```

This runs before lookups and measures:

```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY order_id, order_item_id 
    ORDER BY _extracted_at DESC
  ) as rn
  FROM source_data
)
WHERE rn = 1
```

---

## Complete Fact Configuration

Here's the full `fact_order_items` configuration:

```yaml
- name: fact_order_items
  description: "Order line items fact table"
  
  depends_on:
    - silver_order_items
    - silver_orders  # For order date
    - dim_customer
    - dim_product
    - dim_seller
    - dim_date
  
  # Join order items with orders to get dates and customer
  inputs:
    order_items: $silver_ecommerce.silver_order_items
    orders: $silver_ecommerce.silver_orders
  
  transform:
    steps:
      # Join to get order-level data
      - sql: |
          SELECT 
            oi.order_id,
            oi.order_item_id,
            oi.product_id,
            oi.seller_id,
            o.customer_id,
            o.order_purchase_date,
            oi.price,
            oi.freight_value,
            oi.line_total
          FROM order_items oi
          JOIN orders o ON oi.order_id = o.order_id
  
  pattern:
    type: fact
    params:
      grain: [order_id, order_item_id]
      
      dimensions:
        # Customer lookup
        - source_column: customer_id
          dimension_table: dim_customer
          dimension_key: customer_id
          surrogate_key: customer_sk
          scd2: true
        
        # Product lookup
        - source_column: product_id
          dimension_table: dim_product
          dimension_key: product_id
          surrogate_key: product_sk
        
        # Seller lookup
        - source_column: seller_id
          dimension_table: dim_seller
          dimension_key: seller_id
          surrogate_key: seller_sk
        
        # Date lookup
        - source_column: order_purchase_date
          dimension_table: dim_date
          dimension_key: full_date
          surrogate_key: date_sk
          target_column: order_date_sk  # Rename the FK
      
      orphan_handling: unknown
      
      measures:
        - quantity: "1"
        - price
        - freight_value
        - line_total
        - item_count: "1"
      
      audit:
        load_timestamp: true
        source_system: "ecommerce"
  
  write:
    connection: gold
    path: fact_order_items
    format: delta
    mode: overwrite
```

---

## Testing Your Fact Table

After building, run these checks:

### 1. Row Count Validation

```sql
-- Source count should match fact count (after dedup)
SELECT 
  (SELECT COUNT(*) FROM silver.order_items) as source_count,
  (SELECT COUNT(*) FROM gold.fact_order_items) as fact_count
```

### 2. Orphan Check

```sql
-- Check for unknown members (SK=0)
SELECT 
  'customer' as dimension,
  COUNT(*) as unknown_count
FROM gold.fact_order_items
WHERE customer_sk = 0
UNION ALL
SELECT 
  'product',
  COUNT(*)
FROM gold.fact_order_items
WHERE product_sk = 0
```

If unknown counts are high, investigate dimension coverage.

### 3. Measure Validation

```sql
-- Verify totals match source
SELECT 
  SUM(price) as fact_total,
  (SELECT SUM(price) FROM silver.order_items) as source_total
FROM gold.fact_order_items
```

### 4. Grain Validation

```sql
-- Should return 0 rows
SELECT order_id, order_item_id, COUNT(*)
FROM gold.fact_order_items
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1
```

---

## Common Mistakes

### 1. Missing Dimension Dependencies

❌ Wrong:
```yaml
depends_on: [silver_order_items]  # Missing dimensions!
pattern:
  type: fact
  params:
    dimensions:
      - dimension_table: dim_customer  # Will fail!
```

✅ Right:
```yaml
depends_on: 
  - silver_order_items
  - dim_customer
  - dim_product
  - dim_seller
  - dim_date
```

### 2. Forgetting SCD2 Filter

❌ Wrong:
```yaml
dimensions:
  - source_column: customer_id
    dimension_table: dim_customer  # SCD2 dimension
    dimension_key: customer_id
    surrogate_key: customer_sk
    # Missing scd2: true!
```

This joins to ALL versions, creating duplicates.

### 3. Wrong Grain

❌ Wrong:
```yaml
grain: [order_id]  # Should be [order_id, order_item_id]
```

Grain validation will fail or you'll lose line items.

### 4. Non-Additive Measures

❌ Wrong:
```yaml
measures:
  - discount_pct: "(list_price - price) / list_price * 100"
```

You can't SUM percentages. Store components instead:

✅ Right:
```yaml
measures:
  - list_price
  - price
  - discount_amount: "list_price - price"
# Calculate percentage in reporting layer
```

---

## Quarantine Management

If using quarantine, set up a review process:

```sql
-- Daily quarantine summary
SELECT 
  DATE(_rejected_at) as rejection_date,
  _source_dimension,
  _rejection_reason,
  COUNT(*) as orphan_count
FROM silver.fact_order_items_orphans
WHERE _rejected_at >= CURRENT_DATE - 7
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC
```

Common fixes:
- Missing dimension record → Load the dimension first
- Typo in source data → Fix upstream
- Timing issue → Reload after dimension update

---

## Key Takeaways

1. **Surrogate key lookups connect facts to dimensions** - Configure with SCD2 awareness
2. **Orphan handling protects your pipeline** - Choose unknown/reject/quarantine based on needs
3. **Measures must be additive** - Store components, not percentages
4. **Grain validation catches duplicates** - Define it, enforce it
5. **Test your fact tables** - Row counts, orphan counts, measure totals

---

## Next Steps

With all the pieces in place:
- Bronze layer ✓
- Silver layer ✓
- Date dimension ✓
- Fact table pattern ✓

We're ready to put it all together:

Next article: **From CSV to Star Schema: Complete Walkthrough**.

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]
- **Medium:** Follow for more articles in this series
