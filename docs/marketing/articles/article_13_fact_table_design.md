# Fact Table Design: Grain, Measures, and Validation

*The art of choosing the right level of detail*

---

## TL;DR

Fact table design starts with grain-the level of detail each row represents. Get it wrong and everything downstream breaks. This article covers grain decisions, measure types (additive, semi-additive, non-additive), factless facts for tracking events without measures, and validation strategies to ensure data integrity.

---

## Grain: The Foundation

Grain is the answer to: **"What does one row represent?"**

| Fact Table | Grain |
|------------|-------|
| fact_orders | One row per order |
| fact_order_items | One row per order line item |
| fact_daily_inventory | One row per product per day per warehouse |
| fact_page_views | One row per page view event |

### Why Grain Matters

Wrong grain leads to:
- **Double-counting**: If grain is too coarse, you lose detail
- **Explosion**: If grain is too fine, you have billions of rows
- **Confusion**: Users don't know what numbers mean

### Choosing Your Grain

Ask these questions:

1. **What is the lowest level of detail in the source?**
   - If source has line items, that's your natural grain
   - Don't aggregate before the fact table

2. **What questions will users ask?**
   - "Total orders by month" → Order grain works
   - "Average items per order" → Need line item grain
   - "Revenue by product" → Need line item grain

3. **How much data volume can you handle?**
   - 1 billion rows per day? Consider aggregation
   - 10 million per day? Full transaction grain is fine

### Example: Order Grain vs Item Grain

**Order grain** (`fact_orders`):
| order_id | customer_sk | order_date_sk | order_total |
|----------|-------------|---------------|-------------|
| ORD-001 | 42 | 20180315 | 150.00 |

- ✅ Query: "How many orders per month?"
- ❌ Query: "Revenue by product?" (can't break down by item)

**Item grain** (`fact_order_items`):
| order_id | item_id | product_sk | price |
|----------|---------|------------|-------|
| ORD-001 | 1 | 101 | 75.00 |
| ORD-001 | 2 | 102 | 75.00 |

- ✅ Query: "Revenue by product?"
- ✅ Query: "How many orders per month?" (COUNT DISTINCT order_id)

**Rule**: When in doubt, go with the finer grain. You can always aggregate up.

---

## Grain Declaration

In Odibi, declare grain explicitly:

```yaml
pattern:
  type: fact
  params:
    grain: [order_id, order_item_id]  # Explicit grain declaration
```

This enables:
1. **Documentation**: Clear understanding of what each row means
2. **Validation**: Detect duplicate rows that violate grain
3. **Debugging**: Easier to track data issues

### Grain Validation

If you declare grain, Odibi validates it:

```sql
-- Check for duplicates
SELECT order_id, order_item_id, COUNT(*)
FROM fact_order_items
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1
```

If duplicates exist, the pipeline fails (or warns, depending on config):

```yaml
pattern:
  type: fact
  params:
    grain: [order_id, order_item_id]
    grain_validation:
      on_violation: error  # error | warn | ignore
```

---

## Measure Types

Measures are the numbers you aggregate. But not all measures work the same way.

### Additive Measures

**Can be summed across all dimensions.**

| Measure | Example | Why Additive |
|---------|---------|--------------|
| Revenue | $100 + $200 = $300 | Totals make sense |
| Quantity | 5 + 10 = 15 | Counts add up |
| Discount Amount | $10 + $20 = $30 | Dollar amounts add |

```yaml
measures:
  - revenue
  - quantity
  - discount_amount
```

### Semi-Additive Measures

**Can be summed across some dimensions, but not all.**

| Measure | Additive Over | Non-Additive Over |
|---------|---------------|-------------------|
| Inventory Count | Product, Location | Time (snapshot) |
| Account Balance | Customer | Time (snapshot) |
| Headcount | Department | Time (snapshot) |

You can sum inventory across products (total items in warehouse). But summing inventory across time gives nonsense.

```yaml
# Inventory fact needs special handling
measures:
  - inventory_quantity  # Semi-additive
  
# Query must use MAX/AVG for time
# SUM(inventory_quantity) across dates = WRONG
# MAX(inventory_quantity) for latest = RIGHT
```

### Non-Additive Measures

**Cannot be summed across any dimension.**

| Measure | Why Non-Additive | Use Instead |
|---------|------------------|-------------|
| Unit Price | Average, not sum | AVG, or store revenue + quantity |
| Percentage | Can't sum percentages | Store numerator + denominator |
| Ratio | Can't sum ratios | Store components separately |

```yaml
# ❌ WRONG - storing calculated ratio
measures:
  - profit_margin_pct: "profit / revenue * 100"  # Can't sum!

# ✅ RIGHT - store components
measures:
  - profit
  - revenue
# Calculate margin in reporting layer
```

### Best Practice: Store Components

For any derived metric, store the components:

| Instead of | Store |
|------------|-------|
| `avg_order_value` | `order_total`, `order_count` |
| `conversion_rate` | `conversions`, `visitors` |
| `profit_margin` | `profit`, `revenue` |
| `fill_rate` | `fulfilled_qty`, `ordered_qty` |

```yaml
measures:
  - order_total     # Additive
  - order_count: "1"  # Additive (always 1)
  - profit          # Additive
  - revenue         # Additive
  # Calculate ratios in BI layer
```

---

## Factless Facts

### What Are Factless Facts?

Fact tables without measures. They track **events** or **relationships**.

### Type 1: Event Tracking

"Student attended class on this date"

| student_sk | class_sk | date_sk |
|------------|----------|---------|
| 42 | 101 | 20180315 |
| 42 | 102 | 20180316 |

No measure columns-the row's existence IS the fact.

Query:
```sql
-- How many classes did each student attend?
SELECT student_sk, COUNT(*) as classes_attended
FROM fact_class_attendance
GROUP BY student_sk
```

### Type 2: Coverage/Eligibility

"Which promotions applied to which products on which dates?"

| promotion_sk | product_sk | date_sk |
|--------------|------------|---------|
| 1 | 101 | 20180315 |
| 1 | 101 | 20180316 |
| 2 | 102 | 20180315 |

Query:
```sql
-- What products were on promotion this week?
SELECT DISTINCT product_sk
FROM fact_promotion_coverage
WHERE date_sk BETWEEN 20180315 AND 20180321
```

### Configuration

```yaml
- name: fact_class_attendance
  description: "Factless fact tracking student attendance"
  
  pattern:
    type: fact
    params:
      grain: [student_id, class_id, attendance_date]
      dimensions:
        - source_column: student_id
          dimension_table: dim_student
          surrogate_key: student_sk
        - source_column: class_id
          dimension_table: dim_class
          surrogate_key: class_sk
        - source_column: attendance_date
          dimension_table: dim_date
          surrogate_key: date_sk
      
      # No measures - factless!
      measures: []
      
      # Add count measure for convenience
      # measures:
      #   - attendance_count: "1"
```

---

## Degenerate Dimensions

Some dimension values live directly in the fact table:

| Concept | Example | Why Not Separate Dimension? |
|---------|---------|---------------------------|
| Order ID | `ORD-12345` | 1:1 with fact row, no extra attributes |
| Invoice Number | `INV-9999` | Just a reference number |
| Transaction ID | `TXN-ABC` | Unique per row |

These are **degenerate dimensions**-dimension values without a dimension table.

```yaml
pattern:
  type: fact
  params:
    grain: [order_id, order_item_id]
    
    # Degenerate dimensions stay in fact
    # No lookup needed
    
    dimensions:
      - source_column: customer_id
        dimension_table: dim_customer  # Real dimension
      - source_column: product_id
        dimension_table: dim_product   # Real dimension
    
    # order_id and order_item_id remain as columns
    # They're part of grain, not looked up
```

---

## Fact Table Validation

### 1. Grain Validation

No duplicates at grain level:

```sql
-- Must return 0 rows
SELECT grain_columns, COUNT(*)
FROM fact_table
GROUP BY grain_columns
HAVING COUNT(*) > 1
```

### 2. Referential Integrity

All foreign keys should resolve:

```sql
-- Check for orphans
SELECT COUNT(*)
FROM fact_order_items f
WHERE f.customer_sk NOT IN (SELECT customer_sk FROM dim_customer)
```

With unknown member pattern, this should return 0 (orphans map to SK=0).

### 3. Measure Reasonableness

Sanity checks on measures:

```sql
-- No negative quantities
SELECT COUNT(*) FROM fact_order_items WHERE quantity < 0

-- No insane prices
SELECT COUNT(*) FROM fact_order_items WHERE price > 1000000

-- Totals make sense
SELECT 
  SUM(price) as fact_total,
  (SELECT SUM(price) FROM silver.order_items) as source_total
FROM fact_order_items
```

### 4. Date Range Validation

Facts should fall within expected date range:

```sql
-- Check date bounds
SELECT 
  MIN(order_date_sk) as min_date,
  MAX(order_date_sk) as max_date,
  COUNT(CASE WHEN order_date_sk = 0 THEN 1 END) as unknown_dates
FROM fact_order_items
```

### Configuration

```yaml
pattern:
  type: fact
  params:
    grain: [order_id, order_item_id]
    
    validation:
      grain_check: true
      orphan_check: true
      measure_checks:
        - column: price
          min: 0
          max: 100000
        - column: quantity
          min: 1
          max: 1000
```

---

## Accumulating Snapshot Facts

Some facts track a process with multiple milestones:

**Order Lifecycle**:
| order_sk | order_date_sk | ship_date_sk | delivery_date_sk | return_date_sk |
|----------|---------------|--------------|------------------|----------------|
| 1 | 20180315 | 20180317 | 20180322 | NULL |

Each milestone gets its own date key. The row is **updated** as the order progresses.

```yaml
- name: fact_order_lifecycle
  description: "Accumulating snapshot of order milestones"
  
  pattern:
    type: fact
    params:
      grain: [order_id]
      
      # Multiple date roles
      dimensions:
        - source_column: order_date
          dimension_table: dim_date
          target_column: order_date_sk
        - source_column: ship_date
          dimension_table: dim_date
          target_column: ship_date_sk
        - source_column: delivery_date
          dimension_table: dim_date
          target_column: delivery_date_sk
        - source_column: return_date
          dimension_table: dim_date
          target_column: return_date_sk
      
      # Lag measures
      measures:
        - days_to_ship: "DATEDIFF(ship_date, order_date)"
        - days_to_deliver: "DATEDIFF(delivery_date, order_date)"
        - days_ship_to_deliver: "DATEDIFF(delivery_date, ship_date)"
```

---

## Complete Example

Here's a comprehensive fact table configuration:

```yaml
- name: fact_order_items
  description: "Order line items at transaction grain"
  
  depends_on:
    - silver_order_items
    - silver_orders
    - dim_customer
    - dim_product
    - dim_seller
    - dim_date
  
  # Combine sources
  inputs:
    order_items: $silver_ecommerce.silver_order_items
    orders: $silver_ecommerce.silver_orders
  
  transform:
    steps:
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
            oi.price + oi.freight_value as line_total
          FROM order_items oi
          JOIN orders o ON oi.order_id = o.order_id
  
  pattern:
    type: fact
    params:
      # Explicit grain
      grain: [order_id, order_item_id]
      grain_validation:
        on_violation: error
      
      # Dimension lookups
      dimensions:
        - source_column: customer_id
          dimension_table: dim_customer
          dimension_key: customer_id
          surrogate_key: customer_sk
        - source_column: product_id
          dimension_table: dim_product
          dimension_key: product_id
          surrogate_key: product_sk
        - source_column: seller_id
          dimension_table: dim_seller
          dimension_key: seller_id
          surrogate_key: seller_sk
        - source_column: order_purchase_date
          dimension_table: dim_date
          dimension_key: full_date
          surrogate_key: date_sk
          target_column: order_date_sk
      
      # Orphan handling
      orphan_handling: unknown
      
      # Measures (all additive)
      measures:
        - item_count: "1"  # For counting items
        - price
        - freight_value
        - line_total
      
      # Validation
      validation:
        orphan_check: true
        measure_checks:
          - column: price
            min: 0
          - column: freight_value
            min: 0
      
      # Audit
      audit:
        load_timestamp: true
        source_system: "ecommerce"
  
  write:
    connection: gold
    path: fact_order_items
    format: delta
```

---

## Key Takeaways

1. **Grain first** - Define what each row represents before anything else
2. **Finer grain is safer** - You can always aggregate up, can't disaggregate down
3. **Know your measure types** - Additive, semi-additive, non-additive
4. **Store components, not ratios** - Calculate derived metrics in reporting
5. **Factless facts track events** - The row existence IS the measure
6. **Validate everything** - Grain, referential integrity, measure bounds

---

## Next Steps

With solid fact table design, we'll look at performance optimization:

- Pre-aggregation for dashboard speed
- Aggregation patterns and strategies
- Choosing the right grain for aggregates

Next article: **Pre-Aggregation Strategies for Fast Dashboards**.

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]
- **LinkedIn:** Follow for more articles in this series
