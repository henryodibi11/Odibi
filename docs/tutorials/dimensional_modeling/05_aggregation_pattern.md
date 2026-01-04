# Aggregation Pattern Tutorial

In this tutorial, you'll learn how to use Odibi's `aggregation` pattern to build pre-aggregated tables for faster reporting and dashboards.

**What You'll Learn:**
- Why pre-aggregate fact tables
- Defining grain and measures
- Using aggregate functions (SUM, COUNT, AVG)
- Incremental merge strategies (replace vs sum)

---

## Why Pre-Aggregate?

Consider a dashboard showing "Daily Revenue by Product Category":

**Without pre-aggregation:**
```sql
-- Runs against 10 million fact rows every time
SELECT 
    d.full_date,
    p.category,
    SUM(f.line_total) AS revenue
FROM fact_orders f
JOIN dim_date d ON f.date_sk = d.date_sk
JOIN dim_product p ON f.product_sk = p.product_sk
GROUP BY d.full_date, p.category;
-- Takes 30 seconds
```

**With pre-aggregation:**
```sql
-- Runs against 5,000 aggregated rows
SELECT full_date, category, total_revenue
FROM agg_daily_category_sales;
-- Takes 0.1 seconds
```

Pre-aggregated tables trade storage for speed. For frequently-queried metrics, this is almost always worth it.

---

## Source Data: fact_orders

We'll aggregate the fact table from the previous tutorial (30 rows):

| order_id | customer_sk | product_sk | date_sk | quantity | unit_price | line_total | status |
|----------|-------------|------------|---------|----------|------------|------------|--------|
| ORD001 | 1 | 1 | 20240115 | 1 | 1299.99 | 1299.99 | completed |
| ORD002 | 1 | 2 | 20240115 | 2 | 29.99 | 59.98 | completed |
| ORD003 | 2 | 3 | 20240116 | 1 | 249.99 | 249.99 | completed |
| ORD004 | 3 | 4 | 20240116 | 3 | 49.99 | 149.97 | completed |
| ORD005 | 4 | 5 | 20240117 | 1 | 599.99 | 599.99 | completed |
| ORD006 | 5 | 6 | 20240117 | 1 | 149.99 | 149.99 | completed |
| ORD007 | 6 | 7 | 20240118 | 2 | 399.99 | 799.98 | completed |
| ORD008 | 7 | 8 | 20240118 | 4 | 45.99 | 183.96 | completed |
| ORD009 | 8 | 9 | 20240119 | 1 | 79.99 | 79.99 | completed |
| ORD010 | 9 | 10 | 20240119 | 1 | 189.99 | 189.99 | completed |
| ORD011 | 10 | 1 | 20240120 | 1 | 1299.99 | 1299.99 | completed |
| ORD012 | 11 | 2 | 20240120 | 5 | 29.99 | 149.95 | completed |
| ORD013 | 12 | 3 | 20240121 | 2 | 249.99 | 499.98 | completed |
| ORD014 | 1 | 4 | 20240121 | 1 | 49.99 | 49.99 | completed |
| ORD015 | 2 | 5 | 20240122 | 1 | 599.99 | 599.99 | pending |
| ORD016 | 3 | 6 | 20240122 | 2 | 149.99 | 299.98 | completed |
| ORD017 | 4 | 7 | 20240123 | 1 | 399.99 | 399.99 | completed |
| ORD018 | 5 | 8 | 20240123 | 3 | 45.99 | 137.97 | completed |
| ORD019 | 6 | 9 | 20240124 | 2 | 79.99 | 159.98 | completed |
| ORD020 | 7 | 10 | 20240124 | 1 | 189.99 | 189.99 | completed |
| ORD021 | 8 | 1 | 20240125 | 1 | 1299.99 | 1299.99 | completed |
| ORD022 | 9 | 2 | 20240125 | 3 | 29.99 | 89.97 | completed |
| ORD023 | 10 | 3 | 20240126 | 1 | 249.99 | 249.99 | cancelled |
| ORD024 | 11 | 4 | 20240126 | 2 | 49.99 | 99.98 | completed |
| ORD025 | 12 | 5 | 20240127 | 1 | 599.99 | 599.99 | completed |
| ORD026 | 1 | 6 | 20240127 | 1 | 149.99 | 149.99 | completed |
| ORD027 | 2 | 7 | 20240128 | 1 | 399.99 | 399.99 | completed |
| ORD028 | 3 | 8 | 20240128 | 2 | 45.99 | 91.98 | completed |
| ORD029 | 4 | 9 | 20240115 | 1 | 79.99 | 79.99 | completed |
| ORD030 | 5 | 10 | 20240116 | 1 | 189.99 | 189.99 | completed |

---

## Step 1: Basic Aggregation by Date and Product

Let's aggregate orders by date and product to see daily product sales.

### YAML Configuration

```yaml
project: aggregation_tutorial
engine: pandas

connections:
  warehouse:
    type: file
    path: ./warehouse

story:
  connection: warehouse
  path: stories

pipelines:
  - pipeline: build_aggregates
    nodes:
      - name: agg_daily_product_sales
        read:
          connection: warehouse
          path: fact_orders
          format: parquet
        
        pattern:
          type: aggregation
          params:
            grain: [date_sk, product_sk]
            measures:
              - name: total_revenue
                expr: "SUM(line_total)"
              - name: order_count
                expr: "COUNT(*)"
              - name: total_quantity
                expr: "SUM(quantity)"
            audit:
              load_timestamp: true
        
        write:
          connection: warehouse
          path: agg_daily_product_sales
          format: parquet
          mode: overwrite
```

### Understanding Grain and Measures

**Grain:** The columns that define uniqueness in the output. Here, each combination of `date_sk` + `product_sk` gets one row.

**Measures:** The aggregations to compute. Each measure needs:
- `name`: Output column name
- `expr`: SQL aggregation expression

### Output: agg_daily_product_sales (26 rows)

Here are all 26 aggregated rows:

| date_sk | product_sk | total_revenue | order_count | total_quantity | load_timestamp |
|---------|------------|---------------|-------------|----------------|----------------|
| 20240115 | 1 | 1299.99 | 1 | 1 | 2024-01-30 10:00:00 |
| 20240115 | 2 | 59.98 | 1 | 2 | 2024-01-30 10:00:00 |
| 20240115 | 9 | 79.99 | 1 | 1 | 2024-01-30 10:00:00 |
| 20240116 | 3 | 249.99 | 1 | 1 | 2024-01-30 10:00:00 |
| 20240116 | 4 | 149.97 | 1 | 3 | 2024-01-30 10:00:00 |
| 20240116 | 10 | 189.99 | 1 | 1 | 2024-01-30 10:00:00 |
| 20240117 | 5 | 599.99 | 1 | 1 | 2024-01-30 10:00:00 |
| 20240117 | 6 | 149.99 | 1 | 1 | 2024-01-30 10:00:00 |
| 20240118 | 7 | 799.98 | 1 | 2 | 2024-01-30 10:00:00 |
| 20240118 | 8 | 183.96 | 1 | 4 | 2024-01-30 10:00:00 |
| 20240119 | 9 | 79.99 | 1 | 1 | 2024-01-30 10:00:00 |
| 20240119 | 10 | 189.99 | 1 | 1 | 2024-01-30 10:00:00 |
| 20240120 | 1 | 1299.99 | 1 | 1 | 2024-01-30 10:00:00 |
| 20240120 | 2 | 149.95 | 1 | 5 | 2024-01-30 10:00:00 |
| 20240121 | 3 | 499.98 | 1 | 2 | 2024-01-30 10:00:00 |
| 20240121 | 4 | 49.99 | 1 | 1 | 2024-01-30 10:00:00 |
| 20240122 | 5 | 599.99 | 1 | 1 | 2024-01-30 10:00:00 |
| 20240122 | 6 | 299.98 | 1 | 2 | 2024-01-30 10:00:00 |
| 20240123 | 7 | 399.99 | 1 | 1 | 2024-01-30 10:00:00 |
| 20240123 | 8 | 137.97 | 1 | 3 | 2024-01-30 10:00:00 |
| 20240124 | 9 | 159.98 | 1 | 2 | 2024-01-30 10:00:00 |
| 20240124 | 10 | 189.99 | 1 | 1 | 2024-01-30 10:00:00 |
| 20240125 | 1 | 1299.99 | 1 | 1 | 2024-01-30 10:00:00 |
| 20240125 | 2 | 89.97 | 1 | 3 | 2024-01-30 10:00:00 |
| 20240126 | 3 | 249.99 | 1 | 1 | 2024-01-30 10:00:00 |
| 20240126 | 4 | 99.98 | 1 | 2 | 2024-01-30 10:00:00 |
| 20240127 | 5 | 599.99 | 1 | 1 | 2024-01-30 10:00:00 |
| 20240127 | 6 | 149.99 | 1 | 1 | 2024-01-30 10:00:00 |
| 20240128 | 7 | 399.99 | 1 | 1 | 2024-01-30 10:00:00 |
| 20240128 | 8 | 91.98 | 1 | 2 | 2024-01-30 10:00:00 |

**Result:** 30 fact rows became 26 aggregate rows (some date+product combinations had multiple orders).

---

## Step 2: Adding More Measures

Let's add average and distinct count measures:

```yaml
params:
  grain: [date_sk, product_sk]
  measures:
    - name: total_revenue
      expr: "SUM(line_total)"
    - name: order_count
      expr: "COUNT(*)"
    - name: total_quantity
      expr: "SUM(quantity)"
    - name: avg_order_value
      expr: "AVG(line_total)"
    - name: unique_customers
      expr: "COUNT(DISTINCT customer_sk)"
    - name: max_order_value
      expr: "MAX(line_total)"
    - name: min_order_value
      expr: "MIN(line_total)"
```

### Available Aggregation Functions

| Function | Description | Example |
|----------|-------------|---------|
| `SUM(column)` | Total of values | `SUM(line_total)` |
| `COUNT(*)` | Number of rows | `COUNT(*)` |
| `COUNT(column)` | Non-null count | `COUNT(customer_sk)` |
| `COUNT(DISTINCT column)` | Unique values | `COUNT(DISTINCT customer_sk)` |
| `AVG(column)` | Average value | `AVG(line_total)` |
| `MAX(column)` | Maximum value | `MAX(line_total)` |
| `MIN(column)` | Minimum value | `MIN(line_total)` |

### Complex Expressions

You can use expressions within aggregations:

```yaml
measures:
  # Total after 10% discount
  - name: discounted_revenue
    expr: "SUM(line_total * 0.9)"
  
  # Margin (if cost column existed)
  - name: total_margin
    expr: "SUM(line_total - cost)"
  
  # Discount rate
  - name: avg_discount_rate
    expr: "AVG(discount_amount / line_total)"
```

---

## Step 3: Aggregating by Different Grains

### Daily Sales (Grain: date only)

```yaml
params:
  grain: [date_sk]
  measures:
    - name: total_revenue
      expr: "SUM(line_total)"
    - name: order_count
      expr: "COUNT(*)"
    - name: unique_products
      expr: "COUNT(DISTINCT product_sk)"
    - name: unique_customers
      expr: "COUNT(DISTINCT customer_sk)"
```

**Output: agg_daily_sales (14 rows)**

| date_sk | total_revenue | order_count | unique_products | unique_customers |
|---------|---------------|-------------|-----------------|------------------|
| 20240115 | 1439.96 | 3 | 3 | 2 |
| 20240116 | 589.95 | 3 | 3 | 3 |
| 20240117 | 749.98 | 2 | 2 | 2 |
| 20240118 | 983.94 | 2 | 2 | 2 |
| 20240119 | 269.98 | 2 | 2 | 2 |
| 20240120 | 1449.94 | 2 | 2 | 2 |
| 20240121 | 549.97 | 2 | 2 | 2 |
| 20240122 | 899.97 | 2 | 2 | 2 |
| 20240123 | 537.96 | 2 | 2 | 2 |
| 20240124 | 349.97 | 2 | 2 | 2 |
| 20240125 | 1389.96 | 2 | 2 | 2 |
| 20240126 | 349.97 | 2 | 2 | 2 |
| 20240127 | 749.98 | 2 | 2 | 2 |
| 20240128 | 491.97 | 2 | 2 | 2 |

### Product Sales (Grain: product only)

```yaml
params:
  grain: [product_sk]
  measures:
    - name: total_revenue
      expr: "SUM(line_total)"
    - name: order_count
      expr: "COUNT(*)"
    - name: total_quantity
      expr: "SUM(quantity)"
```

**Output: agg_product_sales (10 rows)**

| product_sk | total_revenue | order_count | total_quantity |
|------------|---------------|-------------|----------------|
| 1 | 3899.97 | 3 | 3 |
| 2 | 299.90 | 3 | 10 |
| 3 | 999.96 | 3 | 4 |
| 4 | 299.94 | 3 | 6 |
| 5 | 1799.97 | 3 | 3 |
| 6 | 599.96 | 3 | 4 |
| 7 | 1599.96 | 3 | 4 |
| 8 | 413.91 | 3 | 9 |
| 9 | 319.96 | 3 | 4 |
| 10 | 569.97 | 3 | 3 |

---

## Step 4: Incremental Merge Strategies

For ongoing pipelines, you don't want to rebuild the entire aggregate table. Incremental strategies allow you to update only affected rows.

### Replace Strategy

**How it works:** New aggregates completely replace existing rows for matching grain keys.

```yaml
params:
  grain: [date_sk, product_sk]
  measures:
    - name: total_revenue
      expr: "SUM(line_total)"
    - name: order_count
      expr: "COUNT(*)"
  incremental:
    timestamp_column: load_timestamp
    merge_strategy: replace
  target: warehouse.agg_daily_product_sales
```

**Example Scenario:**

**Existing agg_daily_product_sales:**

| date_sk | product_sk | total_revenue | order_count |
|---------|------------|---------------|-------------|
| 20240115 | 1 | 1299.99 | 1 |
| 20240115 | 2 | 59.98 | 1 |
| 20240116 | 3 | 249.99 | 1 |

**New orders arrive for 2024-01-15:**

| order_id | product_sk | date_sk | line_total |
|----------|------------|---------|------------|
| ORD100 | 1 | 20240115 | 1299.99 |

**New aggregate computed:**

| date_sk | product_sk | total_revenue | order_count |
|---------|------------|---------------|-------------|
| 20240115 | 1 | 2599.98 | 2 |

**After Replace Merge:**

| date_sk | product_sk | total_revenue | order_count | Note |
|---------|------------|---------------|-------------|------|
| 20240115 | 1 | 2599.98 | 2 | **Replaced** |
| 20240115 | 2 | 59.98 | 1 | Unchanged |
| 20240116 | 3 | 249.99 | 1 | Unchanged |

**Use case:** Best for most scenarios. Handles late-arriving data, corrections, and restatements correctly.

### Sum Strategy

**How it works:** New measure values are added to existing values.

```yaml
params:
  incremental:
    timestamp_column: load_timestamp
    merge_strategy: sum
```

**Example Scenario:**

**Existing:**

| date_sk | product_sk | total_revenue | order_count |
|---------|------------|---------------|-------------|
| 20240115 | 1 | 1299.99 | 1 |

**New aggregate:**

| date_sk | product_sk | total_revenue | order_count |
|---------|------------|---------------|-------------|
| 20240115 | 1 | 1299.99 | 1 |

**After Sum Merge:**

| date_sk | product_sk | total_revenue | order_count |
|---------|------------|---------------|-------------|
| 20240115 | 1 | 2599.98 | 2 |

**Use case:** Only for purely additive metrics (counts, sums) on append-only data.

**Warning:** Do NOT use sum strategy for:
- AVG (would become average of averages, incorrect)
- COUNT DISTINCT (would overcount)
- MIN/MAX (would be wrong)
- Data with updates or late-arriving records

---

## Complete YAML Example

Here's a complete example with multiple aggregate tables:

```yaml
# File: odibi_aggregation_tutorial.yaml
project: aggregation_tutorial
engine: pandas

connections:
  warehouse:
    type: file
    path: ./warehouse

story:
  connection: warehouse
  path: stories

pipelines:
  - pipeline: build_aggregates
    description: "Build all aggregate tables from fact_orders"
    nodes:
      # Daily aggregate at product level
      - name: agg_daily_product_sales
        description: "Daily sales by product"
        read:
          connection: warehouse
          path: fact_orders
          format: parquet
        
        pattern:
          type: aggregation
          params:
            grain:
              - date_sk
              - product_sk
            measures:
              - name: total_revenue
                expr: "SUM(line_total)"
              - name: order_count
                expr: "COUNT(*)"
              - name: total_quantity
                expr: "SUM(quantity)"
              - name: avg_order_value
                expr: "AVG(line_total)"
            having: "SUM(line_total) > 0"
            audit:
              load_timestamp: true
        
        write:
          connection: warehouse
          path: agg_daily_product_sales
          format: parquet
          mode: overwrite
      
      # Daily summary (no product breakdown)
      - name: agg_daily_sales
        description: "Daily sales summary"
        read:
          connection: warehouse
          path: fact_orders
          format: parquet
        
        pattern:
          type: aggregation
          params:
            grain: [date_sk]
            measures:
              - name: total_revenue
                expr: "SUM(line_total)"
              - name: order_count
                expr: "COUNT(*)"
              - name: unique_products
                expr: "COUNT(DISTINCT product_sk)"
              - name: unique_customers
                expr: "COUNT(DISTINCT customer_sk)"
              - name: avg_order_value
                expr: "AVG(line_total)"
            audit:
              load_timestamp: true
        
        write:
          connection: warehouse
          path: agg_daily_sales
          format: parquet
          mode: overwrite
      
      # Product summary (no date breakdown)
      - name: agg_product_sales
        description: "Product sales summary"
        read:
          connection: warehouse
          path: fact_orders
          format: parquet
        
        pattern:
          type: aggregation
          params:
            grain: [product_sk]
            measures:
              - name: total_revenue
                expr: "SUM(line_total)"
              - name: order_count
                expr: "COUNT(*)"
              - name: total_quantity
                expr: "SUM(quantity)"
              - name: unique_customers
                expr: "COUNT(DISTINCT customer_sk)"
            audit:
              load_timestamp: true
        
        write:
          connection: warehouse
          path: agg_product_sales
          format: parquet
          mode: overwrite
```

---

## Using Aggregates in Queries

### Dashboard Query: Daily Revenue Trend

```sql
SELECT 
    a.date_sk,
    d.full_date,
    d.day_of_week,
    a.total_revenue,
    a.order_count
FROM agg_daily_sales a
JOIN dim_date d ON a.date_sk = d.date_sk
ORDER BY a.date_sk;
```

| full_date | day_of_week | total_revenue | order_count |
|-----------|-------------|---------------|-------------|
| 2024-01-15 | Monday | 1439.96 | 3 |
| 2024-01-16 | Tuesday | 589.95 | 3 |
| 2024-01-17 | Wednesday | 749.98 | 2 |
| 2024-01-18 | Thursday | 983.94 | 2 |
| 2024-01-19 | Friday | 269.98 | 2 |
| 2024-01-20 | Saturday | 1449.94 | 2 |
| 2024-01-21 | Sunday | 549.97 | 2 |
| 2024-01-22 | Monday | 899.97 | 2 |
| 2024-01-23 | Tuesday | 537.96 | 2 |
| 2024-01-24 | Wednesday | 349.97 | 2 |
| 2024-01-25 | Thursday | 1389.96 | 2 |
| 2024-01-26 | Friday | 349.97 | 2 |
| 2024-01-27 | Saturday | 749.98 | 2 |
| 2024-01-28 | Sunday | 491.97 | 2 |

### Dashboard Query: Top Products

```sql
SELECT 
    p.name AS product_name,
    p.category,
    a.total_revenue,
    a.order_count,
    a.total_quantity
FROM agg_product_sales a
JOIN dim_product p ON a.product_sk = p.product_sk
ORDER BY a.total_revenue DESC
LIMIT 5;
```

| product_name | category | total_revenue | order_count | total_quantity |
|--------------|----------|---------------|-------------|----------------|
| Laptop Pro 15 | Electronics | 3899.97 | 3 | 3 |
| Standing Desk | Furniture | 1799.97 | 3 | 3 |
| Monitor 27" | Electronics | 1599.96 | 3 | 4 |
| Office Chair | Furniture | 999.96 | 3 | 4 |
| Mechanical Keyboard | Electronics | 599.96 | 3 | 4 |

---

## What You Learned

In this tutorial, you learned:

- **Pre-aggregation** speeds up reporting by reducing data volume
- **Grain** defines the uniqueness (GROUP BY columns) of aggregate rows
- **Measures** define what to compute (SUM, COUNT, AVG, etc.)
- **Complex expressions** can be used within aggregations
- **Replace strategy** is best for most incremental scenarios
- **Sum strategy** works only for purely additive metrics on append-only data
- Aggregate tables are joined back to dimensions for rich reporting

---

## Next Steps

Now let's put it all together with a complete star schema example.

**Next:** [Full Star Schema Tutorial](./06_full_star_schema.md)

---

## Navigation

| Previous | Up | Next |
|----------|----|----|
| [Fact Pattern](./04_fact_pattern.md) | [Tutorials](../getting_started.md) | [Full Star Schema](./06_full_star_schema.md) |

---

## Reference

For complete parameter documentation, see: [Aggregation Pattern Reference](../../patterns/aggregation.md)
