# Pre-Aggregation Strategies for Fast Dashboards

*Trade storage for speed*

---

## TL;DR

Dashboards that query billions of rows are slow. Pre-aggregation creates summary tables at coarser grains, trading storage space for query speed. This article covers when to aggregate, how to choose the right grain, incremental aggregation patterns, and the Odibi aggregation pattern for declarative configuration.

---

## The Problem: Slow Dashboards

Your fact table has 500 million rows. The dashboard query:

```sql
SELECT 
  d.month_name,
  r.region,
  SUM(f.revenue)
FROM fact_sales f
JOIN dim_date d ON f.date_sk = d.date_sk
JOIN dim_region r ON f.region_sk = r.region_sk
WHERE d.year = 2023
GROUP BY d.month_name, r.region
```

Takes 45 seconds. Users give up after 10.

---

## The Solution: Pre-Aggregation

Create a summary table at monthly + region grain:

| month_sk | region_sk | total_revenue | order_count |
|----------|-----------|---------------|-------------|
| 202301 | 1 | 1,250,000 | 15,234 |
| 202301 | 2 | 890,000 | 11,567 |
| 202302 | 1 | 1,380,000 | 16,891 |

Now the query:

```sql
SELECT 
  d.month_name,
  r.region,
  a.total_revenue
FROM agg_monthly_sales_by_region a
JOIN dim_date d ON a.month_sk = d.month_sk
JOIN dim_region r ON a.region_sk = r.region_sk
WHERE d.year = 2023
```

Scans 24 rows instead of 500 million. Instant.

---

## When to Aggregate

### Aggregate When:

| Scenario | Why |
|----------|-----|
| Dashboard loads slowly | Direct query on fact takes too long |
| Same query runs repeatedly | Cache the result |
| Known reporting grain | "Monthly by region" is a standard view |
| Detail not needed | Users never drill to transaction level |

### Don't Aggregate When:

| Scenario | Why |
|----------|-----|
| Data changes frequently | Aggregate staleness issues |
| Users need detail | Can't drill through pre-aggregated data |
| Unknown query patterns | You don't know what grain to use |
| Small data volumes | 1M rows is fast enough without aggregation |

---

## Choosing Aggregation Grain

The aggregation grain determines:
- **Storage**: Finer grain = more rows
- **Flexibility**: Finer grain = more query options
- **Speed**: Coarser grain = faster queries

### Common Aggregation Grains

| Aggregate Table | Grain | Rows (approx) |
|-----------------|-------|---------------|
| Daily by product by store | day × product × store | 365 × 10K × 100 = 365M |
| Daily by product | day × product | 365 × 10K = 3.65M |
| Monthly by category by region | month × category × region | 12 × 50 × 10 = 6K |
| Yearly by category | year × category | 5 × 50 = 250 |

### Rule of Thumb

Start with the grain that matches your most common dashboard query. Add more aggregates as needed.

```
Fact Table (500M rows)
    │
    ▼
Daily Aggregate (3.65M rows)      ← For daily trend charts
    │
    ▼
Monthly Aggregate (36K rows)      ← For monthly reports
    │
    ▼
Yearly Aggregate (500 rows)       ← For YoY comparison
```

---

## The Aggregation Pattern

Odibi provides a declarative aggregation pattern:

```yaml
- name: agg_monthly_sales_by_region
  description: "Monthly sales aggregated by region"
  
  depends_on: [fact_order_items]
  
  read:
    connection: gold
    path: fact_order_items
    format: delta
  
  pattern:
    type: aggregation
    params:
      grain: [month_sk, region_sk]
      
      measures:
        - name: total_revenue
          expr: "SUM(line_total)"
        
        - name: total_orders
          expr: "COUNT(DISTINCT order_id)"
        
        - name: total_items
          expr: "SUM(item_count)"
        
        - name: avg_order_value
          expr: "SUM(line_total) / COUNT(DISTINCT order_id)"
      
      audit:
        load_timestamp: true
  
  write:
    connection: gold
    path: agg_monthly_sales_by_region
    format: delta
```

### How It Works

1. Reads from fact table
2. Groups by grain columns
3. Applies aggregate expressions
4. Writes summary table

Generated SQL:

```sql
SELECT 
  month_sk,
  region_sk,
  SUM(line_total) as total_revenue,
  COUNT(DISTINCT order_id) as total_orders,
  SUM(item_count) as total_items,
  SUM(line_total) / COUNT(DISTINCT order_id) as avg_order_value,
  CURRENT_TIMESTAMP() as load_timestamp
FROM fact_order_items
GROUP BY month_sk, region_sk
```

---

## Joining Dimensions for Aggregation

Often you need dimension attributes for grouping:

```yaml
- name: agg_monthly_sales_by_category
  description: "Monthly sales by product category"
  
  depends_on: 
    - fact_order_items
    - dim_product
    - dim_date
  
  inputs:
    facts: $gold_facts.fact_order_items
    products: $gold_dimensions.dim_product
    dates: $gold_dimensions.dim_date
  
  transform:
    steps:
      # Join to get dimension attributes
      - sql: |
          SELECT 
            d.year,
            d.month,
            d.month_name,
            p.product_category_name,
            f.line_total,
            f.item_count,
            f.order_id
          FROM facts f
          JOIN products p ON f.product_sk = p.product_sk
          JOIN dates d ON f.order_date_sk = d.date_sk
  
  pattern:
    type: aggregation
    params:
      grain: [year, month, product_category_name]
      
      measures:
        - name: total_revenue
          expr: "SUM(line_total)"
        - name: total_items
          expr: "SUM(item_count)"
        - name: unique_orders
          expr: "COUNT(DISTINCT order_id)"
  
  write:
    connection: gold
    path: agg_monthly_sales_by_category
    format: delta
```

---

## Incremental Aggregation

### The Problem

Full aggregation reprocesses all history every run. For 500M rows, that's expensive.

### The Solution: Incremental Updates

Only aggregate new/changed data, then merge with existing aggregate.

```yaml
pattern:
  type: aggregation
  params:
    grain: [date_sk, product_sk]
    
    incremental:
      timestamp_column: load_timestamp
      lookback: "7 days"  # Only process last 7 days
      merge_strategy: replace  # Replace matching grain rows
    
    measures:
      - name: total_revenue
        expr: "SUM(line_total)"
```

### Merge Strategies

| Strategy | Behavior | Use Case |
|----------|----------|----------|
| `replace` | Replace existing rows for grain | Most aggregates |
| `sum` | Add to existing values | Cumulative totals (dangerous!) |
| `max` | Keep larger value | Latest snapshot |

### How Replace Works

```
Existing Aggregate:
| date_sk  | product_sk | revenue |
|----------|------------|---------|
| 20230115 | 101        | 1000    |
| 20230116 | 101        | 1200    |

New Data (for 20230116):
| date_sk  | product_sk | revenue |
|----------|------------|---------|
| 20230116 | 101        | 1500    |  ← Corrected value

After Merge (replace):
| date_sk  | product_sk | revenue |
|----------|------------|---------|
| 20230115 | 101        | 1000    |  ← Unchanged
| 20230116 | 101        | 1500    |  ← Replaced
```

---

## HAVING Clause

Filter aggregates after grouping:

```yaml
pattern:
  type: aggregation
  params:
    grain: [customer_sk]
    
    measures:
      - name: total_spend
        expr: "SUM(line_total)"
      - name: order_count
        expr: "COUNT(DISTINCT order_id)"
    
    having: "COUNT(DISTINCT order_id) > 5"  # Only customers with 5+ orders
```

Useful for:
- Top N customers
- Products with significant volume
- Filtering noise from aggregates

---

## Multiple Aggregation Levels

Build a hierarchy of aggregates:

```yaml
# Level 1: Daily grain
- name: agg_daily_sales
  pattern:
    type: aggregation
    params:
      grain: [date_sk, product_sk, store_sk]
      measures:
        - name: revenue
          expr: "SUM(line_total)"

# Level 2: Monthly grain (built from daily)
- name: agg_monthly_sales
  depends_on: [agg_daily_sales]
  read:
    connection: gold
    path: agg_daily_sales
  pattern:
    type: aggregation
    params:
      grain: [month_sk, product_sk, store_sk]
      measures:
        - name: revenue
          expr: "SUM(revenue)"  # Aggregate from daily

# Level 3: Yearly grain (built from monthly)
- name: agg_yearly_sales
  depends_on: [agg_monthly_sales]
  read:
    connection: gold
    path: agg_monthly_sales
  pattern:
    type: aggregation
    params:
      grain: [year, product_sk, store_sk]
      measures:
        - name: revenue
          expr: "SUM(revenue)"  # Aggregate from monthly
```

Benefits:
- Each level is smaller and faster to compute
- Can run incrementally at each level
- Clear audit trail

---

## Aggregate Awareness in BI Tools

Some BI tools automatically route queries to the right aggregate:

```
User asks: "Revenue by month by region"
↓
BI tool checks: agg_monthly_sales_by_region exists?
↓
Yes → Query aggregate (fast)
No → Query fact table (slow)
```

For tools without aggregate awareness, create views:

```sql
CREATE VIEW v_sales_summary AS
SELECT 
  d.year,
  d.month_name,
  r.region_name,
  a.total_revenue
FROM agg_monthly_sales_by_region a
JOIN dim_date d ON a.month_sk = d.month_sk
JOIN dim_region r ON a.region_sk = r.region_sk
```

---

## Materialized Views vs Aggregation Tables

| Approach | Pros | Cons |
|----------|------|------|
| **Aggregation Tables** | Full control, cross-database | Manual refresh |
| **Materialized Views** | Auto-refresh in some DBs | Database-specific |

Odibi uses aggregation tables for portability across Spark, Databricks, Snowflake, etc.

---

## Complete Example

Here's a full aggregation pipeline:

```yaml
pipelines:
  - pipeline: gold_aggregates
    layer: gold
    description: "Pre-aggregated summary tables"
    
    nodes:
      # Daily by product and region
      - name: agg_daily_product_region
        description: "Daily sales by product and region"
        depends_on: [fact_order_items, dim_product, dim_customer, dim_date]
        
        inputs:
          facts: $gold_facts.fact_order_items
          products: $gold_dimensions.dim_product
          customers: $gold_dimensions.dim_customer
          dates: $gold_dimensions.dim_date
        
        transform:
          steps:
            - sql: |
                SELECT 
                  d.date_sk,
                  d.year,
                  d.month,
                  d.day_of_week,
                  p.product_category_name,
                  c.customer_region,
                  f.line_total,
                  f.item_count,
                  f.order_id
                FROM facts f
                JOIN products p ON f.product_sk = p.product_sk
                JOIN customers c ON f.customer_sk = c.customer_sk
                JOIN dates d ON f.order_date_sk = d.date_sk
        
        pattern:
          type: aggregation
          params:
            grain: [date_sk, product_category_name, customer_region]
            measures:
              - name: total_revenue
                expr: "SUM(line_total)"
              - name: total_items
                expr: "SUM(item_count)"
              - name: order_count
                expr: "COUNT(DISTINCT order_id)"
            audit:
              load_timestamp: true
        
        write:
          connection: gold
          path: agg_daily_product_region
          format: delta

      # Monthly summary (from daily)
      - name: agg_monthly_summary
        description: "Monthly sales summary"
        depends_on: [agg_daily_product_region]
        
        read:
          connection: gold
          path: agg_daily_product_region
          format: delta
        
        pattern:
          type: aggregation
          params:
            grain: [year, month, product_category_name, customer_region]
            measures:
              - name: total_revenue
                expr: "SUM(total_revenue)"
              - name: total_items
                expr: "SUM(total_items)"
              - name: order_count
                expr: "SUM(order_count)"
              - name: days_with_sales
                expr: "COUNT(DISTINCT date_sk)"
            audit:
              load_timestamp: true
        
        write:
          connection: gold
          path: agg_monthly_summary
          format: delta
```

---

## Key Takeaways

1. **Pre-aggregation trades storage for speed** - Worth it for slow dashboards
2. **Choose grain based on query patterns** - Match your most common reports
3. **Incremental aggregation saves time** - Don't reprocess all history
4. **Build hierarchies** - Daily → Monthly → Yearly
5. **Store components, calculate ratios** - Aggregates should be additive
6. **Consider BI tool integration** - Views or aggregate awareness

---

## Next Steps

With fast aggregates in place, we'll look at data quality across the entire pipeline:

- Contract patterns for each layer
- Validation strategies
- Quarantine management

Next article: **Data Quality Patterns: Contracts, Validation, and Quarantine**.

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]
- **Medium:** Follow for more articles in this series
