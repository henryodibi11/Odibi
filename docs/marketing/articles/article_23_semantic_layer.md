# Building a Semantic Layer: Metrics Everyone Can Trust

*Define once, query everywhere*

---

## TL;DR

A semantic layer defines business metrics centrally so everyone gets the same numbers. Instead of each analyst writing their own "revenue" calculation, you define it once: `SUM(line_total) WHERE status = 'completed'`. This article covers building a semantic layer with Odibi: defining metrics, dimensions, and materializations.

---

## The Problem: Metric Chaos

Marketing says revenue is $10M.
Finance says revenue is $9.5M.
Sales says revenue is $11M.

Why?
- Marketing includes pending orders
- Finance excludes refunds
- Sales includes forecasted orders

Everyone has their own definition. Nobody trusts the numbers.

---

## The Solution: Semantic Layer

One authoritative definition:

```yaml
metrics:
  - name: revenue
    description: "Total completed order revenue, net of refunds"
    expr: "SUM(line_total)"
    source: gold.fact_orders
    filters:
      - "status = 'completed'"
      - "refunded = false"
```

Now everyone queries the same metric:

```python
project.query("revenue BY region")
project.query("revenue BY month")
project.query("revenue BY product_category")
```

Same definition. Same numbers. Every time.

---

## Core Concepts

### Metrics

Measurable values you aggregate:

```yaml
metrics:
  - name: revenue
    expr: "SUM(line_total)"
    source: gold.fact_order_items
  
  - name: order_count
    expr: "COUNT(DISTINCT order_id)"
    source: gold.fact_orders
  
  - name: avg_order_value
    expr: "SUM(line_total) / COUNT(DISTINCT order_id)"
    source: gold.fact_order_items
```

### Dimensions

Attributes you group by:

```yaml
dimensions:
  - name: region
    source: gold.dim_customer
    column: customer_region
  
  - name: product_category
    source: gold.dim_product
    column: product_category_name
  
  - name: order_date
    source: gold.dim_date
    column: full_date
    hierarchy: [year, quarter, month, day]
```

### Materializations

Pre-computed aggregates for performance:

```yaml
materializations:
  - name: monthly_revenue_by_region
    metrics: [revenue, order_count]
    dimensions: [region, month]
    output: gold.agg_monthly_region
    schedule: "0 6 * * *"  # Daily at 6 AM
```

---

## Configuration

### Complete Semantic Layer Setup

```yaml
project: "ecommerce_warehouse"
engine: "spark"

connections:
  gold:
    type: delta
    catalog: spark_catalog
    schema_name: gold

# Semantic layer configuration
semantic:
  metrics:
    # Simple metric
    - name: revenue
      description: "Total revenue from completed orders"
      expr: "SUM(line_total)"
      source: gold.fact_order_items
      filters:
        - "order_status = 'completed'"
    
    # Count metric
    - name: order_count
      description: "Number of unique orders"
      expr: "COUNT(DISTINCT order_id)"
      source: gold.fact_orders
    
    # Derived metric (references other metrics)
    - name: avg_order_value
      description: "Average order value"
      expr: "revenue / order_count"
      type: derived
    
    # Metric with time filter
    - name: revenue_last_30_days
      description: "Revenue in last 30 days"
      expr: "SUM(line_total)"
      source: gold.fact_order_items
      filters:
        - "order_date >= CURRENT_DATE - 30"
  
  dimensions:
    # Simple dimension
    - name: region
      description: "Customer region"
      source: gold.dim_customer
      column: customer_region
    
    # Dimension with hierarchy
    - name: date
      description: "Order date"
      source: gold.dim_date
      column: full_date
      hierarchy: [year, quarter, month, week, day]
    
    # Product dimension
    - name: product_category
      description: "Product category"
      source: gold.dim_product
      column: product_category_name
    
    # Dimension from same table as metric
    - name: order_status
      description: "Order status"
      source: gold.fact_orders
      column: order_status
  
  materializations:
    # Daily refresh
    - name: daily_revenue
      metrics: [revenue, order_count]
      dimensions: [date, region]
      output: gold.agg_daily_revenue
      schedule: "0 6 * * *"
    
    # Weekly refresh
    - name: weekly_category_performance
      metrics: [revenue, order_count, avg_order_value]
      dimensions: [product_category, region, month]
      output: gold.agg_category_performance
      schedule: "0 8 * * 1"  # Monday 8 AM
```

---

## Querying the Semantic Layer

### Python API

```python
from odibi import Project

# Load project with semantic layer
project = Project.load("odibi.yaml")

# Simple query
result = project.query("revenue")
print(f"Total Revenue: ${result.value:,.2f}")

# Query by dimension
result = project.query("revenue BY region")
print(result.df)
#   region      revenue
#   Southeast   5,234,567
#   South       2,123,456
#   Northeast   1,567,890

# Multiple dimensions
result = project.query("revenue, order_count BY region, month")
print(result.df)

# With filters
result = project.query(
    "revenue BY product_category",
    filters=["region = 'Southeast'"]
)
```

### SQL Generation

The semantic layer generates SQL:

```python
# Get generated SQL
sql = project.query("revenue BY region", return_sql=True)
print(sql)
```

Output:

```sql
SELECT 
  dc.customer_region as region,
  SUM(f.line_total) as revenue
FROM gold.fact_order_items f
JOIN gold.dim_customer dc ON f.customer_sk = dc.customer_sk
WHERE f.order_status = 'completed'
GROUP BY dc.customer_region
```

---

## Metric Types

### Simple Metrics

Direct aggregation:

```yaml
- name: revenue
  expr: "SUM(line_total)"
  source: gold.fact_order_items
```

### Filtered Metrics

With WHERE clause:

```yaml
- name: completed_orders
  expr: "COUNT(*)"
  source: gold.fact_orders
  filters:
    - "status = 'completed'"
```

### Derived Metrics

Calculations from other metrics:

```yaml
- name: avg_order_value
  expr: "revenue / order_count"
  type: derived

- name: profit_margin
  expr: "(revenue - cost) / revenue * 100"
  type: derived
```

### Time-Relative Metrics

Relative to query time:

```yaml
- name: mtd_revenue
  description: "Month-to-date revenue"
  expr: "SUM(line_total)"
  filters:
    - "order_date >= DATE_TRUNC('month', CURRENT_DATE)"
    - "order_date <= CURRENT_DATE"

- name: yoy_growth
  description: "Year-over-year growth %"
  expr: |
    (SUM(CASE WHEN order_year = YEAR(CURRENT_DATE) THEN line_total END) - 
     SUM(CASE WHEN order_year = YEAR(CURRENT_DATE) - 1 THEN line_total END)) /
    SUM(CASE WHEN order_year = YEAR(CURRENT_DATE) - 1 THEN line_total END) * 100
  type: derived
```

---

## Dimension Hierarchies

Enable drill-down:

```yaml
dimensions:
  - name: date
    source: gold.dim_date
    column: full_date
    hierarchy:
      - year
      - quarter
      - month
      - week
      - day
```

Query at different levels:

```python
# By year
project.query("revenue BY date.year")

# By month
project.query("revenue BY date.month")

# Drill down
project.query("revenue BY date.year, date.month")
```

---

## Materializations

### Why Materialize?

Semantic queries can be expensive:
- Multiple joins
- Large aggregations
- Complex filters

Pre-compute common queries for speed.

### Configuration

```yaml
materializations:
  - name: monthly_summary
    metrics: [revenue, order_count, avg_order_value]
    dimensions: [region, product_category, month]
    output: gold.agg_monthly_summary
    schedule: "0 6 1 * *"  # 1st of month, 6 AM
    
    # Incremental refresh
    incremental:
      timestamp_column: order_date
      lookback: "7 days"
```

### Automatic Materialization Routing

When a query matches a materialization, it uses the pre-computed table:

```python
# This hits agg_monthly_summary, not fact tables
result = project.query("revenue BY region, month")
```

---

## Governance

### Metric Ownership

```yaml
metrics:
  - name: revenue
    owner: finance-team@company.com
    certified: true
    certification_date: 2023-12-01
```

### Documentation

```yaml
metrics:
  - name: revenue
    description: |
      Total revenue from completed orders.
      
      Calculation:
      - Includes: All order line items with status = 'completed'
      - Excludes: Refunded orders, canceled orders
      - Currency: USD (all currencies converted at order date rate)
      
      Owner: Finance Team
      Last reviewed: 2023-12-01
```

### Lineage

Track where metrics come from:

```python
# Show metric lineage
lineage = project.metric_lineage("revenue")
print(lineage)
# revenue
#   └── fact_order_items.line_total
#       └── silver.order_items.price + freight_value
#           └── bronze.order_items
```

---

## BI Tool Integration

### Power BI / Tableau

Generate a view layer for BI tools:

```yaml
materializations:
  - name: bi_revenue_report
    metrics: [revenue, order_count, avg_order_value]
    dimensions: [region, product_category, date.month]
    output: gold.vw_bi_revenue  # View for BI tool
    format: view
```

### SQL Interface

Direct SQL access to semantic layer:

```sql
-- BI tool connects via SQL
SELECT * FROM gold.vw_bi_revenue
WHERE month >= '2023-01'
```

---

## Complete Example

```yaml
# odibi.yaml

project: "ecommerce_analytics"
engine: "spark"

connections:
  gold:
    type: delta
    catalog: spark_catalog
    schema_name: gold

semantic:
  # Core business metrics
  metrics:
    - name: revenue
      description: "Net revenue from completed orders"
      expr: "SUM(line_total)"
      source: gold.fact_order_items
      filters:
        - "order_status = 'completed'"
      owner: finance@company.com
      certified: true
    
    - name: orders
      description: "Count of unique orders"
      expr: "COUNT(DISTINCT order_id)"
      source: gold.fact_orders
      filters:
        - "order_status != 'canceled'"
    
    - name: customers
      description: "Count of unique customers"
      expr: "COUNT(DISTINCT customer_id)"
      source: gold.fact_orders
    
    - name: aov
      description: "Average Order Value"
      expr: "revenue / orders"
      type: derived
    
    - name: late_delivery_rate
      description: "Percentage of late deliveries"
      expr: "SUM(is_late) * 100.0 / COUNT(*)"
      source: gold.fact_orders
  
  # Dimensions for slicing
  dimensions:
    - name: region
      source: gold.dim_customer
      column: customer_region
    
    - name: category
      source: gold.dim_product
      column: product_category_name
    
    - name: seller_state
      source: gold.dim_seller
      column: seller_state
    
    - name: date
      source: gold.dim_date
      column: full_date
      hierarchy: [year, quarter, month, week, day]
  
  # Pre-computed aggregates
  materializations:
    - name: daily_kpis
      metrics: [revenue, orders, customers, aov]
      dimensions: [date.day, region]
      output: gold.agg_daily_kpis
      schedule: "0 5 * * *"
    
    - name: category_performance
      metrics: [revenue, orders]
      dimensions: [category, region, date.month]
      output: gold.agg_category_monthly
      schedule: "0 6 * * *"
```

---

## Key Takeaways

| Concept | Purpose |
|---------|---------|
| **Metrics** | Centralized aggregation definitions |
| **Dimensions** | Consistent grouping attributes |
| **Materializations** | Pre-computed for performance |
| **Governance** | Ownership, certification, documentation |

### The Semantic Layer Mantra

**Define once. Trust always. Query anywhere.**

---

## Next Steps

With analytics in place, let's cover production operations:

- Monitoring
- Alerting
- Retry strategies

Next article: **Production Data Pipelines: Monitoring, Retry, and Testing**.

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]
- **Medium:** Follow for more articles in this series
