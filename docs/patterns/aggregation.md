# Aggregation Pattern

The `AggregationPattern` provides declarative aggregation with configurable grain, measures, and incremental merge strategies.

## Features

- **Declarative grain** (GROUP BY columns)
- **Flexible measure expressions** (SUM, COUNT, AVG, etc.)
- **Incremental aggregation** (merge new data with existing)
- **HAVING clause support**
- **Audit columns**

## Quick Start

```yaml
nodes:
  - name: agg_daily_sales
    read:
      source: gold.fact_orders
    pattern:
      type: aggregation
      params:
        grain: [date_sk, product_sk]
        measures:
          - name: total_revenue
            expr: "SUM(line_total)"
          - name: order_count
            expr: "COUNT(*)"
          - name: avg_order_value
            expr: "AVG(line_total)"
        having: "COUNT(*) > 0"
        audit:
          load_timestamp: true
    write:
      target: gold.agg_daily_product_sales
```

---

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `grain` | list | Yes | - | Columns to GROUP BY (defines uniqueness) |
| `measures` | list | Yes | - | Measure definitions with name and expr |
| `having` | str | No | - | Optional HAVING clause |
| `incremental` | dict | No | - | Incremental merge configuration |
| `target` | str | For incremental | - | Target table for incremental merge |
| `audit` | dict | No | {} | Audit column configuration |

### Measure Definition

```yaml
measures:
  - name: total_revenue      # Output column name
    expr: "SUM(line_total)"  # SQL aggregation expression
```

### Incremental Config

```yaml
incremental:
  timestamp_column: order_date  # Column to identify new data
  merge_strategy: replace       # "replace" or "sum"
```

### Audit Config

```yaml
audit:
  load_timestamp: true    # Add load_timestamp column
  source_system: "etl"    # Add source_system column
```

---

## Measure Expressions

Use standard SQL aggregation functions:

```yaml
measures:
  # Basic aggregations
  - name: total_revenue
    expr: "SUM(line_total)"
  
  - name: order_count
    expr: "COUNT(*)"
  
  - name: unique_customers
    expr: "COUNT(DISTINCT customer_sk)"
  
  - name: avg_order_value
    expr: "AVG(line_total)"
  
  - name: max_order
    expr: "MAX(line_total)"
  
  - name: min_order
    expr: "MIN(line_total)"
  
  # Complex expressions
  - name: total_with_discount
    expr: "SUM(line_total - discount_amount)"
  
  - name: discount_rate
    expr: "SUM(discount_amount) / SUM(line_total)"
```

---

## Grain Configuration

The grain defines the GROUP BY columns and the uniqueness of the output:

### Daily by Product
```yaml
grain: [date_sk, product_sk]
```

### Monthly by Region and Category
```yaml
grain: [year, month, region, category]
```

### Customer Lifetime
```yaml
grain: [customer_sk]
```

---

## Incremental Merge Strategies

### Replace Strategy

New aggregates overwrite existing for matching grain keys:

```yaml
pattern:
  type: aggregation
  params:
    grain: [date_sk, product_sk]
    measures:
      - name: total_revenue
        expr: "SUM(line_total)"
    incremental:
      timestamp_column: order_date
      merge_strategy: replace
    target: gold.agg_daily_sales
```

**Behavior:**
1. Aggregate new source data
2. Find existing rows matching grain keys
3. Remove matched existing rows
4. Union remaining existing + new aggregates

**Use case:** Full recalculation of affected grains (idempotent, handles late data)

### Sum Strategy

Add new measure values to existing aggregates:

```yaml
pattern:
  type: aggregation
  params:
    grain: [date_sk, product_sk]
    measures:
      - name: total_revenue
        expr: "SUM(line_total)"
      - name: order_count
        expr: "COUNT(*)"
    incremental:
      timestamp_column: order_date
      merge_strategy: sum
    target: gold.agg_daily_sales
```

**Behavior:**
1. Aggregate new source data
2. Full outer join with existing on grain
3. Add measure values together
4. Coalesce grain columns

**Use case:** Additive metrics only (faster, but doesn't handle updates)

**Warning:** Sum strategy only works for purely additive measures. Don't use with AVG, DISTINCT counts, or ratios.

---

## Time Rollups

Build aggregate hierarchies at multiple time grains:

```yaml
nodes:
  # Daily aggregate
  - name: agg_daily_sales
    read:
      source: gold.fact_orders
    pattern:
      type: aggregation
      params:
        grain: [date_sk, product_sk]
        measures:
          - name: total_revenue
            expr: "SUM(line_total)"
          - name: order_count
            expr: "COUNT(*)"
    write:
      target: gold.agg_daily_sales

  # Weekly rollup (from daily)
  - name: agg_weekly_sales
    depends_on: [agg_daily_sales]
    read:
      source: gold.agg_daily_sales
    transform:
      steps:
        - function: sql
          params:
            query: |
              SELECT 
                FLOOR(date_sk / 7) * 7 AS week_start_sk,
                product_sk
              FROM df
    pattern:
      type: aggregation
      params:
        grain: [week_start_sk, product_sk]
        measures:
          - name: total_revenue
            expr: "SUM(total_revenue)"
          - name: order_count
            expr: "SUM(order_count)"
    write:
      target: gold.agg_weekly_sales

  # Monthly rollup (from daily)
  - name: agg_monthly_sales
    depends_on: [agg_daily_sales]
    read:
      source: gold.agg_daily_sales
    transform:
      steps:
        - function: sql
          params:
            query: |
              SELECT 
                FLOOR(date_sk / 100) AS month_sk,
                product_sk,
                total_revenue,
                order_count
              FROM df
    pattern:
      type: aggregation
      params:
        grain: [month_sk, product_sk]
        measures:
          - name: total_revenue
            expr: "SUM(total_revenue)"
          - name: order_count
            expr: "SUM(order_count)"
    write:
      target: gold.agg_monthly_sales
```

---

## Full YAML Example

Complete aggregation pipeline with incremental refresh:

```yaml
project:
  name: sales_aggregations

connections:
  warehouse:
    type: delta
    path: /mnt/warehouse

nodes:
  - name: agg_daily_product_sales
    description: "Daily product sales aggregate with incremental refresh"
    read:
      connection: warehouse
      path: fact_orders
    pattern:
      type: aggregation
      params:
        grain:
          - date_sk
          - product_sk
          - region
        measures:
          - name: total_revenue
            expr: "SUM(line_total)"
          - name: total_cost
            expr: "SUM(cost_amount)"
          - name: order_count
            expr: "COUNT(*)"
          - name: units_sold
            expr: "SUM(quantity)"
          - name: unique_customers
            expr: "COUNT(DISTINCT customer_sk)"
          - name: avg_unit_price
            expr: "AVG(unit_price)"
        having: "SUM(line_total) > 0"
        incremental:
          timestamp_column: load_timestamp
          merge_strategy: replace
        target: warehouse.agg_daily_product_sales
        audit:
          load_timestamp: true
          source_system: "aggregation_pipeline"
    write:
      connection: warehouse
      path: agg_daily_product_sales
      mode: overwrite
```

---

## Python API

```python
from odibi.patterns.aggregation import AggregationPattern
from odibi.context import EngineContext
from odibi.enums import EngineType

# Create pattern instance
pattern = AggregationPattern(params={
    "grain": ["date_sk", "product_sk"],
    "measures": [
        {"name": "total_revenue", "expr": "SUM(line_total)"},
        {"name": "order_count", "expr": "COUNT(*)"},
        {"name": "avg_order_value", "expr": "AVG(line_total)"}
    ],
    "having": "COUNT(*) > 0",
    "audit": {
        "load_timestamp": True
    }
})

# Validate configuration
pattern.validate()

# Execute pattern
context = EngineContext(df=fact_df, engine_type=EngineType.SPARK)
result_df = pattern.execute(context)

# Incremental execution
pattern_incremental = AggregationPattern(params={
    "grain": ["date_sk", "product_sk"],
    "measures": [
        {"name": "total_revenue", "expr": "SUM(line_total)"},
        {"name": "order_count", "expr": "COUNT(*)"}
    ],
    "incremental": {
        "timestamp_column": "load_timestamp",
        "merge_strategy": "replace"
    },
    "target": "gold.agg_daily_sales"
})

# Context with new incremental data
incremental_context = EngineContext(df=new_data_df, engine_type=EngineType.SPARK)
merged_df = pattern_incremental.execute(incremental_context)
```

---

## Performance Tips

1. **Pre-aggregate at fine grain first**: Build daily aggregates, then roll up to weekly/monthly
2. **Use replace strategy for late-arriving data**: Ensures correctness
3. **Partition output by time**: For faster incremental queries
4. **Filter source before aggregation**: Apply time windows in read step

---

## See Also

- [Fact Pattern](./fact.md) - Build fact tables
- [Semantic Layer](../semantics/index.md) - Define metrics for ad-hoc queries
- [Materialization](../semantics/materialize.md) - Schedule metric materialization
