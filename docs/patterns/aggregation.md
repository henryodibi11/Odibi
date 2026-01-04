# Aggregation Pattern

The `aggregation` pattern provides declarative aggregation with configurable grain, measures, and incremental merge strategies.

## Integration with Odibi YAML

The aggregation pattern works on data from the read block or a dependency, applies GROUP BY aggregation, and writes the results.

```yaml
project: sales_analytics
engine: spark

connections:
  warehouse:
    type: delta
    path: /mnt/warehouse

story:
  connection: warehouse
  path: stories

pipelines:
  - pipeline: build_aggregates
    nodes:
      - name: agg_daily_sales
        read:
          connection: warehouse
          path: fact_orders
          format: delta
        
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
          connection: warehouse
          path: agg_daily_product_sales
          format: delta
          mode: overwrite
```

---

## Features

- **Declarative grain** (GROUP BY columns)
- **Flexible measure expressions** (SUM, COUNT, AVG, etc.)
- **Incremental aggregation** (merge new data with existing)
- **HAVING clause support**
- **Audit columns**

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
params:
  measures:
    - name: total_revenue      # Output column name
      expr: "SUM(line_total)"  # SQL aggregation expression
```

### Incremental Config

```yaml
params:
  incremental:
    timestamp_column: order_date  # Column to identify new data
    merge_strategy: replace       # "replace" or "sum"
  target: warehouse.agg_daily_sales
```

---

## Measure Expressions

Use standard SQL aggregation functions:

```yaml
params:
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
    
    # Complex expressions
    - name: total_with_discount
      expr: "SUM(line_total - discount_amount)"
    
    - name: discount_rate
      expr: "SUM(discount_amount) / SUM(line_total)"
```

---

## Incremental Merge Strategies

### Replace Strategy

New aggregates overwrite existing for matching grain keys:

```yaml
nodes:
  - name: agg_daily_sales
    read:
      connection: warehouse
      path: fact_orders
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
        target: warehouse.agg_daily_sales
    write:
      connection: warehouse
      path: agg_daily_sales
      mode: overwrite
```

**Use case:** Full recalculation of affected grains (idempotent, handles late data)

### Sum Strategy

Add new measure values to existing aggregates:

```yaml
params:
  incremental:
    timestamp_column: order_date
    merge_strategy: sum
```

**Use case:** Additive metrics only (counts, sums) where data is append-only

**Warning:** Don't use for AVG, DISTINCT counts, or ratios.

---

## Time Rollups

Build aggregate hierarchies at multiple time grains:

```yaml
pipelines:
  - pipeline: build_aggregates
    nodes:
      # Daily aggregate (from fact)
      - name: agg_daily_sales
        read:
          connection: warehouse
          path: fact_orders
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
          connection: warehouse
          path: agg_daily_sales
          mode: overwrite

      # Monthly rollup (from daily aggregate)
      - name: agg_monthly_sales
        depends_on: [agg_daily_sales]
        transform:
          steps:
            - sql: |
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
          connection: warehouse
          path: agg_monthly_sales
          mode: overwrite
```

---

## Full YAML Example

Complete aggregation pipeline with incremental refresh:

```yaml
project: sales_analytics
engine: spark

connections:
  warehouse:
    type: delta
    path: /mnt/warehouse

story:
  connection: warehouse
  path: stories

system:
  connection: warehouse
  path: _system_catalog

pipelines:
  - pipeline: build_aggregates
    description: "Build daily and monthly sales aggregates"
    nodes:
      - name: agg_daily_product_sales
        description: "Daily product sales aggregate"
        read:
          connection: warehouse
          path: fact_orders
          format: delta
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
          format: delta
          mode: overwrite
```

---

## See Also

- [Fact Pattern](./fact.md) - Build fact tables
- [Semantic Layer](../semantics/index.md) - Define metrics for ad-hoc queries
- [Materializing Metrics](../semantics/materialize.md) - Schedule metric materialization
- [YAML Schema Reference](../reference/yaml_schema.md) - Full configuration reference
