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
    merge_strategy: replace       # "replace", "sum", "min", or "max" (default: "replace")
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

The `AggregationPattern` supports four incremental merge strategies that control how new aggregates are combined with existing data. Each strategy serves different use cases based on your data characteristics and business requirements.

### 1. Replace Strategy

**Behavior:** New aggregates completely overwrite existing rows for matching grain keys. Rows with grain keys that only exist in the target are preserved unchanged.

**When to use:**
- Full recalculation of affected grains (idempotent, handles late-arriving data)
- You want to replace outdated aggregates with fresh calculations
- Default strategy when you need simplicity and correctness

**Example:**

```yaml
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
        incremental:
          timestamp_column: order_date
          merge_strategy: replace  # Default strategy
        target: warehouse.agg_daily_sales
    write:
      connection: warehouse
      path: agg_daily_sales
      format: delta
      mode: overwrite
```

**What happens:**

| Existing (Target) | New Aggregate | Result |
|-------------------|---------------|---------|
| date=2024-01-01, product=A, revenue=100 | date=2024-01-01, product=A, revenue=150 | date=2024-01-01, product=A, revenue=**150** (replaced) |
| date=2024-01-02, product=B, revenue=200 | *(not in new)* | date=2024-01-02, product=B, revenue=200 (unchanged) |
| *(not in existing)* | date=2024-01-03, product=C, revenue=300 | date=2024-01-03, product=C, revenue=300 (inserted) |

---

### 2. Sum Strategy

**Behavior:** Adds new measure values to existing measures for matching grain keys. Performs a FULL OUTER JOIN and sums matching rows.

**When to use:**
- Purely additive metrics (counts, sums) where data is append-only
- You want to accumulate values over time without recalculating from source
- Performance optimization when you can add deltas rather than recompute

**⚠️ Warning:** Do NOT use for AVG, DISTINCT counts, ratios, or any non-additive metric. Results will be incorrect.

**Example:**

```yaml
nodes:
  - name: agg_daily_sales
    read:
      connection: warehouse
      path: fact_orders_incremental  # Only new/updated orders
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
        incremental:
          timestamp_column: order_date
          merge_strategy: sum  # Accumulate values
        target: warehouse.agg_daily_sales
    write:
      connection: warehouse
      path: agg_daily_sales
      format: delta
      mode: overwrite
```

**What happens:**

| Existing (Target) | New Aggregate | Result |
|-------------------|---------------|---------|
| date=2024-01-01, product=A, revenue=100, count=5 | date=2024-01-01, product=A, revenue=50, count=2 | date=2024-01-01, product=A, revenue=**150**, count=**7** (summed) |
| date=2024-01-02, product=B, revenue=200, count=10 | *(not in new)* | date=2024-01-02, product=B, revenue=200, count=10 (unchanged) |
| *(not in existing)* | date=2024-01-03, product=C, revenue=300, count=15 | date=2024-01-03, product=C, revenue=300, count=15 (inserted) |

---

### 3. Min Strategy

**Behavior:** Keeps the minimum value for each measure across existing and new rows. Performs a FULL OUTER JOIN and computes MIN for each measure.

**When to use:**
- Tracking minimum values over time (lowest price, shortest duration, earliest timestamp)
- You want to preserve historical minimums even when new data arrives
- Monitoring metrics where you care about the floor value

**Example:**

```yaml
nodes:
  - name: agg_product_min_price
    read:
      connection: warehouse
      path: fact_orders
      format: delta
    pattern:
      type: aggregation
      params:
        grain: [date_sk, product_sk]
        measures:
          - name: min_unit_price
            expr: "MIN(unit_price)"
          - name: min_discount_pct
            expr: "MIN(discount_pct)"
        incremental:
          timestamp_column: order_date
          merge_strategy: min  # Keep minimum values
        target: warehouse.agg_product_min_price
    write:
      connection: warehouse
      path: agg_product_min_price
      format: delta
      mode: overwrite
```

**What happens:**

| Existing (Target) | New Aggregate | Result |
|-------------------|---------------|---------|
| date=2024-01-01, product=A, min_price=10.00 | date=2024-01-01, product=A, min_price=8.50 | date=2024-01-01, product=A, min_price=**8.50** (lower value kept) |
| date=2024-01-02, product=B, min_price=5.00 | date=2024-01-02, product=B, min_price=7.00 | date=2024-01-02, product=B, min_price=**5.00** (existing lower) |
| *(not in existing)* | date=2024-01-03, product=C, min_price=12.00 | date=2024-01-03, product=C, min_price=12.00 (inserted) |

---

### 4. Max Strategy

**Behavior:** Keeps the maximum value for each measure across existing and new rows. Performs a FULL OUTER JOIN and computes MAX for each measure.

**When to use:**
- Tracking maximum values over time (highest price, longest duration, latest timestamp)
- You want to preserve historical maximums even when new data arrives
- Monitoring metrics where you care about the ceiling value

**Example:**

```yaml
nodes:
  - name: agg_product_max_price
    read:
      connection: warehouse
      path: fact_orders
      format: delta
    pattern:
      type: aggregation
      params:
        grain: [date_sk, product_sk]
        measures:
          - name: max_unit_price
            expr: "MAX(unit_price)"
          - name: max_discount_pct
            expr: "MAX(discount_pct)"
        incremental:
          timestamp_column: order_date
          merge_strategy: max  # Keep maximum values
        target: warehouse.agg_product_max_price
    write:
      connection: warehouse
      path: agg_product_max_price
      format: delta
      mode: overwrite
```

**What happens:**

| Existing (Target) | New Aggregate | Result |
|-------------------|---------------|---------|
| date=2024-01-01, product=A, max_price=20.00 | date=2024-01-01, product=A, max_price=25.00 | date=2024-01-01, product=A, max_price=**25.00** (higher value kept) |
| date=2024-01-02, product=B, max_price=30.00 | date=2024-01-02, product=B, max_price=28.00 | date=2024-01-02, product=B, max_price=**30.00** (existing higher) |
| *(not in existing)* | date=2024-01-03, product=C, max_price=35.00 | date=2024-01-03, product=C, max_price=35.00 (inserted) |

---

### Strategy Comparison

| Strategy | Use Case | Pros | Cons |
|----------|----------|------|------|
| **replace** | Full recalculation, late data | Accurate, idempotent, simple | Requires reading source data for affected grains |
| **sum** | Append-only additive metrics | Fast, incremental processing | Only works for SUM/COUNT, not AVG/DISTINCT |
| **min** | Tracking minimum values | Preserves historical lows | Not applicable to most business metrics |
| **max** | Tracking maximum values | Preserves historical highs | Not applicable to most business metrics |

**Recommendation:** Use `replace` (default) unless you have a specific reason to use another strategy.

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

## Data Quality and Quarantine

Aggregation patterns do not directly support quarantine for failed aggregations. However, you can use standard [validation tests](../validation/tests.md) on the source data with `on_fail: quarantine` to route invalid records to quarantine tables before aggregation.

For a complete guide to quarantine functionality, see the [Quarantine Feature Guide](../features/quarantine.md).

---

## See Also

- [Fact Pattern](./fact.md) - Build fact tables
- [Semantic Layer](../semantics/index.md) - Define metrics for ad-hoc queries
- [Materializing Metrics](../semantics/materialize.md) - Schedule metric materialization
- [Quarantine Feature](../features/quarantine.md) - Route invalid data to quarantine tables
- [YAML Schema Reference](../reference/yaml_schema.md) - Full configuration reference
