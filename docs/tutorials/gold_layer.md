# Gold Layer Tutorial

The **Gold Layer** is where business-ready datasets live. Fact tables, aggregations, and semantic metrics—optimized for consumption.

## Layer Philosophy

> "Answers, not data."

Gold is **consumption-optimized**. BI tools, dashboards, and ML models read from Gold. Queries should be fast and intuitive.

| Principle | Why |
|-----------|-----|
| Denormalized | Fewer joins = faster queries |
| Pre-aggregated | Common rollups pre-computed |
| Business-named | Column names match business terms |
| SK-based | Surrogate keys for dimension lookups |

---

## Quick Start: Fact Table

The most common Gold pattern is a fact table with dimension lookups:

```yaml
# pipelines/gold/fact_orders.yaml
pipelines:
  - pipeline: gold_fact_orders
    layer: gold
    nodes:
      - name: fact_orders
        read:
          connection: silver
          table: orders
        pattern:
          type: fact
          params:
            grain: [order_id, line_item_id]
            dimensions:
              - name: dim_customer
                lookup_key: customer_id
                surrogate_key: customer_sk
                target: silver.dim_customer
              - name: dim_product
                lookup_key: product_id
                surrogate_key: product_sk
                target: silver.dim_product
              - name: dim_date
                lookup_key: order_date
                surrogate_key: date_sk
                target: gold.dim_date
            orphan_handling: unknown
        write:
          connection: gold
          table: fact_orders
```

---

## Common Problems & Solutions

### 1. "How do I build a star schema fact table?"

**Problem:** Need to replace natural keys with surrogate keys from dimensions.

**Solution:** Use the fact pattern with dimension lookups.

```yaml
nodes:
  - name: fact_orders
    read:
      connection: silver
      table: orders
    pattern:
      type: fact
      params:
        grain: [order_id]            # One row per order
        dimensions:
          - name: dim_customer
            lookup_key: customer_id
            surrogate_key: customer_sk
            target: silver.dim_customer
          - name: dim_product
            lookup_key: product_id
            surrogate_key: product_sk
            target: silver.dim_product
    write:
      connection: gold
      table: fact_orders
```

**Result:**
```
order_id | customer_sk | product_sk | order_total | order_date
1        | 42          | 15         | 150.00      | 2025-01-15
2        | 42          | 23         | 75.00       | 2025-01-16
```

**See:** [Fact Pattern](../patterns/fact.md)

---

### 2. "Orders reference customers that don't exist (orphans)"

**Problem:** Some orders have customer_id values not in dim_customer.

**Solution:** Configure orphan handling.

```yaml
pattern:
  type: fact
  params:
    grain: [order_id]
    dimensions:
      - name: dim_customer
        lookup_key: customer_id
        surrogate_key: customer_sk
        target: silver.dim_customer
    orphan_handling: unknown         # Assign to unknown member
```

**Options for `orphan_handling`:**
| Option | Behavior |
|--------|----------|
| `unknown` | Assign SK = -1 (unknown member) |
| `quarantine` | Route to quarantine table |
| `error` | Fail the pipeline |
| `null` | Set SK = NULL |

**See:** [Fact Pattern - Orphan Handling](../patterns/fact.md)

---

### 3. "I need a date dimension"

**Problem:** Need a standard date dimension for time-based analysis.

**Solution:** Use the date dimension pattern.

```yaml
nodes:
  - name: dim_date
    pattern:
      type: date_dimension
      params:
        start_date: "2020-01-01"
        end_date: "2030-12-31"
        columns:
          - date_sk               # Surrogate key (YYYYMMDD)
          - full_date             # DATE type
          - day_of_week           # Monday, Tuesday, ...
          - day_of_month          # 1-31
          - month_name            # January, February, ...
          - month_number          # 1-12
          - quarter               # Q1, Q2, Q3, Q4
          - year                  # 2024, 2025, ...
          - is_weekend            # true/false
          - fiscal_year           # Custom fiscal calendar
    write:
      connection: gold
      table: dim_date
```

**See:** [Date Dimension Pattern](../patterns/date_dimension.md)

---

### 4. "I need pre-aggregated metrics"

**Problem:** Dashboards are slow—need pre-computed rollups.

**Solution:** Use the aggregation pattern.

```yaml
nodes:
  - name: daily_sales
    read:
      connection: gold
      table: fact_orders
    pattern:
      type: aggregation
      params:
        dimensions: [date_sk, product_sk]
        measures:
          - name: total_revenue
            expression: "SUM(order_total)"
          - name: order_count
            expression: "COUNT(*)"
          - name: avg_order_value
            expression: "AVG(order_total)"
        incremental: true            # Merge new days
    write:
      connection: gold
      table: agg_daily_sales
```

**Result:**
```
date_sk  | product_sk | total_revenue | order_count | avg_order_value
20250115 | 15         | 1500.00       | 10          | 150.00
20250115 | 23         | 750.00        | 10          | 75.00
```

**See:** [Aggregation Pattern](../patterns/aggregation.md)

---

### 5. "I want to define reusable metrics for BI"

**Problem:** Different dashboards calculate "revenue" differently.

**Solution:** Define semantic metrics.

```yaml
semantic:
  metrics:
    - name: revenue
      expression: "SUM(order_total)"
      description: "Total order revenue"
      format: currency
    
    - name: order_count
      expression: "COUNT(DISTINCT order_id)"
      description: "Number of unique orders"
      format: integer
    
    - name: aov
      expression: "SUM(order_total) / COUNT(DISTINCT order_id)"
      description: "Average order value"
      format: currency
      depends_on: [revenue, order_count]

  dimensions:
    - name: customer_name
      column: dim_customer.name
    
    - name: product_category
      column: dim_product.category
    
    - name: order_month
      column: dim_date.month_name
```

**See:** [Semantic Layer](../semantics/index.md), [Defining Metrics](../semantics/metrics.md)

---

### 6. "How do I materialize semantic metrics to tables?"

**Problem:** Want to query metrics from SQL, not just the API.

**Solution:** Materialize metrics to Gold tables.

```yaml
nodes:
  - name: materialized_revenue
    semantic:
      materialize:
        metrics: [revenue, order_count, aov]
        dimensions: [product_category, order_month]
        target: gold.revenue_by_category_month
```

**See:** [Materializing Metrics](../semantics/materialize.md)

---

### 7. "Reference data rarely changes—skip if unchanged"

**Problem:** Date dimension regenerates every run unnecessarily.

**Solution:** Skip if content hash is unchanged.

```yaml
nodes:
  - name: dim_date
    pattern:
      type: date_dimension
      params:
        start_date: "2020-01-01"
        end_date: "2030-12-31"
    write:
      connection: gold
      table: dim_date
      format: delta
      skip_if_unchanged: true        # Skip if content hash matches
```

**How it works:**
- Before writing, Odibi computes a SHA256 hash of the DataFrame
- Compares to hash stored in Delta table metadata
- Skips write if hashes match (saves storage and compute)

**See:** [Skip If Unchanged Pattern](../patterns/skip_if_unchanged.md)

---

### 8. "How do I validate fact table grain?"

**Problem:** Want to ensure no duplicate rows per grain key.

**Solution:** Add grain validation contract.

```yaml
nodes:
  - name: fact_orders
    read:
      connection: silver
      table: orders
    contracts:
      - type: unique
        columns: [order_id, line_item_id]  # Grain columns
        severity: error
    pattern:
      type: fact
      params:
        grain: [order_id, line_item_id]
    write:
      connection: gold
      table: fact_orders
```

---

## Gold Layer Checklist

Before exposing to BI:

- [ ] **Star schema?** Facts reference dimensions via surrogate keys
- [ ] **Grain validated?** No duplicate rows per grain key
- [ ] **Orphans handled?** Missing dimension members → unknown or quarantine
- [ ] **Pre-aggregated?** Common rollups materialized
- [ ] **Documented?** Semantic layer defines metrics and dimensions

---

## Star Schema Example

```
                   ┌─────────────────┐
                   │   dim_date      │
                   │  date_sk (PK)   │
                   │  full_date      │
                   │  month_name     │
                   │  year           │
                   └────────▲────────┘
                            │
┌─────────────────┐    ┌────┴────────────┐    ┌─────────────────┐
│  dim_customer   │    │   fact_orders    │    │  dim_product    │
│ customer_sk(PK) │◄───│ customer_sk(FK)  │───►│ product_sk (PK) │
│ customer_id     │    │ product_sk (FK)  │    │ product_id      │
│ name            │    │ date_sk (FK)     │    │ name            │
│ city            │    │ order_id         │    │ category        │
│ state           │    │ order_total      │    │ price           │
└─────────────────┘    │ quantity         │    └─────────────────┘
                       └──────────────────┘
```

---

## Next Steps

- [Fact Pattern](../patterns/fact.md) — Detailed fact table configuration
- [Aggregation Pattern](../patterns/aggregation.md) — Pre-computed rollups
- [Semantic Layer Overview](../semantics/index.md) — Reusable metrics
- [Dimensional Modeling Tutorial](dimensional_modeling/01_introduction.md) — Full walkthrough
