# Defining Metrics

This guide covers how to define metrics and dimensions in the Odibi semantic layer.

> **Source Notation:** The `source` field supports three formats: `$pipeline.node` (recommended), `connection.path`, or bare table names. See [Source Notation](../tutorials/dimensional_modeling/08_defining_metrics.md#source-notation) for details.

## MetricDefinition

A metric represents a measurable value that can be aggregated across dimensions.

### Schema

```yaml
metrics:
  - name: revenue              # Required: unique identifier
    label: "Total Revenue"     # Optional: display name for column alias
    description: "..."         # Optional: human-readable description
    expr: "SUM(total_amount)"  # Required: SQL aggregation expression
    source: fact_orders        # Required for simple metrics: source table
    filters:                   # Optional: WHERE conditions
      - "status = 'completed'"
    type: simple               # Optional: "simple" or "derived"
```

### Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | str | Yes | - | Unique metric identifier (lowercase, alphanumeric + underscore) |
| `label` | str | No | name | Display name used as column alias in generated views |
| `description` | str | No | - | Human-readable description |
| `expr` | str | Yes | - | SQL aggregation expression |
| `source` | str | For simple | - | Source table name |
| `filters` | list | No | [] | WHERE conditions to apply |
| `type` | str | No | "simple" | "simple" (direct) or "derived" (references other metrics) |

---

## Simple Metrics

Simple metrics aggregate directly from source data:

```yaml
metrics:
  # Count
  - name: order_count
    expr: "COUNT(*)"
    source: fact_orders

  # Sum
  - name: revenue
    expr: "SUM(total_amount)"
    source: fact_orders

  # Average
  - name: avg_order_value
    expr: "AVG(total_amount)"
    source: fact_orders

  # Distinct count
  - name: unique_customers
    expr: "COUNT(DISTINCT customer_sk)"
    source: fact_orders

  # Min/Max
  - name: max_order
    expr: "MAX(total_amount)"
    source: fact_orders

  # Complex expression
  - name: total_margin
    expr: "SUM(revenue - cost)"
    source: fact_orders
```

---

## Filtered Metrics

Apply filters to constrain the aggregation:

```yaml
metrics:
  # Only completed orders
  - name: completed_revenue
    expr: "SUM(total_amount)"
    source: fact_orders
    filters:
      - "status = 'completed'"

  # Multiple filters (AND)
  - name: domestic_completed_revenue
    expr: "SUM(total_amount)"
    source: fact_orders
    filters:
      - "status = 'completed'"
      - "country = 'USA'"

  # Time-filtered
  - name: last_30_days_orders
    expr: "COUNT(*)"
    source: fact_orders
    filters:
      - "order_date >= CURRENT_DATE - INTERVAL 30 DAY"
```

---

## Derived Metrics

Derived metrics reference other metrics (future enhancement):

```yaml
metrics:
  - name: revenue
    expr: "SUM(total_amount)"
    source: fact_orders

  - name: order_count
    expr: "COUNT(*)"
    source: fact_orders

  # Derived: revenue / order_count
  - name: avg_order_value_derived
    expr: "revenue / order_count"
    type: derived
```

---

## DimensionDefinition

A dimension represents an attribute for grouping and filtering metrics.

### Schema

```yaml
dimensions:
  - name: region               # Required: unique identifier
    label: "Sales Region"      # Optional: display name for column alias
    source: fact_orders        # Required: source table
    column: region             # Optional: column name (defaults to name)
    hierarchy:                 # Optional: drill-down hierarchy
      - year
      - quarter
      - month
    description: "..."         # Optional: human-readable description
```

### Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | str | Yes | - | Unique dimension identifier |
| `label` | str | No | name | Display name used as column alias in generated views |
| `source` | str | Yes | - | Source table name |
| `column` | str | No | name | Column name in source |
| `hierarchy` | list | No | [] | Ordered drill-down columns |
| `description` | str | No | - | Human-readable description |

---

## Dimension Examples

```yaml
dimensions:
  # Simple dimension
  - name: region
    source: fact_orders
    column: region

  # Dimension with different column name
  - name: customer_region
    source: dim_customer
    column: billing_region

  # Date dimension with hierarchy
  - name: order_date
    source: dim_date
    column: full_date
    hierarchy:
      - year
      - quarter
      - month
      - week_of_year
      - full_date

  # Product category hierarchy
  - name: product
    source: dim_product
    column: product_name
    hierarchy:
      - department
      - category
      - subcategory
      - product_name
```

---

## Complete YAML Example

Full semantic layer configuration:

```yaml
semantic_layer:
  metrics:
    # Revenue metrics
    - name: revenue
      description: "Total revenue from all orders"
      expr: "SUM(total_amount)"
      source: fact_orders

    - name: completed_revenue
      description: "Revenue from completed orders only"
      expr: "SUM(total_amount)"
      source: fact_orders
      filters:
        - "status = 'completed'"

    # Volume metrics
    - name: order_count
      description: "Number of orders"
      expr: "COUNT(*)"
      source: fact_orders

    - name: units_sold
      description: "Total units sold"
      expr: "SUM(quantity)"
      source: fact_orders

    # Customer metrics
    - name: unique_customers
      description: "Distinct customer count"
      expr: "COUNT(DISTINCT customer_sk)"
      source: fact_orders

    # Calculated metrics
    - name: avg_order_value
      description: "Average order value"
      expr: "AVG(total_amount)"
      source: fact_orders

    - name: avg_units_per_order
      description: "Average units per order"
      expr: "AVG(quantity)"
      source: fact_orders

  dimensions:
    # Geographic dimensions
    - name: region
      source: dim_customer
      column: region
      description: "Customer region"

    - name: country
      source: dim_customer
      column: country

    - name: city
      source: dim_customer
      column: city

    # Time dimensions
    - name: order_date
      source: dim_date
      column: full_date
      hierarchy: [year, quarter, month, full_date]

    - name: year
      source: dim_date
      column: year

    - name: month
      source: dim_date
      column: month_name

    - name: quarter
      source: dim_date
      column: quarter_name

    # Product dimensions
    - name: category
      source: dim_product
      column: category

    - name: product
      source: dim_product
      column: product_name
      hierarchy: [category, subcategory, product_name]

    # Order dimensions
    - name: channel
      source: fact_orders
      column: sales_channel

    - name: payment_method
      source: fact_orders
      column: payment_type

  materializations:
    - name: daily_revenue_by_region
      metrics: [revenue, order_count, unique_customers]
      dimensions: [region, order_date]
      output: gold/agg_daily_revenue_region

    - name: monthly_revenue_by_category
      metrics: [revenue, units_sold]
      dimensions: [category, month]
      output: gold/agg_monthly_revenue_category
      schedule: "0 2 1 * *"
```

---

## Python API

```python
from odibi.semantics.metrics import (
    MetricDefinition,
    DimensionDefinition,
    SemanticLayerConfig,
    parse_semantic_config
)

# Create metrics programmatically
revenue = MetricDefinition(
    name="revenue",
    description="Total revenue",
    expr="SUM(total_amount)",
    source="fact_orders",
    filters=["status = 'completed'"]
)

# Create dimensions
region = DimensionDefinition(
    name="region",
    source="dim_customer",
    column="region"
)

order_date = DimensionDefinition(
    name="order_date",
    source="dim_date",
    column="full_date",
    hierarchy=["year", "quarter", "month", "full_date"]
)

# Create config
config = SemanticLayerConfig(
    metrics=[revenue],
    dimensions=[region, order_date]
)

# Or parse from YAML
config = parse_semantic_config({
    "metrics": [...],
    "dimensions": [...],
    "materializations": [...]
})

# Validate references
errors = config.validate_references()
if errors:
    print("Validation errors:", errors)

# Lookup by name
metric = config.get_metric("revenue")
dimension = config.get_dimension("region")
```

---

## Validation

The semantic layer validates:

1. **Metric names**: Must be alphanumeric + underscore, lowercase
2. **Non-empty expressions**: `expr` cannot be empty
3. **Materialization references**: All referenced metrics/dimensions must exist

```python
# Validate the config
errors = config.validate_references()
# Returns: ["Materialization 'x' references unknown metric 'y'"]
```

---

## Best Practices

### Naming Conventions
- Use `snake_case` for metric and dimension names
- Be descriptive: `completed_order_revenue` over `rev1`
- Prefix related metrics: `revenue`, `revenue_completed`, `revenue_refunded`

### Filter Usage
- Define filtered variants as separate metrics
- Makes queries cleaner and consistent
- Enables caching of common filter combinations

### Hierarchy Design
- Order from coarsest to finest grain
- Match BI tool drill-down expectations
- Include intermediate levels for flexibility

---

## See Also

- [Querying](./query.md) - Query syntax and execution
- [Materializing](./materialize.md) - Pre-compute metrics
- [Semantic Layer Overview](./index.md) - Architecture and concepts
