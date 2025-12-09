# Defining Metrics Tutorial

In this tutorial, you'll learn how to define metrics in the Odibi semantic layer, from simple aggregations to complex filtered and derived metrics.

**What You'll Learn:**
- Simple metrics (SUM, COUNT, AVG)
- Filtered metrics (with WHERE conditions)
- Multiple metrics together
- Derived metrics (calculated from other metrics)

---

## Source Data: fact_orders

We'll use the fact_orders table from Tutorial 06 (15 sample rows shown):

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
| ORD015 | 2 | 5 | 20240122 | 1 | 599.99 | 599.99 | **pending** |
| ORD023 | 10 | 3 | 20240126 | 1 | 249.99 | 249.99 | **cancelled** |
| ... | ... | ... | ... | ... | ... | ... | ... |

**Total: 30 rows**
- 27 completed
- 1 pending
- 2 cancelled

---

## Step 1: Define a Simple Metric

Let's start with the most basic metric: total revenue.

### Python Code

```python
from odibi.semantics import MetricDefinition

# Define a simple revenue metric
revenue = MetricDefinition(
    name="revenue",
    description="Total revenue from all orders",
    expr="SUM(line_total)",
    source="fact_orders"
)
```

### YAML Alternative

```yaml
metrics:
  - name: revenue
    description: "Total revenue from all orders"
    expr: "SUM(line_total)"
    source: fact_orders
```

### Understanding the Definition

| Field | Value | Purpose |
|-------|-------|---------|
| `name` | `"revenue"` | How you'll reference this metric in queries |
| `description` | `"Total revenue..."` | Human-readable documentation |
| `expr` | `"SUM(line_total)"` | SQL aggregation expression |
| `source` | `"fact_orders"` | Table to aggregate from |

### Source Notation

The `source` field supports three formats:

| Format | Example | Description |
|--------|---------|-------------|
| **$pipeline.node** | `$build_warehouse.fact_orders` | References a pipeline node's write target |
| **connection.path** | `gold.fact_orders` | Explicit connection + path (supports nested paths) |
| **bare name** | `fact_orders` | Uses default connection (manual setup) |

#### Option 1: Node Reference (Recommended)

Reference the pipeline node that produces the table. The semantic layer automatically reads from wherever that node writes:

```yaml
# odibi.yaml
pipelines:
  - pipeline: build_warehouse
    nodes:
      - name: fact_orders
        write:
          connection: gold
          table: fact_orders

semantic:
  metrics:
    - name: revenue
      expr: "SUM(line_total)"
      source: $build_warehouse.fact_orders    # References the node above
```

This approach:
- **DRY** - No duplication; the node already knows its write location
- **Auto-synced** - If you change the node's write config, the semantic layer follows
- Uses the same `$pipeline.node` pattern as cross-pipeline `inputs`

#### Option 2: Connection.Path (Explicit)

For tables that exist outside pipelines or when you want explicit control:

```yaml
semantic:
  metrics:
    - name: revenue
      expr: "SUM(line_total)"
      source: gold.fact_orders    # Resolves to /mnt/data/gold/fact_orders
```

**Nested paths are supported.** The split happens on the **first dot only**, so everything after becomes the path:

```yaml
# Given connection:
connections:
  gold:
    type: delta
    path: /mnt/data/gold

# These all work:
source: gold.fact_orders              # → /mnt/data/gold/fact_orders
source: gold.oee/plant_a/metrics      # → /mnt/data/gold/oee/plant_a/metrics
source: gold.domain/v2/fact_sales     # → /mnt/data/gold/domain/v2/fact_sales
```

For Unity Catalog connections with `catalog` + `schema_name`:

```yaml
connections:
  gold:
    type: delta
    catalog: main
    schema_name: gold_db

source: gold.fact_orders    # → main.gold_db.fact_orders
```

### Query Result

```python
result = query.execute("revenue", context)
```

| revenue |
|---------|
| 9,803.54 |

This includes ALL orders (completed, pending, cancelled).

---

## Step 2: Define a Filtered Metric

Usually, you only want "revenue" to include completed orders. Add a filter:

### Python Code

```python
completed_revenue = MetricDefinition(
    name="completed_revenue",
    description="Revenue from completed orders only",
    expr="SUM(line_total)",
    source="fact_orders",
    filters=["status = 'completed'"]
)
```

### YAML Alternative

```yaml
metrics:
  - name: completed_revenue
    description: "Revenue from completed orders only"
    expr: "SUM(line_total)"
    source: fact_orders
    filters:
      - "status = 'completed'"
```

### How Filters Work

**Without filter (revenue):**
```sql
SELECT SUM(line_total) AS revenue FROM fact_orders;
-- Uses all 30 rows
```

**With filter (completed_revenue):**
```sql
SELECT SUM(line_total) AS completed_revenue 
FROM fact_orders 
WHERE status = 'completed';
-- Uses only 27 completed rows
```

### Data Comparison

**All orders (30 rows) - includes:**

| order_id | line_total | status |
|----------|------------|--------|
| ORD001 | 1299.99 | completed |
| ORD002 | 59.98 | completed |
| ... | ... | ... |
| ORD015 | 599.99 | **pending** (excluded) |
| ORD023 | 249.99 | **cancelled** (excluded) |
| ... | ... | ... |

**Completed only (27 rows):**

| order_id | line_total | status |
|----------|------------|--------|
| ORD001 | 1299.99 | completed |
| ORD002 | 59.98 | completed |
| ... | ... | ... |

### Query Results

| Metric | Value | Rows Included |
|--------|-------|---------------|
| revenue | 9,803.54 | 30 (all) |
| completed_revenue | 8,953.56 | 27 (completed only) |

---

## Step 3: Define Multiple Metrics

Let's define a complete set of metrics:

### Python Code

```python
from odibi.semantics import MetricDefinition, SemanticLayerConfig

# Revenue metrics
revenue = MetricDefinition(
    name="revenue",
    description="Total revenue from completed orders",
    expr="SUM(line_total)",
    source="fact_orders",
    filters=["status = 'completed'"]
)

# Count metrics
order_count = MetricDefinition(
    name="order_count",
    description="Number of orders",
    expr="COUNT(*)",
    source="fact_orders",
    filters=["status = 'completed'"]
)

unique_customers = MetricDefinition(
    name="unique_customers",
    description="Number of unique customers",
    expr="COUNT(DISTINCT customer_sk)",
    source="fact_orders",
    filters=["status = 'completed'"]
)

# Average metrics
avg_order_value = MetricDefinition(
    name="avg_order_value",
    description="Average order value",
    expr="AVG(line_total)",
    source="fact_orders",
    filters=["status = 'completed'"]
)

# Volume metrics
total_quantity = MetricDefinition(
    name="total_quantity",
    description="Total units sold",
    expr="SUM(quantity)",
    source="fact_orders",
    filters=["status = 'completed'"]
)

# Combine into config
config = SemanticLayerConfig(
    metrics=[
        revenue,
        order_count,
        unique_customers,
        avg_order_value,
        total_quantity
    ]
)
```

### YAML Alternative

```yaml
metrics:
  - name: revenue
    description: "Total revenue from completed orders"
    expr: "SUM(line_total)"
    source: fact_orders
    filters:
      - "status = 'completed'"
  
  - name: order_count
    description: "Number of orders"
    expr: "COUNT(*)"
    source: fact_orders
    filters:
      - "status = 'completed'"
  
  - name: unique_customers
    description: "Number of unique customers"
    expr: "COUNT(DISTINCT customer_sk)"
    source: fact_orders
    filters:
      - "status = 'completed'"
  
  - name: avg_order_value
    description: "Average order value"
    expr: "AVG(line_total)"
    source: fact_orders
    filters:
      - "status = 'completed'"
  
  - name: total_quantity
    description: "Total units sold"
    expr: "SUM(quantity)"
    source: fact_orders
    filters:
      - "status = 'completed'"
```

### Query Results

**Single metric:**
```python
result = query.execute("revenue", context)
```

| revenue |
|---------|
| 8,953.56 |

**Multiple metrics:**
```python
result = query.execute("revenue, order_count, avg_order_value", context)
```

| revenue | order_count | avg_order_value |
|---------|-------------|-----------------|
| 8,953.56 | 27 | 331.61 |

---

## Step 4: Define a Derived Metric

A **derived metric** is calculated from other metrics. It doesn't aggregate directly from the source.

### Scenario: Profit Margin

We want to calculate profit margin, which requires cost data. Let's assume we've added a cost column:

**fact_orders with cost (sample):**

| order_id | line_total | cost_total |
|----------|------------|------------|
| ORD001 | 1299.99 | 850.00 |
| ORD002 | 59.98 | 24.00 |
| ORD003 | 249.99 | 120.00 |

### Python Code

```python
# Base metrics
revenue = MetricDefinition(
    name="revenue",
    expr="SUM(line_total)",
    source="fact_orders",
    filters=["status = 'completed'"]
)

total_cost = MetricDefinition(
    name="total_cost",
    expr="SUM(cost_total)",
    source="fact_orders",
    filters=["status = 'completed'"]
)

profit = MetricDefinition(
    name="profit",
    expr="SUM(line_total) - SUM(cost_total)",
    source="fact_orders",
    filters=["status = 'completed'"]
)

# Derived metric (calculated from other metrics)
profit_margin = MetricDefinition(
    name="profit_margin",
    description="Profit as percentage of revenue",
    expr="(revenue - total_cost) / revenue",
    type="derived"  # Indicates this references other metrics
)
```

### YAML Alternative

```yaml
metrics:
  - name: revenue
    expr: "SUM(line_total)"
    source: fact_orders
    filters:
      - "status = 'completed'"
  
  - name: total_cost
    expr: "SUM(cost_total)"
    source: fact_orders
    filters:
      - "status = 'completed'"
  
  - name: profit
    expr: "SUM(line_total) - SUM(cost_total)"
    source: fact_orders
    filters:
      - "status = 'completed'"
  
  - name: profit_margin
    description: "Profit as percentage of revenue"
    expr: "(revenue - total_cost) / revenue"
    type: derived
```

---

## Complete SemanticLayerConfig

Here's the complete configuration with all our metrics:

### Python Code

```python
from odibi.semantics import (
    MetricDefinition,
    DimensionDefinition,
    SemanticLayerConfig
)

config = SemanticLayerConfig(
    metrics=[
        # Revenue metrics
        MetricDefinition(
            name="revenue",
            description="Total revenue from completed orders",
            expr="SUM(line_total)",
            source="fact_orders",
            filters=["status = 'completed'"]
        ),
        MetricDefinition(
            name="pending_revenue",
            description="Revenue from pending orders",
            expr="SUM(line_total)",
            source="fact_orders",
            filters=["status = 'pending'"]
        ),
        
        # Count metrics
        MetricDefinition(
            name="order_count",
            description="Number of completed orders",
            expr="COUNT(*)",
            source="fact_orders",
            filters=["status = 'completed'"]
        ),
        MetricDefinition(
            name="unique_customers",
            description="Number of unique customers",
            expr="COUNT(DISTINCT customer_sk)",
            source="fact_orders",
            filters=["status = 'completed'"]
        ),
        
        # Average metrics
        MetricDefinition(
            name="avg_order_value",
            description="Average order value",
            expr="AVG(line_total)",
            source="fact_orders",
            filters=["status = 'completed'"]
        ),
        
        # Volume metrics
        MetricDefinition(
            name="total_quantity",
            description="Total units sold",
            expr="SUM(quantity)",
            source="fact_orders",
            filters=["status = 'completed'"]
        )
    ],
    dimensions=[]  # We'll add these in the next tutorial
)
```

### YAML Alternative

```yaml
# File: semantic_config.yaml
metrics:
  # Revenue metrics
  - name: revenue
    description: "Total revenue from completed orders"
    expr: "SUM(line_total)"
    source: fact_orders
    filters:
      - "status = 'completed'"
  
  - name: pending_revenue
    description: "Revenue from pending orders"
    expr: "SUM(line_total)"
    source: fact_orders
    filters:
      - "status = 'pending'"
  
  # Count metrics
  - name: order_count
    description: "Number of completed orders"
    expr: "COUNT(*)"
    source: fact_orders
    filters:
      - "status = 'completed'"
  
  - name: unique_customers
    description: "Number of unique customers"
    expr: "COUNT(DISTINCT customer_sk)"
    source: fact_orders
    filters:
      - "status = 'completed'"
  
  # Average metrics
  - name: avg_order_value
    description: "Average order value"
    expr: "AVG(line_total)"
    source: fact_orders
    filters:
      - "status = 'completed'"
  
  # Volume metrics
  - name: total_quantity
    description: "Total units sold"
    expr: "SUM(quantity)"
    source: fact_orders
    filters:
      - "status = 'completed'"

dimensions: []  # Added in next tutorial
```

---

## Available Aggregation Functions

| Function | Description | Example |
|----------|-------------|---------|
| `SUM(column)` | Sum of values | `SUM(line_total)` |
| `COUNT(*)` | Row count | `COUNT(*)` |
| `COUNT(column)` | Non-null count | `COUNT(customer_sk)` |
| `COUNT(DISTINCT column)` | Unique count | `COUNT(DISTINCT customer_sk)` |
| `AVG(column)` | Average | `AVG(line_total)` |
| `MIN(column)` | Minimum | `MIN(line_total)` |
| `MAX(column)` | Maximum | `MAX(line_total)` |

### Complex Expressions

```yaml
# Percentage of total (within group)
- name: revenue_share
  expr: "SUM(line_total) / SUM(SUM(line_total)) OVER ()"

# Conditional sum
- name: high_value_revenue
  expr: "SUM(CASE WHEN line_total > 500 THEN line_total ELSE 0 END)"

# Ratio
- name: items_per_order
  expr: "SUM(quantity) / COUNT(*)"
```

---

## Metric Naming Best Practices

| Do | Don't |
|-----|-------|
| `revenue` | `rev` |
| `order_count` | `cnt` |
| `completed_revenue` | `rev_comp` |
| `avg_order_value` | `aov` (unless standard) |
| `unique_customers` | `cust_distinct` |

**Guidelines:**
- Use `snake_case`
- Be descriptive: `completed_order_revenue` over `rev1`
- Prefix related metrics: `revenue`, `revenue_completed`, `revenue_pending`
- Include the filter in the name: `last_30_days_revenue`

---

## What You Learned

In this tutorial, you learned:

- **Simple metrics** aggregate directly from source: `SUM(line_total)`
- **Filters** constrain which rows are included: `status = 'completed'`
- **Multiple metrics** can be defined and queried together
- **Derived metrics** calculate from other metrics: `revenue - cost`
- The **expr** field uses SQL aggregation syntax
- The **source** field specifies which table to query
- **Naming conventions** make metrics discoverable

---

## Next Steps

Now let's learn how to define dimensions for grouping and filtering.

**Next:** [Defining Dimensions](./09_defining_dimensions.md)

---

## Navigation

| Previous | Up | Next |
|----------|----|----|
| [Semantic Layer Intro](./07_semantic_layer_intro.md) | [Tutorials](../getting_started.md) | [Defining Dimensions](./09_defining_dimensions.md) |

---

## Reference

For complete documentation, see: [Defining Metrics Reference](../../semantics/metrics.md)
