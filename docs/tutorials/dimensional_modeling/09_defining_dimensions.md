# Defining Dimensions Tutorial

In this tutorial, you'll learn how to define semantic layer dimensions for grouping and filtering metrics. Dimensions are the "BY" part of queries like `"revenue BY region"`.

**What You'll Learn:**
- Simple dimensions (single column)
- Dimensions with different column names
- Hierarchical dimensions (drill-down)
- Complete config with metrics AND dimensions

---

## Star Schema Data

We'll use the star schema from Tutorial 06:

### dim_customer (sample)

| customer_sk | customer_id | name | region | city | state |
|-------------|-------------|------|--------|------|-------|
| 1 | C001 | Alice Johnson | North | Chicago | IL |
| 2 | C002 | Bob Smith | South | Houston | TX |
| 3 | C003 | Carol White | North | Detroit | MI |
| 4 | C004 | David Brown | East | New York | NY |
| 5 | C005 | Emma Davis | West | Seattle | WA |
| 6 | C006 | Frank Miller | South | Miami | FL |
| 7 | C007 | Grace Lee | East | Boston | MA |
| 8 | C008 | Henry Wilson | West | Portland | OR |
| 9 | C009 | Ivy Chen | North | Minneapolis | MN |
| 10 | C010 | Jack Taylor | South | Dallas | TX |
| 11 | C011 | Karen Martinez | East | Philadelphia | PA |
| 12 | C012 | Leo Anderson | West | Denver | CO |

### dim_product (sample)

| product_sk | product_id | name | category | subcategory |
|------------|------------|------|----------|-------------|
| 1 | P001 | Laptop Pro 15 | Electronics | Computers |
| 2 | P002 | Wireless Mouse | Electronics | Accessories |
| 3 | P003 | Office Chair | Furniture | Seating |
| 4 | P004 | USB-C Hub | Electronics | Accessories |
| 5 | P005 | Standing Desk | Furniture | Desks |
| 6 | P006 | Mechanical Keyboard | Electronics | Accessories |
| 7 | P007 | Monitor 27" | Electronics | Displays |
| 8 | P008 | Desk Lamp | Furniture | Lighting |
| 9 | P009 | Webcam HD | Electronics | Accessories |
| 10 | P010 | Filing Cabinet | Furniture | Storage |

### dim_date (sample)

| date_sk | full_date | day_of_week | month | month_name | quarter_name | year |
|---------|-----------|-------------|-------|------------|--------------|------|
| 20240115 | 2024-01-15 | Monday | 1 | January | Q1 | 2024 |
| 20240116 | 2024-01-16 | Tuesday | 1 | January | Q1 | 2024 |
| 20240117 | 2024-01-17 | Wednesday | 1 | January | Q1 | 2024 |
| 20240118 | 2024-01-18 | Thursday | 1 | January | Q1 | 2024 |
| 20240119 | 2024-01-19 | Friday | 1 | January | Q1 | 2024 |
| 20240120 | 2024-01-20 | Saturday | 1 | January | Q1 | 2024 |
| 20240121 | 2024-01-21 | Sunday | 1 | January | Q1 | 2024 |
| 20240122 | 2024-01-22 | Monday | 1 | January | Q1 | 2024 |
| 20240123 | 2024-01-23 | Tuesday | 1 | January | Q1 | 2024 |
| 20240124 | 2024-01-24 | Wednesday | 1 | January | Q1 | 2024 |

---

## Step 1: Define a Simple Dimension

The simplest dimension maps a column directly:

### Python Code

```python
from odibi.semantics import DimensionDefinition

# Simple dimension: region from dim_customer
region = DimensionDefinition(
    name="region",
    source="dim_customer",
    column="region"
)
```

### YAML Alternative

```yaml
dimensions:
  - name: region
    source: dim_customer
    column: region
```

### Understanding the Definition

| Field | Value | Purpose |
|-------|-------|---------|
| `name` | `"region"` | How you reference it in queries |
| `source` | `"dim_customer"` | Table containing the dimension |
| `column` | `"region"` | Column to GROUP BY |

### Query Example

```python
result = query.execute("revenue BY region", context)
```

**Result (4 rows):**

| region | revenue |
|--------|---------|
| North | 2,549.88 |
| South | 2,349.93 |
| East | 1,923.88 |
| West | 2,129.87 |

---

## Step 2: Dimension with Different Column Name

Sometimes you want the dimension name to differ from the column name:

### Python Code

```python
# Dimension name differs from column name
customer_city = DimensionDefinition(
    name="customer_city",      # Query uses "customer_city"
    source="dim_customer",
    column="city"              # Actual column is "city"
)
```

### YAML Alternative

```yaml
dimensions:
  - name: customer_city
    source: dim_customer
    column: city
```

### Query Example

```python
result = query.execute("revenue BY customer_city", context)
```

**Result (12 rows):**

| customer_city | revenue |
|---------------|---------|
| Chicago | 1,559.95 |
| Houston | 1,049.97 |
| Detroit | 541.93 |
| New York | 1,079.97 |
| Seattle | 477.97 |
| Miami | 959.95 |
| Boston | 573.94 |
| Portland | 1,379.98 |
| Minneapolis | 469.95 |
| Dallas | 1,549.98 |
| Philadelphia | 249.92 |
| Denver | 1,309.96 |

---

## Step 3: Dimension with Hierarchy

A **hierarchy** defines drill-down levels. Users can start at a high level (year) and drill into details (month, week, day).

### The Drill-Down Concept

```
Year (2024)
  └── Quarter (Q1)
        └── Month (January)
              └── Week (Week 3)
                    └── Day (Jan 15)
```

### Python Code

```python
# Date dimension with hierarchy
order_date = DimensionDefinition(
    name="order_date",
    source="dim_date",
    column="full_date",
    hierarchy=["year", "quarter_name", "month_name", "full_date"]
)
```

### YAML Alternative

```yaml
dimensions:
  - name: order_date
    source: dim_date
    column: full_date
    hierarchy:
      - year
      - quarter_name
      - month_name
      - full_date
```

### Using Hierarchy Levels in Queries

**Top level - Year:**
```python
result = query.execute("revenue BY year", context)
```

| year | revenue |
|------|---------|
| 2024 | 8,953.56 |

**Drill down - Quarter:**
```python
result = query.execute("revenue BY quarter_name", context)
```

| quarter_name | revenue |
|--------------|---------|
| Q1 | 8,953.56 |

**Drill down - Month:**
```python
result = query.execute("revenue BY month_name", context)
```

| month_name | revenue |
|------------|---------|
| January | 8,953.56 |

**Drill down - Day:**
```python
result = query.execute("revenue BY full_date", context)
```

| full_date | revenue |
|-----------|---------|
| 2024-01-15 | 1,439.96 |
| 2024-01-16 | 589.95 |
| 2024-01-17 | 749.98 |
| ... | ... |

---

## Step 4: Define All Dimensions for Our Star Schema

Let's define a complete set of dimensions:

### Python Code

```python
from odibi.semantics import DimensionDefinition

dimensions = [
    # Geographic dimensions (from dim_customer)
    DimensionDefinition(
        name="region",
        source="dim_customer",
        column="region",
        description="Customer geographic region"
    ),
    DimensionDefinition(
        name="city",
        source="dim_customer",
        column="city"
    ),
    DimensionDefinition(
        name="state",
        source="dim_customer",
        column="state"
    ),
    
    # Product dimensions (from dim_product)
    DimensionDefinition(
        name="category",
        source="dim_product",
        column="category",
        description="Product category"
    ),
    DimensionDefinition(
        name="subcategory",
        source="dim_product",
        column="subcategory"
    ),
    DimensionDefinition(
        name="product_name",
        source="dim_product",
        column="name",
        hierarchy=["category", "subcategory", "name"]
    ),
    
    # Time dimensions (from dim_date)
    DimensionDefinition(
        name="year",
        source="dim_date",
        column="year"
    ),
    DimensionDefinition(
        name="quarter",
        source="dim_date",
        column="quarter_name"
    ),
    DimensionDefinition(
        name="month",
        source="dim_date",
        column="month_name",
        hierarchy=["year", "quarter_name", "month_name"]
    ),
    DimensionDefinition(
        name="day_of_week",
        source="dim_date",
        column="day_of_week"
    ),
    
    # Order dimensions (from fact_orders)
    DimensionDefinition(
        name="status",
        source="fact_orders",
        column="status"
    )
]
```

### YAML Alternative

```yaml
dimensions:
  # Geographic dimensions
  - name: region
    source: dim_customer
    column: region
    description: "Customer geographic region"
  
  - name: city
    source: dim_customer
    column: city
  
  - name: state
    source: dim_customer
    column: state
  
  # Product dimensions
  - name: category
    source: dim_product
    column: category
    description: "Product category"
  
  - name: subcategory
    source: dim_product
    column: subcategory
  
  - name: product_name
    source: dim_product
    column: name
    hierarchy:
      - category
      - subcategory
      - name
  
  # Time dimensions
  - name: year
    source: dim_date
    column: year
  
  - name: quarter
    source: dim_date
    column: quarter_name
  
  - name: month
    source: dim_date
    column: month_name
    hierarchy:
      - year
      - quarter_name
      - month_name
  
  - name: day_of_week
    source: dim_date
    column: day_of_week
  
  # Order dimensions
  - name: status
    source: fact_orders
    column: status
```

---

## Complete Config with Metrics AND Dimensions

Here's the full SemanticLayerConfig:

### Python Code

```python
from odibi.semantics import (
    MetricDefinition,
    DimensionDefinition,
    SemanticLayerConfig
)

config = SemanticLayerConfig(
    metrics=[
        MetricDefinition(
            name="revenue",
            description="Total revenue from completed orders",
            expr="SUM(line_total)",
            source="fact_orders",
            filters=["status = 'completed'"]
        ),
        MetricDefinition(
            name="order_count",
            description="Number of completed orders",
            expr="COUNT(*)",
            source="fact_orders",
            filters=["status = 'completed'"]
        ),
        MetricDefinition(
            name="avg_order_value",
            description="Average order value",
            expr="AVG(line_total)",
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
        MetricDefinition(
            name="total_quantity",
            description="Total units sold",
            expr="SUM(quantity)",
            source="fact_orders",
            filters=["status = 'completed'"]
        )
    ],
    dimensions=[
        # Geographic
        DimensionDefinition(name="region", source="dim_customer", column="region"),
        DimensionDefinition(name="city", source="dim_customer", column="city"),
        DimensionDefinition(name="state", source="dim_customer", column="state"),
        
        # Product
        DimensionDefinition(name="category", source="dim_product", column="category"),
        DimensionDefinition(name="subcategory", source="dim_product", column="subcategory"),
        DimensionDefinition(
            name="product_name", 
            source="dim_product", 
            column="name",
            hierarchy=["category", "subcategory", "name"]
        ),
        
        # Time
        DimensionDefinition(name="year", source="dim_date", column="year"),
        DimensionDefinition(name="quarter", source="dim_date", column="quarter_name"),
        DimensionDefinition(
            name="month", 
            source="dim_date", 
            column="month_name",
            hierarchy=["year", "quarter_name", "month_name"]
        ),
        DimensionDefinition(name="day_of_week", source="dim_date", column="day_of_week"),
        
        # Order
        DimensionDefinition(name="status", source="fact_orders", column="status")
    ]
)
```

### YAML Alternative (semantic_config.yaml)

```yaml
# File: semantic_config.yaml
metrics:
  - name: revenue
    description: "Total revenue from completed orders"
    expr: "SUM(line_total)"
    source: fact_orders
    filters: ["status = 'completed'"]
  
  - name: order_count
    description: "Number of completed orders"
    expr: "COUNT(*)"
    source: fact_orders
    filters: ["status = 'completed'"]
  
  - name: avg_order_value
    description: "Average order value"
    expr: "AVG(line_total)"
    source: fact_orders
    filters: ["status = 'completed'"]
  
  - name: unique_customers
    description: "Number of unique customers"
    expr: "COUNT(DISTINCT customer_sk)"
    source: fact_orders
    filters: ["status = 'completed'"]
  
  - name: total_quantity
    description: "Total units sold"
    expr: "SUM(quantity)"
    source: fact_orders
    filters: ["status = 'completed'"]

dimensions:
  # Geographic
  - name: region
    source: dim_customer
    column: region
  - name: city
    source: dim_customer
    column: city
  - name: state
    source: dim_customer
    column: state
  
  # Product
  - name: category
    source: dim_product
    column: category
  - name: subcategory
    source: dim_product
    column: subcategory
  - name: product_name
    source: dim_product
    column: name
    hierarchy: [category, subcategory, name]
  
  # Time
  - name: year
    source: dim_date
    column: year
  - name: quarter
    source: dim_date
    column: quarter_name
  - name: month
    source: dim_date
    column: month_name
    hierarchy: [year, quarter_name, month_name]
  - name: day_of_week
    source: dim_date
    column: day_of_week
  
  # Order
  - name: status
    source: fact_orders
    column: status
```

---

## Example Queries with Dimensions

Now you can run rich queries:

**Revenue by region:**
```python
result = query.execute("revenue BY region", context)
```

| region | revenue |
|--------|---------|
| North | 2,549.88 |
| South | 2,349.93 |
| East | 1,923.88 |
| West | 2,129.87 |

**Revenue by category and region:**
```python
result = query.execute("revenue BY category, region", context)
```

| category | region | revenue |
|----------|--------|---------|
| Electronics | North | 1,549.94 |
| Electronics | South | 1,449.95 |
| Electronics | East | 1,323.91 |
| Electronics | West | 1,079.93 |
| Furniture | North | 999.94 |
| Furniture | South | 899.98 |
| Furniture | East | 599.97 |
| Furniture | West | 1,049.94 |

**Multiple metrics by day of week:**
```python
result = query.execute("revenue, order_count, avg_order_value BY day_of_week", context)
```

| day_of_week | revenue | order_count | avg_order_value |
|-------------|---------|-------------|-----------------|
| Monday | 2,189.94 | 5 | 437.99 |
| Tuesday | 1,177.93 | 5 | 235.59 |
| Wednesday | 1,099.95 | 4 | 275.00 |
| Thursday | 2,373.90 | 4 | 593.48 |
| Friday | 619.96 | 4 | 155.00 |
| Saturday | 1,549.94 | 3 | 516.65 |
| Sunday | 941.94 | 2 | 470.97 |

---

## What You Learned

In this tutorial, you learned:

- **Simple dimensions** map a column for grouping: `region`, `category`
- **Column renaming** lets dimension names differ from columns: `customer_city` → `city`
- **Hierarchies** define drill-down paths: `year > quarter > month`
- Dimensions can come from **dimension tables or fact tables**
- Complete config includes **both metrics and dimensions**
- Queries use dimensions with `BY`: `"revenue BY region, category"`

---

## Next Steps

Now let's learn how to execute queries against our semantic layer.

**Next:** [Querying Metrics](./10_querying_metrics.md)

---

## Navigation

| Previous | Up | Next |
|----------|----|----|
| [Defining Metrics](./08_defining_metrics.md) | [Tutorials](../getting_started.md) | [Querying Metrics](./10_querying_metrics.md) |

---

## Reference

For complete documentation, see: [Defining Metrics Reference](../../semantics/metrics.md)
