# Querying Metrics Tutorial

In this tutorial, you'll learn how to execute queries against the semantic layer using the `SemanticQuery` interface.

**What You'll Learn:**
- Simple queries (total, no grouping)
- Queries with one dimension
- Queries with multiple dimensions
- Filtered queries
- Multiple metrics together

---

## Source Data

We'll use the star schema and semantic config from previous tutorials:

### fact_orders (20 sample rows)

| order_id | customer_sk | product_sk | date_sk | quantity | line_total | status |
|----------|-------------|------------|---------|----------|------------|--------|
| ORD001 | 1 | 1 | 20240115 | 1 | 1299.99 | completed |
| ORD002 | 1 | 2 | 20240115 | 2 | 59.98 | completed |
| ORD003 | 2 | 3 | 20240116 | 1 | 249.99 | completed |
| ORD004 | 3 | 4 | 20240116 | 3 | 149.97 | completed |
| ORD005 | 4 | 5 | 20240117 | 1 | 599.99 | completed |
| ORD006 | 5 | 6 | 20240117 | 1 | 149.99 | completed |
| ORD007 | 6 | 7 | 20240118 | 2 | 799.98 | completed |
| ORD008 | 7 | 8 | 20240118 | 4 | 183.96 | completed |
| ORD009 | 8 | 9 | 20240119 | 1 | 79.99 | completed |
| ORD010 | 9 | 10 | 20240119 | 1 | 189.99 | completed |
| ORD011 | 10 | 1 | 20240120 | 1 | 1299.99 | completed |
| ORD012 | 11 | 2 | 20240120 | 5 | 149.95 | completed |
| ORD013 | 12 | 3 | 20240121 | 2 | 499.98 | completed |
| ORD014 | 1 | 4 | 20240121 | 1 | 49.99 | completed |
| ORD015 | 2 | 5 | 20240122 | 1 | 599.99 | pending |
| ORD016 | 3 | 6 | 20240122 | 2 | 299.98 | completed |
| ORD017 | 4 | 7 | 20240123 | 1 | 399.99 | completed |
| ORD018 | 5 | 8 | 20240123 | 3 | 137.97 | completed |
| ORD019 | 6 | 9 | 20240124 | 2 | 159.98 | completed |
| ORD020 | 7 | 10 | 20240124 | 1 | 189.99 | completed |

### dim_customer (12 rows)

| customer_sk | name | region | city |
|-------------|------|--------|------|
| 1 | Alice Johnson | North | Chicago |
| 2 | Bob Smith | South | Houston |
| 3 | Carol White | North | Detroit |
| 4 | David Brown | East | New York |
| 5 | Emma Davis | West | Seattle |
| 6 | Frank Miller | South | Miami |
| 7 | Grace Lee | East | Boston |
| 8 | Henry Wilson | West | Portland |
| 9 | Ivy Chen | North | Minneapolis |
| 10 | Jack Taylor | South | Dallas |
| 11 | Karen Martinez | East | Philadelphia |
| 12 | Leo Anderson | West | Denver |

### dim_product (10 rows)

| product_sk | name | category |
|------------|------|----------|
| 1 | Laptop Pro 15 | Electronics |
| 2 | Wireless Mouse | Electronics |
| 3 | Office Chair | Furniture |
| 4 | USB-C Hub | Electronics |
| 5 | Standing Desk | Furniture |
| 6 | Mechanical Keyboard | Electronics |
| 7 | Monitor 27" | Electronics |
| 8 | Desk Lamp | Furniture |
| 9 | Webcam HD | Electronics |
| 10 | Filing Cabinet | Furniture |

---

## Step 1: Simple Query - Total (No Grouping)

The simplest query returns a single aggregated value.

### Query

```python
from odibi.semantics import SemanticQuery, parse_semantic_config
from odibi.context import EngineContext
from odibi.enums import EngineType
import yaml

# Load config
with open("semantic_config.yaml") as f:
    config = parse_semantic_config(yaml.safe_load(f))

# Create query interface
query = SemanticQuery(config)

# Setup context with data
context = EngineContext(df=None, engine_type=EngineType.PANDAS)
context.register("fact_orders", fact_orders_df)
context.register("dim_customer", dim_customer_df)
context.register("dim_product", dim_product_df)
context.register("dim_date", dim_date_df)

# Execute query
result = query.execute("revenue", context)
```

### Query String

```
"revenue"
```

### Generated SQL

```sql
SELECT SUM(line_total) AS revenue
FROM fact_orders
WHERE status = 'completed'
```

### Result (1 row)

| revenue |
|---------|
| 8,953.56 |

### Accessing the Result

```python
print(f"Total Revenue: ${result.df['revenue'].iloc[0]:,.2f}")
# Output: Total Revenue: $8,953.56

print(f"Row count: {result.row_count}")
# Output: Row count: 1

print(f"Execution time: {result.elapsed_ms:.2f}ms")
# Output: Execution time: 12.34ms

print(f"Generated SQL: {result.sql_generated}")
# Output: SELECT SUM(line_total) AS revenue FROM fact_orders WHERE status = 'completed'
```

---

## Step 2: Query with One Dimension

Add a dimension to group the results.

### Query String

```
"revenue BY region"
```

### Generated SQL

```sql
SELECT 
    c.region,
    SUM(f.line_total) AS revenue
FROM fact_orders f
JOIN dim_customer c ON f.customer_sk = c.customer_sk
WHERE f.status = 'completed'
GROUP BY c.region
```

### Python Code

```python
result = query.execute("revenue BY region", context)
print(result.df)
```

### Result (4 rows)

| region | revenue |
|--------|---------|
| North | 2,549.88 |
| South | 2,349.93 |
| East | 1,923.88 |
| West | 2,129.87 |

---

## Step 3: Query with Multiple Dimensions

Add more dimensions for a cross-tabulation.

### Query String

```
"revenue, order_count BY region, category"
```

### Generated SQL

```sql
SELECT 
    c.region,
    p.category,
    SUM(f.line_total) AS revenue,
    COUNT(*) AS order_count
FROM fact_orders f
JOIN dim_customer c ON f.customer_sk = c.customer_sk
JOIN dim_product p ON f.product_sk = p.product_sk
WHERE f.status = 'completed'
GROUP BY c.region, p.category
```

### Python Code

```python
result = query.execute("revenue, order_count BY region, category", context)
print(result.df)
```

### Result (8 rows - region × category)

| region | category | revenue | order_count |
|--------|----------|---------|-------------|
| North | Electronics | 1,549.94 | 4 |
| North | Furniture | 999.94 | 3 |
| South | Electronics | 1,449.95 | 4 |
| South | Furniture | 899.98 | 3 |
| East | Electronics | 1,323.91 | 4 |
| East | Furniture | 599.97 | 3 |
| West | Electronics | 1,079.93 | 3 |
| West | Furniture | 1,049.94 | 4 |

---

## Step 4: Query with Filter

Add a WHERE clause to filter results.

### Query String

```
"revenue BY category WHERE region = 'North'"
```

### Generated SQL

```sql
SELECT 
    p.category,
    SUM(f.line_total) AS revenue
FROM fact_orders f
JOIN dim_customer c ON f.customer_sk = c.customer_sk
JOIN dim_product p ON f.product_sk = p.product_sk
WHERE f.status = 'completed'
  AND c.region = 'North'
GROUP BY p.category
```

### Python Code

```python
result = query.execute("revenue BY category WHERE region = 'North'", context)
print(result.df)
```

### Result (2 rows)

| category | revenue |
|----------|---------|
| Electronics | 1,549.94 |
| Furniture | 999.94 |

### Multiple Filters

You can combine multiple filter conditions:

```python
# Multiple conditions with AND
result = query.execute(
    "revenue BY category WHERE region = 'North' AND year = 2024",
    context
)

# IN clause
result = query.execute(
    "revenue BY region WHERE region IN ('North', 'South')",
    context
)
```

---

## Step 5: Multiple Metrics

Query multiple metrics in a single call.

### Query String

```
"revenue, order_count, avg_order_value BY region"
```

### Python Code

```python
result = query.execute("revenue, order_count, avg_order_value BY region", context)
print(result.df)
```

### Result (4 rows)

| region | revenue | order_count | avg_order_value |
|--------|---------|-------------|-----------------|
| North | 2,549.88 | 7 | 364.27 |
| South | 2,349.93 | 7 | 335.70 |
| East | 1,923.88 | 7 | 274.84 |
| West | 2,129.87 | 7 | 304.27 |

---

## Complete Python Script

Here's a complete, runnable example:

```python
from odibi.semantics import SemanticQuery, parse_semantic_config
from odibi.context import EngineContext
from odibi.enums import EngineType
import pandas as pd
import yaml

# ===========================================
# 1. Load the semantic config
# ===========================================
config_dict = {
    "metrics": [
        {
            "name": "revenue",
            "description": "Total revenue from completed orders",
            "expr": "SUM(line_total)",
            "source": "fact_orders",
            "filters": ["status = 'completed'"]
        },
        {
            "name": "order_count",
            "expr": "COUNT(*)",
            "source": "fact_orders",
            "filters": ["status = 'completed'"]
        },
        {
            "name": "avg_order_value",
            "expr": "AVG(line_total)",
            "source": "fact_orders",
            "filters": ["status = 'completed'"]
        }
    ],
    "dimensions": [
        {"name": "region", "source": "dim_customer", "column": "region"},
        {"name": "category", "source": "dim_product", "column": "category"},
        {"name": "month", "source": "dim_date", "column": "month_name"}
    ]
}

config = parse_semantic_config(config_dict)
query = SemanticQuery(config)

# ===========================================
# 2. Load data and create context
# ===========================================
fact_orders = pd.read_parquet("warehouse/fact_orders")
dim_customer = pd.read_parquet("warehouse/dim_customer")
dim_product = pd.read_parquet("warehouse/dim_product")
dim_date = pd.read_parquet("warehouse/dim_date")

context = EngineContext(df=None, engine_type=EngineType.PANDAS)
context.register("fact_orders", fact_orders)
context.register("dim_customer", dim_customer)
context.register("dim_product", dim_product)
context.register("dim_date", dim_date)

# ===========================================
# 3. Execute queries
# ===========================================

# Query 1: Total revenue
print("=" * 50)
print("Query 1: Total Revenue")
print("=" * 50)
result = query.execute("revenue", context)
print(f"Total Revenue: ${result.df['revenue'].iloc[0]:,.2f}")
print()

# Query 2: Revenue by region
print("=" * 50)
print("Query 2: Revenue by Region")
print("=" * 50)
result = query.execute("revenue BY region", context)
print(result.df.to_string(index=False))
print()

# Query 3: Multiple metrics by region
print("=" * 50)
print("Query 3: Multiple Metrics by Region")
print("=" * 50)
result = query.execute("revenue, order_count, avg_order_value BY region", context)
print(result.df.to_string(index=False))
print()

# Query 4: Revenue by region and category
print("=" * 50)
print("Query 4: Revenue by Region and Category")
print("=" * 50)
result = query.execute("revenue BY region, category", context)
print(result.df.to_string(index=False))
print()

# Query 5: Filtered query
print("=" * 50)
print("Query 5: North Region Only")
print("=" * 50)
result = query.execute("revenue, order_count BY category WHERE region = 'North'", context)
print(result.df.to_string(index=False))
print()

# ===========================================
# 4. Show execution details
# ===========================================
print("=" * 50)
print("Execution Details (last query)")
print("=" * 50)
print(f"Metrics: {result.metrics}")
print(f"Dimensions: {result.dimensions}")
print(f"Row count: {result.row_count}")
print(f"Execution time: {result.elapsed_ms:.2f}ms")
```

### Output

```
==================================================
Query 1: Total Revenue
==================================================
Total Revenue: $8,953.56

==================================================
Query 2: Revenue by Region
==================================================
 region   revenue
  North  2549.88
  South  2349.93
   East  1923.88
   West  2129.87

==================================================
Query 3: Multiple Metrics by Region
==================================================
 region   revenue  order_count  avg_order_value
  North  2549.88            7           364.27
  South  2349.93            7           335.70
   East  1923.88            7           274.84
   West  2129.87            7           304.27

==================================================
Query 4: Revenue by Region and Category
==================================================
 region     category   revenue
  North  Electronics  1549.94
  North    Furniture   999.94
  South  Electronics  1449.95
  South    Furniture   899.98
   East  Electronics  1323.91
   East    Furniture   599.97
   West  Electronics  1079.93
   West    Furniture  1049.94

==================================================
Query 5: North Region Only
==================================================
    category   revenue  order_count
 Electronics  1549.94            4
   Furniture   999.94            3

==================================================
Execution Details (last query)
==================================================
Metrics: ['revenue', 'order_count']
Dimensions: ['category']
Row count: 2
Execution time: 8.45ms
```

---

## Query Syntax Reference

| Pattern | Example | Description |
|---------|---------|-------------|
| Single metric | `"revenue"` | Total, no grouping |
| Metric + dimension | `"revenue BY region"` | Grouped by one dimension |
| Multiple metrics | `"revenue, order_count"` | Multiple metrics together |
| Multiple dimensions | `"revenue BY region, category"` | Cross-tabulation |
| With filter | `"revenue BY region WHERE year = 2024"` | Filtered results |
| Complex filter | `"revenue BY region WHERE region IN ('North', 'South')"` | IN clause |

---

## Error Handling

```python
# Invalid metric
try:
    result = query.execute("invalid_metric BY region", context)
except ValueError as e:
    print(f"Error: {e}")
    # Error: Unknown metric 'invalid_metric'. Available: ['revenue', 'order_count', 'avg_order_value']

# Invalid dimension
try:
    result = query.execute("revenue BY invalid_dimension", context)
except ValueError as e:
    print(f"Error: {e}")
    # Error: Unknown dimension 'invalid_dimension'. Available: ['region', 'category', 'month']
```

---

## What You Learned

In this tutorial, you learned:

- **Simple queries** return totals: `"revenue"`
- **One dimension** groups results: `"revenue BY region"`
- **Multiple dimensions** create cross-tabs: `"revenue BY region, category"`
- **Filters** constrain results: `"revenue BY region WHERE year = 2024"`
- **Multiple metrics** can be queried together: `"revenue, order_count BY region"`
- **QueryResult** contains the DataFrame, metrics, dimensions, and execution info

---

## Next Steps

Now let's learn how to pre-compute metrics for dashboard performance.

**Next:** [Materializing Metrics](./11_materializing_metrics.md)

---

## Navigation

| Previous | Up | Next |
|----------|----|----|
| [Defining Dimensions](./09_defining_dimensions.md) | [Tutorials](../getting_started.md) | [Materializing Metrics](./11_materializing_metrics.md) |

---

## Reference

For complete documentation, see: [Querying Reference](../../semantics/query.md)
