# Querying the Semantic Layer

This guide covers how to query the Odibi semantic layer using the `SemanticQuery` interface.

## Query Syntax

The semantic query syntax follows a simple pattern:

```
metric1, metric2 BY dimension1, dimension2 WHERE condition
```

### Examples

```python
# Single metric, single dimension
"revenue BY region"

# Multiple metrics, single dimension
"revenue, order_count BY region"

# Multiple metrics, multiple dimensions
"revenue, order_count BY region, month"

# With WHERE filter
"revenue BY region WHERE year = 2024"

# Complex filter
"revenue BY category WHERE region = 'North' AND status = 'completed'"
```

---

## SemanticQuery Class

### Initialization

```python
from odibi.semantics import SemanticQuery, SemanticLayerConfig

# From config object
config = SemanticLayerConfig(
    metrics=[...],
    dimensions=[...],
    materializations=[...]
)
query = SemanticQuery(config)

# From YAML
import yaml
from odibi.semantics.metrics import parse_semantic_config

with open("semantic_layer.yaml") as f:
    config = parse_semantic_config(yaml.safe_load(f))
query = SemanticQuery(config)
```

### Execute Query

```python
from odibi.context import EngineContext

# Create context with source data
context = EngineContext(df=None, engine_type=EngineType.SPARK, spark=spark)
context.register("fact_orders", fact_orders_df)
context.register("dim_customer", dim_customer_df)

# Execute query
result = query.execute("revenue BY region", context)

# Access results
print(result.df)           # DataFrame with results
print(result.metrics)      # ['revenue']
print(result.dimensions)   # ['region']
print(result.row_count)    # Number of result rows
print(result.elapsed_ms)   # Execution time
print(result.sql_generated)  # Generated SQL (for debugging)
```

### QueryResult

| Field | Type | Description |
|-------|------|-------------|
| `df` | DataFrame | Result DataFrame (Spark or Pandas) |
| `metrics` | List[str] | Metrics that were computed |
| `dimensions` | List[str] | Dimensions used for grouping |
| `row_count` | int | Number of result rows |
| `elapsed_ms` | float | Execution time in milliseconds |
| `sql_generated` | str | Generated SQL query (for debugging) |

---

## Query Examples

### Basic Queries

```python
# Total revenue
result = query.execute("revenue", context)
# Returns single row with total

# Revenue by region
result = query.execute("revenue BY region", context)
# Returns one row per region

# Multiple metrics
result = query.execute("revenue, order_count, avg_order_value BY region", context)
```

### Multi-Dimensional Queries

```python
# Two dimensions
result = query.execute("revenue BY region, category", context)

# Three dimensions
result = query.execute("revenue BY region, category, month", context)

# Time series
result = query.execute("revenue, order_count BY year, month", context)
```

### Filtered Queries

```python
# Single filter
result = query.execute(
    "revenue BY category WHERE region = 'North'",
    context
)

# Multiple filters (combined with AND)
result = query.execute(
    "revenue BY category WHERE region = 'North' AND year = 2024",
    context
)

# Using metric filters + query filters
# If metric has filters, they combine with query filters
result = query.execute("completed_revenue BY region", context)
# Metric filter: status = 'completed'
# Combined: WHERE status = 'completed'
```

---

## Parse and Validate

You can parse and validate queries before execution:

```python
# Parse query string
parsed = query.parse("revenue, order_count BY region, month WHERE year = 2024")

print(parsed.metrics)      # ['revenue', 'order_count']
print(parsed.dimensions)   # ['region', 'month']
print(parsed.filters)      # ['year = 2024']
print(parsed.raw_query)    # Original query string

# Validate against config
errors = query.validate(parsed)
if errors:
    print("Validation errors:", errors)
    # ["Unknown metric 'invalid_metric'. Available: ['revenue', 'order_count']"]
```

---

## Generated SQL

View the SQL generated from a semantic query:

```python
parsed = query.parse("revenue BY region")
sql, source = query.generate_sql(parsed)

print(sql)
# SELECT region, SUM(total_amount) AS revenue 
# FROM fact_orders 
# GROUP BY region

print(source)
# fact_orders
```

---

## Full Python Example

```python
from odibi.semantics import SemanticQuery, parse_semantic_config
from odibi.context import EngineContext
from odibi.enums import EngineType
import yaml

# Load semantic layer config
config_dict = {
    "metrics": [
        {
            "name": "revenue",
            "expr": "SUM(total_amount)",
            "source": "fact_orders",
            "filters": ["status = 'completed'"]
        },
        {
            "name": "order_count",
            "expr": "COUNT(*)",
            "source": "fact_orders"
        },
        {
            "name": "avg_order_value",
            "expr": "AVG(total_amount)",
            "source": "fact_orders"
        }
    ],
    "dimensions": [
        {"name": "region", "source": "fact_orders", "column": "region"},
        {"name": "month", "source": "dim_date", "column": "month_name"}
    ]
}

config = parse_semantic_config(config_dict)
query = SemanticQuery(config)

# Create context with data
context = EngineContext(
    df=None, 
    engine_type=EngineType.PANDAS
)
context.register("fact_orders", orders_df)
context.register("dim_date", dates_df)

# Query 1: Total revenue
result = query.execute("revenue", context)
print(f"Total Revenue: ${result.df['revenue'].iloc[0]:,.2f}")

# Query 2: Revenue by region
result = query.execute("revenue, order_count BY region", context)
print("\nRevenue by Region:")
print(result.df.to_string(index=False))

# Query 3: Monthly trend
result = query.execute("revenue BY month", context)
print("\nMonthly Revenue:")
for _, row in result.df.iterrows():
    print(f"  {row['month']}: ${row['revenue']:,.2f}")

# Query 4: Filtered query
result = query.execute(
    "revenue, avg_order_value BY region WHERE region IN ('North', 'South')",
    context
)
print("\nNorth/South Regions:")
print(result.df.to_string(index=False))

# Check execution performance
print(f"\nQuery executed in {result.elapsed_ms:.2f}ms")
print(f"Generated SQL: {result.sql_generated}")
```

---

## Using with Source DataFrame

Override the context lookup with a specific DataFrame:

```python
# Instead of using context.get(source_table)
# Pass source_df directly
result = query.execute(
    "revenue BY region",
    context,
    source_df=my_filtered_dataframe
)
```

---

## Error Handling

```python
from odibi.semantics import SemanticQuery

try:
    result = query.execute("invalid_metric BY region", context)
except ValueError as e:
    print(f"Query error: {e}")
    # "Invalid semantic query: Unknown metric 'invalid_metric'. Available: ['revenue', 'order_count']"

try:
    result = query.execute("revenue BY invalid_dimension", context)
except ValueError as e:
    print(f"Query error: {e}")
    # "Invalid semantic query: Unknown dimension 'invalid_dimension'. Available: ['region', 'month']"
```

---

## Engine Support

Queries work with both Spark and Pandas:

### Spark
```python
context = EngineContext(
    df=None,
    engine_type=EngineType.SPARK,
    spark=spark_session
)
result = query.execute("revenue BY region", context)
result.df.show()  # Spark DataFrame
```

### Pandas
```python
context = EngineContext(
    df=None,
    engine_type=EngineType.PANDAS
)
result = query.execute("revenue BY region", context)
print(result.df)  # Pandas DataFrame
```

---

## Performance Tips

1. **Materialize frequent queries**: Use `Materializer` for dashboards
2. **Pre-filter source data**: Pass filtered `source_df` parameter
3. **Limit dimensions**: More dimensions = larger result set
4. **Use indexed columns**: Ensure dimension columns are indexed in source

---

## See Also

- [Defining Metrics](./metrics.md) - Create metric and dimension definitions
- [Materializing Metrics](./materialize.md) - Pre-compute for performance
- [Semantic Layer Overview](./index.md) - Architecture and concepts
