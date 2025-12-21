# Writing Transformation Functions in Odibi

This guide explains how to write custom Python transformation functions for Odibi pipelines, focusing on how to access data and manage state.

## The Basics

Every transformation function in Odibi must be decorated with `@transform`. The Odibi engine automatically injects dependencies based on your function signature.

### The `context` Object

The **first argument** to any transformation function is always `context`. This object is your gateway to the entire state of the pipeline execution.

Through `context`, you can:
- Access the output of any previous node.
- Retrieve datasets declared in `depends_on`.
- Inspect available data using `context.list_names()`.

### The `current` Argument

If your function includes an argument named `current`, Odibi will automatically pass the **output of the immediately preceding step** to it.

- **With `current`**: Continues the "chain" of data transformation.
- **Without `current`**: Breaks the chain (useful for generators or starting fresh logic).

## Accessing Other Datasets

While `current` is great for linear transformations (A → B → C), complex logic often requires accessing multiple datasets (e.g., for joins, lookups, or comparisons). You do this using `context.get()`.

### Pattern: Explicit Data Fetching

1.  **Define the Function**: Add a parameter for the dataset name you want to fetch.
2.  **Fetch from Context**: Use `context.get(name)`.
3.  **Configure in YAML**: Pass the node name as a parameter.

#### Python Implementation (`transforms.py`)

```python
from odibi import transform
import pandas as pd

@transform
def enrich_with_lookup(context, current: pd.DataFrame, lookup_node: str):
    """
    Enriches the current stream with data from a lookup node.

    Args:
        context: The Odibi execution context.
        current: The dataframe from the previous step.
        lookup_node: The name of the node containing lookup data (passed from YAML).
    """
    # 1. Fetch the other dataset using context
    if not context.has(lookup_node):
        raise ValueError(f"Lookup node '{lookup_node}' not found in context!")

    lookup_df = context.get(lookup_node)

    # 2. Perform the logic (e.g., merge)
    # Note: For simple merges, SQL is often preferred, but Python is useful
    # for fuzzy matching, complex logic, or API-based enrichment.
    result = current.merge(
        lookup_df,
        on="common_id",
        how="left",
        suffixes=("", "_lookup")
    )

    return result
```

#### YAML Configuration

```yaml
nodes:
  - name: main_process
    depends_on:
      - raw_orders      # The 'current' stream
      - customer_info   # The lookup table
    transform:
      steps:
        - function: enrich_with_lookup
          params:
            lookup_node: "customer_info"
```

## SQL vs. Python: When to use what?

Odibi supports mixing SQL and Python steps in the same node.

| Use **SQL** when... | Use **Python** when... |
| :--- | :--- |
| Joining tables (Standard Joins) | Making API calls (e.g., Geocoding, REST APIs) |
| Aggregations (GROUP BY, SUM) | Complex loops or procedural logic |
| Filtering (WHERE clauses) | Using libraries (NumPy, SciPy, AI models) |
| Renaming/Reordering columns | File operations or custom parsing |

**Example of SQL for Multi-Dataset Access:**
If you just need a standard join, you don't need a Python function. You can reference nodes directly in SQL:

```yaml
transform:
  steps:
    - sql: |
        SELECT o.*, c.email
        FROM current_df AS o
        LEFT JOIN customer_info AS c ON o.id = c.id
```

## SQL Transformations

For standard data transformations, SQL is often cleaner than Python. Odibi supports inline SQL and SQL file references.

### Inline SQL

```yaml
nodes:
  - name: clean_orders
    depends_on: [raw_orders]
    transform:
      steps:
        - sql: |
            SELECT 
              order_id,
              customer_id,
              UPPER(TRIM(status)) AS status,
              CAST(amount AS DECIMAL(10,2)) AS amount,
              COALESCE(discount, 0) AS discount
            FROM raw_orders
            WHERE order_id IS NOT NULL
```

### Multi-Table SQL Joins

Reference any node from `depends_on`:

```yaml
nodes:
  - name: enriched_orders
    depends_on: [clean_orders, customers, products]
    transform:
      steps:
        - sql: |
            SELECT 
              o.*,
              c.customer_name,
              c.segment,
              p.product_name,
              p.category
            FROM clean_orders o
            LEFT JOIN customers c ON o.customer_id = c.id
            LEFT JOIN products p ON o.product_id = p.id
```

### SQL File Reference

For complex queries, use external SQL files:

```yaml
transform:
  steps:
    - sql_file: sql/complex_aggregation.sql
```

**sql/complex_aggregation.sql:**
```sql
WITH daily_totals AS (
    SELECT 
        DATE(order_date) AS order_day,
        customer_id,
        SUM(amount) AS daily_amount
    FROM orders
    GROUP BY DATE(order_date), customer_id
)
SELECT 
    order_day,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(daily_amount) AS revenue
FROM daily_totals
GROUP BY order_day
```

### Window Functions in SQL

```yaml
transform:
  steps:
    - sql: |
        SELECT 
          *,
          ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS order_rank,
          SUM(amount) OVER (PARTITION BY customer_id) AS customer_lifetime_value,
          LAG(amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_order_amount
        FROM orders
```

### Combining SQL and Python Steps

```yaml
transform:
  steps:
    # Step 1: SQL for standard transformations
    - sql: |
        SELECT * FROM raw_orders 
        WHERE status != 'CANCELLED'
    
    # Step 2: Python for complex logic
    - function: enrich_with_api_data
      params:
        api_endpoint: "https://api.example.com/enrichment"
    
    # Step 3: SQL for final shaping
    - sql: |
        SELECT order_id, customer_id, amount, enriched_data
        FROM current_df
        ORDER BY order_date
```

---

## Registering Custom Transforms with @transform

The `@transform` decorator registers your function so Odibi can find it by name in YAML configurations.

### Basic Registration

```python
from odibi import transform

@transform
def clean_names(context, current):
    """Function is registered as 'clean_names' (uses function name)."""
    current['name'] = current['name'].str.strip().str.title()
    return current
```

### Custom Name Registration

```python
@transform("normalize_addresses")
def my_address_normalizer(context, current):
    """Function is registered as 'normalize_addresses'."""
    # ... address normalization logic
    return current
```

### Registration with Category and Parameter Model

```python
from pydantic import BaseModel

class EnrichmentParams(BaseModel):
    lookup_table: str
    join_key: str
    columns: list[str]

@transform(name="enrich_data", category="enrichment", param_model=EnrichmentParams)
def enrich_data(context, current, lookup_table: str, join_key: str, columns: list):
    """
    Registered as 'enrich_data' with parameter validation.
    
    Parameters are validated against EnrichmentParams before execution.
    """
    lookup_df = context.get(lookup_table)
    return current.merge(lookup_df[columns + [join_key]], on=join_key, how='left')
```

### Where to Put Your Transforms

1. **Project-level:** Create `transformations/custom_transforms.py`
2. **Import in project.yaml:**
   ```yaml
   python_imports:
     - transformations.custom_transforms
   ```
3. **Use in nodes:**
   ```yaml
   transform:
     steps:
       - function: normalize_addresses
   ```

---

## Summary of Function Signature Rules

| Signature | Behavior |
| :--- | :--- |
| `def func(context):` | Receives context only. Does not receive previous step output. |
| `def func(context, current):` | Receives context AND the result of the previous step. |
| `def func(context, my_param):` | Receives context and a parameter from YAML. |
| `def func(context, current, my_param):` | Receives all three. |

---

## See Also

- [Patterns Overview](../patterns/README.md) - Built-in transformation patterns
- [Best Practices](./best_practices.md) - Code organization guidelines
- [YAML Schema Reference](../reference/yaml_schema.md) - Full configuration reference
