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

## Summary of Function Signature Rules

| Signature | Behavior |
| :--- | :--- |
| `def func(context):` | Receives context only. Does not receive previous step output. |
| `def func(context, current):` | Receives context AND the result of the previous step. |
| `def func(context, my_param):` | Receives context and a parameter from YAML. |
| `def func(context, current, my_param):` | Receives all three. |
