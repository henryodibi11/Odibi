# Custom Transform Functions Reference

This is the complete reference for writing custom Python transform functions in Odibi.
It covers the `@transform` decorator, the `EngineContext` API, YAML integration, and
practical recipes you can copy-paste into your own pipelines.

> **New to transforms?** Start with [Writing Transformations](./writing_transformations.md)
> for the basics, then come back here for the full API and advanced patterns.

---

## Quick Start

```python
# transforms.py
from odibi import transform

@transform("clean_status")
def clean_status(context, current):
    """Uppercase and trim the status column."""
    current["status"] = current["status"].str.strip().str.upper()
    return current
```

```yaml
# pipeline.yaml
nodes:
  - name: orders_clean
    depends_on: [raw_orders]
    transform:
      steps:
        - function: clean_status
```

That's it — register with `@transform`, reference by name in YAML.

---

## Table of Contents

1. [The @transform Decorator](#the-transform-decorator)
2. [Function Signatures](#function-signatures)
3. [The EngineContext API](#the-enginecontext-api)
4. [Passing Parameters from YAML](#passing-parameters-from-yaml)
5. [Parameter Validation with Pydantic](#parameter-validation-with-pydantic)
6. [Transform Step Types](#transform-step-types)
7. [Recipes](#recipes)
8. [Engine-Aware Functions](#engine-aware-functions)
9. [Gotchas & Tips](#gotchas--tips)

---

## The @transform Decorator

The `@transform` decorator registers your function in the global `FunctionRegistry`
so Odibi can look it up by name at runtime.

### Three Ways to Use It

```python
from odibi import transform

# 1. Bare decorator — registered as "my_func"
@transform
def my_func(context, current):
    return current

# 2. With a custom name — registered as "business_friendly_name"
@transform("business_friendly_name")
def my_func(context, current):
    return current

# 3. With keyword arguments — name + param validation
@transform(name="validated_func", param_model=MyParamsModel)
def my_func(context, current, **kwargs):
    return current
```

### Where to Put Your Functions

```
project/
├── project.yaml
├── transforms/
│   ├── __init__.py          # empty or import your modules
│   ├── cleaning.py          # @transform functions for data cleaning
│   └── enrichment.py        # @transform functions for enrichment
└── pipelines/
    └── silver.yaml
```

Tell Odibi to import them in your `project.yaml`:

```yaml
python_imports:
  - transforms.cleaning
  - transforms.enrichment
```

Every `@transform`-decorated function in those modules is now available by name in
any YAML node's `transform.steps`.

---

## Function Signatures

Odibi injects arguments based on parameter **names** in your function signature.

| Signature | What You Receive |
| :--- | :--- |
| `def func(context)` | `EngineContext` only — no previous step output |
| `def func(context, current)` | `EngineContext` + DataFrame from previous step |
| `def func(context, current, threshold)` | Context + DataFrame + `threshold` param from YAML |
| `def func(context, threshold)` | Context + param only (no chaining) |

### Rules

- **`context`** is always the first parameter — it is an `EngineContext` instance.
- **`current`** receives the output of the preceding step (or the node's input if first step).
- **Any other parameters** are filled from the `params:` dict in YAML.
- **Return value**: Always return a DataFrame. This becomes the input to the next step.

---

## The EngineContext API

The `EngineContext` object is your interface to the running pipeline. It wraps the
current DataFrame and gives you access to the engine, other datasets, SQL, and metadata.

### Properties

| Property | Type | Description |
| :--- | :--- | :--- |
| `context.df` | DataFrame | The current DataFrame (Spark, Pandas, or Polars) |
| `context.columns` | `list[str]` | Column names of the current DataFrame |
| `context.schema` | `dict[str, str]` | Column name → data type mapping |
| `context.engine_type` | `EngineType` | `"spark"`, `"pandas"`, or `"polars"` |
| `context.spark` | `SparkSession \| None` | The active SparkSession (Spark engine only) |
| `context.pii_metadata` | `dict[str, bool]` | PII flags per column (if configured) |

### Methods

#### `context.with_df(df) → EngineContext`

Returns a **new** `EngineContext` with the given DataFrame, preserving all other state.
Use this instead of mutating `context.df` directly.

```python
@transform("add_timestamp")
def add_timestamp(context, current):
    import pandas as pd
    current["loaded_at"] = pd.Timestamp.now()
    return current  # return the DataFrame, not the context
```

> **When to use `with_df`:** When you need to chain `context.sql()` calls or pass
> the context onward. For most functions, just return the DataFrame directly.

#### `context.sql(query) → EngineContext`

Execute a SQL query against the current DataFrame. Use `"df"` as the table alias.

```python
@transform("filter_active")
def filter_active(context):
    result = context.sql("SELECT * FROM df WHERE is_active = true")
    return result.df  # extract the DataFrame from the returned context
```

Chain multiple SQL calls:

```python
@transform("multi_step_sql")
def multi_step_sql(context):
    result = (
        context
        .sql("SELECT *, amount * 1.1 AS adjusted FROM df")
        .sql("SELECT * FROM df WHERE adjusted > 100")
    )
    return result.df
```

> ⚠️ **Streaming Limitation:** `context.sql()` does **not** work with Spark Structured
> Streaming DataFrames. It registers temp views internally, which strips hidden columns
> like `_metadata`. Use DataFrame API (e.g., `.withColumn()`) for streaming transforms instead.

#### `context.get(name) → DataFrame`

Retrieve another registered DataFrame from the pipeline by node name.

```python
@transform("join_with_customers")
def join_with_customers(context, current):
    customers = context.get("dim_customers")
    return current.merge(customers, on="customer_id", how="left")
```

#### `context.register_temp_view(name, df)`

Register a DataFrame as a named view so it can be referenced in subsequent SQL calls.

```python
@transform("complex_join")
def complex_join(context, current):
    rates = context.get("exchange_rates")
    context.register_temp_view("rates", rates)

    result = context.sql("""
        SELECT df.*, rates.rate
        FROM df
        JOIN rates ON df.currency = rates.currency_code
    """)
    return result.df
```

---

## Passing Parameters from YAML

Any keyword argument beyond `context` and `current` is treated as a parameter.
Pass values from your YAML config using the `params:` key:

### Python

```python
@transform("apply_discount")
def apply_discount(context, current, discount_pct: float = 0.0):
    current["final_price"] = current["price"] * (1 - discount_pct)
    return current
```

### YAML

```yaml
transform:
  steps:
    - function: apply_discount
      params:
        discount_pct: 0.15
```

Parameters with default values are optional in YAML. Parameters without defaults
are **required** — Odibi will raise a `ValueError` if they're missing.

---

## Parameter Validation with Pydantic

For functions with complex parameters, define a Pydantic model and pass it to
`@transform`. Odibi will validate the YAML params against the model before calling
your function.

```python
from pydantic import BaseModel, Field
from odibi import transform

class AggParams(BaseModel):
    group_by: list[str]
    metrics: dict[str, str]  # column -> agg function
    min_count: int = Field(default=1, ge=1)

@transform(name="flexible_aggregate", param_model=AggParams)
def flexible_aggregate(context, current, group_by, metrics, min_count=1):
    result = current.groupby(group_by).agg(**metrics)
    return result[result["count"] >= min_count] if "count" in result.columns else result
```

```yaml
transform:
  steps:
    - function: flexible_aggregate
      params:
        group_by: [region, product_line]
        metrics:
          revenue: sum
          orders: count
        min_count: 5
```

If a user passes invalid params (e.g., `min_count: -1`), Odibi raises a clear
validation error before execution even begins.

---

## Transform Step Types

Each step in `transform.steps` supports exactly one of four types:

| Type | Description | Example |
| :--- | :--- | :--- |
| `sql` | Inline SQL query | `sql: "SELECT * FROM df WHERE x > 0"` |
| `sql_file` | Path to external `.sql` file | `sql_file: sql/transform.sql` |
| `function` | Registered Python function | `function: clean_status` |
| `operation` | Built-in operation | `operation: drop_duplicates` |

### Mix-and-Match Example

Steps execute in order — each step receives the output of the previous one:

```yaml
transform:
  steps:
    # Step 1: Filter with SQL
    - sql: "SELECT * FROM df WHERE status = 'ACTIVE'"

    # Step 2: Custom Python logic
    - function: calculate_lifetime_value
      params:
        discount_rate: 0.05

    # Step 3: Deduplicate with built-in operation
    - operation: drop_duplicates
      params:
        subset: [customer_id]

    # Step 4: Final shaping with SQL
    - sql: |
        SELECT customer_id, lifetime_value, segment
        FROM df
        ORDER BY lifetime_value DESC
```

---

## Recipes

### Recipe 1: Conditional Column Logic

```python
@transform("flag_high_value")
def flag_high_value(context, current, threshold: float = 1000.0):
    """Flag orders above a threshold."""
    current["is_high_value"] = current["amount"] > threshold
    return current
```

```yaml
- function: flag_high_value
  params:
    threshold: 5000.0
```

### Recipe 2: Multi-Dataset Join

```python
@transform("enrich_orders")
def enrich_orders(context, current, customer_node: str, product_node: str):
    """Join orders with customer and product dimensions."""
    customers = context.get(customer_node)
    products = context.get(product_node)

    result = (
        current
        .merge(customers[["customer_id", "name", "segment"]], on="customer_id", how="left")
        .merge(products[["product_id", "category"]], on="product_id", how="left")
    )
    return result
```

```yaml
nodes:
  - name: enriched_orders
    depends_on: [raw_orders, dim_customers, dim_products]
    transform:
      steps:
        - function: enrich_orders
          params:
            customer_node: dim_customers
            product_node: dim_products
```

### Recipe 3: Spark Streaming — Add Source File Name

`context.sql()` doesn't work with streaming DataFrames. Use the DataFrame API instead:

```python
from pyspark.sql import functions as F
from odibi import transform

@transform("add_source_file")
def add_source_file(context, current):
    """Add the source file path from Auto Loader's _metadata column."""
    result = current.withColumn("source_file", F.col("_metadata.file_path"))
    return result
```

```yaml
- function: add_source_file
```

### Recipe 4: Engine-Aware Function

```python
@transform("normalize_text")
def normalize_text(context, current, column: str):
    """Normalize a text column — works on any engine."""
    if context.engine_type == "pandas":
        current[column] = current[column].str.strip().str.lower()
    elif context.engine_type == "spark":
        from pyspark.sql import functions as F
        current = current.withColumn(column, F.lower(F.trim(F.col(column))))
    elif context.engine_type == "polars":
        import polars as pl
        current = current.with_columns(
            pl.col(column).str.strip_chars().str.to_lowercase()
        )
    return current
```

### Recipe 5: SQL Chaining via context.sql()

```python
@transform("staged_aggregation")
def staged_aggregation(context):
    """Multi-step SQL — each .sql() feeds the next."""
    result = (
        context
        .sql("""
            SELECT region, product, SUM(revenue) AS total_revenue
            FROM df
            GROUP BY region, product
        """)
        .sql("""
            SELECT region, COUNT(*) AS product_count, SUM(total_revenue) AS region_revenue
            FROM df
            GROUP BY region
        """)
    )
    return result.df
```

### Recipe 6: Using PII Metadata

```python
@transform("mask_pii")
def mask_pii(context, current):
    """Mask columns flagged as PII."""
    for col, is_pii in context.pii_metadata.items():
        if is_pii and col in current.columns:
            current[col] = "***MASKED***"
    return current
```

---

## Engine-Aware Functions

Your function receives `context.engine_type` which is one of `"spark"`, `"pandas"`,
or `"polars"`. Use this to write functions that work across all engines:

```python
@transform("add_row_hash")
def add_row_hash(context, current, columns: list):
    """Add a hash column for change detection — works on any engine."""
    if context.engine_type == "pandas":
        import hashlib
        current["row_hash"] = current[columns].astype(str).agg("-".join, axis=1).apply(
            lambda x: hashlib.md5(x.encode()).hexdigest()
        )
    elif context.engine_type == "spark":
        from pyspark.sql.functions import md5, concat_ws, col
        current = current.withColumn("row_hash", md5(concat_ws("-", *[col(c) for c in columns])))
    elif context.engine_type == "polars":
        import polars as pl
        current = current.with_columns(
            pl.concat_str([pl.col(c).cast(pl.Utf8) for c in columns], separator="-")
            .hash()
            .alias("row_hash")
        )
    return current
```

---

## Gotchas & Tips

### ✅ Do

- **Return a DataFrame** — always. This is what the next step receives.
- **Use `context.with_df(df)`** when chaining `context.sql()` calls.
- **Add type hints** to parameters for readability and IDE support.
- **Use `depends_on`** in YAML when your function calls `context.get("other_node")`.
- **Keep functions small** — one function per transformation concern.

### ❌ Don't

- **Don't mutate `context.df` directly** — return a new DataFrame instead.
- **Don't use `context.sql()` with streaming DataFrames** — temp views strip
  hidden columns like `_metadata`. Use the DataFrame API.
- **Don't forget to list dependencies** — if you call `context.get("x")`,
  add `x` to `depends_on` in the YAML node, or it may not be ready yet.
- **Don't use `.replace()` on context** — use `context.with_df(df)` to get a
  new context with an updated DataFrame.

### Debugging Tips

```python
@transform("debug_inspect")
def debug_inspect(context, current):
    """Temporary debug function — inspect the data mid-pipeline."""
    print(f"Engine: {context.engine_type}")
    print(f"Columns: {context.columns}")
    print(f"Schema: {context.schema}")
    print(f"Row count: {len(current) if hasattr(current, '__len__') else 'N/A'}")
    if context.engine_type == "pandas":
        print(current.head())
    elif context.engine_type == "spark":
        current.show(5)
    return current  # pass through unchanged
```

---

## See Also

- [Writing Transformations](./writing_transformations.md) — Introductory guide with SQL examples
- [Best Practices](./best_practices.md) — Code organization guidelines
- [YAML Schema Reference](../reference/yaml_schema.md) — Full configuration reference
- [Recipes](./recipes.md) — More pipeline recipes
