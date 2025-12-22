# Context API

The Context API provides access to DataFrames, engine-specific features, and execution state within transforms.

## Overview

Every transform function receives a `context` parameter:

```python
from odibi.registry import transform

@transform
def my_transform(context, current, param1: str):
    # context: ExecutionContext with DataFrame access
    # current: The input DataFrame for this node
    return current
```

## Context Classes

Odibi has engine-specific context implementations:

| Engine | Context Class | Key Features |
|--------|---------------|--------------|
| Pandas | `PandasContext` | In-memory DataFrames |
| Polars | `PolarsContext` | Lazy/eager DataFrames |
| Spark | `SparkContext` | Distributed DataFrames, SQL |

All contexts implement the base `Context` interface.

## Core Methods

### get(name)

Retrieve a DataFrame by node name:

```python
@transform
def join_data(context, current):
    # Get another node's output
    customers = context.get("load_customers")
    return current.join(customers, on="customer_id")
```

### set(name, df)

Register a DataFrame for downstream nodes:

```python
@transform
def split_data(context, current):
    valid = current.filter("is_valid = true")
    invalid = current.filter("is_valid = false")
    
    # Register additional output
    context.set("invalid_records", invalid)
    
    return valid  # Primary output
```

### sql(query)

Execute SQL against registered DataFrames (Spark only):

```python
@transform
def sql_transform(context, current):
    # Register current DataFrame as a view
    context.set("input_data", current)
    
    # Execute SQL
    result = context.sql("""
        SELECT customer_id, SUM(amount) as total
        FROM input_data
        GROUP BY customer_id
    """)
    return result
```

## Engine Context

Access engine-specific features via `engine_context`:

### Spark

```python
@transform
def spark_specific(context, current):
    spark = context.engine_context.spark
    
    # Use Spark session directly
    df = spark.read.parquet("/path/to/data")
    
    # Access catalog
    spark.catalog.listTables()
    
    return current
```

### Pandas

```python
@transform
def pandas_specific(context, current):
    # current is already a pd.DataFrame
    # No special engine context needed
    return current.groupby("category").sum()
```

### Polars

```python
@transform
def polars_specific(context, current):
    # current is a polars DataFrame
    import polars as pl
    
    return current.with_columns(
        pl.col("amount").sum().over("category").alias("category_total")
    )
```

## EngineContext Class

The `EngineContext` provides engine metadata:

```python
class EngineContext:
    engine_type: str          # "pandas", "polars", "spark"
    spark: SparkSession       # Only for Spark engine
    
    @property
    def is_spark(self) -> bool: ...
    
    @property
    def is_pandas(self) -> bool: ...
    
    @property
    def is_polars(self) -> bool: ...
```

## Example: Engine-Agnostic Transform

Write transforms that work on all engines:

```python
@transform
def engine_agnostic(context, current, threshold: float = 100):
    engine = context.engine_context
    
    if engine.is_spark:
        from pyspark.sql import functions as F
        return current.filter(F.col("amount") > threshold)
    
    elif engine.is_polars:
        import polars as pl
        return current.filter(pl.col("amount") > threshold)
    
    else:  # pandas
        return current[current["amount"] > threshold]
```

## Available in Context

| Property | Description |
|----------|-------------|
| `engine_context` | Engine-specific context with `spark`, `engine_type` |
| `config` | Current node configuration |
| `connections` | Connection registry |
| `state_manager` | Access to HWM and run state |

## Related

- [Writing Transformations](../guides/writing_transformations.md) — Transform authoring guide
