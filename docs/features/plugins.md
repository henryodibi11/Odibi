# Plugins & Extensibility

Extend Odibi with custom connections, transforms, and engines through a flexible plugin system.

## Overview

Odibi's plugin system provides:
- **Connection plugins**: Add custom data source connectors
- **Transform plugins**: Register custom data transformation functions
- **Engine plugins**: Support for different processing engines
- **Auto-discovery**: Automatic loading of `transforms.py` and `plugins.py`
- **Entry points**: Standard Python packaging for distributable plugins

## Plugin Types

### Connection Plugins

Add support for new data sources by registering connection factories:

```python
from odibi.plugins import register_connection_factory

def create_my_connection(name: str, config: dict):
    """Factory function for custom connection."""
    return MyCustomConnection(
        host=config.get("host"),
        port=config.get("port", 5432),
    )

register_connection_factory("my_db", create_my_connection)
```

Once registered, use in YAML config:

```yaml
connections:
  my_source:
    type: my_db
    host: localhost
    port: 5432
```

### Transform Plugins

Register custom data transformation functions using the `@transform` decorator:

```python
from odibi.registry import transform

@transform
def clean_phone_numbers(context, current, country_code="US"):
    """Standardize phone number format."""
    df = current
    # Transform logic here
    return df

@transform("custom_name")
def my_transform(context, current):
    """Transform with custom registration name."""
    return current
```

Use in pipeline YAML:

```yaml
pipelines:
  - pipeline: process_customers
    nodes:
      - name: standardize
        transform: clean_phone_numbers
        params:
          country_code: "UK"
```

### Engine Plugins

Odibi supports multiple processing engines. The engine is specified at the project level:

```yaml
project: MyProject
engine: spark  # or 'pandas', 'polars'
```

## FunctionRegistry

The `FunctionRegistry` is the central registry for transform functions.

### Registration Methods

```python
from odibi.registry import FunctionRegistry, transform

# Method 1: Using decorator
@transform
def my_transform(context, current, param1: str):
    return current

# Method 2: Direct registration
def another_transform(context, current):
    return current

FunctionRegistry.register(another_transform, name="alt_transform")
```

### Registry API

| Method | Description |
|--------|-------------|
| `register(func, name, param_model)` | Register a function with optional name and Pydantic model |
| `get(name)` | Retrieve a registered function |
| `validate_params(name, params)` | Validate parameters against signature |
| `list_functions()` | List all registered function names |
| `get_function_info(name)` | Get function metadata and signature |
| `get_param_model(name)` | Get Pydantic model for parameter validation |

### Parameter Validation

Use Pydantic models for strict parameter validation:

```python
from pydantic import BaseModel
from odibi.registry import transform, FunctionRegistry

class FilterParams(BaseModel):
    column: str
    min_value: float
    max_value: float = 100.0

@transform(param_model=FilterParams)
def filter_range(context, current, column: str, min_value: float, max_value: float = 100.0):
    return current.filter((current[column] >= min_value) & (current[column] <= max_value))

# Validation happens automatically
FunctionRegistry.validate_params("filter_range", {"column": "price", "min_value": 10})
```

## Connection Factory

The connection factory system allows registering custom connection types.

### Built-in Connections

| Type | Description |
|------|-------------|
| `local` | Local filesystem |
| `http` | HTTP/REST endpoints |
| `azure_blob` / `azure_adls` | Azure Blob Storage / ADLS Gen2 |
| `delta` | Delta Lake tables |
| `sql_server` / `azure_sql` | SQL Server / Azure SQL |

### Custom Factory Pattern

```python
from odibi.plugins import register_connection_factory, get_connection_factory

def create_postgres_connection(name: str, config: dict):
    """Create a PostgreSQL connection."""
    from my_connections import PostgresConnection
    
    return PostgresConnection(
        host=config["host"],
        port=config.get("port", 5432),
        database=config["database"],
        username=config.get("username"),
        password=config.get("password"),
    )

# Register the factory
register_connection_factory("postgres", create_postgres_connection)

# Retrieve a factory (if needed)
factory = get_connection_factory("postgres")
```

## Auto-Discovery

Odibi automatically loads extension files from your project directory.

### Supported Files

| File | Purpose |
|------|---------|
| `transforms.py` | Custom transform functions |
| `plugins.py` | Connection factories and other plugins |

### Search Locations

1. **Config directory**: Same directory as your YAML config
2. **Current working directory**: Where you run the CLI

### Example Structure

```
my_project/
├── config.yaml
├── transforms.py      # Auto-loaded
├── plugins.py         # Auto-loaded
└── data/
```

### transforms.py Example

```python
"""Custom transforms for my project."""

from odibi.registry import transform

@transform
def calculate_metrics(context, current, metrics: list):
    """Calculate custom business metrics."""
    df = current
    for metric in metrics:
        df = df.withColumn(f"{metric}_calculated", ...)
    return df

@transform
def apply_business_rules(context, current, rule_set: str):
    """Apply business rules based on rule set name."""
    # Implementation
    return current
```

### plugins.py Example

```python
"""Custom plugins for my project."""

from odibi.plugins import register_connection_factory

def create_snowflake_connection(name, config):
    from snowflake.connector import connect
    # Create connection
    return SnowflakeWrapper(connect(**config))

register_connection_factory("snowflake", create_snowflake_connection)
```

## Plugin Configuration

### Entry Points Setup

For distributable plugins, use Python entry points in `pyproject.toml`:

```toml
[project.entry-points."odibi.connections"]
postgres = "my_plugin.connections:create_postgres_connection"
snowflake = "my_plugin.connections:create_snowflake_connection"
```

Or in `setup.py`:

```python
setup(
    name="odibi-postgres-plugin",
    entry_points={
        "odibi.connections": [
            "postgres = my_plugin.connections:create_postgres_connection",
        ],
    },
)
```

### Entry Point Groups

| Group | Purpose |
|-------|---------|
| `odibi.connections` | Connection factory functions |

### Loading Plugins

Plugins are loaded automatically at startup:

```python
from odibi.plugins import load_plugins

# Called automatically, but can be invoked manually
load_plugins()
```

## Complete Example

### Project Structure

```
my_etl_project/
├── pyproject.toml
├── config.yaml
├── transforms.py
├── plugins.py
└── src/
    └── my_etl/
        ├── __init__.py
        └── connections/
            └── custom.py
```

### transforms.py

```python
"""Project-specific transforms."""

from odibi.registry import transform
from pydantic import BaseModel

class EnrichmentParams(BaseModel):
    lookup_table: str
    key_column: str
    value_columns: list[str]

@transform(param_model=EnrichmentParams)
def enrich_with_lookup(
    context,
    current,
    lookup_table: str,
    key_column: str,
    value_columns: list[str],
):
    """Enrich data with lookup table values."""
    lookup_df = context.read(lookup_table)
    return current.join(
        lookup_df.select(key_column, *value_columns),
        on=key_column,
        how="left",
    )

@transform
def deduplicate(context, current, key_columns: list, order_by: str = None):
    """Remove duplicate rows based on key columns."""
    if order_by:
        from pyspark.sql import Window
        from pyspark.sql.functions import row_number
        
        window = Window.partitionBy(*key_columns).orderBy(order_by)
        return current.withColumn("_rn", row_number().over(window)) \
                      .filter("_rn = 1") \
                      .drop("_rn")
    return current.dropDuplicates(key_columns)
```

### plugins.py

```python
"""Connection plugins."""

from odibi.plugins import register_connection_factory

def create_redis_connection(name: str, config: dict):
    """Redis cache connection."""
    import redis
    
    return redis.Redis(
        host=config.get("host", "localhost"),
        port=config.get("port", 6379),
        db=config.get("db", 0),
        password=config.get("password"),
    )

register_connection_factory("redis", create_redis_connection)
```

### config.yaml

```yaml
project: CustomerETL
engine: spark

connections:
  source:
    type: azure_adls
    account_name: mystorageaccount
    container: raw
  
  cache:
    type: redis
    host: localhost
    port: 6379

pipelines:
  - pipeline: process_customers
    nodes:
      - name: load_customers
        source: source
        path: customers/

      - name: enrich
        input: load_customers
        transform: enrich_with_lookup
        params:
          lookup_table: reference/regions
          key_column: region_code
          value_columns:
            - region_name
            - country

      - name: dedupe
        input: enrich
        transform: deduplicate
        params:
          key_columns:
            - customer_id
          order_by: updated_at desc
```

## Best Practices

1. **Use Pydantic models** - Validate transform parameters with type safety
2. **Keep plugins focused** - One connection type per factory function
3. **Handle imports lazily** - Import heavy dependencies inside factory functions
4. **Log appropriately** - Use `logger.info()` for successful loads
5. **Provide good defaults** - Make configuration optional where sensible
6. **Document parameters** - Use docstrings for transform functions

## Related

- [Connections](connections.md) - Built-in connection types
- [Transformers](transformers.md) - Built-in transform functions
- [Engines](engines.md) - Supported processing engines
- [Configuration](configuration.md) - YAML configuration reference
