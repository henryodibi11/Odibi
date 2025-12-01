# Execution Engines

Multi-engine architecture for flexible data processing across local development, high-performance workloads, and big data environments.

## Overview

Odibi's engine system provides:
- **Multiple backends**: Pandas, Spark, Polars
- **Unified API**: Consistent interface across engines
- **Automatic selection**: Choose based on workload and environment
- **Performance tuning**: Engine-specific optimizations

## Supported Engines

| Engine | Best For | Dependencies |
|--------|----------|--------------|
| `pandas` | Local development, small datasets (<1GB) | `pip install odibi` |
| `spark` | Big data, Databricks, distributed processing | `pip install odibi[spark]` |
| `polars` | High-performance local processing, medium datasets | `pip install polars` |

### PandasEngine

Default engine for local development with broad format support.

**Strengths:**
- Extensive format support (CSV, Parquet, JSON, Excel, Avro, Delta)
- Rich ecosystem integration
- Familiar API for data scientists
- SQL support via DuckDB (optional)

**Best for:**
- Local development and testing
- Small to medium datasets (<1GB)
- Complex transformations with pandas operations

### SparkEngine

Distributed processing engine for big data workloads.

**Strengths:**
- Horizontal scalability
- Native Databricks integration
- Delta Lake support with ACID transactions
- Streaming pipelines
- Multi-account ADLS support

**Best for:**
- Large datasets (>1GB)
- Production Databricks workflows
- Distributed processing
- Real-time streaming

### PolarsEngine

High-performance engine with lazy evaluation.

**Strengths:**
- Extremely fast (Rust-based)
- Memory efficient with lazy execution
- Multi-threaded by default
- Native scan operations (scan_csv, scan_parquet)

**Best for:**
- High-performance local processing
- Medium to large datasets (1GB-10GB)
- CPU-bound transformations

## Configuration

### Basic Engine Setup

```yaml
project: DataPipeline
engine: pandas  # or spark, polars

connections:
  # ...

pipelines:
  # ...
```

### Engine Options

| Field | Type | Description |
|-------|------|-------------|
| `engine` | string | Engine type: `pandas`, `spark`, `polars` |
| `performance` | object | Performance tuning options |

## Engine Selection Guide

| Scenario | Recommended Engine |
|----------|-------------------|
| Local development | `pandas` |
| Unit testing | `pandas` |
| Databricks production | `spark` |
| Large datasets (>1GB) | `spark` or `polars` |
| CPU-bound local processing | `polars` |
| Streaming pipelines | `spark` |
| Quick prototyping | `pandas` |

## Engine API

All engines implement the same core interface defined in `Engine` base class.

### Core Methods

| Method | Description |
|--------|-------------|
| `read()` | Read data from source |
| `write()` | Write data to destination |
| `execute_sql()` | Execute SQL query |
| `execute_operation()` | Execute built-in operation (pivot, sort, etc.) |
| `get_schema()` | Get DataFrame schema |
| `get_shape()` | Get DataFrame dimensions |
| `count_rows()` | Count rows in DataFrame |
| `count_nulls()` | Count nulls in specified columns |

### Data Operations

| Method | Description |
|--------|-------------|
| `validate_schema()` | Validate DataFrame schema against rules |
| `validate_data()` | Validate data against validation config |
| `get_sample()` | Get sample rows as dictionaries |
| `profile_nulls()` | Calculate null percentage per column |
| `harmonize_schema()` | Match DataFrame to target schema |
| `anonymize()` | Anonymize columns (hash, mask, redact) |

### Table Operations

| Method | Description |
|--------|-------------|
| `table_exists()` | Check if table/path exists |
| `get_table_schema()` | Get schema of existing table |
| `maintain_table()` | Run maintenance (optimize, vacuum) |
| `materialize()` | Materialize lazy dataset into memory |

### Custom Format Support

```python
from odibi.engine import PandasEngine

def read_netcdf(path, **options):
    import xarray as xr
    return xr.open_dataset(path).to_dataframe()

def write_netcdf(df, path, **options):
    import xarray as xr
    xr.Dataset.from_dataframe(df).to_netcdf(path)

PandasEngine.register_format("netcdf", reader=read_netcdf, writer=write_netcdf)
```

## Performance Configuration

### Pandas Performance

```yaml
engine: pandas
performance:
  use_arrow: true    # Use PyArrow backend (faster, less memory)
  use_duckdb: false  # Use DuckDB for SQL (experimental)
```

Arrow backend benefits:
- Faster I/O for Parquet files
- Reduced memory usage
- Better type preservation

### Spark Performance

```yaml
engine: spark
```

Spark is automatically configured with:
- Arrow-based PySpark conversions
- Adaptive Query Execution (AQE)
- Dynamic partition overwrite mode

Additional optimizations via write options:
```yaml
pipelines:
  - pipeline: optimize_example
    nodes:
      - name: write_optimized
        write:
          connection: silver
          format: delta
          path: optimized_table
          options:
            optimize_write: true
            zorder_by: [customer_id, date]
```

### Polars Performance

```yaml
engine: polars
```

Polars features:
- Lazy evaluation by default (scan operations)
- Automatic query optimization
- Multi-threaded execution
- Streaming writes (sink operations)

## Examples

### Switching Engines

Same pipeline, different engines:

```yaml
# Local development
project: DataPipeline
engine: pandas

connections:
  bronze:
    type: local
    path: ./data/bronze

pipelines:
  - pipeline: process_orders
    nodes:
      - name: read_orders
        read:
          connection: bronze
          format: csv
          path: orders.csv
```

```yaml
# Production (Databricks)
project: DataPipeline
engine: spark

connections:
  bronze:
    type: azure_adls
    storage_account: "${STORAGE_ACCOUNT}"
    container: bronze

pipelines:
  - pipeline: process_orders
    nodes:
      - name: read_orders
        read:
          connection: bronze
          format: delta
          path: orders
```

### Using Different Connections

```yaml
project: MultiSource
engine: spark

connections:
  raw_data:
    type: azure_adls
    storage_account: rawstorage
    container: raw

  processed:
    type: azure_adls
    storage_account: procstorage
    container: silver

  sql_source:
    type: azure_sql
    server: myserver.database.windows.net
    database: mydb

pipelines:
  - pipeline: ingest_sql
    nodes:
      - name: read_sql
        read:
          connection: sql_source
          format: sql
          table: dbo.customers

      - name: write_delta
        write:
          connection: processed
          format: delta
          path: customers
```

### Lazy vs Eager Execution

```yaml
# Polars with lazy execution (default)
engine: polars

pipelines:
  - pipeline: lazy_example
    nodes:
      - name: scan_data
        read:
          connection: bronze
          format: parquet
          path: large_dataset/*.parquet
        # Returns LazyFrame - no data loaded yet

      - name: filter_transform
        sql: |
          SELECT * FROM scan_data WHERE status = 'active'
        # Still lazy - builds query plan

      - name: write_result
        write:
          connection: silver
          format: parquet
          path: filtered_data
        # Execution happens here (sink_parquet)
```

### Engine-Specific Features

**Pandas with Delta Time Travel:**
```yaml
engine: pandas

pipelines:
  - pipeline: time_travel
    nodes:
      - name: read_historical
        read:
          connection: bronze
          format: delta
          path: orders
          options:
            versionAsOf: 5  # Read version 5
```

**Spark with Streaming:**
```yaml
engine: spark

pipelines:
  - pipeline: streaming_ingest
    nodes:
      - name: stream_read
        read:
          connection: bronze
          format: delta
          path: events
          streaming: true
```

## Programmatic Engine Usage

```python
from odibi.engine import get_engine_class

# Get engine by name
EngineClass = get_engine_class("pandas")
engine = EngineClass(connections=my_connections)

# Read data
df = engine.read(
    connection=my_connection,
    format="parquet",
    path="data/*.parquet"
)

# Execute SQL
from odibi.context import PandasContext
ctx = PandasContext()
ctx.register("orders", df)
result = engine.execute_sql("SELECT * FROM orders WHERE total > 100", ctx)

# Write data
engine.write(
    df=result,
    connection=output_connection,
    format="delta",
    path="filtered_orders",
    mode="overwrite"
)
```

### Register Custom Engine

```python
from odibi.engine import Engine, register_engine

class DuckDBEngine(Engine):
    name = "duckdb"

    def read(self, connection, format, **kwargs):
        # Custom implementation
        pass

    # Implement other required methods...

register_engine("duckdb", DuckDBEngine)
```

## Best Practices

1. **Match engine to workload** - Use pandas for development, spark for production
2. **Use lazy execution** - Polars and Spark defer computation until needed
3. **Enable Arrow** - Faster I/O and reduced memory for Pandas
4. **Partition large tables** - Use `partition_by` for write performance
5. **Run maintenance** - Enable auto-optimize for Delta tables
6. **Test locally first** - Develop with pandas, deploy with spark

## Related

- [Connections](connections.md) - Data source configuration
- [Pipelines](pipelines.md) - Pipeline definition
- [YAML Schema Reference](../reference/yaml_schema.md) - Full configuration options
