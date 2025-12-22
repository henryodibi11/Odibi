# Getting Started with Spark Engine

This tutorial shows how to use Odibi with Apache Spark for large-scale data processing.

## Prerequisites

- Odibi installed (`pip install odibi`)
- Apache Spark 3.x installed or access to Databricks/Synapse
- Basic familiarity with [Getting Started](getting_started.md)

## Why Spark?

| Use Case | Recommended Engine |
|----------|-------------------|
| Small datasets (<1GB) | `pandas` |
| Medium datasets (1-10GB) | `polars` |
| Large datasets (>10GB) | `spark` |
| Streaming data | `spark` |
| Delta Lake tables | `spark` |

## 1. Configure Spark Engine

Set `engine: spark` in your project configuration:

```yaml
# project.yaml
project: "spark_demo"
engine: spark  # Use Spark instead of Pandas

connections:
  landing:
    type: local
    base_path: /data/landing
  
  bronze:
    type: delta
    catalog: spark_catalog
    schema: bronze

  silver:
    type: delta
    catalog: spark_catalog
    schema: silver

story:
  connection: landing
  path: stories/

system:
  connection: landing
  path: catalog/
```

## 2. Delta Lake Connections

Spark works best with Delta Lake for ACID transactions and time travel:

```yaml
connections:
  # Unity Catalog (Databricks)
  unity_bronze:
    type: delta
    catalog: main
    schema: bronze

  # Hive Metastore
  hive_silver:
    type: delta
    catalog: spark_catalog
    schema: silver

  # Path-based Delta (no catalog)
  adls_gold:
    type: delta
    base_path: abfss://container@account.dfs.core.windows.net/gold
```

## 3. Basic Spark Pipeline

```yaml
# pipelines/bronze/ingest.yaml
pipelines:
  - pipeline: bronze_ingest
    layer: bronze
    nodes:
      - name: raw_orders
        read:
          connection: landing
          format: csv
          path: orders/*.csv
          options:
            header: true
            inferSchema: true
        write:
          connection: bronze
          table: raw_orders
          mode: overwrite
```

## 4. Spark-Specific Features

### Streaming Ingestion

Process real-time data from Kafka or Event Hub:

```yaml
nodes:
  - name: stream_events
    streaming: true  # Enable Spark Structured Streaming
    read:
      connection: event_hub
      format: kafka
      options:
        kafka.bootstrap.servers: "${KAFKA_BROKERS}"
        subscribe: events
        startingOffsets: latest
    write:
      connection: bronze
      table: events
      mode: append
      options:
        checkpointLocation: /checkpoints/events
```

### Delta Optimizations

```yaml
nodes:
  - name: optimize_facts
    read:
      connection: silver
      table: fact_orders
    post_sql:
      - "OPTIMIZE silver.fact_orders ZORDER BY (order_date, customer_sk)"
      - "VACUUM silver.fact_orders RETAIN 168 HOURS"
    write:
      connection: silver
      table: fact_orders
      mode: overwrite
```

### Partition Pruning

```yaml
nodes:
  - name: partitioned_orders
    read:
      connection: bronze
      table: raw_orders
    write:
      connection: silver
      table: orders
      options:
        partitionBy: [order_date]
        replaceWhere: "order_date >= '2024-01-01'"
```

## 5. Performance Tuning

### Spark-Specific Settings

```yaml
performance:
  use_arrow: true              # PyArrow for Pandas UDFs
  default_parallelism: 200     # Spark partitions
  delta_table_properties:
    delta.columnMapping.mode: name
    delta.autoOptimize.optimizeWrite: true
```

### Skip Expensive Operations

For high-throughput Bronze ingestion:

```yaml
performance:
  skip_null_profiling: true    # Skip NULL count (saves 1 Spark job)
  skip_catalog_writes: true    # Skip metadata tracking
  skip_run_logging: true       # Skip run history
```

### Caching Hot DataFrames

```yaml
nodes:
  - name: dim_customer
    cache: true  # Cache in memory for reuse
    read:
      connection: silver
      table: dim_customer
    
  - name: fact_orders
    depends_on: [dim_customer]  # Uses cached dim_customer
    # ...
```

## 6. SCD2 with Spark

Slowly Changing Dimensions work seamlessly with Spark:

```yaml
nodes:
  - name: dim_customer
    read:
      connection: bronze
      table: raw_customers
    pattern:
      type: dimension
      params:
        natural_key: customer_id
        surrogate_key: customer_sk
        scd_type: 2
        track_columns: [name, email, address, city]
        target: silver.dim_customer
        unknown_member: true
    write:
      connection: silver
      table: dim_customer
```

## 7. Running on Databricks

### Option 1: Databricks Asset Bundles

```yaml
# databricks.yml
bundle:
  name: odibi_pipelines

resources:
  jobs:
    daily_pipeline:
      name: "[${bundle.environment}] Daily Pipeline"
      tasks:
        - task_key: run_odibi
          python_wheel_task:
            package_name: odibi
            entry_point: cli
            parameters: ["run", "--config", "project.yaml"]
```

### Option 2: Notebook

```python
# Databricks notebook cell
%pip install odibi

from odibi import run_project

run_project("project.yaml", pipelines=["bronze_ingest"])
```

## 8. Common Issues

### "Java gateway process exited" 

Spark isn't installed or JAVA_HOME not set:
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
export SPARK_HOME=/opt/spark
```

### Out of Memory

Increase driver/executor memory:
```python
spark = SparkSession.builder \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "16g") \
    .getOrCreate()
```

### Slow Small Files

Use Delta's auto-optimize:
```yaml
write:
  options:
    delta.autoOptimize.optimizeWrite: true
    delta.autoOptimize.autoCompact: true
```

## Next Steps

- [Azure Connections](azure_connections.md) - Connect to Azure Blob/ADLS
- [Performance Tuning](../guides/performance_tuning.md) - Optimize large pipelines
- [Dimensional Modeling](dimensional_modeling/) - Build star schemas

## See Also

- [YAML Schema Reference](../reference/yaml_schema.md) - Full configuration options
- [Delta Connection Config](../reference/yaml_schema.md#deltaconnectionconfig) - Delta settings
- [Glossary](../reference/glossary.md) - Terminology reference
