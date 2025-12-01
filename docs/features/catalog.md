# System Catalog

Centralized governance and metadata management for pipelines, execution history, schema evolution, and lineage tracking.

## Overview

Odibi's System Catalog ("The Brain") provides:
- **Pipeline Registry**: Track pipeline and node definitions with version hashing
- **Execution History**: Complete run history with metrics and duration
- **State Management**: High-water marks (HWM) for incremental processing
- **Schema Evolution**: Automatic tracking of schema changes over time
- **Lineage Tracking**: Table-level upstream/downstream relationships
- **Pattern Compliance**: Track medallion architecture adherence

## Configuration

### Basic Catalog Setup

```yaml
system:
  connection: system_storage
  path: _odibi_system

connections:
  system_storage:
    type: adls
    account: "${STORAGE_ACCOUNT}"
    container: metadata
```

### System Config Options

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `connection` | string | Yes | Connection name for catalog storage |
| `path` | string | No | Subdirectory for catalog tables (default: `_odibi_system`) |

## Catalog Tables

The System Catalog consists of Delta tables that automatically bootstrap on first run:

### meta_pipelines

Tracks pipeline definitions and deployment versions.

| Column | Type | Description |
|--------|------|-------------|
| `pipeline_name` | string | Unique pipeline identifier |
| `version_hash` | string | MD5 hash of pipeline configuration |
| `description` | string | Pipeline description |
| `layer` | string | Medallion layer (bronze/silver/gold) |
| `schedule` | string | Cron schedule (if defined) |
| `tags_json` | string | JSON array of aggregated tags |
| `updated_at` | timestamp | Last deployment timestamp |

### meta_nodes

Tracks node configurations within pipelines.

| Column | Type | Description |
|--------|------|-------------|
| `pipeline_name` | string | Parent pipeline name |
| `node_name` | string | Unique node identifier |
| `version_hash` | string | MD5 hash of node configuration |
| `type` | string | Node type: read/transform/write |
| `config_json` | string | Full node configuration as JSON |
| `updated_at` | timestamp | Last deployment timestamp |

### meta_runs

Execution history with metrics. Partitioned by `pipeline_name` and `date`.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | string | Unique execution identifier |
| `pipeline_name` | string | Pipeline name |
| `node_name` | string | Node name |
| `status` | string | SUCCESS, FAILED, RUNNING |
| `rows_processed` | long | Number of rows processed |
| `duration_ms` | long | Execution time in milliseconds |
| `metrics_json` | string | Additional metrics as JSON |
| `timestamp` | timestamp | Execution timestamp |
| `date` | date | Partition date |

### meta_state

High-water mark (HWM) storage for incremental processing. Partitioned by `pipeline_name`.

| Column | Type | Description |
|--------|------|-------------|
| `pipeline_name` | string | Pipeline name |
| `node_name` | string | Node name |
| `hwm_value` | string | Serialized high-water mark value |

### meta_patterns

Tracks pattern compliance for governance.

| Column | Type | Description |
|--------|------|-------------|
| `table_name` | string | Table identifier |
| `pattern_type` | string | Pattern type (SCD2, append, etc.) |
| `configuration` | string | Pattern configuration as JSON |
| `compliance_score` | double | Compliance score (0.0 - 1.0) |

### meta_schemas

Schema version history for drift detection.

| Column | Type | Description |
|--------|------|-------------|
| `table_path` | string | Full table path |
| `schema_version` | long | Incrementing version number |
| `schema_hash` | string | MD5 hash of column definitions |
| `columns` | string | JSON: {"column": "type", ...} |
| `captured_at` | timestamp | When schema was captured |
| `pipeline` | string | Pipeline that wrote the schema |
| `node` | string | Node that wrote the schema |
| `run_id` | string | Execution run ID |
| `columns_added` | array | New columns in this version |
| `columns_removed` | array | Removed columns |
| `columns_type_changed` | array | Columns with type changes |

### meta_lineage

Cross-pipeline table lineage relationships.

| Column | Type | Description |
|--------|------|-------------|
| `source_table` | string | Source table path |
| `target_table` | string | Target table path |
| `source_pipeline` | string | Source pipeline (if known) |
| `source_node` | string | Source node (if known) |
| `target_pipeline` | string | Target pipeline |
| `target_node` | string | Target node |
| `relationship` | string | "feeds" or "derived_from" |
| `last_observed` | timestamp | Last time relationship was seen |
| `run_id` | string | Execution run ID |

## Features

### Pipeline Registration

Pipelines are automatically registered on deployment. The version hash detects configuration drift:

```python
from odibi.catalog import CatalogManager

# Automatic registration during pipeline execution
catalog.register_pipeline(pipeline_config)
```

When a pipeline's configuration changes, the `version_hash` updates, providing:
- Configuration drift detection
- Deployment history tracking
- Audit trail for changes

### Schema Tracking

Automatic schema evolution tracking with change detection:

```python
# Track schema for a table
result = catalog.track_schema(
    table_path="silver/customers",
    schema={"id": "long", "name": "string", "email": "string"},
    pipeline="customer_pipeline",
    node="transform_customers",
    run_id="run_123"
)

# Result includes change details
# {
#   "changed": True,
#   "version": 2,
#   "previous_version": 1,
#   "columns_added": ["email"],
#   "columns_removed": [],
#   "columns_type_changed": []
# }

# Get schema history
history = catalog.get_schema_history("silver/customers", limit=10)
```

### Lineage Tracking

Track table-level data lineage:

```python
# Record a lineage relationship
catalog.record_lineage(
    source_table="bronze/raw_orders",
    target_table="silver/orders",
    target_pipeline="orders_pipeline",
    target_node="transform_orders",
    run_id="run_456",
    relationship="feeds"
)

# Get upstream sources
upstream = catalog.get_upstream("gold/order_summary", depth=3)

# Get downstream consumers
downstream = catalog.get_downstream("bronze/raw_orders", depth=3)
```

### Run History and Metrics

Query execution history for observability:

```python
# Get average duration for a node (last 7 days)
avg_seconds = catalog.get_average_duration("transform_orders", days=7)

# Direct table access for custom queries
runs_df = spark.read.format("delta").load(catalog.tables["meta_runs"])
```

### Catalog Optimization

Maintenance operations for Spark deployments:

```python
# Run VACUUM and OPTIMIZE on meta_runs
catalog.optimize()
```

## CatalogManager API

### Initialization

```python
from odibi.catalog import CatalogManager
from odibi.config import SystemConfig

catalog = CatalogManager(
    spark=spark_session,           # SparkSession (or None for Pandas)
    config=system_config,          # SystemConfig object
    base_path="abfss://...",       # Resolved catalog path
    engine=pandas_engine           # Optional: for Pandas mode
)
```

### Key Methods

| Method | Description |
|--------|-------------|
| `bootstrap()` | Create all system tables if missing |
| `register_pipeline(config)` | Register/update pipeline definition |
| `register_nodes(config)` | Register/update node definitions |
| `log_run(...)` | Record execution run |
| `track_schema(...)` | Track schema version |
| `get_schema_history(table, limit)` | Get schema version history |
| `record_lineage(...)` | Record table lineage relationship |
| `get_upstream(table, depth)` | Get upstream dependencies |
| `get_downstream(table, depth)` | Get downstream consumers |
| `get_average_duration(node, days)` | Get average node duration |
| `optimize()` | Run VACUUM and OPTIMIZE (Spark only) |

## CLI Integration

Query the catalog from the command line:

### List Execution Runs

```bash
# Recent runs
odibi catalog runs config.yaml

# Filter by pipeline, status, and time range
odibi catalog runs config.yaml --pipeline orders_pipeline --status FAILED --days 3

# JSON output
odibi catalog runs config.yaml --format json --limit 50
```

### List Registered Pipelines

```bash
odibi catalog pipelines config.yaml
odibi catalog pipelines config.yaml --format json
```

### List Registered Nodes

```bash
odibi catalog nodes config.yaml
odibi catalog nodes config.yaml --pipeline orders_pipeline
```

### View HWM State

```bash
odibi catalog state config.yaml
odibi catalog state config.yaml --pipeline orders_pipeline
```

### List Registered Assets

```bash
odibi catalog tables config.yaml
odibi catalog tables config.yaml --project MyProject
```

### View Execution Statistics

```bash
# Statistics for last 7 days
odibi catalog stats config.yaml

# Filter by pipeline and time range
odibi catalog stats config.yaml --pipeline orders_pipeline --days 30
```

Output includes:
- Total runs, success/failure counts
- Success rate percentage
- Total and average rows processed
- Average and total runtime
- Runs by pipeline
- Most failed nodes

### CLI Options

| Command | Options |
|---------|---------|
| `runs` | `--pipeline`, `--node`, `--status`, `--days`, `--limit`, `--format` |
| `pipelines` | `--format` |
| `nodes` | `--pipeline`, `--format` |
| `state` | `--pipeline`, `--format` |
| `tables` | `--project`, `--format` |
| `metrics` | `--format` |
| `patterns` | `--format` |
| `stats` | `--pipeline`, `--days` |

## Complete Example

### Project Configuration

```yaml
project: SalesAnalytics
engine: spark

system:
  connection: catalog_storage
  path: _odibi_catalog

connections:
  catalog_storage:
    type: adls
    account: "${STORAGE_ACCOUNT}"
    container: metadata
    
  bronze:
    type: adls
    account: "${STORAGE_ACCOUNT}"
    container: bronze
    
  silver:
    type: adls
    account: "${STORAGE_ACCOUNT}"
    container: silver

pipelines:
  - pipeline: orders_bronze_to_silver
    description: "Transform raw orders to silver layer"
    layer: silver
    nodes:
      - name: read_raw_orders
        type: read
        connection: bronze
        path: raw/orders
        format: delta
        
      - name: transform_orders
        type: transform
        input: read_raw_orders
        transform: |
          SELECT 
            order_id,
            customer_id,
            order_date,
            total_amount
          FROM {input}
          WHERE order_date >= '2024-01-01'
          
      - name: write_orders
        type: write
        input: transform_orders
        connection: silver
        path: orders
        format: delta
        mode: merge
        merge_keys: [order_id]
```

### Querying the Catalog

```bash
# Check registered pipelines
odibi catalog pipelines config.yaml

# Output:
# pipeline_name            | layer  | description                          | version_hash | updated_at
# -------------------------+--------+--------------------------------------+--------------+--------------------
# orders_bronze_to_silver  | silver | Transform raw orders to silver layer | a1b2c3d4...  | 2024-01-30 10:15:00

# View execution history
odibi catalog runs config.yaml --pipeline orders_bronze_to_silver --days 7

# Get statistics
odibi catalog stats config.yaml --pipeline orders_bronze_to_silver

# Output:
# === Execution Statistics (Last 7 Days) ===
# 
# Total Runs:     42
# Successful:     40
# Failed:         2
# Success Rate:   95.2%
# 
# Total Rows:     1,250,000
# Avg Rows/Run:   29,762
# 
# Avg Duration:   12.45s
# Total Runtime:  522.90s
```

### Programmatic Access

```python
from odibi.pipeline import PipelineManager

# Load configuration
manager = PipelineManager.from_yaml("config.yaml")
catalog = manager.catalog_manager

# Query schema history
history = catalog.get_schema_history("silver/orders")
for version in history:
    print(f"v{version['schema_version']}: {version['columns_added']} added")

# Trace lineage
upstream = catalog.get_upstream("gold/order_summary")
for source in upstream:
    print(f"  {'  ' * source['depth']}{source['source_table']}")
```

## Best Practices

1. **Enable catalog early** - Configure the system catalog from project start
2. **Use descriptive names** - Pipeline and node names become permanent identifiers
3. **Monitor statistics** - Regular `odibi catalog stats` reveals performance trends
4. **Review schema changes** - Track breaking changes before they impact downstream
5. **Query lineage** - Understand impact before modifying source tables
6. **Run optimization** - Periodically run `catalog.optimize()` for Spark deployments

## Related

- [Pipeline Configuration](../reference/yaml_schema.md) - YAML schema reference
- [Incremental Processing](incremental.md) - HWM-based incremental loads
- [Alerting](alerting.md) - Notifications for pipeline events
