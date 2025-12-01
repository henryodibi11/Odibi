# Pipelines

Orchestrate complex data workflows with dependency-aware execution, parallel processing, and intelligent error handling.

## Overview

Odibi's pipeline system provides:
- **DAG-based execution**: Automatic dependency resolution with cycle detection
- **Parallel processing**: Execute independent nodes concurrently
- **Error strategies**: Fine-grained control over failure behavior
- **Resume capability**: Skip successfully completed nodes on retry
- **Drift detection**: Compare local config against deployed definitions

### Pipeline vs PipelineManager

| Component | Purpose |
|-----------|---------|
| `Pipeline` | Executes a single pipeline (nodes, graph, engine) |
| `PipelineManager` | Manages multiple pipelines from a YAML config file |

## Core Concepts

### Pipeline

The `Pipeline` class is the executor and orchestrator for a single pipeline. It:
- Builds a dependency graph from node configurations
- Resolves execution order via topological sort
- Manages the execution engine (Pandas or Spark)
- Generates execution stories for observability

### Node

A `Node` is the atomic unit of work. Each node follows a four-phase execution pattern:

```
Read → Transform → Validate → Write
```

| Phase | Description |
|-------|-------------|
| **Read** | Load data from a connection (file, table, API) |
| **Transform** | Apply transformations (SQL, functions, patterns) |
| **Validate** | Run data quality tests, quarantine bad rows |
| **Write** | Persist output to a connection |

### DependencyGraph

The `DependencyGraph` class builds and validates the DAG:

| Feature | Description |
|---------|-------------|
| Missing dependency check | Fails if `depends_on` references undefined nodes |
| Cycle detection | Detects circular dependencies before execution |
| Topological sort | Returns nodes in valid execution order |
| Execution layers | Groups independent nodes for parallel execution |

### PipelineResults

Execution results are captured in `PipelineResults`:

| Field | Type | Description |
|-------|------|-------------|
| `pipeline_name` | string | Name of the executed pipeline |
| `completed` | list | Successfully completed node names |
| `failed` | list | Failed node names |
| `skipped` | list | Skipped node names (dependency failures) |
| `node_results` | dict | Detailed `NodeResult` per node |
| `duration` | float | Total execution time in seconds |
| `story_path` | string | Path to generated execution story |

## Configuration

### YAML Structure

```yaml
project: MyDataPipeline
engine: spark  # or 'pandas'

connections:
  bronze:
    type: local
    path: ./data/bronze
  silver:
    type: local
    path: ./data/silver

pipelines:
  - pipeline: bronze_to_silver
    nodes:
      - name: load_orders
        read:
          connection: bronze
          path: orders.parquet
          format: parquet

      - name: clean_orders
        depends_on: [load_orders]
        transform:
          steps:
            - function: drop_nulls
              params:
                columns: [order_id, customer_id]

      - name: write_orders
        depends_on: [clean_orders]
        write:
          connection: silver
          path: orders_clean.parquet
          format: parquet
          mode: overwrite
```

### Pipeline Config Options

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `pipeline` | string | Yes | Unique pipeline name |
| `nodes` | list | Yes | List of node configurations |

### Node Config Options

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique node name within pipeline |
| `depends_on` | list | No | List of upstream node names |
| `read` | object | No | Read configuration |
| `transform` | object | No | Transform steps configuration |
| `validation` | object | No | Data quality tests |
| `write` | object | No | Write configuration |
| `on_error` | string | No | Error strategy: `fail_fast`, `fail_later`, `ignore` |
| `cache` | bool | No | Cache output in memory |

## Execution Modes

### Serial vs Parallel

```python
# Serial execution (default)
results = manager.run()

# Parallel execution with 4 workers
results = manager.run(parallel=True, max_workers=4)
```

In parallel mode, nodes are grouped into execution layers. Nodes within the same layer have no dependencies on each other and execute concurrently.

### Dry Run Mode

Simulate execution without performing actual read/write operations:

```python
results = manager.run(dry_run=True)
```

Dry run validates:
- Configuration syntax
- Dependency graph structure
- Connection availability

### Resume from Failure

Skip nodes that completed successfully in the previous run:

```python
results = manager.run(resume_from_failure=True)
```

Resume capability:
- Tracks node version hashes to detect config changes
- Restores output from persisted writes
- Invalidates downstream nodes when upstream re-executes

### Error Strategies

Control how the pipeline handles node failures:

| Strategy | Behavior |
|----------|----------|
| `fail_fast` | Stop immediately on first failure |
| `fail_later` | Complete current layer, then stop |
| `ignore` | Log error and continue execution |

```yaml
nodes:
  - name: optional_enrichment
    on_error: ignore  # Continue even if this fails
    # ...
```

Override at runtime:

```python
results = manager.run(on_error="fail_fast")
```

## Features

### Dependency Resolution

The pipeline automatically determines execution order:

```python
# Get execution order
order = pipeline.graph.topological_sort()
# ['load_orders', 'clean_orders', 'write_orders']

# Visualize the graph
print(pipeline.visualize())
```

Output:
```
Dependency Graph:

Layer 1:
  - load_orders

Layer 2:
  - clean_orders (depends on: load_orders)

Layer 3:
  - write_orders (depends on: clean_orders)
```

### Execution Layers

For parallel execution, nodes are grouped into layers:

```python
layers = pipeline.get_execution_layers()
# [['load_orders'], ['clean_orders'], ['write_orders']]
```

Nodes in the same layer can run simultaneously.

### Drift Detection

When a System Catalog is configured, the pipeline detects drift between local and deployed configurations:

```
⚠️ DRIFT DETECTED: Local pipeline definition differs from Catalog.
   Local Hash: a1b2c3d4
   Catalog Hash: e5f6g7h8
   Advice: Deploy changes using 'odibi deploy' before running in production.
```

Deploy to sync:

```python
manager.deploy()  # Deploy all pipelines
manager.deploy("bronze_to_silver")  # Deploy specific pipeline
```

### Lineage Integration

Pipelines automatically emit OpenLineage events when configured:

```yaml
lineage:
  enabled: true
  backend: file
  path: ./lineage
```

Events include:
- Pipeline start/complete
- Node start/complete
- Input/output datasets
- Schema information

## API Examples

### Create from YAML

```python
from odibi.pipeline import Pipeline, PipelineManager

# Recommended: Use Pipeline.from_yaml() for convenience
manager = Pipeline.from_yaml("config.yaml")

# Or directly use PipelineManager
manager = PipelineManager.from_yaml("config.yaml")

# With environment overrides
manager = PipelineManager.from_yaml("config.yaml", env="prod")
```

### Run Pipelines

```python
# Run all pipelines
results = manager.run()

# Run specific pipeline
results = manager.run("bronze_to_silver")

# Run multiple pipelines
results = manager.run(["bronze_to_silver", "silver_to_gold"])

# Run with options
results = manager.run(
    parallel=True,
    max_workers=8,
    dry_run=False,
    resume_from_failure=True,
    on_error="fail_fast"
)
```

### Check Results

```python
# Single pipeline returns PipelineResults
if not results.failed:
    print(f"Success! Processed {len(results.completed)} nodes in {results.duration:.2f}s")
else:
    print(f"Failed nodes: {results.failed}")

# Access individual node results
for node_name, node_result in results.node_results.items():
    print(f"{node_name}: {node_result.rows_processed} rows in {node_result.duration:.2f}s")

# Get story path
if results.story_path:
    print(f"Execution story: {results.story_path}")
```

### Pipeline Validation

```python
# Validate without executing
validation = pipeline.validate()

if validation["valid"]:
    print(f"Pipeline valid with {validation['node_count']} nodes")
    print(f"Execution order: {validation['execution_order']}")
else:
    print(f"Errors: {validation['errors']}")

if validation["warnings"]:
    print(f"Warnings: {validation['warnings']}")
```

### List and Access Pipelines

```python
# List available pipelines
print(manager.list_pipelines())
# ['bronze_to_silver', 'silver_to_gold']

# Get specific pipeline instance
pipeline = manager.get_pipeline("bronze_to_silver")

# Execute single node (for debugging)
result = pipeline.execute_node("clean_orders")
```

## Complete Example

```yaml
project: SalesAnalytics
engine: spark

connections:
  raw:
    type: azure_adls
    account: ${AZURE_STORAGE_ACCOUNT}
    container: raw
  silver:
    type: delta
    path: abfss://silver@${AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/

alerts:
  - type: slack
    url: ${SLACK_WEBHOOK_URL}
    on_events: [on_failure]

pipelines:
  - pipeline: sales_daily
    nodes:
      - name: ingest_transactions
        read:
          connection: raw
          path: transactions/
          format: parquet
          incremental:
            mode: rolling_window
            column: transaction_date
            lookback: 7
            unit: day

      - name: validate_transactions
        depends_on: [ingest_transactions]
        validation:
          tests:
            - type: not_null
              columns: [transaction_id, amount]
            - type: positive
              columns: [amount]
          on_fail: quarantine
          quarantine:
            connection: silver
            path: quarantine/transactions

      - name: aggregate_daily
        depends_on: [validate_transactions]
        transform:
          steps:
            - function: group_by_sum
              params:
                group_cols: [store_id, transaction_date]
                sum_cols: [amount]
        on_error: fail_fast

      - name: write_daily_sales
        depends_on: [aggregate_daily]
        write:
          connection: silver
          path: sales/daily
          format: delta
          mode: merge
          merge_keys: [store_id, transaction_date]
```

```python
from odibi.pipeline import Pipeline

manager = Pipeline.from_yaml("sales_config.yaml")
results = manager.run(parallel=True, max_workers=4)

if results.failed:
    print(f"Pipeline failed: {results.failed}")
else:
    print(f"Daily sales updated: {results.story_path}")
```

## Related

- [Alerting](alerting.md) - Configure notifications for pipeline events
- [Quality Gates](quality_gates.md) - Block pipelines on data quality failures
- [Quarantine Tables](quarantine.md) - Handle invalid rows
- [Lineage](lineage.md) - Track data flow across pipelines
