# Orchestration

Generate production-ready workflow definitions for Apache Airflow and Dagster from your Odibi pipelines.

## Overview

Odibi's orchestration module provides:
- **Airflow Integration**: Generate DAG files with proper task dependencies
- **Dagster Integration**: Create asset definitions with dependency graphs
- **Automatic Dependency Mapping**: Node dependencies become task/asset dependencies
- **CLI Execution**: Each node runs via `odibi run` for isolation

## Airflow Integration

### AirflowExporter Class

The `AirflowExporter` generates Airflow DAG Python files from Odibi pipeline configurations.

```python
from odibi.config import load_config
from odibi.orchestration.airflow import AirflowExporter

config = load_config("odibi.yaml")
exporter = AirflowExporter(config)

# Generate DAG code for a specific pipeline
dag_code = exporter.generate_code("process_orders")

# Write to Airflow DAGs folder
with open("/airflow/dags/odibi_process_orders.py", "w") as f:
    f.write(dag_code)
```

### Generated DAG Structure

The exporter creates a DAG with:
- `BashOperator` tasks for each node
- Proper upstream/downstream dependencies
- Configurable retries from your Odibi config
- Tags for filtering (`odibi`, layer name)

```python
# Generated DAG example
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'odibi_process_orders',
    default_args=default_args,
    description='Process incoming orders',
    schedule_interval=None,
    catchup=False,
    tags=['odibi', 'silver'],
) as dag:

    ingest_orders = BashOperator(
        task_id='ingest_orders',
        bash_command='odibi run --pipeline process_orders --node ingest_orders',
    )

    validate_orders = BashOperator(
        task_id='validate_orders',
        bash_command='odibi run --pipeline process_orders --node validate_orders',
    )

    # Dependencies
    [ingest_orders] >> validate_orders
```

### Airflow Configuration Options

| Option | Source | Description |
|--------|--------|-------------|
| `dag_id` | Auto-generated | `odibi_{pipeline_name}` |
| `owner` | `config.owner` | DAG owner for Airflow UI |
| `retries` | `config.retry.max_attempts` | Retry count (0 if disabled) |
| `tags` | `pipeline.layer` | Includes `odibi` and layer name |
| `description` | `pipeline.description` | Pipeline description |

## Dagster Integration

### DagsterFactory Class

The `DagsterFactory` creates Dagster asset definitions directly from your Odibi configuration.

```python
# definitions.py
from odibi.config import load_config
from odibi.orchestration.dagster import DagsterFactory

config = load_config("odibi.yaml")
defs = DagsterFactory(config).create_definitions()
```

### Asset Features

Each Odibi node becomes a Dagster asset with:
- **Dependency tracking**: `depends_on` becomes asset dependencies
- **Grouping**: Assets grouped by pipeline name
- **Compute kind**: Tagged as `odibi` for UI identification
- **Op tags**: Pipeline and node names for filtering

### Generated Assets

```python
# Dagster creates assets like:
@asset(
    name="validate_orders",
    deps=["ingest_orders"],
    group_name="process_orders",
    description="Validate order data quality",
    compute_kind="odibi",
    op_tags={"odibi/pipeline": "process_orders", "odibi/node": "validate_orders"},
)
def validate_orders(context: AssetExecutionContext):
    # Runs: odibi run --pipeline process_orders --node validate_orders
    ...
```

### Dagster Installation

Dagster is an optional dependency:

```bash
pip install dagster dagster-webserver
```

## Configuration

### Project Configuration for Orchestration

```yaml
project: DataPipeline
owner: data-team      # Used as Airflow DAG owner

retry:
  enabled: true
  max_attempts: 3      # Airflow retry count

pipelines:
  - pipeline: process_orders
    layer: silver      # Used as Airflow tag
    description: "Process incoming orders"
    nodes:
      - name: ingest_orders
        # ...

      - name: validate_orders
        depends_on:
          - ingest_orders
        # ...

      - name: transform_orders
        depends_on:
          - validate_orders
        # ...
```

### Dependency Mapping

Node dependencies in Odibi map directly to orchestrator dependencies:

| Odibi Config | Airflow | Dagster |
|--------------|---------|---------|
| `depends_on: [node_a]` | `[node_a] >> node_b` | `deps=["node_a"]` |
| `depends_on: [a, b]` | `[a, b] >> node_c` | `deps=["a", "b"]` |
| No dependencies | First task | No deps |

## Examples

### Complete Airflow Integration

```python
# scripts/generate_dags.py
from pathlib import Path
from odibi.config import load_config
from odibi.orchestration.airflow import AirflowExporter

def generate_all_dags(config_path: str, output_dir: str):
    config = load_config(config_path)
    exporter = AirflowExporter(config)
    output = Path(output_dir)

    for pipeline in config.pipelines:
        dag_code = exporter.generate_code(pipeline.pipeline)
        dag_file = output / f"odibi_{pipeline.pipeline}.py"
        dag_file.write_text(dag_code)
        print(f"Generated: {dag_file}")

if __name__ == "__main__":
    generate_all_dags("odibi.yaml", "/opt/airflow/dags")
```

### Complete Dagster Integration

```python
# definitions.py
from odibi.config import load_config
from odibi.orchestration.dagster import DagsterFactory

# Load Odibi configuration
config = load_config("odibi.yaml")

# Create Dagster definitions
defs = DagsterFactory(config).create_definitions()

# Run with: dagster dev -f definitions.py
```

### Multi-Pipeline Setup

```yaml
# odibi.yaml
project: DataWarehouse
owner: platform-team

pipelines:
  - pipeline: bronze_ingestion
    layer: bronze
    nodes:
      - name: ingest_customers
        source:
          connection: raw_db
          path: customers

      - name: ingest_orders
        source:
          connection: raw_db
          path: orders

  - pipeline: silver_transformation
    layer: silver
    nodes:
      - name: clean_customers
        depends_on: []
        source:
          connection: bronze
          path: customers

      - name: clean_orders
        depends_on: []
        source:
          connection: bronze
          path: orders

      - name: join_customer_orders
        depends_on:
          - clean_customers
          - clean_orders
```

```python
# Generate DAGs for all pipelines
from odibi.config import load_config
from odibi.orchestration.airflow import AirflowExporter

config = load_config("odibi.yaml")
exporter = AirflowExporter(config)

# Generates separate DAGs:
# - odibi_bronze_ingestion
# - odibi_silver_transformation
for pipeline in config.pipelines:
    code = exporter.generate_code(pipeline.pipeline)
    print(f"--- {pipeline.pipeline} ---")
    print(code)
```

## Best Practices

1. **Use CLI execution** - Both adapters use `odibi run` for process isolation
2. **Set owner** - Configure `owner` in YAML for Airflow ownership
3. **Enable retries** - Configure retry settings in Odibi config
4. **Layer tags** - Use `layer` field for organizing DAGs in Airflow
5. **Generate on deploy** - Regenerate DAG files during CI/CD deployment

## Related

- [Pipeline Configuration](../reference/yaml_schema.md) - YAML schema reference
- [CLI Reference](cli.md) - `odibi run` command details
- Retry configuration is defined in your YAML config under the `retry` section
