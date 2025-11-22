# Managing Environments

Odibi allows you to define a single pipeline configuration that adapts to different contexts (e.g., Local Development, Testing, Production) using the `environments` block. This prevents configuration drift and ensures your pipeline logic remains consistent while infrastructure details change.

## How it Works

Odibi uses a **Base Configuration + Override** model:
1.  **Base Configuration**: Defines your default settings (typically for local development).
2.  **Environment Overrides**: Specific blocks that patch or replace values in the base configuration when that environment is active.

## Configuration Structure

Add an `environments` section to your `project.yaml`:

```yaml
# --- 1. Base Configuration (Default / Local) ---
project: Sales Data Pipeline
engine: pandas
retry:
  enabled: false

connections:
  data_lake:
    type: local
    base_path: ./data/raw

pipelines:
  - pipeline: ingest_sales
    nodes:
      - name: read_csv
        read:
          connection: data_lake
          path: sales.csv

# --- 2. Environment Overrides ---
environments:
  # Production Environment
  prod:
    engine: spark  # Switch to Spark for scale
    retry:
      enabled: true
      max_attempts: 3
    connections:
      data_lake:
        type: azure_adls
        account: mycompanyprod
        container: sales-data
        auth_mode: managed_identity
    story:
      max_sample_rows: 0 # Disable data sampling for security

  # Testing Environment
  test:
    connections:
      data_lake:
        type: local
        base_path: ./data/test_fixtures
```

## Usage

### CLI

Use the `--env` flag to activate an environment.

**Run in Default (Base) Environment:**
```bash
odibi run project.yaml
```

**Run in Production:**
```bash
odibi run project.yaml --env prod
```

### Python API

Pass the `env` parameter when initializing the `PipelineManager`.

```python
from odibi.pipeline import PipelineManager

# Load Prod Configuration
manager = PipelineManager.from_yaml("project.yaml", env="prod")

# Run Pipeline
manager.run("ingest_sales")
```

### Databricks Example

In a Databricks notebook, you can use widgets to switch environments dynamically without changing code.

```python
# 1. Create Widget
dbutils.widgets.dropdown("environment", "dev", ["dev", "test", "prod"])

# 2. Get Selection
current_env = dbutils.widgets.get("environment")

# 3. Run Pipeline
manager = PipelineManager.from_yaml("/dbfs/project.yaml", env=current_env)
manager.run()
```

## Common Use Cases

### 1. Swapping Storage (Local vs. Cloud)
Develop locally with CSVs, deploy to ADLS/S3 without changing pipeline code.

```yaml
connections:
  storage: { type: local, base_path: ./data }

environments:
  prod:
    connections:
      storage: { type: azure_adls, account: prod_acc, container: data }
```

### 2. Scaling Engines (Pandas vs. Spark)
Use Pandas for fast local iteration and unit tests, but switch to Spark for distributed processing in production.

```yaml
engine: pandas

environments:
  prod:
    engine: spark
```

### 3. Security & Privacy
Disable data sampling in stories for production to prevent PII leakage, while keeping it enabled in dev for debugging.

```yaml
story:
  max_sample_rows: 20

environments:
  prod:
    story:
      max_sample_rows: 0
```

### 4. Alerting
Only send Slack/Teams notifications when running in production.

```yaml
alerts: []  # No alerts in dev

environments:
  prod:
    alerts:
      - type: slack
        url: ${SLACK_WEBHOOK}
```
