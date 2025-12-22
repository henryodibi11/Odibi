# State Management

Odibi tracks pipeline execution state, high-water marks (HWM), and run history for resumable, incremental processing.

## Overview

State management enables:

- **Resume from failure**: Skip successfully completed nodes
- **High-water marks**: Track last processed timestamp/ID for incremental loads
- **Run history**: Query past executions and their outcomes
- **Concurrent writes**: Safe multi-pipeline execution with retry logic

## State Backends

### LocalJSONStateBackend

Simple JSON file storage for local development:

```python
from odibi.state import LocalJSONStateBackend

backend = LocalJSONStateBackend(".odibi/state.json")

# Used automatically when no system catalog is configured
```

**Storage location**: `.odibi/state.json`

### CatalogStateBackend

Delta Lake-based storage for production (supports Spark and local deltalake):

```python
from odibi.state import CatalogStateBackend

backend = CatalogStateBackend(
    meta_runs_path="/path/to/meta_runs",
    meta_state_path="/path/to/meta_state",
    spark_session=spark,           # Optional
    storage_options={"key": "val"} # For Azure/S3
)
```

**Configured via** `system` in your YAML config:

```yaml
system:
  connection: my_storage
  path: _system

connections:
  my_storage:
    type: local  # or azure_blob
    base_path: ./data
```

## StateManager API

The `StateManager` wraps a backend and provides high-level operations:

```python
from odibi.state import StateManager, create_state_backend
from odibi.config import load_config_from_file

# Create from config
config = load_config_from_file("odibi.yaml")
backend = create_state_backend(config, project_root=".")
state_mgr = StateManager(backend=backend)
```

### High-Water Marks

```python
# Get HWM value
last_id = state_mgr.get_hwm("orders.last_processed_id")

# Set HWM value
state_mgr.set_hwm("orders.last_processed_id", 12345)

# Batch set (efficient for parallel pipelines)
state_mgr.set_hwm_batch([
    {"key": "orders.hwm", "value": 100},
    {"key": "customers.hwm", "value": 200},
])
```

### Run Status

```python
# Check if node succeeded in last run
success = state_mgr.get_last_run_status("pipeline_name", "node_name")

# Get full run info (metadata, timestamp)
info = state_mgr.get_last_run_info("pipeline_name", "node_name")
# Returns: {"success": True, "metadata": {...}}
```

### Save Pipeline Run

```python
# Called automatically by PipelineManager
state_mgr.save_pipeline_run("my_pipeline", results)
```

## Concurrent Write Handling

The `CatalogStateBackend` handles Delta Lake concurrent write conflicts with automatic retry:

- **Exponential backoff**: 1s, 2s, 4s, 8s, 16s delays
- **Jitter**: Random 0-1s added to prevent thundering herd
- **Max retries**: 5 attempts before failing
- **Conflict detection**: Catches `ConcurrentAppendException` and similar

This enables safe parallel pipeline execution on shared state tables.

## Configuration

### Using System Catalog (Recommended)

```yaml
project: MyProject
engine: spark

system:
  connection: catalog_storage
  path: _system

connections:
  catalog_storage:
    type: azure_blob
    account_name: mystorageaccount
    container: odibi
    auth:
      mode: account_key
      account_key: ${AZURE_STORAGE_KEY}
```

### Local Development (Auto-fallback)

If no `system` config is provided, Odibi uses `LocalJSONStateBackend` automatically:

```
⚠️ No system catalog configured. Using local JSON state backend (local-only mode).
```

## Related

- [Incremental Loading](../patterns/incremental_stateful.md) — HWM-based incremental
- [Catalog](catalog.md) — System catalog for metadata
- [CLI Guide](../guides/cli_master_guide.md) — `odibi catalog state` command
