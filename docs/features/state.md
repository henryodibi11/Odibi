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

## Troubleshooting

### Resume not working - nodes re-run every time

**Symptom:** `--resume` flag doesn't skip completed nodes.

**Causes:**
- No system catalog configured (state not persisted)
- State file deleted or corrupted
- Node name changed between runs

**Fixes:**
```yaml
# Ensure system catalog is configured
system:
  connection: catalog_conn
  meta_runs_path: meta/runs
  meta_state_path: meta/state
```

### High Water Mark (HWM) not updating

**Symptom:** Incremental reads fetch all data instead of new records.

**Causes:**
- First run always does full load (expected)
- HWM column has NULL values
- State backend not persisting

**Fixes:**
```bash
# Check current HWM state
odibi catalog state --config config.yaml

# Verify HWM column has no NULLs
# HWM is set to MAX(column) after successful run
```

### State corruption after failed run

**Symptom:** Pipeline behaves unexpectedly after a failure.

**Fix:** Reset state for specific node:
```bash
# View current state
odibi catalog state --config config.yaml

# If needed, delete and re-run (full load)
# State will be rebuilt on next successful run
```

### Local JSON state lost between environments

**Cause:** `LocalJSONStateBackend` stores state in `.odibi/state.json` locally.

**Fix:** Use `CatalogStateBackend` with Delta Lake for shared/persistent state:
```yaml
system:
  connection: shared_storage
  meta_runs_path: _odibi/runs
  meta_state_path: _odibi/state
```

### Concurrent pipeline runs corrupt state

**Symptom:** State inconsistent when multiple pipelines run simultaneously.

**Fix:** Use Delta Lake catalog backend (supports concurrent writes with retry):
```yaml
system:
  connection: delta_catalog
  meta_state_path: _odibi/state  # Delta table with ACID support
```

## Related

- [Incremental Loading](../patterns/incremental_stateful.md) — HWM-based incremental
- [Catalog](catalog.md) — System catalog for metadata
- [CLI Guide](../guides/cli_master_guide.md) — `odibi catalog state` command
