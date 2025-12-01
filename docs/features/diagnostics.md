# Diagnostics

Tools for debugging, monitoring, and comparing pipeline runs with Delta Lake version analysis and data diff capabilities.

## Overview

Odibi's diagnostics module provides:
- **Delta Diagnostics**: Table history, version comparison, metrics extraction
- **Data Diff**: Row-level comparison, schema comparison, change detection
- **Run Comparison**: Compare pipeline executions to identify drift
- **History Management**: Access and analyze historical pipeline runs

## Delta Diagnostics

### Table Version Comparison

Compare two versions of a Delta table to understand what changed:

```python
from odibi.diagnostics import get_delta_diff

# Basic comparison (metadata only)
diff = get_delta_diff(
    table_path="/path/to/delta/table",
    version_a=5,
    version_b=10,
    spark=spark,  # Optional: use Spark or deltalake (Pandas)
)

print(f"Rows changed: {diff.rows_change}")
print(f"Files changed: {diff.files_change}")
print(f"Size change: {diff.size_change_bytes} bytes")
print(f"Operations: {diff.operations}")
```

### DeltaDiffResult Fields

| Field | Type | Description |
|-------|------|-------------|
| `table_path` | str | Path to the Delta table |
| `version_a` | int | Start version |
| `version_b` | int | End version |
| `rows_change` | int | Net row count change |
| `files_change` | int | Net file count change |
| `size_change_bytes` | int | Net size change in bytes |
| `schema_added` | List[str] | Columns added between versions |
| `schema_removed` | List[str] | Columns removed between versions |
| `schema_current` | List[str] | Current schema columns |
| `schema_previous` | List[str] | Previous schema columns |
| `operations` | List[str] | Operations that occurred between versions |

### Deep Diff Mode

Enable row-level comparison for detailed analysis:

```python
# Deep comparison with key-based diff
diff = get_delta_diff(
    table_path="/path/to/delta/table",
    version_a=5,
    version_b=10,
    spark=spark,
    deep=True,
    keys=["order_id"],  # Primary key columns for update detection
)

print(f"Rows added: {diff.rows_added}")
print(f"Rows removed: {diff.rows_removed}")
print(f"Rows updated: {diff.rows_updated}")

# Sample data
print("Added rows:", diff.sample_added[:5])
print("Removed rows:", diff.sample_removed[:5])
print("Updated rows:", diff.sample_updated[:5])
```

### Drift Detection

Automatically detect significant changes between versions:

```python
from odibi.diagnostics import detect_drift

warning = detect_drift(
    table_path="/path/to/delta/table",
    current_version=10,
    baseline_version=5,
    spark=spark,
    threshold_pct=10.0,  # Alert if >10% row count change
)

if warning:
    print(f"Drift detected: {warning}")
```

Drift detection checks for:
- **Schema drift**: Columns added or removed
- **Data volume drift**: Row count changes exceeding threshold

## Data Diff

### Node Comparison

Compare two executions of the same node:

```python
from odibi.diagnostics import diff_nodes

diff = diff_nodes(node_a, node_b)

print(f"Status change: {diff.status_change}")
print(f"Rows diff: {diff.rows_diff}")
print(f"Schema changed: {diff.schema_change}")
print(f"SQL changed: {diff.sql_changed}")
print(f"Has drift: {diff.has_drift}")
```

### NodeDiffResult Fields

| Field | Type | Description |
|-------|------|-------------|
| `node_name` | str | Name of the node |
| `status_change` | str | Status change (e.g., "success -> failed") |
| `rows_out_a` | int | Output rows from run A |
| `rows_out_b` | int | Output rows from run B |
| `rows_diff` | int | Row count difference (B - A) |
| `schema_change` | bool | Whether schema changed |
| `columns_added` | List[str] | Columns added in run B |
| `columns_removed` | List[str] | Columns removed in run B |
| `sql_changed` | bool | Whether SQL logic changed |
| `config_changed` | bool | Whether configuration changed |
| `transformation_changed` | bool | Whether transformation stack changed |
| `delta_version_change` | str | Delta version change (e.g., "v1 -> v2") |
| `has_drift` | bool | True if any significant drift occurred |

### Run Comparison

Compare two complete pipeline runs:

```python
from odibi.diagnostics import diff_runs

run_diff = diff_runs(run_a, run_b)

print(f"Nodes added: {run_diff.nodes_added}")
print(f"Nodes removed: {run_diff.nodes_removed}")
print(f"Drift sources: {run_diff.drift_source_nodes}")
print(f"Impacted downstream: {run_diff.impacted_downstream_nodes}")

# Examine individual node diffs
for name, node_diff in run_diff.node_diffs.items():
    if node_diff.has_drift:
        print(f"  {name}: {node_diff.status_change or 'data drift'}")
```

### RunDiffResult Fields

| Field | Type | Description |
|-------|------|-------------|
| `run_id_a` | str | Run ID of baseline |
| `run_id_b` | str | Run ID of current run |
| `node_diffs` | Dict[str, NodeDiffResult] | Per-node comparison results |
| `nodes_added` | List[str] | Nodes present in B but not A |
| `nodes_removed` | List[str] | Nodes present in A but not B |
| `drift_source_nodes` | List[str] | Nodes where logic changed |
| `impacted_downstream_nodes` | List[str] | Nodes affected by upstream drift |

## HistoryManager

Manage and access pipeline run history:

```python
from odibi.diagnostics import HistoryManager

manager = HistoryManager(history_path="stories/")

# List available runs
runs = manager.list_runs("process_orders")
for run in runs:
    print(f"Run: {run['run_id']} at {run['timestamp']}")

# Get specific runs
latest = manager.get_latest_run("process_orders")
specific = manager.get_run_by_id("process_orders", "20240130_101500")
previous = manager.get_previous_run("process_orders", "20240130_101500")
```

### HistoryManager Methods

| Method | Description |
|--------|-------------|
| `list_runs(pipeline_name)` | List all runs for a pipeline (newest first) |
| `get_latest_run(pipeline_name)` | Get the most recent run metadata |
| `get_run_by_id(pipeline_name, run_id)` | Get specific run by ID |
| `get_previous_run(pipeline_name, run_id)` | Get the run immediately before specified run |
| `load_run(path)` | Load run metadata from JSON file |

## Examples

### Debugging a Failed Pipeline

```python
from odibi.diagnostics import HistoryManager, diff_runs

manager = HistoryManager("stories/")

# Get the failed run and the last successful run
failed_run = manager.get_latest_run("process_orders")
previous_run = manager.get_previous_run("process_orders", failed_run.run_id)

if previous_run:
    diff = diff_runs(previous_run, failed_run)
    
    # Find what changed
    print("Changes that may have caused failure:")
    for node in diff.drift_source_nodes:
        node_diff = diff.node_diffs[node]
        if node_diff.sql_changed:
            print(f"  - {node}: SQL logic changed")
        if node_diff.config_changed:
            print(f"  - {node}: Configuration changed")
```

### Monitoring Data Quality Over Time

```python
from odibi.diagnostics import get_delta_diff, detect_drift

# Check for unexpected changes after a pipeline run
table_path = "/delta/silver/orders"

# Compare with yesterday's version
drift_warning = detect_drift(
    table_path=table_path,
    current_version=100,
    baseline_version=95,
    spark=spark,
    threshold_pct=5.0,  # Alert on >5% change
)

if drift_warning:
    # Get detailed diff
    diff = get_delta_diff(
        table_path=table_path,
        version_a=95,
        version_b=100,
        spark=spark,
        deep=True,
    )
    
    print(f"Warning: {drift_warning}")
    print(f"Details: +{diff.rows_added} / -{diff.rows_removed} rows")
    
    if diff.schema_added:
        print(f"New columns: {diff.schema_added}")
```

### Comparing Spark vs Pandas Execution

The diagnostics module supports both Spark and Pandas (via `deltalake` library):

```python
from odibi.diagnostics import get_delta_diff

# With Spark (for large tables)
diff_spark = get_delta_diff(
    table_path="/delta/table",
    version_a=1,
    version_b=5,
    spark=spark,
    deep=True,
)

# With Pandas/deltalake (for local development)
diff_pandas = get_delta_diff(
    table_path="/delta/table",
    version_a=1,
    version_b=5,
    spark=None,  # Uses deltalake library
    deep=True,
)
```

### Tracking Schema Evolution

```python
from odibi.diagnostics import get_delta_diff

diff = get_delta_diff(
    table_path="/delta/silver/customers",
    version_a=0,  # Initial version
    version_b=50,  # Current version
    spark=spark,
)

print("Schema Evolution:")
print(f"  Initial columns: {diff.schema_previous}")
print(f"  Current columns: {diff.schema_current}")
print(f"  Added over time: {diff.schema_added}")
print(f"  Removed over time: {diff.schema_removed}")
```

## Best Practices

1. **Use deep mode sparingly** - Deep diff is expensive; use metadata-only diffs for routine monitoring
2. **Define primary keys** - Key-based diff enables update detection, not just add/remove
3. **Set appropriate thresholds** - Tune drift detection thresholds based on expected data patterns
4. **Store history** - Enable story persistence to enable run comparisons over time
5. **Automate drift checks** - Integrate drift detection into pipeline post-run hooks

## Related

- [Stories](stories.md) - Pipeline execution history
- [Schema Tracking](schema_tracking.md) - Schema change monitoring
- [Quality Gates](quality_gates.md) - Data quality validation
- [Lineage](lineage.md) - Data lineage tracking
