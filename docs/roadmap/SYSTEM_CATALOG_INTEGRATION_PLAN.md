# System Catalog Integration Plan

## Overview

This plan addresses gaps in the system catalog integration to achieve the goal:  
**"Declare YAML, run pipeline, everything works automatically."**

Currently only `meta_state` works correctly. All other system tables have integration gaps.

---

## Phase 1: Fix Critical Bugs (Done)

### 1.1 Fix state_manager in NodeExecutor ✅
- **Issue**: `state_manager` wasn't passed to `NodeExecutor`, breaking `skip_if_unchanged`
- **Fix**: Added `state_manager` parameter to `NodeExecutor.__init__` and pass it from `Node` class
- **Status**: Completed this session, needs commit

---

## Phase 2: Auto-Registration on Run

### 2.1 Auto-register pipelines and nodes
- **Location**: `PipelineManager.run()`
- **Action**: Before executing, call `register_pipeline()` and `register_node()` for each pipeline/node being run
- **Behavior**: Idempotent upsert - safe to call on every run
- **Result**: `meta_pipelines` and `meta_nodes` populated automatically

```python
def run(self, pipelines=None, ...):
    # Auto-register before execution
    if self.catalog_manager:
        for name in pipeline_names:
            config = self._pipelines[name].config
            self.catalog_manager.register_pipeline(config, self.project_config)
            for node in config.nodes:
                self.catalog_manager.register_node(config.pipeline, node)

    # ... rest of run logic
```

---

## Phase 3: Wire Catalog Logging into Pipeline Flow

### 3.1 Log runs from Pipeline/NodeExecutor
- **Location**: `Pipeline._execute_node()` or `NodeExecutor` after node completion
- **Action**: Call `catalog_manager.log_run()` with execution results
- **Result**: `meta_runs` populated from YAML flow (not just `Node` class)

### 3.2 Register assets on write
- **Location**: `NodeExecutor._execute_write_phase()` after successful write
- **Action**: Call `catalog_manager.register_asset()` with table details
- **Result**: `meta_tables` populated automatically

### 3.3 Track schema changes
- **Location**: `NodeExecutor._execute_write_phase()` after write
- **Action**:
  1. Get current schema from written data
  2. Call `catalog_manager.track_schema()` if schema differs from previous
- **Result**: `meta_schemas` tracks schema evolution

### 3.4 Log pattern usage
- **Location**: `Pipeline._execute_node()` when pattern-based nodes execute
- **Action**: Call `catalog_manager.log_pattern()` with pattern config
- **Result**: `meta_patterns` populated

### 3.5 Record lineage
- **Location**: `Pipeline._execute_node()` after node completion
- **Action**: Extract source/target from node config, call `record_lineage()`
- **Result**: `meta_lineage` populated automatically

### 3.6 Create and log metrics
- **Location**: `CatalogManager` + `Pipeline`
- **Action**:
  1. Create `log_metrics()` method in `CatalogManager`
  2. Call from Pipeline with execution stats
- **Result**: `meta_metrics` populated

---

## Phase 4: Cleanup/Removal Methods

### 4.1 Remove pipeline
```python
def remove_pipeline(self, pipeline_name: str) -> int:
    """Remove pipeline and cascade to nodes, state entries.
    Returns count of deleted entries."""
```

### 4.2 Remove node
```python
def remove_node(self, pipeline_name: str, node_name: str) -> int:
    """Remove node and associated state entries.
    Returns count of deleted entries."""
```

### 4.3 Cleanup orphans
```python
def cleanup_orphans(self, current_config: ProjectConfig) -> Dict[str, int]:
    """Compare catalog against current config, remove stale entries.
    Returns dict of {table: deleted_count}."""
```

### 4.4 Clear state
```python
def clear_state(self, key_pattern: str) -> int:
    """Remove state entries matching pattern (supports wildcards).
    Returns count of deleted entries."""
```

---

## Phase 5: List/Query Methods on PipelineManager

### 5.1 List methods
```python
def list_pipelines(self) -> pd.DataFrame:
    """List all registered pipelines."""

def list_nodes(self, pipeline: Optional[str] = None) -> pd.DataFrame:
    """List nodes, optionally filtered by pipeline."""

def list_runs(
    self,
    pipeline: Optional[str] = None,
    node: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 10
) -> pd.DataFrame:
    """List recent runs with optional filters."""

def list_tables(self) -> pd.DataFrame:
    """List registered assets from meta_tables."""
```

### 5.2 State methods
```python
def get_state(self, key: str) -> Optional[Dict[str, Any]]:
    """Get specific state entry (HWM, content hash, etc.)."""

def get_all_state(self, prefix: Optional[str] = None) -> pd.DataFrame:
    """Get all state entries, optionally filtered by key prefix."""

def clear_state(self, key: str) -> bool:
    """Remove a state entry. Returns True if deleted."""
```

### 5.3 Schema/Lineage methods
```python
def get_schema_history(
    self,
    table: str,  # Accepts friendly name, resolved automatically
    limit: int = 5
) -> pd.DataFrame:
    """Get schema version history for a table."""

def get_lineage(
    self,
    table: str,
    direction: str = "both"  # "upstream", "downstream", "both"
) -> pd.DataFrame:
    """Get lineage for a table."""
```

### 5.4 Stats/Health methods
```python
def get_pipeline_status(self, pipeline: str) -> Dict[str, Any]:
    """Get last run status, duration, timestamp."""

def get_node_stats(self, node: str, days: int = 7) -> Dict[str, Any]:
    """Get avg duration, row counts, success rate over period."""
```

---

## Phase 6: Smart Path Resolution

### 6.1 Implement `_resolve_table_path()`
All query methods accept user-friendly identifiers:
- Relative path: `"bronze/OEE/vw_OSMPerformanceOEE"`
- Registered table: `"test.vw_OSMPerformanceOEE"`
- Node name: `"opsvisdata_vw_OSMPerformanceOEE"`
- Full path: `"abfss://..."` (used as-is)

```python
def _resolve_table_path(self, identifier: str) -> str:
    # 1. Full path - use as-is
    if self._is_full_path(identifier):
        return identifier

    # 2. Check meta_tables for registered name
    if self.catalog_manager:
        resolved = self.catalog_manager.resolve_table_path(identifier)
        if resolved:
            return resolved

    # 3. Check if it's a node name
    for pipeline in self._pipelines.values():
        for node in pipeline.config.nodes:
            if node.name == identifier and node.write:
                conn = self.connections[node.write.connection]
                return conn.get_path(node.write.path or node.write.table)

    # 4. Resolve using system connection as fallback
    sys_conn = self.connections.get(self.project_config.system.connection)
    return sys_conn.get_path(identifier) if sys_conn else identifier
```

---

## Implementation Order

1. **Phase 2** - Auto-registration (quick win, enables other phases)
2. **Phase 3.1** - Log runs (most visible improvement)
3. **Phase 5.1-5.2** - List and state methods (immediate usability)
4. **Phase 6** - Path resolution (needed for other methods)
5. **Phase 3.2-3.5** - Remaining catalog logging
6. **Phase 4** - Cleanup methods
7. **Phase 5.3-5.4** - Advanced query methods
8. **Phase 3.6** - Metrics (lowest priority)

---

## Testing Requirements

- Unit tests for each new method
- Integration test: run pipeline, verify all 9 tables populated
- Test cleanup methods don't break active pipelines
- Test path resolution with all identifier formats
- Test with both Spark and Pandas engines

---

## Files to Modify

| File | Changes |
|------|---------|
| `odibi/pipeline.py` | Auto-register, log_run, list methods, path resolution |
| `odibi/node.py` | Already fixed (state_manager), add register_asset/track_schema calls |
| `odibi/catalog.py` | Add remove_*, cleanup_orphans, log_metrics methods |
| `tests/unit/test_pipeline_manager.py` | New tests for list/query methods |
| `tests/integration/test_catalog_integration.py` | End-to-end catalog tests |
