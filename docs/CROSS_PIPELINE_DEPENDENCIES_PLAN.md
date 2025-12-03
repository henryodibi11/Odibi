# Cross-Pipeline Dependencies Plan

## Overview

Enable pipelines to reference outputs from other pipelines using a `$pipeline.node` syntax, resolved via Odibi's internal catalog. This supports the medallion architecture pattern (bronze → silver → gold) where silver nodes depend on bronze outputs.

---

## Features

### 1. Odibi Catalog: Node Outputs Registry

**New table: `meta_outputs`**

Stores output metadata for every node that has a `write` block.

```
Schema:
- pipeline_name: STRING (pipeline identifier)
- node_name: STRING (node identifier)
- output_type: STRING ("external_table" | "managed_table")
- connection_name: STRING (nullable, for external tables)
- path: STRING (nullable, storage path)
- format: STRING (delta, parquet, etc.)
- table_name: STRING (nullable, registered table name)
- last_run: TIMESTAMP
- row_count: LONG (nullable)
- updated_at: TIMESTAMP
```

**Primary key:** `(pipeline_name, node_name)`

**Location:** `{base_path}/meta_outputs`

---

### 2. Batch Registration (Performance Critical)

**Problem:** Per-node catalog writes caused 40+ second overhead in Databricks (17 nodes × ~2-3s each).

**Solution:** Collect metadata in-memory during execution, single MERGE at pipeline end.

```python
# In CatalogManager
def register_outputs_batch(self, records: List[Dict[str, Any]]) -> None:
    """
    Batch registers/upserts multiple node outputs to meta_outputs.
    Uses MERGE INTO for efficient upsert.

    Args:
        records: List of dicts with keys: pipeline_name, node_name,
                 output_type, connection_name, path, format, table_name,
                 last_run, row_count
    """
    # Same pattern as register_pipelines_batch / register_nodes_batch
    # Single MERGE INTO delta.`{target_path}`
```

**Pipeline execution flow:**
```python
# In Pipeline.run()
output_records = []

for node in execution_order:
    result = execute_node(node)

    if node.has_write:
        output_records.append({
            "pipeline_name": self.config.pipeline,
            "node_name": node.name,
            "output_type": "external_table" if node.write.path else "managed_table",
            "connection_name": node.write.connection,
            "path": node.write.path,
            "format": node.write.format,
            "table_name": node.write.register_table or node.write.table,
            "last_run": datetime.now(timezone.utc),
            "row_count": result.row_count,
        })

# Single batch write at end
if output_records:
    self.catalog_manager.register_outputs_batch(output_records)
```

---

### 3. Cross-Pipeline Reference Syntax (`$pipeline.node`)

**Usage in silver pipeline:**
```yaml
pipelines:
  - pipeline: transform_silver
    layer: silver
    nodes:
      - name: enriched_downtime
        inputs:
          events: $read_bronze.opsvisdata_ShiftDowntimeEventsview
          calendar: $read_bronze.opsvisdata_vw_calender
          plant: $read_bronze.opsvisdata_vw_dim_plantprocess
        transform:
          - operation: join
            left: events
            right: calendar
            on: [DateId]
          - operation: join
            right: plant
            on: [P_ID]
        write:
          connection: goat_prod
          format: delta
          path: "silver/OEE/enriched_downtime"
```

**Resolution logic:**
```python
def resolve_input_reference(ref: str, catalog: CatalogManager) -> Dict:
    """
    Resolves $pipeline.node to read configuration.

    Args:
        ref: Reference string like "$read_bronze.opsvisdata_vw_calender"
        catalog: CatalogManager instance

    Returns:
        Dict with keys: connection, path, format, table (for engine.read())
    """
    if not ref.startswith("$"):
        raise ValueError(f"Invalid reference: {ref}")

    parts = ref[1:].split(".", 1)  # Remove $ and split
    if len(parts) != 2:
        raise ValueError(f"Invalid reference format: {ref}. Expected $pipeline.node")

    pipeline_name, node_name = parts

    # Query meta_outputs
    output = catalog.get_node_output(pipeline_name, node_name)

    if output is None:
        raise ValueError(
            f"No output found for {ref}. "
            f"Ensure pipeline '{pipeline_name}' has run and node '{node_name}' has a write block."
        )

    if output["output_type"] == "managed_table":
        return {
            "table": output["table_name"],
            "format": output["format"],
        }
    else:  # external_table
        return {
            "connection": output["connection_name"],
            "path": output["path"],
            "format": output["format"],
        }
```

**CatalogManager method:**
```python
def get_node_output(self, pipeline_name: str, node_name: str) -> Optional[Dict]:
    """
    Retrieves output metadata for a specific node.

    Returns:
        Dict with output metadata or None if not found.
    """
    # Query meta_outputs table
    # Use caching for performance (similar to _pipelines_cache pattern)
```

---

### 4. Multi-Input Nodes (`inputs:` block)

**Config schema:**
```yaml
inputs:
  # Reference to another pipeline's node output
  events: $read_bronze.opsvisdata_ShiftDowntimeEventsview

  # Or explicit read config (for non-dependent reads)
  calendar:
    connection: goat_prod
    path: "bronze/OEE/vw_calender"
    format: delta
```

**Execution logic:**
```python
def execute_node_with_inputs(node: NodeConfig, engine: BaseEngine, catalog: CatalogManager):
    """
    Executes a node that has an inputs block.
    """
    dataframes = {}

    for name, ref in node.inputs.items():
        if isinstance(ref, str) and ref.startswith("$"):
            # Cross-pipeline reference
            read_config = resolve_input_reference(ref, catalog)
            dataframes[name] = engine.read(**read_config)
        elif isinstance(ref, dict):
            # Explicit read config
            dataframes[name] = engine.read(**ref)
        else:
            raise ValueError(f"Invalid input format for '{name}': {ref}")

    # Execute transforms with named dataframes
    result = execute_transforms(node.transforms, dataframes)

    return result
```

**Transform execution with named inputs:**
```python
def execute_transforms(transforms: List[TransformConfig], dataframes: Dict[str, DataFrame]):
    """
    Executes transforms using named dataframes.

    First transform references input names directly.
    Subsequent transforms can reference "_result" for chaining.
    """
    result = None

    for transform in transforms:
        if transform.operation == "join":
            left_df = dataframes.get(transform.left) or result
            right_df = dataframes.get(transform.right)
            result = left_df.join(right_df, on=transform.on, how=transform.how or "inner")
        # ... other operations

    return result
```

---

### 5. Config Schema Updates

**NodeConfig additions:**
```python
class NodeConfig(BaseModel):
    name: str
    description: Optional[str] = None

    # Existing
    read: Optional[ReadConfig] = None
    depends_on: Optional[List[str]] = None

    # New: multi-input support
    inputs: Optional[Dict[str, Union[str, ReadConfig]]] = None

    transform: Optional[List[TransformConfig]] = None
    write: Optional[WriteConfig] = None

    @validator("inputs", "read")
    def validate_input_source(cls, v, values):
        # Node must have either 'read' or 'inputs', not both
        if values.get("read") and values.get("inputs"):
            raise ValueError("Node cannot have both 'read' and 'inputs'")
        return v
```

---

## Engine Compatibility

| Feature | Spark | Pandas | Polars |
|---------|-------|--------|--------|
| `meta_outputs` writes | ✅ MERGE | ✅ upsert | ✅ upsert |
| `$pipeline.node` (path-based) | ✅ | ✅ | ✅ |
| `$pipeline.node` (managed table) | ✅ | ❌ | ❌ |
| `inputs:` block | ✅ | ✅ | ✅ |

**Best practice:** Always use `path:` in write config for cross-engine compatibility.

---

## Implementation Order

### Phase 1: Catalog Extension ✅ COMPLETE
1. ✅ Add `meta_outputs` table schema to CatalogManager
2. ✅ Add `_get_schema_meta_outputs()` method
3. ✅ Add `register_outputs_batch()` method (MERGE pattern)
4. ✅ Add `get_node_output()` query method with caching
5. ✅ Update `bootstrap()` to create meta_outputs table

### Phase 2: Pipeline Integration ✅ COMPLETE
1. ✅ Collect output metadata during node execution
2. ✅ Batch write to catalog at pipeline end
3. ✅ Add error handling for catalog write failures

### Phase 3: Reference Resolution ✅ COMPLETE
1. ✅ Add `resolve_input_reference()` function (odibi/references.py)
2. ✅ Validate references at pipeline load time (fail fast)
3. ✅ Add clear error messages for missing dependencies

### Phase 4: Multi-Input Nodes ✅ COMPLETE
1. ✅ Add `inputs` field to NodeConfig
2. ✅ Update node executor to handle inputs block (`_execute_inputs_phase()`)
3. ✅ Update transform executor for named dataframes
4. ✅ Add validation: node has either `read` or `inputs`, not both

### Phase 5: Testing ✅ COMPLETE
1. ✅ Unit tests for `register_outputs_batch()`
2. ✅ Unit tests for `resolve_input_reference()`
3. ✅ Integration test: bronze pipeline writes, silver reads via `$ref`
4. ✅ Performance test: verify single batch write (no per-node overhead)

**Implementation completed:** All 26 tests passing in `tests/unit/test_cross_pipeline_dependencies.py`

---

## Example: Full Bronze → Silver Flow

**Bronze pipeline (`read_bronze.yaml`):**
```yaml
pipeline: read_bronze
layer: bronze
nodes:
  - name: opsvisdata_ShiftDowntimeEventsview
    read:
      connection: opsvisdata
      format: sql
      table: OEE.ShiftDowntimeEventsview
    write:
      connection: goat_prod
      format: delta
      path: "bronze/OEE/shift_downtime_events"
      register_table: test.shift_downtime_events
      add_metadata: true

  - name: opsvisdata_vw_calender
    read:
      connection: opsvisdata
      format: sql
      table: OEE.vw_calender
    write:
      connection: goat_prod
      format: delta
      path: "bronze/OEE/vw_calender"
      register_table: test.vw_calender
```

**After bronze runs, `meta_outputs` contains:**
```
| pipeline_name | node_name                           | connection_name | path                           | format | table_name                  |
|---------------|-------------------------------------|-----------------|--------------------------------|--------|-----------------------------|
| read_bronze   | opsvisdata_ShiftDowntimeEventsview  | goat_prod       | bronze/OEE/shift_downtime_events | delta  | test.shift_downtime_events |
| read_bronze   | opsvisdata_vw_calender              | goat_prod       | bronze/OEE/vw_calender         | delta  | test.vw_calender            |
```

**Silver pipeline (`transform_silver.yaml`):**
```yaml
pipeline: transform_silver
layer: silver
nodes:
  - name: enriched_events
    inputs:
      events: $read_bronze.opsvisdata_ShiftDowntimeEventsview
      calendar: $read_bronze.opsvisdata_vw_calender
    transform:
      - operation: join
        left: events
        right: calendar
        on: [DateId]
    write:
      connection: goat_prod
      format: delta
      path: "silver/OEE/enriched_events"
```

**Resolution:**
```
$read_bronze.opsvisdata_ShiftDowntimeEventsview
  → catalog lookup → {connection: goat_prod, path: bronze/OEE/shift_downtime_events, format: delta}
  → engine.read(connection=goat_prod, path="bronze/OEE/shift_downtime_events", format="delta")
```

---

## Performance Considerations

1. **Batch writes only** — never write to catalog per-node
2. **Cache catalog queries** — `get_node_output()` should cache results
3. **Validate early** — resolve all `$references` at pipeline load, not execution
4. **Fail fast** — clear errors if referenced pipeline/node hasn't run
