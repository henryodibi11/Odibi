# High Water Mark (HWM) Pattern Design

## Overview

The HWM (High Water Mark) pattern enables efficient incremental data loading by:
1. **First run**: Loading the complete historical dataset
2. **Subsequent runs**: Loading only new/changed records

This design document covers the implementation, rationale, and usage.

---

## Design Decisions

### 1. `first_run_query` Placement

**Decision**: Place in `WriteConfig` (not `ReadConfig`)

**Rationale**:
- Write config controls table creation semantics
- On first run, target doesn't exist → write mode is irrelevant
- Read config should be stateless (independent of target table state)

**Example**:
```yaml
write:
  connection: adls
  format: delta
  path: raw/orders
  mode: append                    # Used on subsequent runs
  first_run_query: SELECT * FROM dbo.orders
```

### 2. No `first_run_mode` Parameter

**Decision**: Simplify by removing `first_run_mode`

**Rationale**:
- First run always creates a fresh table (OVERWRITE)
- Second+ runs use configured mode (usually APPEND)
- No need for separate mode configuration

### 3. Options Independence

**Decision**: Options (cluster_by, partition_by, etc.) work normally

**Rationale**:
- Liquid clustering and partitioning applied at table creation (first run)
- No special handling needed
- Works transparently with HWM pattern

### 4. Path-Based Support

**Decision**: Support both catalog tables and path-based Delta

**Rationale**:
- Works without Unity Catalog (ADLS + path-based Delta)
- Works with Spark catalog tables
- Flexible for different deployment scenarios

---

## Implementation Details

### Table Existence Detection

Three engines implement `table_exists()`:

**Spark - Catalog Table**:
```python
self.spark.catalog.tableExists(table)
```

**Spark - Path-Based Delta**:
```python
DeltaTable.isDeltaTable(self.spark, full_path)
```

**Pandas - File Path**:
```python
os.path.exists(full_path)
```

### Write Mode Override

The Node class uses `_determine_write_mode()` to detect first run:

```
If target doesn't exist AND first_run_query configured:
  → return WriteMode.OVERWRITE
Else:
  → return None (use configured mode)
```

---

## Edge Cases Handled

1. **Empty target table**: Treated as non-existent (safe)
2. **Concurrent executions**: Idempotent if using OVERWRITE
3. **Partial failures**: User can re-run; no special recovery needed
4. **Multi-target pipelines**: Each node has independent first-run logic

---

## Supported Scenarios

✅ ADLS + Delta (no Unity Catalog)
✅ Spark catalog tables
✅ Pandas file-based (local + cloud)
✅ Liquid clustering at table creation
✅ Partitioning
✅ External table registration
✅ Multi-target pipelines

---

## Complete Example

```yaml
pipeline: orders_load
nodes:
  - name: load_orders
    read:
      connection: sql_server
      format: sql_server
      table: dbo.orders
      options:
        query: |
          SELECT * FROM dbo.orders 
          WHERE updated_at > (
            SELECT COALESCE(MAX(updated_at), '1900-01-01') 
            FROM raw.orders
          )
    
    write:
      connection: adls
      format: delta
      path: raw/orders
      mode: append
      register_table: raw.orders
      first_run_query: SELECT * FROM dbo.orders
      options:
        cluster_by: order_date
        optimize_write: true
```

**Behavior**:
- **Run 1**: Loads all orders, creates Delta table with clustering
- **Run 2+**: Loads incremental orders, appends to existing table

---

## See Also

- `HWM_IMPLEMENTATION_SUMMARY.md` - Implementation details
- `IMPLEMENTATION_COMPLETE.md` - Testing and validation
