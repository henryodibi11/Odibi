# Cross-Pipeline Dependencies

**Last Updated:** 2025-12-03  
**Status:** ✅ Implemented

---

## Overview

Cross-pipeline dependencies enable pipelines to reference outputs from other pipelines using the `$pipeline.node` syntax. This is essential for implementing the **medallion architecture** pattern where silver nodes depend on bronze outputs, and gold nodes depend on silver outputs.

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Bronze    │────▶│   Silver    │────▶│    Gold     │
│  Pipeline   │     │  Pipeline   │     │  Pipeline   │
└─────────────┘     └─────────────┘     └─────────────┘
       │                   │                   │
       ▼                   ▼                   ▼
   writes to          reads from          reads from
   meta_outputs       $read_bronze.*      $transform_silver.*
```

---

## Use Cases

### 1. Medallion Architecture (Bronze → Silver → Gold)

The most common pattern: ingest raw data in bronze, clean/enrich in silver, aggregate for business in gold.

```yaml
# Bronze: Raw ingestion
pipeline: read_bronze
nodes:
  - name: raw_orders
    read:
      connection: source_db
      table: sales.orders
    write:
      connection: lakehouse
      format: delta
      path: "bronze/orders"

# Silver: Enriched data
pipeline: transform_silver
nodes:
  - name: enriched_orders
    inputs:
      orders: $read_bronze.raw_orders        # ← Cross-pipeline reference
      customers: $read_bronze.raw_customers
    transform:
      steps:
        - operation: join
          left: orders
          right: customers
          on: [customer_id]
    write:
      connection: lakehouse
      format: delta
      path: "silver/enriched_orders"

# Gold: Business aggregates
pipeline: build_gold
nodes:
  - name: daily_sales
    inputs:
      orders: $transform_silver.enriched_orders
    transform:
      steps:
        - sql: |
            SELECT
              DATE(order_date) as date,
              SUM(amount) as total_sales
            FROM orders
            GROUP BY 1
    write:
      connection: lakehouse
      format: delta
      path: "gold/daily_sales"
```

### 2. Multi-Source Joins

When a node needs to join data from multiple sources:

```yaml
- name: enriched_downtime
  inputs:
    events: $read_bronze.shift_events
    calendar: $read_bronze.calendar_dim
    plant: $read_bronze.plant_dim
  transform:
    steps:
      - operation: join
        left: events
        right: calendar
        on: [date_id]
      - operation: join
        right: plant
        on: [plant_id]
```

### 3. Mixing References with Explicit Reads

You can combine cross-pipeline references with explicit read configs:

```yaml
- name: combined_data
  inputs:
    # Cross-pipeline reference
    events: $read_bronze.events

    # Explicit read (for data not from another pipeline)
    reference_data:
      connection: static_files
      path: "reference/lookup_table.csv"
      format: csv
```

---

## YAML Syntax

### The `inputs` Block

```yaml
nodes:
  - name: node_name
    inputs:
      <input_name>: $<pipeline_name>.<node_name>    # Cross-pipeline reference
      <input_name>:                                  # Explicit read config
        connection: <connection_name>
        path: <path>
        format: <format>
```

### Reference Syntax: `$pipeline.node`

| Component | Description |
|-----------|-------------|
| `$` | Prefix indicating a cross-pipeline reference |
| `pipeline` | Name of the source pipeline (from `pipeline:` field) |
| `.` | Separator |
| `node` | Name of the source node (from `name:` field) |

**Examples:**
- `$read_bronze.orders` → Output from node `orders` in pipeline `read_bronze`
- `$ingest_daily.customers` → Output from node `customers` in pipeline `ingest_daily`

---

## How the `meta_outputs` Catalog Table Works

When a node with a `write` block completes, its output metadata is recorded in the system catalog.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `pipeline_name` | STRING | Pipeline identifier |
| `node_name` | STRING | Node identifier |
| `output_type` | STRING | `"external_table"` or `"managed_table"` |
| `connection_name` | STRING | Connection used (for external tables) |
| `path` | STRING | Storage path |
| `format` | STRING | Data format (delta, parquet, etc.) |
| `table_name` | STRING | Registered table name (if any) |
| `last_run` | TIMESTAMP | Last execution time |
| `row_count` | LONG | Row count at last write |
| `updated_at` | TIMESTAMP | Record update time |

### Resolution Flow

```
1. Silver node has: inputs: {events: $read_bronze.shift_events}

2. At load time, Odibi queries meta_outputs:
   SELECT * FROM meta_outputs
   WHERE pipeline_name = 'read_bronze' AND node_name = 'shift_events'

3. Returns: {connection: 'goat_prod', path: 'bronze/OEE/shift_events', format: 'delta'}

4. At runtime: engine.read(connection='goat_prod', path='bronze/OEE/shift_events', format='delta')
```

---

## Performance Notes

### Batch Writes Only

Output metadata is collected in-memory during pipeline execution and written to the catalog **once** at pipeline completion. This avoids per-node I/O overhead.

**Before optimization:** 17 nodes × ~2-3s = ~40s overhead  
**After optimization:** Single batch MERGE = ~2s total

### Caching

The `get_node_output()` method uses caching to avoid repeated catalog queries within the same session.

### Validate Early

All `$references` are validated at pipeline load time (fail fast), not at execution time. This provides immediate feedback if a referenced pipeline hasn't run.

---

## Troubleshooting

### Error: "No output found for $pipeline.node"

```
ReferenceResolutionError: No output found for $read_bronze.shift_events.
Ensure pipeline 'read_bronze' has run and node 'shift_events' has a write block.
```

**Causes:**
1. The referenced pipeline hasn't been run yet
2. The referenced node doesn't have a `write` block
3. Typo in pipeline or node name

**Solutions:**
1. Run the source pipeline first: `odibi run bronze.yaml`
2. Add a `write` block to the source node
3. Check spelling matches exactly (case-sensitive)

### Error: "Cannot have both 'read' and 'inputs'"

```
ValidationError: Node 'my_node': Cannot have both 'read' and 'inputs'.
Use 'read' for single-source nodes or 'inputs' for multi-source cross-pipeline dependencies.
```

**Solution:** Choose one approach:
- Use `read` for simple single-source reads
- Use `inputs` for multi-source or cross-pipeline reads

### Error: "Invalid reference format"

```
ValueError: Invalid reference format: $read_bronze. Expected $pipeline.node
```

**Solution:** Ensure the reference includes both pipeline and node names separated by a dot.

---

## Engine Compatibility

| Feature | Spark | Pandas | Polars |
|---------|:-----:|:------:|:------:|
| `meta_outputs` writes | ✅ | ✅ | ✅ |
| `$pipeline.node` (path-based) | ✅ | ✅ | ✅ |
| `$pipeline.node` (managed table) | ✅ | ❌ | ❌ |
| `inputs:` block | ✅ | ✅ | ✅ |

**Best Practice:** Always use `path:` in write config for cross-engine compatibility.

---

## Files Changed in Implementation

| File | Changes |
|------|---------|
| `odibi/catalog.py` | Added `meta_outputs` table, `register_outputs_batch()`, `get_node_output()` |
| `odibi/config.py` | Added `inputs` field to `NodeConfig` |
| `odibi/node.py` | Added `_execute_inputs_phase()`, `_create_output_record()` |
| `odibi/pipeline.py` | Added batch output registration at pipeline end |
| `odibi/references.py` | New module for reference resolution |
| `tests/unit/test_cross_pipeline_dependencies.py` | 26 new tests |

---

## Related Documentation

- [Configuration Reference](../reference/configuration.md) - NodeConfig with `inputs` field
- [YAML Schema Reference](../reference/yaml_schema.md) - Full schema documentation
- [Catalog Feature](catalog.md) - System catalog details
- [Pipelines](pipelines.md) - Pipeline execution flow
