# Cross-Pipeline Lineage

Track table-level lineage relationships across pipelines for impact analysis and data governance.

## Overview

Odibi tracks lineage at two levels:
- **OpenLineage integration**: Standards-based lineage emission
- **Cross-pipeline lineage**: Table-to-table relationships in the System Catalog

This document covers the cross-pipeline lineage tracking stored in `meta_lineage`.

## How It Works

1. During pipeline execution, read/write operations are recorded
2. Source → Target relationships are stored in `meta_lineage`
3. CLI commands query the lineage graph
4. Impact analysis identifies affected downstream tables

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   bronze/   │────▶│   silver/   │────▶│   gold/     │
│  customers  │     │ dim_customer│     │customer_360 │
└─────────────┘     └─────────────┘     └─────────────┘
       │                   │                   │
       │                   │                   │
   Pipeline A          Pipeline B          Pipeline C
```

## CLI Commands

### Trace Upstream Lineage

Find all sources for a table:

```bash
odibi lineage upstream <table> --config config.yaml
```

**Example:**
```bash
$ odibi lineage upstream gold/customer_360 --config pipeline.yaml

Upstream Lineage: gold/customer_360
============================================================
gold/customer_360
└── silver/dim_customers (silver_pipeline.process_customers)
    └── bronze/customers_raw (bronze_pipeline.ingest_customers)
```

**Options:**
```bash
odibi lineage upstream gold/customer_360 --config config.yaml \
    --depth 5 \         # Traverse up to 5 levels (default: 3)
    --format json       # Output as JSON
```

### Trace Downstream Lineage

Find all consumers of a table:

```bash
odibi lineage downstream <table> --config config.yaml
```

**Example:**
```bash
$ odibi lineage downstream bronze/customers_raw --config pipeline.yaml

Downstream Lineage: bronze/customers_raw
============================================================
bronze/customers_raw
├── silver/dim_customers (silver_pipeline.process_customers)
│   ├── gold/customer_360 (gold_pipeline.build_360)
│   └── gold/churn_features (ml_pipeline.build_features)
└── silver/customer_events (silver_pipeline.process_events)
```

### Impact Analysis

Assess the impact of changes to a table:

```bash
odibi lineage impact <table> --config config.yaml
```

**Example:**
```bash
$ odibi lineage impact bronze/customers_raw --config pipeline.yaml

⚠️  Impact Analysis: bronze/customers_raw
============================================================

Changes to bronze/customers_raw would affect:

  Affected Tables:
    - silver/dim_customers (pipeline: silver_pipeline)
    - gold/customer_360 (pipeline: gold_pipeline)
    - gold/churn_features (pipeline: ml_pipeline)
    - silver/customer_events (pipeline: silver_pipeline)

  Summary:
    Total: 4 downstream table(s) in 3 pipeline(s)
```

## Programmatic Access

### Using LineageTracker

```python
from odibi.lineage import LineageTracker
from odibi.catalog import CatalogManager

# Initialize
catalog = CatalogManager(spark, config, base_path, engine)
tracker = LineageTracker(catalog)

# Record lineage manually
tracker.record_lineage(
    read_config=node.read,
    write_config=node.write,
    pipeline="my_pipeline",
    node="process_data",
    run_id="run-12345",
    connections=connections,
)

# Query upstream
upstream = tracker.get_upstream("gold/customer_360", depth=3)
for record in upstream:
    print(f"{record['source_table']} → {record['target_table']}")

# Query downstream
downstream = tracker.get_downstream("bronze/customers_raw", depth=3)

# Impact analysis
impact = tracker.get_impact_analysis("bronze/customers_raw")
print(f"Affected tables: {impact['affected_tables']}")
print(f"Affected pipelines: {impact['affected_pipelines']}")
```

### Direct Catalog Access

```python
# Record lineage directly
catalog.record_lineage(
    source_table="bronze/customers_raw",
    target_table="silver/dim_customers",
    target_pipeline="silver_pipeline",
    target_node="process_customers",
    run_id="run-12345",
    relationship="feeds",
)

# Query upstream
upstream = catalog.get_upstream("gold/customer_360", depth=3)

# Query downstream
downstream = catalog.get_downstream("bronze/customers_raw", depth=3)
```

## Lineage Record Structure

Each lineage record includes:

| Field | Description |
|-------|-------------|
| `source_table` | Source table path |
| `target_table` | Target table path |
| `source_pipeline` | Pipeline reading from source |
| `source_node` | Node reading from source |
| `target_pipeline` | Pipeline writing to target |
| `target_node` | Node writing to target |
| `relationship` | Type: "feeds" or "derived_from" |
| `last_observed` | Last time this relationship was seen |
| `run_id` | Run ID when recorded |

## Automatic Tracking

Lineage is automatically tracked when:
1. A node has both `read` and `write` configurations
2. The System Catalog is configured
3. The pipeline runs successfully

```yaml
nodes:
  - name: process_customers
    read:
      connection: bronze
      path: customers_raw
      format: delta

    transform:
      steps:
        - sql: "SELECT * FROM df WHERE active = true"

    write:
      connection: silver
      path: dim_customers
      format: delta
```

This automatically records: `bronze/customers_raw → silver/dim_customers`

## Dependency-Based Lineage

Lineage is also tracked for `depends_on` relationships:

```yaml
nodes:
  - name: source_node
    read: { connection: bronze, path: raw_data }
    write: { connection: silver, path: processed_data }

  - name: consumer_node
    depends_on: [source_node]  # Lineage tracked!
    transform:
      steps:
        - sql: "SELECT * FROM source_node"
    write: { connection: gold, path: final_data }
```

## Storage Location

Lineage is stored in the System Catalog:

```yaml
system:
  connection: adls_bronze
  path: _odibi_system
```

Location: `{connection_base_path}/_odibi_system/meta_lineage/`

## Example: Pre-Deployment Impact Check

Before deploying schema changes, check impact:

```python
def pre_deployment_check(catalog, table_to_change):
    """Check impact before deploying changes."""
    downstream = catalog.get_downstream(table_to_change, depth=5)

    if not downstream:
        print(f"✅ No downstream dependencies for {table_to_change}")
        return True

    affected_tables = set()
    affected_pipelines = set()

    for record in downstream:
        affected_tables.add(record['target_table'])
        if record.get('target_pipeline'):
            affected_pipelines.add(record['target_pipeline'])

    print(f"⚠️  Changes to {table_to_change} will affect:")
    print(f"   - {len(affected_tables)} tables")
    print(f"   - {len(affected_pipelines)} pipelines")

    for table in sorted(affected_tables):
        print(f"     • {table}")

    return len(downstream) == 0
```

## Integration with Schema Tracking

Combine lineage with schema tracking for comprehensive governance:

```python
def assess_schema_change_impact(catalog, table_path):
    """Assess impact of recent schema changes."""
    # Get schema changes
    history = catalog.get_schema_history(table_path, limit=2)
    if len(history) < 2:
        return

    latest = history[0]
    removed = latest.get('columns_removed', [])

    if removed:
        # Check downstream impact
        downstream = catalog.get_downstream(table_path)
        print(f"⚠️  Columns {removed} were removed from {table_path}")
        print(f"   This may break {len(downstream)} downstream tables")
```

## Best Practices

1. **Run impact analysis before changes** - Know what you'll affect
2. **Use consistent table naming** - Makes lineage easier to follow
3. **Document cross-pipeline boundaries** - Clarify ownership
4. **Monitor lineage depth** - Deep chains may indicate complexity
5. **Integrate with CI/CD** - Block deployments with unknown impact

## Related

- [Schema Version Tracking](schema_tracking.md) - Track schema changes
- [OpenLineage Integration](../reference/yaml_schema.md#lineageconfig) - Standards-based lineage
