# Schema Version Tracking

Track schema changes over time with automatic versioning, change detection, and CLI tools.

## Overview

Odibi automatically tracks schema changes in the System Catalog:
- **Version history**: Every schema change creates a new version
- **Change detection**: Identifies added, removed, and modified columns
- **CLI tools**: Query history and compare versions

## How It Works

1. After each pipeline run, the output schema is captured
2. Schema is hashed and compared to the previous version
3. If changed, a new version is recorded with change details
4. History is stored in `meta_schemas` table

## Automatic Tracking

Schema tracking happens automatically during pipeline execution when:
- A node writes to a table/path
- The System Catalog is configured

No additional configuration is required.

## CLI Commands

### View Schema History

```bash
odibi schema history <table> --config config.yaml
```

**Example:**
```bash
$ odibi schema history silver/customers --config pipeline.yaml

Schema History: silver/customers
================================================================================
Version    Captured At            Changes
--------------------------------------------------------------------------------
v5         2024-01-30 10:15:00    +loyalty_tier
v4         2024-01-15 08:30:00    ~email (VARCHAR→STRING)
v3         2024-01-01 12:00:00    -legacy_id
v2         2023-12-15 09:00:00    +created_at, +updated_at
v1         2023-12-01 10:00:00    Initial schema (12 columns)
```

**Options:**
```bash
odibi schema history <table> --config config.yaml \
    --limit 20 \        # Show last 20 versions (default: 10)
    --format json       # Output as JSON
```

### Compare Schema Versions

```bash
odibi schema diff <table> --config config.yaml --from-version 3 --to-version 5
```

**Example:**
```bash
$ odibi schema diff silver/customers --config pipeline.yaml --from-version 3 --to-version 5

Schema Diff: silver/customers
From v3 → v5
============================================================
  customer_id                  STRING               (unchanged)
  email                        STRING               (unchanged)
  name                         STRING               (unchanged)
- legacy_id                    STRING               (removed in v5)
+ loyalty_tier                 STRING               (added in v5)
+ created_at                   TIMESTAMP            (added in v5)
+ updated_at                   TIMESTAMP            (added in v5)
```

**Without versions** (compares latest two):
```bash
odibi schema diff silver/customers --config pipeline.yaml
```

## Programmatic Access

### Track Schema Manually

```python
from odibi.catalog import CatalogManager

catalog = CatalogManager(spark, config, base_path, engine)

# Track a schema change
result = catalog.track_schema(
    table_path="silver/customers",
    schema={"customer_id": "STRING", "email": "STRING", "age": "INT"},
    pipeline="customer_pipeline",
    node="process_customers",
    run_id="run-12345",
)

print(f"Changed: {result['changed']}")
print(f"Version: {result['version']}")
print(f"Columns added: {result.get('columns_added', [])}")
print(f"Columns removed: {result.get('columns_removed', [])}")
print(f"Types changed: {result.get('columns_type_changed', [])}")
```

### Query Schema History

```python
# Get history for a table
history = catalog.get_schema_history("silver/customers", limit=10)

for record in history:
    print(f"v{record['schema_version']}: {record['captured_at']}")
    if record.get('columns_added'):
        print(f"  Added: {record['columns_added']}")
```

## Schema Record Structure

Each schema version record includes:

| Field | Description |
|-------|-------------|
| `table_path` | Full path to the table |
| `schema_version` | Auto-incrementing version number |
| `schema_hash` | MD5 hash of column definitions |
| `columns` | JSON of column names and types |
| `captured_at` | Timestamp of capture |
| `pipeline` | Pipeline that made the change |
| `node` | Node that made the change |
| `run_id` | Execution run ID |
| `columns_added` | List of new columns |
| `columns_removed` | List of removed columns |
| `columns_type_changed` | List of columns with type changes |

## Storage Location

Schema history is stored in the System Catalog:

```yaml
system:
  connection: adls_bronze
  path: _odibi_system
```

Location: `{connection_base_path}/_odibi_system/meta_schemas/`

## Example: Detecting Breaking Changes

Use schema tracking to detect breaking changes before they impact downstream:

```python
def check_for_breaking_changes(catalog, table_path):
    """Check if recent schema changes might break downstream."""
    history = catalog.get_schema_history(table_path, limit=2)

    if len(history) < 2:
        return False  # No previous version

    latest = history[0]
    removed = latest.get('columns_removed', [])
    type_changes = latest.get('columns_type_changed', [])

    if removed or type_changes:
        print(f"⚠️ Potential breaking changes in {table_path}")
        if removed:
            print(f"  Removed columns: {removed}")
        if type_changes:
            print(f"  Type changes: {type_changes}")
        return True

    return False
```

## Integration with Lineage

Combine schema tracking with lineage to assess impact:

```bash
# Check what would be affected by a schema change
odibi lineage impact silver/customers --config config.yaml
```

```
⚠️  Impact Analysis: silver/customers
============================================================

Changes to silver/customers would affect:

  Affected Tables:
    - gold/customer_360 (pipeline: gold_pipeline)
    - gold/churn_features (pipeline: ml_pipeline)

  Summary:
    Total: 2 downstream table(s) in 2 pipeline(s)
```

## Best Practices

1. **Review schema changes** - Check history after deployments
2. **Monitor for removals** - Removed columns often break downstream
3. **Document type changes** - Type changes may affect queries
4. **Use lineage for impact** - Know what's affected before changing
5. **Automate checks** - Add schema validation to CI/CD

## Related

- [Cross-Pipeline Lineage](lineage.md) - Impact analysis
- [YAML Schema Reference](../reference/yaml_schema.md#systemconfig)
