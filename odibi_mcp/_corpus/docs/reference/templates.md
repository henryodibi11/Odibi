# Template Generator

Generate YAML templates and documentation directly from Pydantic models. Templates are always in sync with the code—no manual maintenance required.

## Quick Start

```bash
# List all available template types
odibi templates list

# Show YAML template for a connection type
odibi templates show azure_blob

# Show transformer documentation with params
odibi templates transformer scd2

# Generate JSON schema for VS Code autocomplete
odibi templates schema --output odibi.schema.json
```

## CLI Commands

### `odibi templates list`

List all available template types grouped by category.

```bash
odibi templates list
odibi templates list --category connections
odibi templates list --format json
```

**Categories:**
- `connections` - 7 types: local, azure_blob, azure_adls, delta, sql_server, azure_sql, http
- `patterns` - 6 types: dimension, fact, scd2, merge, aggregation, date_dimension
- `configs` - 10 types: project, pipeline, node, read, write, transform, validation, incremental, quarantine, gate

### `odibi templates show <name>`

Show a complete YAML template for any connection, pattern, or config type.

```bash
odibi templates show azure_blob
odibi templates show sql_server
odibi templates show dimension
odibi templates show validation
```

**Options:**
- `--required-only` / `-r` - Only show required fields
- `--no-comments` - Omit description comments

**Example output:**
```yaml
# Azure Blob Storage / ADLS Gen2 connection.

type: azure_blob  # optional
account_name: null  # REQUIRED
container: null  # REQUIRED
auth:  # OPTIONS: key_vault | account_key | sas | connection_string | aad_msi
  # --- Option: key_vault ---
  # mode: key_vault  # optional
  # key_vault: null  # REQUIRED
  # secret: null  # REQUIRED
  # --- Option: account_key ---
  # mode: account_key  # optional
  # account_key: null  # REQUIRED
  # ...
```

### `odibi templates transformer <name>`

Show documentation and example YAML for any of the 56+ transformers.

```bash
odibi templates transformer scd2
odibi templates transformer fluid_properties
odibi templates transformer psychrometrics
```

**Options:**
- `--no-example` - Omit the YAML example snippet

**Example output:**
```markdown
# scd2
Implements SCD Type 2 Logic.

## Parameters
| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| keys | List[str] | Yes | - | Natural keys to identify unique entities |
| track_cols | List[str] | Yes | - | Columns to monitor for changes |
| effective_time_col | str | Yes | - | Source column indicating when change occurred |
...

## Example YAML
- operation: scd2
  params:
    keys: <value>
    track_cols: <value>
    effective_time_col: <value>
```

### `odibi templates schema`

Generate a JSON Schema file for VS Code autocomplete.

```bash
odibi templates schema
odibi templates schema --output odibi.schema.json
odibi templates schema --no-transformers
```

**Options:**
- `--output` / `-o` - Output file path (default: `odibi.schema.json`)
- `--no-transformers` - Exclude transformer definitions
- `--verbose` / `-v` - Show additional output

## Python API

Use the template generator programmatically:

```python
from odibi.tools import show_template, show_transformer, generate_json_schema, list_templates

# List all available templates
templates = list_templates()
# {'connections': ['local', 'azure_blob', ...], 'patterns': [...], 'configs': [...]}

# Generate a connection template
print(show_template("azure_blob"))

# Generate transformer docs
print(show_transformer("scd2"))

# Generate JSON schema
schema = generate_json_schema(output_path="odibi.schema.json")
```

## VS Code Integration

The JSON schema enables autocomplete in VS Code. Configure in `.vscode/settings.json`:

```json
{
    "yaml.schemas": {
        "./odibi.schema.json": [
            "odibi.yaml",
            "project.yaml",
            "**/pipelines/*.yaml"
        ]
    }
}
```

Requires the [YAML extension](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml).

## Template Features

Templates automatically include:

| Feature | Example |
|---------|---------|
| Field descriptions | `# Base directory path` |
| Required/optional markers | `# REQUIRED`, `# optional` |
| Constraints | `(0.0-1.0)`, `(1-)` |
| Enum values | `overwrite \| append \| upsert` |
| All union variants | Shows all 5 auth modes for connections |
| List item options | Shows all 11 validation test types |
| Class docstrings | Section headers from model docstrings |

## Keeping Templates in Sync

Templates are generated directly from the Pydantic models in `odibi/config.py`. When you add or modify a field:

1. Update the Pydantic model with `Field(description="...")`
2. Templates automatically reflect the change
3. Regenerate the JSON schema: `odibi templates schema`

No manual documentation updates needed—the code is the source of truth.
