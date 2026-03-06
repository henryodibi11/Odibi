# Scaffolding and Validation Modules - Implementation Complete

## Summary

Successfully implemented scaffolding and validation modules in odibi core, porting logic from `odibi_mcp/tools/` and integrating it directly into the framework.

## What Was Implemented

### 1. `odibi/scaffold/` Module

**Files Created:**
- `odibi/scaffold/__init__.py` - Public API exports
- `odibi/scaffold/project.py` - Project YAML generation
- `odibi/scaffold/pipeline.py` - Pipeline YAML generation and name sanitization

**Key Functions:**
- `sanitize_node_name(name: str)` - Sanitizes node names to alphanumeric + underscore
- `generate_project_yaml(...)` - Generates complete project.yaml with connections, story, system
- `generate_sql_pipeline(...)` - Generates SQL ingestion pipeline YAML with:
  - Table specs (schema, table, WHERE clause, columns)
  - Incremental loading configuration
  - Target format and schema options
  - Node naming and sanitization

### 2. `odibi/validate/` Module

**Files Created:**
- `odibi/validate/__init__.py` - Public API exports
- `odibi/validate/pipeline.py` - Comprehensive YAML validation

**Key Functions:**
- `validate_yaml(yaml_content: str)` - Validates pipeline/project YAML with:
  - YAML syntax checking
  - Pydantic model validation
  - Pattern parameter validation
  - Transformer parameter validation
  - DAG dependency validation
  - Common mistake detection (source: vs read:, etc.)
  - Structured error/warning responses with fix suggestions

**Validation Covers:**
- Project configs (required/recommended keys)
- Connection configs (type validation)
- Pipeline configs (name, nodes, layer)
- Node configs (name format, read/write blocks, dependencies)
- Pattern-specific required parameters
- Transformer existence and parameter validation

### 3. PipelineManager Integration

**Added Methods to `odibi/pipeline.py`:**
```python
def scaffold_project(self, project_name, connections, **kwargs) -> str
def scaffold_sql_pipeline(self, pipeline_name, source_connection, target_connection, tables, **kwargs) -> str
def validate_yaml(self, yaml_content: str) -> Dict[str, Any]
```

These convenience methods allow:
```python
manager = PipelineManager.from_yaml("config.yaml")

# Generate project scaffold
yaml = manager.scaffold_project("my_project", connections)

# Generate SQL pipeline
yaml = manager.scaffold_sql_pipeline("ingest", "sqldb", "lake", tables)

# Validate YAML
result = manager.validate_yaml(yaml_string)
```

### 4. Test Coverage

**Unit Tests (`tests/unit/test_scaffold_validate.py`):**
- 31 tests covering all scaffold and validation functions
- Tests for edge cases, error handling, and validation rules
- All tests passing ✅

**Integration Tests (`tests/integration/test_pipeline_manager_scaffold.py`):**
- 5 tests verifying PipelineManager integration
- Roundtrip test: scaffold → validate → success
- All tests passing ✅

**Total: 36 tests, 100% passing**

## Validation Features

The validation engine catches:

### Errors (Block Execution)
- YAML syntax errors
- Missing required keys (project, connections, pipeline name)
- Invalid node names (non-alphanumeric characters)
- Wrong keys (source: vs read:, sink: vs write:)
- Missing dependencies in DAG
- Missing pattern required parameters
- Unknown transformers
- Invalid transformer parameters
- Pydantic validation failures

### Warnings (Non-Blocking)
- Missing recommended keys (story, system)
- Deprecated key usage (inputs:, outputs:)
- Empty node lists

### Structured Response Format
```python
{
    "valid": bool,
    "errors": [
        {
            "code": "ERROR_CODE",
            "field_path": "pipelines[0].nodes[1].name",
            "message": "Descriptive error message",
            "fix": "How to fix it"
        }
    ],
    "warnings": [{"code": "...", "message": "..."}],
    "summary": "3 error(s), 1 warning(s)"
}
```

## Code Quality

- ✅ All tests passing (36/36)
- ✅ Ruff linting passed
- ✅ No diagnostics errors
- ✅ Follows odibi conventions
- ✅ Comprehensive docstrings
- ✅ Type hints where appropriate

## Usage Examples

### Scaffold a Project
```python
from odibi.scaffold import generate_project_yaml

connections = {
    "local": {"type": "local", "base_path": "data/"},
    "azure": {"type": "azure_blob", "account_name": "myaccount"}
}

yaml_str = generate_project_yaml(
    "my_project",
    connections,
    imports=["pipelines/bronze/ingest.yaml"],
    story_connection="azure"
)
```

### Scaffold a SQL Pipeline
```python
from odibi.scaffold import generate_sql_pipeline

tables = [
    {
        "schema": "dbo",
        "table": "customers",
        "primary_key": ["id"],
        "incremental_column": "updated_at",
        "incremental_mode": "rolling_window",
        "incremental_lookback": 7,
        "incremental_unit": "day"
    },
    {"schema": "dbo", "table": "orders", "where": "status = 'active'"}
]

yaml_str = generate_sql_pipeline(
    "ingest_crm",
    "crm_db",
    "data_lake",
    tables,
    target_format="delta",
    target_schema="bronze",
    layer="bronze"
)
```

### Validate YAML
```python
from odibi.validate import validate_yaml

result = validate_yaml(yaml_content)
if not result["valid"]:
    for error in result["errors"]:
        print(f"{error['field_path']}: {error['message']}")
        print(f"  Fix: {error['fix']}")
```

### Via PipelineManager
```python
manager = PipelineManager.from_yaml("config.yaml")

# Scaffold
project_yaml = manager.scaffold_project("new_project", connections)
pipeline_yaml = manager.scaffold_sql_pipeline("ingest", "db", "lake", tables)

# Validate
result = manager.validate_yaml(pipeline_yaml)
print(result["summary"])
```

## Benefits

1. **Self-Service YAML Generation** - Users can generate valid YAML without knowing schema details
2. **Early Error Detection** - Validation catches mistakes before pipeline execution
3. **Better Error Messages** - Structured errors with field paths and fix suggestions
4. **Framework Integration** - Works seamlessly with PipelineManager
5. **MCP Tool Foundation** - These modules power the MCP server's builder and validator tools

## Next Steps

This implementation is complete and ready for use. Potential enhancements:
- Add CLI commands (`odibi scaffold`, `odibi validate`)
- Add more pattern-specific scaffolders (SCD2, Merge, etc.)
- Generate sample data alongside YAML
- Interactive scaffold builder (prompt for inputs)

## Related Files

- Source: `odibi_mcp/tools/yaml_builder.py` (original implementation)
- Source: `odibi_mcp/tools/validation.py` (original implementation)
- Core: `odibi/scaffold/` (new module)
- Core: `odibi/validate/` (new module)
- Integration: `odibi/pipeline.py` (PipelineManager methods)
- Tests: `tests/unit/test_scaffold_validate.py`
- Tests: `tests/integration/test_pipeline_manager_scaffold.py`
