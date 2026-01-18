# Odibi Project Rules for Continue

## IMPORTANT: Use MCP Tools First!

Before writing ANY odibi code, call these odibi-knowledge MCP tools:

### Core Tools
- `list_transformers` - See all 52+ available transformers
- `list_patterns` - See all 6 DWH patterns
- `list_connections` - See all connection types
- `explain(name)` - Get docs for any transformer/pattern/connection

### Code Generation Tools
- `get_transformer_signature` - Get exact function signature for custom transformers
- `get_yaml_structure` - Get exact YAML pipeline structure
- `generate_transformer` - Generate complete transformer Python code
- `generate_pipeline_yaml` - Generate complete pipeline YAML config
- `validate_yaml` - Validate YAML before saving

### Decision Support Tools
- `suggest_pattern` - Recommend the right pattern for your use case
- `get_engine_differences` - Spark vs Pandas vs Polars SQL differences
- `get_validation_rules` - All validation rule types with examples

### Documentation Tools
- `get_deep_context` - Get full framework documentation (2300+ lines)
- `get_doc(path)` - Get any specific doc (e.g., "docs/patterns/scd2.md")
- `search_docs(query)` - Search all docs for a term
- `get_example(name)` - Get working example for any pattern/transformer

### Debugging Tools
- `diagnose_error` - Diagnose odibi errors and get fix suggestions
- `query_codebase` - Semantic search over odibi code (RAG)

**Example workflow for creating a custom transformer:**
1. Call `get_transformer_signature` to get the exact pattern
2. Call `list_transformers` to see if similar exists
3. Write the code following the exact signature
4. Call `get_yaml_structure` before writing YAML config

## Required Reading

Before making changes to odibi, read `docs/ODIBI_DEEP_CONTEXT.md` for complete framework context (2,200+ lines covering all features).

## File Paths

Always use absolute paths starting with `D:/odibi/` when creating files.
Example: `D:/odibi/examples/custom_transformers/data/products.csv`
Do NOT use relative paths like `examples/custom_transformers/...`

## Project Structure

```
odibi/
├── engine/          # Engine implementations (spark.py, pandas.py, polars.py)
├── patterns/        # DWH patterns (dimension.py, fact.py, scd2.py, etc.)
├── transformers/    # SQL transforms (must work on all engines)
├── validation/      # Data quality, quarantine, FK validation
├── connections/     # Storage connections (local, azure, delta, etc.)
├── cli/             # CLI commands
├── config.py        # All Pydantic config models (source of truth)
├── context.py       # Engine contexts (Spark temp views, DuckDB)
├── pipeline.py      # Pipeline orchestration
tests/unit/          # Tests matching source structure
docs/                # Documentation
```

## Key Patterns

1. **Engine Parity**: All code must work on Pandas, Spark, AND Polars
2. **SQL-based transforms**: Use SQL (DuckDB for Pandas, Spark SQL for Spark)
3. **Pydantic models**: All config uses Pydantic for validation
4. **Structured logging**: Use `get_logging_context()` for logs

## Critical Runtime Behavior

- Spark: Node outputs registered via `createOrReplaceTempView(node_name)`
- Pandas: SQL executed via DuckDB (different syntax than Spark SQL)
- Node names: Must be alphanumeric + underscore only (Spark compatibility)

## Adding New Features

### New Transformer
1. Check existing: `odibi list transformers`
2. Add to `odibi/transformers/` following existing patterns
3. Register in `odibi/transformers/__init__.py`
4. Add tests in `tests/unit/transformers/`
5. Must work on Pandas AND Spark (engine parity)

### New Pattern
1. Check existing: `odibi list patterns`
2. Add to `odibi/patterns/` extending `Pattern` base class
3. Register in `odibi/patterns/__init__.py`
4. Add tests

### New Connection
1. Check existing: `odibi list connections`
2. Add to `odibi/connections/`
3. Register in `odibi/connections/factory.py`

## Testing

```bash
pytest tests/ -v                        # Run all tests
pytest tests/unit/test_X.py -v          # Run specific test
pytest tests/unit/test_X.py::test_name  # Run single test
pytest --tb=short                       # Shorter tracebacks
```

## Linting (RUN AFTER EVERY FILE CHANGE!)

```bash
ruff check <file> --fix                 # Fix lint issues
ruff format <file>                      # Format code
```
**ALWAYS run both commands after creating or modifying any Python file.**

## Don't

- Don't add features without tests
- Don't break engine parity (if Pandas has it, Spark/Polars need it too)
- Don't use hyphens, dots, or spaces in node names
- Don't suppress linter errors with `# type: ignore` without good reason
- Don't hardcode paths - use connection configs
- Don't use emojis in Python code (Windows encoding issues with charmap codec)

## Code Style

- No comments explaining obvious code
- Docstrings for public functions
- Type hints for function signatures
- Follow existing patterns in the codebase

## Debugging Tips

```bash
odibi run config.yaml --dry-run         # Test without writing
odibi run config.yaml --log-level DEBUG # Verbose logging
odibi story last                        # View last execution report
odibi doctor                            # Check environment health
```

## CRITICAL: Transformer Function Signature

```python
# CORRECT signature - params is a Pydantic model
def my_transformer(context: EngineContext, params: MyParams) -> EngineContext:
    sql = f"SELECT *, {params.column} as new_col FROM df"
    return context.sql(sql)

# WRONG - do NOT use this pattern
def my_transformer(context, current, column, value):  # WRONG!
```

## CRITICAL: Custom Transformer Pattern

```python
from pydantic import BaseModel, Field
from odibi.context import EngineContext
from odibi.registry import FunctionRegistry

class MyParams(BaseModel):
    column: str = Field(..., description="Column name")
    value: str = Field(default="x", description="Default value")

def my_transformer(context: EngineContext, params: MyParams) -> EngineContext:
    sql = f"SELECT *, NULLIF({params.column}, '{params.value}') AS result FROM df"
    return context.sql(sql)

# Register it
FunctionRegistry.register(my_transformer, "my_transformer", MyParams)
```

## CRITICAL: Running Pipelines

```python
# CORRECT
from odibi.pipeline import PipelineManager
pm = PipelineManager.from_yaml("config.yaml")
pm.run("pipeline_name")

# WRONG
pm = PipelineManager("config.yaml")  # WRONG!
```

## CRITICAL: YAML Transform Syntax

```yaml
# CORRECT - use steps with function and params
transform:
  steps:
    - function: my_transformer
      params:
        column: name
        value: "N/A"

# WRONG - this syntax does NOT work
transform:
  - my_transformer:
      column: name  # WRONG!
```

## CRITICAL: YAML Input/Output Syntax

```yaml
nodes:
  - name: my_node
    inputs:                          # NOT 'source:'
      input_1:
        connection: my_connection
        path: data/input.csv
        format: csv                  # REQUIRED: csv, parquet, json, delta
    outputs:                         # NOT 'sink:'
      output_1:
        connection: my_connection
        path: data/output.parquet
        format: parquet              # REQUIRED
```

## CRITICAL: YAML Project Structure

```yaml
# Every pipeline YAML needs these top-level keys:
project: my_project          # Required
connections: {}              # Required
story:                       # Required
  connection: conn_name
  path: stories
system:                      # Required
  connection: conn_name
  path: _system
pipelines: []                # Required - list of pipeline configs
```
