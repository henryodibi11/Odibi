# Odibi Project Rules for Continue

## About Odibi
Odibi is a declarative data pipeline framework with 52+ transformers, 6 DWH patterns, and 41 MCP tools for AI-assisted development.

## MCP Documentation
- **Tool Reference**: `docs/guides/mcp_guide.md` - All 41 tools with examples
- **AI Recipes**: `docs/guides/mcp_recipes.md` - 10 workflow recipes for common tasks
- **System Prompt**: `docs/guides/mcp_system_prompt.md` - Prompts and snippets

## MCP Tool Selection Guide

**Choose the right tool automatically based on task:**

| Task Type | MCP to Use |
|-----------|------------|
| Odibi transformers, patterns, YAML | `odibi-knowledge` tools |
| Query pipeline runs, debug failures | `odibi-knowledge` MCP Facade tools |
| Complex planning, tradeoff analysis | `sequential-thinking` |
| Read/write/search files | `filesystem` |
| Web documentation, API docs | `fetch` |
| Remember user preferences/context | `memory` |
| Git history, commits, branches | `git` |

**Auto-trigger rules:**
- "explain", "list", "generate YAML", "transformer", "pattern" → use `odibi-knowledge`
- "why did pipeline fail", "show sample", "lineage", "schema" → use MCP Facade tools
- "plan", "analyze tradeoffs", "step by step" → use `sequential-thinking`
- "remember", "recall", "my preferences" → use `memory`
- "fetch docs", "get documentation for" → use `fetch`
- "create file", "write to", "save as" → use `filesystem`

## ⚠️ FIRST THING TO DO (MANDATORY)

**At the START of every conversation, call `bootstrap_context` to auto-gather project context:**

```
bootstrap_context()  # Returns: project, connections, pipelines, outputs, patterns, YAML rules
```

This gives you everything needed to understand the project in one call.

---

## ⚠️ CONTEXT-FIRST RULE (MANDATORY)

**Before suggesting ANY solution, the AI MUST gather full context using these workflows.**

### Workflow A: Full Source Understanding (BEFORE Building Anything)
```
list_files(connection, path, "*")     # 1. What files exist?
preview_source(connection, path, 20)   # 2. See ACTUAL data values
infer_schema(connection, path)         # 3. Get exact column names & types
```
**ONLY AFTER seeing real data** may the AI suggest patterns or generate YAML.

### Workflow B: Pipeline Deep Dive (BEFORE Modifying Anything)
```
list_outputs(pipeline)                 # 1. What outputs exist?
output_schema(pipeline, output)        # 2. What's the schema?
lineage_graph(pipeline, true)          # 3. How do nodes connect?
node_describe(pipeline, node)          # 4. What does this node do?
node_sample_in(pipeline, node, 10)     # 5. What data flows in?
story_read(pipeline)                   # 6. Recent run history?
```

### Workflow C: Framework Mastery Check (BEFORE Suggesting Solutions)
```
explain(name)                          # Verify transformer/pattern understanding
get_example(pattern_name)              # Get working example
get_yaml_structure()                   # Verify YAML structure
```
**NEVER guess parameters** — always use `explain` first.

### Workflow D: Complete Debug Investigation (BEFORE Suggesting Fixes)
```
story_read(pipeline)                   # 1. Which node failed?
node_describe(pipeline, failed_node)   # 2. What was it supposed to do?
node_sample_in(pipeline, failed_node)  # 3. What data actually arrived?
node_failed_rows(pipeline, failed_node)# 4. What rows failed validation?
diagnose_error(error_message)          # 5. Get AI diagnosis
```

### Workflow F: New Project Onboarding (Complete Context)
```
get_deep_context()                     # Full framework docs
list_patterns()                        # Available patterns
list_connections()                     # Available connections
list_transformers()                    # Available transformers
list_outputs(pipeline)                 # For each pipeline in scope
lineage_graph(pipeline)                # Understand data flow
```

## Anti-Patterns (NEVER DO)

| ❌ Don't | ✅ Do Instead |
|----------|--------------|
| Guess column names | Use `preview_source` or `infer_schema` |
| Assume transformer params | Use `explain` to verify |
| Generate YAML without validation | Run `validate_yaml` first |
| Suggest fixes without evidence | Use `node_sample_in` to see data |
| Skip lineage understanding | Use `lineage_graph` before changes |

## Context Checklist (Verify Before Acting)

- [ ] Seen actual source data (`preview_source`)
- [ ] Know exact column names/types (`infer_schema`)
- [ ] Understand the pattern (`explain`)
- [ ] Validated YAML (`validate_yaml`)
- [ ] Checked lineage impact (`lineage_graph`)

---

## AI Workflow Recipes (Quick Reference)

### Recipe: Build New Pipeline
1. `list_files` / `preview_source` → understand source data
2. `suggest_pattern` → recommend right pattern  
3. `get_example` → get working YAML template
4. `list_transformers` / `explain` → find transformers
5. `validate_yaml` → validate before presenting

### Recipe: Debug Pipeline Failure
1. `story_read` → check run status
2. `node_describe` → get failed node details
3. `node_sample_in` → see what data node received
4. `node_failed_rows` → see validation failures
5. `diagnose_error` → get fix suggestions

### Recipe: Explore Available Data
1. `list_files(connection, path, pattern)` → list files
2. `preview_source(connection, path, max_rows)` → preview data
3. `infer_schema(connection, path)` → get column types

### Recipe: Learn About Odibi Feature
1. `explain(name)` → get specific feature docs
2. `search_docs(query)` → search documentation
3. `query_codebase(question)` → search source code

## CRITICAL: Safe Editing Practices

**NEVER replace entire files.** Make surgical, targeted edits:
- Edit ONE function/docstring at a time
- Show the specific change, not the whole file
- Use diff-style edits when possible
- After each edit, verify the file still has all its code

**Before editing large files:**
1. Count functions/classes in the file
2. After edit, verify the same count exists
3. If code was accidentally removed, STOP and restore via `git checkout -- <file>`

**Preferred workflow for docstring improvements:**
1. List each function needing changes
2. Edit each docstring ONE AT A TIME
3. Run `ruff check <file>` after each batch

## CRITICAL: Verification After Changes

**After EVERY code change:**
1. Run `ruff check <file> --fix` to fix lint issues
2. Run `ruff format <file>` to format
3. Run relevant tests: `pytest tests/unit/test_<module>.py -v`
4. If tests fail, fix before moving on

**Before committing or saying "done":**
- Verify the file is syntactically valid: `python -m py_compile <file>`
- Check for import errors: `python -c "import odibi.<module>"`

## CRITICAL: What NOT To Do

**NEVER:**
- Delete code unless explicitly asked
- Modify multiple unrelated files in one edit
- Skip running tests after changes
- Assume imports exist — check first
- Add dependencies without asking
- Change function signatures without updating all callers
- Use `# type: ignore` or `# noqa` to hide errors

**ALWAYS ask before:**
- Deleting any function, class, or file
- Refactoring across multiple files
- Adding new dependencies to pyproject.toml
- Changing public API signatures

## CRITICAL: Handling Errors

**If you encounter an error:**
1. Show the full error message
2. Explain what went wrong
3. Propose a fix but WAIT for approval before applying
4. If you broke something, restore with `git checkout -- <file>`

**If tests fail after your change:**
1. Do NOT proceed to other tasks
2. Fix the failing test first
3. If you can't fix it, revert your change

## IMPORTANT: Use MCP Tools First!

Before writing ANY odibi code, call these odibi-knowledge MCP tools:

### MCP Facade Tools (Query Pipelines & Debug)

Use these to query running pipelines, view data, and debug failures:

**Discovery Tools** - Explore data sources:
| Tool | Parameters | Example |
|------|------------|---------|
| `list_files` | `connection`, `path`, `pattern` | `list_files("my_storage", "raw_data", "*.json")` |
| `preview_source` | `connection`, `path`, `max_rows`, `sheet` | `preview_source("my_storage", "raw_data/file.json", 10)` |
| `infer_schema` | `connection`, `path` | `infer_schema("my_storage", "raw_data/file.csv")` |
| `list_tables` | `connection`, `schema`, `pattern` | `list_tables("my_database", "dbo", "*")` |
| `describe_table` | `connection`, `table`, `schema` | `describe_table("my_database", "dim_date", "dbo")` |
| `list_sheets` | `connection`, `path` | `list_sheets("my_storage", "data.xlsx")` |

**Story Tools** - Inspect pipeline runs:
| Tool | Parameters | Example |
|------|------------|---------|
| `story_read` | `pipeline` | `story_read("bronze")` |
| `story_diff` | `pipeline`, `run_a`, `run_b` | `story_diff("bronze", "latest", "previous")` |
| `node_describe` | `pipeline`, `node` | `node_describe("bronze", "customers_raw")` |

**Sample Tools** - View data:
| Tool | Parameters | Example |
|------|------------|---------|
| `node_sample` | `pipeline`, `node`, `max_rows` | `node_sample("bronze", "customers_raw", 10)` |
| `node_sample_in` | `pipeline`, `node`, `input_name`, `max_rows` | `node_sample_in("bronze", "customers_raw", "default", 10)` |
| `node_failed_rows` | `pipeline`, `node`, `max_rows` | `node_failed_rows("bronze", "customers_raw", 50)` |

**Lineage Tools**:
| Tool | Parameters | Example |
|------|------------|---------|
| `lineage_upstream` | `pipeline`, `node`, `depth` | `lineage_upstream("silver", "fact_orders", 3)` |
| `lineage_downstream` | `pipeline`, `node`, `depth` | `lineage_downstream("bronze", "customers_raw", 3)` |
| `lineage_graph` | `pipeline`, `include_external` | `lineage_graph("bronze", false)` |

**Schema Tools**:
| Tool | Parameters | Example |
|------|------------|---------|
| `list_outputs` | `pipeline` | `list_outputs("bronze")` |
| `output_schema` | `pipeline`, `output_name` | `output_schema("bronze", "customers_raw")` |

**Catalog Tools**:
| Tool | Parameters | Example |
|------|------------|---------|
| `node_stats` | `pipeline`, `node` | `node_stats("bronze", "customers_raw")` |
| `pipeline_stats` | `pipeline` | `pipeline_stats("bronze")` |
| `failure_summary` | `pipeline`, `max_failures` | `failure_summary("bronze", 10)` |
| `schema_history` | `pipeline`, `node` | `schema_history("bronze", "customers_raw")` |

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

**Fallback order if MCP times out:**
1. Try CLI commands: `python -m odibi list transformers --format json`
2. Read `docs/ODIBI_DEEP_CONTEXT.md` for comprehensive reference

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
1. Check existing: `python -m odibi list transformers`
2. Add to `odibi/transformers/` following existing patterns
3. Register in `odibi/transformers/__init__.py`
4. Add tests in `tests/unit/transformers/`
5. Must work on Pandas AND Spark (engine parity)

### New Pattern
1. Check existing: `python -m odibi list patterns`
2. Add to `odibi/patterns/` extending `Pattern` base class
3. Register in `odibi/patterns/__init__.py`
4. Add tests

### New Connection
1. Check existing: `python -m odibi list connections`
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
python -m odibi run config.yaml --dry-run         # Test without writing
python -m odibi run config.yaml --log-level DEBUG # Verbose logging
python -m odibi story last                        # View last execution report
python -m odibi doctor                            # Check environment health
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
