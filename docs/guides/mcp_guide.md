# Odibi MCP Server Guide

The **odibi-knowledge** MCP (Model Context Protocol) server exposes 53 knowledge tools for AI assistants to interact with your odibi pipelines, understand the framework, and help build data pipelines. Execution is done via shell commands.

## Smart Discovery Tools (New!)

These tools enable the "Lazy Bronze" workflow - point at a data source and get a working bronze layer:

| Tool | Description |
|------|-------------|
| `map_environment` | Scout a connection (storage or SQL) to understand what exists. Returns folder structure, file patterns, table counts, and recommendations. |
| `profile_source` | Self-correcting profiler that figures out how to read a file or table. Detects encoding, delimiter, skip rows, schema. Iterates until data looks right. |
| `generate_bronze_node` | Generate Odibi YAML for a bronze layer node from a profile result. |
| `test_node` | Test a node definition in-memory without persisting. Validates output and suggests fixes. |

### Lazy Bronze Workflow

```
1. map_environment(connection)          → What's here?
2. profile_source(connection, path)     → How do I read it?
3. generate_bronze_node(profile)        → Give me the YAML
4. test_node(yaml)                      → Verify it works
5. [iterate if needed]                  → Fix issues
6. Save to project                      → Done!
```

## Quick Start

### 1. Install Dependencies

```bash
pip install odibi mcp chromadb python-dotenv scikit-learn
```

### 2. Configure Your IDE

#### Continue (VS Code)

Add to `~/.continue/config.yaml`:

```yaml
mcpServers:
  - name: odibi-knowledge
    command: python
    args:
      - D:/odibi/run_mcp.py  # Path to odibi
    env:
      ODIBI_CONFIG: C:/Users/yourname/project/exploration.yaml  # Connections for discovery
      ODIBI_PROJECTS_DIR: C:/Users/yourname/project/projects    # Auto-discover generated projects
```

**Environment Variables:**
- `ODIBI_CONFIG` - Path to exploration.yaml (connections for data discovery)
- `ODIBI_PROJECTS_DIR` - Path to projects folder (for auto-discovery of generated projects)

#### Other MCP Clients

The server runs via stdio:
```bash
python run_mcp.py
```

### 3. Test the Connection

Ask your AI assistant:
```
Use odibi MCP list_transformers
```

---

## Available Tools (53 Total)

### 🚀 Start Here: Bootstrap Tool

| Tool | Description | Example |
|------|-------------|---------|
| `bootstrap_context` | **CALL FIRST** - Auto-gather full project context (connections, pipelines, outputs, patterns, YAML rules) | `{}` |

### Knowledge Tools (21)

| Tool | Description | Example |
|------|-------------|---------|
| `list_transformers` | List all 52+ transformers | `{}` |
| `list_patterns` | List all 6 DWH patterns | `{}` |
| `list_connections` | List connection types | `{}` |
| `explain` | Get detailed docs for any feature | `{"name": "scd2"}` |
| `get_transformer_signature` | Correct transformer signature | `{}` |
| `get_yaml_structure` | Correct YAML structure | `{}` |
| `get_deep_context` | Full 69K char framework docs | `{}` |
| `get_index_stats` | Codebase index statistics | `{}` |
| `list_docs` | List documentation files | `{"category": "patterns"}` |
| `search_docs` | Search documentation | `{"query": "SCD2"}` |
| `get_doc` | Get specific doc file | `{"doc_path": "docs/patterns/scd2.md"}` |
| `validate_yaml` | Validate pipeline YAML | `{"yaml_content": "..."}` |
| `diagnose_error` | Get fix suggestions for errors | `{"error_message": "KeyError: 'col'"}` |
| `get_example` | Get pattern example YAML | `{"pattern_name": "dimension"}` |
| `suggest_pattern` | Suggest pattern for use case | `{"use_case": "track changes"}` |
| `get_engine_differences` | Spark/Pandas/Polars differences | `{}` |
| `get_validation_rules` | Available validation rules | `{}` |
| `generate_transformer` | Generate transformer code | `{"name": "my_func", ...}` |
| `generate_pipeline_yaml` | Generate pipeline YAML | `{"project_name": "test", ...}` |
| `query_codebase` | Search odibi source code | `{"question": "how does SCD2 work?"}` |
| `reindex` | Rebuild codebase index | `{"force": true}` |

### Story/Run Tools (4)

| Tool | Description | Example |
|------|-------------|---------|
| `story_read` | Get pipeline run status | `{"pipeline": "bronze"}` |
| `story_diff` | Compare two runs | `{"pipeline": "bronze", "run_a": "...", "run_b": "..."}` |
| `node_describe` | Get node execution details | `{"pipeline": "bronze", "node": "my_node"}` |

### Sample Data Tools (3)

| Tool | Description | Example |
|------|-------------|---------|
| `node_sample` | Get node output sample | `{"pipeline": "bronze", "node": "my_node", "max_rows": 10}` |
| `node_sample_in` | Get node input sample | `{"pipeline": "bronze", "node": "my_node"}` |
| `node_failed_rows` | Get validation failures | `{"pipeline": "bronze", "node": "my_node"}` |

### Catalog/Stats Tools (4)

| Tool | Description | Example |
|------|-------------|---------|
| `node_stats` | Node execution statistics | `{"pipeline": "bronze", "node": "my_node"}` |
| `pipeline_stats` | Pipeline-level statistics | `{"pipeline": "bronze"}` |
| `failure_summary` | Recent failures across pipelines | `{"max_failures": 100}` |
| `schema_history` | Schema changes over time | `{"pipeline": "bronze", "node": "my_node"}` |

> **Note:** Catalog tools require `skip_catalog_writes: false` in your config. They return empty results when disabled.

### Lineage Tools (3)

| Tool | Description | Example |
|------|-------------|---------|
| `lineage_upstream` | Find upstream dependencies | `{"pipeline": "bronze", "node": "my_node", "depth": 3}` |
| `lineage_downstream` | Find downstream dependents | `{"pipeline": "bronze", "node": "my_node", "depth": 3}` |
| `lineage_graph` | Full pipeline lineage graph | `{"pipeline": "bronze", "include_external": true}` |

### Schema Tools (3)

| Tool | Description | Example |
|------|-------------|---------|
| `list_outputs` | List pipeline outputs | `{"pipeline": "bronze"}` |
| `output_schema` | Get output column schema | `{"pipeline": "bronze", "output_name": "my_node"}` |
| `compare_schemas` | Compare schemas between two sources | `{"source_connection": "raw", "source_path": "data.csv", "target_connection": "bronze", "target_path": "output.parquet"}` |

### Discovery Tools (12)

| Tool | Description | Example |
|------|-------------|---------|
| `map_environment` | **Scout connection** - understand what exists, detect patterns. Call FIRST | `{"connection": "my_adls", "path": "raw/"}` |
| `profile_source` | **Self-correcting profiler** - figures out encoding, delimiter, skip rows | `{"connection": "my_adls", "path": "raw/data.csv"}` |
| `profile_folder` | **Batch profiler** - profile all files in a folder, group by options | `{"connection": "my_adls", "folder_path": "raw/", "pattern": "*.csv"}` |
| `generate_bronze_node` | **Generate bronze YAML** from profile result | `{"profile": {...}, "output_connection": "bronze"}` |
| `test_node` | **Test node in-memory** - validate before saving | `{"node_yaml": "...", "max_rows": 100}` |
| `list_schemas` | List schemas in SQL database with table counts | `{"connection": "my_sql"}` |
| `describe_table` | Describe SQL table (quick metadata) | `{"connection": "my_sql", "table": "my_table"}` |
| `list_sheets` | List sheet names in Excel file | `{"connection": "my_conn", "path": "data.xlsx"}` |
| `debug_env` | Debug environment setup - shows .env loading, env vars, and connection status | `{}` |
| `diagnose` | Diagnose MCP environment - check paths, env vars, connections | `{}` |
| `diagnose_path` | Check if a specific path exists and list contents | `{"path": "projects/data"}` |

### Execution - Use Shell!

Execution tools have been removed. **Use shell commands instead:**

```powershell
python -m odibi run X.yaml          # Run pipeline
python -m odibi run X.yaml --dry-run # Validate
python -m odibi doctor              # Check environment
python -m odibi story last          # View last run
Get-ChildItem -Recurse -Filter "*.yaml"  # Find files
```

> **Agent Behavior:** Execute shell commands directly. Don't show commands and wait - run them yourself.

---

## Common Workflows

### 1. Lazy Bronze (Recommended!)

Point at a data source and get a working bronze layer:

```
Use odibi MCP map_environment with connection="raw_adls", path="/"
→ "Found 12 CSVs in Reliability_Report/, 3 parquet files in Daily/"

Use odibi MCP profile_source with connection="raw_adls", path="Reliability_Report/IP24.csv"
→ {encoding: "windows-1252", delimiter: "\t", skipRows: 5, schema: [...], confidence: 0.94}

Use odibi MCP generate_bronze_node with profile={...from above...}
→ YAML node definition ready to use

Use odibi MCP test_node with node_yaml="..."
→ {status: "success", rows_read: 1234, ready_to_save: true}
```

### 2. Explore Available Data

```
Use odibi MCP map_environment with connection="my_adls", path="raw/"
→ Shows all files, tables, patterns detected

Use odibi MCP profile_source with connection="my_adls", path="raw/customers.csv"
→ Returns full schema, sample data, encoding, delimiter, AI suggestions
```

### 3. Build a New Pipeline

```
Use odibi MCP get_yaml_structure
Use odibi MCP suggest_pattern with use_case="build customer dimension with history"
Use odibi MCP get_example with pattern_name="dimension"
Use odibi MCP list_transformers
```

### 4. Debug a Failed Run

```
Use odibi MCP story_read with pipeline="bronze"
Use odibi MCP node_describe with pipeline="bronze", node="failed_node"
Use odibi MCP node_sample_in with pipeline="bronze", node="failed_node"
Use odibi MCP node_failed_rows with pipeline="bronze", node="failed_node"
Use odibi MCP diagnose_error with error_message="<the error message>"
```

### 5. Understand the Framework

```
Use odibi MCP get_deep_context
Use odibi MCP explain with name="scd2"
Use odibi MCP search_docs with query="validation"
Use odibi MCP query_codebase with question="how does merge pattern work?"
```

---

## Exploration Mode (New!)

**Exploration mode** lets AI explore your data sources without a full pipeline configuration. Perfect for:
- Initial data discovery and profiling
- Working with an analyst to understand source data
- Quick data exploration before building pipelines

### Exploration Config

Create a minimal `exploration.yaml` (or `mcp_config.yaml`):

```yaml
# exploration.yaml - just connections, no pipelines needed
project: my_exploration  # optional

connections:
  my_sql:
    type: azure_sql
    connection_string: ${SQL_CONN}
  my_adls:
    type: azure_adls
    account_name: mystorageaccount
    container: raw
  local:
    type: local
    path: ./data/samples
```

### What Works in Exploration Mode

| Tool | Works | Description |
|------|-------|-------------|
| `map_environment` | ✅ | Scout any connection (files, tables, patterns) |
| `profile_source` | ✅ | Profile any file/table with full schema |
| `profile_folder` | ✅ | Batch profile all files in a folder |
| `describe_table` | ✅ | Get column info from SQL |
| `list_schemas` | ✅ | List SQL schemas with table counts |
| `list_sheets` | ✅ | Excel sheet names |
| `generate_bronze_node` | ✅ | Generate YAML from profile |
| `test_node` | ✅ | Test generated YAML |
| `list_projects` | ✅ | List all projects in ODIBI_PROJECTS_DIR |
| `story_read` | ✅* | Auto-discovers from ODIBI_PROJECTS_DIR |
| `node_sample` | ✅* | Auto-discovers from ODIBI_PROJECTS_DIR |
| `lineage_*` | ✅* | Auto-discovers from ODIBI_PROJECTS_DIR |

*These tools auto-discover projects from `ODIBI_PROJECTS_DIR` by pipeline name.

### Example Workflow

1. **Create exploration.yaml** with your connections
2. **Set environment variables**:
   - `ODIBI_CONFIG=./exploration.yaml`
   - `ODIBI_PROJECTS_DIR=./projects`
3. **Ask AI to explore**:
   ```
   Use odibi MCP map_environment with connection="my_sql"
   Use odibi MCP profile_source with connection="my_sql", path="dbo.customers"
   Use odibi MCP generate_bronze_node → saves to projects/ folder
   ```
4. **Run pipeline**: `python -m odibi run projects/my_project.yaml`
5. **Ask AI to sample**: `Use odibi MCP node_sample with pipeline="my_pipeline", node="my_node"`
   - Automatically finds the project in ODIBI_PROJECTS_DIR

---

## Configuration

### Project Configuration

The MCP server reads your `odibi.yaml` to access:
- **Connections** - For discovery tools
- **Pipelines** - For lineage, schema, story tools
- **Story path** - For run status and samples
- **System catalog** - For statistics (if enabled)

### Environment Variables

| Variable | Description |
|----------|-------------|
| `ODIBI_CONFIG` | Path to your odibi.yaml |
| `MCP_CONFIG` | Path to MCP-specific config (optional) |
| `ODIBI_USE_TFIDF_EMBEDDINGS` | Set to `1` on Windows to use keyword search fallback |

### Windows Notes

On Windows, the `query_codebase` and `reindex` tools use a keyword-based fallback instead of semantic embeddings due to PyTorch DLL issues. This is automatic when using `run_mcp.py`.

---

## Troubleshooting

### "No project context" errors

Make sure `ODIBI_CONFIG` points to a valid odibi.yaml file.

### Empty results from catalog tools

Check your config has `skip_catalog_writes: false`. Catalog tools need the system catalog to be populated.

### "No outputs found" for list_outputs

Ensure your pipeline nodes have `write:` blocks defined with `connection`, `path`, and `format`.

### query_codebase crashes on Windows

This is handled automatically by the keyword fallback. If you still have issues, set:
```bash
set ODIBI_USE_TFIDF_EMBEDDINGS=1
```

---

## See Also

- [ODIBI_DEEP_CONTEXT.md](../ODIBI_DEEP_CONTEXT.md) - Full framework documentation
- [YAML Reference](../reference/yaml_schema.md) - Complete YAML schema
- [Patterns Guide](../patterns/) - DWH pattern documentation
