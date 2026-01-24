# Odibi MCP Server Guide

The **odibi-knowledge** MCP (Model Context Protocol) server exposes 45 tools for AI assistants to interact with your odibi pipelines, understand the framework, and help build data pipelines.

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
      ODIBI_CONFIG: C:/Users/yourname/project/odibi.yaml  # Your project config
```

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

## Available Tools (45 Total)

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

### Schema Tools (2)

| Tool | Description | Example |
|------|-------------|---------|
| `list_outputs` | List pipeline outputs | `{"pipeline": "bronze"}` |
| `output_schema` | Get output column schema | `{"pipeline": "bronze", "output_name": "my_node"}` |

### Discovery Tools (8)

| Tool | Description | Example |
|------|-------------|---------|
| `list_files` | List files in a connection | `{"connection": "my_conn", "path": "data/", "pattern": "*.csv"}` |
| `preview_source` | Preview source data (supports sheet param for Excel) | `{"connection": "my_conn", "path": "data.csv", "max_rows": 10}` |
| `infer_schema` | Infer schema from file | `{"connection": "my_conn", "path": "data.csv"}` |
| `list_tables` | List SQL tables | `{"connection": "my_sql", "schema": "dbo"}` |
| `describe_table` | Describe SQL table | `{"connection": "my_sql", "table": "my_table"}` |
| `list_sheets` | List sheet names in Excel file | `{"connection": "my_conn", "path": "data.xlsx"}` |
| `discover_database` | **Crawl SQL database** - get all tables with schemas and samples in one call | `{"connection": "my_sql", "schema": "dbo"}` |
| `discover_storage` | **Crawl file storage** - get all files with schemas and samples in one call | `{"connection": "my_adls", "path": "raw/"}` |

---

## Common Workflows

### 1. Explore Available Data

```
Use odibi MCP list_files with connection="my_adls", path="raw/", pattern="*.csv"
Use odibi MCP preview_source with connection="my_adls", path="raw/customers.csv", max_rows=5
Use odibi MCP infer_schema with connection="my_adls", path="raw/customers.csv"
```

### 2. Build a New Pipeline

```
Use odibi MCP get_yaml_structure
Use odibi MCP suggest_pattern with use_case="build customer dimension with history"
Use odibi MCP get_example with pattern_name="dimension"
Use odibi MCP list_transformers
```

### 3. Debug a Failed Run

```
Use odibi MCP story_read with pipeline="bronze"
Use odibi MCP node_describe with pipeline="bronze", node="failed_node"
Use odibi MCP node_sample_in with pipeline="bronze", node="failed_node"
Use odibi MCP node_failed_rows with pipeline="bronze", node="failed_node"
Use odibi MCP diagnose_error with error_message="<the error message>"
```

### 4. Understand the Framework

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
| `list_files` | ✅ | Browse any connection |
| `list_tables` | ✅ | List SQL database tables |
| `describe_table` | ✅ | Get column info from SQL |
| `preview_source` | ✅ | Sample data from files/tables |
| `infer_schema` | ✅ | Auto-detect types |
| `list_sheets` | ✅ | Excel sheet names |
| `story_read` | ❌ | Needs full project.yaml |
| `node_sample` | ❌ | Needs full project.yaml |
| `lineage_*` | ❌ | Needs full project.yaml |

### Example Workflow

1. **Create exploration.yaml** with your connections
2. **Set environment variable**: `ODIBI_CONFIG=./exploration.yaml`
3. **Ask AI to explore**:
   ```
   Use odibi MCP list_tables with connection="my_sql", schema="dbo"
   Use odibi MCP preview_source with connection="my_sql", source="dbo.customers", limit=10
   Use odibi MCP describe_table with connection="my_sql", table="dbo.orders"
   ```

When ready to build pipelines, graduate to a full `project.yaml` with pipelines, story, and system sections.

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
