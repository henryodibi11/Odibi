# Odibi MCP Server Guide

The **odibi-knowledge** MCP (Model Context Protocol) server exposes 15 knowledge tools for AI assistants to interact with your odibi pipelines, understand the framework, and help build data pipelines. Execution is done via shell commands.

## Smart Discovery Tools

These tools help you explore data sources and understand what exists:

| Tool | Description |
|------|-------------|
| `map_environment` | Scout a connection (storage or SQL) to understand what exists. Returns folder structure, file patterns, table counts, and recommendations. |
| `profile_source` | Self-correcting profiler that figures out how to read a file or table. Detects encoding, delimiter, skip rows, schema. Iterates until data looks right. |
| `profile_folder` | Batch profiler - profile all files in a folder, group by options. |

> **Note:** For generating bronze layer YAML, write YAML manually following [docs/reference/yaml_schema.md](../reference/yaml_schema.md).

### Inline Connection Specs

Discovery tools now accept **either** a connection name OR an inline connection spec. This allows exploration without a config file:

```python
# Named connection (from exploration.yaml)
map_environment("wwi")
profile_source("wwi", "Sales.Orders")

# Inline connection spec (no config needed)
map_environment({
    "type": "azure_sql",
    "server": "myserver.database.windows.net",
    "database": "MyDB",
    "driver": "ODBC Driver 17 for SQL Server",
    "username": "${SQL_USER}",
    "password": "${SQL_PASSWORD}"
})
```

**Security:** Secrets must use `${ENV_VAR}` syntax. Plaintext passwords are rejected.

### Discovery Workflow

```
1. map_environment(connection)          → What's here?
2. profile_source(connection, path)     → How do I read it?
3. Write YAML manually                  → Follow docs/reference/yaml_schema.md
4. validate_yaml(yaml_content)          → Verify syntax
5. Run pipeline via shell               → python -m odibi run X.yaml
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

## Available Tools (15 Total)

### 🚀 Start Here: Bootstrap Tool

| Tool | Description | Example |
|------|-------------|---------|
| `bootstrap_context` | **CALL FIRST** - Auto-gather full project context (connections, pipelines, outputs, patterns, YAML rules) | `{}` |

### Discovery Tools (3)

| Tool | Description | Example |
|------|-------------|---------|
| `map_environment` | **Scout connection** - understand what exists, detect patterns. Call FIRST | `{"connection": "my_adls", "path": "raw/"}` |
| `profile_source` | **Self-correcting profiler** - figures out encoding, delimiter, skip rows | `{"connection": "my_adls", "path": "raw/data.csv"}` |
| `profile_folder` | **Batch profiler** - profile all files in a folder, group by options | `{"connection": "my_adls", "folder_path": "raw/", "pattern": "*.csv"}` |

### Knowledge Tools (5)

| Tool | Description | Example |
|------|-------------|---------|
| `list_transformers` | List all 52+ transformers | `{}` |
| `list_patterns` | List all 6 DWH patterns | `{}` |
| `list_connections` | List connection types | `{}` |
| `explain` | Get detailed docs for any feature | `{"name": "scd2"}` |
| `get_validation_rules` | Available validation rules | `{}` |

> **Note:** For YAML structure reference, see [docs/reference/yaml_schema.md](../reference/yaml_schema.md). For codebase questions, use grep on the `docs/` folder.

### Validation & Debug Tools (2)

| Tool | Description | Example |
|------|-------------|---------|
| `validate_yaml` | Validate pipeline YAML | `{"yaml_content": "..."}` |
| `diagnose_error` | Get fix suggestions for errors | `{"error_message": "KeyError: 'col'"}` |

### Story/Run Tools (1)

| Tool | Description | Example |
|------|-------------|---------|
| `story_read` | Get pipeline run status | `{"pipeline": "bronze"}` |

### Sample Data Tools (2)

| Tool | Description | Example |
|------|-------------|---------|
| `node_sample` | Get node output sample | `{"pipeline": "bronze", "node": "my_node", "max_rows": 10}` |
| `node_failed_rows` | Get validation failures | `{"pipeline": "bronze", "node": "my_node"}` |

### Lineage Tools (1)

| Tool | Description | Example |
|------|-------------|---------|
| `lineage_graph` | Full pipeline lineage graph | `{"pipeline": "bronze", "include_external": true}` |

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

### 1. Explore Available Data

```
Use odibi MCP map_environment with connection="raw_adls", path="/"
→ "Found 12 CSVs in Reliability_Report/, 3 parquet files in Daily/"

Use odibi MCP profile_source with connection="raw_adls", path="Reliability_Report/IP24.csv"
→ {encoding: "windows-1252", delimiter: "\t", skipRows: 5, schema: [...], confidence: 0.94}
```

### 2. Build a New Pipeline

```
Use odibi MCP list_patterns
→ Review available patterns (Dimension, Fact, SCD2, Merge, Aggregation, Date Dimension)

Use odibi MCP explain with name="scd2"
→ Get detailed documentation on the pattern

Use odibi MCP list_transformers
→ Find transformers you need

# Write YAML manually following docs/reference/yaml_schema.md
# Then validate:
Use odibi MCP validate_yaml with yaml_content="..."
```

### 3. Debug a Failed Run

```
Use odibi MCP story_read with pipeline="bronze"
→ See which node failed

Use odibi MCP node_sample with pipeline="bronze", node="failed_node"
→ Check output data

Use odibi MCP node_failed_rows with pipeline="bronze", node="failed_node"
→ See validation failures

Use odibi MCP diagnose_error with error_message="<the error message>"
→ Get fix suggestions
```

### 4. Understand the Framework

```
Use odibi MCP explain with name="scd2"
→ Detailed docs for any feature

Use odibi MCP list_transformers
→ See all available transformers

Use odibi MCP get_validation_rules
→ See available validation rules

# For deeper questions, use grep on docs/ folder
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
| `story_read` | ✅* | Auto-discovers from ODIBI_PROJECTS_DIR |
| `node_sample` | ✅* | Auto-discovers from ODIBI_PROJECTS_DIR |
| `lineage_graph` | ✅* | Auto-discovers from ODIBI_PROJECTS_DIR |

*These tools auto-discover projects from `ODIBI_PROJECTS_DIR` by pipeline name.

> **Note:** For generating bronze layer YAML, write YAML manually following [docs/reference/yaml_schema.md](../reference/yaml_schema.md).

### Example Workflow

1. **Create exploration.yaml** with your connections
2. **Set environment variables**:
   - `ODIBI_CONFIG=./exploration.yaml`
   - `ODIBI_PROJECTS_DIR=./projects`
3. **Ask AI to explore**:
   ```
   Use odibi MCP map_environment with connection="my_sql"
   Use odibi MCP profile_source with connection="my_sql", path="dbo.customers"
   # Write YAML manually following docs/reference/yaml_schema.md
   Use odibi MCP validate_yaml with yaml_content="..."
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

---

## Troubleshooting

### "No project context" errors

Make sure `ODIBI_CONFIG` points to a valid odibi.yaml file.

### Empty results from story/sample tools

Ensure pipelines have been run at least once and story files exist.

### YAML validation fails

Check your YAML against [docs/reference/yaml_schema.md](../reference/yaml_schema.md) for the correct structure.

---

## See Also

- [ODIBI_DEEP_CONTEXT.md](../ODIBI_DEEP_CONTEXT.md) - Full framework documentation
- [YAML Reference](../reference/yaml_schema.md) - Complete YAML schema
- [Patterns Guide](../patterns/) - DWH pattern documentation
