# Odibi MCP Server

A Model Context Protocol (MCP) interface for AI assistants to interact with the Odibi data engineering framework.

## Overview

The MCP Server provides AI assistants with:
- **Smart Discovery** - Auto-profile data sources and generate pipeline YAML
- Pipeline metadata and run stories
- Data catalogs and statistics
- Schema information and lineage
- Knowledge base access (transformers, patterns, docs)

## Key Principles

1. **READ-ONLY**: MCP never mutates data, triggers runs, or alters state
2. **SINGLE-PROJECT**: Each request operates within one project context
3. **TYPED RESPONSES**: All responses use Pydantic models
4. **AI-FRIENDLY**: Responses include `next_step` and `ready_for` for tool chaining

## Tools Available (~34 tools)

### Smart Discovery Tools (`odibi_mcp/tools/smart.py`) ⭐ NEW
The "Lazy Bronze" workflow for auto-generating pipelines:

- `map_environment` - **STEP 1**: Scout a connection (files/tables, patterns, formats)
- `profile_source` - **STEP 2**: Self-correcting profiler (encoding, delimiter, schema)
- `profile_folder` - Batch profile all files in a folder
- `generate_bronze_node` - **STEP 3**: Generate Odibi YAML from profile
- `test_node` - **STEP 4**: Validate generated YAML, get fix instructions

**Workflow:** `map_environment` → `profile_source` → `generate_bronze_node` → `test_node`

### Knowledge Tools
- `bootstrap_context` - Auto-gather full project context (call FIRST)
- `get_deep_context` - Get ODIBI_DEEP_CONTEXT.md (2200+ lines)
- `list_transformers` - List all 52+ transformers
- `list_patterns` - List all 6 DWH patterns
- `list_connections` - List all connection types
- `explain` - Get detailed docs for any feature
- `get_transformer_signature` - Exact signature for custom transformers
- `get_yaml_structure` - Exact YAML structure for configs
- `query_codebase` - Semantic search over odibi code

### YAML Builder Tools (`odibi_mcp/tools/yaml_builder.py`)
- `generate_sql_pipeline` - Generate pipeline YAML for SQL tables
- `validate_odibi_config` - Validate YAML config
- `generate_project_yaml` - Generate project.yaml

### Discovery Tools (`odibi_mcp/tools/discovery.py`)
- `describe_table` - Get SQL table schema (lightweight)
- `list_sheets` - List Excel sheet names
- `list_schemas` - List SQL database schemas
- `compare_schemas` - Compare two data sources

### Story Tools (`odibi_mcp/tools/story.py`)
- `story_read` - Read pipeline run metadata
- `story_diff` - Compare two pipeline runs
- `node_describe` - Get detailed node information

### Sample Tools (`odibi_mcp/tools/sample.py`)
- `node_sample` - Get sample output data
- `node_sample_in` - Get sample input data
- `node_failed_rows` - Get validation failures

### Catalog Tools (`odibi_mcp/tools/catalog.py`)
- `node_stats` - Node-level statistics
- `pipeline_stats` - Pipeline-level statistics
- `failure_summary` - Failure aggregations
- `schema_history` - Schema change history

### Lineage Tools (`odibi_mcp/tools/lineage.py`)
- `lineage_upstream` - Trace data sources
- `lineage_downstream` - Trace data consumers
- `lineage_graph` - Full lineage graph

### Schema Tools (`odibi_mcp/tools/schema.py`)
- `output_schema` - Get output schema
- `list_outputs` - List pipeline outputs

## Running the Server

```bash
python -m odibi_mcp.server
```

Or with environment config:
```bash
ODIBI_CONFIG=path/to/project.yaml python -m odibi_mcp.server
```

## Configuration

See `mcp_config.example.yaml` for configuration options.
