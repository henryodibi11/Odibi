# Odibi MCP Facade

A read-only Model Context Protocol (MCP) interface for AI assistants to interact with the Odibi data engineering framework.

## Overview

The MCP Facade provides AI assistants with safe, read-only access to:
- Pipeline metadata and stories
- Data catalogs and run statistics
- Schema information and lineage
- Source discovery (with access controls)

## Key Principles

1. **READ-ONLY**: MCP never mutates data, triggers runs, or alters state
2. **SINGLE-PROJECT**: Each request operates within one project context
3. **DENY-BY-DEFAULT**: Path discovery requires explicit allowlists
4. **TYPED RESPONSES**: All responses use Pydantic models
5. **PHYSICAL REFS GATED**: 3 conditions must pass to expose physical paths

## Tools Available

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

### Discovery Tools (`odibi_mcp/tools/discovery.py`)
- `list_files` - List files (access controlled)
- `list_tables` - List database tables
- `infer_schema` - Infer schema from source
- `describe_table` - Get table schema
- `preview_source` - Preview source data

## Access Control

### AccessContext
```python
from odibi_mcp.contracts.access import AccessContext, ConnectionPolicy

ctx = AccessContext(
    authorized_projects={"my_project"},
    environment="production",
    connection_policies={
        "adls_main": ConnectionPolicy(
            connection="adls_main",
            allowed_path_prefixes=["/data/bronze/"],
            allow_physical_refs=False,
        )
    },
    physical_refs_enabled=False,
)
```

### Physical Ref Gating

Physical paths are only exposed when ALL conditions pass:
1. Caller requests `include_physical=True`
2. `ConnectionPolicy.allow_physical_refs=True`
3. `AccessContext.physical_refs_enabled=True`

## Running the Server

```bash
python -m odibi_mcp.server
```

## Configuration

See `mcp_config.example.yaml` for configuration options.
