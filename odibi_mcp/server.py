from __future__ import annotations

"""MCP Server for Odibi Knowledge.

Exposes odibi knowledge through the Model Context Protocol (MCP).
"""

from odibi_mcp.audit.logger import AuditLogger
from odibi_mcp.audit.entry import AuditEntry
from datetime import datetime
import json
import logging
import os
import sys
from pathlib import Path

# Ensure odibi is importable
ODIBI_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ODIBI_ROOT))
sys.path.insert(0, str(ODIBI_ROOT / "_archive"))

# Load .env file if present (checks cwd, then ODIBI_CONFIG dir, then odibi root)
_dotenv_loaded = False
_dotenv_path = None
try:
    from dotenv import load_dotenv

    # Try multiple locations for .env
    env_locations = [
        Path.cwd() / ".env",
        Path(os.environ.get("ODIBI_CONFIG", "")).parent / ".env"
        if os.environ.get("ODIBI_CONFIG")
        else None,
        ODIBI_ROOT / ".env",
    ]
    for env_path in env_locations:
        if env_path and env_path.exists():
            load_dotenv(env_path, override=True)
            _dotenv_loaded = True
            _dotenv_path = str(env_path)
            break
except ImportError:
    pass  # dotenv not installed, rely on system env vars

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import (
    TextContent,
    Tool,
)

from .knowledge import get_knowledge

# Import facade tools
from odibi_mcp.tools.story import story_read, story_diff, node_describe
from odibi_mcp.tools.sample import node_sample, node_sample_in, node_failed_rows
from odibi_mcp.tools.catalog import (
    node_stats,
    pipeline_stats,
    failure_summary,
    schema_history,
)
from odibi_mcp.tools.lineage import lineage_upstream, lineage_downstream, lineage_graph
from odibi_mcp.tools.schema import output_schema, list_outputs, compare_schemas
from odibi_mcp.tools.discovery import (
    list_files,
    list_tables,
    infer_schema,
    describe_table,
    preview_source,
    list_sheets,
    discover_database,
    list_schemas,
    discover_storage,
)
from odibi_mcp.tools.yaml_builder import (
    generate_sql_pipeline,
    validate_odibi_config,
    generate_project_yaml,
)
from dataclasses import asdict, is_dataclass
from pydantic import BaseModel


def to_json_serializable(obj):
    """Convert dataclasses and Pydantic models to JSON-serializable dicts."""
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, BaseModel):
        return obj.model_dump(mode="json")
    if is_dataclass(obj) and not isinstance(obj, type):
        return {k: to_json_serializable(v) for k, v in asdict(obj).items()}
    if isinstance(obj, dict):
        return {k: to_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_json_serializable(v) for v in obj]
    if hasattr(obj, "__dict__"):
        return {k: to_json_serializable(v) for k, v in obj.__dict__.items()}
    return str(obj)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
audit_logger = AuditLogger(logger)
# Create MCP server
server = Server("odibi-knowledge")


@server.list_tools()
async def list_tools() -> list[Tool]:
    """List available tools."""
    return [
        Tool(
            name="debug_env",
            description="Debug environment setup - shows if .env was loaded, which env vars are set (without values), and connection status.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        Tool(
            name="list_transformers",
            description="List all 52+ odibi transformers with their parameters. Use when you need to know what transformers are available.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        Tool(
            name="list_patterns",
            description="List all 6 odibi DWH patterns (dimension, fact, scd2, merge, aggregation, date_dimension). Use when you need pattern info.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        Tool(
            name="list_connections",
            description="List all odibi connection types (local, azure_adls, sql_server, delta, etc.).",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        Tool(
            name="explain",
            description="Get detailed documentation for a specific transformer, pattern, or connection by name.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Name of the transformer, pattern, or connection to explain",
                    },
                },
                "required": ["name"],
            },
        ),
        Tool(
            name="get_transformer_signature",
            description="Get the EXACT function signature pattern for creating custom odibi transformers. ALWAYS use this before writing transformer code.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        Tool(
            name="get_yaml_structure",
            description="Get the EXACT YAML structure for odibi pipeline configs. ALWAYS use this before writing YAML configs.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        Tool(
            name="query_codebase",
            description="Semantic search over the odibi codebase. Use for open-ended questions like 'how does X work' or 'where is Y implemented'.",
            inputSchema={
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "Natural language question about the odibi codebase",
                    },
                    "k": {
                        "type": "integer",
                        "description": "Number of results to return (default: 8)",
                        "default": 8,
                    },
                },
                "required": ["question"],
            },
        ),
        Tool(
            name="reindex",
            description="Reindex the odibi codebase for semantic search. Use --force to rebuild from scratch.",
            inputSchema={
                "type": "object",
                "properties": {
                    "force": {
                        "type": "boolean",
                        "description": "Delete existing index and rebuild from scratch",
                        "default": False,
                    },
                },
                "required": [],
            },
        ),
        Tool(
            name="get_index_stats",
            description="Get statistics about the current odibi codebase index.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        Tool(
            name="get_deep_context",
            description="Get ODIBI_DEEP_CONTEXT.md - the comprehensive 2200+ line framework documentation. Use this FIRST when you need to understand odibi deeply.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        Tool(
            name="bootstrap_context",
            description="AUTO-GATHER full project context. Call this FIRST when starting work on an odibi project. Returns: project config, connections, pipelines with outputs, transformer count, patterns, critical YAML rules, and suggested next steps. No parameters needed - reads from ODIBI_CONFIG.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        Tool(
            name="get_doc",
            description="Get a specific documentation file by path (e.g., 'docs/patterns/scd2.md', 'docs/guides/best_practices.md').",
            inputSchema={
                "type": "object",
                "properties": {
                    "doc_path": {
                        "type": "string",
                        "description": "Relative path to the doc file, e.g., 'docs/patterns/scd2.md' or just 'scd2.md'",
                    },
                },
                "required": ["doc_path"],
            },
        ),
        Tool(
            name="list_docs",
            description="List available documentation files. Categories: patterns, tutorials, guides, features, reference, examples, context.",
            inputSchema={
                "type": "object",
                "properties": {
                    "category": {
                        "type": "string",
                        "description": "Optional category filter (patterns, tutorials, guides, features, reference, examples, context)",
                    },
                },
                "required": [],
            },
        ),
        Tool(
            name="search_docs",
            description="Search all documentation files for a keyword or phrase.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search term to find in docs",
                    },
                },
                "required": ["query"],
            },
        ),
        Tool(
            name="generate_transformer",
            description="Generate complete Python code for a custom transformer. Use when creating new transformers.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Transformer function name (snake_case)",
                    },
                    "params": {
                        "type": "array",
                        "description": "List of parameters: [{name, type, description, required}]",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "type": {"type": "string", "default": "str"},
                                "description": {"type": "string"},
                                "required": {"type": "boolean", "default": True},
                            },
                        },
                    },
                    "description": {
                        "type": "string",
                        "description": "What the transformer does",
                    },
                },
                "required": ["name", "params"],
            },
        ),
        Tool(
            name="generate_pipeline_yaml",
            description="Generate a complete pipeline YAML config. Use when creating new pipelines.",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_name": {"type": "string", "description": "Project name"},
                    "input_path": {
                        "type": "string",
                        "description": "Path to input file",
                    },
                    "input_format": {
                        "type": "string",
                        "description": "Format: csv, parquet, json, delta",
                    },
                    "output_path": {
                        "type": "string",
                        "description": "Path to output file",
                    },
                    "output_format": {
                        "type": "string",
                        "description": "Format: csv, parquet, json, delta",
                    },
                    "transforms": {
                        "type": "array",
                        "description": "Optional list of transforms: [{function, params}]",
                        "items": {
                            "type": "object",
                            "properties": {
                                "function": {"type": "string"},
                                "params": {"type": "object"},
                            },
                        },
                    },
                },
                "required": [
                    "project_name",
                    "input_path",
                    "input_format",
                    "output_path",
                    "output_format",
                ],
            },
        ),
        Tool(
            name="validate_yaml",
            description="Validate odibi pipeline YAML and return errors. ALWAYS use before saving YAML configs.",
            inputSchema={
                "type": "object",
                "properties": {
                    "yaml_content": {
                        "type": "string",
                        "description": "The YAML content to validate",
                    },
                },
                "required": ["yaml_content"],
            },
        ),
        Tool(
            name="diagnose_error",
            description="Diagnose an odibi error message and suggest fixes. Use when debugging failures.",
            inputSchema={
                "type": "object",
                "properties": {
                    "error_message": {
                        "type": "string",
                        "description": "The error message from odibi",
                    },
                },
                "required": ["error_message"],
            },
        ),
        Tool(
            name="get_example",
            description="Get a working example for a pattern or transformer. Use to understand usage.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pattern_name": {
                        "type": "string",
                        "description": "Name of pattern (scd2, merge, dimension) or transformer (add_column, filter)",
                    },
                },
                "required": ["pattern_name"],
            },
        ),
        Tool(
            name="suggest_pattern",
            description="Recommend the best odibi pattern for a use case. Use BEFORE choosing a pattern.",
            inputSchema={
                "type": "object",
                "properties": {
                    "use_case": {
                        "type": "string",
                        "description": "Description of what you want to accomplish (e.g., 'track customer changes over time')",
                    },
                },
                "required": ["use_case"],
            },
        ),
        Tool(
            name="get_engine_differences",
            description="Get critical differences between Spark, Pandas, and Polars engines. Use when writing cross-engine SQL.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        Tool(
            name="get_validation_rules",
            description="Get all odibi validation rule types with examples. Use when adding data quality checks.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        # ============ MCP FACADE TOOLS ============
        # Story tools
        Tool(
            name="story_read",
            description="Read pipeline execution story (status, duration, errors). Use to check run results.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline": {"type": "string", "description": "Pipeline name"},
                    "run_id": {
                        "type": "string",
                        "description": "Optional run ID (defaults to latest)",
                    },
                },
                "required": ["pipeline"],
            },
        ),
        Tool(
            name="story_diff",
            description="Compare two pipeline runs to see what changed.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline": {"type": "string", "description": "Pipeline name"},
                    "run_a": {"type": "string", "description": "First run ID"},
                    "run_b": {"type": "string", "description": "Second run ID"},
                },
                "required": ["pipeline", "run_a", "run_b"],
            },
        ),
        Tool(
            name="node_describe",
            description="Get node config, inputs, outputs, transform steps.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline": {"type": "string", "description": "Pipeline name"},
                    "node": {"type": "string", "description": "Node name"},
                },
                "required": ["pipeline", "node"],
            },
        ),
        # Sample tools
        Tool(
            name="node_sample",
            description="Get sample output data from a node.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline": {"type": "string", "description": "Pipeline name"},
                    "node": {"type": "string", "description": "Node name"},
                    "max_rows": {
                        "type": "integer",
                        "description": "Max rows to return",
                        "default": 100,
                    },
                },
                "required": ["pipeline", "node"],
            },
        ),
        Tool(
            name="node_sample_in",
            description="Get sample input data to a node.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline": {"type": "string", "description": "Pipeline name"},
                    "node": {"type": "string", "description": "Node name"},
                    "input_name": {
                        "type": "string",
                        "description": "Input name",
                        "default": "default",
                    },
                    "max_rows": {
                        "type": "integer",
                        "description": "Max rows to return",
                        "default": 100,
                    },
                },
                "required": ["pipeline", "node"],
            },
        ),
        Tool(
            name="node_failed_rows",
            description="Get rows that failed validation for a node.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline": {"type": "string", "description": "Pipeline name"},
                    "node": {"type": "string", "description": "Node name"},
                    "max_rows": {
                        "type": "integer",
                        "description": "Max rows to return",
                        "default": 50,
                    },
                },
                "required": ["pipeline", "node"],
            },
        ),
        # Catalog tools
        Tool(
            name="node_stats",
            description="Get node statistics (row counts, duration, success rate).",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline": {"type": "string", "description": "Pipeline name"},
                    "node": {"type": "string", "description": "Node name"},
                },
                "required": ["pipeline", "node"],
            },
        ),
        Tool(
            name="pipeline_stats",
            description="Get pipeline-level statistics.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline": {"type": "string", "description": "Pipeline name"},
                },
                "required": ["pipeline"],
            },
        ),
        Tool(
            name="failure_summary",
            description="Summarize failures across runs.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline": {
                        "type": "string",
                        "description": "Optional pipeline filter",
                    },
                    "max_failures": {
                        "type": "integer",
                        "description": "Max failures to return",
                        "default": 100,
                    },
                },
                "required": [],
            },
        ),
        Tool(
            name="schema_history",
            description="View schema changes over time for a node.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline": {"type": "string", "description": "Pipeline name"},
                    "node": {"type": "string", "description": "Node name"},
                },
                "required": ["pipeline", "node"],
            },
        ),
        # Lineage tools
        Tool(
            name="lineage_upstream",
            description="Find what feeds into a node (data sources).",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline": {"type": "string", "description": "Pipeline name"},
                    "node": {"type": "string", "description": "Node name"},
                    "depth": {
                        "type": "integer",
                        "description": "Max depth",
                        "default": 3,
                    },
                },
                "required": ["pipeline", "node"],
            },
        ),
        Tool(
            name="lineage_downstream",
            description="Find what a node feeds into (consumers).",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline": {"type": "string", "description": "Pipeline name"},
                    "node": {"type": "string", "description": "Node name"},
                    "depth": {
                        "type": "integer",
                        "description": "Max depth",
                        "default": 3,
                    },
                },
                "required": ["pipeline", "node"],
            },
        ),
        Tool(
            name="lineage_graph",
            description="Get full lineage DAG for a pipeline.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline": {"type": "string", "description": "Pipeline name"},
                    "include_external": {
                        "type": "boolean",
                        "description": "Include external sources",
                        "default": False,
                    },
                },
                "required": ["pipeline"],
            },
        ),
        # Schema tools
        Tool(
            name="output_schema",
            description="Get schema of a pipeline output.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline": {"type": "string", "description": "Pipeline name"},
                    "output_name": {"type": "string", "description": "Output name"},
                },
                "required": ["pipeline", "output_name"],
            },
        ),
        Tool(
            name="list_outputs",
            description="List all outputs for a pipeline.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline": {"type": "string", "description": "Pipeline name"},
                },
                "required": ["pipeline"],
            },
        ),
        # Discovery tools
        Tool(
            name="list_files",
            description="List files in a connection path.",
            inputSchema={
                "type": "object",
                "properties": {
                    "connection": {"type": "string", "description": "Connection name"},
                    "path": {"type": "string", "description": "Path to list"},
                    "pattern": {
                        "type": "string",
                        "description": "Glob pattern",
                        "default": "*",
                    },
                },
                "required": ["connection", "path"],
            },
        ),
        Tool(
            name="list_tables",
            description="List tables in a SQL connection.",
            inputSchema={
                "type": "object",
                "properties": {
                    "connection": {"type": "string", "description": "Connection name"},
                    "schema": {
                        "type": "string",
                        "description": "Schema name",
                        "default": "dbo",
                    },
                    "pattern": {
                        "type": "string",
                        "description": "Table pattern",
                        "default": "*",
                    },
                },
                "required": ["connection"],
            },
        ),
        Tool(
            name="infer_schema",
            description="Infer schema from a file.",
            inputSchema={
                "type": "object",
                "properties": {
                    "connection": {"type": "string", "description": "Connection name"},
                    "path": {"type": "string", "description": "Path to file"},
                },
                "required": ["connection", "path"],
            },
        ),
        Tool(
            name="describe_table",
            description="Describe a SQL table.",
            inputSchema={
                "type": "object",
                "properties": {
                    "connection": {"type": "string", "description": "Connection name"},
                    "table": {"type": "string", "description": "Table name"},
                    "schema": {
                        "type": "string",
                        "description": "Schema name",
                        "default": "dbo",
                    },
                },
                "required": ["connection", "table"],
            },
        ),
        Tool(
            name="preview_source",
            description="Preview source data (first N rows). For Excel files, use 'sheet' parameter to specify which sheet to read. Use list_sheets first to discover available sheets.",
            inputSchema={
                "type": "object",
                "properties": {
                    "connection": {"type": "string", "description": "Connection name"},
                    "path": {"type": "string", "description": "Path to source"},
                    "max_rows": {
                        "type": "integer",
                        "description": "Max rows",
                        "default": 100,
                    },
                    "sheet": {
                        "type": "string",
                        "description": "For Excel files: sheet name to read (default: first sheet)",
                    },
                },
                "required": ["connection", "path"],
            },
        ),
        Tool(
            name="list_sheets",
            description="List all sheet names in an Excel file (.xlsx, .xls). Use this to discover sheets before calling preview_source with sheet parameter.",
            inputSchema={
                "type": "object",
                "properties": {
                    "connection": {"type": "string", "description": "Connection name"},
                    "path": {"type": "string", "description": "Path to Excel file"},
                },
                "required": ["connection", "path"],
            },
        ),
        Tool(
            name="list_schemas",
            description="List all schemas in a SQL database with table counts. Call FIRST before discover_database to see what schemas exist.",
            inputSchema={
                "type": "object",
                "properties": {
                    "connection": {"type": "string", "description": "SQL connection name"},
                },
                "required": ["connection"],
            },
        ),
        Tool(
            name="discover_database",
            description="Discover tables in a SQL database (structure-first, shallow by default). Returns table names, columns, row counts. Samples OFF by default - use preview_source for samples.",
            inputSchema={
                "type": "object",
                "properties": {
                    "connection": {"type": "string", "description": "SQL connection name"},
                    "schema": {
                        "type": "string",
                        "default": "dbo",
                        "description": "Database schema to scan",
                    },
                    "max_tables": {
                        "type": "integer",
                        "default": 20,
                        "description": "Maximum tables to discover (default: 20, shallow)",
                    },
                    "sample_rows": {
                        "type": "integer",
                        "default": 0,
                        "description": "Sample rows per table (default: 0 = none, use preview_source instead)",
                    },
                },
                "required": ["connection"],
            },
        ),
        Tool(
            name="discover_storage",
            description="Discover files in storage (structure-first, shallow by default). Returns file names, formats, schemas. Samples and recursion OFF by default - use preview_source for samples.",
            inputSchema={
                "type": "object",
                "properties": {
                    "connection": {"type": "string", "description": "Storage connection name"},
                    "path": {
                        "type": "string",
                        "default": "",
                        "description": "Base path to scan",
                    },
                    "pattern": {
                        "type": "string",
                        "default": "*",
                        "description": "File pattern (e.g., '*.csv', '*.xlsx')",
                    },
                    "max_files": {
                        "type": "integer",
                        "default": 10,
                        "description": "Maximum files to discover (default: 10, shallow)",
                    },
                    "sample_rows": {
                        "type": "integer",
                        "default": 0,
                        "description": "Sample rows per file (default: 0 = none, use preview_source instead)",
                    },
                    "recursive": {
                        "type": "boolean",
                        "default": False,
                        "description": "Scan subdirectories (default: false = shallow)",
                    },
                },
                "required": ["connection"],
            },
        ),
        Tool(
            name="compare_schemas",
            description="Compare schemas between two data sources. Returns differences in columns, types, and nullability. Use to validate source-to-target compatibility before building pipelines.",
            inputSchema={
                "type": "object",
                "properties": {
                    "source_connection": {
                        "type": "string",
                        "description": "Connection name for source",
                    },
                    "source_path": {"type": "string", "description": "Path to source file/table"},
                    "target_connection": {
                        "type": "string",
                        "description": "Connection name for target",
                    },
                    "target_path": {"type": "string", "description": "Path to target file/table"},
                    "source_sheet": {
                        "type": "string",
                        "description": "Optional sheet name for Excel source",
                    },
                    "target_sheet": {
                        "type": "string",
                        "description": "Optional sheet name for Excel target",
                    },
                },
                "required": [
                    "source_connection",
                    "source_path",
                    "target_connection",
                    "target_path",
                ],
            },
        ),
        # ============ YAML BUILDER TOOLS ============
        Tool(
            name="generate_sql_pipeline",
            description="""Generate CORRECT Odibi pipeline YAML for SQL database ingestion.

ALWAYS use this instead of manually writing YAML for SQL sources!

This tool:
1. Uses the CORRECT schema (read: with format: sql and query:)
2. Generates valid node names (sanitized to alphanumeric + underscore)
3. Creates proper top-level 'pipelines:' key for imported files
4. Validates before outputting

Input discovered tables from discover_database or list_tables.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "pipeline_name": {
                        "type": "string",
                        "description": "Name for the pipeline (e.g., 'bronze_customers')",
                    },
                    "source_connection": {
                        "type": "string",
                        "description": "SQL database connection name",
                    },
                    "target_connection": {
                        "type": "string",
                        "description": "Target storage connection name",
                    },
                    "tables": {
                        "type": "array",
                        "description": "Tables to ingest",
                        "items": {
                            "type": "object",
                            "properties": {
                                "schema": {"type": "string", "default": "dbo"},
                                "table": {"type": "string", "description": "Table name"},
                                "where": {"type": "string", "description": "Optional WHERE clause"},
                                "columns": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "Columns to select (default: all)",
                                },
                                "primary_key": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "Primary key columns",
                                },
                                "incremental_column": {
                                    "type": "string",
                                    "description": "Column for incremental loading",
                                },
                            },
                            "required": ["table"],
                        },
                    },
                    "target_format": {
                        "type": "string",
                        "default": "delta",
                        "description": "Output format (delta, parquet, csv)",
                    },
                    "target_schema": {
                        "type": "string",
                        "description": "Schema prefix for target tables",
                    },
                    "layer": {
                        "type": "string",
                        "default": "bronze",
                        "description": "Pipeline layer (bronze, silver, gold)",
                    },
                    "node_prefix": {
                        "type": "string",
                        "default": "",
                        "description": "Prefix for node names",
                    },
                },
                "required": ["pipeline_name", "source_connection", "target_connection", "tables"],
            },
        ),
        Tool(
            name="validate_odibi_config",
            description="""Validate Odibi YAML config using actual Pydantic models.

BETTER than validate_yaml! This catches:
- Wrong keys (inputs: instead of read:, sql: instead of query:)
- Missing required fields (format:, connection:)
- Invalid node names
- Missing top-level 'pipelines:' key in imported files

Use BEFORE saving any Odibi YAML config.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "yaml_content": {
                        "type": "string",
                        "description": "The YAML content to validate",
                    },
                    "check_imports": {
                        "type": "boolean",
                        "default": False,
                        "description": "Also validate imported pipeline files",
                    },
                },
                "required": ["yaml_content"],
            },
        ),
        Tool(
            name="generate_project_yaml",
            description="""Generate a complete project.yaml file with connections, story, system config.

Use to create a new Odibi project with correct structure.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_name": {
                        "type": "string",
                        "description": "Project name",
                    },
                    "connections": {
                        "type": "array",
                        "description": "Connection definitions",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string", "description": "Connection name"},
                                "type": {
                                    "type": "string",
                                    "description": "Connection type (local, azure_sql, adls, etc.)",
                                },
                            },
                            "required": ["name", "type"],
                        },
                    },
                    "imports": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Pipeline files to import (e.g., ['pipelines/bronze/orders.yaml'])",
                    },
                    "story_connection": {
                        "type": "string",
                        "description": "Connection for story output (defaults to first connection)",
                    },
                    "system_connection": {
                        "type": "string",
                        "description": "Connection for system data (defaults to first connection)",
                    },
                },
                "required": ["project_name", "connections"],
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Handle tool calls."""
    knowledge = get_knowledge()

    # Tools that should include CRITICAL_CONTEXT in their response
    # These are the tools most likely to be called before generating YAML/code
    CONTEXT_INJECTION_TOOLS = {
        "list_transformers",
        "list_patterns",
        "list_connections",
        "explain",
        "get_example",
        "suggest_pattern",
        "validate_yaml",
        "validate_odibi_config",
        "generate_sql_pipeline",
        "diagnose_error",
        "query_codebase",
        "search_docs",
        "get_engine_differences",
        "get_validation_rules",
    }

    start_time = datetime.utcnow()
    request_id = str(arguments.get("request_id") or f"{name}-{int(start_time.timestamp())}")
    project = "unknown"  # Could extract this from result in future
    redacted_args = AuditLogger.redact_args(arguments)

    try:
        if name == "debug_env":
            # Debug tool to check environment setup
            from odibi_mcp.context import get_project_context

            ctx = get_project_context()
            debug_info = {
                "dotenv_loaded": _dotenv_loaded,
                "dotenv_path": _dotenv_path,
                "odibi_config": os.environ.get("ODIBI_CONFIG", "NOT SET"),
                "sql_user_set": "SQL_USER" in os.environ
                and os.environ["SQL_USER"] != "${SQL_USER}",
                "sql_password_set": "SQL_PASSWORD" in os.environ
                and os.environ["SQL_PASSWORD"] != "${SQL_PASSWORD}",
                "cwd": str(Path.cwd()),
                "connections_initialized": list(ctx.connections.keys()) if ctx else [],
                "connections_defined": list(ctx.config.get("connections", {}).keys())
                if ctx
                else [],
            }
            return [TextContent(type="text", text=json.dumps(debug_info, indent=2))]
        elif name == "list_transformers":
            result = knowledge.list_transformers()
        elif name == "list_patterns":
            result = knowledge.list_patterns()
        elif name == "list_connections":
            result = knowledge.list_connections()
        elif name == "explain":
            result = knowledge.explain(arguments["name"])
        elif name == "get_transformer_signature":
            return [TextContent(type="text", text=knowledge.get_transformer_signature())]
        elif name == "get_yaml_structure":
            return [TextContent(type="text", text=knowledge.get_yaml_structure())]
        elif name == "query_codebase":
            result = knowledge.query_codebase(
                arguments["question"],
                k=arguments.get("k", 8),
            )
        elif name == "reindex":
            result = knowledge.reindex(force=arguments.get("force", False))
        elif name == "get_index_stats":
            result = knowledge.get_index_stats()
        elif name == "get_deep_context":
            return [TextContent(type="text", text=knowledge.get_deep_context())]
        elif name == "bootstrap_context":
            result = knowledge.bootstrap_context()
        elif name == "get_doc":
            result = knowledge.get_doc(arguments["doc_path"])
        elif name == "list_docs":
            result = knowledge.list_docs(category=arguments.get("category"))
        elif name == "search_docs":
            result = knowledge.search_docs(arguments["query"])
        elif name == "generate_transformer":
            code = knowledge.generate_transformer(
                name=arguments["name"],
                params=arguments.get("params", []),
                description=arguments.get("description", ""),
            )
            return [TextContent(type="text", text=code)]
        elif name == "generate_pipeline_yaml":
            yaml_content = knowledge.generate_pipeline_yaml(
                project_name=arguments["project_name"],
                input_path=arguments["input_path"],
                input_format=arguments["input_format"],
                output_path=arguments["output_path"],
                output_format=arguments["output_format"],
                transforms=arguments.get("transforms"),
            )
            return [TextContent(type="text", text=yaml_content)]
        elif name == "validate_yaml":
            result = knowledge.validate_yaml(arguments["yaml_content"])
        elif name == "diagnose_error":
            result = knowledge.diagnose_error(arguments["error_message"])
        elif name == "get_example":
            result = knowledge.get_example(arguments["pattern_name"])
        elif name == "suggest_pattern":
            result = knowledge.suggest_pattern(arguments["use_case"])
        elif name == "get_engine_differences":
            result = knowledge.get_engine_differences()
        elif name == "get_validation_rules":
            result = knowledge.get_validation_rules()
        # ============ MCP FACADE TOOL HANDLERS ============
        # Story tools
        elif name == "story_read":
            res = story_read(
                pipeline=arguments["pipeline"],
                run_selector=arguments.get("run_id"),
            )
            result = to_json_serializable(res)
        elif name == "story_diff":
            res = story_diff(
                pipeline=arguments["pipeline"],
                run_a=arguments["run_a"],
                run_b=arguments["run_b"],
            )
            result = to_json_serializable(res)
        elif name == "node_describe":
            res = node_describe(
                pipeline=arguments["pipeline"],
                node=arguments["node"],
            )
            result = to_json_serializable(res)
        # Sample tools
        elif name == "node_sample":
            res = node_sample(
                pipeline=arguments["pipeline"],
                node=arguments["node"],
                max_rows=arguments.get("max_rows", 100),
            )
            result = to_json_serializable(res)
        elif name == "node_sample_in":
            res = node_sample_in(
                pipeline=arguments["pipeline"],
                node=arguments["node"],
                input_name=arguments.get("input_name", "default"),
                max_rows=arguments.get("max_rows", 100),
            )
            result = to_json_serializable(res)
        elif name == "node_failed_rows":
            res = node_failed_rows(
                pipeline=arguments["pipeline"],
                node=arguments["node"],
                max_rows=arguments.get("max_rows", 50),
            )
            result = to_json_serializable(res)
        # Catalog tools
        elif name == "node_stats":
            res = node_stats(
                pipeline=arguments["pipeline"],
                node=arguments["node"],
            )
            result = to_json_serializable(res)
        elif name == "pipeline_stats":
            res = pipeline_stats(
                pipeline=arguments["pipeline"],
            )
            result = to_json_serializable(res)
        elif name == "failure_summary":
            res = failure_summary(
                pipeline=arguments.get("pipeline"),
                max_failures=arguments.get("max_failures", 100),
            )
            result = to_json_serializable(res)
        elif name == "schema_history":
            res = schema_history(
                pipeline=arguments["pipeline"],
                node=arguments["node"],
            )
            result = to_json_serializable(res)
        # Lineage tools
        elif name == "lineage_upstream":
            res = lineage_upstream(
                pipeline=arguments["pipeline"],
                node=arguments["node"],
                depth=arguments.get("depth", 3),
            )
            result = to_json_serializable(res)
        elif name == "lineage_downstream":
            res = lineage_downstream(
                pipeline=arguments["pipeline"],
                node=arguments["node"],
                depth=arguments.get("depth", 3),
            )
            result = to_json_serializable(res)
        elif name == "lineage_graph":
            res = lineage_graph(
                pipeline=arguments["pipeline"],
                include_external=arguments.get("include_external", False),
            )
            result = to_json_serializable(res)
        # Schema tools
        elif name == "output_schema":
            res = output_schema(
                pipeline=arguments["pipeline"],
                output_name=arguments["output_name"],
            )
            result = to_json_serializable(res)
        elif name == "list_outputs":
            res = list_outputs(
                pipeline=arguments["pipeline"],
            )
            result = to_json_serializable(res)
        elif name == "compare_schemas":
            res = compare_schemas(
                source_connection=arguments["source_connection"],
                source_path=arguments["source_path"],
                target_connection=arguments["target_connection"],
                target_path=arguments["target_path"],
                source_sheet=arguments.get("source_sheet"),
                target_sheet=arguments.get("target_sheet"),
            )
            result = to_json_serializable(res)
        # Discovery tools
        elif name == "list_files":
            res = list_files(
                connection=arguments["connection"],
                path=arguments["path"],
                pattern=arguments.get("pattern", "*"),
            )
            result = to_json_serializable(res)
        elif name == "list_tables":
            res = list_tables(
                connection=arguments["connection"],
                schema=arguments.get("schema", "dbo"),
                pattern=arguments.get("pattern", "*"),
            )
            result = to_json_serializable(res)
        elif name == "infer_schema":
            res = infer_schema(
                connection=arguments["connection"],
                path=arguments["path"],
            )
            result = to_json_serializable(res)
        elif name == "describe_table":
            res = describe_table(
                connection=arguments["connection"],
                table=arguments["table"],
                schema=arguments.get("schema", "dbo"),
            )
            result = to_json_serializable(res)
        elif name == "preview_source":
            res = preview_source(
                connection=arguments["connection"],
                path=arguments["path"],
                max_rows=arguments.get("max_rows", 100),
                sheet=arguments.get("sheet"),
            )
            result = to_json_serializable(res)
        elif name == "list_sheets":
            res = list_sheets(
                connection=arguments["connection"],
                path=arguments["path"],
            )
            result = to_json_serializable(res)
        elif name == "list_schemas":
            res = list_schemas(connection=arguments["connection"])
            result = to_json_serializable(res)
        elif name == "discover_database":
            res = discover_database(
                connection=arguments["connection"],
                schema=arguments.get("schema", "dbo"),
                max_tables=arguments.get("max_tables", 50),
                sample_rows=arguments.get("sample_rows", 5),
            )
            result = to_json_serializable(res)
        elif name == "discover_storage":
            res = discover_storage(
                connection=arguments["connection"],
                path=arguments.get("path", ""),
                pattern=arguments.get("pattern", "*"),
                max_files=arguments.get("max_files", 20),
                sample_rows=arguments.get("sample_rows", 5),
                recursive=arguments.get("recursive", True),
            )
            result = to_json_serializable(res)
        # ============ YAML BUILDER TOOL HANDLERS ============
        elif name == "generate_sql_pipeline":
            res = generate_sql_pipeline(
                pipeline_name=arguments["pipeline_name"],
                source_connection=arguments["source_connection"],
                target_connection=arguments["target_connection"],
                tables=arguments["tables"],
                target_format=arguments.get("target_format", "delta"),
                target_schema=arguments.get("target_schema"),
                layer=arguments.get("layer", "bronze"),
                node_prefix=arguments.get("node_prefix", ""),
            )
            result = to_json_serializable(res)
        elif name == "validate_odibi_config":
            res = validate_odibi_config(
                yaml_content=arguments["yaml_content"],
                check_imports=arguments.get("check_imports", False),
            )
            result = to_json_serializable(res)
        elif name == "generate_project_yaml":
            yaml_content = generate_project_yaml(
                project_name=arguments["project_name"],
                connections=arguments["connections"],
                imports=arguments.get("imports"),
                story_connection=arguments.get("story_connection"),
                system_connection=arguments.get("system_connection"),
            )
            return [TextContent(type="text", text=yaml_content)]
        else:
            result = {"error": f"Unknown tool: {name}"}

        # Auto-inject CRITICAL_CONTEXT for key tools
        # This ensures AI always sees correct YAML syntax, no matter which tool it calls
        if name in CONTEXT_INJECTION_TOOLS:
            result = knowledge._with_context(result)

        duration_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
        audit_logger.log(
            AuditEntry(
                timestamp=start_time,
                request_id=request_id,
                tool_name=name,
                project=project,
                environment="production",
                connection=None,
                resource_logical=None,
                args_summary=redacted_args,
                duration_ms=duration_ms,
                success=True,
                error_type=None,
                bytes_read_estimate=None,
                policy_applied={},
            )
        )
        return [TextContent(type="text", text=json.dumps(result, indent=2))]

    except Exception as e:
        from odibi_mcp.utils.errors import wrap_exception

        duration_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
        mcp_error = wrap_exception(e, tool_name=name, request_id=request_id)

        audit_logger.log(
            AuditEntry(
                timestamp=start_time,
                request_id=request_id,
                tool_name=name,
                project=project,
                environment="production",
                connection=None,
                resource_logical=None,
                args_summary=redacted_args,
                duration_ms=duration_ms,
                success=False,
                error_type=mcp_error.code.value,
                bytes_read_estimate=None,
                policy_applied={},
            )
        )
        logger.exception(f"Error in tool {name}")
        error_response = {
            "error": mcp_error.message,
            "code": mcp_error.code.value,
            "tool": mcp_error.tool_name,
            "request_id": mcp_error.request_id,
        }
        if mcp_error.details:
            error_response["details"] = mcp_error.details
        return [TextContent(type="text", text=json.dumps(error_response))]


async def main():
    """Run the MCP server."""
    logger.info("Starting odibi-knowledge MCP server...")

    # Initialize project context from environment
    from odibi_mcp.context import initialize_from_env

    ctx = initialize_from_env()
    if ctx:
        logger.info(f"Loaded project: {ctx.project_name} from {ctx.config_path}")
    else:
        logger.warning("No project config found - facade tools will return empty results")

    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


def run():
    """Entry point for the server."""
    import asyncio

    asyncio.run(main())


if __name__ == "__main__":
    run()
