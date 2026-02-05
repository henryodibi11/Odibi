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
    describe_table,
    list_sheets,
    list_schemas,
)
from odibi_mcp.tools.smart import (
    map_environment,
    profile_source,
    profile_folder,
    generate_bronze_node,
    test_node,
    download_sql,
    download_table,
    download_file,
)
from odibi_mcp.tools.yaml_builder import (
    generate_sql_pipeline,
    validate_odibi_config,
    generate_project_yaml,
)
from odibi_mcp.tools.diagnose import diagnose, diagnose_path

# Removed execution tools - use shell instead (run_python, run_odibi, find_path, execute_pipeline)
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
            name="list_projects",
            description="List all project YAML files in the projects directory. Shows project name and pipelines defined in each.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        Tool(
            name="list_transformers",
            description="""List all 52+ odibi transformers with brief descriptions.

Returns transformer names and what they do. Use to find which transformer to use.

NEXT STEP: Call explain("<transformer_name>") for params and YAML example.""",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        Tool(
            name="list_patterns",
            description="""List all 6 odibi DWH patterns: dimension, fact, scd2, merge, aggregation, date_dimension.

Returns pattern names and descriptions. Use to choose the right pattern.

NEXT STEP: Call explain("<pattern_name>") for full params and YAML example.""",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        Tool(
            name="list_connections",
            description="""List all odibi connection types: local, azure_adls, azure_sql, delta, http, etc.

Returns connection types and required config fields.

NEXT STEP: Call explain("<connection_type>") for full config example.""",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        Tool(
            name="explain",
            description="""Get detailed docs, params, and YAML example for any odibi feature.

USE FOR: transformers, patterns, connections, validation, incremental, write modes.

EXAMPLES:
- explain("scd2") → SCD Type 2 pattern with full YAML
- explain("deduplicate") → Deduplicate transformer with params
- explain("azure_adls") → ADLS connection config
- explain("validation") → Data quality rules""",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Feature name: transformer, pattern, connection, or topic",
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
            description="""Get compact YAML structure reference for odibi pipelines.

Returns quick reference with required structure and available options.

WHEN TO DRILL DEEPER with explain():
- Need SCD2/merge/dimension pattern details → explain("scd2"), explain("merge")
- Need transformer params → explain("deduplicate"), explain("add_column")
- Need validation rules → explain("validation")
- Need incremental loading → explain("incremental")
- Need write modes/partitioning → explain("write")

For generating complete YAMLs automatically, use generate_bronze_node instead.""",
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
        # Discovery tools (kept: describe_table, list_sheets, list_schemas)
        # NOTE: list_files, list_tables, infer_schema, preview_source, discover_database, discover_storage
        # have been consolidated into smart tools: map_environment and profile_source
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
            description="List all schemas in a SQL database with table counts. Use before map_environment to see what schemas exist.",
            inputSchema={
                "type": "object",
                "properties": {
                    "connection": {"type": "string", "description": "SQL connection name"},
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
        # ============ SMART DISCOVERY TOOLS ============
        Tool(
            name="map_environment",
            description="""🔍 SCOUT a connection to discover what data exists. START HERE for any new data source.

BEHAVIOR - BE PERSISTENT:
1. Call with path="" to see root level
2. If you see "[FOLDER]" entries or "contains N subfolder(s)" → ALWAYS drill deeper by calling again with that folder path
3. Keep drilling until you find actual files (CSV, Parquet, Excel, JSON)
4. NEVER stop at the first level if you only see folders

DECISION TREE:
- See folders only? → DRILL DEEPER: map_environment(conn, "folder_name")
- See files? → PROFILE THEM: profile_source(conn, "folder/file.csv")
- See mixed? → Profile the files, drill into remaining folders
- SQL connection? → You'll get schemas/tables → profile_source(conn, "Schema.Table")

EXAMPLES:
  map_environment("raw_adls", "")           # Start at root
  map_environment("raw_adls", "raw data")   # Drill into folder
  map_environment("raw_adls", "raw data/sales")  # Drill deeper
  map_environment("wwi", "")                # SQL: shows schemas + tables

NEXT ACTIONS (pick one):
- Folder shows subfolders → map_environment(conn, "path/to/subfolder")
- Found files → profile_source(conn, "path/to/file.csv")
- Want to download → download_file(conn, "path/to/file", "./local.parquet")
- SQL table → profile_source(conn, "Schema.TableName")

⚠️ DO NOT GIVE UP if you only see folders. Keep calling until you find data files.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "connection": {
                        "type": "string",
                        "description": "Connection name from exploration.yaml",
                    },
                    "path": {
                        "type": "string",
                        "default": "",
                        "description": "Path to explore. '' for root. 'folder/subfolder' to drill deeper.",
                    },
                },
                "required": ["connection"],
            },
        ),
        Tool(
            name="profile_source",
            description="""📊 PROFILE a file or SQL table to get schema, sample data, and ready-to-use config.

WHEN TO USE:
- After map_environment finds files you want to onboard
- When you know the exact file path or table name
- Before generating pipeline YAML

WHAT YOU GET:
- schema: column names, types, nullability, patterns detected
- sample_rows: 10 rows of actual data
- file_options: encoding, delimiter, skip_rows (auto-detected for CSV)
- candidate_keys: likely primary key columns
- candidate_watermarks: likely incremental loading columns
- ready_for: pre-filled dict for generate_bronze_node()

EXAMPLES:
  profile_source("raw_adls", "raw data/sales/orders.csv")
  profile_source("raw_adls", "exports/data.parquet")
  profile_source("wwi", "Sales.Orders")      # SQL table
  profile_source("wwi_dw", "Fact.Sale")      # SQL fact table

DECISION TREE after profiling:
- confidence > 0.8 + no errors → generate_bronze_node(profile.ready_for)
- errors/warnings about encoding → profile_source again or adjust options
- want to analyze locally first → download_file() or download_sql()
- need more context → map_environment() on parent folder

NEXT ACTION: Pass ready_for dict to generate_bronze_node() to create runnable pipeline YAML.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "connection": {
                        "type": "string",
                        "description": "Connection name from exploration.yaml",
                    },
                    "path": {
                        "type": "string",
                        "description": "File path (e.g., 'folder/file.csv') or SQL table (e.g., 'Sales.Orders')",
                    },
                    "max_attempts": {
                        "type": "integer",
                        "default": 5,
                        "description": "Max profiling attempts for tricky CSV files",
                    },
                },
                "required": ["connection", "path"],
            },
        ),
        Tool(
            name="profile_folder",
            description="""📁 BATCH PROFILE many files at once. Use when a folder has multiple similar files.

WHEN TO USE (instead of profile_source):
- Folder has 5+ files you need to onboard
- Want to check if all files share same encoding/delimiter
- Need to find outlier files with different formats

WHAT YOU GET:
- options_groups: Files clustered by encoding/delimiter/skip_rows
- consistent_options: True if all files match (great for bulk ingestion)
- recommended_options: Best options based on majority
- file_profiles: Summary of each file profiled

EXAMPLES:
  profile_folder("raw_adls", "raw data/sales", "*.csv")       # All CSVs
  profile_folder("raw_adls", "exports/2024", "IP24*.csv")     # Pattern match
  profile_folder("local", "data/invoices", "*", max_files=20) # First 20 files

DECISION TREE:
- consistent_options=true → generate_bronze_node for the whole folder
- consistent_options=false → review options_groups, decide per-group
- errors on some files → check those files individually with profile_source

NEXT ACTION: Use recommended_options with generate_bronze_node() for bulk pipeline.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "connection": {
                        "type": "string",
                        "description": "Storage connection name (ADLS, S3, local)",
                    },
                    "folder_path": {
                        "type": "string",
                        "default": "",
                        "description": "Folder path to scan",
                    },
                    "pattern": {
                        "type": "string",
                        "default": "*",
                        "description": "File pattern glob (e.g., '*.csv', 'IP24*', '*.parquet')",
                    },
                    "max_files": {
                        "type": "integer",
                        "default": 50,
                        "description": "Max files to profile (increase for larger folders)",
                    },
                    "max_attempts": {
                        "type": "integer",
                        "default": 3,
                        "description": "Retry attempts per file for tricky CSVs",
                    },
                },
                "required": ["connection"],
            },
        ),
        Tool(
            name="generate_bronze_node",
            description="""⚡ GENERATE RUNNABLE YAML from profile results. The final step in data onboarding.

THIS IS THE PAYOFF - You get a complete, validated Odibi project YAML that you can:
1. Save to a .yaml file
2. Run immediately: python -m odibi run <file>.yaml

HOW TO USE:
1. Get ready_for dict from profile_source() or profile_folder()
2. Pass it here: generate_bronze_node(profile=ready_for_dict)
3. Save the yaml_content to a file
4. Run it!

EXAMPLES:
  generate_bronze_node(profile=ready_for_dict)
  generate_bronze_node(profile=ready_for_dict, node_name="sales_orders")
  generate_bronze_node(profile=ready_for_dict, local_output=False)  # Write to cloud

WHAT'S INCLUDED:
- project: auto-named from source
- connections: FULL configs pulled from exploration.yaml
- story + system: for run tracking and catalog
- pipeline: with your bronze node (source → bronze layer)

DECISION TREE:
- YAML generated successfully → Save it and run with odibi
- validation errors → Fix the profile and regenerate
- want to test first → test_node(yaml_content)

NEXT ACTION: Save yaml_content to file, then run: python -m odibi run <file>.yaml""",
            inputSchema={
                "type": "object",
                "properties": {
                    "profile": {
                        "type": "object",
                        "description": "The ready_for dict from profile_source response",
                    },
                    "node_name": {
                        "type": "string",
                        "description": "Optional node name (auto-generated if not provided)",
                    },
                    "output_connection": {
                        "type": "string",
                        "description": "Output connection name (defaults to source connection)",
                    },
                    "output_path": {
                        "type": "string",
                        "description": "Optional output path (auto-generated if not provided)",
                    },
                    "include_project": {
                        "type": "boolean",
                        "default": True,
                        "description": "If true (default), generates complete project YAML. If false, generates just pipeline.",
                    },
                    "local_output": {
                        "type": "boolean",
                        "default": True,
                        "description": "If true (default), forces output to local connection for testing. Set false to write to source connection.",
                    },
                },
                "required": ["profile"],
            },
        ),
        Tool(
            name="test_node",
            description="""🧪 TEST a node YAML before running. Validates in-memory without writing output.

WHEN TO USE:
- Before running a generated pipeline for the first time
- After manually editing YAML to check it still works
- When debugging encoding/delimiter issues

WHAT IT DOES:
1. Reads source with specified options
2. Applies any transformers
3. Validates output schema and data quality
4. Reports issues with actionable fixes

RETURNS:
- ready_to_save=true → Safe to run the pipeline
- ready_to_save=false → Check errors[] and fixes[] for what to change

DECISION TREE:
- ready_to_save=true → Run the pipeline: python -m odibi run <file>.yaml
- encoding error → Re-profile with different encoding or update YAML
- schema mismatch → Check source file hasn't changed

WORKFLOW: map_environment → profile_source → generate_bronze_node → test_node → RUN""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node_yaml": {"type": "string", "description": "Node YAML definition to test"},
                    "max_rows": {
                        "type": "integer",
                        "default": 100,
                        "description": "Max rows to read",
                    },
                },
                "required": ["node_yaml"],
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
        # ============ DOWNLOAD TOOLS - For AI Local Analysis ============
        Tool(
            name="download_sql",
            description="""⬇️ DOWNLOAD SQL query results to local file. For ad-hoc analysis without building pipelines.

WHEN TO USE:
- Quick data extraction for analysis
- Testing queries before building pipelines
- Getting samples for development
- Exporting filtered subsets

FORMAT (auto-detected from extension):
- .parquet → Fast, compressed (recommended)
- .csv → Human readable, Excel-compatible
- .json → For APIs/JavaScript
- .xlsx → Direct Excel workbook

EXAMPLES:
  download_sql("wwi", "SELECT * FROM Sales.Orders WHERE OrderYear=2024", "./data/orders.parquet")
  download_sql("wwi", "SELECT TOP 100 * FROM Sales.Customers", "./data/customers.csv")
  download_sql("wwi_dw", "SELECT * FROM Fact.Sale", "C:/full/path/to/sales.parquet", limit=50000)

PATH TIPS:
- "./" prefix → relative to workspace root (recommended)
- Full absolute path → any folder you want

⚠️ AVOID bare paths like "data/file.csv" without "./" - they resolve unpredictably.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "connection": {
                        "oneOf": [
                            {"type": "string", "description": "Connection name from config"},
                            {
                                "type": "object",
                                "description": "Inline connection spec with type, server, database, etc.",
                            },
                        ],
                        "description": "SQL connection name or inline spec",
                    },
                    "query": {
                        "type": "string",
                        "description": "SQL query to execute",
                    },
                    "output_path": {
                        "type": "string",
                        "description": "Where to save. Use './data/file.csv' (relative to workspace) or full path 'C:/Users/name/Downloads/file.csv'. AVOID bare 'Downloads/file.csv'.",
                    },
                    "limit": {
                        "type": "integer",
                        "default": 10000,
                        "description": "Max rows to download (default 10000)",
                    },
                },
                "required": ["connection", "query", "output_path"],
            },
        ),
        Tool(
            name="download_table",
            description="""⬇️ DOWNLOAD entire SQL table. Shortcut for download_sql("SELECT * FROM table").

WHEN TO USE:
- Quick export without writing SQL
- Reference/dimension tables (usually small)
- Full table snapshots for testing

EXAMPLES:
  download_table("wwi", "Sales.Orders", "./data/orders.parquet")
  download_table("wwi", "Dimension.Customer", "./data/customers.csv")
  download_table("wwi_dw", "Dimension.Date", "C:/any/folder/dates.parquet", limit=50000)

⚠️ Use "./" for workspace-relative, or full absolute path. Limit default: 10,000 rows.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "connection": {
                        "oneOf": [
                            {"type": "string", "description": "Connection name from config"},
                            {"type": "object", "description": "Inline connection spec"},
                        ],
                        "description": "SQL connection name or inline spec",
                    },
                    "table": {
                        "type": "string",
                        "description": "Table name in schema.table format (e.g., 'Sales.Orders')",
                    },
                    "output_path": {
                        "type": "string",
                        "description": "Where to save. Use './data/file.csv' (relative to workspace) or full path 'C:/Users/name/Downloads/file.csv'.",
                    },
                    "limit": {
                        "type": "integer",
                        "default": 10000,
                        "description": "Max rows to download",
                    },
                },
                "required": ["connection", "table", "output_path"],
            },
        ),
        Tool(
            name="download_file",
            description="""⬇️ DOWNLOAD file from cloud storage (ADLS/S3/GCS) to local. Raw byte copy.

WHEN TO USE:
- After map_environment finds files you want locally
- Get Excel/CSV/Parquet for local analysis
- Download any file type (images, PDFs, etc.)

WORKS WITH: CSV, Parquet, JSON, Excel, images, PDFs - anything!
Files are copied as-is (raw bytes, no transformation).

EXAMPLES:
  download_file("raw_adls", "reports/daily.csv", "./data/daily.csv")
  download_file("raw_adls", "exports/2024/data.parquet", "./data/exports.parquet")
  download_file("raw_adls", "raw data/sales/jan.xlsx", "C:/any/folder/jan.xlsx")

⚠️ Use "./" for workspace-relative, or full absolute path. AVOID bare paths like "data/file.csv".""",
            inputSchema={
                "type": "object",
                "properties": {
                    "connection": {
                        "oneOf": [
                            {"type": "string", "description": "Storage connection name"},
                            {"type": "object", "description": "Inline storage connection spec"},
                        ],
                        "description": "Storage connection name or inline spec",
                    },
                    "source_path": {
                        "type": "string",
                        "description": "Path within storage (e.g., 'raw/data.csv')",
                    },
                    "output_path": {
                        "type": "string",
                        "description": "Where to save. Use './data/file.csv' (relative to workspace) or full path 'C:/Users/name/Downloads/file.csv'.",
                    },
                },
                "required": ["connection", "source_path", "output_path"],
            },
        ),
        # ============ DIAGNOSTIC TOOLS ============
        Tool(
            name="diagnose",
            description="""🔧 DIAGNOSE when tools fail. First step for troubleshooting.

CALL THIS WHEN:
- Connection errors or "not found" messages
- Tools return unexpected errors
- Need to verify setup before starting

WHAT IT CHECKS:
- .env file loaded and from where
- Environment variables present (ADLS_ACCOUNT, etc.)
- exploration.yaml path and validity
- Connection definitions available

DECISION TREE from results:
- env_loaded=false → Create .env file or set ODIBI_CONFIG env var
- missing env vars → Add them to .env (ADLS_ACCOUNT_NAME, ADLS_KEY, etc.)
- no connections → Check exploration.yaml path and content
- all good → Try map_environment again

NEXT ACTION: Fix issues shown, then retry the failing tool.""",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        Tool(
            name="diagnose_path",
            description="""🔧 CHECK if a path exists. Debug for "path not found" errors.

WHEN TO USE:
- map_environment returns empty or errors
- File path from suggested_sources doesn't work
- Need to verify folder structure

WHAT IT RETURNS:
- exists: true/false
- contents: list of files/folders if exists
- error: what went wrong if not

NEXT ACTION: Use the correct path with map_environment or profile_source.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Path to diagnose (can be relative or absolute)",
                    },
                },
                "required": ["path"],
            },
        ),
        # ============ EXECUTION TOOLS REMOVED ============
        # Use shell instead: python -m odibi run X.yaml
        # Removed: run_python, run_odibi, find_path, execute_pipeline
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
            from odibi_mcp.context import get_project_context, list_projects as list_projects_fn

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
        elif name == "list_projects":
            from odibi_mcp.context import list_projects as list_projects_fn, get_projects_dir

            projects = list_projects_fn()
            projects_dir = get_projects_dir()
            result = {
                "projects_dir": str(projects_dir) if projects_dir else None,
                "count": len(projects),
                "projects": projects,
            }
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
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
        # Discovery tools (kept: describe_table, list_sheets, list_schemas)
        elif name == "describe_table":
            res = describe_table(
                connection=arguments["connection"],
                table=arguments["table"],
                schema=arguments.get("schema", "dbo"),
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
        # ============ SMART DISCOVERY TOOL HANDLERS ============
        elif name == "map_environment":
            res = map_environment(
                connection=arguments["connection"],
                path=arguments.get("path", ""),
            )
            result = to_json_serializable(res)
        elif name == "profile_source":
            res = profile_source(
                connection=arguments["connection"],
                path=arguments["path"],
                max_attempts=arguments.get("max_attempts", 5),
            )
            result = to_json_serializable(res)
        elif name == "profile_folder":
            res = profile_folder(
                connection=arguments["connection"],
                folder_path=arguments.get("folder_path", ""),
                pattern=arguments.get("pattern", "*"),
                max_files=arguments.get("max_files", 50),
                max_attempts=arguments.get("max_attempts", 3),
            )
            result = to_json_serializable(res)
        elif name == "generate_bronze_node":
            res = generate_bronze_node(
                profile=arguments["profile"],
                node_name=arguments.get("node_name"),
                output_connection=arguments.get("output_connection"),
                output_path=arguments.get("output_path"),
                include_project=arguments.get("include_project", True),
                local_output=arguments.get("local_output", True),
            )
            result = to_json_serializable(res)
        elif name == "test_node":
            res = test_node(
                node_yaml=arguments["node_yaml"],
                max_rows=arguments.get("max_rows", 100),
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
        elif name == "diagnose":
            res = diagnose()
            result = to_json_serializable(res)
        elif name == "diagnose_path":
            result = diagnose_path(path=arguments["path"])
        # ============ DOWNLOAD TOOL HANDLERS ============
        elif name == "download_sql":
            res = download_sql(
                connection=arguments["connection"],
                query=arguments["query"],
                output_path=arguments["output_path"],
                limit=arguments.get("limit", 10000),
            )
            result = to_json_serializable(res)
        elif name == "download_table":
            res = download_table(
                connection=arguments["connection"],
                table=arguments["table"],
                output_path=arguments["output_path"],
                limit=arguments.get("limit", 10000),
            )
            result = to_json_serializable(res)
        elif name == "download_file":
            res = download_file(
                connection=arguments["connection"],
                source_path=arguments["source_path"],
                output_path=arguments["output_path"],
            )
            result = to_json_serializable(res)
        # Execution tools removed - use shell instead
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
