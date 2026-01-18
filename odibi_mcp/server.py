"""MCP Server for Odibi Knowledge.

Exposes odibi knowledge through the Model Context Protocol (MCP).
"""

# ruff: noqa: E402
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

# Ensure odibi is importable
ODIBI_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ODIBI_ROOT))
sys.path.insert(0, str(ODIBI_ROOT / "_archive"))

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import (
    TextContent,
    Tool,
)

from .knowledge import get_knowledge

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create MCP server
server = Server("odibi-knowledge")


@server.list_tools()
async def list_tools() -> list[Tool]:
    """List available tools."""
    return [
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
                    "input_path": {"type": "string", "description": "Path to input file"},
                    "input_format": {
                        "type": "string",
                        "description": "Format: csv, parquet, json, delta",
                    },
                    "output_path": {"type": "string", "description": "Path to output file"},
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
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Handle tool calls."""
    knowledge = get_knowledge()

    try:
        if name == "list_transformers":
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
        else:
            result = {"error": f"Unknown tool: {name}"}

        return [TextContent(type="text", text=json.dumps(result, indent=2))]

    except Exception as e:
        logger.exception(f"Error in tool {name}")
        return [TextContent(type="text", text=json.dumps({"error": str(e)}))]


async def main():
    """Run the MCP server."""
    logger.info("Starting odibi-knowledge MCP server...")
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


def run():
    """Entry point for the server."""
    import asyncio

    asyncio.run(main())


if __name__ == "__main__":
    run()
