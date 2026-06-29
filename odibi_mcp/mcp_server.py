"""MCP server for odibi_mcp — exposes Odibi tools via FastMCP.

Usage:
    python -m odibi_mcp.mcp_server          # stdio transport (default)
    fastmcp run odibi_mcp.mcp_server:mcp    # or via fastmcp CLI
"""
from __future__ import annotations

import json
from typing import Any

from fastmcp import FastMCP
from odibi_mcp.dispatcher import OdibiDispatcher

# Create FastMCP server instance
mcp = FastMCP("odibi-knowledge")

# Create dispatcher singleton
_dispatcher = OdibiDispatcher()


@mcp.tool()
def odibi_execute(action: str, args_json: str | None = None) -> str:
    """Execute an Odibi action via the universal dispatcher.
    
    This is the gateway to all 37+ Odibi actions across 9 categories:
    - Workflows: run_workflow, resume_workflow, list_workflows, get_workflow
    - Discovery: map_environment, profile_source, profile_folder
    - Inspection: story_read, node_sample, node_failed_rows, lineage_graph
    - Construction: list_transformers, list_patterns, apply_pattern_template, suggest_pipeline, create_ingestion_pipeline
    - Validation: validate_yaml, validate_pipeline, test_pipeline, diagnose
    - Task Guidance: get_task_guidance, list_task_types
    - Onboarding: onboard, get_schema, search_docs, get_doc, list_docs, list_examples, get_example, list_skills, get_skill
    - Download: download_sql, download_table, download_file
    - Session Builder: create_pipeline, add_node, configure_read, configure_write, configure_transform, get_pipeline_state, render_pipeline_yaml, list_sessions, discard_pipeline
    
    Args:
        action: Action name (e.g., 'profile_source', 'run_workflow', 'validate_yaml')
        args_json: JSON string of keyword arguments for the action (optional)
                  Example: '{"source_path": "/data/file.csv", "profile_level": "full"}'
    
    Returns:
        JSON string containing the action result or error message
    
    Examples:
        odibi_execute("list_workflows")
        odibi_execute("profile_source", '{"source_path": "/data/sensors.csv"}')
        odibi_execute("run_workflow", '{"workflow_name": "ingestion", "params": {"date": "2024-01-01"}}')
    """
    try:
        # Parse args_json if provided
        kwargs: dict[str, Any] = {}
        if args_json is not None and args_json.strip():
            try:
                kwargs = json.loads(args_json)
                if not isinstance(kwargs, dict):
                    return json.dumps({
                        "error": "args_json must be a JSON object (dict), not " + type(kwargs).__name__,
                        "tip": "Pass arguments as JSON object: '{\"key\": \"value\"}'"
                    })
            except json.JSONDecodeError as e:
                return json.dumps({
                    "error": f"Invalid JSON in args_json: {str(e)}",
                    "tip": "Ensure args_json is valid JSON. Example: '{\"key\": \"value\"}'"
                })
        
        # Dispatch to action
        result = _dispatcher.dispatch(action, **kwargs)
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({
            "error": f"Unexpected error executing {action}: {str(e)}",
            "action": action,
            "tip": "Run odibi_help() to see available actions and usage"
        })


@mcp.tool()
def odibi_help(category: str | None = None, action: str | None = None) -> str:
    """Get help on Odibi actions and capabilities.
    
    Args:
        category: Optional category filter. Available categories:
                 - Workflows
                 - Discovery
                 - Inspection
                 - Construction
                 - Validation
                 - Task Guidance
                 - Onboarding
                 - Download
                 - Session Builder
        action: Optional action name for detailed help on a specific action
    
    Returns:
        JSON string containing help documentation
    
    Examples:
        odibi_help()  # Full action catalog
        odibi_help(category="Discovery")  # Actions in Discovery category
        odibi_help(action="profile_source")  # Detailed help for profile_source
    """
    try:
        result = _dispatcher.help(category=category, action=action)
        return json.dumps(result, indent=2)
    except Exception as e:
        return json.dumps({
            "error": f"Error getting help: {str(e)}",
            "tip": "Try odibi_help() with no arguments for the full catalog"
        })
