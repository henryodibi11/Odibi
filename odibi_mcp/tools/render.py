"""Shared YAML rendering logic for MCP pipeline construction tools.

Produces complete, runnable YAML by merging pipeline config with project context.
Used by both construction.py (Phase 1) and builder.py (Phase 2).
"""

from typing import Any, Dict, List

import yaml

from odibi.config import PipelineConfig
from odibi_mcp.context import get_project_context


def _ensure_local_connection(connections: Dict[str, Any]) -> str:
    """Find or create a local connection for story/system output.

    Returns the connection name to use. If no local connection exists,
    adds one called 'local_output' to the connections dict.
    """
    for name, cfg in connections.items():
        if isinstance(cfg, dict) and cfg.get("type") in ("local", "file"):
            return name

    # No local connection exists — create one
    connections["local_output"] = {
        "type": "local",
        "base_path": "./output",
    }
    return "local_output"


def render_runnable_yaml(
    pipeline_config: PipelineConfig,
    warnings: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Render a validated PipelineConfig into a complete, runnable YAML string.

    Pulls project context (connections, story, system) from the global
    MCPProjectContext so the output is self-contained and ready to run.
    Always includes story and system sections — the AI never needs to set these.

    Args:
        pipeline_config: Validated Pydantic pipeline config.
        warnings: Mutable list to append warnings to.

    Returns:
        {
            "yaml": str,
            "valid": bool,
            "errors": list,
            "warnings": list,  (same reference as input)
        }
    """
    pipeline_dump = pipeline_config.model_dump(mode="json", exclude_defaults=True)

    ctx = get_project_context()
    if ctx and ctx.config:
        all_connections = ctx.config.get("connections", {})

        # Validate that pipeline connections exist in the project
        for node in pipeline_dump.get("nodes", []):
            for section in ("read", "write"):
                conn_name = (node.get(section) or {}).get("connection")
                if conn_name and conn_name not in all_connections:
                    warnings.append(
                        {
                            "code": "UNKNOWN_CONNECTION",
                            "message": f"Connection '{conn_name}' not found in project config",
                            "available": list(all_connections.keys()),
                        }
                    )

        full_config = {
            "project": ctx.project_name,
            "engine": ctx.config.get("engine", "pandas"),
            "connections": all_connections,
            "pipelines": [pipeline_dump],
        }

        # Always include story and system — use context values or sensible defaults
        story_cfg = ctx.config.get("story")
        system_cfg = ctx.config.get("system")

        if story_cfg and system_cfg:
            full_config["story"] = story_cfg
            full_config["system"] = system_cfg
        else:
            local_conn = _ensure_local_connection(all_connections)
            full_config["story"] = story_cfg or {
                "connection": local_conn,
                "path": "_stories",
            }
            full_config["system"] = system_cfg or {
                "connection": local_conn,
                "path": "_system",
            }
    else:
        # No project context — still produce a runnable YAML with local defaults
        node_connections = {}
        for node in pipeline_dump.get("nodes", []):
            for section in ("read", "write"):
                conn = (node.get(section) or {}).get("connection")
                if conn:
                    node_connections[conn] = {}

        local_conn = _ensure_local_connection(node_connections)

        full_config = {
            "project": pipeline_config.pipeline,
            "engine": "pandas",
            "connections": node_connections,
            "pipelines": [pipeline_dump],
            "story": {"connection": local_conn, "path": "_stories"},
            "system": {"connection": local_conn, "path": "_system"},
        }

        warnings.append(
            {
                "code": "NO_PROJECT_CONTEXT",
                "message": (
                    "No project config loaded. Connection configs are empty placeholders — "
                    "you must fill in connection details (type, credentials, etc.) before running."
                ),
            }
        )

    yaml_content = yaml.safe_dump(
        full_config,
        default_flow_style=False,
        sort_keys=False,
    )

    # Round-trip validation: re-parse the pipeline through Pydantic
    try:
        reparsed = yaml.safe_load(yaml_content)
        PipelineConfig(**reparsed["pipelines"][0])
    except Exception as e:
        return {
            "yaml": "",
            "valid": False,
            "errors": [
                {
                    "code": "ROUND_TRIP_FAILED",
                    "message": f"Generated YAML failed round-trip validation: {str(e)}",
                    "fix": "This is a bug in the tool - please report it",
                }
            ],
            "warnings": warnings,
        }

    return {
        "yaml": yaml_content,
        "valid": True,
        "errors": [],
        "warnings": warnings,
    }
