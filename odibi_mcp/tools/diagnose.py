# odibi_mcp/tools/diagnose.py
"""Diagnostic tool for troubleshooting MCP environment issues."""

import os
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Any

from odibi_mcp.context import get_project_context

logger = logging.getLogger(__name__)


@dataclass
class DiagnoseResult:
    """Result of environment diagnosis."""

    status: str  # "healthy", "issues_found", "error"
    summary: str
    environment: Dict[str, Any] = field(default_factory=dict)
    paths: Dict[str, Any] = field(default_factory=dict)
    connections: Dict[str, Any] = field(default_factory=dict)
    issues: List[str] = field(default_factory=list)
    suggestions: List[str] = field(default_factory=list)


def diagnose() -> DiagnoseResult:
    """
    Diagnose the MCP environment and report issues.

    Checks:
    - Environment variables (ODIBI_CONFIG, ODIBI_PROJECTS_DIR)
    - Project context loading
    - Story and system paths
    - Connection initialization
    - .env file presence and format

    Returns:
        DiagnoseResult with status, issues found, and suggestions
    """
    issues = []
    suggestions = []
    environment = {}
    paths = {}
    connections_info = {}

    # Check environment variables
    odibi_config = os.environ.get("ODIBI_CONFIG", "")
    odibi_projects_dir = os.environ.get("ODIBI_PROJECTS_DIR", "")

    environment["ODIBI_CONFIG"] = odibi_config or "(not set)"
    environment["ODIBI_PROJECTS_DIR"] = odibi_projects_dir or "(not set)"
    environment["cwd"] = os.getcwd()

    if not odibi_config:
        issues.append("ODIBI_CONFIG environment variable is not set")
        suggestions.append("Set ODIBI_CONFIG to point to your exploration.yaml or project.yaml")
    elif not Path(odibi_config).exists():
        issues.append(f"ODIBI_CONFIG points to non-existent file: {odibi_config}")
        suggestions.append("Check the path in your Continue config.yaml mcpServers section")
    else:
        paths["config_file"] = {"path": odibi_config, "exists": True}

    if not odibi_projects_dir:
        issues.append("ODIBI_PROJECTS_DIR is not set")
        suggestions.append("Set ODIBI_PROJECTS_DIR to your projects folder for auto-discovery")
    elif not Path(odibi_projects_dir).exists():
        issues.append(f"ODIBI_PROJECTS_DIR points to non-existent folder: {odibi_projects_dir}")
    else:
        # List projects
        projects_path = Path(odibi_projects_dir)
        yaml_files = list(projects_path.glob("*.yaml")) + list(projects_path.glob("*.yml"))
        paths["projects_dir"] = {
            "path": odibi_projects_dir,
            "exists": True,
            "project_files": [f.name for f in yaml_files[:10]],  # Limit to 10
        }

    # Check .env file
    env_locations = []
    if odibi_config:
        config_dir = Path(odibi_config).parent
        env_path = config_dir / ".env"
        env_locations.append(env_path)

    for env_path in env_locations:
        if env_path.exists():
            paths["env_file"] = {"path": str(env_path), "exists": True}
            # Check for common issues (without reading secrets)
            try:
                with open(env_path, "r") as f:
                    content = f.read()
                    lines = content.strip().split("\n")
                    for i, line in enumerate(lines, 1):
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        if "=" not in line:
                            issues.append(f".env line {i}: Missing '=' in '{line[:20]}...'")
                        elif line.startswith('"') or line.startswith("'"):
                            issues.append(f".env line {i}: Line starts with quote (should be VAR=value)")
            except Exception as e:
                issues.append(f"Could not parse .env file: {e}")
            break
    else:
        if odibi_config:
            issues.append(".env file not found next to ODIBI_CONFIG")
            suggestions.append(f"Create .env file at: {Path(odibi_config).parent / '.env'}")

    # Check project context
    try:
        ctx = get_project_context()
        if ctx:
            environment["project_name"] = ctx.project_name
            environment["mode"] = ctx.mode

            # Check story configuration
            if ctx.story_connection and ctx.story_path:
                paths["story"] = {
                    "connection": ctx.story_connection,
                    "path": ctx.story_path,
                }
                # Try to resolve actual path
                try:
                    ctx.initialize_connections()
                    conn = ctx.get_connection(ctx.story_connection)
                    story_base = Path(conn.get_path(ctx.story_path))
                    paths["story"]["resolved_path"] = str(story_base)
                    paths["story"]["exists"] = story_base.exists()

                    if story_base.exists():
                        # List pipeline directories
                        subdirs = [d.name for d in story_base.iterdir() if d.is_dir()]
                        paths["story"]["pipelines"] = subdirs[:10]
                    else:
                        issues.append(f"Story path does not exist: {story_base}")
                        suggestions.append("Run a pipeline first to generate story files")
                except Exception as e:
                    issues.append(f"Could not resolve story path: {e}")
            else:
                if ctx.mode == "full":
                    issues.append("Story configuration missing in full project mode")

            # Check system configuration
            if ctx.catalog_connection:
                paths["system"] = {"connection": ctx.catalog_connection}

            # Check connections
            try:
                ctx.initialize_connections()
                for name, conn in ctx.connections.items():
                    conn_type = type(conn).__name__
                    connections_info[name] = {
                        "type": conn_type,
                        "initialized": True,
                    }
                    # Add base_path for local connections
                    if hasattr(conn, "base_path"):
                        connections_info[name]["base_path"] = str(getattr(conn, "base_path", ""))
                    if hasattr(conn, "account"):
                        connections_info[name]["account"] = getattr(conn, "account", "")
            except Exception as e:
                issues.append(f"Connection initialization failed: {e}")
        else:
            issues.append("No project context loaded")
            suggestions.append("Check ODIBI_CONFIG points to a valid YAML file")
    except Exception as e:
        issues.append(f"Failed to load project context: {e}")

    # Determine overall status
    if not issues:
        status = "healthy"
        summary = "Environment is properly configured"
    else:
        status = "issues_found"
        summary = f"Found {len(issues)} issue(s) that may affect MCP tools"

    return DiagnoseResult(
        status=status,
        summary=summary,
        environment=environment,
        paths=paths,
        connections=connections_info,
        issues=issues,
        suggestions=suggestions,
    )


def diagnose_path(path: str) -> Dict[str, Any]:
    """
    Diagnose a specific path - check if it exists, list contents.

    Args:
        path: Path to diagnose (relative or absolute)

    Returns:
        Dictionary with path info, existence, and contents
    """
    result = {
        "input_path": path,
        "exists": False,
        "is_file": False,
        "is_directory": False,
        "contents": [],
        "error": None,
    }

    try:
        p = Path(path)

        # Try relative to cwd first
        if not p.is_absolute():
            cwd_path = Path.cwd() / path
            if cwd_path.exists():
                p = cwd_path

        result["resolved_path"] = str(p.absolute())
        result["exists"] = p.exists()

        if p.exists():
            result["is_file"] = p.is_file()
            result["is_directory"] = p.is_dir()

            if p.is_dir():
                contents = []
                for item in sorted(p.iterdir())[:50]:  # Limit to 50 items
                    item_info = {
                        "name": item.name,
                        "is_dir": item.is_dir(),
                    }
                    if item.is_file():
                        item_info["size"] = item.stat().st_size
                    contents.append(item_info)
                result["contents"] = contents
            elif p.is_file():
                result["size"] = p.stat().st_size
                result["suffix"] = p.suffix
    except Exception as e:
        result["error"] = str(e)

    return result
