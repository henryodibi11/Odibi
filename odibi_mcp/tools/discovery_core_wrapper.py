"""Discovery tools that delegate to odibi core.

This replaces the complex logic in smart.py and discovery.py with thin wrappers
that call connection.discover_catalog(), connection.profile(), etc.
"""

from typing import Any, Dict
from odibi_mcp.context import get_project_context


def map_environment_core(connection: str, path: str = "") -> Dict[str, Any]:
    """Map connection environment using core discovery.

    Delegates to connection.discover_catalog() or connection.list_files().

    Args:
        connection: Connection name
        path: Path to explore (empty for root)

    Returns:
        MapEnvironmentResponse-compatible dict
    """
    ctx = get_project_context()

    if not ctx:
        return {
            "error": "No project context. Set ODIBI_CONFIG environment variable.",
            "connection": connection,
            "scanned_at": None,
            "summary": {},
            "structure": [],
            "recommendations": [],
            "next_step": "",
            "suggested_sources": [],
            "errors": ["No project context available"],
        }

    conn = ctx.connections.get(connection)

    if not conn:
        return {
            "error": f"Connection '{connection}' not found",
            "connection": connection,
            "scanned_at": None,
            "summary": {},
            "structure": [],
            "recommendations": [],
            "next_step": "",
            "suggested_sources": [],
            "errors": [f"Connection '{connection}' not found in config"],
        }

    try:
        from datetime import datetime

        # Use core discovery
        if path:
            # List files in specific path (filesystem connections)
            if hasattr(conn, "list_files"):
                files = conn.list_files(path=path, limit=100)
                folders = (
                    conn.list_folders(path=path, limit=50) if hasattr(conn, "list_folders") else []
                )

                return {
                    "connection": connection,
                    "connection_type": type(conn).__name__,
                    "scanned_at": datetime.utcnow(),
                    "summary": {
                        "total_files": len(files),
                        "total_folders": len(folders),
                        "path": path,
                    },
                    "structure": files + folders,
                    "recommendations": [
                        f"Found {len(files)} files in {path}",
                        "Use profile_source to analyze specific files",
                    ],
                    "next_step": "profile_source",
                    "suggested_sources": [f.get("path", f.get("name")) for f in files[:5]],
                    "errors": [],
                }
            else:
                # SQL connection - list tables in schema
                if hasattr(conn, "list_tables"):
                    tables = conn.list_tables(schema=path or "dbo")

                    return {
                        "connection": connection,
                        "connection_type": type(conn).__name__,
                        "scanned_at": datetime.utcnow(),
                        "summary": {"total_tables": len(tables), "schema": path or "dbo"},
                        "structure": tables,
                        "recommendations": [
                            f"Found {len(tables)} tables in schema '{path or 'dbo'}'",
                            "Use profile_source to analyze specific tables",
                        ],
                        "next_step": "profile_source",
                        "suggested_sources": [t.get("name") for t in tables[:5]],
                        "errors": [],
                    }

        # No path - discover catalog
        catalog = conn.discover_catalog(include_schema=False, include_stats=False, limit=200)

        return {
            "connection": connection,
            "connection_type": catalog.get("connection_type", type(conn).__name__),
            "scanned_at": catalog.get("generated_at", datetime.utcnow()),
            "summary": {
                "total_datasets": catalog.get("total_datasets", 0),
                "schemas": catalog.get("schemas", []),
                "formats": catalog.get("formats", {}),
            },
            "structure": catalog.get("tables", []) or catalog.get("files", []) or [],
            "recommendations": catalog.get("suggestions", []),
            "next_step": catalog.get("next_step", "profile_source"),
            "suggested_sources": [],
            "errors": [],
        }

    except NotImplementedError as e:
        return {
            "error": str(e),
            "connection": connection,
            "scanned_at": None,
            "summary": {},
            "structure": [],
            "recommendations": ["This connection type does not support discovery yet"],
            "next_step": "",
            "suggested_sources": [],
            "errors": [str(e)],
        }
    except Exception as e:
        return {
            "error": str(e),
            "connection": connection,
            "scanned_at": None,
            "summary": {},
            "structure": [],
            "recommendations": [],
            "next_step": "",
            "suggested_sources": [],
            "errors": [str(e)],
        }


def profile_source_core(connection: str, path: str, max_attempts: int = 5) -> Dict[str, Any]:
    """Profile a data source using core discovery.

    Delegates to connection.profile().

    Args:
        connection: Connection name
        path: File path or table name
        max_attempts: Max profiling attempts (unused, for compatibility)

    Returns:
        ProfileSourceResponse-compatible dict
    """
    ctx = get_project_context()

    if not ctx:
        return {
            "error": "No project context available",
            "connection": connection,
            "path": path,
            "format": None,
            "confidence": 0.0,
            "warnings": [],
            "errors": ["No project context. Set ODIBI_CONFIG environment variable."],
        }

    conn = ctx.connections.get(connection)

    if not conn:
        return {
            "error": f"Connection '{connection}' not found",
            "connection": connection,
            "path": path,
            "format": None,
            "confidence": 0.0,
            "warnings": [],
            "errors": [f"Connection '{connection}' not found"],
        }

    try:
        # Use core profiling
        profile = conn.profile(dataset=path, sample_rows=1000)

        # Get schema
        schema = conn.get_schema(dataset=path) if hasattr(conn, "get_schema") else {}

        # Transform to ProfileSourceResponse format
        return {
            "connection": connection,
            "path": path,
            "format": profile.get("dataset", {}).get("format", "unknown"),
            "confidence": 0.9,  # Core profiling is reliable
            "schema": schema,
            "sample_rows": [],  # Can add if needed
            "candidate_keys": profile.get("candidate_keys", []),
            "candidate_watermarks": profile.get("candidate_watermarks", []),
            "row_count_estimate": profile.get("total_rows"),
            "suggestions": profile.get("suggestions", []),
            "warnings": profile.get("warnings", []),
            "errors": [],
            "next_step": "apply_pattern_template or suggest_pipeline",
            "ready_for": {"suggest_pipeline": {"profile": profile}},
        }

    except NotImplementedError as e:
        return {
            "error": str(e),
            "connection": connection,
            "path": path,
            "format": None,
            "confidence": 0.0,
            "warnings": [],
            "errors": [str(e)],
        }
    except Exception as e:
        return {
            "error": str(e),
            "connection": connection,
            "path": path,
            "format": None,
            "confidence": 0.0,
            "warnings": [],
            "errors": [str(e)],
        }
