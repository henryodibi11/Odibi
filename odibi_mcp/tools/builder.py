"""Phase 2: Session-Based Incremental Pipeline Builder.

Allows agents to build complex multi-node pipelines incrementally:
1. create_pipeline() - Start a builder session
2. add_node() - Add nodes one by one
3. configure_read/write/transform/validation() - Configure node properties
4. get_pipeline_state() - Inspect current state
5. render_pipeline_yaml() - Finalize and serialize to YAML

Design:
- Thread-safe sessions with locks
- TTL-based eviction (30 minutes)
- Idempotent operations
- Capacity limits (max 10 sessions)
"""

from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import threading
import uuid

from odibi.config import (
    PipelineConfig,
    NodeConfig,
    ReadConfig,
    WriteConfig,
)
from odibi.registry import FunctionRegistry
from odibi.transformers import register_standard_library

# Ensure transformers are registered
register_standard_library()


# Session storage
_sessions: Dict[str, "PipelineBuilderSession"] = {}
_sessions_lock = threading.Lock()
_MAX_SESSIONS = 10
_SESSION_TTL_MINUTES = 30


@dataclass
class PipelineBuilderSession:
    """A pipeline builder session."""

    session_id: str
    pipeline_name: str
    layer: str = "gold"
    nodes: List[Dict[str, Any]] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_touched: datetime = field(default_factory=datetime.utcnow)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def touch(self):
        """Update last accessed timestamp."""
        self.last_touched = datetime.utcnow()

    def is_expired(self) -> bool:
        """Check if session has exceeded TTL."""
        age = datetime.utcnow() - self.last_touched
        return age > timedelta(minutes=_SESSION_TTL_MINUTES)

    def get_node(self, node_name: str) -> Optional[Dict[str, Any]]:
        """Find a node by name."""
        for node in self.nodes:
            if node.get("name") == node_name:
                return node
        return None

    def get_node_names(self) -> List[str]:
        """Get all node names."""
        return [n.get("name") for n in self.nodes if n.get("name")]


def _evict_expired_sessions():
    """Remove expired sessions (called with lock held)."""
    expired = [sid for sid, sess in _sessions.items() if sess.is_expired()]
    for sid in expired:
        del _sessions[sid]
    return len(expired)


def _evict_oldest_session():
    """Remove oldest session by LRU (called with lock held)."""
    if not _sessions:
        return
    oldest_id = min(_sessions.keys(), key=lambda k: _sessions[k].last_touched)
    del _sessions[oldest_id]


def create_pipeline(pipeline_name: str, layer: str = "gold") -> Dict[str, Any]:
    """Start a new pipeline builder session.

    Args:
        pipeline_name: Name for the pipeline (alphanumeric + underscore)
        layer: Pipeline layer (bronze, silver, gold)

    Returns:
        {
            "session_id": str,
            "pipeline_name": str,
            "layer": str,
            "expires_in_minutes": int,
            "next_step": "Call add_node to add your first node"
        }
    """
    import re

    # Validate pipeline name
    if not re.match(r"^[a-zA-Z0-9_]+$", pipeline_name):
        return {
            "session_id": None,
            "error": {
                "code": "INVALID_NAME",
                "message": f"Pipeline name '{pipeline_name}' is invalid",
                "fix": "Use only alphanumeric characters and underscores",
            },
        }

    with _sessions_lock:
        # Evict expired sessions
        _evict_expired_sessions()

        # Check capacity
        if len(_sessions) >= _MAX_SESSIONS:
            _evict_oldest_session()

        # Create session
        session_id = str(uuid.uuid4())
        session = PipelineBuilderSession(
            session_id=session_id, pipeline_name=pipeline_name, layer=layer
        )

        _sessions[session_id] = session

        return {
            "session_id": session_id,
            "pipeline_name": pipeline_name,
            "layer": layer,
            "expires_in_minutes": _SESSION_TTL_MINUTES,
            "sessions_active": len(_sessions),
            "next_step": "Call add_node to add your first node",
        }


def add_node(
    session_id: str, node_name: str, depends_on: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Add a node to the pipeline.

    Args:
        session_id: Session ID from create_pipeline
        node_name: Name for the node (alphanumeric + underscore)
        depends_on: Optional list of node names this depends on

    Returns:
        {
            "node_name": str,
            "depends_on": list,
            "next_step": "Configure read, write, or transform for this node"
        }
    """
    import re

    # Validate node name
    if not re.match(r"^[a-zA-Z0-9_]+$", node_name):
        return {
            "error": {
                "code": "INVALID_NAME",
                "message": f"Node name '{node_name}' is invalid",
                "fix": "Use only alphanumeric characters and underscores",
            }
        }

    with _sessions_lock:
        session = _sessions.get(session_id)
        if not session:
            return {
                "error": {
                    "code": "SESSION_NOT_FOUND",
                    "message": f"Session '{session_id}' not found",
                    "fix": "Call create_pipeline to start a new session",
                }
            }

    # Lock this session
    with session.lock:
        session.touch()

        # Check if node already exists
        if session.get_node(node_name):
            return {
                "error": {
                    "code": "NODE_EXISTS",
                    "message": f"Node '{node_name}' already exists",
                    "fix": "Use a different node name or modify the existing node",
                }
            }

        # Validate dependencies exist
        if depends_on:
            existing_nodes = session.get_node_names()
            missing = [d for d in depends_on if d not in existing_nodes]
            if missing:
                return {
                    "error": {
                        "code": "MISSING_DEPENDENCY",
                        "message": f"Dependency nodes not found: {missing}",
                        "existing_nodes": existing_nodes,
                        "fix": f"Add nodes {missing} first, or remove from depends_on",
                    }
                }

        # Create node
        node = {"name": node_name, "depends_on": depends_on or []}

        session.nodes.append(node)

        return {
            "session_id": session_id,
            "node_name": node_name,
            "depends_on": depends_on or [],
            "node_count": len(session.nodes),
            "next_step": f"Configure read, write, or transform for '{node_name}'",
        }


def configure_read(
    session_id: str,
    node_name: str,
    connection: str,
    format: str,
    table: Optional[str] = None,
    path: Optional[str] = None,
    query: Optional[str] = None,
    options: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Configure read operation for a node.

    Args:
        session_id: Session ID
        node_name: Node to configure
        connection: Connection name
        format: Data format (sql, csv, parquet, json, delta)
        table: Table name (for SQL format)
        path: File path (for file formats)
        query: SQL query (overrides table)
        options: Additional read options

    Returns:
        Status and next steps
    """
    with _sessions_lock:
        session = _sessions.get(session_id)
        if not session:
            return {"error": {"code": "SESSION_NOT_FOUND", "message": "Session not found"}}

    with session.lock:
        session.touch()

        node = session.get_node(node_name)
        if not node:
            return {"error": {"code": "NODE_NOT_FOUND", "message": f"Node '{node_name}' not found"}}

        # Build read config
        read_config = {
            "connection": connection,
            "format": format,
        }

        if query:
            read_config["query"] = query
        elif table:
            read_config["table"] = table
        elif path:
            read_config["path"] = path
        else:
            return {
                "error": {
                    "code": "MISSING_SOURCE",
                    "message": "Either table, path, or query is required",
                    "fix": "Provide one of: table, path, or query",
                }
            }

        if options:
            read_config["options"] = options

        # Validate through Pydantic
        try:
            ReadConfig(**read_config)
        except Exception as e:
            return {
                "error": {
                    "code": "INVALID_READ_CONFIG",
                    "message": str(e),
                    "fix": "Check read configuration parameters",
                }
            }

        node["read"] = read_config

        return {
            "session_id": session_id,
            "node_name": node_name,
            "read_configured": True,
            "next_step": "Configure write or transform for this node",
        }


def configure_write(
    session_id: str,
    node_name: str,
    connection: str,
    format: str,
    path: Optional[str] = None,
    table: Optional[str] = None,
    mode: str = "overwrite",
    keys: Optional[List[str]] = None,
    partition_by: Optional[List[str]] = None,
    options: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Configure write operation for a node.

    Args:
        session_id: Session ID
        node_name: Node to configure
        connection: Connection name
        format: Output format (delta, parquet, csv, json)
        path: Output path
        table: Output table name
        mode: Write mode (overwrite, append, upsert, append_once, merge)
        keys: Key columns (required for upsert/append_once/merge)
        partition_by: Partition columns
        options: Additional write options

    Returns:
        Status and next steps
    """
    with _sessions_lock:
        session = _sessions.get(session_id)
        if not session:
            return {"error": {"code": "SESSION_NOT_FOUND", "message": "Session not found"}}

    with session.lock:
        session.touch()

        node = session.get_node(node_name)
        if not node:
            return {"error": {"code": "NODE_NOT_FOUND", "message": f"Node '{node_name}' not found"}}

        # Build write config
        write_config = {
            "connection": connection,
            "format": format,
            "mode": mode,
        }

        if path:
            write_config["path"] = path
        elif table:
            write_config["table"] = table
        else:
            return {
                "error": {
                    "code": "MISSING_TARGET",
                    "message": "Either path or table is required",
                    "fix": "Provide path or table",
                }
            }

        # Handle keys for upsert/append_once modes
        if mode in ("upsert", "append_once"):
            if not keys:
                return {
                    "error": {
                        "code": "MODE_REQUIRES_KEYS",
                        "message": f"Write mode '{mode}' requires 'keys'",
                        "fix": "Provide keys parameter with key columns",
                    }
                }
            if not options:
                options = {}
            options["keys"] = keys

        if mode == "merge" and not keys:
            return {
                "error": {
                    "code": "MODE_REQUIRES_MERGE_KEYS",
                    "message": "Write mode 'merge' requires 'keys'",
                    "fix": "Provide keys parameter",
                }
            }

        if partition_by:
            write_config["partition_by"] = partition_by

        if options:
            write_config["options"] = options

        if mode == "merge" and keys:
            write_config["merge_keys"] = keys

        # Validate through Pydantic
        try:
            WriteConfig(**write_config)
        except Exception as e:
            return {
                "error": {
                    "code": "INVALID_WRITE_CONFIG",
                    "message": str(e),
                    "fix": "Check write configuration parameters",
                }
            }

        node["write"] = write_config

        return {
            "session_id": session_id,
            "node_name": node_name,
            "write_configured": True,
            "mode": mode,
            "next_step": "Add transforms or add another node",
        }


def configure_transform(
    session_id: str, node_name: str, steps: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Configure transformation steps for a node.

    Args:
        session_id: Session ID
        node_name: Node to configure
        steps: List of transform steps [{"function": "name", "params": {...}}]

    Returns:
        Status and next steps
    """
    with _sessions_lock:
        session = _sessions.get(session_id)
        if not session:
            return {"error": {"code": "SESSION_NOT_FOUND", "message": "Session not found"}}

    with session.lock:
        session.touch()

        node = session.get_node(node_name)
        if not node:
            return {"error": {"code": "NODE_NOT_FOUND", "message": f"Node '{node_name}' not found"}}

        # Validate steps
        errors = []
        for i, step in enumerate(steps):
            func_name = step.get("function")
            if not func_name:
                errors.append(
                    {
                        "step": i,
                        "code": "MISSING_FUNCTION",
                        "message": "Transform step missing 'function' field",
                    }
                )
                continue

            # Validate function exists
            if not FunctionRegistry.has_function(func_name):
                errors.append(
                    {
                        "step": i,
                        "code": "UNKNOWN_TRANSFORMER",
                        "message": f"Transformer '{func_name}' not found",
                        "fix": "Use list_transformers to see available functions",
                    }
                )
                continue

            # Validate params
            try:
                FunctionRegistry.validate_params(func_name, step.get("params", {}))
            except ValueError as e:
                errors.append(
                    {
                        "step": i,
                        "code": "INVALID_PARAMS",
                        "message": str(e),
                        "fix": f"Check parameters for '{func_name}'",
                    }
                )

        if errors:
            return {
                "error": {
                    "code": "INVALID_TRANSFORM_STEPS",
                    "message": f"{len(errors)} validation error(s) in transform steps",
                    "details": errors,
                }
            }

        # Build transform config
        node["transform"] = {"steps": steps}

        return {
            "session_id": session_id,
            "node_name": node_name,
            "transform_configured": True,
            "step_count": len(steps),
            "next_step": "Add more nodes or call render_pipeline_yaml",
        }


def get_pipeline_state(session_id: str) -> Dict[str, Any]:
    """Get current state of pipeline builder session.

    Args:
        session_id: Session ID

    Returns:
        Current pipeline structure
    """
    with _sessions_lock:
        session = _sessions.get(session_id)
        if not session:
            return {"error": {"code": "SESSION_NOT_FOUND", "message": "Session not found"}}

    with session.lock:
        session.touch()

        return {
            "session_id": session_id,
            "pipeline_name": session.pipeline_name,
            "layer": session.layer,
            "node_count": len(session.nodes),
            "nodes": [
                {
                    "name": n.get("name"),
                    "has_read": "read" in n,
                    "has_write": "write" in n,
                    "has_transform": "transform" in n,
                    "depends_on": n.get("depends_on", []),
                }
                for n in session.nodes
            ],
            "age_minutes": int((datetime.utcnow() - session.created_at).total_seconds() / 60),
            "expires_in_minutes": max(
                0,
                _SESSION_TTL_MINUTES
                - int((datetime.utcnow() - session.last_touched).total_seconds() / 60),
            ),
        }


def render_pipeline_yaml(session_id: str) -> Dict[str, Any]:
    """Finalize and render pipeline to YAML.

    Performs full validation before rendering:
    - All nodes have read/write configured
    - DAG validation (no missing deps, no cycles)
    - Pattern/transformer parameter validation
    - Round-trip YAML validation

    Args:
        session_id: Session ID

    Returns:
        {
            "yaml": str,
            "valid": bool,
            "errors": [...],
            "warnings": [...]
        }
    """
    with _sessions_lock:
        session = _sessions.get(session_id)
        if not session:
            return {"error": {"code": "SESSION_NOT_FOUND", "message": "Session not found"}}

    with session.lock:
        session.touch()

        errors = []
        warnings = []

        # Check all nodes are complete
        for node in session.nodes:
            node_name = node.get("name")
            if "write" not in node:
                errors.append(
                    {
                        "code": "INCOMPLETE_NODE",
                        "node": node_name,
                        "message": f"Node '{node_name}' missing write configuration",
                        "fix": f"Call configure_write for node '{node_name}'",
                    }
                )

        if errors:
            return {"yaml": "", "valid": False, "errors": errors, "warnings": warnings}

        # Build pipeline config
        try:
            pipeline_config = PipelineConfig(
                pipeline=session.pipeline_name,
                layer=session.layer,
                nodes=[NodeConfig(**n) for n in session.nodes],
            )
        except Exception as e:
            return {
                "yaml": "",
                "valid": False,
                "errors": [
                    {
                        "code": "PYDANTIC_VALIDATION_FAILED",
                        "message": str(e),
                        "fix": "Check node configurations",
                    }
                ],
                "warnings": warnings,
            }

        # Serialize to YAML
        import yaml

        yaml_content = yaml.safe_dump(
            {"pipelines": [pipeline_config.model_dump(mode="json", exclude_none=True)]},
            default_flow_style=False,
            sort_keys=False,
        )

        # Round-trip validation
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
                        "message": f"Generated YAML failed validation: {str(e)}",
                        "fix": "This is a bug - please report it",
                    }
                ],
                "warnings": warnings,
            }

        return {
            "yaml": yaml_content,
            "valid": True,
            "errors": [],
            "warnings": warnings,
            "session_id": session_id,
            "pipeline_name": session.pipeline_name,
            "node_count": len(session.nodes),
            "next_step": "Save YAML to file and run with: python -m odibi run <file>.yaml",
        }


def list_sessions() -> Dict[str, Any]:
    """List all active builder sessions.

    Returns:
        List of active sessions with metadata
    """
    with _sessions_lock:
        # Evict expired
        evicted = _evict_expired_sessions()

        sessions = []
        for sid, sess in _sessions.items():
            age_minutes = int((datetime.utcnow() - sess.created_at).total_seconds() / 60)
            expires_in = max(
                0,
                _SESSION_TTL_MINUTES
                - int((datetime.utcnow() - sess.last_touched).total_seconds() / 60),
            )

            sessions.append(
                {
                    "session_id": sid,
                    "pipeline_name": sess.pipeline_name,
                    "layer": sess.layer,
                    "node_count": len(sess.nodes),
                    "age_minutes": age_minutes,
                    "expires_in_minutes": expires_in,
                }
            )

        return {
            "sessions": sessions,
            "count": len(sessions),
            "evicted": evicted,
            "capacity": f"{len(sessions)}/{_MAX_SESSIONS}",
        }


def discard_pipeline(session_id: str) -> Dict[str, Any]:
    """Discard a builder session without rendering.

    Args:
        session_id: Session ID to discard

    Returns:
        Confirmation
    """
    with _sessions_lock:
        session = _sessions.pop(session_id, None)
        if not session:
            return {"error": {"code": "SESSION_NOT_FOUND", "message": "Session not found"}}

        return {
            "discarded": True,
            "session_id": session_id,
            "pipeline_name": session.pipeline_name,
            "nodes_discarded": len(session.nodes),
        }
