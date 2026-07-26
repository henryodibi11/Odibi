from __future__ import annotations

import copy
import hashlib
import hmac
import json
import os
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Dict, FrozenSet, List, Optional, Set

import yaml
from pydantic import BaseModel, Field, field_validator


class ActionEffect(str, Enum):
    """Portable effect classes enforced at the universal dispatcher."""

    PUBLIC_READ = "public_read"
    SENSITIVE_READ = "sensitive_read"
    EXECUTION = "execution"
    FILE_WRITE = "file_write"
    SESSION_MUTATION = "session_mutation"


RESTRICTED_EFFECTS: FrozenSet[ActionEffect] = frozenset(
    effect for effect in ActionEffect if effect is not ActionEffect.PUBLIC_READ
)


@dataclass(frozen=True)
class ApplicationIdentity:
    """An authenticated application principal with explicit effect grants."""

    subject: str
    authorized_effects: FrozenSet[ActionEffect]

    def __post_init__(self) -> None:
        if not isinstance(self.subject, str) or not self.subject.strip():
            raise ValueError("Application identity subject must be a non-empty string")
        if not isinstance(self.authorized_effects, frozenset) or any(
            not isinstance(effect, ActionEffect) for effect in self.authorized_effects
        ):
            raise TypeError("authorized_effects must be a frozenset of ActionEffect values")

    def authorizes(self, effect: ActionEffect) -> bool:
        """Return whether this principal is authorized for an action effect."""
        return effect is ActionEffect.PUBLIC_READ or effect in self.authorized_effects

    @classmethod
    def authenticated_application(cls) -> ApplicationIdentity:
        """Create the principal represented by the configured HTTP Bearer token."""
        return cls(subject="configured-bearer-application", authorized_effects=RESTRICTED_EFFECTS)

    @classmethod
    def trusted_local(cls) -> ApplicationIdentity:
        """Create the explicit principal used by the direct in-process bootstrap."""
        return cls(subject="trusted-local", authorized_effects=RESTRICTED_EFFECTS)


def authenticate_bearer_identity(
    authorization_header: Optional[str], configured_token: Optional[str]
) -> Optional[ApplicationIdentity]:
    """Authenticate one configured application token without exposing its value."""
    if not isinstance(authorization_header, str) or not isinstance(configured_token, str):
        return None
    if not configured_token or any(character.isspace() for character in configured_token):
        return None

    scheme, separator, candidate = authorization_header.partition(" ")
    if (
        separator != " "
        or scheme.lower() != "bearer"
        or not candidate
        or any(character.isspace() for character in candidate)
    ):
        return None
    if not hmac.compare_digest(candidate.encode("utf-8"), configured_token.encode("utf-8")):
        return None
    return ApplicationIdentity.authenticated_application()


RUNTIME_DATA_ACTIONS: FrozenSet[str] = frozenset(
    {
        "map_environment",
        "profile_source",
        "profile_folder",
        "suggest_pipeline",
        "story_read",
        "node_sample",
        "node_failed_rows",
        "lineage_graph",
        "diagnose",
        "download_sql",
        "download_table",
        "download_file",
    }
)

_DOWNLOAD_ACTIONS = frozenset({"download_sql", "download_table", "download_file"})
_CONNECTION_ACTIONS = frozenset(
    {
        "map_environment",
        "profile_source",
        "profile_folder",
        "suggest_pipeline",
        "download_sql",
        "download_table",
        "download_file",
    }
)
_STORY_ACTIONS = frozenset({"story_read", "node_sample", "node_failed_rows"})
_PIPELINE_ACTIONS = _STORY_ACTIONS | {"lineage_graph"}
_SQL_CONNECTION_TYPES = frozenset({"azure_sql", "sql_server"})
_STORAGE_CONNECTION_TYPES = frozenset({"local", "azure_adls", "azure_blob"})
_SAFE_DOWNLOAD_SUFFIXES = frozenset({".csv", ".json", ".parquet"})
_SENSITIVE_RESPONSE_KEYS = frozenset(
    {
        "access_key",
        "account_key",
        "api_key",
        "client_secret",
        "connection_config",
        "connection_string",
        "credential",
        "credentials",
        "password",
        "private_key",
        "sas_token",
        "secret",
        "storage_options",
        "token",
    }
)
_COMPONENT_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_SQL_COMPONENT = r"[A-Za-z_][A-Za-z0-9_]{0,127}"
_SQL_IDENTIFIER = rf"{_SQL_COMPONENT}(?:\.{_SQL_COMPONENT}){{0,2}}"
_SQL_IDENTIFIER_RE = re.compile(rf"{_SQL_IDENTIFIER}\Z")
_SQL_SELECT_RE = re.compile(
    rf"SELECT\s+(?:\*|{_SQL_IDENTIFIER}(?:\s*,\s*{_SQL_IDENTIFIER})*)"
    rf"\s+FROM\s+{_SQL_IDENTIFIER}\Z",
    re.IGNORECASE,
)
_GLOB_RE = re.compile(r"[A-Za-z0-9*?_.\[\]-]{1,128}\Z")
_PHYSICAL_PATH_FRAGMENT_RE = re.compile(r"(?:^|[\s(=])(?:/[^\s]+|[A-Za-z]:[\\/][^\s]*|\\\\[^\s]+)")
_LINEAGE_SOURCE_PIPELINE_LIMIT = 256
_LINEAGE_SOURCE_NODE_LIMIT = 256
_LINEAGE_SOURCE_EDGE_LIMIT = 4096
_LINEAGE_NODE_LIMIT = 64
_LINEAGE_EDGE_LIMIT = 128
_LINEAGE_IDENTIFIER_LENGTH_LIMIT = 128
_LINEAGE_RESPONSE_BYTE_LIMIT = 64 * 1024


class RuntimeAccessDenied(Exception):
    """A sanitized remote runtime-data access denial."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class RemotePatternRenderProjection:
    """Nominal authority to render the fixed remote-safe fact template."""


_REMOTE_PATTERN_RENDER_ARGUMENTS = (
    ("pattern", "fact"),
    ("table_name", "fact_pipeline"),
    ("connection", "local_input"),
    ("source_path", "input.csv"),
)


def prepare_remote_pattern_render(
    args: tuple[object, ...], caller_kwargs: Dict[str, object]
) -> RemotePatternRenderProjection:
    """Return a data-free projection only for the exact reviewed remote template."""
    if args or len(caller_kwargs) != len(_REMOTE_PATTERN_RENDER_ARGUMENTS):
        raise RuntimeAccessDenied("REMOTE_RENDER_PROJECTION_REQUIRED")
    for name, expected in _REMOTE_PATTERN_RENDER_ARGUMENTS:
        if name not in caller_kwargs:
            raise RuntimeAccessDenied("REMOTE_RENDER_PROJECTION_REQUIRED")
        value = caller_kwargs[name]
        if type(value) is not str or value != expected:
            raise RuntimeAccessDenied("REMOTE_RENDER_PROJECTION_REQUIRED")
    return RemotePatternRenderProjection()


@dataclass(frozen=True)
class LogicalLineageNode:
    """One validated logical node identifier from the managed inline snapshot."""

    id: str


@dataclass(frozen=True)
class LogicalLineageEdge:
    """One validated pipeline-internal dependency from the managed inline snapshot."""

    source: str
    target: str


@dataclass(frozen=True)
class RemoteLogicalLineageProjection:
    """Closed authority for one bounded remote logical-lineage response."""

    pipeline: str
    nodes: tuple[LogicalLineageNode, ...]
    edges: tuple[LogicalLineageEdge, ...]


@dataclass(frozen=True)
class PreparedRuntimeCall:
    """Validated helper arguments and operator-owned paths for one remote call."""

    action: str
    kwargs: Dict[str, object]
    project_root: Path
    config_path: Path
    config_snapshot: Dict[str, object] = field(repr=False)
    config_fingerprint: str = field(repr=False)
    output_path: Optional[Path] = None
    public_output_path: Optional[str] = None
    logical_lineage: Optional[RemoteLogicalLineageProjection] = None

    def validated_config_snapshot(self) -> Dict[str, object]:
        """Return an isolated copy only while it matches the validated snapshot."""
        snapshot = copy.deepcopy(self.config_snapshot)
        if _config_fingerprint(snapshot) != self.config_fingerprint:
            raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED")
        return snapshot


@dataclass(frozen=True)
class ManagedProjectAccess:
    """Remote-only project, path, and export contract for runtime-data actions."""

    project: Optional[str]
    project_root: Optional[str | Path]
    config_path: Optional[str | Path]
    export_root: Optional[str | Path] = None

    @classmethod
    def from_environment(cls) -> "ManagedProjectAccess":
        """Build the operator-owned contract without accepting caller roots."""
        return cls(
            project=os.environ.get("ODIBI_MCP_PROJECT"),
            project_root=os.environ.get("ODIBI_MCP_PROJECT_ROOT"),
            config_path=os.environ.get("ODIBI_CONFIG"),
            export_root=os.environ.get("ODIBI_MCP_EXPORT_ROOT"),
        )

    def prepare(self, action: str, caller_kwargs: Dict[str, object]) -> PreparedRuntimeCall:
        """Validate and normalize a remote call before helper import or effects."""
        if action not in RUNTIME_DATA_ACTIONS:
            raise RuntimeAccessDenied("RUNTIME_SCOPE_UNSUPPORTED")

        kwargs = dict(caller_kwargs)
        caller_project = kwargs.pop("project", None)
        if (
            not _is_safe_component(caller_project)
            or not _is_safe_component(self.project)
            or caller_project != self.project
        ):
            raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED")

        project_root = _canonical_directory(self.project_root, "PROJECT_SCOPE_REQUIRED")
        config_path = _canonical_file(self.config_path, "PROJECT_SCOPE_REQUIRED")
        if not _is_contained(config_path, project_root):
            raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED")

        prepared_kwargs = self._prepare_action_inputs(action, kwargs, project_root)
        if action == "diagnose":
            raise RuntimeAccessDenied("PHYSICAL_REFERENCES_DISABLED")

        config = _load_project_config(config_path)
        if config.get("project") != self.project:
            raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED")

        logical_lineage = None
        if action == "lineage_graph":
            pipeline = prepared_kwargs["pipeline"]
            assert isinstance(pipeline, str)
            logical_lineage = _prepare_remote_logical_lineage(config, pipeline)
        else:
            self._validate_config_membership(
                action,
                prepared_kwargs,
                config,
                project_root,
                config_path,
            )

        output_path = None
        public_output_path = None
        if action in _DOWNLOAD_ACTIONS:
            export_root = _canonical_directory(self.export_root, "EXPORT_SCOPE_REQUIRED")
            if not _is_contained(export_root, project_root):
                raise RuntimeAccessDenied("EXPORT_SCOPE_REQUIRED")
            filename = prepared_kwargs.pop("filename")
            assert isinstance(filename, str)
            output_path = (export_root / filename).resolve(strict=False)
            if (
                output_path.parent != export_root
                or output_path.exists()
                or output_path.is_symlink()
            ):
                raise RuntimeAccessDenied("EXPORT_SCOPE_REQUIRED")
            prepared_kwargs["output_path"] = str(output_path)
            public_output_path = output_path.relative_to(project_root).as_posix()

        config_snapshot = copy.deepcopy(config)
        return PreparedRuntimeCall(
            action=action,
            kwargs=prepared_kwargs,
            project_root=project_root,
            config_path=config_path,
            config_snapshot=config_snapshot,
            config_fingerprint=_config_fingerprint(config_snapshot),
            output_path=output_path,
            public_output_path=public_output_path,
            logical_lineage=logical_lineage,
        )

    def _prepare_action_inputs(
        self,
        action: str,
        kwargs: Dict[str, object],
        project_root: Path,
    ) -> Dict[str, object]:
        allowed = {
            "map_environment": {"connection", "path", "pattern", "limit"},
            "profile_source": {"connection", "path", "max_rows"},
            "profile_folder": {"connection", "folder_path", "pattern", "max_files"},
            "suggest_pipeline": {"source_path", "connection", "intent"},
            "story_read": {"pipeline", "run_id"},
            "node_sample": {"pipeline", "node", "limit"},
            "node_failed_rows": {"pipeline", "node", "limit"},
            "lineage_graph": {"pipeline"},
            "diagnose": set(),
            "download_sql": {"connection", "query", "filename", "limit"},
            "download_table": {"connection", "table", "filename", "limit"},
            "download_file": {"connection", "source_path", "filename"},
        }[action]
        if set(kwargs) - allowed:
            raise RuntimeAccessDenied("INVALID_RUNTIME_ARGUMENT")

        prepared = dict(kwargs)
        if action in _CONNECTION_ACTIONS:
            connection = prepared.get("connection")
            if not _is_safe_component(connection):
                raise RuntimeAccessDenied("INVALID_RUNTIME_ARGUMENT")

        if action in _PIPELINE_ACTIONS:
            pipeline = prepared.get("pipeline")
            if not _is_safe_component(pipeline):
                raise RuntimeAccessDenied("INVALID_RUNTIME_ARGUMENT")
        if action in {"node_sample", "node_failed_rows"}:
            if not _is_safe_component(prepared.get("node")):
                raise RuntimeAccessDenied("INVALID_RUNTIME_ARGUMENT")
            prepared["limit"] = _positive_int(prepared.get("limit", 10), 100)
        if action == "story_read":
            run_id = prepared.get("run_id")
            if run_id is not None and not _is_safe_component(run_id):
                raise RuntimeAccessDenied("INVALID_RUNTIME_ARGUMENT")

        if action == "map_environment":
            path = prepared.get("path", "")
            if not isinstance(path, str):
                raise RuntimeAccessDenied("INVALID_RUNTIME_ARGUMENT")
            prepared["path"] = _relative_path(path, project_root) if path else ""
            pattern = prepared.get("pattern", "")
            if pattern and (not isinstance(pattern, str) or _GLOB_RE.fullmatch(pattern) is None):
                raise RuntimeAccessDenied("INVALID_RUNTIME_ARGUMENT")
            if not isinstance(pattern, str):
                raise RuntimeAccessDenied("INVALID_RUNTIME_ARGUMENT")
            prepared["pattern"] = pattern
            prepared["limit"] = _positive_int(prepared.get("limit", 100), 100)
        elif action == "profile_source":
            path = prepared.get("path")
            if not isinstance(path, str):
                raise RuntimeAccessDenied("INVALID_RUNTIME_ARGUMENT")
            prepared["path"] = _relative_path(path, project_root)
            prepared["max_rows"] = _positive_int(prepared.get("max_rows", 100), 100)
        elif action == "profile_folder":
            folder_path = prepared.get("folder_path")
            if not isinstance(folder_path, str):
                raise RuntimeAccessDenied("INVALID_RUNTIME_ARGUMENT")
            prepared["folder_path"] = _relative_path(folder_path, project_root)
            pattern = prepared.get("pattern", "*")
            if not isinstance(pattern, str) or _GLOB_RE.fullmatch(pattern) is None:
                raise RuntimeAccessDenied("INVALID_RUNTIME_ARGUMENT")
            prepared["pattern"] = pattern
            prepared["max_files"] = _positive_int(prepared.get("max_files", 20), 50)
        elif action == "suggest_pipeline":
            source_path = prepared.get("source_path")
            if not isinstance(source_path, str):
                raise RuntimeAccessDenied("INVALID_RUNTIME_ARGUMENT")
            prepared["source_path"] = _relative_path(source_path, project_root)
            intent = prepared.get("intent")
            if (
                not isinstance(intent, str)
                or not intent.strip()
                or len(intent) > 500
                or any(ord(character) < 32 for character in intent)
            ):
                raise RuntimeAccessDenied("INVALID_RUNTIME_ARGUMENT")
            prepared["intent"] = intent.strip()
        elif action == "download_sql":
            query = prepared.get("query")
            if not _is_read_only_query(query):
                raise RuntimeAccessDenied("INVALID_RUNTIME_ARGUMENT")
            assert isinstance(query, str)
            prepared["query"] = query.strip()
            prepared["limit"] = _positive_int(prepared.get("limit", 1000), 1000)
            prepared["filename"] = _download_filename(prepared.get("filename"))
        elif action == "download_table":
            table = prepared.get("table")
            if not isinstance(table, str) or _SQL_IDENTIFIER_RE.fullmatch(table) is None:
                raise RuntimeAccessDenied("INVALID_RUNTIME_ARGUMENT")
            prepared["limit"] = _positive_int(prepared.get("limit", 1000), 1000)
            prepared["filename"] = _download_filename(prepared.get("filename"))
        elif action == "download_file":
            source_path = prepared.get("source_path")
            if not isinstance(source_path, str):
                raise RuntimeAccessDenied("INVALID_RUNTIME_ARGUMENT")
            source_path = _relative_path(source_path, project_root)
            filename = _download_filename(prepared.get("filename"))
            if Path(source_path).suffix.lower() != Path(filename).suffix.lower():
                raise RuntimeAccessDenied("INVALID_RUNTIME_ARGUMENT")
            prepared["source_path"] = source_path
            prepared["filename"] = filename

        return prepared

    def _validate_config_membership(
        self,
        action: str,
        kwargs: Dict[str, object],
        config: Dict[str, object],
        project_root: Path,
        config_path: Path,
    ) -> None:
        connections = config.get("connections")
        if not isinstance(connections, dict):
            raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED")

        if action in _CONNECTION_ACTIONS:
            connection = kwargs["connection"]
            if connection not in connections or not isinstance(connections[connection], dict):
                raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED")
            connection_config = connections[connection]
            connection_type = str(connection_config.get("type", "local")).lower()
            if connection_type not in _SQL_CONNECTION_TYPES | _STORAGE_CONNECTION_TYPES:
                raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED")
            if action in {"download_sql", "download_table"} and (
                connection_type not in _SQL_CONNECTION_TYPES
            ):
                raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED")
            if action in {"profile_folder", "download_file"} and (
                connection_type not in _STORAGE_CONNECTION_TYPES
            ):
                raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED")
            path_key = "path" if action in {"map_environment", "profile_source"} else None
            if action == "suggest_pipeline":
                path_key = "source_path"
            if action in {"profile_folder", "download_file"}:
                path_key = "folder_path" if action == "profile_folder" else "source_path"
            if path_key is not None:
                value = kwargs[path_key]
                assert isinstance(value, str)
                if connection_type in _SQL_CONNECTION_TYPES:
                    if value and _SQL_IDENTIFIER_RE.fullmatch(value) is None:
                        raise RuntimeAccessDenied("INVALID_RUNTIME_ARGUMENT")
                else:
                    base = _local_connection_base(
                        connection_config,
                        project_root,
                        config_path,
                    )
                    kwargs[path_key] = _relative_path(value, base or project_root) if value else ""

        if action in _STORY_ACTIONS:
            pipeline = kwargs["pipeline"]
            pipelines = config.get("pipelines")
            if not isinstance(pipelines, list) or not any(
                isinstance(item, dict) and item.get("pipeline", item.get("name")) == pipeline
                for item in pipelines
            ):
                raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED")
            story = config.get("story")
            if not isinstance(story, dict):
                raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED")
            story_connection = story.get("connection")
            story_path = story.get("path", "stories")
            if story_connection not in connections or not isinstance(story_path, str):
                raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED")
            connection_config = connections[story_connection]
            if not isinstance(connection_config, dict):
                raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED")
            connection_type = str(connection_config.get("type", "local")).lower()
            if connection_type not in _STORAGE_CONNECTION_TYPES:
                raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED")
            base = _local_connection_base(connection_config, project_root, config_path)
            if base is None:
                if not _is_safe_component(connection_config.get("container")):
                    raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED")
                _cloud_relative_path(connection_config.get("path_prefix"))
                _cloud_relative_path(story_path)
            else:
                _configured_local_path(story_path, base, project_root)


def sanitize_runtime_result(value: object, prepared: PreparedRuntimeCall) -> object:
    """Remove host details and attach explicit policy metadata to a remote result."""
    sanitized = _sanitize_runtime_value(value)
    if not isinstance(sanitized, dict):
        if prepared.action == "map_environment":
            raise RuntimeAccessDenied("RUNTIME_DATA_UNAVAILABLE")
        return sanitized

    if prepared.public_output_path is not None and "output_path" in sanitized:
        sanitized["output_path"] = prepared.public_output_path
    policy = sanitized.get("policy_applied")
    if not isinstance(policy, dict):
        policy = {}
    policy["project_scoped"] = True

    if prepared.action == "map_environment":
        limit = prepared.kwargs.get("limit")
        if type(limit) is not int:
            raise RuntimeAccessDenied("PRIVACY_LIMIT_REQUIRED")
        truncated = _cap_map_environment_result(sanitized, limit)
        sanitized["truncated"] = truncated
        sanitized["truncated_reason"] = "enumeration_limit" if truncated else None
        policy["enumeration_capped"] = truncated
        policy["enumeration_limit"] = limit
    elif prepared.action == "profile_source":
        sanitized["truncated"] = True
        sanitized["truncated_reason"] = "sampling_only"
        policy["sample_capped"] = True
    elif prepared.action == "suggest_pipeline":
        policy["sample_capped"] = True
    elif prepared.action == "profile_folder":
        total = sanitized.get("total_files")
        profiled = sanitized.get("profiled_count")
        truncated = isinstance(total, int) and isinstance(profiled, int) and total > profiled
        sanitized["truncated"] = truncated
        sanitized["truncated_reason"] = "file_limit" if truncated else None
        policy["sample_capped"] = truncated

    sanitized["policy_applied"] = policy
    return sanitized


def render_remote_logical_lineage_projection(
    projection: RemoteLogicalLineageProjection,
) -> Dict[str, object]:
    """Render only the closed logical identifiers carried by a nominal projection."""
    if type(projection) is not RemoteLogicalLineageProjection:
        raise RuntimeAccessDenied("LOGICAL_PROJECTION_UNAVAILABLE")
    if not _is_safe_component(projection.pipeline) or type(projection.nodes) is not tuple:
        raise RuntimeAccessDenied("LOGICAL_PROJECTION_UNAVAILABLE")
    if type(projection.edges) is not tuple:
        raise RuntimeAccessDenied("LOGICAL_PROJECTION_UNAVAILABLE")
    if len(projection.nodes) > _LINEAGE_SOURCE_NODE_LIMIT:
        raise RuntimeAccessDenied("LOGICAL_PROJECTION_UNAVAILABLE")
    if len(projection.edges) > _LINEAGE_SOURCE_EDGE_LIMIT:
        raise RuntimeAccessDenied("LOGICAL_PROJECTION_UNAVAILABLE")

    node_ids: list[str] = []
    node_id_set: set[str] = set()
    for node in projection.nodes:
        if type(node) is not LogicalLineageNode or not _is_safe_component(node.id):
            raise RuntimeAccessDenied("LOGICAL_PROJECTION_UNAVAILABLE")
        if node.id in node_id_set:
            raise RuntimeAccessDenied("LOGICAL_PROJECTION_UNAVAILABLE")
        node_ids.append(node.id)
        node_id_set.add(node.id)

    edge_pair_set: set[tuple[str, str]] = set()
    for edge in projection.edges:
        if (
            type(edge) is not LogicalLineageEdge
            or not _is_safe_component(edge.source)
            or not _is_safe_component(edge.target)
            or edge.source not in node_id_set
            or edge.target not in node_id_set
        ):
            raise RuntimeAccessDenied("LOGICAL_PROJECTION_UNAVAILABLE")
        pair = (edge.source, edge.target)
        if pair in edge_pair_set:
            raise RuntimeAccessDenied("LOGICAL_PROJECTION_UNAVAILABLE")
        edge_pair_set.add(pair)

    returned_node_ids = node_ids[:_LINEAGE_NODE_LIMIT]
    returned_node_set = set(returned_node_ids)
    eligible_edges = [
        edge
        for edge in projection.edges
        if edge.source in returned_node_set and edge.target in returned_node_set
    ]
    returned_edges = eligible_edges[:_LINEAGE_EDGE_LIMIT]
    nodes_truncated = len(returned_node_ids) < len(projection.nodes)
    edges_truncated = len(returned_edges) < len(projection.edges)

    result: Dict[str, object] = {
        "kind": "logical_lineage_graph",
        "pipeline": projection.pipeline,
        "status": "configured",
        "nodes": [{"id": node_id, "type": "pipeline_node"} for node_id in returned_node_ids],
        "edges": [
            {"source": edge.source, "target": edge.target, "kind": "dependency"}
            for edge in returned_edges
        ],
        "counts": {
            "nodes_total": len(projection.nodes),
            "nodes_returned": len(returned_node_ids),
            "edges_total": len(projection.edges),
            "edges_returned": len(returned_edges),
        },
        "truncated": nodes_truncated or edges_truncated,
        "truncation": {"nodes": nodes_truncated, "edges": edges_truncated},
        "policy_applied": {
            "project_scoped": True,
            "logical_only": True,
            "inline_snapshot_only": True,
            "node_limit": _LINEAGE_NODE_LIMIT,
            "edge_limit": _LINEAGE_EDGE_LIMIT,
            "identifier_length_limit": _LINEAGE_IDENTIFIER_LENGTH_LIMIT,
            "response_byte_limit": _LINEAGE_RESPONSE_BYTE_LIMIT,
        },
    }
    if len(json.dumps(result, indent=2).encode("utf-8")) > _LINEAGE_RESPONSE_BYTE_LIMIT:
        raise RuntimeAccessDenied("LOGICAL_PROJECTION_UNAVAILABLE")
    return result


def _prepare_remote_logical_lineage(
    config: Dict[str, object], pipeline: str
) -> RemoteLogicalLineageProjection:
    """Project explicit inline node dependencies without resolving config imports."""
    if "imports" in config:
        raise RuntimeAccessDenied("LOGICAL_PROJECTION_UNAVAILABLE")
    pipelines = config.get("pipelines")
    if not isinstance(pipelines, list) or len(pipelines) > _LINEAGE_SOURCE_PIPELINE_LIMIT:
        raise RuntimeAccessDenied("LOGICAL_PROJECTION_UNAVAILABLE")

    matches: list[Dict[str, object]] = []
    for item in pipelines:
        if not isinstance(item, dict):
            raise RuntimeAccessDenied("LOGICAL_PROJECTION_UNAVAILABLE")
        identifier = item.get("pipeline", item.get("name"))
        if not _is_safe_component(identifier):
            raise RuntimeAccessDenied("LOGICAL_PROJECTION_UNAVAILABLE")
        if identifier == pipeline:
            matches.append(item)
    if len(matches) != 1:
        raise RuntimeAccessDenied("LOGICAL_PROJECTION_UNAVAILABLE")

    raw_nodes = matches[0].get("nodes")
    if not isinstance(raw_nodes, list) or len(raw_nodes) > _LINEAGE_SOURCE_NODE_LIMIT:
        raise RuntimeAccessDenied("LOGICAL_PROJECTION_UNAVAILABLE")

    nodes: list[LogicalLineageNode] = []
    raw_node_mappings: list[Dict[str, object]] = []
    node_ids: set[str] = set()
    for raw_node in raw_nodes:
        if not isinstance(raw_node, dict):
            raise RuntimeAccessDenied("LOGICAL_PROJECTION_UNAVAILABLE")
        node_id = raw_node.get("name")
        if not _is_safe_component(node_id) or node_id in node_ids:
            raise RuntimeAccessDenied("LOGICAL_PROJECTION_UNAVAILABLE")
        assert isinstance(node_id, str)
        node_ids.add(node_id)
        nodes.append(LogicalLineageNode(id=node_id))
        raw_node_mappings.append(raw_node)

    edges: list[LogicalLineageEdge] = []
    edge_pairs: set[tuple[str, str]] = set()
    dependency_count = 0
    for node, raw_node in zip(nodes, raw_node_mappings):
        dependencies = raw_node.get("depends_on", [])
        if not isinstance(dependencies, list):
            raise RuntimeAccessDenied("LOGICAL_PROJECTION_UNAVAILABLE")
        dependency_count += len(dependencies)
        if dependency_count > _LINEAGE_SOURCE_EDGE_LIMIT:
            raise RuntimeAccessDenied("LOGICAL_PROJECTION_UNAVAILABLE")
        for dependency in dependencies:
            if not _is_safe_component(dependency) or dependency not in node_ids:
                raise RuntimeAccessDenied("LOGICAL_PROJECTION_UNAVAILABLE")
            assert isinstance(dependency, str)
            pair = (dependency, node.id)
            if pair in edge_pairs:
                raise RuntimeAccessDenied("LOGICAL_PROJECTION_UNAVAILABLE")
            edge_pairs.add(pair)
            edges.append(LogicalLineageEdge(source=dependency, target=node.id))

    return RemoteLogicalLineageProjection(
        pipeline=pipeline,
        nodes=tuple(nodes),
        edges=tuple(edges),
    )


def _canonical_directory(value: object, code: str) -> Path:
    if not isinstance(value, (str, Path)) or not str(value) or not Path(value).is_absolute():
        raise RuntimeAccessDenied(code)
    try:
        path = Path(value).resolve(strict=True)
    except (OSError, RuntimeError):
        raise RuntimeAccessDenied(code) from None
    if not path.is_dir():
        raise RuntimeAccessDenied(code)
    return path


def _canonical_file(value: object, code: str) -> Path:
    if not isinstance(value, (str, Path)) or not str(value) or not Path(value).is_absolute():
        raise RuntimeAccessDenied(code)
    try:
        path = Path(value).resolve(strict=True)
    except (OSError, RuntimeError):
        raise RuntimeAccessDenied(code) from None
    if not path.is_file():
        raise RuntimeAccessDenied(code)
    return path


def _is_contained(candidate: Path, root: Path) -> bool:
    return candidate == root or root in candidate.parents


def _is_safe_component(value: object) -> bool:
    return isinstance(value, str) and _COMPONENT_RE.fullmatch(value) is not None


def _positive_int(value: object, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise RuntimeAccessDenied("PRIVACY_LIMIT_REQUIRED")
    return value


def _relative_path(value: str, root: Path) -> str:
    if (
        not value
        or len(value) > 1024
        or any(ord(character) < 32 for character in value)
        or "%" in value
        or "\\" in value
        or "//" in value
        or ":" in value
        or PurePosixPath(value).is_absolute()
        or PureWindowsPath(value).drive
        or PureWindowsPath(value).root
    ):
        raise RuntimeAccessDenied("PATH_SCOPE_REQUIRED")
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise RuntimeAccessDenied("PATH_SCOPE_REQUIRED")
    candidate = (root / value).resolve(strict=False)
    if not _is_contained(candidate, root):
        raise RuntimeAccessDenied("PATH_SCOPE_REQUIRED")
    return "/".join(parts)


def _cloud_relative_path(value: object) -> str:
    """Validate an operator-configured path without treating a cloud prefix as local."""
    if not isinstance(value, str):
        raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED")
    if (
        not value
        or len(value) > 1024
        or any(ord(character) < 32 for character in value)
        or "%" in value
        or "\\" in value
        or "//" in value
        or ":" in value
        or PurePosixPath(value).is_absolute()
        or PureWindowsPath(value).drive
        or PureWindowsPath(value).root
    ):
        raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED")
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED")
    return "/".join(parts)


def _configured_local_path(value: str, base: Path, project_root: Path) -> Path:
    """Resolve an operator-owned config path while keeping it inside the project."""
    if not value or "\x00" in value:
        raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED")
    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = base / candidate
    try:
        candidate = candidate.resolve(strict=False)
    except (OSError, RuntimeError):
        raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED") from None
    if not _is_contained(candidate, project_root) or (
        candidate.exists() and not candidate.is_dir()
    ):
        raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED")
    return candidate


def _download_filename(value: object) -> str:
    if not _is_safe_component(value):
        raise RuntimeAccessDenied("INVALID_RUNTIME_ARGUMENT")
    assert isinstance(value, str)
    if Path(value).suffix.lower() not in _SAFE_DOWNLOAD_SUFFIXES:
        raise RuntimeAccessDenied("DOWNLOAD_FORMAT_REQUIRED")
    return value


def _is_read_only_query(value: object) -> bool:
    if not isinstance(value, str) or not value.strip() or len(value) > 10_000:
        return False
    query = value.strip()
    return _SQL_SELECT_RE.fullmatch(query) is not None


def _load_project_config(config_path: Path) -> Dict[str, object]:
    try:
        with config_path.open(encoding="utf-8") as stream:
            config = yaml.safe_load(stream)
    except (OSError, RuntimeError, ValueError, yaml.YAMLError):
        raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED") from None
    if not isinstance(config, dict):
        raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED")
    return config


def _config_fingerprint(config: Dict[str, object]) -> str:
    payload = yaml.safe_dump(config, sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _local_connection_base(
    connection_config: Dict[str, object],
    project_root: Path,
    config_path: Path,
) -> Optional[Path]:
    if str(connection_config.get("type", "local")).lower() != "local":
        return None
    raw_base = connection_config.get("base_path", ".")
    if not isinstance(raw_base, str) or not raw_base or "\x00" in raw_base or "://" in raw_base:
        raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED")
    base = Path(raw_base)
    if not base.is_absolute():
        base = config_path.parent / base
    try:
        base = base.resolve(strict=False)
    except (OSError, RuntimeError):
        raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED") from None
    if not _is_contained(base, project_root) or (base.exists() and not base.is_dir()):
        raise RuntimeAccessDenied("PROJECT_SCOPE_REQUIRED")
    return base


def _sanitize_runtime_value(value: object, key: Optional[str] = None) -> object:
    if (
        key in {"error", "error_message", "errors"}
        and value is not None
        and value != ""
        and value != []
    ):
        return (
            ["Runtime data is unavailable."]
            if isinstance(value, list)
            else "Runtime data is unavailable."
        )
    if key == "query":
        return "[redacted]"
    if key is not None and key.lower() in _SENSITIVE_RESPONSE_KEYS:
        return "[redacted]"
    if isinstance(value, dict):
        return {
            item_key: _sanitize_runtime_value(item, item_key) for item_key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_sanitize_runtime_value(item) for item in value]
    if isinstance(value, str) and (
        "://" in value
        or PurePosixPath(value).is_absolute()
        or bool(PureWindowsPath(value).drive)
        or bool(PureWindowsPath(value).root)
        or _PHYSICAL_PATH_FRAGMENT_RE.search(value) is not None
    ):
        return "[physical reference withheld]"
    return value


def _cap_map_environment_result(result: Dict[str, object], limit: int) -> bool:
    """Defensively cap every final map identifier collection."""
    truncated = False
    for key in (
        "structure",
        "suggested_sources",
        "suggestions",
        "recommendations",
        "ready_for",
    ):
        collection = result.get(key)
        if isinstance(collection, list) and len(collection) > limit:
            result[key] = collection[:limit]
            truncated = True

    structure = result.get("structure")
    if isinstance(structure, list):
        for item in structure:
            if not isinstance(item, dict):
                continue
            for key in ("sample_tables", "sample_files"):
                identifiers = item.get(key)
                if isinstance(identifiers, list) and len(identifiers) > limit:
                    item[key] = identifiers[:limit]
                    truncated = True

    ready_for = result.get("ready_for")
    if isinstance(ready_for, dict):
        for key, identifiers in ready_for.items():
            if isinstance(identifiers, list) and len(identifiers) > limit:
                ready_for[key] = identifiers[:limit]
                truncated = True

    if result.get("truncated") is True:
        truncated = True
    return truncated


class ConnectionPolicy(BaseModel):
    """
    Per-connection access policy. Deny-by-default for path discovery.
    Matches MCP spec.
    """

    connection: str
    allowed_path_prefixes: List[str] = Field(default_factory=list)
    denied_path_prefixes: List[str] = Field(default_factory=list)
    explicit_allow_all: bool = False
    max_depth: int = 5
    allow_physical_refs: bool = False

    @field_validator("allowed_path_prefixes", mode="after")
    def validate_path_access(cls, v, info):
        explicit_allow_all = info.data.get("explicit_allow_all", False)
        if not v and not explicit_allow_all:
            # Valid state - will deny all path-based discovery
            pass
        return v

    def is_path_allowed(self, path: str) -> bool:
        if any(path.startswith(prefix) for prefix in self.denied_path_prefixes):
            return False
        if self.explicit_allow_all:
            return True
        if not self.allowed_path_prefixes:
            return False
        return any(path.startswith(prefix) for prefix in self.allowed_path_prefixes)


class AccessContext(BaseModel):
    """
    Unified access enforcement context. Injected once, enforced everywhere.
    Matches MCP spec.
    """

    authorized_projects: Set[str]
    environment: str = "production"
    connection_policies: Dict[str, ConnectionPolicy] = Field(default_factory=dict)
    physical_refs_enabled: bool = False

    def check_project(self, project: str) -> None:
        if project not in self.authorized_projects:
            raise PermissionError(f"Access denied: project '{project}' not authorized")

    def check_connection(self, connection: str) -> ConnectionPolicy:
        if connection not in self.connection_policies:
            raise PermissionError(f"Access denied: connection '{connection}' not configured")
        return self.connection_policies[connection]

    def check_path(self, connection: str, path: str) -> None:
        policy = self.check_connection(connection)
        if not policy.is_path_allowed(path):
            raise PermissionError(
                f"Access denied: path '{path}' not allowed for connection '{connection}'"
            )

    def can_include_physical(self, connection: str) -> bool:
        if not self.physical_refs_enabled:
            return False
        policy = self.connection_policies.get(connection)
        return policy is not None and policy.allow_physical_refs
