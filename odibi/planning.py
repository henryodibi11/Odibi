"""Pure, bounded logical planning for caller-supplied Odibi YAML.

This module intentionally depends only on the Python standard library and
PyYAML. It does not import Odibi's runtime configuration, registries, engines,
connections, extensions, or execution graph. Planning owns only in-memory
parser state and returns immutable logical data transfer objects.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import math
import re
from types import MappingProxyType
from typing import Dict, FrozenSet, List, Literal, Mapping, Optional, Set, Tuple
import unicodedata

import yaml
from yaml.events import (
    AliasEvent,
    DocumentStartEvent,
    MappingEndEvent,
    MappingStartEvent,
    ScalarEvent,
    SequenceEndEvent,
    SequenceStartEvent,
)
from yaml.nodes import MappingNode, ScalarNode
from yaml.resolver import BaseResolver


@dataclass(frozen=True, init=False)
class PlanningLimits:
    """Fixed structural limits for one immutable planning operation.

    The explicit slots and initializer preserve the frozen, slotted contract on
    every supported Python version, including Python 3.9.
    """

    __slots__ = (
        "max_input_bytes",
        "max_yaml_depth",
        "max_yaml_nodes",
        "max_aliases",
        "max_pipelines",
        "max_nodes_per_pipeline",
        "max_nodes_total",
        "max_edges_total",
        "max_diagnostics",
        "max_identifier_codepoints",
        "max_emitted_string_codepoints",
        "max_response_bytes",
    )

    max_input_bytes: int
    max_yaml_depth: int
    max_yaml_nodes: int
    max_aliases: int
    max_pipelines: int
    max_nodes_per_pipeline: int
    max_nodes_total: int
    max_edges_total: int
    max_diagnostics: int
    max_identifier_codepoints: int
    max_emitted_string_codepoints: int
    max_response_bytes: int

    def __init__(
        self,
        max_input_bytes: int = 1_048_576,
        max_yaml_depth: int = 32,
        max_yaml_nodes: int = 20_000,
        max_aliases: int = 64,
        max_pipelines: int = 128,
        max_nodes_per_pipeline: int = 1_024,
        max_nodes_total: int = 4_096,
        max_edges_total: int = 16_384,
        max_diagnostics: int = 256,
        max_identifier_codepoints: int = 128,
        max_emitted_string_codepoints: int = 512,
        max_response_bytes: int = 2_097_152,
    ) -> None:
        """Initialize caller-tightenable immutable ceilings."""
        values = (
            max_input_bytes,
            max_yaml_depth,
            max_yaml_nodes,
            max_aliases,
            max_pipelines,
            max_nodes_per_pipeline,
            max_nodes_total,
            max_edges_total,
            max_diagnostics,
            max_identifier_codepoints,
            max_emitted_string_codepoints,
            max_response_bytes,
        )
        for name, value in zip(self.__slots__, values):
            object.__setattr__(self, name, value)


DEFAULT_PLANNING_LIMITS = PlanningLimits()


def _tuples_to_lists(value: object) -> object:
    """Recursively convert planner-owned tuple containers to JSON arrays."""
    if isinstance(value, tuple):
        return [_tuples_to_lists(item) for item in value]
    if isinstance(value, list):
        return [_tuples_to_lists(item) for item in value]
    if isinstance(value, dict):
        return {key: _tuples_to_lists(item) for key, item in value.items()}
    return value


@dataclass(frozen=True)
class DiagnosticSubject:
    """Logical location of a diagnostic; never a filesystem location."""

    __slots__ = ("kind", "name")

    kind: Literal["document", "project", "pipeline", "node"]
    name: Optional[str]


@dataclass(frozen=True)
class PlanningDiagnostic:
    """Bounded diagnostic selected from a fixed, non-reflective catalog."""

    __slots__ = ("code", "severity", "category", "subject", "message")

    code: str
    severity: Literal["error", "warning"]
    category: Literal["syntax", "schema", "limit", "unresolved", "internal"]
    subject: DiagnosticSubject
    message: str


@dataclass(frozen=True)
class LogicalEdge:
    """A logical dependency edge between named nodes."""

    __slots__ = ("source", "target")

    source: str
    target: str


@dataclass(frozen=True)
class LogicalNodePlan:
    """A logical node projection without paths, provider config, or parameters."""

    __slots__ = ("name", "kind", "depends_on", "resolution")

    name: str
    kind: Literal["read", "transform", "write", "unknown"]
    depends_on: Tuple[str, ...]
    resolution: Literal["resolved", "unresolved"]


@dataclass(frozen=True)
class LogicalPipelinePlan:
    """A bounded logical DAG for one declared pipeline."""

    __slots__ = ("name", "nodes", "edges")

    name: str
    nodes: Tuple[LogicalNodePlan, ...]
    edges: Tuple[LogicalEdge, ...]


@dataclass(frozen=True)
class LogicalProjectPlan:
    """A logical project projection safe to cross package/CLI/MCP boundaries."""

    __slots__ = ("name", "pipelines")

    name: Optional[str]
    pipelines: Tuple[LogicalPipelinePlan, ...]


@dataclass(frozen=True)
class PlanningResponse:
    """Versioned immutable planner response shared by every adapter."""

    __slots__ = ("schema_version", "status", "plan", "diagnostics", "truncated")

    schema_version: Literal["1.0"]
    status: Literal["planned", "unresolved", "invalid"]
    plan: Optional[LogicalProjectPlan]
    diagnostics: Tuple[PlanningDiagnostic, ...]
    truncated: bool

    def to_dict(self) -> Dict[str, object]:
        """Return the exact JSON-compatible public response shape."""
        value = _tuples_to_lists(asdict(self))
        if not isinstance(value, dict):
            raise TypeError("PlanningResponse conversion must produce a dictionary")
        return value

    def to_json(self) -> str:
        """Return canonical UTF-8 JSON text with stable key and whitespace rules."""
        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )


_LIMIT_FIELDS: Tuple[str, ...] = PlanningLimits.__slots__
_DIAGNOSTIC_MESSAGES: Mapping[str, str] = MappingProxyType(
    {
        "INVALID_LIMITS": "Planning limits are invalid.",
        "INVALID_UTF8": "Input must be valid UTF-8.",
        "YAML_SYNTAX_INVALID": "YAML syntax is invalid.",
        "MULTIPLE_DOCUMENTS": "Exactly one YAML document is required.",
        "SCHEMA_INVALID": "The document does not match the immutable planning schema.",
        "REFERENCE_INVALID": "A logical dependency reference is invalid.",
        "DEPENDENCY_CYCLE": "The logical dependency graph contains a cycle.",
        "INPUT_LIMIT_EXCEEDED": "Input exceeds the immutable planning limit.",
        "STRUCTURE_LIMIT_EXCEEDED": "YAML structure exceeds an immutable planning limit.",
        "PLAN_LIMIT_EXCEEDED": "Logical plan exceeds an immutable planning limit.",
        "RESPONSE_LIMIT_EXCEEDED": "Serialized response exceeds the immutable planning limit.",
        "UNRESOLVED_EXTENSION_DEPENDENCY": (
            "Meaning depends on excluded extension or plugin code."
        ),
        "UNRESOLVED_PROVIDER_DEPENDENCY": (
            "Meaning depends on an excluded connection or provider."
        ),
        "UNRESOLVED_RUNTIME_DEPENDENCY": "Meaning depends on excluded runtime state.",
        "INTERNAL_PLANNING_FAILURE": "Immutable planning could not be completed.",
        "INTERNAL_SERIALIZATION_FAILURE": ("Immutable planning output could not be serialized."),
    }
)
_ALLOWED_YAML_TAGS: FrozenSet[str] = frozenset(
    {
        "tag:yaml.org,2002:null",
        "tag:yaml.org,2002:bool",
        "tag:yaml.org,2002:int",
        "tag:yaml.org,2002:float",
        "tag:yaml.org,2002:str",
        "tag:yaml.org,2002:seq",
        "tag:yaml.org,2002:map",
    }
)
_PROJECT_FIELDS: FrozenSet[str] = frozenset(
    {
        "project",
        "engine",
        "connections",
        "pipelines",
        "story",
        "system",
        "lineage",
        "description",
        "version",
        "owner",
        "vars",
        "retry",
        "logging",
        "alerts",
        "performance",
        "environments",
        "semantic",
        "imports",
    }
)
_PROJECT_STRING_FIELDS: FrozenSet[str] = frozenset({"description", "version", "owner"})
_PROJECT_MAPPING_FIELDS: FrozenSet[str] = frozenset(
    {
        "connections",
        "story",
        "system",
        "lineage",
        "vars",
        "retry",
        "logging",
        "performance",
        "environments",
        "semantic",
    }
)
_PROJECT_RUNTIME_FIELDS: FrozenSet[str] = frozenset(
    {
        "engine",
        "story",
        "system",
        "lineage",
        "vars",
        "retry",
        "logging",
        "alerts",
        "performance",
        "environments",
        "semantic",
    }
)
_PIPELINE_FIELDS: FrozenSet[str] = frozenset(
    {
        "pipeline",
        "description",
        "layer",
        "owner",
        "freshness_sla",
        "freshness_anchor",
        "nodes",
        "tags",
        "auto_cache_threshold",
    }
)
_PIPELINE_STRING_FIELDS: FrozenSet[str] = frozenset(
    {"description", "layer", "owner", "freshness_sla", "freshness_anchor"}
)
_NODE_FIELDS: FrozenSet[str] = frozenset(
    {
        "name",
        "description",
        "explanation",
        "explanation_file",
        "runbook_url",
        "enabled",
        "tags",
        "depends_on",
        "columns",
        "read",
        "inputs",
        "transform",
        "write",
        "streaming",
        "transformer",
        "params",
        "pre_sql",
        "post_sql",
        "materialized",
        "cache",
        "log_level",
        "on_error",
        "validation",
        "contracts",
        "schema_policy",
        "privacy",
        "sensitive",
        "source_yaml",
        "pattern",
        "extension",
        "plugin",
        "custom",
    }
)
_NODE_STRING_FIELDS: FrozenSet[str] = frozenset(
    {
        "description",
        "explanation",
        "explanation_file",
        "runbook_url",
        "transformer",
        "materialized",
        "log_level",
        "on_error",
        "source_yaml",
    }
)
_NODE_MAPPING_FIELDS: FrozenSet[str] = frozenset(
    {
        "columns",
        "read",
        "inputs",
        "transform",
        "write",
        "params",
        "validation",
        "schema_policy",
        "privacy",
    }
)
_NODE_STRING_LIST_FIELDS: FrozenSet[str] = frozenset({"tags", "depends_on", "pre_sql", "post_sql"})
_NODE_BOOLEAN_FIELDS: FrozenSet[str] = frozenset({"enabled", "streaming", "cache", "sensitive"})
_EXTENSION_FIELDS: FrozenSet[str] = frozenset(
    {"extension", "plugin", "custom", "pattern", "transformer"}
)
_RUNTIME_NODE_FIELDS: FrozenSet[str] = frozenset(
    {
        "transform",
        "streaming",
        "pre_sql",
        "post_sql",
        "materialized",
        "cache",
        "log_level",
        "on_error",
        "validation",
        "contracts",
        "schema_policy",
        "privacy",
        "sensitive",
        "source_yaml",
    }
)
_URI_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")
_SEVERITY_RANK: Mapping[str, int] = MappingProxyType({"error": 0, "warning": 1})


class _PlanningFailure(Exception):
    """Internal control flow carrying only a fixed diagnostic identity."""

    __slots__ = ("code", "category")

    def __init__(self, code: str, category: str) -> None:
        super().__init__(code)
        self.code = code
        self.category = category


class _BoundedSafeLoader(yaml.SafeLoader):
    """Planner-owned SafeLoader with merge-key and duplicate-key rejection."""

    def flatten_mapping(self, node: MappingNode) -> None:
        """Reject every YAML merge key before SafeLoader can apply it."""
        for key_node, _ in node.value:
            if isinstance(key_node, ScalarNode) and key_node.value == "<<":
                raise _PlanningFailure("SCHEMA_INVALID", "schema")
        super().flatten_mapping(node)


def _construct_unique_mapping(
    loader: _BoundedSafeLoader, node: MappingNode, deep: bool = False
) -> Dict[object, object]:
    """Construct one mapping while rejecting duplicates and complex keys."""
    loader.flatten_mapping(node)
    result: Dict[object, object] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if type(key) is not str or key in result:
            raise _PlanningFailure("SCHEMA_INVALID", "schema")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_BoundedSafeLoader.add_constructor(BaseResolver.DEFAULT_MAPPING_TAG, _construct_unique_mapping)


def _validate_limits(limits: PlanningLimits) -> PlanningLimits:
    """Accept only nominal limits that tighten every compiled ceiling."""
    if type(limits) is not PlanningLimits:
        raise _PlanningFailure("INVALID_LIMITS", "limit")
    for field_name in _LIMIT_FIELDS:
        value = getattr(limits, field_name)
        maximum = getattr(DEFAULT_PLANNING_LIMITS, field_name)
        if type(value) is not int or value <= 0 or value > maximum:
            raise _PlanningFailure("INVALID_LIMITS", "limit")
    return limits


def _validate_input(yaml_text: str, limits: PlanningLimits) -> str:
    """Validate exact text type, strict UTF-8 representability, and byte bound."""
    if type(yaml_text) is not str:
        raise _PlanningFailure("INVALID_UTF8", "syntax")
    byte_count = 0
    for character in yaml_text:
        codepoint = ord(character)
        if 0xD800 <= codepoint <= 0xDFFF:
            raise _PlanningFailure("INVALID_UTF8", "syntax")
        if codepoint <= 0x7F:
            width = 1
        elif codepoint <= 0x7FF:
            width = 2
        elif codepoint <= 0xFFFF:
            width = 3
        else:
            width = 4
        if byte_count > limits.max_input_bytes - width:
            raise _PlanningFailure("INPUT_LIMIT_EXCEEDED", "limit")
        byte_count += width
    return yaml_text


def _scan_yaml_events(yaml_text: str, limits: PlanningLimits) -> None:
    """Bound document count, depth, nodes, aliases, and explicit tags before load."""
    document_count = 0
    depth = 0
    node_count = 0
    alias_count = 0
    try:
        for event in yaml.parse(yaml_text, Loader=_BoundedSafeLoader):
            if isinstance(event, DocumentStartEvent):
                document_count += 1
                if document_count > 1:
                    raise _PlanningFailure("MULTIPLE_DOCUMENTS", "syntax")
            if isinstance(event, AliasEvent):
                alias_count += 1
                if alias_count > limits.max_aliases:
                    raise _PlanningFailure("STRUCTURE_LIMIT_EXCEEDED", "limit")
                continue
            if isinstance(event, (MappingStartEvent, SequenceStartEvent, ScalarEvent)):
                node_count += 1
                if node_count > limits.max_yaml_nodes:
                    raise _PlanningFailure("STRUCTURE_LIMIT_EXCEEDED", "limit")
                if event.tag is not None and event.tag not in _ALLOWED_YAML_TAGS:
                    raise _PlanningFailure("SCHEMA_INVALID", "schema")
            if isinstance(event, (MappingStartEvent, SequenceStartEvent)):
                depth += 1
                if depth > limits.max_yaml_depth:
                    raise _PlanningFailure("STRUCTURE_LIMIT_EXCEEDED", "limit")
            elif isinstance(event, (MappingEndEvent, SequenceEndEvent)):
                depth -= 1
    except _PlanningFailure:
        raise
    except yaml.YAMLError:
        raise _PlanningFailure("YAML_SYNTAX_INVALID", "syntax") from None
    if document_count != 1:
        raise _PlanningFailure("MULTIPLE_DOCUMENTS", "syntax")


def _validate_safe_graph(value: object) -> None:
    """Reject cycles and every constructed value outside safe JSON-like types."""
    active: Set[int] = set()
    visited: Set[int] = set()

    def visit(item: object) -> None:
        if item is None or type(item) in (str, bool, int):
            return
        if type(item) is float:
            if not math.isfinite(item):
                raise _PlanningFailure("SCHEMA_INVALID", "schema")
            return
        if type(item) not in (dict, list):
            raise _PlanningFailure("SCHEMA_INVALID", "schema")
        identity = id(item)
        if identity in active:
            raise _PlanningFailure("STRUCTURE_LIMIT_EXCEEDED", "limit")
        if identity in visited:
            return
        active.add(identity)
        if type(item) is dict:
            for key, child in item.items():
                if type(key) is not str:
                    raise _PlanningFailure("SCHEMA_INVALID", "schema")
                visit(child)
        else:
            for child in item:
                visit(child)
        active.remove(identity)
        visited.add(identity)

    visit(value)


def _load_one_bounded_document(yaml_text: str, limits: PlanningLimits) -> object:
    """Load one pre-scanned document with a local planner-owned SafeLoader."""
    _scan_yaml_events(yaml_text, limits)
    loader = _BoundedSafeLoader(yaml_text)
    try:
        parsed = loader.get_single_data()
    except _PlanningFailure:
        raise
    except yaml.YAMLError:
        raise _PlanningFailure("YAML_SYNTAX_INVALID", "syntax") from None
    finally:
        loader.dispose()
    _validate_safe_graph(parsed)
    return parsed


def _schema_failure() -> None:
    """Raise the one fixed closed-schema failure."""
    raise _PlanningFailure("SCHEMA_INVALID", "schema")


def _normalize_identifier(value: object, limits: PlanningLimits) -> str:
    """Validate and NFC-normalize one logical-only identifier."""
    if type(value) is not str:
        _schema_failure()
    normalized = unicodedata.normalize("NFC", value)
    if not normalized or len(normalized) > limits.max_identifier_codepoints:
        _schema_failure()
    if normalized in (".", "..") or "/" in normalized or "\\" in normalized:
        _schema_failure()
    if _URI_SCHEME.match(normalized):
        _schema_failure()
    if any(unicodedata.category(character).startswith("C") for character in normalized):
        _schema_failure()
    return normalized


def _require_optional_strings(document: Dict[str, object], fields: FrozenSet[str]) -> None:
    """Require each present metadata field to be a string or null."""
    for field_name in fields:
        if field_name in document and document[field_name] is not None:
            if type(document[field_name]) is not str:
                _schema_failure()


def _require_string_list(value: object) -> None:
    """Require an exact list containing only strings."""
    if type(value) is not list or any(type(item) is not str for item in value):
        _schema_failure()


def _validate_project_shape(document: Dict[str, object]) -> None:
    """Validate non-logical project fields without retaining their values."""
    if not set(document).issubset(_PROJECT_FIELDS):
        _schema_failure()
    _require_optional_strings(document, _PROJECT_STRING_FIELDS)
    if "engine" in document and document["engine"] is not None:
        if type(document["engine"]) is not str:
            _schema_failure()
    for field_name in _PROJECT_MAPPING_FIELDS:
        if field_name in document and document[field_name] is not None:
            if type(document[field_name]) is not dict:
                _schema_failure()
    if "alerts" in document and type(document["alerts"]) is not list:
        _schema_failure()
    if "imports" in document:
        imports = document["imports"]
        if type(imports) not in (str, list, dict):
            _schema_failure()


def _validate_pipeline_shape(pipeline: Dict[str, object]) -> None:
    """Validate the closed set of pipeline-level fields."""
    if not set(pipeline).issubset(_PIPELINE_FIELDS):
        _schema_failure()
    _require_optional_strings(pipeline, _PIPELINE_STRING_FIELDS)
    if "tags" in pipeline:
        _require_string_list(pipeline["tags"])
    if "auto_cache_threshold" in pipeline:
        threshold = pipeline["auto_cache_threshold"]
        if threshold is not None and type(threshold) is not int:
            _schema_failure()


def _validate_node_shape(node: Dict[str, object]) -> None:
    """Validate the planner-owned closed node vocabulary and coarse value types."""
    if not set(node).issubset(_NODE_FIELDS):
        _schema_failure()
    _require_optional_strings(node, _NODE_STRING_FIELDS)
    for field_name in _NODE_MAPPING_FIELDS:
        if field_name in node and node[field_name] is not None:
            if type(node[field_name]) is not dict:
                _schema_failure()
    for field_name in _NODE_STRING_LIST_FIELDS:
        if field_name in node:
            _require_string_list(node[field_name])
    for field_name in _NODE_BOOLEAN_FIELDS:
        if field_name in node and type(node[field_name]) is not bool:
            _schema_failure()
    if "contracts" in node and type(node["contracts"]) is not list:
        _schema_failure()
    for field_name in ("pattern", "extension", "plugin", "custom"):
        if field_name in node and type(node[field_name]) not in (str, dict):
            _schema_failure()
    operations = frozenset(
        {
            "read",
            "inputs",
            "transform",
            "write",
            "transformer",
            "pattern",
            "extension",
            "plugin",
            "custom",
        }
    )
    if not set(node).intersection(operations):
        _schema_failure()


def _value_is_active(value: object) -> bool:
    """Return whether an allowed configuration value requests runtime meaning."""
    if value is None or value is False:
        return False
    if value in ((), [], {}):
        return False
    return True


def _classify_node(node: Dict[str, object]) -> str:
    """Classify a node only from closed top-level key presence."""
    if "transform" in node or "transformer" in node:
        return "transform"
    if "read" in node or "inputs" in node:
        return "read"
    if "write" in node:
        return "write"
    if set(node).intersection(_EXTENSION_FIELDS):
        return "unknown"
    _schema_failure()
    return "unknown"


def _node_unresolved_codes(document: Dict[str, object], node: Dict[str, object]) -> Tuple[str, ...]:
    """Classify excluded extension, provider, and runtime dependencies by key."""
    codes: Set[str] = set()
    if _value_is_active(document.get("imports")) or any(
        _value_is_active(node.get(field_name)) for field_name in _EXTENSION_FIELDS
    ):
        codes.add("UNRESOLVED_EXTENSION_DEPENDENCY")
    if _value_is_active(document.get("connections")):
        codes.add("UNRESOLVED_PROVIDER_DEPENDENCY")
    for field_name in ("read", "inputs", "write"):
        if field_name in node and _value_is_active(node[field_name]):
            codes.add("UNRESOLVED_PROVIDER_DEPENDENCY")
    if any(_value_is_active(document.get(field_name)) for field_name in _PROJECT_RUNTIME_FIELDS):
        codes.add("UNRESOLVED_RUNTIME_DEPENDENCY")
    for field_name in _RUNTIME_NODE_FIELDS:
        if field_name in node and _value_is_active(node[field_name]):
            codes.add("UNRESOLVED_RUNTIME_DEPENDENCY")
    return tuple(sorted(codes))


def _diagnostic(code: str, category: str, subject: DiagnosticSubject) -> PlanningDiagnostic:
    """Construct one diagnostic exclusively from the immutable message catalog."""
    return PlanningDiagnostic(
        code=code,
        severity="error",
        category=category,
        subject=subject,
        message=_DIAGNOSTIC_MESSAGES[code],
    )


def _canonical_diagnostics(
    diagnostics: List[PlanningDiagnostic], limits: PlanningLimits
) -> Tuple[Tuple[PlanningDiagnostic, ...], bool]:
    """Deduplicate, sort, and deterministically truncate diagnostics."""
    ordered = sorted(
        set(diagnostics),
        key=lambda item: (
            _SEVERITY_RANK[item.severity],
            item.category,
            item.code,
            item.subject.kind,
            item.subject.name or "",
        ),
    )
    truncated = len(ordered) > limits.max_diagnostics
    return tuple(ordered[: limits.max_diagnostics]), truncated


def _project_pipeline(
    pipeline: Dict[str, object],
    document: Dict[str, object],
    limits: PlanningLimits,
) -> Tuple[LogicalPipelinePlan, List[PlanningDiagnostic], int]:
    """Project one source-ordered pipeline and validate its logical DAG."""
    _validate_pipeline_shape(pipeline)
    name = _normalize_identifier(pipeline.get("pipeline"), limits)
    raw_nodes = pipeline.get("nodes")
    if type(raw_nodes) is not list or not raw_nodes:
        _schema_failure()
    if len(raw_nodes) > limits.max_nodes_per_pipeline:
        raise _PlanningFailure("PLAN_LIMIT_EXCEEDED", "limit")

    node_names: Set[str] = set()
    node_drafts: List[Tuple[str, str, Tuple[str, ...], Tuple[str, ...]]] = []
    diagnostics: List[PlanningDiagnostic] = []
    for raw_node in raw_nodes:
        if type(raw_node) is not dict:
            _schema_failure()
        _validate_node_shape(raw_node)
        node_name = _normalize_identifier(raw_node.get("name"), limits)
        if node_name in node_names:
            _schema_failure()
        node_names.add(node_name)
        raw_dependencies = raw_node.get("depends_on", [])
        _require_string_list(raw_dependencies)
        dependencies = tuple(
            sorted({_normalize_identifier(item, limits) for item in raw_dependencies})
        )
        kind = _classify_node(raw_node)
        unresolved_codes = _node_unresolved_codes(document, raw_node)
        node_drafts.append((node_name, kind, dependencies, unresolved_codes))

    edge_count = 0
    edges: List[LogicalEdge] = []
    followers: Dict[str, List[str]] = {node_name: [] for node_name in node_names}
    indegree: Dict[str, int] = {node_name: 0 for node_name in node_names}
    plans: List[LogicalNodePlan] = []
    for node_name, kind, dependencies, unresolved_codes in node_drafts:
        if any(dependency not in node_names for dependency in dependencies):
            raise _PlanningFailure("REFERENCE_INVALID", "schema")
        edge_count += len(dependencies)
        for dependency in dependencies:
            edges.append(LogicalEdge(source=dependency, target=node_name))
            followers[dependency].append(node_name)
            indegree[node_name] += 1
        resolution = "unresolved" if unresolved_codes else "resolved"
        plans.append(
            LogicalNodePlan(
                name=node_name,
                kind=kind,
                depends_on=dependencies,
                resolution=resolution,
            )
        )
        subject = DiagnosticSubject(kind="node", name=node_name)
        for code in unresolved_codes:
            diagnostics.append(_diagnostic(code, "unresolved", subject))

    ready = [node_name for node_name, count in indegree.items() if count == 0]
    visited_count = 0
    while ready:
        current = ready.pop()
        visited_count += 1
        for follower in followers[current]:
            indegree[follower] -= 1
            if indegree[follower] == 0:
                ready.append(follower)
    if visited_count != len(node_names):
        raise _PlanningFailure("DEPENDENCY_CYCLE", "schema")

    edges.sort(key=lambda item: (item.source, item.target))
    return (
        LogicalPipelinePlan(name=name, nodes=tuple(plans), edges=tuple(edges)),
        diagnostics,
        edge_count,
    )


def _project_closed_logical_schema(
    parsed: object, limits: PlanningLimits
) -> Tuple[LogicalProjectPlan, List[PlanningDiagnostic]]:
    """Validate the closed document shape and project its bounded logical graph."""
    if type(parsed) is not dict:
        _schema_failure()
    _validate_project_shape(parsed)
    project_value = parsed.get("project")
    project_name = None if project_value is None else _normalize_identifier(project_value, limits)
    raw_pipelines = parsed.get("pipelines")
    if type(raw_pipelines) is not list or not raw_pipelines:
        _schema_failure()
    if len(raw_pipelines) > limits.max_pipelines:
        raise _PlanningFailure("PLAN_LIMIT_EXCEEDED", "limit")

    pipeline_names: Set[str] = set()
    plans: List[LogicalPipelinePlan] = []
    diagnostics: List[PlanningDiagnostic] = []
    total_nodes = 0
    total_edges = 0
    for raw_pipeline in raw_pipelines:
        if type(raw_pipeline) is not dict:
            _schema_failure()
        plan, pipeline_diagnostics, edge_count = _project_pipeline(raw_pipeline, parsed, limits)
        if plan.name in pipeline_names:
            _schema_failure()
        pipeline_names.add(plan.name)
        total_nodes += len(plan.nodes)
        total_edges += edge_count
        if total_nodes > limits.max_nodes_total or total_edges > limits.max_edges_total:
            raise _PlanningFailure("PLAN_LIMIT_EXCEEDED", "limit")
        plans.append(plan)
        diagnostics.extend(pipeline_diagnostics)
    return LogicalProjectPlan(name=project_name, pipelines=tuple(plans)), diagnostics


def _finalize_response(
    draft: Tuple[LogicalProjectPlan, List[PlanningDiagnostic]], limits: PlanningLimits
) -> PlanningResponse:
    """Compute status and canonical diagnostic retention from a safe logical draft."""
    plan, diagnostics = draft
    finalized, truncated = _canonical_diagnostics(diagnostics, limits)
    status = "unresolved" if diagnostics else "planned"
    return PlanningResponse(
        schema_version="1.0",
        status=status,
        plan=plan,
        diagnostics=finalized,
        truncated=truncated,
    )


def _failure_response(code: str, category: str, *, truncated: bool = False) -> PlanningResponse:
    """Return one fixed, sanitized invalid response."""
    return PlanningResponse(
        schema_version="1.0",
        status="invalid",
        plan=None,
        diagnostics=(_diagnostic(code, category, DiagnosticSubject(kind="document", name=None)),),
        truncated=truncated,
    )


def _enforce_encoded_response_bound(
    response: PlanningResponse, limits: PlanningLimits
) -> PlanningResponse:
    """Replace an oversized or unserializable response with one fixed failure."""
    try:
        emitted_non_identifiers = (
            response.schema_version,
            response.status,
            *(
                value
                for diagnostic in response.diagnostics
                for value in (
                    diagnostic.code,
                    diagnostic.severity,
                    diagnostic.category,
                    diagnostic.subject.kind,
                    diagnostic.message,
                )
            ),
            *(
                value
                for pipeline in response.plan.pipelines
                if response.plan is not None
                for node in pipeline.nodes
                for value in (node.kind, node.resolution)
            ),
        )
        if any(
            len(value) > limits.max_emitted_string_codepoints for value in emitted_non_identifiers
        ):
            raise _PlanningFailure("INTERNAL_PLANNING_FAILURE", "internal")
        encoded_size = len(response.to_json().encode("utf-8"))
    except _PlanningFailure:
        raise
    except Exception:
        raise _PlanningFailure("INTERNAL_SERIALIZATION_FAILURE", "internal") from None
    if encoded_size > limits.max_response_bytes:
        return _failure_response("RESPONSE_LIMIT_EXCEEDED", "limit", truncated=True)
    return response


def plan_pipeline_yaml(
    yaml_text: str,
    *,
    limits: PlanningLimits = DEFAULT_PLANNING_LIMITS,
) -> PlanningResponse:
    """Build a bounded logical plan from supplied YAML text without runtime effects."""
    try:
        checked_limits = _validate_limits(limits)
        checked_text = _validate_input(yaml_text, checked_limits)
        parsed = _load_one_bounded_document(checked_text, checked_limits)
        draft = _project_closed_logical_schema(parsed, checked_limits)
        response = _finalize_response(draft, checked_limits)
        return _enforce_encoded_response_bound(response, checked_limits)
    except _PlanningFailure as failure:
        return _failure_response(failure.code, failure.category)
    except Exception:
        return _failure_response("INTERNAL_PLANNING_FAILURE", "internal")


def plan_pipeline_bytes(
    yaml_bytes: bytes,
    *,
    limits: PlanningLimits = DEFAULT_PLANNING_LIMITS,
) -> PlanningResponse:
    """Decode bounded UTF-8 bytes and delegate to the immutable text planner."""
    try:
        checked_limits = _validate_limits(limits)
    except _PlanningFailure as failure:
        return _failure_response(failure.code, failure.category)
    except Exception:
        return _failure_response("INTERNAL_PLANNING_FAILURE", "internal")
    if type(yaml_bytes) is not bytes or len(yaml_bytes) > checked_limits.max_input_bytes:
        return _failure_response("INPUT_LIMIT_EXCEEDED", "limit")
    try:
        yaml_text = yaml_bytes.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return _failure_response("INVALID_UTF8", "syntax")
    return plan_pipeline_yaml(yaml_text, limits=checked_limits)


__all__ = [
    "DEFAULT_PLANNING_LIMITS",
    "DiagnosticSubject",
    "LogicalEdge",
    "LogicalNodePlan",
    "LogicalPipelinePlan",
    "LogicalProjectPlan",
    "PlanningDiagnostic",
    "PlanningLimits",
    "PlanningResponse",
    "plan_pipeline_bytes",
    "plan_pipeline_yaml",
]
