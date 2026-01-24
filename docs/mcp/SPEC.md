# Odibi MCP Facade — Complete Implementation Plan (v4.1)

> **Revision Notes:** Final enterprise-grade spec with polish pass. Key changes from v3: unified AccessContext, physical ref gate enforced everywhere, deny-by-default discovery, universal RunSelector invariant, complete TimeWindow validation, typed lists replace parallel arrays. v4.1 adds: explicit read-only invariant, data source precedence, truncated_reason enum, single-project rule.

## Overview

A thin, read-only MCP layer over Odibi's existing manager, exposing Stories, Catalog, Lineage, and Source Discovery for AI consumption. YAML-first is preserved—AI accelerates pipeline creation but you own the source of truth.

**Design Principles:**
- No re-resolution of configs or credentials
- Read-only (no pipeline execution, no writes)
- Unified access enforcement via AccessContext
- Credentials stay server-side
- Samples respect PII/privacy configuration
- Logical references by default, physical paths require explicit gate
- Explicit run selection on all stateful reads
- Strongly typed responses (no parallel arrays)
- Deny-by-default for path-based discovery
- Single-project context per request

**Foundational Invariants:**

> **READ-ONLY INVARIANT:** The MCP facade MUST NOT mutate data, trigger pipeline runs, or alter any state. It is a read-only projection over resolved Odibi artifacts.

> **SINGLE-PROJECT INVARIANT:** MCP tools MUST operate within a single project context per request. Cross-project joins, queries, or aggregations are explicitly forbidden to prevent payload explosion, lineage confusion, and isolation violations.

**Data Source Precedence:**

When resolving data for a request, tools MUST follow this precedence order:
1. **Story artifacts** — If present and complete for the selected run
2. **Manager-resolved metadata** — Pipeline/node config as resolved by PipelineManager
3. **Materialized output reads** — Only when explicitly requested AND no story/metadata available

This ensures consistency and minimizes unnecessary reads from storage.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        AI Client                            │
│         "Build me a bronze pipeline for sales data"         │
└─────────────────────────┬───────────────────────────────────┘
                          │ MCP Protocol
┌─────────────────────────▼───────────────────────────────────┐
│                    Odibi MCP Server                         │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Response Envelope: request_id, tool, project, timing  │ │
│  └────────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  AccessContext: unified enforcement across all layers  │ │
│  └────────────────────────────────────────────────────────┘ │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌───────┐ │
│  │ Story   │ │ Catalog │ │ Lineage │ │ Schema  │ │Source │ │
│  │ Tools   │ │ Tools   │ │ Tools   │ │ Tools   │ │Discov.│ │
│  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘ └───┬───┘ │
│       │          │          │          │           │       │
│  ┌────▼──────────▼──────────▼──────────▼───────────▼────┐  │
│  │              Audit Logger (all tool calls)           │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                   Existing Odibi Core                       │
│ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌────────┐ │
│ │StoryMetadata│ │CatalogManager│ │ Connections │ │ Engine │ │
│ │DocGenerator │ │              │ │ (resolved)  │ │(read)  │ │
│ └─────────────┘ └─────────────┘ └─────────────┘ └────────┘ │
└─────────────────────────────────────────────────────────────┘
```

---

## Core Contracts

### Unified Access Context

Single context injected once, enforced by every layer (Stories, Catalog, Lineage, Discovery):

```python
from pydantic import BaseModel, Field, validator
from typing import Set, Dict, List, Optional

class ConnectionPolicy(BaseModel):
    """Per-connection access policy. Deny-by-default for path discovery."""
    connection: str
    
    # Path-based discovery: DENY by default
    # Must have at least one allowed prefix OR explicit_allow_all=True
    allowed_path_prefixes: List[str] = Field(default_factory=list)
    denied_path_prefixes: List[str] = Field(default_factory=list)
    explicit_allow_all: bool = False  # Must be explicitly set to allow all paths
    
    # Limits
    max_depth: int = 5
    
    # Physical ref policy
    allow_physical_refs: bool = False  # Must be explicitly enabled
    
    @validator("allowed_path_prefixes", always=True)
    def validate_path_access(cls, v, values):
        """Enforce deny-by-default: require explicit allowlist or explicit_allow_all."""
        if not v and not values.get("explicit_allow_all", False):
            # Empty allowed list with no explicit_allow_all means deny all paths
            pass  # Valid state - will deny all path-based discovery
        return v
    
    def is_path_allowed(self, path: str) -> bool:
        """Check if path is allowed under this policy."""
        # Check denied first
        if any(path.startswith(prefix) for prefix in self.denied_path_prefixes):
            return False
        
        # If explicit_allow_all, allow anything not denied
        if self.explicit_allow_all:
            return True
        
        # Otherwise must match an allowed prefix
        if not self.allowed_path_prefixes:
            return False  # Deny by default
        
        return any(path.startswith(prefix) for prefix in self.allowed_path_prefixes)


class AccessContext(BaseModel):
    """
    Unified access enforcement context.
    Injected once at MCP server init, enforced across all layers.
    """
    # Project scoping
    authorized_projects: Set[str]
    
    # Environment (for env-specific access rules)
    environment: str = "production"  # dev, test, production
    
    # Connection policies (deny-by-default)
    connection_policies: Dict[str, ConnectionPolicy] = Field(default_factory=dict)
    
    # Global physical ref policy
    physical_refs_enabled: bool = False  # Master switch
    
    def check_project(self, project: str) -> None:
        """Raise PermissionError if project not authorized."""
        if project not in self.authorized_projects:
            raise PermissionError(f"Access denied: project '{project}' not authorized")
    
    def check_connection(self, connection: str) -> ConnectionPolicy:
        """Get connection policy, raise if not configured."""
        if connection not in self.connection_policies:
            raise PermissionError(f"Access denied: connection '{connection}' not configured")
        return self.connection_policies[connection]
    
    def check_path(self, connection: str, path: str) -> None:
        """Check path against connection policy."""
        policy = self.check_connection(connection)
        if not policy.is_path_allowed(path):
            raise PermissionError(f"Access denied: path '{path}' not allowed for connection '{connection}'")
    
    def can_include_physical(self, connection: str) -> bool:
        """Check if physical refs can be included for this connection."""
        if not self.physical_refs_enabled:
            return False
        policy = self.connection_policies.get(connection)
        return policy is not None and policy.allow_physical_refs
```

### Response Envelope

Every MCP response is wrapped in a typed envelope:

```python
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Literal
from datetime import datetime
from uuid import uuid4
from odibi import __version__

class PolicyApplied(BaseModel):
    """Record of policies applied to this response."""
    project_scoped: bool = True
    connection_policy: Optional[str] = None
    path_filtered: bool = False
    sample_capped: bool = False
    physical_ref_allowed: bool = False  # Explicit indicator
    physical_ref_included: bool = False  # Whether it was actually included

class MCPEnvelope(BaseModel):
    """Standard envelope for all MCP responses."""
    request_id: str = Field(default_factory=lambda: str(uuid4()))
    tool_name: str
    mcp_schema_version: str = __version__
    project: str
    environment: str = "production"
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    duration_ms: Optional[float] = None
    truncated: bool = False
    truncated_reason: Optional[TruncatedReason] = None
    policy_applied: PolicyApplied = Field(default_factory=PolicyApplied)

class TruncatedReason(str, Enum):
    """Explicit reasons for response truncation."""
    ROW_LIMIT = "row_limit"           # Hit max rows cap
    COLUMN_LIMIT = "column_limit"     # Hit max columns cap
    BYTE_LIMIT = "byte_limit"         # Hit max bytes for schema inference
    CELL_LIMIT = "cell_limit"         # Cell content truncated
    POLICY_MASKING = "policy_masking" # Content masked by policy
    SAMPLING_ONLY = "sampling_only"   # Only sample returned, full data available
    PAGINATION = "pagination"         # More results available via next_token

class MCPErrorEnvelope(MCPEnvelope):
    """Error response envelope."""
    success: Literal[False] = False
    error: str
    error_type: str  # validation_failed, not_found, permission_denied, connection_error
    retryable: bool = False
    details: Optional[List[Dict[str, Any]]] = None

class MCPSuccessEnvelope(MCPEnvelope):
    """Success response envelope with typed payload."""
    success: Literal[True] = True
    data: Any  # Typed per-tool response
```

### Run Selector (Universal Invariant)

**INVARIANT:** All tools that read node materializations, story artifacts, or any stateful data MUST accept RunSelector.

```python
from typing import Literal, Union

class RunById(BaseModel):
    run_id: str

RunSelector = Union[
    Literal["latest_successful"],
    Literal["latest_attempt"],
    RunById
]

# Default: latest_successful
DEFAULT_RUN_SELECTOR: RunSelector = "latest_successful"

# Tools that MUST accept RunSelector:
# - story_read, story_diff, node_describe
# - node_sample, node_sample_in, node_failed_rows
# - output_schema, list_outputs
# - lineage_graph
# - Any future tool reading materialized state
```

### Resource Reference (Physical Gate Enforced)

Physical refs require: `include_physical=True` AND `policy.allow_physical_refs=True` AND `access_context.physical_refs_enabled=True`

```python
class ResourceRef(BaseModel):
    """Logical reference to a data resource. Physical ref gated by policy."""
    kind: Literal["delta_table", "delta_path", "sql_table", "file", "directory"]
    logical_name: str  # e.g., "sales_bronze.clean_orders"
    connection: str    # logical connection name
    
    # Physical ref: ONLY included when ALL gates pass
    # Gate 1: Caller passes include_physical=True
    # Gate 2: ConnectionPolicy.allow_physical_refs=True
    # Gate 3: AccessContext.physical_refs_enabled=True
    physical_ref: Optional[str] = None  # Guaranteed None unless all gates pass

def resolve_resource_ref(
    logical_name: str,
    connection: str,
    kind: str,
    physical_path: str,
    include_physical: bool,
    access_context: AccessContext,
) -> ResourceRef:
    """
    Resolve a resource reference with physical ref gating.
    """
    # Check all three gates
    can_include = (
        include_physical and
        access_context.physical_refs_enabled and
        access_context.can_include_physical(connection)
    )
    
    return ResourceRef(
        kind=kind,
        logical_name=logical_name,
        connection=connection,
        physical_ref=physical_path if can_include else None,
    )
```

### Time Window (Complete Validation)

```python
from pydantic import BaseModel, Field, root_validator
from datetime import datetime
from typing import Optional

class TimeWindow(BaseModel):
    """
    Explicit time semantics for catalog queries.
    At least one of (days) or (start AND end) must be provided.
    """
    days: Optional[int] = Field(default=None, ge=1, le=365)
    start: Optional[datetime] = None  # Inclusive
    end: Optional[datetime] = None    # Exclusive
    timezone: str = "UTC"
    
    @root_validator
    def validate_window(cls, values):
        days = values.get("days")
        start = values.get("start")
        end = values.get("end")
        
        # Check: cannot mix days with start/end
        if days is not None and (start is not None or end is not None):
            raise ValueError("Cannot specify both 'days' and 'start/end'")
        
        # Check: must have at least one
        if days is None and start is None and end is None:
            # Default to 7 days
            values["days"] = 7
        
        # Check: if start/end provided, both must be present
        if (start is None) != (end is None):
            raise ValueError("Both 'start' and 'end' must be provided together")
        
        # Check: start < end
        if start is not None and end is not None:
            if start >= end:
                raise ValueError("'start' must be before 'end'")
        
        return values
    
    def to_range(self) -> tuple[datetime, datetime]:
        """Convert to (start, end) tuple in UTC."""
        from datetime import timedelta
        import pytz
        
        tz = pytz.timezone(self.timezone)
        now = datetime.now(tz)
        
        if self.days is not None:
            end = now
            start = now - timedelta(days=self.days)
        else:
            start = self.start.astimezone(pytz.UTC) if self.start.tzinfo else tz.localize(self.start).astimezone(pytz.UTC)
            end = self.end.astimezone(pytz.UTC) if self.end.tzinfo else tz.localize(self.end).astimezone(pytz.UTC)
        
        return (start, end)
```

---

## Typed Response Models (No Parallel Arrays)

### Column Specification

```python
class ColumnSpec(BaseModel):
    """Typed column specification. Replaces parallel arrays."""
    name: str
    dtype: str
    nullable: bool = True
    description: Optional[str] = None
    semantic_type: Optional[str] = None  # "pii", "metric", "dimension", "key"
    sample_values: Optional[List[Any]] = None  # Max 3 examples
```

### Schema Response

```python
class SchemaResponse(BaseModel):
    """Typed schema response. No parallel arrays."""
    columns: List[ColumnSpec]
    row_count: Optional[int] = None
    partition_columns: List[str] = Field(default_factory=list)
```

### Graph Data

```python
class GraphNode(BaseModel):
    id: str
    type: Literal["source", "transform", "sink"]
    label: str
    layer: Optional[str] = None  # bronze, silver, gold
    status: Optional[str] = None  # success, failed, skipped

class GraphEdge(BaseModel):
    from_node: str
    to_node: str
    edge_type: Literal["data_flow", "dependency"] = "data_flow"

class GraphData(BaseModel):
    nodes: List[GraphNode]
    edges: List[GraphEdge]
```

### Schema Change

```python
class ColumnChange(BaseModel):
    name: str
    old_type: str
    new_type: str

class SchemaChange(BaseModel):
    run_id: str
    timestamp: datetime
    added: List[ColumnSpec] = Field(default_factory=list)
    removed: List[str] = Field(default_factory=list)
    type_changed: List[ColumnChange] = Field(default_factory=list)
```

### Diff Summary

```python
class DiffSummary(BaseModel):
    """Schema and row-level diff without exposing raw values by default."""
    schema_diff: SchemaChange
    row_count_diff: int  # run_b - run_a
    null_count_changes: Dict[str, int]  # column -> delta
    distinct_count_changes: Dict[str, int]  # column -> delta
    
    # Raw sample diffs: ONLY when explicitly requested AND permitted
    sample_diff_included: bool = False
    sample_diff: Optional[List[Dict[str, Any]]] = None
```

### Node Stats

```python
class StatPoint(BaseModel):
    """Single data point in a time series."""
    timestamp: datetime
    value: float

class NodeStatsResponse(BaseModel):
    """Typed node statistics. No parallel arrays."""
    pipeline: str
    node: str
    window: TimeWindow
    run_count: int
    success_count: int
    failure_count: int
    failure_rate: float
    avg_duration_seconds: float
    duration_trend: List[StatPoint]
    row_count_trend: List[StatPoint]
```

### File Listing

```python
class FileInfo(BaseModel):
    """Typed file info. Replaces Dict[str, Any]."""
    logical_name: str
    size_bytes: int
    modified: datetime
    is_directory: bool = False
    # Physical path: gated by policy
    physical_path: Optional[str] = None

class ListFilesResponse(BaseModel):
    """Paginated file listing."""
    connection: str
    path: str
    files: List[FileInfo]
    next_token: Optional[str] = None
    truncated: bool = False
    total_count: Optional[int] = None
```

---

## Tool Categories

### Universal Invariants

1. **RunSelector Required:** All tools reading materializations or artifacts accept `run_selector: RunSelector = "latest_successful"`
2. **Physical Refs Gated:** All tools returning ResourceRef accept `include_physical: bool = False`
3. **AccessContext Enforced:** All tools validate against the unified AccessContext

### 1. Project Tools

| Tool | Parameters | Returns |
|------|------------|---------|
| `project_scan` | - | pipelines[], last_run, resolution_status |
| `project_health` | `window: TimeWindow = {days: 7}` | success_rate, failures, anomalies |

### 2. Story Tools (observe runs)

| Tool | Parameters | Returns |
|------|------------|---------|
| `story_read` | `pipeline, run_selector = "latest_successful"` | nodes[], status, duration, schema_changes: List[SchemaChange], graph_data: GraphData |
| `story_diff` | `pipeline, run_a: RunSelector, run_b: RunSelector, include_sample_diff: bool = False` | DiffSummary |
| `node_describe` | `pipeline, node, run_selector = "latest_successful"` | operation, validations, schema: SchemaResponse, duration |

### 3. Sample Tools (explicit, limited)

| Tool | Parameters | Returns |
|------|------------|---------|
| `node_sample` | `pipeline, node, run_selector, limit=10, columns?: List[str]` | rows[], truncated, truncated_reason, total_rows |
| `node_sample_in` | `pipeline, node, run_selector, limit=10, columns?: List[str]` | rows[], truncated, truncated_reason, total_rows |
| `node_failed_rows` | `pipeline, node, validation, run_selector, limit=5` | rows[], validation_name, fail_count |

### 4. Catalog Tools (historical queries)

| Tool | Parameters | Returns |
|------|------------|---------|
| `node_stats` | `pipeline, node, window: TimeWindow` | NodeStatsResponse |
| `pipeline_stats` | `pipeline, window: TimeWindow` | PipelineStatsResponse |
| `failure_summary` | `pipeline?, window: TimeWindow` | FailureSummaryResponse |
| `schema_history` | `pipeline, node, run_selector, limit=10` | List[SchemaChange] |

### 5. Lineage Tools

| Tool | Parameters | Returns |
|------|------------|---------|
| `lineage_upstream` | `resource: ResourceRef, include_physical: bool = False` | upstream: List[ResourceRef], depth |
| `lineage_downstream` | `resource: ResourceRef, include_physical: bool = False` | downstream: List[ResourceRef], depth |
| `lineage_graph` | `pipeline, run_selector, include_physical: bool = False` | GraphData |

### 6. Schema Tools (for building downstream)

| Tool | Parameters | Returns |
|------|------------|---------|
| `output_schema` | `pipeline, node, run_selector` | SchemaResponse |
| `list_outputs` | `project?, run_selector, include_physical: bool = False` | List[ResourceRef] |

### 7. Source Discovery Tools (deny-by-default)

**Policy:** Path-based discovery is DENIED unless connection has `explicit_allow_all=True` or matching `allowed_path_prefixes`.

| Tool | Parameters | Returns |
|------|------------|---------|
| `list_files` | `connection, path?, limit=100, next_token?, include_physical: bool = False` | ListFilesResponse |
| `list_schemas` | `connection` | ListSchemasResponse — List all schemas with table counts. **Call FIRST before discover_database** |
| `list_tables` | `connection, schema?, limit=100` | ListTablesResponse |
| `infer_schema` | `connection, path, format, max_rows=100, max_bytes=1MB` | SchemaResponse |
| `describe_table` | `connection, table` | SchemaResponse |
| `preview_source` | `connection, path, format, limit=5, columns?: List[str]` | PreviewResponse |
| `discover_database` | `connection, schema?, max_tables=20, sample_rows=0` | DiscoverDatabaseResponse — Structure-first discovery |
| `discover_storage` | `connection, path?, pattern?, max_files=20, sample_rows=0, recursive=False` | DiscoverStorageResponse — File discovery |
| `debug_env` | (none) | DebugEnvResponse — Shows .env loading, env vars set, connection status |

**Discovery Limits:**

```python
class DiscoveryLimits(BaseModel):
    """Hard limits for source discovery operations."""
    max_files_per_call: int = 100
    max_rows_for_schema: int = 100
    max_bytes_for_schema: int = 1_048_576  # 1MB
    max_preview_rows: int = 10
    max_preview_columns: int = 15
    max_cell_chars: int = 100
```

### 8. Build Tools (existing MCP, retained)

| Tool | Description |
|------|-------------|
| `suggest_pattern(use_case)` | Recommend pattern for use case |
| `list_transformers()` | List all 52+ transformers |
| `list_patterns()` | List all 6 DWH patterns |
| `list_connections()` | List connection types |
| `generate_pipeline_yaml(...)` | Generate pipeline YAML |
| `generate_transformer(...)` | Generate custom transformer code |
| `validate_yaml(yaml_content)` | Validate YAML before save |
| `diagnose_error(error_message)` | Debug pipeline errors |
| `get_example(pattern_name)` | Get working example |

---

## Authorization Implementation

### AccessContext Enforcement

```python
class OdibiMCPServer:
    def __init__(
        self,
        catalog: CatalogManager,
        story_loader: StoryLoader,
        connection_resolver: ConnectionResolver,
        engine: Engine,
        access_context: AccessContext,
    ):
        self.catalog = catalog
        self.story_loader = story_loader
        self.connection_resolver = connection_resolver
        self.engine = engine
        self.access = access_context
        
        # Inject access context into all layers
        self.catalog.set_access_context(access_context)
        self.story_loader.set_access_context(access_context)
    
    async def call_tool(self, name: str, args: dict) -> MCPEnvelope:
        # AccessContext is enforced at each layer automatically
        ...
```

### Layer-Level Enforcement

```python
class CatalogManager:
    def set_access_context(self, ctx: AccessContext) -> None:
        self._access = ctx
    
    def query_node_runs(self, pipeline: str, node: str, window: TimeWindow) -> pd.DataFrame:
        """Query with automatic project scoping."""
        df = self._read_delta("meta_node_runs")
        df = self._apply_project_scope(df)  # Uses self._access
        df = self._apply_time_window(df, window)
        return df.query(f"pipeline_name == '{pipeline}' and node_name == '{node}'")
    
    def _apply_project_scope(self, df: Any) -> Any:
        """Canonical project filter - works with Spark/Pandas/Polars."""
        projects = list(self._access.authorized_projects)
        
        if hasattr(df, "filter") and hasattr(df, "columns"):  # Spark
            from pyspark.sql.functions import col
            return df.filter(col("project").isin(projects))
        elif hasattr(df, "query"):  # Pandas
            return df[df["project"].isin(projects)]
        else:  # Polars
            import polars as pl
            return df.filter(pl.col("project").is_in(projects))


class StoryLoader:
    def set_access_context(self, ctx: AccessContext) -> None:
        self._access = ctx
    
    def load(self, pipeline: str, run_selector: RunSelector) -> PipelineStoryMetadata:
        story = self._load_story(pipeline, run_selector)
        self._access.check_project(story.project)  # Enforce access
        return story


class SourceDiscovery:
    def __init__(self, access: AccessContext, engine: Engine):
        self._access = access
        self._engine = engine
    
    def list_files(self, connection: str, path: str, ...) -> ListFilesResponse:
        # Check connection access
        policy = self._access.check_connection(connection)
        
        # Check path access (deny-by-default)
        self._access.check_path(connection, path)
        
        # Proceed with listing...
```

### Configuration

```yaml
# mcp_config.yaml
access_context:
  authorized_projects:
    - "sales_analytics"
    - "inventory_pipeline"
  
  environment: "production"
  
  physical_refs_enabled: false  # Master switch - default OFF
  
  connection_policies:
    adls_raw:
      # Deny-by-default: must specify allowed prefixes
      allowed_path_prefixes:
        - "sales/"
        - "inventory/"
      denied_path_prefixes:
        - "sales/pii/"
        - "sales/restricted/"
      max_depth: 3
      allow_physical_refs: false
    
    adls_curated:
      # Explicit allow-all for curated layer
      explicit_allow_all: true
      max_depth: 5
      allow_physical_refs: false
    
    sql_warehouse:
      # SQL connections: table-level access
      explicit_allow_all: true  # Can list all tables
      allow_physical_refs: false
```

---

## Audit Logging

```python
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any

@dataclass
class AuditEntry:
    timestamp: datetime
    request_id: str
    tool_name: str
    project: str
    environment: str
    connection: Optional[str]
    resource_logical: Optional[str]  # Logical name only - never physical
    args_summary: Dict[str, Any]  # Redacted sensitive values
    duration_ms: float
    success: bool
    error_type: Optional[str]
    bytes_read_estimate: Optional[int]
    policy_applied: Dict[str, bool]

class AuditLogger:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def log(self, entry: AuditEntry) -> None:
        self.logger.info(
            "MCP tool call",
            extra={
                "request_id": entry.request_id,
                "tool": entry.tool_name,
                "project": entry.project,
                "env": entry.environment,
                "connection": entry.connection,
                "resource": entry.resource_logical,
                "duration_ms": entry.duration_ms,
                "success": entry.success,
                "error_type": entry.error_type,
                "policy": entry.policy_applied,
            }
        )
    
    @staticmethod
    def redact_args(args: Dict[str, Any]) -> Dict[str, Any]:
        """Redact sensitive arguments before logging."""
        redact_keys = {"password", "token", "secret", "key", "credential"}
        return {
            k: "[REDACTED]" if any(r in k.lower() for r in redact_keys) else v
            for k, v in args.items()
        }
```

---

## Sample Limiting

```python
MAX_SAMPLE_ROWS = 10
MAX_SAMPLE_COLS = 15
MAX_CELL_CHARS = 100

def limit_sample(
    rows: List[Dict[str, Any]],
    limit: int = MAX_SAMPLE_ROWS,
    columns: Optional[List[str]] = None,
) -> tuple[List[Dict[str, Any]], bool, Optional[str]]:
    """
    Apply hard limits to sample data.
    Returns: (limited_rows, truncated, truncated_reason)
    """
    truncated = False
    reason = None
    
    # Row limit
    if len(rows) > limit:
        rows = rows[:limit]
        truncated = True
        reason = "row_limit"
    
    # Column limit
    if rows:
        available_cols = list(rows[0].keys())
        if columns:
            cols = [c for c in columns if c in available_cols][:MAX_SAMPLE_COLS]
        else:
            cols = available_cols[:MAX_SAMPLE_COLS]
        
        if len(available_cols) > len(cols):
            truncated = True
            reason = reason or "column_limit"
        
        rows = [{k: v for k, v in row.items() if k in cols} for row in rows]
    
    # Cell truncation
    for row in rows:
        for k, v in row.items():
            if isinstance(v, str) and len(v) > MAX_CELL_CHARS:
                row[k] = v[:MAX_CELL_CHARS - 3] + "..."
                truncated = True
                reason = reason or "cell_limit"
    
    return rows, truncated, reason
```

---

## Error Handling

```python
from pydantic import ValidationError
from uuid import uuid4

async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    request_id = str(uuid4())
    start_time = datetime.utcnow()
    project = "unknown"
    
    try:
        result = await dispatch_tool(name, arguments)
        project = result.project
        
        duration_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
        
        envelope = MCPSuccessEnvelope(
            request_id=request_id,
            tool_name=name,
            project=project,
            environment=access_context.environment,
            duration_ms=duration_ms,
            truncated=result.truncated,
            truncated_reason=result.truncated_reason,
            policy_applied=result.policy_applied,
            data=result.data,
        )
        
        audit_logger.log(AuditEntry(
            timestamp=start_time,
            request_id=request_id,
            tool_name=name,
            project=project,
            environment=access_context.environment,
            duration_ms=duration_ms,
            success=True,
            error_type=None,
            policy_applied=envelope.policy_applied.dict(),
            ...
        ))
        
        return [TextContent(type="text", text=envelope.model_dump_json())]
    
    except PermissionError as e:
        return _error_response(request_id, name, start_time, str(e), "permission_denied", retryable=False)
    
    except ValidationError as e:
        return _error_response(request_id, name, start_time, "Validation failed", "validation_failed",
                               retryable=False, details=e.errors())
    
    except FileNotFoundError as e:
        return _error_response(request_id, name, start_time, str(e), "not_found", retryable=False)
    
    except ConnectionError as e:
        return _error_response(request_id, name, start_time, str(e), "connection_error", retryable=True)
    
    except Exception as e:
        return _error_response(request_id, name, start_time, str(e), "internal_error", retryable=False)
```

---

## Example AI Workflow

**User:** "Build me a bronze pipeline for the new sales data in adls_raw"

```
AI: [list_files("adls_raw", "sales/", limit=20)]
    → {
        request_id: "abc-123",
        tool_name: "list_files",
        project: "sales_analytics",
        environment: "production",
        success: true,
        policy_applied: {
          project_scoped: true,
          connection_policy: "adls_raw",
          path_filtered: true,
          physical_ref_allowed: false,
          physical_ref_included: false
        },
        data: {
          connection: "adls_raw",
          path: "sales/",
          files: [
            {logical_name: "sales/2025-01.csv", size_bytes: 1024000, modified: "...", physical_path: null},
            {logical_name: "sales/2025-02.csv", size_bytes: 1048000, modified: "...", physical_path: null}
          ],
          next_token: null,
          truncated: false
        }
      }

AI: [infer_schema("adls_raw", "sales/2025-01.csv", "csv")]
    → {
        success: true,
        policy_applied: {sample_capped: true},
        data: {
          columns: [
            {name: "order_id", dtype: "int", nullable: false},
            {name: "customer_email", dtype: "str", nullable: true, semantic_type: "pii"},
            {name: "amount", dtype: "float", nullable: true},
            {name: "order_date", dtype: "date", nullable: true}
          ]
        }
      }

AI: [preview_source("adls_raw", "sales/2025-01.csv", "csv", limit=3)]
    → sample rows with full typing

AI: [suggest_pattern("ingest raw sales CSV to bronze delta")]
    → "Use basic transform pattern with delta write"

AI: [list_outputs("sales_analytics", run_selector="latest_successful", include_physical=false)]
    → List[ResourceRef] with logical names only

AI: [generate_pipeline_yaml(...)]
    → generates YAML

AI: [validate_yaml(generated_yaml)]
    → confirms valid
```

---

## Implementation Order

| Phase | Tasks | Effort |
|-------|-------|--------|
| **1. Core contracts** | AccessContext, MCPEnvelope, RunSelector, ResourceRef, PolicyApplied | 4h |
| **2. Typed models** | ColumnSpec, SchemaResponse, GraphData, SchemaChange, DiffSummary, NodeStatsResponse | 3h |
| **3. Access enforcement** | Layer injection, project scoping, connection policies, path checks | 3h |
| **4. Audit logger** | AuditEntry, logging integration, arg redaction | 1h |
| **5. Story tools** | story_read, story_diff, node_describe with typed responses | 3h |
| **6. Sample tools** | node_sample, node_sample_in, node_failed_rows with limits | 2h |
| **7. Catalog tools** | TimeWindow, node_stats, pipeline_stats, failure_summary, schema_history | 3h |
| **8. Lineage tools** | lineage_upstream, lineage_downstream, lineage_graph with ResourceRef | 2h |
| **9. Schema tools** | output_schema, list_outputs with ResourceRef gating | 2h |
| **10. Source discovery** | list_files, list_tables, infer_schema, describe_table, preview_source with deny-by-default | 5h |
| **11. Error handling** | Structured errors with envelope, retryable flag | 1h |
| **12. Tests** | Unit tests for access, limits, pagination, typed responses | 6h |

**Total: ~35 hours**

---

## What This Does NOT Do

| Anti-pattern | Why excluded |
|--------------|--------------|
| Execute arbitrary SQL | Credential leak risk, non-determinism |
| Trigger pipeline runs | MCP is read-only; write = second orchestrator |
| Return physical paths by default | Require explicit gate (3 conditions must pass) |
| Allow unbounded discovery | Deny-by-default; require explicit allowlist |
| Auto-refresh Stories | Expensive; keep as pipeline-run side effect |
| Re-resolve YAML configs | Manager already did this; reuse resolved state |
| Modify YAML files | AI generates, user saves. YAML-first preserved |
| Expose raw row diffs | Hashed summaries by default; raw requires explicit flag |
| Use parallel arrays | All responses use typed models |

---

## Security Model

| Layer | Mechanism |
|-------|-----------|
| **Credentials** | Stay server-side (env vars / Key Vault / MSI). Never returned. |
| **Unified access** | AccessContext enforced across Stories, Catalog, Lineage, Discovery |
| **Project scoping** | Canonical filter in all layers (Spark/Pandas/Polars) |
| **Connection policies** | Deny-by-default; require explicit allowlist or `explicit_allow_all` |
| **Path filtering** | `allowed_path_prefixes` + `denied_path_prefixes` checked on every call |
| **Physical ref gate** | 3 conditions: caller flag + policy flag + master switch |
| **PII in samples** | Privacy Suite anonymizes before Story capture |
| **Sensitive columns** | `sensitive: true` redacts at sample collection |
| **Sample limits** | Hard caps: 10 rows, 15 cols, 100 chars/cell |
| **Discovery limits** | 100 files/call, 1MB schema inference, pagination required |
| **Read-only** | No writes, no execution, no mutations |
| **Audit trail** | All tool calls logged with request_id, resource refs (logical only) |

---

## Success Criteria

1. **AI can discover:** "What sources exist at this connection? What's the schema?"
2. **AI can build:** "Generate a bronze pipeline for this CSV with proper PII handling"
3. **AI can observe:** "What happened in the last run? Why did it fail?"
4. **AI can trace:** "What upstream tables feed into this fact table?"
5. **AI cannot:** See unauthorized projects, access unallowed paths, see physical refs, trigger runs
6. **YAML-first preserved:** AI accelerates creation, user owns source of truth
7. **Debuggable:** Every response has request_id, timestamp, policies applied
8. **Auditable:** All tool calls logged with redacted args and logical resource refs
9. **Type-safe:** All responses use typed models, no parallel arrays
10. **Deterministic:** Deny-by-default discovery, explicit run selection everywhere

---

## Appendix: Invariants Summary

| Invariant | Enforcement |
|-----------|-------------|
| **Read-only** | MCP facade MUST NOT mutate data, trigger runs, or alter state |
| **Single-project** | MCP tools MUST operate within a single project context per request |
| **RunSelector universal** | All stateful reads accept `run_selector` parameter |
| **Physical refs gated** | Requires `include_physical=True` + `policy.allow_physical_refs` + `access.physical_refs_enabled` |
| **Deny-by-default discovery** | Path access requires explicit allowlist or `explicit_allow_all=True` |
| **No parallel arrays** | All schemas use `List[ColumnSpec]`, not `columns[] + types[]` |
| **AccessContext everywhere** | Single context injected once, enforced by all layers |
| **Typed responses** | All tools return Pydantic models, not Dict[str, Any] |
| **Data source precedence** | Story → Manager metadata → Materialized output (in order) |
| **Truncation typed** | All truncation uses `TruncatedReason` enum, not free-form strings |

---

## Appendix: Resolved Concerns (Oracle Critiques v1-v3)

| Concern | Resolution |
|---------|------------|
| Auth filter assumes pandas | Unified AccessContext with canonical `_apply_project_scope()` for all engines |
| Paths leak despite policy | ResourceRef with 3-gate physical ref enforcement |
| Run selection undefined | Universal `RunSelector` invariant on all stateful reads |
| Weak typing (Dict[str, Any]) | All responses use typed Pydantic models |
| Source discovery unbounded | Deny-by-default with `ConnectionPolicy` allowlists |
| Errors lose context | `MCPEnvelope` with `request_id`, `tool_name`, `policy_applied` |
| Time semantics unclear | `TimeWindow` with complete validation (ordering, presence) |
| Diff can leak rows | `DiffSummary` with hashed counts by default |
| No audit trail | `AuditLogger` with structured entries |
| Parallel arrays fragile | Replaced with `List[ColumnSpec]` everywhere |
| Empty allowlist = all allowed | Flipped to deny-by-default; require `explicit_allow_all=True` |
| Physical ref gate unclear | Explicit 3-gate check documented and enforced |
| Read-only implicit | Added explicit READ-ONLY INVARIANT statement |
| Story vs output precedence | Added Data Source Precedence order (Story → Metadata → Output) |
| truncated_reason free-form | Added `TruncatedReason` enum with explicit values |
| Cross-project queries possible | Added SINGLE-PROJECT INVARIANT banning cross-project operations |
