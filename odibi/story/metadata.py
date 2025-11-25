"""
Story Metadata Tracking
========================

Tracks detailed metadata for pipeline execution stories.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class DeltaWriteInfo:
    """
    Metadata specific to Delta Lake writes.
    """

    version: int
    timestamp: Optional[datetime] = None
    operation: Optional[str] = None
    operation_metrics: Dict[str, Any] = field(default_factory=dict)
    # For linking back to specific commit info if needed
    read_version: Optional[int] = None  # The version we read FROM (if applicable)


@dataclass
class NodeExecutionMetadata:
    """
    Metadata for a single node execution.

    Captures all relevant information about a node's execution including
    performance metrics, data transformations, and error details.
    """

    node_name: str
    operation: str
    status: str  # "success", "failed", "skipped"
    duration: float

    # Data metrics
    rows_in: Optional[int] = None
    rows_out: Optional[int] = None
    rows_change: Optional[int] = None
    rows_change_pct: Optional[float] = None
    sample_in: Optional[List[Dict[str, Any]]] = None
    sample_data: Optional[List[Dict[str, Any]]] = None

    # Schema tracking
    schema_in: Optional[List[str]] = None
    schema_out: Optional[List[str]] = None
    columns_added: List[str] = field(default_factory=list)
    columns_removed: List[str] = field(default_factory=list)
    columns_renamed: List[str] = field(default_factory=list)

    # Execution Logic & Lineage
    executed_sql: List[str] = field(default_factory=list)
    sql_hash: Optional[str] = None
    transformation_stack: List[str] = field(default_factory=list)
    config_snapshot: Optional[Dict[str, Any]] = None

    # Delta & Data Info
    delta_info: Optional[DeltaWriteInfo] = None
    data_diff: Optional[Dict[str, Any]] = None  # Stores diff summary (added/removed samples)
    environment: Optional[Dict[str, Any]] = None  # Captured execution environment

    # Source & Quality
    source_files: List[str] = field(default_factory=list)
    null_profile: Optional[Dict[str, float]] = None

    # Error info
    error_message: Optional[str] = None
    error_type: Optional[str] = None

    # Timestamps
    started_at: Optional[str] = None
    completed_at: Optional[str] = None

    def calculate_row_change(self):
        """Calculate row count change metrics."""
        if self.rows_in is not None and self.rows_out is not None:
            self.rows_change = self.rows_out - self.rows_in
            if self.rows_in > 0:
                self.rows_change_pct = (self.rows_change / self.rows_in) * 100
            else:
                self.rows_change_pct = 0.0 if self.rows_out == 0 else 100.0

    def calculate_schema_changes(self):
        """Calculate schema changes between input and output."""
        if self.schema_in and self.schema_out:
            set_in = set(self.schema_in)
            set_out = set(self.schema_out)

            self.columns_added = list(set_out - set_in)
            self.columns_removed = list(set_in - set_out)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        base_dict = {
            "node_name": self.node_name,
            "operation": self.operation,
            "status": self.status,
            "duration": self.duration,
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "rows_change": self.rows_change,
            "rows_change_pct": self.rows_change_pct,
            "sample_in": self.sample_in,
            "sample_data": self.sample_data,
            "schema_in": self.schema_in,
            "schema_out": self.schema_out,
            "columns_added": self.columns_added,
            "columns_removed": self.columns_removed,
            "error_message": self.error_message,
            "error_type": self.error_type,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "executed_sql": self.executed_sql,
            "sql_hash": self.sql_hash,
            "transformation_stack": self.transformation_stack,
            "config_snapshot": self.config_snapshot,
            "data_diff": self.data_diff,
            "environment": self.environment,
            "source_files": self.source_files,
            "null_profile": self.null_profile,
        }

        if self.delta_info:
            base_dict["delta_info"] = {
                "version": self.delta_info.version,
                "timestamp": (
                    self.delta_info.timestamp.isoformat() if self.delta_info.timestamp else None
                ),
                "operation": self.delta_info.operation,
                "operation_metrics": self.delta_info.operation_metrics,
                "read_version": self.delta_info.read_version,
            }

        return base_dict

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NodeExecutionMetadata":
        """Create instance from dictionary."""
        delta_info = None
        if "delta_info" in data and data["delta_info"]:
            d_info = data["delta_info"]
            # Parse timestamp if present
            ts = None
            if d_info.get("timestamp"):
                try:
                    ts = datetime.fromisoformat(d_info["timestamp"])
                except ValueError:
                    pass

            delta_info = DeltaWriteInfo(
                version=d_info.get("version"),
                timestamp=ts,
                operation=d_info.get("operation"),
                operation_metrics=d_info.get("operation_metrics", {}),
                read_version=d_info.get("read_version"),
            )

        # Filter out unknown keys to be safe
        valid_keys = cls.__annotations__.keys()
        clean_data = {k: v for k, v in data.items() if k in valid_keys}

        # Remove nested objects handled separately
        if "delta_info" in clean_data:
            del clean_data["delta_info"]

        return cls(delta_info=delta_info, **clean_data)


@dataclass
class PipelineStoryMetadata:
    """
    Complete metadata for a pipeline run story.

    Aggregates information about the entire pipeline execution including
    all node executions, overall status, and project context.
    """

    pipeline_name: str
    pipeline_layer: Optional[str] = None

    # Execution info
    run_id: str = field(default_factory=lambda: datetime.now().strftime("%Y%m%d_%H%M%S"))
    started_at: str = field(default_factory=lambda: datetime.now().isoformat())
    completed_at: Optional[str] = None
    duration: float = 0.0

    # Status
    total_nodes: int = 0
    completed_nodes: int = 0
    failed_nodes: int = 0
    skipped_nodes: int = 0

    # Node details
    nodes: List[NodeExecutionMetadata] = field(default_factory=list)

    # Project context
    project: Optional[str] = None
    plant: Optional[str] = None
    asset: Optional[str] = None
    business_unit: Optional[str] = None

    # Story settings
    theme: str = "default"
    include_samples: bool = True
    max_sample_rows: int = 10

    def add_node(self, node_metadata: NodeExecutionMetadata):
        """
        Add node execution metadata.

        Args:
            node_metadata: Metadata for the node execution
        """
        self.nodes.append(node_metadata)
        self.total_nodes += 1

        if node_metadata.status == "success":
            self.completed_nodes += 1
        elif node_metadata.status == "failed":
            self.failed_nodes += 1
        elif node_metadata.status == "skipped":
            self.skipped_nodes += 1

    def get_success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_nodes == 0:
            return 0.0
        return (self.completed_nodes / self.total_nodes) * 100

    def get_total_rows_processed(self) -> int:
        """Calculate total rows processed across all nodes."""
        total = 0
        for node in self.nodes:
            if node.rows_out is not None:
                total += node.rows_out
        return total

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "pipeline_name": self.pipeline_name,
            "pipeline_layer": self.pipeline_layer,
            "run_id": self.run_id,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration": self.duration,
            "total_nodes": self.total_nodes,
            "completed_nodes": self.completed_nodes,
            "failed_nodes": self.failed_nodes,
            "skipped_nodes": self.skipped_nodes,
            "success_rate": self.get_success_rate(),
            "total_rows_processed": self.get_total_rows_processed(),
            "nodes": [node.to_dict() for node in self.nodes],
            "project": self.project,
            "plant": self.plant,
            "asset": self.asset,
            "business_unit": self.business_unit,
            "theme": self.theme,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PipelineStoryMetadata":
        """Create instance from dictionary."""
        nodes_data = data.get("nodes", [])
        nodes = [NodeExecutionMetadata.from_dict(n) for n in nodes_data]

        # Filter valid keys
        valid_keys = cls.__annotations__.keys()
        clean_data = {k: v for k, v in data.items() if k in valid_keys}

        # Handle nested
        if "nodes" in clean_data:
            del clean_data["nodes"]

        return cls(nodes=nodes, **clean_data)

    @classmethod
    def from_json(cls, path: str) -> "PipelineStoryMetadata":
        """Load from a JSON file."""
        import json

        with open(path, "r") as f:
            data = json.load(f)
        return cls.from_dict(data)
