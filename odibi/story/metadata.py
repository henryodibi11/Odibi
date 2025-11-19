"""
Story Metadata Tracking
========================

Tracks detailed metadata for pipeline execution stories.
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from datetime import datetime


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
    sample_data: Optional[List[Dict[str, Any]]] = None

    # Schema tracking
    schema_in: Optional[List[str]] = None
    schema_out: Optional[List[str]] = None
    columns_added: List[str] = field(default_factory=list)
    columns_removed: List[str] = field(default_factory=list)
    columns_renamed: List[str] = field(default_factory=list)

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
        return {
            "node_name": self.node_name,
            "operation": self.operation,
            "status": self.status,
            "duration": self.duration,
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "rows_change": self.rows_change,
            "rows_change_pct": self.rows_change_pct,
            "sample_data": self.sample_data,
            "schema_in": self.schema_in,
            "schema_out": self.schema_out,
            "columns_added": self.columns_added,
            "columns_removed": self.columns_removed,
            "error_message": self.error_message,
            "error_type": self.error_type,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        }


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
