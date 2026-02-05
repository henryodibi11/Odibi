"""
Story Metadata Tracking
========================

Tracks detailed metadata for pipeline execution stories.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from odibi.utils.logging_context import get_logging_context


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
    rows_written: Optional[int] = None
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
    error_traceback: Optional[str] = None
    error_traceback_cleaned: Optional[str] = None
    error_suggestions: List[str] = field(default_factory=list)
    error_context: Optional[Dict[str, Any]] = None
    validation_warnings: List[str] = field(default_factory=list)

    # Execution steps (troubleshooting)
    execution_steps: List[str] = field(default_factory=list)

    # Failed rows samples (per validation name -> sample rows)
    failed_rows_samples: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    failed_rows_counts: Dict[str, int] = field(default_factory=dict)
    failed_rows_truncated: bool = False
    truncated_validations: List[str] = field(default_factory=list)

    # Retry history
    retry_history: List[Dict[str, Any]] = field(default_factory=list)

    # Historical Context (Catalog)
    historical_avg_rows: Optional[float] = None
    historical_avg_duration: Optional[float] = None

    # Anomaly Flags (Phase 1 - Triage)
    is_anomaly: bool = False
    anomaly_reasons: List[str] = field(default_factory=list)
    is_slow: bool = False  # 3x slower than historical avg
    has_row_anomaly: bool = False  # ±50% rows vs historical avg

    # Cross-run changes (Phase 3)
    changed_from_last_success: bool = False
    changes_detected: List[str] = field(default_factory=list)  # e.g. ["sql", "schema", "rows"]
    previous_sql_hash: Optional[str] = None
    previous_rows_out: Optional[int] = None
    previous_duration: Optional[float] = None
    previous_config_snapshot: Optional[Dict[str, Any]] = None  # For config diff viewer

    # Duration history for sparkline (last N runs)
    # Format: [{"run_id": "...", "duration": 1.5}, ...]
    duration_history: Optional[List[Dict[str, Any]]] = None

    # Timestamps
    started_at: Optional[str] = None
    completed_at: Optional[str] = None

    # Phase 5: Quality & Documentation
    description: Optional[str] = None  # From NodeConfig.description
    explanation: Optional[str] = None  # Markdown explanation from NodeConfig.explanation
    runbook_url: Optional[str] = None  # From NodeConfig.runbook_url
    column_statistics: Optional[Dict[str, Dict[str, Any]]] = None  # min/max/mean/stddev per column

    # Column drop warning threshold (triggered if >30% columns dropped)
    column_drop_warning: Optional[str] = None  # Warning message if significant columns dropped

    def calculate_row_change(self):
        """Calculate row count change metrics."""
        ctx = get_logging_context()
        if self.rows_in is not None and self.rows_out is not None:
            self.rows_change = self.rows_out - self.rows_in
            if self.rows_in > 0:
                self.rows_change_pct = (self.rows_change / self.rows_in) * 100
            else:
                self.rows_change_pct = 0.0 if self.rows_out == 0 else 100.0
            ctx.debug(
                "Row change calculated",
                node=self.node_name,
                rows_in=self.rows_in,
                rows_out=self.rows_out,
                change=self.rows_change,
                change_pct=self.rows_change_pct,
            )

    def calculate_schema_changes(self):
        """Calculate schema changes between input and output."""
        ctx = get_logging_context()
        if self.schema_in and self.schema_out:
            set_in = set(self.schema_in)
            set_out = set(self.schema_out)

            self.columns_added = list(set_out - set_in)
            self.columns_removed = list(set_in - set_out)

            if self.columns_added or self.columns_removed:
                ctx.debug(
                    "Schema changes detected",
                    node=self.node_name,
                    columns_added=self.columns_added,
                    columns_removed=self.columns_removed,
                )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        base_dict = {
            "node_name": self.node_name,
            "operation": self.operation,
            "status": self.status,
            "duration": self.duration,
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "rows_written": self.rows_written,
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
            "error_traceback": self.error_traceback,
            "error_traceback_cleaned": self.error_traceback_cleaned,
            "validation_warnings": self.validation_warnings,
            "execution_steps": self.execution_steps,
            "failed_rows_samples": self.failed_rows_samples,
            "failed_rows_counts": self.failed_rows_counts,
            "failed_rows_truncated": self.failed_rows_truncated,
            "truncated_validations": self.truncated_validations,
            "retry_history": self.retry_history,
            "historical_avg_rows": self.historical_avg_rows,
            "historical_avg_duration": self.historical_avg_duration,
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
            "is_anomaly": self.is_anomaly,
            "anomaly_reasons": self.anomaly_reasons,
            "is_slow": self.is_slow,
            "has_row_anomaly": self.has_row_anomaly,
            "changed_from_last_success": self.changed_from_last_success,
            "changes_detected": self.changes_detected,
            "previous_sql_hash": self.previous_sql_hash,
            "previous_rows_out": self.previous_rows_out,
            "previous_duration": self.previous_duration,
            "previous_config_snapshot": self.previous_config_snapshot,
            "duration_history": self.duration_history,
            "description": self.description,
            "explanation": self.explanation,
            "runbook_url": self.runbook_url,
            "column_statistics": self.column_statistics,
            "column_drop_warning": self.column_drop_warning,
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
        ctx = get_logging_context()
        ctx.debug(
            "Collecting node metadata from dict",
            node_name=data.get("node_name"),
        )

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
            ctx.debug(
                "Delta version info extracted",
                node_name=data.get("node_name"),
                version=d_info.get("version"),
                operation=d_info.get("operation"),
            )

        # Filter out unknown keys to be safe
        valid_keys = cls.__annotations__.keys()
        clean_data = {k: v for k, v in data.items() if k in valid_keys}

        # Remove nested objects handled separately
        if "delta_info" in clean_data:
            del clean_data["delta_info"]

        # Log data diff collection if present
        if "data_diff" in data and data["data_diff"]:
            ctx.debug(
                "Data diff collected",
                node_name=data.get("node_name"),
                has_added=bool(data["data_diff"].get("added")),
                has_removed=bool(data["data_diff"].get("removed")),
            )

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

    # Graph data for interactive DAG (Phase 2)
    graph_data: Optional[Dict[str, Any]] = None

    # Cross-run comparison (Phase 3)
    change_summary: Optional[Dict[str, Any]] = None
    compared_to_run_id: Optional[str] = None
    git_info: Optional[Dict[str, str]] = None

    def add_node(self, node_metadata: NodeExecutionMetadata):
        """
        Add node execution metadata.

        Args:
            node_metadata: Metadata for the node execution
        """
        ctx = get_logging_context()
        self.nodes.append(node_metadata)
        self.total_nodes += 1

        if node_metadata.status == "success":
            self.completed_nodes += 1
        elif node_metadata.status == "failed":
            self.failed_nodes += 1
        elif node_metadata.status == "skipped":
            self.skipped_nodes += 1

        ctx.debug(
            "Node metadata added to story",
            pipeline=self.pipeline_name,
            node=node_metadata.node_name,
            status=node_metadata.status,
            total_nodes=self.total_nodes,
        )

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

    def get_total_rows_in(self) -> int:
        """Calculate total input rows across all nodes."""
        total = 0
        for node in self.nodes:
            if node.rows_in is not None:
                total += node.rows_in
        return total

    def get_rows_dropped(self) -> int:
        """Calculate total rows dropped (filtered) across all nodes."""
        dropped = 0
        for node in self.nodes:
            if node.rows_in is not None and node.rows_out is not None:
                diff = node.rows_in - node.rows_out
                if diff > 0:
                    dropped += diff
        return dropped

    def get_final_output_rows(self) -> Optional[int]:
        """Get the row count from the last successful node (final output)."""
        for node in reversed(self.nodes):
            if node.status == "success" and node.rows_out is not None:
                return node.rows_out
        return None

    def get_alert_summary(self) -> Dict[str, Any]:
        """Get a summary suitable for alert payloads.

        Returns:
            Dictionary with key metrics for alerts
        """
        return {
            "total_rows_processed": self.get_total_rows_processed(),
            "total_rows_in": self.get_total_rows_in(),
            "rows_dropped": self.get_rows_dropped(),
            "final_output_rows": self.get_final_output_rows(),
            "success_rate": self.get_success_rate(),
            "completed_nodes": self.completed_nodes,
            "failed_nodes": self.failed_nodes,
            "skipped_nodes": self.skipped_nodes,
        }

    def get_failed_node_names(self) -> List[str]:
        """Get names of all failed nodes."""
        return [n.node_name for n in self.nodes if n.status == "failed"]

    def get_first_failure(self) -> Optional["NodeExecutionMetadata"]:
        """Get the first failed node (by execution order)."""
        for node in self.nodes:
            if node.status == "failed":
                return node
        return None

    def get_anomalous_nodes(self) -> List["NodeExecutionMetadata"]:
        """Get all nodes with anomalies (slow or row count deviation)."""
        return [n for n in self.nodes if n.is_anomaly]

    def get_run_health_summary(self) -> Dict[str, Any]:
        """Get run health summary for triage header.

        Returns:
            Dictionary with health info for quick triage
        """
        failed_names = self.get_failed_node_names()
        first_failure = self.get_first_failure()
        anomalous = self.get_anomalous_nodes()

        return {
            "has_failures": len(failed_names) > 0,
            "failed_count": len(failed_names),
            "failed_nodes": failed_names,
            "first_failure_node": first_failure.node_name if first_failure else None,
            "first_failure_error": first_failure.error_message if first_failure else None,
            "first_failure_type": first_failure.error_type if first_failure else None,
            "anomaly_count": len(anomalous),
            "anomalous_nodes": [n.node_name for n in anomalous],
            "overall_status": "failed" if failed_names else "success",
        }

    def get_data_quality_summary(self) -> Dict[str, Any]:
        """Get data quality summary across all nodes.

        Returns:
            Dictionary with quality metrics for Phase 5 Data Quality Summary card
        """
        total_validations_failed = 0
        total_failed_rows = 0
        top_null_columns: List[Dict[str, Any]] = []
        nodes_with_warnings = []

        for node in self.nodes:
            # Count validation warnings
            if node.validation_warnings:
                total_validations_failed += len(node.validation_warnings)
                nodes_with_warnings.append(node.node_name)

            # Sum failed rows
            if node.failed_rows_counts:
                for count in node.failed_rows_counts.values():
                    total_failed_rows += count

            # Collect null profile data
            if node.null_profile:
                for col, null_pct in node.null_profile.items():
                    if null_pct and null_pct > 0:
                        top_null_columns.append(
                            {
                                "node": node.node_name,
                                "column": col,
                                "null_pct": null_pct,
                            }
                        )

        # Sort by null percentage descending and take top 10
        top_null_columns.sort(key=lambda x: x["null_pct"], reverse=True)
        top_null_columns = top_null_columns[:10]

        return {
            "total_validations_failed": total_validations_failed,
            "total_failed_rows": total_failed_rows,
            "top_null_columns": top_null_columns,
            "nodes_with_warnings": nodes_with_warnings,
            "has_quality_issues": total_validations_failed > 0 or total_failed_rows > 0,
        }

    def get_freshness_info(self) -> Optional[Dict[str, Any]]:
        """Get data freshness indicator from date columns.

        Looks for max timestamp in date/timestamp columns from sample data.

        Returns:
            Dictionary with freshness info or None if not available
        """
        latest_timestamp = None
        latest_column = None
        latest_node = None

        date_patterns = ["date", "time", "timestamp", "created", "updated", "modified", "_at"]

        for node in reversed(self.nodes):  # Start from last node (output)
            if node.status != "success" or not node.sample_data:
                continue

            if not node.sample_data:
                continue

            for sample_row in node.sample_data:
                for col, val in sample_row.items():
                    # Check if column name suggests date/time
                    col_lower = col.lower()
                    if not any(p in col_lower for p in date_patterns):
                        continue

                    if val is None:
                        continue

                    # Try to parse as datetime
                    try:
                        from datetime import datetime as dt

                        if isinstance(val, str):
                            # Try common formats
                            for fmt in [
                                "%Y-%m-%d %H:%M:%S",
                                "%Y-%m-%dT%H:%M:%S",
                                "%Y-%m-%d",
                            ]:
                                try:
                                    parsed = dt.strptime(val[:19], fmt)
                                    if latest_timestamp is None or parsed > latest_timestamp:
                                        latest_timestamp = parsed
                                        latest_column = col
                                        latest_node = node.node_name
                                    break
                                except (ValueError, TypeError):
                                    continue
                    except Exception:
                        pass

        if latest_timestamp:
            return {
                "timestamp": latest_timestamp.isoformat(),
                "column": latest_column,
                "node": latest_node,
                "formatted": latest_timestamp.strftime("%Y-%m-%d %H:%M"),
            }
        return None

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
            "graph_data": self.graph_data,
            "change_summary": self.change_summary,
            "compared_to_run_id": self.compared_to_run_id,
            "git_info": self.git_info,
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
