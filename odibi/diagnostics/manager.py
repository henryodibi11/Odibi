"""
Diagnostics Manager
===================

Handles loading and managing run history for diagnostics.
"""

import json
from pathlib import Path
from typing import List, Optional, Dict
from datetime import datetime

from odibi.story.metadata import PipelineStoryMetadata, NodeExecutionMetadata, DeltaWriteInfo


class HistoryManager:
    """Manages access to pipeline run history."""

    def __init__(self, history_path: str = "stories/"):
        """
        Initialize history manager.

        Args:
            history_path: Path where stories are stored
        """
        self.history_path = Path(history_path)
        self.is_remote = "://" in history_path

    def list_runs(self, pipeline_name: str) -> List[Dict[str, str]]:
        """
        List available runs for a pipeline.

        Returns:
            List of dicts with keys: run_id, timestamp, path
        """
        runs = []

        if self.is_remote:
            # Remote listing not implemented yet
            return []

        if not self.history_path.exists():
            return []

        # Look for .json files
        # Pattern: {pipeline_name}_{timestamp}.json
        pattern = f"{pipeline_name}_*.json"

        for path in self.history_path.glob(pattern):
            try:
                # Parse timestamp from filename
                # Filename: name_YYYYMMDD_HHMMSS.json
                parts = path.stem.split("_")
                if len(parts) >= 3:
                    ts_str = f"{parts[-2]}_{parts[-1]}"
                    # Validate format
                    datetime.strptime(ts_str, "%Y%m%d_%H%M%S")

                    runs.append({"run_id": ts_str, "timestamp": ts_str, "path": str(path)})
            except (ValueError, IndexError):
                continue

        # Sort by timestamp descending (newest first)
        runs.sort(key=lambda x: x["timestamp"], reverse=True)
        return runs

    def get_latest_run(self, pipeline_name: str) -> Optional[PipelineStoryMetadata]:
        """Get the most recent run metadata."""
        runs = self.list_runs(pipeline_name)
        if not runs:
            return None

        return self.load_run(runs[0]["path"])

    def get_run_by_id(self, pipeline_name: str, run_id: str) -> Optional[PipelineStoryMetadata]:
        """Get specific run metadata."""
        runs = self.list_runs(pipeline_name)
        for run in runs:
            if run["run_id"] == run_id:
                return self.load_run(run["path"])
        return None

    def get_previous_run(
        self, pipeline_name: str, current_run_id: str
    ) -> Optional[PipelineStoryMetadata]:
        """Get the run immediately preceding the specified one."""
        runs = self.list_runs(pipeline_name)

        found_current = False
        for run in runs:
            if found_current:
                return self.load_run(run["path"])

            if run["run_id"] == current_run_id:
                found_current = True

        return None

    def load_run(self, path: str) -> PipelineStoryMetadata:
        """Load run metadata from JSON file."""
        if self.is_remote:
            raise NotImplementedError("Remote history loading not supported yet")

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        return self._dict_to_metadata(data)

    def _dict_to_metadata(self, data: Dict) -> PipelineStoryMetadata:
        """Convert dictionary to PipelineStoryMetadata object."""
        nodes = []
        for n in data.get("nodes", []):
            # Reconstruct Delta Info
            delta_info = None
            if n.get("delta_info"):
                d = n["delta_info"]
                delta_info = DeltaWriteInfo(
                    version=d.get("version"),
                    timestamp=(
                        datetime.fromisoformat(d.get("timestamp")) if d.get("timestamp") else None
                    ),
                    operation=d.get("operation"),
                    operation_metrics=d.get("operation_metrics"),
                    read_version=d.get("read_version"),
                )

            node = NodeExecutionMetadata(
                node_name=n["node_name"],
                operation=n.get("operation", "unknown"),
                status=n.get("status", "unknown"),
                duration=n.get("duration", 0.0),
                rows_in=n.get("rows_in"),
                rows_out=n.get("rows_out"),
                rows_change=n.get("rows_change"),
                rows_change_pct=n.get("rows_change_pct"),
                sample_data=n.get("sample_data"),
                schema_in=n.get("schema_in"),
                schema_out=n.get("schema_out"),
                columns_added=n.get("columns_added", []),
                columns_removed=n.get("columns_removed", []),
                columns_renamed=n.get("columns_renamed", []),
                executed_sql=n.get("executed_sql", []),
                sql_hash=n.get("sql_hash"),
                transformation_stack=n.get("transformation_stack", []),
                config_snapshot=n.get("config_snapshot"),
                delta_info=delta_info,
                data_diff=n.get("data_diff"),
                error_message=n.get("error_message"),
                error_type=n.get("error_type"),
                started_at=n.get("started_at"),
                completed_at=n.get("completed_at"),
            )
            nodes.append(node)

        return PipelineStoryMetadata(
            pipeline_name=data["pipeline_name"],
            pipeline_layer=data.get("pipeline_layer"),
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
            duration=data.get("duration", 0.0),
            total_nodes=data.get("total_nodes", 0),
            completed_nodes=data.get("completed_nodes", 0),
            failed_nodes=data.get("failed_nodes", 0),
            skipped_nodes=data.get("skipped_nodes", 0),
            nodes=nodes,
            project=data.get("project"),
            plant=data.get("plant"),
            asset=data.get("asset"),
            business_unit=data.get("business_unit"),
            theme=data.get("theme", "default"),
        )
