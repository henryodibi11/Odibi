import json
from typing import Dict, Any, Optional
from pathlib import Path

STATE_FILE = ".odibi/state.json"


class StateManager:
    """Manages execution state for checkpointing."""

    def __init__(self, project_root: str = "."):
        self.state_path = Path(project_root) / STATE_FILE
        self.state: Dict[str, Any] = self._load_state()

    def _load_state(self) -> Dict[str, Any]:
        if not self.state_path.exists():
            return {"pipelines": {}}
        try:
            with open(self.state_path, "r") as f:
                return json.load(f)
        except Exception:
            return {"pipelines": {}}

    def save_pipeline_run(self, pipeline_name: str, results: Any):
        """Save pipeline run results."""
        # results is PipelineResults object
        # We need to serialize it

        # Convert results to dict if it's an object
        if hasattr(results, "to_dict"):
            data = results.to_dict()
        else:
            data = results

        # We store the status of each node
        # We need to know if a node succeeded
        node_status = {}
        if hasattr(results, "node_results"):
            for name, res in results.node_results.items():
                node_status[name] = {
                    "success": res.success,
                    "timestamp": res.metadata.get("timestamp"),
                }

        self.state["pipelines"][pipeline_name] = {
            "last_run": data.get("end_time"),
            "nodes": node_status,
        }

        self._save()

    def _save(self):
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_path, "w") as f:
            json.dump(self.state, f, indent=2)

    def get_last_run_status(self, pipeline_name: str, node_name: str) -> Optional[bool]:
        """Get success status of a node from last run.

        Returns:
            True if success, False if failed, None if not found
        """
        pipeline = self.state["pipelines"].get(pipeline_name)
        if not pipeline:
            return None

        node = pipeline.get("nodes", {}).get(node_name)
        if not node:
            return None

        return node.get("success")
