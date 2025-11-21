import json
from typing import Dict, Any, Optional
from pathlib import Path

try:
    import portalocker
except ImportError:
    portalocker = None

STATE_FILE = ".odibi/state.json"


class StateManager:
    """Manages execution state for checkpointing."""

    def __init__(self, project_root: str = "."):
        self.state_path = Path(project_root) / STATE_FILE
        self.lock_path = self.state_path.with_suffix(".lock")
        # Load initial state (read-only view)
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
        """Save pipeline run results with concurrency locking."""
        # results is PipelineResults object
        if hasattr(results, "to_dict"):
            data = results.to_dict()
        else:
            data = results

        # Node status
        node_status = {}
        if hasattr(results, "node_results"):
            for name, res in results.node_results.items():
                node_status[name] = {
                    "success": res.success,
                    "timestamp": res.metadata.get("timestamp"),
                }

        pipeline_data = {
            "last_run": data.get("end_time"),
            "nodes": node_status,
        }

        self.state_path.parent.mkdir(parents=True, exist_ok=True)

        if portalocker:
            # Atomic update with lock
            try:
                with portalocker.Lock(self.lock_path, timeout=60):
                    # Refresh state from disk inside lock to prevent overwrites
                    self.state = self._load_state()

                    self.state["pipelines"][pipeline_name] = pipeline_data

                    with open(self.state_path, "w") as f:
                        json.dump(self.state, f, indent=2)
            except portalocker.exceptions.LockException:
                # Fallback or raise?
                # For state file, maybe we shouldn't crash the pipeline if lock fails,
                # but it risks corruption. Better to log warning and try to write?
                print(
                    "Warning: Could not acquire lock for state file. State might be inconsistent."
                )
                self._unsafe_save(pipeline_name, pipeline_data)
        else:
            self._unsafe_save(pipeline_name, pipeline_data)

    def _unsafe_save(self, pipeline_name: str, pipeline_data: Dict[str, Any]):
        """Save without locking (fallback)."""
        # Refresh state to minimize race window
        self.state = self._load_state()
        self.state["pipelines"][pipeline_name] = pipeline_data

        with open(self.state_path, "w") as f:
            json.dump(self.state, f, indent=2)

    def _save(self):
        """Deprecated internal save."""
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
