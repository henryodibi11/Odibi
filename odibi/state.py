import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional

try:
    import portalocker
except ImportError:
    portalocker = None

STATE_FILE = ".odibi/state.json"


class StateBackend(ABC):
    @abstractmethod
    def load_state(self) -> Dict[str, Any]:
        """Return state in the current in-memory format, e.g. {'pipelines': {...}}."""
        ...

    @abstractmethod
    def save_pipeline_run(self, pipeline_name: str, pipeline_data: Dict[str, Any]) -> None:
        """Persist the given pipeline_data into backend."""
        ...

    @abstractmethod
    def get_last_run_status(self, pipeline_name: str, node_name: str) -> Optional[bool]:
        """Get success status of a node from last run."""
        ...

    @abstractmethod
    def get_hwm(self, key: str) -> Any:
        """Get High-Water Mark value for a key."""
        ...

    @abstractmethod
    def set_hwm(self, key: str, value: Any) -> None:
        """Set High-Water Mark value for a key."""
        ...


class LocalFileStateBackend(StateBackend):
    def __init__(self, project_root: str = "."):
        self.state_path = Path(project_root) / STATE_FILE
        self.lock_path = self.state_path.with_suffix(".lock")

    def load_state(self) -> Dict[str, Any]:
        if not self.state_path.exists():
            return {"pipelines": {}}
        try:
            with open(self.state_path, "r") as f:
                return json.load(f)
        except Exception:
            return {"pipelines": {}}

    def save_pipeline_run(self, pipeline_name: str, pipeline_data: Dict[str, Any]):
        """Save pipeline run results with concurrency locking."""
        self.state_path.parent.mkdir(parents=True, exist_ok=True)

        if portalocker:
            # Atomic update with lock
            try:
                with portalocker.Lock(self.lock_path, timeout=60):
                    # Refresh state from disk inside lock to prevent overwrites
                    current_state = self.load_state()
                    if "pipelines" not in current_state:
                        current_state["pipelines"] = {}

                    current_state["pipelines"][pipeline_name] = pipeline_data

                    with open(self.state_path, "w") as f:
                        json.dump(current_state, f, indent=2)
            except portalocker.exceptions.LockException:
                print(
                    "Warning: Could not acquire lock for state file. State might be inconsistent."
                )
                self._unsafe_save(pipeline_name, pipeline_data)
        else:
            self._unsafe_save(pipeline_name, pipeline_data)

    def _unsafe_save(self, pipeline_name: str, pipeline_data: Dict[str, Any]):
        """Save without locking (fallback)."""
        # Refresh state to minimize race window
        current_state = self.load_state()
        if "pipelines" not in current_state:
            current_state["pipelines"] = {}

        current_state["pipelines"][pipeline_name] = pipeline_data

        with open(self.state_path, "w") as f:
            json.dump(current_state, f, indent=2)

    def get_last_run_status(self, pipeline_name: str, node_name: str) -> Optional[bool]:
        state = self.load_state()
        pipeline = state.get("pipelines", {}).get(pipeline_name)
        if not pipeline:
            return None

        node = pipeline.get("nodes", {}).get(node_name)
        if not node:
            return None

        return node.get("success")

    def get_hwm(self, key: str) -> Any:
        state = self.load_state()
        return state.get("incremental", {}).get(key)

    def set_hwm(self, key: str, value: Any) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)

        if portalocker:
            try:
                with portalocker.Lock(self.lock_path, timeout=60):
                    current_state = self.load_state()
                    if "incremental" not in current_state:
                        current_state["incremental"] = {}

                    current_state["incremental"][key] = value

                    with open(self.state_path, "w") as f:
                        json.dump(current_state, f, indent=2)
            except portalocker.exceptions.LockException:
                self._unsafe_set_hwm(key, value)
        else:
            self._unsafe_set_hwm(key, value)

    def _unsafe_set_hwm(self, key: str, value: Any):
        current_state = self.load_state()
        if "incremental" not in current_state:
            current_state["incremental"] = {}
        current_state["incremental"][key] = value
        with open(self.state_path, "w") as f:
            json.dump(current_state, f, indent=2)


class DeltaStateBackend(StateBackend):
    def __init__(self, spark_session: Any, table_name: str = "odibi_meta.state"):
        self.spark = spark_session
        self.table_name = table_name
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        try:
            # Check if table exists, if not create it
            # We use a simple schema for state tracking
            self.spark.sql(
                f"""
              CREATE TABLE IF NOT EXISTS {self.table_name} (
                pipeline_name STRING,
                node_name     STRING,
                last_run      TIMESTAMP,
                success       BOOLEAN,
                metadata      STRING,
                updated_at    TIMESTAMP
              )
              USING DELTA
            """
            )
        except Exception:
            # Fallback or log warning?
            # In some environments (e.g. read-only), this might fail.
            pass

    def load_state(self) -> Dict[str, Any]:
        try:
            # Check if table exists using catalog (safer than try/catch on select)
            # But for broad compatibility, a simple SELECT LIMIT 0 or tableExists check is good.
            # Note: tableExists might need catalog qualifier.
            # Let's try reading.

            df = self.spark.table(self.table_name)

            # We need to reconstruct the dict: pipelines -> {pipeline_name} -> nodes -> {node_name}
            # Strategy: Fetch all rows.
            # If table is huge, we should filter by current pipeline?
            # But load_state interface implies full state load.
            # For now, assume metadata table is small enough.

            rows = df.collect()

            state = {"pipelines": {}}
            for row in rows:
                p = row.pipeline_name
                n = row.node_name

                if p not in state["pipelines"]:
                    state["pipelines"][p] = {"nodes": {}, "last_run": None}

                pipeline_dict = state["pipelines"][p]
                pipeline_dict["nodes"][n] = {
                    "success": row.success,
                    "timestamp": row.last_run.isoformat() if row.last_run else None,
                }
                # Metadata? if we stored JSON in metadata col, load it here

            return state
        except Exception:
            # Table might not exist or other error
            return {"pipelines": {}}

    def save_pipeline_run(self, pipeline_name: str, pipeline_data: Dict[str, Any]) -> None:
        from pyspark.sql import functions as F

        # Flatten pipeline_data to rows
        rows = []
        nodes_data = pipeline_data.get("nodes", {})

        if not nodes_data:
            return

        for node_name, node_info in nodes_data.items():
            # Parse timestamp
            ts_str = node_info.get("timestamp")
            # We rely on Spark to handle string-to-timestamp or we parse it?
            # Spark can often handle ISO strings.

            rows.append(
                (
                    pipeline_name,
                    node_name,
                    ts_str,
                    bool(node_info.get("success")),
                    None,  # Metadata
                )
            )

        if not rows:
            return

        # Create DataFrame for updates
        # Define schema to ensure types
        from pyspark.sql.types import (
            BooleanType,
            StringType,
            StructField,
            StructType,
        )

        # Note: ts_str is string here, so we might need to cast or use StringType in schema then cast
        schema = StructType(
            [
                StructField("pipeline_name", StringType(), False),
                StructField("node_name", StringType(), False),
                StructField("last_run_str", StringType(), True),
                StructField("success", BooleanType(), True),
                StructField("metadata", StringType(), True),
            ]
        )

        updates_df = self.spark.createDataFrame(rows, schema)
        updates_df = (
            updates_df.withColumn("last_run", F.to_timestamp(F.col("last_run_str")))
            .withColumn("updated_at", F.current_timestamp())
            .drop("last_run_str")
        )

        # Use MERGE
        # Register temp view
        view_name = f"_odibi_state_updates_{abs(hash(pipeline_name))}"
        updates_df.createOrReplaceTempView(view_name)

        merge_sql = f"""
          MERGE INTO {self.table_name} AS t
          USING {view_name} AS s
          ON t.pipeline_name = s.pipeline_name AND t.node_name = s.node_name
          WHEN MATCHED THEN UPDATE SET
            t.last_run   = s.last_run,
            t.success    = s.success,
            t.metadata   = s.metadata,
            t.updated_at = s.updated_at
          WHEN NOT MATCHED THEN INSERT (pipeline_name, node_name, last_run, success, metadata, updated_at)
          VALUES (s.pipeline_name, s.node_name, s.last_run, s.success, s.metadata, s.updated_at)
        """

        self.spark.sql(merge_sql)

        # Cleanup
        self.spark.catalog.dropTempView(view_name)

    def get_last_run_status(self, pipeline_name: str, node_name: str) -> Optional[bool]:
        try:
            from pyspark.sql import functions as F

            df = self.spark.table(self.table_name)
            row = (
                df.filter(
                    (F.col("pipeline_name") == pipeline_name) & (F.col("node_name") == node_name)
                )
                .select("success")
                .first()
            )

            if row:
                return row.success
            return None
        except Exception:
            return None

    def get_hwm(self, key: str) -> Any:
        try:
            from pyspark.sql import functions as F

            df = self.spark.table(self.table_name)
            # Use specific pipeline name for HWM
            row = (
                df.filter((F.col("pipeline_name") == "_incremental") & (F.col("node_name") == key))
                .select("metadata")
                .first()
            )

            if row and row.metadata:
                try:
                    data = json.loads(row.metadata)
                    return data.get("hwm")
                except Exception:
                    return None
            return None
        except Exception:
            return None

    def set_hwm(self, key: str, value: Any) -> None:
        # Reuse logic from save_pipeline_run but simpler
        # We construct a "pipeline_run" dict that matches what save_pipeline_run expects?
        # No, save_pipeline_run flattens.
        # Let's implement set_hwm directly using MERGE.

        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            BooleanType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        # Prepare metadata JSON
        metadata_json = json.dumps({"hwm": value})

        # Row: pipeline="_incremental", node=key, last_run=None, success=True, metadata=json
        rows = [
            (
                "_incremental",
                key,
                None,  # last_run timestamp
                True,  # success (implied)
                metadata_json,
            )
        ]

        schema = StructType(
            [
                StructField("pipeline_name", StringType(), False),
                StructField("node_name", StringType(), False),
                StructField("last_run", TimestampType(), True),
                StructField("success", BooleanType(), True),
                StructField("metadata", StringType(), True),
            ]
        )

        updates_df = self.spark.createDataFrame(rows, schema)
        updates_df = updates_df.withColumn("updated_at", F.current_timestamp())

        view_name = f"_odibi_hwm_updates_{abs(hash(key))}"
        updates_df.createOrReplaceTempView(view_name)

        merge_sql = f"""
          MERGE INTO {self.table_name} AS t
          USING {view_name} AS s
          ON t.pipeline_name = s.pipeline_name AND t.node_name = s.node_name
          WHEN MATCHED THEN UPDATE SET
            t.metadata   = s.metadata,
            t.updated_at = s.updated_at
          WHEN NOT MATCHED THEN INSERT (pipeline_name, node_name, last_run, success, metadata, updated_at)
          VALUES (s.pipeline_name, s.node_name, s.last_run, s.success, s.metadata, s.updated_at)
        """

        try:
            self.spark.sql(merge_sql)
        except Exception:
            # If table doesn't exist or other issue
            pass
        finally:
            self.spark.catalog.dropTempView(view_name)


class StateManager:
    """Manages execution state for checkpointing."""

    def __init__(self, project_root: str = ".", backend: Optional[StateBackend] = None):
        self.backend = backend or LocalFileStateBackend(project_root)
        self.state: Dict[str, Any] = self.backend.load_state()

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

        self.backend.save_pipeline_run(pipeline_name, pipeline_data)
        # Refresh in-memory state
        self.state = self.backend.load_state()

    def get_last_run_status(self, pipeline_name: str, node_name: str) -> Optional[bool]:
        """Get success status of a node from last run.

        Returns:
            True if success, False if failed, None if not found
        """
        return self.backend.get_last_run_status(pipeline_name, node_name)

    def get_hwm(self, key: str) -> Any:
        """Get High-Water Mark value for a key."""
        return self.backend.get_hwm(key)

    def set_hwm(self, key: str, value: Any) -> None:
        """Set High-Water Mark value for a key."""
        self.backend.set_hwm(key, value)
