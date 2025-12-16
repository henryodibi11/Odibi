import json
import logging
import os
import random
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def _retry_delta_operation(func, max_retries: int = 5, base_delay: float = 1.0):
    """Retry a Delta operation with exponential backoff on concurrency conflicts.

    Only logs debug during retries. Raises after all retries fail.

    Args:
        func: Callable to execute.
        max_retries: Maximum retry attempts (default 5 for high concurrency).
        base_delay: Base delay in seconds (doubles each retry).
    """
    for attempt in range(max_retries + 1):
        try:
            return func()
        except Exception as e:
            error_str = str(e)
            is_concurrent = any(
                msg in error_str
                for msg in [
                    "ConcurrentAppendException",
                    "ConcurrentDeleteReadException",
                    "ConcurrentDeleteDeleteException",
                    "DELTA_CONCURRENT",
                    "concurrent",
                    "conflict",
                ]
            )
            if not is_concurrent or attempt >= max_retries:
                raise
            # Exponential backoff with jitter (1s, 2s, 4s, 8s, 16s = ~31s total)
            delay = base_delay * (2**attempt) + random.uniform(0, 1.0)
            logger.debug(
                f"Delta concurrent write (attempt {attempt + 1}/{max_retries + 1}), "
                f"retrying in {delay:.2f}s..."
            )
            time.sleep(delay)


# Suppress noisy delta-rs transaction conflict warnings (handled by retry)
# Must be set before deltalake is imported
if "RUST_LOG" not in os.environ:
    os.environ["RUST_LOG"] = "deltalake_core::kernel::transaction=error"

# Try to import deltalake, but don't fail yet (it might be a Spark run)
try:
    import pandas as pd
    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake
except ImportError:
    DeltaTable = None
    write_deltalake = None
    pd = None
    pa = None


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
    def get_last_run_info(self, pipeline_name: str, node_name: str) -> Optional[Dict[str, Any]]:
        """Get status and metadata of a node from last run."""
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

    def set_hwm_batch(self, updates: List[Dict[str, Any]]) -> None:
        """Set multiple High-Water Mark values in a single operation.

        Default implementation calls set_hwm() for each update.
        Subclasses should override for efficient batch writes.

        Args:
            updates: List of dicts with keys: key, value
        """
        for update in updates:
            self.set_hwm(update["key"], update["value"])


class LocalJSONStateBackend(StateBackend):
    """
    Local JSON-based State Backend.
    Used for local development or when System Catalog is not configured.
    """

    def __init__(self, state_path: str):
        self.state_path = state_path
        self.state = self._load_from_disk()

    def _load_from_disk(self) -> Dict[str, Any]:
        if os.path.exists(self.state_path):
            try:
                with open(self.state_path, "r") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load state from {self.state_path}: {e}")
        return {"pipelines": {}, "hwm": {}}

    def _save_to_disk(self) -> None:
        os.makedirs(os.path.dirname(self.state_path), exist_ok=True)
        with open(self.state_path, "w") as f:
            json.dump(self.state, f, indent=2, default=str)

    def load_state(self) -> Dict[str, Any]:
        return self.state

    def save_pipeline_run(self, pipeline_name: str, pipeline_data: Dict[str, Any]) -> None:
        if "pipelines" not in self.state:
            self.state["pipelines"] = {}
        self.state["pipelines"][pipeline_name] = pipeline_data
        self._save_to_disk()

    def get_last_run_info(self, pipeline_name: str, node_name: str) -> Optional[Dict[str, Any]]:
        pipe = self.state.get("pipelines", {}).get(pipeline_name, {})
        nodes = pipe.get("nodes", {})
        return nodes.get(node_name)

    def get_last_run_status(self, pipeline_name: str, node_name: str) -> Optional[bool]:
        info = self.get_last_run_info(pipeline_name, node_name)
        if info:
            return info.get("success")
        return None

    def get_hwm(self, key: str) -> Any:
        return self.state.get("hwm", {}).get(key)

    def set_hwm(self, key: str, value: Any) -> None:
        if "hwm" not in self.state:
            self.state["hwm"] = {}
        self.state["hwm"][key] = value
        self._save_to_disk()


class CatalogStateBackend(StateBackend):
    """
    Unified State Backend using Delta Tables (System Catalog).
    Supports both Spark and Local (via deltalake) execution.
    """

    def __init__(
        self,
        meta_runs_path: str,
        meta_state_path: str,
        spark_session: Any = None,
        storage_options: Optional[Dict[str, str]] = None,
    ):
        self.meta_runs_path = meta_runs_path
        self.meta_state_path = meta_state_path
        self.spark = spark_session
        self.storage_options = storage_options or {}

    def load_state(self) -> Dict[str, Any]:
        """
        Load state. For Catalog backend, we generally return empty
        and rely on direct queries for specific info.
        """
        return {"pipelines": {}}

    def save_pipeline_run(self, pipeline_name: str, pipeline_data: Dict[str, Any]) -> None:
        # CatalogManager already logs runs (meta_runs) during execution.
        # We do not need to duplicate this here, avoiding schema conflicts.
        pass

    def _save_runs_spark(self, rows):
        pass

    def _save_runs_local(self, rows):
        pass

    def get_last_run_info(self, pipeline_name: str, node_name: str) -> Optional[Dict[str, Any]]:
        if self.spark:
            return self._get_last_run_spark(pipeline_name, node_name)
        return self._get_last_run_local(pipeline_name, node_name)

    def _get_last_run_spark(self, pipeline_name, node_name):
        from pyspark.sql import functions as F

        try:
            df = self.spark.read.format("delta").load(self.meta_runs_path)
            row = (
                df.filter(
                    (F.col("pipeline_name") == pipeline_name) & (F.col("node_name") == node_name)
                )
                .select("status", "metadata")
                .orderBy(F.col("timestamp").desc())
                .first()
            )
            if row:
                meta = {}
                if row.metadata:
                    try:
                        meta = json.loads(row.metadata)
                    except Exception as e:
                        logger.debug(f"Failed to parse metadata JSON: {e}")
                return {"success": (row.status == "SUCCESS"), "metadata": meta}
        except Exception as e:
            logger.warning(
                f"Failed to get last run info from {self.meta_runs_path} "
                f"for {pipeline_name}/{node_name}: {e}"
            )
        return None

    def _get_last_run_local(self, pipeline_name, node_name):
        if not DeltaTable:
            return None

        try:
            dt = DeltaTable(self.meta_runs_path, storage_options=self.storage_options)
            ds = dt.to_pyarrow_dataset()
            import pyarrow.compute as pc

            filter_expr = (pc.field("pipeline_name") == pipeline_name) & (
                pc.field("node_name") == node_name
            )
            # Scan with filter
            table = ds.to_table(filter=filter_expr)

            if table.num_rows == 0:
                return None

            # Sort by timestamp desc to get latest
            # PyArrow table sort? Convert to pandas for easier sorting if small history
            # Or use duckdb

            df = table.to_pandas()
            if "timestamp" in df.columns:
                df = df.sort_values("timestamp", ascending=False)

            row = df.iloc[0]

            meta = {}
            if row.get("metadata"):
                try:
                    meta = json.loads(row["metadata"])
                except Exception as e:
                    logger.debug(f"Failed to parse metadata JSON: {e}")

            status = row.get("status")
            return {"success": (status == "SUCCESS"), "metadata": meta}

        except Exception as e:
            logger.warning(
                f"Failed to get last run info from {self.meta_runs_path} "
                f"for {pipeline_name}/{node_name}: {e}"
            )
            return None

    def get_last_run_status(self, pipeline_name: str, node_name: str) -> Optional[bool]:
        info = self.get_last_run_info(pipeline_name, node_name)
        if info:
            return info.get("success")
        return None

    def get_hwm(self, key: str) -> Any:
        if self.spark:
            return self._get_hwm_spark(key)
        return self._get_hwm_local(key)

    def _get_hwm_spark(self, key):
        from pyspark.sql import functions as F

        try:
            df = self.spark.read.format("delta").load(self.meta_state_path)
            row = df.filter(F.col("key") == key).select("value").first()
            if row and row.value:
                try:
                    return json.loads(row.value)
                except Exception as e:
                    logger.debug(f"Failed to parse HWM value as JSON for key '{key}': {e}")
                    return row.value
        except Exception as e:
            error_str = str(e)
            if "PATH_NOT_FOUND" in error_str or "does not exist" in error_str.lower():
                logger.debug(
                    f"HWM state table does not exist yet at {self.meta_state_path}. "
                    "It will be created on first write."
                )
            else:
                logger.warning(
                    f"Failed to get HWM for key '{key}' from {self.meta_state_path}: {e}"
                )
        return None

    def _get_hwm_local(self, key):
        if not DeltaTable:
            return None
        try:
            dt = DeltaTable(self.meta_state_path, storage_options=self.storage_options)
            ds = dt.to_pyarrow_dataset()
            import pyarrow.compute as pc

            filter_expr = pc.field("key") == key
            table = ds.to_table(filter=filter_expr)

            if table.num_rows == 0:
                return None

            val_str = table.column("value")[0].as_py()
            if val_str:
                try:
                    return json.loads(val_str)
                except Exception as e:
                    logger.debug(f"Failed to parse HWM value as JSON for key '{key}': {e}")
                    return val_str
        except Exception as e:
            logger.warning(f"Failed to get HWM for key '{key}' from {self.meta_state_path}: {e}")
        return None

    def set_hwm(self, key: str, value: Any) -> None:
        val_str = json.dumps(value, default=str)
        row = {"key": key, "value": val_str, "updated_at": datetime.now(timezone.utc)}

        def _do_set():
            if self.spark:
                self._set_hwm_spark(row)
            else:
                self._set_hwm_local(row)

        _retry_delta_operation(_do_set)

    def _set_hwm_spark(self, row):
        from pyspark.sql.types import StringType, StructField, StructType, TimestampType

        schema = StructType(
            [
                StructField("key", StringType(), False),
                StructField("value", StringType(), True),
                StructField("updated_at", TimestampType(), True),
            ]
        )

        updates_df = self.spark.createDataFrame([row], schema)

        if not self._spark_table_exists(self.meta_state_path):
            updates_df.write.format("delta").mode("overwrite").save(self.meta_state_path)
            return

        view_name = f"_odibi_hwm_updates_{abs(hash(row['key']))}"
        updates_df.createOrReplaceTempView(view_name)

        merge_sql = f"""
          MERGE INTO delta.`{self.meta_state_path}` AS t
          USING {view_name} AS s
          ON t.key = s.key
          WHEN MATCHED THEN UPDATE SET
            t.value = s.value,
            t.updated_at = s.updated_at
          WHEN NOT MATCHED THEN INSERT *
        """
        self.spark.sql(merge_sql)
        self.spark.catalog.dropTempView(view_name)

    def _set_hwm_local(self, row):
        if not DeltaTable:
            raise ImportError("deltalake library is required for local state backend.")

        df = pd.DataFrame([row])
        df["updated_at"] = pd.to_datetime(df["updated_at"])

        try:
            dt = DeltaTable(self.meta_state_path, storage_options=self.storage_options)
            (
                dt.merge(
                    source=df,
                    predicate="target.key = source.key",
                    source_alias="source",
                    target_alias="target",
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
        except (ValueError, Exception):
            write_deltalake(
                self.meta_state_path,
                df,
                mode="append",
                storage_options=self.storage_options,
                schema_mode="merge",
            )

    def _spark_table_exists(self, path: str) -> bool:
        try:
            return self.spark.read.format("delta").load(path).count() >= 0
        except Exception as e:
            logger.debug(f"Table does not exist at {path}: {e}")
            return False

    def set_hwm_batch(self, updates: List[Dict[str, Any]]) -> None:
        """Set multiple High-Water Mark values in a single MERGE operation.

        This is much more efficient than calling set_hwm() for each update
        individually, especially when running parallel pipelines with many nodes.

        Args:
            updates: List of dicts with keys: key, value
        """
        if not updates:
            return

        timestamp = datetime.utcnow()
        rows = [
            {
                "key": u["key"],
                "value": json.dumps(u["value"], default=str),
                "updated_at": timestamp,
            }
            for u in updates
        ]

        def _do_batch_set():
            if self.spark:
                self._set_hwm_batch_spark(rows)
            else:
                self._set_hwm_batch_local(rows)

        _retry_delta_operation(_do_batch_set)

    def _set_hwm_batch_spark(self, rows: List[Dict[str, Any]]) -> None:
        from pyspark.sql.types import StringType, StructField, StructType, TimestampType

        schema = StructType(
            [
                StructField("key", StringType(), False),
                StructField("value", StringType(), True),
                StructField("updated_at", TimestampType(), True),
            ]
        )

        updates_df = self.spark.createDataFrame(rows, schema)

        if not self._spark_table_exists(self.meta_state_path):
            updates_df.write.format("delta").mode("overwrite").save(self.meta_state_path)
            return

        view_name = "_odibi_hwm_batch_updates"
        updates_df.createOrReplaceTempView(view_name)

        merge_sql = f"""
          MERGE INTO delta.`{self.meta_state_path}` AS t
          USING {view_name} AS s
          ON t.key = s.key
          WHEN MATCHED THEN UPDATE SET
            t.value = s.value,
            t.updated_at = s.updated_at
          WHEN NOT MATCHED THEN INSERT *
        """
        self.spark.sql(merge_sql)
        self.spark.catalog.dropTempView(view_name)
        logger.debug(f"Batch set {len(rows)} HWM value(s) via Spark")

    def _set_hwm_batch_local(self, rows: List[Dict[str, Any]]) -> None:
        if not DeltaTable:
            raise ImportError("deltalake library is required for local state backend.")

        df = pd.DataFrame(rows)
        df["updated_at"] = pd.to_datetime(df["updated_at"])

        try:
            dt = DeltaTable(self.meta_state_path, storage_options=self.storage_options)
            (
                dt.merge(
                    source=df,
                    predicate="target.key = source.key",
                    source_alias="source",
                    target_alias="target",
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
        except (ValueError, Exception):
            write_deltalake(
                self.meta_state_path,
                df,
                mode="append",
                storage_options=self.storage_options,
                schema_mode="merge",
            )
        logger.debug(f"Batch set {len(rows)} HWM value(s) locally")


class StateManager:
    """Manages execution state for checkpointing."""

    def __init__(self, project_root: str = ".", backend: Optional[StateBackend] = None):
        self.backend = backend
        # Note: If backend is None, it should be injected.
        # But we won't fallback to LocalFileStateBackend here anymore as it's removed.
        if not self.backend:
            raise ValueError("StateBackend must be provided to StateManager")

        self.state: Dict[str, Any] = self.backend.load_state()

    def save_pipeline_run(self, pipeline_name: str, results: Any):
        """Save pipeline run results."""
        if hasattr(results, "to_dict"):
            data = results.to_dict()
        else:
            data = results

        node_status = {}
        if hasattr(results, "node_results"):
            for name, res in results.node_results.items():
                node_status[name] = {
                    "success": res.success,
                    "timestamp": res.metadata.get("timestamp"),
                    "metadata": res.metadata,
                }

        pipeline_data = {
            "last_run": data.get("end_time"),
            "nodes": node_status,
        }

        self.backend.save_pipeline_run(pipeline_name, pipeline_data)
        self.state = self.backend.load_state()

    def get_last_run_info(self, pipeline_name: str, node_name: str) -> Optional[Dict[str, Any]]:
        """Get status and metadata of a node from last run."""
        return self.backend.get_last_run_info(pipeline_name, node_name)

    def get_last_run_status(self, pipeline_name: str, node_name: str) -> Optional[bool]:
        """Get success status of a node from last run."""
        return self.backend.get_last_run_status(pipeline_name, node_name)

    def get_hwm(self, key: str) -> Any:
        """Get High-Water Mark value for a key."""
        return self.backend.get_hwm(key)

    def set_hwm(self, key: str, value: Any) -> None:
        """Set High-Water Mark value for a key."""
        self.backend.set_hwm(key, value)

    def set_hwm_batch(self, updates: List[Dict[str, Any]]) -> None:
        """Set multiple High-Water Mark values in a single operation.

        Args:
            updates: List of dicts with keys: key, value
        """
        self.backend.set_hwm_batch(updates)


def create_state_backend(
    config: Any, project_root: str = ".", spark_session: Any = None
) -> StateBackend:
    """
    Factory to create state backend from ProjectConfig.

    Args:
        config: ProjectConfig object
        project_root: Root directory for local files
        spark_session: Optional SparkSession for Delta backend

    Returns:
        Configured StateBackend
    """
    # Fallback to Local JSON if no System Config
    if not config.system:
        import logging

        logger = logging.getLogger(__name__)
        logger.warning(
            "No system catalog configured. Using local JSON state backend (local-only mode)."
        )
        state_path = os.path.join(project_root, ".odibi", "state.json")
        return LocalJSONStateBackend(state_path)

    system_conn_name = config.system.connection
    conn_config = config.connections.get(system_conn_name)

    if not conn_config:
        raise ValueError(f"System connection '{system_conn_name}' not found.")

    # Helper to get attribute from dict or object
    def _get(obj, key, default=None):
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)

    base_uri = ""
    storage_options = {}

    conn_type = _get(conn_config, "type")

    # Determine Base URI based on connection type
    if conn_type == "local":
        base_path = _get(conn_config, "base_path")
        if not os.path.isabs(base_path):
            base_path = os.path.join(project_root, base_path)

        # Ensure directory exists
        try:
            os.makedirs(base_path, exist_ok=True)
        except Exception:
            pass

        base_uri = os.path.join(base_path, config.system.path)

    elif conn_type == "azure_blob":
        # Construct abfss://
        account = _get(conn_config, "account_name")
        container = _get(conn_config, "container")
        base_uri = f"abfss://{container}@{account}.dfs.core.windows.net/{config.system.path}"

        # Set up storage options
        # Depends on auth mode
        auth = _get(conn_config, "auth", {})
        auth_mode = _get(auth, "mode")
        if auth_mode == "account_key":
            storage_options = {
                "account_name": account,
                "account_key": _get(auth, "account_key"),
            }
        elif auth_mode == "sas":
            storage_options = {
                "account_name": account,
                "sas_token": _get(auth, "sas_token"),
            }
        # For MSI/KeyVault, it's more complex for deltalake-python without extra config
        # But Spark handles it if configured in environment

    else:
        # Fallback for other types or throw error if not supported for system catalog
        # For simplicity, try to treat as local path if it looks like one?
        # Or raise error
        # Assuming local or azure blob for now as they are main supported backends
        # If delta connection?
        if conn_type == "delta":
            # If the connection itself is delta, it might point to a catalog/schema
            # But system catalog needs specific path structure.
            # For now assume system connection is a storage connection.
            pass

    if not base_uri:
        # Default fallback if something went wrong or unsupported
        base_uri = os.path.join(project_root, ".odibi/system")

    meta_state_path = f"{base_uri}/meta_state"
    meta_runs_path = f"{base_uri}/meta_runs"

    return CatalogStateBackend(
        meta_runs_path=meta_runs_path,
        meta_state_path=meta_state_path,
        spark_session=spark_session,
        storage_options=storage_options,
    )
