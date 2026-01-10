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
        environment: Optional[str] = None,
    ):
        self.meta_runs_path = meta_runs_path
        self.meta_state_path = meta_state_path
        self.spark = spark_session
        self.storage_options = storage_options or {}
        self.environment = environment

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
        row = {
            "key": key,
            "value": val_str,
            "environment": self.environment,
            "updated_at": datetime.now(timezone.utc),
        }

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
                StructField("environment", StringType(), True),
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
            t.environment = s.environment,
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

        timestamp = datetime.now(timezone.utc)
        rows = [
            {
                "key": u["key"],
                "value": json.dumps(u["value"], default=str),
                "environment": self.environment,
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
                StructField("environment", StringType(), True),
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
            t.environment = s.environment,
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


class SqlServerSystemBackend(StateBackend):
    """
    SQL Server State Backend for centralized system tables.

    Stores meta_runs and meta_state in SQL Server tables for cross-environment
    visibility and querying. Useful when you want a single source of truth
    for pipeline observability across dev/qat/prod environments.

    Example config:
    ```yaml
    system:
      connection: sql_server
      schema_name: odibi_system
      environment: prod
    ```
    """

    # SQL Server table DDL
    META_RUNS_DDL = """
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'meta_runs' AND schema_id = SCHEMA_ID(:schema))
    BEGIN
        CREATE TABLE [{schema}].[meta_runs] (
            run_id NVARCHAR(100),
            pipeline_name NVARCHAR(255),
            node_name NVARCHAR(255),
            status NVARCHAR(50),
            rows_processed BIGINT,
            duration_ms BIGINT,
            metrics_json NVARCHAR(MAX),
            environment NVARCHAR(50),
            timestamp DATETIME2,
            date DATE
        )
    END
    """

    META_STATE_DDL = """
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'meta_state' AND schema_id = SCHEMA_ID(:schema))
    BEGIN
        CREATE TABLE [{schema}].[meta_state] (
            [key] NVARCHAR(500) PRIMARY KEY,
            [value] NVARCHAR(MAX),
            environment NVARCHAR(50),
            updated_at DATETIME2
        )
    END
    """

    def __init__(
        self,
        connection: Any,
        schema_name: str = "odibi_system",
        environment: Optional[str] = None,
    ):
        """
        Initialize SQL Server System Backend.

        Args:
            connection: AzureSQL connection object
            schema_name: Schema for system tables (default: odibi_system)
            environment: Environment tag for records (e.g., 'dev', 'prod')
        """
        self.connection = connection
        self.schema_name = schema_name
        self.environment = environment
        self._tables_created = False

    def _ensure_tables(self) -> None:
        """Create system tables if they don't exist."""
        if self._tables_created:
            return

        try:
            # Create schema if not exists
            schema_ddl = f"""
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{self.schema_name}')
            BEGIN
                EXEC('CREATE SCHEMA [{self.schema_name}]')
            END
            """
            self.connection.execute(schema_ddl)

            # Create tables
            runs_ddl = self.META_RUNS_DDL.replace("{schema}", self.schema_name).replace(
                ":schema", f"'{self.schema_name}'"
            )
            self.connection.execute(runs_ddl)

            state_ddl = self.META_STATE_DDL.replace("{schema}", self.schema_name).replace(
                ":schema", f"'{self.schema_name}'"
            )
            self.connection.execute(state_ddl)

            self._tables_created = True
            logger.debug(f"SQL Server system tables ensured in schema {self.schema_name}")
        except Exception as e:
            logger.warning(f"Failed to ensure SQL Server system tables: {e}")

    def load_state(self) -> Dict[str, Any]:
        """Load state - returns empty dict for SQL Server backend."""
        return {"pipelines": {}}

    def save_pipeline_run(self, pipeline_name: str, pipeline_data: Dict[str, Any]) -> None:
        """Pipeline runs are logged via log_run, not this method."""
        pass

    def get_last_run_info(self, pipeline_name: str, node_name: str) -> Optional[Dict[str, Any]]:
        """Get last run info from SQL Server."""
        self._ensure_tables()
        try:
            sql = f"""
            SELECT TOP 1 status, metrics_json
            FROM [{self.schema_name}].[meta_runs]
            WHERE pipeline_name = :pipeline_name AND node_name = :node_name
            ORDER BY timestamp DESC
            """
            result = self.connection.execute(
                sql, {"pipeline_name": pipeline_name, "node_name": node_name}
            )
            if result:
                row = result[0]
                meta = {}
                if row[1]:
                    try:
                        meta = json.loads(row[1])
                    except Exception:
                        pass
                return {"success": row[0] == "SUCCESS", "metadata": meta}
        except Exception as e:
            logger.warning(f"Failed to get last run info: {e}")
        return None

    def get_last_run_status(self, pipeline_name: str, node_name: str) -> Optional[bool]:
        """Get last run status."""
        info = self.get_last_run_info(pipeline_name, node_name)
        return info.get("success") if info else None

    def get_hwm(self, key: str) -> Any:
        """Get HWM value from SQL Server."""
        self._ensure_tables()
        try:
            sql = f"""
            SELECT [value] FROM [{self.schema_name}].[meta_state]
            WHERE [key] = :key
            """
            result = self.connection.execute(sql, {"key": key})
            if result and result[0][0]:
                try:
                    return json.loads(result[0][0])
                except Exception:
                    return result[0][0]
        except Exception as e:
            logger.warning(f"Failed to get HWM: {e}")
        return None

    def set_hwm(self, key: str, value: Any) -> None:
        """Set HWM value in SQL Server using MERGE."""
        self._ensure_tables()
        val_str = json.dumps(value, default=str)
        try:
            sql = f"""
            MERGE [{self.schema_name}].[meta_state] AS target
            USING (SELECT :key AS [key]) AS source
            ON target.[key] = source.[key]
            WHEN MATCHED THEN
                UPDATE SET [value] = :value, environment = :env, updated_at = GETUTCDATE()
            WHEN NOT MATCHED THEN
                INSERT ([key], [value], environment, updated_at)
                VALUES (:key, :value, :env, GETUTCDATE());
            """
            self.connection.execute(sql, {"key": key, "value": val_str, "env": self.environment})
        except Exception as e:
            logger.warning(f"Failed to set HWM: {e}")

    def set_hwm_batch(self, updates: List[Dict[str, Any]]) -> None:
        """Set multiple HWM values."""
        for update in updates:
            self.set_hwm(update["key"], update["value"])

    def log_run(
        self,
        run_id: str,
        pipeline_name: str,
        node_name: str,
        status: str,
        rows_processed: int = 0,
        duration_ms: int = 0,
        metrics_json: str = "{}",
    ) -> None:
        """Log a run to SQL Server meta_runs table."""
        self._ensure_tables()
        try:
            sql = f"""
            INSERT INTO [{self.schema_name}].[meta_runs]
            (run_id, pipeline_name, node_name, status, rows_processed, duration_ms,
             metrics_json, environment, timestamp, date)
            VALUES (:run_id, :pipeline, :node, :status, :rows, :duration,
                    :metrics, :env, GETUTCDATE(), CAST(GETUTCDATE() AS DATE))
            """
            self.connection.execute(
                sql,
                {
                    "run_id": run_id,
                    "pipeline": pipeline_name,
                    "node": node_name,
                    "status": status,
                    "rows": rows_processed,
                    "duration": duration_ms,
                    "metrics": metrics_json,
                    "env": self.environment,
                },
            )
        except Exception as e:
            logger.warning(f"Failed to log run to SQL Server: {e}")

    def log_runs_batch(self, records: List[Dict[str, Any]]) -> None:
        """Log multiple runs to SQL Server."""
        for record in records:
            self.log_run(
                run_id=record["run_id"],
                pipeline_name=record["pipeline_name"],
                node_name=record["node_name"],
                status=record["status"],
                rows_processed=record.get("rows_processed", 0),
                duration_ms=record.get("duration_ms", 0),
                metrics_json=record.get("metrics_json", "{}"),
            )


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
    environment = getattr(config.system, "environment", None)

    # SQL Server backend - centralized system tables
    if conn_type in ("sql_server", "azure_sql"):
        from odibi.connections.factory import create_connection

        # Create the SQL connection
        connection = create_connection(system_conn_name, conn_config)
        schema_name = getattr(config.system, "schema_name", None) or "odibi_system"

        logger.info(f"Using SQL Server system backend: {system_conn_name}, schema: {schema_name}")
        return SqlServerSystemBackend(
            connection=connection,
            schema_name=schema_name,
            environment=environment,
        )

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
        environment=environment,
    )


def create_sync_source_backend(
    sync_from_config: Any,
    connections: Dict[str, Any],
    project_root: str = ".",
) -> StateBackend:
    """
    Create a source StateBackend for sync operations.

    Args:
        sync_from_config: SyncFromConfig with connection/path/schema_name
        connections: Dictionary of connection configs
        project_root: Root directory for local paths

    Returns:
        Configured StateBackend for reading source data
    """

    def _get(obj, key, default=None):
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)

    conn_name = _get(sync_from_config, "connection")
    conn_config = connections.get(conn_name)

    if not conn_config:
        raise ValueError(f"Sync source connection '{conn_name}' not found in connections.")

    conn_type = _get(conn_config, "type")

    # SQL Server source
    if conn_type in ("sql_server", "azure_sql"):
        from odibi.connections.factory import create_connection

        connection = create_connection(conn_name, conn_config)
        schema_name = _get(sync_from_config, "schema_name") or "odibi_system"
        return SqlServerSystemBackend(
            connection=connection,
            schema_name=schema_name,
            environment=None,
        )

    # File-based source (local, azure_blob)
    base_uri = ""
    storage_options = {}
    path = _get(sync_from_config, "path") or "_odibi_system"

    if conn_type == "local":
        base_path = _get(conn_config, "base_path")
        if not os.path.isabs(base_path):
            base_path = os.path.join(project_root, base_path)
        base_uri = os.path.join(base_path, path)

    elif conn_type == "azure_blob":
        account = _get(conn_config, "account_name")
        container = _get(conn_config, "container")
        base_uri = f"abfss://{container}@{account}.dfs.core.windows.net/{path}"

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

    if not base_uri:
        base_uri = os.path.join(project_root, path)

    meta_state_path = f"{base_uri}/meta_state"
    meta_runs_path = f"{base_uri}/meta_runs"

    return CatalogStateBackend(
        meta_runs_path=meta_runs_path,
        meta_state_path=meta_state_path,
        spark_session=None,
        storage_options=storage_options,
        environment=None,
    )


def sync_system_data(
    source_backend: StateBackend,
    target_backend: StateBackend,
    tables: Optional[List[str]] = None,
) -> Dict[str, int]:
    """
    Sync system data from source backend to target backend.

    Reads meta_runs and meta_state from source and writes to target.

    Args:
        source_backend: Source StateBackend to read from
        target_backend: Target StateBackend to write to
        tables: Optional list of tables to sync ('runs', 'state'). Default: both.

    Returns:
        Dict with counts: {'runs': N, 'state': M}
    """
    if tables is None:
        tables = ["runs", "state"]

    result = {"runs": 0, "state": 0}

    # Sync runs (meta_runs)
    if "runs" in tables:
        runs_count = _sync_runs(source_backend, target_backend)
        result["runs"] = runs_count
        logger.info(f"Synced {runs_count} run records")

    # Sync state (meta_state / HWM)
    if "state" in tables:
        state_count = _sync_state(source_backend, target_backend)
        result["state"] = state_count
        logger.info(f"Synced {state_count} state records")

    return result


def _sync_runs(source: StateBackend, target: StateBackend) -> int:
    """Sync runs from source to target."""
    records = []

    # Read runs from source
    if isinstance(source, CatalogStateBackend):
        if not DeltaTable or not pd:
            logger.warning("Delta/Pandas not available for reading source runs")
            return 0

        try:
            dt = DeltaTable(source.meta_runs_path, storage_options=source.storage_options)
            df = dt.to_pandas()
            if df.empty:
                return 0

            for _, row in df.iterrows():
                records.append(
                    {
                        "run_id": row.get("run_id"),
                        "pipeline_name": row.get("pipeline_name"),
                        "node_name": row.get("node_name"),
                        "status": row.get("status"),
                        "rows_processed": int(row.get("rows_processed", 0) or 0),
                        "duration_ms": int(row.get("duration_ms", 0) or 0),
                        "metrics_json": row.get("metrics_json") or row.get("metadata") or "{}",
                    }
                )
        except Exception as e:
            logger.warning(f"Failed to read runs from source: {e}")
            return 0

    elif isinstance(source, SqlServerSystemBackend):
        source._ensure_tables()
        try:
            sql = f"""SELECT run_id, pipeline_name, node_name, status, rows_processed,
                      duration_ms, metrics_json FROM [{source.schema_name}].[meta_runs]"""
            rows = source.connection.execute(sql)
            if rows:
                for row in rows:
                    records.append(
                        {
                            "run_id": row[0],
                            "pipeline_name": row[1],
                            "node_name": row[2],
                            "status": row[3],
                            "rows_processed": int(row[4] or 0),
                            "duration_ms": int(row[5] or 0),
                            "metrics_json": row[6] or "{}",
                        }
                    )
        except Exception as e:
            logger.warning(f"Failed to read runs from SQL source: {e}")
            return 0

    if not records:
        return 0

    # Write runs to target
    if isinstance(target, SqlServerSystemBackend):
        target.log_runs_batch(records)
    elif isinstance(target, CatalogStateBackend):
        _write_runs_to_catalog(target, records)

    return len(records)


def _write_runs_to_catalog(target: CatalogStateBackend, records: List[Dict]) -> None:
    """Write run records to CatalogStateBackend."""
    if not pd or not write_deltalake:
        logger.warning("Delta/Pandas not available for writing runs")
        return

    df = pd.DataFrame(records)
    df["timestamp"] = datetime.now(timezone.utc)
    df["date"] = datetime.now(timezone.utc).date()
    df["environment"] = target.environment

    def _write():
        write_deltalake(
            target.meta_runs_path,
            df,
            mode="append",
            storage_options=target.storage_options,
            schema_mode="merge",
        )

    _retry_delta_operation(_write)


def _sync_state(source: StateBackend, target: StateBackend) -> int:
    """Sync HWM state from source to target."""
    hwm_records = []

    # Read state from source
    if isinstance(source, CatalogStateBackend):
        if not DeltaTable or not pd:
            logger.warning("Delta/Pandas not available for reading source state")
            return 0

        try:
            dt = DeltaTable(source.meta_state_path, storage_options=source.storage_options)
            df = dt.to_pandas()
            if df.empty:
                return 0

            for _, row in df.iterrows():
                key = row.get("key")
                value = row.get("value")
                if key:
                    try:
                        hwm_records.append({"key": key, "value": json.loads(value)})
                    except (json.JSONDecodeError, TypeError):
                        hwm_records.append({"key": key, "value": value})
        except Exception as e:
            logger.warning(f"Failed to read state from source: {e}")
            return 0

    elif isinstance(source, SqlServerSystemBackend):
        source._ensure_tables()
        try:
            sql = f"SELECT [key], [value] FROM [{source.schema_name}].[meta_state]"
            rows = source.connection.execute(sql)
            if rows:
                for row in rows:
                    key, value = row[0], row[1]
                    if key:
                        try:
                            hwm_records.append({"key": key, "value": json.loads(value)})
                        except (json.JSONDecodeError, TypeError):
                            hwm_records.append({"key": key, "value": value})
        except Exception as e:
            logger.warning(f"Failed to read state from SQL source: {e}")
            return 0

    if not hwm_records:
        return 0

    # Write state to target
    target.set_hwm_batch(hwm_records)

    return len(hwm_records)
