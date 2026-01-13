import hashlib
import json
import logging
import random
import time
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        ArrayType,
        DateType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
except ImportError:
    # Fallback for environments without PySpark (e.g., pure Pandas mode)
    SparkSession = Any

    class DataType:
        pass

    class StringType(DataType):
        pass

    class LongType(DataType):
        pass

    class DoubleType(DataType):
        pass

    class DateType(DataType):
        pass

    class TimestampType(DataType):
        pass

    class ArrayType(DataType):
        def __init__(self, elementType):
            self.elementType = elementType

    class StructField:
        def __init__(self, name, dtype, nullable=True):
            self.name = name
            self.dataType = dtype

    class StructType:
        def __init__(self, fields):
            self.fields = fields


from odibi.config import SystemConfig

logger = logging.getLogger(__name__)


class CatalogManager:
    """
    Manages the Odibi System Catalog (The Brain).
    Handles bootstrapping and interaction with meta-tables.
    """

    def __init__(
        self,
        spark: Optional[SparkSession],
        config: SystemConfig,
        base_path: str,
        engine: Optional[Any] = None,
        connection: Optional[Any] = None,
    ):
        """
        Initialize the Catalog Manager.

        Args:
            spark: Active SparkSession (optional if engine is provided)
            config: SystemConfig object
            base_path: Absolute path to the system catalog directory (resolved from connection).
                       Example: "abfss://container@account.dfs.core.windows.net/_odibi_system"
            engine: Execution engine (optional, for Pandas mode)
            connection: Connection object for storage credentials (optional, for Pandas mode)
        """
        self.spark = spark
        self.config = config
        self.base_path = base_path.rstrip("/")
        self.engine = engine
        self.connection = connection
        self._project: Optional[str] = None

        # Table Paths
        self.tables = {
            "meta_tables": f"{self.base_path}/meta_tables",
            "meta_runs": f"{self.base_path}/meta_runs",
            "meta_patterns": f"{self.base_path}/meta_patterns",
            "meta_metrics": f"{self.base_path}/meta_metrics",
            "meta_state": f"{self.base_path}/meta_state",
            "meta_pipelines": f"{self.base_path}/meta_pipelines",
            "meta_nodes": f"{self.base_path}/meta_nodes",
            "meta_schemas": f"{self.base_path}/meta_schemas",
            "meta_lineage": f"{self.base_path}/meta_lineage",
            "meta_outputs": f"{self.base_path}/meta_outputs",
            # Leverage Summary Tables (Observability)
            "meta_pipeline_runs": f"{self.base_path}/meta_pipeline_runs",
            "meta_node_runs": f"{self.base_path}/meta_node_runs",
            "meta_failures": f"{self.base_path}/meta_failures",
            "meta_observability_errors": f"{self.base_path}/meta_observability_errors",
            "meta_derived_applied_runs": f"{self.base_path}/meta_derived_applied_runs",
            "meta_daily_stats": f"{self.base_path}/meta_daily_stats",
            "meta_pipeline_health": f"{self.base_path}/meta_pipeline_health",
            "meta_sla_status": f"{self.base_path}/meta_sla_status",
        }

        # Cache for meta table reads (invalidated on write operations)
        self._pipelines_cache: Optional[Dict[str, Dict[str, Any]]] = None
        self._nodes_cache: Optional[Dict[str, Dict[str, str]]] = None
        self._outputs_cache: Optional[Dict[str, Dict[str, Any]]] = None

    @property
    def is_spark_mode(self) -> bool:
        """Check if running in Spark mode."""
        return self.spark is not None

    @property
    def is_pandas_mode(self) -> bool:
        """Check if running in Pandas mode."""
        return self.engine is not None and self.engine.name == "pandas"

    @property
    def is_sql_server_mode(self) -> bool:
        """Check if running with SQL Server system backend."""
        if self.connection is None:
            return False
        # Check if connection is AzureSQL type
        conn_type = getattr(self.connection, "__class__", None)
        if conn_type is None:
            return False
        return conn_type.__name__ in ("AzureSQL", "SqlServerConnection")

    @property
    def project(self) -> Optional[str]:
        """Get the project name for tagging catalog records."""
        return self._project

    @project.setter
    def project(self, value: Optional[str]) -> None:
        """Set the project name for tagging catalog records."""
        self._project = value

    def _get_storage_options(self) -> Dict[str, Any]:
        """Get storage options for pandas/delta-rs operations.

        Returns:
            Dict with storage credentials if connection supports it, else empty dict.
        """
        if self.connection and hasattr(self.connection, "pandas_storage_options"):
            return self.connection.pandas_storage_options()
        return {}

    @property
    def has_backend(self) -> bool:
        """Check if any backend (Spark or engine) is available."""
        return self.spark is not None or self.engine is not None

    def invalidate_cache(self) -> None:
        """Invalidate all cached meta table data."""
        self._pipelines_cache = None
        self._nodes_cache = None
        self._outputs_cache = None

    def _retry_with_backoff(self, func, max_retries: int = 5, base_delay: float = 1.0):
        """Retry a function with exponential backoff and jitter for concurrent writes.

        Only retries on Delta Lake concurrency exceptions. Other exceptions are
        raised immediately. Warnings are only logged after all retries fail.

        Args:
            func: Callable to execute.
            max_retries: Maximum retry attempts (default 5 for high concurrency).
            base_delay: Base delay in seconds (doubles each retry).

        Returns:
            Result of the function.

        Raises:
            Exception: If all retries fail or non-retryable error occurs.
        """
        for attempt in range(max_retries + 1):
            try:
                return func()
            except Exception as e:
                error_str = str(e)
                # Check for Delta concurrency exceptions
                is_concurrent_error = any(
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
                if not is_concurrent_error or attempt >= max_retries:
                    raise
                # Exponential backoff with jitter (1s, 2s, 4s, 8s, 16s = ~31s total)
                delay = base_delay * (2**attempt) + random.uniform(0, 1.0)
                logger.debug(
                    f"Delta concurrent write (attempt {attempt + 1}/{max_retries + 1}), "
                    f"retrying in {delay:.2f}s..."
                )
                time.sleep(delay)

    def _get_all_pipelines_cached(self) -> Dict[str, Dict[str, Any]]:
        """Get all pipelines with caching."""
        if self._pipelines_cache is not None:
            return self._pipelines_cache

        self._pipelines_cache = {}
        if not self.spark and not self.engine:
            return self._pipelines_cache

        try:
            if self.spark:
                df = self.spark.read.format("delta").load(self.tables["meta_pipelines"])
                rows = df.collect()
                for row in rows:
                    row_dict = row.asDict()
                    self._pipelines_cache[row_dict["pipeline_name"]] = row_dict
            elif self.engine:
                df = self._read_local_table(self.tables["meta_pipelines"])
                if not df.empty and "pipeline_name" in df.columns:
                    for _, row in df.iterrows():
                        self._pipelines_cache[row["pipeline_name"]] = row.to_dict()
        except Exception as e:
            logger.debug(f"Could not cache pipelines: {e}")
            self._pipelines_cache = {}

        return self._pipelines_cache

    def _get_all_nodes_cached(self) -> Dict[str, Dict[str, str]]:
        """Get all nodes grouped by pipeline with caching."""
        if self._nodes_cache is not None:
            return self._nodes_cache

        self._nodes_cache = {}
        if not self.spark and not self.engine:
            return self._nodes_cache

        try:
            if self.spark:
                df = self.spark.read.format("delta").load(self.tables["meta_nodes"])
                rows = df.select("pipeline_name", "node_name", "version_hash").collect()
                for row in rows:
                    p_name = row["pipeline_name"]
                    if p_name not in self._nodes_cache:
                        self._nodes_cache[p_name] = {}
                    self._nodes_cache[p_name][row["node_name"]] = row["version_hash"]
            elif self.engine:
                df = self._read_local_table(self.tables["meta_nodes"])
                if not df.empty and "pipeline_name" in df.columns:
                    for _, row in df.iterrows():
                        p_name = row["pipeline_name"]
                        if p_name not in self._nodes_cache:
                            self._nodes_cache[p_name] = {}
                        self._nodes_cache[p_name][row["node_name"]] = row["version_hash"]
        except Exception as e:
            logger.debug(f"Could not cache nodes: {e}")
            self._nodes_cache = {}

        return self._nodes_cache

    def bootstrap(self) -> None:
        """
        Ensures all system tables exist. Creates them if missing.
        """
        if not self.spark and not self.engine:
            logger.warning(
                "Neither SparkSession nor Engine available. Skipping System Catalog bootstrap."
            )
            return

        logger.info(f"Bootstrapping System Catalog at {self.base_path}...")

        self._ensure_table("meta_tables", self._get_schema_meta_tables())
        self._ensure_table(
            "meta_runs",
            self._get_schema_meta_runs(),
            partition_cols=["pipeline_name", "date"],
            schema_evolution=True,
        )
        self._ensure_table("meta_patterns", self._get_schema_meta_patterns())
        self._ensure_table("meta_metrics", self._get_schema_meta_metrics())
        self._ensure_table("meta_state", self._get_schema_meta_state())
        self._ensure_table("meta_pipelines", self._get_schema_meta_pipelines())
        self._ensure_table("meta_nodes", self._get_schema_meta_nodes())
        self._ensure_table("meta_schemas", self._get_schema_meta_schemas())
        self._ensure_table("meta_lineage", self._get_schema_meta_lineage())
        self._ensure_table("meta_outputs", self._get_schema_meta_outputs())

        # Leverage Summary Tables (Observability)
        self._ensure_table("meta_pipeline_runs", self._get_schema_meta_pipeline_runs())
        self._ensure_table(
            "meta_node_runs",
            self._get_schema_meta_node_runs(),
            partition_cols=["pipeline_name"],
        )
        self._ensure_table(
            "meta_failures",
            self._get_schema_meta_failures(),
            partition_cols=["date"],
        )
        self._ensure_table(
            "meta_observability_errors",
            self._get_schema_meta_observability_errors(),
            partition_cols=["date"],
        )
        self._ensure_table(
            "meta_derived_applied_runs", self._get_schema_meta_derived_applied_runs()
        )
        self._ensure_table("meta_daily_stats", self._get_schema_meta_daily_stats())
        self._ensure_table("meta_pipeline_health", self._get_schema_meta_pipeline_health())
        self._ensure_table("meta_sla_status", self._get_schema_meta_sla_status())

    def _ensure_table(
        self,
        name: str,
        schema: StructType,
        partition_cols: Optional[list] = None,
        schema_evolution: bool = False,
    ) -> None:
        path = self.tables[name]
        if not self._table_exists(path):
            logger.info(f"Creating system table: {name} at {path}")

            if self.spark:
                # Create empty DataFrame with schema
                writer = self.spark.createDataFrame([], schema).write.format("delta")
                if partition_cols:
                    writer = writer.partitionBy(*partition_cols)
                writer.save(path)
            elif self.engine and self.engine.name == "pandas":
                # Pandas/Local Mode
                import os

                import pandas as pd

                os.makedirs(path, exist_ok=True)

                # Attempt to create Delta Table if library exists (using Arrow for strict typing)
                try:
                    import pyarrow as pa
                    from deltalake import write_deltalake

                    def map_to_arrow_type(dtype):
                        s_type = str(dtype)
                        if isinstance(dtype, StringType) or "StringType" in s_type:
                            return pa.string()
                        if isinstance(dtype, LongType) or "LongType" in s_type:
                            return pa.int64()
                        if isinstance(dtype, DoubleType) or "DoubleType" in s_type:
                            return pa.float64()
                        if isinstance(dtype, TimestampType) or "TimestampType" in s_type:
                            return pa.timestamp("us", tz="UTC")
                        if isinstance(dtype, DateType) or "DateType" in s_type:
                            return pa.date32()
                        if isinstance(dtype, ArrayType) or "ArrayType" in s_type:
                            # Access element type safely
                            elem_type = getattr(dtype, "elementType", StringType())
                            return pa.list_(map_to_arrow_type(elem_type))
                        return pa.string()

                    # Define Arrow Schema
                    arrow_fields = []
                    for field in schema.fields:
                        arrow_fields.append(pa.field(field.name, map_to_arrow_type(field.dataType)))

                    arrow_schema = pa.schema(arrow_fields)

                    # Create Empty Table
                    # Note: We pass a dict of empty lists. PyArrow handles the rest using schema.
                    data = {f.name: [] for f in schema.fields}
                    table = pa.Table.from_pydict(data, schema=arrow_schema)

                    storage_opts = self._get_storage_options()
                    write_deltalake(
                        path,
                        table,
                        mode="overwrite",
                        partition_by=partition_cols,
                        storage_options=storage_opts if storage_opts else None,
                    )
                    logger.info(f"Initialized Delta table: {name}")

                except ImportError:
                    # Fallback to Pandas/Parquet if Delta/Arrow not available
                    # Prepare empty DataFrame with correct columns and types
                    data = {}

                    def get_pd_type(dtype):
                        if isinstance(dtype, StringType) or "StringType" in str(type(dtype)):
                            return "string"
                        if isinstance(dtype, LongType) or "LongType" in str(type(dtype)):
                            return "int64"
                        if isinstance(dtype, DoubleType) or "DoubleType" in str(type(dtype)):
                            return "float64"
                        if isinstance(dtype, TimestampType) or "TimestampType" in str(type(dtype)):
                            return "datetime64[ns, UTC]"
                        if isinstance(dtype, DateType) or "DateType" in str(type(dtype)):
                            return "datetime64[ns]"
                        return "object"

                    for field in schema.fields:
                        pd_type = get_pd_type(field.dataType)
                        data[field.name] = pd.Series([], dtype=pd_type)

                    df = pd.DataFrame(data)

                    # Fallback to Parquet
                    # Pandas to_parquet with partition_cols
                    df.to_parquet(path, partition_cols=partition_cols)
                    logger.info(f"Initialized Parquet table: {name} (Delta library not found)")
                except Exception as e:
                    logger.error(f"Failed to create local system table {name}: {e}")
                    raise e
        else:
            # If table exists and schema evolution is requested (only for Pandas/Delta mode currently)
            if schema_evolution and self.engine and self.engine.name == "pandas":
                try:
                    from deltalake import DeltaTable, write_deltalake

                    storage_opts = self._get_storage_options()
                    _ = DeltaTable(path, storage_options=storage_opts if storage_opts else None)
                    # Basic schema evolution: overwrite schema if we are appending?
                    # For now, let's just log. True evolution is complex.
                    # A simple fix for "fields mismatch" is to allow schema merge.
                    pass
                except ImportError:
                    pass
            logger.debug(f"System table exists: {name}")
            self._migrate_schema_if_needed(name, path, schema)

    def _migrate_schema_if_needed(self, name: str, path: str, expected_schema: StructType) -> None:
        """
        Migrate table schema if there are incompatible type changes.
        This handles cases like ArrayType -> StringType migrations.
        """
        try:
            if self.spark:
                existing_df = self.spark.read.format("delta").load(path)
                existing_fields = {f.name: f.dataType for f in existing_df.schema.fields}
                expected_fields = {f.name: f.dataType for f in expected_schema.fields}

                needs_migration = False
                for field_name, expected_type in expected_fields.items():
                    if field_name in existing_fields:
                        existing_type = existing_fields[field_name]
                        if type(existing_type) is not type(expected_type):
                            logger.info(
                                f"Schema migration needed for {name}.{field_name}: "
                                f"{existing_type} -> {expected_type}"
                            )
                            needs_migration = True
                            break

                if needs_migration:
                    logger.info(f"Migrating schema for {name}...")
                    migrated_df = existing_df
                    for field in expected_schema.fields:
                        if field.name in existing_fields:
                            existing_type = existing_fields[field.name]
                            if not isinstance(existing_type, type(field.dataType)):
                                from pyspark.sql import functions as F

                                if isinstance(existing_type, ArrayType) and isinstance(
                                    field.dataType, StringType
                                ):
                                    migrated_df = migrated_df.withColumn(
                                        field.name, F.to_json(F.col(field.name))
                                    )

                    migrated_df.write.format("delta").mode("overwrite").option(
                        "overwriteSchema", "true"
                    ).save(path)
                    logger.info(f"Schema migration completed for {name}")

            elif self.engine and self.engine.name == "pandas":
                from deltalake import DeltaTable

                storage_opts = self._get_storage_options()
                dt = DeltaTable(path, storage_options=storage_opts if storage_opts else None)
                existing_schema = dt.schema()
                existing_fields = {f.name: f.type for f in existing_schema.fields}

                needs_migration = False
                for field in expected_schema.fields:
                    if field.name in existing_fields:
                        existing_type_str = str(existing_fields[field.name])
                        expected_type_str = field.dataType.simpleString()
                        if "array" in existing_type_str.lower() and expected_type_str == "string":
                            needs_migration = True
                            break

                if needs_migration:
                    logger.info(f"Migrating schema for {name}...")
                    import json

                    df = dt.to_pandas()
                    for field in expected_schema.fields:
                        if field.name in df.columns and field.name in existing_fields:
                            existing_type_str = str(existing_fields[field.name])
                            if "array" in existing_type_str.lower():
                                df[field.name] = df[field.name].apply(
                                    lambda x: json.dumps(x) if isinstance(x, list) else x
                                )

                    from deltalake import write_deltalake

                    storage_opts = self._get_storage_options()
                    write_deltalake(
                        path,
                        df,
                        mode="overwrite",
                        overwrite_schema=True,
                        storage_options=storage_opts if storage_opts else None,
                    )
                    logger.info(f"Schema migration completed for {name}")

        except Exception as e:
            logger.warning(f"Schema migration check failed for {name}: {e}")

    def _table_exists(self, path: str) -> bool:
        if self.spark:
            try:
                self.spark.read.format("delta").load(path).limit(0).collect()
                return True
            except Exception as e:
                # If AnalysisException or "Path does not exist", return False
                # Otherwise, if it's an auth error, we might want to warn.
                msg = str(e).lower()
                if (
                    "path does not exist" in msg
                    or "filenotfound" in msg
                    or "analysisexception" in type(e).__name__.lower()
                ):
                    return False

                logger.warning(f"Error checking if table exists at {path}: {e}")
                return False
        elif self.engine:
            import os

            # For cloud paths, try to load with delta-rs
            if path.startswith(("abfss://", "az://", "s3://", "gs://", "https://")):
                try:
                    from deltalake import DeltaTable

                    storage_opts = self._get_storage_options()
                    DeltaTable(path, storage_options=storage_opts if storage_opts else None)
                    return True
                except Exception:
                    return False

            # For local paths, check if directory exists and has content
            if not os.path.exists(path):
                return False
            if os.path.isdir(path):
                # Check if empty or contains relevant files
                if not os.listdir(path):
                    return False
                return True
            return False
        return False

    def _get_schema_meta_tables(self) -> StructType:
        """
        meta_tables (Inventory): Tracks physical assets.
        """
        return StructType(
            [
                StructField("project", StringType(), True),
                StructField("table_name", StringType(), True),
                StructField("path", StringType(), True),
                StructField("format", StringType(), True),
                StructField("pattern_type", StringType(), True),
                StructField("schema_hash", StringType(), True),
                StructField("updated_at", TimestampType(), True),
            ]
        )

    def _get_schema_meta_runs(self) -> StructType:
        """
        meta_runs (Observability): Tracks execution history.
        """
        return StructType(
            [
                StructField("run_id", StringType(), True),
                StructField("project", StringType(), True),
                StructField("pipeline_name", StringType(), True),
                StructField("node_name", StringType(), True),
                StructField("status", StringType(), True),
                StructField("rows_processed", LongType(), True),
                StructField("duration_ms", LongType(), True),
                StructField("metrics_json", StringType(), True),
                StructField("environment", StringType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("date", DateType(), True),
            ]
        )

    def _get_schema_meta_patterns(self) -> StructType:
        """
        meta_patterns (Governance): Tracks pattern compliance.
        """
        return StructType(
            [
                StructField("table_name", StringType(), True),
                StructField("pattern_type", StringType(), True),
                StructField("configuration", StringType(), True),
                StructField("compliance_score", DoubleType(), True),
            ]
        )

    def _get_schema_meta_metrics(self) -> StructType:
        """
        meta_metrics (Semantics): Tracks business logic.
        Note: dimensions is stored as JSON string for cross-engine portability.
        """
        return StructType(
            [
                StructField("metric_name", StringType(), True),
                StructField("definition_sql", StringType(), True),
                StructField("dimensions", StringType(), True),
                StructField("source_table", StringType(), True),
            ]
        )

    def _get_schema_meta_state(self) -> StructType:
        """
        meta_state (HWM Key-Value Store): Tracks high-water marks for incremental loads.
        Uses a generic key/value pattern for flexibility.
        """
        return StructType(
            [
                StructField("key", StringType(), False),
                StructField("value", StringType(), True),
                StructField("environment", StringType(), True),
                StructField("updated_at", TimestampType(), True),
            ]
        )

    def _get_schema_meta_pipelines(self) -> StructType:
        """
        meta_pipelines (Definitions): Tracks pipeline configurations.
        """
        return StructType(
            [
                StructField("pipeline_name", StringType(), True),
                StructField("version_hash", StringType(), True),
                StructField("description", StringType(), True),
                StructField("layer", StringType(), True),
                StructField("schedule", StringType(), True),
                StructField("tags_json", StringType(), True),
                StructField("updated_at", TimestampType(), True),
            ]
        )

    def _get_schema_meta_nodes(self) -> StructType:
        """
        meta_nodes (Definitions): Tracks node configurations within pipelines.
        """
        return StructType(
            [
                StructField("pipeline_name", StringType(), True),
                StructField("node_name", StringType(), True),
                StructField("version_hash", StringType(), True),
                StructField("type", StringType(), True),  # read/transform/write
                StructField("config_json", StringType(), True),
                StructField("updated_at", TimestampType(), True),
            ]
        )

    def _get_schema_meta_schemas(self) -> StructType:
        """
        meta_schemas (Schema Version Tracking): Tracks schema changes over time.
        """
        return StructType(
            [
                StructField("table_path", StringType(), False),
                StructField("schema_version", LongType(), False),
                StructField("schema_hash", StringType(), False),
                StructField("columns", StringType(), False),  # JSON: {"col": "type", ...}
                StructField("captured_at", TimestampType(), False),
                StructField("pipeline", StringType(), True),
                StructField("node", StringType(), True),
                StructField("run_id", StringType(), True),
                StructField("columns_added", StringType(), True),  # JSON array as string
                StructField("columns_removed", StringType(), True),  # JSON array as string
                StructField("columns_type_changed", StringType(), True),  # JSON array as string
            ]
        )

    def _get_schema_meta_lineage(self) -> StructType:
        """
        meta_lineage (Cross-Pipeline Lineage): Tracks table-level lineage relationships.
        """
        return StructType(
            [
                StructField("source_table", StringType(), False),
                StructField("target_table", StringType(), False),
                StructField("source_pipeline", StringType(), True),
                StructField("source_node", StringType(), True),
                StructField("target_pipeline", StringType(), True),
                StructField("target_node", StringType(), True),
                StructField("relationship", StringType(), False),  # "feeds" | "derived_from"
                StructField("last_observed", TimestampType(), False),
                StructField("run_id", StringType(), True),
            ]
        )

    def _get_schema_meta_outputs(self) -> StructType:
        """
        meta_outputs (Node Outputs Registry): Tracks output metadata for cross-pipeline dependencies.

        Stores output metadata for every node that has a `write` block.
        Primary key: (pipeline_name, node_name)
        """
        return StructType(
            [
                StructField("pipeline_name", StringType(), False),
                StructField("node_name", StringType(), False),
                StructField(
                    "output_type", StringType(), False
                ),  # "external_table" | "managed_table"
                StructField("connection_name", StringType(), True),
                StructField("path", StringType(), True),
                StructField("format", StringType(), True),
                StructField("table_name", StringType(), True),
                StructField("last_run", TimestampType(), False),
                StructField("row_count", LongType(), True),
                StructField("updated_at", TimestampType(), False),
            ]
        )

    # ========================================================================
    # Leverage Summary Tables - Schemas
    # ========================================================================

    def _get_schema_meta_pipeline_runs(self) -> StructType:
        """
        meta_pipeline_runs (Fact): Pipeline execution log.
        One row per pipeline execution. Append-only.
        """
        return StructType(
            [
                StructField("run_id", StringType(), False),  # PK, UUID
                StructField("project", StringType(), True),
                StructField("pipeline_name", StringType(), False),
                StructField("owner", StringType(), True),
                StructField("layer", StringType(), True),
                StructField("run_start_at", TimestampType(), False),
                StructField("run_end_at", TimestampType(), False),
                StructField("duration_ms", LongType(), False),
                StructField("status", StringType(), False),  # SUCCESS | FAILURE
                StructField("nodes_total", LongType(), True),
                StructField("nodes_succeeded", LongType(), True),
                StructField("nodes_failed", LongType(), True),
                StructField("nodes_skipped", LongType(), True),
                StructField("rows_processed", LongType(), True),
                StructField("error_summary", StringType(), True),  # max 500 chars
                StructField("terminal_nodes", StringType(), True),  # comma-separated
                StructField("environment", StringType(), True),
                StructField("databricks_cluster_id", StringType(), True),
                StructField("databricks_job_id", StringType(), True),
                StructField("databricks_workspace_id", StringType(), True),
                StructField("estimated_cost_usd", DoubleType(), True),
                StructField("actual_cost_usd", DoubleType(), True),
                StructField(
                    "cost_source", StringType(), True
                ),  # configured_rate | databricks_billing | none
                StructField("created_at", TimestampType(), False),
            ]
        )

    def _get_schema_meta_node_runs(self) -> StructType:
        """
        meta_node_runs (Fact): Node execution log.
        One row per node execution. Append-only.
        """
        return StructType(
            [
                StructField("run_id", StringType(), False),  # FK to pipeline run
                StructField("node_id", StringType(), False),  # UUID for this node execution
                StructField("project", StringType(), True),
                StructField("pipeline_name", StringType(), False),
                StructField("node_name", StringType(), False),
                StructField("status", StringType(), False),  # SUCCESS | FAILURE | SKIPPED
                StructField("run_start_at", TimestampType(), True),
                StructField("run_end_at", TimestampType(), True),
                StructField("duration_ms", LongType(), True),
                StructField("rows_processed", LongType(), True),
                StructField("estimated_cost_usd", DoubleType(), True),
                StructField("metrics_json", StringType(), True),  # flat dict, scalars only
                StructField("environment", StringType(), True),
                StructField("created_at", TimestampType(), False),
            ]
        )

    def _get_schema_meta_failures(self) -> StructType:
        """
        meta_failures (Fact): Failure details.
        One row per failure event. Append-only.
        """
        return StructType(
            [
                StructField("failure_id", StringType(), False),  # PK, UUID
                StructField("run_id", StringType(), False),  # FK to pipeline run
                StructField("project", StringType(), True),
                StructField("pipeline_name", StringType(), False),
                StructField("node_name", StringType(), False),
                StructField("error_type", StringType(), False),  # Exception class name
                StructField("error_message", StringType(), True),  # max 1000 chars
                StructField("error_code", StringType(), True),  # future taxonomy
                StructField("stack_trace", StringType(), True),  # max 2000 chars
                StructField("environment", StringType(), True),
                StructField("timestamp", TimestampType(), False),
                StructField("date", DateType(), False),  # for partitioning
            ]
        )

    def _get_schema_meta_observability_errors(self) -> StructType:
        """
        meta_observability_errors (Fact): Observability system failures.
        One row per observability failure. Append-only.
        """
        return StructType(
            [
                StructField("error_id", StringType(), False),  # PK, UUID
                StructField("run_id", StringType(), True),
                StructField("pipeline_name", StringType(), True),
                StructField("component", StringType(), False),  # catalog_update, billing_query
                StructField("error_message", StringType(), True),  # max 500 chars
                StructField("timestamp", TimestampType(), False),
                StructField("date", DateType(), False),  # for partitioning
            ]
        )

    def _get_schema_meta_derived_applied_runs(self) -> StructType:
        """
        meta_derived_applied_runs (Guard Table): Idempotency guard for derived tables.
        One row per (derived_table, run_id). Prevents duplicate processing.
        """
        return StructType(
            [
                StructField("derived_table", StringType(), False),  # PK (with run_id)
                StructField("run_id", StringType(), False),  # PK (with derived_table)
                StructField("claim_token", StringType(), False),  # UUID of claiming process
                StructField("status", StringType(), False),  # CLAIMED | APPLIED | FAILED
                StructField("claimed_at", TimestampType(), False),
                StructField("applied_at", TimestampType(), True),
                StructField("error_message", StringType(), True),  # max 500 chars
            ]
        )

    def _get_schema_meta_daily_stats(self) -> StructType:
        """
        meta_daily_stats (Derived): Daily aggregates.
        PK: (date, pipeline_name). Upsert on pipeline completion.
        """
        return StructType(
            [
                StructField("date", DateType(), False),  # PK (with pipeline_name)
                StructField("pipeline_name", StringType(), False),  # PK (with date)
                StructField("runs", LongType(), False),
                StructField("successes", LongType(), False),
                StructField("failures", LongType(), False),
                StructField("total_rows", LongType(), True),
                StructField("total_duration_ms", LongType(), True),
                StructField("estimated_cost_usd", DoubleType(), True),
                StructField("actual_cost_usd", DoubleType(), True),
                StructField(
                    "cost_source", StringType(), True
                ),  # configured_rate | databricks_billing | none | mixed
                StructField("cost_is_actual", LongType(), True),  # 0/1 for cross-engine compat
            ]
        )

    def _get_schema_meta_pipeline_health(self) -> StructType:
        """
        meta_pipeline_health (Derived): Current health snapshot.
        PK: pipeline_name. Upsert on pipeline completion.
        """
        return StructType(
            [
                StructField("pipeline_name", StringType(), False),  # PK
                StructField("owner", StringType(), True),
                StructField("layer", StringType(), True),
                StructField("total_runs", LongType(), False),
                StructField("total_successes", LongType(), False),
                StructField("total_failures", LongType(), False),
                StructField("success_rate_7d", DoubleType(), True),
                StructField("success_rate_30d", DoubleType(), True),
                StructField("avg_duration_ms_7d", DoubleType(), True),
                StructField("total_rows_30d", LongType(), True),
                StructField("estimated_cost_30d", DoubleType(), True),
                StructField("last_success_at", TimestampType(), True),
                StructField("last_failure_at", TimestampType(), True),
                StructField("last_run_at", TimestampType(), False),
                StructField("updated_at", TimestampType(), False),
            ]
        )

    def _get_schema_meta_sla_status(self) -> StructType:
        """
        meta_sla_status (Derived): Freshness compliance snapshot.
        PK: project_name, pipeline_name. Upsert on pipeline completion.
        """
        return StructType(
            [
                StructField("project_name", StringType(), False),  # PK
                StructField("pipeline_name", StringType(), False),  # PK
                StructField("owner", StringType(), True),
                StructField("freshness_sla", StringType(), True),  # e.g., "6h"
                StructField(
                    "freshness_anchor", StringType(), True
                ),  # run_completion | table_max_timestamp | watermark_state
                StructField("freshness_sla_minutes", LongType(), True),
                StructField("last_success_at", TimestampType(), True),
                StructField("minutes_since_success", LongType(), True),
                StructField("sla_met", LongType(), True),  # 0/1 for cross-engine compat
                StructField("hours_overdue", DoubleType(), True),
                StructField("updated_at", TimestampType(), False),
            ]
        )

    def get_registered_pipeline(self, pipeline_name: str) -> Optional[Dict[str, Any]]:
        """
        Get existing registered pipeline record with version_hash.

        Args:
            pipeline_name: Name of the pipeline to look up

        Returns:
            Dict with pipeline record including version_hash, or None if not found
        """
        pipelines_cache = self._get_all_pipelines_cached()
        return pipelines_cache.get(pipeline_name)

    def get_registered_nodes(self, pipeline_name: str) -> Dict[str, str]:
        """
        Get existing registered nodes for a pipeline with their version hashes.

        Args:
            pipeline_name: Name of the pipeline to look up nodes for

        Returns:
            Dict mapping node_name -> version_hash for all registered nodes
        """
        nodes_cache = self._get_all_nodes_cached()
        return nodes_cache.get(pipeline_name, {})

    def get_all_registered_pipelines(self) -> Dict[str, str]:
        """
        Get all registered pipelines with their version hashes.

        Returns:
            Dict mapping pipeline_name -> version_hash
        """
        pipelines_cache = self._get_all_pipelines_cached()
        return {name: data.get("version_hash", "") for name, data in pipelines_cache.items()}

    def get_all_registered_nodes(self, pipeline_names: List[str]) -> Dict[str, Dict[str, str]]:
        """
        Get all registered nodes for multiple pipelines with their version hashes.

        Args:
            pipeline_names: List of pipeline names to look up nodes for

        Returns:
            Dict mapping pipeline_name -> {node_name -> version_hash}
        """
        nodes_cache = self._get_all_nodes_cached()
        return {name: nodes_cache.get(name, {}) for name in pipeline_names}

    def register_pipelines_batch(
        self,
        records: List[Dict[str, Any]],
    ) -> None:
        """
        Batch registers/upserts multiple pipeline definitions to meta_pipelines.

        Args:
            records: List of dicts with keys: pipeline_name, version_hash, description,
                     layer, schedule, tags_json
        """
        if not self.spark and not self.engine:
            return

        if not records:
            return

        try:
            from datetime import datetime, timezone

            if self.spark:
                from pyspark.sql import functions as F

                schema = self._get_schema_meta_pipelines()
                input_schema = StructType(schema.fields[:-1])  # Exclude updated_at

                rows = [
                    (
                        r["pipeline_name"],
                        r["version_hash"],
                        r["description"],
                        r["layer"],
                        r["schedule"],
                        r["tags_json"],
                    )
                    for r in records
                ]
                df = self.spark.createDataFrame(rows, input_schema)
                df = df.withColumn("updated_at", F.current_timestamp())

                view_name = "_odibi_meta_pipelines_batch_upsert"
                df.createOrReplaceTempView(view_name)

                target_path = self.tables["meta_pipelines"]

                merge_sql = f"""
                    MERGE INTO delta.`{target_path}` AS target
                    USING {view_name} AS source
                    ON target.pipeline_name = source.pipeline_name
                    WHEN MATCHED THEN UPDATE SET
                        target.version_hash = source.version_hash,
                        target.description = source.description,
                        target.layer = source.layer,
                        target.schedule = source.schedule,
                        target.tags_json = source.tags_json,
                        target.updated_at = source.updated_at
                    WHEN NOT MATCHED THEN INSERT *
                """
                self.spark.sql(merge_sql)
                self.spark.catalog.dropTempView(view_name)

            elif self.engine:
                import pandas as pd

                data = {
                    "pipeline_name": [r["pipeline_name"] for r in records],
                    "version_hash": [r["version_hash"] for r in records],
                    "description": [r["description"] for r in records],
                    "layer": [r["layer"] for r in records],
                    "schedule": [r["schedule"] for r in records],
                    "tags_json": [r["tags_json"] for r in records],
                    "updated_at": [datetime.now(timezone.utc) for _ in records],
                }
                df = pd.DataFrame(data)

                def do_write():
                    self.engine.write(
                        df,
                        connection=self.connection,
                        format="delta",
                        path=self.tables["meta_pipelines"],
                        mode="upsert",
                        options={"keys": ["pipeline_name"]},
                    )

                self._retry_with_backoff(do_write)

            self._pipelines_cache = None
            logger.debug(f"Batch registered {len(records)} pipeline(s)")

        except Exception as e:
            logger.warning(f"Failed to batch register pipelines: {e}")

    def register_nodes_batch(
        self,
        records: List[Dict[str, Any]],
    ) -> None:
        """
        Batch registers/upserts multiple node definitions to meta_nodes.

        Args:
            records: List of dicts with keys: pipeline_name, node_name, version_hash,
                     type, config_json
        """
        if not self.spark and not self.engine:
            return

        if not records:
            return

        try:
            from datetime import datetime, timezone

            if self.spark:
                from pyspark.sql import functions as F

                schema = self._get_schema_meta_nodes()
                input_schema = StructType(schema.fields[:-1])  # Exclude updated_at

                rows = [
                    (
                        r["pipeline_name"],
                        r["node_name"],
                        r["version_hash"],
                        r["type"],
                        r["config_json"],
                    )
                    for r in records
                ]
                df = self.spark.createDataFrame(rows, input_schema)
                df = df.withColumn("updated_at", F.current_timestamp())

                view_name = "_odibi_meta_nodes_batch_upsert"
                df.createOrReplaceTempView(view_name)

                target_path = self.tables["meta_nodes"]

                merge_sql = f"""
                    MERGE INTO delta.`{target_path}` AS target
                    USING {view_name} AS source
                    ON target.pipeline_name = source.pipeline_name
                       AND target.node_name = source.node_name
                    WHEN MATCHED THEN UPDATE SET
                        target.version_hash = source.version_hash,
                        target.type = source.type,
                        target.config_json = source.config_json,
                        target.updated_at = source.updated_at
                    WHEN NOT MATCHED THEN INSERT *
                """
                self.spark.sql(merge_sql)
                self.spark.catalog.dropTempView(view_name)

            elif self.engine:
                import pandas as pd

                data = {
                    "pipeline_name": [r["pipeline_name"] for r in records],
                    "node_name": [r["node_name"] for r in records],
                    "version_hash": [r["version_hash"] for r in records],
                    "type": [r["type"] for r in records],
                    "config_json": [r["config_json"] for r in records],
                    "updated_at": [datetime.now(timezone.utc) for _ in records],
                }
                df = pd.DataFrame(data)

                def do_write():
                    self.engine.write(
                        df,
                        connection=self.connection,
                        format="delta",
                        path=self.tables["meta_nodes"],
                        mode="upsert",
                        options={"keys": ["pipeline_name", "node_name"]},
                    )

                self._retry_with_backoff(do_write)

            self._nodes_cache = None
            logger.debug(f"Batch registered {len(records)} node(s)")

        except Exception as e:
            logger.warning(f"Failed to batch register nodes: {e}")

    def register_outputs_batch(
        self,
        records: List[Dict[str, Any]],
    ) -> None:
        """
        Batch registers/upserts multiple node outputs to meta_outputs.

        Uses MERGE INTO for efficient upsert. This is performance critical -
        all outputs are collected during pipeline execution and written in a
        single batch at the end.

        Args:
            records: List of dicts with keys:
                - pipeline_name: str (pipeline identifier)
                - node_name: str (node identifier)
                - output_type: str ("external_table" | "managed_table")
                - connection_name: str (nullable, for external tables)
                - path: str (nullable, storage path)
                - format: str (delta, parquet, etc.)
                - table_name: str (nullable, registered table name)
                - last_run: datetime (execution timestamp)
                - row_count: int (nullable)
        """
        if not self.spark and not self.engine:
            return

        if not records:
            return

        try:
            if self.spark:
                from pyspark.sql import functions as F

                schema = self._get_schema_meta_outputs()
                input_schema = StructType(schema.fields[:-1])  # Exclude updated_at

                rows = [
                    (
                        r["pipeline_name"],
                        r["node_name"],
                        r["output_type"],
                        r.get("connection_name"),
                        r.get("path"),
                        r.get("format"),
                        r.get("table_name"),
                        r["last_run"],
                        r.get("row_count"),
                    )
                    for r in records
                ]
                df = self.spark.createDataFrame(rows, input_schema)
                df = df.withColumn("updated_at", F.current_timestamp())

                view_name = "_odibi_meta_outputs_batch_upsert"
                df.createOrReplaceTempView(view_name)

                target_path = self.tables["meta_outputs"]

                merge_sql = f"""
                    MERGE INTO delta.`{target_path}` AS target
                    USING {view_name} AS source
                    ON target.pipeline_name = source.pipeline_name
                       AND target.node_name = source.node_name
                    WHEN MATCHED THEN UPDATE SET
                        target.output_type = source.output_type,
                        target.connection_name = source.connection_name,
                        target.path = source.path,
                        target.format = source.format,
                        target.table_name = source.table_name,
                        target.last_run = source.last_run,
                        target.row_count = source.row_count,
                        target.updated_at = source.updated_at
                    WHEN NOT MATCHED THEN INSERT *
                """
                self.spark.sql(merge_sql)
                self.spark.catalog.dropTempView(view_name)

            elif self.engine:
                import pandas as pd

                data = {
                    "pipeline_name": [r["pipeline_name"] for r in records],
                    "node_name": [r["node_name"] for r in records],
                    "output_type": [r["output_type"] for r in records],
                    "connection_name": [r.get("connection_name") for r in records],
                    "path": [r.get("path") for r in records],
                    "format": [r.get("format") for r in records],
                    "table_name": [r.get("table_name") for r in records],
                    "last_run": [r["last_run"] for r in records],
                    "row_count": [r.get("row_count") for r in records],
                    "updated_at": [datetime.now(timezone.utc) for _ in records],
                }
                df = pd.DataFrame(data)

                def do_write():
                    self.engine.write(
                        df,
                        connection=self.connection,
                        format="delta",
                        path=self.tables["meta_outputs"],
                        mode="upsert",
                        options={"keys": ["pipeline_name", "node_name"]},
                    )

                self._retry_with_backoff(do_write)

            self._outputs_cache = None
            logger.debug(f"Batch registered {len(records)} output(s)")

        except Exception as e:
            logger.warning(f"Failed to batch register outputs: {e}")

    def _get_all_outputs_cached(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all outputs with caching.

        Returns:
            Dict mapping "{pipeline_name}.{node_name}" -> output record
        """
        # Thread-safe check: if cache exists and is populated, return it
        if self._outputs_cache is not None:
            return self._outputs_cache

        # Build cache in a local variable first to avoid race conditions
        cache: Dict[str, Dict[str, Any]] = {}
        if not self.spark and not self.engine:
            self._outputs_cache = cache
            return self._outputs_cache

        try:
            if self.spark:
                df = self.spark.read.format("delta").load(self.tables["meta_outputs"])
                rows = df.collect()
                for row in rows:
                    row_dict = row.asDict()
                    key = f"{row_dict['pipeline_name']}.{row_dict['node_name']}"
                    cache[key] = row_dict
            elif self.engine:
                df = self._read_local_table(self.tables["meta_outputs"])
                if not df.empty and "pipeline_name" in df.columns:
                    for _, row in df.iterrows():
                        key = f"{row['pipeline_name']}.{row['node_name']}"
                        cache[key] = row.to_dict()
        except Exception as e:
            logger.warning(f"Could not cache outputs from {self.tables.get('meta_outputs')}: {e}")

        # Atomic assignment after building complete cache
        self._outputs_cache = cache
        return self._outputs_cache

    def get_node_output(
        self,
        pipeline_name: str,
        node_name: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieves output metadata for a specific node.

        Used for cross-pipeline dependency resolution ($pipeline.node references).

        Args:
            pipeline_name: Name of the pipeline
            node_name: Name of the node

        Returns:
            Dict with output metadata or None if not found.
            Keys: pipeline_name, node_name, output_type, connection_name,
                  path, format, table_name, last_run, row_count
        """
        outputs_cache = self._get_all_outputs_cached()
        key = f"{pipeline_name}.{node_name}"
        return outputs_cache.get(key)

    def register_outputs_from_config(
        self,
        pipeline_config: Any,
    ) -> int:
        """
        Pre-register node outputs from pipeline config without running the pipeline.

        Scans pipeline nodes for output locations (write blocks, merge/scd2 params)
        and registers them to meta_outputs. This enables cross-pipeline references
        without requiring the source pipeline to have run first.

        Args:
            pipeline_config: Pipeline configuration object with nodes

        Returns:
            Number of outputs registered
        """
        from datetime import datetime

        records = []
        pipeline_name = pipeline_config.pipeline

        for node in pipeline_config.nodes:
            output_info = self._extract_node_output_info(node)
            if output_info:
                records.append(
                    {
                        "pipeline_name": pipeline_name,
                        "node_name": node.name,
                        "output_type": output_info.get("output_type", "external_table"),
                        "connection_name": output_info.get("connection"),
                        "path": output_info.get("path"),
                        "format": output_info.get("format", "delta"),
                        "table_name": output_info.get("register_table"),
                        "last_run": datetime.now(),
                        "row_count": None,
                    }
                )

        if records:
            self.register_outputs_batch(records)
            self._outputs_cache = None

        return len(records)

    def _extract_node_output_info(self, node_config: Any) -> Optional[Dict[str, Any]]:
        """
        Extract output location from a node config.

        Checks in order of precedence:
        1. Explicit write block
        2. merge/scd2 in transform steps
        3. Top-level merge/scd2 transformer

        Args:
            node_config: Node configuration object

        Returns:
            Dict with connection, path, format, register_table or None
        """
        if node_config.write:
            write_cfg = node_config.write
            output_type = (
                "managed_table" if write_cfg.table and not write_cfg.path else "external_table"
            )
            return {
                "connection": write_cfg.connection,
                "path": write_cfg.path,
                "format": write_cfg.format or "delta",
                "register_table": write_cfg.register_table or write_cfg.table,
                "output_type": output_type,
            }

        output_functions = {"merge", "scd2"}

        if node_config.transform and node_config.transform.steps:
            for step in reversed(node_config.transform.steps):
                if isinstance(step, str):
                    continue

                if hasattr(step, "function") and step.function in output_functions:
                    params = step.params or {}
                    connection = params.get("connection")
                    path = params.get("path") or params.get("target")
                    register_table = params.get("register_table")

                    if connection and path:
                        return {
                            "connection": connection,
                            "path": path,
                            "format": "delta",
                            "register_table": register_table,
                            "output_type": "managed_table" if register_table else "external_table",
                        }

        if node_config.transformer in output_functions and node_config.params:
            params = node_config.params
            connection = params.get("connection")
            path = params.get("path") or params.get("target")
            register_table = params.get("register_table")

            if connection and path:
                return {
                    "connection": connection,
                    "path": path,
                    "format": "delta",
                    "register_table": register_table,
                    "output_type": "managed_table" if register_table else "external_table",
                }

        return None

    def _prepare_pipeline_record(self, pipeline_config: Any) -> Dict[str, Any]:
        """Prepare a pipeline record for batch registration."""
        from odibi.utils.hashing import calculate_pipeline_hash

        version_hash = calculate_pipeline_hash(pipeline_config)

        all_tags = set()
        for node in pipeline_config.nodes:
            if node.tags:
                all_tags.update(node.tags)

        return {
            "pipeline_name": pipeline_config.pipeline,
            "version_hash": version_hash,
            "description": pipeline_config.description or "",
            "layer": pipeline_config.layer or "",
            "schedule": "",
            "tags_json": json.dumps(list(all_tags)),
        }

    def register_pipeline(
        self,
        pipeline_config: Any,
        project_config: Optional[Any] = None,
        skip_if_unchanged: bool = False,
    ) -> bool:
        """
        Registers/Upserts a pipeline definition to meta_pipelines.

        .. deprecated::
            Use :meth:`register_pipelines_batch` for better performance.

        Args:
            pipeline_config: The pipeline configuration object
            project_config: Optional project configuration
            skip_if_unchanged: If True, skip write if version_hash matches existing

        Returns:
            True if write was performed, False if skipped
        """
        import warnings

        warnings.warn(
            "register_pipeline is deprecated, use register_pipelines_batch for better performance",
            DeprecationWarning,
            stacklevel=2,
        )

        if not self.spark and not self.engine:
            return False

        try:
            record = self._prepare_pipeline_record(pipeline_config)

            if skip_if_unchanged:
                existing = self.get_registered_pipeline(pipeline_config.pipeline)
                if existing and existing.get("version_hash") == record["version_hash"]:
                    logger.debug(f"Skipping pipeline '{pipeline_config.pipeline}' - unchanged")
                    return False

            self.register_pipelines_batch([record])
            return True

        except Exception as e:
            logger.warning(f"Failed to register pipeline '{pipeline_config.pipeline}': {e}")
            return False

    def _prepare_node_record(self, pipeline_name: str, node_config: Any) -> Dict[str, Any]:
        """Prepare a node record for batch registration."""
        from odibi.utils.hashing import calculate_node_hash

        version_hash = calculate_node_hash(node_config)

        node_type = "transform"
        if node_config.read:
            node_type = "read"
        if node_config.write:
            node_type = "write"

        if hasattr(node_config, "model_dump"):
            dump = node_config.model_dump(mode="json", exclude={"description", "tags", "log_level"})
        else:
            dump = node_config.model_dump(exclude={"description", "tags", "log_level"})

        return {
            "pipeline_name": pipeline_name,
            "node_name": node_config.name,
            "version_hash": version_hash,
            "type": node_type,
            "config_json": json.dumps(dump),
        }

    def register_node(
        self,
        pipeline_name: str,
        node_config: Any,
        skip_if_unchanged: bool = False,
        existing_hash: Optional[str] = None,
    ) -> bool:
        """
        Registers/Upserts a node definition to meta_nodes.

        .. deprecated::
            Use :meth:`register_nodes_batch` for better performance.

        Args:
            pipeline_name: Name of the parent pipeline
            node_config: The node configuration object
            skip_if_unchanged: If True, skip write if version_hash matches existing
            existing_hash: Pre-fetched existing hash (to avoid re-reading)

        Returns:
            True if write was performed, False if skipped
        """
        import warnings

        warnings.warn(
            "register_node is deprecated, use register_nodes_batch for better performance",
            DeprecationWarning,
            stacklevel=2,
        )

        if not self.spark and not self.engine:
            return False

        try:
            record = self._prepare_node_record(pipeline_name, node_config)

            if skip_if_unchanged:
                current_hash = existing_hash
                if current_hash is None:
                    nodes = self.get_registered_nodes(pipeline_name)
                    current_hash = nodes.get(node_config.name)

                if current_hash == record["version_hash"]:
                    logger.debug(f"Skipping node '{node_config.name}' - unchanged")
                    return False

            self.register_nodes_batch([record])
            return True

        except Exception as e:
            logger.warning(f"Failed to register node '{node_config.name}': {e}")
            return False

    def log_run(
        self,
        run_id: str,
        pipeline_name: str,
        node_name: str,
        status: str,
        rows_processed: Optional[int] = 0,
        duration_ms: Optional[int] = 0,
        metrics_json: Optional[str] = "{}",
    ) -> None:
        """
        Logs execution telemetry to meta_runs.

        Note: For better performance with multiple nodes, use log_runs_batch() instead.
        """
        environment = getattr(self.config, "environment", None)
        project = self._project

        # SQL Server mode - direct insert
        if self.is_sql_server_mode:
            self._log_run_sql_server(
                run_id,
                project,
                pipeline_name,
                node_name,
                status,
                rows_processed,
                duration_ms,
                metrics_json,
                environment,
            )
            return

        if not self.spark and not self.engine:
            return

        def _do_log_run():
            if self.spark:
                from pyspark.sql import functions as F

                rows = [
                    (
                        run_id,
                        project,
                        pipeline_name,
                        node_name,
                        status,
                        rows_processed,
                        duration_ms,
                        metrics_json,
                        environment,
                    )
                ]
                schema = self._get_schema_meta_runs()
                input_schema = StructType(schema.fields[:-2])

                df = self.spark.createDataFrame(rows, input_schema)
                df = df.withColumn("timestamp", F.current_timestamp()).withColumn(
                    "date", F.to_date(F.col("timestamp"))
                )

                df.write.format("delta").mode("append").save(self.tables["meta_runs"])
            elif self.engine:
                from datetime import datetime, timezone

                import pandas as pd

                timestamp = datetime.now(timezone.utc)

                data = {
                    "run_id": [run_id],
                    "project": [project],
                    "pipeline_name": [pipeline_name],
                    "node_name": [node_name],
                    "status": [status],
                    "rows_processed": [rows_processed],
                    "duration_ms": [duration_ms],
                    "metrics_json": [metrics_json],
                    "environment": [environment],
                    "timestamp": [timestamp],
                    "date": [timestamp.date()],
                }
                df = pd.DataFrame(data)

                self.engine.write(
                    df,
                    connection=self.connection,
                    format="delta",
                    path=self.tables["meta_runs"],
                    mode="append",
                )

        try:
            self._retry_with_backoff(_do_log_run)
        except Exception as e:
            logger.warning(f"Failed to log run to system catalog: {e}")

    def _log_run_sql_server(
        self,
        run_id: str,
        project: Optional[str],
        pipeline_name: str,
        node_name: str,
        status: str,
        rows_processed: int,
        duration_ms: int,
        metrics_json: str,
        environment: Optional[str],
    ) -> None:
        """Log a run to SQL Server meta_runs table."""
        schema_name = getattr(self.config, "schema_name", None) or "odibi_system"
        try:
            sql = f"""
            INSERT INTO [{schema_name}].[meta_runs]
            (run_id, project, pipeline_name, node_name, status, rows_processed, duration_ms,
             metrics_json, environment, timestamp, date)
            VALUES (:run_id, :project, :pipeline, :node, :status, :rows, :duration,
                    :metrics, :env, GETUTCDATE(), CAST(GETUTCDATE() AS DATE))
            """
            self.connection.execute(
                sql,
                {
                    "run_id": run_id,
                    "project": project,
                    "pipeline": pipeline_name,
                    "node": node_name,
                    "status": status,
                    "rows": rows_processed or 0,
                    "duration": duration_ms or 0,
                    "metrics": metrics_json or "{}",
                    "env": environment,
                },
            )
        except Exception as e:
            logger.warning(f"Failed to log run to SQL Server: {e}")

    def log_runs_batch(
        self,
        records: List[Dict[str, Any]],
    ) -> None:
        """
        Batch logs multiple execution records to meta_runs in a single write.

        This is much more efficient than calling log_run() for each node individually.

        Args:
            records: List of dicts with keys: run_id, pipeline_name, node_name,
                     status, rows_processed, duration_ms, metrics_json
        """
        if not records:
            return

        environment = getattr(self.config, "environment", None)
        project = self._project

        # SQL Server mode - batch insert
        if self.is_sql_server_mode:
            for r in records:
                self._log_run_sql_server(
                    r["run_id"],
                    project,
                    r["pipeline_name"],
                    r["node_name"],
                    r["status"],
                    r.get("rows_processed", 0),
                    r.get("duration_ms", 0),
                    r.get("metrics_json", "{}"),
                    environment,
                )
            logger.debug(f"Batch logged {len(records)} run records to SQL Server")
            return

        if not self.spark and not self.engine:
            return

        def _do_batch_log():
            if self.spark:
                from pyspark.sql import functions as F

                rows = [
                    (
                        r["run_id"],
                        project,
                        r["pipeline_name"],
                        r["node_name"],
                        r["status"],
                        r.get("rows_processed", 0),
                        r.get("duration_ms", 0),
                        r.get("metrics_json", "{}"),
                        environment,
                    )
                    for r in records
                ]
                schema = self._get_schema_meta_runs()
                input_schema = StructType(schema.fields[:-2])

                df = self.spark.createDataFrame(rows, input_schema)
                df = df.withColumn("timestamp", F.current_timestamp()).withColumn(
                    "date", F.to_date(F.col("timestamp"))
                )

                df.write.format("delta").mode("append").save(self.tables["meta_runs"])
                logger.debug(f"Batch logged {len(records)} run records to meta_runs")

            elif self.engine:
                from datetime import datetime, timezone

                import pandas as pd

                timestamp = datetime.now(timezone.utc)

                data = {
                    "run_id": [r["run_id"] for r in records],
                    "project": [project] * len(records),
                    "pipeline_name": [r["pipeline_name"] for r in records],
                    "node_name": [r["node_name"] for r in records],
                    "status": [r["status"] for r in records],
                    "rows_processed": [r.get("rows_processed", 0) for r in records],
                    "duration_ms": [r.get("duration_ms", 0) for r in records],
                    "metrics_json": [r.get("metrics_json", "{}") for r in records],
                    "environment": [environment] * len(records),
                    "timestamp": [timestamp] * len(records),
                    "date": [timestamp.date()] * len(records),
                }
                df = pd.DataFrame(data)

                self.engine.write(
                    df,
                    connection=self.connection,
                    format="delta",
                    path=self.tables["meta_runs"],
                    mode="append",
                )
                logger.debug(f"Batch logged {len(records)} run records to meta_runs")

        try:
            self._retry_with_backoff(_do_batch_log)
        except Exception as e:
            logger.warning(f"Failed to batch log runs to system catalog: {e}")

    # =========================================================================
    # LEVERAGE SUMMARY TABLES - OBSERVABILITY LOGGING
    # =========================================================================

    def log_pipeline_run(self, pipeline_run: Dict[str, Any]) -> None:
        """
        Log a completed pipeline run to meta_pipeline_runs.

        Append-only, called exactly once per pipeline run after completion.

        Args:
            pipeline_run: Dict with keys:
                run_id, pipeline_name, owner, layer, run_start_at, run_end_at,
                duration_ms, status, nodes_total, nodes_succeeded, nodes_failed,
                nodes_skipped, rows_processed, error_summary, terminal_nodes,
                environment, created_at
        """
        if self.is_sql_server_mode:
            self._log_pipeline_run_sql_server(pipeline_run)
            return

        if not self.spark and not self.engine:
            return

        project = self._project

        def _do_log():
            if self.spark:
                from pyspark.sql.types import (
                    LongType,
                    StringType,
                    StructField,
                    StructType,
                    TimestampType,
                )

                schema = StructType(
                    [
                        StructField("run_id", StringType(), False),
                        StructField("project", StringType(), True),
                        StructField("pipeline_name", StringType(), False),
                        StructField("owner", StringType(), True),
                        StructField("layer", StringType(), True),
                        StructField("run_start_at", TimestampType(), True),
                        StructField("run_end_at", TimestampType(), True),
                        StructField("duration_ms", LongType(), True),
                        StructField("status", StringType(), True),
                        StructField("nodes_total", LongType(), True),
                        StructField("nodes_succeeded", LongType(), True),
                        StructField("nodes_failed", LongType(), True),
                        StructField("nodes_skipped", LongType(), True),
                        StructField("rows_processed", LongType(), True),
                        StructField("error_summary", StringType(), True),
                        StructField("terminal_nodes", StringType(), True),
                        StructField("environment", StringType(), True),
                        StructField("databricks_cluster_id", StringType(), True),
                        StructField("databricks_job_id", StringType(), True),
                        StructField("databricks_workspace_id", StringType(), True),
                        StructField("estimated_cost_usd", DoubleType(), True),
                        StructField("actual_cost_usd", DoubleType(), True),
                        StructField("cost_source", StringType(), True),
                        StructField("created_at", TimestampType(), True),
                    ]
                )

                row = (
                    pipeline_run["run_id"],
                    project,
                    pipeline_run["pipeline_name"],
                    pipeline_run.get("owner"),
                    pipeline_run.get("layer"),
                    pipeline_run.get("run_start_at"),
                    pipeline_run.get("run_end_at"),
                    pipeline_run.get("duration_ms"),
                    pipeline_run.get("status"),
                    pipeline_run.get("nodes_total"),
                    pipeline_run.get("nodes_succeeded"),
                    pipeline_run.get("nodes_failed"),
                    pipeline_run.get("nodes_skipped"),
                    pipeline_run.get("rows_processed"),
                    pipeline_run.get("error_summary"),
                    pipeline_run.get("terminal_nodes"),
                    pipeline_run.get("environment"),
                    pipeline_run.get("databricks_cluster_id"),
                    pipeline_run.get("databricks_job_id"),
                    pipeline_run.get("databricks_workspace_id"),
                    pipeline_run.get("estimated_cost_usd"),
                    pipeline_run.get("actual_cost_usd"),
                    pipeline_run.get("cost_source"),
                    pipeline_run.get("created_at"),
                )

                df = self.spark.createDataFrame([row], schema)
                df.write.format("delta").mode("append").save(self.tables["meta_pipeline_runs"])
                logger.debug(f"Logged pipeline run {pipeline_run['run_id']}")

            elif self.engine:
                import pandas as pd

                data = {
                    "run_id": [pipeline_run["run_id"]],
                    "project": [project],
                    "pipeline_name": [pipeline_run["pipeline_name"]],
                    "owner": [pipeline_run.get("owner")],
                    "layer": [pipeline_run.get("layer")],
                    "run_start_at": [pipeline_run.get("run_start_at")],
                    "run_end_at": [pipeline_run.get("run_end_at")],
                    "duration_ms": [pipeline_run.get("duration_ms")],
                    "status": [pipeline_run.get("status")],
                    "nodes_total": [pipeline_run.get("nodes_total")],
                    "nodes_succeeded": [pipeline_run.get("nodes_succeeded")],
                    "nodes_failed": [pipeline_run.get("nodes_failed")],
                    "nodes_skipped": [pipeline_run.get("nodes_skipped")],
                    "rows_processed": [pipeline_run.get("rows_processed")],
                    "error_summary": [pipeline_run.get("error_summary")],
                    "terminal_nodes": [pipeline_run.get("terminal_nodes")],
                    "environment": [pipeline_run.get("environment")],
                    "databricks_cluster_id": [pipeline_run.get("databricks_cluster_id")],
                    "databricks_job_id": [pipeline_run.get("databricks_job_id")],
                    "databricks_workspace_id": [pipeline_run.get("databricks_workspace_id")],
                    "estimated_cost_usd": [pipeline_run.get("estimated_cost_usd")],
                    "actual_cost_usd": [pipeline_run.get("actual_cost_usd")],
                    "cost_source": [pipeline_run.get("cost_source")],
                    "created_at": [pipeline_run.get("created_at")],
                }
                df = pd.DataFrame(data)

                self.engine.write(
                    df,
                    connection=self.connection,
                    format="delta",
                    path=self.tables["meta_pipeline_runs"],
                    mode="append",
                )
                logger.debug(f"Logged pipeline run {pipeline_run['run_id']}")

        try:
            self._retry_with_backoff(_do_log)
        except Exception as e:
            logger.warning(f"Failed to log pipeline run: {e}")

    def _log_pipeline_run_sql_server(self, pipeline_run: Dict[str, Any]) -> None:
        """SQL Server: Log pipeline run to meta_pipeline_runs."""
        if not self._sql_server_table_exists("meta_pipeline_runs"):
            raise NotImplementedError(
                "meta_pipeline_runs table does not exist in SQL Server. "
                "SQL Server backend for observability tables not yet implemented."
            )

        schema_name = getattr(self.config, "schema_name", None) or "odibi_system"
        project = self._project

        # Handle NaN values - SQL Server rejects float NaN
        rows_processed = pipeline_run.get("rows_processed")
        if (
            rows_processed is not None
            and isinstance(rows_processed, float)
            and rows_processed != rows_processed
        ):
            rows_processed = None

        try:
            sql = f"""
            MERGE INTO [{schema_name}].[meta_pipeline_runs] AS target
            USING (SELECT :run_id AS run_id) AS source
            ON target.run_id = source.run_id
            WHEN MATCHED THEN UPDATE SET
                project = :project,
                pipeline_name = :pipeline_name,
                owner = :owner,
                layer = :layer,
                run_start_at = :run_start_at,
                run_end_at = :run_end_at,
                duration_ms = :duration_ms,
                status = :status,
                nodes_total = :nodes_total,
                nodes_succeeded = :nodes_succeeded,
                nodes_failed = :nodes_failed,
                nodes_skipped = :nodes_skipped,
                rows_processed = :rows_processed,
                error_summary = :error_summary,
                terminal_nodes = :terminal_nodes,
                environment = :environment,
                databricks_cluster_id = :databricks_cluster_id,
                databricks_job_id = :databricks_job_id,
                databricks_workspace_id = :databricks_workspace_id,
                created_at = :created_at
            WHEN NOT MATCHED THEN INSERT
                (run_id, project, pipeline_name, owner, layer, run_start_at, run_end_at, duration_ms,
                 status, nodes_total, nodes_succeeded, nodes_failed, nodes_skipped,
                 rows_processed, error_summary, terminal_nodes, environment,
                 databricks_cluster_id, databricks_job_id, databricks_workspace_id, created_at)
            VALUES (:run_id, :project, :pipeline_name, :owner, :layer, :run_start_at, :run_end_at, :duration_ms,
                    :status, :nodes_total, :nodes_succeeded, :nodes_failed, :nodes_skipped,
                    :rows_processed, :error_summary, :terminal_nodes, :environment,
                    :databricks_cluster_id, :databricks_job_id, :databricks_workspace_id, :created_at);
            """
            self.connection.execute(
                sql,
                {
                    "run_id": pipeline_run["run_id"],
                    "project": project,
                    "pipeline_name": pipeline_run["pipeline_name"],
                    "owner": pipeline_run.get("owner"),
                    "layer": pipeline_run.get("layer"),
                    "run_start_at": pipeline_run.get("run_start_at"),
                    "run_end_at": pipeline_run.get("run_end_at"),
                    "duration_ms": pipeline_run.get("duration_ms"),
                    "status": pipeline_run.get("status"),
                    "nodes_total": pipeline_run.get("nodes_total"),
                    "nodes_succeeded": pipeline_run.get("nodes_succeeded"),
                    "nodes_failed": pipeline_run.get("nodes_failed"),
                    "nodes_skipped": pipeline_run.get("nodes_skipped"),
                    "rows_processed": rows_processed,
                    "error_summary": pipeline_run.get("error_summary"),
                    "terminal_nodes": pipeline_run.get("terminal_nodes"),
                    "environment": pipeline_run.get("environment"),
                    "databricks_cluster_id": pipeline_run.get("databricks_cluster_id"),
                    "databricks_job_id": pipeline_run.get("databricks_job_id"),
                    "databricks_workspace_id": pipeline_run.get("databricks_workspace_id"),
                    "created_at": pipeline_run.get("created_at"),
                },
            )
            logger.debug(f"Upserted pipeline run to SQL Server: {pipeline_run['run_id']}")
        except Exception as e:
            logger.warning(f"Failed to log pipeline run to SQL Server: {e}")

    def _sql_server_table_exists(self, table_name: str) -> bool:
        """Check if a table exists in SQL Server."""
        if not self.is_sql_server_mode:
            return False
        schema_name = getattr(self.config, "schema_name", None) or "odibi_system"
        try:
            result = self.connection.execute(
                """
                SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
                """,
                {"schema": schema_name, "table": table_name},
            )
            return result.fetchone() is not None
        except Exception:
            return False

    def log_node_runs_batch(self, node_results: List[Dict[str, Any]]) -> None:
        """
        Batch log node execution results to meta_node_runs.

        Append-only, called once per pipeline run with all node results.

        Args:
            node_results: List of dicts with keys:
                run_id, node_id, pipeline_name, node_name, status,
                run_start_at, run_end_at, duration_ms, rows_processed,
                metrics_json, environment, created_at
        """
        if not node_results:
            return

        if self.is_sql_server_mode:
            self._log_node_runs_batch_sql_server(node_results)
            return

        if not self.spark and not self.engine:
            return

        project = self._project

        def _do_log():
            if self.spark:
                from pyspark.sql.types import (
                    LongType,
                    StringType,
                    StructField,
                    StructType,
                    TimestampType,
                )

                schema = StructType(
                    [
                        StructField("run_id", StringType(), False),
                        StructField("node_id", StringType(), False),
                        StructField("project", StringType(), True),
                        StructField("pipeline_name", StringType(), False),
                        StructField("node_name", StringType(), False),
                        StructField("status", StringType(), True),
                        StructField("run_start_at", TimestampType(), True),
                        StructField("run_end_at", TimestampType(), True),
                        StructField("duration_ms", LongType(), True),
                        StructField("rows_processed", LongType(), True),
                        StructField("estimated_cost_usd", DoubleType(), True),
                        StructField("metrics_json", StringType(), True),
                        StructField("environment", StringType(), True),
                        StructField("created_at", TimestampType(), True),
                    ]
                )

                rows = [
                    (
                        r["run_id"],
                        r["node_id"],
                        project,
                        r["pipeline_name"],
                        r["node_name"],
                        r.get("status"),
                        r.get("run_start_at"),
                        r.get("run_end_at"),
                        r.get("duration_ms"),
                        r.get("rows_processed"),
                        r.get("estimated_cost_usd"),
                        r.get("metrics_json"),
                        r.get("environment"),
                        r.get("created_at"),
                    )
                    for r in node_results
                ]

                df = self.spark.createDataFrame(rows, schema)
                df.write.format("delta").mode("append").save(self.tables["meta_node_runs"])
                logger.debug(f"Batch logged {len(node_results)} node runs")

            elif self.engine:
                import pandas as pd

                data = {
                    "run_id": [r["run_id"] for r in node_results],
                    "node_id": [r["node_id"] for r in node_results],
                    "project": [project] * len(node_results),
                    "pipeline_name": [r["pipeline_name"] for r in node_results],
                    "node_name": [r["node_name"] for r in node_results],
                    "status": [r.get("status") for r in node_results],
                    "run_start_at": [r.get("run_start_at") for r in node_results],
                    "run_end_at": [r.get("run_end_at") for r in node_results],
                    "duration_ms": [r.get("duration_ms") for r in node_results],
                    "rows_processed": [r.get("rows_processed") for r in node_results],
                    "estimated_cost_usd": [r.get("estimated_cost_usd") for r in node_results],
                    "metrics_json": [r.get("metrics_json") for r in node_results],
                    "environment": [r.get("environment") for r in node_results],
                    "created_at": [r.get("created_at") for r in node_results],
                }
                df = pd.DataFrame(data)

                self.engine.write(
                    df,
                    connection=self.connection,
                    format="delta",
                    path=self.tables["meta_node_runs"],
                    mode="append",
                )
                logger.debug(f"Batch logged {len(node_results)} node runs")

        try:
            self._retry_with_backoff(_do_log)
        except Exception as e:
            logger.warning(f"Failed to batch log node runs: {e}")

    def _log_node_runs_batch_sql_server(self, node_results: List[Dict[str, Any]]) -> None:
        """SQL Server: Batch log node runs."""
        if not self._sql_server_table_exists("meta_node_runs"):
            raise NotImplementedError(
                "meta_node_runs table does not exist in SQL Server. "
                "SQL Server backend for observability tables not yet implemented."
            )

        schema_name = getattr(self.config, "schema_name", None) or "odibi_system"
        project = self._project
        try:
            for r in node_results:
                # Handle NaN values - SQL Server rejects float NaN
                rows_processed = r.get("rows_processed")
                if (
                    rows_processed is not None
                    and isinstance(rows_processed, float)
                    and rows_processed != rows_processed
                ):
                    rows_processed = None

                sql = f"""
                MERGE INTO [{schema_name}].[meta_node_runs] AS target
                USING (SELECT :run_id AS run_id, :node_id AS node_id) AS source
                ON target.run_id = source.run_id AND target.node_id = source.node_id
                WHEN MATCHED THEN UPDATE SET
                    project = :project,
                    pipeline_name = :pipeline_name,
                    node_name = :node_name,
                    status = :status,
                    run_start_at = :run_start_at,
                    run_end_at = :run_end_at,
                    duration_ms = :duration_ms,
                    rows_processed = :rows_processed,
                    metrics_json = :metrics_json,
                    environment = :environment,
                    created_at = :created_at
                WHEN NOT MATCHED THEN INSERT
                    (run_id, node_id, project, pipeline_name, node_name, status,
                     run_start_at, run_end_at, duration_ms, rows_processed,
                     metrics_json, environment, created_at)
                VALUES (:run_id, :node_id, :project, :pipeline_name, :node_name, :status,
                        :run_start_at, :run_end_at, :duration_ms, :rows_processed,
                        :metrics_json, :environment, :created_at);
                """
                self.connection.execute(
                    sql,
                    {
                        "run_id": r["run_id"],
                        "node_id": r["node_id"],
                        "project": project,
                        "pipeline_name": r["pipeline_name"],
                        "node_name": r["node_name"],
                        "status": r.get("status"),
                        "run_start_at": r.get("run_start_at"),
                        "run_end_at": r.get("run_end_at"),
                        "duration_ms": r.get("duration_ms"),
                        "rows_processed": rows_processed,
                        "metrics_json": r.get("metrics_json"),
                        "environment": r.get("environment"),
                        "created_at": r.get("created_at"),
                    },
                )
            logger.debug(f"Upserted {len(node_results)} node runs to SQL Server")
        except Exception as e:
            logger.warning(f"Failed to batch log node runs to SQL Server: {e}")

    def log_failure(
        self,
        failure_id: str,
        run_id: str,
        pipeline_name: str,
        node_name: str,
        error_type: str,
        error_message: str,
        stack_trace: Optional[str] = None,
    ) -> None:
        """
        Log a node failure to meta_failures.

        Append-only, called on each node exception.

        Args:
            failure_id: Unique ID for this failure (UUID)
            run_id: Pipeline run ID
            pipeline_name: Name of the pipeline
            node_name: Name of the failed node
            error_type: Exception class name
            error_message: Error message (max 1000 chars)
            stack_trace: Stack trace (max 2000 chars)
        """
        if self.is_sql_server_mode:
            self._log_failure_sql_server(
                failure_id, run_id, pipeline_name, node_name, error_type, error_message, stack_trace
            )
            return

        if not self.spark and not self.engine:
            return

        now = datetime.now(timezone.utc)
        project = self._project
        environment = getattr(self.config, "environment", None)

        def _do_log():
            if self.spark:
                from pyspark.sql.types import (
                    DateType,
                    StringType,
                    StructField,
                    StructType,
                    TimestampType,
                )

                schema = StructType(
                    [
                        StructField("failure_id", StringType(), False),
                        StructField("run_id", StringType(), False),
                        StructField("project", StringType(), True),
                        StructField("pipeline_name", StringType(), False),
                        StructField("node_name", StringType(), False),
                        StructField("error_type", StringType(), True),
                        StructField("error_message", StringType(), True),
                        StructField("error_code", StringType(), True),
                        StructField("stack_trace", StringType(), True),
                        StructField("environment", StringType(), True),
                        StructField("timestamp", TimestampType(), True),
                        StructField("date", DateType(), True),
                    ]
                )

                row = (
                    failure_id,
                    run_id,
                    project,
                    pipeline_name,
                    node_name,
                    error_type,
                    error_message[:1000] if error_message else None,
                    None,  # error_code (future taxonomy)
                    stack_trace[:2000] if stack_trace else None,
                    environment,
                    now,
                    now.date(),
                )

                df = self.spark.createDataFrame([row], schema)
                df.write.format("delta").mode("append").save(self.tables["meta_failures"])
                logger.debug(f"Logged failure {failure_id} for node {node_name}")

            elif self.engine:
                import pandas as pd

                data = {
                    "failure_id": [failure_id],
                    "run_id": [run_id],
                    "project": [project],
                    "pipeline_name": [pipeline_name],
                    "node_name": [node_name],
                    "error_type": [error_type],
                    "error_message": [error_message[:1000] if error_message else None],
                    "error_code": [None],  # future taxonomy
                    "stack_trace": [stack_trace[:2000] if stack_trace else None],
                    "environment": [environment],
                    "timestamp": [now],
                    "date": [now.date()],
                }
                df = pd.DataFrame(data)

                self.engine.write(
                    df,
                    connection=self.connection,
                    format="delta",
                    path=self.tables["meta_failures"],
                    mode="append",
                )
                logger.debug(f"Logged failure {failure_id} for node {node_name}")

        try:
            self._retry_with_backoff(_do_log)
        except Exception as e:
            logger.warning(f"Failed to log failure: {e}")

    def _log_failure_sql_server(
        self,
        failure_id: str,
        run_id: str,
        pipeline_name: str,
        node_name: str,
        error_type: str,
        error_message: str,
        stack_trace: Optional[str],
    ) -> None:
        """SQL Server: Log failure to meta_failures."""
        if not self._sql_server_table_exists("meta_failures"):
            raise NotImplementedError(
                "meta_failures table does not exist in SQL Server. "
                "SQL Server backend for observability tables not yet implemented."
            )

        schema_name = getattr(self.config, "schema_name", None) or "odibi_system"
        project = self._project
        environment = getattr(self.config, "environment", None)
        try:
            sql = f"""
            INSERT INTO [{schema_name}].[meta_failures]
            (failure_id, run_id, project, pipeline_name, node_name, error_type,
             error_message, error_code, stack_trace, environment, timestamp, date)
            VALUES (:failure_id, :run_id, :project, :pipeline_name, :node_name, :error_type,
                    :error_message, NULL, :stack_trace, :environment, GETUTCDATE(), CAST(GETUTCDATE() AS DATE))
            """
            self.connection.execute(
                sql,
                {
                    "failure_id": failure_id,
                    "run_id": run_id,
                    "project": project,
                    "pipeline_name": pipeline_name,
                    "node_name": node_name,
                    "error_type": error_type,
                    "error_message": error_message[:1000] if error_message else None,
                    "stack_trace": stack_trace[:2000] if stack_trace else None,
                    "environment": environment,
                },
            )
            logger.debug(f"Logged failure to SQL Server: {failure_id}")
        except Exception as e:
            logger.warning(f"Failed to log failure to SQL Server: {e}")

    # =========================================================================
    # LEVERAGE SUMMARY TABLES - QUERY HELPERS
    # =========================================================================

    def get_run_ids(
        self, pipeline_name: Optional[str] = None, since: Optional[date] = None
    ) -> List[str]:
        """
        Get run_ids from meta_pipeline_runs filtered by criteria.

        Args:
            pipeline_name: Optional pipeline name filter
            since: Optional date boundary (run_end_at >= since)

        Returns:
            List of run_id strings
        """

        if self.is_sql_server_mode:
            return self._get_run_ids_sql_server(pipeline_name, since)
        elif self.is_spark_mode:
            return self._get_run_ids_spark(pipeline_name, since)
        elif self.is_pandas_mode:
            return self._get_run_ids_pandas(pipeline_name, since)
        else:
            return []

    def _get_run_ids_spark(self, pipeline_name: Optional[str], since: Optional[date]) -> List[str]:
        """Spark: Query meta_pipeline_runs for run_ids."""
        from pyspark.sql import functions as F

        try:
            df = self.spark.read.format("delta").load(self.tables["meta_pipeline_runs"])
            if pipeline_name:
                df = df.filter(F.col("pipeline_name") == pipeline_name)
            if since:
                df = df.filter(F.col("run_end_at") >= F.lit(str(since)))
            return [row["run_id"] for row in df.select("run_id").collect()]
        except Exception as e:
            logger.warning(f"Failed to get run_ids: {e}")
            return []

    def _get_run_ids_pandas(self, pipeline_name: Optional[str], since: Optional[date]) -> List[str]:
        """Pandas/delta-rs: Query meta_pipeline_runs for run_ids."""
        try:
            from deltalake import DeltaTable

            storage_opts = self._get_storage_options()
            dt = DeltaTable(self.tables["meta_pipeline_runs"], storage_options=storage_opts or None)
            df = dt.to_pandas()

            if pipeline_name:
                df = df[df["pipeline_name"] == pipeline_name]
            if since:
                import pandas as pd

                df["run_end_at"] = pd.to_datetime(df["run_end_at"])
                df = df[df["run_end_at"].dt.date >= since]
            return df["run_id"].tolist()
        except Exception as e:
            logger.warning(f"Failed to get run_ids: {e}")
            return []

    def _get_run_ids_sql_server(
        self, pipeline_name: Optional[str], since: Optional[date]
    ) -> List[str]:
        """SQL Server: Query meta_pipeline_runs for run_ids."""
        if not self._sql_server_table_exists("meta_pipeline_runs"):
            raise NotImplementedError(
                "meta_pipeline_runs table does not exist in SQL Server. "
                "SQL Server backend for observability tables not yet implemented."
            )

        schema_name = getattr(self.config, "schema_name", None) or "odibi_system"
        try:
            sql = f"SELECT run_id FROM [{schema_name}].[meta_pipeline_runs] WHERE 1=1"
            params = {}
            if pipeline_name:
                sql += " AND pipeline_name = :pipeline_name"
                params["pipeline_name"] = pipeline_name
            if since:
                sql += " AND run_end_at >= :since"
                params["since"] = since
            result = self.connection.execute(sql, params)
            return [row[0] for row in result] if result else []
        except Exception as e:
            logger.warning(f"Failed to get run_ids from SQL Server: {e}")
            return []

    def get_pipeline_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a single pipeline run record by run_id.

        Args:
            run_id: Pipeline run ID

        Returns:
            Dict with pipeline run fields, or None if not found
        """
        if self.is_sql_server_mode:
            return self._get_pipeline_run_sql_server(run_id)
        elif self.is_spark_mode:
            return self._get_pipeline_run_spark(run_id)
        elif self.is_pandas_mode:
            return self._get_pipeline_run_pandas(run_id)
        else:
            return None

    def _get_pipeline_run_spark(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Spark: Get pipeline run by run_id."""
        from pyspark.sql import functions as F

        try:
            df = self.spark.read.format("delta").load(self.tables["meta_pipeline_runs"])
            df = df.filter(F.col("run_id") == run_id)
            rows = df.collect()
            if rows:
                return rows[0].asDict()
            return None
        except Exception as e:
            logger.warning(f"Failed to get pipeline run: {e}")
            return None

    def _get_pipeline_run_pandas(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Pandas/delta-rs: Get pipeline run by run_id."""
        try:
            from deltalake import DeltaTable

            storage_opts = self._get_storage_options()
            dt = DeltaTable(self.tables["meta_pipeline_runs"], storage_options=storage_opts or None)
            df = dt.to_pandas()
            df = df[df["run_id"] == run_id]
            if not df.empty:
                return df.iloc[0].to_dict()
            return None
        except Exception as e:
            logger.warning(f"Failed to get pipeline run: {e}")
            return None

    def _get_pipeline_run_sql_server(self, run_id: str) -> Optional[Dict[str, Any]]:
        """SQL Server: Get pipeline run by run_id."""
        if not self._sql_server_table_exists("meta_pipeline_runs"):
            raise NotImplementedError(
                "meta_pipeline_runs table does not exist in SQL Server. "
                "SQL Server backend for observability tables not yet implemented."
            )

        schema_name = getattr(self.config, "schema_name", None) or "odibi_system"
        try:
            sql = f"""
                SELECT run_id, pipeline_name, owner, layer, run_start_at, run_end_at,
                       duration_ms, status, nodes_total, nodes_succeeded, nodes_failed,
                       nodes_skipped, rows_processed, error_summary, terminal_nodes,
                       environment, created_at
                FROM [{schema_name}].[meta_pipeline_runs]
                WHERE run_id = :run_id
            """
            result = self.connection.execute(sql, {"run_id": run_id})
            rows = list(result) if result else []
            if rows:
                row = rows[0]
                return {
                    "run_id": row[0],
                    "pipeline_name": row[1],
                    "owner": row[2],
                    "layer": row[3],
                    "run_start_at": row[4],
                    "run_end_at": row[5],
                    "duration_ms": row[6],
                    "status": row[7],
                    "nodes_total": row[8],
                    "nodes_succeeded": row[9],
                    "nodes_failed": row[10],
                    "nodes_skipped": row[11],
                    "rows_processed": row[12],
                    "error_summary": row[13],
                    "terminal_nodes": row[14],
                    "environment": row[15],
                    "created_at": row[16],
                }
            return None
        except Exception as e:
            logger.warning(f"Failed to get pipeline run from SQL Server: {e}")
            return None

    def log_pattern(
        self,
        table_name: str,
        pattern_type: str,
        configuration: str,
        compliance_score: float,
    ) -> None:
        """
        Logs pattern usage to meta_patterns.
        """
        if not self.spark and not self.engine:
            return

        def _do_log_pattern():
            if self.spark:
                rows = [
                    (
                        table_name,
                        pattern_type,
                        configuration,
                        compliance_score,
                    )
                ]
                schema = self._get_schema_meta_patterns()

                df = self.spark.createDataFrame(rows, schema)

                # Append to meta_patterns
                df.write.format("delta").mode("append").save(self.tables["meta_patterns"])

            elif self.engine:
                import pandas as pd

                data = {
                    "table_name": [table_name],
                    "pattern_type": [pattern_type],
                    "configuration": [configuration],
                    "compliance_score": [compliance_score],
                }
                df = pd.DataFrame(data)

                self.engine.write(
                    df,
                    connection=self.connection,
                    format="delta",
                    path=self.tables["meta_patterns"],
                    mode="append",
                )

        try:
            self._retry_with_backoff(_do_log_pattern)
        except Exception as e:
            logger.warning(f"Failed to log pattern to system catalog: {e}")

    def register_asset(
        self,
        project_name: str,
        table_name: str,
        path: str,
        format: str,
        pattern_type: str,
        schema_hash: str = "",
    ) -> None:
        """
        Registers/Upserts a physical asset to meta_tables.
        """
        if not self.spark and not self.engine:
            return

        def _do_register():
            if self.spark:
                from pyspark.sql import functions as F

                # Prepare data
                rows = [
                    (
                        project_name,
                        table_name,
                        path,
                        format,
                        pattern_type,
                        schema_hash,
                    )
                ]
                schema = self._get_schema_meta_tables()
                input_schema = StructType(schema.fields[:-1])  # Exclude updated_at

                df = self.spark.createDataFrame(rows, input_schema)
                df = df.withColumn("updated_at", F.current_timestamp())

                # Merge Logic
                # We need a temp view
                view_name = f"_odibi_meta_tables_upsert_{abs(hash(table_name))}"
                df.createOrReplaceTempView(view_name)

                target_path = self.tables["meta_tables"]

                merge_sql = f"""
                    MERGE INTO delta.`{target_path}` AS target
                    USING {view_name} AS source
                    ON target.project = source.project
                       AND target.table_name = source.table_name
                    WHEN MATCHED THEN UPDATE SET
                        target.path = source.path,
                        target.format = source.format,
                        target.pattern_type = source.pattern_type,
                        target.schema_hash = source.schema_hash,
                        target.updated_at = source.updated_at
                    WHEN NOT MATCHED THEN INSERT *
                """
                self.spark.sql(merge_sql)
                self.spark.catalog.dropTempView(view_name)
            elif self.engine:
                from datetime import datetime, timezone

                import pandas as pd

                # Construct DataFrame
                data = {
                    "project": [project_name],
                    "table_name": [table_name],
                    "path": [path],
                    "format": [format],
                    "pattern_type": [pattern_type],
                    "schema_hash": [schema_hash],
                    "updated_at": [datetime.now(timezone.utc)],
                }
                df = pd.DataFrame(data)

                target_path = self.tables["meta_tables"]

                # Use Merge transformer if available, or manual engine merge?
                # Since we are inside catalog, using transformer might be circular.
                # Let's use engine.write with mode='upsert' if engine supports it?
                # PandasEngine.write(..., mode='upsert') delegates to _handle_generic_upsert
                # or _write_delta which calls dt.merge.

                self.engine.write(
                    df,
                    connection=self.connection,
                    format="delta",
                    path=target_path,
                    mode="upsert",
                    options={"keys": ["project", "table_name"]},
                )

        try:
            self._retry_with_backoff(_do_register)
        except Exception as e:
            logger.warning(f"Failed to register asset in system catalog: {e}")

    def resolve_table_path(self, table_name: str) -> Optional[str]:
        """
        Resolves logical table name (e.g. 'gold.orders') to physical path.
        """
        if self.spark:
            try:
                from pyspark.sql import functions as F

                df = self.spark.read.format("delta").load(self.tables["meta_tables"])
                # Filter
                row = df.filter(F.col("table_name") == table_name).select("path").first()

                return row.path if row else None
            except Exception:
                return None
        elif self.engine:
            df = self._read_local_table(self.tables["meta_tables"])
            if df.empty:
                return None

            # Pandas filtering
            if "table_name" not in df.columns:
                return None

            row = df[df["table_name"] == table_name]
            if not row.empty:
                return row.iloc[0]["path"]
            return None

        return None

    def get_pipeline_hash(self, pipeline_name: str) -> Optional[str]:
        """
        Retrieves the version hash of a pipeline from the catalog.
        """
        if self.spark:
            try:
                from pyspark.sql import functions as F

                df = self.spark.read.format("delta").load(self.tables["meta_pipelines"])
                row = (
                    df.filter(F.col("pipeline_name") == pipeline_name)
                    .select("version_hash")
                    .first()
                )
                return row.version_hash if row else None
            except Exception:
                return None
        elif self.engine:
            df = self._read_local_table(self.tables["meta_pipelines"])
            if df.empty:
                return None
            if "pipeline_name" not in df.columns or "version_hash" not in df.columns:
                return None

            # Ensure we get the latest one if duplicates exist (though upsert should prevent)
            # But reading parquet fallback might have duplicates.
            # Sorting by updated_at desc
            if "updated_at" in df.columns:
                df = df.sort_values("updated_at", ascending=False)

            row = df[df["pipeline_name"] == pipeline_name]
            if not row.empty:
                return row.iloc[0]["version_hash"]
            return None
        return None

    def get_average_volume(self, node_name: str, days: int = 7) -> Optional[float]:
        """
        Calculates average rows processed for a node over last N days.
        """
        if self.spark:
            try:
                from pyspark.sql import functions as F

                df = self.spark.read.format("delta").load(self.tables["meta_runs"])

                # Filter by node and success status
                stats = (
                    df.filter(
                        (F.col("node_name") == node_name)
                        & (F.col("status") == "SUCCESS")
                        & (F.col("timestamp") >= F.date_sub(F.current_date(), days))
                    )
                    .agg(F.avg("rows_processed"))
                    .first()
                )

                return stats[0] if stats else None
            except Exception:
                return None
        elif self.engine:
            df = self._read_local_table(self.tables["meta_runs"])
            if df.empty:
                return None

            # Need status, node_name, rows_processed, timestamp
            required = ["status", "node_name", "rows_processed", "timestamp"]
            if not all(col in df.columns for col in required):
                return None

            from datetime import datetime, timedelta, timezone

            import pandas as pd

            cutoff = datetime.now(timezone.utc) - timedelta(days=days)

            # Ensure timestamp is datetime
            if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
                try:
                    df["timestamp"] = pd.to_datetime(df["timestamp"])
                except Exception:
                    return None

            filtered = df[
                (df["node_name"] == node_name)
                & (df["status"] == "SUCCESS")
                & (df["timestamp"] >= cutoff)
            ]

            if filtered.empty:
                return None

            return float(filtered["rows_processed"].mean())

        return None

    def get_average_duration(self, node_name: str, days: int = 7) -> Optional[float]:
        """
        Calculates average duration (seconds) for a node over last N days.
        """
        if self.spark:
            try:
                from pyspark.sql import functions as F

                df = self.spark.read.format("delta").load(self.tables["meta_runs"])

                stats = (
                    df.filter(
                        (F.col("node_name") == node_name)
                        & (F.col("status") == "SUCCESS")
                        & (F.col("timestamp") >= F.date_sub(F.current_date(), days))
                    )
                    .agg(F.avg("duration_ms"))
                    .first()
                )

                return stats[0] / 1000.0 if stats and stats[0] is not None else None
            except Exception:
                return None
        elif self.engine:
            df = self._read_local_table(self.tables["meta_runs"])
            if df.empty:
                return None

            from datetime import datetime, timedelta, timezone

            import pandas as pd

            cutoff = datetime.now(timezone.utc) - timedelta(days=days)

            if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
                try:
                    df["timestamp"] = pd.to_datetime(df["timestamp"])
                except Exception:
                    return None

            filtered = df[
                (df["node_name"] == node_name)
                & (df["status"] == "SUCCESS")
                & (df["timestamp"] >= cutoff)
            ]

            if filtered.empty:
                return None

            avg_ms = float(filtered["duration_ms"].mean())
            return avg_ms / 1000.0

        return None

    def _read_table(self, path: str):
        """
        Read system table using Spark (for remote paths) or local methods.
        Returns pandas DataFrame. Empty DataFrame on failure.
        """
        import pandas as pd

        # Use Spark for remote paths (ADLS, S3, etc.) or when Spark is available
        if self.spark:
            try:
                spark_df = self.spark.read.format("delta").load(path)
                return spark_df.toPandas()
            except Exception as e:
                logger.debug(f"Could not read table via Spark at {path}: {e}")
                return pd.DataFrame()

        # Fallback to local reading for non-Spark environments
        return self._read_local_table(path)

    def _read_local_table(self, path: str):
        """
        Helper to read local system tables (Delta or Parquet).
        Returns empty DataFrame on failure.
        """
        import pandas as pd

        storage_opts = self._get_storage_options()

        try:
            # Try Delta first if library available
            try:
                from deltalake import DeltaTable

                dt = DeltaTable(path, storage_options=storage_opts if storage_opts else None)
                return dt.to_pandas()
            except ImportError:
                # Delta library not installed, proceed to parquet fallback
                pass
            except Exception:
                # Not a valid delta table? Fallback to parquet
                pass

            # Fallback: Read as Parquet (directory or file)
            return pd.read_parquet(path, storage_options=storage_opts if storage_opts else None)

        except Exception as e:
            # Only log debug to avoid noise if table just doesn't exist or is empty yet
            logger.debug(f"Could not read local table at {path}: {e}")
            return pd.DataFrame()

    def _hash_schema(self, schema: Dict[str, str]) -> str:
        """Generate MD5 hash of column definitions for change detection."""
        sorted_schema = json.dumps(schema, sort_keys=True)
        return hashlib.md5(sorted_schema.encode("utf-8")).hexdigest()

    def _get_latest_schema(self, table_path: str) -> Optional[Dict[str, Any]]:
        """Get the most recent schema record for a table."""
        if self.spark:
            try:
                from pyspark.sql import functions as F

                df = self.spark.read.format("delta").load(self.tables["meta_schemas"])
                row = (
                    df.filter(F.col("table_path") == table_path)
                    .orderBy(F.col("schema_version").desc())
                    .first()
                )
                if row:
                    return row.asDict()
                return None
            except Exception:
                return None
        elif self.engine:
            df = self._read_local_table(self.tables["meta_schemas"])
            if df.empty or "table_path" not in df.columns:
                return None

            filtered = df[df["table_path"] == table_path]
            if filtered.empty:
                return None

            if "schema_version" in filtered.columns:
                filtered = filtered.sort_values("schema_version", ascending=False)
            return filtered.iloc[0].to_dict()

        return None

    def track_schema(
        self,
        table_path: str,
        schema: Dict[str, str],
        pipeline: str,
        node: str,
        run_id: str,
    ) -> Dict[str, Any]:
        """
        Track schema version for a table.

        Args:
            table_path: Full path to the table (e.g., "silver/customers")
            schema: Dictionary of column names to types
            pipeline: Pipeline name
            node: Node name
            run_id: Execution run ID

        Returns:
            Dict with version info and detected changes:
            - changed: bool indicating if schema changed
            - version: current schema version number
            - previous_version: previous version (if exists)
            - columns_added: list of new columns
            - columns_removed: list of removed columns
            - columns_type_changed: list of columns with type changes
        """
        if not self.spark and not self.engine:
            return {"changed": False, "version": 0}

        try:
            schema_hash = self._hash_schema(schema)
            previous = self._get_latest_schema(table_path)

            if previous and previous.get("schema_hash") == schema_hash:
                return {"changed": False, "version": previous.get("schema_version", 1)}

            changes: Dict[str, Any] = {
                "columns_added": [],
                "columns_removed": [],
                "columns_type_changed": [],
            }

            if previous:
                prev_cols_str = previous.get("columns", "{}")
                prev_cols = json.loads(prev_cols_str) if isinstance(prev_cols_str, str) else {}

                changes["columns_added"] = list(set(schema.keys()) - set(prev_cols.keys()))
                changes["columns_removed"] = list(set(prev_cols.keys()) - set(schema.keys()))
                changes["columns_type_changed"] = [
                    col for col in schema if col in prev_cols and schema[col] != prev_cols[col]
                ]
                new_version = previous.get("schema_version", 0) + 1
            else:
                new_version = 1

            record = {
                "table_path": table_path,
                "schema_version": new_version,
                "schema_hash": schema_hash,
                "columns": json.dumps(schema),
                "captured_at": datetime.now(timezone.utc),
                "pipeline": pipeline,
                "node": node,
                "run_id": run_id,
                "columns_added": (
                    json.dumps(changes["columns_added"]) if changes["columns_added"] else None
                ),
                "columns_removed": (
                    json.dumps(changes["columns_removed"]) if changes["columns_removed"] else None
                ),
                "columns_type_changed": (
                    json.dumps(changes["columns_type_changed"])
                    if changes["columns_type_changed"]
                    else None
                ),
            }

            if self.spark:
                df = self.spark.createDataFrame([record], schema=self._get_schema_meta_schemas())
                df.write.format("delta").mode("append").save(self.tables["meta_schemas"])

            elif self.engine:
                import pandas as pd

                df = pd.DataFrame([record])
                self.engine.write(
                    df,
                    connection=self.connection,
                    format="delta",
                    path=self.tables["meta_schemas"],
                    mode="append",
                )

            result = {
                "changed": True,
                "version": new_version,
                "previous_version": previous.get("schema_version") if previous else None,
                **changes,
            }

            logger.info(
                f"Schema tracked for {table_path}: v{new_version} "
                f"(+{len(changes['columns_added'])}/-{len(changes['columns_removed'])}/"
                f"~{len(changes['columns_type_changed'])})"
            )

            return result

        except Exception as e:
            logger.warning(f"Failed to track schema for {table_path}: {e}")
            return {"changed": False, "version": 0, "error": str(e)}

    def get_schema_history(
        self,
        table_path: str,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get schema version history for a table.

        Args:
            table_path: Full path to the table (e.g., "silver/customers")
            limit: Maximum number of versions to return (default: 10)

        Returns:
            List of schema version records, most recent first
        """
        if not self.spark and not self.engine:
            return []

        try:
            if self.spark:
                from pyspark.sql import functions as F

                df = self.spark.read.format("delta").load(self.tables["meta_schemas"])
                rows = (
                    df.filter(F.col("table_path") == table_path)
                    .orderBy(F.col("schema_version").desc())
                    .limit(limit)
                    .collect()
                )
                return [row.asDict() for row in rows]

            elif self.engine:
                df = self._read_local_table(self.tables["meta_schemas"])
                if df.empty or "table_path" not in df.columns:
                    return []

                filtered = df[df["table_path"] == table_path]
                if filtered.empty:
                    return []

                if "schema_version" in filtered.columns:
                    filtered = filtered.sort_values("schema_version", ascending=False)

                return filtered.head(limit).to_dict("records")

        except Exception as e:
            logger.warning(f"Failed to get schema history for {table_path}: {e}")
            return []

        return []

    def record_lineage(
        self,
        source_table: str,
        target_table: str,
        target_pipeline: str,
        target_node: str,
        run_id: str,
        source_pipeline: Optional[str] = None,
        source_node: Optional[str] = None,
        relationship: str = "feeds",
    ) -> None:
        """
        Record a lineage relationship between tables.

        Args:
            source_table: Source table path
            target_table: Target table path
            target_pipeline: Pipeline name writing to target
            target_node: Node name writing to target
            run_id: Execution run ID
            source_pipeline: Source pipeline name (if known)
            source_node: Source node name (if known)
            relationship: Type of relationship ("feeds" or "derived_from")
        """
        if not self.spark and not self.engine:
            return

        def _do_record():
            record = {
                "source_table": source_table,
                "target_table": target_table,
                "source_pipeline": source_pipeline,
                "source_node": source_node,
                "target_pipeline": target_pipeline,
                "target_node": target_node,
                "relationship": relationship,
                "last_observed": datetime.now(timezone.utc),
                "run_id": run_id,
            }

            if self.spark:
                view_name = f"_odibi_lineage_upsert_{abs(hash(f'{source_table}_{target_table}'))}"
                df = self.spark.createDataFrame([record], schema=self._get_schema_meta_lineage())
                df.createOrReplaceTempView(view_name)

                target_path = self.tables["meta_lineage"]

                merge_sql = f"""
                    MERGE INTO delta.`{target_path}` AS target
                    USING {view_name} AS source
                    ON target.source_table = source.source_table
                       AND target.target_table = source.target_table
                    WHEN MATCHED THEN UPDATE SET
                        target.source_pipeline = source.source_pipeline,
                        target.source_node = source.source_node,
                        target.target_pipeline = source.target_pipeline,
                        target.target_node = source.target_node,
                        target.relationship = source.relationship,
                        target.last_observed = source.last_observed,
                        target.run_id = source.run_id
                    WHEN NOT MATCHED THEN INSERT *
                """
                self.spark.sql(merge_sql)
                self.spark.catalog.dropTempView(view_name)

            elif self.engine:
                import pandas as pd

                df = pd.DataFrame([record])
                self.engine.write(
                    df,
                    connection=self.connection,
                    format="delta",
                    path=self.tables["meta_lineage"],
                    mode="upsert",
                    options={"keys": ["source_table", "target_table"]},
                )

            logger.debug(f"Recorded lineage: {source_table} -> {target_table}")

        try:
            self._retry_with_backoff(_do_record)
        except Exception as e:
            logger.warning(f"Failed to record lineage: {e}")

    def record_lineage_batch(
        self,
        records: List[Dict[str, Any]],
    ) -> None:
        """
        Batch records multiple lineage relationships to meta_lineage in a single MERGE.

        This is much more efficient than calling record_lineage() for each relationship
        individually, especially when running parallel pipelines with many nodes.

        Args:
            records: List of dicts with keys: source_table, target_table, target_pipeline,
                     target_node, run_id, source_pipeline (optional), source_node (optional),
                     relationship (optional, defaults to "feeds")
        """
        if not self.spark and not self.engine:
            return

        if not records:
            return

        def _do_batch_record():
            timestamp = datetime.now(timezone.utc)

            if self.spark:
                rows = [
                    (
                        r["source_table"],
                        r["target_table"],
                        r.get("source_pipeline"),
                        r.get("source_node"),
                        r["target_pipeline"],
                        r["target_node"],
                        r.get("relationship", "feeds"),
                        timestamp,
                        r["run_id"],
                    )
                    for r in records
                ]
                schema = self._get_schema_meta_lineage()
                df = self.spark.createDataFrame(rows, schema)

                view_name = "_odibi_meta_lineage_batch_upsert"
                df.createOrReplaceTempView(view_name)

                target_path = self.tables["meta_lineage"]

                merge_sql = f"""
                    MERGE INTO delta.`{target_path}` AS target
                    USING {view_name} AS source
                    ON target.source_table = source.source_table
                       AND target.target_table = source.target_table
                    WHEN MATCHED THEN UPDATE SET
                        target.source_pipeline = source.source_pipeline,
                        target.source_node = source.source_node,
                        target.target_pipeline = source.target_pipeline,
                        target.target_node = source.target_node,
                        target.relationship = source.relationship,
                        target.last_observed = source.last_observed,
                        target.run_id = source.run_id
                    WHEN NOT MATCHED THEN INSERT *
                """
                self.spark.sql(merge_sql)
                self.spark.catalog.dropTempView(view_name)

            elif self.engine:
                import pandas as pd

                data = {
                    "source_table": [r["source_table"] for r in records],
                    "target_table": [r["target_table"] for r in records],
                    "source_pipeline": [r.get("source_pipeline") for r in records],
                    "source_node": [r.get("source_node") for r in records],
                    "target_pipeline": [r["target_pipeline"] for r in records],
                    "target_node": [r["target_node"] for r in records],
                    "relationship": [r.get("relationship", "feeds") for r in records],
                    "last_observed": [timestamp] * len(records),
                    "run_id": [r["run_id"] for r in records],
                }
                df = pd.DataFrame(data)

                self.engine.write(
                    df,
                    connection=self.connection,
                    format="delta",
                    path=self.tables["meta_lineage"],
                    mode="upsert",
                    options={"keys": ["source_table", "target_table"]},
                )

            logger.debug(f"Batch recorded {len(records)} lineage relationship(s)")

        try:
            self._retry_with_backoff(_do_batch_record)
        except Exception as e:
            logger.warning(f"Failed to batch record lineage: {e}")

    def register_assets_batch(
        self,
        records: List[Dict[str, Any]],
    ) -> None:
        """
        Batch registers/upserts multiple physical assets to meta_tables in a single MERGE.

        This is much more efficient than calling register_asset() for each asset
        individually, especially when running parallel pipelines with many nodes.

        Args:
            records: List of dicts with keys: project, table_name, path, format,
                     pattern_type, schema_hash (optional, defaults to "")
        """
        if not self.spark and not self.engine:
            return

        if not records:
            return

        def _do_batch_register():
            timestamp = datetime.now(timezone.utc)

            if self.spark:
                from pyspark.sql import functions as F

                schema = self._get_schema_meta_tables()
                input_schema = StructType(schema.fields[:-1])  # Exclude updated_at

                rows = [
                    (
                        r.get("project") or r.get("project_name"),
                        r["table_name"],
                        r["path"],
                        r["format"],
                        r["pattern_type"],
                        r.get("schema_hash", ""),
                    )
                    for r in records
                ]
                df = self.spark.createDataFrame(rows, input_schema)
                df = df.withColumn("updated_at", F.current_timestamp())

                view_name = "_odibi_meta_tables_batch_upsert"
                df.createOrReplaceTempView(view_name)

                target_path = self.tables["meta_tables"]

                merge_sql = f"""
                    MERGE INTO delta.`{target_path}` AS target
                    USING {view_name} AS source
                    ON target.project = source.project
                       AND target.table_name = source.table_name
                    WHEN MATCHED THEN UPDATE SET
                        target.path = source.path,
                        target.format = source.format,
                        target.pattern_type = source.pattern_type,
                        target.schema_hash = source.schema_hash,
                        target.updated_at = source.updated_at
                    WHEN NOT MATCHED THEN INSERT *
                """
                self.spark.sql(merge_sql)
                self.spark.catalog.dropTempView(view_name)

            elif self.engine:
                import pandas as pd

                data = {
                    "project": [r.get("project") or r.get("project_name") for r in records],
                    "table_name": [r["table_name"] for r in records],
                    "path": [r["path"] for r in records],
                    "format": [r["format"] for r in records],
                    "pattern_type": [r["pattern_type"] for r in records],
                    "schema_hash": [r.get("schema_hash", "") for r in records],
                    "updated_at": [timestamp] * len(records),
                }
                df = pd.DataFrame(data)

                self.engine.write(
                    df,
                    connection=self.connection,
                    format="delta",
                    path=self.tables["meta_tables"],
                    mode="upsert",
                    options={"keys": ["project", "table_name"]},
                )

            logger.debug(f"Batch registered {len(records)} asset(s)")

        try:
            self._retry_with_backoff(_do_batch_register)
        except Exception as e:
            logger.warning(f"Failed to batch register assets: {e}")

    def get_upstream(
        self,
        table_path: str,
        depth: int = 3,
    ) -> List[Dict[str, Any]]:
        """
        Get all upstream sources for a table.

        Args:
            table_path: Table to trace upstream from
            depth: Maximum depth to traverse

        Returns:
            List of upstream lineage records with depth information
        """
        if not self.spark and not self.engine:
            return []

        upstream = []
        visited = set()
        queue = [(table_path, 0)]

        try:
            while queue:
                current, level = queue.pop(0)
                if current in visited or level > depth:
                    continue
                visited.add(current)

                if self.spark:
                    from pyspark.sql import functions as F

                    df = self.spark.read.format("delta").load(self.tables["meta_lineage"])
                    sources = df.filter(F.col("target_table") == current).collect()
                    for row in sources:
                        record = row.asDict()
                        record["depth"] = level
                        upstream.append(record)
                        queue.append((record["source_table"], level + 1))

                elif self.engine:
                    df = self._read_local_table(self.tables["meta_lineage"])
                    if df.empty or "target_table" not in df.columns:
                        break

                    sources = df[df["target_table"] == current]
                    for _, row in sources.iterrows():
                        record = row.to_dict()
                        record["depth"] = level
                        upstream.append(record)
                        queue.append((record["source_table"], level + 1))

        except Exception as e:
            logger.warning(f"Failed to get upstream lineage for {table_path}: {e}")

        return upstream

    def get_downstream(
        self,
        table_path: str,
        depth: int = 3,
    ) -> List[Dict[str, Any]]:
        """
        Get all downstream consumers of a table.

        Args:
            table_path: Table to trace downstream from
            depth: Maximum depth to traverse

        Returns:
            List of downstream lineage records with depth information
        """
        if not self.spark and not self.engine:
            return []

        downstream = []
        visited = set()
        queue = [(table_path, 0)]

        try:
            while queue:
                current, level = queue.pop(0)
                if current in visited or level > depth:
                    continue
                visited.add(current)

                if self.spark:
                    from pyspark.sql import functions as F

                    df = self.spark.read.format("delta").load(self.tables["meta_lineage"])
                    targets = df.filter(F.col("source_table") == current).collect()
                    for row in targets:
                        record = row.asDict()
                        record["depth"] = level
                        downstream.append(record)
                        queue.append((record["target_table"], level + 1))

                elif self.engine:
                    df = self._read_local_table(self.tables["meta_lineage"])
                    if df.empty or "source_table" not in df.columns:
                        break

                    targets = df[df["source_table"] == current]
                    for _, row in targets.iterrows():
                        record = row.to_dict()
                        record["depth"] = level
                        downstream.append(record)
                        queue.append((record["target_table"], level + 1))

        except Exception as e:
            logger.warning(f"Failed to get downstream lineage for {table_path}: {e}")

        return downstream

    def optimize(self) -> None:
        """
        Runs VACUUM and OPTIMIZE (Z-Order) on meta_runs.
        Spark-only feature.
        """
        if not self.spark:
            return

        try:
            logger.info("Starting Catalog Optimization...")

            # 1. meta_runs
            # VACUUM: Remove files older than 7 days (Spark requires check disable or careful setting)
            # Note: default retention check might block < 168 hours.
            # We'll use RETAIN 168 HOURS (7 days) to be safe.
            self.spark.sql(f"VACUUM delta.`{self.tables['meta_runs']}` RETAIN 168 HOURS")

            # OPTIMIZE: Z-ORDER BY timestamp (for range queries)
            # We also have 'pipeline_name' and 'date' as partitions.
            # Z-Ordering by timestamp helps within the partitions.
            self.spark.sql(f"OPTIMIZE delta.`{self.tables['meta_runs']}` ZORDER BY (timestamp)")

            logger.info("Catalog Optimization completed successfully.")

        except Exception as e:
            logger.warning(f"Catalog Optimization failed: {e}")

    # -------------------------------------------------------------------------
    # Phase 3.6: Metrics Logging
    # -------------------------------------------------------------------------

    def log_metrics(
        self,
        metric_name: str,
        definition_sql: str,
        dimensions: List[str],
        source_table: str,
    ) -> None:
        """Log a business metric definition to meta_metrics.

        Args:
            metric_name: Name of the metric
            definition_sql: SQL definition of the metric
            dimensions: List of dimension columns
            source_table: Source table for the metric
        """
        if not self.spark and not self.engine:
            return

        def _do_log_metrics():
            import json

            if self.spark:
                dimensions_json = json.dumps(dimensions)
                rows = [(metric_name, definition_sql, dimensions_json, source_table)]
                schema = self._get_schema_meta_metrics()

                df = self.spark.createDataFrame(rows, schema)
                df.write.format("delta").mode("append").save(self.tables["meta_metrics"])

            elif self.engine:
                import pandas as pd

                data = {
                    "metric_name": [metric_name],
                    "definition_sql": [definition_sql],
                    "dimensions": [json.dumps(dimensions)],
                    "source_table": [source_table],
                }
                df = pd.DataFrame(data)

                self.engine.write(
                    df,
                    connection=self.connection,
                    format="delta",
                    path=self.tables["meta_metrics"],
                    mode="append",
                )

            logger.debug(f"Logged metric: {metric_name}")

        try:
            self._retry_with_backoff(_do_log_metrics)
        except Exception as e:
            logger.warning(f"Failed to log metric to system catalog: {e}")

    # -------------------------------------------------------------------------
    # Phase 4: Cleanup/Removal Methods
    # -------------------------------------------------------------------------

    def remove_pipeline(self, pipeline_name: str) -> int:
        """Remove pipeline and cascade to nodes, state entries.

        Args:
            pipeline_name: Name of the pipeline to remove

        Returns:
            Count of deleted entries
        """
        if not self.spark and not self.engine:
            return 0

        deleted_count = 0

        try:
            if self.spark:
                from pyspark.sql import functions as F

                # Delete from meta_pipelines
                df = self.spark.read.format("delta").load(self.tables["meta_pipelines"])
                df.cache()
                initial_count = df.count()
                df_filtered = df.filter(F.col("pipeline_name") != pipeline_name)
                df_filtered.write.format("delta").mode("overwrite").save(
                    self.tables["meta_pipelines"]
                )
                deleted_count += initial_count - df_filtered.count()
                df.unpersist()

                # Delete associated nodes from meta_nodes
                df_nodes = self.spark.read.format("delta").load(self.tables["meta_nodes"])
                df_nodes.cache()
                nodes_initial = df_nodes.count()
                df_nodes_filtered = df_nodes.filter(F.col("pipeline_name") != pipeline_name)
                df_nodes_filtered.write.format("delta").mode("overwrite").save(
                    self.tables["meta_nodes"]
                )
                deleted_count += nodes_initial - df_nodes_filtered.count()
                df_nodes.unpersist()

            elif self.engine:
                # Delete from meta_pipelines
                df = self._read_local_table(self.tables["meta_pipelines"])
                if not df.empty and "pipeline_name" in df.columns:
                    initial_count = len(df)
                    df = df[df["pipeline_name"] != pipeline_name]
                    self.engine.write(
                        df,
                        connection=self.connection,
                        format="delta",
                        path=self.tables["meta_pipelines"],
                        mode="overwrite",
                    )
                    deleted_count += initial_count - len(df)

                # Delete associated nodes from meta_nodes
                df_nodes = self._read_local_table(self.tables["meta_nodes"])
                if not df_nodes.empty and "pipeline_name" in df_nodes.columns:
                    nodes_initial = len(df_nodes)
                    df_nodes = df_nodes[df_nodes["pipeline_name"] != pipeline_name]
                    self.engine.write(
                        df_nodes,
                        connection=self.connection,
                        format="delta",
                        path=self.tables["meta_nodes"],
                        mode="overwrite",
                    )
                    deleted_count += nodes_initial - len(df_nodes)

            self.invalidate_cache()
            logger.info(f"Removed pipeline '{pipeline_name}': {deleted_count} entries deleted")

        except Exception as e:
            logger.warning(f"Failed to remove pipeline: {e}")

        return deleted_count

    def remove_node(self, pipeline_name: str, node_name: str) -> int:
        """Remove node and associated state entries.

        Args:
            pipeline_name: Pipeline name
            node_name: Node name to remove

        Returns:
            Count of deleted entries
        """
        if not self.spark and not self.engine:
            return 0

        deleted_count = 0

        try:
            if self.spark:
                from pyspark.sql import functions as F

                # Delete from meta_nodes
                df = self.spark.read.format("delta").load(self.tables["meta_nodes"])
                df.cache()
                initial_count = df.count()
                df_filtered = df.filter(
                    ~((F.col("pipeline_name") == pipeline_name) & (F.col("node_name") == node_name))
                )
                df_filtered.write.format("delta").mode("overwrite").save(self.tables["meta_nodes"])
                deleted_count = initial_count - df_filtered.count()
                df.unpersist()

            elif self.engine:
                df = self._read_local_table(self.tables["meta_nodes"])
                if not df.empty and "pipeline_name" in df.columns and "node_name" in df.columns:
                    initial_count = len(df)
                    df = df[
                        ~((df["pipeline_name"] == pipeline_name) & (df["node_name"] == node_name))
                    ]
                    self.engine.write(
                        df,
                        connection=self.connection,
                        format="delta",
                        path=self.tables["meta_nodes"],
                        mode="overwrite",
                    )
                    deleted_count = initial_count - len(df)

            self._nodes_cache = None
            logger.info(
                f"Removed node '{node_name}' from pipeline '{pipeline_name}': "
                f"{deleted_count} entries deleted"
            )

        except Exception as e:
            logger.warning(f"Failed to remove node: {e}")

        return deleted_count

    def cleanup_orphans(self, current_config: Any) -> Dict[str, int]:
        """Compare catalog against current config, remove stale entries.

        Args:
            current_config: ProjectConfig with current pipeline definitions

        Returns:
            Dict of {table: deleted_count}
        """
        if not self.spark and not self.engine:
            return {}

        results = {"meta_pipelines": 0, "meta_nodes": 0}

        try:
            # Get current pipeline and node names from config
            current_pipelines = set()
            current_nodes = {}  # {pipeline_name: set(node_names)}

            for pipeline in current_config.pipelines:
                current_pipelines.add(pipeline.pipeline)
                current_nodes[pipeline.pipeline] = {node.name for node in pipeline.nodes}

            if self.spark:
                from pyspark.sql import functions as F

                # Cleanup orphan pipelines
                df_pipelines = self.spark.read.format("delta").load(self.tables["meta_pipelines"])
                df_pipelines.cache()
                initial_pipelines = df_pipelines.count()
                df_pipelines_filtered = df_pipelines.filter(
                    F.col("pipeline_name").isin(list(current_pipelines))
                )
                df_pipelines_filtered.write.format("delta").mode("overwrite").save(
                    self.tables["meta_pipelines"]
                )
                results["meta_pipelines"] = initial_pipelines - df_pipelines_filtered.count()
                df_pipelines.unpersist()

                # Cleanup orphan nodes
                df_nodes = self.spark.read.format("delta").load(self.tables["meta_nodes"])
                df_nodes.cache()
                initial_nodes = df_nodes.count()

                # Filter: keep only nodes that belong to current pipelines and exist in config
                valid_nodes = []
                for p_name, nodes in current_nodes.items():
                    for n_name in nodes:
                        valid_nodes.append((p_name, n_name))

                if valid_nodes:
                    valid_df = self.spark.createDataFrame(
                        valid_nodes, ["pipeline_name", "node_name"]
                    )
                    df_nodes_filtered = df_nodes.join(
                        valid_df, ["pipeline_name", "node_name"], "inner"
                    )
                else:
                    df_nodes_filtered = df_nodes.limit(0)

                df_nodes_filtered.write.format("delta").mode("overwrite").save(
                    self.tables["meta_nodes"]
                )
                results["meta_nodes"] = initial_nodes - df_nodes_filtered.count()
                df_nodes.unpersist()

            elif self.engine:
                # Cleanup orphan pipelines
                df_pipelines = self._read_local_table(self.tables["meta_pipelines"])
                if not df_pipelines.empty and "pipeline_name" in df_pipelines.columns:
                    initial_pipelines = len(df_pipelines)
                    df_pipelines = df_pipelines[
                        df_pipelines["pipeline_name"].isin(current_pipelines)
                    ]
                    self.engine.write(
                        df_pipelines,
                        connection=self.connection,
                        format="delta",
                        path=self.tables["meta_pipelines"],
                        mode="overwrite",
                    )
                    results["meta_pipelines"] = initial_pipelines - len(df_pipelines)

                # Cleanup orphan nodes
                df_nodes = self._read_local_table(self.tables["meta_nodes"])
                if not df_nodes.empty and "pipeline_name" in df_nodes.columns:
                    initial_nodes = len(df_nodes)

                    valid_node_tuples = set()
                    for p_name, nodes in current_nodes.items():
                        for n_name in nodes:
                            valid_node_tuples.add((p_name, n_name))

                    df_nodes["_valid"] = df_nodes.apply(
                        lambda row: (row["pipeline_name"], row["node_name"]) in valid_node_tuples,
                        axis=1,
                    )
                    df_nodes = df_nodes[df_nodes["_valid"]].drop(columns=["_valid"])

                    self.engine.write(
                        df_nodes,
                        connection=self.connection,
                        format="delta",
                        path=self.tables["meta_nodes"],
                        mode="overwrite",
                    )
                    results["meta_nodes"] = initial_nodes - len(df_nodes)

            self.invalidate_cache()
            logger.info(
                f"Cleanup orphans completed: {results['meta_pipelines']} pipelines, "
                f"{results['meta_nodes']} nodes removed"
            )

        except Exception as e:
            logger.warning(f"Failed to cleanup orphans: {e}")

        return results

    def clear_state_key(self, key: str) -> bool:
        """Remove a single state entry by key.

        Args:
            key: State key to remove

        Returns:
            True if deleted, False otherwise
        """
        if not self.spark and not self.engine:
            return False

        try:
            if self.spark:
                from pyspark.sql import functions as F

                df = self.spark.read.format("delta").load(self.tables["meta_state"])
                initial_count = df.count()
                df = df.filter(F.col("key") != key)
                df.write.format("delta").mode("overwrite").save(self.tables["meta_state"])
                return df.count() < initial_count

            elif self.engine:
                df = self._read_local_table(self.tables["meta_state"])
                if df.empty or "key" not in df.columns:
                    return False

                initial_count = len(df)
                df = df[df["key"] != key]

                if len(df) < initial_count:
                    self.engine.write(
                        df,
                        connection=self.connection,
                        format="delta",
                        path=self.tables["meta_state"],
                        mode="overwrite",
                    )
                    return True

                return False

        except Exception as e:
            logger.warning(f"Failed to clear state key '{key}': {e}")
            return False

    def clear_state_pattern(self, key_pattern: str) -> int:
        """Remove state entries matching pattern (supports wildcards).

        Args:
            key_pattern: Pattern with optional * wildcards

        Returns:
            Count of deleted entries
        """
        if not self.spark and not self.engine:
            return 0

        try:
            if self.spark:
                from pyspark.sql import functions as F

                df = self.spark.read.format("delta").load(self.tables["meta_state"])
                initial_count = df.count()

                # Convert wildcard pattern to SQL LIKE pattern
                like_pattern = key_pattern.replace("*", "%")
                df = df.filter(~F.col("key").like(like_pattern))
                df.write.format("delta").mode("overwrite").save(self.tables["meta_state"])

                return initial_count - df.count()

            elif self.engine:
                import re

                df = self._read_local_table(self.tables["meta_state"])
                if df.empty or "key" not in df.columns:
                    return 0

                initial_count = len(df)

                # Convert wildcard pattern to regex
                regex_pattern = "^" + key_pattern.replace("*", ".*") + "$"
                pattern = re.compile(regex_pattern)
                df = df[~df["key"].apply(lambda x: bool(pattern.match(str(x))))]

                if len(df) < initial_count:
                    self.engine.write(
                        df,
                        connection=self.connection,
                        format="delta",
                        path=self.tables["meta_state"],
                        mode="overwrite",
                    )

                return initial_count - len(df)

        except Exception as e:
            logger.warning(f"Failed to clear state pattern '{key_pattern}': {e}")
            return 0
