import logging
from typing import Any, Optional

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
    ):
        """
        Initialize the Catalog Manager.

        Args:
            spark: Active SparkSession (optional if engine is provided)
            config: SystemConfig object
            base_path: Absolute path to the system catalog directory (resolved from connection).
                       Example: "abfss://container@account.dfs.core.windows.net/_odibi_system"
            engine: Execution engine (optional, for Pandas mode)
        """
        self.spark = spark
        self.config = config
        self.base_path = base_path.rstrip("/")
        self.engine = engine

        # Table Paths
        self.tables = {
            "meta_tables": f"{self.base_path}/meta_tables",
            "meta_runs": f"{self.base_path}/meta_runs",
            "meta_patterns": f"{self.base_path}/meta_patterns",
            "meta_metrics": f"{self.base_path}/meta_metrics",
            "meta_state": f"{self.base_path}/meta_state",
            "meta_pipelines": f"{self.base_path}/meta_pipelines",
            "meta_nodes": f"{self.base_path}/meta_nodes",
        }

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
        self._ensure_table(
            "meta_state", self._get_schema_meta_state(), partition_cols=["pipeline_name"]
        )
        self._ensure_table("meta_pipelines", self._get_schema_meta_pipelines())
        self._ensure_table("meta_nodes", self._get_schema_meta_nodes())

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

                    write_deltalake(path, table, mode="overwrite", partition_by=partition_cols)
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

                    _ = DeltaTable(path)
                    # Basic schema evolution: overwrite schema if we are appending?
                    # For now, let's just log. True evolution is complex.
                    # A simple fix for "fields mismatch" is to allow schema merge.
                    pass
                except ImportError:
                    pass
            logger.debug(f"System table exists: {name}")

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

            # Check if directory exists and has content
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
                StructField("project_name", StringType(), True),
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
                StructField("pipeline_name", StringType(), True),
                StructField("node_name", StringType(), True),
                StructField("status", StringType(), True),
                StructField("rows_processed", LongType(), True),
                StructField("duration_ms", LongType(), True),
                StructField("metrics_json", StringType(), True),
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
        """
        return StructType(
            [
                StructField("metric_name", StringType(), True),
                StructField("definition_sql", StringType(), True),
                StructField("dimensions", ArrayType(StringType()), True),
                StructField("source_table", StringType(), True),
            ]
        )

    def _get_schema_meta_state(self) -> StructType:
        """
        meta_state (Checkpoints): Tracks incremental progress.
        """
        return StructType(
            [
                StructField("pipeline_name", StringType(), True),
                StructField("node_name", StringType(), True),
                StructField("hwm_value", StringType(), True),
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

    def register_pipeline(
        self,
        pipeline_config: Any,
        project_config: Optional[Any] = None,
    ) -> None:
        """
        Registers/Upserts a pipeline definition to meta_pipelines.
        """
        if not self.spark and not self.engine:
            return

        try:
            import hashlib
            import json
            from datetime import datetime, timezone

            # 1. Calculate Pipeline Hash (Configuration State)
            # We hash the entire pipeline config (including nodes) to track version changes
            if hasattr(pipeline_config, "model_dump"):
                dump = pipeline_config.model_dump(mode="json")
            else:
                dump = pipeline_config.dict()

            dump_str = json.dumps(dump, sort_keys=True)
            version_hash = hashlib.md5(dump_str.encode("utf-8")).hexdigest()

            # 2. Prepare Fields
            pipeline_name = pipeline_config.pipeline
            description = pipeline_config.description or ""
            layer = pipeline_config.layer or ""

            # Schedule is not yet in PipelineConfig, try to find it in vars or metadata if available
            # For now, we leave it empty or future-proof it
            schedule = ""

            # Aggregate tags from nodes for high-level view
            all_tags = set()
            for node in pipeline_config.nodes:
                if node.tags:
                    all_tags.update(node.tags)
            tags_json = json.dumps(list(all_tags))

            # 3. Upsert
            if self.spark:
                from pyspark.sql import functions as F

                rows = [
                    (
                        pipeline_name,
                        version_hash,
                        description,
                        layer,
                        schedule,
                        tags_json,
                    )
                ]
                schema = self._get_schema_meta_pipelines()
                input_schema = StructType(schema.fields[:-1])  # Exclude updated_at

                df = self.spark.createDataFrame(rows, input_schema)
                df = df.withColumn("updated_at", F.current_timestamp())

                # Merge Logic
                view_name = f"_odibi_meta_pipelines_upsert_{abs(hash(pipeline_name))}"
                df.createOrReplaceTempView(view_name)

                target_path = self.tables["meta_pipelines"]

                # Only update if hash changed (Optimization) or force update timestamp?
                # We usually want to know when it was last deployed.

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
                    "pipeline_name": [pipeline_name],
                    "version_hash": [version_hash],
                    "description": [description],
                    "layer": [layer],
                    "schedule": [schedule],
                    "tags_json": [tags_json],
                    "updated_at": [datetime.now(timezone.utc)],
                }
                df = pd.DataFrame(data)

                self.engine.write(
                    df,
                    connection=None,
                    format="delta",
                    path=self.tables["meta_pipelines"],
                    mode="upsert",
                    options={"keys": ["pipeline_name"]},
                )

        except Exception as e:
            logger.warning(f"Failed to register pipeline '{pipeline_config.pipeline}': {e}")

    def register_node(
        self,
        pipeline_name: str,
        node_config: Any,
    ) -> None:
        """
        Registers/Upserts a node definition to meta_nodes.
        """
        if not self.spark and not self.engine:
            return

        try:
            import hashlib
            import json
            from datetime import datetime, timezone

            # 1. Calculate Node Hash
            if hasattr(node_config, "model_dump"):
                dump = node_config.model_dump(
                    mode="json", exclude={"description", "tags", "log_level"}
                )
            else:
                dump = node_config.dict(exclude={"description", "tags", "log_level"})

            dump_str = json.dumps(dump, sort_keys=True)
            version_hash = hashlib.md5(dump_str.encode("utf-8")).hexdigest()

            # 2. Determine Type
            node_type = "transform"
            if node_config.read:
                node_type = "read"
            if node_config.write:
                node_type = "write"
                # If it has both, it's usually a loader/ETL node, effectively "write" is the dominant effect

            # 3. Serialize Config
            # We store the full config for runtime retrieval
            config_json = json.dumps(dump)

            # 4. Upsert
            if self.spark:
                from pyspark.sql import functions as F

                rows = [
                    (
                        pipeline_name,
                        node_config.name,
                        version_hash,
                        node_type,
                        config_json,
                    )
                ]
                schema = self._get_schema_meta_nodes()
                input_schema = StructType(schema.fields[:-1])

                df = self.spark.createDataFrame(rows, input_schema)
                df = df.withColumn("updated_at", F.current_timestamp())

                view_name = f"_odibi_meta_nodes_upsert_{abs(hash(node_config.name))}"
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
                    "pipeline_name": [pipeline_name],
                    "node_name": [node_config.name],
                    "version_hash": [version_hash],
                    "type": [node_type],
                    "config_json": [config_json],
                    "updated_at": [datetime.now(timezone.utc)],
                }
                df = pd.DataFrame(data)

                self.engine.write(
                    df,
                    connection=None,
                    format="delta",
                    path=self.tables["meta_nodes"],
                    mode="upsert",
                    options={"keys": ["pipeline_name", "node_name"]},
                )

        except Exception as e:
            logger.warning(f"Failed to register node '{node_config.name}': {e}")

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
        """
        if not self.spark and not self.engine:
            return

        try:
            if self.spark:
                from pyspark.sql import functions as F

                rows = [
                    (
                        run_id,
                        pipeline_name,
                        node_name,
                        status,
                        rows_processed,
                        duration_ms,
                        metrics_json,
                    )
                ]
                schema = self._get_schema_meta_runs()
                # Schema has timestamp and date at the end, which we'll add via withColumn
                # So we use a subset schema for creation
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
                    "pipeline_name": [pipeline_name],
                    "node_name": [node_name],
                    "status": [status],
                    "rows_processed": [rows_processed],
                    "duration_ms": [duration_ms],
                    "metrics_json": [metrics_json],
                    "timestamp": [timestamp],
                    "date": [timestamp.date()],
                }
                df = pd.DataFrame(data)

                # Use engine to write (handles Delta if available)
                # Note: System tables are usually Delta.
                # We assume 'meta_runs' path is a Delta table path
                self.engine.write(
                    df,
                    connection=None,  # direct path
                    format="delta",
                    path=self.tables["meta_runs"],
                    mode="append",
                    options={"schema_mode": "merge"},  # Allow schema evolution
                )

        except Exception as e:
            logger.warning(f"Failed to log run to system catalog: {e}")

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

        try:
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
                    connection=None,
                    format="delta",
                    path=self.tables["meta_patterns"],
                    mode="append",
                )

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

        try:
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
                    ON target.project_name = source.project_name
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
                    "project_name": [project_name],
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
                    connection=None,
                    format="delta",
                    path=target_path,
                    mode="upsert",
                    options={"keys": ["project_name", "table_name"]},
                )

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

    def _read_local_table(self, path: str):
        """
        Helper to read local system tables (Delta or Parquet).
        Returns empty DataFrame on failure.
        """
        import pandas as pd

        # Suppress verbose internal logs if necessary

        try:
            # Try Delta first if library available
            try:
                from deltalake import DeltaTable

                return DeltaTable(path).to_pandas()
            except ImportError:
                # Delta library not installed, proceed to parquet fallback
                pass
            except Exception:
                # Not a valid delta table? Fallback to parquet
                pass

            # Fallback: Read as Parquet (directory or file)
            return pd.read_parquet(path)

        except Exception as e:
            # Only log debug to avoid noise if table just doesn't exist or is empty yet
            logger.debug(f"Could not read local table at {path}: {e}")
            return pd.DataFrame()

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
