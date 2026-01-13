"""Spark execution engine (Phase 2B: Delta Lake support).

Status: Phase 2B implemented - Delta Lake read/write, VACUUM, history, restore
"""

import re
import time
from typing import Any, Dict, List, Optional, Tuple

from odibi.enums import EngineType
from odibi.exceptions import TransformError
from odibi.utils.logging_context import get_logging_context

from .base import Engine


def _extract_spark_error_message(error: Exception) -> str:
    """Extract a clean, user-friendly error message from Spark/Py4J exceptions.

    Removes Java stack traces and Py4J noise, keeping only the useful error info.

    Args:
        error: The exception to clean

    Returns:
        Clean error message without Java stack traces
    """
    error_str = str(error)

    # For AnalysisException, extract the error class and message up to SQLSTATE or line info
    # Format: [ERROR_CLASS] message. Did you mean...? SQLSTATE: xxx; line X pos Y;\n'Plan...
    match = re.match(
        r"(\[[\w._]+\])\s*(.+?)(?:\s*SQLSTATE|\s*;\s*line|\n'|\n\tat|$)",
        error_str,
        re.DOTALL,
    )
    if match:
        error_class = match.group(1)
        message = match.group(2).strip().rstrip(".")
        return f"{error_class} {message}"

    # For other Spark errors, try to extract the first meaningful line
    lines = error_str.split("\n")
    for line in lines:
        line = line.strip()
        # Skip Java stack trace lines
        if re.match(r"at (org\.|java\.|scala\.|py4j\.)", line):
            continue
        # Skip empty or noise lines
        if not line or line.startswith("Py4JJavaError") or line == ":":
            continue
        # Return first meaningful line
        if len(line) > 10:
            # Truncate very long messages
            if len(line) > 200:
                return line[:200] + "..."
            return line

    # Fallback: return first 200 chars
    return error_str[:200] + "..." if len(error_str) > 200 else error_str


class SparkEngine(Engine):
    """Spark execution engine with PySpark backend.

    Phase 2A: Basic read/write + ADLS multi-account support
    Phase 2B: Delta Lake support
    """

    name = "spark"
    engine_type = EngineType.SPARK

    def __init__(
        self,
        connections: Optional[Dict[str, Any]] = None,
        spark_session: Any = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        """Initialize Spark engine with import guard.

        Args:
            connections: Dictionary of connection objects (for multi-account config)
            spark_session: Existing SparkSession (optional, creates new if None)
            config: Engine configuration (optional)

        Raises:
            ImportError: If pyspark not installed
        """
        ctx = get_logging_context().with_context(engine="spark")
        ctx.debug("Initializing SparkEngine", connections_count=len(connections or {}))

        try:
            from pyspark.sql import SparkSession
        except ImportError as e:
            ctx.error(
                "PySpark not installed",
                error_type="ImportError",
                suggestion="pip install odibi[spark]",
            )
            raise ImportError(
                "Spark support requires 'pip install odibi[spark]'. "
                "See docs/setup_databricks.md for setup instructions."
            ) from e

        start_time = time.time()

        # Configure Delta Lake support
        try:
            from delta import configure_spark_with_delta_pip

            builder = SparkSession.builder.appName("odibi").config(
                "spark.sql.sources.partitionOverwriteMode", "dynamic"
            )

            # Performance Optimizations
            builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")
            builder = builder.config("spark.sql.adaptive.enabled", "true")

            # Reduce Verbosity
            builder = builder.config(
                "spark.driver.extraJavaOptions", "-Dlog4j.rootCategory=ERROR, console"
            )
            builder = builder.config(
                "spark.executor.extraJavaOptions", "-Dlog4j.rootCategory=ERROR, console"
            )

            self.spark = spark_session or configure_spark_with_delta_pip(builder).getOrCreate()
            self.spark.sparkContext.setLogLevel("ERROR")

            ctx.debug("Delta Lake support enabled")

        except ImportError:
            ctx.debug("Delta Lake not available, using standard Spark")
            builder = SparkSession.builder.appName("odibi").config(
                "spark.sql.sources.partitionOverwriteMode", "dynamic"
            )

            # Performance Optimizations
            builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")
            builder = builder.config("spark.sql.adaptive.enabled", "true")

            # Reduce Verbosity
            builder = builder.config(
                "spark.driver.extraJavaOptions", "-Dlog4j.rootCategory=ERROR, console"
            )

            self.spark = spark_session or builder.getOrCreate()
            self.spark.sparkContext.setLogLevel("ERROR")

        self.config = config or {}
        self.connections = connections or {}

        # Configure all ADLS connections upfront
        self._configure_all_connections()

        # Apply user-defined Spark configs from performance settings
        self._apply_spark_config()

        elapsed = (time.time() - start_time) * 1000
        ctx.info(
            "SparkEngine initialized",
            elapsed_ms=round(elapsed, 2),
            app_name=self.spark.sparkContext.appName,
            spark_version=self.spark.version,
            connections_configured=len(self.connections),
            using_existing_session=spark_session is not None,
        )

    def _configure_all_connections(self) -> None:
        """Configure Spark with all ADLS connection credentials.

        This sets all storage account keys upfront so Spark can access
        multiple accounts. Keys are scoped by account name, so no conflicts.
        """
        ctx = get_logging_context().with_context(engine="spark")

        for conn_name, connection in self.connections.items():
            if hasattr(connection, "configure_spark"):
                ctx.log_connection(
                    connection_type=type(connection).__name__,
                    connection_name=conn_name,
                    action="configure_spark",
                )
                try:
                    connection.configure_spark(self.spark)
                    ctx.debug(f"Configured ADLS connection: {conn_name}")
                except Exception as e:
                    ctx.error(
                        f"Failed to configure ADLS connection: {conn_name}",
                        error_type=type(e).__name__,
                        error_message=str(e),
                    )
                    raise

    def _apply_spark_config(self) -> None:
        """Apply user-defined Spark configurations from performance settings.

        Applies configs via spark.conf.set() for runtime-settable options.
        For existing sessions (e.g., Databricks), only modifiable configs take effect.
        """
        ctx = get_logging_context().with_context(engine="spark")

        performance = self.config.get("performance", {})
        spark_config = performance.get("spark_config", {})

        if not spark_config:
            return

        ctx.debug("Applying Spark configuration", config_count=len(spark_config))

        for key, value in spark_config.items():
            try:
                self.spark.conf.set(key, value)
                ctx.debug(
                    f"Applied Spark config: {key}={value}", config_key=key, config_value=value
                )
            except Exception as e:
                ctx.warning(
                    f"Failed to set Spark config '{key}'",
                    config_key=key,
                    error_message=str(e),
                    suggestion="This config may require session restart",
                )

    def _apply_table_properties(
        self, target: str, properties: Dict[str, str], is_table: bool = False
    ) -> None:
        """Apply table properties to a Delta table.

        Performance: Batches all properties into a single ALTER TABLE statement
        to avoid multiple round-trips to the catalog.
        """
        if not properties:
            return

        ctx = get_logging_context().with_context(engine="spark")

        try:
            table_ref = target if is_table else f"delta.`{target}`"
            ctx.debug(
                f"Applying table properties to {target}",
                properties_count=len(properties),
                is_table=is_table,
            )

            props_list = [f"'{k}' = '{v}'" for k, v in properties.items()]
            props_str = ", ".join(props_list)
            sql = f"ALTER TABLE {table_ref} SET TBLPROPERTIES ({props_str})"
            self.spark.sql(sql)
            ctx.debug(f"Set {len(properties)} table properties in single statement")

        except Exception as e:
            ctx.warning(
                f"Failed to set table properties on {target}",
                error_type=type(e).__name__,
                error_message=str(e),
            )

    def _optimize_delta_write(
        self, target: str, options: Dict[str, Any], is_table: bool = False
    ) -> None:
        """Run Delta Lake optimization (OPTIMIZE / ZORDER)."""
        should_optimize = options.get("optimize_write", False)
        zorder_by = options.get("zorder_by")

        if not should_optimize and not zorder_by:
            return

        ctx = get_logging_context().with_context(engine="spark")
        start_time = time.time()

        try:
            if is_table:
                sql = f"OPTIMIZE {target}"
            else:
                sql = f"OPTIMIZE delta.`{target}`"

            if zorder_by:
                if isinstance(zorder_by, str):
                    zorder_by = [zorder_by]
                cols = ", ".join(zorder_by)
                sql += f" ZORDER BY ({cols})"

            ctx.debug("Running Delta optimization", sql=sql, target=target)
            self.spark.sql(sql)

            elapsed = (time.time() - start_time) * 1000
            ctx.info(
                "Delta optimization completed",
                target=target,
                zorder_by=zorder_by,
                elapsed_ms=round(elapsed, 2),
            )

        except Exception as e:
            elapsed = (time.time() - start_time) * 1000
            ctx.warning(
                f"Optimization failed for {target}",
                error_type=type(e).__name__,
                error_message=str(e),
                elapsed_ms=round(elapsed, 2),
            )

    def _get_last_delta_commit_info(
        self, target: str, is_table: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Get metadata for the most recent Delta commit."""
        ctx = get_logging_context().with_context(engine="spark")

        try:
            from delta.tables import DeltaTable

            if is_table:
                dt = DeltaTable.forName(self.spark, target)
            else:
                dt = DeltaTable.forPath(self.spark, target)

            last_commit = dt.history(1).collect()[0]

            def safe_get(row, field):
                if hasattr(row, field):
                    return getattr(row, field)
                if hasattr(row, "__getitem__"):
                    try:
                        return row[field]
                    except (KeyError, ValueError):
                        return None
                return None

            commit_info = {
                "version": safe_get(last_commit, "version"),
                "timestamp": safe_get(last_commit, "timestamp"),
                "operation": safe_get(last_commit, "operation"),
                "operation_metrics": safe_get(last_commit, "operationMetrics"),
                "read_version": safe_get(last_commit, "readVersion"),
            }

            ctx.debug(
                "Delta commit metadata retrieved",
                target=target,
                version=commit_info.get("version"),
                operation=commit_info.get("operation"),
            )

            return commit_info

        except Exception as e:
            ctx.warning(
                f"Failed to fetch Delta commit info for {target}",
                error_type=type(e).__name__,
                error_message=str(e),
            )
            return None

    def harmonize_schema(self, df, target_schema: Dict[str, str], policy: Any):
        """Harmonize DataFrame schema with target schema according to policy."""
        from pyspark.sql.functions import col, lit

        from odibi.config import OnMissingColumns, OnNewColumns, SchemaMode

        ctx = get_logging_context().with_context(engine="spark")

        target_cols = list(target_schema.keys())
        current_cols = df.columns

        missing = set(target_cols) - set(current_cols)
        new_cols = set(current_cols) - set(target_cols)

        ctx.debug(
            "Schema harmonization",
            target_columns=len(target_cols),
            current_columns=len(current_cols),
            missing_columns=list(missing) if missing else None,
            new_columns=list(new_cols) if new_cols else None,
            policy_mode=policy.mode.value if hasattr(policy.mode, "value") else str(policy.mode),
        )

        # Check Validations
        if missing and policy.on_missing_columns == OnMissingColumns.FAIL:
            ctx.error(
                f"Schema Policy Violation: Missing columns {missing}",
                missing_columns=list(missing),
            )
            raise ValueError(f"Schema Policy Violation: Missing columns {missing}")

        if new_cols and policy.on_new_columns == OnNewColumns.FAIL:
            ctx.error(
                f"Schema Policy Violation: New columns {new_cols}",
                new_columns=list(new_cols),
            )
            raise ValueError(f"Schema Policy Violation: New columns {new_cols}")

        # Apply Transformations
        if policy.mode == SchemaMode.EVOLVE and policy.on_new_columns == OnNewColumns.ADD_NULLABLE:
            res = df
            for c in missing:
                res = res.withColumn(c, lit(None))
            ctx.debug("Schema evolved: added missing columns as null")
            return res
        else:
            select_exprs = []
            for c in target_cols:
                if c in current_cols:
                    select_exprs.append(col(c))
                else:
                    select_exprs.append(lit(None).alias(c))

            ctx.debug("Schema enforced: projected to target schema")
            return df.select(*select_exprs)

    def anonymize(self, df, columns: List[str], method: str, salt: Optional[str] = None):
        """Anonymize columns using Spark functions."""
        from pyspark.sql.functions import col, concat, lit, regexp_replace, sha2

        ctx = get_logging_context().with_context(engine="spark")
        ctx.debug(
            "Anonymizing columns",
            columns=columns,
            method=method,
            has_salt=salt is not None,
        )

        res = df
        for c in columns:
            if c not in df.columns:
                ctx.warning(f"Column '{c}' not found for anonymization, skipping", column=c)
                continue

            if method == "hash":
                if salt:
                    res = res.withColumn(c, sha2(concat(col(c), lit(salt)), 256))
                else:
                    res = res.withColumn(c, sha2(col(c), 256))

            elif method == "mask":
                res = res.withColumn(c, regexp_replace(col(c), ".(?=.{4})", "*"))

            elif method == "redact":
                res = res.withColumn(c, lit("[REDACTED]"))

        ctx.debug(f"Anonymization completed using {method}")
        return res

    def get_schema(self, df) -> Dict[str, str]:
        """Get DataFrame schema with types."""
        return {f.name: f.dataType.simpleString() for f in df.schema}

    def get_shape(self, df) -> Tuple[int, int]:
        """Get DataFrame shape as (rows, columns)."""
        return (df.count(), len(df.columns))

    def count_rows(self, df) -> int:
        """Count rows in DataFrame."""
        return df.count()

    def read(
        self,
        connection: Any,
        format: str,
        table: Optional[str] = None,
        path: Optional[str] = None,
        streaming: bool = False,
        schema: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
        as_of_version: Optional[int] = None,
        as_of_timestamp: Optional[str] = None,
    ) -> Any:
        """Read data using Spark.

        Args:
            connection: Connection object (with get_path method)
            format: Data format (csv, parquet, json, delta, sql_server)
            table: Table name
            path: File path
            streaming: Whether to read as a stream (readStream)
            schema: Schema string in DDL format (required for streaming file sources)
            options: Format-specific options (including versionAsOf for Delta time travel)
            as_of_version: Time travel version
            as_of_timestamp: Time travel timestamp

        Returns:
            Spark DataFrame (or Streaming DataFrame)
        """
        ctx = get_logging_context().with_context(engine="spark")
        start_time = time.time()
        options = options or {}

        source_identifier = table or path or "unknown"
        ctx.debug(
            "Starting Spark read",
            format=format,
            source=source_identifier,
            streaming=streaming,
            as_of_version=as_of_version,
            as_of_timestamp=as_of_timestamp,
        )

        # Handle Time Travel options
        if as_of_version is not None:
            options["versionAsOf"] = as_of_version
            ctx.debug(f"Time travel enabled: version {as_of_version}")
        if as_of_timestamp is not None:
            options["timestampAsOf"] = as_of_timestamp
            ctx.debug(f"Time travel enabled: timestamp {as_of_timestamp}")

        # SQL Server / Azure SQL Support
        if format in ["sql", "sql_server", "azure_sql"]:
            if streaming:
                ctx.error("Streaming not supported for SQL Server / Azure SQL")
                raise ValueError("Streaming not supported for SQL Server / Azure SQL yet.")

            if not hasattr(connection, "get_spark_options"):
                conn_type = type(connection).__name__
                msg = f"Connection type '{conn_type}' does not support Spark SQL read"
                ctx.error(msg, connection_type=conn_type)
                raise ValueError(msg)

            jdbc_options = connection.get_spark_options()
            merged_options = {**jdbc_options, **options}

            # Extract filter for SQL pushdown
            sql_filter = merged_options.pop("filter", None)

            if "query" in merged_options:
                merged_options.pop("dbtable", None)
                # If filter provided with query, append to WHERE clause
                if sql_filter:
                    existing_query = merged_options["query"]
                    # Wrap existing query and add filter
                    if "WHERE" in existing_query.upper():
                        merged_options["query"] = f"({existing_query}) AND ({sql_filter})"
                    else:
                        subquery = f"SELECT * FROM ({existing_query}) AS _subq WHERE {sql_filter}"
                        merged_options["query"] = subquery
                    ctx.debug(f"Applied SQL pushdown filter to query: {sql_filter}")
            elif table:
                # Build query with filter pushdown instead of using dbtable
                if sql_filter:
                    merged_options.pop("dbtable", None)
                    merged_options["query"] = f"SELECT * FROM {table} WHERE {sql_filter}"
                    ctx.debug(f"Applied SQL pushdown filter: {sql_filter}")
                else:
                    merged_options["dbtable"] = table
            elif "dbtable" not in merged_options:
                ctx.error("SQL format requires 'table' config or 'query' option")
                raise ValueError("SQL format requires 'table' config or 'query' option")

            ctx.debug("Executing JDBC read", has_query="query" in merged_options)

            try:
                df = self.spark.read.format("jdbc").options(**merged_options).load()
                elapsed = (time.time() - start_time) * 1000
                partition_count = df.rdd.getNumPartitions()

                ctx.log_file_io(path=source_identifier, format=format, mode="read")
                ctx.log_spark_metrics(partition_count=partition_count)
                ctx.info(
                    "JDBC read completed",
                    source=source_identifier,
                    elapsed_ms=round(elapsed, 2),
                    partitions=partition_count,
                )
                return df

            except Exception as e:
                elapsed = (time.time() - start_time) * 1000
                ctx.error(
                    "JDBC read failed",
                    source=source_identifier,
                    error_type=type(e).__name__,
                    error_message=str(e),
                    elapsed_ms=round(elapsed, 2),
                )
                raise

        # Read based on format
        if table:
            # Managed/External Table (Catalog)
            ctx.debug(f"Reading from catalog table: {table}")

            if streaming:
                reader = self.spark.readStream.format(format)
            else:
                reader = self.spark.read.format(format)

            for key, value in options.items():
                reader = reader.option(key, value)

            try:
                df = reader.table(table)

                if "filter" in options:
                    df = df.filter(options["filter"])
                    ctx.debug(f"Applied filter: {options['filter']}")

                elapsed = (time.time() - start_time) * 1000

                if not streaming:
                    partition_count = df.rdd.getNumPartitions()
                    ctx.log_spark_metrics(partition_count=partition_count)
                    ctx.log_file_io(path=table, format=format, mode="read")
                    ctx.info(
                        f"Table read completed: {table}",
                        elapsed_ms=round(elapsed, 2),
                        partitions=partition_count,
                    )
                else:
                    ctx.info(f"Streaming read started: {table}", elapsed_ms=round(elapsed, 2))

                return df

            except Exception as e:
                elapsed = (time.time() - start_time) * 1000
                ctx.error(
                    f"Table read failed: {table}",
                    error_type=type(e).__name__,
                    error_message=str(e),
                    elapsed_ms=round(elapsed, 2),
                )
                raise

        elif path:
            # File Path
            full_path = connection.get_path(path)
            ctx.debug(f"Reading from path: {full_path}")

            # Auto-detect encoding for CSV (Batch only)
            if not streaming and format == "csv" and options.get("auto_encoding"):
                options = options.copy()
                options.pop("auto_encoding")

                if "encoding" not in options:
                    try:
                        from odibi.utils.encoding import detect_encoding

                        detected = detect_encoding(connection, path)
                        if detected:
                            options["encoding"] = detected
                            ctx.debug(f"Detected encoding: {detected}", path=path)
                    except ImportError:
                        pass
                    except Exception as e:
                        ctx.warning(
                            f"Encoding detection failed for {path}",
                            error_message=str(e),
                        )

            if streaming:
                reader = self.spark.readStream.format(format)
                if schema:
                    reader = reader.schema(schema)
                    ctx.debug(f"Applied schema for streaming read: {schema[:100]}...")
                else:
                    # Determine if we should warn about missing schema
                    # Formats that can infer schema: delta, parquet, avro (embedded schema)
                    # cloudFiles with schemaLocation or self-describing formats (avro, parquet) are fine
                    should_warn = True

                    if format in ["delta", "parquet"]:
                        should_warn = False
                    elif format == "cloudFiles":
                        cloud_format = options.get("cloudFiles.format", "")
                        has_schema_location = "cloudFiles.schemaLocation" in options
                        # avro and parquet have embedded schemas
                        if cloud_format in ["avro", "parquet"] or has_schema_location:
                            should_warn = False

                    if should_warn:
                        ctx.warning(
                            f"Streaming read from '{format}' format without schema. "
                            "Schema inference is not supported for streaming sources. "
                            "Consider adding 'schema' to your read config."
                        )
            else:
                reader = self.spark.read.format(format)
                if schema:
                    reader = reader.schema(schema)

            for key, value in options.items():
                if key == "header" and isinstance(value, bool):
                    value = str(value).lower()
                reader = reader.option(key, value)

            try:
                df = reader.load(full_path)

                if "filter" in options:
                    df = df.filter(options["filter"])
                    ctx.debug(f"Applied filter: {options['filter']}")

                elapsed = (time.time() - start_time) * 1000

                if not streaming:
                    partition_count = df.rdd.getNumPartitions()
                    ctx.log_spark_metrics(partition_count=partition_count)
                    ctx.log_file_io(path=path, format=format, mode="read")
                    ctx.info(
                        f"File read completed: {path}",
                        elapsed_ms=round(elapsed, 2),
                        partitions=partition_count,
                        format=format,
                    )
                else:
                    ctx.info(f"Streaming read started: {path}", elapsed_ms=round(elapsed, 2))

                return df

            except Exception as e:
                elapsed = (time.time() - start_time) * 1000
                ctx.error(
                    f"File read failed: {path}",
                    error_type=type(e).__name__,
                    error_message=str(e),
                    elapsed_ms=round(elapsed, 2),
                    format=format,
                )
                raise
        else:
            ctx.error("Either path or table must be provided")
            raise ValueError("Either path or table must be provided")

    def write(
        self,
        df: Any,
        connection: Any,
        format: str,
        table: Optional[str] = None,
        path: Optional[str] = None,
        register_table: Optional[str] = None,
        mode: str = "overwrite",
        options: Optional[Dict[str, Any]] = None,
        streaming_config: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Write data using Spark.

        Args:
            df: Spark DataFrame to write
            connection: Connection object
            format: Output format (csv, parquet, json, delta)
            table: Table name
            path: File path
            register_table: Name to register as external table (if path is used)
            mode: Write mode (overwrite, append, error, ignore, upsert, append_once)
            options: Format-specific options (including partition_by for partitioning)
            streaming_config: StreamingWriteConfig for streaming DataFrames

        Returns:
            Optional dictionary containing Delta commit metadata (if format=delta),
            or streaming query info (if streaming)
        """
        ctx = get_logging_context().with_context(engine="spark")
        start_time = time.time()
        options = options or {}

        if getattr(df, "isStreaming", False) is True:
            return self._write_streaming(
                df=df,
                connection=connection,
                format=format,
                table=table,
                path=path,
                register_table=register_table,
                options=options,
                streaming_config=streaming_config,
            )

        target_identifier = table or path or "unknown"
        try:
            partition_count = df.rdd.getNumPartitions()
        except Exception:
            partition_count = 1  # Fallback for mocks or unsupported DataFrames

        # Auto-coalesce DataFrames for Delta writes to reduce file overhead
        # Use coalesce_partitions option to explicitly set target partitions
        # NOTE: We avoid df.count() here as it would trigger double-evaluation of lazy DataFrames
        coalesce_partitions = options.pop("coalesce_partitions", None)
        if (
            coalesce_partitions
            and isinstance(partition_count, int)
            and partition_count > coalesce_partitions
        ):
            df = df.coalesce(coalesce_partitions)
            ctx.debug(
                f"Coalesced DataFrame to {coalesce_partitions} partition(s)",
                original_partitions=partition_count,
            )
            partition_count = coalesce_partitions

        ctx.debug(
            "Starting Spark write",
            format=format,
            target=target_identifier,
            mode=mode,
            partitions=partition_count,
        )

        # SQL Server / Azure SQL Support
        if format in ["sql", "sql_server", "azure_sql"]:
            if not hasattr(connection, "get_spark_options"):
                conn_type = type(connection).__name__
                msg = f"Connection type '{conn_type}' does not support Spark SQL write"
                ctx.error(msg, connection_type=conn_type)
                raise ValueError(msg)

            jdbc_options = connection.get_spark_options()
            merged_options = {**jdbc_options, **options}

            if table:
                merged_options["dbtable"] = table
            elif "dbtable" not in merged_options:
                ctx.error("SQL format requires 'table' config or 'dbtable' option")
                raise ValueError("SQL format requires 'table' config or 'dbtable' option")

            # Handle MERGE mode for SQL Server
            if mode == "merge":
                merge_keys = options.get("merge_keys")
                merge_options = options.get("merge_options")

                if not merge_keys:
                    ctx.error("MERGE mode requires 'merge_keys' in options")
                    raise ValueError(
                        "MERGE mode requires 'merge_keys' in options. "
                        "Specify the key columns for the MERGE ON clause."
                    )

                from odibi.writers.sql_server_writer import SqlServerMergeWriter

                writer = SqlServerMergeWriter(connection)
                ctx.debug(
                    "Executing SQL Server MERGE",
                    target=table,
                    merge_keys=merge_keys,
                )

                try:
                    result = writer.merge(
                        df=df,
                        spark_engine=self,
                        target_table=table,
                        merge_keys=merge_keys,
                        options=merge_options,
                        jdbc_options=jdbc_options,
                    )
                    elapsed = (time.time() - start_time) * 1000
                    ctx.log_file_io(path=target_identifier, format=format, mode="write")
                    ctx.info(
                        "SQL Server MERGE completed",
                        target=target_identifier,
                        mode=mode,
                        inserted=result.inserted,
                        updated=result.updated,
                        deleted=result.deleted,
                        elapsed_ms=round(elapsed, 2),
                    )
                    return {
                        "mode": "merge",
                        "inserted": result.inserted,
                        "updated": result.updated,
                        "deleted": result.deleted,
                        "total_affected": result.total_affected,
                    }

                except Exception as e:
                    elapsed = (time.time() - start_time) * 1000
                    ctx.error(
                        "SQL Server MERGE failed",
                        target=target_identifier,
                        error_type=type(e).__name__,
                        error_message=str(e),
                        elapsed_ms=round(elapsed, 2),
                    )
                    raise

            # Handle enhanced overwrite with strategies
            if mode == "overwrite" and options.get("overwrite_options"):
                from odibi.writers.sql_server_writer import SqlServerMergeWriter

                overwrite_options = options.get("overwrite_options")
                writer = SqlServerMergeWriter(connection)

                ctx.debug(
                    "Executing SQL Server enhanced overwrite",
                    target=table,
                    strategy=(
                        overwrite_options.strategy.value
                        if hasattr(overwrite_options, "strategy")
                        else "truncate_insert"
                    ),
                )

                try:
                    result = writer.overwrite_spark(
                        df=df,
                        target_table=table,
                        options=overwrite_options,
                        jdbc_options=jdbc_options,
                    )
                    elapsed = (time.time() - start_time) * 1000
                    ctx.log_file_io(path=target_identifier, format=format, mode="write")
                    ctx.info(
                        "SQL Server enhanced overwrite completed",
                        target=target_identifier,
                        strategy=result.strategy,
                        rows_written=result.rows_written,
                        elapsed_ms=round(elapsed, 2),
                    )
                    return {
                        "mode": "overwrite",
                        "strategy": result.strategy,
                        "rows_written": result.rows_written,
                    }

                except Exception as e:
                    elapsed = (time.time() - start_time) * 1000
                    ctx.error(
                        "SQL Server enhanced overwrite failed",
                        target=target_identifier,
                        error_type=type(e).__name__,
                        error_message=str(e),
                        elapsed_ms=round(elapsed, 2),
                    )
                    raise

            if mode not in ["overwrite", "append", "ignore", "error"]:
                if mode == "fail":
                    mode = "error"
                else:
                    ctx.error(f"Write mode '{mode}' not supported for Spark SQL write")
                    raise ValueError(f"Write mode '{mode}' not supported for Spark SQL write")

            ctx.debug("Executing JDBC write", target=table or merged_options.get("dbtable"))

            try:
                df.write.format("jdbc").options(**merged_options).mode(mode).save()
                elapsed = (time.time() - start_time) * 1000
                ctx.log_file_io(path=target_identifier, format=format, mode="write")
                ctx.info(
                    "JDBC write completed",
                    target=target_identifier,
                    mode=mode,
                    elapsed_ms=round(elapsed, 2),
                )
                return None

            except Exception as e:
                elapsed = (time.time() - start_time) * 1000
                ctx.error(
                    "JDBC write failed",
                    target=target_identifier,
                    error_type=type(e).__name__,
                    error_message=str(e),
                    elapsed_ms=round(elapsed, 2),
                )
                raise

        # Handle Upsert/AppendOnce (Delta Only)
        if mode in ["upsert", "append_once"]:
            if format != "delta":
                ctx.error(f"Mode '{mode}' only supported for Delta format")
                raise NotImplementedError(
                    f"Mode '{mode}' only supported for Delta format in Spark engine."
                )

            keys = options.get("keys")
            if not keys:
                ctx.error(f"Mode '{mode}' requires 'keys' list in options")
                raise ValueError(f"Mode '{mode}' requires 'keys' list in options")

            if isinstance(keys, str):
                keys = [keys]

            exists = self.table_exists(connection, table, path)
            ctx.debug("Table existence check for merge", target=target_identifier, exists=exists)

            if not exists:
                mode = "overwrite"
                ctx.debug("Target does not exist, falling back to overwrite mode")
            else:
                from delta.tables import DeltaTable

                target_dt = None
                target_name = ""
                is_table_target = False

                if table:
                    target_dt = DeltaTable.forName(self.spark, table)
                    target_name = table
                    is_table_target = True
                elif path:
                    full_path = connection.get_path(path)
                    target_dt = DeltaTable.forPath(self.spark, full_path)
                    target_name = full_path
                    is_table_target = False

                condition = " AND ".join([f"target.`{k}` = source.`{k}`" for k in keys])
                ctx.debug("Executing Delta merge", merge_mode=mode, keys=keys, condition=condition)

                merge_builder = target_dt.alias("target").merge(df.alias("source"), condition)

                try:
                    if mode == "upsert":
                        merge_builder.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                    elif mode == "append_once":
                        merge_builder.whenNotMatchedInsertAll().execute()

                    elapsed = (time.time() - start_time) * 1000
                    ctx.info(
                        "Delta merge completed",
                        target=target_name,
                        mode=mode,
                        elapsed_ms=round(elapsed, 2),
                    )

                    self._optimize_delta_write(target_name, options, is_table=is_table_target)
                    commit_info = self._get_last_delta_commit_info(
                        target_name, is_table=is_table_target
                    )

                    if commit_info:
                        ctx.debug(
                            "Delta commit info",
                            version=commit_info.get("version"),
                            operation=commit_info.get("operation"),
                        )

                    return commit_info

                except Exception as e:
                    elapsed = (time.time() - start_time) * 1000
                    ctx.error(
                        "Delta merge failed",
                        target=target_name,
                        error_type=type(e).__name__,
                        error_message=str(e),
                        elapsed_ms=round(elapsed, 2),
                    )
                    raise

        # Get output location
        if table:
            # Managed/External Table (Catalog)
            ctx.debug(f"Writing to catalog table: {table}")
            writer = df.write.format(format).mode(mode)

            partition_by = options.get("partition_by")
            if partition_by:
                if isinstance(partition_by, str):
                    partition_by = [partition_by]
                writer = writer.partitionBy(*partition_by)
                ctx.debug(f"Partitioning by: {partition_by}")

            for key, value in options.items():
                writer = writer.option(key, value)

            try:
                writer.saveAsTable(table)
                elapsed = (time.time() - start_time) * 1000

                ctx.log_file_io(
                    path=table,
                    format=format,
                    mode=mode,
                    partitions=partition_by,
                )
                ctx.info(
                    f"Table write completed: {table}",
                    mode=mode,
                    elapsed_ms=round(elapsed, 2),
                )

                if format == "delta":
                    self._optimize_delta_write(table, options, is_table=True)
                    return self._get_last_delta_commit_info(table, is_table=True)
                return None

            except Exception as e:
                elapsed = (time.time() - start_time) * 1000
                ctx.error(
                    f"Table write failed: {table}",
                    error_type=type(e).__name__,
                    error_message=str(e),
                    elapsed_ms=round(elapsed, 2),
                )
                raise

        elif path:
            full_path = connection.get_path(path)
        else:
            ctx.error("Either path or table must be provided")
            raise ValueError("Either path or table must be provided")

        # Extract partition_by option
        partition_by = options.pop("partition_by", None) or options.pop("partitionBy", None)

        # Extract cluster_by option (Liquid Clustering)
        cluster_by = options.pop("cluster_by", None)

        # Warn about partitioning anti-patterns
        if partition_by and cluster_by:
            import warnings

            ctx.warning(
                "Conflict: Both 'partition_by' and 'cluster_by' are set",
                partition_by=partition_by,
                cluster_by=cluster_by,
            )
            warnings.warn(
                "⚠️  Conflict: Both 'partition_by' and 'cluster_by' (Liquid Clustering) are set. "
                "Liquid Clustering supersedes partitioning. 'partition_by' will be ignored "
                "if the table is being created now.",
                UserWarning,
            )

        elif partition_by:
            import warnings

            ctx.warning(
                "Partitioning warning: ensure low-cardinality columns",
                partition_by=partition_by,
            )
            warnings.warn(
                "⚠️  Partitioning can cause performance issues if misused. "
                "Only partition on low-cardinality columns (< 1000 unique values) "
                "and ensure each partition has > 1000 rows.",
                UserWarning,
            )

        # Handle Upsert/Append-Once for Delta Lake (Path-based only for now)
        if format == "delta" and mode in ["upsert", "append_once"]:
            try:
                from delta.tables import DeltaTable
            except ImportError:
                ctx.error("Delta Lake support requires 'delta-spark'")
                raise ImportError("Delta Lake support requires 'delta-spark'")

            if "keys" not in options:
                ctx.error(f"Mode '{mode}' requires 'keys' list in options")
                raise ValueError(f"Mode '{mode}' requires 'keys' list in options")

            if DeltaTable.isDeltaTable(self.spark, full_path):
                ctx.debug(f"Performing Delta merge at path: {full_path}")
                delta_table = DeltaTable.forPath(self.spark, full_path)
                keys = options["keys"]
                if isinstance(keys, str):
                    keys = [keys]

                condition = " AND ".join([f"target.{k} = source.{k}" for k in keys])
                merger = delta_table.alias("target").merge(df.alias("source"), condition)

                try:
                    if mode == "upsert":
                        merger.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                    else:
                        merger.whenNotMatchedInsertAll().execute()

                    elapsed = (time.time() - start_time) * 1000
                    ctx.info(
                        "Delta merge completed at path",
                        path=path,
                        mode=mode,
                        elapsed_ms=round(elapsed, 2),
                    )

                    if register_table:
                        try:
                            table_in_catalog = self.spark.catalog.tableExists(register_table)
                            needs_registration = not table_in_catalog

                            # Handle orphan catalog entries (only for path-not-found errors)
                            if table_in_catalog:
                                try:
                                    self.spark.table(register_table).limit(0).collect()
                                    ctx.debug(
                                        f"Table '{register_table}' already registered and valid"
                                    )
                                except Exception as verify_err:
                                    error_str = str(verify_err)
                                    is_orphan = (
                                        "DELTA_PATH_DOES_NOT_EXIST" in error_str
                                        or "Path does not exist" in error_str
                                        or "FileNotFoundException" in error_str
                                    )
                                    if is_orphan:
                                        ctx.warning(
                                            f"Table '{register_table}' is orphan, re-registering"
                                        )
                                        try:
                                            self.spark.sql(f"DROP TABLE IF EXISTS {register_table}")
                                        except Exception:
                                            pass
                                        needs_registration = True
                                    else:
                                        ctx.debug(
                                            f"Table '{register_table}' verify failed, "
                                            "skipping registration"
                                        )

                            if needs_registration:
                                create_sql = (
                                    f"CREATE TABLE IF NOT EXISTS {register_table} "
                                    f"USING DELTA LOCATION '{full_path}'"
                                )
                                self.spark.sql(create_sql)
                                ctx.info(f"Registered table: {register_table}", path=full_path)
                        except Exception as e:
                            ctx.error(
                                f"Failed to register external table '{register_table}'",
                                error_message=str(e),
                            )

                    self._optimize_delta_write(full_path, options, is_table=False)
                    return self._get_last_delta_commit_info(full_path, is_table=False)

                except Exception as e:
                    elapsed = (time.time() - start_time) * 1000
                    ctx.error(
                        "Delta merge failed at path",
                        path=path,
                        error_type=type(e).__name__,
                        error_message=str(e),
                        elapsed_ms=round(elapsed, 2),
                    )
                    raise
            else:
                mode = "overwrite"
                ctx.debug("Target does not exist, falling back to overwrite mode")

        # Write based on format (Path-based)
        ctx.debug(f"Writing to path: {full_path}")

        # Handle Liquid Clustering (New Table Creation via SQL)
        if format == "delta" and cluster_by:
            should_create = False
            target_name = None

            if table:
                target_name = table
                if mode == "overwrite":
                    should_create = True
                elif mode == "append":
                    if not self.spark.catalog.tableExists(table):
                        should_create = True
            elif path:
                full_path = connection.get_path(path)
                target_name = f"delta.`{full_path}`"
                if mode == "overwrite":
                    should_create = True
                elif mode == "append":
                    try:
                        from delta.tables import DeltaTable

                        if not DeltaTable.isDeltaTable(self.spark, full_path):
                            should_create = True
                    except ImportError:
                        pass

            if should_create:
                if isinstance(cluster_by, str):
                    cluster_by = [cluster_by]

                cols = ", ".join(cluster_by)
                temp_view = f"odibi_temp_writer_{abs(hash(str(target_name)))}"
                df.createOrReplaceTempView(temp_view)

                create_cmd = (
                    "CREATE OR REPLACE TABLE"
                    if mode == "overwrite"
                    else "CREATE TABLE IF NOT EXISTS"
                )

                sql = (
                    f"{create_cmd} {target_name} USING DELTA CLUSTER BY ({cols}) "
                    f"AS SELECT * FROM {temp_view}"
                )

                ctx.debug("Creating clustered Delta table", sql=sql, cluster_by=cluster_by)

                try:
                    self.spark.sql(sql)
                    self.spark.catalog.dropTempView(temp_view)

                    elapsed = (time.time() - start_time) * 1000
                    ctx.info(
                        "Clustered Delta table created",
                        target=target_name,
                        cluster_by=cluster_by,
                        elapsed_ms=round(elapsed, 2),
                    )

                    if register_table and path:
                        try:
                            reg_sql = (
                                f"CREATE TABLE IF NOT EXISTS {register_table} "
                                f"USING DELTA LOCATION '{full_path}'"
                            )
                            self.spark.sql(reg_sql)
                            ctx.info(f"Registered table: {register_table}")
                        except Exception:
                            pass

                    if format == "delta":
                        self._optimize_delta_write(
                            target_name if table else full_path, options, is_table=bool(table)
                        )
                        return self._get_last_delta_commit_info(
                            target_name if table else full_path, is_table=bool(table)
                        )
                    return None

                except Exception as e:
                    elapsed = (time.time() - start_time) * 1000
                    ctx.error(
                        "Failed to create clustered Delta table",
                        error_type=type(e).__name__,
                        error_message=str(e),
                        elapsed_ms=round(elapsed, 2),
                    )
                    raise

        # Extract table_properties from options
        table_properties = options.pop("table_properties", None)

        # For column mapping and other properties that must be set BEFORE write
        original_configs = {}
        if table_properties and format == "delta":
            for prop_name, prop_value in table_properties.items():
                spark_conf_key = (
                    f"spark.databricks.delta.properties.defaults.{prop_name.replace('delta.', '')}"
                )
                try:
                    original_configs[spark_conf_key] = self.spark.conf.get(spark_conf_key, None)
                except Exception:
                    original_configs[spark_conf_key] = None
                self.spark.conf.set(spark_conf_key, prop_value)
            ctx.debug(
                "Applied table properties as session defaults",
                properties=list(table_properties.keys()),
            )

        writer = df.write.format(format).mode(mode)

        if partition_by:
            if isinstance(partition_by, str):
                partition_by = [partition_by]
            writer = writer.partitionBy(*partition_by)
            ctx.debug(f"Partitioning by: {partition_by}")

        for key, value in options.items():
            writer = writer.option(key, value)

        try:
            writer.save(full_path)
            elapsed = (time.time() - start_time) * 1000

            ctx.log_file_io(
                path=path,
                format=format,
                mode=mode,
                partitions=partition_by,
            )
            ctx.info(
                f"File write completed: {path}",
                format=format,
                mode=mode,
                elapsed_ms=round(elapsed, 2),
            )

        except Exception as e:
            elapsed = (time.time() - start_time) * 1000
            ctx.error(
                f"File write failed: {path}",
                error_type=type(e).__name__,
                error_message=str(e),
                elapsed_ms=round(elapsed, 2),
            )
            raise
        finally:
            for conf_key, original_value in original_configs.items():
                if original_value is None:
                    self.spark.conf.unset(conf_key)
                else:
                    self.spark.conf.set(conf_key, original_value)

        if format == "delta":
            self._optimize_delta_write(full_path, options, is_table=False)

        if register_table and format == "delta":
            try:
                table_in_catalog = self.spark.catalog.tableExists(register_table)
                needs_registration = not table_in_catalog

                # Handle orphan catalog entries: table exists but points to deleted path
                # Only treat as orphan if it's specifically a DELTA_PATH_DOES_NOT_EXIST error
                if table_in_catalog:
                    try:
                        self.spark.table(register_table).limit(0).collect()
                        ctx.debug(
                            f"Table '{register_table}' already registered and valid, "
                            "skipping registration"
                        )
                    except Exception as verify_err:
                        error_str = str(verify_err)
                        is_orphan = (
                            "DELTA_PATH_DOES_NOT_EXIST" in error_str
                            or "Path does not exist" in error_str
                            or "FileNotFoundException" in error_str
                        )

                        if is_orphan:
                            # Orphan entry - table in catalog but path was deleted
                            ctx.warning(
                                f"Table '{register_table}' is orphan (path deleted), "
                                "dropping and re-registering",
                                error_message=error_str[:200],
                            )
                            try:
                                self.spark.sql(f"DROP TABLE IF EXISTS {register_table}")
                            except Exception:
                                pass  # Best effort cleanup
                            needs_registration = True
                        else:
                            # Other error (auth, network, etc.) - don't drop, just log
                            ctx.debug(
                                f"Table '{register_table}' exists but verify failed "
                                "(not orphan), skipping registration",
                                error_message=error_str[:200],
                            )

                if needs_registration:
                    ctx.debug(f"Registering table '{register_table}' at '{full_path}'")
                    reg_sql = (
                        f"CREATE TABLE IF NOT EXISTS {register_table} "
                        f"USING DELTA LOCATION '{full_path}'"
                    )
                    self.spark.sql(reg_sql)
                    ctx.info(f"Registered table: {register_table}", path=full_path)
            except Exception as e:
                ctx.error(
                    f"Failed to register table '{register_table}'",
                    error_message=str(e),
                )
                raise RuntimeError(
                    f"Failed to register external table '{register_table}': {e}"
                ) from e

        if format == "delta":
            return self._get_last_delta_commit_info(full_path, is_table=False)

        return None

    def _write_streaming(
        self,
        df,
        connection: Any,
        format: str,
        table: Optional[str] = None,
        path: Optional[str] = None,
        register_table: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
        streaming_config: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Write streaming DataFrame using Spark Structured Streaming.

        Args:
            df: Streaming Spark DataFrame
            connection: Connection object
            format: Output format (delta, kafka, etc.)
            table: Table name
            path: File path
            register_table: Name to register as external table (if path is used)
            options: Format-specific options
            streaming_config: StreamingWriteConfig with streaming parameters

        Returns:
            Dictionary with streaming query information
        """
        ctx = get_logging_context().with_context(engine="spark")
        start_time = time.time()
        options = options or {}

        if streaming_config is None:
            ctx.error("Streaming DataFrame requires streaming_config")
            raise ValueError(
                "Streaming DataFrame detected but no streaming_config provided. "
                "Add a 'streaming' section to your write config with at least "
                "'checkpoint_location' specified."
            )

        target_identifier = table or path or "unknown"

        checkpoint_location = streaming_config.checkpoint_location
        if checkpoint_location and connection:
            if not checkpoint_location.startswith(
                ("abfss://", "s3://", "gs://", "dbfs://", "hdfs://", "wasbs://")
            ):
                checkpoint_location = connection.get_path(checkpoint_location)
                ctx.debug(
                    "Resolved checkpoint location through connection",
                    original=streaming_config.checkpoint_location,
                    resolved=checkpoint_location,
                )

        ctx.debug(
            "Starting streaming write",
            format=format,
            target=target_identifier,
            output_mode=streaming_config.output_mode,
            checkpoint=checkpoint_location,
        )

        writer = df.writeStream.format(format)
        writer = writer.outputMode(streaming_config.output_mode)
        writer = writer.option("checkpointLocation", checkpoint_location)

        if streaming_config.query_name:
            writer = writer.queryName(streaming_config.query_name)

        if streaming_config.trigger:
            trigger = streaming_config.trigger
            if trigger.once:
                writer = writer.trigger(once=True)
            elif trigger.available_now:
                writer = writer.trigger(availableNow=True)
            elif trigger.processing_time:
                writer = writer.trigger(processingTime=trigger.processing_time)
            elif trigger.continuous:
                writer = writer.trigger(continuous=trigger.continuous)

        partition_by = options.pop("partition_by", None) or options.pop("partitionBy", None)
        if partition_by:
            if isinstance(partition_by, str):
                partition_by = [partition_by]
            writer = writer.partitionBy(*partition_by)
            ctx.debug(f"Partitioning by: {partition_by}")

        for key, value in options.items():
            writer = writer.option(key, value)

        # Capture Delta version before streaming to detect if new data was written
        version_before = None
        if path and format == "delta":
            try:
                full_path = connection.get_path(path)
                history_df = self.spark.sql(f"DESCRIBE HISTORY delta.`{full_path}` LIMIT 1")
                rows = history_df.collect()
                if rows:
                    version_before = (
                        rows[0].version if hasattr(rows[0], "version") else rows[0]["version"]
                    )
            except Exception:
                pass  # Table may not exist yet

        try:
            if table:
                query = writer.toTable(table)
                ctx.info(
                    f"Streaming query started: writing to table {table}",
                    query_id=str(query.id),
                    query_name=query.name,
                )
            elif path:
                full_path = connection.get_path(path)
                query = writer.start(full_path)
                ctx.info(
                    f"Streaming query started: writing to path {path}",
                    query_id=str(query.id),
                    query_name=query.name,
                )
            else:
                ctx.error("Either path or table must be provided for streaming write")
                raise ValueError(
                    "Streaming write operation failed: neither 'path' nor 'table' was provided. "
                    "Specify a file path or table name in your streaming configuration."
                )

            elapsed = (time.time() - start_time) * 1000

            result = {
                "streaming": True,
                "query_id": str(query.id),
                "query_name": query.name,
                "status": "running",
                "target": target_identifier,
                "output_mode": streaming_config.output_mode,
                "checkpoint_location": streaming_config.checkpoint_location,
                "elapsed_ms": round(elapsed, 2),
            }

            should_wait = streaming_config.await_termination
            if streaming_config.trigger:
                trigger = streaming_config.trigger
                if trigger.once or trigger.available_now:
                    should_wait = True

            if should_wait:
                ctx.info(
                    "Awaiting streaming query termination",
                    timeout_seconds=streaming_config.timeout_seconds,
                )
                query.awaitTermination(streaming_config.timeout_seconds)
                result["status"] = "terminated"
                elapsed = (time.time() - start_time) * 1000
                result["elapsed_ms"] = round(elapsed, 2)

                # Get rows written from streaming query progress
                rows_written = None

                # Method 1: Sum all micro-batch progress (most accurate for streaming)
                # For autoloader, numInputRows = rows read from source files in this run
                try:
                    recent_progress = query.recentProgress
                    if recent_progress:
                        total_rows = 0
                        for prog in recent_progress:
                            if prog:
                                # Use numInputRows - this is rows processed from source
                                # NOT sink.numOutputRows which can be cumulative
                                input_rows = prog.get("numInputRows")
                                if input_rows is not None:
                                    total_rows += input_rows
                        if total_rows > 0:
                            rows_written = total_rows
                except Exception:
                    pass

                # Method 1b: Try lastProgress if recentProgress is empty
                if rows_written is None:
                    try:
                        last_progress = query.lastProgress
                        if last_progress:
                            input_rows = last_progress.get("numInputRows")
                            if input_rows is not None and input_rows > 0:
                                rows_written = input_rows
                    except Exception:
                        pass

                # Method 2: Check Delta table history for rows written in THIS run only
                # Compare version before/after to detect if new data was written
                if rows_written is None and path and format == "delta":
                    try:
                        full_path = connection.get_path(path)
                        history_df = self.spark.sql(f"DESCRIBE HISTORY delta.`{full_path}` LIMIT 1")
                        rows = history_df.collect()
                        if rows:
                            row = rows[0]
                            version_after = (
                                row.version if hasattr(row, "version") else row["version"]
                            )

                            # If version hasn't changed, no new data was written
                            if version_before is not None and version_after == version_before:
                                rows_written = 0
                            else:
                                # New version created - get the row count from metrics
                                metrics = (
                                    row.operationMetrics
                                    if hasattr(row, "operationMetrics")
                                    else row["operationMetrics"]
                                )
                                if metrics:
                                    # For streaming: numAddedRows is rows added in this batch
                                    added_rows = metrics.get("numAddedRows")
                                    if added_rows is not None:
                                        rows_written = int(added_rows)
                                    else:
                                        # Fallback to numOutputRows
                                        output_rows = metrics.get("numOutputRows")
                                        if output_rows is not None:
                                            rows_written = int(output_rows)
                    except Exception:
                        pass

                if rows_written is not None:
                    result["_cached_row_count"] = rows_written

                ctx.info(
                    "Streaming query terminated",
                    query_id=str(query.id),
                    elapsed_ms=round(elapsed, 2),
                    rows_written=rows_written,
                )

                if register_table and path and format == "delta":
                    full_path = connection.get_path(path)
                    try:
                        self.spark.sql(
                            f"CREATE TABLE IF NOT EXISTS {register_table} "
                            f"USING DELTA LOCATION '{full_path}'"
                        )
                        ctx.info(
                            f"Registered external table: {register_table}",
                            path=full_path,
                        )
                        result["registered_table"] = register_table
                    except Exception as reg_err:
                        ctx.warning(
                            f"Failed to register external table '{register_table}'",
                            error=str(reg_err),
                        )
            else:
                result["streaming_query"] = query
                if register_table:
                    ctx.warning(
                        "register_table ignored for continuous streaming. "
                        "Table will be registered after query terminates or manually."
                    )

            return result

        except Exception as e:
            elapsed = (time.time() - start_time) * 1000
            ctx.error(
                "Streaming write failed",
                target=target_identifier,
                error_type=type(e).__name__,
                error_message=str(e),
                elapsed_ms=round(elapsed, 2),
            )
            raise

    def execute_sql(self, sql: str, context: Any = None) -> Any:
        """Execute SQL query in Spark.

        Args:
            sql: SQL query string
            context: Execution context (optional, not used for Spark)

        Returns:
            Spark DataFrame with query results
        """
        ctx = get_logging_context().with_context(engine="spark")
        start_time = time.time()

        ctx.debug("Executing Spark SQL", query_preview=sql[:200] if len(sql) > 200 else sql)

        try:
            result = self.spark.sql(sql)
            elapsed = (time.time() - start_time) * 1000
            partition_count = result.rdd.getNumPartitions()

            ctx.log_spark_metrics(partition_count=partition_count)
            ctx.info(
                "Spark SQL executed",
                elapsed_ms=round(elapsed, 2),
                partitions=partition_count,
            )

            return result

        except Exception as e:
            elapsed = (time.time() - start_time) * 1000
            error_type = type(e).__name__
            clean_message = _extract_spark_error_message(e)

            if "AnalysisException" in error_type:
                ctx.error(
                    "Spark SQL Analysis Error",
                    error_type=error_type,
                    error_message=clean_message,
                    query_preview=sql[:200] if len(sql) > 200 else sql,
                    elapsed_ms=round(elapsed, 2),
                )
                raise TransformError(f"Spark SQL Analysis Error: {clean_message}")

            if "ParseException" in error_type:
                ctx.error(
                    "Spark SQL Parse Error",
                    error_type=error_type,
                    error_message=clean_message,
                    query_preview=sql[:200] if len(sql) > 200 else sql,
                    elapsed_ms=round(elapsed, 2),
                )
                raise TransformError(f"Spark SQL Parse Error: {clean_message}")

            ctx.error(
                "Spark SQL execution failed",
                error_type=error_type,
                error_message=clean_message,
                elapsed_ms=round(elapsed, 2),
            )
            raise TransformError(f"Spark SQL Error: {clean_message}")

    def execute_transform(self, *args, **kwargs):
        raise NotImplementedError(
            "SparkEngine.execute_transform() will be implemented in Phase 2B. "
            "See PHASES.md for implementation plan."
        )

    def execute_operation(self, operation: str, params: Dict[str, Any], df) -> Any:
        """Execute built-in operation on Spark DataFrame."""
        ctx = get_logging_context().with_context(engine="spark")
        params = params or {}

        ctx.debug(f"Executing operation: {operation}", params=list(params.keys()))

        if operation == "pivot":
            group_by = params.get("group_by", [])
            pivot_column = params.get("pivot_column")
            value_column = params.get("value_column")
            agg_func = params.get("agg_func", "first")

            if not pivot_column or not value_column:
                ctx.error("Pivot requires 'pivot_column' and 'value_column'")
                raise ValueError("Pivot requires 'pivot_column' and 'value_column'")

            if isinstance(group_by, str):
                group_by = [group_by]

            agg_expr = {value_column: agg_func}
            return df.groupBy(*group_by).pivot(pivot_column).agg(agg_expr)

        elif operation == "drop_duplicates":
            subset = params.get("subset")
            if subset:
                if isinstance(subset, str):
                    subset = [subset]
                return df.dropDuplicates(subset=subset)
            return df.dropDuplicates()

        elif operation == "fillna":
            value = params.get("value")
            subset = params.get("subset")
            return df.fillna(value, subset=subset)

        elif operation == "drop":
            columns = params.get("columns")
            if not columns:
                return df
            if isinstance(columns, str):
                columns = [columns]
            return df.drop(*columns)

        elif operation == "rename":
            columns = params.get("columns")
            if not columns:
                return df

            res = df
            for old_name, new_name in columns.items():
                res = res.withColumnRenamed(old_name, new_name)
            return res

        elif operation == "sort":
            by = params.get("by")
            ascending = params.get("ascending", True)

            if not by:
                return df

            if isinstance(by, str):
                by = [by]

            if not ascending:
                from pyspark.sql.functions import desc

                sort_cols = [desc(c) for c in by]
                return df.orderBy(*sort_cols)

            return df.orderBy(*by)

        elif operation == "sample":
            fraction = params.get("frac", 0.1)
            seed = params.get("random_state")
            with_replacement = params.get("replace", False)
            return df.sample(withReplacement=with_replacement, fraction=fraction, seed=seed)

        else:
            # Fallback: check if operation is a registered transformer
            from odibi.context import EngineContext
            from odibi.registry import FunctionRegistry

            ctx.debug(
                f"Checking registry for operation: {operation}",
                registered_functions=list(FunctionRegistry._functions.keys())[:10],
                has_function=FunctionRegistry.has_function(operation),
            )

            if FunctionRegistry.has_function(operation):
                ctx.debug(f"Executing registered transformer as operation: {operation}")
                func = FunctionRegistry.get_function(operation)
                param_model = FunctionRegistry.get_param_model(operation)

                # Create EngineContext from current df
                from odibi.context import SparkContext

                engine_ctx = EngineContext(
                    context=SparkContext(self.spark),
                    df=df,
                    engine=self,
                    engine_type=self.engine_type,
                )

                # Validate and instantiate params
                if param_model:
                    validated_params = param_model(**params)
                    result_ctx = func(engine_ctx, validated_params)
                else:
                    result_ctx = func(engine_ctx, **params)

                return result_ctx.df

            ctx.error(f"Unsupported operation for Spark engine: {operation}")
            raise ValueError(f"Unsupported operation for Spark engine: {operation}")

    def count_nulls(self, df, columns: List[str]) -> Dict[str, int]:
        """Count nulls in specified columns."""
        from pyspark.sql.functions import col, count, when

        missing = set(columns) - set(df.columns)
        if missing:
            raise ValueError(f"Columns not found in DataFrame: {', '.join(missing)}")

        aggs = [count(when(col(c).isNull(), c)).alias(c) for c in columns]
        result = df.select(*aggs).collect()[0].asDict()
        return result

    def validate_schema(self, df, schema_rules: Dict[str, Any]) -> List[str]:
        """Validate DataFrame schema."""
        failures = []

        if "required_columns" in schema_rules:
            required = schema_rules["required_columns"]
            missing = set(required) - set(df.columns)
            if missing:
                failures.append(f"Missing required columns: {', '.join(missing)}")

        if "types" in schema_rules:
            type_map = {
                "int": ["integer", "long", "short", "byte", "bigint"],
                "float": ["double", "float"],
                "str": ["string"],
                "bool": ["boolean"],
            }

            for col_name, expected_type in schema_rules["types"].items():
                if col_name not in df.columns:
                    failures.append(f"Column '{col_name}' not found for type validation")
                    continue

                actual_type = dict(df.dtypes)[col_name]
                expected_dtypes = type_map.get(expected_type, [expected_type])

                if actual_type not in expected_dtypes:
                    failures.append(
                        f"Column '{col_name}' has type '{actual_type}', expected '{expected_type}'"
                    )

        return failures

    def validate_data(self, df, validation_config: Any) -> List[str]:
        """Validate DataFrame against rules."""
        from pyspark.sql.functions import col

        ctx = get_logging_context().with_context(engine="spark")
        failures = []

        if validation_config.not_empty:
            if df.isEmpty():
                failures.append("DataFrame is empty")

        if validation_config.no_nulls:
            null_counts = self.count_nulls(df, validation_config.no_nulls)
            for col_name, count in null_counts.items():
                if count > 0:
                    failures.append(f"Column '{col_name}' has {count} null values")

        if validation_config.schema_validation:
            schema_failures = self.validate_schema(df, validation_config.schema_validation)
            failures.extend(schema_failures)

        if validation_config.ranges:
            for col_name, bounds in validation_config.ranges.items():
                if col_name in df.columns:
                    min_val = bounds.get("min")
                    max_val = bounds.get("max")

                    if min_val is not None:
                        count = df.filter(col(col_name) < min_val).count()
                        if count > 0:
                            failures.append(f"Column '{col_name}' has values < {min_val}")

                    if max_val is not None:
                        count = df.filter(col(col_name) > max_val).count()
                        if count > 0:
                            failures.append(f"Column '{col_name}' has values > {max_val}")
                else:
                    failures.append(f"Column '{col_name}' not found for range validation")

        if validation_config.allowed_values:
            for col_name, allowed in validation_config.allowed_values.items():
                if col_name in df.columns:
                    count = df.filter(~col(col_name).isin(allowed)).count()
                    if count > 0:
                        failures.append(f"Column '{col_name}' has invalid values")
                else:
                    failures.append(f"Column '{col_name}' not found for allowed values validation")

        ctx.log_validation_result(
            passed=len(failures) == 0,
            rule_name="data_validation",
            failures=failures if failures else None,
        )

        return failures

    def get_sample(self, df, n: int = 10) -> List[Dict[str, Any]]:
        """Get sample rows as list of dictionaries."""
        return [row.asDict() for row in df.limit(n).collect()]

    def table_exists(
        self, connection: Any, table: Optional[str] = None, path: Optional[str] = None
    ) -> bool:
        """Check if table or location exists.

        Handles orphan catalog entries where the table is registered but
        the underlying Delta path no longer exists.
        """
        ctx = get_logging_context().with_context(engine="spark")

        if table:
            try:
                if not self.spark.catalog.tableExists(table):
                    ctx.debug(f"Table does not exist: {table}")
                    return False
                # Table exists in catalog - verify it's actually readable
                # This catches orphan entries where path was deleted
                self.spark.table(table).limit(0).collect()
                ctx.debug(f"Table existence check: {table}", exists=True)
                return True
            except Exception as e:
                # Table exists in catalog but underlying data is gone (orphan entry)
                # This is expected during first-run detection - log at debug level
                ctx.debug(
                    f"Table {table} exists in catalog but is not accessible (treating as first run)",
                    error_message=str(e),
                )
                return False
        elif path:
            try:
                from delta.tables import DeltaTable

                full_path = connection.get_path(path)
                exists = DeltaTable.isDeltaTable(self.spark, full_path)
                ctx.debug(f"Delta table existence check: {path}", exists=exists)
                return exists
            except ImportError:
                try:
                    full_path = connection.get_path(path)
                    exists = (
                        self.spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.FileSystem.get(
                            self.spark.sparkContext._jsc.hadoopConfiguration()
                        ).exists(
                            self.spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path(
                                full_path
                            )
                        )
                    )
                    ctx.debug(f"Path existence check: {path}", exists=exists)
                    return exists
                except Exception as e:
                    ctx.warning(f"Path existence check failed: {path}", error_message=str(e))
                    return False
            except Exception as e:
                ctx.warning(f"Table existence check failed: {path}", error_message=str(e))
                return False
        return False

    def get_table_schema(
        self,
        connection: Any,
        table: Optional[str] = None,
        path: Optional[str] = None,
        format: Optional[str] = None,
    ) -> Optional[Dict[str, str]]:
        """Get schema of an existing table/file."""
        ctx = get_logging_context().with_context(engine="spark")

        try:
            if table:
                if self.spark.catalog.tableExists(table):
                    schema = self.get_schema(self.spark.table(table))
                    ctx.debug(f"Retrieved schema for table: {table}", columns=len(schema))
                    return schema
            elif path:
                full_path = connection.get_path(path)
                if format == "delta":
                    from delta.tables import DeltaTable

                    if DeltaTable.isDeltaTable(self.spark, full_path):
                        schema = self.get_schema(DeltaTable.forPath(self.spark, full_path).toDF())
                        ctx.debug(f"Retrieved Delta schema: {path}", columns=len(schema))
                        return schema
                elif format == "parquet":
                    schema = self.get_schema(self.spark.read.parquet(full_path))
                    ctx.debug(f"Retrieved Parquet schema: {path}", columns=len(schema))
                    return schema
                elif format:
                    schema = self.get_schema(self.spark.read.format(format).load(full_path))
                    ctx.debug(f"Retrieved schema: {path}", format=format, columns=len(schema))
                    return schema
        except Exception as e:
            ctx.warning(
                "Failed to get schema",
                table=table,
                path=path,
                error_message=str(e),
            )
        return None

    def vacuum_delta(
        self,
        connection: Any,
        path: str,
        retention_hours: int = 168,
    ) -> None:
        """VACUUM a Delta table to remove old files."""
        ctx = get_logging_context().with_context(engine="spark")
        start_time = time.time()

        ctx.debug(
            "Starting Delta VACUUM",
            path=path,
            retention_hours=retention_hours,
        )

        try:
            from delta.tables import DeltaTable
        except ImportError:
            ctx.error("Delta Lake support requires 'delta-spark'")
            raise ImportError(
                "Delta Lake support requires 'pip install odibi[spark]' "
                "with delta-spark. "
                "See README.md for installation instructions."
            )

        full_path = connection.get_path(path)

        try:
            delta_table = DeltaTable.forPath(self.spark, full_path)
            delta_table.vacuum(retention_hours / 24.0)

            elapsed = (time.time() - start_time) * 1000
            ctx.info(
                "Delta VACUUM completed",
                path=path,
                retention_hours=retention_hours,
                elapsed_ms=round(elapsed, 2),
            )

        except Exception as e:
            elapsed = (time.time() - start_time) * 1000
            ctx.error(
                "Delta VACUUM failed",
                path=path,
                error_type=type(e).__name__,
                error_message=str(e),
                elapsed_ms=round(elapsed, 2),
            )
            raise

    def get_delta_history(
        self, connection: Any, path: str, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get Delta table history."""
        ctx = get_logging_context().with_context(engine="spark")
        start_time = time.time()

        ctx.debug("Fetching Delta history", path=path, limit=limit)

        try:
            from delta.tables import DeltaTable
        except ImportError:
            ctx.error("Delta Lake support requires 'delta-spark'")
            raise ImportError(
                "Delta Lake support requires 'pip install odibi[spark]' "
                "with delta-spark. "
                "See README.md for installation instructions."
            )

        full_path = connection.get_path(path)

        try:
            delta_table = DeltaTable.forPath(self.spark, full_path)
            history_df = delta_table.history(limit) if limit else delta_table.history()
            history = [row.asDict() for row in history_df.collect()]

            elapsed = (time.time() - start_time) * 1000
            ctx.info(
                "Delta history retrieved",
                path=path,
                versions_returned=len(history),
                elapsed_ms=round(elapsed, 2),
            )

            return history

        except Exception as e:
            elapsed = (time.time() - start_time) * 1000
            ctx.error(
                "Failed to get Delta history",
                path=path,
                error_type=type(e).__name__,
                error_message=str(e),
                elapsed_ms=round(elapsed, 2),
            )
            raise

    def restore_delta(self, connection: Any, path: str, version: int) -> None:
        """Restore Delta table to a specific version."""
        ctx = get_logging_context().with_context(engine="spark")
        start_time = time.time()

        ctx.debug("Restoring Delta table", path=path, version=version)

        try:
            from delta.tables import DeltaTable
        except ImportError:
            ctx.error("Delta Lake support requires 'delta-spark'")
            raise ImportError(
                "Delta Lake support requires 'pip install odibi[spark]' "
                "with delta-spark. "
                "See README.md for installation instructions."
            )

        full_path = connection.get_path(path)

        try:
            delta_table = DeltaTable.forPath(self.spark, full_path)
            delta_table.restoreToVersion(version)

            elapsed = (time.time() - start_time) * 1000
            ctx.info(
                "Delta table restored",
                path=path,
                version=version,
                elapsed_ms=round(elapsed, 2),
            )

        except Exception as e:
            elapsed = (time.time() - start_time) * 1000
            ctx.error(
                "Delta restore failed",
                path=path,
                version=version,
                error_type=type(e).__name__,
                error_message=str(e),
                elapsed_ms=round(elapsed, 2),
            )
            raise

    def maintain_table(
        self,
        connection: Any,
        format: str,
        table: Optional[str] = None,
        path: Optional[str] = None,
        config: Optional[Any] = None,
    ) -> None:
        """Run table maintenance operations (optimize, vacuum)."""
        if format != "delta" or not config or not config.enabled:
            return

        ctx = get_logging_context().with_context(engine="spark")
        start_time = time.time()

        if table:
            target = table
        elif path:
            full_path = connection.get_path(path)
            target = f"delta.`{full_path}`"
        else:
            return

        ctx.debug("Starting table maintenance", target=target)

        try:
            ctx.debug(f"Running OPTIMIZE on {target}")
            self.spark.sql(f"OPTIMIZE {target}")

            retention = config.vacuum_retention_hours
            if retention is not None and retention > 0:
                ctx.debug(f"Running VACUUM on {target}", retention_hours=retention)
                self.spark.sql(f"VACUUM {target} RETAIN {retention} HOURS")

            elapsed = (time.time() - start_time) * 1000
            ctx.info(
                "Table maintenance completed",
                target=target,
                vacuum_retention_hours=retention,
                elapsed_ms=round(elapsed, 2),
            )

        except Exception as e:
            elapsed = (time.time() - start_time) * 1000
            ctx.warning(
                f"Auto-optimize failed for {target}",
                error_type=type(e).__name__,
                error_message=str(e),
                elapsed_ms=round(elapsed, 2),
            )

    def get_source_files(self, df) -> List[str]:
        """Get list of source files that generated this DataFrame."""
        try:
            return df.inputFiles()
        except Exception:
            return []

    def profile_nulls(self, df) -> Dict[str, float]:
        """Calculate null percentage for each column."""
        from pyspark.sql.functions import col, mean, when

        aggs = []
        for c in df.columns:
            aggs.append(mean(when(col(c).isNull(), 1).otherwise(0)).alias(c))

        if not aggs:
            return {}

        try:
            result = df.select(*aggs).collect()[0].asDict()
            return result
        except Exception:
            return {}

    def filter_greater_than(self, df, column: str, value: Any) -> Any:
        """Filter DataFrame where column > value.

        Automatically casts string columns to timestamp for proper comparison.
        Tries multiple date formats including Oracle-style (DD-MON-YY).
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType

        col_type = df.schema[column].dataType
        if isinstance(col_type, StringType):
            ts_col = self._parse_string_to_timestamp(F.col(column))
            return df.filter(ts_col > value)
        return df.filter(F.col(column) > value)

    def _parse_string_to_timestamp(self, col):
        """Parse string column to timestamp, trying multiple formats.

        Supports:
        - ISO format: 2024-04-20 07:11:01
        - Oracle format: 20-APR-24 07:11:01.0 (handles uppercase months)
        """
        from pyspark.sql import functions as F

        result = F.to_timestamp(col)

        result = F.coalesce(result, F.to_timestamp(col, "yyyy-MM-dd HH:mm:ss"))
        result = F.coalesce(result, F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss"))
        result = F.coalesce(result, F.to_timestamp(col, "MM/dd/yyyy HH:mm:ss"))

        col_oracle = F.concat(
            F.substring(col, 1, 3),
            F.upper(F.substring(col, 4, 1)),
            F.lower(F.substring(col, 5, 2)),
            F.substring(col, 7, 100),
        )
        result = F.coalesce(result, F.to_timestamp(col_oracle, "dd-MMM-yy HH:mm:ss.S"))
        result = F.coalesce(result, F.to_timestamp(col_oracle, "dd-MMM-yy HH:mm:ss"))

        return result

    def filter_coalesce(self, df, col1: str, col2: str, op: str, value: Any) -> Any:
        """Filter using COALESCE(col1, col2) op value.

        Automatically casts string columns to timestamp for proper comparison.
        Tries multiple date formats including Oracle-style (DD-MON-YY).
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType

        col1_type = df.schema[col1].dataType
        col2_type = df.schema[col2].dataType

        if isinstance(col1_type, StringType):
            c1 = self._parse_string_to_timestamp(F.col(col1))
        else:
            c1 = F.col(col1)

        if isinstance(col2_type, StringType):
            c2 = self._parse_string_to_timestamp(F.col(col2))
        else:
            c2 = F.col(col2)

        coalesced = F.coalesce(c1, c2)

        if op == ">":
            return df.filter(coalesced > value)
        elif op == ">=":
            return df.filter(coalesced >= value)
        elif op == "<":
            return df.filter(coalesced < value)
        elif op == "<=":
            return df.filter(coalesced <= value)
        elif op == "=":
            return df.filter(coalesced == value)
        else:
            return df.filter(f"COALESCE({col1}, {col2}) {op} '{value}'")

    def add_write_metadata(
        self,
        df: Any,
        metadata_config: Any,
        source_connection: Optional[str] = None,
        source_table: Optional[str] = None,
        source_path: Optional[str] = None,
        is_file_source: bool = False,
    ) -> Any:
        """Add metadata columns to DataFrame before writing (Bronze layer lineage).

        Args:
            df: Spark DataFrame
            metadata_config: WriteMetadataConfig or True (for all defaults)
            source_connection: Name of the source connection
            source_table: Name of the source table (SQL sources)
            source_path: Path of the source file (file sources)
            is_file_source: True if source is a file-based read

        Returns:
            DataFrame with metadata columns added
        """
        from pyspark.sql import functions as F

        from odibi.config import WriteMetadataConfig

        if metadata_config is True:
            config = WriteMetadataConfig()
        elif isinstance(metadata_config, WriteMetadataConfig):
            config = metadata_config
        else:
            return df

        if config.extracted_at:
            df = df.withColumn("_extracted_at", F.current_timestamp())

        if config.source_file and is_file_source and source_path:
            df = df.withColumn("_source_file", F.lit(source_path))

        if config.source_connection and source_connection:
            df = df.withColumn("_source_connection", F.lit(source_connection))

        if config.source_table and source_table:
            df = df.withColumn("_source_table", F.lit(source_table))

        return df
