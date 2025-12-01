"""Spark execution engine (Phase 2B: Delta Lake support).

Status: Phase 2B implemented - Delta Lake read/write, VACUUM, history, restore
"""

from typing import Any, Dict, List, Optional, Tuple

from odibi.exceptions import TransformError

from .base import Engine


class SparkEngine(Engine):
    """Spark execution engine with PySpark backend.

    Phase 2A: Basic read/write + ADLS multi-account support
    Phase 2B: Delta Lake support
    """

    name = "spark"

    def __init__(
        self,
        connections: Optional[Dict[str, Any]] = None,
        spark_session=None,
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
        try:
            from pyspark.sql import SparkSession
        except ImportError as e:
            raise ImportError(
                "Spark support requires 'pip install odibi[spark]'. "
                "See docs/setup_databricks.md for setup instructions."
            ) from e

        # Configure Delta Lake support
        try:
            from delta import configure_spark_with_delta_pip

            builder = SparkSession.builder.appName("odibi").config(
                "spark.sql.sources.partitionOverwriteMode", "dynamic"
            )

            # Performance Optimizations
            # 1. Enable Arrow for PySpark (faster conversion to/from Pandas)
            builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")
            # 2. Enable Adaptive Query Execution (usually default in 3.x, but ensuring it)
            builder = builder.config("spark.sql.adaptive.enabled", "true")

            # 3. Reduce Verbosity (Silence Py4J and Spark logs)
            builder = builder.config(
                "spark.driver.extraJavaOptions", "-Dlog4j.rootCategory=ERROR, console"
            )
            builder = builder.config(
                "spark.executor.extraJavaOptions", "-Dlog4j.rootCategory=ERROR, console"
            )

            self.spark = spark_session or configure_spark_with_delta_pip(builder).getOrCreate()

            # Programmatically set log level after creation
            self.spark.sparkContext.setLogLevel("ERROR")

        except ImportError:
            # Delta not available - use regular Spark
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

    def _configure_all_connections(self) -> None:
        """Configure Spark with all ADLS connection credentials.

        This sets all storage account keys upfront so Spark can access
        multiple accounts. Keys are scoped by account name, so no conflicts.
        """
        for conn_name, connection in self.connections.items():
            if hasattr(connection, "configure_spark"):
                connection.configure_spark(self.spark)

    def _apply_spark_config(self) -> None:
        """Apply user-defined Spark configurations from performance settings.

        Applies configs via spark.conf.set() for runtime-settable options.
        For existing sessions (e.g., Databricks), only modifiable configs take effect.

        Common runtime-safe configs:
        - spark.sql.shuffle.partitions
        - spark.sql.adaptive.enabled
        - spark.sql.adaptive.coalescePartitions.enabled
        - spark.databricks.delta.optimizeWrite.enabled
        - spark.databricks.delta.autoCompact.enabled
        """
        import logging

        logger = logging.getLogger(__name__)

        performance = self.config.get("performance", {})
        spark_config = performance.get("spark_config", {})

        if not spark_config:
            return

        for key, value in spark_config.items():
            try:
                self.spark.conf.set(key, value)
                logger.debug(f"Applied Spark config: {key}={value}")
            except Exception as e:
                logger.warning(
                    f"Failed to set Spark config '{key}': {e}. "
                    "This config may require session restart."
                )

    def _apply_table_properties(
        self, target: str, properties: Dict[str, str], is_table: bool = False
    ) -> None:
        """Apply table properties to a Delta table.

        Args:
            target: Table name or file path
            properties: Dictionary of property name -> value
            is_table: True if target is a table name, False if path

        Example properties:
            {"delta.columnMapping.mode": "name"}
        """
        if not properties:
            return

        try:
            table_ref = target if is_table else f"delta.`{target}`"

            for prop_name, prop_value in properties.items():
                sql = f"ALTER TABLE {table_ref} SET TBLPROPERTIES ('{prop_name}' = '{prop_value}')"
                self.spark.sql(sql)

        except Exception as e:
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to set table properties on {target}: {e}")

    def _optimize_delta_write(
        self, target: str, options: Dict[str, Any], is_table: bool = False
    ) -> None:
        """Run Delta Lake optimization (OPTIMIZE / ZORDER).

        Args:
            target: Table name or file path
            options: Write options containing 'optimize_write' and 'zorder_by'
            is_table: True if target is a table name, False if path
        """
        should_optimize = options.get("optimize_write", False)
        zorder_by = options.get("zorder_by")

        if not should_optimize and not zorder_by:
            return

        try:
            # Construct SQL command
            if is_table:
                sql = f"OPTIMIZE {target}"
            else:
                sql = f"OPTIMIZE delta.`{target}`"

            if zorder_by:
                if isinstance(zorder_by, str):
                    zorder_by = [zorder_by]
                # Join columns
                cols = ", ".join(zorder_by)
                sql += f" ZORDER BY ({cols})"

            self.spark.sql(sql)

        except Exception as e:
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(f"Optimization failed for {target}: {e}")

    def _get_last_delta_commit_info(
        self, target: str, is_table: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Get metadata for the most recent Delta commit.

        Args:
            target: Table name or file path
            is_table: True if target is a table name

        Returns:
            Dictionary with version, timestamp, operation, metrics
        """
        try:
            from delta.tables import DeltaTable

            if is_table:
                dt = DeltaTable.forName(self.spark, target)
            else:
                dt = DeltaTable.forPath(self.spark, target)

            # Get last commit
            last_commit = dt.history(1).collect()[0]

            # Safely access Row fields (handles PySpark Row which supports dictionary-like access OR attribute access)
            def safe_get(row, field):
                if hasattr(row, field):
                    return getattr(row, field)
                if hasattr(row, "__getitem__"):
                    try:
                        return row[field]
                    except (KeyError, ValueError):
                        return None
                return None

            return {
                "version": safe_get(last_commit, "version"),
                "timestamp": safe_get(last_commit, "timestamp"),
                "operation": safe_get(last_commit, "operation"),
                "operation_metrics": safe_get(last_commit, "operationMetrics"),
                "read_version": safe_get(last_commit, "readVersion"),
            }
        except Exception as e:
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to fetch Delta commit info for {target}: {e}")
            return None

    def harmonize_schema(self, df, target_schema: Dict[str, str], policy: Any):
        """Harmonize DataFrame schema with target schema according to policy."""
        from pyspark.sql.functions import col, lit

        from odibi.config import OnMissingColumns, OnNewColumns, SchemaMode

        target_cols = list(target_schema.keys())
        current_cols = df.columns

        missing = set(target_cols) - set(current_cols)
        new_cols = set(current_cols) - set(target_cols)

        # 1. Check Validations
        if missing and policy.on_missing_columns == OnMissingColumns.FAIL:
            raise ValueError(f"Schema Policy Violation: Missing columns {missing}")

        if new_cols and policy.on_new_columns == OnNewColumns.FAIL:
            raise ValueError(f"Schema Policy Violation: New columns {new_cols}")

        # 2. Apply Transformations
        if policy.mode == SchemaMode.EVOLVE and policy.on_new_columns == OnNewColumns.ADD_NULLABLE:
            # Evolve: Add missing cols, Keep new cols
            res = df
            for c in missing:
                res = res.withColumn(c, lit(None))
            return res
        else:
            # Enforce / Ignore New: Project to target schema
            select_exprs = []
            for c in target_cols:
                if c in current_cols:
                    select_exprs.append(col(c))
                else:
                    # Missing column, add as null
                    select_exprs.append(lit(None).alias(c))

            return df.select(*select_exprs)

    def anonymize(self, df, columns: List[str], method: str, salt: Optional[str] = None):
        """Anonymize columns using Spark functions."""
        from pyspark.sql.functions import col, concat, lit, regexp_replace, sha2

        res = df
        for c in columns:
            if c not in df.columns:
                continue

            if method == "hash":
                if salt:
                    # sha2(concat(col, salt), 256)
                    res = res.withColumn(c, sha2(concat(col(c), lit(salt)), 256))
                else:
                    res = res.withColumn(c, sha2(col(c), 256))

            elif method == "mask":
                # Mask all but last 4 characters
                # Logic: Replace any character that is followed by at least 4 characters with '*'
                # Regex: '.(?=.{4})'
                # Note: If string is shorter than 4, it won't match and won't be masked (which is usually desired behavior or we mask all)
                # Spec says "regex masking". Let's use standard pattern.
                res = res.withColumn(c, regexp_replace(col(c), ".(?=.{4})", "*"))

            elif method == "redact":
                res = res.withColumn(c, lit("[REDACTED]"))

        return res

    def get_schema(self, df) -> Dict[str, str]:
        """Get DataFrame schema with types.

        Args:
            df: Spark DataFrame

        Returns:
            Dict[str, str]: Column name -> Type string
        """
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
        options: Optional[Dict[str, Any]] = None,
        as_of_version: Optional[int] = None,
        as_of_timestamp: Optional[str] = None,
    ):
        """Read data using Spark.

        Args:
            connection: Connection object (with get_path method)
            format: Data format (csv, parquet, json, delta, sql_server)
            table: Table name
            path: File path
            streaming: Whether to read as a stream (readStream)
            options: Format-specific options (including versionAsOf for Delta time travel)
            as_of_version: Time travel version
            as_of_timestamp: Time travel timestamp

        Returns:
            Spark DataFrame (or Streaming DataFrame)
        """
        options = options or {}

        # Handle Time Travel options (Inject into options for Delta)
        if as_of_version is not None:
            options["versionAsOf"] = as_of_version
        if as_of_timestamp is not None:
            options["timestampAsOf"] = as_of_timestamp

        # SQL Server / Azure SQL Support
        if format in ["sql", "sql_server", "azure_sql"]:
            if streaming:
                raise ValueError("Streaming not supported for SQL Server / Azure SQL yet.")

            if not hasattr(connection, "get_spark_options"):
                raise ValueError(
                    f"Connection type '{type(connection).__name__}' does not support Spark SQL read"
                )

            jdbc_options = connection.get_spark_options()

            # Merge with user options (user options take precedence)
            merged_options = {**jdbc_options, **options}

            # Prioritize 'query' option if present (e.g. from Incremental read)
            if "query" in merged_options:
                # If query is present, ensure dbtable is NOT present to avoid Spark error:
                # "Both 'dbtable' and 'query' can not be specified at the same time."
                merged_options.pop("dbtable", None)
            elif table:
                merged_options["dbtable"] = table
            elif "dbtable" not in merged_options:
                raise ValueError("SQL format requires 'table' config or 'query' option")

            return self.spark.read.format("jdbc").options(**merged_options).load()

        # Read based on format
        if table:
            # Managed/External Table (Catalog)
            if streaming:
                reader = self.spark.readStream.format(format)
            else:
                reader = self.spark.read.format(format)

            # Apply options
            for key, value in options.items():
                reader = reader.option(key, value)

            df = reader.table(table)

            # Apply filter if present (Smart Read support)
            if "filter" in options:
                df = df.filter(options["filter"])

            return df

        elif path:
            # File Path
            full_path = connection.get_path(path)

            # Auto-detect encoding for CSV (Batch only)
            if not streaming and format == "csv" and options.get("auto_encoding"):
                # Create copy to not modify original options
                options = options.copy()
                options.pop("auto_encoding")

                if "encoding" not in options:
                    try:
                        # Local import to avoid circular dependency if any
                        import logging

                        from odibi.utils.encoding import detect_encoding

                        logger = logging.getLogger(__name__)

                        detected = detect_encoding(connection, path)
                        if detected:
                            options["encoding"] = detected
                            logger.info(f"Detected encoding '{detected}' for {path}")
                    except ImportError:
                        pass  # optional dependencies might be missing
                    except Exception as e:
                        import logging

                        logger = logging.getLogger(__name__)
                        logger.warning(f"Encoding detection failed for {path}: {e}")

            if streaming:
                reader = self.spark.readStream.format(format)
                # Handle schema inference for CSV/JSON in streaming (required)
                if (
                    format in ["csv", "json"]
                    and "schema" not in options
                    and "inferSchema" not in options
                ):
                    # Force inference for better UX, though usually recommended to provide schema
                    # For now, we let Spark fail or user provide it.
                    pass
            else:
                reader = self.spark.read.format(format)

            # Apply options
            for key, value in options.items():
                # Normalize header for Spark (True -> "true")
                if key == "header" and isinstance(value, bool):
                    value = str(value).lower()

                reader = reader.option(key, value)

            df = reader.load(full_path)

            # Apply filter if present (Smart Read support)
            if "filter" in options:
                df = df.filter(options["filter"])

            return df
        else:
            raise ValueError("Either path or table must be provided")

    def write(
        self,
        df,
        connection: Any,
        format: str,
        table: Optional[str] = None,
        path: Optional[str] = None,
        register_table: Optional[str] = None,
        mode: str = "overwrite",
        options: Optional[Dict[str, Any]] = None,
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

        Returns:
            Optional dictionary containing Delta commit metadata (if format=delta)
        """
        options = options or {}

        # SQL Server / Azure SQL Support
        if format in ["sql", "sql_server", "azure_sql"]:
            # ... existing SQL logic ...
            if not hasattr(connection, "get_spark_options"):
                raise ValueError(
                    f"Connection type '{type(connection).__name__}' does not support Spark SQL write"
                )

            jdbc_options = connection.get_spark_options()
            merged_options = {**jdbc_options, **options}

            if table:
                merged_options["dbtable"] = table
            elif "dbtable" not in merged_options:
                raise ValueError("SQL format requires 'table' config or 'dbtable' option")

            # Map mode
            if mode not in ["overwrite", "append", "ignore", "error"]:
                if mode == "fail":
                    mode = "error"
                else:
                    raise ValueError(f"Write mode '{mode}' not supported for Spark SQL write")

            df.write.format("jdbc").options(**merged_options).mode(mode).save()
            return

        # Handle Upsert/AppendOnce (Delta Only)
        if mode in ["upsert", "append_once"]:
            if format != "delta":
                raise NotImplementedError(
                    f"Mode '{mode}' only supported for Delta format in Spark engine."
                )

            keys = options.get("keys")
            if not keys:
                raise ValueError(f"Mode '{mode}' requires 'keys' list in options")

            if isinstance(keys, str):
                keys = [keys]

            # Check if target exists
            exists = self.table_exists(connection, table, path)

            if not exists:
                # Fallback to overwrite (creation)
                mode = "overwrite"
            else:
                # Perform Merge
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

                # Build condition: target.key = source.key
                condition = " AND ".join([f"target.`{k}` = source.`{k}`" for k in keys])

                merge_builder = target_dt.alias("target").merge(df.alias("source"), condition)

                if mode == "upsert":
                    merge_builder.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                elif mode == "append_once":
                    merge_builder.whenNotMatchedInsertAll().execute()

                # Optimization & Metadata
                self._optimize_delta_write(target_name, options, is_table=is_table_target)
                return self._get_last_delta_commit_info(target_name, is_table=is_table_target)

        # Get output location
        if table:
            # Managed/External Table (Catalog)
            writer = df.write.format(format).mode(mode)

            partition_by = options.get("partition_by")
            # Apply partitioning if specified
            if partition_by:
                if isinstance(partition_by, str):
                    partition_by = [partition_by]
                writer = writer.partitionBy(*partition_by)

            # Apply other options
            for key, value in options.items():
                writer = writer.option(key, value)

            writer.saveAsTable(table)

            if format == "delta":
                self._optimize_delta_write(table, options, is_table=True)
                return self._get_last_delta_commit_info(table, is_table=True)
            return None

        elif path:
            full_path = connection.get_path(path)
        else:
            raise ValueError("Either path or table must be provided")

        # Extract partition_by option
        partition_by = options.pop("partition_by", None) or options.pop("partitionBy", None)

        # Extract cluster_by option (Liquid Clustering)
        cluster_by = options.pop("cluster_by", None)

        # Warn about partitioning anti-patterns
        if partition_by and cluster_by:
            import warnings

            warnings.warn(
                "⚠️  Conflict: Both 'partition_by' and 'cluster_by' (Liquid Clustering) are set. "
                "Liquid Clustering supersedes partitioning. 'partition_by' will be ignored "
                "if the table is being created now.",
                UserWarning,
            )

        elif partition_by:
            import warnings

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
                raise ImportError("Delta Lake support requires 'delta-spark'")

            if "keys" not in options:
                raise ValueError(f"Mode '{mode}' requires 'keys' list in options")

            # Check if table exists
            if DeltaTable.isDeltaTable(self.spark, full_path):
                delta_table = DeltaTable.forPath(self.spark, full_path)
                keys = options["keys"]
                if isinstance(keys, str):
                    keys = [keys]

                condition = " AND ".join([f"target.{k} = source.{k}" for k in keys])

                merger = delta_table.alias("target").merge(df.alias("source"), condition)

                if mode == "upsert":
                    merger.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                else:  # append_once
                    merger.whenNotMatchedInsertAll().execute()

                # Register if requested (even after merge)
                if register_table:
                    try:
                        self.spark.sql(
                            f"CREATE TABLE IF NOT EXISTS {register_table} USING DELTA LOCATION '{full_path}'"
                        )
                    except Exception as e:
                        import logging

                        logger = logging.getLogger(__name__)
                        logger.error(f"Failed to register external table '{register_table}': {e}")
                        # Don't raise, as data write was successful

                self._optimize_delta_write(full_path, options, is_table=False)
                return self._get_last_delta_commit_info(full_path, is_table=False)
            else:
                # Table does not exist, fall back to standard write (create)
                # Usually initial write is overwrite/create
                mode = "overwrite"

        # Write based on format (Path-based)

        # Handle Liquid Clustering (New Table Creation via SQL)
        # We only do this if we are creating a new table (overwrite or append-to-non-existent)
        # For Delta, CTAS (Create Table As Select) is the most reliable way to set CLUSTER BY
        if format == "delta" and cluster_by:
            # Check if we should use CTAS
            # If table exists and mode is append, we just append (clustering is already set)
            # If table exists and mode is overwrite, we REPLACE TABLE ... CLUSTER BY

            should_create = False
            target_name = None

            if table:
                target_name = table
                if mode == "overwrite":
                    should_create = True
                elif mode == "append":
                    # Check existence
                    if not self.spark.catalog.tableExists(table):
                        should_create = True
            elif path:
                full_path = connection.get_path(path)
                target_name = f"delta.`{full_path}`"
                if mode == "overwrite":
                    should_create = True
                elif mode == "append":
                    # Check existence via DeltaTable
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

                sql = f"{create_cmd} {target_name} USING DELTA CLUSTER BY ({cols}) AS SELECT * FROM {temp_view}"

                self.spark.sql(sql)
                self.spark.catalog.dropTempView(temp_view)

                # Register as External Table if requested (Path-based)
                if register_table and path:
                    try:
                        self.spark.sql(
                            f"CREATE TABLE IF NOT EXISTS {register_table} USING DELTA LOCATION '{full_path}'"
                        )
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

        # Extract table_properties from options (don't pass to writer.option)
        table_properties = options.pop("table_properties", None)

        # For column mapping and other properties that must be set BEFORE write,
        # temporarily set Spark session defaults
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

        writer = df.write.format(format).mode(mode)

        # Apply partitioning if specified
        if partition_by:
            if isinstance(partition_by, str):
                partition_by = [partition_by]
            writer = writer.partitionBy(*partition_by)

        # Apply other options
        for key, value in options.items():
            writer = writer.option(key, value)

        try:
            writer.save(full_path)
        finally:
            # Restore original Spark session configs
            for conf_key, original_value in original_configs.items():
                if original_value is None:
                    self.spark.conf.unset(conf_key)
                else:
                    self.spark.conf.set(conf_key, original_value)

        if format == "delta":
            self._optimize_delta_write(full_path, options, is_table=False)

        # Register as External Table if requested
        if register_table and format == "delta":
            try:
                print(f"[ODIBI] Registering external table '{register_table}' at '{full_path}'")
                self.spark.sql(
                    f"CREATE TABLE IF NOT EXISTS {register_table} USING DELTA LOCATION '{full_path}'"
                )
                print(f"[ODIBI] Successfully registered table '{register_table}'")
            except Exception as e:
                print(f"[ODIBI] Failed to register external table '{register_table}': {e}")
                raise RuntimeError(
                    f"Failed to register external table '{register_table}': {e}"
                ) from e
        else:
            print(f"[ODIBI] Skipping register_table: register_table={register_table}, format={format}")

        if format == "delta":
            return self._get_last_delta_commit_info(full_path, is_table=False)

        return None

    def add_write_metadata(
        self,
        df,
        metadata_config,
        source_connection: Optional[str] = None,
        source_table: Optional[str] = None,
        source_path: Optional[str] = None,
        is_file_source: bool = False,
    ):
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
        from pyspark.sql.functions import current_timestamp, input_file_name, lit

        from odibi.config import WriteMetadataConfig

        # Normalize config: True -> all defaults
        if metadata_config is True:
            config = WriteMetadataConfig()
        elif isinstance(metadata_config, WriteMetadataConfig):
            config = metadata_config
        else:
            return df  # None or invalid -> no metadata

        # _extracted_at: always applicable
        if config.extracted_at:
            df = df.withColumn("_extracted_at", current_timestamp())

        # _source_file: only for file sources
        if config.source_file and is_file_source:
            # input_file_name() returns the file path for each row
            # This only works if the DataFrame was read from files
            df = df.withColumn("_source_file", input_file_name())

        # _source_connection: all sources
        if config.source_connection and source_connection:
            df = df.withColumn("_source_connection", lit(source_connection))

        # _source_table: SQL sources only
        if config.source_table and source_table:
            df = df.withColumn("_source_table", lit(source_table))

        return df

    def execute_sql(self, sql: str, context) -> Any:
        """Execute SQL query using Spark SQL.

        Args:
            sql: SQL query string
            context: Context object (SparkContext)

        Returns:
            Result DataFrame

        Raises:
            TransformError: If SQL execution fails
        """
        # Register all DataFrames as temporary views
        # Context doesn't have .items(), use list_names() and get()
        for name in context.list_names():
            df = context.get(name)
            df.createOrReplaceTempView(name)

        try:
            return self.spark.sql(sql)
        except Exception as e:
            # Try to identify AnalysisException by name to avoid hard dependency on pyspark import
            if "AnalysisException" in type(e).__name__:
                raise TransformError(f"Spark SQL Analysis Error: {e}") from e
            if "ParseException" in type(e).__name__:
                raise TransformError(f"Spark SQL Parse Error: {e}") from e
            raise e

    def execute_transform(self, *args, **kwargs):
        raise NotImplementedError(
            "SparkEngine.execute_transform() will be implemented in Phase 2B. "
            "See PHASES.md for implementation plan."
        )

    def execute_operation(self, operation: str, params: Dict[str, Any], df) -> Any:
        """Execute built-in operation on Spark DataFrame.

        Args:
            operation: Operation name
            params: Operation parameters
            df: Spark DataFrame

        Returns:
            Transformed Spark DataFrame
        """
        params = params or {}

        if operation == "pivot":
            # Spark implementation of pivot
            # Params: group_by, pivot_column, value_column, agg_func
            group_by = params.get("group_by", [])
            pivot_column = params.get("pivot_column")
            value_column = params.get("value_column")
            agg_func = params.get("agg_func", "first")

            if not pivot_column or not value_column:
                raise ValueError("Pivot requires 'pivot_column' and 'value_column'")

            if isinstance(group_by, str):
                group_by = [group_by]

            # Simple mapping for aggregation functions
            # Spark's agg accepts dict {col: func_name}
            agg_expr = {value_column: agg_func}

            return df.groupBy(*group_by).pivot(pivot_column).agg(agg_expr)

        elif operation == "drop_duplicates":
            # Spark uses dropDuplicates(subset=...)
            subset = params.get("subset")
            if subset:
                if isinstance(subset, str):
                    subset = [subset]
                return df.dropDuplicates(subset=subset)
            return df.dropDuplicates()

        elif operation == "fillna":
            # Spark uses fillna(value, subset=...)
            # Pandas uses fillna(value=..., subset=...) or dict
            value = params.get("value")
            subset = params.get("subset")
            return df.fillna(value, subset=subset)

        elif operation == "drop":
            # Spark uses drop(*cols)
            columns = params.get("columns")
            if not columns:
                return df
            if isinstance(columns, str):
                columns = [columns]
            return df.drop(*columns)

        elif operation == "rename":
            # Spark uses withColumnRenamed(existing, new) per column
            # Params: columns={"old": "new"}
            columns = params.get("columns")
            if not columns:
                return df

            res = df
            for old_name, new_name in columns.items():
                res = res.withColumnRenamed(old_name, new_name)
            return res

        elif operation == "sort":
            # Spark uses orderBy/sort
            by = params.get("by")
            ascending = params.get("ascending", True)

            if not by:
                return df

            if isinstance(by, str):
                by = [by]

            if not ascending:
                from pyspark.sql.functions import desc

                # If multiple cols, desc applies to all? Spark API is complex here.
                # Simplification: Sort all descending if ascending=False
                sort_cols = [desc(c) for c in by]
                return df.orderBy(*sort_cols)

            return df.orderBy(*by)

        elif operation == "sample":
            # Spark uses sample(withReplacement, fraction, seed)
            # Pandas uses n=... or frac=...
            fraction = params.get("frac", 0.1)
            seed = params.get("random_state")
            with_replacement = params.get("replace", False)
            return df.sample(withReplacement=with_replacement, fraction=fraction, seed=seed)

        else:
            raise ValueError(f"Unsupported operation for Spark engine: {operation}")

    def count_nulls(self, df, columns: List[str]) -> Dict[str, int]:
        """Count nulls in specified columns.

        Args:
            df: Spark DataFrame
            columns: Columns to check

        Returns:
            Dictionary of column -> null count
        """
        from pyspark.sql.functions import col, count, when

        # Validate columns exist
        missing = set(columns) - set(df.columns)
        if missing:
            raise ValueError(f"Columns not found in DataFrame: {', '.join(missing)}")

        # Build aggregation expression
        aggs = [count(when(col(c).isNull(), c)).alias(c) for c in columns]

        # Execute single pass
        result = df.select(*aggs).collect()[0].asDict()

        return result

    def validate_schema(self, df, schema_rules: Dict[str, Any]) -> List[str]:
        """Validate DataFrame schema.

        Args:
            df: Spark DataFrame
            schema_rules: Validation rules

        Returns:
            List of validation failures
        """
        failures = []

        # Check required columns
        if "required_columns" in schema_rules:
            required = schema_rules["required_columns"]
            missing = set(required) - set(df.columns)
            if missing:
                failures.append(f"Missing required columns: {', '.join(missing)}")

        # Check column types
        if "types" in schema_rules:
            # Map common types to Spark type strings
            # Note: Spark types are like 'integer', 'string', 'double', 'boolean'
            type_map = {
                "int": ["integer", "long", "short", "byte"],
                "float": ["double", "float"],
                "str": ["string"],
                "bool": ["boolean"],
            }

            for col_name, expected_type in schema_rules["types"].items():
                if col_name not in df.columns:
                    failures.append(f"Column '{col_name}' not found for type validation")
                    continue

                # Get actual type (simple string representation)
                # e.g. 'integer', 'string', 'array<string>'
                actual_type = dict(df.dtypes)[col_name]
                expected_dtypes = type_map.get(expected_type, [expected_type])

                if actual_type not in expected_dtypes:
                    failures.append(
                        f"Column '{col_name}' has type '{actual_type}', expected '{expected_type}'"
                    )

        return failures

    def validate_data(self, df, validation_config: Any) -> List[str]:
        """Validate DataFrame against rules.

        Args:
            df: Spark DataFrame
            validation_config: ValidationConfig object

        Returns:
            List of validation failure messages
        """
        from pyspark.sql.functions import col

        failures = []

        # Check not empty
        if validation_config.not_empty:
            if df.isEmpty():
                failures.append("DataFrame is empty")

        # Check for nulls in specified columns
        if validation_config.no_nulls:
            null_counts = self.count_nulls(df, validation_config.no_nulls)
            for col_name, count in null_counts.items():
                if count > 0:
                    failures.append(f"Column '{col_name}' has {count} null values")

        # Schema validation
        if validation_config.schema_validation:
            schema_failures = self.validate_schema(df, validation_config.schema_validation)
            failures.extend(schema_failures)

        # Range validation
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

        # Allowed values validation
        if validation_config.allowed_values:
            for col_name, allowed in validation_config.allowed_values.items():
                if col_name in df.columns:
                    # Check for values not in allowed list
                    count = df.filter(~col(col_name).isin(allowed)).count()
                    if count > 0:
                        failures.append(f"Column '{col_name}' has invalid values")
                else:
                    failures.append(f"Column '{col_name}' not found for allowed values validation")

        return failures

    def get_sample(self, df, n: int = 10) -> List[Dict[str, Any]]:
        """Get sample rows as list of dictionaries.

        Args:
            df: Spark DataFrame
            n: Number of rows to return

        Returns:
            List of row dictionaries
        """
        return [row.asDict() for row in df.limit(n).collect()]

    def table_exists(
        self, connection: Any, table: Optional[str] = None, path: Optional[str] = None
    ) -> bool:
        """Check if table or location exists.

        Args:
            connection: Connection object
            table: Table name (for catalog tables)
            path: File path (for path-based Delta tables)

        Returns:
            True if table/location exists, False otherwise
        """
        if table:
            # Check catalog table
            return self.spark.catalog.tableExists(table)
        elif path:
            # Check path-based Delta table
            try:
                from delta.tables import DeltaTable

                full_path = connection.get_path(path)
                return DeltaTable.isDeltaTable(self.spark, full_path)
            except ImportError:
                # Delta not available, try simple file existence
                try:
                    full_path = connection.get_path(path)
                    return self.spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.FileSystem.get(
                        self.spark.sparkContext._jsc.hadoopConfiguration()
                    ).exists(
                        self.spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path(full_path)
                    )
                except Exception:
                    return False
            except Exception:
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
        try:
            if table:
                if self.spark.catalog.tableExists(table):
                    return self.get_schema(self.spark.table(table))
            elif path:
                full_path = connection.get_path(path)
                if format == "delta":
                    from delta.tables import DeltaTable

                    if DeltaTable.isDeltaTable(self.spark, full_path):
                        return self.get_schema(DeltaTable.forPath(self.spark, full_path).toDF())
                elif format == "parquet":
                    return self.get_schema(self.spark.read.parquet(full_path))
                elif format:
                    # Generic try
                    return self.get_schema(self.spark.read.format(format).load(full_path))
        except Exception:
            pass
        return None

    def vacuum_delta(
        self,
        connection: Any,
        path: str,
        retention_hours: int = 168,
    ) -> None:
        """VACUUM a Delta table to remove old files.

        Args:
            connection: Connection object
            path: Delta table path
            retention_hours: Retention period (default 168 = 7 days)
        """
        try:
            from delta.tables import DeltaTable
        except ImportError:
            raise ImportError(
                "Delta Lake support requires 'pip install odibi[spark]' "
                "with delta-spark. "
                "See README.md for installation instructions."
            )

        full_path = connection.get_path(path)
        delta_table = DeltaTable.forPath(self.spark, full_path)
        delta_table.vacuum(retention_hours / 24.0)  # Convert hours to days

    def get_delta_history(
        self, connection: Any, path: str, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get Delta table history.

        Args:
            connection: Connection object
            path: Delta table path
            limit: Maximum number of versions to return

        Returns:
            List of version metadata dictionaries
        """
        try:
            from delta.tables import DeltaTable
        except ImportError:
            raise ImportError(
                "Delta Lake support requires 'pip install odibi[spark]' "
                "with delta-spark. "
                "See README.md for installation instructions."
            )

        full_path = connection.get_path(path)
        delta_table = DeltaTable.forPath(self.spark, full_path)
        history_df = delta_table.history(limit) if limit else delta_table.history()

        # Convert to list of dictionaries
        return [row.asDict() for row in history_df.collect()]

    def restore_delta(self, connection: Any, path: str, version: int) -> None:
        """Restore Delta table to a specific version.

        Args:
            connection: Connection object
            path: Delta table path
            version: Version number to restore to
        """
        try:
            from delta.tables import DeltaTable
        except ImportError:
            raise ImportError(
                "Delta Lake support requires 'pip install odibi[spark]' "
                "with delta-spark. "
                "See README.md for installation instructions."
            )

        full_path = connection.get_path(path)
        delta_table = DeltaTable.forPath(self.spark, full_path)
        delta_table.restoreToVersion(version)

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

        # Determine target identifier
        if table:
            target = table
        elif path:
            full_path = connection.get_path(path)
            target = f"delta.`{full_path}`"
        else:
            return

        import logging

        logger = logging.getLogger(__name__)

        try:
            # 1. OPTIMIZE (Compaction)
            # We always run basic optimization. If Z-Order was needed, it should have been
            # configured in write options (zorder_by) which runs during write.
            # This acts as a catch-all compaction.
            logger.info(f"Running Auto-Optimize (Compaction) on {target}...")
            self.spark.sql(f"OPTIMIZE {target}")

            # 2. VACUUM (Cleanup)
            retention = config.vacuum_retention_hours
            if retention is not None and retention > 0:
                logger.info(
                    f"Running Auto-Optimize (VACUUM) on {target} (Retention: {retention}h)..."
                )
                self.spark.sql(f"VACUUM {target} RETAIN {retention} HOURS")

        except Exception as e:
            logger.warning(f"Auto-optimize failed for {target}: {e}")

    def get_source_files(self, df) -> List[str]:
        """Get list of source files that generated this DataFrame.

        Args:
            df: Spark DataFrame

        Returns:
            List of file paths
        """
        try:
            return df.inputFiles()
        except Exception:
            # inputFiles() might fail for non-file sources or complex transformations
            return []

    def profile_nulls(self, df) -> Dict[str, float]:
        """Calculate null percentage for each column.

        Args:
            df: Spark DataFrame

        Returns:
            Dictionary of {column_name: null_percentage} (0.0 to 1.0)
        """
        from pyspark.sql.functions import col, mean, when

        # Build aggregation expression for all columns in one pass
        # mean(when(col.isNull, 1).otherwise(0))
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
        """Filter DataFrame where column > value."""
        # Use SQL expression string for consistency with tests and simpler debugging
        return df.filter(f"{column} > '{value}'")

    def filter_coalesce(self, df, col1: str, col2: str, op: str, value: Any) -> Any:
        """Filter using COALESCE(col1, col2) op value."""
        # Use SQL expression string
        return df.filter(f"COALESCE({col1}, {col2}) {op} '{value}'")
