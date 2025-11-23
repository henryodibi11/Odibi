"""Spark execution engine (Phase 2B: Delta Lake support).

Status: Phase 2B implemented - Delta Lake read/write, VACUUM, history, restore
"""

from typing import Any, Dict, List, Tuple, Optional
from .base import Engine
from odibi.exceptions import TransformError


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

            self.spark = spark_session or configure_spark_with_delta_pip(builder).getOrCreate()
        except ImportError:
            # Delta not available - use regular Spark
            builder = SparkSession.builder.appName("odibi").config(
                "spark.sql.sources.partitionOverwriteMode", "dynamic"
            )

            # Performance Optimizations
            builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")
            builder = builder.config("spark.sql.adaptive.enabled", "true")

            self.spark = spark_session or builder.getOrCreate()

        self.config = config or {}
        self.connections = connections or {}

        # Configure all ADLS connections upfront
        self._configure_all_connections()

    def _configure_all_connections(self) -> None:
        """Configure Spark with all ADLS connection credentials.

        This sets all storage account keys upfront so Spark can access
        multiple accounts. Keys are scoped by account name, so no conflicts.
        """
        for conn_name, connection in self.connections.items():
            if hasattr(connection, "configure_spark"):
                connection.configure_spark(self.spark)

    def get_schema(self, df) -> List[str]:
        """Get DataFrame schema as list of column names.

        Args:
            df: Spark DataFrame

        Returns:
            List of column names
        """
        return df.columns

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
        options: Optional[Dict[str, Any]] = None,
    ):
        """Read data using Spark.

        Args:
            connection: Connection object (with get_path method)
            format: Data format (csv, parquet, json, delta, sql_server)
            table: Table name
            path: File path
            options: Format-specific options (including versionAsOf for Delta time travel)

        Returns:
            Spark DataFrame
        """
        options = options or {}

        # SQL Server / Azure SQL Support
        if format in ["sql_server", "azure_sql"]:
            if not hasattr(connection, "get_spark_options"):
                raise ValueError(
                    f"Connection type '{type(connection).__name__}' does not support Spark SQL read"
                )

            jdbc_options = connection.get_spark_options()

            # Merge with user options (user options take precedence)
            merged_options = {**jdbc_options, **options}

            if table:
                merged_options["dbtable"] = table
            elif "query" in options:
                merged_options["query"] = options["query"]
            elif "dbtable" not in merged_options:
                raise ValueError("SQL format requires 'table' config or 'query' option")

            return self.spark.read.format("jdbc").options(**merged_options).load()

        # Read based on format
        if table:
            # Managed/External Table (Catalog)
            reader = self.spark.read.format(format)

            # Apply options
            for key, value in options.items():
                reader = reader.option(key, value)

            return reader.table(table)

        elif path:
            # File Path
            full_path = connection.get_path(path)

            # Auto-detect encoding for CSV
            if format == "csv" and options.get("auto_encoding"):
                # Create copy to not modify original options
                options = options.copy()
                options.pop("auto_encoding")

                if "encoding" not in options:
                    try:
                        from odibi.utils.encoding import detect_encoding

                        # Local import to avoid circular dependency if any
                        import logging

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

            reader = self.spark.read.format(format)

            # Apply options
            for key, value in options.items():
                # Normalize header for Spark (True -> "true")
                if key == "header" and isinstance(value, bool):
                    value = str(value).lower()

                reader = reader.option(key, value)

            return reader.load(full_path)
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
    ) -> None:
        """Write data using Spark.

        Args:
            df: Spark DataFrame to write
            connection: Connection object
            format: Output format (csv, parquet, json, delta)
            table: Table name
            path: File path
            register_table: Name to register as external table (if path is used)
            mode: Write mode (overwrite, append, error, ignore)
            options: Format-specific options (including partition_by for partitioning)
        """
        options = options or {}

        # SQL Server / Azure SQL Support
        if format in ["sql_server", "azure_sql"]:
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
            return

        elif path:
            full_path = connection.get_path(path)
        else:
            raise ValueError("Either path or table must be provided")

        # Extract partition_by option
        partition_by = options.pop("partition_by", None) or options.pop("partitionBy", None)

        # Warn about partitioning anti-patterns
        if partition_by:
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
                    self.spark.sql(
                        f"CREATE TABLE IF NOT EXISTS {register_table} USING DELTA LOCATION '{full_path}'"
                    )
                return
            else:
                # Table does not exist, fall back to standard write (create)
                # Usually initial write is overwrite/create
                mode = "overwrite"

        # Write based on format (Path-based)
        writer = df.write.format(format).mode(mode)

        # Apply partitioning if specified
        if partition_by:
            if isinstance(partition_by, str):
                partition_by = [partition_by]
            writer = writer.partitionBy(*partition_by)

        # Apply other options
        for key, value in options.items():
            writer = writer.option(key, value)

        writer.save(full_path)

        # Register as External Table if requested
        if register_table and format == "delta":
            # Only Delta supports easy external registration like this usually
            # Parquet can too, but Delta is the main use case.
            try:
                self.spark.sql(
                    f"CREATE TABLE IF NOT EXISTS {register_table} USING DELTA LOCATION '{full_path}'"
                )
            except Exception as e:
                import logging

                logger = logging.getLogger(__name__)
                logger.warning(f"Failed to register external table '{register_table}': {e}")

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
                        f"Column '{col_name}' has type '{actual_type}', "
                        f"expected '{expected_type}'"
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
