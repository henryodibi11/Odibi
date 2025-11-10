"""Spark execution engine (Phase 2B: Delta Lake support).

Status: Phase 2B implemented - Delta Lake read/write, VACUUM, history, restore
"""

from typing import Any, Dict, List, Tuple, Optional
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

            builder = SparkSession.builder.appName("odibi")
            self.spark = spark_session or configure_spark_with_delta_pip(builder).getOrCreate()
        except ImportError:
            # Delta not available - use regular Spark
            self.spark = spark_session or SparkSession.builder.appName("odibi").getOrCreate()

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

    def get_schema(self, df) -> List[Tuple[str, str]]:
        """Get DataFrame schema as list of (name, type) tuples."""
        return [(f.name, f.dataType.simpleString()) for f in df.schema]

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
            format: Data format (csv, parquet, json, delta)
            table: Table name
            path: File path
            options: Format-specific options (including versionAsOf for Delta time travel)

        Returns:
            Spark DataFrame
        """
        options = options or {}

        # Get full path from connection
        if path:
            full_path = connection.get_path(path)
        elif table:
            full_path = connection.get_path(table)
        else:
            raise ValueError("Either path or table must be provided")

        # Read based on format
        reader = self.spark.read.format(format)

        # Apply options
        for key, value in options.items():
            reader = reader.option(key, value)

        return reader.load(full_path)

    def write(
        self,
        df,
        connection: Any,
        format: str,
        table: Optional[str] = None,
        path: Optional[str] = None,
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
            mode: Write mode (overwrite, append, error, ignore)
            options: Format-specific options (including partition_by for partitioning)
        """
        options = options or {}

        # Get full path from connection
        if path:
            full_path = connection.get_path(path)
        elif table:
            full_path = connection.get_path(table)
        else:
            raise ValueError("Either path or table must be provided")

        # Extract partition_by option
        partition_by = options.pop("partition_by", None)

        # Warn about partitioning anti-patterns
        if partition_by:
            import warnings

            warnings.warn(
                "⚠️  Partitioning can cause performance issues if misused. "
                "Only partition on low-cardinality columns (< 1000 unique values) "
                "and ensure each partition has > 1000 rows.",
                UserWarning,
            )

        # Write based on format
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

    def execute_sql(self, sql: str, context) -> Any:
        """Execute SQL query using Spark SQL.

        Args:
            sql: SQL query string
            context: Execution context (not used for Spark, uses temp views)

        Returns:
            Result DataFrame
        """
        return self.spark.sql(sql)

    def execute_transform(self, *args, **kwargs):
        raise NotImplementedError(
            "SparkEngine.execute_transform() will be implemented in Phase 2B. "
            "See PHASES.md for implementation plan."
        )

    def execute_operation(self, *args, **kwargs):
        raise NotImplementedError(
            "SparkEngine.execute_operation() will be implemented in Phase 2B. "
            "See PHASES.md for implementation plan."
        )

    def count_nulls(self, df, columns: List[str]) -> Dict[str, int]:
        """Count nulls in specified columns (Phase 1 stub)."""
        raise NotImplementedError(
            "SparkEngine.count_nulls() will be implemented in Phase 3. "
            "See PHASES.md for implementation plan."
        )

    def validate_schema(self, df, schema_rules: Dict[str, Any]) -> List[str]:
        """Validate DataFrame schema (Phase 1 stub)."""
        raise NotImplementedError(
            "SparkEngine.validate_schema() will be implemented in Phase 3. "
            "See PHASES.md for implementation plan."
        )

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
                "Delta Lake support requires 'pip install odibi[spark]' with delta-spark. "
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
                "Delta Lake support requires 'pip install odibi[spark]' with delta-spark. "
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
                "Delta Lake support requires 'pip install odibi[spark]' with delta-spark. "
                "See README.md for installation instructions."
            )

        full_path = connection.get_path(path)
        delta_table = DeltaTable.forPath(self.spark, full_path)
        delta_table.restoreToVersion(version)
