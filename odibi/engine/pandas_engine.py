"""Pandas engine implementation."""

from typing import Any, Dict, List, Optional
import pandas as pd
from pathlib import Path
from urllib.parse import urlparse

from odibi.engine.base import Engine
from odibi.context import Context, PandasContext
from odibi.exceptions import TransformError


class PandasEngine(Engine):
    """Pandas-based execution engine."""

    def __init__(self):
        """Initialize Pandas engine."""
        pass

    def _merge_storage_options(
        self, connection: Any, options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Merge connection storage options with user options.

        Args:
            connection: Connection object (may have pandas_storage_options method)
            options: User-provided options

        Returns:
            Merged options dictionary
        """
        options = options or {}

        # If connection provides storage_options (e.g., AzureADLS), merge them
        if hasattr(connection, "pandas_storage_options"):
            conn_storage_opts = connection.pandas_storage_options()
            user_storage_opts = options.get("storage_options", {})

            # User options override connection options
            merged_storage_opts = {**conn_storage_opts, **user_storage_opts}

            # Return options with merged storage_options
            return {**options, "storage_options": merged_storage_opts}

        return options

    def read(
        self,
        connection: Any,
        format: str,
        table: Optional[str] = None,
        path: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """Read data using Pandas.

        Args:
            connection: Connection object (with get_path method)
            format: Data format
            table: Table name
            path: File path
            options: Format-specific options

        Returns:
            Pandas DataFrame
        """
        options = options or {}

        # Get full path from connection
        if path:
            full_path = connection.get_path(path)
        elif table:
            full_path = connection.get_path(table)
        else:
            raise ValueError("Either path or table must be provided")

        # Merge storage options for cloud connections
        merged_options = self._merge_storage_options(connection, options)

        # Read based on format
        if format == "csv":
            return pd.read_csv(full_path, **merged_options)
        elif format == "parquet":
            return pd.read_parquet(full_path, **merged_options)
        elif format == "json":
            return pd.read_json(full_path, **merged_options)
        elif format == "excel":
            return pd.read_excel(full_path, **merged_options)
        elif format == "delta":
            try:
                from deltalake import DeltaTable
            except ImportError:
                raise ImportError(
                    "Delta Lake support requires 'pip install odibi[pandas]' or 'pip install deltalake'. "
                    "See README.md for installation instructions."
                )

            # Merge storage options for cloud connections
            storage_opts = merged_options.get("storage_options", {})

            # Handle version parameter for time travel
            version = merged_options.get("versionAsOf")

            # Read Delta table
            dt = DeltaTable(full_path, storage_options=storage_opts, version=version)
            return dt.to_pandas()
        elif format == "avro":
            try:
                import fastavro
            except ImportError:
                raise ImportError(
                    "Avro support requires 'pip install odibi[pandas]' or 'pip install fastavro'. "
                    "See README.md for installation instructions."
                )

            # Use fsspec for remote URIs (abfss://, s3://, etc.)
            parsed = urlparse(full_path)
            if parsed.scheme and parsed.scheme not in ["file", ""]:
                # Remote file - use fsspec
                import fsspec

                storage_opts = merged_options.get("storage_options", {})
                with fsspec.open(full_path, "rb", **storage_opts) as f:
                    reader = fastavro.reader(f)
                    records = [record for record in reader]
                return pd.DataFrame(records)
            else:
                # Local file - use standard open
                with open(full_path, "rb") as f:
                    reader = fastavro.reader(f)
                    records = [record for record in reader]
                return pd.DataFrame(records)
        else:
            raise ValueError(f"Unsupported format for Pandas engine: {format}")

    def write(
        self,
        df: pd.DataFrame,
        connection: Any,
        format: str,
        table: Optional[str] = None,
        path: Optional[str] = None,
        mode: str = "overwrite",
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Write data using Pandas.

        Args:
            df: DataFrame to write
            connection: Connection object
            format: Output format
            table: Table name
            path: File path
            mode: Write mode
            options: Format-specific options
        """
        options = options or {}

        # Get full path from connection
        if path:
            full_path = connection.get_path(path)
        elif table:
            full_path = connection.get_path(table)
        else:
            raise ValueError("Either path or table must be provided")

        # Merge storage options for cloud connections
        merged_options = self._merge_storage_options(connection, options)

        # Only create local directories (skip for remote URIs like abfss://, s3://)
        parsed = urlparse(full_path)
        if not parsed.scheme or parsed.scheme == "file":
            Path(full_path).parent.mkdir(parents=True, exist_ok=True)

        # Write based on format
        if format == "csv":
            mode_param = "w" if mode == "overwrite" else "a"
            df.to_csv(full_path, mode=mode_param, index=False, **merged_options)
        elif format == "parquet":
            # Parquet doesn't support append easily
            df.to_parquet(full_path, index=False, **merged_options)
        elif format == "json":
            mode_param = "w" if mode == "overwrite" else "a"
            df.to_json(full_path, orient="records", **merged_options)
        elif format == "excel":
            df.to_excel(full_path, index=False, **merged_options)
        elif format == "delta":
            try:
                from deltalake import write_deltalake
            except ImportError:
                raise ImportError(
                    "Delta Lake support requires 'pip install odibi[pandas]' or 'pip install deltalake'. "
                    "See README.md for installation instructions."
                )

            import warnings

            # Merge storage options for cloud connections
            storage_opts = merged_options.get("storage_options", {})

            # Get partition columns if specified
            partition_by = merged_options.get("partition_by")

            # Warn about partitioning anti-patterns
            if partition_by:
                warnings.warn(
                    "⚠️  Partitioning can cause performance issues if misused. "
                    "Only partition on low-cardinality columns (< 1000 unique values) "
                    "and ensure each partition has > 1000 rows.",
                    UserWarning,
                )

            # Convert mode to Delta mode
            delta_mode = "overwrite" if mode == "overwrite" else "append"

            # Write Delta table
            write_deltalake(
                full_path,
                df,
                mode=delta_mode,
                partition_by=partition_by,
                storage_options=storage_opts,
            )
        elif format == "avro":
            try:
                import fastavro
            except ImportError:
                raise ImportError(
                    "Avro support requires 'pip install odibi[pandas]' or 'pip install fastavro'. "
                    "See README.md for installation instructions."
                )

            records = df.to_dict("records")
            schema = self._infer_avro_schema(df)

            # Use fsspec for remote URIs (abfss://, s3://, etc.)
            parsed = urlparse(full_path)
            if parsed.scheme and parsed.scheme not in ["file", ""]:
                # Remote file - use fsspec
                import fsspec

                storage_opts = merged_options.get("storage_options", {})
                write_mode = "wb" if mode == "overwrite" else "ab"
                with fsspec.open(full_path, write_mode, **storage_opts) as f:
                    fastavro.writer(f, schema, records)
            else:
                # Local file - use standard open
                write_mode = "wb" if mode == "overwrite" else "ab"
                with open(full_path, write_mode) as f:
                    fastavro.writer(f, schema, records)
        else:
            raise ValueError(f"Unsupported format for Pandas engine: {format}")

    def execute_sql(self, sql: str, context: Context) -> pd.DataFrame:
        """Execute SQL query using DuckDB (if available) or pandasql.

        Args:
            sql: SQL query string
            context: Execution context

        Returns:
            Result DataFrame
        """
        if not isinstance(context, PandasContext):
            raise TypeError("PandasEngine requires PandasContext")

        # Try to use DuckDB for SQL
        try:
            import duckdb

            # Create in-memory database
            conn = duckdb.connect(":memory:")

            # Register all DataFrames from context
            for name in context.list_names():
                df = context.get(name)
                conn.register(name, df)

            # Execute query
            result = conn.execute(sql).df()
            conn.close()

            return result

        except ImportError:
            # Fallback: try pandasql
            try:
                from pandasql import sqldf

                # Build local namespace with DataFrames
                locals_dict = {name: context.get(name) for name in context.list_names()}

                return sqldf(sql, locals_dict)

            except ImportError:
                raise TransformError(
                    "SQL execution requires 'duckdb' or 'pandasql'. "
                    "Install with: pip install duckdb"
                )

    def execute_operation(
        self, operation: str, params: Dict[str, Any], df: pd.DataFrame
    ) -> pd.DataFrame:
        """Execute built-in operation.

        Args:
            operation: Operation name
            params: Operation parameters
            df: Input DataFrame

        Returns:
            Result DataFrame
        """
        if operation == "pivot":
            return self._pivot(df, params)
        else:
            raise ValueError(f"Unsupported operation: {operation}")

    def _pivot(self, df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
        """Execute pivot operation.

        Args:
            df: Input DataFrame
            params: Pivot parameters

        Returns:
            Pivoted DataFrame
        """
        group_by = params.get("group_by", [])
        pivot_column = params["pivot_column"]
        value_column = params["value_column"]
        agg_func = params.get("agg_func", "first")

        result = df.pivot_table(
            index=group_by, columns=pivot_column, values=value_column, aggfunc=agg_func
        ).reset_index()

        # Flatten column names if multi-level
        if isinstance(result.columns, pd.MultiIndex):
            result.columns = ["_".join(col).strip("_") for col in result.columns.values]

        return result

    def get_schema(self, df: pd.DataFrame) -> List[str]:
        """Get DataFrame column names.

        Args:
            df: DataFrame

        Returns:
            List of column names
        """
        return df.columns.tolist()

    def get_shape(self, df: pd.DataFrame) -> tuple:
        """Get DataFrame shape.

        Args:
            df: DataFrame

        Returns:
            (rows, columns)
        """
        return df.shape

    def count_rows(self, df: pd.DataFrame) -> int:
        """Count rows in DataFrame.

        Args:
            df: DataFrame

        Returns:
            Row count
        """
        return len(df)

    def count_nulls(self, df: pd.DataFrame, columns: List[str]) -> Dict[str, int]:
        """Count nulls in specified columns.

        Args:
            df: DataFrame
            columns: Columns to check

        Returns:
            Dictionary of column -> null count
        """
        null_counts = {}
        for col in columns:
            if col in df.columns:
                null_counts[col] = int(df[col].isna().sum())
            else:
                raise ValueError(f"Column '{col}' not found in DataFrame")
        return null_counts

    def validate_schema(self, df: pd.DataFrame, schema_rules: Dict[str, Any]) -> List[str]:
        """Validate DataFrame schema.

        Args:
            df: DataFrame
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
            type_map = {
                "int": ["int64", "int32", "int16", "int8"],
                "float": ["float64", "float32"],
                "str": ["object", "string"],
                "bool": ["bool"],
            }

            for col, expected_type in schema_rules["types"].items():
                if col not in df.columns:
                    failures.append(f"Column '{col}' not found for type validation")
                    continue

                actual_type = str(df[col].dtype)
                expected_dtypes = type_map.get(expected_type, [expected_type])

                if actual_type not in expected_dtypes:
                    failures.append(
                        f"Column '{col}' has type '{actual_type}', expected '{expected_type}'"
                    )

        return failures

    def _infer_avro_schema(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Infer Avro schema from pandas DataFrame.

        Args:
            df: DataFrame to infer schema from

        Returns:
            Avro schema dictionary
        """
        type_mapping = {
            "int64": "long",
            "int32": "int",
            "float64": "double",
            "float32": "float",
            "bool": "boolean",
            "object": "string",
            "string": "string",
        }

        fields = []
        for col in df.columns:
            dtype_str = str(df[col].dtype)
            avro_type = type_mapping.get(dtype_str, "string")

            # Handle nullable columns
            if df[col].isnull().any():
                avro_type = ["null", avro_type]

            fields.append({"name": col, "type": avro_type})

        return {"type": "record", "name": "DataFrame", "fields": fields}

    def vacuum_delta(
        self,
        connection: Any,
        path: str,
        retention_hours: int = 168,
        dry_run: bool = False,
        enforce_retention_duration: bool = True,
    ) -> Dict[str, Any]:
        """VACUUM a Delta table to remove old files.

        Args:
            connection: Connection object
            path: Delta table path
            retention_hours: Retention period (default 168 = 7 days)
            dry_run: If True, only show files to be deleted
            enforce_retention_duration: If False, allows retention < 168 hours (testing only)

        Returns:
            Dictionary with files_deleted count
        """
        try:
            from deltalake import DeltaTable
        except ImportError:
            raise ImportError(
                "Delta Lake support requires 'pip install odibi[pandas]' or 'pip install deltalake'. "
                "See README.md for installation instructions."
            )

        full_path = connection.get_path(path)

        # Get storage options if connection provides them
        storage_opts = {}
        if hasattr(connection, "pandas_storage_options"):
            storage_opts = connection.pandas_storage_options()

        dt = DeltaTable(full_path, storage_options=storage_opts)
        deleted_files = dt.vacuum(
            retention_hours=retention_hours,
            dry_run=dry_run,
            enforce_retention_duration=enforce_retention_duration,
        )

        return {"files_deleted": len(deleted_files)}

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
            from deltalake import DeltaTable
        except ImportError:
            raise ImportError(
                "Delta Lake support requires 'pip install odibi[pandas]' or 'pip install deltalake'. "
                "See README.md for installation instructions."
            )

        full_path = connection.get_path(path)

        # Get storage options if connection provides them
        storage_opts = {}
        if hasattr(connection, "pandas_storage_options"):
            storage_opts = connection.pandas_storage_options()

        dt = DeltaTable(full_path, storage_options=storage_opts)
        history = dt.history(limit=limit)

        return history

    def restore_delta(self, connection: Any, path: str, version: int) -> None:
        """Restore Delta table to a specific version.

        Args:
            connection: Connection object
            path: Delta table path
            version: Version number to restore to
        """
        try:
            from deltalake import DeltaTable
        except ImportError:
            raise ImportError(
                "Delta Lake support requires 'pip install odibi[pandas]' or 'pip install deltalake'. "
                "See README.md for installation instructions."
            )

        full_path = connection.get_path(path)

        # Get storage options if connection provides them
        storage_opts = {}
        if hasattr(connection, "pandas_storage_options"):
            storage_opts = connection.pandas_storage_options()

        dt = DeltaTable(full_path, storage_options=storage_opts)
        dt.restore(version)
