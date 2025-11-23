"""Pandas engine implementation."""

from typing import Any, Dict, List, Optional, Union, Iterator
import pandas as pd
from pathlib import Path
from urllib.parse import urlparse
import glob

from odibi.engine.base import Engine
from odibi.context import Context, PandasContext
from odibi.exceptions import TransformError


class PandasEngine(Engine):
    """Pandas-based execution engine."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize Pandas engine.

        Args:
            config: Engine configuration (optional)
        """
        self.config = config or {}
        # Check for performance flags
        performance = self.config.get("performance", {})

        # Determine desired state
        if hasattr(performance, "use_arrow"):
            desired_use_arrow = performance.use_arrow
        elif isinstance(performance, dict):
            desired_use_arrow = performance.get("use_arrow", True)
        else:
            desired_use_arrow = True

        # Verify availability
        if desired_use_arrow:
            try:
                import pyarrow  # noqa: F401

                self.use_arrow = True
            except ImportError:
                import logging

                logger = logging.getLogger(__name__)
                logger.warning(
                    "Apache Arrow not found. Disabling Arrow optimizations. "
                    "Install 'pyarrow' to enable."
                )
                self.use_arrow = False
        else:
            self.use_arrow = False

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
    ) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        """Read data using Pandas.

        Args:
            connection: Connection object (with get_path method)
            format: Data format
            table: Table name
            path: File path
            options: Format-specific options (including chunksize)

        Returns:
            Pandas DataFrame or Iterator[pd.DataFrame]
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

        # Sanitize options for pandas compatibility
        if "header" in merged_options:
            # YAML 'header: true' -> Python True, but read_csv expects 0 (int) or None
            if merged_options["header"] is True:
                merged_options["header"] = 0
            # YAML 'header: false' -> Python False, but read_csv expects None
            elif merged_options["header"] is False:
                merged_options["header"] = None

        # Custom Readers
        if format in self._custom_readers:
            return self._custom_readers[format](full_path, **merged_options)

        # Handle glob patterns for local files
        is_glob = False
        if path and ("*" in path or "?" in path or "[" in path):
            parsed = urlparse(str(full_path))
            # Only expand for local files (no scheme, file://, or drive letter)
            is_local = (
                not parsed.scheme
                or parsed.scheme == "file"
                or (len(parsed.scheme) == 1 and parsed.scheme.isalpha())
            )

            if is_local:
                glob_path = str(full_path)
                if glob_path.startswith("file:///"):
                    glob_path = glob_path[8:]
                elif glob_path.startswith("file://"):
                    glob_path = glob_path[7:]

                matched_files = glob.glob(glob_path)
                if not matched_files:
                    raise FileNotFoundError(f"No files matched pattern: {glob_path}")

                full_path = matched_files
                is_glob = True

        # Prepare read options
        read_kwargs = merged_options.copy()
        if self.use_arrow:
            # Use PyArrow backend for memory efficiency and speed
            # Available in Pandas 2.0+
            read_kwargs["dtype_backend"] = "pyarrow"

        # Read based on format
        if format == "csv":
            try:
                if is_glob and isinstance(full_path, list):
                    dfs = [pd.read_csv(f, **read_kwargs) for f in full_path]
                    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
                return pd.read_csv(full_path, **read_kwargs)
            except UnicodeDecodeError:
                # Retry with common fallbacks
                # Note: Arrow engine might be stricter, so we might need to drop it for retry?
                # But let's try keeping it if possible, or fallback completely.
                # Simplify: Just update encoding in kwargs
                read_kwargs["encoding"] = "latin1"
                if is_glob and isinstance(full_path, list):
                    dfs = [pd.read_csv(f, **read_kwargs) for f in full_path]
                    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
                return pd.read_csv(full_path, **read_kwargs)
            except pd.errors.ParserError:
                # Retry with bad lines skipped
                read_kwargs["on_bad_lines"] = "skip"
                if is_glob and isinstance(full_path, list):
                    dfs = [pd.read_csv(f, **read_kwargs) for f in full_path]
                    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
                return pd.read_csv(full_path, **read_kwargs)
        elif format == "parquet":
            # read_parquet handles list of files
            return pd.read_parquet(full_path, **read_kwargs)
        elif format == "json":
            if is_glob and isinstance(full_path, list):
                dfs = [pd.read_json(f, **read_kwargs) for f in full_path]
                return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

            return pd.read_json(full_path, **read_kwargs)
        elif format == "excel":
            # Excel doesn't support dtype_backend arg directly in older versions or engine dependent
            # But Pandas 2.0 might. Let's check safely.
            # read_excel does NOT support dtype_backend as of 2.0.3 typically.
            # We skip arrow backend for Excel for now to be safe.
            excel_kwargs = merged_options.copy()
            return pd.read_excel(full_path, **excel_kwargs)
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

            if self.use_arrow:
                # Zero-copy to Arrow, then to Pandas with Arrow dtypes
                # Check if to_pandas supports arrow_options (deltalake >= 0.15.0)
                import inspect

                sig = inspect.signature(dt.to_pandas)

                if "arrow_options" in sig.parameters:
                    return dt.to_pandas(
                        partitions=None, arrow_options={"types_mapper": pd.ArrowDtype}
                    )
                else:
                    # Fallback for older deltalake versions
                    # Convert via Arrow manually to ensure pyarrow backed
                    return dt.to_pyarrow_table().to_pandas(types_mapper=pd.ArrowDtype)
            else:
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
        elif format in ["sql_server", "azure_sql"]:
            if not hasattr(connection, "read_table"):
                raise ValueError(
                    f"Connection type '{type(connection).__name__}' does not support SQL operations"
                )

            if table:
                # Extract schema from table name if present
                if "." in table:
                    schema, table_name = table.split(".", 1)
                else:
                    schema, table_name = "dbo", table

                return connection.read_table(table_name=table_name, schema=schema)
            else:
                # Check for query in options
                query = options.get("query")
                if query:
                    return connection.read_sql(query)
                raise ValueError("SQL format requires 'table' config or 'query' in options")
        else:
            raise ValueError(f"Unsupported format for Pandas engine: {format}")

    def write(
        self,
        df: Union[pd.DataFrame, Iterator[pd.DataFrame]],
        connection: Any,
        format: str,
        table: Optional[str] = None,
        path: Optional[str] = None,
        mode: str = "overwrite",
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Write data using Pandas.

        Args:
            df: DataFrame or Iterator of DataFrames to write
            connection: Connection object
            format: Output format
            table: Table name
            path: File path
            mode: Write mode
            options: Format-specific options
        """
        options = options or {}

        # Handle iterator/generator input
        from collections.abc import Iterator

        if isinstance(df, Iterator):
            first_chunk = True
            for chunk in df:
                # Determine mode for this chunk
                # First chunk uses user-provided mode, subsequent chunks append
                current_mode = mode if first_chunk else "append"

                # Determine options for this chunk
                current_options = options.copy()

                # For CSV, if appending (subsequent chunks), we generally don't want to repeat header
                # unless user explicitly handled it.
                # If user passed header=False globally, we respect it.
                # If user passed header=True (default), we only want it for first chunk.
                if not first_chunk and format == "csv":
                    # Only override if not explicitly set to False (though usually it defaults to True)
                    if current_options.get("header") is not False:
                        current_options["header"] = False

                # Write the chunk
                self.write(
                    chunk,
                    connection,
                    format,
                    table,
                    path,
                    mode=current_mode,
                    options=current_options,
                )
                first_chunk = False
            return

        # SQL Server / Azure SQL Support
        if format in ["sql_server", "azure_sql"]:
            if not hasattr(connection, "write_table"):
                raise ValueError(
                    f"Connection type '{type(connection).__name__}' does not support SQL operations"
                )

            if not table:
                raise ValueError("SQL format requires 'table' config")

            # Extract schema from table name if present
            if "." in table:
                schema, table_name = table.split(".", 1)
            else:
                schema, table_name = "dbo", table

            # Map mode to if_exists
            if_exists = "replace"  # overwrite
            if mode == "append":
                if_exists = "append"
            elif mode == "fail":
                if_exists = "fail"

            chunksize = options.get("chunksize", 1000)

            connection.write_table(
                df=df,
                table_name=table_name,
                schema=schema,
                if_exists=if_exists,
                chunksize=chunksize,
            )
            return

        # Get full path from connection
        if path:
            full_path = connection.get_path(path)
        elif table:
            full_path = connection.get_path(table)
        else:
            raise ValueError("Either path or table must be provided")

        # Merge storage options for cloud connections
        merged_options = self._merge_storage_options(connection, options)

        # Clean up custom options that are not supported by pandas writers
        # 'keys' is used for upsert/append_once logic but not by to_csv/to_parquet
        writer_options = merged_options.copy()
        writer_options.pop("keys", None)

        # Custom Writers
        if format in self._custom_writers:
            self._custom_writers[format](df, full_path, mode=mode, **writer_options)
            return

        # Only create local directories (skip for remote URIs like abfss://, s3://)
        parsed = urlparse(full_path)
        # On Windows, drive letters can be parsed as schemes (e.g. "c")
        is_windows_drive = (
            len(parsed.scheme) == 1 and parsed.scheme.isalpha() if parsed.scheme else False
        )

        if not parsed.scheme or parsed.scheme == "file" or is_windows_drive:
            Path(full_path).parent.mkdir(parents=True, exist_ok=True)

        # --- Generic Upsert/Append-Once Logic for File Formats (CSV, Parquet, JSON) ---
        # Delta Lake has its own native implementation below.
        if mode in ["upsert", "append_once"] and format != "delta":
            if "keys" not in options:
                raise ValueError(f"Mode '{mode}' requires 'keys' list in options")

            keys = options["keys"]
            if isinstance(keys, str):
                keys = [keys]

            # Try to read existing file
            # Note: This approach rewrites the entire file and is not suitable for massive datasets.
            # For massive datasets, use Delta Lake.
            existing_df = None
            try:
                # Use cleaned writer_options for reading to avoid passing 'keys' to readers
                if format == "csv":
                    existing_df = pd.read_csv(full_path, **writer_options)
                elif format == "parquet":
                    existing_df = pd.read_parquet(full_path, **writer_options)
                elif format == "json":
                    existing_df = pd.read_json(full_path, **writer_options)
                elif format == "excel":
                    existing_df = pd.read_excel(full_path, **writer_options)
            except (FileNotFoundError, Exception):
                # File likely doesn't exist (or other read error), treat as new write
                pass

            if existing_df is not None:
                # Handle Iterator input for merge (must materialize)
                if isinstance(df, Iterator):
                    df = pd.concat(df, ignore_index=True)

                if mode == "append_once":
                    # Filter out rows from 'df' that are already in 'existing_df'
                    # Left anti join logic

                    # Check if keys exist
                    missing_keys = set(keys) - set(df.columns)
                    if missing_keys:
                        raise KeyError(f"Keys {missing_keys} not found in input data")

                    missing_existing = set(keys) - set(existing_df.columns)
                    if missing_existing:
                        # Existing file schema mismatch - append anyway or fail?
                        # Safest is to fail or assume distinct. Let's append to avoid data loss,
                        # but append_once implies we want to avoid dups.
                        # If keys missing in existing, we can't check.
                        pass
                    else:
                        # Identify new rows
                        # Use merge with indicator
                        # Only check on keys
                        merged = df.merge(existing_df[keys], on=keys, how="left", indicator=True)
                        new_rows = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

                        # Update df to only be the new rows
                        # For CSV/JSON which support append, we can just append the new rows.
                        # For Parquet/Excel, we must rewrite the whole file with (Existing + New).
                        if format in ["csv", "json"]:
                            df = new_rows
                            mode = "append"
                        else:
                            # Parquet/Excel - rewrite everything
                            df = pd.concat([existing_df, new_rows], ignore_index=True)
                            mode = "overwrite"

                elif mode == "upsert":
                    # Update existing rows, insert new rows
                    # Strategy:
                    # 1. Set index to keys
                    # 2. Combine/Update
                    # 3. Reset index

                    # Check if keys exist
                    missing_keys = set(keys) - set(df.columns)
                    if missing_keys:
                        raise KeyError(f"Keys {missing_keys} not found in input data")

                    # Update existing with new values
                    # combine_first: updates nulls. We want overwrite.
                    # We can concat and drop duplicates keeping last, BUT that assumes full row match?
                    # No, we update based on key.

                    # 1. Remove rows from existing that are in input (based on keys)
                    # indicator merge to find rows to keep
                    merged_indicator = existing_df.merge(
                        df[keys], on=keys, how="left", indicator=True
                    )
                    rows_to_keep = existing_df[merged_indicator["_merge"] == "left_only"]

                    # 2. Concat rows_to_keep + input df
                    df = pd.concat([rows_to_keep, df], ignore_index=True)

                    # 3. Write mode becomes overwrite (replacing file with merged result)
                    mode = "overwrite"

        # Write based on format
        if format == "csv":
            mode_param = "w" if mode == "overwrite" else "a"
            df.to_csv(full_path, mode=mode_param, index=False, **writer_options)
        elif format == "parquet":
            # Parquet doesn't support append easily
            df.to_parquet(full_path, index=False, **writer_options)
        elif format == "json":
            mode_param = "w" if mode == "overwrite" else "a"
            df.to_json(full_path, orient="records", mode=mode_param, **writer_options)
        elif format == "excel":
            df.to_excel(full_path, index=False, **writer_options)
        elif format == "delta":
            try:
                from deltalake import write_deltalake, DeltaTable
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
            if mode == "upsert" or mode == "append_once":
                # Use merge for upsert/append_once
                if "keys" not in options:
                    raise ValueError(f"Mode '{mode}' requires 'keys' list in options")

                # Load existing table to check if it exists
                try:
                    dt = DeltaTable(full_path, storage_options=storage_opts)
                except Exception:
                    # If table doesn't exist, 'upsert' and 'append_once' act like 'write' (create new)
                    # We fall back to standard write_deltalake with mode='overwrite' (or 'error'?)
                    # Standard behavior is usually to create if not exists.
                    # Let's use write_deltalake directly for creation.
                    # print(f"DEBUG: DeltaTable init failed: {e}")
                    write_deltalake(
                        full_path,
                        df,
                        mode="overwrite",  # Initial write is always overwrite/create
                        partition_by=partition_by,
                        storage_options=storage_opts,
                    )
                    return

                # Construct merge predicate
                keys = options["keys"]
                if isinstance(keys, str):
                    keys = [keys]

                predicate = " AND ".join([f"target.{k} = source.{k}" for k in keys])

                merger = dt.merge(
                    source=df, predicate=predicate, source_alias="source", target_alias="target"
                )

                if mode == "upsert":
                    # Update existing, Insert new
                    merger.when_matched_update_all().when_not_matched_insert_all().execute()
                else:  # append_once
                    # Only Insert new (deduplicate)
                    merger.when_not_matched_insert_all().execute()

            else:
                # Standard write modes (overwrite/append)
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
        elif format in ["sql_server", "azure_sql"]:
            if not hasattr(connection, "write_table"):
                raise ValueError(
                    f"Connection type '{type(connection).__name__}' does not support SQL operations"
                )

            if not table:
                raise ValueError("SQL format requires 'table' config")

            # Extract schema from table name if present
            if "." in table:
                schema, table_name = table.split(".", 1)
            else:
                schema, table_name = "dbo", table

            # Map mode to if_exists
            if_exists = "replace"  # overwrite
            if mode == "append":
                if_exists = "append"
            elif mode == "fail":
                if_exists = "fail"

            chunksize = options.get("chunksize", 1000)

            connection.write_table(
                df=df,
                table_name=table_name,
                schema=schema,
                if_exists=if_exists,
                chunksize=chunksize,
            )
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

                # Handle chunked data (Iterator)
                from collections.abc import Iterator

                if isinstance(df, Iterator):
                    # Warning: Materializing iterator for SQL execution
                    # TODO: Investigate DuckDB streaming support for iterators
                    df = pd.concat(df, ignore_index=True)

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
                locals_dict = {}
                for name in context.list_names():
                    df = context.get(name)

                    # Handle chunked data (Iterator)
                    from collections.abc import Iterator

                    if isinstance(df, Iterator):
                        df = pd.concat(df, ignore_index=True)

                    locals_dict[name] = df

                return sqldf(sql, locals_dict)

            except ImportError:
                raise TransformError(
                    "SQL execution requires 'duckdb' or 'pandasql'. "
                    "Install with: pip install duckdb"
                )

    def execute_operation(
        self,
        operation: str,
        params: Dict[str, Any],
        df: Union[pd.DataFrame, Iterator[pd.DataFrame]],
    ) -> pd.DataFrame:
        """Execute built-in operation.

        Args:
            operation: Operation name
            params: Operation parameters
            df: Input DataFrame or Iterator

        Returns:
            Result DataFrame
        """
        # Handle chunked data (Iterator)
        from collections.abc import Iterator

        if isinstance(df, Iterator):
            # Warning: Materializing iterator for operation execution
            df = pd.concat(df, ignore_index=True)

        if operation == "pivot":
            return self._pivot(df, params)
        elif operation == "drop_duplicates":
            return df.drop_duplicates(**params)
        elif operation == "fillna":
            return df.fillna(**params)
        elif operation == "drop":
            return df.drop(**params)
        elif operation == "rename":
            return df.rename(**params)
        elif operation == "sort":
            return df.sort_values(**params)
        elif operation == "sample":
            return df.sample(**params)
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

        # Validate columns exist
        required_columns = set()
        if isinstance(group_by, list):
            required_columns.update(group_by)
        elif isinstance(group_by, str):
            required_columns.add(group_by)
            group_by = [group_by]

        required_columns.add(pivot_column)
        required_columns.add(value_column)

        missing = required_columns - set(df.columns)
        if missing:
            raise KeyError(
                f"Columns not found in DataFrame for pivot operation: {missing}. "
                f"Available: {list(df.columns)}"
            )

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

    def validate_data(self, df: pd.DataFrame, validation_config: Any) -> List[str]:
        """Validate DataFrame against rules.

        Args:
            df: DataFrame
            validation_config: ValidationConfig object

        Returns:
            List of validation failure messages
        """
        failures = []

        # Check not empty
        if validation_config.not_empty:
            if len(df) == 0:
                failures.append("DataFrame is empty")

        # Check for nulls in specified columns
        if validation_config.no_nulls:
            null_counts = self.count_nulls(df, validation_config.no_nulls)
            for col, count in null_counts.items():
                if count > 0:
                    failures.append(f"Column '{col}' has {count} null values")

        # Schema validation
        if validation_config.schema_validation:
            schema_failures = self.validate_schema(df, validation_config.schema_validation)
            failures.extend(schema_failures)

        # Range validation
        if validation_config.ranges:
            for col, bounds in validation_config.ranges.items():
                if col in df.columns:
                    min_val = bounds.get("min")
                    max_val = bounds.get("max")

                    if min_val is not None:
                        min_violations = df[df[col] < min_val]
                        if len(min_violations) > 0:
                            failures.append(f"Column '{col}' has values < {min_val}")

                    if max_val is not None:
                        max_violations = df[df[col] > max_val]
                        if len(max_violations) > 0:
                            failures.append(f"Column '{col}' has values > {max_val}")
                else:
                    failures.append(f"Column '{col}' not found for range validation")

        # Allowed values validation
        if validation_config.allowed_values:
            for col, allowed in validation_config.allowed_values.items():
                if col in df.columns:
                    # Check for values not in allowed list
                    invalid = df[~df[col].isin(allowed)]
                    if len(invalid) > 0:
                        failures.append(f"Column '{col}' has invalid values")
                else:
                    failures.append(f"Column '{col}' not found for allowed values validation")

        return failures

    def get_sample(self, df: pd.DataFrame, n: int = 10) -> List[Dict[str, Any]]:
        """Get sample rows as list of dictionaries.

        Args:
            df: DataFrame
            n: Number of rows to return

        Returns:
            List of row dictionaries
        """
        return df.head(n).to_dict("records")

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
