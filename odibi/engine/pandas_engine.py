"""Pandas engine implementation."""

from typing import Any, Dict, List, Optional, Union, Iterator
import pandas as pd
from pathlib import Path
from urllib.parse import urlparse
import glob
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor


from odibi.engine.base import Engine
from odibi.context import Context, PandasContext
from odibi.exceptions import TransformError


class PandasEngine(Engine):
    """Pandas-based execution engine."""

    name = "pandas"

    def __init__(
        self,
        connections: Optional[Dict[str, Any]] = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        """Initialize Pandas engine.

        Args:
            connections: Dictionary of connection objects
            config: Engine configuration (optional)
        """
        self.connections = connections or {}
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

    def _process_df(
        self, df: Union[pd.DataFrame, Iterator[pd.DataFrame]], query: Optional[str]
    ) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        """Apply post-read processing (filtering)."""
        if query and df is not None:
            # Handle Iterator
            from collections.abc import Iterator

            if isinstance(df, Iterator):
                # Filter each chunk
                return (chunk.query(query) for chunk in df)

            if not df.empty:
                try:
                    return df.query(query)
                except Exception as e:
                    import logging

                    logger = logging.getLogger(__name__)
                    logger.warning(f"Failed to apply query '{query}': {e}")
        return df

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

    def _read_parallel(self, read_func: Any, paths: List[str], **kwargs) -> pd.DataFrame:
        """Read multiple files in parallel using threads.

        Args:
            read_func: Pandas read function (e.g. pd.read_csv)
            paths: List of file paths
            kwargs: Arguments to pass to read_func

        Returns:
            Concatenated DataFrame
        """
        # Conservative worker count to avoid OOM on large files
        max_workers = min(8, os.cpu_count() or 4)

        dfs = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # map preserves order
            results = executor.map(lambda p: read_func(p, **kwargs), paths)
            dfs = list(results)

        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def read(
        self,
        connection: Any,
        format: str,
        table: Optional[str] = None,
        path: Optional[str] = None,
        streaming: bool = False,
        options: Optional[Dict[str, Any]] = None,
    ) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        """Read data using Pandas.

        Args:
            connection: Connection object (with get_path method)
            format: Data format
            table: Table name
            path: File path
            streaming: Streaming flag (NOT SUPPORTED in Pandas)
            options: Format-specific options (including chunksize)

        Returns:
            Pandas DataFrame or Iterator[pd.DataFrame]
        """
        if streaming:
            raise ValueError(
                "Streaming is not supported in the Pandas engine. "
                "Please use 'engine: spark' for streaming pipelines."
            )

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

        # Extract 'query' option for post-read filtering (Pandas engine support for HWM pattern)
        post_read_query = read_kwargs.pop("query", None)

        if self.use_arrow:
            # Use PyArrow backend for memory efficiency and speed
            # Available in Pandas 2.0+
            read_kwargs["dtype_backend"] = "pyarrow"

        # Read based on format
        if format == "csv":
            try:
                if is_glob and isinstance(full_path, list):
                    df = self._read_parallel(pd.read_csv, full_path, **read_kwargs)
                    df.attrs["odibi_source_files"] = full_path
                    return self._process_df(df, post_read_query)

                df = pd.read_csv(full_path, **read_kwargs)
                if hasattr(df, "attrs"):
                    df.attrs["odibi_source_files"] = [str(full_path)]
                return self._process_df(df, post_read_query)
            except UnicodeDecodeError:
                # Retry with common fallbacks
                # Note: Arrow engine might be stricter, so we might need to drop it for retry?
                # But let's try keeping it if possible, or fallback completely.
                # Simplify: Just update encoding in kwargs
                read_kwargs["encoding"] = "latin1"
                if is_glob and isinstance(full_path, list):
                    df = self._read_parallel(pd.read_csv, full_path, **read_kwargs)
                    df.attrs["odibi_source_files"] = full_path
                    return self._process_df(df, post_read_query)

                df = pd.read_csv(full_path, **read_kwargs)
                if hasattr(df, "attrs"):
                    df.attrs["odibi_source_files"] = [str(full_path)]
                return self._process_df(df, post_read_query)
            except pd.errors.ParserError:
                # Retry with bad lines skipped
                read_kwargs["on_bad_lines"] = "skip"
                if is_glob and isinstance(full_path, list):
                    df = self._read_parallel(pd.read_csv, full_path, **read_kwargs)
                    df.attrs["odibi_source_files"] = full_path
                    return self._process_df(df, post_read_query)

                df = pd.read_csv(full_path, **read_kwargs)
                if hasattr(df, "attrs"):
                    df.attrs["odibi_source_files"] = [str(full_path)]
                return self._process_df(df, post_read_query)
        elif format == "parquet":
            # read_parquet handles list of files
            df = pd.read_parquet(full_path, **read_kwargs)
            if isinstance(full_path, list):
                df.attrs["odibi_source_files"] = full_path
            else:
                df.attrs["odibi_source_files"] = [str(full_path)]
            return self._process_df(df, post_read_query)
        elif format == "json":
            if is_glob and isinstance(full_path, list):
                df = self._read_parallel(pd.read_json, full_path, **read_kwargs)
                df.attrs["odibi_source_files"] = full_path
                return self._process_df(df, post_read_query)

            df = pd.read_json(full_path, **read_kwargs)
            if hasattr(df, "attrs"):
                df.attrs["odibi_source_files"] = [str(full_path)]
            return self._process_df(df, post_read_query)
        elif format == "excel":
            # Excel doesn't support dtype_backend arg directly in older versions or engine dependent
            # But Pandas 2.0 might. Let's check safely.
            # read_excel does NOT support dtype_backend as of 2.0.3 typically.
            # We skip arrow backend for Excel for now to be safe.
            excel_kwargs = merged_options.copy()
            return self._process_df(pd.read_excel(full_path, **excel_kwargs), post_read_query)
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
                    return self._process_df(
                        dt.to_pandas(
                            partitions=None, arrow_options={"types_mapper": pd.ArrowDtype}
                        ),
                        post_read_query,
                    )
                else:
                    # Fallback for older deltalake versions
                    # Convert via Arrow manually to ensure pyarrow backed
                    return self._process_df(
                        dt.to_pyarrow_table().to_pandas(types_mapper=pd.ArrowDtype), post_read_query
                    )
            else:
                return self._process_df(dt.to_pandas(), post_read_query)
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
                return self._process_df(pd.DataFrame(records), post_read_query)
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
        register_table: Optional[str] = None,
        mode: str = "overwrite",
        options: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Write data using Pandas.

        Args:
            df: DataFrame or Iterator of DataFrames to write
            connection: Connection object
            format: Output format
            table: Table name
            path: File path
            register_table: Name to register as external table (not used in Pandas)
            mode: Write mode
            options: Format-specific options

        Returns:
             Optional dictionary containing Delta commit metadata (if format=delta)
        """
        options = options or {}

        # Handle iterator/generator input
        from collections.abc import Iterator

        if isinstance(df, Iterator):
            return self._write_iterator(df, connection, format, table, path, mode, options)

        # SQL Server / Azure SQL Support
        if format in ["sql_server", "azure_sql"]:
            return self._write_sql(df, connection, table, mode, options)

        # Get full path from connection
        if path:
            full_path = connection.get_path(path)
        elif table:
            full_path = connection.get_path(table)
        else:
            raise ValueError("Either path or table must be provided")

        # Merge storage options for cloud connections
        merged_options = self._merge_storage_options(connection, options)

        # Custom Writers
        if format in self._custom_writers:
            # Clean up custom options
            writer_options = merged_options.copy()
            writer_options.pop("keys", None)
            self._custom_writers[format](df, full_path, mode=mode, **writer_options)
            return None

        # Ensure directory exists (local only)
        self._ensure_directory(full_path)

        # Warn about partitioning
        self._check_partitioning(merged_options)

        # Delta Lake Write
        if format == "delta":
            return self._write_delta(df, full_path, mode, merged_options)

        # Handle Generic Upsert/Append-Once for non-Delta
        if mode in ["upsert", "append_once"]:
            df, mode = self._handle_generic_upsert(df, full_path, format, mode, merged_options)

        # Standard File Write
        return self._write_file(df, full_path, format, mode, merged_options)

    def _write_iterator(
        self,
        df_iter: Iterator[pd.DataFrame],
        connection: Any,
        format: str,
        table: Optional[str],
        path: Optional[str],
        mode: str,
        options: Dict[str, Any],
    ) -> None:
        """Handle writing of iterator/generator."""
        first_chunk = True
        for chunk in df_iter:
            # Determine mode for this chunk
            current_mode = mode if first_chunk else "append"
            current_options = options.copy()

            # Handle CSV header for chunks
            if not first_chunk and format == "csv":
                if current_options.get("header") is not False:
                    current_options["header"] = False

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
        return None

    def _write_sql(
        self,
        df: pd.DataFrame,
        connection: Any,
        table: Optional[str],
        mode: str,
        options: Dict[str, Any],
    ) -> None:
        """Handle SQL writing."""
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
        return None

    def _ensure_directory(self, full_path: str) -> None:
        """Ensure parent directory exists for local files."""
        parsed = urlparse(str(full_path))
        is_windows_drive = (
            len(parsed.scheme) == 1 and parsed.scheme.isalpha() if parsed.scheme else False
        )

        if not parsed.scheme or parsed.scheme == "file" or is_windows_drive:
            Path(full_path).parent.mkdir(parents=True, exist_ok=True)

    def _check_partitioning(self, options: Dict[str, Any]) -> None:
        """Warn about potential partitioning issues."""
        partition_by = options.get("partition_by") or options.get("partitionBy")
        if partition_by:
            import warnings

            warnings.warn(
                "⚠️  Partitioning can cause performance issues if misused. "
                "Only partition on low-cardinality columns (< 1000 unique values) "
                "and ensure each partition has > 1000 rows.",
                UserWarning,
            )

    def _write_delta(
        self,
        df: pd.DataFrame,
        full_path: str,
        mode: str,
        merged_options: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Handle Delta Lake writing."""
        try:
            from deltalake import write_deltalake, DeltaTable
        except ImportError:
            raise ImportError(
                "Delta Lake support requires 'pip install odibi[pandas]' or 'pip install deltalake'. "
                "See README.md for installation instructions."
            )

        storage_opts = merged_options.get("storage_options", {})

        # Map modes
        delta_mode = "overwrite"
        if mode == "append":
            delta_mode = "append"
        elif mode == "error" or mode == "fail":
            delta_mode = "error"
        elif mode == "ignore":
            delta_mode = "ignore"

        # Handle upsert/append_once logic
        if mode == "upsert":
            keys = merged_options.get("keys")
            if not keys:
                raise ValueError("Upsert requires 'keys' in options")

            if isinstance(keys, str):
                keys = [keys]

            dt = DeltaTable(full_path, storage_options=storage_opts)
            (
                dt.merge(
                    source=df,
                    predicate=" AND ".join([f"s.{k} = t.{k}" for k in keys]),
                    source_alias="s",
                    target_alias="t",
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
        elif mode == "append_once":
            keys = merged_options.get("keys")
            if not keys:
                raise ValueError("Append_once requires 'keys' in options")

            if isinstance(keys, str):
                keys = [keys]

            dt = DeltaTable(full_path, storage_options=storage_opts)
            (
                dt.merge(
                    source=df,
                    predicate=" AND ".join([f"s.{k} = t.{k}" for k in keys]),
                    source_alias="s",
                    target_alias="t",
                )
                .when_not_matched_insert_all()
                .execute()
            )
        else:
            # Filter options supported by write_deltalake
            write_kwargs = {
                k: v
                for k, v in merged_options.items()
                if k
                in [
                    "partition_by",
                    "mode",
                    "overwrite_schema",
                    "schema_mode",
                    "name",
                    "description",
                    "configuration",
                ]
            }

            write_deltalake(
                full_path, df, mode=delta_mode, storage_options=storage_opts, **write_kwargs
            )

        # Return commit info
        dt = DeltaTable(full_path, storage_options=storage_opts)
        history = dt.history(limit=1)
        latest = history[0]

        return {
            "version": dt.version(),
            "timestamp": datetime.fromtimestamp(latest.get("timestamp", 0) / 1000),
            "operation": latest.get("operation"),
            "operation_metrics": latest.get("operationMetrics", {}),
            "read_version": latest.get("readVersion"),
        }

    def _handle_generic_upsert(
        self,
        df: pd.DataFrame,
        full_path: str,
        format: str,
        mode: str,
        options: Dict[str, Any],
    ) -> tuple[pd.DataFrame, str]:
        """Handle upsert/append_once for standard files by merging with existing data."""
        if "keys" not in options:
            raise ValueError(f"Mode '{mode}' requires 'keys' list in options")

        keys = options["keys"]
        if isinstance(keys, str):
            keys = [keys]

        # Try to read existing file
        existing_df = None
        try:
            read_opts = options.copy()
            read_opts.pop("keys", None)

            if format == "csv":
                existing_df = pd.read_csv(full_path, **read_opts)
            elif format == "parquet":
                existing_df = pd.read_parquet(full_path, **read_opts)
            elif format == "json":
                existing_df = pd.read_json(full_path, **read_opts)
            elif format == "excel":
                existing_df = pd.read_excel(full_path, **read_opts)
        except Exception:
            # File doesn't exist or can't be read
            return df, "overwrite"  # Treat as new write

        if existing_df is None:
            return df, "overwrite"

        if mode == "append_once":
            # Check if keys exist
            missing_keys = set(keys) - set(df.columns)
            if missing_keys:
                raise KeyError(f"Keys {missing_keys} not found in input data")

            # Identify new rows
            merged = df.merge(existing_df[keys], on=keys, how="left", indicator=True)
            new_rows = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

            if format in ["csv", "json"]:
                return new_rows, "append"
            else:
                # Rewrite everything
                return pd.concat([existing_df, new_rows], ignore_index=True), "overwrite"

        elif mode == "upsert":
            # Check if keys exist
            missing_keys = set(keys) - set(df.columns)
            if missing_keys:
                raise KeyError(f"Keys {missing_keys} not found in input data")

            # 1. Remove rows from existing that are in input
            merged_indicator = existing_df.merge(df[keys], on=keys, how="left", indicator=True)
            rows_to_keep = existing_df[merged_indicator["_merge"] == "left_only"]

            # 2. Concat rows_to_keep + input df
            # 3. Write mode becomes overwrite
            return pd.concat([rows_to_keep, df], ignore_index=True), "overwrite"

        return df, mode

    def _write_file(
        self,
        df: pd.DataFrame,
        full_path: str,
        format: str,
        mode: str,
        merged_options: Dict[str, Any],
    ) -> None:
        """Handle standard file writing (CSV, Parquet, etc.)."""
        writer_options = merged_options.copy()
        writer_options.pop("keys", None)

        # Remove storage_options for local pandas writers usually?
        # Some pandas writers accept storage_options (parquet, csv with fsspec)

        if format == "csv":
            mode_param = "w"
            if mode == "append":
                mode_param = "a"
                if not os.path.exists(full_path):
                    # If file doesn't exist, include header
                    writer_options["header"] = True
                else:
                    # If appending, don't write header unless explicit
                    if "header" not in writer_options:
                        writer_options["header"] = False

            df.to_csv(full_path, index=False, mode=mode_param, **writer_options)

        elif format == "parquet":
            if mode == "append":
                # Pandas read_parquet doesn't support append directly usually.
                # We implement simple read-concat-write for local files
                if os.path.exists(full_path):
                    existing = pd.read_parquet(full_path, **merged_options)
                    df = pd.concat([existing, df], ignore_index=True)

            df.to_parquet(full_path, index=False, **writer_options)

        elif format == "json":
            if mode == "append":
                writer_options["mode"] = "a"

            # Default to records if not specified
            if "orient" not in writer_options:
                writer_options["orient"] = "records"

            df.to_json(full_path, **writer_options)

        elif format == "excel":
            if mode == "append":
                # Simple append for excel
                if os.path.exists(full_path):
                    with pd.ExcelWriter(full_path, mode="a", if_sheet_exists="overlay") as writer:
                        df.to_excel(writer, index=False, **writer_options)
                    return

            df.to_excel(full_path, index=False, **writer_options)

        elif format == "avro":
            try:
                import fastavro
            except ImportError:
                raise ImportError("Avro support requires 'pip install fastavro'")

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
                open_mode = "wb"
                if mode == "append" and os.path.exists(full_path):
                    open_mode = "a+b"

                with open(full_path, open_mode) as f:
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

                # Handle chunked data (Iterator)
                from collections.abc import Iterator

                if isinstance(df, Iterator):
                    # Warning: Materializing iterator for SQL execution
                    # Note: DuckDB doesn't support streaming from iterator yet
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

    def get_schema(self, df: pd.DataFrame) -> Dict[str, str]:
        """Get DataFrame schema with types.

        Args:
            df: DataFrame

        Returns:
            Dict[str, str]: Column name -> Type string
        """
        return {col: str(df[col].dtype) for col in df.columns}

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

    def table_exists(
        self, connection: Any, table: Optional[str] = None, path: Optional[str] = None
    ) -> bool:
        """Check if table or location exists.

        Args:
            connection: Connection object
            table: Table name (not used in Pandas—no catalog)
            path: File path

        Returns:
            True if file/directory exists, False otherwise
        """
        if path:
            full_path = connection.get_path(path)
            return os.path.exists(full_path)
        return False

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

    def get_source_files(self, df: pd.DataFrame) -> List[str]:
        """Get list of source files that generated this DataFrame.

        Args:
            df: DataFrame

        Returns:
            List of file paths
        """
        if hasattr(df, "attrs"):
            return df.attrs.get("odibi_source_files", [])
        return []

    def profile_nulls(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate null percentage for each column.

        Args:
            df: DataFrame

        Returns:
            Dictionary of {column_name: null_percentage}
        """
        # mean() of boolean DataFrame gives the percentage of True values
        return df.isna().mean().to_dict()
