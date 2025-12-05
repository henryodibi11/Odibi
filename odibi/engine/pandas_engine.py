"""Pandas engine implementation."""

import glob
import hashlib
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union
from urllib.parse import urlparse

import pandas as pd

from odibi.context import Context, PandasContext
from odibi.engine.base import Engine
from odibi.enums import EngineType
from odibi.exceptions import TransformError
from odibi.utils.logging_context import get_logging_context

__all__ = ["PandasEngine", "LazyDataset"]


@dataclass
class LazyDataset:
    """Lazy representation of a dataset (file) for out-of-core processing."""

    path: Union[str, List[str]]
    format: str
    options: Dict[str, Any]
    connection: Optional[Any] = None  # To resolve path/credentials if needed

    def __repr__(self):
        return f"LazyDataset(path={self.path}, format={self.format})"


class PandasEngine(Engine):
    """Pandas-based execution engine."""

    name = "pandas"
    engine_type = EngineType.PANDAS

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

        # Check for DuckDB
        self.use_duckdb = False
        # Default to False to ensure stability with existing tests (Lazy Loading is opt-in)
        if self.config.get("performance", {}).get("use_duckdb", False):
            try:
                import duckdb  # noqa: F401

                self.use_duckdb = True
            except ImportError:
                pass

    def materialize(self, df: Any) -> Any:
        """Materialize lazy dataset."""
        if isinstance(df, LazyDataset):
            # Re-invoke read but force materialization (by bypassing Lazy check)
            # We pass the resolved path directly
            # Note: We need to handle the case where path was resolved.
            # LazyDataset.path should be the FULL path.
            return self._read_file(
                full_path=df.path, format=df.format, options=df.options, connection=df.connection
            )
        return df

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
        schema: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
        as_of_version: Optional[int] = None,
        as_of_timestamp: Optional[str] = None,
    ) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        """Read data using Pandas (or LazyDataset)."""
        ctx = get_logging_context().with_context(engine="pandas")
        start = time.time()

        source = path or table
        ctx.debug(
            "Starting read operation",
            format=format,
            path=source,
            streaming=streaming,
            use_arrow=self.use_arrow,
        )

        if streaming:
            ctx.error(
                "Streaming not supported in Pandas engine",
                format=format,
                path=source,
            )
            raise ValueError(
                "Streaming is not supported in the Pandas engine. "
                "Please use 'engine: spark' for streaming pipelines."
            )

        options = options or {}

        # Get full path from connection
        if path:
            if connection:
                full_path = connection.get_path(path)
            else:
                full_path = path
        elif table:
            if connection:
                full_path = connection.get_path(table)
            else:
                ctx.error(
                    "Connection required when specifying 'table'",
                    table=table,
                )
                raise ValueError("Connection is required when specifying 'table'.")
        else:
            ctx.error("Neither path nor table provided for read operation")
            raise ValueError("Either path or table must be provided")

        # Merge storage options for cloud connections
        merged_options = self._merge_storage_options(connection, options)

        # Sanitize options for pandas compatibility
        if "header" in merged_options:
            if merged_options["header"] is True:
                merged_options["header"] = 0
            elif merged_options["header"] is False:
                merged_options["header"] = None

        # Handle Time Travel options
        if as_of_version is not None:
            merged_options["versionAsOf"] = as_of_version
            ctx.debug("Time travel enabled", version=as_of_version)
        if as_of_timestamp is not None:
            merged_options["timestampAsOf"] = as_of_timestamp
            ctx.debug("Time travel enabled", timestamp=as_of_timestamp)

        # Check for Lazy/DuckDB optimization
        can_lazy_load = False

        if can_lazy_load:
            ctx.debug("Using lazy loading via DuckDB", path=str(full_path))
            if isinstance(full_path, (str, Path)):
                return LazyDataset(
                    path=str(full_path),
                    format=format,
                    options=merged_options,
                    connection=connection,
                )
            elif isinstance(full_path, list):
                return LazyDataset(
                    path=full_path, format=format, options=merged_options, connection=connection
                )

        result = self._read_file(full_path, format, merged_options, connection)

        # Log metrics for materialized DataFrames
        elapsed = (time.time() - start) * 1000
        if isinstance(result, pd.DataFrame):
            row_count = len(result)
            memory_mb = result.memory_usage(deep=True).sum() / (1024 * 1024)

            ctx.log_file_io(
                path=str(full_path) if not isinstance(full_path, list) else str(full_path[0]),
                format=format,
                mode="read",
                rows=row_count,
            )
            ctx.log_pandas_metrics(
                memory_mb=memory_mb,
                dtypes={col: str(dtype) for col, dtype in result.dtypes.items()},
            )
            ctx.info(
                "Read completed",
                format=format,
                rows=row_count,
                elapsed_ms=round(elapsed, 2),
                memory_mb=round(memory_mb, 2),
            )

        return result

    def _read_file(
        self,
        full_path: Union[str, List[str], Any],
        format: str,
        options: Dict[str, Any],
        connection: Any = None,
    ) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        """Internal file reading logic."""
        ctx = get_logging_context().with_context(engine="pandas")

        ctx.debug(
            "Reading file",
            path=str(full_path) if not isinstance(full_path, list) else f"{len(full_path)} files",
            format=format,
        )

        # Custom Readers
        if format in self._custom_readers:
            ctx.debug(f"Using custom reader for format: {format}")
            return self._custom_readers[format](full_path, **options)

        # Handle glob patterns for local files
        is_glob = False
        if isinstance(full_path, (str, Path)) and (
            "*" in str(full_path) or "?" in str(full_path) or "[" in str(full_path)
        ):
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
                    ctx.error(
                        "No files matched glob pattern",
                        pattern=glob_path,
                    )
                    raise FileNotFoundError(f"No files matched pattern: {glob_path}")

                ctx.info(
                    "Glob pattern expanded",
                    pattern=glob_path,
                    matched_files=len(matched_files),
                )
                full_path = matched_files
                is_glob = True

        # Prepare read options
        read_kwargs = options.copy()

        # Extract 'query' or 'filter' option for post-read filtering
        post_read_query = read_kwargs.pop("query", None) or read_kwargs.pop("filter", None)

        if self.use_arrow:
            read_kwargs["dtype_backend"] = "pyarrow"

        # Read based on format
        if format == "csv":
            try:
                if is_glob and isinstance(full_path, list):
                    ctx.debug(
                        "Parallel CSV read",
                        file_count=len(full_path),
                    )
                    df = self._read_parallel(pd.read_csv, full_path, **read_kwargs)
                    df.attrs["odibi_source_files"] = full_path
                    return self._process_df(df, post_read_query)

                df = pd.read_csv(full_path, **read_kwargs)
                if hasattr(df, "attrs"):
                    df.attrs["odibi_source_files"] = [str(full_path)]
                return self._process_df(df, post_read_query)
            except UnicodeDecodeError:
                ctx.warning(
                    "UnicodeDecodeError, retrying with latin1 encoding",
                    path=str(full_path),
                )
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
                ctx.warning(
                    "ParserError, retrying with on_bad_lines='skip'",
                    path=str(full_path),
                )
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
            ctx.debug("Reading parquet", path=str(full_path))
            df = pd.read_parquet(full_path, **read_kwargs)
            if isinstance(full_path, list):
                df.attrs["odibi_source_files"] = full_path
            else:
                df.attrs["odibi_source_files"] = [str(full_path)]
            return self._process_df(df, post_read_query)
        elif format == "json":
            if is_glob and isinstance(full_path, list):
                ctx.debug(
                    "Parallel JSON read",
                    file_count=len(full_path),
                )
                df = self._read_parallel(pd.read_json, full_path, **read_kwargs)
                df.attrs["odibi_source_files"] = full_path
                return self._process_df(df, post_read_query)

            df = pd.read_json(full_path, **read_kwargs)
            if hasattr(df, "attrs"):
                df.attrs["odibi_source_files"] = [str(full_path)]
            return self._process_df(df, post_read_query)
        elif format == "excel":
            ctx.debug("Reading Excel file", path=str(full_path))
            read_kwargs.pop("dtype_backend", None)
            return self._process_df(pd.read_excel(full_path, **read_kwargs), post_read_query)
        elif format == "delta":
            ctx.debug("Reading Delta table", path=str(full_path))
            try:
                from deltalake import DeltaTable
            except ImportError:
                ctx.error(
                    "Delta Lake library not installed",
                    path=str(full_path),
                )
                raise ImportError(
                    "Delta Lake support requires 'pip install odibi[pandas]' "
                    "or 'pip install deltalake'. See README.md for installation instructions."
                )

            storage_opts = options.get("storage_options", {})
            version = options.get("versionAsOf")
            timestamp = options.get("timestampAsOf")

            if timestamp is not None:
                from datetime import datetime as dt_module

                if isinstance(timestamp, str):
                    ts = dt_module.fromisoformat(timestamp.replace("Z", "+00:00"))
                else:
                    ts = timestamp
                dt = DeltaTable(full_path, storage_options=storage_opts)
                dt.load_with_datetime(ts)
                ctx.debug("Delta table loaded with timestamp", timestamp=str(ts))
            elif version is not None:
                dt = DeltaTable(full_path, storage_options=storage_opts, version=version)
                ctx.debug("Delta table loaded with version", version=version)
            else:
                dt = DeltaTable(full_path, storage_options=storage_opts)
                ctx.debug("Delta table loaded (latest version)")

            if self.use_arrow:
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
                    return self._process_df(
                        dt.to_pyarrow_table().to_pandas(types_mapper=pd.ArrowDtype),
                        post_read_query,
                    )
            else:
                return self._process_df(dt.to_pandas(), post_read_query)
        elif format == "avro":
            ctx.debug("Reading Avro file", path=str(full_path))
            try:
                import fastavro
            except ImportError:
                ctx.error(
                    "fastavro library not installed",
                    path=str(full_path),
                )
                raise ImportError(
                    "Avro support requires 'pip install odibi[pandas]' "
                    "or 'pip install fastavro'. See README.md for installation instructions."
                )

            parsed = urlparse(full_path)
            if parsed.scheme and parsed.scheme not in ["file", ""]:
                import fsspec

                storage_opts = options.get("storage_options", {})
                with fsspec.open(full_path, "rb", **storage_opts) as f:
                    reader = fastavro.reader(f)
                    records = [record for record in reader]
                return pd.DataFrame(records)
            else:
                with open(full_path, "rb") as f:
                    reader = fastavro.reader(f)
                    records = [record for record in reader]
                return self._process_df(pd.DataFrame(records), post_read_query)
        elif format in ["sql_server", "azure_sql"]:
            ctx.debug("Reading SQL table", table=str(full_path), format=format)
            if not hasattr(connection, "read_table"):
                ctx.error(
                    "Connection does not support SQL operations",
                    connection_type=type(connection).__name__,
                )
                raise ValueError(
                    f"Connection type '{type(connection).__name__}' does not support SQL operations"
                )

            table_name = str(full_path)
            if "." in table_name:
                schema, tbl = table_name.split(".", 1)
            else:
                schema, tbl = "dbo", table_name

            ctx.debug("Executing SQL read", schema=schema, table=tbl)
            return connection.read_table(table_name=tbl, schema=schema)
        else:
            ctx.error("Unsupported format", format=format)
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
        streaming_config: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Write data using Pandas."""
        ctx = get_logging_context().with_context(engine="pandas")
        start = time.time()

        destination = path or table
        ctx.debug(
            "Starting write operation",
            format=format,
            destination=destination,
            mode=mode,
        )

        # Ensure materialization if LazyDataset
        df = self.materialize(df)

        options = options or {}

        # Handle iterator/generator input
        from collections.abc import Iterator

        if isinstance(df, Iterator):
            ctx.debug("Writing iterator/generator input")
            return self._write_iterator(df, connection, format, table, path, mode, options)

        row_count = len(df)
        memory_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)

        ctx.log_pandas_metrics(
            memory_mb=memory_mb,
            dtypes={col: str(dtype) for col, dtype in df.dtypes.items()},
        )

        # SQL Server / Azure SQL Support
        if format in ["sql_server", "azure_sql"]:
            ctx.debug("Writing to SQL", table=table, mode=mode)
            return self._write_sql(df, connection, table, mode, options)

        # Get full path from connection
        if path:
            if connection:
                full_path = connection.get_path(path)
            else:
                full_path = path
        elif table:
            if connection:
                full_path = connection.get_path(table)
            else:
                ctx.error("Connection required when specifying 'table'", table=table)
                raise ValueError("Connection is required when specifying 'table'.")
        else:
            ctx.error("Neither path nor table provided for write operation")
            raise ValueError("Either path or table must be provided")

        # Merge storage options for cloud connections
        merged_options = self._merge_storage_options(connection, options)

        # Custom Writers
        if format in self._custom_writers:
            ctx.debug(f"Using custom writer for format: {format}")
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
            ctx.debug("Writing Delta table", path=str(full_path), mode=mode)
            result = self._write_delta(df, full_path, mode, merged_options)
            elapsed = (time.time() - start) * 1000
            ctx.log_file_io(
                path=str(full_path),
                format=format,
                mode=mode,
                rows=row_count,
            )
            ctx.info(
                "Write completed",
                format=format,
                rows=row_count,
                elapsed_ms=round(elapsed, 2),
            )
            return result

        # Handle Generic Upsert/Append-Once for non-Delta
        if mode in ["upsert", "append_once"]:
            ctx.debug(f"Handling {mode} mode for non-Delta format")
            df, mode = self._handle_generic_upsert(df, full_path, format, mode, merged_options)
            row_count = len(df)

        # Standard File Write
        result = self._write_file(df, full_path, format, mode, merged_options)

        elapsed = (time.time() - start) * 1000
        ctx.log_file_io(
            path=str(full_path),
            format=format,
            mode=mode,
            rows=row_count,
        )
        ctx.info(
            "Write completed",
            format=format,
            rows=row_count,
            elapsed_ms=round(elapsed, 2),
        )

        return result

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
            from deltalake import DeltaTable, write_deltalake
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

            # Convert datetime columns to microseconds for Avro timestamp-micros
            df_avro = df.copy()
            for col in df_avro.columns:
                if pd.api.types.is_datetime64_any_dtype(df_avro[col].dtype):
                    df_avro[col] = df_avro[col].apply(
                        lambda x: int(x.timestamp() * 1_000_000) if pd.notna(x) else None
                    )

            records = df_avro.to_dict("records")
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

    def add_write_metadata(
        self,
        df: pd.DataFrame,
        metadata_config,
        source_connection: Optional[str] = None,
        source_table: Optional[str] = None,
        source_path: Optional[str] = None,
        is_file_source: bool = False,
    ) -> pd.DataFrame:
        """Add metadata columns to DataFrame before writing (Bronze layer lineage).

        Args:
            df: Pandas DataFrame
            metadata_config: WriteMetadataConfig or True (for all defaults)
            source_connection: Name of the source connection
            source_table: Name of the source table (SQL sources)
            source_path: Path of the source file (file sources)
            is_file_source: True if source is a file-based read

        Returns:
            DataFrame with metadata columns added
        """
        from odibi.config import WriteMetadataConfig

        # Normalize config: True -> all defaults
        if metadata_config is True:
            config = WriteMetadataConfig()
        elif isinstance(metadata_config, WriteMetadataConfig):
            config = metadata_config
        else:
            return df  # None or invalid -> no metadata

        # Work on a copy to avoid modifying original
        df = df.copy()

        # _extracted_at: always applicable
        if config.extracted_at:
            df["_extracted_at"] = pd.Timestamp.now()

        # _source_file: only for file sources
        if config.source_file and is_file_source and source_path:
            df["_source_file"] = source_path

        # _source_connection: all sources
        if config.source_connection and source_connection:
            df["_source_connection"] = source_connection

        # _source_table: SQL sources only
        if config.source_table and source_table:
            df["_source_table"] = source_table

        return df

    def _register_lazy_view_unused(self, conn, name: str, df: Any) -> None:
        """Register a LazyDataset as a DuckDB view."""
        duck_fmt = df.format
        if duck_fmt == "json":
            duck_fmt = "json_auto"

        if isinstance(df.path, list):
            paths = ", ".join([f"'{p}'" for p in df.path])
            conn.execute(
                f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_{duck_fmt}([{paths}])"
            )
        else:
            conn.execute(
                f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_{duck_fmt}('{df.path}')"
            )

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
                dataset_obj = context.get(name)

                # Debug check
                # print(f"DEBUG: Registering {name} type={type(dataset_obj)} LazyDataset={LazyDataset}")

                # Handle LazyDataset (DuckDB optimization)
                # if isinstance(dataset_obj, LazyDataset):
                #     self._register_lazy_view(conn, name, dataset_obj)
                #     # Log that we used DuckDB on file
                #     # logger.info(f"Executing SQL via DuckDB on lazy file: {dataset_obj.path}")
                #     continue

                # Handle chunked data (Iterator)
                from collections.abc import Iterator

                if isinstance(dataset_obj, Iterator):
                    # Warning: Materializing iterator for SQL execution
                    # Note: DuckDB doesn't support streaming from iterator yet
                    dataset_obj = pd.concat(dataset_obj, ignore_index=True)

                conn.register(name, dataset_obj)

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
        # Materialize LazyDataset
        df = self.materialize(df)

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
            # Fallback: check if operation is a registered transformer
            from odibi.context import EngineContext
            from odibi.registry import FunctionRegistry

            if FunctionRegistry.has_function(operation):
                func = FunctionRegistry.get_function(operation)
                param_model = FunctionRegistry.get_param_model(operation)

                # Create EngineContext from current df
                engine_ctx = EngineContext(df=df, engine=self, engine_type=self.engine_type)

                # Validate and instantiate params
                if param_model:
                    validated_params = param_model(**params)
                    result_ctx = func(engine_ctx, validated_params)
                else:
                    result_ctx = func(engine_ctx, **params)

                return result_ctx.df

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

    def harmonize_schema(
        self, df: pd.DataFrame, target_schema: Dict[str, str], policy: Any
    ) -> pd.DataFrame:
        """Harmonize DataFrame schema with target schema according to policy."""
        # Ensure materialization
        df = self.materialize(df)

        from odibi.config import OnMissingColumns, OnNewColumns, SchemaMode

        target_cols = list(target_schema.keys())
        current_cols = df.columns.tolist()

        missing = set(target_cols) - set(current_cols)
        new_cols = set(current_cols) - set(target_cols)

        # 1. Check Validations
        if missing and policy.on_missing_columns == OnMissingColumns.FAIL:
            raise ValueError(f"Schema Policy Violation: Missing columns {missing}")

        if new_cols and policy.on_new_columns == OnNewColumns.FAIL:
            raise ValueError(f"Schema Policy Violation: New columns {new_cols}")

        # 2. Apply Transformations
        if policy.mode == SchemaMode.EVOLVE and policy.on_new_columns == OnNewColumns.ADD_NULLABLE:
            # Evolve: Add missing columns, Keep new columns
            for col in missing:
                df[col] = None
        else:
            # Enforce / Ignore New: Project to target schema (Drops new, Adds missing)
            # Note: reindex adds NaN for missing columns
            df = df.reindex(columns=target_cols)

        return df

    def anonymize(
        self, df: Any, columns: List[str], method: str, salt: Optional[str] = None
    ) -> pd.DataFrame:
        """Anonymize specified columns."""
        # Ensure materialization
        df = self.materialize(df)

        res = df.copy()

        for col in columns:
            if col not in res.columns:
                continue

            if method == "hash":
                # Vectorized Hashing (via map/apply)
                # Note: True vectorization requires C-level support (e.g. pyarrow.compute)
                # Standard Pandas apply is the fallback but we can optimize string handling

                # Convert to string, handling nulls
                # s_col = res[col].astype(str)
                # Nulls become 'nan'/'None' string, we want to preserve them or hash them consistently?
                # Typically nulls should remain null.

                mask_nulls = res[col].isna()

                def _hash_val(val):
                    to_hash = val
                    if salt:
                        to_hash += salt
                    return hashlib.sha256(to_hash.encode("utf-8")).hexdigest()

                # Apply only to non-nulls
                res.loc[~mask_nulls, col] = res.loc[~mask_nulls, col].astype(str).apply(_hash_val)

            elif method == "mask":
                # Vectorized Masking
                # Mask all but last 4 characters

                mask_nulls = res[col].isna()
                s_valid = res.loc[~mask_nulls, col].astype(str)

                # Use vectorized regex replacement
                # Replace any character that is followed by 4 characters with '*'
                res.loc[~mask_nulls, col] = s_valid.str.replace(r".(?=.{4})", "*", regex=True)

            elif method == "redact":
                res[col] = "[REDACTED]"

        return res

    def get_schema(self, df: Any) -> Dict[str, str]:
        """Get DataFrame schema with types.

        Args:
            df: DataFrame or LazyDataset

        Returns:
            Dict[str, str]: Column name -> Type string
        """
        if isinstance(df, LazyDataset):
            if self.use_duckdb:
                try:
                    import duckdb

                    conn = duckdb.connect(":memory:")
                    self._register_lazy_view(conn, "df", df)
                    res = conn.execute("DESCRIBE SELECT * FROM df").df()
                    return dict(zip(res["column_name"], res["column_type"]))
                except Exception:
                    pass
            df = self.materialize(df)

        return {col: str(df[col].dtype) for col in df.columns}

    def get_shape(self, df: Any) -> tuple:
        """Get DataFrame shape.

        Args:
            df: DataFrame or LazyDataset

        Returns:
            (rows, columns)
        """
        if isinstance(df, LazyDataset):
            cols = len(self.get_schema(df))
            rows = self.count_rows(df)
            return (rows, cols)
        return df.shape

    def count_rows(self, df: Any) -> int:
        """Count rows in DataFrame.

        Args:
            df: DataFrame or LazyDataset

        Returns:
            Row count
        """
        if isinstance(df, LazyDataset):
            if self.use_duckdb:
                try:
                    import duckdb

                    conn = duckdb.connect(":memory:")
                    self._register_lazy_view(conn, "df", df)
                    res = conn.execute("SELECT count(*) FROM df").fetchone()
                    return res[0] if res else 0
                except Exception:
                    pass
            df = self.materialize(df)

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
        # Ensure materialization
        df = self.materialize(df)

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
                # Handle pyarrow types (e.g. int64[pyarrow])
                if "[" in actual_type and "pyarrow" in actual_type:
                    actual_type = actual_type.split("[")[0]

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
            dtype = df[col].dtype
            dtype_str = str(dtype)

            # Handle datetime types with Avro logical types
            if pd.api.types.is_datetime64_any_dtype(dtype):
                avro_type = {
                    "type": "long",
                    "logicalType": "timestamp-micros",
                }
            elif dtype_str == "date" or (hasattr(dtype, "name") and "date" in dtype.name.lower()):
                avro_type = {
                    "type": "int",
                    "logicalType": "date",
                }
            elif pd.api.types.is_timedelta64_dtype(dtype):
                avro_type = {
                    "type": "long",
                    "logicalType": "time-micros",
                }
            else:
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
        # Ensure materialization
        df = self.materialize(df)

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

    def get_sample(self, df: Any, n: int = 10) -> List[Dict[str, Any]]:
        """Get sample rows as list of dictionaries.

        Args:
            df: DataFrame or LazyDataset
            n: Number of rows to return

        Returns:
            List of row dictionaries
        """
        if isinstance(df, LazyDataset):
            if self.use_duckdb:
                try:
                    import duckdb

                    conn = duckdb.connect(":memory:")
                    self._register_lazy_view(conn, "df", df)
                    res_df = conn.execute(f"SELECT * FROM df LIMIT {n}").df()
                    return res_df.to_dict("records")
                except Exception:
                    pass
            df = self.materialize(df)

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

    def get_table_schema(
        self,
        connection: Any,
        table: Optional[str] = None,
        path: Optional[str] = None,
        format: Optional[str] = None,
    ) -> Optional[Dict[str, str]]:
        """Get schema of an existing table/file."""
        try:
            if table and format in ["sql_server", "azure_sql"]:
                # SQL Server: Read empty result
                query = f"SELECT TOP 0 * FROM {table}"
                df = connection.read_sql(query)
                return self.get_schema(df)

            if path:
                full_path = connection.get_path(path)
                if not os.path.exists(full_path):
                    return None

                if format == "delta":
                    from deltalake import DeltaTable

                    dt = DeltaTable(full_path)
                    # Use pyarrow schema to pandas schema to avoid reading data
                    arrow_schema = dt.schema().to_pyarrow()
                    empty_df = arrow_schema.empty_table().to_pandas()
                    return self.get_schema(empty_df)

                elif format == "parquet":
                    import pyarrow.parquet as pq

                    target_path = full_path
                    if os.path.isdir(full_path):
                        # Find first parquet file
                        files = glob.glob(os.path.join(full_path, "*.parquet"))
                        if not files:
                            return None
                        target_path = files[0]

                    schema = pq.read_schema(target_path)
                    empty_df = schema.empty_table().to_pandas()
                    return self.get_schema(empty_df)

                elif format == "csv":
                    df = pd.read_csv(full_path, nrows=0)
                    return self.get_schema(df)

        except (FileNotFoundError, PermissionError):
            return None
        except ImportError as e:
            # Log missing optional dependency
            import logging

            logging.getLogger(__name__).warning(
                f"Could not infer schema due to missing dependency: {e}"
            )
            return None
        except Exception as e:
            import logging

            logging.getLogger(__name__).warning(f"Failed to infer schema for {table or path}: {e}")
            return None
        return None

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
        ctx = get_logging_context().with_context(engine="pandas")
        start = time.time()

        ctx.debug(
            "Starting Delta VACUUM",
            path=path,
            retention_hours=retention_hours,
            dry_run=dry_run,
        )

        try:
            from deltalake import DeltaTable
        except ImportError:
            ctx.error("Delta Lake library not installed", path=path)
            raise ImportError(
                "Delta Lake support requires 'pip install odibi[pandas]' "
                "or 'pip install deltalake'. See README.md for installation instructions."
            )

        full_path = connection.get_path(path)

        storage_opts = {}
        if hasattr(connection, "pandas_storage_options"):
            storage_opts = connection.pandas_storage_options()

        dt = DeltaTable(full_path, storage_options=storage_opts)
        deleted_files = dt.vacuum(
            retention_hours=retention_hours,
            dry_run=dry_run,
            enforce_retention_duration=enforce_retention_duration,
        )

        elapsed = (time.time() - start) * 1000
        ctx.info(
            "Delta VACUUM completed",
            path=str(full_path),
            files_deleted=len(deleted_files),
            dry_run=dry_run,
            elapsed_ms=round(elapsed, 2),
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
        ctx = get_logging_context().with_context(engine="pandas")
        start = time.time()

        ctx.debug("Getting Delta table history", path=path, limit=limit)

        try:
            from deltalake import DeltaTable
        except ImportError:
            ctx.error("Delta Lake library not installed", path=path)
            raise ImportError(
                "Delta Lake support requires 'pip install odibi[pandas]' "
                "or 'pip install deltalake'. See README.md for installation instructions."
            )

        full_path = connection.get_path(path)

        storage_opts = {}
        if hasattr(connection, "pandas_storage_options"):
            storage_opts = connection.pandas_storage_options()

        dt = DeltaTable(full_path, storage_options=storage_opts)
        history = dt.history(limit=limit)

        elapsed = (time.time() - start) * 1000
        ctx.info(
            "Delta history retrieved",
            path=str(full_path),
            versions_returned=len(history) if history else 0,
            elapsed_ms=round(elapsed, 2),
        )

        return history

    def restore_delta(self, connection: Any, path: str, version: int) -> None:
        """Restore Delta table to a specific version.

        Args:
            connection: Connection object
            path: Delta table path
            version: Version number to restore to
        """
        ctx = get_logging_context().with_context(engine="pandas")
        start = time.time()

        ctx.info("Starting Delta table restore", path=path, target_version=version)

        try:
            from deltalake import DeltaTable
        except ImportError:
            ctx.error("Delta Lake library not installed", path=path)
            raise ImportError(
                "Delta Lake support requires 'pip install odibi[pandas]' "
                "or 'pip install deltalake'. See README.md for installation instructions."
            )

        full_path = connection.get_path(path)

        storage_opts = {}
        if hasattr(connection, "pandas_storage_options"):
            storage_opts = connection.pandas_storage_options()

        dt = DeltaTable(full_path, storage_options=storage_opts)
        dt.restore(version)

        elapsed = (time.time() - start) * 1000
        ctx.info(
            "Delta table restored",
            path=str(full_path),
            restored_to_version=version,
            elapsed_ms=round(elapsed, 2),
        )

    def maintain_table(
        self,
        connection: Any,
        format: str,
        table: Optional[str] = None,
        path: Optional[str] = None,
        config: Optional[Any] = None,
    ) -> None:
        """Run table maintenance operations (optimize, vacuum)."""
        ctx = get_logging_context().with_context(engine="pandas")

        if format != "delta" or not config or not config.enabled:
            return

        if not path and not table:
            return

        full_path = connection.get_path(path if path else table)
        start = time.time()

        ctx.info("Starting table maintenance", path=str(full_path))

        try:
            from deltalake import DeltaTable
        except ImportError:
            ctx.warning(
                "Auto-optimize skipped: 'deltalake' library not installed",
                path=str(full_path),
            )
            return

        try:
            storage_opts = {}
            if hasattr(connection, "pandas_storage_options"):
                storage_opts = connection.pandas_storage_options()

            dt = DeltaTable(full_path, storage_options=storage_opts)

            ctx.info("Running Delta OPTIMIZE (compaction)", path=str(full_path))
            dt.optimize.compact()

            retention = config.vacuum_retention_hours
            if retention is not None and retention > 0:
                ctx.info(
                    "Running Delta VACUUM",
                    path=str(full_path),
                    retention_hours=retention,
                )
                dt.vacuum(
                    retention_hours=retention,
                    enforce_retention_duration=True,
                    dry_run=False,
                )

            elapsed = (time.time() - start) * 1000
            ctx.info(
                "Table maintenance completed",
                path=str(full_path),
                elapsed_ms=round(elapsed, 2),
            )

        except Exception as e:
            ctx.warning(
                "Auto-optimize failed",
                path=str(full_path),
                error=str(e),
            )

    def get_source_files(self, df: Any) -> List[str]:
        """Get list of source files that generated this DataFrame.

        Args:
            df: DataFrame or LazyDataset

        Returns:
            List of file paths
        """
        if isinstance(df, LazyDataset):
            if isinstance(df.path, list):
                return df.path
            return [str(df.path)]

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
        # Ensure materialization
        df = self.materialize(df)

        # mean() of boolean DataFrame gives the percentage of True values
        return df.isna().mean().to_dict()

    def filter_greater_than(self, df: pd.DataFrame, column: str, value: Any) -> pd.DataFrame:
        """Filter DataFrame where column > value."""
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")

        try:
            # Handle timestamp string comparison
            if pd.api.types.is_datetime64_any_dtype(df[column]) and isinstance(value, str):
                value = pd.to_datetime(value)

            return df[df[column] > value]
        except Exception as e:
            raise ValueError(f"Failed to filter {column} > {value}: {e}")

    def filter_coalesce(
        self, df: pd.DataFrame, col1: str, col2: str, op: str, value: Any
    ) -> pd.DataFrame:
        """Filter using COALESCE(col1, col2) op value."""
        if col1 not in df.columns:
            # If fallback only? No, usually primary must exist.
            raise ValueError(f"Column '{col1}' not found")

        # If col2 missing, behave like col1
        if col2 not in df.columns:
            s = df[col1]
        else:
            s = df[col1].combine_first(df[col2])

        try:
            if pd.api.types.is_datetime64_any_dtype(s) and isinstance(value, str):
                value = pd.to_datetime(value)

            if op == ">=":
                return df[s >= value]
            elif op == ">":
                return df[s > value]
            elif op == "<=":
                return df[s <= value]
            elif op == "<":
                return df[s < value]
            elif op == "==" or op == "=":
                return df[s == value]
            else:
                raise ValueError(f"Unsupported operator: {op}")
        except Exception as e:
            raise ValueError(f"Failed to filter COALESCE({col1}, {col2}) {op} {value}: {e}")
