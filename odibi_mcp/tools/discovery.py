# odibi_mcp/tools/discovery.py
"""Source discovery MCP tools - wired to real connections."""

import logging
from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime
from pathlib import Path

from odibi_mcp.contracts.discovery import FileInfo, ListFilesResponse
from odibi_mcp.contracts.schema import SchemaResponse, ColumnSpec
from odibi_mcp.discovery.limits import DiscoveryLimits, DEFAULT_LIMITS
from odibi_mcp.context import get_project_context

logger = logging.getLogger(__name__)


@dataclass
class TableInfo:
    """Information about a database table."""

    name: str
    schema_name: str
    row_count: Optional[int]
    column_count: int


@dataclass
class ListTablesResponse:
    """Response for list_tables."""

    connection: str
    tables: List[TableInfo]
    truncated: bool
    total_count: Optional[int]


@dataclass
class PreviewResponse:
    """Response for preview_source."""

    connection: str
    path: str
    rows: List[dict]
    schema: SchemaResponse
    truncated: bool
    total_rows: Optional[int]


def _is_cloud_connection(conn) -> bool:
    """Check if connection is a cloud connection (Azure, S3, etc.)."""
    conn_type = type(conn).__name__
    return conn_type in ("AzureADLS", "S3Connection", "GCSConnection")


def _list_cloud_files(conn, path: str, pattern: str, max_files: int) -> List[FileInfo]:
    """List files from cloud storage using fsspec."""
    try:
        import fsspec

        # Get storage options from connection
        storage_options = {}
        if hasattr(conn, "pandas_storage_options"):
            storage_options = conn.pandas_storage_options()

        # Construct full URI
        full_path = conn.get_path(path)

        # Determine filesystem type
        if full_path.startswith(("abfss://", "abfs://", "az://")):
            fs = fsspec.filesystem("abfs", **storage_options)
            # Remove protocol prefix for fsspec
            if "://" in full_path:
                fs_path = full_path.split("://", 1)[1]
                # Handle container@account format
                if "@" in fs_path.split("/")[0]:
                    parts = fs_path.split("/", 1)
                    container_account = parts[0]
                    rest = parts[1] if len(parts) > 1 else ""
                    container = container_account.split("@")[0]
                    fs_path = f"{container}/{rest}"
            else:
                fs_path = full_path
        else:
            # Local filesystem
            fs = fsspec.filesystem("file")
            fs_path = full_path

        # List files
        if pattern == "*":
            files = fs.ls(fs_path, detail=True)
        else:
            import fnmatch

            all_files = fs.ls(fs_path, detail=True)
            files = [
                f for f in all_files if fnmatch.fnmatch(f.get("name", "").split("/")[-1], pattern)
            ]

        file_infos = []
        for f in files[:max_files]:
            name = f.get("name", "").split("/")[-1]
            mtime = f.get("mtime")
            # Handle mtime as timestamp or None
            if mtime is None:
                mtime = datetime.now()
            elif isinstance(mtime, (int, float)):
                mtime = datetime.fromtimestamp(mtime)

            file_infos.append(
                FileInfo(
                    logical_name=name,
                    physical_path=f.get("name", ""),
                    size_bytes=f.get("size", 0) or 0,
                    modified=mtime,
                    is_directory=f.get("type") == "directory",
                )
            )

        return file_infos, len(files)
    except Exception as e:
        logger.error(f"Error listing cloud files: {e}")
        return [], 0


def list_files(
    connection: str,
    path: str,
    pattern: str = "*",
    max_files: Optional[int] = None,
    limits: Optional[DiscoveryLimits] = None,
) -> ListFilesResponse:
    """
    List files at a path in a connection.

    Uses the actual connection to list files.
    """
    if limits is None:
        limits = DEFAULT_LIMITS
    if max_files is None:
        max_files = limits.max_files_per_request

    ctx = get_project_context()
    if not ctx:
        return ListFilesResponse(
            connection=connection,
            path=path,
            files=[],
            next_token=None,
            truncated=False,
            total_count=0,
        )

    try:
        conn = ctx.get_connection(connection)

        # Handle cloud connections differently
        if _is_cloud_connection(conn):
            file_infos, total_count = _list_cloud_files(conn, path, pattern, max_files)
            return ListFilesResponse(
                connection=connection,
                path=path,
                files=file_infos,
                next_token=None,
                truncated=total_count > max_files,
                total_count=total_count,
            )

        # Local filesystem
        full_path = Path(conn.get_path(path))

        if not full_path.exists():
            return ListFilesResponse(
                connection=connection,
                path=path,
                files=[],
                next_token=None,
                truncated=False,
                total_count=0,
            )

        # List files matching pattern
        if full_path.is_dir():
            if pattern == "*":
                all_files = list(full_path.iterdir())
            else:
                all_files = list(full_path.glob(pattern))
        else:
            all_files = [full_path]

        # Apply limit
        truncated = len(all_files) > max_files
        files_to_return = all_files[:max_files]

        file_infos = []
        for f in files_to_return:
            try:
                stat = f.stat()
                file_infos.append(
                    FileInfo(
                        logical_name=f.name,
                        physical_path=str(
                            f.relative_to(full_path.parent) if f != full_path else f.name
                        ),
                        size_bytes=stat.st_size,
                        modified=datetime.fromtimestamp(stat.st_mtime),
                        is_directory=f.is_dir(),
                    )
                )
            except Exception:
                file_infos.append(
                    FileInfo(
                        logical_name=f.name,
                        physical_path=str(f),
                        size_bytes=0,
                        modified=datetime.now(),
                        is_directory=f.is_dir() if f.exists() else False,
                    )
                )

        return ListFilesResponse(
            connection=connection,
            path=path,
            files=file_infos,
            next_token=None,
            truncated=truncated,
            total_count=len(all_files),
        )

    except Exception:
        logger.exception(f"Error listing files: {connection}/{path}")
        return ListFilesResponse(
            connection=connection,
            path=path,
            files=[],
            next_token=None,
            truncated=False,
            total_count=0,
        )


def list_tables(
    connection: str,
    schema: str = "dbo",
    pattern: str = "*",
    limits: Optional[DiscoveryLimits] = None,
) -> ListTablesResponse:
    """
    List tables in a database connection.

    Uses the actual SQL connection.
    """
    if limits is None:
        limits = DEFAULT_LIMITS

    ctx = get_project_context()
    if not ctx:
        return ListTablesResponse(
            connection=connection,
            tables=[],
            truncated=False,
            total_count=0,
        )

    try:
        conn = ctx.get_connection(connection)

        # Check if this is a SQL connection
        if not hasattr(conn, "execute_query"):
            return ListTablesResponse(
                connection=connection,
                tables=[],
                truncated=False,
                total_count=0,
            )

        # Query table metadata
        query = f"""
        SELECT
            TABLE_NAME,
            TABLE_SCHEMA
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}'
        AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """

        result = conn.execute_query(query)

        tables = []
        for row in result:
            table_name = row[0] if isinstance(row, tuple) else row.get("TABLE_NAME")
            schema_name = row[1] if isinstance(row, tuple) else row.get("TABLE_SCHEMA", schema)
            tables.append(
                TableInfo(
                    name=table_name,
                    schema_name=schema_name,
                    row_count=None,  # Would need another query
                    column_count=0,
                )
            )

        truncated = len(tables) > limits.max_tables_per_request

        return ListTablesResponse(
            connection=connection,
            tables=tables[: limits.max_tables_per_request],
            truncated=truncated,
            total_count=len(tables),
        )

    except Exception:
        logger.exception(f"Error listing tables: {connection}/{schema}")
        return ListTablesResponse(
            connection=connection,
            tables=[],
            truncated=False,
            total_count=0,
        )


def infer_schema(
    connection: str,
    path: str,
    limits: Optional[DiscoveryLimits] = None,
) -> SchemaResponse:
    """
    Infer schema from a file or table.

    Reads the file/table and returns column information.
    """
    if limits is None:
        limits = DEFAULT_LIMITS

    ctx = get_project_context()
    if not ctx:
        return SchemaResponse(columns=[], row_count=None, partition_columns=[])

    try:
        conn = ctx.get_connection(connection)
        full_path = Path(conn.get_path(path))

        if not full_path.exists():
            return SchemaResponse(columns=[], row_count=None, partition_columns=[])

        # Use pandas to infer schema
        import pandas as pd

        suffix = full_path.suffix.lower()

        if suffix == ".csv":
            df = pd.read_csv(full_path, nrows=100)
        elif suffix == ".parquet":
            df = pd.read_parquet(full_path)
        elif suffix == ".json":
            df = pd.read_json(full_path, lines=True, nrows=100)
        else:
            return SchemaResponse(columns=[], row_count=None, partition_columns=[])

        columns = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            nullable = df[col].isnull().any()
            columns.append(
                ColumnSpec(
                    name=col,
                    dtype=dtype,
                    nullable=nullable,
                )
            )

        return SchemaResponse(
            columns=columns,
            row_count=len(df),
            partition_columns=[],
        )

    except Exception:
        logger.exception(f"Error inferring schema: {connection}/{path}")
        return SchemaResponse(columns=[], row_count=None, partition_columns=[])


def describe_table(
    connection: str,
    table: str,
    schema: str = "dbo",
) -> SchemaResponse:
    """
    Get schema for a database table.

    Queries INFORMATION_SCHEMA.COLUMNS.
    """
    ctx = get_project_context()
    if not ctx:
        return SchemaResponse(columns=[], row_count=None, partition_columns=[])

    try:
        conn = ctx.get_connection(connection)

        if not hasattr(conn, "execute_query"):
            return SchemaResponse(columns=[], row_count=None, partition_columns=[])

        query = f"""
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}'
        AND TABLE_NAME = '{table}'
        ORDER BY ORDINAL_POSITION
        """

        result = conn.execute_query(query)

        columns = []
        for row in result:
            if isinstance(row, tuple):
                name, dtype, nullable = row
            else:
                name = row.get("COLUMN_NAME")
                dtype = row.get("DATA_TYPE")
                nullable = row.get("IS_NULLABLE")

            columns.append(
                ColumnSpec(
                    name=name,
                    dtype=dtype,
                    nullable=nullable == "YES",
                )
            )

        return SchemaResponse(
            columns=columns,
            row_count=None,
            partition_columns=[],
        )

    except Exception:
        logger.exception(f"Error describing table: {connection}/{schema}.{table}")
        return SchemaResponse(columns=[], row_count=None, partition_columns=[])


def preview_source(
    connection: str,
    path: str,
    format: Optional[str] = None,
    max_rows: int = 100,
    limits: Optional[DiscoveryLimits] = None,
) -> PreviewResponse:
    """
    Preview rows from a source file.

    Reads actual data from the connection.
    """
    if limits is None:
        limits = DEFAULT_LIMITS

    max_rows = min(max_rows, limits.max_preview_rows)

    ctx = get_project_context()
    if not ctx:
        return PreviewResponse(
            connection=connection,
            path=path,
            rows=[],
            schema=SchemaResponse(columns=[], row_count=None, partition_columns=[]),
            truncated=False,
            total_rows=None,
        )

    try:
        conn = ctx.get_connection(connection)
        full_path = conn.get_path(path)

        import pandas as pd

        # Determine format from extension or explicit parameter
        if format:
            suffix = f".{format}"
        elif "." in path:
            suffix = "." + path.split(".")[-1].lower()
        else:
            suffix = ""

        # Get storage options for cloud connections
        storage_options = {}
        if hasattr(conn, "pandas_storage_options"):
            storage_options = conn.pandas_storage_options()

        # Handle cloud vs local differently
        is_cloud = _is_cloud_connection(conn)

        if suffix == ".csv":
            if is_cloud:
                df = pd.read_csv(full_path, nrows=max_rows + 1, storage_options=storage_options)
            else:
                df = pd.read_csv(full_path, nrows=max_rows + 1)
        elif suffix == ".parquet":
            if is_cloud:
                df = pd.read_parquet(full_path, storage_options=storage_options)
            else:
                df = pd.read_parquet(full_path)
            df = df.head(max_rows + 1)
        elif suffix == ".json":
            if is_cloud:
                # For cloud JSON, read via fsspec
                import fsspec

                with fsspec.open(full_path, mode="r", **storage_options) as f:
                    import json

                    data = json.load(f)
                    if isinstance(data, list):
                        df = pd.DataFrame(data[: max_rows + 1])
                    else:
                        df = pd.DataFrame([data])
            else:
                try:
                    df = pd.read_json(full_path, lines=True, nrows=max_rows + 1)
                except ValueError:
                    # Try non-lines format
                    df = pd.read_json(full_path)
                    df = df.head(max_rows + 1)
        elif suffix in (".xlsx", ".xls"):
            if is_cloud:
                import fsspec

                with fsspec.open(full_path, mode="rb", **storage_options) as f:
                    df = pd.read_excel(f, nrows=max_rows + 1)
            else:
                df = pd.read_excel(full_path, nrows=max_rows + 1)
        else:
            return PreviewResponse(
                connection=connection,
                path=path,
                rows=[],
                schema=SchemaResponse(columns=[], row_count=None, partition_columns=[]),
                truncated=False,
                total_rows=None,
            )

        truncated = len(df) > max_rows
        df = df.head(max_rows)

        # Build schema
        columns = []
        for col in df.columns:
            columns.append(
                ColumnSpec(
                    name=col,
                    dtype=str(df[col].dtype),
                    nullable=df[col].isnull().any(),
                )
            )

        # Convert to records
        rows = df.fillna("").to_dict(orient="records")

        return PreviewResponse(
            connection=connection,
            path=path,
            rows=rows,
            schema=SchemaResponse(columns=columns, row_count=len(df), partition_columns=[]),
            truncated=truncated,
            total_rows=None,
        )

    except Exception:
        logger.exception(f"Error previewing source: {connection}/{path}")
        return PreviewResponse(
            connection=connection,
            path=path,
            rows=[],
            schema=SchemaResponse(columns=[], row_count=None, partition_columns=[]),
            truncated=False,
            total_rows=None,
        )
