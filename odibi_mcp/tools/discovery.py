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
    error: Optional[str] = None


@dataclass
class ListSheetsResponse:
    """Response for list_sheets (Excel files)."""

    connection: str
    path: str
    sheets: List[str]
    error: Optional[str] = None


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
    sheet: Optional[str] = None,
    limits: Optional[DiscoveryLimits] = None,
) -> PreviewResponse:
    """
    Preview rows from a source file.

    Reads actual data from the connection.

    Args:
        connection: Connection name from config
        path: Path to the file within the connection
        format: Override format detection (csv, parquet, json, xlsx)
        max_rows: Maximum rows to return (default 100)
        sheet: For Excel files, specify the sheet name to read
        limits: Discovery limits configuration
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
            # Excel reading with optional sheet name
            excel_kwargs = {"nrows": max_rows + 1, "engine": "openpyxl"}
            if sheet:
                excel_kwargs["sheet_name"] = sheet

            if is_cloud:
                import fsspec

                with fsspec.open(full_path, mode="rb", **storage_options) as f:
                    df = pd.read_excel(f, **excel_kwargs)
            else:
                df = pd.read_excel(full_path, **excel_kwargs)
        else:
            return PreviewResponse(
                connection=connection,
                path=path,
                rows=[],
                schema=SchemaResponse(columns=[], row_count=None, partition_columns=[]),
                truncated=False,
                total_rows=None,
                error=f"Unsupported format: {suffix}. Supported: csv, parquet, json, xlsx, xls",
            )

        # Check if we got an empty dataframe
        if df.empty:
            return PreviewResponse(
                connection=connection,
                path=path,
                rows=[],
                schema=SchemaResponse(columns=[], row_count=0, partition_columns=[]),
                truncated=False,
                total_rows=0,
                error="No data found. For Excel files, try specifying sheet parameter. "
                "Use list_sheets() to discover available sheets.",
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

    except ImportError as e:
        logger.exception(f"Missing dependency for {connection}/{path}: {e}")
        return PreviewResponse(
            connection=connection,
            path=path,
            rows=[],
            schema=SchemaResponse(columns=[], row_count=None, partition_columns=[]),
            truncated=False,
            total_rows=None,
            error=f"Missing dependency: {e}. Install with: pip install openpyxl",
        )
    except Exception as e:
        logger.exception(f"Error previewing source: {connection}/{path}")
        return PreviewResponse(
            connection=connection,
            path=path,
            rows=[],
            schema=SchemaResponse(columns=[], row_count=None, partition_columns=[]),
            truncated=False,
            total_rows=None,
            error=str(e),
        )


def list_sheets(
    connection: str,
    path: str,
) -> ListSheetsResponse:
    """
    List all sheet names in an Excel file.

    Use this to discover available sheets before calling preview_source with sheet parameter.

    Args:
        connection: Connection name from config
        path: Path to the Excel file (.xlsx or .xls)

    Returns:
        ListSheetsResponse with list of sheet names
    """
    ctx = get_project_context()
    if not ctx:
        return ListSheetsResponse(
            connection=connection,
            path=path,
            sheets=[],
            error="No project context. Set ODIBI_CONFIG environment variable.",
        )

    try:
        conn = ctx.get_connection(connection)
        full_path = conn.get_path(path)

        # Check extension
        suffix = "." + path.split(".")[-1].lower() if "." in path else ""
        if suffix not in (".xlsx", ".xls"):
            return ListSheetsResponse(
                connection=connection,
                path=path,
                sheets=[],
                error=f"Not an Excel file: {suffix}. Expected .xlsx or .xls",
            )

        import pandas as pd

        # Get storage options for cloud connections
        storage_options = {}
        if hasattr(conn, "pandas_storage_options"):
            storage_options = conn.pandas_storage_options()

        is_cloud = _is_cloud_connection(conn)

        if is_cloud:
            import fsspec

            with fsspec.open(full_path, mode="rb", **storage_options) as f:
                excel_file = pd.ExcelFile(f, engine="openpyxl")
                sheets = excel_file.sheet_names
        else:
            excel_file = pd.ExcelFile(full_path, engine="openpyxl")
            sheets = excel_file.sheet_names

        return ListSheetsResponse(
            connection=connection,
            path=path,
            sheets=sheets,
        )

    except ImportError as e:
        return ListSheetsResponse(
            connection=connection,
            path=path,
            sheets=[],
            error=f"Missing dependency: {e}. Install with: pip install openpyxl",
        )
    except Exception as e:
        logger.exception(f"Error listing sheets: {connection}/{path}")
        return ListSheetsResponse(
            connection=connection,
            path=path,
            sheets=[],
            error=str(e),
        )


@dataclass
class TableDiscoveryInfo:
    """Detailed info about a discovered table."""

    name: str
    schema_name: str
    row_count: Optional[int]
    columns: List[ColumnSpec]
    sample_rows: List[dict]


@dataclass
class DiscoverDatabaseResponse:
    """Response for discover_database."""

    connection: str
    schema_name: str
    tables: List[TableDiscoveryInfo]
    total_tables: int
    error: Optional[str] = None


def discover_database(
    connection: str,
    schema: str = "dbo",
    max_tables: int = 50,
    sample_rows: int = 5,
) -> DiscoverDatabaseResponse:
    """
    Discover all tables in a SQL database with schemas and samples.

    Crawls the database to provide comprehensive overview:
    - Lists all tables in the schema
    - Gets column info for each table
    - Samples rows from each table

    Use for initial data exploration before building pipelines.
    """
    ctx = get_project_context()
    if not ctx:
        return DiscoverDatabaseResponse(
            connection=connection,
            schema_name=schema,
            tables=[],
            total_tables=0,
            error="No project context available",
        )

    try:
        conn = ctx.get_connection(connection)

        if not hasattr(conn, "execute_query"):
            return DiscoverDatabaseResponse(
                connection=connection,
                schema_name=schema,
                tables=[],
                total_tables=0,
                error=f"Connection '{connection}' is not a SQL database",
            )

        # Get all tables
        tables_query = f"""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}'
        AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
        table_rows = conn.execute_query(tables_query)
        table_names = [
            row[0] if isinstance(row, tuple) else row.get("TABLE_NAME") for row in table_rows
        ]

        total_tables = len(table_names)
        table_names = table_names[:max_tables]

        discovered_tables = []

        for table_name in table_names:
            try:
                # Get columns
                cols_query = f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}'
                ORDER BY ORDINAL_POSITION
                """
                col_rows = conn.execute_query(cols_query)

                columns = []
                for col_row in col_rows:
                    if isinstance(col_row, tuple):
                        col_name, col_type, nullable = col_row
                    else:
                        col_name = col_row.get("COLUMN_NAME")
                        col_type = col_row.get("DATA_TYPE")
                        nullable = col_row.get("IS_NULLABLE")

                    columns.append(
                        ColumnSpec(
                            name=col_name,
                            dtype=col_type,
                            nullable=nullable == "YES",
                        )
                    )

                # Get row count
                count_query = f"SELECT COUNT(*) FROM [{schema}].[{table_name}]"
                count_result = conn.execute_query(count_query)
                row_count = (
                    count_result[0][0]
                    if count_result and isinstance(count_result[0], tuple)
                    else count_result[0].get("", 0)
                    if count_result
                    else 0
                )

                # Get sample rows
                sample_query = f"SELECT TOP {sample_rows} * FROM [{schema}].[{table_name}]"
                sample_result = conn.execute_query(sample_query)

                # Convert to dicts
                col_names = [c.name for c in columns]
                samples = []
                for row in sample_result[:sample_rows]:
                    if isinstance(row, tuple):
                        samples.append(dict(zip(col_names, row)))
                    else:
                        samples.append(dict(row))

                discovered_tables.append(
                    TableDiscoveryInfo(
                        name=table_name,
                        schema_name=schema,
                        row_count=row_count,
                        columns=columns,
                        sample_rows=samples,
                    )
                )

            except Exception as e:
                logger.warning(f"Error discovering table {table_name}: {e}")
                discovered_tables.append(
                    TableDiscoveryInfo(
                        name=table_name,
                        schema_name=schema,
                        row_count=None,
                        columns=[],
                        sample_rows=[],
                    )
                )

        return DiscoverDatabaseResponse(
            connection=connection,
            schema_name=schema,
            tables=discovered_tables,
            total_tables=total_tables,
        )

    except Exception as e:
        logger.exception(f"Error discovering database: {connection}")
        return DiscoverDatabaseResponse(
            connection=connection,
            schema_name=schema,
            tables=[],
            total_tables=0,
            error=str(e),
        )


@dataclass
class FileDiscoveryInfo:
    """Detailed info about a discovered file."""

    path: str
    format: str
    size_bytes: Optional[int]
    columns: List[ColumnSpec]
    sample_rows: List[dict]
    row_count: Optional[int] = None


@dataclass
class DiscoverStorageResponse:
    """Response for discover_storage."""

    connection: str
    base_path: str
    files: List[FileDiscoveryInfo]
    total_files: int
    by_format: dict  # {"csv": 5, "parquet": 3, ...}
    error: Optional[str] = None


def discover_storage(
    connection: str,
    path: str = "",
    pattern: str = "*",
    max_files: int = 20,
    sample_rows: int = 5,
) -> DiscoverStorageResponse:
    """
    Discover files in storage with schemas and samples.

    Crawls the storage path to provide comprehensive overview:
    - Lists all matching files
    - Infers schema for each file
    - Samples rows from each file

    Use for initial data exploration before building pipelines.
    """
    ctx = get_project_context()
    if not ctx:
        return DiscoverStorageResponse(
            connection=connection,
            base_path=path,
            files=[],
            total_files=0,
            by_format={},
            error="No project context available",
        )

    try:
        # Verify connection exists
        ctx.get_connection(connection)

        # List files first
        files_response = list_files(
            connection=connection,
            path=path,
            pattern=pattern,
            limits=DiscoveryLimits(max_files_per_request=max_files * 2),
        )

        if not files_response.files:
            return DiscoverStorageResponse(
                connection=connection,
                base_path=path,
                files=[],
                total_files=0,
                by_format={},
            )

        # Filter to supported formats
        supported_formats = {"csv", "parquet", "json", "xlsx", "xls"}
        data_files = [
            f
            for f in files_response.files
            if any(f.name.lower().endswith(f".{fmt}") for fmt in supported_formats)
        ]

        total_files = len(data_files)
        data_files = data_files[:max_files]

        discovered_files = []
        format_counts: dict = {}

        for file_info in data_files:
            file_path = file_info.path or file_info.name
            file_ext = Path(file_info.name).suffix.lower().lstrip(".")

            format_counts[file_ext] = format_counts.get(file_ext, 0) + 1

            try:
                # Infer schema
                schema_resp = infer_schema(connection=connection, path=file_path)

                # Preview data
                preview_resp = preview_source(
                    connection=connection,
                    path=file_path,
                    limit=sample_rows,
                )

                discovered_files.append(
                    FileDiscoveryInfo(
                        path=file_path,
                        format=file_ext,
                        size_bytes=file_info.size_bytes,
                        columns=schema_resp.columns,
                        sample_rows=preview_resp.rows[:sample_rows],
                        row_count=preview_resp.total_rows,
                    )
                )

            except Exception as e:
                logger.warning(f"Error discovering file {file_path}: {e}")
                discovered_files.append(
                    FileDiscoveryInfo(
                        path=file_path,
                        format=file_ext,
                        size_bytes=file_info.size_bytes,
                        columns=[],
                        sample_rows=[],
                    )
                )

        return DiscoverStorageResponse(
            connection=connection,
            base_path=path,
            files=discovered_files,
            total_files=total_files,
            by_format=format_counts,
        )

    except Exception as e:
        logger.exception(f"Error discovering storage: {connection}/{path}")
        return DiscoverStorageResponse(
            connection=connection,
            base_path=path,
            files=[],
            total_files=0,
            by_format={},
            error=str(e),
        )
