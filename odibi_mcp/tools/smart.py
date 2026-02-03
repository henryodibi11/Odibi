# odibi_mcp/tools/smart.py
"""Smart discovery tools for AI-assisted data onboarding.

These tools follow the SmartResponse pattern with:
- Self-correcting behavior (iterate until data looks right)
- Confidence scores
- Next step suggestions
- Ready-for parameters for chaining
"""

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from odibi_mcp.context import get_project_context
from odibi_mcp.contracts.smart import (
    ColumnInfo,
    DecisionHint,
    FileOptions,
    FileProfileSummary,
    FixInstruction,
    FolderInfo,
    GenerateBronzeNodeResponse,
    MapEnvironmentResponse,
    OdibiSuggestions,
    OptionsGroup,
    ProfileFolderResponse,
    ProfileSourceResponse,
    SchemaInfo,
    TestNodeResponse,
    TransformerSuggestion,
    ValidationResult,
)

logger = logging.getLogger(__name__)

# Mojibake detection pattern
MOJIBAKE_PATTERN = re.compile(r"[�Ã¢â€™" "¿]+|\\xc3|\\xe2|Ã©|Ã¨|Ã |Ã¼")

# Common delimiters to try
DELIMITERS = [",", "\t", "|", ";", ":"]

# Common encodings to try
ENCODINGS = ["utf-8", "windows-1252", "latin-1", "iso-8859-1", "cp1252"]


def _is_storage_connection(conn) -> bool:
    """Check if connection is a storage connection."""
    conn_type = type(conn).__name__
    return conn_type in ("LocalConnection", "AzureADLS", "S3Connection", "GCSConnection")


def _is_sql_connection(conn) -> bool:
    """Check if connection is a SQL connection."""
    conn_type = type(conn).__name__
    return conn_type in ("AzureSQLConnection", "SQLServerConnection", "SQLConnection", "AzureSQL")


def _detect_pattern(file_names: List[str]) -> Optional[str]:
    """Detect naming patterns in file names."""
    if not file_names:
        return None

    # Check for date patterns
    date_patterns = [
        (r"\d{4}[-/]\d{2}[-/]\d{2}", "date partitioned (YYYY-MM-DD)"),
        (r"\d{8}", "date suffix (YYYYMMDD)"),
        (r"\d{4}[-/]\d{2}", "monthly (YYYY-MM)"),
    ]

    for pattern, description in date_patterns:
        matches = sum(1 for f in file_names if re.search(pattern, f))
        if matches > len(file_names) * 0.5:
            return description

    # Check for common prefixes
    if len(file_names) > 1:
        prefix = file_names[0]
        for name in file_names[1:]:
            while prefix and not name.startswith(prefix):
                prefix = prefix[:-1]
        if len(prefix) > 3:
            return f"common prefix: {prefix}*"

    return None


def _detect_encoding(raw_bytes: bytes) -> Tuple[str, float]:
    """Detect encoding from raw bytes."""
    try:
        from charset_normalizer import from_bytes

        result = from_bytes(raw_bytes)
        best = result.best()
        if best:
            # charset_normalizer uses different attribute names in different versions
            confidence = getattr(best, "encoding_confidence", None) or getattr(
                best, "coherence", 0.5
            )
            return best.encoding, confidence
    except ImportError:
        pass

    try:
        import chardet

        result = chardet.detect(raw_bytes)
        if result and result.get("encoding"):
            return result["encoding"], result.get("confidence", 0.5)
    except ImportError:
        pass

    # Fallback: try UTF-8, then windows-1252
    try:
        raw_bytes.decode("utf-8")
        return "utf-8", 0.7
    except UnicodeDecodeError:
        return "windows-1252", 0.5


def _detect_delimiter(lines: List[str]) -> Tuple[str, float]:
    """Detect delimiter by checking column count consistency."""
    best_delimiter = ","
    best_score = 0.0

    for delim in DELIMITERS:
        counts = [len(line.split(delim)) for line in lines if line.strip()]
        if not counts:
            continue

        # Check consistency: most common count should be dominant
        from collections import Counter

        count_freq = Counter(counts)
        most_common_count, most_common_freq = count_freq.most_common(1)[0]

        if most_common_count < 2:
            continue

        consistency = most_common_freq / len(counts)
        # Prefer more columns (less likely to be accidental)
        score = consistency * (1 + most_common_count / 20)

        if score > best_score:
            best_score = score
            best_delimiter = delim

    return best_delimiter, min(best_score, 1.0)


def _detect_skip_rows(lines: List[str], delimiter: str) -> int:
    """Detect how many header/metadata rows to skip."""
    if not lines:
        return 0

    # Find the first row that looks like a proper header
    # (multiple columns, consistent with data rows below)
    best_header_row = 0
    best_col_count = 0

    for i, line in enumerate(lines[:20]):  # Check first 20 lines
        if not line.strip():
            continue

        cols = len(line.split(delimiter))

        # Skip rows with very few columns (likely metadata)
        if cols < 3:
            continue

        # Check if next non-empty rows have similar column count
        next_lines = [ln for ln in lines[i + 1 : i + 10] if ln.strip()]
        if not next_lines:
            continue

        next_counts = [len(ln.split(delimiter)) for ln in next_lines]

        # Count how many following rows have same or similar column count
        matching = sum(1 for c in next_counts if abs(c - cols) <= 1)

        # If most following rows match, this is likely the header
        if matching >= len(next_counts) * 0.7 and cols > best_col_count:
            best_header_row = i
            best_col_count = cols

    return best_header_row


def _detect_mojibake(text: str) -> bool:
    """Check if text contains mojibake characters."""
    return bool(MOJIBAKE_PATTERN.search(text))


def _compute_cardinality(distinct_count: int, total_count: int) -> str:
    """Compute cardinality category from distinct/total ratio."""
    if total_count == 0:
        return "unknown"
    ratio = distinct_count / total_count
    if ratio > 0.99:
        return "unique"
    elif ratio > 0.5:
        return "high"
    elif ratio > 0.1:
        return "medium"
    else:
        return "low"


def _detect_column_pattern(col_name: str, dtype: str, sample_values: List[Any]) -> Optional[str]:
    """Detect common patterns in column data."""
    dtype_lower = dtype.lower()

    # Date patterns
    if "date" in dtype_lower or "timestamp" in dtype_lower:
        return f"datetime:{dtype_lower}"

    # Check VARCHAR columns for date-like strings
    if "varchar" in dtype_lower or dtype_lower == "object":
        if sample_values:
            sample_str = str(sample_values[0]) if sample_values[0] else ""
            # ISO date pattern
            if re.match(r"^\d{4}-\d{2}-\d{2}$", sample_str):
                return "date:YYYY-MM-DD"
            if re.match(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}", sample_str):
                return "timestamp:ISO"
            # US date
            if re.match(r"^\d{2}/\d{2}/\d{4}$", sample_str):
                return "date:MM/DD/YYYY"
            # EU date
            if re.match(r"^\d{2}\.\d{2}\.\d{4}$", sample_str):
                return "date:DD.MM.YYYY"
            # Email
            if re.match(r"^[\w.-]+@[\w.-]+\.\w+$", sample_str):
                return "email"
            # UUID
            if re.match(
                r"^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$",
                sample_str.lower(),
            ):
                return "uuid"

    return None


def _suggest_transform_for_column(
    col_name: str, dtype: str, pattern: Optional[str]
) -> Optional[str]:
    """Suggest a transformer based on column characteristics."""
    # Columns with spaces need renaming
    if " " in col_name or not col_name.replace("_", "").isalnum():
        return "rename_columns"

    # Unnamed columns from pandas
    if col_name.startswith("Unnamed:") or col_name.startswith("_c"):
        return "rename_columns"

    # String dates should be cast
    if pattern and pattern.startswith("date:") and "varchar" in dtype.lower():
        return "cast_columns"

    # String timestamps should be cast
    if pattern and pattern.startswith("timestamp:") and "varchar" in dtype.lower():
        return "cast_columns"

    return None


def _generate_odibi_suggestions(
    schema: List[ColumnInfo],
    source_type: str,
    path: str,
    candidate_keys: List[str],
    candidate_watermarks: List[str],
) -> OdibiSuggestions:
    """Generate Odibi-aware suggestions based on profile analysis."""
    suggestions = OdibiSuggestions()
    col_names = [c.name.lower() for c in schema]
    path_lower = path.lower()

    # Detect pattern based on naming conventions and columns
    if any(x in path_lower for x in ["dim_", "dimension", "lookup", "master"]):
        suggestions.suggested_pattern = "dimension"
        suggestions.pattern_reason = "File/table name suggests dimension table"
    elif any(x in path_lower for x in ["fact_", "transaction", "event", "log"]):
        suggestions.suggested_pattern = "fact"
        suggestions.pattern_reason = "File/table name suggests fact table"
    elif any(
        col in col_names for col in ["valid_from", "valid_to", "effective_date", "expiry_date"]
    ):
        suggestions.suggested_pattern = "scd2"
        suggestions.pattern_reason = "Contains SCD2-style validity columns"
    elif any(
        col in col_names for col in ["created_at", "updated_at", "modified_date", "last_modified"]
    ):
        suggestions.suggested_pattern = "fact"
        suggestions.pattern_reason = "Contains audit timestamp columns"

    # Collect transformers
    transformer_suggestions: List[TransformerSuggestion] = []

    # Check for columns needing rename
    rename_cols = [c.name for c in schema if c.suggested_transform == "rename_columns"]
    if rename_cols:
        transformer_suggestions.append(
            TransformerSuggestion(
                transformer="rename_columns",
                reason="Columns have spaces or special characters",
                columns=rename_cols,
            )
        )

    # Check for columns needing cast
    cast_cols = [c.name for c in schema if c.suggested_transform == "cast_columns"]
    if cast_cols:
        transformer_suggestions.append(
            TransformerSuggestion(
                transformer="cast_columns",
                reason="String columns contain date/timestamp values",
                columns=cast_cols,
            )
        )

    # Check for unnamed columns (suggest drop or rename)
    unnamed_cols = [c.name for c in schema if c.name.startswith("Unnamed:")]
    if unnamed_cols:
        transformer_suggestions.append(
            TransformerSuggestion(
                transformer="drop_columns",
                reason="Unnamed columns (likely index) - consider dropping",
                columns=unnamed_cols,
            )
        )

    suggestions.suggested_transformers = transformer_suggestions

    # Incremental loading hint
    for wm in candidate_watermarks:
        for c in schema:
            if c.name == wm:
                suggestions.incremental_hint = {
                    "column": wm,
                    "type": c.dtype,
                }
                break
        if suggestions.incremental_hint:
            break

    # Partition hints (low cardinality columns)
    partition_candidates = [
        c.name
        for c in schema
        if c.cardinality == "low" and c.dtype.lower() not in ("float", "double", "decimal")
    ]
    if partition_candidates:
        suggestions.partition_hint = partition_candidates[:3]

    # Decision hints for ambiguous cases
    decisions: List[DecisionHint] = []

    # Multiple date columns - which is the watermark?
    if len(candidate_watermarks) > 1:
        decisions.append(
            DecisionHint(
                question="Multiple timestamp columns found. Which should be the watermark for incremental loading?",
                options=candidate_watermarks,
                default=candidate_watermarks[0],
            )
        )

    # Multiple ID columns
    if len(candidate_keys) > 1:
        decisions.append(
            DecisionHint(
                question="Multiple ID columns found. Which is the primary key?",
                options=candidate_keys,
                default=candidate_keys[0],
            )
        )

    # Unnamed columns
    if unnamed_cols:
        decisions.append(
            DecisionHint(
                question=f"Found {len(unnamed_cols)} unnamed columns. Keep or drop?",
                options=["drop", "rename to _c0, _c1, ...", "keep as-is"],
                default="drop",
            )
        )

    suggestions.decisions_needed = decisions

    return suggestions


def _validate_dataframe(df, sample_text: str = "") -> ValidationResult:
    """Validate a parsed dataframe for quality issues."""

    # Column count consistency (already parsed, so should be fine)
    col_consistent = True

    # Mojibake detection
    mojibake = False
    for col in df.columns:
        if _detect_mojibake(str(col)):
            mojibake = True
            break
    if not mojibake:
        sample_str = df.head(10).to_string()
        mojibake = _detect_mojibake(sample_str)

    # Empty columns
    empty_cols = [col for col in df.columns if df[col].isna().all()]

    # Type consistency
    type_consistency = {}
    for col in df.columns:
        non_null = df[col].dropna()
        if len(non_null) == 0:
            type_consistency[col] = 0.0
        else:
            # Check if values are consistent type
            types = non_null.apply(lambda x: type(x).__name__)
            from collections import Counter

            type_counts = Counter(types)
            most_common = type_counts.most_common(1)[0][1]
            type_consistency[col] = most_common / len(types)

    return ValidationResult(
        column_count_consistent=col_consistent,
        mojibake_detected=mojibake,
        empty_columns=empty_cols,
        type_consistency=type_consistency,
    )


def map_environment(connection: str | dict, path: str = "") -> MapEnvironmentResponse:
    """
    Scout a connection to understand what exists.

    For storage: scans folders, detects file patterns, groups by format.
    For SQL: lists schemas, tables, row counts.

    Args:
        connection: Either a connection name (str) or inline spec (dict).
            Inline spec example:
            {
                "type": "azure_sql",
                "server": "myserver.database.windows.net",
                "database": "MyDB",
                "driver": "ODBC Driver 17 for SQL Server",
                "username": "${SQL_USER}",
                "password": "${SQL_PASSWORD}"
            }
        path: Optional path to scan (for storage connections)
    """
    from odibi_mcp.context import resolve_connection

    conn_name = connection if isinstance(connection, str) else "inline"

    try:
        conn, conn_name = resolve_connection(connection)
    except Exception as e:
        return MapEnvironmentResponse(
            connection=conn_name,
            connection_type="unknown",
            scanned_at=datetime.now(),
            summary={},
            structure=[],
            errors=[f"Connection error: {e}"],
        )

    scanned_at = datetime.now()

    if _is_storage_connection(conn):
        return _map_storage(connection, conn, path, scanned_at)
    elif _is_sql_connection(conn):
        return _map_sql(connection, conn, scanned_at)
    else:
        return MapEnvironmentResponse(
            connection=connection,
            connection_type=type(conn).__name__,
            scanned_at=scanned_at,
            summary={},
            structure=[],
            errors=[f"Unsupported connection type: {type(conn).__name__}"],
        )


def _map_storage(connection: str, conn, path: str, scanned_at: datetime) -> MapEnvironmentResponse:
    """Map a storage connection."""
    import fsspec

    try:
        storage_options = {}
        if hasattr(conn, "pandas_storage_options"):
            storage_options = conn.pandas_storage_options()

        full_path = conn.get_path(path) if path else conn.get_path("")

        # Determine filesystem
        if full_path.startswith(("abfss://", "abfs://", "az://")):
            fs = fsspec.filesystem("abfs", **storage_options)
            # Normalize path for fsspec
            if "://" in full_path:
                fs_path = full_path.split("://", 1)[1]
                if "@" in fs_path.split("/")[0]:
                    parts = fs_path.split("/", 1)
                    container = parts[0].split("@")[0]
                    rest = parts[1] if len(parts) > 1 else ""
                    fs_path = f"{container}/{rest}"
            else:
                fs_path = full_path
        else:
            fs = fsspec.filesystem("file")
            fs_path = full_path

        # List items at this level
        try:
            items = fs.ls(fs_path, detail=True)
        except Exception as e:
            return MapEnvironmentResponse(
                connection=connection,
                connection_type="storage",
                scanned_at=scanned_at,
                summary={},
                structure=[],
                errors=[f"Failed to list path: {e}"],
            )

        # Track folders and subfolders separately
        folders: Dict[str, List[str]] = {}  # folder -> list of files
        subfolders: Dict[str, List[str]] = {}  # folder -> list of subfolder names

        for item in items:
            name = item.get("name", "").split("/")[-1]
            item_type = item.get("type", "file")

            if item_type == "directory":
                # Scan subdirectory
                try:
                    sub_items = fs.ls(item["name"], detail=True)
                    sub_files = []
                    sub_dirs = []
                    for si in sub_items:
                        si_name = si.get("name", "").split("/")[-1]
                        if si.get("type") == "directory":
                            sub_dirs.append(si_name)
                        else:
                            sub_files.append(si_name)
                    folders[name] = sub_files
                    if sub_dirs:
                        subfolders[name] = sub_dirs
                except Exception:
                    folders[name] = []
            else:
                if "" not in folders:
                    folders[""] = []
                folders[""].append(name)

        # Build structure
        structure = []
        total_files = 0
        format_counts: Dict[str, int] = {}
        suggested_sources = []

        for folder_path, files in folders.items():
            file_count = len(files)
            total_files += file_count

            # Count formats
            folder_formats: Dict[str, int] = {}
            for f in files:
                ext = Path(f).suffix.lower().lstrip(".")
                if ext:
                    folder_formats[ext] = folder_formats.get(ext, 0) + 1
                    format_counts[ext] = format_counts.get(ext, 0) + 1

            # Detect pattern
            pattern = _detect_pattern(files)

            # Get subfolder info for this folder
            folder_subfolders = subfolders.get(folder_path, [])

            folder_info = FolderInfo(
                path=folder_path or "(root)",
                file_count=file_count,
                pattern=pattern,
                formats=folder_formats,
                sample_files=files[:5],
            )
            # Add subfolder info if present (FolderInfo may not have this field, add to sample_files note)
            if folder_subfolders and file_count == 0:
                # Override sample_files to show subfolders when no files
                folder_info.sample_files = [f"[FOLDER] {sf}/" for sf in folder_subfolders[:5]]
                folder_info.pattern = f"contains {len(folder_subfolders)} subfolder(s) - drill deeper with map_environment"
            structure.append(folder_info)

            # Suggest first file of each format
            for f in files[:3]:
                full_file_path = f"{path}/{folder_path}/{f}" if folder_path else f"{path}/{f}"
                full_file_path = full_file_path.strip("/")
                if full_file_path not in suggested_sources:
                    suggested_sources.append(full_file_path)

            # If folder has subfolders but no files, suggest drilling into first subfolder
            if folder_subfolders and file_count == 0:
                for sf in folder_subfolders[:2]:
                    drill_path = f"{path}/{folder_path}/{sf}" if folder_path else f"{path}/{sf}"
                    drill_path = drill_path.strip("/")
                    if drill_path not in suggested_sources:
                        suggested_sources.append(drill_path)

        # Generate recommendations
        recommendations = []
        if format_counts.get("csv", 0) > 5:
            recommendations.append(
                f"Found {format_counts['csv']} CSV files - consider batch profiling"
            )
        if format_counts.get("parquet", 0) > 0:
            recommendations.append("Parquet files detected - schema is embedded, simpler to ingest")
        if any(fi.pattern and "partition" in (fi.pattern or "").lower() for fi in structure):
            recommendations.append("Date partitioning detected - configure incremental loading")

        return MapEnvironmentResponse(
            connection=connection,
            connection_type="storage",
            scanned_at=scanned_at,
            summary={
                "total_files": total_files,
                "total_folders": len(folders),
                "by_format": format_counts,
            },
            structure=structure,
            recommendations=recommendations
            + [
                "WORKFLOW: Call profile_source for each file you want to ingest, then generate_bronze_node, then test_node",
                f"START WITH: profile_source(connection='{connection}', path='{suggested_sources[0] if suggested_sources else ''}')",
            ],
            next_step="profile_source",
            suggested_sources=suggested_sources[:10],
            errors=[],
        )

    except Exception as e:
        logger.exception(f"Error mapping storage: {connection}")
        return MapEnvironmentResponse(
            connection=connection,
            connection_type="storage",
            scanned_at=scanned_at,
            summary={},
            structure=[],
            errors=[str(e)],
        )


def _map_sql(connection: str, conn, scanned_at: datetime) -> MapEnvironmentResponse:
    """Map a SQL connection."""
    try:
        # Query schemas and table counts
        query = """
        SELECT
            TABLE_SCHEMA,
            COUNT(*) as table_count
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        GROUP BY TABLE_SCHEMA
        ORDER BY table_count DESC
        """

        # Try different execute methods based on connection type
        if hasattr(conn, "execute_sql"):
            result = conn.execute_sql(query)
        elif hasattr(conn, "execute"):
                        result = conn.execute(query)
        elif hasattr(conn, "read_sql_query"):
            df = conn.read_sql_query(query)
            result = df.values.tolist()
        else:
            raise AttributeError("No suitable execute method found")

        structure = []
        suggested_sources = []
        total_tables = 0

        for row in result:
            if isinstance(row, (tuple, list)):
                schema_name, table_count = row[0], row[1]
            elif hasattr(row, "get"):
                schema_name = row.get("TABLE_SCHEMA")
                table_count = row.get("table_count")
            else:
                # pyodbc.Row or similar - access by index
                schema_name, table_count = row[0], row[1]

            total_tables += table_count

            # Get sample tables
            tables_query = f"""
            SELECT TOP 5 TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema_name}'
            AND TABLE_TYPE = 'BASE TABLE'
            """
            if hasattr(conn, "execute_sql"):
                tables_result = conn.execute_sql(tables_query)
            elif hasattr(conn, "execute"):
                tables_result = conn.execute(tables_query)
            elif hasattr(conn, "read_sql_query"):
                tables_result = conn.read_sql_query(tables_query).values.tolist()
            else:
                tables_result = []
            sample_tables = []
            for r in tables_result:
                if isinstance(r, (tuple, list)):
                    sample_tables.append(r[0])
                elif hasattr(r, "get"):
                    sample_tables.append(r.get("TABLE_NAME"))
                else:
                    sample_tables.append(r[0])

            schema_info = SchemaInfo(
                name=schema_name,
                table_count=table_count,
                sample_tables=sample_tables,
            )
            structure.append(schema_info)

            # Add to suggested sources
            for t in sample_tables[:2]:
                suggested_sources.append(f"{schema_name}.{t}")

        recommendations = []
        if total_tables > 50:
            recommendations.append(
                f"Large database with {total_tables} tables - prioritize key entities"
            )

        return MapEnvironmentResponse(
            connection=connection,
            connection_type="sql",
            scanned_at=scanned_at,
            summary={
                "total_tables": total_tables,
                "total_schemas": len(structure),
            },
            structure=structure,
            recommendations=recommendations,
            next_step="profile_source",
            suggested_sources=suggested_sources[:10],
            errors=[],
        )

    except Exception as e:
        logger.exception(f"Error mapping SQL: {connection}")
        return MapEnvironmentResponse(
            connection=connection,
            connection_type="sql",
            scanned_at=scanned_at,
            summary={},
            structure=[],
            errors=[str(e)],
        )


def profile_source(connection: str | dict, path: str, max_attempts: int = 5) -> ProfileSourceResponse:
    """
    Self-correcting profiler that figures out how to read a source.

    Iterates through encoding/delimiter combinations until data looks right.

    Args:
        connection: Either a connection name (str) or inline spec (dict).
            Inline spec example:
            {
                "type": "azure_sql",
                "server": "myserver.database.windows.net",
                "database": "MyDB",
                "driver": "ODBC Driver 17 for SQL Server",
                "username": "${SQL_USER}",
                "password": "${SQL_PASSWORD}"
            }
        path: Path to the source (e.g., "schema.table" for SQL, "folder/file.csv" for storage)
        max_attempts: Max attempts for CSV encoding/delimiter detection
    """
    from odibi_mcp.context import resolve_connection

    conn_name = connection if isinstance(connection, str) else "inline"

    try:
        conn, conn_name = resolve_connection(connection)
    except Exception as e:
        return ProfileSourceResponse(
            connection=conn_name,
            path=path,
            source_type="unknown",
            errors=[f"Connection error: {e}"],
        )

    if _is_sql_connection(conn):
        return _profile_sql_table(conn_name, conn, path)
    else:
        return _profile_file(conn_name, conn, path, max_attempts)


def _profile_file(connection: str, conn, path: str, max_attempts: int) -> ProfileSourceResponse:
    """Profile a file source using DuckDB's sniff_csv for robust detection."""
    import pandas as pd

    # Determine file format
    suffix = Path(path).suffix.lower()

    if suffix == ".parquet":
        return _profile_parquet(connection, conn, path)
    elif suffix in (".xlsx", ".xls"):
        return _profile_excel(connection, conn, path)
    elif suffix in (".json", ".ndjson", ".jsonl"):
        return _profile_json(connection, conn, path)
    elif suffix == ".avro":
        return _profile_avro(connection, conn, path)

    # Check if path is a Delta table (has _delta_log folder)
    try:
        import fsspec

        storage_options = {}
        if hasattr(conn, "pandas_storage_options"):
            storage_options = conn.pandas_storage_options()

        full_path = conn.get_path(path)
        if full_path.startswith(("abfss://", "abfs://", "az://")):
            fs = fsspec.filesystem("abfs", **storage_options)
        else:
            fs = fsspec.filesystem("file")

        # Check for _delta_log
        delta_log_path = (
            f"{full_path}/_delta_log" if not full_path.endswith("/") else f"{full_path}_delta_log"
        )
        if fs.exists(delta_log_path):
            return _profile_delta(connection, conn, path)
    except Exception:
        pass  # Not a delta table, continue with CSV

    # CSV/text file - use DuckDB sniff_csv for detection
    try:
        storage_options = {}
        if hasattr(conn, "pandas_storage_options"):
            storage_options = conn.pandas_storage_options()

        full_path = conn.get_path(path)
        warnings = []

        # For cloud storage, download to temp file for DuckDB
        # DuckDB can read Azure directly but needs azure extension configured
        import tempfile
        import fsspec

        temp_file = None
        local_path = full_path

        if full_path.startswith(("abfss://", "abfs://", "az://")):
            fs = fsspec.filesystem("abfs", **storage_options)
            # Download to temp file for DuckDB to sniff
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix or ".csv")
            with fs.open(full_path, "rb") as remote_f:
                # Read first 1MB for sniffing (enough for header detection)
                content = remote_f.read(1024 * 1024)
                temp_file.write(content)
            temp_file.close()
            local_path = temp_file.name

        try:
            # Use DuckDB's sniff_csv for robust dialect detection
            import duckdb

            try:
                sniff_result = duckdb.sql(
                    f"SELECT * FROM sniff_csv('{local_path}', sample_size=10000)"
                ).fetchone()
            except Exception as duck_err:
                # DuckDB failed (encoding issues, malformed CSV, etc.)
                # Fall back to legacy detection
                logger.debug(f"DuckDB sniff failed, using legacy: {duck_err}")
                if temp_file:
                    import os

                    try:
                        os.unlink(temp_file.name)
                    except Exception:
                        pass
                return _profile_file_legacy(connection, conn, path, max_attempts)

            # sniff_csv returns:
            # (Delimiter, Quote, Escape, NewLineDelimiter, SkipRows, HasHeader,
            #  Columns, DateFormat, TimestampFormat, UserArguments, Prompt)
            detected_delimiter = sniff_result[0]
            detected_quote = sniff_result[1]
            detected_escape = sniff_result[2]
            skip_rows = sniff_result[4]
            has_header = sniff_result[5]
            columns_struct = sniff_result[6]  # List of {'name': str, 'type': str}
            date_format = sniff_result[7]
            timestamp_format = sniff_result[8]

            # Read raw bytes for encoding detection and validation
            with open(local_path, "rb") as f:
                raw_bytes = f.read(32768)  # 32KB for better validation

            # Detect encoding (DuckDB doesn't report encoding)
            detected_encoding, _ = _detect_encoding(raw_bytes)

            # Validate DuckDB's detection - check if result looks reasonable
            # SAP files often have metadata headers that confuse sniffers
            duckdb_looks_wrong = False

            # Check 1: If only 1 column detected, delimiter is probably wrong
            if len(columns_struct) <= 1:
                duckdb_looks_wrong = True
                warnings.append("DuckDB detected only 1 column - likely wrong delimiter")

            # Check 2: If column name is very long (>50 chars), probably parsed wrong
            if columns_struct and len(columns_struct[0].get("name", "")) > 50:
                duckdb_looks_wrong = True
                warnings.append("DuckDB column name too long - likely wrong parse")

            # Check 3: Try our own detection and compare
            text = raw_bytes.decode(detected_encoding, errors="replace")
            lines = text.split("\n")[:50]
            our_delimiter, _ = _detect_delimiter(lines)
            our_skip_rows = _detect_skip_rows(lines, our_delimiter)

            # If our detection differs significantly, prefer ours for SAP-style files
            if our_delimiter != detected_delimiter:
                # Count columns with each delimiter on data lines
                our_cols = (
                    len(lines[our_skip_rows].split(our_delimiter))
                    if our_skip_rows < len(lines)
                    else 0
                )
                duck_cols = len(columns_struct)

                if our_cols > duck_cols:
                    duckdb_looks_wrong = True
                    warnings.append(
                        f"Legacy detection found more columns ({our_cols} vs {duck_cols})"
                    )

            if duckdb_looks_wrong:
                # Fall back to legacy detection
                if temp_file:
                    import os

                    try:
                        os.unlink(temp_file.name)
                    except Exception:
                        pass
                return _profile_file_legacy(connection, conn, path, max_attempts)

            # Build file options
            file_options = FileOptions(
                encoding=detected_encoding,
                delimiter=detected_delimiter,
                quote_char=detected_quote if detected_quote else '"',
                skip_rows=skip_rows,
                header_row=0 if has_header else None,
            )

            # Extract column info from DuckDB's detection
            duckdb_schema = []
            for col_info in columns_struct:
                col_name = col_info["name"]
                col_type = col_info["type"]
                duckdb_schema.append({"name": col_name, "dtype": col_type})

            # Now read with pandas using detected options to get sample data
            df = pd.read_csv(
                local_path,
                encoding=detected_encoding,
                delimiter=detected_delimiter,
                skiprows=skip_rows,
                nrows=100,
                quotechar=detected_quote if detected_quote else '"',
                escapechar=detected_escape if detected_escape else None,
                header=0 if has_header else None,
            )

            # If no header, generate column names
            if not has_header and df.columns.tolist() == list(range(len(df.columns))):
                # Use DuckDB's detected column names
                if duckdb_schema:
                    df.columns = [c["name"] for c in duckdb_schema]

            validation = _validate_dataframe(df)

            # Calculate confidence - DuckDB is highly reliable
            confidence = 0.90
            if validation.mojibake_detected:
                confidence -= 0.15
                warnings.append("Mojibake detected - encoding may need adjustment")
            if len(validation.empty_columns) > 0:
                confidence -= 0.05

            # Build schema from actual data with enhanced column info
            schema = []
            candidate_keys = []
            candidate_watermarks = []
            total_count = len(df)

            for i, col in enumerate(df.columns):
                col_name = str(col)
                # Use DuckDB's type if available, else pandas dtype
                dtype = duckdb_schema[i]["dtype"] if i < len(duckdb_schema) else str(df[col].dtype)
                sample_vals = df[col].dropna().head(3).tolist()

                # Compute enhanced stats
                null_count = int(df[col].isna().sum())
                null_pct = null_count / total_count if total_count > 0 else 0
                distinct_count = int(df[col].nunique())
                cardinality = _compute_cardinality(distinct_count, total_count)

                # Detect patterns and suggest transforms
                pattern = _detect_column_pattern(col_name, dtype, sample_vals)
                suggested = _suggest_transform_for_column(col_name, dtype, pattern)

                schema.append(
                    ColumnInfo(
                        name=col_name,
                        dtype=dtype,
                        nullable=null_count > 0,
                        null_count=null_count,
                        null_pct=round(null_pct, 4),
                        cardinality=cardinality,
                        distinct_count=distinct_count,
                        sample_values=sample_vals,
                        detected_pattern=pattern,
                        suggested_transform=suggested,
                    )
                )

                # Detect candidate keys and watermarks
                dtype_lower = dtype.lower()
                name_lower = col_name.lower()
                if ("int" in dtype_lower or "bigint" in dtype_lower) and "id" in name_lower:
                    candidate_keys.append(col_name)
                if "date" in dtype_lower or "timestamp" in dtype_lower:
                    candidate_watermarks.append(col_name)

            sample_rows = df.head(10).fillna("").to_dict(orient="records")

            # Generate Odibi suggestions
            suggestions = _generate_odibi_suggestions(
                schema, "csv", path, candidate_keys, candidate_watermarks
            )

            return ProfileSourceResponse(
                connection=connection,
                path=path,
                source_type="csv",
                attempts=1,
                confidence=confidence,
                file_options=file_options,
                schema=schema,
                sample_rows=sample_rows,
                validation=validation,
                candidate_keys=candidate_keys,
                candidate_watermarks=candidate_watermarks,
                suggestions=suggestions,
                warnings=warnings,
                errors=[],
                next_step="generate_bronze_node",
                ready_for={
                    "connection": connection,
                    "path": path,
                    "source_type": "csv",
                    "options": {
                        "encoding": file_options.encoding,
                        "delimiter": file_options.delimiter,
                        "skipRows": file_options.skip_rows,
                        "header": str(has_header).lower(),
                        "quote": file_options.quote_char,
                    },
                    "schema": [{"name": c.name, "dtype": c.dtype} for c in schema],
                    "dateFormat": date_format,
                    "timestampFormat": timestamp_format,
                },
            )

        finally:
            # Clean up temp file
            if temp_file:
                import os

                try:
                    os.unlink(temp_file.name)
                except Exception:
                    pass

    except Exception:
        logger.exception(f"Error profiling file: {path}")
        # Fall back to legacy detection if DuckDB fails
        return _profile_file_legacy(connection, conn, path, max_attempts)


def _profile_file_legacy(
    connection: str, conn, path: str, max_attempts: int
) -> ProfileSourceResponse:
    """Legacy profile using custom detection (fallback if DuckDB fails)."""
    import pandas as pd

    try:
        storage_options = {}
        if hasattr(conn, "pandas_storage_options"):
            storage_options = conn.pandas_storage_options()

        full_path = conn.get_path(path)

        # Read raw bytes for detection
        import fsspec

        if full_path.startswith(("abfss://", "abfs://", "az://")):
            fs = fsspec.filesystem("abfs", **storage_options)
        else:
            fs = fsspec.filesystem("file")

        with fs.open(full_path, "rb") as f:
            raw_bytes = f.read(16384)  # 16KB sample

        # Detect encoding
        detected_encoding, encoding_conf = _detect_encoding(raw_bytes)

        # Decode and split into lines
        text = raw_bytes.decode(detected_encoding, errors="replace")
        lines = text.split("\n")[:50]

        # Detect delimiter
        detected_delimiter, delim_conf = _detect_delimiter(lines)

        # Detect skip rows
        skip_rows = _detect_skip_rows(lines, detected_delimiter)

        # Build initial options
        file_options = FileOptions(
            encoding=detected_encoding,
            delimiter=detected_delimiter,
            skip_rows=skip_rows,
            header_row=0,
        )

        # Try parsing with detected options
        best_result = None
        best_confidence = 0.0
        attempts = 0
        warnings = []

        for attempt in range(max_attempts):
            attempts = attempt + 1
            try:
                df = pd.read_csv(
                    full_path,
                    encoding=file_options.encoding,
                    delimiter=file_options.delimiter,
                    skiprows=file_options.skip_rows,
                    nrows=100,
                    storage_options=storage_options if storage_options else None,
                )

                validation = _validate_dataframe(df)

                # Calculate confidence
                confidence = 0.5
                if not validation.mojibake_detected:
                    confidence += 0.25
                if validation.column_count_consistent:
                    confidence += 0.15
                if len(validation.empty_columns) == 0:
                    confidence += 0.1

                if confidence > best_confidence:
                    best_confidence = confidence
                    best_result = (df, file_options, validation)

                # If good enough, stop
                if confidence >= 0.85:
                    break

                # Try adjustments
                if validation.mojibake_detected and attempt < max_attempts - 1:
                    # Try different encoding
                    current_idx = (
                        ENCODINGS.index(file_options.encoding)
                        if file_options.encoding in ENCODINGS
                        else -1
                    )
                    next_idx = (current_idx + 1) % len(ENCODINGS)
                    file_options.encoding = ENCODINGS[next_idx]
                    warnings.append(f"Attempt {attempts}: Trying encoding {file_options.encoding}")

            except Exception as e:
                warnings.append(f"Attempt {attempts}: Parse error - {e}")
                # Try different options
                if "codec" in str(e).lower() or "decode" in str(e).lower():
                    current_idx = (
                        ENCODINGS.index(file_options.encoding)
                        if file_options.encoding in ENCODINGS
                        else -1
                    )
                    next_idx = (current_idx + 1) % len(ENCODINGS)
                    file_options.encoding = ENCODINGS[next_idx]

        if best_result is None:
            return ProfileSourceResponse(
                connection=connection,
                path=path,
                source_type="csv",
                attempts=attempts,
                confidence=0.0,
                errors=["Failed to parse file after all attempts"],
                warnings=warnings,
            )

        df, final_options, validation = best_result

        # Build schema
        schema = []
        for col in df.columns:
            schema.append(
                ColumnInfo(
                    name=str(col),
                    dtype=str(df[col].dtype),
                    nullable=df[col].isna().any(),
                    null_count=int(df[col].isna().sum()),
                    sample_values=df[col].dropna().head(3).tolist(),
                )
            )

        # Detect candidate keys and watermarks
        candidate_keys = []
        candidate_watermarks = []
        for col_info in schema:
            if "int" in col_info.dtype.lower() and "id" in col_info.name.lower():
                candidate_keys.append(col_info.name)
            if "date" in col_info.dtype.lower() or "datetime" in col_info.name.lower():
                candidate_watermarks.append(col_info.name)

        sample_rows = df.head(10).fillna("").to_dict(orient="records")

        return ProfileSourceResponse(
            connection=connection,
            path=path,
            source_type="csv",
            attempts=attempts,
            confidence=best_confidence,
            file_options=final_options,
            schema=schema,
            sample_rows=sample_rows,
            validation=validation,
            candidate_keys=candidate_keys,
            candidate_watermarks=candidate_watermarks,
            warnings=warnings + ["Used legacy detection (DuckDB unavailable)"],
            errors=[],
            next_step="generate_bronze_node",
            ready_for={
                "connection": connection,
                "path": path,
                "source_type": "csv",
                "options": {
                    "encoding": final_options.encoding,
                    "delimiter": final_options.delimiter,
                    "skipRows": final_options.skip_rows,
                    "header": str(final_options.header_row is not None).lower(),
                },
                "schema": [{"name": c.name, "dtype": c.dtype} for c in schema],
            },
        )

    except Exception as e:
        logger.exception(f"Error profiling file (legacy): {path}")
        return ProfileSourceResponse(
            connection=connection,
            path=path,
            source_type="csv",
            errors=[str(e)],
        )


def _profile_parquet(connection: str, conn, path: str) -> ProfileSourceResponse:
    """Profile a parquet file (simpler - schema is embedded)."""
    import pandas as pd

    try:
        storage_options = {}
        if hasattr(conn, "pandas_storage_options"):
            storage_options = conn.pandas_storage_options()

        full_path = conn.get_path(path)
        df = pd.read_parquet(full_path, storage_options=storage_options or None)
        df_sample = df.head(100)

        # Build enhanced schema
        schema = []
        candidate_keys = []
        candidate_watermarks = []
        total_count = len(df_sample)

        for col in df.columns:
            col_name = str(col)
            dtype = str(df[col].dtype)
            sample_vals = df[col].dropna().head(3).tolist()

            # Compute enhanced stats
            null_count = int(df[col].isna().sum())
            null_pct = null_count / total_count if total_count > 0 else 0
            distinct_count = int(df_sample[col].nunique())
            cardinality = _compute_cardinality(distinct_count, total_count)

            # Detect patterns and suggest transforms
            pattern = _detect_column_pattern(col_name, dtype, sample_vals)
            suggested = _suggest_transform_for_column(col_name, dtype, pattern)

            schema.append(
                ColumnInfo(
                    name=col_name,
                    dtype=dtype,
                    nullable=null_count > 0,
                    null_count=null_count,
                    null_pct=round(null_pct, 4),
                    cardinality=cardinality,
                    distinct_count=distinct_count,
                    sample_values=sample_vals,
                    detected_pattern=pattern,
                    suggested_transform=suggested,
                )
            )

            # Detect keys/watermarks
            dtype_lower = dtype.lower()
            name_lower = col_name.lower()
            if ("int" in dtype_lower or "bigint" in dtype_lower) and "id" in name_lower:
                candidate_keys.append(col_name)
            if "date" in dtype_lower or "timestamp" in dtype_lower:
                candidate_watermarks.append(col_name)

        validation = _validate_dataframe(df_sample)
        sample_rows = df_sample.head(10).fillna("").to_dict(orient="records")

        # Generate Odibi suggestions
        suggestions = _generate_odibi_suggestions(
            schema, "parquet", path, candidate_keys, candidate_watermarks
        )

        return ProfileSourceResponse(
            connection=connection,
            path=path,
            source_type="parquet",
            attempts=1,
            confidence=0.95,
            schema=schema,
            sample_rows=sample_rows,
            validation=validation,
            candidate_keys=candidate_keys,
            candidate_watermarks=candidate_watermarks,
            suggestions=suggestions,
            warnings=[],
            errors=[],
            next_step="generate_bronze_node",
            ready_for={
                "connection": connection,
                "path": path,
                "source_type": "parquet",
                "options": {},
                "schema": [{"name": c.name, "dtype": c.dtype} for c in schema],
            },
        )

    except Exception as e:
        return ProfileSourceResponse(
            connection=connection,
            path=path,
            source_type="parquet",
            errors=[str(e)],
        )


def _profile_excel(connection: str, conn, path: str) -> ProfileSourceResponse:
    """Profile an Excel file."""
    import pandas as pd

    try:
        storage_options = {}
        if hasattr(conn, "pandas_storage_options"):
            storage_options = conn.pandas_storage_options()

        full_path = conn.get_path(path)

        # For cloud, need to download first
        if hasattr(conn, "pandas_storage_options") and storage_options:
            import fsspec

            fs = fsspec.filesystem("abfs", **storage_options)
            with fs.open(full_path, "rb") as f:
                df = pd.read_excel(f, nrows=100, engine="openpyxl")
        else:
            df = pd.read_excel(full_path, nrows=100, engine="openpyxl")

        schema = []
        for col in df.columns:
            schema.append(
                ColumnInfo(
                    name=str(col),
                    dtype=str(df[col].dtype),
                    nullable=df[col].isna().any(),
                    null_count=int(df[col].isna().sum()),
                    sample_values=df[col].dropna().head(3).tolist(),
                )
            )

        validation = _validate_dataframe(df)
        sample_rows = df.head(10).fillna("").to_dict(orient="records")

        return ProfileSourceResponse(
            connection=connection,
            path=path,
            source_type="excel",
            attempts=1,
            confidence=0.9,
            schema=schema,
            sample_rows=sample_rows,
            validation=validation,
            warnings=[],
            errors=[],
            next_step="generate_bronze_node",
            ready_for={
                "connection": connection,
                "path": path,
                "source_type": "excel",
                "options": {},
                "schema": [{"name": c.name, "dtype": c.dtype} for c in schema],
            },
        )

    except Exception as e:
        return ProfileSourceResponse(
            connection=connection,
            path=path,
            source_type="excel",
            errors=[str(e)],
        )


def _profile_sql_table(connection: str, conn, path: str) -> ProfileSourceResponse:
    """Profile a SQL table."""
    try:
        # Parse schema.table
        if "." in path:
            schema_name, table_name = path.split(".", 1)
        else:
            schema_name, table_name = "dbo", path

        # Get columns
        columns_query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}'
        AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """

        if hasattr(conn, "execute_sql"):
            result = conn.execute_sql(columns_query)
        elif hasattr(conn, "execute"):
            result = conn.execute(columns_query)
        else:
            result = conn.read_sql_query(columns_query).values.tolist()

        schema = []
        candidate_keys = []
        candidate_watermarks = []

        for row in result:
            if isinstance(row, (tuple, list)):
                col_name, dtype, nullable = row[0], row[1], row[2]
            elif hasattr(row, "get"):
                col_name = row.get("COLUMN_NAME")
                dtype = row.get("DATA_TYPE")
                nullable = row.get("IS_NULLABLE")
            else:
                # pyodbc.Row - access by index
                col_name, dtype, nullable = row[0], row[1], row[2]

            col_info = ColumnInfo(
                name=col_name,
                dtype=dtype,
                nullable=nullable == "YES",
            )
            schema.append(col_info)

            # Detect candidate keys
            if "id" in col_name.lower() and dtype in ("int", "bigint", "uniqueidentifier"):
                candidate_keys.append(col_name)

            # Detect watermarks
            if dtype in ("datetime", "datetime2", "date", "timestamp"):
                candidate_watermarks.append(col_name)

        # Get sample rows
        sample_query = f"SELECT TOP 10 * FROM [{schema_name}].[{table_name}]"
        if hasattr(conn, "execute_sql"):
            sample_result = conn.execute_sql(sample_query)
        elif hasattr(conn, "execute"):
            sample_result = conn.execute(sample_query)
        else:
            sample_result = conn.read_sql_query(sample_query).values.tolist()

        sample_rows = []
        for row in sample_result:
            if isinstance(row, (tuple, list)):
                sample_rows.append(dict(zip([c.name for c in schema], row)))
            elif hasattr(row, "get"):
                sample_rows.append(dict(row))
            else:
                # pyodbc.Row - convert to dict via column names
                sample_rows.append(dict(zip([c.name for c in schema], row)))

        return ProfileSourceResponse(
            connection=connection,
            path=path,
            source_type="sql_table",
            attempts=1,
            confidence=0.95,
            schema=schema,
            sample_rows=sample_rows,
            validation=ValidationResult(),
            candidate_keys=candidate_keys,
            candidate_watermarks=candidate_watermarks,
            warnings=[],
            errors=[],
            next_step="generate_bronze_node",
            ready_for={
                "connection": connection,
                "path": path,
                "source_type": "sql_table",
                "schema_name": schema_name,
                "table_name": table_name,
                "schema": [{"name": c.name, "dtype": c.dtype} for c in schema],
            },
        )

    except Exception as e:
        return ProfileSourceResponse(
            connection=connection,
            path=path,
            source_type="sql_table",
            errors=[str(e)],
        )


def _profile_json(connection: str, conn, path: str) -> ProfileSourceResponse:
    """Profile a JSON/NDJSON file using DuckDB's read_json for robust detection."""
    try:
        import duckdb
        import tempfile
        import fsspec

        storage_options = {}
        if hasattr(conn, "pandas_storage_options"):
            storage_options = conn.pandas_storage_options()

        full_path = conn.get_path(path)
        warnings = []

        # For cloud storage, download to temp file for DuckDB
        temp_file = None
        local_path = full_path
        suffix = Path(path).suffix.lower()

        if full_path.startswith(("abfss://", "abfs://", "az://")):
            fs = fsspec.filesystem("abfs", **storage_options)
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix or ".json")
            with fs.open(full_path, "rb") as remote_f:
                content = remote_f.read(2 * 1024 * 1024)  # 2MB for JSON sniffing
                temp_file.write(content)
            temp_file.close()
            local_path = temp_file.name

        try:
            # Use DuckDB to auto-detect JSON format and schema
            # DuckDB will auto-detect: array, NDJSON, or unstructured
            con = duckdb.connect()

            # First, get schema by describing
            describe_result = con.execute(
                f"DESCRIBE SELECT * FROM read_json_auto('{local_path}', sample_size=1000)"
            ).fetchall()

            # Read sample data
            sample_df = con.execute(
                f"SELECT * FROM read_json_auto('{local_path}', sample_size=1000) LIMIT 100"
            ).fetchdf()

            # Detect format (NDJSON vs array)
            json_format = "ndjson" if suffix in (".ndjson", ".jsonl") else "json"

            # Build schema with enhanced column info
            schema = []
            candidate_keys = []
            candidate_watermarks = []

            for col_name, col_type, *_ in describe_result:
                col_type_str = str(col_type)
                sample_vals = (
                    sample_df[col_name].dropna().head(3).tolist()
                    if col_name in sample_df.columns
                    else []
                )

                # Compute stats
                total_count = len(sample_df)
                null_count = (
                    int(sample_df[col_name].isna().sum()) if col_name in sample_df.columns else 0
                )
                null_pct = null_count / total_count if total_count > 0 else 0
                distinct_count = (
                    int(sample_df[col_name].nunique()) if col_name in sample_df.columns else 0
                )
                cardinality = _compute_cardinality(distinct_count, total_count)

                # Detect patterns and suggest transforms
                pattern = _detect_column_pattern(col_name, col_type_str, sample_vals)
                suggested = _suggest_transform_for_column(col_name, col_type_str, pattern)

                schema.append(
                    ColumnInfo(
                        name=col_name,
                        dtype=col_type_str,
                        nullable=null_count > 0,
                        null_count=null_count,
                        null_pct=round(null_pct, 4),
                        cardinality=cardinality,
                        distinct_count=distinct_count,
                        sample_values=sample_vals,
                        detected_pattern=pattern,
                        suggested_transform=suggested,
                    )
                )

                # Detect keys/watermarks
                if (
                    "int" in col_type_str.lower() or "bigint" in col_type_str.lower()
                ) and "id" in col_name.lower():
                    candidate_keys.append(col_name)
                if "date" in col_type_str.lower() or "timestamp" in col_type_str.lower():
                    candidate_watermarks.append(col_name)

            sample_rows = sample_df.head(10).fillna("").to_dict(orient="records")
            validation = _validate_dataframe(sample_df)

            # Generate Odibi suggestions
            suggestions = _generate_odibi_suggestions(
                schema, json_format, path, candidate_keys, candidate_watermarks
            )

            return ProfileSourceResponse(
                connection=connection,
                path=path,
                source_type=json_format,
                attempts=1,
                confidence=0.90,
                schema=schema,
                sample_rows=sample_rows,
                validation=validation,
                candidate_keys=candidate_keys,
                candidate_watermarks=candidate_watermarks,
                suggestions=suggestions,
                warnings=warnings,
                errors=[],
                next_step="generate_bronze_node",
                ready_for={
                    "connection": connection,
                    "path": path,
                    "source_type": json_format,
                    "options": {"format": json_format},
                    "schema": [{"name": c.name, "dtype": c.dtype} for c in schema],
                },
            )

        finally:
            if temp_file:
                import os

                try:
                    os.unlink(temp_file.name)
                except Exception:
                    pass

    except Exception as e:
        logger.exception(f"Error profiling JSON file: {path}")
        return ProfileSourceResponse(
            connection=connection,
            path=path,
            source_type="json",
            errors=[str(e)],
        )


def _profile_avro(connection: str, conn, path: str) -> ProfileSourceResponse:
    """Profile an Avro file - schema is embedded in file header."""
    try:
        import tempfile
        import fsspec

        storage_options = {}
        if hasattr(conn, "pandas_storage_options"):
            storage_options = conn.pandas_storage_options()

        full_path = conn.get_path(path)
        warnings = []

        # For cloud storage, download to temp file
        temp_file = None
        local_path = full_path

        if full_path.startswith(("abfss://", "abfs://", "az://")):
            fs = fsspec.filesystem("abfs", **storage_options)
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".avro")
            with fs.open(full_path, "rb") as remote_f:
                content = remote_f.read(5 * 1024 * 1024)  # 5MB should include header + some data
                temp_file.write(content)
            temp_file.close()
            local_path = temp_file.name

        try:
            # Try DuckDB's avro extension first (preferred)
            try:
                import duckdb

                con = duckdb.connect()
                con.execute("INSTALL avro; LOAD avro;")

                # Get schema
                describe_result = con.execute(
                    f"DESCRIBE SELECT * FROM read_avro('{local_path}')"
                ).fetchall()

                # Get sample data
                sample_df = con.execute(
                    f"SELECT * FROM read_avro('{local_path}') LIMIT 100"
                ).fetchdf()

                schema = []
                candidate_keys = []
                candidate_watermarks = []

                for col_name, col_type, *_ in describe_result:
                    col_type_str = str(col_type)
                    sample_vals = (
                        sample_df[col_name].dropna().head(3).tolist()
                        if col_name in sample_df.columns
                        else []
                    )

                    total_count = len(sample_df)
                    null_count = (
                        int(sample_df[col_name].isna().sum())
                        if col_name in sample_df.columns
                        else 0
                    )
                    null_pct = null_count / total_count if total_count > 0 else 0
                    distinct_count = (
                        int(sample_df[col_name].nunique()) if col_name in sample_df.columns else 0
                    )
                    cardinality = _compute_cardinality(distinct_count, total_count)

                    pattern = _detect_column_pattern(col_name, col_type_str, sample_vals)
                    suggested = _suggest_transform_for_column(col_name, col_type_str, pattern)

                    schema.append(
                        ColumnInfo(
                            name=col_name,
                            dtype=col_type_str,
                            nullable=null_count > 0,
                            null_count=null_count,
                            null_pct=round(null_pct, 4),
                            cardinality=cardinality,
                            distinct_count=distinct_count,
                            sample_values=sample_vals,
                            detected_pattern=pattern,
                            suggested_transform=suggested,
                        )
                    )

                    if (
                        "int" in col_type_str.lower() or "bigint" in col_type_str.lower()
                    ) and "id" in col_name.lower():
                        candidate_keys.append(col_name)
                    if "date" in col_type_str.lower() or "timestamp" in col_type_str.lower():
                        candidate_watermarks.append(col_name)

                sample_rows = sample_df.head(10).fillna("").to_dict(orient="records")
                validation = _validate_dataframe(sample_df)

                suggestions = _generate_odibi_suggestions(
                    schema, "avro", path, candidate_keys, candidate_watermarks
                )

                return ProfileSourceResponse(
                    connection=connection,
                    path=path,
                    source_type="avro",
                    attempts=1,
                    confidence=0.95,
                    schema=schema,
                    sample_rows=sample_rows,
                    validation=validation,
                    candidate_keys=candidate_keys,
                    candidate_watermarks=candidate_watermarks,
                    suggestions=suggestions,
                    warnings=warnings,
                    errors=[],
                    next_step="generate_bronze_node",
                    ready_for={
                        "connection": connection,
                        "path": path,
                        "source_type": "avro",
                        "options": {},
                        "schema": [{"name": c.name, "dtype": c.dtype} for c in schema],
                    },
                )

            except Exception as duckdb_err:
                # Fallback to fastavro
                logger.debug(f"DuckDB avro extension failed, trying fastavro: {duckdb_err}")
                warnings.append("DuckDB avro extension unavailable, using fastavro")

                try:
                    from fastavro import reader as avro_reader

                    with open(local_path, "rb") as f:
                        reader = avro_reader(f)
                        avro_schema = reader.writer_schema

                        # Extract schema from Avro schema
                        schema = []
                        if avro_schema.get("type") == "record":
                            for field in avro_schema.get("fields", []):
                                field_name = field["name"]
                                field_type = field["type"]
                                # Handle union types (nullable)
                                if isinstance(field_type, list):
                                    non_null = [t for t in field_type if t != "null"]
                                    field_type_str = non_null[0] if non_null else "null"
                                    nullable = "null" in field_type
                                else:
                                    field_type_str = str(field_type)
                                    nullable = False

                                schema.append(
                                    ColumnInfo(
                                        name=field_name,
                                        dtype=field_type_str,
                                        nullable=nullable,
                                    )
                                )

                        # Read sample rows
                        sample_rows = []
                        f.seek(0)
                        reader = avro_reader(f)
                        for i, record in enumerate(reader):
                            if i >= 10:
                                break
                            sample_rows.append(record)

                    return ProfileSourceResponse(
                        connection=connection,
                        path=path,
                        source_type="avro",
                        attempts=1,
                        confidence=0.85,
                        schema=schema,
                        sample_rows=sample_rows,
                        validation=ValidationResult(),
                        warnings=warnings,
                        errors=[],
                        next_step="generate_bronze_node",
                        ready_for={
                            "connection": connection,
                            "path": path,
                            "source_type": "avro",
                            "options": {},
                            "schema": [{"name": c.name, "dtype": c.dtype} for c in schema],
                        },
                    )

                except ImportError:
                    return ProfileSourceResponse(
                        connection=connection,
                        path=path,
                        source_type="avro",
                        errors=[
                            "Neither DuckDB avro extension nor fastavro available. Install fastavro: pip install fastavro"
                        ],
                    )

        finally:
            if temp_file:
                import os

                try:
                    os.unlink(temp_file.name)
                except Exception:
                    pass

    except Exception as e:
        logger.exception(f"Error profiling Avro file: {path}")
        return ProfileSourceResponse(
            connection=connection,
            path=path,
            source_type="avro",
            errors=[str(e)],
        )


def _profile_delta(connection: str, conn, path: str) -> ProfileSourceResponse:
    """Profile a Delta Lake table - schema is in transaction log."""
    try:
        storage_options = {}
        if hasattr(conn, "pandas_storage_options"):
            storage_options = conn.pandas_storage_options()

        full_path = conn.get_path(path)
        warnings = []

        # Try DuckDB delta extension first
        try:
            import duckdb

            con = duckdb.connect()
            con.execute("INSTALL delta; LOAD delta;")

            # For Azure, set up secrets
            if storage_options:
                account_name = storage_options.get("account_name", "")
                account_key = storage_options.get("account_key", "")
                if account_name and account_key:
                    con.execute(
                        f"""
                        CREATE SECRET azure_secret (
                            TYPE AZURE,
                            ACCOUNT_NAME '{account_name}'
                        );
                    """
                    )

            # Get schema
            describe_result = con.execute(
                f"DESCRIBE SELECT * FROM delta_scan('{full_path}')"
            ).fetchall()

            # Get sample data
            sample_df = con.execute(f"SELECT * FROM delta_scan('{full_path}') LIMIT 100").fetchdf()

            schema = []
            candidate_keys = []
            candidate_watermarks = []

            for col_name, col_type, *_ in describe_result:
                col_type_str = str(col_type)
                sample_vals = (
                    sample_df[col_name].dropna().head(3).tolist()
                    if col_name in sample_df.columns
                    else []
                )

                total_count = len(sample_df)
                null_count = (
                    int(sample_df[col_name].isna().sum()) if col_name in sample_df.columns else 0
                )
                null_pct = null_count / total_count if total_count > 0 else 0
                distinct_count = (
                    int(sample_df[col_name].nunique()) if col_name in sample_df.columns else 0
                )
                cardinality = _compute_cardinality(distinct_count, total_count)

                pattern = _detect_column_pattern(col_name, col_type_str, sample_vals)
                suggested = _suggest_transform_for_column(col_name, col_type_str, pattern)

                schema.append(
                    ColumnInfo(
                        name=col_name,
                        dtype=col_type_str,
                        nullable=null_count > 0,
                        null_count=null_count,
                        null_pct=round(null_pct, 4),
                        cardinality=cardinality,
                        distinct_count=distinct_count,
                        sample_values=sample_vals,
                        detected_pattern=pattern,
                        suggested_transform=suggested,
                    )
                )

                if (
                    "int" in col_type_str.lower() or "bigint" in col_type_str.lower()
                ) and "id" in col_name.lower():
                    candidate_keys.append(col_name)
                if "date" in col_type_str.lower() or "timestamp" in col_type_str.lower():
                    candidate_watermarks.append(col_name)

            sample_rows = sample_df.head(10).fillna("").to_dict(orient="records")
            validation = _validate_dataframe(sample_df)

            suggestions = _generate_odibi_suggestions(
                schema, "delta", path, candidate_keys, candidate_watermarks
            )

            return ProfileSourceResponse(
                connection=connection,
                path=path,
                source_type="delta",
                attempts=1,
                confidence=0.95,
                schema=schema,
                sample_rows=sample_rows,
                validation=validation,
                candidate_keys=candidate_keys,
                candidate_watermarks=candidate_watermarks,
                suggestions=suggestions,
                warnings=warnings,
                errors=[],
                next_step="generate_bronze_node",
                ready_for={
                    "connection": connection,
                    "path": path,
                    "source_type": "delta",
                    "options": {},
                    "schema": [{"name": c.name, "dtype": c.dtype} for c in schema],
                },
            )

        except Exception as duckdb_err:
            # Fallback to deltalake Python library
            logger.debug(f"DuckDB delta extension failed, trying deltalake: {duckdb_err}")
            warnings.append("DuckDB delta extension unavailable, using deltalake library")

            try:
                from deltalake import DeltaTable

                # Build storage options for deltalake
                delta_opts = {}
                if storage_options:
                    delta_opts["account_name"] = storage_options.get("account_name", "")
                    delta_opts["account_key"] = storage_options.get("account_key", "")

                dt = DeltaTable(full_path, storage_options=delta_opts if delta_opts else None)
                arrow_schema = dt.schema().to_pyarrow()

                schema = []
                for field in arrow_schema:
                    schema.append(
                        ColumnInfo(
                            name=field.name,
                            dtype=str(field.type),
                            nullable=field.nullable,
                        )
                    )

                # Read sample
                sample_df = dt.to_pandas()[:100]
                sample_rows = sample_df.head(10).fillna("").to_dict(orient="records")

                return ProfileSourceResponse(
                    connection=connection,
                    path=path,
                    source_type="delta",
                    attempts=1,
                    confidence=0.90,
                    schema=schema,
                    sample_rows=sample_rows,
                    validation=ValidationResult(),
                    warnings=warnings,
                    errors=[],
                    next_step="generate_bronze_node",
                    ready_for={
                        "connection": connection,
                        "path": path,
                        "source_type": "delta",
                        "options": {},
                        "schema": [{"name": c.name, "dtype": c.dtype} for c in schema],
                    },
                )

            except ImportError:
                return ProfileSourceResponse(
                    connection=connection,
                    path=path,
                    source_type="delta",
                    errors=[
                        "Neither DuckDB delta extension nor deltalake library available. Install: pip install deltalake"
                    ],
                )

    except Exception as e:
        logger.exception(f"Error profiling Delta table: {path}")
        return ProfileSourceResponse(
            connection=connection,
            path=path,
            source_type="delta",
            errors=[str(e)],
        )


def profile_folder(
    connection: str,
    folder_path: str = "",
    pattern: str = "*",
    max_files: int = 50,
    max_attempts: int = 3,
) -> ProfileFolderResponse:
    """
    Batch profile all files in a folder.

    Profiles multiple files, groups them by detected options, and identifies
    which files can share the same read configuration. Enables bulk bronze
    node generation.
    """
    ctx = get_project_context()
    if not ctx:
        return ProfileFolderResponse(
            connection=connection,
            folder_path=folder_path,
            pattern=pattern,
            total_files=0,
            profiled_count=0,
            success_count=0,
            error_count=0,
            confidence=0.0,
            errors=["No project context available. Set ODIBI_CONFIG env var."],
        )

    try:
        conn = ctx.get_connection(connection)
    except Exception as e:
        return ProfileFolderResponse(
            connection=connection,
            folder_path=folder_path,
            pattern=pattern,
            total_files=0,
            profiled_count=0,
            success_count=0,
            error_count=0,
            confidence=0.0,
            errors=[f"Connection not found: {e}"],
        )

    if not _is_storage_connection(conn):
        return ProfileFolderResponse(
            connection=connection,
            folder_path=folder_path,
            pattern=pattern,
            total_files=0,
            profiled_count=0,
            success_count=0,
            error_count=0,
            confidence=0.0,
            errors=["profile_folder only works with storage connections (ADLS, S3, local)"],
        )

    try:
        import fnmatch

        # List files in folder
        base_path = conn.get_path(folder_path)
        storage_options = {}
        if hasattr(conn, "pandas_storage_options"):
            storage_options = conn.pandas_storage_options()

        import fsspec

        if base_path.startswith(("abfss://", "abfs://", "az://")):
            fs = fsspec.filesystem("abfs", **storage_options)
        else:
            fs = fsspec.filesystem("file")

        # List directory contents
        try:
            all_items = fs.ls(base_path, detail=True)
        except FileNotFoundError:
            return ProfileFolderResponse(
                connection=connection,
                folder_path=folder_path,
                pattern=pattern,
                total_files=0,
                profiled_count=0,
                success_count=0,
                error_count=0,
                confidence=0.0,
                errors=[f"Folder not found: {folder_path}"],
            )

        # Filter to files matching pattern
        files = []
        for item in all_items:
            if item.get("type") == "file":
                name = Path(item["name"]).name
                if fnmatch.fnmatch(name.lower(), pattern.lower()):
                    files.append(item["name"])

        if not files:
            return ProfileFolderResponse(
                connection=connection,
                folder_path=folder_path,
                pattern=pattern,
                total_files=0,
                profiled_count=0,
                success_count=0,
                error_count=0,
                confidence=0.0,
                warnings=[f"No files matching pattern '{pattern}' in {folder_path}"],
                errors=[],
            )

        total_files = len(files)
        files_to_profile = files[:max_files]

        # Profile each file
        file_profiles: List[FileProfileSummary] = []
        success_count = 0
        error_count = 0
        total_confidence = 0.0

        for file_path in files_to_profile:
            # Get relative path for profile_source
            rel_path = file_path
            if hasattr(conn, "get_relative_path"):
                rel_path = conn.get_relative_path(file_path)
            else:
                # Try to extract relative path from full path
                if folder_path and folder_path in file_path:
                    idx = file_path.find(folder_path)
                    if idx >= 0:
                        rel_path = file_path[idx:]
                else:
                    # Use just the filename portion after container/bucket
                    parts = file_path.replace("\\", "/").split("/")
                    # Skip protocol and container parts
                    if len(parts) > 2:
                        rel_path = "/".join(parts[1:])  # Skip container

            # Call _profile_file directly for efficiency
            result = _profile_file(connection, conn, rel_path, max_attempts)

            # Build summary
            options = {}
            if result.file_options:
                options = {
                    "encoding": result.file_options.encoding,
                    "delimiter": result.file_options.delimiter,
                    "skipRows": result.file_options.skip_rows,
                }
            elif result.ready_for and "options" in result.ready_for:
                options = result.ready_for["options"]

            summary = FileProfileSummary(
                path=rel_path,
                source_type=result.source_type,
                confidence=result.confidence,
                options=options,
                column_count=len(result.schema) if result.schema else 0,
                sample_columns=[c.name for c in result.schema[:5]] if result.schema else [],
                warnings=result.warnings,
                errors=result.errors,
                ready_for=result.ready_for,
            )
            file_profiles.append(summary)

            if result.errors:
                error_count += 1
            else:
                success_count += 1
                total_confidence += result.confidence

        profiled_count = len(file_profiles)
        avg_confidence = total_confidence / success_count if success_count > 0 else 0.0

        # Group files by options (for CSV files with encoding/delimiter/skipRows)
        options_groups: List[OptionsGroup] = []
        options_map: Dict[str, List[str]] = {}

        for profile in file_profiles:
            if not profile.errors and profile.options:
                # Create a hashable key from options
                key_parts = [
                    profile.options.get("encoding", "utf-8"),
                    profile.options.get("delimiter", ","),
                    str(profile.options.get("skipRows", 0)),
                ]
                key = "|".join(key_parts)
                if key not in options_map:
                    options_map[key] = []
                options_map[key].append(profile.path)

        for key, file_list in options_map.items():
            parts = key.split("|")
            options = {
                "encoding": parts[0],
                "delimiter": parts[1] if parts[1] != "\t" else "\\t",
                "skipRows": int(parts[2]) if parts[2].isdigit() else 0,
            }
            # Use raw delimiter for the actual options dict
            options["delimiter"] = parts[1]
            options_groups.append(
                OptionsGroup(
                    options=options,
                    files=file_list,
                    count=len(file_list),
                )
            )

        # Sort by count (largest groups first)
        options_groups.sort(key=lambda g: g.count, reverse=True)

        # Determine if options are consistent
        consistent_options = len(options_groups) == 1 and error_count == 0

        # Get recommended options (from largest group or None if inconsistent)
        recommended_options = {}
        if options_groups:
            recommended_options = options_groups[0].options.copy()

        # Build warnings
        warnings = []
        if total_files > max_files:
            warnings.append(f"Sampled {max_files} of {total_files} files")
        if len(options_groups) > 1:
            warnings.append(
                f"Found {len(options_groups)} different option configurations - review before bulk generation"
            )
        if error_count > 0:
            warnings.append(
                f"{error_count} files failed profiling - check file_profiles for details"
            )

        # Build ready_for based on results
        ready_for: Dict[str, Any] = {}
        if consistent_options and success_count > 0:
            ready_for = {
                "action": "generate_bronze_nodes",
                "connection": connection,
                "folder_path": folder_path,
                "pattern": pattern,
                "options": recommended_options,
                "files": [p.path for p in file_profiles if not p.errors],
            }
            next_step = "generate_bronze_node (call for each file, or use bulk generation)"
        elif success_count > 0:
            ready_for = {
                "action": "review_options_groups",
                "groups": [
                    {"options": g.options, "files": g.files[:5], "count": g.count}
                    for g in options_groups
                ],
            }
            next_step = "Review options_groups - generate nodes per group with matching options"
        else:
            next_step = "Fix connection or path - no files profiled successfully"

        return ProfileFolderResponse(
            connection=connection,
            folder_path=folder_path,
            pattern=pattern,
            total_files=total_files,
            profiled_count=profiled_count,
            success_count=success_count,
            error_count=error_count,
            confidence=avg_confidence,
            options_groups=options_groups,
            file_profiles=file_profiles,
            consistent_options=consistent_options,
            recommended_options=recommended_options,
            warnings=warnings,
            errors=[],
            next_step=next_step,
            ready_for=ready_for,
        )

    except Exception as e:
        logger.exception(f"Error profiling folder: {folder_path}")
        return ProfileFolderResponse(
            connection=connection,
            folder_path=folder_path,
            pattern=pattern,
            total_files=0,
            profiled_count=0,
            success_count=0,
            error_count=0,
            confidence=0.0,
            errors=[str(e)],
        )


def generate_bronze_node(
    profile: Dict[str, Any],
    node_name: Optional[str] = None,
    output_connection: Optional[str] = None,
    output_path: Optional[str] = None,
    include_project: bool = True,
    local_output: bool = True,
) -> GenerateBronzeNodeResponse:
    """Generate Odibi YAML for a bronze layer node from a profile result.

    Smart behavior:
    - If output_connection not specified, uses source connection (write back to same storage)
    - If output_connection doesn't exist, falls back to source connection with warning
    - Generates complete runnable PROJECT YAML (with connections, system, story)
    - include_project=True (default): outputs full project config ready to run
    - include_project=False: outputs just the pipeline (for import into existing project)
    - local_output=True: forces output to local connection (useful for testing ADLS/SQL sources locally)
    """
    warnings = []

    # Extract from profile (could be ready_for dict or full profile)
    source_connection = profile.get("connection", "")
    source_path = profile.get("path", "")
    source_type = profile.get("source_type", "csv")
    options = profile.get("options", {})
    schema_name = profile.get("schema_name", "")
    table_name = profile.get("table_name", "")

    # Generate node name if not provided
    if not node_name:
        base_name = Path(source_path).stem if source_path else table_name
        # Sanitize: alphanumeric and underscore only
        node_name = re.sub(r"[^a-zA-Z0-9_]", "_", base_name).lower()
        node_name = re.sub(r"_+", "_", node_name).strip("_")

    # Smart output connection selection
    ctx = get_project_context()
    available_connections = []
    if ctx:
        try:
            # Get list of available connections
            if hasattr(ctx, "connections") and ctx.connections:
                available_connections = list(ctx.connections.keys())
        except Exception:
            pass

    # Determine output connection
    if local_output:
        # Force output to local connection for testing
        output_connection = "local"
    elif output_connection is None:
        # Default: try "bronze", fall back to source connection
        if "bronze" in available_connections:
            output_connection = "bronze"
        else:
            output_connection = source_connection
            warnings.append(
                f"No 'bronze' connection found. Using source connection '{source_connection}' for output. "
                f"Available connections: {available_connections}"
            )
    elif output_connection not in available_connections and available_connections:
        # Specified connection doesn't exist
        warnings.append(
            f"Connection '{output_connection}' not found. "
            f"Available connections: {available_connections}. "
            f"Falling back to source connection '{source_connection}'."
        )
        output_connection = source_connection

    # Generate output path if not provided
    if not output_path:
        output_path = f"bronze/{node_name}"

    # Build node YAML using correct Odibi format
    # - read: for inputs (with named inputs like "default")
    # - write: for outputs (flat structure, not nested under "default")
    # - options use pandas-compatible lowercase names
    node_config: Dict[str, Any] = {"name": node_name}

    # Normalize options to pandas-compatible format
    normalized_options = {}
    if options:
        for key, value in options.items():
            # Convert camelCase to lowercase pandas options
            if key == "skipRows":
                normalized_options["skiprows"] = value
            elif key == "delimiter":
                normalized_options["sep"] = value
            elif key == "skip_blank_lines":
                normalized_options["skip_blank_lines"] = value
            else:
                normalized_options[key] = value

    if source_type == "sql_table":
        # SQL source - read: is flat structure
        node_config["read"] = {
            "connection": source_connection,
            "format": "sql",
            "query": f"SELECT * FROM [{schema_name}].[{table_name}]",
        }
    else:
        # File source - read: is flat structure
        read_config: Dict[str, Any] = {
            "connection": source_connection,
            "path": source_path,
            "format": source_type if source_type != "excel" else "xlsx",
        }

        if normalized_options:
            read_config["options"] = normalized_options

        node_config["read"] = read_config

    # Output config - flat structure under "write:", not nested under "default"
    node_config["write"] = {
        "connection": output_connection,
        "path": output_path,
        "format": "delta",
    }

    # Generate pipeline name from node name
    pipeline_name = f"bronze_{node_name}"

    # Build pipeline config
    pipeline_config = {
        "pipeline": pipeline_name,
        "description": f"Bronze ingestion for {node_name}",
        "layer": "bronze",
        "nodes": [node_config],
    }

    if include_project:
        # Build complete runnable PROJECT YAML
        # Get connection config from context - USE RAW CONFIG to preserve all settings
        connection_configs = {}
        
        if ctx and hasattr(ctx, "config") and ctx.config.get("connections"):
            # Use raw connection configs from config dict (preserves full YAML structure)
            raw_connections = ctx.config.get("connections", {})
            for conn_name, conn_config in raw_connections.items():
                # Deep copy to avoid modifying original
                connection_configs[conn_name] = dict(conn_config)
            logger.debug(f"Using raw config connections: {list(connection_configs.keys())}")
        
        # If no raw config, try to reconstruct from connection objects
        if not connection_configs and ctx and hasattr(ctx, "connections") and ctx.connections:
            logger.warning("No raw config found, reconstructing connections from objects")
            for conn_name, conn_obj in ctx.connections.items():
                conn_type = type(conn_obj).__name__
                if conn_type == "LocalConnection":
                    connection_configs[conn_name] = {
                        "type": "local",
                        "path": getattr(conn_obj, "root_path", "./data"),
                    }
                elif conn_type == "AzureADLS":
                    connection_configs[conn_name] = {
                        "type": "azure_adls",
                        "account_name": getattr(conn_obj, "account_name", ""),
                        "account_key": "${AZURE_STORAGE_ACCOUNT_KEY}",
                        "container": getattr(conn_obj, "container", ""),
                    }
                elif conn_type in ("AzureSQLConnection", "AzureSQL"):
                    connection_configs[conn_name] = {
                        "type": "azure_sql",
                        "server": getattr(conn_obj, "server", ""),
                        "database": getattr(conn_obj, "database", ""),
                        "driver": getattr(conn_obj, "driver", "ODBC Driver 17 for SQL Server"),
                        "username": "${SQL_USER}",
                        "password": "${SQL_PASSWORD}",
                    }
                else:
                    # Unknown type - add minimal config with warning
                    connection_configs[conn_name] = {"type": conn_type}
                    warnings.append(f"Unknown connection type '{conn_type}' for '{conn_name}' - config may be incomplete")

        # Ensure we have a local connection for system/story
        if "local" not in connection_configs:
            connection_configs["local"] = {
                "type": "local",
                "path": "./data",
            }

        # Use project name from config if available, otherwise generate from node name
        project_name = ctx.project_name if ctx and ctx.project_name else f"{node_name}_project"
        
        yaml_dict = {
            "project": project_name,
            "connections": connection_configs,
            "system": {
                "connection": "local",
                "path": "_system",
            },
            "story": {
                "connection": "local",
                "path": "stories",
            },
            "pipelines": [pipeline_config],
        }
    else:
        # Just the pipeline (for import)
        yaml_dict = pipeline_config

    # Generate YAML string
    yaml_content = yaml.dump(yaml_dict, default_flow_style=False, sort_keys=False)

    return GenerateBronzeNodeResponse(
        node_name=node_name,
        yaml_content=yaml_content,
        warnings=warnings,
        errors=[],
        next_step="test_node"
        if not include_project
        else "save and run with: python -m odibi run <filename>.yaml",
        ready_for={"node_yaml": yaml_content},
    )


def test_node(node_yaml: str, max_rows: int = 100) -> TestNodeResponse:
    """Test a node definition in-memory without persisting."""
    import pandas as pd

    ctx = get_project_context()
    if not ctx:
        return TestNodeResponse(
            status="error",
            errors=["No project context available"],
        )

    try:
        # Parse YAML
        node_config = yaml.safe_load(node_yaml)

        # Get node name and config
        if not node_config:
            return TestNodeResponse(status="error", errors=["Empty YAML"])

        # Handle multiple YAML formats:
        # 1. Complete pipeline: {pipeline: ..., nodes: [{name: ..., inputs: ...}]}
        # 2. Nodes only: {nodes: [{name: ..., inputs: ...}]}
        # 3. Old format: {node_name: {source: ..., write: ...}}
        if "nodes" in node_config:
            # New format with nodes list
            nodes = node_config["nodes"]
            if not nodes:
                return TestNodeResponse(status="error", errors=["No nodes in YAML"])
            config = nodes[0]
            node_name = config.get("name", "unknown")
            # Get input config from inputs.default
            inputs = config.get("inputs", {})
            source = inputs.get("default", {})
        elif "pipeline" in node_config:
            # Shouldn't happen but handle edge case
            return TestNodeResponse(status="error", errors=["Pipeline YAML without nodes list"])
        else:
            # Old format: {node_name: {source: ..., write: ...}}
            node_name = list(node_config.keys())[0]
            config = node_config[node_name]
            source = config.get("source", {})

        connection_name = source.get("connection")
        source_format = source.get("format", "csv")
        source_path = source.get("path", "")
        options = source.get("options", {})

        conn = ctx.get_connection(connection_name)

        # Build read options
        storage_options = {}
        if hasattr(conn, "pandas_storage_options"):
            storage_options = conn.pandas_storage_options()

        full_path = conn.get_path(source_path) if source_path else None

        # Read based on format
        if source_format == "sql":
            query = source.get("query", "")
            result = conn.execute_query(query + f" OFFSET 0 ROWS FETCH NEXT {max_rows} ROWS ONLY")
            df = pd.DataFrame(result)
        elif source_format in ("csv", "txt"):
            read_options = {
                "encoding": options.get("encoding", "utf-8"),
                "delimiter": options.get("delimiter", ","),
                "skiprows": options.get("skipRows", 0),
                "nrows": max_rows,
            }
            if storage_options:
                read_options["storage_options"] = storage_options

            df = pd.read_csv(full_path, **read_options)
        elif source_format == "parquet":
            df = pd.read_parquet(
                full_path, storage_options=storage_options if storage_options else None
            )
            df = df.head(max_rows)
        elif source_format in ("xlsx", "excel"):
            if storage_options:
                import fsspec

                fs = fsspec.filesystem("abfs", **storage_options)
                with fs.open(full_path, "rb") as f:
                    df = pd.read_excel(f, nrows=max_rows, engine="openpyxl")
            else:
                df = pd.read_excel(full_path, nrows=max_rows, engine="openpyxl")
        else:
            return TestNodeResponse(
                status="error",
                errors=[f"Unsupported format: {source_format}"],
            )

        # Validate
        validation = _validate_dataframe(df)

        # Build schema
        schema = []
        for col in df.columns:
            schema.append(
                ColumnInfo(
                    name=str(col),
                    dtype=str(df[col].dtype),
                    nullable=df[col].isna().any(),
                )
            )

        sample_rows = df.head(10).fillna("").to_dict(orient="records")

        # Generate suggestions and fix instructions if issues found
        suggestions = []
        fixes: List[FixInstruction] = []

        if validation.mojibake_detected:
            suggestions.append("Mojibake detected - try different encoding (windows-1252, latin-1)")
            fixes.append(
                FixInstruction(
                    action="change_encoding",
                    description="Try a different encoding to fix character corruption",
                    path="source.options.encoding",
                    current_value=options.get("encoding", "utf-8"),
                    suggested_value="windows-1252",
                    retry_with={**options, "encoding": "windows-1252"},
                )
            )

        # Only flag empty columns for CSV (not parquet/SQL where it's normal)
        if validation.empty_columns and source_format in ("csv", "txt"):
            suggestions.append(f"Empty columns found: {validation.empty_columns} - check delimiter")
            fixes.append(
                FixInstruction(
                    action="check_delimiter",
                    description="Empty columns may indicate wrong delimiter",
                    path="source.options.delimiter",
                    current_value=options.get("delimiter", ","),
                    suggested_value="\\t" if options.get("delimiter") == "," else ",",
                )
            )

        ready_to_save = not validation.mojibake_detected

        return TestNodeResponse(
            status="success",
            rows_read=len(df),
            sample_rows=sample_rows,
            schema=schema,
            validation=validation,
            errors=[],
            suggestions=suggestions,
            fixes=fixes,
            ready_to_save=ready_to_save,
        )

    except Exception as e:
        logger.exception("Error testing node")
        suggestions = []
        fixes: List[FixInstruction] = []
        error_str = str(e).lower()

        if "codec" in error_str or "decode" in error_str:
            suggestions.append("Encoding error - try encoding: windows-1252 or latin-1")
            fixes.append(
                FixInstruction(
                    action="change_encoding",
                    description="Encoding error - file uses different character encoding",
                    path="source.options.encoding",
                    suggested_value="windows-1252",
                )
            )
        if "delimiter" in error_str or "tokenizing" in error_str:
            suggestions.append("Parsing error - try different delimiter (tab, pipe, semicolon)")
            fixes.append(
                FixInstruction(
                    action="change_delimiter",
                    description="CSV parsing failed - try different delimiter",
                    path="source.options.delimiter",
                    suggested_value="\\t",
                )
            )
        if "not found" in error_str and "column" in error_str:
            fixes.append(
                FixInstruction(
                    action="check_columns",
                    description="Column name mismatch - verify column names match file",
                    path="source.options.skipRows",
                )
            )
        if "skiprows" in error_str or "skip" in error_str:
            fixes.append(
                FixInstruction(
                    action="adjust_skiprows",
                    description="Try adjusting skipRows value",
                    path="source.options.skipRows",
                )
            )

        return TestNodeResponse(
            status="error",
            errors=[str(e)],
            suggestions=suggestions
            + [
                "WORKFLOW: If parsing fails, call profile_source again or adjust options in the YAML and re-test"
            ],
            fixes=fixes,
        )


# =============================================================================
# Download Tools - For AI Local Analysis
# =============================================================================


def download_sql(
    connection: str | dict,
    query: str,
    output_path: str,
    limit: int = 10000,
) -> dict:
    """
    Run a SQL query and save results to a local file.

    Enables AI assistants to download data locally for analysis.
    Saves as Parquet by default for efficiency; format detected from output_path extension.

    Args:
        connection: Either a connection name (str) or inline spec (dict).
            Inline spec example:
            {
                "type": "azure_sql",
                "server": "myserver.database.windows.net",
                "database": "MyDB",
                "driver": "ODBC Driver 17 for SQL Server",
                "username": "${SQL_USER}",
                "password": "${SQL_PASSWORD}"
            }
        query: SQL query to execute (will have LIMIT/TOP applied)
        output_path: Local path to save results (e.g., "./data/results.parquet")
        limit: Max rows to download (default 10000 to prevent huge downloads)

    Returns:
        Dict with status, rows_saved, output_path, columns, errors

    Example:
        download_sql("wwi", "SELECT * FROM Sales.Orders", "./data/orders.parquet")
        download_sql("wwi", "SELECT * FROM Sales.Orders WHERE OrderDate > '2024-01-01'", "./orders.csv", limit=5000)
    """
    import pandas as pd
    from pathlib import Path

    from odibi_mcp.context import resolve_connection

    conn_name = connection if isinstance(connection, str) else "inline"

    try:
        conn, conn_name = resolve_connection(connection)
    except Exception as e:
        return {
            "status": "error",
            "connection": conn_name,
            "errors": [f"Connection error: {e}"],
        }

    if not _is_sql_connection(conn):
        return {
            "status": "error",
            "connection": conn_name,
            "errors": [f"download_sql requires a SQL connection, got: {type(conn).__name__}"],
        }

    try:
        # Apply limit to query if not already present
        query_upper = query.upper().strip()
        if "TOP " not in query_upper and "LIMIT " not in query_upper:
            # For SQL Server, inject TOP after SELECT
            if query_upper.startswith("SELECT"):
                query = query[:6] + f" TOP {limit} " + query[6:]
            else:
                # Fallback: wrap in subquery
                query = f"SELECT TOP {limit} * FROM ({query}) AS limited_query"

        # Execute query using available method
        if hasattr(conn, "read_sql"):
            df = conn.read_sql(query)
        elif hasattr(conn, "read_sql_query"):
            df = conn.read_sql_query(query)
        elif hasattr(conn, "execute_sql"):
            result = conn.execute_sql(query)
            # Convert result to DataFrame
            if hasattr(result, "fetchall"):
                rows = result.fetchall()
                columns = [desc[0] for desc in result.description]
                df = pd.DataFrame(rows, columns=columns)
            else:
                # Result is list of rows
                if result:
                    first = result[0]
                    if isinstance(first, (tuple, list)):
                        # Need column names from a DESCRIBE or assume numbered
                        df = pd.DataFrame(result)
                    elif hasattr(first, "keys"):
                        df = pd.DataFrame([dict(r) for r in result])
                    else:
                        # pyodbc.Row - get column names from cursor_description
                        if hasattr(first, "cursor_description"):
                            columns = [desc[0] for desc in first.cursor_description]
                            df = pd.DataFrame([tuple(r) for r in result], columns=columns)
                        else:
                            df = pd.DataFrame(result)
                else:
                    df = pd.DataFrame()
        elif hasattr(conn, "execute"):
            # Use execute + pandas
            result = conn.execute(query)
            if hasattr(result, "fetchall"):
                rows = result.fetchall()
                if result.description:
                    columns = [desc[0] for desc in result.description]
                    df = pd.DataFrame(rows, columns=columns)
                else:
                    df = pd.DataFrame(rows)
            else:
                df = pd.DataFrame(result) if result else pd.DataFrame()
        else:
            return {
                "status": "error",
                "connection": conn_name,
                "errors": ["Connection does not support SQL execution methods"],
            }

        # Determine output format from extension
        output_path_obj = Path(output_path)
        output_path_obj.parent.mkdir(parents=True, exist_ok=True)
        suffix = output_path_obj.suffix.lower()

        if suffix == ".csv":
            df.to_csv(output_path, index=False)
        elif suffix == ".json":
            df.to_json(output_path, orient="records", indent=2)
        elif suffix in (".xlsx", ".xls"):
            df.to_excel(output_path, index=False)
        else:
            # Default to parquet
            if not suffix or suffix not in (".parquet", ".pq"):
                output_path = str(output_path_obj.with_suffix(".parquet"))
            df.to_parquet(output_path, index=False)

        return {
            "status": "success",
            "connection": conn_name,
            "rows_saved": len(df),
            "columns": list(df.columns),
            "output_path": str(output_path),
            "format": suffix.lstrip(".") if suffix else "parquet",
        }

    except Exception as e:
        logger.exception(f"Error downloading SQL: {query[:100]}...")
        return {
            "status": "error",
            "connection": conn_name,
            "query": query[:200] + "..." if len(query) > 200 else query,
            "errors": [str(e)],
        }


def download_table(
    connection: str | dict,
    table: str,
    output_path: str,
    limit: int = 10000,
) -> dict:
    """
    Download a full table (with row limit) to a local file.

    Convenience wrapper around download_sql for downloading entire tables.

    Args:
        connection: Either a connection name (str) or inline spec (dict)
        table: Table name in schema.table format (e.g., "Sales.Orders" or "dbo.Customers")
        output_path: Local path to save results
        limit: Max rows to download (default 10000)

    Returns:
        Dict with status, rows_saved, output_path, columns, errors

    Example:
        download_table("wwi", "Sales.Orders", "./data/orders.parquet")
        download_table("wwi", "Dimension.Customer", "./customers.csv", limit=5000)
    """
    # Parse table name to ensure proper quoting
    if "." in table:
        schema, tbl = table.split(".", 1)
        query = f"SELECT * FROM [{schema}].[{tbl}]"
    else:
        query = f"SELECT * FROM [{table}]"

    return download_sql(connection, query, output_path, limit)


def download_file(
    connection: str | dict,
    source_path: str,
    output_path: str,
) -> dict:
    """
    Copy a file from ADLS/storage to local filesystem.

    For AI assistants to download files for local analysis.

    Args:
        connection: Either a connection name (str) or inline spec (dict).
            Inline spec example for ADLS:
            {
                "type": "azure_adls",
                "account_name": "mystorageaccount",
                "container": "mycontainer",
                "account_key": "${AZURE_STORAGE_ACCOUNT_KEY}"
            }
        source_path: Path within the storage connection (e.g., "raw/data.csv")
        output_path: Local path to save the file

    Returns:
        Dict with status, source_path, output_path, bytes_copied, errors

    Example:
        download_file("raw_adls", "reports/daily.csv", "./data/daily.csv")
        download_file("raw_adls", "exports/data.parquet", "./local/data.parquet")
    """
    import fsspec
    from pathlib import Path

    from odibi_mcp.context import resolve_connection

    conn_name = connection if isinstance(connection, str) else "inline"

    try:
        conn, conn_name = resolve_connection(connection)
    except Exception as e:
        return {
            "status": "error",
            "connection": conn_name,
            "errors": [f"Connection error: {e}"],
        }

    if not _is_storage_connection(conn):
        return {
            "status": "error",
            "connection": conn_name,
            "errors": [
                f"download_file requires a storage connection, got: {type(conn).__name__}. "
                "For SQL, use download_sql or download_table instead."
            ],
        }

    try:
        storage_options = {}
        if hasattr(conn, "pandas_storage_options"):
            storage_options = conn.pandas_storage_options()

        full_source_path = conn.get_path(source_path)

        # Create output directory if needed
        output_path_obj = Path(output_path)
        output_path_obj.parent.mkdir(parents=True, exist_ok=True)

        # Determine source filesystem
        if full_source_path.startswith(("abfss://", "abfs://", "az://")):
            fs = fsspec.filesystem("abfs", **storage_options)
            # Normalize path for fsspec
            if "://" in full_source_path:
                fs_path = full_source_path.split("://", 1)[1]
                if "@" in fs_path.split("/")[0]:
                    parts = fs_path.split("/", 1)
                    container = parts[0].split("@")[0]
                    rest = parts[1] if len(parts) > 1 else ""
                    fs_path = f"{container}/{rest}"
            else:
                fs_path = full_source_path
        elif full_source_path.startswith("s3://"):
            fs = fsspec.filesystem("s3", **storage_options)
            fs_path = full_source_path.replace("s3://", "")
        elif full_source_path.startswith("gs://"):
            fs = fsspec.filesystem("gcs", **storage_options)
            fs_path = full_source_path.replace("gs://", "")
        else:
            # Local filesystem - just copy
            fs = fsspec.filesystem("file")
            fs_path = full_source_path

        # Download the file
        with fs.open(fs_path, "rb") as remote_f:
            content = remote_f.read()

        with open(output_path, "wb") as local_f:
            local_f.write(content)

        return {
            "status": "success",
            "connection": conn_name,
            "source_path": source_path,
            "output_path": str(output_path_obj.absolute()),
            "bytes_copied": len(content),
        }

    except FileNotFoundError as e:
        return {
            "status": "error",
            "connection": conn_name,
            "source_path": source_path,
            "errors": [f"File not found: {e}"],
        }
    except Exception as e:
        logger.exception(f"Error downloading file: {source_path}")
        return {
            "status": "error",
            "connection": conn_name,
            "source_path": source_path,
            "errors": [str(e)],
        }
