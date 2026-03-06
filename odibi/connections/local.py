"""Local filesystem connection."""

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from odibi.connections.base import BaseConnection
from odibi.discovery.types import (
    CatalogSummary,
    Column,
    DatasetRef,
    FreshnessResult,
    PartitionInfo,
    Schema,
    TableProfile,
)
from odibi.discovery.utils import detect_file_format, detect_partitions, infer_format_from_path
from odibi.utils.logging_context import get_logging_context


class LocalConnection(BaseConnection):
    """Connection to local filesystem or URI-based paths (e.g. dbfs:/, file://)."""

    def __init__(self, base_path: str = "./data"):
        """Initialize local connection.

        Args:
            base_path: Base directory for all paths (can be local path or URI)
        """
        ctx = get_logging_context()
        ctx.log_connection(
            connection_type="local",
            connection_name="LocalConnection",
            action="init",
            base_path=base_path,
        )

        self.base_path_str = base_path
        # Detect URIs: "://" (standard URIs) or "dbfs:/" (Databricks)
        # Windows paths like "C:/path" or "D:\path" should NOT be treated as URIs
        # Windows drive letters are single character followed by ":/" which differs from "dbfs:/"
        self.is_uri = "://" in base_path or (
            ":/" in base_path and len(base_path.split(":/")[0]) > 1
        )

        if not self.is_uri:
            self.base_path = Path(base_path)
            ctx.debug(
                "LocalConnection initialized with filesystem path",
                base_path=base_path,
                is_uri=False,
            )
        else:
            self.base_path = None  # Not used for URIs
            ctx.debug(
                "LocalConnection initialized with URI path",
                base_path=base_path,
                is_uri=True,
            )

    def get_path(self, relative_path: str) -> str:
        """Get full path for a relative path.

        Args:
            relative_path: Relative path from base

        Returns:
            Full absolute path or URI
        """
        ctx = get_logging_context()

        if self.is_uri:
            # Use os.path for simple string joining, handling slashes manually for consistency
            # Strip leading slash from relative to avoid root replacement
            clean_rel = relative_path.lstrip("/").lstrip("\\")
            # Handle cases where base_path might not have trailing slash
            if self.base_path_str.endswith("/") or self.base_path_str.endswith("\\"):
                full_path = f"{self.base_path_str}{clean_rel}"
            else:
                # Use forward slash for URIs
                full_path = f"{self.base_path_str}/{clean_rel}"

            ctx.debug(
                "Resolved URI path",
                relative_path=relative_path,
                full_path=full_path,
            )
            return full_path
        else:
            # Standard local path logic
            full_path = self.base_path / relative_path
            resolved = str(full_path.absolute())

            ctx.debug(
                "Resolved local path",
                relative_path=relative_path,
                full_path=resolved,
            )
            return resolved

    def validate(self) -> None:
        """Validate that base path exists or can be created.

        Raises:
            ConnectionError: If validation fails
        """
        ctx = get_logging_context()
        ctx.debug(
            "Validating LocalConnection",
            base_path=self.base_path_str,
            is_uri=self.is_uri,
        )

        if self.is_uri:
            # Cannot validate/create URIs with local os module
            # Assume valid or handled by engine
            ctx.debug(
                "Skipping URI validation (handled by engine)",
                base_path=self.base_path_str,
            )
        else:
            # Create base directory if it doesn't exist
            try:
                self.base_path.mkdir(parents=True, exist_ok=True)
                ctx.info(
                    "LocalConnection validated successfully",
                    base_path=str(self.base_path.absolute()),
                    created=not self.base_path.exists(),
                )
            except Exception as e:
                ctx.error(
                    "LocalConnection validation failed",
                    base_path=self.base_path_str,
                    error=str(e),
                )
                raise

    def list_files(self, path: str = "", pattern: str = "*", limit: int = 1000) -> List[Dict]:
        """List files in local path.

        Args:
            path: Relative path within base_path
            pattern: Glob pattern for filtering (default: "*")
            limit: Maximum number of files to return

        Returns:
            List of dicts with keys: name, path, size, modified, format
        """
        ctx = get_logging_context()
        ctx.debug("Listing local files", path=path, pattern=pattern, limit=limit)

        if self.is_uri:
            ctx.warning("list_files not supported for URI paths")
            return []

        try:
            full_path = self.base_path / path if path else self.base_path
            all_files = []

            for file_path in full_path.glob(pattern):
                if file_path.is_file():
                    stat = file_path.stat()
                    all_files.append(
                        {
                            "name": file_path.name,
                            "path": str(file_path.absolute()),
                            "size": stat.st_size,
                            "modified": datetime.fromtimestamp(stat.st_mtime),
                            "format": infer_format_from_path(file_path.name),
                        }
                    )
                    if len(all_files) >= limit:
                        break

            ctx.info("Listed local files", count=len(all_files))
            return all_files

        except Exception as e:
            ctx.warning("Failed to list local files", error=str(e), path=path)
            return []

    def list_folders(self, path: str = "", limit: int = 100) -> List[str]:
        """List folders in local path.

        Args:
            path: Relative path within base_path
            limit: Maximum number of folders to return

        Returns:
            List of folder paths
        """
        ctx = get_logging_context()
        ctx.debug("Listing local folders", path=path, limit=limit)

        if self.is_uri:
            ctx.warning("list_folders not supported for URI paths")
            return []

        try:
            full_path = self.base_path / path if path else self.base_path
            folders = []

            for item in full_path.iterdir():
                if item.is_dir():
                    folders.append(str(item.absolute()))
                    if len(folders) >= limit:
                        break

            ctx.info("Listed local folders", count=len(folders))
            return folders

        except Exception as e:
            ctx.warning("Failed to list local folders", error=str(e), path=path)
            return []

    def discover_catalog(
        self, include_schema: bool = False, include_stats: bool = False, limit: int = 200
    ) -> Dict[str, Any]:
        """Discover datasets in local filesystem.

        Args:
            include_schema: Sample files and infer schema
            include_stats: Include row counts and stats
            limit: Maximum datasets to return

        Returns:
            CatalogSummary dict
        """
        ctx = get_logging_context()
        ctx.info(
            "Discovering local catalog",
            base_path=self.base_path_str,
            include_schema=include_schema,
            include_stats=include_stats,
        )

        if self.is_uri:
            ctx.warning("discover_catalog not supported for URI paths")
            return CatalogSummary(
                connection_name=self.base_path_str,
                connection_type="local",
                total_datasets=0,
                next_step="URI paths not supported for catalog discovery",
            ).model_dump()

        try:
            folders = []
            files = []
            formats = {}

            for item in self.base_path.iterdir():
                if len(folders) + len(files) >= limit:
                    break

                if item.is_dir():
                    file_format = detect_file_format(str(item))

                    folders.append(
                        DatasetRef(
                            name=item.name,
                            kind="folder",
                            path=str(item.absolute()),
                            format=file_format,
                        )
                    )

                    if file_format:
                        formats[file_format] = formats.get(file_format, 0) + 1

                elif item.is_file():
                    stat = item.stat()
                    file_format = infer_format_from_path(item.name)

                    files.append(
                        DatasetRef(
                            name=item.name,
                            kind="file",
                            path=str(item.absolute()),
                            format=file_format,
                            size_bytes=stat.st_size,
                            modified_at=datetime.fromtimestamp(stat.st_mtime),
                        )
                    )

                    if file_format:
                        formats[file_format] = formats.get(file_format, 0) + 1

            summary = CatalogSummary(
                connection_name=self.base_path_str,
                connection_type="local",
                folders=[f.model_dump() for f in folders],
                files=[f.model_dump() for f in files],
                total_datasets=len(folders) + len(files),
                formats=formats,
                next_step="Use get_schema() to inspect individual datasets",
            )

            ctx.info(
                "Local catalog discovery complete",
                total_datasets=summary.total_datasets,
                folders=len(folders),
                files=len(files),
            )

            return summary.model_dump()

        except Exception as e:
            ctx.error("Failed to discover local catalog", error=str(e))
            return CatalogSummary(
                connection_name=self.base_path_str,
                connection_type="local",
                total_datasets=0,
                next_step=f"Error: {str(e)}",
            ).model_dump()

    def get_schema(self, dataset: str) -> Dict[str, Any]:
        """Get schema for a dataset.

        Args:
            dataset: Relative path to file or folder

        Returns:
            Schema dict with columns
        """
        ctx = get_logging_context()
        ctx.info("Getting local schema", dataset=dataset)

        try:
            import pandas as pd

            full_path = self.get_path(dataset)
            file_format = detect_file_format(full_path)

            if file_format == "parquet" or file_format == "delta":
                df = pd.read_parquet(full_path).head(0)
            elif file_format == "csv":
                df = pd.read_csv(full_path, nrows=1000).head(0)
            elif file_format == "json":
                df = pd.read_json(full_path, lines=True, nrows=1000).head(0)
            else:
                ctx.warning("Unsupported format for schema inference", format=file_format)
                return Schema(
                    dataset=DatasetRef(name=dataset, kind="file", format=file_format),
                    columns=[],
                ).model_dump()

            columns = [Column(name=col, dtype=str(dtype)) for col, dtype in df.dtypes.items()]

            schema = Schema(
                dataset=DatasetRef(name=dataset, kind="file", format=file_format),
                columns=columns,
            )

            ctx.info("Schema retrieved", column_count=len(columns))
            return schema.model_dump()

        except Exception as e:
            ctx.error("Failed to get schema", dataset=dataset, error=str(e))
            return Schema(
                dataset=DatasetRef(name=dataset, kind="file"),
                columns=[],
            ).model_dump()

    def profile(
        self, dataset: str, sample_rows: int = 1000, columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Profile a dataset with statistics.

        Args:
            dataset: Relative path to file or folder
            sample_rows: Number of rows to sample (max 10000)
            columns: Specific columns to profile (None = all)

        Returns:
            TableProfile dict with stats
        """
        ctx = get_logging_context()
        ctx.info("Profiling local dataset", dataset=dataset, sample_rows=sample_rows)

        sample_rows = min(sample_rows, 10000)  # Cap at 10k

        try:
            import pandas as pd

            full_path = self.get_path(dataset)
            file_format = detect_file_format(full_path)

            # Read sample
            if file_format == "parquet" or file_format == "delta":
                df = pd.read_parquet(full_path).head(sample_rows)
            elif file_format == "csv":
                df = pd.read_csv(full_path, nrows=sample_rows)
            elif file_format == "json":
                df = pd.read_json(full_path, lines=True, nrows=sample_rows)
            else:
                ctx.warning("Unsupported format for profiling", format=file_format)
                return TableProfile(
                    dataset=DatasetRef(name=dataset, kind="file", format=file_format),
                    rows_sampled=0,
                    columns=[],
                ).model_dump()

            # Profile columns
            profile_cols = columns or df.columns.tolist()
            profiled = []

            for col in profile_cols:
                if col not in df.columns:
                    continue

                null_count = int(df[col].isnull().sum())
                null_pct = null_count / len(df) if len(df) > 0 else 0
                distinct_count = int(df[col].nunique())

                # Cardinality heuristic
                if distinct_count == len(df):
                    cardinality = "unique"
                elif distinct_count > len(df) * 0.9:
                    cardinality = "high"
                elif distinct_count > len(df) * 0.1:
                    cardinality = "medium"
                else:
                    cardinality = "low"

                sample_values = df[col].dropna().head(5).tolist()

                profiled.append(
                    Column(
                        name=col,
                        dtype=str(df[col].dtype),
                        null_count=null_count,
                        null_pct=round(null_pct, 3),
                        cardinality=cardinality,
                        distinct_count=distinct_count,
                        sample_values=sample_values,
                    )
                )

            # Detect candidate keys (unique non-null columns)
            candidate_keys = [
                c.name for c in profiled if c.cardinality == "unique" and c.null_count == 0
            ]

            # Detect candidate watermarks (datetime columns)
            candidate_watermarks = [
                c.name
                for c in profiled
                if "datetime" in c.dtype.lower() or "date" in c.dtype.lower()
            ]

            completeness = 1.0 - (df.isnull().sum().sum() / (len(df) * len(df.columns)))

            profile = TableProfile(
                dataset=DatasetRef(name=dataset, kind="file", format=file_format),
                rows_sampled=len(df),
                columns=profiled,
                candidate_keys=candidate_keys,
                candidate_watermarks=candidate_watermarks,
                completeness=round(completeness, 3),
            )

            ctx.info("Profiling complete", rows_sampled=len(df), columns=len(profiled))
            return profile.model_dump()

        except Exception as e:
            ctx.error("Failed to profile dataset", dataset=dataset, error=str(e))
            return TableProfile(
                dataset=DatasetRef(name=dataset, kind="file"),
                rows_sampled=0,
                columns=[],
            ).model_dump()

    def detect_partitions(self, path: str = "") -> Dict[str, Any]:
        """Detect partition structure in local path.

        Args:
            path: Relative path to scan (default: base_path)

        Returns:
            PartitionInfo dict
        """
        ctx = get_logging_context()
        ctx.info("Detecting local partitions", path=path)

        if self.is_uri:
            ctx.warning("detect_partitions not supported for URI paths")
            return PartitionInfo(root=path, keys=[], example_values={}).model_dump()

        try:
            full_path = self.base_path / path if path else self.base_path

            # Walk directory and collect paths
            all_paths = []
            for root, dirs, files in os.walk(full_path):
                for file in files[:100]:  # Sample first 100
                    all_paths.append(os.path.join(root, file))
                if len(all_paths) >= 100:
                    break

            partition_info = detect_partitions(all_paths)

            result = PartitionInfo(
                root=str(full_path),
                keys=partition_info.get("keys", []),
                example_values=partition_info.get("example_values", {}),
                format=partition_info.get("format", "none"),
                partition_count=len(all_paths),
            )

            ctx.info("Partition detection complete", keys=result.keys)
            return result.model_dump()

        except Exception as e:
            ctx.error("Failed to detect partitions", path=path, error=str(e))
            return PartitionInfo(root=path, keys=[], example_values={}).model_dump()

    def get_freshness(self, dataset: str) -> Dict[str, Any]:
        """Get data freshness for a dataset.

        Args:
            dataset: Relative path to file or folder

        Returns:
            FreshnessResult dict
        """
        ctx = get_logging_context()
        ctx.info("Checking local freshness", dataset=dataset)

        if self.is_uri:
            ctx.warning("get_freshness not supported for URI paths")
            return FreshnessResult(
                dataset=DatasetRef(name=dataset, kind="file"), source="metadata"
            ).model_dump()

        try:
            full_path = Path(self.get_path(dataset))

            if full_path.exists():
                stat = full_path.stat()
                last_modified = datetime.fromtimestamp(stat.st_mtime)
                age_hours = (datetime.utcnow() - last_modified).total_seconds() / 3600

                result = FreshnessResult(
                    dataset=DatasetRef(name=dataset, kind="file"),
                    last_updated=last_modified,
                    source="metadata",
                    age_hours=round(age_hours, 2),
                )

                ctx.info("Freshness check complete", age_hours=age_hours)
                return result.model_dump()

        except Exception as e:
            ctx.error("Failed to check freshness", dataset=dataset, error=str(e))

        return FreshnessResult(
            dataset=DatasetRef(name=dataset, kind="file"), source="metadata"
        ).model_dump()
