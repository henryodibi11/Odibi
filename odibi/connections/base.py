"""Base connection interface."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class BaseConnection(ABC):
    """Abstract base class for connections."""

    @abstractmethod
    def get_path(self, relative_path: str) -> str:
        """Get full path for a relative path.

        Args:
            relative_path: Relative path or table name

        Returns:
            Full path to resource
        """
        pass

    @abstractmethod
    def validate(self) -> None:
        """Validate connection configuration.

        Raises:
            ConnectionError: If validation fails
        """
        pass

    # ============ DISCOVERY API (Optional - Override in Subclasses) ============

    def discover_catalog(
        self,
        include_schema: bool = False,
        include_stats: bool = False,
        limit: int = 200,
        recursive: bool = True,
    ) -> Dict[str, Any]:
        """Discover available datasets (tables/files) in this connection.

        Args:
            include_schema: Include column schemas
            include_stats: Include row counts and stats
            limit: Max datasets to return per namespace
            recursive: Recursively scan all subfolders/schemas (default: True)

        Returns:
            CatalogSummary dict with datasets

        Raises:
            NotImplementedError: If connection type doesn't support discovery
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support discovery. "
            "Override discover_catalog() to add support."
        )

    def get_schema(self, dataset: str) -> Dict[str, Any]:
        """Get schema (columns + types) for a dataset.

        Args:
            dataset: Table name or file path

        Returns:
            Schema dict with columns
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not support get_schema")

    def profile(
        self, dataset: str, sample_rows: int = 1000, columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Profile a dataset with statistics.

        Args:
            dataset: Table/file to profile
            sample_rows: Number of rows to sample
            columns: Specific columns to profile (None = all)

        Returns:
            TableProfile dict with stats
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not support profiling")

    def get_freshness(self, dataset: str, timestamp_column: Optional[str] = None) -> Dict[str, Any]:
        """Get data freshness information.

        Args:
            dataset: Table/file to check
            timestamp_column: Column to check (SQL only)

        Returns:
            FreshnessResult dict
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not support freshness checks")
