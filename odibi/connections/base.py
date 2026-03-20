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
        path: str = "",
        pattern: str = "",
    ) -> Dict[str, Any]:
        """Discover available datasets (tables/files) in this connection.

        Args:
            include_schema: Include column schemas
            include_stats: Include row counts and stats
            limit: Max datasets to return per namespace
            recursive: Recursively scan all subfolders/schemas (default: True)
            path: Scope search to specific subfolder/schema
            pattern: Filter by pattern (e.g. "*.csv", "fact_*")

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

    def preview(
        self, dataset: str, rows: int = 5, columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Preview sample rows from a dataset.

        Args:
            dataset: Table name or file path
            rows: Number of rows to return (default: 5, max: 100)
            columns: Specific columns to include (None = all)

        Returns:
            PreviewResult dict with sample rows
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not support preview")

    def get_freshness(self, dataset: str, timestamp_column: Optional[str] = None) -> Dict[str, Any]:
        """Get data freshness information.

        Args:
            dataset: Table/file to check
            timestamp_column: Column to check (SQL only)

        Returns:
            FreshnessResult dict
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not support freshness checks")

    # ============ SQL DIALECT HELPERS (Override in SQL Subclasses) ============

    sql_dialect: str = ""
    default_schema: str = ""

    def quote_identifier(self, name: str) -> str:
        """Quote an identifier (table/column name) for this SQL dialect.

        Default implementation returns the name unquoted.
        SQL subclasses should override with dialect-specific quoting.
        """
        return name

    def qualify_table(self, table_name: str, schema: str = "") -> str:
        """Build a fully qualified table reference.

        Args:
            table_name: Table name
            schema: Schema name (uses default_schema if empty)

        Returns:
            Qualified table reference for this dialect
        """
        schema = schema or self.default_schema
        if schema:
            return f"{self.quote_identifier(schema)}.{self.quote_identifier(table_name)}"
        return self.quote_identifier(table_name)

    def build_select_query(
        self,
        table_name: str,
        schema: str = "",
        where: str = "",
        limit: int = -1,
        columns: str = "*",
    ) -> str:
        """Build a SELECT query in this connection's SQL dialect.

        Args:
            table_name: Table name
            schema: Schema name
            where: Optional WHERE clause (without 'WHERE' keyword)
            limit: Row limit (-1 for no limit, 0 for schema-only)
            columns: Column list (default "*")

        Returns:
            SQL SELECT statement string
        """
        qualified = self.qualify_table(table_name, schema)
        query = f"SELECT {columns} FROM {qualified}"
        if where:
            query += f" WHERE {where}"
        if limit >= 0:
            query += f" LIMIT {limit}"
        return query
