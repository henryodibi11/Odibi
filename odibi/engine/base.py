"""Base engine interface."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from odibi.context import Context


class Engine(ABC):
    """Abstract base class for execution engines."""

    # Custom format registry
    _custom_readers: Dict[str, Any] = {}
    _custom_writers: Dict[str, Any] = {}

    @classmethod
    def register_format(cls, fmt: str, reader: Optional[Any] = None, writer: Optional[Any] = None):
        """Register custom format reader/writer.

        Args:
            fmt: Format name (e.g. 'netcdf')
            reader: Function(path, **options) -> DataFrame
            writer: Function(df, path, **options) -> None
        """
        if reader:
            cls._custom_readers[fmt] = reader
        if writer:
            cls._custom_writers[fmt] = writer

    @abstractmethod
    def read(
        self,
        connection: Any,
        format: str,
        table: Optional[str] = None,
        path: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Read data from source.

        Args:
            connection: Connection object
            format: Data format (csv, parquet, delta, etc.)
            table: Table name (for SQL/Delta)
            path: File path (for file-based sources)
            options: Format-specific options

        Returns:
            DataFrame (engine-specific type)
        """
        pass

    def materialize(self, df: Any) -> Any:
        """Materialize lazy dataset into memory (DataFrame).

        Args:
            df: DataFrame or LazyDataset

        Returns:
            Materialized DataFrame
        """
        return df

    @abstractmethod
    def write(
        self,
        df: Any,
        connection: Any,
        format: str,
        table: Optional[str] = None,
        path: Optional[str] = None,
        mode: str = "overwrite",
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Write data to destination.

        Args:
            df: DataFrame to write
            connection: Connection object
            format: Output format
            table: Table name (for SQL/Delta)
            path: File path (for file-based outputs)
            mode: Write mode (overwrite/append)
            options: Format-specific options
        """
        pass

    @abstractmethod
    def execute_sql(self, sql: str, context: Context) -> Any:
        """Execute SQL query.

        Args:
            sql: SQL query string
            context: Execution context with registered DataFrames

        Returns:
            Result DataFrame
        """
        pass

    @abstractmethod
    def execute_operation(self, operation: str, params: Dict[str, Any], df: Any) -> Any:
        """Execute built-in operation (pivot, etc.).

        Args:
            operation: Operation name
            params: Operation parameters
            df: Input DataFrame

        Returns:
            Result DataFrame
        """
        pass

    @abstractmethod
    def get_schema(self, df: Any) -> Any:
        """Get DataFrame schema.

        Args:
            df: DataFrame

        Returns:
            Dict[str, str] mapping column names to types, or List[str] of names (deprecated)
        """
        pass

    @abstractmethod
    def get_shape(self, df: Any) -> tuple:
        """Get DataFrame shape.

        Args:
            df: DataFrame

        Returns:
            (rows, columns)
        """
        pass

    @abstractmethod
    def count_rows(self, df: Any) -> int:
        """Count rows in DataFrame.

        Args:
            df: DataFrame

        Returns:
            Row count
        """
        pass

    @abstractmethod
    def count_nulls(self, df: Any, columns: List[str]) -> Dict[str, int]:
        """Count nulls in specified columns.

        Args:
            df: DataFrame
            columns: Columns to check

        Returns:
            Dictionary of column -> null count
        """
        pass

    @abstractmethod
    def validate_schema(self, df: Any, schema_rules: Dict[str, Any]) -> List[str]:
        """Validate DataFrame schema.

        Args:
            df: DataFrame
            schema_rules: Validation rules

        Returns:
            List of validation failures (empty if valid)
        """
        pass

    @abstractmethod
    def validate_data(self, df: Any, validation_config: Any) -> List[str]:
        """Validate data against rules.

        Args:
            df: DataFrame to validate
            validation_config: ValidationConfig object

        Returns:
            List of validation failure messages (empty if valid)
        """
        pass

    @abstractmethod
    def get_sample(self, df: Any, n: int = 10) -> List[Dict[str, Any]]:
        """Get sample rows as list of dictionaries.

        Args:
            df: DataFrame
            n: Number of rows to return

        Returns:
            List of row dictionaries
        """
        pass

    def get_source_files(self, df: Any) -> List[str]:
        """Get list of source files that generated this DataFrame.

        Args:
            df: DataFrame

        Returns:
            List of file paths (or empty list if not applicable/supported)
        """
        return []

    def profile_nulls(self, df: Any) -> Dict[str, float]:
        """Calculate null percentage for each column.

        Args:
            df: DataFrame

        Returns:
            Dictionary of {column_name: null_percentage} (0.0 to 1.0)
        """
        return {}

    @abstractmethod
    def table_exists(
        self, connection: Any, table: Optional[str] = None, path: Optional[str] = None
    ) -> bool:
        """Check if table or location exists.

        Args:
            connection: Connection object
            table: Table name (for catalog tables)
            path: File path (for path-based tables)

        Returns:
            True if table/location exists, False otherwise
        """
        pass

    @abstractmethod
    def harmonize_schema(self, df: Any, target_schema: Dict[str, str], policy: Any) -> Any:
        """Harmonize DataFrame schema with target schema according to policy.

        Args:
            df: Input DataFrame
            target_schema: Target schema (column name -> type)
            policy: SchemaPolicyConfig object

        Returns:
            Harmonized DataFrame
        """
        pass

    @abstractmethod
    def anonymize(
        self, df: Any, columns: List[str], method: str, salt: Optional[str] = None
    ) -> Any:
        """Anonymize specified columns.

        Args:
            df: DataFrame to anonymize
            columns: List of columns to anonymize
            method: Method ('hash', 'mask', 'redact')
            salt: Optional salt for hashing

        Returns:
            Anonymized DataFrame
        """
        pass

    def get_table_schema(
        self,
        connection: Any,
        table: Optional[str] = None,
        path: Optional[str] = None,
        format: Optional[str] = None,
    ) -> Optional[Dict[str, str]]:
        """Get schema of an existing table/file.

        Args:
            connection: Connection object
            table: Table name
            path: File path
            format: Data format (optional, helps with file-based sources)

        Returns:
            Schema dict or None if table doesn't exist or schema fetch fails.
        """
        return None

    def maintain_table(
        self,
        connection: Any,
        format: str,
        table: Optional[str] = None,
        path: Optional[str] = None,
        config: Optional[Any] = None,
    ) -> None:
        """Run table maintenance operations (optimize, vacuum).

        Args:
            connection: Connection object
            format: Table format
            table: Table name
            path: Table path
            config: AutoOptimizeConfig object
        """
        pass
