"""Base engine interface."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from odibi.context import Context


class Engine(ABC):
    """Abstract base class for execution engines."""
    
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
    def execute_operation(
        self,
        operation: str,
        params: Dict[str, Any],
        df: Any
    ) -> Any:
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
    def get_schema(self, df: Any) -> List[str]:
        """Get DataFrame column names.
        
        Args:
            df: DataFrame
            
        Returns:
            List of column names
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
    def validate_schema(
        self,
        df: Any,
        schema_rules: Dict[str, Any]
    ) -> List[str]:
        """Validate DataFrame schema.
        
        Args:
            df: DataFrame
            schema_rules: Validation rules
            
        Returns:
            List of validation failures (empty if valid)
        """
        pass
