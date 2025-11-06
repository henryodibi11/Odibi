"""Pandas engine implementation."""

from typing import Any, Dict, List, Optional
import pandas as pd
from pathlib import Path

from odibi.engine.base import Engine
from odibi.context import Context, PandasContext
from odibi.exceptions import TransformError


class PandasEngine(Engine):
    """Pandas-based execution engine."""
    
    def __init__(self):
        """Initialize Pandas engine."""
        pass
    
    def read(
        self,
        connection: Any,
        format: str,
        table: Optional[str] = None,
        path: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """Read data using Pandas.
        
        Args:
            connection: Connection object (with get_path method)
            format: Data format
            table: Table name
            path: File path
            options: Format-specific options
            
        Returns:
            Pandas DataFrame
        """
        options = options or {}
        
        # Get full path from connection
        if path:
            full_path = connection.get_path(path)
        elif table:
            full_path = connection.get_path(table)
        else:
            raise ValueError("Either path or table must be provided")
        
        # Read based on format
        if format == "csv":
            return pd.read_csv(full_path, **options)
        elif format == "parquet":
            return pd.read_parquet(full_path, **options)
        elif format == "json":
            return pd.read_json(full_path, **options)
        elif format == "excel":
            return pd.read_excel(full_path, **options)
        else:
            raise ValueError(f"Unsupported format for Pandas engine: {format}")
    
    def write(
        self,
        df: pd.DataFrame,
        connection: Any,
        format: str,
        table: Optional[str] = None,
        path: Optional[str] = None,
        mode: str = "overwrite",
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Write data using Pandas.
        
        Args:
            df: DataFrame to write
            connection: Connection object
            format: Output format
            table: Table name
            path: File path
            mode: Write mode
            options: Format-specific options
        """
        options = options or {}
        
        # Get full path from connection
        if path:
            full_path = connection.get_path(path)
        elif table:
            full_path = connection.get_path(table)
        else:
            raise ValueError("Either path or table must be provided")
        
        # Ensure parent directory exists
        Path(full_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Write based on format
        if format == "csv":
            mode_param = "w" if mode == "overwrite" else "a"
            df.to_csv(full_path, mode=mode_param, index=False, **options)
        elif format == "parquet":
            # Parquet doesn't support append easily
            df.to_parquet(full_path, index=False, **options)
        elif format == "json":
            mode_param = "w" if mode == "overwrite" else "a"
            df.to_json(full_path, orient="records", **options)
        elif format == "excel":
            df.to_excel(full_path, index=False, **options)
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
                locals_dict = {
                    name: context.get(name)
                    for name in context.list_names()
                }
                
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
        df: pd.DataFrame
    ) -> pd.DataFrame:
        """Execute built-in operation.
        
        Args:
            operation: Operation name
            params: Operation parameters
            df: Input DataFrame
            
        Returns:
            Result DataFrame
        """
        if operation == "pivot":
            return self._pivot(df, params)
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
        
        result = df.pivot_table(
            index=group_by,
            columns=pivot_column,
            values=value_column,
            aggfunc=agg_func
        ).reset_index()
        
        # Flatten column names if multi-level
        if isinstance(result.columns, pd.MultiIndex):
            result.columns = ['_'.join(col).strip('_') for col in result.columns.values]
        
        return result
    
    def get_schema(self, df: pd.DataFrame) -> List[str]:
        """Get DataFrame column names.
        
        Args:
            df: DataFrame
            
        Returns:
            List of column names
        """
        return df.columns.tolist()
    
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
    
    def validate_schema(
        self,
        df: pd.DataFrame,
        schema_rules: Dict[str, Any]
    ) -> List[str]:
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
