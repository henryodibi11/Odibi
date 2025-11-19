"""
Azure SQL Database Connection
==============================

Provides connectivity to Azure SQL databases with authentication support.
"""

from typing import Optional, Dict, Any, List
import pandas as pd
from odibi.connections.base import BaseConnection
from odibi.exceptions import ConnectionError


class AzureSQL(BaseConnection):
    """
    Azure SQL Database connection.

    Supports:
    - SQL authentication (username/password)
    - Azure Active Directory Managed Identity
    - Connection pooling
    - Read/write operations via SQLAlchemy
    """

    def __init__(
        self,
        server: str,
        database: str,
        driver: str = "ODBC Driver 18 for SQL Server",
        username: Optional[str] = None,
        password: Optional[str] = None,
        auth_mode: str = "aad_msi",  # "aad_msi" or "sql"
        port: int = 1433,
        timeout: int = 30,
        **kwargs,
    ):
        """
        Initialize Azure SQL connection.

        Args:
            server: SQL server hostname (e.g., 'myserver.database.windows.net')
            database: Database name
            driver: ODBC driver name (default: ODBC Driver 18 for SQL Server)
            username: SQL auth username (required if auth_mode='sql')
            password: SQL auth password (required if auth_mode='sql')
            auth_mode: Authentication mode ('aad_msi' or 'sql')
            port: SQL Server port (default: 1433)
            timeout: Connection timeout in seconds (default: 30)
        """
        self.server = server
        self.database = database
        self.driver = driver
        self.username = username
        self.password = password
        self.auth_mode = auth_mode
        self.port = port
        self.timeout = timeout
        self._engine = None

    def odbc_dsn(self) -> str:
        """Build ODBC connection string.

        Returns:
            ODBC DSN string

        Example:
            >>> conn = AzureSQL(server="myserver.database.windows.net", database="mydb")
            >>> conn.odbc_dsn()
            'Driver={ODBC Driver 18 for SQL Server};Server=tcp:myserver...'
        """
        dsn = (
            f"Driver={{{self.driver}}};"
            f"Server=tcp:{self.server},1433;"
            f"Database={self.database};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )

        if self.username and self.password:
            dsn += f"UID={self.username};PWD={self.password};"
        else:
            dsn += "Authentication=ActiveDirectoryMsi;"

        return dsn

    def get_path(self, relative_path: str) -> str:
        """Get table reference for relative path."""
        return relative_path

    def validate(self) -> None:
        """Validate Azure SQL connection configuration."""
        if not self.server:
            raise ValueError("Azure SQL connection requires 'server'")
        if not self.database:
            raise ValueError("Azure SQL connection requires 'database'")

        if self.auth_mode == "sql":
            if not self.username or not self.password:
                raise ValueError("Azure SQL with auth_mode='sql' requires username and password")

    def get_engine(self):
        """
        Get or create SQLAlchemy engine.

        Returns:
            SQLAlchemy engine instance

        Raises:
            ConnectionError: If connection fails or drivers missing
        """
        if self._engine is not None:
            return self._engine

        try:
            from sqlalchemy import create_engine
            from urllib.parse import quote_plus
        except ImportError:
            raise ConnectionError(
                connection_name=f"AzureSQL({self.server})",
                reason="Required packages 'sqlalchemy' or 'pyodbc' not found.",
                suggestions=[
                    "Install required packages: pip install sqlalchemy pyodbc",
                    "Or install odibi with azure extras: pip install 'odibi[azure]'"
                ]
            )

        try:
            # Build connection string
            conn_str = self.odbc_dsn()
            connection_url = f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}"

            # Create engine with connection pooling
            self._engine = create_engine(
                connection_url,
                pool_pre_ping=True,  # Verify connections before use
                pool_recycle=3600,  # Recycle connections after 1 hour
                echo=False,
            )
            
            # Test connection
            with self._engine.connect() as conn:
                pass
                
            return self._engine

        except Exception as e:
            suggestions = self._get_error_suggestions(str(e))
            raise ConnectionError(
                connection_name=f"AzureSQL({self.server})",
                reason=f"Failed to create engine: {str(e)}",
                suggestions=suggestions
            )

    def read_sql(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Execute SQL query and return results as DataFrame.

        Args:
            query: SQL query string
            params: Optional query parameters for parameterized queries

        Returns:
            Query results as pandas DataFrame
            
        Raises:
            ConnectionError: If execution fails
        """
        try:
            engine = self.get_engine()
            return pd.read_sql(query, engine, params=params)
        except Exception as e:
            if isinstance(e, ConnectionError):
                raise
            raise ConnectionError(
                connection_name=f"AzureSQL({self.server})",
                reason=f"Query execution failed: {str(e)}",
                suggestions=self._get_error_suggestions(str(e))
            )

    def read_table(self, table_name: str, schema: Optional[str] = "dbo") -> pd.DataFrame:
        """
        Read entire table into DataFrame.

        Args:
            table_name: Name of the table
            schema: Schema name (default: dbo)

        Returns:
            Table contents as pandas DataFrame
        """
        if schema:
            query = f"SELECT * FROM [{schema}].[{table_name}]"
        else:
            query = f"SELECT * FROM [{table_name}]"

        return self.read_sql(query)

    def write_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: Optional[str] = "dbo",
        if_exists: str = "replace",
        index: bool = False,
        chunksize: Optional[int] = 1000,
    ) -> int:
        """
        Write DataFrame to SQL table.

        Args:
            df: DataFrame to write
            table_name: Name of the table
            schema: Schema name (default: dbo)
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
            index: Whether to write DataFrame index as column
            chunksize: Number of rows to write in each batch (default: 1000)

        Returns:
            Number of rows written
            
        Raises:
            ConnectionError: If write fails
        """
        try:
            engine = self.get_engine()

            rows_written = df.to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists=if_exists,
                index=index,
                chunksize=chunksize,
                method="multi",  # Use multi-row INSERT for better performance
            )

            return rows_written if rows_written is not None else len(df)
        except Exception as e:
            if isinstance(e, ConnectionError):
                raise
            raise ConnectionError(
                connection_name=f"AzureSQL({self.server})",
                reason=f"Write operation failed: {str(e)}",
                suggestions=self._get_error_suggestions(str(e))
            )

    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute SQL statement (INSERT, UPDATE, DELETE, etc.).

        Args:
            sql: SQL statement
            params: Optional parameters for parameterized query

        Returns:
            Result from execution
            
        Raises:
            ConnectionError: If execution fails
        """
        try:
            engine = self.get_engine()
            from sqlalchemy import text

            with engine.connect() as conn:
                result = conn.execute(text(sql), params or {})
                conn.commit()
                return result
        except Exception as e:
            if isinstance(e, ConnectionError):
                raise
            raise ConnectionError(
                connection_name=f"AzureSQL({self.server})",
                reason=f"Statement execution failed: {str(e)}",
                suggestions=self._get_error_suggestions(str(e))
            )

    def close(self):
        """Close database connection and dispose of engine."""
        if self._engine:
            self._engine.dispose()
            self._engine = None

    def _get_error_suggestions(self, error_msg: str) -> List[str]:
        """Generate suggestions based on error message."""
        suggestions = []
        error_lower = error_msg.lower()

        if "login failed" in error_lower:
            suggestions.append("Check username and password")
            suggestions.append(f"Verify auth_mode is correct (current: {self.auth_mode})")
            if "identity" in error_lower:
                suggestions.append("Ensure Managed Identity has access to the database")
        
        if "firewall" in error_lower or "tcp provider" in error_lower:
            suggestions.append("Check Azure SQL Server firewall rules")
            suggestions.append("Ensure client IP is allowed")
            
        if "driver" in error_lower:
            suggestions.append(f"Verify ODBC driver '{self.driver}' is installed")
            suggestions.append("On Linux: sudo apt-get install msodbcsql18")
            
        return suggestions

