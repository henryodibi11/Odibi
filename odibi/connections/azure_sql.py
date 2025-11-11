"""
Azure SQL Database Connection
==============================

Provides connectivity to Azure SQL databases with authentication support.
"""

from typing import Optional, Dict, Any
import pandas as pd
from .base import BaseConnection


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
            ImportError: If required packages not installed
        """
        if self._engine is not None:
            return self._engine

        try:
            from sqlalchemy import create_engine
            from urllib.parse import quote_plus
        except ImportError:
            raise ImportError(
                "SQLAlchemy is required for Azure SQL operations. "
                "Install with: pip install sqlalchemy pyodbc"
            )

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

        return self._engine

    def read_sql(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Execute SQL query and return results as DataFrame.

        Args:
            query: SQL query string
            params: Optional query parameters for parameterized queries

        Returns:
            Query results as pandas DataFrame

        Example:
            >>> conn = AzureSQL(server="myserver.database.windows.net", database="mydb")
            >>> df = conn.read_sql("SELECT * FROM users WHERE age > :min_age", {"min_age": 18})
        """
        engine = self.get_engine()
        return pd.read_sql(query, engine, params=params)

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

        Example:
            >>> conn = AzureSQL(server="myserver.database.windows.net", database="mydb")
            >>> conn.write_table(df, "users", if_exists="append")
        """
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

    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute SQL statement (INSERT, UPDATE, DELETE, etc.).

        Args:
            sql: SQL statement
            params: Optional parameters for parameterized query

        Returns:
            Result from execution
        """
        engine = self.get_engine()

        with engine.connect() as conn:
            result = conn.execute(sql, params or {})
            conn.commit()
            return result

    def close(self):
        """Close database connection and dispose of engine."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
