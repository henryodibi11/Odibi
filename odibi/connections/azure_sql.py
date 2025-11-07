"""Azure SQL Database connection (Phase 1: DSN builder only)."""

from typing import Optional
from .base import BaseConnection


class AzureSQL(BaseConnection):
    """Azure SQL Database connection.

    Phase 1: ODBC DSN string generation only
    Phase 3: Read/write via SQLAlchemy + pyodbc
    """

    def __init__(
        self,
        server: str,
        database: str,
        driver: str = "ODBC Driver 18 for SQL Server",
        username: Optional[str] = None,
        password: Optional[str] = None,
        **kwargs,
    ):
        """Initialize Azure SQL connection.

        Args:
            server: SQL server hostname (without 'tcp:' prefix)
            database: Database name
            driver: ODBC driver name
            username: SQL auth username (optional, uses Azure AD if None)
            password: SQL auth password (optional)
        """
        self.server = server
        self.database = database
        self.driver = driver
        self.username = username
        self.password = password

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
