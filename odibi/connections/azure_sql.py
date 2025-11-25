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
        auth_mode: str = "aad_msi",  # "aad_msi", "sql", "key_vault"
        key_vault_name: Optional[str] = None,
        secret_name: Optional[str] = None,
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
            auth_mode: Authentication mode ('aad_msi', 'sql', 'key_vault')
            key_vault_name: Key Vault name (required if auth_mode='key_vault')
            secret_name: Secret name containing password (required if auth_mode='key_vault')
            port: SQL Server port (default: 1433)
            timeout: Connection timeout in seconds (default: 30)
        """
        self.server = server
        self.database = database
        self.driver = driver
        self.username = username
        self.password = password
        self.auth_mode = auth_mode
        self.key_vault_name = key_vault_name
        self.secret_name = secret_name
        self.port = port
        self.timeout = timeout
        self._engine = None
        self._cached_key = None  # For consistency with ADLS / parallel fetch

    def get_password(self) -> Optional[str]:
        """Get password (cached)."""
        if self.password:
            return self.password

        if self._cached_key:
            return self._cached_key

        if self.auth_mode == "key_vault":
            if not self.key_vault_name or not self.secret_name:
                raise ValueError("key_vault mode requires key_vault_name and secret_name")

            try:
                from azure.identity import DefaultAzureCredential
                from azure.keyvault.secrets import SecretClient

                credential = DefaultAzureCredential()
                kv_uri = f"https://{self.key_vault_name}.vault.azure.net"
                client = SecretClient(vault_url=kv_uri, credential=credential)
                secret = client.get_secret(self.secret_name)
                self._cached_key = secret.value
                return self._cached_key
            except ImportError:
                raise ImportError(
                    "Key Vault support requires 'azure-identity' and 'azure-keyvault-secrets'"
                )

        return None

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
            f"TrustServerCertificate=yes;"
            f"Connection Timeout=30;"
        )

        pwd = self.get_password()
        if self.username and pwd:
            dsn += f"UID={self.username};PWD={pwd};"
        elif self.auth_mode == "aad_msi":
            dsn += "Authentication=ActiveDirectoryMsi;"
        elif self.auth_mode == "aad_service_principal":
            # Not fully supported via ODBC string simply without token usually
            pass

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
            if not self.username:
                raise ValueError("Azure SQL with auth_mode='sql' requires username")
            if not self.password and not (self.key_vault_name and self.secret_name):
                raise ValueError(
                    "Azure SQL with auth_mode='sql' requires password (or key_vault_name/secret_name)"
                )

        if self.auth_mode == "key_vault":
            if not self.key_vault_name or not self.secret_name:
                raise ValueError(
                    "Azure SQL with auth_mode='key_vault' requires key_vault_name and secret_name"
                )
            if not self.username:
                raise ValueError("Azure SQL with auth_mode='key_vault' requires username")

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
                    "Or install odibi with azure extras: pip install 'odibi[azure]'",
                ],
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
            with self._engine.connect():
                pass

            return self._engine

        except Exception as e:
            suggestions = self._get_error_suggestions(str(e))
            raise ConnectionError(
                connection_name=f"AzureSQL({self.server})",
                reason=f"Failed to create engine: {str(e)}",
                suggestions=suggestions,
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
                suggestions=self._get_error_suggestions(str(e)),
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
                suggestions=self._get_error_suggestions(str(e)),
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
                suggestions=self._get_error_suggestions(str(e)),
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

    def get_spark_options(self) -> Dict[str, str]:
        """Get Spark JDBC options.

        Returns:
            Dictionary of Spark JDBC options (url, user, password, etc.)
        """
        # Build JDBC URL
        # Note: Spark uses its own JDBC driver (usually com.microsoft.sqlserver.jdbc.SQLServerDriver)
        # We rely on the driver being present in the Spark environment.

        jdbc_url = f"jdbc:sqlserver://{self.server}:{self.port};databaseName={self.database};"

        if self.auth_mode == "aad_msi":
            # For MSI, append authentication property to URL
            jdbc_url += "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;authentication=ActiveDirectoryMsi;"
        elif self.auth_mode == "aad_service_principal":
            # Not fully implemented in init yet, but placeholder
            pass

        options = {
            "url": jdbc_url,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }

        if self.auth_mode == "sql" or self.auth_mode == "key_vault":
            if self.username:
                options["user"] = self.username

            pwd = self.get_password()
            if pwd:
                options["password"] = pwd

        return options
