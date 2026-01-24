"""
Azure SQL Database Connection
==============================

Provides connectivity to Azure SQL databases with authentication support.
"""

from typing import Any, Dict, List, Optional

import pandas as pd

from odibi.connections.base import BaseConnection
from odibi.exceptions import ConnectionError
from odibi.utils.error_suggestions import get_suggestions_for_connection
from odibi.utils.logging import logger
from odibi.utils.logging_context import get_logging_context


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
        ctx = get_logging_context()
        ctx.log_connection(
            connection_type="azure_sql",
            connection_name=f"{server}/{database}",
            action="init",
            server=server,
            database=database,
            auth_mode=auth_mode,
            port=port,
        )

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

        ctx.debug(
            "AzureSQL connection initialized",
            server=server,
            database=database,
            auth_mode=auth_mode,
            driver=driver,
        )

    def get_password(self) -> Optional[str]:
        """Get password (cached)."""
        ctx = get_logging_context()

        if self.password:
            ctx.debug(
                "Using provided password",
                server=self.server,
                database=self.database,
            )
            return self.password

        if self._cached_key:
            ctx.debug(
                "Using cached password",
                server=self.server,
                database=self.database,
            )
            return self._cached_key

        if self.auth_mode == "key_vault":
            if not self.key_vault_name or not self.secret_name:
                ctx.error(
                    "Key Vault mode requires key_vault_name and secret_name",
                    server=self.server,
                    database=self.database,
                )
                raise ValueError(
                    f"key_vault mode requires 'key_vault_name' and 'secret_name' "
                    f"for connection to {self.server}/{self.database}. "
                    f"Got key_vault_name={self.key_vault_name or '(missing)'}, "
                    f"secret_name={self.secret_name or '(missing)'}."
                )

            ctx.debug(
                "Fetching password from Key Vault",
                server=self.server,
                key_vault_name=self.key_vault_name,
                secret_name=self.secret_name,
            )

            try:
                from azure.identity import DefaultAzureCredential
                from azure.keyvault.secrets import SecretClient

                credential = DefaultAzureCredential()
                kv_uri = f"https://{self.key_vault_name}.vault.azure.net"
                client = SecretClient(vault_url=kv_uri, credential=credential)
                secret = client.get_secret(self.secret_name)
                self._cached_key = secret.value
                logger.register_secret(self._cached_key)

                ctx.info(
                    "Successfully fetched password from Key Vault",
                    server=self.server,
                    key_vault_name=self.key_vault_name,
                )
                return self._cached_key
            except ImportError as e:
                ctx.error(
                    "Key Vault support requires azure libraries",
                    server=self.server,
                    error=str(e),
                )
                raise ImportError(
                    "Key Vault support requires 'azure-identity' and 'azure-keyvault-secrets'. "
                    "Install with: pip install odibi[azure]"
                )

        ctx.debug(
            "No password required for auth_mode",
            server=self.server,
            auth_mode=self.auth_mode,
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
        ctx = get_logging_context()
        ctx.debug(
            "Building ODBC connection string",
            server=self.server,
            database=self.database,
            auth_mode=self.auth_mode,
        )

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
            ctx.debug(
                "Using SQL authentication",
                server=self.server,
                username=self.username,
            )
        elif self.auth_mode == "aad_msi":
            dsn += "Authentication=ActiveDirectoryMsi;"
            ctx.debug(
                "Using AAD Managed Identity authentication",
                server=self.server,
            )
        elif self.auth_mode == "aad_service_principal":
            # Not fully supported via ODBC string simply without token usually
            ctx.debug(
                "Using AAD Service Principal authentication",
                server=self.server,
            )

        return dsn

    def get_path(self, relative_path: str) -> str:
        """Get table reference for relative path."""
        return relative_path

    def validate(self) -> None:
        """Validate Azure SQL connection configuration."""
        ctx = get_logging_context()
        ctx.debug(
            "Validating AzureSQL connection",
            server=self.server,
            database=self.database,
            auth_mode=self.auth_mode,
        )

        if not self.server:
            ctx.error("AzureSQL validation failed: missing 'server'")
            raise ValueError(
                "Azure SQL connection requires 'server'. "
                "Provide the SQL server hostname (e.g., server: 'myserver.database.windows.net')."
            )
        if not self.database:
            ctx.error(
                "AzureSQL validation failed: missing 'database'",
                server=self.server,
            )
            raise ValueError(
                f"Azure SQL connection requires 'database' for server '{self.server}'."
            )

        if self.auth_mode == "sql":
            if not self.username:
                ctx.error(
                    "AzureSQL validation failed: SQL auth requires username",
                    server=self.server,
                    database=self.database,
                )
                raise ValueError(
                    f"Azure SQL with auth_mode='sql' requires 'username' "
                    f"for connection to {self.server}/{self.database}."
                )
            if not self.password and not (self.key_vault_name and self.secret_name):
                ctx.error(
                    "AzureSQL validation failed: SQL auth requires password",
                    server=self.server,
                    database=self.database,
                )
                raise ValueError(
                    "Azure SQL with auth_mode='sql' requires password "
                    "(or key_vault_name/secret_name)"
                )

        if self.auth_mode == "key_vault":
            if not self.key_vault_name or not self.secret_name:
                ctx.error(
                    "AzureSQL validation failed: key_vault mode missing config",
                    server=self.server,
                    database=self.database,
                )
                raise ValueError(
                    "Azure SQL with auth_mode='key_vault' requires key_vault_name and secret_name"
                )
            if not self.username:
                ctx.error(
                    "AzureSQL validation failed: key_vault mode requires username",
                    server=self.server,
                    database=self.database,
                )
                raise ValueError("Azure SQL with auth_mode='key_vault' requires username")

        ctx.info(
            "AzureSQL connection validated successfully",
            server=self.server,
            database=self.database,
            auth_mode=self.auth_mode,
        )

    def get_engine(self) -> Any:
        """
        Get or create SQLAlchemy engine.

        Returns:
            SQLAlchemy engine instance

        Raises:
            ConnectionError: If connection fails or drivers missing
        """
        ctx = get_logging_context()

        if self._engine is not None:
            ctx.debug(
                "Using cached SQLAlchemy engine",
                server=self.server,
                database=self.database,
            )
            return self._engine

        ctx.debug(
            "Creating SQLAlchemy engine",
            server=self.server,
            database=self.database,
        )

        try:
            from urllib.parse import quote_plus

            from sqlalchemy import create_engine
        except ImportError as e:
            ctx.error(
                "SQLAlchemy import failed",
                server=self.server,
                database=self.database,
                error=str(e),
            )
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

            ctx.debug(
                "Creating SQLAlchemy engine with connection pooling",
                server=self.server,
                database=self.database,
            )

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

            ctx.info(
                "SQLAlchemy engine created successfully",
                server=self.server,
                database=self.database,
            )

            return self._engine

        except Exception as e:
            suggestions = self._get_error_suggestions(str(e))
            ctx.error(
                "Failed to create SQLAlchemy engine",
                server=self.server,
                database=self.database,
                error=str(e),
                suggestions=suggestions,
            )
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
        ctx = get_logging_context()
        ctx.debug(
            "Executing SQL query",
            server=self.server,
            database=self.database,
            query_length=len(query),
        )

        try:
            engine = self.get_engine()
            # Get raw DBAPI connection for pandas compatibility
            with engine.connect() as conn:
                # Use the underlying DBAPI connection
                raw_conn = conn.connection
                result = pd.read_sql(query, raw_conn, params=params)

            ctx.info(
                "SQL query executed successfully",
                server=self.server,
                database=self.database,
                rows_returned=len(result),
            )
            return result
        except Exception as e:
            if isinstance(e, ConnectionError):
                raise
            ctx.error(
                "SQL query execution failed",
                server=self.server,
                database=self.database,
                error=str(e),
            )
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
        ctx = get_logging_context()
        ctx.info(
            "Reading table",
            server=self.server,
            database=self.database,
            table_name=table_name,
            schema=schema,
        )

        if schema:
            query = f"SELECT * FROM [{schema}].[{table_name}]"
        else:
            query = f"SELECT * FROM [{table_name}]"

        return self.read_sql(query)

    def read_sql_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Execute a SQL query and return results as DataFrame.

        Use this for custom SELECT queries (e.g., to exclude unsupported columns).

        Args:
            query: SQL SELECT query
            params: Optional parameters for parameterized query

        Returns:
            Query results as pandas DataFrame
        """
        return self.read_sql(query, params)

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
        ctx = get_logging_context()
        ctx.info(
            "Writing DataFrame to table",
            server=self.server,
            database=self.database,
            table_name=table_name,
            schema=schema,
            rows=len(df),
            if_exists=if_exists,
            chunksize=chunksize,
        )

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

            result_rows = rows_written if rows_written is not None else len(df)
            ctx.info(
                "Table write completed successfully",
                server=self.server,
                database=self.database,
                table_name=table_name,
                rows_written=result_rows,
            )
            return result_rows
        except Exception as e:
            if isinstance(e, ConnectionError):
                raise
            ctx.error(
                "Table write failed",
                server=self.server,
                database=self.database,
                table_name=table_name,
                error=str(e),
            )
            raise ConnectionError(
                connection_name=f"AzureSQL({self.server})",
                reason=f"Write operation failed: {str(e)}",
                suggestions=self._get_error_suggestions(str(e)),
            )

    def execute_sql(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute SQL statement (INSERT, UPDATE, DELETE, etc.).

        Alias for execute() - used by SqlServerMergeWriter.

        Args:
            sql: SQL statement
            params: Optional parameters for parameterized query

        Returns:
            Result from execution

        Raises:
            ConnectionError: If execution fails
        """
        return self.execute(sql, params)

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
        ctx = get_logging_context()
        ctx.debug(
            "Executing SQL statement",
            server=self.server,
            database=self.database,
            statement_length=len(sql),
        )

        try:
            engine = self.get_engine()
            from sqlalchemy import text

            # Use begin() for proper transaction handling in SQLAlchemy 1.4+
            with engine.begin() as conn:
                result = conn.execute(text(sql), params or {})
                # Fetch all results before transaction ends
                if result.returns_rows:
                    rows = result.fetchall()
                else:
                    rows = None
                # Transaction auto-commits on exit from begin() context

                ctx.info(
                    "SQL statement executed successfully",
                    server=self.server,
                    database=self.database,
                )
                return rows
        except Exception as e:
            if isinstance(e, ConnectionError):
                raise
            ctx.error(
                "SQL statement execution failed",
                server=self.server,
                database=self.database,
                error=str(e),
            )
            raise ConnectionError(
                connection_name=f"AzureSQL({self.server})",
                reason=f"Statement execution failed: {str(e)}",
                suggestions=self._get_error_suggestions(str(e)),
            )

    def close(self):
        """Close database connection and dispose of engine."""
        ctx = get_logging_context()
        ctx.debug(
            "Closing AzureSQL connection",
            server=self.server,
            database=self.database,
        )

        if self._engine:
            self._engine.dispose()
            self._engine = None
            ctx.info(
                "AzureSQL connection closed",
                server=self.server,
                database=self.database,
            )

    def _get_error_suggestions(self, error_msg: str) -> List[str]:
        """Generate suggestions using centralized error suggestion engine."""
        try:
            error = Exception(error_msg)
            return get_suggestions_for_connection(
                error=error,
                connection_name=self.name if hasattr(self, "name") else "azure_sql",
                connection_type="azure_sql",
                auth_mode=self.auth_mode,
            )
        except Exception:
            return []

    def get_spark_options(self) -> Dict[str, str]:
        """Get Spark JDBC options.

        Returns:
            Dictionary of Spark JDBC options (url, user, password, etc.)
        """
        ctx = get_logging_context()
        ctx.info(
            "Building Spark JDBC options",
            server=self.server,
            database=self.database,
            auth_mode=self.auth_mode,
        )

        jdbc_url = (
            f"jdbc:sqlserver://{self.server}:{self.port};"
            f"databaseName={self.database};encrypt=true;trustServerCertificate=true;"
        )

        if self.auth_mode == "aad_msi":
            jdbc_url += (
                "hostNameInCertificate=*.database.windows.net;"
                "loginTimeout=30;authentication=ActiveDirectoryMsi;"
            )
            ctx.debug(
                "Configured JDBC URL for AAD MSI",
                server=self.server,
            )
        elif self.auth_mode == "aad_service_principal":
            # Not fully implemented in init yet, but placeholder
            ctx.debug(
                "Configured JDBC URL for AAD Service Principal",
                server=self.server,
            )

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

            ctx.debug(
                "Added SQL authentication to Spark options",
                server=self.server,
                username=self.username,
            )

        ctx.info(
            "Spark JDBC options built successfully",
            server=self.server,
            database=self.database,
        )

        return options
