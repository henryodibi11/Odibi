"""
Azure SQL Database Connection
==============================

Provides connectivity to Azure SQL databases with authentication support.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from odibi.connections.base import BaseConnection
from odibi.discovery.types import (
    CatalogSummary,
    Column,
    DatasetRef,
    FreshnessResult,
    Schema,
    TableProfile,
)
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
        trust_server_certificate: bool = True,
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
            trust_server_certificate: Whether to trust the server certificate
                (default: True for backward compatibility).
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
        self.trust_server_certificate = trust_server_certificate
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
            f"Server=tcp:{self.server},{self.port};"
            f"Database={self.database};"
            f"Encrypt=yes;"
            f"TrustServerCertificate={'yes' if self.trust_server_certificate else 'no'};"
            f"Connection Timeout={self.timeout};"
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
        """Get table reference for relative path.

        In Azure SQL, the relative path is the table reference itself
        (e.g., "schema.table" or "table"), so this method returns it as-is.

        Args:
            relative_path: Table reference (e.g., "dbo.users", "customers")

        Returns:
            Same table reference unchanged
        """
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
                reason=f"Failed to create engine: {self._sanitize_error(str(e))}",
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
            # Use SQLAlchemy connection directly (preferred by pandas)
            with engine.connect() as conn:
                result = pd.read_sql(query, conn, params=params)

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
                reason=f"Query execution failed: {self._sanitize_error(str(e))}",
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
                reason=f"Write operation failed: {self._sanitize_error(str(e))}",
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
                reason=f"Statement execution failed: {self._sanitize_error(str(e))}",
                suggestions=self._get_error_suggestions(str(e)),
            )

    # -------------------------------------------------------------------------
    # Discovery Methods
    # -------------------------------------------------------------------------

    def list_schemas(self) -> List[str]:
        """List all schemas in the database.

        Returns:
            List of schema names

        Example:
            >>> conn = AzureSQL(server="...", database="mydb")
            >>> schemas = conn.list_schemas()
            >>> print(schemas)
            ['dbo', 'staging', 'warehouse']
        """
        ctx = get_logging_context()
        ctx.debug("Listing schemas", server=self.server, database=self.database)

        query = """
            SELECT SCHEMA_NAME
            FROM INFORMATION_SCHEMA.SCHEMATA
            WHERE SCHEMA_NAME NOT IN ('db_owner', 'db_accessadmin', 'db_securityadmin',
                                      'db_ddladmin', 'db_backupoperator', 'db_datareader',
                                      'db_datawriter', 'db_denydatareader', 'db_denydatawriter',
                                      'sys', 'INFORMATION_SCHEMA', 'guest')
            ORDER BY SCHEMA_NAME
        """

        try:
            df = self.read_sql(query)
            schemas = df["SCHEMA_NAME"].tolist()
            ctx.info("Schemas listed successfully", count=len(schemas))
            return schemas
        except Exception as e:
            ctx.error("Failed to list schemas", error=str(e))
            return []

    def list_tables(self, schema: str = "dbo") -> List[Dict]:
        """List tables and views in a schema.

        Args:
            schema: Schema name (default: "dbo")

        Returns:
            List of dicts with keys: name, type (table/view), schema

        Example:
            >>> conn = AzureSQL(server="...", database="mydb")
            >>> tables = conn.list_tables("dbo")
            >>> print(tables)
            [{'name': 'customers', 'type': 'table', 'schema': 'dbo'},
             {'name': 'orders', 'type': 'table', 'schema': 'dbo'}]
        """
        ctx = get_logging_context()
        ctx.debug("Listing tables", schema=schema)

        query = """
            SELECT TABLE_NAME, TABLE_TYPE, TABLE_SCHEMA
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = :schema
            ORDER BY TABLE_NAME
        """

        try:
            df = self.read_sql(query, params={"schema": schema})
            tables = []
            for _, row in df.iterrows():
                tables.append(
                    {
                        "name": row["TABLE_NAME"],
                        "type": "table" if row["TABLE_TYPE"] == "BASE TABLE" else "view",
                        "schema": row["TABLE_SCHEMA"],
                    }
                )
            ctx.info("Tables listed successfully", schema=schema, count=len(tables))
            return tables
        except Exception as e:
            ctx.error("Failed to list tables", schema=schema, error=str(e))
            return []

    def get_table_info(self, table: str) -> Dict:
        """Get detailed schema information for a table.

        Args:
            table: Table name (can be "schema.table" or just "table")

        Returns:
            Schema-like dict with dataset and columns info

        Example:
            >>> conn = AzureSQL(server="...", database="mydb")
            >>> info = conn.get_table_info("dbo.customers")
            >>> print(info['columns'])
            [{'name': 'customer_id', 'dtype': 'int', ...}, ...]
        """
        ctx = get_logging_context()

        # Parse schema and table name
        if "." in table:
            parts = table.split(".")
            schema = parts[0]
            table_name = parts[1]
        else:
            schema = "dbo"
            table_name = table

        ctx.debug("Getting table info", schema=schema, table=table_name)

        query = """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
            ORDER BY ORDINAL_POSITION
        """

        try:
            df = self.read_sql(query, params={"schema": schema, "table": table_name})

            columns = []
            for _, row in df.iterrows():
                columns.append(
                    {
                        "name": row["COLUMN_NAME"],
                        "dtype": row["DATA_TYPE"],
                        "nullable": row["IS_NULLABLE"] == "YES",
                    }
                )

            # Try to get row count
            row_count = None
            try:
                count_query = """
                    SELECT SUM(p.rows) AS row_count
                    FROM sys.tables t
                    INNER JOIN sys.partitions p ON t.object_id = p.object_id
                    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name = :schema AND t.name = :table
                    AND p.index_id IN (0,1)
                """
                count_df = self.read_sql(
                    count_query, params={"schema": schema, "table": table_name}
                )
                if not count_df.empty and count_df["row_count"].iloc[0] is not None:
                    row_count = int(count_df["row_count"].iloc[0])
            except Exception:
                pass  # Row count is optional

            dataset = DatasetRef(
                name=table_name,
                namespace=schema,
                kind="table",
                path=f"{schema}.{table_name}",
                row_count=row_count,
            )

            schema_obj = Schema(dataset=dataset, columns=[Column(**c) for c in columns])

            ctx.info("Table info retrieved", schema=schema, table=table_name, columns=len(columns))
            return schema_obj.model_dump()

        except Exception as e:
            ctx.error("Failed to get table info", schema=schema, table=table_name, error=str(e))
            return {}

    def discover_catalog(
        self,
        include_schema: bool = False,
        include_stats: bool = False,
        limit: Optional[int] = None,
        recursive: bool = True,
        path: str = "",
        pattern: str = "",
    ) -> Dict:
        """Discover all datasets in the database.

        Args:
            include_schema: If True, include column information for each table
            include_stats: If True, include row counts and stats
            limit: Maximum number of datasets per schema
            recursive: Ignored for SQL (schemas are flat)
            path: Scope to specific schema (e.g. "dbo", "sales")
            pattern: Filter table names by pattern (e.g. "fact_*", "*_2024")

        Returns:
            CatalogSummary dict with schemas and tables

        Example:
            >>> conn = AzureSQL(server="...", database="mydb")
            >>> catalog = conn.discover_catalog(include_schema=True, limit=10)
            >>> print(catalog['total_datasets'])
            25
        """
        ctx = get_logging_context()
        ctx.info(
            "Discovering catalog",
            include_schema=include_schema,
            include_stats=include_stats,
            limit=limit,
        )

        # Filter schemas if path is specified (path = schema name for SQL)
        all_schemas = self.list_schemas()
        if path:
            schemas = [s for s in all_schemas if s == path]
            if not schemas:
                ctx.warning(f"Schema not found: {path}")
                return CatalogSummary(
                    connection_name=f"{self.server}/{self.database}",
                    connection_type="azure_sql",
                    schemas=[],
                    tables=[],
                    total_datasets=0,
                    next_step=f"Schema '{path}' not found. Available: {', '.join(all_schemas)}",
                ).model_dump()
        else:
            schemas = all_schemas

        all_tables = []
        import fnmatch

        has_pattern = bool(pattern)

        for schema in schemas:
            tables = self.list_tables(schema)

            for table_info in tables:
                # Apply pattern filter to table name
                if has_pattern and not fnmatch.fnmatch(table_info["name"], pattern):
                    continue

                if limit and len(all_tables) >= limit:
                    break
                dataset = DatasetRef(
                    name=table_info["name"],
                    namespace=table_info["schema"],
                    kind="table" if table_info["type"] == "table" else "view",
                    path=f"{table_info['schema']}.{table_info['name']}",
                )

                # Optionally get schema and stats
                if include_schema or include_stats:
                    try:
                        full_info = self.get_table_info(
                            f"{table_info['schema']}.{table_info['name']}"
                        )
                        if full_info and "dataset" in full_info:
                            dataset = DatasetRef(**full_info["dataset"])
                    except Exception as e:
                        ctx.debug(
                            "Could not get extended info for table",
                            table=table_info["name"],
                            error=str(e),
                        )

                all_tables.append(dataset)

        catalog = CatalogSummary(
            connection_name=f"{self.server}/{self.database}",
            connection_type="azure_sql",
            schemas=schemas,
            tables=all_tables,
            total_datasets=len(all_tables),
            next_step="Use profile() to analyze specific tables or get_table_info() for schema details",
            suggestions=[
                f"Found {len(all_tables)} tables across {len(schemas)} schemas",
                "Use include_schema=True to get column details",
                "Use include_stats=True for row counts",
            ],
        )

        ctx.info("Catalog discovery complete", total_datasets=len(all_tables), schemas=len(schemas))
        return catalog.model_dump()

    def profile(
        self,
        dataset: str,
        sample_rows: int = 1000,
        columns: Optional[List[str]] = None,
    ) -> Dict:
        """Profile a table with statistical analysis.

        Args:
            dataset: Table name (can be "schema.table" or just "table")
            sample_rows: Number of rows to sample (default: 1000)
            columns: Specific columns to profile (None = all columns)

        Returns:
            TableProfile dict with profiling statistics

        Example:
            >>> conn = AzureSQL(server="...", database="mydb")
            >>> profile = conn.profile("dbo.customers", sample_rows=5000)
            >>> print(profile['candidate_keys'])
            ['customer_id']
        """
        ctx = get_logging_context()

        # Parse schema and table name
        if "." in dataset:
            parts = dataset.split(".")
            schema = parts[0]
            table_name = parts[1]
        else:
            schema = "dbo"
            table_name = dataset

        ctx.info("Profiling table", schema=schema, table=table_name, sample_rows=sample_rows)

        # Read sample data
        col_filter = ", ".join(f"[{c}]" for c in columns) if columns else "*"
        query = f"SELECT TOP ({sample_rows}) {col_filter} FROM [{schema}].[{table_name}]"

        try:
            df = self.read_sql(query)

            # Get total row count
            total_rows = None
            try:
                count_query = """
                    SELECT SUM(p.rows) AS row_count
                    FROM sys.tables t
                    INNER JOIN sys.partitions p ON t.object_id = p.object_id
                    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name = :schema AND t.name = :table
                    AND p.index_id IN (0,1)
                """
                count_df = self.read_sql(
                    count_query, params={"schema": schema, "table": table_name}
                )
                if not count_df.empty and count_df["row_count"].iloc[0] is not None:
                    total_rows = int(count_df["row_count"].iloc[0])
            except Exception:
                total_rows = len(df)

            # Profile columns
            profiled_columns = []
            candidate_keys = []
            candidate_watermarks = []

            for col in df.columns:
                null_count = int(df[col].isnull().sum())
                null_pct = null_count / len(df) if len(df) > 0 else 0
                distinct_count = int(df[col].nunique())

                # Determine cardinality
                if distinct_count == len(df):
                    cardinality = "unique"
                    candidate_keys.append(col)
                elif distinct_count > len(df) * 0.9:
                    cardinality = "high"
                elif distinct_count < 10:
                    cardinality = "low"
                else:
                    cardinality = "medium"

                # Check if datetime (candidate watermark)
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    candidate_watermarks.append(col)

                # Get sample values (non-null)
                sample_values = df[col].dropna().head(5).tolist()

                profiled_columns.append(
                    Column(
                        name=col,
                        dtype=str(df[col].dtype),
                        nullable=null_count > 0,
                        null_count=null_count,
                        null_pct=round(null_pct, 4),
                        cardinality=cardinality,
                        distinct_count=distinct_count,
                        sample_values=sample_values,
                    )
                )

            # Calculate overall completeness
            total_cells = len(df) * len(df.columns)
            null_cells = df.isnull().sum().sum()
            completeness = 1 - (null_cells / total_cells) if total_cells > 0 else 0

            dataset_ref = DatasetRef(
                name=table_name,
                namespace=schema,
                kind="table",
                path=f"{schema}.{table_name}",
                row_count=total_rows,
            )

            profile = TableProfile(
                dataset=dataset_ref,
                rows_sampled=len(df),
                total_rows=total_rows,
                columns=profiled_columns,
                candidate_keys=candidate_keys,
                candidate_watermarks=candidate_watermarks,
                completeness=round(completeness, 4),
                suggestions=[
                    f"Sampled {len(df)} of {total_rows} rows"
                    if total_rows
                    else f"Sampled {len(df)} rows",
                    f"Found {len(candidate_keys)} candidate key columns: {candidate_keys}"
                    if candidate_keys
                    else "No unique key columns found",
                    f"Found {len(candidate_watermarks)} timestamp columns: {candidate_watermarks}"
                    if candidate_watermarks
                    else "No timestamp columns for incremental loading",
                ],
            )

            ctx.info(
                "Table profiling complete",
                schema=schema,
                table=table_name,
                rows_sampled=len(df),
                columns=len(profiled_columns),
                candidate_keys=len(candidate_keys),
            )

            return profile.model_dump()

        except Exception as e:
            ctx.error("Failed to profile table", schema=schema, table=table_name, error=str(e))
            return {}

    def get_freshness(
        self,
        dataset: str,
        timestamp_column: Optional[str] = None,
    ) -> Dict:
        """Get data freshness information.

        Args:
            dataset: Table name (can be "schema.table" or just "table")
            timestamp_column: Column to check for max timestamp (optional)

        Returns:
            FreshnessResult dict with last_updated timestamp

        Example:
            >>> conn = AzureSQL(server="...", database="mydb")
            >>> freshness = conn.get_freshness("dbo.orders", timestamp_column="order_date")
            >>> print(freshness['last_updated'])
            2024-03-15 10:30:00
        """
        ctx = get_logging_context()

        # Parse schema and table name
        if "." in dataset:
            parts = dataset.split(".")
            schema = parts[0]
            table_name = parts[1]
        else:
            schema = "dbo"
            table_name = dataset

        ctx.debug("Getting freshness", schema=schema, table=table_name, column=timestamp_column)

        dataset_ref = DatasetRef(
            name=table_name,
            namespace=schema,
            kind="table",
            path=f"{schema}.{table_name}",
        )

        # If timestamp column specified, query data
        if timestamp_column:
            try:
                query = f"SELECT MAX([{timestamp_column}]) AS max_ts FROM [{schema}].[{table_name}]"
                df = self.read_sql(query)

                if not df.empty and df["max_ts"].iloc[0] is not None:
                    last_updated = pd.to_datetime(df["max_ts"].iloc[0])
                    age_hours = (datetime.utcnow() - last_updated).total_seconds() / 3600

                    result = FreshnessResult(
                        dataset=dataset_ref,
                        last_updated=last_updated,
                        source="data",
                        age_hours=round(age_hours, 2),
                        details={"timestamp_column": timestamp_column},
                    )

                    ctx.info(
                        "Freshness retrieved from data",
                        schema=schema,
                        table=table_name,
                        age_hours=age_hours,
                    )
                    return result.model_dump()
            except Exception as e:
                ctx.debug("Could not get freshness from data column", error=str(e))

        # Fallback to table metadata
        try:
            query = """
                SELECT t.modify_date
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = :schema AND t.name = :table
            """
            df = self.read_sql(query, params={"schema": schema, "table": table_name})

            if not df.empty and df["modify_date"].iloc[0] is not None:
                last_updated = pd.to_datetime(df["modify_date"].iloc[0])
                age_hours = (datetime.utcnow() - last_updated).total_seconds() / 3600

                result = FreshnessResult(
                    dataset=dataset_ref,
                    last_updated=last_updated,
                    source="metadata",
                    age_hours=round(age_hours, 2),
                    details={"note": "Table modification time from sys.tables"},
                )

                ctx.info(
                    "Freshness retrieved from metadata",
                    schema=schema,
                    table=table_name,
                    age_hours=age_hours,
                )
                return result.model_dump()
        except Exception as e:
            ctx.error("Failed to get freshness", schema=schema, table=table_name, error=str(e))

        return {}

    def close(self):
        """Close database connection and dispose of engine.

        Cleanly closes the SQLAlchemy connection pool and disposes of the engine.
        Safe to call multiple times (subsequent calls are no-ops).
        Should be called when done with the connection to free database resources.
        """
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

    def _sanitize_error(self, error_msg: str) -> str:
        """Remove credentials from error messages to prevent leaks.

        Args:
            error_msg: Raw error message that may contain credentials.

        Returns:
            Sanitized error message with credentials redacted.
        """
        import re

        sanitized = re.sub(r"PWD=[^;]*", "PWD=***", error_msg)
        sanitized = re.sub(r"password=[^;]*", "password=***", sanitized, flags=re.IGNORECASE)
        sanitized = re.sub(r"UID=[^;]*", "UID=***", sanitized)
        sanitized = re.sub(r"user=[^;]*", "user=***", sanitized, flags=re.IGNORECASE)
        return sanitized

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
            f"databaseName={self.database};encrypt=true;"
            f"trustServerCertificate={'true' if self.trust_server_certificate else 'false'};"
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
