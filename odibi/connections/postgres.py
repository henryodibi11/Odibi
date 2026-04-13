"""
PostgreSQL Database Connection
===============================

Provides connectivity to PostgreSQL databases with standard authentication.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd

from odibi.connections.base import BaseConnection
from odibi.discovery.types import (
    CatalogSummary,
    Column,
    DatasetRef,
    FreshnessResult,
    PreviewResult,
    Relationship,
    Schema,
    TableProfile,
)
from odibi.exceptions import ConnectionError
from odibi.utils.logging_context import get_logging_context


class PostgreSQLConnection(BaseConnection):
    """
    PostgreSQL database connection.

    Supports:
    - Standard authentication (username/password)
    - SSL connections
    - Connection pooling via SQLAlchemy
    - Read/write operations
    - Spark JDBC integration
    """

    sql_dialect = "postgres"
    default_schema = "public"

    def __init__(
        self,
        host: str,
        database: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        port: int = 5432,
        timeout: int = 30,
        sslmode: str = "prefer",
        **kwargs,
    ):
        """
        Initialize PostgreSQL connection.

        Args:
            host: PostgreSQL server hostname
            database: Database name
            username: Database username
            password: Database password
            port: PostgreSQL port (default: 5432)
            timeout: Connection timeout in seconds (default: 30)
            sslmode: SSL mode ('disable', 'allow', 'prefer', 'require',
                     'verify-ca', 'verify-full'). Default: 'prefer'
        """
        ctx = get_logging_context()
        ctx.log_connection(
            connection_type="postgres",
            connection_name=f"{host}/{database}",
            action="init",
            server=host,
            database=database,
            port=port,
        )

        self.host = host
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.timeout = timeout
        self.sslmode = sslmode
        self._engine = None

        ctx.debug(
            "PostgreSQL connection initialized",
            server=host,
            database=database,
            port=port,
            sslmode=sslmode,
        )

    def get_path(self, relative_path: str) -> str:
        """Get table reference for relative path.

        In PostgreSQL, the relative path is the table reference itself
        (e.g., "schema.table" or "table"), so this method returns it as-is.
        """
        return relative_path

    def validate(self) -> None:
        """Validate PostgreSQL connection configuration."""
        ctx = get_logging_context()
        ctx.debug(
            "Validating PostgreSQL connection",
            server=self.host,
            database=self.database,
        )

        if not self.host:
            ctx.error("PostgreSQL validation failed: missing 'host'")
            raise ValueError(
                "PostgreSQL connection requires 'host'. "
                "Provide the server hostname (e.g., host: 'localhost')."
            )
        if not self.database:
            ctx.error(
                "PostgreSQL validation failed: missing 'database'",
                server=self.host,
            )
            raise ValueError(f"PostgreSQL connection requires 'database' for host '{self.host}'.")

        ctx.info(
            "PostgreSQL connection validated successfully",
            server=self.host,
            database=self.database,
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
                server=self.host,
                database=self.database,
            )
            return self._engine

        ctx.debug(
            "Creating SQLAlchemy engine",
            server=self.host,
            database=self.database,
        )

        try:
            from sqlalchemy import create_engine
        except ImportError as e:
            ctx.error(
                "SQLAlchemy import failed",
                server=self.host,
                database=self.database,
                error=str(e),
            )
            raise ConnectionError(
                connection_name=f"PostgreSQL({self.host})",
                reason="Required package 'sqlalchemy' not found.",
                suggestions=[
                    "Install required packages: pip install sqlalchemy psycopg2-binary",
                    "Or install odibi with postgres extras: pip install 'odibi[postgres]'",
                ],
            )

        try:
            from urllib.parse import quote_plus

            # Build PostgreSQL connection URL
            if self.username and self.password:
                user_part = f"{quote_plus(self.username)}:{quote_plus(self.password)}@"
            elif self.username:
                user_part = f"{quote_plus(self.username)}@"
            else:
                user_part = ""

            connection_url = (
                f"postgresql+psycopg2://{user_part}{self.host}:{self.port}/{self.database}"
            )

            connect_args = {}
            if self.sslmode != "prefer":
                connect_args["sslmode"] = self.sslmode
            connect_args["connect_timeout"] = self.timeout

            ctx.debug(
                "Creating SQLAlchemy engine with connection pooling",
                server=self.host,
                database=self.database,
            )

            self._engine = create_engine(
                connection_url,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False,
                connect_args=connect_args,
            )

            # Test connection
            with self._engine.connect():
                pass

            ctx.info(
                "SQLAlchemy engine created successfully",
                server=self.host,
                database=self.database,
            )

            return self._engine

        except Exception as e:
            ctx.error(
                "Failed to create SQLAlchemy engine",
                server=self.host,
                database=self.database,
                error=str(e),
            )
            raise ConnectionError(
                connection_name=f"PostgreSQL({self.host})",
                reason=f"Failed to create engine: {self._sanitize_error(str(e))}",
                suggestions=self._get_error_suggestions(str(e)),
            )

    def read_sql(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Execute SQL query and return results as DataFrame.

        Args:
            query: SQL query string
            params: Optional query parameters for parameterized queries

        Returns:
            Query results as pandas DataFrame
        """
        ctx = get_logging_context()
        ctx.debug(
            "Executing SQL query",
            server=self.host,
            database=self.database,
            query_length=len(query),
        )

        try:
            engine = self.get_engine()
            with engine.connect() as conn:
                result = pd.read_sql(query, conn, params=params)

            ctx.info(
                "SQL query executed successfully",
                server=self.host,
                database=self.database,
                rows_returned=len(result),
            )
            return result
        except Exception as e:
            if isinstance(e, ConnectionError):
                raise
            ctx.error(
                "SQL query execution failed",
                server=self.host,
                database=self.database,
                error=str(e),
            )
            raise ConnectionError(
                connection_name=f"PostgreSQL({self.host})",
                reason=f"Query execution failed: {self._sanitize_error(str(e))}",
                suggestions=self._get_error_suggestions(str(e)),
            )

    def read_table(self, table_name: str, schema: Optional[str] = None) -> pd.DataFrame:
        """
        Read entire table into DataFrame.

        Args:
            table_name: Name of the table
            schema: Schema name (default: public)

        Returns:
            Table contents as pandas DataFrame
        """
        schema = schema or self.default_schema
        ctx = get_logging_context()
        ctx.info(
            "Reading table",
            server=self.host,
            database=self.database,
            table_name=table_name,
            schema=schema,
        )

        query = self.build_select_query(table_name, schema)
        return self.read_sql(query)

    def read_sql_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Execute a SQL query and return results as DataFrame."""
        return self.read_sql(query, params)

    def write_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: Optional[str] = None,
        if_exists: str = "replace",
        index: bool = False,
        chunksize: Optional[int] = 1000,
    ) -> int:
        """
        Write DataFrame to SQL table.

        Args:
            df: DataFrame to write
            table_name: Name of the table
            schema: Schema name (default: public)
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
            index: Whether to write DataFrame index as column
            chunksize: Number of rows to write in each batch

        Returns:
            Number of rows written
        """
        schema = schema or self.default_schema
        ctx = get_logging_context()
        ctx.info(
            "Writing DataFrame to table",
            server=self.host,
            database=self.database,
            table_name=table_name,
            schema=schema,
            rows=len(df),
            if_exists=if_exists,
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
                method="multi",
            )

            result_rows = rows_written if rows_written is not None else len(df)
            ctx.info(
                "Table write completed successfully",
                server=self.host,
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
                server=self.host,
                database=self.database,
                table_name=table_name,
                error=str(e),
            )
            raise ConnectionError(
                connection_name=f"PostgreSQL({self.host})",
                reason=f"Write operation failed: {self._sanitize_error(str(e))}",
                suggestions=self._get_error_suggestions(str(e)),
            )

    def execute_sql(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute SQL statement. Alias for execute()."""
        return self.execute(sql, params)

    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute SQL statement (INSERT, UPDATE, DELETE, etc.).

        Args:
            sql: SQL statement
            params: Optional parameters for parameterized query

        Returns:
            Result from execution
        """
        ctx = get_logging_context()
        ctx.debug(
            "Executing SQL statement",
            server=self.host,
            database=self.database,
            statement_length=len(sql),
        )

        try:
            engine = self.get_engine()
            from sqlalchemy import text

            with engine.begin() as conn:
                result = conn.execute(text(sql), params or {})
                if result.returns_rows:
                    rows = result.fetchall()
                else:
                    rows = None

                ctx.info(
                    "SQL statement executed successfully",
                    server=self.host,
                    database=self.database,
                )
                return rows
        except Exception as e:
            if isinstance(e, ConnectionError):
                raise
            ctx.error(
                "SQL statement execution failed",
                server=self.host,
                database=self.database,
                error=str(e),
            )
            raise ConnectionError(
                connection_name=f"PostgreSQL({self.host})",
                reason=f"Statement execution failed: {self._sanitize_error(str(e))}",
                suggestions=self._get_error_suggestions(str(e)),
            )

    # -------------------------------------------------------------------------
    # SQL Dialect Helpers
    # -------------------------------------------------------------------------

    def quote_identifier(self, name: str) -> str:
        """Quote an identifier using PostgreSQL double-quote notation."""
        return f'"{name}"'

    def qualify_table(self, table_name: str, schema: str = "") -> str:
        """Build a PostgreSQL qualified table reference."""
        schema = schema or self.default_schema
        if schema:
            return f'"{schema}"."{table_name}"'
        return f'"{table_name}"'

    def build_select_query(
        self,
        table_name: str,
        schema: str = "",
        where: str = "",
        limit: int = -1,
        columns: str = "*",
    ) -> str:
        """Build a SELECT query using PostgreSQL syntax."""
        qualified = self.qualify_table(table_name, schema)
        query = f"SELECT {columns} FROM {qualified}"
        if where:
            query += f" WHERE {where}"
        if limit >= 0:
            query += f" LIMIT {limit}"
        return query

    # -------------------------------------------------------------------------
    # Discovery Methods
    # -------------------------------------------------------------------------

    def list_schemas(self) -> List[str]:
        """List all schemas in the database."""
        ctx = get_logging_context()
        ctx.debug("Listing schemas", server=self.host, database=self.database)

        query = """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN (
                'pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1'
            )
            AND schema_name NOT LIKE 'pg_temp_%'
            AND schema_name NOT LIKE 'pg_toast_temp_%'
            ORDER BY schema_name
        """

        try:
            df = self.read_sql(query)
            schemas = df["schema_name"].tolist()
            ctx.info("Schemas listed successfully", count=len(schemas))
            return schemas
        except Exception as e:
            ctx.error("Failed to list schemas", error=str(e))
            return []

    def list_tables(self, schema: str = "public") -> List[Dict]:
        """List tables and views in a schema."""
        ctx = get_logging_context()
        ctx.debug("Listing tables", schema=schema)

        query = """
            SELECT table_name, table_type, table_schema
            FROM information_schema.tables
            WHERE table_schema = :schema
            ORDER BY table_name
        """

        try:
            df = self.read_sql(query, params={"schema": schema})
            tables = []
            for _, row in df.iterrows():
                tables.append(
                    {
                        "name": row["table_name"],
                        "type": "table" if row["table_type"] == "BASE TABLE" else "view",
                        "schema": row["table_schema"],
                    }
                )
            ctx.info("Tables listed successfully", schema=schema, count=len(tables))
            return tables
        except Exception as e:
            ctx.error("Failed to list tables", schema=schema, error=str(e))
            return []

    def get_table_info(self, table: str) -> Dict:
        """Get detailed schema information for a table."""
        ctx = get_logging_context()

        if "." in table:
            parts = table.split(".")
            schema = parts[0]
            table_name = parts[1]
        else:
            schema = "public"
            table_name = table

        ctx.debug("Getting table info", schema=schema, table=table_name)

        query = """
            SELECT column_name, data_type, is_nullable, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position
        """

        try:
            df = self.read_sql(query, params={"schema": schema, "table": table_name})

            columns = []
            for _, row in df.iterrows():
                columns.append(
                    {
                        "name": row["column_name"],
                        "dtype": row["data_type"],
                        "nullable": row["is_nullable"] == "YES",
                    }
                )

            # Try to get approximate row count
            row_count = None
            try:
                count_query = """
                    SELECT n_live_tup AS row_count
                    FROM pg_stat_user_tables
                    WHERE schemaname = :schema AND relname = :table
                """
                count_df = self.read_sql(
                    count_query, params={"schema": schema, "table": table_name}
                )
                if not count_df.empty and count_df["row_count"].iloc[0] is not None:
                    row_count = int(count_df["row_count"].iloc[0])
            except Exception:
                pass

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
        """Discover all datasets in the database."""
        ctx = get_logging_context()
        ctx.info(
            "Discovering catalog",
            include_schema=include_schema,
            include_stats=include_stats,
            limit=limit,
        )

        all_schemas = self.list_schemas()
        if path:
            schemas = [s for s in all_schemas if s == path]
            if not schemas:
                ctx.warning(f"Schema not found: {path}")
                return CatalogSummary(
                    connection_name=f"{self.host}/{self.database}",
                    connection_type="postgres",
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
            connection_name=f"{self.host}/{self.database}",
            connection_type="postgres",
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
        """Profile a table with statistical analysis."""
        ctx = get_logging_context()

        if "." in dataset:
            parts = dataset.split(".")
            schema = parts[0]
            table_name = parts[1]
        else:
            schema = "public"
            table_name = dataset

        ctx.info("Profiling table", schema=schema, table=table_name, sample_rows=sample_rows)

        col_filter = ", ".join(f'"{c}"' for c in columns) if columns else "*"
        query = self.build_select_query(table_name, schema, columns=col_filter, limit=sample_rows)

        try:
            df = self.read_sql(query)

            # Get approximate total row count
            total_rows = None
            try:
                count_query = """
                    SELECT n_live_tup AS row_count
                    FROM pg_stat_user_tables
                    WHERE schemaname = :schema AND relname = :table
                """
                count_df = self.read_sql(
                    count_query, params={"schema": schema, "table": table_name}
                )
                if not count_df.empty and count_df["row_count"].iloc[0] is not None:
                    total_rows = int(count_df["row_count"].iloc[0])
            except Exception:
                total_rows = len(df)

            profiled_columns = []
            candidate_keys = []
            candidate_watermarks = []

            for col in df.columns:
                null_count = int(df[col].isnull().sum())
                null_pct = null_count / len(df) if len(df) > 0 else 0
                distinct_count = int(df[col].nunique())

                if distinct_count == len(df):
                    cardinality = "unique"
                    candidate_keys.append(col)
                elif distinct_count > len(df) * 0.9:
                    cardinality = "high"
                elif distinct_count < 10:
                    cardinality = "low"
                else:
                    cardinality = "medium"

                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    candidate_watermarks.append(col)

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
                    (
                        f"Sampled {len(df)} of {total_rows} rows"
                        if total_rows
                        else f"Sampled {len(df)} rows"
                    ),
                    (
                        f"Found {len(candidate_keys)} candidate key columns: {candidate_keys}"
                        if candidate_keys
                        else "No unique key columns found"
                    ),
                    (
                        f"Found {len(candidate_watermarks)} timestamp columns: {candidate_watermarks}"
                        if candidate_watermarks
                        else "No timestamp columns for incremental loading"
                    ),
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

    def preview(
        self, dataset: str, rows: int = 5, columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Preview sample rows from a PostgreSQL table."""
        ctx = get_logging_context()

        max_rows = min(rows, 100)

        if "." in dataset:
            parts = dataset.split(".")
            schema = parts[0]
            table_name = parts[1]
        else:
            schema = "public"
            table_name = dataset

        ctx.info("Previewing table", schema=schema, table=table_name, rows=max_rows)

        try:
            col_filter = "*"
            if columns:
                col_filter = ", ".join(f'"{c}"' for c in columns)

            query = self.build_select_query(table_name, schema, columns=col_filter, limit=max_rows)
            df = self.read_sql(query)

            # Get approximate row count
            total_rows = None
            try:
                count_query = """
                    SELECT n_live_tup AS row_count
                    FROM pg_stat_user_tables
                    WHERE schemaname = :schema AND relname = :table
                """
                count_df = self.read_sql(
                    count_query, params={"schema": schema, "table": table_name}
                )
                if not count_df.empty and count_df["row_count"].iloc[0] is not None:
                    total_rows = int(count_df["row_count"].iloc[0])
            except Exception:
                pass

            result = PreviewResult(
                dataset=DatasetRef(
                    name=table_name,
                    namespace=schema,
                    kind="table",
                    path=f"{schema}.{table_name}",
                    row_count=total_rows,
                ),
                columns=df.columns.tolist(),
                rows=df.to_dict(orient="records"),
                total_rows=total_rows,
                truncated=(total_rows or 0) > max_rows,
            )

            ctx.info("Preview complete", schema=schema, table=table_name, rows_returned=len(df))
            return result.model_dump()

        except Exception as e:
            ctx.error("Failed to preview table", schema=schema, table=table_name, error=str(e))
            return PreviewResult(
                dataset=DatasetRef(name=table_name, namespace=schema, kind="table"),
            ).model_dump()

    def relationships(self, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """Discover foreign key relationships in the database."""
        ctx = get_logging_context()
        ctx.info("Discovering relationships", schema=schema or "all")

        query = """
            SELECT
                tc.constraint_name AS fk_name,
                ccu.table_schema AS parent_schema,
                ccu.table_name AS parent_table,
                ccu.column_name AS parent_column,
                kcu.table_schema AS child_schema,
                kcu.table_name AS child_table,
                kcu.column_name AS child_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
                AND tc.table_schema = ccu.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
        """

        if schema:
            query += "\n            AND (kcu.table_schema = :schema OR ccu.table_schema = :schema)"

        query += "\n            ORDER BY tc.constraint_name, kcu.ordinal_position"

        try:
            params = {"schema": schema} if schema else None
            df = self.read_sql(query, params=params)

            if df.empty:
                ctx.info("No foreign key relationships found", schema=schema or "all")
                return []

            relationships = []
            for fk_name, group in df.groupby("fk_name"):
                first = group.iloc[0]
                keys = [(row["parent_column"], row["child_column"]) for _, row in group.iterrows()]

                rel = Relationship(
                    parent=DatasetRef(
                        name=first["parent_table"],
                        namespace=first["parent_schema"],
                        kind="table",
                        path=f"{first['parent_schema']}.{first['parent_table']}",
                    ),
                    child=DatasetRef(
                        name=first["child_table"],
                        namespace=first["child_schema"],
                        kind="table",
                        path=f"{first['child_schema']}.{first['child_table']}",
                    ),
                    keys=keys,
                    source="declared",
                    confidence=1.0,
                    details={"constraint_name": fk_name},
                )
                relationships.append(rel.model_dump())

            ctx.info("Relationships discovered", count=len(relationships))
            return relationships

        except Exception as e:
            ctx.error("Failed to discover relationships", error=str(e))
            return []

    def get_freshness(
        self,
        dataset: str,
        timestamp_column: Optional[str] = None,
    ) -> Dict:
        """Get data freshness information."""
        ctx = get_logging_context()

        if "." in dataset:
            parts = dataset.split(".")
            schema = parts[0]
            table_name = parts[1]
        else:
            schema = "public"
            table_name = dataset

        ctx.debug("Getting freshness", schema=schema, table=table_name, column=timestamp_column)

        dataset_ref = DatasetRef(
            name=table_name,
            namespace=schema,
            kind="table",
            path=f"{schema}.{table_name}",
        )

        if timestamp_column:
            try:
                query = f'SELECT MAX("{timestamp_column}") AS max_ts FROM "{schema}"."{table_name}"'
                df = self.read_sql(query)

                if not df.empty and df["max_ts"].iloc[0] is not None:
                    last_updated = pd.to_datetime(df["max_ts"].iloc[0])
                    age_hours = (
                        datetime.now(timezone.utc)
                        - last_updated.to_pydatetime().replace(tzinfo=timezone.utc)
                    ).total_seconds() / 3600

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

        # Fallback to table metadata (last analyze/autoanalyze time)
        try:
            query = """
                SELECT GREATEST(
                    COALESCE(last_analyze, '1970-01-01'),
                    COALESCE(last_autoanalyze, '1970-01-01')
                ) AS modify_date
                FROM pg_stat_user_tables
                WHERE schemaname = :schema AND relname = :table
            """
            df = self.read_sql(query, params={"schema": schema, "table": table_name})

            if not df.empty and df["modify_date"].iloc[0] is not None:
                last_updated = pd.to_datetime(df["modify_date"].iloc[0])
                if last_updated.year > 1970:
                    age_hours = (
                        datetime.now(timezone.utc)
                        - last_updated.to_pydatetime().replace(tzinfo=timezone.utc)
                    ).total_seconds() / 3600

                    result = FreshnessResult(
                        dataset=dataset_ref,
                        last_updated=last_updated,
                        source="metadata",
                        age_hours=round(age_hours, 2),
                        details={"note": "Last analyze time from pg_stat_user_tables"},
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

    def get_spark_options(self) -> Dict[str, str]:
        """Get Spark JDBC options for PostgreSQL.

        Returns:
            Dictionary of Spark JDBC options (url, driver, user, password)

        Note:
            Spark requires the PostgreSQL JDBC driver jar on the classpath.
            Add it via: --packages org.postgresql:postgresql:42.7.3
        """
        ctx = get_logging_context()
        ctx.info(
            "Building Spark JDBC options",
            server=self.host,
            database=self.database,
        )

        jdbc_url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

        if self.sslmode not in ("prefer", "disable"):
            jdbc_url += f"?sslmode={self.sslmode}"

        options = {
            "url": jdbc_url,
            "driver": "org.postgresql.Driver",
        }

        if self.username:
            options["user"] = self.username
        if self.password:
            options["password"] = self.password

        ctx.info(
            "Spark JDBC options built successfully",
            server=self.host,
            database=self.database,
        )

        return options

    def close(self):
        """Close database connection and dispose of engine."""
        ctx = get_logging_context()
        ctx.debug(
            "Closing PostgreSQL connection",
            server=self.host,
            database=self.database,
        )

        if self._engine:
            self._engine.dispose()
            self._engine = None
            ctx.info(
                "PostgreSQL connection closed",
                server=self.host,
                database=self.database,
            )

    def _sanitize_error(self, error_msg: str) -> str:
        """Remove credentials from error messages."""
        import re

        sanitized = re.sub(r"password=[^&\s]*", "password=***", error_msg, flags=re.IGNORECASE)
        sanitized = re.sub(r"user=[^&\s]*", "user=***", sanitized, flags=re.IGNORECASE)
        sanitized = re.sub(r"://[^@]+@", "://***:***@", sanitized)
        return sanitized

    def _get_error_suggestions(self, error_msg: str) -> List[str]:
        """Generate suggestions for common PostgreSQL errors."""
        suggestions = []
        lower = error_msg.lower()

        if "could not connect" in lower or "connection refused" in lower:
            suggestions.extend(
                [
                    f"Check that PostgreSQL is running on {self.host}:{self.port}",
                    "Verify pg_hba.conf allows connections from this host",
                    "Check firewall rules",
                ]
            )
        elif "authentication failed" in lower or "password" in lower:
            suggestions.extend(
                [
                    "Verify username and password are correct",
                    "Check pg_hba.conf authentication method",
                ]
            )
        elif "does not exist" in lower:
            suggestions.extend(
                [
                    f"Verify database '{self.database}' exists",
                    "Run: SELECT datname FROM pg_database;",
                ]
            )
        elif "psycopg2" in lower:
            suggestions.extend(
                [
                    "Install the PostgreSQL driver: pip install psycopg2-binary",
                    "Or install odibi with postgres extras: pip install 'odibi[postgres]'",
                ]
            )

        return suggestions
