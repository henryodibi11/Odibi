"""Tests for PostgreSQL connection.

Tests the PostgreSQLConnection class including:
- Initialization and validation
- SQL dialect helpers (quoting, query building)
- Factory registration
- Engine parity (format detection, default schema, merge guards)
"""

from unittest.mock import Mock, patch

import pandas as pd
import pytest

from odibi.connections.postgres import PostgreSQLConnection
from odibi.connections.sql_utils import SQL_FORMATS, is_sql_format
from odibi.connections.base import BaseConnection


# ============================================================================
# SQL Dialect Helpers
# ============================================================================


class TestPostgreSQLDialect:
    """Test PostgreSQL dialect properties and methods."""

    def test_sql_dialect(self):
        conn = PostgreSQLConnection(host="localhost", database="testdb")
        assert conn.sql_dialect == "postgres"

    def test_default_schema(self):
        conn = PostgreSQLConnection(host="localhost", database="testdb")
        assert conn.default_schema == "public"

    def test_quote_identifier(self):
        conn = PostgreSQLConnection(host="localhost", database="testdb")
        assert conn.quote_identifier("my_table") == '"my_table"'
        assert conn.quote_identifier("Column Name") == '"Column Name"'

    def test_qualify_table_default_schema(self):
        conn = PostgreSQLConnection(host="localhost", database="testdb")
        assert conn.qualify_table("orders") == '"public"."orders"'

    def test_qualify_table_explicit_schema(self):
        conn = PostgreSQLConnection(host="localhost", database="testdb")
        assert conn.qualify_table("orders", "sales") == '"sales"."orders"'

    def test_build_select_query_basic(self):
        conn = PostgreSQLConnection(host="localhost", database="testdb")
        query = conn.build_select_query("orders")
        assert query == 'SELECT * FROM "public"."orders"'

    def test_build_select_query_with_schema(self):
        conn = PostgreSQLConnection(host="localhost", database="testdb")
        query = conn.build_select_query("orders", schema="sales")
        assert query == 'SELECT * FROM "sales"."orders"'

    def test_build_select_query_with_where(self):
        conn = PostgreSQLConnection(host="localhost", database="testdb")
        query = conn.build_select_query("orders", where="id > 5")
        assert query == 'SELECT * FROM "public"."orders" WHERE id > 5'

    def test_build_select_query_with_limit(self):
        conn = PostgreSQLConnection(host="localhost", database="testdb")
        query = conn.build_select_query("orders", limit=10)
        assert query == 'SELECT * FROM "public"."orders" LIMIT 10'

    def test_build_select_query_limit_zero(self):
        """Schema inference uses LIMIT 0 instead of TOP 0."""
        conn = PostgreSQLConnection(host="localhost", database="testdb")
        query = conn.build_select_query("orders", limit=0)
        assert query == 'SELECT * FROM "public"."orders" LIMIT 0'
        assert "TOP" not in query

    def test_build_select_query_full(self):
        conn = PostgreSQLConnection(host="localhost", database="testdb")
        query = conn.build_select_query(
            "orders", schema="sales", where="amount > 100", limit=50, columns="id, amount"
        )
        assert query == 'SELECT id, amount FROM "sales"."orders" WHERE amount > 100 LIMIT 50'


# ============================================================================
# SQL Server Dialect Comparison
# ============================================================================


class TestAzureSQLDialect:
    """Verify AzureSQL dialect still works correctly after changes."""

    def test_sql_dialect(self):
        from odibi.connections.azure_sql import AzureSQL

        conn = AzureSQL(
            server="localhost", database="testdb", auth_mode="sql", username="sa", password="test"
        )
        assert conn.sql_dialect == "mssql"

    def test_default_schema(self):
        from odibi.connections.azure_sql import AzureSQL

        assert AzureSQL.default_schema == "dbo"

    def test_build_select_query_uses_top(self):
        from odibi.connections.azure_sql import AzureSQL

        conn = AzureSQL(
            server="localhost", database="testdb", auth_mode="sql", username="sa", password="test"
        )
        query = conn.build_select_query("orders", limit=10)
        assert "TOP (10)" in query
        assert "LIMIT" not in query

    def test_qualify_table_uses_brackets(self):
        from odibi.connections.azure_sql import AzureSQL

        conn = AzureSQL(
            server="localhost", database="testdb", auth_mode="sql", username="sa", password="test"
        )
        assert conn.qualify_table("orders") == "[dbo].[orders]"


# ============================================================================
# Initialization & Validation
# ============================================================================


class TestPostgreSQLInit:
    """Test connection initialization."""

    def test_default_port(self):
        conn = PostgreSQLConnection(host="localhost", database="testdb")
        assert conn.port == 5432

    def test_custom_port(self):
        conn = PostgreSQLConnection(host="localhost", database="testdb", port=5433)
        assert conn.port == 5433

    def test_default_sslmode(self):
        conn = PostgreSQLConnection(host="localhost", database="testdb")
        assert conn.sslmode == "prefer"

    def test_get_path_returns_as_is(self):
        conn = PostgreSQLConnection(host="localhost", database="testdb")
        assert conn.get_path("public.orders") == "public.orders"

    def test_validate_missing_host(self):
        conn = PostgreSQLConnection(host="", database="testdb")
        with pytest.raises(ValueError, match="host"):
            conn.validate()

    def test_validate_missing_database(self):
        conn = PostgreSQLConnection(host="localhost", database="")
        with pytest.raises(ValueError, match="database"):
            conn.validate()

    def test_validate_success(self):
        conn = PostgreSQLConnection(host="localhost", database="testdb")
        conn.validate()  # Should not raise

    def test_is_base_connection(self):
        conn = PostgreSQLConnection(host="localhost", database="testdb")
        assert isinstance(conn, BaseConnection)


# ============================================================================
# Spark JDBC Options
# ============================================================================


class TestSparkOptions:
    """Test Spark JDBC option generation."""

    def test_basic_jdbc_url(self):
        conn = PostgreSQLConnection(host="pg.example.com", database="analytics", port=5432)
        opts = conn.get_spark_options()
        assert opts["url"] == "jdbc:postgresql://pg.example.com:5432/analytics"
        assert opts["driver"] == "org.postgresql.Driver"

    def test_jdbc_with_auth(self):
        conn = PostgreSQLConnection(
            host="localhost", database="testdb", username="myuser", password="mypass"
        )
        opts = conn.get_spark_options()
        assert opts["user"] == "myuser"
        assert opts["password"] == "mypass"

    def test_jdbc_ssl_appended(self):
        conn = PostgreSQLConnection(host="localhost", database="testdb", sslmode="require")
        opts = conn.get_spark_options()
        assert "sslmode=require" in opts["url"]


# ============================================================================
# SQL Format Detection
# ============================================================================


class TestSQLFormatDetection:
    """Test centralized SQL format detection."""

    def test_sql_formats_includes_postgres(self):
        assert "postgres" in SQL_FORMATS
        assert "postgresql" in SQL_FORMATS

    def test_sql_formats_includes_sql_server(self):
        assert "sql" in SQL_FORMATS
        assert "sql_server" in SQL_FORMATS
        assert "azure_sql" in SQL_FORMATS

    def test_is_sql_format_postgres(self):
        assert is_sql_format("postgres") is True
        assert is_sql_format("postgresql") is True

    def test_is_sql_format_sql_server(self):
        assert is_sql_format("sql_server") is True
        assert is_sql_format("azure_sql") is True
        assert is_sql_format("sql") is True

    def test_is_sql_format_non_sql(self):
        assert is_sql_format("delta") is False
        assert is_sql_format("parquet") is False
        assert is_sql_format("csv") is False


# ============================================================================
# Factory Registration
# ============================================================================


class TestPostgresFactory:
    """Test factory registration and creation."""

    def test_factory_registered(self):
        from odibi.plugins import get_connection_factory

        # Ensure builtins are registered
        from odibi.connections.factory import register_builtins

        register_builtins()

        factory = get_connection_factory("postgres")
        assert factory is not None

    def test_factory_postgresql_alias(self):
        from odibi.plugins import get_connection_factory
        from odibi.connections.factory import register_builtins

        register_builtins()

        factory = get_connection_factory("postgresql")
        assert factory is not None

    def test_factory_creates_connection(self):
        from odibi.connections.factory import create_postgres_connection

        conn = create_postgres_connection(
            "test_pg",
            {
                "host": "localhost",
                "database": "testdb",
                "username": "user",
                "password": "pass",
                "port": 5432,
            },
        )
        assert isinstance(conn, PostgreSQLConnection)
        assert conn.host == "localhost"
        assert conn.database == "testdb"
        assert conn.username == "user"

    def test_factory_missing_host_raises(self):
        from odibi.connections.factory import create_postgres_connection

        with pytest.raises(ValueError, match="missing 'host'"):
            create_postgres_connection("test_pg", {"database": "testdb"})

    def test_factory_missing_database_raises(self):
        from odibi.connections.factory import create_postgres_connection

        with pytest.raises(ValueError, match="missing 'database'"):
            create_postgres_connection("test_pg", {"host": "localhost"})

    def test_factory_supports_auth_block(self):
        from odibi.connections.factory import create_postgres_connection

        conn = create_postgres_connection(
            "test_pg",
            {
                "host": "localhost",
                "database": "testdb",
                "auth": {
                    "username": "pguser",
                    "password": "pgpass",
                },
            },
        )
        assert conn.username == "pguser"
        assert conn.password == "pgpass"


# ============================================================================
# Engine Parity: Merge Guard
# ============================================================================


class TestMergeGuard:
    """Test that merge mode is rejected for PostgreSQL connections."""

    def test_pandas_merge_rejects_postgres(self):
        from odibi.engine.pandas_engine import PandasEngine

        engine = PandasEngine()
        conn = Mock()
        conn.sql_dialect = "postgres"
        df = pd.DataFrame({"id": [1]})

        with pytest.raises(NotImplementedError, match="only supported for SQL Server"):
            engine._write_sql(df, conn, "tbl", "merge", {"merge_keys": ["id"]})

    def test_pandas_merge_allows_mssql(self):
        """Verify SQL Server merge still works."""
        from odibi.engine.pandas_engine import PandasEngine

        engine = PandasEngine()
        conn = Mock()
        conn.sql_dialect = "mssql"
        df = pd.DataFrame({"id": [1]})

        mock_result = Mock()
        mock_result.inserted = 1
        mock_result.updated = 0
        mock_result.deleted = 0
        mock_result.total_affected = 1
        mock_writer = Mock()
        mock_writer.merge_pandas.return_value = mock_result

        mock_module = Mock()
        mock_module.SqlServerMergeWriter.return_value = mock_writer
        with patch.dict("sys.modules", {"odibi.writers.sql_server_writer": mock_module}):
            result = engine._write_sql(df, conn, "tbl", "merge", {"merge_keys": ["id"]})
        assert result["mode"] == "merge"


# ============================================================================
# Engine Parity: Default Schema
# ============================================================================


class TestEngineDefaultSchema:
    """Test that engines use connection's default_schema."""

    def test_pandas_uses_connection_default_schema(self):
        from odibi.engine.pandas_engine import PandasEngine

        engine = PandasEngine()
        conn = Mock()
        conn.default_schema = "public"
        conn.sql_dialect = "postgres"
        conn.read_table.return_value = pd.DataFrame({"id": [1]})

        engine._write_sql(pd.DataFrame({"id": [1]}), conn, "orders", "append", {})

        conn.write_table.assert_called_once()
        call_kwargs = conn.write_table.call_args
        assert (
            call_kwargs.kwargs.get("schema") == "public" or call_kwargs[1].get("schema") == "public"
        )


# ============================================================================
# Error Handling
# ============================================================================


class TestErrorSanitization:
    """Test credential sanitization in error messages."""

    def test_sanitize_password(self):
        conn = PostgreSQLConnection(host="localhost", database="testdb")
        result = conn._sanitize_error("password=mysecret&timeout=30")
        assert "mysecret" not in result
        assert "***" in result

    def test_sanitize_connection_url(self):
        conn = PostgreSQLConnection(host="localhost", database="testdb")
        result = conn._sanitize_error("postgresql://user:secret@host:5432/db")
        assert "secret" not in result
        assert "***" in result

    def test_error_suggestions_connection_refused(self):
        conn = PostgreSQLConnection(host="localhost", database="testdb", port=5432)
        suggestions = conn._get_error_suggestions("could not connect to server: connection refused")
        assert any("running" in s.lower() for s in suggestions)

    def test_error_suggestions_auth_failed(self):
        conn = PostgreSQLConnection(host="localhost", database="testdb")
        suggestions = conn._get_error_suggestions("authentication failed for user")
        assert any("password" in s.lower() for s in suggestions)

    def test_error_suggestions_psycopg2(self):
        conn = PostgreSQLConnection(host="localhost", database="testdb")
        suggestions = conn._get_error_suggestions("No module named 'psycopg2'")
        assert any("psycopg2" in s for s in suggestions)


# ============================================================================
# Node Quoting Parity
# ============================================================================


class TestNodeQuoting:
    """Test that node.py quotes PostgreSQL columns correctly."""

    def test_postgres_format_uses_double_quotes(self):
        from odibi.node import NodeExecutor

        executor = NodeExecutor.__new__(NodeExecutor)
        result = executor._quote_sql_column("updated_at", "postgres", "pandas")
        assert result == '"updated_at"'

    def test_postgresql_format_uses_double_quotes(self):
        from odibi.node import NodeExecutor

        executor = NodeExecutor.__new__(NodeExecutor)
        result = executor._quote_sql_column("col", "postgresql", "pandas")
        assert result == '"col"'

    def test_sql_server_still_uses_brackets(self):
        from odibi.node import NodeExecutor

        executor = NodeExecutor.__new__(NodeExecutor)
        result = executor._quote_sql_column("col", "sql_server", "pandas")
        assert result == "[col]"
