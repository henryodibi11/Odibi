"""Tests for odibi.connections.postgres.PostgreSQLConnection — full coverage."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.connections.postgres import PostgreSQLConnection
from odibi.exceptions import ConnectionError

# ---------------------------------------------------------------------------
# Autouse fixtures
# ---------------------------------------------------------------------------

_mock_ctx = MagicMock()


@pytest.fixture(autouse=True)
def _patch_logging():
    with patch("odibi.connections.postgres.get_logging_context", return_value=_mock_ctx):
        yield


@pytest.fixture
def conn():
    return PostgreSQLConnection(
        host="localhost", database="testdb", username="user", password="pass"
    )


@pytest.fixture
def conn_no_auth():
    return PostgreSQLConnection(host="localhost", database="testdb")


@pytest.fixture
def conn_user_only():
    return PostgreSQLConnection(host="localhost", database="testdb", username="user")


@pytest.fixture
def conn_engine(conn):
    """Connection with a pre-set mock engine."""
    mock_engine = MagicMock()
    conn._engine = mock_engine
    return conn, mock_engine


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------


class TestInit:
    def test_defaults(self, conn):
        assert conn.host == "localhost"
        assert conn.database == "testdb"
        assert conn.port == 5432
        assert conn.timeout == 30
        assert conn.sslmode == "prefer"
        assert conn._engine is None

    def test_custom_params(self):
        c = PostgreSQLConnection(
            host="pg.example.com",
            database="prod",
            username="admin",
            password="secret",
            port=5433,
            timeout=60,
            sslmode="require",
        )
        assert c.port == 5433
        assert c.sslmode == "require"


# ---------------------------------------------------------------------------
# get_path / validate
# ---------------------------------------------------------------------------


class TestGetPath:
    def test_returns_path_unchanged(self, conn):
        assert conn.get_path("public.users") == "public.users"
        assert conn.get_path("my_table") == "my_table"


class TestValidate:
    def test_success(self, conn):
        conn.validate()  # no exception

    def test_missing_host(self):
        c = PostgreSQLConnection(host="", database="testdb")
        with pytest.raises(ValueError, match="requires 'host'"):
            c.validate()

    def test_missing_database(self):
        c = PostgreSQLConnection(host="localhost", database="")
        with pytest.raises(ValueError, match="requires 'database'"):
            c.validate()


# ---------------------------------------------------------------------------
# get_engine
# ---------------------------------------------------------------------------


class TestGetEngine:
    def test_cached_engine(self, conn_engine):
        conn, engine = conn_engine
        result = conn.get_engine()
        assert result is engine

    def test_creates_engine_user_pass(self, conn):
        mock_engine = MagicMock()
        with patch("sqlalchemy.create_engine", return_value=mock_engine):
            result = conn.get_engine()
        assert result is mock_engine
        assert conn._engine is mock_engine

    def test_creates_engine_user_only(self, conn_user_only):
        mock_engine = MagicMock()
        with patch("sqlalchemy.create_engine", return_value=mock_engine):
            result = conn_user_only.get_engine()
        assert result is mock_engine

    def test_creates_engine_no_auth(self, conn_no_auth):
        mock_engine = MagicMock()
        with patch("sqlalchemy.create_engine", return_value=mock_engine):
            result = conn_no_auth.get_engine()
        assert result is mock_engine

    def test_creates_engine_sslmode_non_prefer(self):
        c = PostgreSQLConnection(host="pg.example.com", database="db", sslmode="require")
        mock_engine = MagicMock()
        with patch("sqlalchemy.create_engine", return_value=mock_engine):
            result = c.get_engine()
        assert result is mock_engine

    def test_sqlalchemy_import_error(self, conn):
        with patch("builtins.__import__", side_effect=ImportError("no sqlalchemy")):
            with pytest.raises(ConnectionError):
                conn.get_engine()

    def test_connection_error(self, conn):
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = Exception("connection refused")
        with patch("sqlalchemy.create_engine", return_value=mock_engine):
            with pytest.raises(ConnectionError, match="Failed to create engine"):
                conn.get_engine()


# ---------------------------------------------------------------------------
# read_sql / read_table / read_sql_query
# ---------------------------------------------------------------------------


class TestReadSql:
    def test_success(self, conn_engine):
        conn, engine = conn_engine
        expected = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        with patch("odibi.connections.postgres.pd.read_sql", return_value=expected):
            result = conn.read_sql("SELECT * FROM t")
        assert len(result) == 2

    def test_with_params(self, conn_engine):
        conn, engine = conn_engine
        expected = pd.DataFrame({"id": [1]})
        with patch("odibi.connections.postgres.pd.read_sql", return_value=expected):
            result = conn.read_sql("SELECT * FROM t WHERE id = :id", params={"id": 1})
        assert len(result) == 1

    def test_connection_error_reraised(self, conn_engine):
        conn, engine = conn_engine
        with patch(
            "odibi.connections.postgres.pd.read_sql",
            side_effect=ConnectionError(connection_name="pg", reason="fail", suggestions=[]),
        ):
            with pytest.raises(ConnectionError):
                conn.read_sql("SELECT 1")

    def test_generic_error_wrapped(self, conn_engine):
        conn, engine = conn_engine
        with patch(
            "odibi.connections.postgres.pd.read_sql",
            side_effect=RuntimeError("boom"),
        ):
            with pytest.raises(ConnectionError, match="Query execution failed"):
                conn.read_sql("SELECT 1")


class TestReadTable:
    def test_default_schema(self, conn_engine):
        conn, engine = conn_engine
        expected = pd.DataFrame({"id": [1]})
        with patch("odibi.connections.postgres.pd.read_sql", return_value=expected):
            result = conn.read_table("users")
        assert len(result) == 1

    def test_custom_schema(self, conn_engine):
        conn, engine = conn_engine
        expected = pd.DataFrame({"id": [1]})
        with patch("odibi.connections.postgres.pd.read_sql", return_value=expected):
            result = conn.read_table("users", schema="sales")
        assert len(result) == 1


class TestReadSqlQuery:
    def test_delegates_to_read_sql(self, conn_engine):
        conn, engine = conn_engine
        expected = pd.DataFrame({"x": [1]})
        with patch("odibi.connections.postgres.pd.read_sql", return_value=expected):
            result = conn.read_sql_query("SELECT 1 AS x")
        assert len(result) == 1


# ---------------------------------------------------------------------------
# write_table
# ---------------------------------------------------------------------------


class TestWriteTable:
    def test_success(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        with patch.object(df, "to_sql", return_value=2):
            result = conn.write_table(df, "users")
        assert result == 2

    def test_to_sql_returns_none(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame({"id": [1, 2]})
        with patch.object(df, "to_sql", return_value=None):
            result = conn.write_table(df, "users")
        assert result == 2  # falls back to len(df)

    def test_error_wrapped(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame({"id": [1]})
        with patch.object(df, "to_sql", side_effect=RuntimeError("fail")):
            with pytest.raises(ConnectionError, match="Write operation failed"):
                conn.write_table(df, "users")

    def test_connection_error_reraised(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame({"id": [1]})
        with patch.object(
            df,
            "to_sql",
            side_effect=ConnectionError(connection_name="pg", reason="fail", suggestions=[]),
        ):
            with pytest.raises(ConnectionError):
                conn.write_table(df, "users")


# ---------------------------------------------------------------------------
# execute / execute_sql
# ---------------------------------------------------------------------------


class TestExecute:
    def test_returns_rows(self, conn_engine):
        conn, engine = conn_engine
        mock_result = MagicMock()
        mock_result.returns_rows = True
        mock_result.fetchall.return_value = [("row1",)]
        mock_conn_obj = MagicMock()
        mock_conn_obj.execute.return_value = mock_result
        engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn_obj)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        result = conn.execute("SELECT 1")
        assert result == [("row1",)]

    def test_no_rows(self, conn_engine):
        conn, engine = conn_engine
        mock_result = MagicMock()
        mock_result.returns_rows = False
        mock_conn_obj = MagicMock()
        mock_conn_obj.execute.return_value = mock_result
        engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn_obj)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        result = conn.execute("INSERT INTO t VALUES (1)")
        assert result is None

    def test_error_wrapped(self, conn_engine):
        conn, engine = conn_engine
        engine.begin.side_effect = RuntimeError("fail")
        with pytest.raises(ConnectionError, match="Statement execution failed"):
            conn.execute("DROP TABLE t")

    def test_connection_error_reraised(self, conn_engine):
        conn, engine = conn_engine
        engine.begin.side_effect = ConnectionError(
            connection_name="pg", reason="fail", suggestions=[]
        )
        with pytest.raises(ConnectionError):
            conn.execute("DROP TABLE t")

    def test_execute_sql_alias(self, conn_engine):
        conn, engine = conn_engine
        mock_result = MagicMock()
        mock_result.returns_rows = False
        mock_conn_obj = MagicMock()
        mock_conn_obj.execute.return_value = mock_result
        engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn_obj)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        result = conn.execute_sql("UPDATE t SET x=1")
        assert result is None


# ---------------------------------------------------------------------------
# SQL dialect helpers
# ---------------------------------------------------------------------------


class TestSqlHelpers:
    def test_quote_identifier(self, conn):
        assert conn.quote_identifier("column") == '"column"'

    def test_qualify_table_with_schema(self, conn):
        assert conn.qualify_table("users", "sales") == '"sales"."users"'

    def test_qualify_table_default_schema(self, conn):
        assert conn.qualify_table("users") == '"public"."users"'

    def test_build_select_basic(self, conn):
        q = conn.build_select_query("users", "public")
        assert q == 'SELECT * FROM "public"."users"'

    def test_build_select_with_where(self, conn):
        q = conn.build_select_query("users", "public", where="id > 5")
        assert "WHERE id > 5" in q

    def test_build_select_with_limit(self, conn):
        q = conn.build_select_query("users", "public", limit=10)
        assert "LIMIT 10" in q

    def test_build_select_with_columns(self, conn):
        q = conn.build_select_query("users", "public", columns="id, name")
        assert "SELECT id, name" in q

    def test_build_select_with_where_and_limit(self, conn):
        q = conn.build_select_query("users", "public", where="active=1", limit=5)
        assert "WHERE active=1" in q
        assert "LIMIT 5" in q


# ---------------------------------------------------------------------------
# Discovery: list_schemas, list_tables
# ---------------------------------------------------------------------------


class TestListSchemas:
    def test_success(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame({"schema_name": ["public", "staging"]})
        with patch("odibi.connections.postgres.pd.read_sql", return_value=df):
            result = conn.list_schemas()
        assert result == ["public", "staging"]

    def test_error_returns_empty(self, conn_engine):
        conn, engine = conn_engine
        with patch("odibi.connections.postgres.pd.read_sql", side_effect=Exception("fail")):
            result = conn.list_schemas()
        assert result == []


class TestListTables:
    def test_success(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame(
            {
                "table_name": ["users", "v_users"],
                "table_type": ["BASE TABLE", "VIEW"],
                "table_schema": ["public", "public"],
            }
        )
        with patch("odibi.connections.postgres.pd.read_sql", return_value=df):
            result = conn.list_tables("public")
        assert len(result) == 2
        assert result[0]["type"] == "table"
        assert result[1]["type"] == "view"

    def test_error_returns_empty(self, conn_engine):
        conn, engine = conn_engine
        with patch("odibi.connections.postgres.pd.read_sql", side_effect=Exception("fail")):
            result = conn.list_tables()
        assert result == []


# ---------------------------------------------------------------------------
# get_table_info
# ---------------------------------------------------------------------------


class TestGetTableInfo:
    def test_schema_dot_table(self, conn_engine):
        conn, engine = conn_engine
        cols_df = pd.DataFrame(
            {
                "column_name": ["id", "name"],
                "data_type": ["integer", "varchar"],
                "is_nullable": ["NO", "YES"],
                "ordinal_position": [1, 2],
            }
        )
        count_df = pd.DataFrame({"row_count": [100]})
        with patch(
            "odibi.connections.postgres.pd.read_sql",
            side_effect=[cols_df, count_df],
        ):
            result = conn.get_table_info("sales.orders")
        assert result["dataset"]["namespace"] == "sales"
        assert len(result["columns"]) == 2

    def test_table_only(self, conn_engine):
        conn, engine = conn_engine
        cols_df = pd.DataFrame(
            {
                "column_name": ["id"],
                "data_type": ["int"],
                "is_nullable": ["NO"],
                "ordinal_position": [1],
            }
        )
        count_df = pd.DataFrame({"row_count": [None]})
        with patch(
            "odibi.connections.postgres.pd.read_sql",
            side_effect=[cols_df, count_df],
        ):
            result = conn.get_table_info("orders")
        assert result["dataset"]["namespace"] == "public"

    def test_count_error(self, conn_engine):
        conn, engine = conn_engine
        cols_df = pd.DataFrame(
            {
                "column_name": ["id"],
                "data_type": ["int"],
                "is_nullable": ["NO"],
                "ordinal_position": [1],
            }
        )
        with patch(
            "odibi.connections.postgres.pd.read_sql",
            side_effect=[cols_df, Exception("no stats")],
        ):
            result = conn.get_table_info("orders")
        assert result["dataset"]["row_count"] is None

    def test_error_returns_empty(self, conn_engine):
        conn, engine = conn_engine
        with patch("odibi.connections.postgres.pd.read_sql", side_effect=Exception("fail")):
            result = conn.get_table_info("orders")
        assert result == {}


# ---------------------------------------------------------------------------
# discover_catalog
# ---------------------------------------------------------------------------


class TestDiscoverCatalog:
    def test_basic(self, conn_engine):
        conn, engine = conn_engine
        schemas_df = pd.DataFrame({"schema_name": ["public"]})
        tables_df = pd.DataFrame(
            {
                "table_name": ["users"],
                "table_type": ["BASE TABLE"],
                "table_schema": ["public"],
            }
        )
        with patch(
            "odibi.connections.postgres.pd.read_sql",
            side_effect=[schemas_df, tables_df],
        ):
            result = conn.discover_catalog()
        assert result["total_datasets"] == 1

    def test_path_filter_not_found(self, conn_engine):
        conn, engine = conn_engine
        schemas_df = pd.DataFrame({"schema_name": ["public"]})
        with patch("odibi.connections.postgres.pd.read_sql", return_value=schemas_df):
            result = conn.discover_catalog(path="nonexistent")
        assert result["total_datasets"] == 0

    def test_pattern_filter(self, conn_engine):
        conn, engine = conn_engine
        schemas_df = pd.DataFrame({"schema_name": ["public"]})
        tables_df = pd.DataFrame(
            {
                "table_name": ["fact_orders", "dim_product"],
                "table_type": ["BASE TABLE", "BASE TABLE"],
                "table_schema": ["public", "public"],
            }
        )
        with patch(
            "odibi.connections.postgres.pd.read_sql",
            side_effect=[schemas_df, tables_df],
        ):
            result = conn.discover_catalog(pattern="fact_*")
        assert result["total_datasets"] == 1

    def test_limit(self, conn_engine):
        conn, engine = conn_engine
        schemas_df = pd.DataFrame({"schema_name": ["public"]})
        tables_df = pd.DataFrame(
            {
                "table_name": ["t1", "t2", "t3"],
                "table_type": ["BASE TABLE"] * 3,
                "table_schema": ["public"] * 3,
            }
        )
        with patch(
            "odibi.connections.postgres.pd.read_sql",
            side_effect=[schemas_df, tables_df],
        ):
            result = conn.discover_catalog(limit=2)
        assert result["total_datasets"] == 2

    def test_include_schema(self, conn_engine):
        conn, engine = conn_engine
        pd.DataFrame({"schema_name": ["public"]})
        pd.DataFrame(
            {
                "table_name": ["users"],
                "table_type": ["BASE TABLE"],
                "table_schema": ["public"],
            }
        )
        info_result = {
            "dataset": {
                "name": "users",
                "namespace": "public",
                "kind": "table",
                "path": "public.users",
                "row_count": 10,
            }
        }
        with patch.object(conn, "list_schemas", return_value=["public"]):
            with patch.object(
                conn,
                "list_tables",
                return_value=[{"name": "users", "type": "table", "schema": "public"}],
            ):
                with patch.object(conn, "get_table_info", return_value=info_result):
                    result = conn.discover_catalog(include_schema=True)
        assert result["total_datasets"] == 1

    def test_include_schema_error_graceful(self, conn_engine):
        conn, engine = conn_engine
        with patch.object(conn, "list_schemas", return_value=["public"]):
            with patch.object(
                conn,
                "list_tables",
                return_value=[{"name": "users", "type": "table", "schema": "public"}],
            ):
                with patch.object(conn, "get_table_info", side_effect=Exception("fail")):
                    result = conn.discover_catalog(include_schema=True)
        assert result["total_datasets"] == 1


# ---------------------------------------------------------------------------
# profile
# ---------------------------------------------------------------------------


class TestProfile:
    def _make_profile_df(self):
        return pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["a", "b", "c", "d", "e"],
                "category": ["x", "x", "x", "x", "x"],
                "score": [1.0, 2.0, 3.0, 4.0, 5.0],
                "ts": pd.to_datetime(["2024-01-01"] * 5),
            }
        )

    def test_basic_profile(self, conn_engine):
        conn, engine = conn_engine
        df = self._make_profile_df()
        count_df = pd.DataFrame({"row_count": [100]})
        with patch(
            "odibi.connections.postgres.pd.read_sql",
            side_effect=[df, count_df],
        ):
            result = conn.profile("public.users")
        assert result["rows_sampled"] == 5
        assert len(result["columns"]) == 5
        assert "ts" in result["candidate_watermarks"]

    def test_profile_schema_table_parsing(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame({"id": [1, 2, 3]})
        count_df = pd.DataFrame({"row_count": [None]})
        with patch(
            "odibi.connections.postgres.pd.read_sql",
            side_effect=[df, count_df],
        ):
            result = conn.profile("users")  # no schema prefix
        assert result["rows_sampled"] == 3

    def test_profile_count_error(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame({"id": [1, 2]})
        with patch(
            "odibi.connections.postgres.pd.read_sql",
            side_effect=[df, Exception("no stats")],
        ):
            result = conn.profile("users")
        assert result["total_rows"] == 2  # fallback

    def test_profile_with_columns_filter(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame({"id": [1, 2]})
        count_df = pd.DataFrame({"row_count": [10]})
        with patch(
            "odibi.connections.postgres.pd.read_sql",
            side_effect=[df, count_df],
        ):
            result = conn.profile("users", columns=["id"])
        assert result["rows_sampled"] == 2

    def test_profile_error_returns_empty(self, conn_engine):
        conn, engine = conn_engine
        with patch("odibi.connections.postgres.pd.read_sql", side_effect=Exception("fail")):
            result = conn.profile("users")
        assert result == {}

    def test_cardinality_levels(self, conn_engine):
        conn, engine = conn_engine
        # 10 rows: unique=10 distinct, high>9, medium=4-9, low<10
        df = pd.DataFrame(
            {
                "unique_col": list(range(10)),
                "high_col": list(range(10)),  # all unique = "unique" actually
                "medium_col": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                "low_col": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            }
        )
        count_df = pd.DataFrame({"row_count": [10]})
        with patch(
            "odibi.connections.postgres.pd.read_sql",
            side_effect=[df, count_df],
        ):
            result = conn.profile("users")
        cols_by_name = {c["name"]: c for c in result["columns"]}
        assert cols_by_name["unique_col"]["cardinality"] == "unique"
        assert cols_by_name["low_col"]["cardinality"] == "low"


# ---------------------------------------------------------------------------
# preview
# ---------------------------------------------------------------------------


class TestPreview:
    def test_basic(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        count_df = pd.DataFrame({"row_count": [100]})
        with patch(
            "odibi.connections.postgres.pd.read_sql",
            side_effect=[df, count_df],
        ):
            result = conn.preview("public.users", rows=5)
        assert len(result["rows"]) == 2
        assert result["total_rows"] == 100

    def test_preview_no_schema(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame({"id": [1]})
        count_df = pd.DataFrame({"row_count": [None]})
        with patch(
            "odibi.connections.postgres.pd.read_sql",
            side_effect=[df, count_df],
        ):
            result = conn.preview("users")
        assert result["dataset"]["namespace"] == "public"

    def test_preview_with_columns(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame({"id": [1]})
        count_df = pd.DataFrame({"row_count": [5]})
        with patch(
            "odibi.connections.postgres.pd.read_sql",
            side_effect=[df, count_df],
        ):
            result = conn.preview("users", columns=["id"])
        assert len(result["rows"]) == 1

    def test_preview_capped_at_100(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame({"id": list(range(100))})
        count_df = pd.DataFrame({"row_count": [1000]})
        with patch(
            "odibi.connections.postgres.pd.read_sql",
            side_effect=[df, count_df],
        ):
            result = conn.preview("users", rows=200)
        assert result["truncated"] is True

    def test_preview_error(self, conn_engine):
        conn, engine = conn_engine
        with patch("odibi.connections.postgres.pd.read_sql", side_effect=Exception("fail")):
            result = conn.preview("users")
        assert result["rows"] == []

    def test_count_error_graceful(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame({"id": [1]})
        with patch(
            "odibi.connections.postgres.pd.read_sql",
            side_effect=[df, Exception("no stats")],
        ):
            result = conn.preview("users")
        assert result["total_rows"] is None


# ---------------------------------------------------------------------------
# relationships
# ---------------------------------------------------------------------------


class TestRelationships:
    def test_with_results(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame(
            {
                "fk_name": ["fk_order_customer", "fk_order_customer"],
                "parent_schema": ["public", "public"],
                "parent_table": ["customers", "customers"],
                "parent_column": ["id", "id"],
                "child_schema": ["public", "public"],
                "child_table": ["orders", "orders"],
                "child_column": ["customer_id", "customer_id"],
            }
        )
        with patch("odibi.connections.postgres.pd.read_sql", return_value=df):
            result = conn.relationships()
        assert len(result) == 1

    def test_with_schema_filter(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame(
            columns=[
                "fk_name",
                "parent_schema",
                "parent_table",
                "parent_column",
                "child_schema",
                "child_table",
                "child_column",
            ]
        )
        with patch("odibi.connections.postgres.pd.read_sql", return_value=df):
            result = conn.relationships(schema="sales")
        assert result == []

    def test_empty(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame(
            columns=[
                "fk_name",
                "parent_schema",
                "parent_table",
                "parent_column",
                "child_schema",
                "child_table",
                "child_column",
            ]
        )
        with patch("odibi.connections.postgres.pd.read_sql", return_value=df):
            result = conn.relationships()
        assert result == []

    def test_error(self, conn_engine):
        conn, engine = conn_engine
        with patch("odibi.connections.postgres.pd.read_sql", side_effect=Exception("fail")):
            result = conn.relationships()
        assert result == []


# ---------------------------------------------------------------------------
# get_freshness
# ---------------------------------------------------------------------------


class TestGetFreshness:
    def test_with_timestamp_column(self, conn_engine):
        conn, engine = conn_engine
        ts = datetime(2024, 3, 15, 10, 0, 0)
        df = pd.DataFrame({"max_ts": [ts]})
        with patch("odibi.connections.postgres.pd.read_sql", return_value=df):
            result = conn.get_freshness("public.orders", timestamp_column="created_at")
        assert result["source"] == "data"
        assert "age_hours" in result

    def test_with_timestamp_column_no_schema(self, conn_engine):
        conn, engine = conn_engine
        ts = datetime(2024, 3, 15, 10, 0, 0)
        df = pd.DataFrame({"max_ts": [ts]})
        with patch("odibi.connections.postgres.pd.read_sql", return_value=df):
            result = conn.get_freshness("orders", timestamp_column="created_at")
        assert result["dataset"]["namespace"] == "public"

    def test_timestamp_column_null(self, conn_engine):
        conn, engine = conn_engine
        data_df = pd.DataFrame({"max_ts": [None]})
        meta_df = pd.DataFrame({"modify_date": [datetime(2024, 1, 1)]})
        with patch(
            "odibi.connections.postgres.pd.read_sql",
            side_effect=[data_df, meta_df],
        ):
            result = conn.get_freshness("orders", timestamp_column="ts")
        assert result["source"] == "metadata"

    def test_timestamp_column_error_falls_through(self, conn_engine):
        conn, engine = conn_engine
        meta_df = pd.DataFrame({"modify_date": [datetime(2024, 1, 1)]})
        with patch(
            "odibi.connections.postgres.pd.read_sql",
            side_effect=[Exception("col not found"), meta_df],
        ):
            result = conn.get_freshness("orders", timestamp_column="bad_col")
        assert result["source"] == "metadata"

    def test_metadata_fallback(self, conn_engine):
        conn, engine = conn_engine
        meta_df = pd.DataFrame({"modify_date": [datetime(2024, 3, 15)]})
        with patch("odibi.connections.postgres.pd.read_sql", return_value=meta_df):
            result = conn.get_freshness("orders")
        assert result["source"] == "metadata"

    def test_metadata_1970_skip(self, conn_engine):
        conn, engine = conn_engine
        meta_df = pd.DataFrame({"modify_date": [datetime(1970, 1, 1)]})
        with patch("odibi.connections.postgres.pd.read_sql", return_value=meta_df):
            result = conn.get_freshness("orders")
        assert result == {}

    def test_metadata_null(self, conn_engine):
        conn, engine = conn_engine
        meta_df = pd.DataFrame({"modify_date": [None]})
        with patch("odibi.connections.postgres.pd.read_sql", return_value=meta_df):
            result = conn.get_freshness("orders")
        assert result == {}

    def test_metadata_error(self, conn_engine):
        conn, engine = conn_engine
        with patch("odibi.connections.postgres.pd.read_sql", side_effect=Exception("fail")):
            result = conn.get_freshness("orders")
        assert result == {}


# ---------------------------------------------------------------------------
# get_spark_options
# ---------------------------------------------------------------------------


class TestGetSparkOptions:
    def test_basic(self, conn):
        opts = conn.get_spark_options()
        assert "jdbc:postgresql://localhost:5432/testdb" in opts["url"]
        assert opts["driver"] == "org.postgresql.Driver"
        assert opts["user"] == "user"
        assert opts["password"] == "pass"

    def test_sslmode_require(self):
        c = PostgreSQLConnection(
            host="pg.example.com",
            database="db",
            username="u",
            password="p",
            sslmode="require",
        )
        opts = c.get_spark_options()
        assert "sslmode=require" in opts["url"]

    def test_sslmode_disable(self):
        c = PostgreSQLConnection(
            host="pg.example.com",
            database="db",
            sslmode="disable",
        )
        opts = c.get_spark_options()
        assert "user" not in opts
        assert "password" not in opts

    def test_sslmode_prefer(self, conn):
        opts = conn.get_spark_options()
        assert "sslmode" not in opts["url"]


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------


class TestClose:
    def test_with_engine(self, conn_engine):
        conn, engine = conn_engine
        conn.close()
        engine.dispose.assert_called_once()
        assert conn._engine is None

    def test_without_engine(self, conn):
        conn.close()  # no error


# ---------------------------------------------------------------------------
# _sanitize_error / _get_error_suggestions
# ---------------------------------------------------------------------------


class TestSanitizeError:
    def test_removes_password(self, conn):
        result = conn._sanitize_error("password=secret123 host=localhost")
        assert "secret123" not in result
        assert "***" in result

    def test_removes_user(self, conn):
        result = conn._sanitize_error("user=admin host=localhost")
        assert "admin" not in result

    def test_removes_url_creds(self, conn):
        result = conn._sanitize_error("postgresql://admin:secret@host/db")
        assert "admin" not in result
        assert "secret" not in result


class TestGetErrorSuggestions:
    def test_connection_refused(self, conn):
        suggestions = conn._get_error_suggestions("could not connect to server")
        assert any("running" in s.lower() for s in suggestions)

    def test_auth_failed(self, conn):
        suggestions = conn._get_error_suggestions("authentication failed for user")
        assert any("password" in s.lower() for s in suggestions)

    def test_db_not_exists(self, conn):
        suggestions = conn._get_error_suggestions("database 'foo' does not exist")
        assert any("exists" in s.lower() for s in suggestions)

    def test_psycopg2_error(self, conn):
        suggestions = conn._get_error_suggestions("No module named psycopg2")
        assert any("psycopg2" in s.lower() for s in suggestions)

    def test_unknown_error(self, conn):
        suggestions = conn._get_error_suggestions("some random error")
        assert suggestions == []
