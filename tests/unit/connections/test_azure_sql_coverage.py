"""Tests for odibi.connections.azure_sql.AzureSQL — full coverage."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.connections.azure_sql import AzureSQL
from odibi.exceptions import ConnectionError

# ---------------------------------------------------------------------------
# Autouse fixtures
# ---------------------------------------------------------------------------

_mock_ctx = MagicMock()


@pytest.fixture(autouse=True)
def _patch_logging():
    with patch("odibi.connections.azure_sql.get_logging_context", return_value=_mock_ctx):
        yield


@pytest.fixture(autouse=True)
def _patch_logger():
    with patch("odibi.connections.azure_sql.logger"):
        yield


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def conn_sql():
    """SQL auth connection."""
    return AzureSQL(
        server="myserver.database.windows.net",
        database="mydb",
        username="admin",
        password="secret",
        auth_mode="sql",
    )


@pytest.fixture
def conn_msi():
    """AAD MSI connection."""
    return AzureSQL(
        server="myserver.database.windows.net",
        database="mydb",
        auth_mode="aad_msi",
    )


@pytest.fixture
def conn_kv():
    """Key Vault connection."""
    return AzureSQL(
        server="myserver.database.windows.net",
        database="mydb",
        username="admin",
        auth_mode="key_vault",
        key_vault_name="myvault",
        secret_name="dbpassword",
    )


@pytest.fixture
def conn_engine(conn_sql):
    """SQL auth connection with pre-set mock engine."""
    mock_engine = MagicMock()
    conn_sql._engine = mock_engine
    return conn_sql, mock_engine


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------


class TestInit:
    def test_defaults(self, conn_msi):
        assert conn_msi.server == "myserver.database.windows.net"
        assert conn_msi.database == "mydb"
        assert conn_msi.driver == "ODBC Driver 18 for SQL Server"
        assert conn_msi.auth_mode == "aad_msi"
        assert conn_msi.port == 1433
        assert conn_msi.timeout == 30
        assert conn_msi.trust_server_certificate is True
        assert conn_msi._engine is None
        assert conn_msi._cached_key is None

    def test_sql_auth(self, conn_sql):
        assert conn_sql.username == "admin"
        assert conn_sql.password == "secret"
        assert conn_sql.auth_mode == "sql"


# ---------------------------------------------------------------------------
# get_password
# ---------------------------------------------------------------------------


class TestGetPassword:
    def test_direct_password(self, conn_sql):
        assert conn_sql.get_password() == "secret"

    def test_cached_key(self, conn_msi):
        conn_msi._cached_key = "cached_pw"
        assert conn_msi.get_password() == "cached_pw"

    def test_key_vault(self, conn_kv):
        mock_secret = MagicMock()
        mock_secret.value = "kv_password"
        mock_client = MagicMock()
        mock_client.get_secret.return_value = mock_secret
        with (
            patch(
                "odibi.connections.azure_sql.DefaultAzureCredential",
                create=True,
            ),
            patch(
                "odibi.connections.azure_sql.SecretClient",
                create=True,
                return_value=mock_client,
            ),
        ):
            # Need to mock the import inside get_password
            import sys

            mock_identity = MagicMock()
            mock_identity.DefaultAzureCredential = MagicMock()
            mock_kv = MagicMock()
            mock_kv.SecretClient = MagicMock(return_value=mock_client)
            with patch.dict(
                sys.modules,
                {
                    "azure": MagicMock(),
                    "azure.identity": mock_identity,
                    "azure.keyvault": MagicMock(),
                    "azure.keyvault.secrets": mock_kv,
                },
            ):
                result = conn_kv.get_password()
        assert result == "kv_password"

    def test_key_vault_missing_config(self):
        c = AzureSQL(
            server="s",
            database="d",
            auth_mode="key_vault",
            key_vault_name=None,
            secret_name=None,
        )
        with pytest.raises(ValueError, match="key_vault_name"):
            c.get_password()

    def test_key_vault_import_error(self, conn_kv):
        import sys

        # Clear any cached key
        conn_kv._cached_key = None
        conn_kv.password = None
        with patch.dict(sys.modules, {"azure.identity": None, "azure.keyvault.secrets": None}):
            # Force ImportError by patching the import
            original_import = (
                __builtins__.__import__ if hasattr(__builtins__, "__import__") else __import__
            )

            def mock_import(name, *args, **kwargs):
                if "azure.identity" in name or "azure.keyvault" in name:
                    raise ImportError("no azure")
                return original_import(name, *args, **kwargs)

            with patch("builtins.__import__", side_effect=mock_import):
                with pytest.raises(ImportError, match="azure-identity"):
                    conn_kv.get_password()

    def test_no_password_aad_msi(self, conn_msi):
        assert conn_msi.get_password() is None


# ---------------------------------------------------------------------------
# odbc_dsn
# ---------------------------------------------------------------------------


class TestOdbcDsn:
    def test_sql_auth(self, conn_sql):
        dsn = conn_sql.odbc_dsn()
        assert "UID=admin" in dsn
        assert "PWD=secret" in dsn
        assert "TrustServerCertificate=yes" in dsn

    def test_aad_msi(self, conn_msi):
        dsn = conn_msi.odbc_dsn()
        assert "Authentication=ActiveDirectoryMsi" in dsn
        assert "UID=" not in dsn

    def test_aad_service_principal(self):
        c = AzureSQL(server="s", database="d", auth_mode="aad_service_principal")
        dsn = c.odbc_dsn()
        assert "ActiveDirectoryMsi" not in dsn

    def test_trust_server_cert_false(self):
        c = AzureSQL(
            server="s",
            database="d",
            auth_mode="sql",
            username="u",
            password="p",
            trust_server_certificate=False,
        )
        dsn = c.odbc_dsn()
        assert "TrustServerCertificate=no" in dsn


# ---------------------------------------------------------------------------
# get_path / validate
# ---------------------------------------------------------------------------


class TestGetPath:
    def test_returns_unchanged(self, conn_sql):
        assert conn_sql.get_path("dbo.users") == "dbo.users"


class TestValidate:
    def test_success_sql(self, conn_sql):
        conn_sql.validate()

    def test_success_msi(self, conn_msi):
        conn_msi.validate()

    def test_missing_server(self):
        c = AzureSQL(server="", database="d")
        with pytest.raises(ValueError, match="requires 'server'"):
            c.validate()

    def test_missing_database(self):
        c = AzureSQL(server="s", database="")
        with pytest.raises(ValueError, match="requires 'database'"):
            c.validate()

    def test_sql_no_username(self):
        c = AzureSQL(server="s", database="d", auth_mode="sql")
        with pytest.raises(ValueError, match="requires 'username'"):
            c.validate()

    def test_sql_no_password(self):
        c = AzureSQL(server="s", database="d", auth_mode="sql", username="u")
        with pytest.raises(ValueError, match="requires password"):
            c.validate()

    def test_key_vault_no_vault_name(self):
        c = AzureSQL(
            server="s",
            database="d",
            auth_mode="key_vault",
            secret_name="secret",
        )
        with pytest.raises(ValueError, match="key_vault_name"):
            c.validate()

    def test_key_vault_no_username(self):
        c = AzureSQL(
            server="s",
            database="d",
            auth_mode="key_vault",
            key_vault_name="vault",
            secret_name="secret",
        )
        with pytest.raises(ValueError, match="requires username"):
            c.validate()

    def test_key_vault_success(self, conn_kv):
        conn_kv.validate()


# ---------------------------------------------------------------------------
# get_engine
# ---------------------------------------------------------------------------


class TestGetEngine:
    def test_cached(self, conn_engine):
        conn, engine = conn_engine
        assert conn.get_engine() is engine

    def test_creates_engine(self, conn_sql):
        mock_engine = MagicMock()
        with patch("sqlalchemy.create_engine", return_value=mock_engine):
            result = conn_sql.get_engine()
        assert result is mock_engine

    def test_connection_error(self, conn_sql):
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = Exception("connection refused")
        with patch("sqlalchemy.create_engine", return_value=mock_engine):
            with pytest.raises(ConnectionError, match="Failed to create engine"):
                conn_sql.get_engine()


# ---------------------------------------------------------------------------
# read_sql / read_table / read_sql_query
# ---------------------------------------------------------------------------


class TestReadSql:
    def test_success(self, conn_engine):
        conn, engine = conn_engine
        expected = pd.DataFrame({"id": [1, 2]})
        with patch("odibi.connections.azure_sql.pd.read_sql", return_value=expected):
            result = conn.read_sql("SELECT * FROM t")
        assert len(result) == 2

    def test_connection_error_reraised(self, conn_engine):
        conn, engine = conn_engine
        with patch(
            "odibi.connections.azure_sql.pd.read_sql",
            side_effect=ConnectionError(connection_name="az", reason="fail", suggestions=[]),
        ):
            with pytest.raises(ConnectionError):
                conn.read_sql("SELECT 1")

    def test_generic_error_wrapped(self, conn_engine):
        conn, engine = conn_engine
        with patch(
            "odibi.connections.azure_sql.pd.read_sql",
            side_effect=RuntimeError("boom"),
        ):
            with pytest.raises(ConnectionError, match="Query execution failed"):
                conn.read_sql("SELECT 1")


class TestReadTable:
    def test_with_schema(self, conn_engine):
        conn, engine = conn_engine
        expected = pd.DataFrame({"id": [1]})
        with patch("odibi.connections.azure_sql.pd.read_sql", return_value=expected):
            result = conn.read_table("users", schema="dbo")
        assert len(result) == 1

    def test_no_schema(self, conn_engine):
        conn, engine = conn_engine
        expected = pd.DataFrame({"id": [1]})
        with patch("odibi.connections.azure_sql.pd.read_sql", return_value=expected):
            result = conn.read_table("users", schema=None)
        assert len(result) == 1


class TestReadSqlQuery:
    def test_delegates(self, conn_engine):
        conn, engine = conn_engine
        expected = pd.DataFrame({"x": [1]})
        with patch("odibi.connections.azure_sql.pd.read_sql", return_value=expected):
            result = conn.read_sql_query("SELECT 1 AS x")
        assert len(result) == 1


# ---------------------------------------------------------------------------
# write_table
# ---------------------------------------------------------------------------


class TestWriteTable:
    def test_success(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame({"id": [1, 2]})
        with patch.object(df, "to_sql", return_value=2):
            result = conn.write_table(df, "users")
        assert result == 2

    def test_to_sql_none(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame({"id": [1, 2]})
        with patch.object(df, "to_sql", return_value=None):
            result = conn.write_table(df, "users")
        assert result == 2

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
            side_effect=ConnectionError(connection_name="az", reason="fail", suggestions=[]),
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
        result = conn.execute("UPDATE t SET x=1")
        assert result is None

    def test_error_wrapped(self, conn_engine):
        conn, engine = conn_engine
        engine.begin.side_effect = RuntimeError("fail")
        with pytest.raises(ConnectionError, match="Statement execution failed"):
            conn.execute("DROP TABLE t")

    def test_connection_error_reraised(self, conn_engine):
        conn, engine = conn_engine
        engine.begin.side_effect = ConnectionError(
            connection_name="az", reason="fail", suggestions=[]
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
        result = conn.execute_sql("INSERT INTO t VALUES (1)")
        assert result is None


# ---------------------------------------------------------------------------
# SQL dialect helpers
# ---------------------------------------------------------------------------


class TestSqlHelpers:
    def test_quote_identifier(self, conn_sql):
        assert conn_sql.quote_identifier("col") == "[col]"

    def test_qualify_table(self, conn_sql):
        assert conn_sql.qualify_table("users", "dbo") == "[dbo].[users]"

    def test_qualify_table_default(self, conn_sql):
        assert conn_sql.qualify_table("users") == "[dbo].[users]"

    def test_build_select_basic(self, conn_sql):
        q = conn_sql.build_select_query("users", "dbo")
        assert "SELECT * FROM [dbo].[users]" in q

    def test_build_select_with_limit(self, conn_sql):
        q = conn_sql.build_select_query("users", "dbo", limit=10)
        assert "TOP (10)" in q

    def test_build_select_with_where(self, conn_sql):
        q = conn_sql.build_select_query("users", "dbo", where="id > 5")
        assert "WHERE id > 5" in q

    def test_build_select_no_limit(self, conn_sql):
        q = conn_sql.build_select_query("users", "dbo", limit=-1)
        assert "TOP" not in q


# ---------------------------------------------------------------------------
# Discovery: list_schemas, list_tables
# ---------------------------------------------------------------------------


class TestListSchemas:
    def test_success(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame({"SCHEMA_NAME": ["dbo", "staging"]})
        with patch("odibi.connections.azure_sql.pd.read_sql", return_value=df):
            result = conn.list_schemas()
        assert result == ["dbo", "staging"]

    def test_error(self, conn_engine):
        conn, engine = conn_engine
        with patch("odibi.connections.azure_sql.pd.read_sql", side_effect=Exception("fail")):
            result = conn.list_schemas()
        assert result == []


class TestListTables:
    def test_success(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame(
            {
                "TABLE_NAME": ["users", "v_users"],
                "TABLE_TYPE": ["BASE TABLE", "VIEW"],
                "TABLE_SCHEMA": ["dbo", "dbo"],
            }
        )
        with patch("odibi.connections.azure_sql.pd.read_sql", return_value=df):
            result = conn.list_tables("dbo")
        assert len(result) == 2
        assert result[0]["type"] == "table"
        assert result[1]["type"] == "view"

    def test_error(self, conn_engine):
        conn, engine = conn_engine
        with patch("odibi.connections.azure_sql.pd.read_sql", side_effect=Exception("fail")):
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
                "COLUMN_NAME": ["id", "name"],
                "DATA_TYPE": ["int", "nvarchar"],
                "IS_NULLABLE": ["NO", "YES"],
                "ORDINAL_POSITION": [1, 2],
            }
        )
        count_df = pd.DataFrame({"row_count": [50]})
        with patch("odibi.connections.azure_sql.pd.read_sql", side_effect=[cols_df, count_df]):
            result = conn.get_table_info("dbo.users")
        assert result["dataset"]["namespace"] == "dbo"
        assert len(result["columns"]) == 2

    def test_table_only(self, conn_engine):
        conn, engine = conn_engine
        cols_df = pd.DataFrame(
            {
                "COLUMN_NAME": ["id"],
                "DATA_TYPE": ["int"],
                "IS_NULLABLE": ["NO"],
                "ORDINAL_POSITION": [1],
            }
        )
        count_df = pd.DataFrame({"row_count": [None]})
        with patch("odibi.connections.azure_sql.pd.read_sql", side_effect=[cols_df, count_df]):
            result = conn.get_table_info("users")
        assert result["dataset"]["namespace"] == "dbo"

    def test_error(self, conn_engine):
        conn, engine = conn_engine
        with patch("odibi.connections.azure_sql.pd.read_sql", side_effect=Exception("fail")):
            result = conn.get_table_info("users")
        assert result == {}


# ---------------------------------------------------------------------------
# discover_catalog
# ---------------------------------------------------------------------------


class TestDiscoverCatalog:
    def test_basic(self, conn_engine):
        conn, engine = conn_engine
        with patch.object(conn, "list_schemas", return_value=["dbo"]):
            with patch.object(
                conn,
                "list_tables",
                return_value=[
                    {"name": "users", "type": "table", "schema": "dbo"},
                ],
            ):
                result = conn.discover_catalog()
        assert result["total_datasets"] == 1

    def test_path_not_found(self, conn_engine):
        conn, engine = conn_engine
        with patch.object(conn, "list_schemas", return_value=["dbo"]):
            result = conn.discover_catalog(path="nonexistent")
        assert result["total_datasets"] == 0

    def test_pattern_filter(self, conn_engine):
        conn, engine = conn_engine
        with patch.object(conn, "list_schemas", return_value=["dbo"]):
            with patch.object(
                conn,
                "list_tables",
                return_value=[
                    {"name": "fact_orders", "type": "table", "schema": "dbo"},
                    {"name": "dim_product", "type": "table", "schema": "dbo"},
                ],
            ):
                result = conn.discover_catalog(pattern="fact_*")
        assert result["total_datasets"] == 1

    def test_limit(self, conn_engine):
        conn, engine = conn_engine
        with patch.object(conn, "list_schemas", return_value=["dbo"]):
            with patch.object(
                conn,
                "list_tables",
                return_value=[
                    {"name": "t1", "type": "table", "schema": "dbo"},
                    {"name": "t2", "type": "table", "schema": "dbo"},
                    {"name": "t3", "type": "table", "schema": "dbo"},
                ],
            ):
                result = conn.discover_catalog(limit=2)
        assert result["total_datasets"] == 2

    def test_include_schema(self, conn_engine):
        conn, engine = conn_engine
        info_result = {
            "dataset": {
                "name": "users",
                "namespace": "dbo",
                "kind": "table",
                "path": "dbo.users",
                "row_count": 10,
            }
        }
        with patch.object(conn, "list_schemas", return_value=["dbo"]):
            with patch.object(
                conn,
                "list_tables",
                return_value=[{"name": "users", "type": "table", "schema": "dbo"}],
            ):
                with patch.object(conn, "get_table_info", return_value=info_result):
                    result = conn.discover_catalog(include_schema=True)
        assert result["total_datasets"] == 1

    def test_include_schema_error(self, conn_engine):
        conn, engine = conn_engine
        with patch.object(conn, "list_schemas", return_value=["dbo"]):
            with patch.object(
                conn,
                "list_tables",
                return_value=[{"name": "users", "type": "table", "schema": "dbo"}],
            ):
                with patch.object(conn, "get_table_info", side_effect=Exception("fail")):
                    result = conn.discover_catalog(include_schema=True)
        assert result["total_datasets"] == 1


# ---------------------------------------------------------------------------
# profile
# ---------------------------------------------------------------------------


class TestProfile:
    def test_basic(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["a", "b", "c", "d", "e"],
                "cat": ["x"] * 5,
                "ts": pd.to_datetime(["2024-01-01"] * 5),
            }
        )
        count_df = pd.DataFrame({"row_count": [100]})
        with patch("odibi.connections.azure_sql.pd.read_sql", side_effect=[df, count_df]):
            result = conn.profile("dbo.users")
        assert result["rows_sampled"] == 5
        assert "ts" in result["candidate_watermarks"]

    def test_no_schema(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame({"id": [1, 2]})
        count_df = pd.DataFrame({"row_count": [None]})
        with patch("odibi.connections.azure_sql.pd.read_sql", side_effect=[df, count_df]):
            result = conn.profile("users")
        assert result["rows_sampled"] == 2

    def test_count_error(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame({"id": [1]})
        with patch("odibi.connections.azure_sql.pd.read_sql", side_effect=[df, Exception("fail")]):
            result = conn.profile("users")
        assert result["total_rows"] == 1

    def test_with_columns(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame({"id": [1]})
        count_df = pd.DataFrame({"row_count": [5]})
        with patch("odibi.connections.azure_sql.pd.read_sql", side_effect=[df, count_df]):
            result = conn.profile("users", columns=["id"])
        assert result["rows_sampled"] == 1

    def test_error(self, conn_engine):
        conn, engine = conn_engine
        with patch("odibi.connections.azure_sql.pd.read_sql", side_effect=Exception("fail")):
            result = conn.profile("users")
        assert result == {}


# ---------------------------------------------------------------------------
# preview
# ---------------------------------------------------------------------------


class TestPreview:
    def test_basic(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame({"id": [1, 2]})
        count_df = pd.DataFrame({"row_count": [100]})
        with patch("odibi.connections.azure_sql.pd.read_sql", side_effect=[df, count_df]):
            result = conn.preview("dbo.users", rows=5)
        assert len(result["rows"]) == 2
        assert result["total_rows"] == 100

    def test_no_schema(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame({"id": [1]})
        count_df = pd.DataFrame({"row_count": [None]})
        with patch("odibi.connections.azure_sql.pd.read_sql", side_effect=[df, count_df]):
            result = conn.preview("users")
        assert result["dataset"]["namespace"] == "dbo"

    def test_with_columns(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame({"id": [1]})
        count_df = pd.DataFrame({"row_count": [5]})
        with patch("odibi.connections.azure_sql.pd.read_sql", side_effect=[df, count_df]):
            result = conn.preview("users", columns=["id"])
        assert len(result["rows"]) == 1

    def test_capped_at_100(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame({"id": list(range(100))})
        count_df = pd.DataFrame({"row_count": [1000]})
        with patch("odibi.connections.azure_sql.pd.read_sql", side_effect=[df, count_df]):
            result = conn.preview("users", rows=200)
        assert result["truncated"] is True

    def test_error(self, conn_engine):
        conn, engine = conn_engine
        with patch("odibi.connections.azure_sql.pd.read_sql", side_effect=Exception("fail")):
            result = conn.preview("users")
        assert result["rows"] == []


# ---------------------------------------------------------------------------
# relationships
# ---------------------------------------------------------------------------


class TestRelationships:
    def test_with_results(self, conn_engine):
        conn, engine = conn_engine
        df = pd.DataFrame(
            {
                "fk_name": ["fk_order_customer", "fk_order_customer"],
                "parent_schema": ["dbo", "dbo"],
                "parent_table": ["customers", "customers"],
                "parent_column": ["id", "id"],
                "child_schema": ["dbo", "dbo"],
                "child_table": ["orders", "orders"],
                "child_column": ["customer_id", "customer_id"],
            }
        )
        with patch("odibi.connections.azure_sql.pd.read_sql", return_value=df):
            result = conn.relationships()
        assert len(result) == 1

    def test_with_schema_filter(self, conn_engine):
        conn, engine = conn_engine
        empty = pd.DataFrame(
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
        with patch("odibi.connections.azure_sql.pd.read_sql", return_value=empty):
            result = conn.relationships(schema="dbo")
        assert result == []

    def test_error(self, conn_engine):
        conn, engine = conn_engine
        with patch("odibi.connections.azure_sql.pd.read_sql", side_effect=Exception("fail")):
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
        with patch("odibi.connections.azure_sql.pd.read_sql", return_value=df):
            result = conn.get_freshness("dbo.orders", timestamp_column="created_at")
        assert result["source"] == "data"

    def test_no_schema(self, conn_engine):
        conn, engine = conn_engine
        ts = datetime(2024, 3, 15, 10, 0, 0)
        df = pd.DataFrame({"max_ts": [ts]})
        with patch("odibi.connections.azure_sql.pd.read_sql", return_value=df):
            result = conn.get_freshness("orders", timestamp_column="ts")
        assert result["dataset"]["namespace"] == "dbo"

    def test_timestamp_null_fallback(self, conn_engine):
        conn, engine = conn_engine
        data_df = pd.DataFrame({"max_ts": [None]})
        meta_df = pd.DataFrame({"modify_date": [datetime(2024, 1, 1)]})
        with patch("odibi.connections.azure_sql.pd.read_sql", side_effect=[data_df, meta_df]):
            result = conn.get_freshness("orders", timestamp_column="ts")
        assert result["source"] == "metadata"

    def test_metadata_only(self, conn_engine):
        conn, engine = conn_engine
        meta_df = pd.DataFrame({"modify_date": [datetime(2024, 3, 15)]})
        with patch("odibi.connections.azure_sql.pd.read_sql", return_value=meta_df):
            result = conn.get_freshness("orders")
        assert result["source"] == "metadata"

    def test_metadata_null(self, conn_engine):
        conn, engine = conn_engine
        meta_df = pd.DataFrame({"modify_date": [None]})
        with patch("odibi.connections.azure_sql.pd.read_sql", return_value=meta_df):
            result = conn.get_freshness("orders")
        assert result == {}

    def test_error(self, conn_engine):
        conn, engine = conn_engine
        with patch("odibi.connections.azure_sql.pd.read_sql", side_effect=Exception("fail")):
            result = conn.get_freshness("orders")
        assert result == {}


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------


class TestClose:
    def test_with_engine(self, conn_engine):
        conn, engine = conn_engine
        conn.close()
        engine.dispose.assert_called_once()
        assert conn._engine is None

    def test_without_engine(self, conn_sql):
        conn_sql.close()  # no error


# ---------------------------------------------------------------------------
# _sanitize_error / _get_error_suggestions
# ---------------------------------------------------------------------------


class TestSanitizeError:
    def test_pwd(self, conn_sql):
        result = conn_sql._sanitize_error("PWD=secret;host=x")
        assert "secret" not in result
        assert "PWD=***" in result

    def test_uid(self, conn_sql):
        result = conn_sql._sanitize_error("UID=admin;host=x")
        assert "admin" not in result

    def test_password_ci(self, conn_sql):
        result = conn_sql._sanitize_error("password=secret123;host=x")
        assert "secret123" not in result

    def test_user_ci(self, conn_sql):
        result = conn_sql._sanitize_error("user=admin;host=x")
        assert "admin" not in result


class TestGetErrorSuggestions:
    def test_delegates_to_centralized(self, conn_sql):
        with patch(
            "odibi.connections.azure_sql.get_suggestions_for_connection",
            return_value=["suggestion1"],
        ):
            result = conn_sql._get_error_suggestions("some error")
        assert result == ["suggestion1"]

    def test_error_returns_empty(self, conn_sql):
        with patch(
            "odibi.connections.azure_sql.get_suggestions_for_connection",
            side_effect=Exception("fail"),
        ):
            result = conn_sql._get_error_suggestions("some error")
        assert result == []


# ---------------------------------------------------------------------------
# get_spark_options
# ---------------------------------------------------------------------------


class TestGetSparkOptions:
    def test_aad_msi(self, conn_msi):
        opts = conn_msi.get_spark_options()
        assert "ActiveDirectoryMsi" in opts["url"]
        assert "user" not in opts
        assert "password" not in opts

    def test_sql_auth(self, conn_sql):
        opts = conn_sql.get_spark_options()
        assert opts["user"] == "admin"
        assert opts["password"] == "secret"

    def test_aad_service_principal(self):
        c = AzureSQL(server="s", database="d", auth_mode="aad_service_principal")
        opts = c.get_spark_options()
        assert "user" not in opts

    def test_key_vault_with_password(self, conn_kv):
        conn_kv.password = "resolved_pw"
        opts = conn_kv.get_spark_options()
        assert opts["password"] == "resolved_pw"

    def test_trust_server_cert_false(self):
        c = AzureSQL(
            server="s",
            database="d",
            auth_mode="sql",
            username="u",
            password="p",
            trust_server_certificate=False,
        )
        opts = c.get_spark_options()
        assert "trustServerCertificate=false" in opts["url"]
