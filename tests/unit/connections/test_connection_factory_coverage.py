"""Comprehensive unit tests for odibi.connections.factory."""

from unittest.mock import MagicMock, patch

import pytest

from odibi.connections.factory import (
    create_azure_blob_connection,
    create_delta_connection,
    create_http_connection,
    create_local_connection,
    create_postgres_connection,
    create_sql_server_connection,
    register_builtins,
)


@pytest.fixture(autouse=True)
def _patch_logging_context():
    with patch("odibi.connections.factory.get_logging_context") as mock_ctx:
        mock_ctx.return_value = MagicMock()
        yield


@pytest.fixture(autouse=True)
def _patch_logger():
    with patch("odibi.connections.factory.logger") as mock_logger:
        mock_logger.register_secret = MagicMock()
        yield


# ===================================================================
# 1. create_local_connection
# ===================================================================


class TestCreateLocalConnection:
    @patch("odibi.connections.factory.LocalConnection", create=True)
    def test_basic(self, MockLocal):
        with patch("odibi.connections.local.LocalConnection", MockLocal):
            result = create_local_connection("test", {"base_path": "/tmp/data"})
        assert result is not None

    def test_default_base_path(self):
        with patch("odibi.connections.local.LocalConnection") as MockLocal:
            create_local_connection("test", {})
            MockLocal.assert_called_once_with(base_path="./data")


# ===================================================================
# 2. create_http_connection
# ===================================================================


class TestCreateHttpConnection:
    def test_basic(self):
        with patch("odibi.connections.http.HttpConnection") as MockHttp:
            create_http_connection(
                "api",
                {
                    "base_url": "https://example.com",
                    "headers": {"Authorization": "Bearer x"},
                    "auth": ("user", "pass"),
                },
            )
            MockHttp.assert_called_once_with(
                base_url="https://example.com",
                headers={"Authorization": "Bearer x"},
                auth=("user", "pass"),
            )


# ===================================================================
# 3. create_azure_blob_connection
# ===================================================================


class TestCreateAzureBlobConnection:
    def test_sas_token_auto_detect(self):
        with patch("odibi.connections.azure_adls.AzureADLS") as MockADLS:
            config = {
                "account_name": "acct1",
                "container": "c1",
                "sas_token": "?sv=2020",
            }
            create_azure_blob_connection("blob1", config)
            call_kwargs = MockADLS.call_args[1]
            assert call_kwargs["auth_mode"] == "sas_token"

    def test_key_vault_auto_detect(self):
        with patch("odibi.connections.azure_adls.AzureADLS") as MockADLS:
            config = {
                "account_name": "acct1",
                "container": "c1",
                "key_vault_name": "kv1",
                "secret_name": "s1",
            }
            create_azure_blob_connection("blob2", config)
            call_kwargs = MockADLS.call_args[1]
            assert call_kwargs["auth_mode"] == "key_vault"

    def test_direct_key_auto_detect(self):
        with patch("odibi.connections.azure_adls.AzureADLS") as MockADLS:
            config = {
                "account_name": "acct1",
                "container": "c1",
                "account_key": "abc123",
            }
            create_azure_blob_connection("blob3", config)
            call_kwargs = MockADLS.call_args[1]
            assert call_kwargs["auth_mode"] == "direct_key"

    def test_service_principal_auto_detect(self):
        with patch("odibi.connections.azure_adls.AzureADLS") as MockADLS:
            config = {
                "account_name": "acct1",
                "container": "c1",
                "tenant_id": "t1",
                "client_id": "c1",
                "client_secret": "s1",
            }
            create_azure_blob_connection("blob4", config)
            call_kwargs = MockADLS.call_args[1]
            assert call_kwargs["auth_mode"] == "service_principal"

    def test_managed_identity_fallback(self):
        with patch("odibi.connections.azure_adls.AzureADLS") as MockADLS:
            config = {
                "account_name": "acct1",
                "container": "c1",
            }
            create_azure_blob_connection("blob5", config)
            call_kwargs = MockADLS.call_args[1]
            assert call_kwargs["auth_mode"] == "managed_identity"

    def test_missing_account_name_raises(self):
        with patch("odibi.connections.azure_adls.AzureADLS"):
            with pytest.raises(ValueError, match="missing 'account_name'"):
                create_azure_blob_connection("blob6", {"container": "c1"})

    def test_import_error(self):
        with patch(
            "odibi.connections.factory.create_azure_blob_connection.__module__",
            new="odibi.connections.factory",
        ):
            import builtins

            original_import = builtins.__import__

            def mock_import(name, *args, **kwargs):
                if name == "odibi.connections.azure_adls":
                    raise ImportError("no azure")
                return original_import(name, *args, **kwargs)

            with patch("builtins.__import__", side_effect=mock_import):
                with pytest.raises(ImportError, match="odibi\\[azure\\]"):
                    create_azure_blob_connection(
                        "blob7",
                        {
                            "account_name": "a",
                            "container": "c",
                        },
                    )

    def test_eager_validation_mode(self):
        with patch("odibi.connections.azure_adls.AzureADLS") as MockADLS:
            config = {
                "account_name": "acct1",
                "container": "c1",
                "auth_mode": "managed_identity",
                "validation_mode": "eager",
            }
            create_azure_blob_connection("blob8", config)
            call_kwargs = MockADLS.call_args[1]
            assert call_kwargs["validate"] is True

    def test_explicit_auth_mode_no_auto_detect(self):
        with patch("odibi.connections.azure_adls.AzureADLS") as MockADLS:
            config = {
                "account_name": "acct1",
                "container": "c1",
                "auth_mode": "key_vault",
            }
            create_azure_blob_connection("blob9", config)
            call_kwargs = MockADLS.call_args[1]
            assert call_kwargs["auth_mode"] == "key_vault"


# ===================================================================
# 4. create_delta_connection
# ===================================================================


class TestCreateDeltaConnection:
    def test_path_based_returns_local(self):
        with patch("odibi.connections.local.LocalConnection") as MockLocal:
            create_delta_connection("d1", {"path": "/data/lake"})
            MockLocal.assert_called_once_with(base_path="/data/lake")

    def test_catalog_based(self):
        result = create_delta_connection("d2", {"catalog": "main", "schema": "silver"})
        assert result.get_path("table1") == "main.silver.table1"

    def test_catalog_default_schema(self):
        result = create_delta_connection("d3", {"catalog": "main"})
        assert result.get_path("t") == "main.default.t"

    def test_catalog_validate_noop(self):
        result = create_delta_connection("d4", {"catalog": "main"})
        result.validate()  # should not raise

    def test_catalog_storage_options_empty(self):
        result = create_delta_connection("d5", {"catalog": "main"})
        assert result.pandas_storage_options() == {}


# ===================================================================
# 5. create_sql_server_connection
# ===================================================================


class TestCreateSqlServerConnection:
    def test_sql_auth_auto_detect(self):
        with patch("odibi.connections.azure_sql.AzureSQL") as MockSQL:
            config = {
                "host": "server.database.windows.net",
                "database": "mydb",
                "username": "admin",
                "password": "pass123",
            }
            create_sql_server_connection("sql1", config)
            call_kwargs = MockSQL.call_args[1]
            assert call_kwargs["auth_mode"] == "sql"

    def test_key_vault_auth_auto_detect(self):
        with patch("odibi.connections.azure_sql.AzureSQL") as MockSQL:
            config = {
                "host": "server.database.windows.net",
                "database": "mydb",
                "username": "admin",
                "key_vault_name": "kv1",
                "secret_name": "s1",
            }
            create_sql_server_connection("sql2", config)
            call_kwargs = MockSQL.call_args[1]
            assert call_kwargs["auth_mode"] == "key_vault"

    def test_aad_msi_fallback(self):
        with patch("odibi.connections.azure_sql.AzureSQL") as MockSQL:
            config = {
                "host": "server.database.windows.net",
                "database": "mydb",
            }
            create_sql_server_connection("sql3", config)
            call_kwargs = MockSQL.call_args[1]
            assert call_kwargs["auth_mode"] == "aad_msi"

    def test_sql_login_normalized(self):
        with patch("odibi.connections.azure_sql.AzureSQL") as MockSQL:
            config = {
                "host": "server.database.windows.net",
                "database": "mydb",
                "auth_mode": "sql_login",
                "username": "admin",
                "password": "pass",
            }
            create_sql_server_connection("sql4", config)
            call_kwargs = MockSQL.call_args[1]
            assert call_kwargs["auth_mode"] == "sql"

    def test_missing_host_raises(self):
        with patch("odibi.connections.azure_sql.AzureSQL"):
            with pytest.raises(ValueError, match="missing 'host'"):
                create_sql_server_connection("sql5", {"database": "db"})

    def test_missing_database_raises(self):
        with patch("odibi.connections.azure_sql.AzureSQL"):
            with pytest.raises(ValueError, match="missing 'database'"):
                create_sql_server_connection("sql6", {"host": "h"})

    def test_import_error(self):
        import builtins

        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "odibi.connections.azure_sql":
                raise ImportError("no azure")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            with pytest.raises(ImportError, match="odibi\\[azure\\]"):
                create_sql_server_connection(
                    "sql7",
                    {
                        "host": "h",
                        "database": "d",
                    },
                )


# ===================================================================
# 6. create_postgres_connection
# ===================================================================


class TestCreatePostgresConnection:
    def test_basic(self):
        with patch("odibi.connections.postgres.PostgreSQLConnection") as MockPG:
            config = {
                "host": "pg.example.com",
                "database": "mydb",
                "username": "admin",
                "password": "pass",
            }
            create_postgres_connection("pg1", config)
            call_kwargs = MockPG.call_args[1]
            assert call_kwargs["host"] == "pg.example.com"
            assert call_kwargs["database"] == "mydb"

    def test_missing_host_raises(self):
        with patch("odibi.connections.postgres.PostgreSQLConnection"):
            with pytest.raises(ValueError, match="missing 'host'"):
                create_postgres_connection("pg2", {"database": "db"})

    def test_missing_database_raises(self):
        with patch("odibi.connections.postgres.PostgreSQLConnection"):
            with pytest.raises(ValueError, match="missing 'database'"):
                create_postgres_connection("pg3", {"host": "h"})

    def test_import_error(self):
        import builtins

        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "odibi.connections.postgres":
                raise ImportError("no postgres")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            with pytest.raises(ImportError, match="odibi\\[postgres\\]"):
                create_postgres_connection(
                    "pg4",
                    {
                        "host": "h",
                        "database": "d",
                    },
                )

    def test_server_alias_for_host(self):
        with patch("odibi.connections.postgres.PostgreSQLConnection") as MockPG:
            config = {
                "server": "pg.example.com",
                "database": "mydb",
            }
            create_postgres_connection("pg5", config)
            call_kwargs = MockPG.call_args[1]
            assert call_kwargs["host"] == "pg.example.com"


# ===================================================================
# 7. register_builtins
# ===================================================================


class TestRegisterBuiltins:
    def test_no_error(self):
        with patch("odibi.connections.factory.register_connection_factory") as mock_reg:
            register_builtins()
            assert mock_reg.call_count >= 8
