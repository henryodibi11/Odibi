"""Unit tests for connection factory."""

from unittest.mock import MagicMock, patch

import pytest

from odibi.connections.factory import (
    create_azure_blob_connection,
    create_delta_connection,
    create_http_connection,
    create_local_connection,
    create_sql_server_connection,
    register_builtins,
)


class TestCreateLocalConnection:
    """Tests for create_local_connection factory."""

    def test_creates_local_connection(self):
        """Should create LocalConnection with config."""
        conn = create_local_connection("test_local", {"base_path": "/data/test"})

        from odibi.connections.local import LocalConnection

        assert isinstance(conn, LocalConnection)
        assert conn.base_path_str == "/data/test"

    def test_defaults_to_data_path(self):
        """Should default base_path to ./data."""
        conn = create_local_connection("test_local", {})

        assert conn.base_path_str == "./data"


class TestCreateHttpConnection:
    """Tests for create_http_connection factory."""

    def test_creates_http_connection(self):
        """Should create HttpConnection with config."""
        conn = create_http_connection(
            "test_http",
            {"base_url": "https://api.example.com"},
        )

        from odibi.connections.http import HttpConnection

        assert isinstance(conn, HttpConnection)
        assert "api.example.com" in conn.base_url

    def test_passes_headers(self):
        """Should pass headers to connection."""
        conn = create_http_connection(
            "test_http",
            {
                "base_url": "https://api.example.com",
                "headers": {"Accept": "application/json"},
            },
        )

        assert conn.headers["Accept"] == "application/json"

    def test_passes_auth(self):
        """Should pass auth to connection."""
        conn = create_http_connection(
            "test_http",
            {
                "base_url": "https://api.example.com",
                "auth": {"token": "my-token"},
            },
        )

        assert "Authorization" in conn.headers


class TestCreateAzureBlobConnection:
    """Tests for create_azure_blob_connection factory."""

    @patch("odibi.connections.factory.AzureADLS", create=True)
    def test_creates_azure_blob_connection(self, mock_adls_class):
        """Should create AzureADLS connection."""
        mock_conn = MagicMock()
        mock_adls_class.return_value = mock_conn

        with patch.dict(
            "sys.modules", {"odibi.connections.azure_adls": MagicMock(AzureADLS=mock_adls_class)}
        ):
            from odibi.connections import factory

            result = factory.create_azure_blob_connection(
                "test_blob",
                {
                    "account_name": "myaccount",
                    "container": "mycontainer",
                },
            )

        assert result is mock_conn

    def test_raises_on_missing_account(self):
        """Should raise if account_name missing."""
        with pytest.raises(ValueError, match="account_name"):
            create_azure_blob_connection(
                "test_blob",
                {"container": "mycontainer"},
            )

    def test_accepts_account_alias(self):
        """Should accept 'account' as alias for 'account_name'."""
        with patch("odibi.connections.azure_adls.AzureADLS") as mock_adls:
            mock_adls.return_value = MagicMock()

            create_azure_blob_connection(
                "test_blob",
                {
                    "account": "myaccount",
                    "container": "mycontainer",
                },
            )

            mock_adls.assert_called_once()
            call_kwargs = mock_adls.call_args[1]
            assert call_kwargs["account"] == "myaccount"


class TestCreateDeltaConnection:
    """Tests for create_delta_connection factory."""

    def test_creates_local_connection_for_path(self):
        """Should create LocalConnection for path-based config."""
        conn = create_delta_connection(
            "test_delta",
            {"path": "/data/delta"},
        )

        from odibi.connections.local import LocalConnection

        assert isinstance(conn, LocalConnection)

    def test_creates_catalog_connection(self):
        """Should create DeltaCatalogConnection for catalog config."""
        conn = create_delta_connection(
            "test_delta",
            {"catalog": "main", "schema": "silver"},
        )

        assert conn.catalog == "main"
        assert conn.schema == "silver"

    def test_catalog_connection_get_path(self):
        """Should return fully qualified table name."""
        conn = create_delta_connection(
            "test_delta",
            {"catalog": "main", "schema": "silver"},
        )

        path = conn.get_path("dim_customer")
        assert path == "main.silver.dim_customer"

    def test_catalog_default_schema(self):
        """Should default schema to 'default'."""
        conn = create_delta_connection(
            "test_delta",
            {"catalog": "main"},
        )

        assert conn.schema == "default"


class TestCreateSqlServerConnection:
    """Tests for create_sql_server_connection factory."""

    @patch("odibi.connections.azure_sql.AzureSQL")
    def test_creates_sql_connection(self, mock_sql_class):
        """Should create AzureSQL connection."""
        mock_conn = MagicMock()
        mock_sql_class.return_value = mock_conn

        result = create_sql_server_connection(
            "test_sql",
            {
                "host": "myserver.database.windows.net",
                "database": "mydb",
            },
        )

        assert result is mock_conn

    def test_raises_on_missing_host(self):
        """Should raise if host/server missing."""
        with pytest.raises(ValueError, match="host.*server"):
            create_sql_server_connection(
                "test_sql",
                {"database": "mydb"},
            )

    @patch("odibi.connections.azure_sql.AzureSQL")
    def test_accepts_server_alias(self, mock_sql_class):
        """Should accept 'server' as alias for 'host'."""
        mock_sql_class.return_value = MagicMock()

        create_sql_server_connection(
            "test_sql",
            {
                "server": "myserver.database.windows.net",
                "database": "mydb",
            },
        )

        mock_sql_class.assert_called_once()
        call_kwargs = mock_sql_class.call_args[1]
        assert call_kwargs["server"] == "myserver.database.windows.net"

    @patch("odibi.connections.azure_sql.AzureSQL")
    def test_auto_detects_sql_auth(self, mock_sql_class):
        """Should auto-detect sql auth mode."""
        mock_sql_class.return_value = MagicMock()

        create_sql_server_connection(
            "test_sql",
            {
                "host": "myserver",
                "database": "mydb",
                "username": "admin",
                "password": "secret",
            },
        )

        call_kwargs = mock_sql_class.call_args[1]
        assert call_kwargs["auth_mode"] == "sql"


class TestRegisterBuiltins:
    """Tests for register_builtins function."""

    @patch("odibi.connections.factory.register_connection_factory")
    def test_registers_all_builtins(self, mock_register):
        """Should register all built-in connection types."""
        register_builtins()

        registered_types = [call[0][0] for call in mock_register.call_args_list]

        assert "local" in registered_types
        assert "http" in registered_types
        assert "azure_blob" in registered_types
        assert "azure_adls" in registered_types
        assert "delta" in registered_types
        assert "sql_server" in registered_types
        assert "azure_sql" in registered_types

    @patch("odibi.connections.factory.register_connection_factory")
    def test_registers_correct_factories(self, mock_register):
        """Should register correct factory functions."""
        register_builtins()

        factory_map = {call[0][0]: call[0][1] for call in mock_register.call_args_list}

        assert factory_map["local"] is create_local_connection
        assert factory_map["http"] is create_http_connection
        assert factory_map["azure_blob"] is create_azure_blob_connection
        assert factory_map["delta"] is create_delta_connection
        assert factory_map["sql_server"] is create_sql_server_connection
