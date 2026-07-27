"""Unit tests for connection factory."""

import copy
import hmac
import socket
import urllib.request
from unittest.mock import MagicMock, patch

import pytest
import requests
from pydantic import ValidationError

from odibi.config import HttpApiKeyAuth, HttpBasicAuth, HttpBearerAuth, HttpConnectionConfig
from odibi.connections.factory import (
    create_azure_blob_connection,
    create_delta_connection,
    create_http_connection,
    create_local_connection,
    create_sql_server_connection,
    register_builtins,
)


@pytest.fixture(autouse=True)
def restore_global_logger_secrets():
    """Keep process-global secret registration isolated between tests."""
    import odibi.connections.factory as factory_module
    import odibi.connections.http as http_module
    import odibi.utils.logging as logging_module

    secrets = logging_module.logger._secrets
    original_secrets = set(secrets)
    assert factory_module.logger is logging_module.logger
    assert http_module.logger is logging_module.logger

    try:
        yield
    finally:
        secrets.clear()
        secrets.update(original_secrets)


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

    def test_non_empty_manual_headers_are_not_mutated_by_auth(self):
        config = {
            "base_url": "https://offline.invalid",
            "headers": {"Accept": "application/json", "Authorization": "manual"},
            "auth": {"token": "replacement-sentinel"},
        }
        original = copy.deepcopy(config)

        conn = create_http_connection("offline", config)

        assert config == original
        assert conn.headers is not config["headers"]
        assert conn.headers["Accept"] == "application/json"
        assert hmac.compare_digest(conn.headers["Authorization"], "Bearer replacement-sentinel")

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

    @pytest.mark.parametrize(
        ("auth", "expected_name", "expected_value"),
        [
            ({"mode": "none"}, None, None),
            (
                {"mode": "basic", "username": "offline-user", "password": "basic-sentinel"},
                "Authorization",
                "Basic b2ZmbGluZS11c2VyOmJhc2ljLXNlbnRpbmVs",
            ),
            (
                {"mode": "bearer", "token": "bearer-sentinel"},
                "Authorization",
                "Bearer bearer-sentinel",
            ),
            (
                {"mode": "api_key", "api_key": "key-sentinel"},
                "Authorization",
                "Bearer key-sentinel",
            ),
            (
                {
                    "mode": "api_key",
                    "api_key": "key-sentinel",
                    "header_name": "X-API-Key",
                    "value_template": "prefix-{token}-suffix",
                },
                "X-API-Key",
                "prefix-key-sentinel-suffix",
            ),
        ],
    )
    def test_public_model_dump_to_factory(self, auth, expected_name, expected_value):
        config = HttpConnectionConfig(base_url="https://offline.invalid", auth=auth)
        dumped = config.model_dump()
        original = copy.deepcopy(dumped)

        conn = create_http_connection("offline", dumped)

        assert dumped == original
        if expected_name is None:
            assert conn.headers == {}
        else:
            assert hmac.compare_digest(conn.headers[expected_name], expected_value)

    def test_api_key_with_braces_is_replaced_once(self):
        config = HttpConnectionConfig(
            base_url="https://offline.invalid",
            auth={"mode": "api_key", "api_key": "key-{other}-sentinel"},
        )

        conn = create_http_connection("offline", config.model_dump())

        assert hmac.compare_digest(conn.headers["Authorization"], "Bearer key-{other}-sentinel")

    @pytest.mark.parametrize(
        "value_template",
        ["no placeholder", "{token}{token}", "{other}", "{token}-{other}", "{{token}}"],
    )
    def test_api_key_rejects_invalid_templates_without_echo(self, value_template):
        sentinel = "template-secret-sentinel"
        auth = {"mode": "api_key", "api_key": sentinel, "value_template": value_template}

        with pytest.raises(ValueError) as exc_info:
            create_http_connection("offline", {"base_url": "https://offline.invalid", "auth": auth})

        assert str(exc_info.value) == (
            "HTTP API-key value_template must contain exactly one literal "
            "'{token}' placeholder and no other braces"
        )
        assert sentinel not in str(exc_info.value)

    def test_mode_api_key_mapping_requires_key(self):
        with pytest.raises(ValueError) as exc_info:
            create_http_connection(
                "offline",
                {"base_url": "https://offline.invalid", "auth": {"mode": "api_key"}},
            )

        assert str(exc_info.value) == "HTTP API-key authentication requires a non-empty 'api_key'"

    def test_no_mode_api_key_through_factory_remains_raw_x_api_key(self):
        conn = create_http_connection(
            "offline",
            {"base_url": "https://offline.invalid", "auth": {"api_key": "raw-sentinel"}},
        )

        assert hmac.compare_digest(conn.headers["X-API-Key"], "raw-sentinel")
        assert "Authorization" not in conn.headers

    def test_factory_registers_raw_and_rendered_api_key_before_connection_creation(self):
        events = []

        def register(secret):
            events.append(("register", secret))

        def construct(**kwargs):
            events.append(("construct", kwargs["auth"]))
            return MagicMock()

        with (
            patch("odibi.connections.factory.logger.register_secret", side_effect=register),
            patch("odibi.connections.http.HttpConnection", side_effect=construct),
        ):
            create_http_connection(
                "offline",
                {
                    "base_url": "https://offline.invalid",
                    "auth": {"mode": "api_key", "api_key": "raw-sentinel"},
                },
            )

        assert events[:2] == [
            ("register", "raw-sentinel"),
            ("register", "Bearer raw-sentinel"),
        ]
        assert events[2][0] == "construct"

    def test_real_offline_auth_lifecycle_redacts_after_logging_reconfiguration(
        self, monkeypatch, capsys
    ):
        import logging

        import odibi.connections.factory as factory_module
        import odibi.connections.http as http_module
        import odibi.utils.logging as logging_module
        from odibi.utils.setup_helpers import fetch_keyvault_secret

        attempts = []

        def blocked(name):
            def tripwire(*args, **kwargs):
                attempts.append(name)
                raise AssertionError(f"offline tripwire called: {name}")

            return tripwire

        monkeypatch.setattr(socket, "getaddrinfo", blocked("socket.getaddrinfo"))
        monkeypatch.setattr(socket, "create_connection", blocked("socket.create_connection"))
        monkeypatch.setattr(socket.socket, "connect", blocked("socket.connect"))
        monkeypatch.setattr(requests.sessions.Session, "request", blocked("requests"))
        monkeypatch.setattr(urllib.request, "urlopen", blocked("urllib"))
        monkeypatch.setattr("odibi.utils.setup_helpers.fetch_keyvault_secret", blocked("keyvault"))
        assert fetch_keyvault_secret is not None

        raw = "offline-raw-lifecycle-sentinel"
        rendered = f"Bearer {raw}"
        config = HttpConnectionConfig(
            base_url="https://offline.invalid",
            headers={"Accept": "application/json"},
            auth={"mode": "api_key", "api_key": raw},
        )
        dumped = config.model_dump()
        original = copy.deepcopy(dumped)
        logger = logging_module.logger
        secrets = logger._secrets
        original_secrets = set(secrets)
        original_structured = logger.structured
        original_level = logger.level
        stdlib_loggers = {
            name: logging.getLogger(name)
            for name in [
                "odibi",
                "py4j",
                "azure",
                "azure.core.pipeline.policies.http_logging_policy",
                "adlfs",
                "urllib3",
                "fsspec",
            ]
        }
        original_stdlib_levels = {name: item.level for name, item in stdlib_loggers.items()}

        try:
            conn = create_http_connection("offline", dumped)
            logging_module.configure_logging(structured=True, level="INFO")
            logging_module.logger.info(raw)
            logging_module.logger.info(rendered)
            output = capsys.readouterr().out

            assert dumped == original
            assert hmac.compare_digest(conn.headers["Authorization"], rendered)
            assert logging_module.logger is logger
            assert factory_module.logger is logger
            assert http_module.logger is logger
            assert logger._secrets is secrets
            assert raw in secrets and rendered in secrets
            assert logger.structured is True
            assert logger.level == logging.INFO
            assert output.count("[REDACTED]") >= 2
            assert raw not in output
            assert rendered not in output
            assert attempts == []
        finally:
            secrets.clear()
            secrets.update(original_secrets)
            logger._configure(original_structured, logging.getLevelName(original_level))
            for name, stdlib_logger in stdlib_loggers.items():
                stdlib_logger.setLevel(original_stdlib_levels[name])

    @pytest.mark.parametrize("alias", ["key", "header"])
    def test_public_api_key_rejects_unsupported_aliases_without_input_echo(self, alias):
        sentinel = "rejected-credential-sentinel"

        with pytest.raises(ValidationError) as exc_info:
            HttpConnectionConfig(
                base_url="https://offline.invalid",
                auth={"mode": "api_key", "api_key": sentinel, alias: sentinel},
            )

        diagnostic = str(exc_info.value)
        assert alias in diagnostic
        assert "Unknown key" in diagnostic
        assert sentinel not in diagnostic

    def test_api_key_is_required_and_non_empty(self):
        for auth in ({"mode": "api_key"}, {"mode": "api_key", "api_key": ""}):
            with pytest.raises(ValidationError):
                HttpConnectionConfig(base_url="https://offline.invalid", auth=auth)

    def test_auth_repr_hides_secrets_but_dump_preserves_them(self):
        models = [
            HttpBasicAuth(username="user", password="basic-repr-sentinel"),
            HttpBearerAuth(token="bearer-repr-sentinel"),
            HttpApiKeyAuth(api_key="api-repr-sentinel"),
        ]

        for model in models:
            secret = next(
                value
                for key, value in model.model_dump().items()
                if key in {"password", "token", "api_key"}
            )
            assert secret not in repr(model)
            assert secret in model.model_dump().values()


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
