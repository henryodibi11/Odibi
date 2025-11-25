"""Tests for odibi.utils.setup_helpers (Phase 2C)."""

from unittest.mock import Mock, patch

import pytest

from odibi.connections import AzureADLS
from odibi.utils.setup_helpers import (
    KeyVaultFetchResult,
    configure_connections_parallel,
    fetch_keyvault_secret,
    fetch_keyvault_secrets_parallel,
    validate_databricks_environment,
)


class TestKeyVaultFetchResult:
    """Tests for KeyVaultFetchResult dataclass."""

    def test_success_result(self):
        result = KeyVaultFetchResult(
            connection_name="bronze",
            account="mystorageaccount",
            success=True,
            secret_value="secret123",
            duration_ms=150.0,
        )
        assert result.success is True
        assert result.secret_value == "secret123"
        assert result.error is None
        assert result.duration_ms == 150.0

    def test_error_result(self):
        error = Exception("Key Vault error")
        result = KeyVaultFetchResult(
            connection_name="silver",
            account="storage2",
            success=False,
            error=error,
            duration_ms=200.0,
        )
        assert result.success is False
        assert result.secret_value is None
        assert result.error == error


class TestFetchKeyvaultSecret:
    """Tests for fetch_keyvault_secret function."""

    @patch("azure.keyvault.secrets.SecretClient")
    @patch("azure.identity.DefaultAzureCredential")
    def test_successful_fetch(self, mock_credential, mock_client_class):
        mock_secret = Mock()
        mock_secret.value = "my-storage-key"

        mock_client = Mock()
        mock_client.get_secret.return_value = mock_secret
        mock_client_class.return_value = mock_client

        result = fetch_keyvault_secret(
            connection_name="bronze",
            key_vault_name="mykeyvault",
            secret_name="storage-key",
            timeout=30.0,
        )

        assert result.success is True
        assert result.secret_value == "my-storage-key"
        assert result.error is None
        assert result.duration_ms is not None
        assert result.duration_ms >= 0

    @patch("azure.keyvault.secrets.SecretClient")
    @patch("azure.identity.DefaultAzureCredential")
    def test_fetch_with_exception(self, mock_credential, mock_client_class):
        mock_client = Mock()
        mock_client.get_secret.side_effect = Exception("Key Vault error")
        mock_client_class.return_value = mock_client

        result = fetch_keyvault_secret(
            connection_name="bronze",
            key_vault_name="mykeyvault",
            secret_name="storage-key",
        )

        assert result.success is False
        assert result.secret_value is None
        assert result.error is not None
        assert "Key Vault error" in str(result.error)

    def test_import_error_handling(self):
        with patch.dict("sys.modules", {"azure.identity": None}):
            result = fetch_keyvault_secret(
                connection_name="bronze",
                key_vault_name="mykeyvault",
                secret_name="storage-key",
            )
            assert result.success is False


class TestFetchKeyvaultSecretsParallel:
    """Tests for fetch_keyvault_secrets_parallel function."""

    def test_no_keyvault_connections(self):
        connections = {
            "local": AzureADLS(
                account="storage1",
                container="data",
                auth_mode="direct_key",
                account_key="key123",
            ),
        }

        results = fetch_keyvault_secrets_parallel(connections, verbose=False)

        assert len(results) == 1
        assert results["local"].success is True

    @patch("odibi.utils.setup_helpers.fetch_keyvault_secret")
    def test_parallel_fetch_success(self, mock_fetch):
        connections = {
            "bronze": AzureADLS(
                account="storage1",
                container="bronze",
                auth_mode="key_vault",
                key_vault_name="kv1",
                secret_name="secret1",
                validate=False,
            ),
            "silver": AzureADLS(
                account="storage2",
                container="silver",
                auth_mode="key_vault",
                key_vault_name="kv2",
                secret_name="secret2",
                validate=False,
            ),
        }

        mock_fetch.side_effect = [
            KeyVaultFetchResult(
                connection_name="bronze",
                account="storage1",
                success=True,
                secret_value="key1",
                duration_ms=100.0,
            ),
            KeyVaultFetchResult(
                connection_name="silver",
                account="storage2",
                success=True,
                secret_value="key2",
                duration_ms=120.0,
            ),
        ]

        results = fetch_keyvault_secrets_parallel(connections, max_workers=2, verbose=False)

        assert len(results) == 2
        assert all(r.success for r in results.values())
        assert mock_fetch.call_count == 2

    @patch("odibi.utils.setup_helpers.fetch_keyvault_secret")
    def test_parallel_fetch_with_failure(self, mock_fetch):
        connections = {
            "bronze": AzureADLS(
                account="storage1",
                container="bronze",
                auth_mode="key_vault",
                key_vault_name="kv1",
                secret_name="secret1",
                validate=False,
            ),
            "silver": AzureADLS(
                account="storage2",
                container="silver",
                auth_mode="key_vault",
                key_vault_name="kv2",
                secret_name="secret2",
                validate=False,
            ),
        }

        mock_fetch.side_effect = [
            KeyVaultFetchResult(
                connection_name="bronze",
                account="storage1",
                success=True,
                secret_value="key1",
                duration_ms=100.0,
            ),
            KeyVaultFetchResult(
                connection_name="silver",
                account="storage2",
                success=False,
                error=Exception("Auth failed"),
                duration_ms=150.0,
            ),
        ]

        results = fetch_keyvault_secrets_parallel(connections, max_workers=2, verbose=False)

        assert len(results) == 2
        assert results["bronze"].success is True
        assert results["silver"].success is False

    def test_mixed_connection_types(self):
        connections = {
            "direct": AzureADLS(
                account="storage1",
                container="data",
                auth_mode="direct_key",
                account_key="key123",
            ),
            "keyvault": AzureADLS(
                account="storage2",
                container="data",
                auth_mode="key_vault",
                key_vault_name="kv1",
                secret_name="secret1",
                validate=False,
            ),
        }

        with patch("odibi.utils.setup_helpers.fetch_keyvault_secret") as mock_fetch:
            mock_fetch.return_value = KeyVaultFetchResult(
                connection_name="keyvault",
                account="storage2",
                success=True,
                secret_value="key2",
                duration_ms=100.0,
            )

            results = fetch_keyvault_secrets_parallel(connections, verbose=False)

            assert len(results) == 2
            assert results["direct"].success is True
            assert results["keyvault"].success is True
            assert mock_fetch.call_count == 1


class TestConfigureConnectionsParallel:
    """Tests for configure_connections_parallel function."""

    @patch("odibi.utils.setup_helpers.fetch_keyvault_secrets_parallel")
    def test_successful_configuration(self, mock_fetch):
        connections = {
            "bronze": AzureADLS(
                account="storage1",
                container="bronze",
                auth_mode="key_vault",
                key_vault_name="kv1",
                secret_name="secret1",
                validate=False,
            ),
        }

        mock_fetch.return_value = {
            "bronze": KeyVaultFetchResult(
                connection_name="bronze",
                account="storage1",
                success=True,
                secret_value="secret-key-123",
                duration_ms=100.0,
            ),
        }

        configured, errors = configure_connections_parallel(
            connections, prefetch_secrets=True, verbose=False
        )

        assert len(errors) == 0
        assert configured["bronze"]._cached_key == "secret-key-123"

    @patch("odibi.utils.setup_helpers.fetch_keyvault_secrets_parallel")
    def test_configuration_with_errors(self, mock_fetch):
        connections = {
            "bronze": AzureADLS(
                account="storage1",
                container="bronze",
                auth_mode="key_vault",
                key_vault_name="kv1",
                secret_name="secret1",
                validate=False,
            ),
        }

        mock_fetch.return_value = {
            "bronze": KeyVaultFetchResult(
                connection_name="bronze",
                account="storage1",
                success=False,
                error=Exception("Auth failed"),
                duration_ms=100.0,
            ),
        }

        configured, errors = configure_connections_parallel(
            connections, prefetch_secrets=True, verbose=False
        )

        assert len(errors) == 1
        assert "Failed to fetch secret" in errors[0]

    def test_skip_prefetch(self):
        connections = {
            "bronze": AzureADLS(
                account="storage1",
                container="bronze",
                auth_mode="direct_key",
                account_key="key123",
            ),
        }

        configured, errors = configure_connections_parallel(
            connections, prefetch_secrets=False, verbose=False
        )

        assert len(errors) == 0
        assert configured == connections


class TestValidateDatabricksEnvironment:
    """Tests for validate_databricks_environment function."""

    def test_non_databricks_environment(self):
        with patch.dict("os.environ", {}, clear=True):
            result = validate_databricks_environment(verbose=False)

            assert result["is_databricks"] is False
            assert result["runtime_version"] is None

    @patch.dict("os.environ", {"DATABRICKS_RUNTIME_VERSION": "12.2.x-scala2.12"})
    def test_databricks_environment(self):
        result = validate_databricks_environment(verbose=False)

        assert result["is_databricks"] is True
        assert result["runtime_version"] == "12.2.x-scala2.12"

    @patch.dict("os.environ", {"DATABRICKS_RUNTIME_VERSION": "12.2.x-scala2.12"})
    def test_spark_available(self):
        pytest.importorskip("pyspark")

        with patch("pyspark.sql.SparkSession") as mock_spark:
            mock_session = Mock()
            mock_spark.getActiveSession.return_value = mock_session

            result = validate_databricks_environment(verbose=False)

            assert result["is_databricks"] is True
            assert result["spark_available"] is True
