"""Tests for Azure ADLS authentication (Phase 2A - Mocked)."""

import os
from unittest.mock import MagicMock, patch

import pytest

# Skip entire module if azure-identity is not installed
pytest.importorskip("azure.identity")

from odibi.connections.azure_adls import AzureADLS  # noqa: E402


class TestAzureADLSValidation:
    """Test validation for both auth modes."""

    def test_validation_key_vault_mode_missing_vault_name(self):
        """Test validation fails when key_vault_name is missing."""
        with pytest.raises(ValueError, match="key_vault mode requires"):
            AzureADLS(
                account="myaccount",
                container="mycontainer",
                auth_mode="key_vault",
                secret_name="my-secret",
                # missing key_vault_name
            )

    def test_validation_key_vault_mode_missing_secret_name(self):
        """Test validation fails when secret_name is missing."""
        with pytest.raises(ValueError, match="key_vault mode requires"):
            AzureADLS(
                account="myaccount",
                container="mycontainer",
                auth_mode="key_vault",
                key_vault_name="my-vault",
                # missing secret_name
            )

    def test_validation_direct_key_mode_missing_account_key(self):
        """Test validation fails when account_key is missing."""
        with pytest.raises(ValueError, match="direct_key mode requires"):
            AzureADLS(
                account="myaccount",
                container="mycontainer",
                auth_mode="direct_key",
                # missing account_key
            )

    def test_validation_service_principal_missing_fields(self):
        """Test validation fails when service principal fields are missing."""
        with pytest.raises(ValueError, match="service_principal mode requires"):
            AzureADLS(
                account="myaccount",
                container="mycontainer",
                auth_mode="service_principal",
                tenant_id="tid",
                client_id="cid",
                # missing client_secret
            )

    def test_validation_success_service_principal(self):
        """Test validation succeeds with valid service principal config."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="service_principal",
            tenant_id="tid",
            client_id="cid",
            client_secret="csecret",
        )
        assert conn.auth_mode == "service_principal"
        assert conn.tenant_id == "tid"

    def test_validation_success_managed_identity(self):
        """Test validation succeeds for managed identity."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="managed_identity",
        )
        assert conn.auth_mode == "managed_identity"

    def test_validation_unsupported_auth_mode(self):
        """Test validation fails with unsupported auth mode."""
        with pytest.raises(ValueError, match="Unsupported auth_mode"):
            AzureADLS(
                account="myaccount",
                container="mycontainer",
                auth_mode="unsupported_mode",
            )

    def test_validation_missing_account(self):
        """Test validation fails when account is missing."""
        with pytest.raises(ValueError, match="requires 'account'"):
            AzureADLS(
                account="",
                container="mycontainer",
                auth_mode="direct_key",
                account_key="test-key",
            )

    def test_validation_missing_container(self):
        """Test validation fails when container is missing."""
        with pytest.raises(ValueError, match="requires 'container'"):
            AzureADLS(
                account="myaccount",
                container="",
                auth_mode="direct_key",
                account_key="test-key",
            )

    def test_validation_success_key_vault_mode(self):
        """Test validation succeeds with valid key_vault config."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="key_vault",
            key_vault_name="my-vault",
            secret_name="my-secret",
        )
        assert conn.account == "myaccount"
        assert conn.container == "mycontainer"

    def test_validation_success_direct_key_mode(self):
        """Test validation succeeds with valid direct_key config."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="direct_key",
            account_key="test-key-123",
        )
        assert conn.account == "myaccount"
        assert conn.container == "mycontainer"


class TestAzureADLSDirectKeyAuth:
    """Test direct_key authentication mode."""

    def test_get_storage_key_direct_key(self):
        """Test get_storage_key returns account_key in direct_key mode."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="direct_key",
            account_key="test-key-123",
        )

        key = conn.get_storage_key()
        assert key == "test-key-123"

    def test_production_warning_direct_key(self):
        """Test warning is shown when using direct_key in production."""
        with patch.dict(os.environ, {"ODIBI_ENV": "production"}):
            with pytest.warns(UserWarning, match="not recommended"):
                AzureADLS(
                    account="myaccount",
                    container="mycontainer",
                    auth_mode="direct_key",
                    account_key="test-key",
                )

    def test_no_warning_direct_key_non_production(self):
        """Test no warning when using direct_key outside production."""
        with patch.dict(os.environ, {}, clear=True):
            # Should not raise warning
            conn = AzureADLS(
                account="myaccount",
                container="mycontainer",
                auth_mode="direct_key",
                account_key="test-key",
            )
            assert conn.auth_mode == "direct_key"


class TestAzureADLSKeyVaultAuth:
    """Test Key Vault authentication mode (mocked)."""

    @pytest.fixture
    def mock_key_vault(self):
        """Mock Azure Key Vault SecretClient."""
        with (
            patch("azure.identity.DefaultAzureCredential") as mock_cred,
            patch("azure.keyvault.secrets.SecretClient") as mock_client,
        ):
            # Mock secret value
            mock_secret = MagicMock()
            mock_secret.value = "mocked-storage-key-from-keyvault"

            # Mock client.get_secret()
            mock_client_instance = MagicMock()
            mock_client_instance.get_secret.return_value = mock_secret
            mock_client.return_value = mock_client_instance

            yield {
                "credential": mock_cred,
                "client": mock_client,
                "client_instance": mock_client_instance,
                "secret": mock_secret,
            }

    def test_get_storage_key_key_vault(self, mock_key_vault):
        """Test get_storage_key fetches from Key Vault."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="key_vault",
            key_vault_name="my-vault",
            secret_name="my-secret",
        )

        key = conn.get_storage_key()

        # Verify Key Vault was called
        mock_key_vault["client"].assert_called_once()
        mock_key_vault["client_instance"].get_secret.assert_called_once_with("my-secret")

        # Verify key matches mocked value
        assert key == "mocked-storage-key-from-keyvault"

    def test_caching_key_vault(self, mock_key_vault):
        """Test Key Vault key is cached after first fetch."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="key_vault",
            key_vault_name="my-vault",
            secret_name="my-secret",
        )

        # First call - fetches from Key Vault
        key1 = conn.get_storage_key()
        assert key1 == "mocked-storage-key-from-keyvault"

        # Second call - uses cache (no additional API call)
        key2 = conn.get_storage_key()
        assert key2 == "mocked-storage-key-from-keyvault"

        # Verify get_secret was called only once
        assert mock_key_vault["client_instance"].get_secret.call_count == 1


class TestAzureADLSPandasIntegration:
    """Test pandas storage options."""

    def test_pandas_storage_options_direct_key(self):
        """Test pandas_storage_options returns correct dict for direct_key."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="direct_key",
            account_key="test-key-123",
        )

        opts = conn.pandas_storage_options()

        assert opts == {"account_name": "myaccount", "account_key": "test-key-123"}

    def test_pandas_storage_options_service_principal(self):
        """Test pandas_storage_options returns correct dict for service_principal."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="service_principal",
            tenant_id="tid",
            client_id="cid",
            client_secret="csecret",
        )

        opts = conn.pandas_storage_options()
        assert opts == {
            "account_name": "myaccount",
            "tenant_id": "tid",
            "client_id": "cid",
            "client_secret": "csecret",
        }

    def test_pandas_storage_options_managed_identity(self):
        """Test pandas_storage_options returns correct dict for managed_identity."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="managed_identity",
        )

        opts = conn.pandas_storage_options()
        assert opts == {"account_name": "myaccount", "anon": False}

    @patch("azure.keyvault.secrets.SecretClient")
    @patch("azure.identity.DefaultAzureCredential")
    def test_pandas_storage_options_key_vault(self, mock_cred, mock_client):
        """Test pandas_storage_options fetches from Key Vault."""
        # Mock secret
        mock_secret = MagicMock()
        mock_secret.value = "vault-key"
        mock_client_instance = MagicMock()
        mock_client_instance.get_secret.return_value = mock_secret
        mock_client.return_value = mock_client_instance

        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="key_vault",
            key_vault_name="my-vault",
            secret_name="my-secret",
        )

        opts = conn.pandas_storage_options()

        assert opts == {"account_name": "myaccount", "account_key": "vault-key"}


class TestAzureADLSSparkIntegration:
    """Test Spark session configuration."""

    def test_configure_spark_direct_key(self):
        """Test configure_spark sets correct Spark config."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="direct_key",
            account_key="test-key-123",
        )

        # Mock Spark session
        mock_spark = MagicMock()

        conn.configure_spark(mock_spark)

        # Verify Spark config was set
        expected_key = "fs.azure.account.key.myaccount.dfs.core.windows.net"
        mock_spark.conf.set.assert_called_once_with(expected_key, "test-key-123")

    def test_configure_spark_service_principal(self):
        """Test configure_spark sets OAuth config for service_principal."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="service_principal",
            tenant_id="tid",
            client_id="cid",
            client_secret="csecret",
        )

        mock_spark = MagicMock()
        conn.configure_spark(mock_spark)

        # Check calls
        prefix = "fs.azure.account.auth.type.myaccount.dfs.core.windows.net"
        mock_spark.conf.set.assert_any_call(prefix, "OAuth")

        prefix = "fs.azure.account.oauth.provider.type.myaccount.dfs.core.windows.net"
        mock_spark.conf.set.assert_any_call(
            prefix, "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
        )

    def test_configure_spark_managed_identity(self):
        """Test configure_spark sets OAuth config for managed_identity."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="managed_identity",
        )

        mock_spark = MagicMock()
        conn.configure_spark(mock_spark)

        # Check calls
        prefix = "fs.azure.account.auth.type.myaccount.dfs.core.windows.net"
        mock_spark.conf.set.assert_any_call(prefix, "OAuth")

        prefix = "fs.azure.account.oauth.provider.type.myaccount.dfs.core.windows.net"
        mock_spark.conf.set.assert_any_call(
            prefix, "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider"
        )

    @patch("azure.keyvault.secrets.SecretClient")
    @patch("azure.identity.DefaultAzureCredential")
    def test_configure_spark_key_vault(self, mock_cred, mock_client):
        """Test configure_spark fetches from Key Vault and sets Spark config."""
        # Mock secret
        mock_secret = MagicMock()
        mock_secret.value = "vault-key"
        mock_client_instance = MagicMock()
        mock_client_instance.get_secret.return_value = mock_secret
        mock_client.return_value = mock_client_instance

        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="key_vault",
            key_vault_name="my-vault",
            secret_name="my-secret",
        )

        # Mock Spark session
        mock_spark = MagicMock()

        conn.configure_spark(mock_spark)

        # Verify Spark config was set with Key Vault key
        expected_key = "fs.azure.account.key.myaccount.dfs.core.windows.net"
        mock_spark.conf.set.assert_called_once_with(expected_key, "vault-key")


class TestAzureADLSURIGeneration:
    """Test URI generation."""

    def test_uri_simple_path(self):
        """Test URI generation with simple path."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="direct_key",
            account_key="test-key",
        )

        uri = conn.uri("folder/file.parquet")

        expected = "abfss://mycontainer@myaccount.dfs.core.windows.net/folder/file.parquet"
        assert uri == expected

    def test_uri_with_path_prefix(self):
        """Test URI generation with path_prefix."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            path_prefix="bronze/raw",
            auth_mode="direct_key",
            account_key="test-key",
        )

        uri = conn.uri("sales/data.parquet")

        expected = (
            "abfss://mycontainer@myaccount.dfs.core.windows.net/bronze/raw/sales/data.parquet"
        )
        assert uri == expected

    def test_uri_handles_leading_slash(self):
        """Test URI generation handles leading slashes."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="direct_key",
            account_key="test-key",
        )

        uri = conn.uri("/folder/file.csv")

        expected = "abfss://mycontainer@myaccount.dfs.core.windows.net/folder/file.csv"
        assert uri == expected

    def test_get_path_delegates_to_uri(self):
        """Test get_path delegates to uri method."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="direct_key",
            account_key="test-key",
        )

        path = conn.get_path("test/file.parquet")

        assert path == conn.uri("test/file.parquet")
