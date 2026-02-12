"""Tests for AzureADLS connection."""

import sys
import warnings
from unittest.mock import MagicMock, Mock, patch

import pytest

from odibi.connections.azure_adls import AzureADLS


class TestAzureADLSInitialization:
    """Tests for AzureADLS initialization."""

    def test_init_with_key_vault_mode(self):
        """Test initialization with key_vault auth mode."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="key_vault",
            key_vault_name="myvault",
            secret_name="mysecret",
        )
        assert conn.account == "myaccount"
        assert conn.container == "mycontainer"
        assert conn.auth_mode == "key_vault"
        assert conn.key_vault_name == "myvault"
        assert conn.secret_name == "mysecret"

    def test_init_with_direct_key_mode(self):
        """Test initialization with direct_key auth mode."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="direct_key",
            account_key="mykey123",
        )
        assert conn.account == "myaccount"
        assert conn.auth_mode == "direct_key"
        assert conn.account_key == "mykey123"

    def test_init_with_sas_token_mode(self):
        """Test initialization with sas_token auth mode."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="sas_token",
            sas_token="?sv=2021-06-08&ss=b&srt=sco",
        )
        assert conn.auth_mode == "sas_token"
        assert conn.sas_token == "?sv=2021-06-08&ss=b&srt=sco"

    def test_init_with_service_principal_mode(self):
        """Test initialization with service_principal auth mode."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="service_principal",
            tenant_id="tenant123",
            client_id="client123",
            client_secret="secret123",
        )
        assert conn.auth_mode == "service_principal"
        assert conn.tenant_id == "tenant123"
        assert conn.client_id == "client123"
        assert conn.client_secret == "secret123"

    def test_init_with_managed_identity_mode(self):
        """Test initialization with managed_identity auth mode."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="managed_identity",
        )
        assert conn.auth_mode == "managed_identity"

    def test_init_with_path_prefix(self):
        """Test initialization with path_prefix."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            path_prefix="/data/raw/",
            auth_mode="direct_key",
            account_key="key123",
        )
        assert conn.path_prefix == "data/raw"  # Stripped of slashes

    def test_init_without_validation(self):
        """Test initialization with validate=False skips validation."""
        # Should not raise even though account is missing
        conn = AzureADLS(
            account="",
            container="",
            auth_mode="direct_key",
            validate=False,
        )
        assert conn.account == ""


class TestAzureADLSValidation:
    """Tests for AzureADLS validation logic."""

    def test_validate_missing_account(self):
        """Test validation fails when account is missing."""
        with pytest.raises(ValueError, match="ADLS connection requires 'account'"):
            AzureADLS(
                account="",
                container="mycontainer",
                auth_mode="direct_key",
            )

    def test_validate_missing_container(self):
        """Test validation fails when container is missing."""
        with pytest.raises(ValueError, match="ADLS connection requires 'container'"):
            AzureADLS(
                account="myaccount",
                container="",
                auth_mode="direct_key",
            )

    def test_validate_key_vault_missing_vault_name(self):
        """Test validation fails for key_vault mode without key_vault_name."""
        with pytest.raises(ValueError, match="key_vault mode requires"):
            AzureADLS(
                account="myaccount",
                container="mycontainer",
                auth_mode="key_vault",
                secret_name="mysecret",
            )

    def test_validate_key_vault_missing_secret_name(self):
        """Test validation fails for key_vault mode without secret_name."""
        with pytest.raises(ValueError, match="key_vault mode requires"):
            AzureADLS(
                account="myaccount",
                container="mycontainer",
                auth_mode="key_vault",
                key_vault_name="myvault",
            )

    def test_validate_direct_key_missing_account_key(self):
        """Test validation fails for direct_key mode without account_key."""
        with pytest.raises(ValueError, match="direct_key mode requires 'account_key'"):
            AzureADLS(
                account="myaccount",
                container="mycontainer",
                auth_mode="direct_key",
            )

    def test_validate_direct_key_production_warning(self):
        """Test warning is issued when using direct_key in production."""
        with patch.dict("os.environ", {"ODIBI_ENV": "production"}):
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                AzureADLS(
                    account="myaccount",
                    container="mycontainer",
                    auth_mode="direct_key",
                    account_key="key123",
                )
                assert len(w) == 1
                assert "direct_key in production is not recommended" in str(w[0].message)

    def test_validate_sas_token_missing_token(self):
        """Test validation fails for sas_token mode without token or key vault."""
        with pytest.raises(ValueError, match="sas_token mode requires"):
            AzureADLS(
                account="myaccount",
                container="mycontainer",
                auth_mode="sas_token",
            )

    def test_validate_sas_token_with_key_vault_fallback(self):
        """Test sas_token mode accepts key_vault_name/secret_name as fallback."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="sas_token",
            key_vault_name="myvault",
            secret_name="mysecret",
        )
        assert conn.auth_mode == "sas_token"

    def test_validate_service_principal_missing_tenant_id(self):
        """Test validation fails for service_principal without tenant_id."""
        with pytest.raises(ValueError, match="service_principal mode requires"):
            AzureADLS(
                account="myaccount",
                container="mycontainer",
                auth_mode="service_principal",
                client_id="client123",
                client_secret="secret123",
            )

    def test_validate_service_principal_missing_client_id(self):
        """Test validation fails for service_principal without client_id."""
        with pytest.raises(ValueError, match="service_principal mode requires"):
            AzureADLS(
                account="myaccount",
                container="mycontainer",
                auth_mode="service_principal",
                tenant_id="tenant123",
                client_secret="secret123",
            )

    def test_validate_service_principal_missing_client_secret(self):
        """Test validation fails for service_principal without client_secret."""
        with pytest.raises(ValueError, match="service_principal mode requires 'client_secret'"):
            AzureADLS(
                account="myaccount",
                container="mycontainer",
                auth_mode="service_principal",
                tenant_id="tenant123",
                client_id="client123",
            )

    def test_validate_service_principal_with_key_vault_fallback(self):
        """Test service_principal mode accepts key_vault for client_secret."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="service_principal",
            tenant_id="tenant123",
            client_id="client123",
            key_vault_name="myvault",
            secret_name="mysecret",
        )
        assert conn.auth_mode == "service_principal"

    def test_validate_unsupported_auth_mode(self):
        """Test validation fails for unsupported auth_mode."""
        with pytest.raises(ValueError, match="Unsupported auth_mode"):
            AzureADLS(
                account="myaccount",
                container="mycontainer",
                auth_mode="invalid_mode",
            )

    def test_validate_managed_identity_success(self):
        """Test validation succeeds for managed_identity mode."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="managed_identity",
        )
        assert conn.auth_mode == "managed_identity"


class TestAzureADLSPathResolution:
    """Tests for path resolution methods."""

    def test_uri_simple_path(self):
        """Test URI generation with simple path."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="direct_key",
            account_key="key123",
        )
        uri = conn.uri("folder/file.csv")
        assert uri == "abfss://mycontainer@myaccount.dfs.core.windows.net/folder/file.csv"

    def test_uri_with_path_prefix(self):
        """Test URI generation with path_prefix."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            path_prefix="data/raw",
            auth_mode="direct_key",
            account_key="key123",
        )
        uri = conn.uri("folder/file.csv")
        assert uri == "abfss://mycontainer@myaccount.dfs.core.windows.net/data/raw/folder/file.csv"

    def test_uri_with_leading_slash(self):
        """Test URI generation strips leading slash from path."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="direct_key",
            account_key="key123",
        )
        uri = conn.uri("/folder/file.csv")
        assert uri == "abfss://mycontainer@myaccount.dfs.core.windows.net/folder/file.csv"

    def test_get_path(self):
        """Test get_path returns full URI."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="direct_key",
            account_key="key123",
        )
        path = conn.get_path("folder/file.csv")
        assert path == "abfss://mycontainer@myaccount.dfs.core.windows.net/folder/file.csv"


class TestAzureADLSGetStorageKey:
    """Tests for get_storage_key method."""

    def test_get_storage_key_direct_key(self):
        """Test get_storage_key returns account_key in direct_key mode."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="direct_key",
            account_key="mykey123",
        )
        key = conn.get_storage_key()
        assert key == "mykey123"

    def test_get_storage_key_key_vault_success(self):
        """Test get_storage_key fetches from Key Vault successfully."""
        mock_secret = Mock()
        mock_secret.value = "vault_key_123"

        mock_client = MagicMock()
        mock_client.get_secret.return_value = mock_secret

        # Create mock modules for azure.identity and azure.keyvault.secrets
        mock_identity = MagicMock()
        mock_identity.DefaultAzureCredential = MagicMock()
        
        mock_keyvault = MagicMock()
        mock_keyvault.secrets = MagicMock()
        mock_keyvault.secrets.SecretClient = MagicMock(return_value=mock_client)

        with patch.dict(sys.modules, {
            "azure": MagicMock(),
            "azure.identity": mock_identity,
            "azure.keyvault": mock_keyvault,
            "azure.keyvault.secrets": mock_keyvault.secrets,
        }):
            conn = AzureADLS(
                account="myaccount",
                container="mycontainer",
                auth_mode="key_vault",
                key_vault_name="myvault",
                secret_name="mysecret",
            )

            key = conn.get_storage_key()
            assert key == "vault_key_123"
            mock_client.get_secret.assert_called_once_with("mysecret")

    def test_get_storage_key_key_vault_timeout(self):
        """Test get_storage_key raises TimeoutError on timeout."""
        mock_client = MagicMock()

        def slow_get_secret(name):
            import time

            time.sleep(5)
            return Mock(value="key")

        mock_client.get_secret.side_effect = slow_get_secret

        # Create mock modules
        mock_identity = MagicMock()
        mock_identity.DefaultAzureCredential = MagicMock()
        
        mock_keyvault = MagicMock()
        mock_keyvault.secrets = MagicMock()
        mock_keyvault.secrets.SecretClient = MagicMock(return_value=mock_client)

        with patch.dict(sys.modules, {
            "azure": MagicMock(),
            "azure.identity": mock_identity,
            "azure.keyvault": mock_keyvault,
            "azure.keyvault.secrets": mock_keyvault.secrets,
        }):
            conn = AzureADLS(
                account="myaccount",
                container="mycontainer",
                auth_mode="key_vault",
                key_vault_name="myvault",
                secret_name="mysecret",
            )

            with pytest.raises(TimeoutError, match="Key Vault fetch timed out"):
                conn.get_storage_key(timeout=0.1)

    def test_get_storage_key_key_vault_missing_azure_libs(self):
        """Test get_storage_key raises ImportError when azure libs not installed."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="key_vault",
            key_vault_name="myvault",
            secret_name="mysecret",
        )

        # Without mocking the azure modules, the import will fail
        with pytest.raises(ImportError, match="azure-identity"):
            conn.get_storage_key()

    def test_get_storage_key_caching(self):
        """Test get_storage_key caches the key on first fetch."""
        mock_secret = Mock()
        mock_secret.value = "cached_key"

        mock_client = MagicMock()
        mock_client.get_secret.return_value = mock_secret

        # Create mock modules
        mock_identity = MagicMock()
        mock_identity.DefaultAzureCredential = MagicMock()
        
        mock_keyvault = MagicMock()
        mock_keyvault.secrets = MagicMock()
        mock_keyvault.secrets.SecretClient = MagicMock(return_value=mock_client)

        with patch.dict(sys.modules, {
            "azure": MagicMock(),
            "azure.identity": mock_identity,
            "azure.keyvault": mock_keyvault,
            "azure.keyvault.secrets": mock_keyvault.secrets,
        }):
            conn = AzureADLS(
                account="myaccount",
                container="mycontainer",
                auth_mode="key_vault",
                key_vault_name="myvault",
                secret_name="mysecret",
            )

            # First call
            key1 = conn.get_storage_key()
            # Second call should use cache
            key2 = conn.get_storage_key()

            assert key1 == "cached_key"
            assert key2 == "cached_key"
            # get_secret should only be called once
            assert mock_client.get_secret.call_count == 1

    def test_get_storage_key_sas_token(self):
        """Test get_storage_key returns sas_token in sas_token mode."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="sas_token",
            sas_token="?sv=2021-06-08",
        )
        key = conn.get_storage_key()
        assert key == "?sv=2021-06-08"

    def test_get_storage_key_other_modes_return_none(self):
        """Test get_storage_key returns None for service_principal and managed_identity."""
        conn_sp = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="service_principal",
            tenant_id="tenant123",
            client_id="client123",
            client_secret="secret123",
        )
        assert conn_sp.get_storage_key() is None

        conn_mi = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="managed_identity",
        )
        assert conn_mi.get_storage_key() is None


class TestAzureADLSPandasStorageOptions:
    """Tests for pandas_storage_options method."""

    def test_pandas_storage_options_direct_key(self):
        """Test pandas storage options for direct_key mode."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="direct_key",
            account_key="mykey123",
        )
        options = conn.pandas_storage_options()
        assert options["account_name"] == "myaccount"
        assert options["account_key"] == "mykey123"

    def test_pandas_storage_options_sas_token_strips_question_mark(self):
        """Test pandas storage options strips leading ? from SAS token."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="sas_token",
            sas_token="?sv=2021-06-08&ss=b",
        )
        options = conn.pandas_storage_options()
        assert options["account_name"] == "myaccount"
        assert options["sas_token"] == "sv=2021-06-08&ss=b"  # No leading ?

    def test_pandas_storage_options_sas_token_without_question_mark(self):
        """Test pandas storage options with SAS token without leading ?."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="sas_token",
            sas_token="sv=2021-06-08&ss=b",
        )
        options = conn.pandas_storage_options()
        assert options["sas_token"] == "sv=2021-06-08&ss=b"

    def test_pandas_storage_options_service_principal(self):
        """Test pandas storage options for service_principal mode."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="service_principal",
            tenant_id="tenant123",
            client_id="client123",
            client_secret="secret123",
        )
        options = conn.pandas_storage_options()
        assert options["account_name"] == "myaccount"
        assert options["tenant_id"] == "tenant123"
        assert options["client_id"] == "client123"
        assert options["client_secret"] == "secret123"

    def test_pandas_storage_options_managed_identity(self):
        """Test pandas storage options for managed_identity mode."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="managed_identity",
        )
        options = conn.pandas_storage_options()
        assert options["account_name"] == "myaccount"
        assert options["anon"] is False


class TestAzureADLSConfigureSpark:
    """Tests for configure_spark method."""

    def test_configure_spark_direct_key(self):
        """Test Spark configuration for direct_key mode."""
        mock_spark = MagicMock()
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="direct_key",
            account_key="mykey123",
        )
        conn.configure_spark(mock_spark)

        expected_key = "fs.azure.account.key.myaccount.dfs.core.windows.net"
        mock_spark.conf.set.assert_called_once_with(expected_key, "mykey123")

    def test_configure_spark_sas_token(self):
        """Test Spark configuration for sas_token mode."""
        mock_spark = MagicMock()
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="sas_token",
            sas_token="?sv=2021-06-08",
        )
        conn.configure_spark(mock_spark)

        # Verify multiple config calls were made
        calls = [call[0] for call in mock_spark.conf.set.call_args_list]
        
        # Check auth type is set to SAS
        auth_type_calls = [c for c in calls if "account.auth.type" in c[0]]
        assert len(auth_type_calls) > 0
        assert auth_type_calls[0][1] == "SAS"

        # Check SAS token is set (without leading ?)
        token_calls = [c for c in calls if "sas.fixed.token" in c[0]]
        assert len(token_calls) > 0
        assert token_calls[0][1] == "sv=2021-06-08"  # No leading ?

    def test_configure_spark_service_principal(self):
        """Test Spark configuration for service_principal mode."""
        mock_spark = MagicMock()
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="service_principal",
            tenant_id="tenant123",
            client_id="client123",
            client_secret="secret123",
        )
        conn.configure_spark(mock_spark)

        calls = [call[0] for call in mock_spark.conf.set.call_args_list]
        
        # Check OAuth is configured
        auth_type_calls = [c for c in calls if "account.auth.type" in c[0]]
        assert len(auth_type_calls) > 0
        assert auth_type_calls[0][1] == "OAuth"

        # Check client_id is set
        client_id_calls = [c for c in calls if "oauth2.client.id" in c[0]]
        assert len(client_id_calls) > 0
        assert client_id_calls[0][1] == "client123"

        # Check endpoint is set
        endpoint_calls = [c for c in calls if "oauth2.client.endpoint" in c[0]]
        assert len(endpoint_calls) > 0
        assert "tenant123" in endpoint_calls[0][1]

    def test_configure_spark_managed_identity(self):
        """Test Spark configuration for managed_identity mode."""
        mock_spark = MagicMock()
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="managed_identity",
        )
        conn.configure_spark(mock_spark)

        calls = [call[0] for call in mock_spark.conf.set.call_args_list]
        
        # Check OAuth is configured
        auth_type_calls = [c for c in calls if "account.auth.type" in c[0]]
        assert len(auth_type_calls) > 0
        assert auth_type_calls[0][1] == "OAuth"

        # Check MSI token provider is set
        provider_calls = [c for c in calls if "oauth.provider.type" in c[0]]
        assert len(provider_calls) > 0
        assert "MsiTokenProvider" in provider_calls[0][1]


class TestAzureADLSGetClientSecret:
    """Tests for get_client_secret method."""

    def test_get_client_secret_returns_literal(self):
        """Test get_client_secret returns literal client_secret."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="service_principal",
            tenant_id="tenant123",
            client_id="client123",
            client_secret="secret123",
        )
        secret = conn.get_client_secret()
        assert secret == "secret123"

    def test_get_client_secret_returns_cached(self):
        """Test get_client_secret returns cached value if available."""
        conn = AzureADLS(
            account="myaccount",
            container="mycontainer",
            auth_mode="service_principal",
            tenant_id="tenant123",
            client_id="client123",
            client_secret="secret123",
        )
        # Simulate cached secret
        conn._cached_key = "cached_secret"
        secret = conn.get_client_secret()
        assert secret == "cached_secret"
