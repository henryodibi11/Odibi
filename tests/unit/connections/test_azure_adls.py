"""Unit tests for Azure ADLS connection."""

from types import ModuleType
from unittest.mock import MagicMock, Mock, patch

import pytest

from odibi.connections.azure_adls import AzureADLS


class TestAzureADLSInit:
    """Tests for AzureADLS initialization."""

    def test_init_with_key_vault_mode(self):
        """Test initialization with key_vault mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="key_vault",
            key_vault_name="myvault",
            secret_name="mysecret",
        )
        assert conn.account == "mystorageacct"
        assert conn.container == "mycontainer"
        assert conn.auth_mode == "key_vault"
        assert conn.key_vault_name == "myvault"
        assert conn.secret_name == "mysecret"
        assert conn.path_prefix == ""
        assert conn._cached_storage_key is None
        assert conn._cached_client_secret is None

    def test_init_with_direct_key_mode(self):
        """Test initialization with direct_key mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="direct_key",
            account_key="mykey123",
        )
        assert conn.auth_mode == "direct_key"
        assert conn.account_key == "mykey123"

    def test_init_with_sas_token_mode(self):
        """Test initialization with sas_token mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="sas_token",
            sas_token="?sv=2020-08-04&ss=b&srt=sco&sp=rwdlac",
        )
        assert conn.auth_mode == "sas_token"
        assert conn.sas_token == "?sv=2020-08-04&ss=b&srt=sco&sp=rwdlac"

    def test_init_with_service_principal_mode(self):
        """Test initialization with service_principal mode."""
        conn = AzureADLS(
            account="mystorageacct",
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
        """Test initialization with managed_identity mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="managed_identity",
        )
        assert conn.auth_mode == "managed_identity"

    def test_init_with_path_prefix(self):
        """Test initialization with path prefix."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            path_prefix="/bronze/raw/",
            auth_mode="direct_key",
            account_key="key123",
        )
        assert conn.path_prefix == "bronze/raw"

    def test_init_strips_path_prefix_slashes(self):
        """Test that path prefix slashes are stripped."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            path_prefix="///bronze///",
            auth_mode="direct_key",
            account_key="key123",
        )
        assert conn.path_prefix == "bronze"

    def test_init_without_validate(self):
        """Test initialization without validation."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="key_vault",
            validate=False,
        )
        # Should not raise even though key_vault_name and secret_name are missing
        assert conn.auth_mode == "key_vault"


class TestValidate:
    """Tests for connection validation."""

    def test_validate_key_vault_success(self):
        """Test validation succeeds with key_vault mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="key_vault",
            key_vault_name="myvault",
            secret_name="mysecret",
        )
        # Should not raise
        conn.validate()

    def test_validate_direct_key_success(self):
        """Test validation succeeds with direct_key mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="direct_key",
            account_key="mykey123",
        )
        # Should not raise
        conn.validate()

    def test_validate_sas_token_success(self):
        """Test validation succeeds with sas_token mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="sas_token",
            sas_token="?sv=2020-08-04&ss=b&srt=sco&sp=rwdlac",
        )
        # Should not raise
        conn.validate()

    def test_validate_sas_token_with_key_vault_success(self):
        """Test validation succeeds with sas_token mode using Key Vault."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="sas_token",
            key_vault_name="myvault",
            secret_name="mysecret",
        )
        # Should not raise
        conn.validate()

    def test_validate_service_principal_success(self):
        """Test validation succeeds with service_principal mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="service_principal",
            tenant_id="tenant123",
            client_id="client123",
            client_secret="secret123",
        )
        # Should not raise
        conn.validate()

    def test_validate_service_principal_with_key_vault_success(self):
        """Test validation succeeds with service_principal using Key Vault for secret."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="service_principal",
            tenant_id="tenant123",
            client_id="client123",
            key_vault_name="myvault",
            secret_name="mysecret",
        )
        # Should not raise
        conn.validate()

    def test_validate_managed_identity_success(self):
        """Test validation succeeds with managed_identity mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="managed_identity",
        )
        # Should not raise
        conn.validate()

    def test_validate_fails_without_account(self):
        """Test validation fails when account is missing."""
        with pytest.raises(ValueError, match="ADLS connection requires 'account'"):
            AzureADLS(
                account="",
                container="mycontainer",
                auth_mode="direct_key",
                account_key="key123",
            )

    def test_validate_fails_without_container(self):
        """Test validation fails when container is missing."""
        with pytest.raises(ValueError, match="ADLS connection requires 'container'"):
            AzureADLS(
                account="mystorageacct",
                container="",
                auth_mode="direct_key",
                account_key="key123",
            )

    def test_validate_fails_key_vault_without_vault_name(self):
        """Test validation fails for key_vault without vault name."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="key_vault",
            secret_name="mysecret",
            validate=False,
        )
        with pytest.raises(ValueError, match="key_vault mode requires 'key_vault_name' and 'secret_name'"):
            conn.validate()

    def test_validate_fails_key_vault_without_secret_name(self):
        """Test validation fails for key_vault without secret name."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="key_vault",
            key_vault_name="myvault",
            validate=False,
        )
        with pytest.raises(ValueError, match="key_vault mode requires 'key_vault_name' and 'secret_name'"):
            conn.validate()

    def test_validate_fails_direct_key_without_account_key(self):
        """Test validation fails for direct_key without account_key."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="direct_key",
            validate=False,
        )
        with pytest.raises(ValueError, match="direct_key mode requires 'account_key'"):
            conn.validate()

    def test_validate_fails_sas_token_without_token_or_vault(self):
        """Test validation fails for sas_token without token or Key Vault."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="sas_token",
            validate=False,
        )
        with pytest.raises(ValueError, match="sas_token mode requires 'sas_token'"):
            conn.validate()

    def test_validate_fails_service_principal_without_tenant_id(self):
        """Test validation fails for service_principal without tenant_id."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="service_principal",
            client_id="client123",
            client_secret="secret123",
            validate=False,
        )
        with pytest.raises(
            ValueError, match="service_principal mode requires 'tenant_id' and 'client_id'"
        ):
            conn.validate()

    def test_validate_fails_service_principal_without_client_id(self):
        """Test validation fails for service_principal without client_id."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="service_principal",
            tenant_id="tenant123",
            client_secret="secret123",
            validate=False,
        )
        with pytest.raises(
            ValueError, match="service_principal mode requires 'tenant_id' and 'client_id'"
        ):
            conn.validate()

    def test_validate_fails_service_principal_without_client_secret_or_vault(self):
        """Test validation fails for service_principal without client_secret or Key Vault."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="service_principal",
            tenant_id="tenant123",
            client_id="client123",
            validate=False,
        )
        with pytest.raises(ValueError, match="service_principal mode requires 'client_secret'"):
            conn.validate()

    def test_validate_fails_unsupported_auth_mode(self):
        """Test validation fails for unsupported auth_mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="invalid_mode",
            validate=False,
        )
        with pytest.raises(ValueError, match="Unsupported auth_mode: 'invalid_mode'"):
            conn.validate()

    def test_validate_warns_direct_key_in_production(self):
        """Test that direct_key mode warns in production."""
        with patch.dict("os.environ", {"ODIBI_ENV": "production"}):
            with pytest.warns(UserWarning, match="Using direct_key in production is not recommended"):
                AzureADLS(
                    account="mystorageacct",
                    container="mycontainer",
                    auth_mode="direct_key",
                    account_key="key123",
                )


class TestGetStorageKey:
    """Tests for get_storage_key method."""

    def test_get_storage_key_returns_cached_key(self):
        """Test that cached storage key is returned if available."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="key_vault",
            key_vault_name="myvault",
            secret_name="mysecret",
        )
        conn._cached_storage_key = "cached_key_value"

        key = conn.get_storage_key()

        assert key == "cached_key_value"

    def test_get_storage_key_returns_direct_key(self):
        """Test that direct account key is returned for direct_key mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="direct_key",
            account_key="direct_key_123",
        )

        key = conn.get_storage_key()

        assert key == "direct_key_123"

    def test_get_storage_key_returns_sas_token(self):
        """Test that SAS token is returned for sas_token mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="sas_token",
            sas_token="?sv=2020-08-04&ss=b",
        )

        key = conn.get_storage_key()

        assert key == "?sv=2020-08-04&ss=b"

    def test_get_storage_key_returns_cached_for_sas_token(self):
        """Test that cached key is returned for sas_token mode if available."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="sas_token",
            sas_token="?sv=2020-08-04&ss=b",
        )
        conn._cached_storage_key = "cached_sas_token"

        key = conn.get_storage_key()

        assert key == "cached_sas_token"

    def test_get_storage_key_returns_none_for_service_principal(self):
        """Test that None is returned for service_principal mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="service_principal",
            tenant_id="tenant123",
            client_id="client123",
            client_secret="secret123",
        )

        key = conn.get_storage_key()

        assert key is None

    def test_get_storage_key_returns_none_for_managed_identity(self):
        """Test that None is returned for managed_identity mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="managed_identity",
        )

        key = conn.get_storage_key()

        assert key is None

    def test_get_storage_key_fetches_from_key_vault(self):
        """Test fetching storage key from Azure Key Vault."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="key_vault",
            key_vault_name="myvault",
            secret_name="mysecret",
        )

        mock_secret = Mock()
        mock_secret.value = "vault_key_value"
        mock_client = Mock()
        mock_client.get_secret.return_value = mock_secret

        # Create mock modules
        mock_credential_class = MagicMock()
        mock_client_class = MagicMock(return_value=mock_client)

        azure_identity = ModuleType("azure.identity")
        azure_identity.DefaultAzureCredential = mock_credential_class
        azure_keyvault = ModuleType("azure.keyvault")
        azure_keyvault_secrets = ModuleType("azure.keyvault.secrets")
        azure_keyvault_secrets.SecretClient = mock_client_class

        with patch.dict(
            "sys.modules",
            {
                "azure.identity": azure_identity,
                "azure.keyvault": azure_keyvault,
                "azure.keyvault.secrets": azure_keyvault_secrets,
            },
        ):
            with patch("odibi.connections.azure_adls.logger") as mock_logger:
                key = conn.get_storage_key()

                assert key == "vault_key_value"
                assert conn._cached_storage_key == "vault_key_value"
                mock_client_class.assert_called_once_with(
                    vault_url="https://myvault.vault.azure.net",
                    credential=mock_credential_class.return_value,
                )
                mock_client.get_secret.assert_called_once_with("mysecret")
                mock_logger.register_secret.assert_called_once_with("vault_key_value")

    def test_get_storage_key_key_vault_import_error(self):
        """Test that missing azure libraries raises ImportError."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="key_vault",
            key_vault_name="myvault",
            secret_name="mysecret",
        )

        import builtins

        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name in ("azure.identity", "azure.keyvault.secrets"):
                raise ImportError("No module named azure")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            with pytest.raises(
                ImportError,
                match="Key Vault authentication requires 'azure-identity' and 'azure-keyvault-secrets'",
            ):
                conn.get_storage_key()

    def test_get_storage_key_key_vault_timeout(self):
        """Test that Key Vault fetch timeout raises TimeoutError."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="key_vault",
            key_vault_name="myvault",
            secret_name="mysecret",
        )

        # Create mock azure modules
        mock_credential_class = MagicMock()
        mock_client = Mock()
        mock_client_class = MagicMock(return_value=mock_client)

        azure_identity = ModuleType("azure.identity")
        azure_identity.DefaultAzureCredential = mock_credential_class
        azure_keyvault = ModuleType("azure.keyvault")
        azure_keyvault_secrets = ModuleType("azure.keyvault.secrets")
        azure_keyvault_secrets.SecretClient = mock_client_class

        with patch.dict(
            "sys.modules",
            {
                "azure.identity": azure_identity,
                "azure.keyvault": azure_keyvault,
                "azure.keyvault.secrets": azure_keyvault_secrets,
            },
        ):
            # Mock the executor to raise TimeoutError when result() is called
            import concurrent.futures
            
            mock_future = Mock()
            mock_future.result.side_effect = concurrent.futures.TimeoutError()
            
            with patch("concurrent.futures.ThreadPoolExecutor") as mock_executor_class:
                mock_executor_instance = Mock()
                mock_executor_instance.submit.return_value = mock_future
                mock_executor_instance.__enter__ = Mock(return_value=mock_executor_instance)
                mock_executor_instance.__exit__ = Mock(return_value=False)
                mock_executor_class.return_value = mock_executor_instance

                with pytest.raises(TimeoutError, match="Key Vault fetch timed out after"):
                    conn.get_storage_key(timeout=1.0)


class TestGetClientSecret:
    """Tests for get_client_secret method."""

    def test_get_client_secret_returns_cached_secret(self):
        """Test that cached client secret is returned if available."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="service_principal",
            tenant_id="tenant123",
            client_id="client123",
            client_secret="secret123",
        )
        conn._cached_client_secret = "cached_secret_value"

        secret = conn.get_client_secret()

        assert secret == "cached_secret_value"

    def test_get_client_secret_returns_literal_secret(self):
        """Test that literal client_secret is returned if no cached value."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="service_principal",
            tenant_id="tenant123",
            client_id="client123",
            client_secret="literal_secret_123",
        )

        secret = conn.get_client_secret()

        assert secret == "literal_secret_123"

    def test_get_client_secret_returns_none_if_not_provided(self):
        """Test that None is returned if no client_secret is provided."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="managed_identity",
        )

        secret = conn.get_client_secret()

        assert secret is None

    def test_cached_storage_key_and_client_secret_are_separate(self):
        """Test that _cached_storage_key and _cached_client_secret are separate fields."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="service_principal",
            tenant_id="tenant123",
            client_id="client123",
            client_secret="secret123",
        )

        # Set both cached values to different values
        conn._cached_storage_key = "storage_key_value"
        conn._cached_client_secret = "client_secret_value"

        # Verify they are separate
        assert conn._cached_storage_key == "storage_key_value"
        assert conn._cached_client_secret == "client_secret_value"
        assert conn.get_client_secret() == "client_secret_value"
        # service_principal mode returns cached key if set (even though not typically used)
        # The important part is that the cached values are separate
        assert conn.get_storage_key() == "storage_key_value"  # Returns cached value from cache lock
        # Clear cache to verify service_principal returns None when no cached value
        conn._cached_storage_key = None
        assert conn.get_storage_key() is None  # service_principal mode returns None


class TestPandasStorageOptions:
    """Tests for pandas_storage_options method."""

    def test_pandas_storage_options_key_vault_mode(self):
        """Test storage options for key_vault mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="key_vault",
            key_vault_name="myvault",
            secret_name="mysecret",
        )
        conn._cached_storage_key = "cached_key"

        options = conn.pandas_storage_options()

        assert options["account_name"] == "mystorageacct"
        assert options["account_key"] == "cached_key"

    def test_pandas_storage_options_direct_key_mode(self):
        """Test storage options for direct_key mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="direct_key",
            account_key="mykey123",
        )

        options = conn.pandas_storage_options()

        assert options["account_name"] == "mystorageacct"
        assert options["account_key"] == "mykey123"

    def test_pandas_storage_options_sas_token_mode(self):
        """Test storage options for sas_token mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="sas_token",
            sas_token="?sv=2020-08-04&ss=b",
        )

        options = conn.pandas_storage_options()

        assert options["account_name"] == "mystorageacct"
        assert options["sas_token"] == "sv=2020-08-04&ss=b"  # Leading '?' stripped

    def test_pandas_storage_options_sas_token_without_question_mark(self):
        """Test storage options for sas_token mode without leading '?'."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="sas_token",
            sas_token="sv=2020-08-04&ss=b",
        )

        options = conn.pandas_storage_options()

        assert options["account_name"] == "mystorageacct"
        assert options["sas_token"] == "sv=2020-08-04&ss=b"

    def test_pandas_storage_options_service_principal_mode(self):
        """Test storage options for service_principal mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="service_principal",
            tenant_id="tenant123",
            client_id="client123",
            client_secret="secret123",
        )

        options = conn.pandas_storage_options()

        assert options["account_name"] == "mystorageacct"
        assert options["tenant_id"] == "tenant123"
        assert options["client_id"] == "client123"
        assert options["client_secret"] == "secret123"

    def test_pandas_storage_options_service_principal_with_cached_secret(self):
        """Test storage options for service_principal with cached client_secret."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="service_principal",
            tenant_id="tenant123",
            client_id="client123",
            client_secret="secret123",
        )
        conn._cached_client_secret = "cached_secret"

        options = conn.pandas_storage_options()

        assert options["client_secret"] == "cached_secret"

    def test_pandas_storage_options_managed_identity_mode(self):
        """Test storage options for managed_identity mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="managed_identity",
        )

        options = conn.pandas_storage_options()

        assert options["account_name"] == "mystorageacct"
        assert options["anon"] is False


class TestConfigureSpark:
    """Tests for configure_spark method."""

    def test_configure_spark_key_vault_mode(self):
        """Test Spark configuration for key_vault mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="key_vault",
            key_vault_name="myvault",
            secret_name="mysecret",
        )
        conn._cached_storage_key = "cached_key"

        mock_spark = Mock()
        conn.configure_spark(mock_spark)

        expected_key = "fs.azure.account.key.mystorageacct.dfs.core.windows.net"
        mock_spark.conf.set.assert_called_once_with(expected_key, "cached_key")

    def test_configure_spark_direct_key_mode(self):
        """Test Spark configuration for direct_key mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="direct_key",
            account_key="mykey123",
        )

        mock_spark = Mock()
        conn.configure_spark(mock_spark)

        expected_key = "fs.azure.account.key.mystorageacct.dfs.core.windows.net"
        mock_spark.conf.set.assert_called_with(expected_key, "mykey123")

    def test_configure_spark_sas_token_mode(self):
        """Test Spark configuration for sas_token mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="sas_token",
            sas_token="?sv=2020-08-04&ss=b",
        )

        mock_spark = Mock()
        conn.configure_spark(mock_spark)

        # Verify all SAS token configuration keys
        calls = [call.args for call in mock_spark.conf.set.call_args_list]
        keys_set = {call[0]: call[1] for call in calls}

        assert keys_set["fs.azure.account.auth.type.mystorageacct.dfs.core.windows.net"] == "SAS"
        assert (
            keys_set["fs.azure.sas.token.provider.type.mystorageacct.dfs.core.windows.net"]
            == "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
        )
        assert keys_set["fs.azure.sas.fixed.token.mystorageacct.dfs.core.windows.net"] == "sv=2020-08-04&ss=b"
        assert keys_set["fs.azure.account.hns.enabled.mystorageacct.dfs.core.windows.net"] == "false"
        assert keys_set["fs.azure.skip.user.group.metadata.during.initialization"] == "true"

    def test_configure_spark_service_principal_mode(self):
        """Test Spark configuration for service_principal mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="service_principal",
            tenant_id="tenant123",
            client_id="client123",
            client_secret="secret123",
        )

        mock_spark = Mock()
        conn.configure_spark(mock_spark)

        calls = [call.args for call in mock_spark.conf.set.call_args_list]
        keys_set = {call[0]: call[1] for call in calls}

        assert keys_set["fs.azure.account.auth.type.mystorageacct.dfs.core.windows.net"] == "OAuth"
        assert (
            keys_set["fs.azure.account.oauth.provider.type.mystorageacct.dfs.core.windows.net"]
            == "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
        )
        assert keys_set["fs.azure.account.oauth2.client.id.mystorageacct.dfs.core.windows.net"] == "client123"
        assert keys_set["fs.azure.account.oauth2.client.secret.mystorageacct.dfs.core.windows.net"] == "secret123"
        assert (
            keys_set["fs.azure.account.oauth2.client.endpoint.mystorageacct.dfs.core.windows.net"]
            == "https://login.microsoftonline.com/tenant123/oauth2/token"
        )

    def test_configure_spark_managed_identity_mode(self):
        """Test Spark configuration for managed_identity mode."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="managed_identity",
        )

        mock_spark = Mock()
        conn.configure_spark(mock_spark)

        calls = [call.args for call in mock_spark.conf.set.call_args_list]
        keys_set = {call[0]: call[1] for call in calls}

        assert keys_set["fs.azure.account.auth.type.mystorageacct.dfs.core.windows.net"] == "OAuth"
        assert (
            keys_set["fs.azure.account.oauth.provider.type.mystorageacct.dfs.core.windows.net"]
            == "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider"
        )


class TestUri:
    """Tests for uri method."""

    def test_uri_without_path_prefix(self):
        """Test URI building without path prefix."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="direct_key",
            account_key="key123",
        )

        uri = conn.uri("folder/file.csv")

        assert uri == "abfss://mycontainer@mystorageacct.dfs.core.windows.net/folder/file.csv"

    def test_uri_with_path_prefix(self):
        """Test URI building with path prefix."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            path_prefix="bronze/raw",
            auth_mode="direct_key",
            account_key="key123",
        )

        uri = conn.uri("folder/file.csv")

        assert uri == "abfss://mycontainer@mystorageacct.dfs.core.windows.net/bronze/raw/folder/file.csv"

    def test_uri_strips_leading_slash_from_path(self):
        """Test that leading slash is stripped from path."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="direct_key",
            account_key="key123",
        )

        uri = conn.uri("/folder/file.csv")

        assert uri == "abfss://mycontainer@mystorageacct.dfs.core.windows.net/folder/file.csv"

    def test_uri_with_nested_path(self):
        """Test URI building with deeply nested path."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="direct_key",
            account_key="key123",
        )

        uri = conn.uri("bronze/customers/2024/01/data.parquet")

        assert uri == "abfss://mycontainer@mystorageacct.dfs.core.windows.net/bronze/customers/2024/01/data.parquet"


class TestGetPath:
    """Tests for get_path method."""

    def test_get_path_delegates_to_uri(self):
        """Test that get_path delegates to uri method."""
        conn = AzureADLS(
            account="mystorageacct",
            container="mycontainer",
            auth_mode="direct_key",
            account_key="key123",
        )

        path = conn.get_path("folder/file.csv")
        expected = conn.uri("folder/file.csv")

        assert path == expected
        assert path == "abfss://mycontainer@mystorageacct.dfs.core.windows.net/folder/file.csv"
