"""Azure Data Lake Storage Gen2 connection (Phase 2A: Multi-mode authentication)."""

import os
import posixpath
import warnings
from typing import Optional, Dict, Any
from .base import BaseConnection


class AzureADLS(BaseConnection):
    """Azure Data Lake Storage Gen2 connection.

    Phase 2A: Multi-mode authentication + multi-account support
    Supports key_vault (recommended), direct_key, service_principal, and managed_identity.
    """

    def __init__(
        self,
        account: str,
        container: str,
        path_prefix: str = "",
        auth_mode: str = "key_vault",
        key_vault_name: Optional[str] = None,
        secret_name: Optional[str] = None,
        account_key: Optional[str] = None,
        tenant_id: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        validate: bool = True,
        **kwargs,
    ):
        """Initialize ADLS connection.

        Args:
            account: Storage account name (e.g., 'mystorageaccount')
            container: Container/filesystem name
            path_prefix: Optional prefix for all paths
            auth_mode: Authentication mode ('key_vault', 'direct_key', 'service_principal', 'managed_identity')
            key_vault_name: Azure Key Vault name (required for key_vault mode)
            secret_name: Secret name in Key Vault (required for key_vault mode)
            account_key: Storage account key (required for direct_key mode)
            tenant_id: Azure Tenant ID (required for service_principal)
            client_id: Service Principal Client ID (required for service_principal)
            client_secret: Service Principal Client Secret (required for service_principal)
            validate: Validate configuration on init
        """
        self.account = account
        self.container = container
        self.path_prefix = path_prefix.strip("/") if path_prefix else ""
        self.auth_mode = auth_mode
        self.key_vault_name = key_vault_name
        self.secret_name = secret_name
        self.account_key = account_key
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret

        self._cached_key: Optional[str] = None

        if validate:
            self.validate()

    def validate(self) -> None:
        """Validate ADLS connection configuration.

        Raises:
            ValueError: If required fields are missing for the selected auth_mode
        """
        if not self.account:
            raise ValueError("ADLS connection requires 'account'")
        if not self.container:
            raise ValueError("ADLS connection requires 'container'")

        if self.auth_mode == "key_vault":
            if not self.key_vault_name or not self.secret_name:
                raise ValueError(
                    f"key_vault mode requires 'key_vault_name' and 'secret_name' "
                    f"for connection to {self.account}/{self.container}"
                )
        elif self.auth_mode == "direct_key":
            if not self.account_key:
                raise ValueError(
                    f"direct_key mode requires 'account_key' "
                    f"for connection to {self.account}/{self.container}"
                )

            # Warn in production
            if os.getenv("ODIBI_ENV") == "production":
                warnings.warn(
                    f"⚠️  Using direct_key in production is not recommended. "
                    f"Use auth_mode: key_vault. Connection: {self.account}/{self.container}",
                    UserWarning,
                )
        elif self.auth_mode == "service_principal":
            if not self.tenant_id or not self.client_id or not self.client_secret:
                raise ValueError(
                    f"service_principal mode requires 'tenant_id', 'client_id', and 'client_secret' "
                    f"for connection to {self.account}/{self.container}"
                )
        elif self.auth_mode == "managed_identity":
            # No specific config required, but we might check if environment supports it
            pass
        else:
            raise ValueError(
                f"Unsupported auth_mode: '{self.auth_mode}'. "
                f"Use 'key_vault', 'direct_key', 'service_principal', or 'managed_identity'."
            )

    def get_storage_key(self, timeout: float = 30.0) -> Optional[str]:
        """Get storage account key (cached).

        Only relevant for 'key_vault' and 'direct_key' modes.

        Args:
            timeout: Timeout for Key Vault operations in seconds (default: 30.0)

        Returns:
            Storage account key or None if not applicable for auth_mode

        Raises:
            ImportError: If azure libraries not installed (key_vault mode)
            TimeoutError: If Key Vault fetch exceeds timeout
            Exception: If Key Vault access fails
        """
        # Return cached key if available
        if self._cached_key:
            return self._cached_key

        if self.auth_mode == "key_vault":
            try:
                from azure.identity import DefaultAzureCredential
                from azure.keyvault.secrets import SecretClient
                import concurrent.futures
            except ImportError as e:
                raise ImportError(
                    "Key Vault authentication requires 'azure-identity' and 'azure-keyvault-secrets'. "
                    "Install with: pip install odibi[azure] or pip install azure-identity azure-keyvault-secrets"
                ) from e

            # Create Key Vault client
            credential = DefaultAzureCredential()
            kv_uri = f"https://{self.key_vault_name}.vault.azure.net"
            client = SecretClient(vault_url=kv_uri, credential=credential)

            # Fetch secret with timeout protection
            def _fetch():
                secret = client.get_secret(self.secret_name)
                return secret.value

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_fetch)
                try:
                    self._cached_key = future.result(timeout=timeout)
                    return self._cached_key
                except concurrent.futures.TimeoutError:
                    raise TimeoutError(
                        f"Key Vault fetch timed out after {timeout}s for "
                        f"vault '{self.key_vault_name}', secret '{self.secret_name}'"
                    )

        elif self.auth_mode == "direct_key":
            return self.account_key

        # For other modes (SP, MI), we don't use an account key
        return None

    def pandas_storage_options(self) -> Dict[str, Any]:
        """Get storage options for pandas/fsspec.

        Returns:
            Dictionary with appropriate authentication parameters for fsspec
        """
        base_options = {"account_name": self.account}

        if self.auth_mode in ["key_vault", "direct_key"]:
            return {**base_options, "account_key": self.get_storage_key()}

        elif self.auth_mode == "service_principal":
            return {
                **base_options,
                "tenant_id": self.tenant_id,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }

        elif self.auth_mode == "managed_identity":
            # adlfs supports using DefaultAzureCredential implicitly if anon=False
            # and no other creds provided, assuming azure.identity is installed
            return {**base_options, "anon": False}

        return base_options

    def configure_spark(self, spark) -> None:
        """Configure Spark session with storage credentials.

        Args:
            spark: SparkSession instance
        """
        if self.auth_mode in ["key_vault", "direct_key"]:
            config_key = f"fs.azure.account.key.{self.account}.dfs.core.windows.net"
            spark.conf.set(config_key, self.get_storage_key())

        elif self.auth_mode == "service_principal":
            # Configure OAuth for ADLS Gen2
            # Ref: https://hadoop.apache.org/docs/stable/hadoop-azure/abfs.html
            prefix = f"fs.azure.account.auth.type.{self.account}.dfs.core.windows.net"
            spark.conf.set(prefix, "OAuth")

            prefix = f"fs.azure.account.oauth.provider.type.{self.account}.dfs.core.windows.net"
            spark.conf.set(prefix, "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")

            prefix = f"fs.azure.account.oauth2.client.id.{self.account}.dfs.core.windows.net"
            spark.conf.set(prefix, self.client_id)

            prefix = f"fs.azure.account.oauth2.client.secret.{self.account}.dfs.core.windows.net"
            spark.conf.set(prefix, self.client_secret)

            prefix = f"fs.azure.account.oauth2.client.endpoint.{self.account}.dfs.core.windows.net"
            endpoint = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/token"
            spark.conf.set(prefix, endpoint)

        elif self.auth_mode == "managed_identity":
            prefix = f"fs.azure.account.auth.type.{self.account}.dfs.core.windows.net"
            spark.conf.set(prefix, "OAuth")

            prefix = f"fs.azure.account.oauth.provider.type.{self.account}.dfs.core.windows.net"
            spark.conf.set(prefix, "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider")

    def uri(self, path: str) -> str:
        """Build abfss:// URI for given path.

        Args:
            path: Relative path within container

        Returns:
            Full abfss:// URI

        Example:
            >>> conn = AzureADLS(account="myaccount", container="data", auth_mode="direct_key", account_key="key123")
            >>> conn.uri("folder/file.csv")
            'abfss://data@myaccount.dfs.core.windows.net/folder/file.csv'
        """
        if self.path_prefix:
            full_path = posixpath.join(self.path_prefix, path.lstrip("/"))
        else:
            full_path = path.lstrip("/")

        return f"abfss://{self.container}@{self.account}.dfs.core.windows.net/{full_path}"

    def get_path(self, relative_path: str) -> str:
        """Get full abfss:// URI for relative path."""
        return self.uri(relative_path)
