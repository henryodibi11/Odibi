"""Azure Data Lake Storage Gen2 connection (Phase 2A: Key Vault authentication)."""

import os
import posixpath
import warnings
from typing import Optional
from .base import BaseConnection


class AzureADLS(BaseConnection):
    """Azure Data Lake Storage Gen2 connection.

    Phase 2A: Key Vault authentication + multi-account support
    Supports both key_vault (recommended) and direct_key auth modes.
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
        validate: bool = True,
        **kwargs,
    ):
        """Initialize ADLS connection.

        Args:
            account: Storage account name (e.g., 'mystorageaccount')
            container: Container/filesystem name
            path_prefix: Optional prefix for all paths
            auth_mode: Authentication mode ('key_vault' or 'direct_key')
            key_vault_name: Azure Key Vault name (required for key_vault mode)
            secret_name: Secret name in Key Vault (required for key_vault mode)
            account_key: Storage account key (required for direct_key mode)
            validate: Validate configuration on init
        """
        self.account = account
        self.container = container
        self.path_prefix = path_prefix.strip("/") if path_prefix else ""
        self.auth_mode = auth_mode
        self.key_vault_name = key_vault_name
        self.secret_name = secret_name
        self.account_key = account_key
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
        else:
            raise ValueError(
                f"Unsupported auth_mode: '{self.auth_mode}'. " f"Use 'key_vault' or 'direct_key'."
            )

    def get_storage_key(self, timeout: float = 30.0) -> str:
        """Get storage account key (cached).

        Args:
            timeout: Timeout for Key Vault operations in seconds (default: 30.0)

        Returns:
            Storage account key

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

        raise ValueError(f"Invalid auth_mode: {self.auth_mode}")

    def pandas_storage_options(self) -> dict:
        """Get storage options for pandas/fsspec.

        Returns:
            Dictionary with account_name and account_key for fsspec
        """
        return {"account_name": self.account, "account_key": self.get_storage_key()}

    def configure_spark(self, spark) -> None:
        """Configure Spark session with storage account key.

        Args:
            spark: SparkSession instance
        """
        config_key = f"fs.azure.account.key.{self.account}.dfs.core.windows.net"
        spark.conf.set(config_key, self.get_storage_key())

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
