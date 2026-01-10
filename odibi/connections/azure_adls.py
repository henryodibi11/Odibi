"""Azure Data Lake Storage Gen2 connection (Phase 2A: Multi-mode authentication)."""

import os
import posixpath
import threading
import warnings
from typing import Any, Dict, Optional

from odibi.utils.logging import logger
from odibi.utils.logging_context import get_logging_context

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
        sas_token: Optional[str] = None,
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
            auth_mode: Authentication mode
                ('key_vault', 'direct_key', 'sas_token', 'service_principal', 'managed_identity')
            key_vault_name: Azure Key Vault name (required for key_vault mode)
            secret_name: Secret name in Key Vault (required for key_vault mode)
            account_key: Storage account key (required for direct_key mode)
            sas_token: Shared Access Signature token (required for sas_token mode)
            tenant_id: Azure Tenant ID (required for service_principal)
            client_id: Service Principal Client ID (required for service_principal)
            client_secret: Service Principal Client Secret (required for service_principal)
            validate: Validate configuration on init
        """
        ctx = get_logging_context()
        ctx.log_connection(
            connection_type="azure_adls",
            connection_name=f"{account}/{container}",
            action="init",
            account=account,
            container=container,
            auth_mode=auth_mode,
            path_prefix=path_prefix or "(none)",
        )

        self.account = account
        self.container = container
        self.path_prefix = path_prefix.strip("/") if path_prefix else ""
        self.auth_mode = auth_mode
        self.key_vault_name = key_vault_name
        self.secret_name = secret_name
        self.account_key = account_key
        self.sas_token = sas_token
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret

        self._cached_key: Optional[str] = None
        self._cache_lock = threading.Lock()

        if validate:
            self.validate()

    def validate(self) -> None:
        """Validate ADLS connection configuration.

        Raises:
            ValueError: If required fields are missing for the selected auth_mode
        """
        ctx = get_logging_context()
        ctx.debug(
            "Validating AzureADLS connection",
            account=self.account,
            container=self.container,
            auth_mode=self.auth_mode,
        )

        if not self.account:
            ctx.error("ADLS connection validation failed: missing 'account'")
            raise ValueError(
                "ADLS connection requires 'account'. "
                "Provide the storage account name (e.g., account: 'mystorageaccount')."
            )
        if not self.container:
            ctx.error(
                "ADLS connection validation failed: missing 'container'",
                account=self.account,
            )
            raise ValueError(
                f"ADLS connection requires 'container' for account '{self.account}'. "
                "Provide the container/filesystem name."
            )

        if self.auth_mode == "key_vault":
            if not self.key_vault_name or not self.secret_name:
                ctx.error(
                    "ADLS key_vault mode validation failed",
                    account=self.account,
                    container=self.container,
                    key_vault_name=self.key_vault_name or "(missing)",
                    secret_name=self.secret_name or "(missing)",
                )
                raise ValueError(
                    f"key_vault mode requires 'key_vault_name' and 'secret_name' "
                    f"for connection to {self.account}/{self.container}"
                )
        elif self.auth_mode == "direct_key":
            if not self.account_key:
                ctx.error(
                    "ADLS direct_key mode validation failed: missing account_key",
                    account=self.account,
                    container=self.container,
                )
                raise ValueError(
                    f"direct_key mode requires 'account_key' "
                    f"for connection to {self.account}/{self.container}"
                )

            # Warn in production
            if os.getenv("ODIBI_ENV") == "production":
                ctx.warning(
                    "Using direct_key in production is not recommended",
                    account=self.account,
                    container=self.container,
                )
                warnings.warn(
                    f"⚠️  Using direct_key in production is not recommended. "
                    f"Use auth_mode: key_vault. Connection: {self.account}/{self.container}",
                    UserWarning,
                )
        elif self.auth_mode == "sas_token":
            if not self.sas_token and not (self.key_vault_name and self.secret_name):
                ctx.error(
                    "ADLS sas_token mode validation failed",
                    account=self.account,
                    container=self.container,
                )
                raise ValueError(
                    f"sas_token mode requires 'sas_token' (or key_vault_name/secret_name) "
                    f"for connection to {self.account}/{self.container}"
                )
        elif self.auth_mode == "service_principal":
            if not self.tenant_id or not self.client_id:
                ctx.error(
                    "ADLS service_principal mode validation failed",
                    account=self.account,
                    container=self.container,
                    missing="tenant_id and/or client_id",
                )
                raise ValueError(
                    f"service_principal mode requires 'tenant_id' and 'client_id' "
                    f"for connection to {self.account}/{self.container}. "
                    f"Got tenant_id={self.tenant_id or '(missing)'}, "
                    f"client_id={self.client_id or '(missing)'}."
                )

            if not self.client_secret and not (self.key_vault_name and self.secret_name):
                ctx.error(
                    "ADLS service_principal mode validation failed: missing client_secret",
                    account=self.account,
                    container=self.container,
                )
                raise ValueError(
                    f"service_principal mode requires 'client_secret' "
                    f"(or key_vault_name/secret_name) for {self.account}/{self.container}"
                )
        elif self.auth_mode == "managed_identity":
            # No specific config required, but we might check if environment supports it
            ctx.debug(
                "Using managed_identity auth mode",
                account=self.account,
                container=self.container,
            )
        else:
            ctx.error(
                "ADLS validation failed: unsupported auth_mode",
                account=self.account,
                container=self.container,
                auth_mode=self.auth_mode,
            )
            raise ValueError(
                f"Unsupported auth_mode: '{self.auth_mode}'. "
                f"Use 'key_vault', 'direct_key', 'service_principal', or 'managed_identity'."
            )

        ctx.info(
            "AzureADLS connection validated successfully",
            account=self.account,
            container=self.container,
            auth_mode=self.auth_mode,
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
        ctx = get_logging_context()

        with self._cache_lock:
            # Return cached key if available (double-check inside lock)
            if self._cached_key:
                ctx.debug(
                    "Using cached storage key",
                    account=self.account,
                    container=self.container,
                )
                return self._cached_key

            if self.auth_mode == "key_vault":
                ctx.debug(
                    "Fetching storage key from Key Vault",
                    account=self.account,
                    key_vault_name=self.key_vault_name,
                    secret_name=self.secret_name,
                    timeout=timeout,
                )

                try:
                    import concurrent.futures

                    from azure.identity import DefaultAzureCredential
                    from azure.keyvault.secrets import SecretClient
                except ImportError as e:
                    ctx.error(
                        "Key Vault authentication failed: missing azure libraries",
                        account=self.account,
                        error=str(e),
                    )
                    raise ImportError(
                        "Key Vault authentication requires 'azure-identity' and "
                        "'azure-keyvault-secrets'. Install with: pip install odibi[azure]"
                    ) from e

                # Create Key Vault client
                credential = DefaultAzureCredential()
                kv_uri = f"https://{self.key_vault_name}.vault.azure.net"
                client = SecretClient(vault_url=kv_uri, credential=credential)

                ctx.debug(
                    "Connecting to Key Vault",
                    key_vault_uri=kv_uri,
                    secret_name=self.secret_name,
                )

                # Fetch secret with timeout protection
                def _fetch():
                    secret = client.get_secret(self.secret_name)
                    return secret.value

                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(_fetch)
                    try:
                        self._cached_key = future.result(timeout=timeout)
                        logger.register_secret(self._cached_key)
                        ctx.info(
                            "Successfully fetched storage key from Key Vault",
                            account=self.account,
                            key_vault_name=self.key_vault_name,
                        )
                        return self._cached_key
                    except concurrent.futures.TimeoutError:
                        ctx.error(
                            "Key Vault fetch timed out",
                            account=self.account,
                            key_vault_name=self.key_vault_name,
                            secret_name=self.secret_name,
                            timeout=timeout,
                        )
                        raise TimeoutError(
                            f"Key Vault fetch timed out after {timeout}s for "
                            f"vault '{self.key_vault_name}', secret '{self.secret_name}'"
                        )

            elif self.auth_mode == "direct_key":
                ctx.debug(
                    "Using direct account key",
                    account=self.account,
                )
                return self.account_key

            elif self.auth_mode == "sas_token":
                # Return cached key (fetched from KV) if available, else sas_token arg
                ctx.debug(
                    "Using SAS token",
                    account=self.account,
                    from_cache=bool(self._cached_key),
                )
                return self._cached_key or self.sas_token

            # For other modes (SP, MI), we don't use an account key
            ctx.debug(
                "No storage key required for auth_mode",
                account=self.account,
                auth_mode=self.auth_mode,
            )
            return None

    def get_client_secret(self) -> Optional[str]:
        """Get Service Principal client secret (cached or literal)."""
        return self._cached_key or self.client_secret

    def pandas_storage_options(self) -> Dict[str, Any]:
        """Get storage options for pandas/fsspec.

        Returns:
            Dictionary with appropriate authentication parameters for fsspec
        """
        ctx = get_logging_context()
        ctx.debug(
            "Building pandas storage options",
            account=self.account,
            container=self.container,
            auth_mode=self.auth_mode,
        )

        base_options = {"account_name": self.account}

        if self.auth_mode in ["key_vault", "direct_key"]:
            return {**base_options, "account_key": self.get_storage_key()}

        elif self.auth_mode == "sas_token":
            # Use get_storage_key() which handles KV fallback for SAS
            return {**base_options, "sas_token": self.get_storage_key()}

        elif self.auth_mode == "service_principal":
            return {
                **base_options,
                "tenant_id": self.tenant_id,
                "client_id": self.client_id,
                "client_secret": self.get_client_secret(),
            }

        elif self.auth_mode == "managed_identity":
            # adlfs supports using DefaultAzureCredential implicitly if anon=False
            # and no other creds provided, assuming azure.identity is installed
            return {**base_options, "anon": False}

        return base_options

    def configure_spark(self, spark: "Any") -> None:
        """Configure Spark session with storage credentials.

        Args:
            spark: SparkSession instance
        """
        ctx = get_logging_context()
        ctx.info(
            "Configuring Spark for AzureADLS",
            account=self.account,
            container=self.container,
            auth_mode=self.auth_mode,
        )

        if self.auth_mode in ["key_vault", "direct_key"]:
            config_key = f"fs.azure.account.key.{self.account}.dfs.core.windows.net"
            spark.conf.set(config_key, self.get_storage_key())
            ctx.debug(
                "Set Spark config for account key",
                config_key=config_key,
            )

        elif self.auth_mode == "sas_token":
            # SAS Token Configuration
            # fs.azure.sas.token.provider.type -> FixedSASTokenProvider
            # fs.azure.sas.fixed.token -> <token>
            provider_key = f"fs.azure.account.auth.type.{self.account}.dfs.core.windows.net"
            spark.conf.set(provider_key, "SAS")

            sas_provider_key = (
                f"fs.azure.sas.token.provider.type.{self.account}.dfs.core.windows.net"
            )
            spark.conf.set(
                sas_provider_key, "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
            )

            sas_token = self.get_storage_key()

            sas_token_key = f"fs.azure.sas.fixed.token.{self.account}.dfs.core.windows.net"
            spark.conf.set(sas_token_key, sas_token)

            ctx.debug(
                "Set Spark config for SAS token",
                auth_type_key=provider_key,
                provider_key=sas_provider_key,
            )

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
            spark.conf.set(prefix, self.get_client_secret())

            prefix = f"fs.azure.account.oauth2.client.endpoint.{self.account}.dfs.core.windows.net"
            endpoint = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/token"
            spark.conf.set(prefix, endpoint)

            ctx.debug(
                "Set Spark config for service principal OAuth",
                tenant_id=self.tenant_id,
                client_id=self.client_id,
            )

        elif self.auth_mode == "managed_identity":
            prefix = f"fs.azure.account.auth.type.{self.account}.dfs.core.windows.net"
            spark.conf.set(prefix, "OAuth")

            prefix = f"fs.azure.account.oauth.provider.type.{self.account}.dfs.core.windows.net"
            spark.conf.set(prefix, "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider")

            ctx.debug(
                "Set Spark config for managed identity",
                account=self.account,
            )

        ctx.info(
            "Spark configuration complete",
            account=self.account,
            auth_mode=self.auth_mode,
        )

    def uri(self, path: str) -> str:
        """Build abfss:// URI for given path.

        Args:
            path: Relative path within container

        Returns:
            Full abfss:// URI

        Example:
            >>> conn = AzureADLS(
            ...     account="myaccount", container="data",
            ...     auth_mode="direct_key", account_key="key123"
            ... )
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
        ctx = get_logging_context()
        full_uri = self.uri(relative_path)

        ctx.debug(
            "Resolved ADLS path",
            account=self.account,
            container=self.container,
            relative_path=relative_path,
            full_uri=full_uri,
        )

        return full_uri
