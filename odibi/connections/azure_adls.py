"""Azure Data Lake Storage Gen2 connection (Phase 2A: Multi-mode authentication)."""

import os
import posixpath
import threading
import warnings
from datetime import datetime
from typing import Any, Dict, List, Optional

from odibi.discovery.types import (
    CatalogSummary,
    Column,
    DatasetRef,
    FreshnessResult,
    PartitionInfo,
    PreviewResult,
    Schema,
    TableProfile,
)
from odibi.discovery.utils import detect_file_format, detect_partitions, infer_format_from_path
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

        self._cached_storage_key: Optional[str] = None
        self._cached_client_secret: Optional[str] = None
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
            if self._cached_storage_key:
                ctx.debug(
                    "Using cached storage key",
                    account=self.account,
                    container=self.container,
                )
                return self._cached_storage_key

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
                        self._cached_storage_key = future.result(timeout=timeout)
                        logger.register_secret(self._cached_storage_key)
                        ctx.info(
                            "Successfully fetched storage key from Key Vault",
                            account=self.account,
                            key_vault_name=self.key_vault_name,
                        )
                        return self._cached_storage_key
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
                    from_cache=bool(self._cached_storage_key),
                )
                return self._cached_storage_key or self.sas_token

            # For other modes (SP, MI), we don't use an account key
            ctx.debug(
                "No storage key required for auth_mode",
                account=self.account,
                auth_mode=self.auth_mode,
            )
            return None

    def get_client_secret(self) -> Optional[str]:
        """Get Service Principal client secret (cached or literal).

        Returns the cached secret if available (loaded from Azure Key Vault or
        environment variable during initialization), otherwise returns the literal
        client_secret value from the configuration.

        Returns:
            Client secret string, or None if not using Service Principal authentication
        """
        return self._cached_client_secret or self.client_secret

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
            # Strip leading '?' if present for fsspec compatibility
            sas_token = self.get_storage_key()
            if sas_token and sas_token.startswith("?"):
                sas_token = sas_token[1:]
            return {**base_options, "sas_token": sas_token}

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

            # Strip leading '?' if present - FixedSASTokenProvider expects token without it
            if sas_token and sas_token.startswith("?"):
                sas_token = sas_token[1:]
                ctx.debug("Stripped leading '?' from SAS token for Spark configuration")

            sas_token_key = f"fs.azure.sas.fixed.token.{self.account}.dfs.core.windows.net"
            spark.conf.set(sas_token_key, sas_token)

            # Disable ACL/namespace checks that SAS tokens don't support
            # The getAccessControl operation fails with SAS tokens on ADLS Gen2
            # These settings tell the driver to skip those checks
            spark.conf.set(
                f"fs.azure.account.hns.enabled.{self.account}.dfs.core.windows.net", "false"
            )
            spark.conf.set("fs.azure.skip.user.group.metadata.during.initialization", "true")

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

    def _get_fs(self):
        """Get fsspec filesystem instance for this connection."""
        try:
            import adlfs
        except ImportError as e:
            raise ImportError(
                "Azure ADLS discovery requires 'adlfs'. Install with: pip install odibi[azure]"
            ) from e

        storage_opts = self.pandas_storage_options()
        fs = adlfs.AzureBlobFileSystem(**storage_opts)
        return fs

    def list_files(self, path: str = "", pattern: str = "*", limit: int = 1000) -> List[Dict]:
        """List files in ADLS path.

        Args:
            path: Relative path within container (default: path_prefix)
            pattern: Glob pattern for filtering (default: "*")
            limit: Maximum number of files to return

        Returns:
            List of dicts with keys: name, path, size, modified, format
        """
        ctx = get_logging_context()
        ctx.debug(
            "Listing ADLS files",
            account=self.account,
            container=self.container,
            path=path,
            pattern=pattern,
            limit=limit,
        )

        try:
            fs = self._get_fs()
            full_path = f"{self.container}/{self.path_prefix}/{path}".strip("/")

            # Use glob for pattern matching
            import fnmatch

            all_files = []
            for entry in fs.ls(full_path, detail=True):
                if entry["type"] == "file":
                    file_name = entry["name"].split("/")[-1]
                    if fnmatch.fnmatch(file_name, pattern):
                        all_files.append(
                            {
                                "name": file_name,
                                "path": entry["name"],
                                "size": entry.get("size", 0),
                                "modified": entry.get("last_modified"),
                                "format": infer_format_from_path(file_name),
                            }
                        )
                        if len(all_files) >= limit:
                            break

            ctx.info("Listed ADLS files", count=len(all_files))
            return all_files

        except Exception as e:
            ctx.warning("Failed to list ADLS files", error=str(e), path=path)
            return []

    def list_folders(self, path: str = "", limit: int = 100) -> List[str]:
        """List folders in ADLS path.

        Args:
            path: Relative path within container
            limit: Maximum number of folders to return

        Returns:
            List of folder paths
        """
        ctx = get_logging_context()
        ctx.debug(
            "Listing ADLS folders",
            account=self.account,
            container=self.container,
            path=path,
            limit=limit,
        )

        try:
            fs = self._get_fs()
            full_path = f"{self.container}/{self.path_prefix}/{path}".strip("/")

            folders = []
            for entry in fs.ls(full_path, detail=True):
                if entry["type"] == "directory":
                    folders.append(entry["name"])
                    if len(folders) >= limit:
                        break

            ctx.info("Listed ADLS folders", count=len(folders))
            return folders

        except Exception as e:
            ctx.warning("Failed to list ADLS folders", error=str(e), path=path)
            return []

    def discover_catalog(
        self,
        include_schema: bool = False,
        include_stats: bool = False,
        limit: int = 200,
        recursive: bool = True,
        path: str = "",
        pattern: str = "",
    ) -> Dict[str, Any]:
        """Discover datasets in ADLS container.

        Args:
            include_schema: Sample files and infer schema
            include_stats: Include row counts and stats
            limit: Maximum datasets to return
            recursive: Recursively scan all subfolders (default: True)
            path: Scope search to specific subfolder in container
            pattern: Filter by pattern (e.g. "*.csv", "sales_*")

        Returns:
            CatalogSummary dict
        """
        ctx = get_logging_context()
        ctx.info(
            "Discovering ADLS catalog",
            account=self.account,
            container=self.container,
            include_schema=include_schema,
            include_stats=include_stats,
        )

        try:
            fs = self._get_fs()
            base_path = f"{self.container}/{self.path_prefix}".strip("/")

            # Use path parameter to scope search
            if path:
                base_path = f"{base_path}/{path}".strip("/")

            folders = []
            files = []
            formats = {}

            # Compile pattern for filtering if provided
            import fnmatch

            has_pattern = bool(pattern)

            # Use walk for recursive or ls for shallow
            if recursive:
                entries_to_scan = []
                for root, dirs, file_names in fs.walk(base_path, maxdepth=None, detail=True):
                    # Add directories
                    for dir_name, dir_info in dirs.items():
                        entries_to_scan.append(
                            {"name": dir_info["name"], "type": "directory", **dir_info}
                        )
                    # Add files
                    for file_name, file_info in file_names.items():
                        entries_to_scan.append(
                            {"name": file_info["name"], "type": "file", **file_info}
                        )
            else:
                entries_to_scan = fs.ls(base_path, detail=True)

            for entry in entries_to_scan:
                if len(folders) + len(files) >= limit:
                    break

                entry_name = entry["name"].split("/")[-1]

                # Apply pattern filter if specified
                if has_pattern and not fnmatch.fnmatch(entry_name, pattern):
                    continue

                if entry["type"] == "directory":
                    folder_name = entry_name
                    file_format = detect_file_format(entry["name"], fs)

                    folders.append(
                        DatasetRef(
                            name=folder_name,
                            kind="folder",
                            path=entry["name"],
                            format=file_format,
                            size_bytes=entry.get("size", 0),
                        )
                    )

                    if file_format:
                        formats[file_format] = formats.get(file_format, 0) + 1

                elif entry["type"] == "file":
                    file_name = entry_name
                    file_format = infer_format_from_path(file_name)

                    files.append(
                        DatasetRef(
                            name=file_name,
                            kind="file",
                            path=entry["name"],
                            format=file_format,
                            size_bytes=entry.get("size", 0),
                            modified_at=entry.get("last_modified"),
                        )
                    )

                    if file_format:
                        formats[file_format] = formats.get(file_format, 0) + 1

            summary = CatalogSummary(
                connection_name=f"{self.account}/{self.container}",
                connection_type="azure_adls",
                folders=[f.model_dump() for f in folders],
                files=[f.model_dump() for f in files],
                total_datasets=len(folders) + len(files),
                formats=formats,
                next_step="Use get_schema() to inspect individual datasets",
            )

            ctx.info(
                "ADLS catalog discovery complete",
                total_datasets=summary.total_datasets,
                folders=len(folders),
                files=len(files),
            )

            return summary.model_dump()

        except Exception as e:
            ctx.error("Failed to discover ADLS catalog", error=str(e))
            return CatalogSummary(
                connection_name=f"{self.account}/{self.container}",
                connection_type="azure_adls",
                total_datasets=0,
                next_step=f"Error: {str(e)}",
            ).model_dump()

    def get_schema(self, dataset: str) -> Dict[str, Any]:
        """Get schema for a dataset.

        Args:
            dataset: Relative path to file or folder

        Returns:
            Schema dict with columns
        """
        ctx = get_logging_context()
        ctx.info("Getting ADLS schema", dataset=dataset)

        try:
            import pandas as pd

            full_path = self.uri(dataset)
            storage_opts = self.pandas_storage_options()

            # Detect format
            file_format = detect_file_format(dataset, self._get_fs())

            if file_format == "parquet" or file_format == "delta":
                df = pd.read_parquet(full_path, storage_options=storage_opts).head(0)
            elif file_format == "csv":
                df = pd.read_csv(full_path, storage_options=storage_opts, nrows=1000).head(0)
            elif file_format == "json":
                df = pd.read_json(
                    full_path, storage_options=storage_opts, lines=True, nrows=1000
                ).head(0)
            else:
                ctx.warning("Unsupported format for schema inference", format=file_format)
                return Schema(
                    dataset=DatasetRef(name=dataset, kind="file", format=file_format),
                    columns=[],
                ).model_dump()

            columns = [Column(name=col, dtype=str(dtype)) for col, dtype in df.dtypes.items()]

            schema = Schema(
                dataset=DatasetRef(name=dataset, kind="file", format=file_format),
                columns=columns,
            )

            ctx.info("Schema retrieved", column_count=len(columns))
            return schema.model_dump()

        except Exception as e:
            ctx.error("Failed to get schema", dataset=dataset, error=str(e))
            return Schema(
                dataset=DatasetRef(name=dataset, kind="file"),
                columns=[],
            ).model_dump()

    def profile(
        self, dataset: str, sample_rows: int = 1000, columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Profile a dataset with statistics.

        Args:
            dataset: Relative path to file or folder
            sample_rows: Number of rows to sample (max 10000)
            columns: Specific columns to profile (None = all)

        Returns:
            TableProfile dict with stats
        """
        ctx = get_logging_context()
        ctx.info("Profiling ADLS dataset", dataset=dataset, sample_rows=sample_rows)

        sample_rows = min(sample_rows, 10000)  # Cap at 10k

        try:
            import pandas as pd

            full_path = self.uri(dataset)
            storage_opts = self.pandas_storage_options()
            file_format = detect_file_format(dataset, self._get_fs())

            # Read sample
            if file_format == "parquet" or file_format == "delta":
                df = pd.read_parquet(full_path, storage_options=storage_opts).head(sample_rows)
            elif file_format == "csv":
                df = pd.read_csv(full_path, storage_options=storage_opts, nrows=sample_rows)
            elif file_format == "json":
                df = pd.read_json(
                    full_path, storage_options=storage_opts, lines=True, nrows=sample_rows
                )
            else:
                ctx.warning("Unsupported format for profiling", format=file_format)
                return TableProfile(
                    dataset=DatasetRef(name=dataset, kind="file", format=file_format),
                    rows_sampled=0,
                    columns=[],
                ).model_dump()

            # Profile columns
            profile_cols = columns or df.columns.tolist()
            profiled = []

            for col in profile_cols:
                if col not in df.columns:
                    continue

                null_count = int(df[col].isnull().sum())
                null_pct = null_count / len(df) if len(df) > 0 else 0
                distinct_count = int(df[col].nunique())

                # Cardinality heuristic
                if distinct_count == len(df):
                    cardinality = "unique"
                elif distinct_count > len(df) * 0.9:
                    cardinality = "high"
                elif distinct_count > len(df) * 0.1:
                    cardinality = "medium"
                else:
                    cardinality = "low"

                sample_values = df[col].dropna().head(5).tolist()

                profiled.append(
                    Column(
                        name=col,
                        dtype=str(df[col].dtype),
                        null_count=null_count,
                        null_pct=round(null_pct, 3),
                        cardinality=cardinality,
                        distinct_count=distinct_count,
                        sample_values=sample_values,
                    )
                )

            # Detect candidate keys (unique non-null columns)
            candidate_keys = [
                c.name for c in profiled if c.cardinality == "unique" and c.null_count == 0
            ]

            # Detect candidate watermarks (datetime columns)
            candidate_watermarks = [
                c.name
                for c in profiled
                if "datetime" in c.dtype.lower() or "date" in c.dtype.lower()
            ]

            completeness = 1.0 - (df.isnull().sum().sum() / (len(df) * len(df.columns)))

            profile = TableProfile(
                dataset=DatasetRef(name=dataset, kind="file", format=file_format),
                rows_sampled=len(df),
                columns=profiled,
                candidate_keys=candidate_keys,
                candidate_watermarks=candidate_watermarks,
                completeness=round(completeness, 3),
            )

            ctx.info("Profiling complete", rows_sampled=len(df), columns=len(profiled))
            return profile.model_dump()

        except Exception as e:
            ctx.error("Failed to profile dataset", dataset=dataset, error=str(e))
            return TableProfile(
                dataset=DatasetRef(name=dataset, kind="file"),
                rows_sampled=0,
                columns=[],
            ).model_dump()

    def preview(
        self, dataset: str, rows: int = 5, columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Preview sample rows from an ADLS dataset."""
        ctx = get_logging_context()
        ctx.info("Previewing ADLS dataset", dataset=dataset, rows=rows)

        max_rows = min(rows, 100)  # Cap at 100

        try:
            import pandas as pd

            full_path = self.uri(dataset)
            storage_opts = self.pandas_storage_options()
            file_format = detect_file_format(dataset, self._get_fs())

            if file_format == "parquet" or file_format == "delta":
                df = pd.read_parquet(full_path, storage_options=storage_opts).head(max_rows)
            elif file_format == "csv":
                df = pd.read_csv(full_path, storage_options=storage_opts, nrows=max_rows)
            elif file_format == "json":
                df = pd.read_json(
                    full_path, storage_options=storage_opts, lines=True, nrows=max_rows
                )
            else:
                ctx.warning("Unsupported format for preview", format=file_format)
                return PreviewResult(
                    dataset=DatasetRef(name=dataset, kind="file", format=file_format),
                ).model_dump()

            if columns:
                df = df[[c for c in columns if c in df.columns]]

            result = PreviewResult(
                dataset=DatasetRef(name=dataset, kind="file", format=file_format),
                columns=df.columns.tolist(),
                rows=df.head(max_rows).to_dict(orient="records"),
                truncated=len(df) >= max_rows,
                format=file_format,
            )

            ctx.info("Preview complete", rows_returned=len(result.rows))
            return result.model_dump()

        except Exception as e:
            ctx.error("Failed to preview dataset", dataset=dataset, error=str(e))
            return PreviewResult(
                dataset=DatasetRef(name=dataset, kind="file"),
            ).model_dump()

    def detect_partitions(self, path: str = "") -> Dict[str, Any]:
        """Detect partition structure in ADLS path.

        Args:
            path: Relative path to scan (default: path_prefix)

        Returns:
            PartitionInfo dict
        """
        ctx = get_logging_context()
        ctx.info("Detecting ADLS partitions", path=path)

        try:
            fs = self._get_fs()
            full_path = f"{self.container}/{self.path_prefix}/{path}".strip("/")

            # List all files recursively
            all_paths = []
            try:
                all_paths = [
                    entry["name"]
                    for entry in fs.ls(full_path, detail=True, recursive=True)
                    if entry["type"] == "file"
                ][:100]  # Sample first 100
            except Exception:
                pass

            partition_info = detect_partitions(all_paths)

            result = PartitionInfo(
                root=full_path,
                keys=partition_info.get("keys", []),
                example_values=partition_info.get("example_values", {}),
                format=partition_info.get("format", "none"),
                partition_count=len(all_paths),
            )

            ctx.info("Partition detection complete", keys=result.keys)
            return result.model_dump()

        except Exception as e:
            ctx.error("Failed to detect partitions", path=path, error=str(e))
            return PartitionInfo(root=path, keys=[], example_values={}).model_dump()

    def get_freshness(self, dataset: str) -> Dict[str, Any]:
        """Get data freshness for a dataset.

        Args:
            dataset: Relative path to file or folder

        Returns:
            FreshnessResult dict
        """
        ctx = get_logging_context()
        ctx.info("Checking ADLS freshness", dataset=dataset)

        try:
            fs = self._get_fs()
            full_path = f"{self.container}/{self.path_prefix}/{dataset}".strip("/")

            # Get file/folder metadata
            info = fs.info(full_path)
            last_modified = info.get("last_modified")

            if last_modified:
                if isinstance(last_modified, str):
                    last_modified = datetime.fromisoformat(last_modified.replace("Z", "+00:00"))

                age_hours = (datetime.utcnow() - last_modified).total_seconds() / 3600

                result = FreshnessResult(
                    dataset=DatasetRef(name=dataset, kind="file"),
                    last_updated=last_modified,
                    source="metadata",
                    age_hours=round(age_hours, 2),
                )

                ctx.info("Freshness check complete", age_hours=age_hours)
                return result.model_dump()

        except Exception as e:
            ctx.error("Failed to check freshness", dataset=dataset, error=str(e))

        return FreshnessResult(
            dataset=DatasetRef(name=dataset, kind="file"), source="metadata"
        ).model_dump()
