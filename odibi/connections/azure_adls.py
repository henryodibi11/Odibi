"""Azure Data Lake Storage Gen2 connection (Phase 1: Path resolution only)."""

import posixpath
from .base import BaseConnection


class AzureADLS(BaseConnection):
    """Azure Data Lake Storage Gen2 connection.

    Phase 1: Path/URI resolution only (no network I/O)
    Phase 3: Read/write implementation with azure-storage-blob
    """

    def __init__(
        self,
        account: str,
        container: str,
        path_prefix: str = "",
        auth_mode: str = "managed_identity",
        **kwargs,
    ):
        """Initialize ADLS connection.

        Args:
            account: Storage account name (e.g., 'mystorageaccount')
            container: Container/filesystem name
            path_prefix: Optional prefix for all paths
            auth_mode: Authentication mode (managed_identity, cli, sas)
        """
        self.account = account
        self.container = container
        self.path_prefix = path_prefix.strip("/")
        self.auth_mode = auth_mode

    def uri(self, path: str) -> str:
        """Build abfss:// URI for given path.

        Args:
            path: Relative path within container

        Returns:
            Full abfss:// URI

        Example:
            >>> conn = AzureADLS(account="myaccount", container="data")
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

    def validate(self) -> None:
        """Validate ADLS connection configuration."""
        if not self.account:
            raise ValueError("ADLS connection requires 'account'")
        if not self.container:
            raise ValueError("ADLS connection requires 'container'")
