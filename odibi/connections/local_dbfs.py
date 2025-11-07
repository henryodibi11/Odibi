"""Local DBFS mock for testing Databricks pipelines locally."""

from pathlib import Path
from typing import Union
from .base import BaseConnection


class LocalDBFS(BaseConnection):
    """Mock DBFS connection for local development.

    Maps dbfs:/ paths to local filesystem for testing.
    Useful for developing Databricks pipelines locally.
    """

    def __init__(self, root: Union[str, Path] = ".dbfs"):
        """Initialize local DBFS mock.

        Args:
            root: Local directory to use as DBFS root (default: .dbfs)
        """
        self.root = Path(root).resolve()

    def resolve(self, path: str) -> str:
        """Resolve dbfs:/ path to local filesystem path.

        Args:
            path: DBFS path (e.g., 'dbfs:/FileStore/data.csv')

        Returns:
            Absolute local filesystem path

        Example:
            >>> conn = LocalDBFS(root="/tmp/dbfs")
            >>> conn.resolve("dbfs:/FileStore/data.csv")
            '/tmp/dbfs/FileStore/data.csv'
        """
        # Remove dbfs:/ prefix
        clean_path = path.replace("dbfs:/", "").lstrip("/")

        # Join with root
        local_path = self.root / clean_path

        return str(local_path)

    def ensure_dir(self, path: str) -> None:
        """Create parent directories for given path.

        Args:
            path: DBFS path
        """
        local_path = Path(self.resolve(path))
        local_path.parent.mkdir(parents=True, exist_ok=True)

    def get_path(self, relative_path: str) -> str:
        """Get local filesystem path for DBFS path."""
        return self.resolve(relative_path)

    def validate(self) -> None:
        """Validate local DBFS configuration."""
        pass  # No validation needed for local mock
