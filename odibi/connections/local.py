"""Local filesystem connection."""

from pathlib import Path
from odibi.connections.base import BaseConnection


class LocalConnection(BaseConnection):
    """Connection to local filesystem."""

    def __init__(self, base_path: str = "./data"):
        """Initialize local connection.

        Args:
            base_path: Base directory for all paths
        """
        self.base_path = Path(base_path)

    def get_path(self, relative_path: str) -> str:
        """Get full path for a relative path.

        Args:
            relative_path: Relative path from base

        Returns:
            Full absolute path
        """
        full_path = self.base_path / relative_path
        return str(full_path.absolute())

    def validate(self) -> None:
        """Validate that base path exists or can be created.

        Raises:
            ConnectionError: If validation fails
        """
        # Create base directory if it doesn't exist
        self.base_path.mkdir(parents=True, exist_ok=True)
