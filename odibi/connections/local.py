"""Local filesystem connection."""

import os
from pathlib import Path
from odibi.connections.base import BaseConnection


class LocalConnection(BaseConnection):
    """Connection to local filesystem or URI-based paths (e.g. dbfs:/, file://)."""

    def __init__(self, base_path: str = "./data"):
        """Initialize local connection.

        Args:
            base_path: Base directory for all paths (can be local path or URI)
        """
        self.base_path_str = base_path
        self.is_uri = "://" in base_path or ":/" in base_path

        if not self.is_uri:
            self.base_path = Path(base_path)
        else:
            self.base_path = None  # Not used for URIs

    def get_path(self, relative_path: str) -> str:
        """Get full path for a relative path.

        Args:
            relative_path: Relative path from base

        Returns:
            Full absolute path or URI
        """
        if self.is_uri:
            # Use os.path for simple string joining, handling slashes manually for consistency
            # Strip leading slash from relative to avoid root replacement
            clean_rel = relative_path.lstrip("/").lstrip("\\")
            # Handle cases where base_path might not have trailing slash
            if self.base_path_str.endswith("/") or self.base_path_str.endswith("\\"):
                return f"{self.base_path_str}{clean_rel}"
            else:
                # Use forward slash for URIs
                return f"{self.base_path_str}/{clean_rel}"
        else:
            # Standard local path logic
            full_path = self.base_path / relative_path
            return str(full_path.absolute())

    def validate(self) -> None:
        """Validate that base path exists or can be created.

        Raises:
            ConnectionError: If validation fails
        """
        if self.is_uri:
            # Cannot validate/create URIs with local os module
            # Assume valid or handled by engine
            pass
        else:
            # Create base directory if it doesn't exist
            self.base_path.mkdir(parents=True, exist_ok=True)
