"""Local filesystem connection."""

from pathlib import Path

from odibi.connections.base import BaseConnection
from odibi.utils.logging_context import get_logging_context


class LocalConnection(BaseConnection):
    """Connection to local filesystem or URI-based paths (e.g. dbfs:/, file://)."""

    def __init__(self, base_path: str = "./data"):
        """Initialize local connection.

        Args:
            base_path: Base directory for all paths (can be local path or URI)
        """
        ctx = get_logging_context()
        ctx.log_connection(
            connection_type="local",
            connection_name="LocalConnection",
            action="init",
            base_path=base_path,
        )

        self.base_path_str = base_path
        self.is_uri = "://" in base_path or ":/" in base_path

        if not self.is_uri:
            self.base_path = Path(base_path)
            ctx.debug(
                "LocalConnection initialized with filesystem path",
                base_path=base_path,
                is_uri=False,
            )
        else:
            self.base_path = None  # Not used for URIs
            ctx.debug(
                "LocalConnection initialized with URI path",
                base_path=base_path,
                is_uri=True,
            )

    def get_path(self, relative_path: str) -> str:
        """Get full path for a relative path.

        Args:
            relative_path: Relative path from base

        Returns:
            Full absolute path or URI
        """
        ctx = get_logging_context()

        if self.is_uri:
            # Use os.path for simple string joining, handling slashes manually for consistency
            # Strip leading slash from relative to avoid root replacement
            clean_rel = relative_path.lstrip("/").lstrip("\\")
            # Handle cases where base_path might not have trailing slash
            if self.base_path_str.endswith("/") or self.base_path_str.endswith("\\"):
                full_path = f"{self.base_path_str}{clean_rel}"
            else:
                # Use forward slash for URIs
                full_path = f"{self.base_path_str}/{clean_rel}"

            ctx.debug(
                "Resolved URI path",
                relative_path=relative_path,
                full_path=full_path,
            )
            return full_path
        else:
            # Standard local path logic
            full_path = self.base_path / relative_path
            resolved = str(full_path.absolute())

            ctx.debug(
                "Resolved local path",
                relative_path=relative_path,
                full_path=resolved,
            )
            return resolved

    def validate(self) -> None:
        """Validate that base path exists or can be created.

        Raises:
            ConnectionError: If validation fails
        """
        ctx = get_logging_context()
        ctx.debug(
            "Validating LocalConnection",
            base_path=self.base_path_str,
            is_uri=self.is_uri,
        )

        if self.is_uri:
            # Cannot validate/create URIs with local os module
            # Assume valid or handled by engine
            ctx.debug(
                "Skipping URI validation (handled by engine)",
                base_path=self.base_path_str,
            )
        else:
            # Create base directory if it doesn't exist
            try:
                self.base_path.mkdir(parents=True, exist_ok=True)
                ctx.info(
                    "LocalConnection validated successfully",
                    base_path=str(self.base_path.absolute()),
                    created=not self.base_path.exists(),
                )
            except Exception as e:
                ctx.error(
                    "LocalConnection validation failed",
                    base_path=self.base_path_str,
                    error=str(e),
                )
                raise
