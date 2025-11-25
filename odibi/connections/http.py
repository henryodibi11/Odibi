"""HTTP Connection implementation."""

from typing import Any, Dict, Optional
from urllib.parse import urljoin

from odibi.connections.base import BaseConnection


class HttpConnection(BaseConnection):
    """Connection to HTTP/HTTPS APIs."""

    def __init__(
        self,
        base_url: str,
        headers: Optional[Dict[str, str]] = None,
        auth: Optional[Dict[str, str]] = None,
        validate: bool = True,
    ):
        """Initialize HTTP connection.

        Args:
            base_url: Base URL for API
            headers: Default headers
            auth: Authentication details
            validate: Whether to validate connection (ping)
        """
        self.base_url = base_url.rstrip("/") + "/"
        self.headers = headers or {}

        if auth:
            if "token" in auth:
                self.headers["Authorization"] = f"Bearer {auth['token']}"
            elif "username" in auth and "password" in auth:
                import base64

                creds = f"{auth['username']}:{auth['password']}"
                b64_creds = base64.b64encode(creds.encode()).decode()
                self.headers["Authorization"] = f"Basic {b64_creds}"
            elif "api_key" in auth:
                # Common pattern: X-API-Key header or similar
                header_name = auth.get("header_name", "X-API-Key")
                self.headers[header_name] = auth["api_key"]

        if validate:
            self.validate()

    def validate(self) -> None:
        """Validate connection configuration.

        Raises:
            ValueError: If validation fails
        """
        if not self.base_url:
            raise ValueError("HTTP connection requires 'base_url'")

    def get_path(self, path: str) -> str:
        """Resolve endpoint path.

        Args:
            path: API endpoint (e.g., 'v1/users')

        Returns:
            Full URL
        """
        if path.startswith("http://") or path.startswith("https://"):
            return path

        # urljoin can be tricky if base_url doesn't end with /
        return urljoin(self.base_url, path.lstrip("/"))

    def pandas_storage_options(self) -> Dict[str, Any]:
        """Get storage options for Pandas/fsspec.

        Returns:
            Dictionary with headers
        """
        # For HTTP(S) in Pandas (urllib), storage_options ARE the headers.
        return self.headers
