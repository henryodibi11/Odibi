"""HTTP Connection implementation."""

import re
from typing import Any, Dict, Optional
from urllib.parse import urljoin

from odibi.connections.base import BaseConnection
from odibi.utils.logging import logger


_HTTP_HEADER_NAME_RE = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")


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
                value = f"Bearer {auth['token']}"
                self._set_auth_header("Authorization", value, auth["token"])
            elif "username" in auth and "password" in auth:
                import base64

                creds = f"{auth['username']}:{auth['password']}"
                b64_creds = base64.b64encode(creds.encode()).decode()
                value = f"Basic {b64_creds}"
                self._set_auth_header("Authorization", value, auth["password"], creds)
            elif "api_key" in auth:
                # Common pattern: X-API-Key header or similar
                header_name = auth.get("header_name", "X-API-Key")
                self._set_auth_header(header_name, auth["api_key"], auth["api_key"])

        if validate:
            self.validate()

    def _set_auth_header(self, header_name: str, value: str, *secrets: str) -> None:
        """Validate, register, and install one generated authentication header."""
        for secret in (*secrets, value):
            logger.register_secret(secret)

        if not isinstance(header_name, str) or not _HTTP_HEADER_NAME_RE.fullmatch(header_name):
            raise ValueError("HTTP authentication header name is invalid")
        if not isinstance(value, str) or any(ord(char) < 32 or ord(char) == 127 for char in value):
            raise ValueError("HTTP authentication header value contains a control character")
        self.headers[header_name] = value

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
