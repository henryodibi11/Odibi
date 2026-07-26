"""HTTP app wrapper for Databricks App deployment.

Exposes the Odibi MCP server over HTTP/SSE transport
for use by Genie Code and other Databricks agents.

Application construction is intentionally fail-fast. Import or FastMCP startup
errors propagate to uvicorn and the deployment supervisor instead of creating a
live fallback endpoint that could disclose server internals.

Usage:
    uvicorn odibi_mcp.databricks_app:http_app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import ipaddress
import os
import re

from starlette.middleware.cors import CORSMiddleware
from starlette.responses import PlainTextResponse

from odibi_mcp.mcp_server import mcp

_CORS_ENV = "ODIBI_MCP_CORS_ORIGINS"
_CORS_CONFIG_ERROR = "Invalid ODIBI_MCP_CORS_ORIGINS configuration"
_CORS_DENIAL = "Cross-origin request denied"
_MAX_CORS_CONFIG_CHARS = 4096
_MAX_CORS_ORIGINS = 16
_MAX_ORIGIN_CHARS = 512
_DNS_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")
_CORS_REQUEST_HEADERS = [
    "Accept",
    "Authorization",
    "Content-Type",
    "MCP-Protocol-Version",
]


def _normalize_origin(value: str) -> str:
    """Return one canonical browser origin or raise without echoing input."""
    if not value or len(value) > _MAX_ORIGIN_CHARS:
        raise ValueError(_CORS_CONFIG_ERROR)
    try:
        value.encode("ascii")
    except UnicodeEncodeError:
        raise ValueError(_CORS_CONFIG_ERROR) from None
    if any(character.isspace() or ord(character) < 32 for character in value):
        raise ValueError(_CORS_CONFIG_ERROR)
    if "%" in value or "\\" in value:
        raise ValueError(_CORS_CONFIG_ERROR)

    raw_scheme, separator, authority = value.partition("://")
    scheme = raw_scheme.lower()
    if (
        not separator
        or scheme not in {"http", "https"}
        or not authority
        or any(character in authority for character in "/?#@")
    ):
        raise ValueError(_CORS_CONFIG_ERROR)

    bracketed = authority.startswith("[")
    port_text = None
    if bracketed:
        closing_bracket = authority.find("]")
        if closing_bracket < 0:
            raise ValueError(_CORS_CONFIG_ERROR)
        host = authority[1:closing_bracket]
        suffix = authority[closing_bracket + 1 :]
        if suffix:
            if not suffix.startswith(":") or not suffix[1:].isdigit():
                raise ValueError(_CORS_CONFIG_ERROR)
            port_text = suffix[1:]
    else:
        if "[" in authority or "]" in authority or authority.count(":") > 1:
            raise ValueError(_CORS_CONFIG_ERROR)
        if ":" in authority:
            host, port_text = authority.rsplit(":", 1)
            if not port_text.isdigit():
                raise ValueError(_CORS_CONFIG_ERROR)
        else:
            host = authority

    if not host:
        raise ValueError(_CORS_CONFIG_ERROR)
    port = int(port_text) if port_text is not None else None
    host = host.lower()
    is_loopback = host == "localhost"
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        if bracketed or host.endswith(".") or len(host) > 253:
            raise ValueError(_CORS_CONFIG_ERROR) from None
        labels = host.split(".")
        if not labels or any(not _DNS_LABEL.fullmatch(label) for label in labels):
            raise ValueError(_CORS_CONFIG_ERROR) from None
        normalized_host = host
    else:
        if bracketed != (address.version == 6):
            raise ValueError(_CORS_CONFIG_ERROR)
        is_loopback = address.is_loopback
        normalized_host = f"[{address.compressed}]" if address.version == 6 else address.compressed

    if scheme == "http" and not is_loopback:
        raise ValueError(_CORS_CONFIG_ERROR)
    if port is not None and not 1 <= port <= 65535:
        raise ValueError(_CORS_CONFIG_ERROR)

    default_port = 80 if scheme == "http" else 443
    port_suffix = "" if port is None or port == default_port else f":{port}"
    return f"{scheme}://{normalized_host}{port_suffix}"


def _configured_cors_origins(raw_value: str | None) -> tuple[str, ...]:
    """Parse the bounded operator-owned exact origin allowlist."""
    if raw_value is None:
        return ()
    if not raw_value or len(raw_value) > _MAX_CORS_CONFIG_CHARS:
        raise RuntimeError(_CORS_CONFIG_ERROR)

    values = raw_value.split(",")
    if not values or len(values) > _MAX_CORS_ORIGINS or any(not value for value in values):
        raise RuntimeError(_CORS_CONFIG_ERROR)

    try:
        origins = tuple(_normalize_origin(value) for value in values)
    except ValueError:
        raise RuntimeError(_CORS_CONFIG_ERROR) from None
    if len(set(origins)) != len(origins):
        raise RuntimeError(_CORS_CONFIG_ERROR)
    return origins


class _OriginPolicyMiddleware:
    """Reject explicit untrusted browser origins before FastMCP dispatch."""

    def __init__(self, app, allowed_origins: tuple[str, ...]) -> None:
        self.app = app
        self.allowed_origins = frozenset(allowed_origins)

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] == "http":
            origin_headers = [
                value for name, value in scope.get("headers", ()) if name.lower() == b"origin"
            ]
            if origin_headers:
                try:
                    if len(origin_headers) != 1:
                        raise ValueError(_CORS_CONFIG_ERROR)
                    origin = _normalize_origin(origin_headers[0].decode("ascii"))
                except (UnicodeDecodeError, ValueError):
                    origin = None
                if origin not in self.allowed_origins:
                    response = PlainTextResponse(_CORS_DENIAL, status_code=403)
                    await response(scope, receive, send)
                    return
                preflight_headers = [
                    value
                    for name, value in scope.get("headers", ())
                    if name.lower() == b"access-control-request-method"
                ]
                is_preflight = scope["method"] == "OPTIONS" and len(preflight_headers) == 1
                if scope["method"] != "POST" and not is_preflight:
                    response = PlainTextResponse(_CORS_DENIAL, status_code=403)
                    await response(scope, receive, send)
                    return
                scope = dict(scope)
                scope["headers"] = [
                    (name, origin.encode("ascii")) if name.lower() == b"origin" else (name, value)
                    for name, value in scope.get("headers", ())
                ]
        await self.app(scope, receive, send)


def create_http_app():
    """Build the fail-closed stateless FastMCP ASGI application."""
    allowed_origins = _configured_cors_origins(os.environ.get(_CORS_ENV))
    app = mcp.http_app(stateless_http=True)
    if allowed_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=list(allowed_origins),
            allow_credentials=False,
            allow_methods=["POST"],
            allow_headers=_CORS_REQUEST_HEADERS,
            expose_headers=[],
        )
    app.add_middleware(_OriginPolicyMiddleware, allowed_origins=allowed_origins)
    return app


http_app = create_http_app()


def main() -> None:
    """Serve the HTTP app with uvicorn.

    Entry point for the `odibi-mcp-http` console script and
    `python -m odibi_mcp.databricks_app`. Host/port via ODIBI_MCP_HOST / PORT.
    NOTE: the Databricks Apps deploy uses app.yaml (uvicorn) directly; this is
    for local/manual runs.
    """
    import uvicorn

    uvicorn.run(
        http_app,
        host=os.environ.get("ODIBI_MCP_HOST", "0.0.0.0"),
        port=int(os.environ.get("PORT", os.environ.get("ODIBI_MCP_PORT", "8000"))),
    )


if __name__ == "__main__":
    main()
