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

from starlette.middleware.cors import CORSMiddleware

from odibi_mcp.mcp_server import mcp

http_app = mcp.http_app(stateless_http=True)
http_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def main() -> None:
    """Serve the HTTP app with uvicorn.

    Entry point for the `odibi-mcp-http` console script and
    `python -m odibi_mcp.databricks_app`. Host/port via ODIBI_MCP_HOST / PORT.
    NOTE: the Databricks Apps deploy uses app.yaml (uvicorn) directly; this is
    for local/manual runs.
    """
    import os

    import uvicorn

    uvicorn.run(
        http_app,
        host=os.environ.get("ODIBI_MCP_HOST", "0.0.0.0"),
        port=int(os.environ.get("PORT", os.environ.get("ODIBI_MCP_PORT", "8000"))),
    )


if __name__ == "__main__":
    main()
