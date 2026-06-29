"""HTTP app wrapper for Databricks App deployment.

Exposes the Odibi MCP server over HTTP/SSE transport
for use by Genie Code and other Databricks agents.

Usage:
    uvicorn odibi_mcp.databricks_app:http_app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

from starlette.middleware.cors import CORSMiddleware

# Import the existing MCP instance from mcp_server.py
from odibi_mcp.mcp_server import mcp

# Create stateless HTTP app (required by Databricks Apps)
http_app = mcp.http_app(stateless_http=True)

# Add CORS middleware for Databricks workspace access
# Allow all origins since the app is already behind Databricks auth
http_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)