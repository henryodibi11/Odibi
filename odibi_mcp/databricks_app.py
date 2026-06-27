"""Databricks App entry point for the Odibi MCP server.

Serves the existing MCP server over Streamable HTTP at /mcp for Genie Code.

Usage (local testing):
    uvicorn odibi_mcp.databricks_app:app --host 0.0.0.0 --port 8000
    # Then connect: Genie Code Settings → MCP → Custom → http://localhost:8000/mcp

Usage (Databricks App):
    Set ``odibi_mcp.databricks_app:app`` as the ASGI entry point.
    Genie Code connects at https://<app-url>/mcp

Requirements:
    pip install odibi[mcp]   # mcp>=1.0.0 pulls in starlette, uvicorn
"""

from __future__ import annotations

import contextlib
import logging
import os
import sys
from pathlib import Path

# --- Bootstrap (same as server.py) ---
logging.basicConfig(level=logging.INFO, stream=sys.stderr)
logger = logging.getLogger(__name__)

ODIBI_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ODIBI_ROOT))
sys.path.insert(0, str(ODIBI_ROOT / "_archive"))

# Load .env if present
try:
    from dotenv import load_dotenv

    for env_path in [
        Path.cwd() / ".env",
        Path(os.environ.get("ODIBI_CONFIG", "")).parent / ".env"
        if os.environ.get("ODIBI_CONFIG")
        else None,
        ODIBI_ROOT / ".env",
    ]:
        if env_path and env_path.exists():
            load_dotenv(env_path, override=True)
            break
except ImportError:
    pass

# --- Build the FastMCP wrapper ---
from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

# Create a FastMCP instance in stateless mode (Databricks requirement)
mcp = FastMCP(
    "odibi-knowledge",
    stateless_http=True,
    json_response=True,
)

# Re-register all tools and resources from the existing server by importing
# the tool functions and wiring them into the FastMCP instance.
# Rather than duplicating 2000 lines of tool definitions, we mount the
# existing low-level Server's handlers directly.
from odibi_mcp.server import server as _low_level_server

# Patch the FastMCP's internal server with our fully-configured one.
# FastMCP wraps mcp.server.Server — we replace its _mcp_server with ours
# so all the @server.list_tools, @server.call_tool handlers are preserved.
mcp._mcp_server = _low_level_server


# --- Health endpoint ---
async def health(request):
    return JSONResponse({"status": "ok", "server": "odibi-mcp"})


# --- Lifespan: initialize project context + session manager ---
@contextlib.asynccontextmanager
async def lifespan(app: Starlette):
    from odibi_mcp.context import initialize_from_env

    ctx = initialize_from_env()
    if ctx:
        logger.info(f"Loaded project: {ctx.project_name} from {ctx.config_path}")
    else:
        logger.warning("No project config found - facade tools will return empty results")

    async with mcp.session_manager.run():
        yield


# --- Assemble the ASGI app ---
# Databricks expects MCP at /mcp. FastMCP's streamable_http_app() serves
# at settings.streamable_http_path ("/mcp" by default), so mounting at "/"
# gives us /mcp automatically.
app = Starlette(
    routes=[
        Route("/health", health, methods=["GET"]),
        Mount("/", app=mcp.streamable_http_app()),
    ],
    lifespan=lifespan,
)

# CORS — Databricks workspace needs to reach this server
app = CORSMiddleware(
    app,
    allow_origins=[
        os.environ.get("DATABRICKS_HOST", ""),
        "https://*.cloud.databricks.com",
        "https://*.azuredatabricks.net",
        "http://localhost:8000",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE"],
    expose_headers=["Mcp-Session-Id"],
)


def main():
    """Run locally for testing."""
    import argparse

    import uvicorn

    parser = argparse.ArgumentParser(description="Odibi MCP Server (HTTP for Databricks)")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8000)))
    parser.add_argument("--host", type=str, default="0.0.0.0")
    args = parser.parse_args()

    logger.info(f"Starting Odibi MCP HTTP server on {args.host}:{args.port}")
    logger.info(f"MCP endpoint: http://{args.host}:{args.port}/mcp")
    logger.info(f"Health check: http://{args.host}:{args.port}/health")
    uvicorn.run(
        "odibi_mcp.databricks_app:app",
        host=args.host,
        port=args.port,
    )


if __name__ == "__main__":
    main()
