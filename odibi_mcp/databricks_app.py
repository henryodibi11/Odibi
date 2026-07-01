"""HTTP app wrapper for Databricks App deployment.

Exposes the Odibi MCP server over HTTP/SSE transport
for use by Genie Code and other Databricks agents.

Usage:
    uvicorn odibi_mcp.databricks_app:http_app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import sys

# Minimal version to isolate the failure
print("[INIT] databricks_app module loading...", file=sys.stderr, flush=True)

try:
    print("[INIT] Step 1: Importing CORS...", file=sys.stderr, flush=True)
    from starlette.middleware.cors import CORSMiddleware
    print("[INIT] Step 1: OK", file=sys.stderr, flush=True)
    
    print("[INIT] Step 2: Importing mcp_server...", file=sys.stderr, flush=True)
    from mcp_server import mcp  # No package prefix - files are at /workspace/ directly
    print(f"[INIT] Step 2: OK - mcp={mcp}", file=sys.stderr, flush=True)
    
    print("[INIT] Step 3: Creating HTTP app...", file=sys.stderr, flush=True)
    http_app = mcp.http_app(stateless_http=True)
    print(f"[INIT] Step 3: OK - app={http_app}", file=sys.stderr, flush=True)
    
    print("[INIT] Step 4: Adding CORS middleware...", file=sys.stderr, flush=True)
    http_app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    print("[INIT] Step 4: OK - Initialization complete!", file=sys.stderr, flush=True)
    
except Exception as e:
    print(f"[INIT] EXCEPTION: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
    import traceback
    traceback.print_exc(file=sys.stderr)
    
    # Create a minimal error app
    from starlette.applications import Starlette
    from starlette.responses import PlainTextResponse
    from starlette.routing import Route
    
    error_msg = f"{type(e).__name__}: {e}\n\n{traceback.format_exc()}"
    
    async def error_handler(request):
        return PlainTextResponse(error_msg, status_code=500)
    
    http_app = Starlette(routes=[Route("/", error_handler), Route("/{path:path}", error_handler)])
    print(f"[INIT] Created fallback error app", file=sys.stderr, flush=True)


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