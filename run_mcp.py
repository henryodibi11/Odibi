"""Wrapper script to run odibi MCP server with .env loading."""

import os
import sys

# On Windows, use TF-IDF fallback for embeddings to avoid PyTorch DLL issues
# This enables query_codebase to work without sentence-transformers
if sys.platform == "win32":
    os.environ.setdefault("ODIBI_USE_TFIDF_EMBEDDINGS", "1")

# Load .env from the project directory
from dotenv import load_dotenv

# Try to load from ODIBI_CONFIG's directory
config_path = os.environ.get("ODIBI_CONFIG", "")
if config_path:
    env_path = os.path.join(os.path.dirname(config_path), ".env")
    if os.path.exists(env_path):
        load_dotenv(env_path)

# Now run the MCP server
from odibi_mcp.server import run

run()
