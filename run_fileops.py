#!/usr/bin/env python
"""Entry point to run the odibi-fileops MCP server."""

import asyncio
import sys
from pathlib import Path

# Ensure odibi_mcp is importable
sys.path.insert(0, str(Path(__file__).parent))

from odibi_mcp.fileops.server import main

if __name__ == "__main__":
    asyncio.run(main())
