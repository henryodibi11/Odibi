#!/usr/bin/env python
"""Entry point to run the Odibi Workflow MCP server."""

import sys
from pathlib import Path

# Ensure odibi_mcp is importable
sys.path.insert(0, str(Path(__file__).parent))

from odibi_mcp.workflow.server import main
import asyncio

if __name__ == "__main__":
    asyncio.run(main())
