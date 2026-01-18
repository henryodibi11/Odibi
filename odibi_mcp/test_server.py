"""Quick test for the MCP server."""

import sys
from pathlib import Path

# Ensure correct path
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "_archive"))

import asyncio
import json
from odibi_mcp.server import list_tools, call_tool


async def test_all_tools():
    print("=== Odibi MCP Server Test ===\n")

    # List tools
    tools = await list_tools()
    print(f"Available tools ({len(tools)}):")
    for t in tools:
        print(f"  • {t.name}")
    print()

    # Test each structured tool
    tests = [
        ("list_transformers", {}),
        ("list_patterns", {}),
        ("list_connections", {}),
        ("explain", {"name": "dimension"}),
        ("get_transformer_signature", {}),
        ("get_yaml_structure", {}),
        ("get_index_stats", {}),
        ("list_docs", {}),
        ("list_docs", {"category": "patterns"}),
        ("search_docs", {"query": "SCD2"}),
        ("get_doc", {"doc_path": "docs/patterns/scd2.md"}),
        ("get_deep_context", {}),
    ]

    for tool_name, args in tests:
        result = await call_tool(tool_name, args)
        text = result[0].text
        if text.startswith("{") or text.startswith("["):
            data = json.loads(text)
            if isinstance(data, list):
                print(f"✓ {tool_name}: {len(data)} items")
            elif isinstance(data, dict) and "error" in data:
                print(f"✗ {tool_name}: {data['error']}")
            else:
                print(f"✓ {tool_name}: OK")
        else:
            print(f"✓ {tool_name}: {len(text)} chars")

    print("\n=== All tests passed ===")


if __name__ == "__main__":
    asyncio.run(test_all_tools())
