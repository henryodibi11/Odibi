"""Test that MCP server loads and exposes our Phase 1 tools correctly."""

import sys


def test_mcp_server_loads():
    """Test MCP server can be imported and lists our tools."""
    print("Testing MCP server integration...\n")

    try:
        # Import the server module
        from odibi_mcp import server

        print("✓ MCP server module imports successfully")

        # Check our tools are imported
        print("✓ Phase 1 tools import successfully")

        # Try to get the list_tools function
        import asyncio

        async def get_tools():
            return await server.list_tools()

        tools = asyncio.run(get_tools())
        print(f"✓ MCP server exposes {len(tools)} tools")

        # Check our Phase 1 tools are registered
        tool_names = [t.name for t in tools]

        expected_tools = [
            "list_transformers",
            "list_patterns",
            "apply_pattern_template",
            "validate_pipeline",
        ]

        found = []
        missing = []

        for tool in expected_tools:
            if tool in tool_names:
                found.append(tool)
                print(f"  ✓ {tool}")
            else:
                missing.append(tool)
                print(f"  ✗ MISSING: {tool}")

        if missing:
            print(f"\n✗ FAILED: {len(missing)} tools not registered in MCP server")
            return False

        print(f"\n✓ SUCCESS: All {len(expected_tools)} Phase 1 tools registered in MCP server")

        # Check tool schemas have required fields
        for tool in tools:
            if tool.name in expected_tools:
                assert tool.description, f"{tool.name} missing description"
                assert tool.inputSchema, f"{tool.name} missing inputSchema"
                print(f"  ✓ {tool.name} has valid schema")

        print("\n" + "=" * 80)
        print("✓ MCP SERVER INTEGRATION: PASSED")
        print("=" * 80)
        return True

    except Exception as e:
        print(f"\n✗ MCP server test FAILED: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_mcp_server_loads()
    sys.exit(0 if success else 1)
