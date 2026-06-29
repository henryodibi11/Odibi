"""Integration test for OdibiDispatcher."""

from odibi_mcp.dispatcher import OdibiDispatcher
import json


def test_dispatcher_basic():
    dispatcher = OdibiDispatcher()
    
    print("🧪 Testing Dispatcher Integration\n")
    
    # Test 1: Full help
    print("Test 1: Full help catalog")
    result = dispatcher.help()
    assert result["kind"] == "odibi_help"
    assert result["total_actions"] == 43
    print(f"✅ Full catalog: {result['total_actions']} actions")
    
    # Test 2: Category help
    print("\nTest 2: Category help")
    result = dispatcher.help(category="Workflows")
    assert result["category"] == "Workflows"
    print(f"✅ Workflows: {len(result['actions'])} actions")
    
    # Test 3: Unknown action
    print("\nTest 3: Error handling")
    result = dispatcher.dispatch("nonexistent")
    assert "error" in result
    print(f"✅ Error handled: {result['error'][:50]}...")
    
    print("\n🎉 All tests passed!")

if __name__ == "__main__":
    test_dispatcher_basic()
