"""Integration test for OdibiDispatcher."""

from odibi_mcp.dispatcher import OdibiDispatcher


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

    # Test 4: actually DISPATCH discovery actions (not just list them) — these
    # delegate to OdibiKnowledge and were silently broken before.
    print("\nTest 4: Dispatch discovery actions")
    for action, kwargs in [
        ("onboard", {}),
        ("list_skills", {}),
        ("search_docs", {"query": "simulation"}),
        ("list_examples", {"pattern": "simulation"}),
        ("get_schema", {"component": "read"}),
    ]:
        r = dispatcher.dispatch(action, **kwargs)
        assert isinstance(r, dict) and set(r.keys()) - {"error", "tip", "available"}, (
            f"{action} returned an error: {r}"
        )
        print(f"✅ {action} dispatched")

    print("\n🎉 All tests passed!")

if __name__ == "__main__":
    test_dispatcher_basic()
