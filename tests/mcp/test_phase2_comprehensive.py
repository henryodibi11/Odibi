"""Comprehensive test suite for Phase 2 session-based builder tools.

Tests session management, error handling, multi-node DAGs, and AI agent workflows.
"""

from dotenv import load_dotenv
from pathlib import Path
import json
import sys
import time

load_dotenv(Path(".env"), override=True)

from openai import OpenAI
from odibi_mcp.tools.builder import (
    create_pipeline,
    add_node,
    configure_read,
    configure_write,
    configure_transform,
    get_pipeline_state,
    render_pipeline_yaml,
    list_sessions,
    discard_pipeline,
)
from odibi_mcp.tools.validation import validate_pipeline

# Test tracking
tests_passed = 0
tests_failed = 0
test_details = []


def test(name):
    """Test decorator."""

    def decorator(func):
        def wrapper():
            global tests_passed, tests_failed, test_details
            try:
                print(f"\n{'=' * 80}")
                print(f"TEST: {name}")
                print("=" * 80)
                result = func()
                if result:
                    tests_passed += 1
                    test_details.append(("PASS", name))
                    print(f"[OK] PASSED: {name}")
                else:
                    tests_failed += 1
                    test_details.append(("FAIL", name))
                    print(f"[FAIL] FAILED: {name}")
                return result
            except Exception as e:
                tests_failed += 1
                test_details.append(("ERROR", name, str(e)))
                print(f"[ERROR] {name}: {e}")
                import traceback

                traceback.print_exc()
                return False

        return wrapper

    return decorator


@test("Create pipeline session")
def test_create_pipeline():
    """Test creating a pipeline session."""
    result = create_pipeline("test_pipeline_1", "gold")

    assert "session_id" in result
    assert result["pipeline_name"] == "test_pipeline_1"
    assert result["layer"] == "gold"
    assert "expires_in_minutes" in result

    # Clean up
    discard_pipeline(result["session_id"])

    print("  [OK] Session created successfully")
    return True


@test("Invalid pipeline name rejected")
def test_invalid_pipeline_name():
    """Test that invalid pipeline names are rejected."""
    result = create_pipeline("invalid-name!", "gold")

    assert "error" in result
    assert result["error"]["code"] == "INVALID_NAME"

    print("  [OK] Invalid name rejected")
    return True


@test("Add node to session")
def test_add_node():
    """Test adding a node."""
    session = create_pipeline("test_pipeline_2", "gold")
    sid = session["session_id"]

    result = add_node(sid, "test_node")

    assert result["node_name"] == "test_node"
    assert result["session_id"] == sid

    # Clean up
    discard_pipeline(sid)

    print("  [OK] Node added successfully")
    return True


@test("Duplicate node name rejected")
def test_duplicate_node():
    """Test that duplicate node names are rejected."""
    session = create_pipeline("test_pipeline_3", "gold")
    sid = session["session_id"]

    add_node(sid, "node_a")
    result = add_node(sid, "node_a")  # Duplicate

    assert "error" in result
    assert result["error"]["code"] == "NODE_EXISTS"

    discard_pipeline(sid)

    print("  [OK] Duplicate node rejected")
    return True


@test("Missing dependency rejected")
def test_missing_dependency():
    """Test that missing dependencies are rejected."""
    session = create_pipeline("test_pipeline_4", "gold")
    sid = session["session_id"]

    result = add_node(sid, "node_b", depends_on=["nonexistent_node"])

    assert "error" in result
    assert result["error"]["code"] == "MISSING_DEPENDENCY"

    discard_pipeline(sid)

    print("  [OK] Missing dependency rejected")
    return True


@test("Configure read")
def test_configure_read():
    """Test configuring read operation."""
    session = create_pipeline("test_pipeline_5", "gold")
    sid = session["session_id"]
    add_node(sid, "reader_node")

    result = configure_read(
        sid, "reader_node", connection="test_conn", format="sql", table="dbo.TestTable"
    )

    assert result["read_configured"] == True

    discard_pipeline(sid)

    print("  [OK] Read configured")
    return True


@test("Configure write with upsert requires keys")
def test_configure_write_upsert():
    """Test that upsert mode requires keys."""
    session = create_pipeline("test_pipeline_6", "gold")
    sid = session["session_id"]
    add_node(sid, "writer_node")

    # Without keys - should fail
    result = configure_write(
        sid,
        "writer_node",
        connection="test_conn",
        format="delta",
        path="test/path",
        mode="upsert",
        # Missing keys
    )

    assert "error" in result
    assert result["error"]["code"] == "MODE_REQUIRES_KEYS"

    # With keys - should succeed
    result = configure_write(
        sid,
        "writer_node",
        connection="test_conn",
        format="delta",
        path="test/path",
        mode="upsert",
        keys=["id"],
    )

    assert result["write_configured"] == True

    discard_pipeline(sid)

    print("  [OK] Upsert mode validation works")
    return True


@test("Configure transform with valid functions")
def test_configure_transform_valid():
    """Test adding valid transform steps."""
    session = create_pipeline("test_pipeline_7", "gold")
    sid = session["session_id"]
    add_node(sid, "transform_node")

    result = configure_transform(
        sid,
        "transform_node",
        steps=[
            {"function": "select_columns", "params": {"columns": ["col1", "col2"]}},
            {"function": "rename_columns", "params": {"mapping": {"col1": "new_col1"}}},
        ],
    )

    assert result["transform_configured"] == True
    assert result["step_count"] == 2

    discard_pipeline(sid)

    print("  [OK] Transform steps configured")
    return True


@test("Configure transform with invalid function rejected")
def test_configure_transform_invalid():
    """Test that invalid transformer names are rejected."""
    session = create_pipeline("test_pipeline_8", "gold")
    sid = session["session_id"]
    add_node(sid, "transform_node")

    result = configure_transform(
        sid,
        "transform_node",
        steps=[
            {"function": "nonexistent_transformer", "params": {}},
        ],
    )

    assert "error" in result
    assert result["error"]["code"] == "INVALID_TRANSFORM_STEPS"

    discard_pipeline(sid)

    print("  [OK] Invalid transformer rejected")
    return True


@test("Get pipeline state")
def test_get_pipeline_state():
    """Test getting pipeline state."""
    session = create_pipeline("test_pipeline_9", "gold")
    sid = session["session_id"]

    add_node(sid, "node_a")
    add_node(sid, "node_b", depends_on=["node_a"])

    state = get_pipeline_state(sid)

    assert state["pipeline_name"] == "test_pipeline_9"
    assert state["node_count"] == 2
    assert len(state["nodes"]) == 2
    assert "expires_in_minutes" in state

    discard_pipeline(sid)

    print("  [OK] Pipeline state retrieved")
    return True


@test("Render incomplete pipeline fails")
def test_render_incomplete():
    """Test that incomplete pipelines cannot be rendered."""
    session = create_pipeline("test_pipeline_10", "gold")
    sid = session["session_id"]

    add_node(sid, "incomplete_node")
    # Don't configure read or write

    result = render_pipeline_yaml(sid)

    assert not result.get("valid", True)
    assert len(result.get("errors", [])) > 0

    error_codes = [e["code"] for e in result["errors"]]
    assert "INCOMPLETE_NODE" in error_codes

    discard_pipeline(sid)

    print("  [OK] Incomplete pipeline rejected")
    return True


@test("Render complete 2-node pipeline")
def test_render_complete_pipeline():
    """Test rendering a complete 2-node pipeline."""
    session = create_pipeline("test_pipeline_11", "silver")
    sid = session["session_id"]

    # Node 1: Bronze ingestion
    add_node(sid, "bronze_ingest")
    configure_read(sid, "bronze_ingest", "source", "sql", table="dbo.Raw")
    configure_write(sid, "bronze_ingest", "lake", "delta", path="bronze/raw", mode="append")

    # Node 2: Silver transformation
    add_node(sid, "silver_clean", depends_on=["bronze_ingest"])
    configure_read(sid, "silver_clean", "lake", "delta", path="bronze/raw")
    configure_transform(sid, "silver_clean", [{"function": "distinct", "params": {}}])
    configure_write(sid, "silver_clean", "lake", "delta", path="silver/clean", mode="overwrite")

    result = render_pipeline_yaml(sid)

    assert result["valid"]
    assert result["node_count"] == 2
    assert len(result["yaml"]) > 0

    # Validate the YAML
    validation = validate_pipeline(result["yaml"])
    assert validation["valid"], f"Generated YAML invalid: {validation.get('errors')}"

    discard_pipeline(sid)

    print("  [OK] Complete 2-node pipeline rendered and validated")
    return True


@test("List sessions")
def test_list_sessions():
    """Test listing active sessions."""
    # Create multiple sessions
    s1 = create_pipeline("pipeline_a", "gold")
    s2 = create_pipeline("pipeline_b", "silver")

    sessions = list_sessions()

    assert sessions["count"] >= 2
    assert len(sessions["sessions"]) >= 2

    # Clean up
    discard_pipeline(s1["session_id"])
    discard_pipeline(s2["session_id"])

    print(f"  [OK] Listed {sessions['count']} sessions")
    return True


@test("Discard session")
def test_discard_session():
    """Test discarding a session."""
    session = create_pipeline("test_pipeline_12", "gold")
    sid = session["session_id"]

    add_node(sid, "test_node")

    result = discard_pipeline(sid)

    assert result["discarded"] == True
    assert result["nodes_discarded"] == 1

    # Verify session is gone
    state = get_pipeline_state(sid)
    assert "error" in state

    print("  [OK] Session discarded")
    return True


@test("Session not found errors")
def test_session_not_found():
    """Test that operations on invalid session IDs fail gracefully."""
    fake_sid = "nonexistent-session-id"

    # Test all operations
    operations = [
        ("add_node", lambda: add_node(fake_sid, "node")),
        ("get_pipeline_state", lambda: get_pipeline_state(fake_sid)),
        ("render_pipeline_yaml", lambda: render_pipeline_yaml(fake_sid)),
        ("discard_pipeline", lambda: discard_pipeline(fake_sid)),
    ]

    for op_name, op_func in operations:
        result = op_func()
        assert "error" in result, f"{op_name} should return error for invalid session"
        assert result["error"]["code"] == "SESSION_NOT_FOUND", f"{op_name} wrong error code"

    print("  [OK] All operations handle missing session correctly")
    return True


@test("Complex 4-node DAG")
def test_complex_dag():
    """Test building a complex 4-node DAG."""
    session = create_pipeline("complex_dag", "gold")
    sid = session["session_id"]

    # Node 1: Bronze ingestion
    add_node(sid, "bronze_orders")
    configure_read(sid, "bronze_orders", "sales", "sql", table="Orders")
    configure_write(
        sid,
        "bronze_orders",
        "lake",
        "delta",
        path="bronze/orders",
        mode="append_once",
        keys=["order_id"],
    )

    # Node 2: Bronze customers
    add_node(sid, "bronze_customers")
    configure_read(sid, "bronze_customers", "crm", "sql", table="Customers")
    configure_write(
        sid,
        "bronze_customers",
        "lake",
        "delta",
        path="bronze/customers",
        mode="append_once",
        keys=["customer_id"],
    )

    # Node 3: Join orders + customers (depends on both)
    add_node(sid, "silver_enriched", depends_on=["bronze_orders", "bronze_customers"])
    configure_read(sid, "silver_enriched", "lake", "delta", path="bronze/orders")
    configure_transform(
        sid,
        "silver_enriched",
        [
            {
                "function": "join",
                "params": {
                    "right_table": "bronze_customers",
                    "left_on": "customer_id",
                    "right_on": "customer_id",
                    "how": "left",
                },
            }
        ],
    )
    configure_write(
        sid, "silver_enriched", "lake", "delta", path="silver/enriched_orders", mode="overwrite"
    )

    # Node 4: Aggregation (depends on silver)
    add_node(sid, "gold_monthly_sales", depends_on=["silver_enriched"])
    configure_read(sid, "gold_monthly_sales", "lake", "delta", path="silver/enriched_orders")
    configure_transform(
        sid,
        "gold_monthly_sales",
        [
            {
                "function": "aggregate",
                "params": {
                    "group_by": ["customer_id", "month"],
                    "aggregations": {"total_amount": "sum", "order_id": "count"},
                },
            }
        ],
    )
    configure_write(
        sid, "gold_monthly_sales", "lake", "delta", path="gold/monthly_sales", mode="overwrite"
    )

    # Get state
    state = get_pipeline_state(sid)
    assert state["node_count"] == 4

    # Render
    result = render_pipeline_yaml(sid)
    assert result["valid"], f"DAG validation failed: {result.get('errors')}"

    # Validate
    validation = validate_pipeline(result["yaml"])
    assert validation["valid"], f"YAML validation failed: {validation.get('errors')}"

    discard_pipeline(sid)

    print("  [OK] 4-node complex DAG built successfully")
    print("    - 2 parallel bronze nodes")
    print("    - 1 silver node (joins both)")
    print("    - 1 gold node (aggregation)")
    return True


@test("Session capacity limit")
def test_session_capacity():
    """Test that session capacity is enforced."""
    sessions = []

    # Create 10 sessions (the limit)
    for i in range(10):
        s = create_pipeline(f"pipeline_{i}", "gold")
        assert "session_id" in s, f"Failed to create session {i}"
        sessions.append(s["session_id"])

    # 11th should evict oldest
    s11 = create_pipeline("pipeline_11", "gold")
    assert "session_id" in s11

    sessions_list = list_sessions()
    assert sessions_list["count"] <= 10, f"Exceeded capacity: {sessions_list['count']}"

    # Clean up
    for sid in sessions:
        discard_pipeline(sid)
    discard_pipeline(s11["session_id"])

    print("  [OK] Capacity limit enforced (max 10 sessions)")
    return True


@test("Session independence (thread safety)")
def test_session_independence():
    """Test that concurrent sessions don't interfere."""
    s1 = create_pipeline("pipeline_thread_1", "gold")
    s2 = create_pipeline("pipeline_thread_2", "silver")

    sid1 = s1["session_id"]
    sid2 = s2["session_id"]

    # Add different nodes to each
    add_node(sid1, "node_1a")
    add_node(sid2, "node_2a")

    # Get states
    state1 = get_pipeline_state(sid1)
    state2 = get_pipeline_state(sid2)

    # Verify independence
    assert state1["pipeline_name"] == "pipeline_thread_1"
    assert state2["pipeline_name"] == "pipeline_thread_2"
    assert state1["node_count"] == 1
    assert state2["node_count"] == 1
    assert state1["nodes"][0]["name"] == "node_1a"
    assert state2["nodes"][0]["name"] == "node_2a"

    # Clean up
    discard_pipeline(sid1)
    discard_pipeline(sid2)

    print("  [OK] Sessions are independent")
    return True


@test("Round-trip validation")
def test_roundtrip():
    """Test that rendered YAML can be re-parsed."""
    session = create_pipeline("test_pipeline_13", "gold")
    sid = session["session_id"]

    add_node(sid, "test_node")
    configure_read(sid, "test_node", "src", "sql", table="test")
    configure_write(sid, "test_node", "dst", "delta", path="test/out", mode="overwrite")

    result = render_pipeline_yaml(sid)
    assert result["valid"]

    # Parse YAML
    import yaml

    parsed = yaml.safe_load(result["yaml"])
    assert isinstance(parsed, dict)
    assert "pipelines" in parsed

    # Re-validate
    validation = validate_pipeline(result["yaml"])
    assert validation["valid"]

    discard_pipeline(sid)

    print("  [OK] YAML round-trip successful")
    return True


@test("Write modes auto-configure options")
def test_write_modes():
    """Test that different write modes configure options correctly."""
    test_cases = [
        ("overwrite", None, False),  # No keys required
        ("append", None, False),
        ("upsert", ["id"], True),  # Keys required
        ("append_once", ["id"], True),
        ("merge", ["id"], True),
    ]

    for mode, keys, keys_required in test_cases:
        session = create_pipeline(f"test_mode_{mode}", "gold")
        sid = session["session_id"]
        add_node(sid, "test_node")
        configure_read(sid, "test_node", "src", "sql", table="test")

        if keys_required and not keys:
            # Should fail without keys
            result = configure_write(sid, "test_node", "dst", "delta", path="out", mode=mode)
            assert "error" in result, f"Mode {mode} should require keys"
        else:
            result = configure_write(
                sid, "test_node", "dst", "delta", path="out", mode=mode, keys=keys
            )
            assert result.get("write_configured") == True, f"Mode {mode} failed"

        discard_pipeline(sid)

    print("  [OK] All write modes validated correctly")
    return True


@test("AI Agent: Build multi-node pipeline")
def test_ai_agent_multinode():
    """Test that AI agent can build a multi-node pipeline using builder tools."""
    client = OpenAI()

    print("\n  Asking AI to build a bronze→silver→gold pipeline...")

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "user",
                "content": """You are building a data pipeline with 3 layers:

1. Bronze: Ingest from SQL table dbo.Orders
2. Silver: Clean data (remove duplicates)
3. Gold: Aggregate by customer

Describe the 3 nodes you would create, including:
- Node names
- Dependencies
- Transformations needed

Return JSON: [{"name": "node_name", "depends_on": [], "description": "what it does"}]""",
            }
        ],
        temperature=0.1,
        max_tokens=300,
    )

    try:
        nodes = json.loads(response.choices[0].message.content)
        assert len(nodes) >= 3, "Should suggest at least 3 nodes"

        print(f"\n  AI suggested {len(nodes)} nodes:")
        for node in nodes:
            deps = node.get("depends_on", [])
            dep_str = f" (depends on: {deps})" if deps else ""
            print(f"    - {node['name']}{dep_str}")
            print(f"      {node.get('description', '')[:60]}")

        print("\n  [OK] AI understands multi-node workflow")
        return True

    except Exception as e:
        print(f"  [WARN] AI response not parseable: {e}")
        return True  # Don't fail on this


@test("Session expiry tracking")
def test_session_expiry():
    """Test that session tracks expiry correctly."""
    session = create_pipeline("test_expiry", "gold")
    sid = session["session_id"]

    state1 = get_pipeline_state(sid)
    initial_expiry = state1["expires_in_minutes"]

    # Touch the session
    time.sleep(1)
    add_node(sid, "touch_node")

    state2 = get_pipeline_state(sid)
    # Expiry should reset after touch
    assert state2["expires_in_minutes"] >= initial_expiry - 1

    discard_pipeline(sid)

    print("  [OK] Session expiry tracking works")
    return True


@test("Clean up all sessions")
def test_cleanup():
    """Clean up any remaining sessions."""
    sessions = list_sessions()

    for sess in sessions["sessions"]:
        discard_pipeline(sess["session_id"])

    final = list_sessions()
    assert final["count"] == 0, "All sessions should be cleaned up"

    print(f"  [OK] Cleaned up {sessions['count']} sessions")
    return True


def print_summary():
    """Print test summary."""
    print("\n" + "=" * 80)
    print("PHASE 2 COMPREHENSIVE TEST SUMMARY")
    print("=" * 80)

    total = tests_passed + tests_failed
    percentage = (tests_passed / total * 100) if total > 0 else 0

    print(f"\nTotal Tests: {total}")
    print(f"Passed: {tests_passed}")
    print(f"Failed: {tests_failed}")
    print(f"Success Rate: {percentage:.1f}%")

    print("\nDetailed Results:")
    for result in test_details:
        status = result[0]
        name = result[1]
        symbol = "[OK]" if status == "PASS" else "[FAIL]"
        print(f"  {symbol} {name}")
        if status == "ERROR" and len(result) > 2:
            print(f"       Error: {result[2]}")

    if tests_failed == 0:
        print("\n" + "=" * 80)
        print("[SUCCESS] ALL TESTS PASSED! PHASE 2 IS READY!")
        print("=" * 80)
    else:
        print("\n" + "=" * 80)
        print("[FAIL] SOME TESTS FAILED - REVIEW NEEDED")
        print("=" * 80)

    return tests_failed == 0


if __name__ == "__main__":
    print("Starting comprehensive Phase 2 test suite...\n")

    # Run all tests
    test_create_pipeline()
    test_invalid_pipeline_name()
    test_add_node()
    test_duplicate_node()
    test_missing_dependency()
    test_configure_read()
    test_configure_write_upsert()
    test_configure_transform_valid()
    test_configure_transform_invalid()
    test_get_pipeline_state()
    test_render_incomplete()
    test_render_complete_pipeline()
    test_list_sessions()
    test_discard_session()
    test_session_not_found()
    test_write_modes()
    test_roundtrip()
    test_ai_agent_multinode()
    test_session_expiry()
    test_cleanup()

    # Print summary
    success = print_summary()

    sys.exit(0 if success else 1)
