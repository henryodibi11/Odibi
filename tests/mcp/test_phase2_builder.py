"""Test Phase 2 session-based incremental builder."""

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


def test_phase2_workflow():
    """Test complete Phase 2 workflow."""

    print("=" * 80)
    print("PHASE 2: SESSION-BASED BUILDER TEST")
    print("=" * 80)

    # Step 1: Create session
    print("\n[STEP 1] Create pipeline session...")
    session = create_pipeline("bronze_to_gold", "silver")

    if "error" in session:
        print(f"  ERROR: {session['error']}")
        return False

    sid = session["session_id"]
    print(f"  ✓ Session created: {sid[:8]}...")
    print(f"  Pipeline: {session['pipeline_name']}")
    print(f"  Layer: {session['layer']}")

    # Step 2: Add first node
    print("\n[STEP 2] Add bronze node...")
    result = add_node(sid, "ingest_raw_data")

    if "error" in result:
        print(f"  ERROR: {result['error']}")
        return False

    print(f"  ✓ Node added: {result['node_name']}")

    # Step 3: Configure read
    print("\n[STEP 3] Configure read from SQL...")
    result = configure_read(
        sid, "ingest_raw_data", connection="source_db", format="sql", table="dbo.RawData"
    )

    if "error" in result:
        print(f"  ERROR: {result['error']}")
        return False

    print("  ✓ Read configured")

    # Step 4: Configure write
    print("\n[STEP 4] Configure write to Delta...")
    result = configure_write(
        sid,
        "ingest_raw_data",
        connection="lake",
        format="delta",
        path="bronze/raw_data",
        mode="append_once",
        keys=["record_id"],
    )

    if "error" in result:
        print(f"  ERROR: {result['error']}")
        return False

    print(f"  ✓ Write configured (mode: {result['mode']})")

    # Step 5: Add second node with dependency
    print("\n[STEP 5] Add silver node with dependency...")
    result = add_node(sid, "clean_data", depends_on=["ingest_raw_data"])

    if "error" in result:
        print(f"  ERROR: {result['error']}")
        return False

    print(f"  ✓ Node added with dependency: {result['depends_on']}")

    # Step 6: Configure read from first node
    print("\n[STEP 6] Configure read from bronze layer...")
    result = configure_read(
        sid, "clean_data", connection="lake", format="delta", path="bronze/raw_data"
    )
    print("  ✓ Read configured")

    # Step 7: Add transformations
    print("\n[STEP 7] Add transformation steps...")
    result = configure_transform(
        sid,
        "clean_data",
        steps=[
            {"function": "distinct", "params": {}},
            {"function": "fill_nulls", "params": {"values": {"status": "UNKNOWN"}}},
        ],
    )

    if "error" in result:
        print(f"  ERROR: {result['error']}")
        return False

    print(f"  ✓ {result['step_count']} transform steps added")

    # Step 8: Configure write for second node
    print("\n[STEP 8] Configure write to silver...")
    result = configure_write(
        sid,
        "clean_data",
        connection="lake",
        format="delta",
        path="silver/clean_data",
        mode="overwrite",
    )
    print("  ✓ Write configured")

    # Step 9: Get pipeline state
    print("\n[STEP 9] Get pipeline state...")
    state = get_pipeline_state(sid)

    print(f"  Pipeline: {state['pipeline_name']}")
    print(f"  Nodes: {state['node_count']}")
    for node in state["nodes"]:
        print(
            f"    - {node['name']} (read={node['has_read']}, write={node['has_write']}, transform={node['has_transform']})"
        )

    # Step 10: Render final YAML
    print("\n[STEP 10] Render pipeline YAML...")
    result = render_pipeline_yaml(sid)

    if not result["valid"]:
        print("  ERRORS:")
        for err in result.get("errors", []):
            print(f"    - {err}")
        return False

    print(f"  ✓ YAML generated: {len(result['yaml'])} characters")
    print(f"  ✓ Nodes: {result['node_count']}")

    # Step 11: Validate the generated YAML
    print("\n[STEP 11] Validate generated YAML...")
    validation = validate_pipeline(result["yaml"])

    print(f"  {validation['summary']}")

    if not validation["valid"]:
        print("  Errors:")
        for err in validation["errors"]:
            print(f"    - {err['message']}")
        return False

    # Step 12: Show YAML
    print("\n[STEP 12] Generated YAML:")
    print("-" * 80)
    print(result["yaml"])
    print("-" * 80)

    # Step 13: Test list sessions
    print("\n[STEP 13] List active sessions...")
    sessions = list_sessions()
    print(f"  Active sessions: {sessions['count']}")
    print(f"  Capacity: {sessions['capacity']}")

    # Step 14: Discard session
    print("\n[STEP 14] Discard session...")
    result = discard_pipeline(sid)
    print(f"  ✓ Discarded: {result['pipeline_name']}")

    print("\n" + "=" * 80)
    print("✓ PHASE 2: ALL STEPS PASSED")
    print("=" * 80)

    return True


if __name__ == "__main__":
    success = test_phase2_workflow()
    exit(0 if success else 1)
