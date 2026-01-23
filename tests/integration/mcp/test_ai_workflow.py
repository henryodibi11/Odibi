# tests/integration/mcp/test_ai_workflow.py
"""Integration tests for full AI workflow scenarios."""

import pytest
import json
from odibi_mcp.contracts.access import AccessContext


def test_ai_investigates_pipeline_failure(monkeypatch, tmp_path):
    """
    Simulate AI investigating a pipeline failure:
    1. Get pipeline stats
    2. List failures
    3. Read story for failed run
    4. Describe failing node
    """
    from odibi_mcp.tools import story as story_module
    from odibi_mcp.tools.catalog import failure_summary, node_stats

    # Setup mock story
    story_dir = tmp_path / "stories" / "sales_pipeline" / "run_001"
    story_dir.mkdir(parents=True)

    story_data = {
        "status": "FAILED",
        "duration": 5.0,
        "nodes": [
            {"name": "source_node", "status": "success", "operation": "read", "duration": 2.0},
            {"name": "transform_sales", "status": "failed", "error": "Column not found"},
        ],
    }

    with open(story_dir / "story.json", "w") as f:
        json.dump(story_data, f)

    class MockContext:
        def get_story_base_path(self):
            return tmp_path / "stories"

    monkeypatch.setattr(story_module, "get_project_context", lambda: MockContext())

    # Step 1: Get failure summary
    failures = failure_summary(pipeline="sales_pipeline")
    assert failures.total_failures >= 0

    # Step 2: Get node stats
    stats = node_stats(pipeline="sales_pipeline", node="transform_sales")
    assert stats.pipeline == "sales_pipeline"

    # Step 3: Read story
    story = story_module.story_read(pipeline="sales_pipeline")
    assert story.status == "FAILED"

    # Step 4: Describe failing node
    node = story_module.node_describe(
        pipeline="sales_pipeline",
        node="source_node",
    )
    assert node.operation == "read"


def test_ai_explores_data_sources():
    """
    Simulate AI exploring data sources:
    1. List available connections
    2. List files in a path
    3. Infer schema
    4. Preview data
    """
    from odibi_mcp.tools.discovery import list_files, infer_schema, preview_source

    # Step 1: List files (returns empty without real backend)
    files = list_files(connection="adls_main", path="/data/bronze/")
    assert files.connection == "adls_main"

    # Step 2: Infer schema
    schema = infer_schema(connection="adls_main", path="/data/bronze/sales.parquet")
    assert schema.columns == []  # Empty without real backend

    # Step 3: Preview
    preview = preview_source(connection="adls_main", path="/data/bronze/sales.parquet")
    assert preview.rows == []


def test_ai_traces_lineage():
    """
    Simulate AI tracing data lineage:
    1. Get upstream lineage
    2. Get downstream lineage
    3. Get full graph
    """
    from odibi_mcp.tools.lineage import lineage_upstream, lineage_downstream, lineage_graph

    # Upstream
    upstream = lineage_upstream(pipeline="sales_pipeline", node="final_output")
    assert upstream.source.logical_name == "sales_pipeline/final_output"

    # Downstream
    downstream = lineage_downstream(pipeline="sales_pipeline", node="source_data")
    assert downstream.source.logical_name == "sales_pipeline/source_data"

    # Full graph
    graph = lineage_graph(pipeline="sales_pipeline")
    assert graph.nodes == []  # Empty without real backend


def test_ai_compares_runs(monkeypatch, tmp_path):
    """
    Simulate AI comparing two pipeline runs:
    1. Read story for run A
    2. Read story for run B
    3. Compute diff
    """
    from odibi_mcp.tools import story as story_module

    # Create two mock stories
    for run_id, row_count in [("run_001", 100), ("run_002", 100)]:
        story_dir = tmp_path / "stories" / "sales_pipeline" / run_id
        story_dir.mkdir(parents=True)

        story_data = {
            "status": "success",
            "run_id": run_id,
            "nodes": [{"name": "node_a", "status": "success", "row_count": row_count}],
        }

        with open(story_dir / "story.json", "w") as f:
            json.dump(story_data, f)

    class MockContext:
        def get_story_base_path(self):
            return tmp_path / "stories"

    monkeypatch.setattr(story_module, "get_project_context", lambda: MockContext())

    # Compare runs
    diff = story_module.story_diff(
        pipeline="sales_pipeline",
        run_a="run_001",
        run_b="run_002",
    )

    assert diff.row_count_diff == 0  # Same row count
    assert diff.sample_diff_included is False


def test_access_enforcement_blocks_unauthorized():
    """Test that access enforcement blocks unauthorized access."""
    ctx = AccessContext(
        authorized_projects={"allowed_project"},
        environment="production",
    )

    # Trying to access unauthorized project should raise
    with pytest.raises(PermissionError):
        ctx.check_project("unauthorized_project")
