# tests/integration/mcp/test_story_e2e.py
"""End-to-end tests for story tools."""

import json


def test_story_read_e2e(monkeypatch, tmp_path):
    """Test story_read with mock story files."""
    from odibi_mcp.tools import story as story_module

    # Create mock story
    story_dir = tmp_path / "stories" / "test_pipe" / "run_001"
    story_dir.mkdir(parents=True)

    story_data = {
        "status": "SUCCESS",
        "duration": 10.5,
        "nodes": [
            {"name": "node_1", "status": "success", "row_count": 100},
            {"name": "node_2", "status": "success", "row_count": 200},
            {"name": "node_3", "status": "success", "row_count": 300},
        ],
    }

    with open(story_dir / "story.json", "w") as f:
        json.dump(story_data, f)

    class MockContext:
        def get_story_base_path(self):
            return tmp_path / "stories"

    monkeypatch.setattr(story_module, "get_project_context", lambda: MockContext())

    result = story_module.story_read(pipeline="test_pipe")

    assert result.pipeline == "test_pipe"
    assert result.status == "SUCCESS"
    assert len(result.nodes) == 3


def test_node_describe_e2e(monkeypatch, tmp_path):
    """Test node_describe with mock story files."""
    from odibi_mcp.tools import story as story_module

    # Create mock story
    story_dir = tmp_path / "stories" / "test_pipeline" / "run_001"
    story_dir.mkdir(parents=True)

    story_data = {
        "status": "success",
        "nodes": [
            {
                "name": "source_node",
                "status": "success",
                "operation": "read",
                "duration": 2.0,
                "rows_out": 500,
            },
        ],
    }

    with open(story_dir / "story.json", "w") as f:
        json.dump(story_data, f)

    class MockContext:
        def get_story_base_path(self):
            return tmp_path / "stories"

    monkeypatch.setattr(story_module, "get_project_context", lambda: MockContext())

    result = story_module.node_describe(
        pipeline="test_pipeline",
        node="source_node",
    )

    assert result.operation == "read"
    assert result.duration == 2.0


def test_story_diff_e2e(monkeypatch, tmp_path):
    """Test story_diff with mock story files."""
    from odibi_mcp.tools import story as story_module

    # Create two mock stories with different row counts
    for run_id, row_count in [("run_001", 100), ("run_002", 150)]:
        story_dir = tmp_path / "stories" / "test_pipeline" / run_id
        story_dir.mkdir(parents=True)

        story_data = {
            "status": "success",
            "run_id": run_id,
            "end_time": "2026-01-01T00:00:00",
            "nodes": [
                {"name": "node_a", "status": "success", "row_count": row_count},
            ],
        }

        with open(story_dir / "story.json", "w") as f:
            json.dump(story_data, f)

    class MockContext:
        def get_story_base_path(self):
            return tmp_path / "stories"

    monkeypatch.setattr(story_module, "get_project_context", lambda: MockContext())

    result = story_module.story_diff(
        pipeline="test_pipeline",
        run_a="run_001",
        run_b="run_002",
    )

    assert result.row_count_diff == 50  # 150 - 100
    assert result.sample_diff_included is False
