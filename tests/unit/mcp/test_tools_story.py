from odibi_mcp.contracts.selectors import DEFAULT_RUN_SELECTOR


def test_story_read_basic(monkeypatch, tmp_path):
    """Test story_read returns proper result structure."""
    import json
    from odibi_mcp.tools import story as story_module

    # Create a mock story file
    story_dir = tmp_path / "stories" / "test_pipeline" / "run_001"
    story_dir.mkdir(parents=True)

    story_data = {
        "status": "success",
        "duration": 12.3,
        "run_id": "run_001",
        "start_time": "2026-01-01T00:00:00",
        "end_time": "2026-01-01T00:00:12",
        "nodes": [
            {"name": "node_a", "status": "success", "duration": 5.0, "row_count": 100},
            {"name": "node_b", "status": "success", "duration": 7.3, "row_count": 200},
        ],
    }

    with open(story_dir / "story.json", "w") as f:
        json.dump(story_data, f)

    # Mock project context - patch directly in the story module
    class MockContext:
        story_connection = "local"
        story_path = "stories"

        def get_story_base_path(self):
            return tmp_path / "stories"

        def is_exploration_mode(self):
            return False

        def get_connection(self, name):
            class MockConn:
                def get_path(self, path):
                    return str(tmp_path / path)

            return MockConn()

    monkeypatch.setattr(story_module, "get_project_context", lambda: MockContext())

    result = story_module.story_read(
        pipeline="test_pipeline",
        run_selector=DEFAULT_RUN_SELECTOR,
    )

    assert result.pipeline == "test_pipeline"
    assert result.status == "success"
    assert result.duration_seconds == 12.3
    assert result.node_count == 2
    assert result.success_count == 2
