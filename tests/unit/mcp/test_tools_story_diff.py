def test_story_diff_basic(monkeypatch, tmp_path):
    """Test story_diff returns proper result structure."""
    import json
    from odibi_mcp.tools import story as story_module

    # Create mock story files for two runs
    for run_id, row_count in [("run_a", 100), ("run_b", 150)]:
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

    result = story_module.story_diff(
        pipeline="test_pipeline",
        run_a="run_a",
        run_b="run_b",
    )

    # Story B has 50 more rows than A
    assert result.row_count_diff == 50
