def test_node_describe_basic(monkeypatch, tmp_path):
    """Test node_describe returns proper result structure."""
    import json
    from odibi_mcp.tools import story as story_module

    # Create a mock story file
    story_dir = tmp_path / "stories" / "test_pipeline" / "run_001"
    story_dir.mkdir(parents=True)

    story_data = {
        "status": "success",
        "nodes": [
            {
                "name": "node_a",
                "status": "success",
                "operation": "transform",
                "duration": 5.0,
                "rows_in": 100,
                "rows_out": 95,
                "inputs": [{"name": "input_1", "path": "data/input.csv"}],
                "outputs": [{"name": "output_1", "path": "data/output.parquet"}],
                "transform": {"steps": [{"function": "filter", "params": {"column": "active"}}]},
                "validations": [{"rule": "not_null", "column": "id"}],
            },
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

    result = story_module.node_describe(
        pipeline="test_pipeline",
        node="node_a",
    )

    assert result.pipeline == "test_pipeline"
    assert result.node == "node_a"
    assert result.status == "success"
    assert result.operation == "transform"
    assert result.duration == 5.0
    assert result.row_count_in == 100
    assert result.row_count_out == 95
    assert len(result.transform_steps) == 1
