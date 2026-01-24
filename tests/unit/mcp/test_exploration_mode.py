# tests/unit/mcp/test_exploration_mode.py
"""Tests for MCP exploration mode (connections-only config)."""

import pytest
import yaml


def test_exploration_config_loading(tmp_path):
    """Test loading exploration-only config (no pipelines)."""
    from odibi_mcp.context import MCPProjectContext

    # Create exploration config
    config = {
        "project": "my_exploration",
        "connections": {
            "local": {"type": "local", "path": str(tmp_path / "data")},
        },
    }

    config_path = tmp_path / "exploration.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config, f)

    ctx = MCPProjectContext.from_exploration_config(str(config_path))

    assert ctx.project_name == "my_exploration"
    assert ctx.mode == "exploration"
    assert ctx.is_exploration_mode() is True
    assert ctx.story_connection is None
    assert ctx.story_path is None


def test_exploration_config_auto_detection(tmp_path):
    """Test auto-detection of exploration config."""
    from odibi_mcp.context import _is_exploration_config, _is_full_project_config

    exploration = {
        "project": "test",
        "connections": {"local": {"type": "local", "path": "./data"}},
    }

    full = {
        "project": "test",
        "connections": {"local": {"type": "local", "path": "./data"}},
        "pipelines": [{"pipeline": "bronze", "nodes": []}],
        "story": {"connection": "local", "path": "stories"},
        "system": {"connection": "local", "path": "_system"},
    }

    assert _is_exploration_config(exploration) is True
    assert _is_exploration_config(full) is False

    assert _is_full_project_config(full) is True
    assert _is_full_project_config(exploration) is False


def test_story_tools_return_error_in_exploration_mode(monkeypatch):
    """Test that story tools return informative error in exploration mode."""
    from odibi_mcp.tools import story as story_module

    class MockExplorationContext:
        story_connection = None
        story_path = None

        def is_exploration_mode(self):
            return True

    monkeypatch.setattr(story_module, "get_project_context", lambda: MockExplorationContext())

    result = story_module.story_read(pipeline="test")

    assert result.status == "unavailable"
    assert "exploration mode" in result.error_message.lower()


def test_exploration_config_requires_connections(tmp_path):
    """Test that exploration config fails without connections."""
    from odibi_mcp.context import MCPProjectContext

    # Config without connections
    config = {"project": "test"}

    config_path = tmp_path / "bad_config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config, f)

    with pytest.raises(ValueError, match="connections"):
        MCPProjectContext.from_exploration_config(str(config_path))
