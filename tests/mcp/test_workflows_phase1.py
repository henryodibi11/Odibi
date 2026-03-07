"""Tests for workflow engine Phase 1: Core functionality."""

import pytest

from odibi_mcp.tools.workflows import (
    run_workflow,
    list_workflows,
    get_workflow,
    _get_path,
    _set_path,
    _apply_templates,
)


class TestStateHelpers:
    """Test state management helpers."""

    def test_get_path_simple(self):
        state = {"params": {"name": "test"}}
        assert _get_path(state, "params.name") == "test"

    def test_get_path_nested(self):
        state = {"results": {"apply": {"yaml": "content"}}}
        assert _get_path(state, "results.apply.yaml") == "content"

    def test_get_path_missing_returns_default(self):
        state = {"params": {}}
        assert _get_path(state, "params.missing", "default") == "default"

    def test_set_path_creates_nested_dicts(self):
        state = {}
        _set_path(state, "params.nested.value", 123)
        assert state == {"params": {"nested": {"value": 123}}}

    def test_apply_templates_simple(self):
        state = {"params": {"name": "test"}}
        result = _apply_templates("Hello {params.name}", state)
        assert result == "Hello test"

    def test_apply_templates_missing_value(self):
        state = {"params": {}}
        result = _apply_templates("Value: {params.missing}", state)
        assert result == "Value: "

    def test_apply_templates_dict(self):
        state = {"params": {"x": "a", "y": "b"}}
        result = _apply_templates({"key1": "{params.x}", "key2": "{params.y}"}, state)
        assert result == {"key1": "a", "key2": "b"}

    def test_apply_templates_list(self):
        state = {"params": {"val": "test"}}
        result = _apply_templates(["{params.val}", "static"], state)
        assert result == ["test", "static"]


class TestWorkflowDiscovery:
    """Test workflow listing and retrieval."""

    def test_list_workflows_returns_available(self):
        result = list_workflows()
        assert "workflows" in result
        assert len(result["workflows"]) >= 1
        assert any(w["name"] == "validate_yaml_simple" for w in result["workflows"])

    def test_get_workflow_returns_definition(self):
        result = get_workflow("validate_yaml_simple")
        assert "workflow" in result
        assert result["workflow"]["name"] == "validate_yaml_simple"
        assert "steps" in result["workflow"]

    def test_get_workflow_unknown_returns_error(self):
        result = get_workflow("nonexistent")
        assert "error" in result
        assert result["error"]["code"] == "UNKNOWN_WORKFLOW"


class TestBasicExecution:
    """Test basic workflow execution."""

    def test_validate_yaml_simple_with_valid_yaml(self):
        test_yaml = """
project: test
engine: pandas
connections:
  local: {type: local, base_path: .}
pipelines:
  - pipeline: test
    nodes:
      - name: n1
        read: {connection: local, path: test.csv, format: csv}
        write: {connection: local, path: out.parquet, format: parquet, mode: overwrite}
story: {connection: local, path: _s}
system: {connection: local, path: _sys}
"""
        result = run_workflow("validate_yaml_simple", {"yaml": test_yaml})

        assert result["status"] == "COMPLETED"
        assert result["progress"] == 100.0
        assert "events" in result
        assert len(result["events"]) > 0

    def test_validate_yaml_simple_with_invalid_yaml(self):
        bad_yaml = "invalid: yaml: syntax::"
        result = run_workflow("validate_yaml_simple", {"yaml": bad_yaml})

        assert result["status"] == "COMPLETED"
        # Should complete (doesn't crash), validation result shows invalid

    def test_workflow_returns_events(self):
        result = run_workflow("validate_yaml_simple", {"yaml": "project: test"})

        assert "events" in result
        events = result["events"]
        assert len(events) > 0
        # Should have log, tool_call events
        event_types = {e["type"] for e in events}
        assert "log" in event_types

    def test_workflow_returns_duration(self):
        result = run_workflow("validate_yaml_simple", {"yaml": "project: test"})

        assert "duration_s" in result
        assert result["duration_s"] >= 0


class TestStepTypes:
    """Test individual step type execution."""

    def test_set_step_updates_state(self):
        # Create minimal workflow with just a set step
        from odibi_mcp.tools.workflows import Engine

        wf = {
            "steps": [
                {"type": "set", "values": {"results.test": "value123"}},
            ]
        }
        engine = Engine(wf, {})
        result = engine.run()

        assert result["status"] == "COMPLETED"
        assert _get_path(result["state"], "results.test") == "value123"

    def test_log_step_creates_event(self):
        from odibi_mcp.tools.workflows import Engine

        wf = {
            "steps": [
                {"type": "log", "message": "Test message"},
            ]
        }
        engine = Engine(wf, {})
        result = engine.run()

        assert result["status"] == "COMPLETED"
        log_events = [e for e in result["events"] if e["type"] == "log" and "message" in e]
        assert len(log_events) > 0
        assert log_events[0]["message"] == "Test message"

    def test_call_step_invokes_tool(self):
        from odibi_mcp.tools.workflows import Engine

        wf = {
            "steps": [
                {
                    "type": "call",
                    "tool": "test_pipeline",
                    "args": {"yaml_content": "project: test", "mode": "validate"},
                    "assign": "results.validation",
                },
            ]
        }
        engine = Engine(wf, {})
        result = engine.run()

        assert result["status"] == "COMPLETED"
        assert _get_path(result["state"], "results.validation") is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
