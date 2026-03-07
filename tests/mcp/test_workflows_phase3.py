"""Tests for Phase 3 story workflows and tools."""

import pytest
from odibi_mcp.tools.workflows import Engine, list_workflows, get_workflow
from odibi_mcp.tools.story import node_sample, NodeSampleResult


class TestStoryToolRegistration:
    """Test story tools are registered."""

    def test_story_tools_in_registry(self):
        """Story tools available in workflow tool registry."""
        from odibi_mcp.tools.workflows import TOOL_REGISTRY

        assert "story_read" in TOOL_REGISTRY
        assert "node_sample" in TOOL_REGISTRY
        assert "node_failed_rows" in TOOL_REGISTRY
        assert "lineage_graph" in TOOL_REGISTRY

    def test_story_tools_callable(self):
        """Story tools can be called directly."""
        # These should return dataclass results with error status since no story exists
        result = node_sample("nonexistent", "node1")
        assert isinstance(result, NodeSampleResult)
        assert result.status in ["not_found", "unavailable", "error"]


class TestPhase3Workflows:
    """Test Phase 3 story workflows."""

    def test_phase3_workflows_registered(self):
        """Phase 3 workflows are available."""
        workflows = list_workflows()
        names = [w["name"] for w in workflows["workflows"]]

        assert "inspect_pipeline_run" in names
        assert "debug_failed_run" in names

    def test_inspect_pipeline_run_structure(self):
        """inspect_pipeline_run has correct structure."""
        resp = get_workflow("inspect_pipeline_run")
        wf = resp["workflow"]

        assert "description" in wf
        assert "steps" in wf

        # Should call story_read
        story_read_calls = [s for s in wf["steps"] if s.get("tool") == "story_read"]
        assert len(story_read_calls) > 0

        # Should have retry on story_read
        assert story_read_calls[0].get("retry") is not None

    def test_debug_failed_run_structure(self):
        """debug_failed_run has correct structure."""
        resp = get_workflow("debug_failed_run")
        wf = resp["workflow"]

        assert "description" in wf
        assert "steps" in wf

        # Should use loop
        loop_steps = [s for s in wf["steps"] if s.get("type") == "loop"]
        assert len(loop_steps) > 0

        # Should call story_read
        tools_called = [s.get("tool") for s in wf["steps"] if s.get("tool")]
        assert "story_read" in tools_called

        # node_failed_rows should be in loop body
        loop_body = loop_steps[0].get("body", [])
        body_tools = [s.get("tool") for s in loop_body if s.get("tool")]
        assert "node_failed_rows" in body_tools

    def test_inspect_pipeline_run_pauses_for_input(self):
        """inspect_pipeline_run pauses for pipeline name."""
        resp = get_workflow("inspect_pipeline_run")
        engine = Engine(resp["workflow"], {})
        result = engine.run()

        assert result["status"] == "AWAITING_INPUT"
        assert "resume_token" in result
        prompts = result.get("prompts", [])
        assert any("pipeline" in p["path"].lower() for p in prompts)

    def test_debug_failed_run_pauses_for_input(self):
        """debug_failed_run pauses for pipeline name."""
        resp = get_workflow("debug_failed_run")
        engine = Engine(resp["workflow"], {})
        result = engine.run()

        assert result["status"] == "AWAITING_INPUT"
        assert "resume_token" in result
        prompts = result.get("prompts", [])
        assert any("pipeline" in p["path"].lower() for p in prompts)


class TestStoryToolWorkflowIntegration:
    """Test story tools work in workflows."""

    def test_workflow_calls_story_read(self):
        """Workflow can call story_read tool."""
        workflow = {
            "steps": [
                {"type": "set", "values": {"params.pipeline": "test_pipeline"}},
                {
                    "type": "call",
                    "tool": "story_read",
                    "args": {"pipeline": "{params.pipeline}"},
                    "assign": "results.story",
                },
                {"type": "log", "message": "Story status: {results.story.status}"},
            ]
        }

        engine = Engine(workflow, {})
        result = engine.run()

        assert result["status"] == "COMPLETED"
        # Story read should return not_found or unavailable (no real story)
        assert "story" in engine.state["results"]

    def test_workflow_calls_node_sample(self):
        """Workflow can call node_sample tool."""
        workflow = {
            "steps": [
                {"type": "set", "values": {"params.pipeline": "test", "params.node": "node1"}},
                {
                    "type": "call",
                    "tool": "node_sample",
                    "args": {"pipeline": "{params.pipeline}", "node": "{params.node}", "limit": 10},
                    "assign": "results.sample",
                },
                {"type": "log", "message": "Sample status: {results.sample.status}"},
            ]
        }

        engine = Engine(workflow, {})
        result = engine.run()

        assert result["status"] == "COMPLETED"
        assert "sample" in engine.state["results"]

    def test_workflow_with_retry_on_story_tool(self):
        """Story tools respect retry configuration."""
        workflow = {
            "steps": [
                {"type": "set", "values": {"params.pipeline": "test"}},
                {
                    "type": "call",
                    "tool": "story_read",
                    "args": {"pipeline": "{params.pipeline}"},
                    "retry": {"max_attempts": 2, "backoff": 0.1, "retry_on_exception": True},
                    "assign": "results.story",
                },
            ]
        }

        engine = Engine(workflow, {})
        result = engine.run()

        assert result["status"] == "COMPLETED"


class TestWorkflowCount:
    """Verify total workflow count."""

    def test_six_workflows_available(self):
        """All 6 workflows are registered."""
        workflows = list_workflows()
        names = [w["name"] for w in workflows["workflows"]]

        assert len(names) == 6
        expected = [
            "validate_yaml_simple",
            "build_and_validate",
            "debug_pipeline",
            "iterate_until_valid",
            "inspect_pipeline_run",
            "debug_failed_run",
        ]
        for exp in expected:
            assert exp in names


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
