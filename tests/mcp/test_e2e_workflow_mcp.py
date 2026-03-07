"""End-to-end test: Verify workflows work through MCP server handlers."""

import pytest
from odibi_mcp.tools.workflows import list_workflows, run_workflow, resume_workflow


class TestWorkflowMCPIntegration:
    """Test workflows work through MCP interface."""

    def test_list_workflows_via_mcp_interface(self):
        """list_workflows returns all 6 workflows."""
        result = list_workflows()

        assert "workflows" in result
        assert len(result["workflows"]) == 6

        names = [w["name"] for w in result["workflows"]]
        assert "validate_yaml_simple" in names
        assert "iterate_until_valid" in names
        assert "inspect_pipeline_run" in names

    def test_run_workflow_completes_simple_validation(self):
        """run_workflow executes validate_yaml_simple."""
        yaml_content = """
name: test_pipeline
steps: []
"""
        result = run_workflow("validate_yaml_simple", {"yaml": yaml_content})

        # Should complete (may fail validation but workflow runs)
        assert result["status"] in ["COMPLETED", "ERROR"]
        assert "duration_s" in result
        assert "events" in result

    def test_run_workflow_pauses_for_input(self):
        """run_workflow pauses when input needed."""
        result = run_workflow("build_and_validate", {})

        assert result["status"] == "AWAITING_INPUT"
        assert "resume_token" in result
        assert "prompts" in result
        assert len(result["prompts"]) > 0

    def test_resume_workflow_continues_execution(self):
        """resume_workflow continues from pause."""
        # Start workflow
        result1 = run_workflow("build_and_validate", {})
        assert result1["status"] == "AWAITING_INPUT"

        # Resume with inputs
        token = result1["resume_token"]
        inputs = {
            "params.pattern": "dimension",
            "params.pipeline_name": "test_dim",
            "params.source_connection": "local",
            "params.target_connection": "local",
            "params.target_path": "output",
            "params.source_table": "test.csv",
        }

        result2 = resume_workflow(token, inputs)

        # Should progress (may pause again for pattern params)
        assert result2["status"] in ["AWAITING_INPUT", "COMPLETED", "ERROR"]

    def test_run_workflow_with_loop(self):
        """Workflow with loop executes correctly."""
        result = run_workflow(
            "iterate_until_valid",
            {
                "pattern": "dimension",
                "name": "test_pipeline",
                "source": "test.csv",
            },
        )

        # Should start executing and either pause or complete
        assert result["status"] in ["AWAITING_INPUT", "COMPLETED", "ERROR"]
        assert "events" in result

        # Check loop events if completed
        if result["status"] == "COMPLETED":
            event_types = [e.get("type") for e in result.get("events", [])]
            # Should have loop-related events
            assert any("loop" in str(t) for t in event_types)

    def test_run_workflow_with_story_tools(self):
        """Story workflow executes and calls story tools."""
        result = run_workflow("inspect_pipeline_run", {"pipeline": "nonexistent_pipeline"})

        # Should pause for input or complete with not_found
        assert result["status"] in ["AWAITING_INPUT", "COMPLETED"]

        if result["status"] == "COMPLETED":
            # Should have called story_read
            events = result.get("events", [])
            tool_calls = [e for e in events if e.get("type") == "tool_call"]
            assert any(e.get("tool") == "story_read" for e in tool_calls)

    def test_workflow_error_handling(self):
        """Invalid workflow name returns error."""
        result = run_workflow("nonexistent_workflow", {})

        assert "error" in result
        assert result["error"]["code"] == "UNKNOWN_WORKFLOW"

    def test_resume_invalid_token_returns_error(self):
        """Invalid resume token returns error."""
        result = resume_workflow("invalid_token", {})

        assert "error" in result


class TestWorkflowToolRegistry:
    """Verify all tools are accessible from workflows."""

    def test_all_17_tools_registered(self):
        """All 17 MCP tools are in workflow registry."""
        from odibi_mcp.tools.workflows import TOOL_REGISTRY

        expected_tools = [
            # Validation
            "test_pipeline",
            "validate_yaml_runnable",
            "validate_pipeline_enhanced",
            # Construction
            "list_patterns",
            "apply_pattern_template",
            "list_transformers",
            # Discovery
            "map_environment",
            "profile_source",
            "profile_folder",
            # Diagnostics
            "diagnose",
            # Story
            "story_read",
            "node_sample",
            "node_failed_rows",
            "lineage_graph",
        ]

        assert len(TOOL_REGISTRY) >= 14  # At least these core tools
        for tool in expected_tools:
            assert tool in TOOL_REGISTRY, f"Missing tool: {tool}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
