"""Tests for workflow engine Phase 2: Construction workflows."""

import pytest

from odibi_mcp.tools.workflows import run_workflow, resume_workflow, list_workflows


class TestBuildAndValidateWorkflow:
    """Test build_and_validate workflow."""

    def test_build_with_all_params_completes(self):
        """Test workflow completes when all params provided."""
        result = run_workflow(
            "build_and_validate",
            {
                "pattern": "dimension",
                "pipeline_name": "dim_test",
                "source_connection": "local",
                "target_connection": "local",
                "target_path": "gold/test",
                "source_table": "test.csv",
                "natural_key": "id",
                "surrogate_key": "test_sk",
            },
        )

        assert result["status"] == "COMPLETED"
        assert result["progress"] == 100.0

        # Should have generated YAML
        yaml = result["outputs"].get("yaml")
        assert yaml is not None
        assert len(yaml) > 0
        assert "dim_test" in yaml

    def test_build_pauses_for_missing_params(self):
        """Test workflow pauses when params are missing."""
        result = run_workflow("build_and_validate", {})

        assert result["status"] == "AWAITING_INPUT"
        assert "resume_token" in result
        assert "prompts" in result
        assert len(result["prompts"]) == 6  # Basic params

    def test_build_pauses_for_pattern_specific_params(self):
        """Test workflow asks for pattern-specific params (dimension)."""
        # Provide basic params but not dimension-specific
        result = run_workflow(
            "build_and_validate",
            {
                "pattern": "dimension",
                "pipeline_name": "test",
                "source_connection": "local",
                "target_connection": "local",
                "target_path": "gold/test",
                "source_table": "test.csv",
            },
        )

        assert result["status"] == "AWAITING_INPUT"
        assert "prompts" in result

        # Should ask for natural_key and surrogate_key
        prompt_paths = {p["path"] for p in result["prompts"]}
        assert "params.natural_key" in prompt_paths
        assert "params.surrogate_key" in prompt_paths

    def test_build_scd2_asks_for_scd2_params(self):
        """Test workflow asks for SCD2-specific params."""
        result = run_workflow(
            "build_and_validate",
            {
                "pattern": "scd2",
                "pipeline_name": "scd2_test",
                "source_connection": "local",
                "target_connection": "local",
                "target_path": "gold/scd2_test",
                "source_table": "test.csv",
            },
        )

        assert result["status"] == "AWAITING_INPUT"

        # Should ask for keys and tracked_columns
        prompt_paths = {p["path"] for p in result["prompts"]}
        assert "params.keys" in prompt_paths
        assert "params.tracked_columns" in prompt_paths

    def test_resume_with_inputs_continues(self):
        """Test resuming workflow with provided inputs."""
        # Start workflow
        r1 = run_workflow("build_and_validate", {"pattern": "dimension"})
        assert r1["status"] == "AWAITING_INPUT"

        # Resume with remaining params
        r2 = resume_workflow(
            r1["resume_token"],
            {
                "params.pipeline_name": "test",
                "params.source_connection": "local",
                "params.target_connection": "local",
                "params.target_path": "gold/test",
                "params.source_table": "test.csv",
            },
        )

        # Should now ask for dimension-specific params
        assert r2["status"] == "AWAITING_INPUT"
        assert "params.natural_key" in {p["path"] for p in r2["prompts"]}

        # Resume again with dimension params
        r3 = resume_workflow(
            r2["resume_token"],
            {
                "params.natural_key": "id",
                "params.surrogate_key": "test_sk",
            },
        )

        # Should complete
        assert r3["status"] == "COMPLETED"
        assert r3["outputs"].get("yaml") is not None


class TestDebugPipelineWorkflow:
    """Test debug_pipeline workflow."""

    def test_debug_with_valid_yaml(self):
        """Test debug workflow with valid YAML."""
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

        result = run_workflow("debug_pipeline", {"yaml": test_yaml})

        assert result["status"] == "COMPLETED"
        # Should have validation result
        assert "results" in result.get("state", {})

    def test_debug_pauses_without_yaml(self):
        """Test debug workflow pauses when YAML not provided."""
        result = run_workflow("debug_pipeline", {})

        assert result["status"] == "AWAITING_INPUT"
        assert any(p["path"] == "params.yaml" for p in result["prompts"])


class TestWorkflowListing:
    """Test workflow discovery."""

    def test_list_workflows_includes_phase2(self):
        """Test all Phase 2 workflows are listed."""
        result = list_workflows()

        workflow_names = {w["name"] for w in result["workflows"]}
        assert "validate_yaml_simple" in workflow_names  # Phase 1
        assert "build_and_validate" in workflow_names  # Phase 2
        assert "debug_pipeline" in workflow_names  # Phase 2


class TestErrorHandling:
    """Test error handling."""

    def test_unknown_workflow_returns_error(self):
        """Test running unknown workflow returns error."""
        result = run_workflow("nonexistent_workflow", {})

        assert "error" in result
        assert result["error"]["code"] == "UNKNOWN_WORKFLOW"
        assert "available" in result

    def test_invalid_resume_token_returns_error(self):
        """Test invalid resume token returns error."""
        result = resume_workflow("invalid_token", {})

        assert "error" in result
        assert result["error"]["code"] == "INVALID_TOKEN"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
