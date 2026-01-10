"""Tests for PipelineResults and debug_summary functionality."""

from odibi.pipeline import PipelineResults
from odibi.node import NodeResult


class TestPipelineResultsDebugSummary:
    """Test debug_summary() method for meaningful output."""

    def test_debug_summary_empty_on_success(self):
        """debug_summary should return empty string when no failures."""
        results = PipelineResults(
            pipeline_name="test_pipeline",
            completed=["node1", "node2"],
            failed=[],
            skipped=[],
        )

        assert results.debug_summary() == ""

    def test_debug_summary_shows_failed_nodes(self):
        """debug_summary should list all failed nodes."""
        results = PipelineResults(
            pipeline_name="test_pipeline",
            completed=["node1"],
            failed=["node2", "node3"],
            skipped=[],
        )

        summary = results.debug_summary()

        assert "node2" in summary
        assert "node3" in summary
        assert "test_pipeline" in summary

    def test_debug_summary_includes_error_message(self):
        """debug_summary should show truncated error messages."""
        node_result = NodeResult(
            node_name="failing_node",
            success=False,
            duration=1.5,
            error=Exception("Connection timeout after 30 seconds"),
        )

        results = PipelineResults(
            pipeline_name="test_pipeline",
            completed=[],
            failed=["failing_node"],
            node_results={"failing_node": node_result},
        )

        summary = results.debug_summary()

        assert "failing_node" in summary
        assert "Connection timeout" in summary

    def test_debug_summary_includes_story_path(self):
        """debug_summary should show story path when available."""
        results = PipelineResults(
            pipeline_name="test_pipeline",
            completed=[],
            failed=["node1"],
            story_path="stories/2026-01-10_run.json",
        )

        summary = results.debug_summary()

        assert "stories/2026-01-10_run.json" in summary
        assert "odibi story show" in summary

    def test_debug_summary_suggests_story_last_with_node(self):
        """debug_summary should suggest odibi story last --node for failed node."""
        results = PipelineResults(
            pipeline_name="test_pipeline",
            completed=[],
            failed=["dim_customer"],
            story_path="stories/run.json",
        )

        summary = results.debug_summary()

        assert "odibi story last --node dim_customer" in summary

    def test_debug_summary_suggests_doctor_command(self):
        """debug_summary should suggest odibi doctor for environment issues."""
        results = PipelineResults(
            pipeline_name="test_pipeline",
            completed=[],
            failed=["node1"],
        )

        summary = results.debug_summary()

        assert "odibi doctor" in summary

    def test_debug_summary_truncates_long_errors(self):
        """debug_summary should truncate very long error messages."""
        long_error = "A" * 500  # Very long error message
        node_result = NodeResult(
            node_name="failing_node",
            success=False,
            duration=1.0,
            error=Exception(long_error),
        )

        results = PipelineResults(
            pipeline_name="test_pipeline",
            completed=[],
            failed=["failing_node"],
            node_results={"failing_node": node_result},
        )

        summary = results.debug_summary()

        # Error should be truncated to 200 chars
        assert len(summary) < len(long_error) + 500  # Some overhead for formatting

    def test_debug_summary_handles_no_story_path(self):
        """debug_summary should work when story_path is None."""
        results = PipelineResults(
            pipeline_name="test_pipeline",
            completed=[],
            failed=["node1"],
            story_path=None,
        )

        summary = results.debug_summary()

        assert "Next Steps" in summary
        assert "logs" in summary.lower()  # Should mention checking logs


class TestPipelineResultsBasics:
    """Test basic PipelineResults functionality."""

    def test_to_dict(self):
        """to_dict should return complete dictionary."""
        results = PipelineResults(
            pipeline_name="test_pipeline",
            completed=["node1"],
            failed=["node2"],
            skipped=["node3"],
            duration=10.5,
            start_time="2026-01-10T10:00:00",
            end_time="2026-01-10T10:00:10",
        )

        d = results.to_dict()

        assert d["pipeline_name"] == "test_pipeline"
        assert d["completed"] == ["node1"]
        assert d["failed"] == ["node2"]
        assert d["skipped"] == ["node3"]
        assert d["duration"] == 10.5

    def test_get_node_result(self):
        """get_node_result should return NodeResult for known node."""
        node_result = NodeResult(node_name="node1", success=True, duration=0.5)
        results = PipelineResults(
            pipeline_name="test_pipeline",
            node_results={"node1": node_result},
        )

        assert results.get_node_result("node1") == node_result
        assert results.get_node_result("unknown") is None
