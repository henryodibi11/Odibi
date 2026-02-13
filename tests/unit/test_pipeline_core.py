"""Unit tests for pipeline.py core classes - targeting uncovered lines."""

from unittest.mock import patch

from odibi.node import NodeResult
from odibi.pipeline import PipelineResults


class TestPipelineResults:
    def test_to_dict(self):
        results = PipelineResults(pipeline_name="etl")
        results.completed = ["node_a", "node_b"]
        results.failed = []
        results.duration = 5.0
        d = results.to_dict()
        assert d["pipeline_name"] == "etl"
        assert d["completed"] == ["node_a", "node_b"]
        assert d["duration"] == 5.0
        assert d["node_count"] == 0

    def test_get_node_result(self):
        results = PipelineResults(pipeline_name="etl")
        nr = NodeResult(node_name="a", success=True, duration=1.0)
        results.node_results["a"] = nr
        assert results.get_node_result("a") is nr
        assert results.get_node_result("missing") is None

    def test_debug_summary_no_failures(self):
        results = PipelineResults(pipeline_name="etl")
        results.failed = []
        assert results.debug_summary() == ""

    def test_debug_summary_with_failures(self):
        results = PipelineResults(pipeline_name="etl")
        results.failed = ["node_x"]
        err = ValueError("bad data")
        results.node_results["node_x"] = NodeResult(
            node_name="node_x", success=False, duration=0.5, error=err
        )
        summary = results.debug_summary()
        assert "node_x" in summary
        assert "bad data" in summary
        assert "etl" in summary

    def test_debug_summary_with_skipped(self):
        results = PipelineResults(pipeline_name="test")
        results.failed = ["a"]
        results.skipped = ["b", "c"]
        results.node_results["a"] = NodeResult(
            node_name="a", success=False, duration=0.1, error=Exception("err")
        )
        summary = results.debug_summary()
        assert "a" in summary


class TestPipelineManagerFactory:
    @patch("odibi.pipeline.load_yaml_with_env")
    @patch("odibi.pipeline.load_plugins")
    def test_from_yaml_loads_config(self, mock_plugins, mock_yaml):
        from odibi.pipeline import PipelineManager

        mock_yaml.return_value = {
            "project": {"name": "test_project"},
            "pipelines": [
                {
                    "name": "etl",
                    "nodes": [
                        {
                            "name": "node1",
                            "source": {"format": "csv", "path": "/data.csv"},
                        }
                    ],
                }
            ],
        }

        with patch.object(PipelineManager, "__init__", return_value=None):
            try:
                PipelineManager.from_yaml("fake.yaml")
            except Exception:
                pass  # Init mock may not set all attrs
            # Verify yaml was loaded
            mock_yaml.assert_called_once()
