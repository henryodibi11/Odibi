"""Tests for disabled node filtering in Pipeline.run()."""

from unittest.mock import MagicMock, patch

import pytest

from odibi.config import NodeConfig, PipelineConfig
from odibi.pipeline import Pipeline


def make_node(name, enabled=True, depends_on=None):
    return NodeConfig(
        name=name,
        enabled=enabled,
        depends_on=depends_on or [],
        read={"connection": "dummy", "format": "csv", "path": "dummy.csv"},
    )


def make_pipeline(nodes):
    return PipelineConfig(pipeline="test", nodes=nodes)


@pytest.fixture
def disabled_pipeline():
    """Pipeline with one disabled node."""
    return make_pipeline([make_node("node_a", enabled=False)])


@pytest.fixture
def mixed_pipeline():
    """Pipeline with enabled and disabled nodes."""
    return make_pipeline(
        [
            make_node("enabled_node", enabled=True),
            make_node("disabled_node", enabled=False),
        ]
    )


@pytest.fixture
def dependent_pipeline():
    """Pipeline where an enabled node depends on a disabled node."""
    return make_pipeline(
        [
            make_node("disabled_parent", enabled=False),
            make_node("child_node", enabled=True, depends_on=["disabled_parent"]),
        ]
    )


class TestDisabledNodeFiltering:
    """Verify that enabled: false nodes are skipped during execution."""

    def _run_pipeline(self, config, parallel=False):
        with patch("odibi.pipeline.create_context") as mock_ctx:
            mock_ctx.return_value = MagicMock()
            pipeline = Pipeline(
                pipeline_config=config,
                engine="pandas",
                connections={"dummy": MagicMock()},
            )
            with patch.object(pipeline, "_send_alerts"):
                with patch("odibi.pipeline.Node") as mock_node_cls:
                    mock_node_instance = MagicMock()
                    mock_result = MagicMock()
                    mock_result.success = True
                    mock_result.duration = 0.1
                    mock_result.rows_processed = 10
                    mock_result.metadata = {}
                    mock_result.error = None
                    mock_node_instance.execute.return_value = mock_result
                    mock_node_cls.return_value = mock_node_instance

                    results = pipeline.run(parallel=parallel)
                    return results, mock_node_cls

    def test_disabled_node_skipped_serial(self, disabled_pipeline):
        results, mock_node_cls = self._run_pipeline(disabled_pipeline, parallel=False)
        assert "node_a" in results.skipped
        assert "node_a" not in results.completed
        mock_node_cls.assert_not_called()

    def test_disabled_node_skipped_parallel(self, disabled_pipeline):
        results, mock_node_cls = self._run_pipeline(disabled_pipeline, parallel=True)
        assert "node_a" in results.skipped
        assert "node_a" not in results.completed
        mock_node_cls.assert_not_called()

    def test_mixed_enabled_disabled_serial(self, mixed_pipeline):
        results, mock_node_cls = self._run_pipeline(mixed_pipeline, parallel=False)
        assert "disabled_node" in results.skipped
        assert "disabled_node" not in results.completed
        assert "enabled_node" in results.completed

    def test_mixed_enabled_disabled_parallel(self, mixed_pipeline):
        results, mock_node_cls = self._run_pipeline(mixed_pipeline, parallel=True)
        assert "disabled_node" in results.skipped
        assert "disabled_node" not in results.completed
        assert "enabled_node" in results.completed

    def test_dependent_of_disabled_also_skipped_serial(self, dependent_pipeline):
        results, _ = self._run_pipeline(dependent_pipeline, parallel=False)
        assert "disabled_parent" in results.skipped
        assert "child_node" in results.skipped
        assert results.completed == []

    def test_dependent_of_disabled_also_skipped_parallel(self, dependent_pipeline):
        results, _ = self._run_pipeline(dependent_pipeline, parallel=True)
        assert "disabled_parent" in results.skipped
        assert "child_node" in results.skipped
        assert results.completed == []
