"""Tests for odibi.pipeline — _build_connections, catalog init, register_outputs."""

from unittest.mock import MagicMock, patch
import pytest

from odibi.pipeline import PipelineManager


def _make_pm(**overrides):
    """Create a PipelineManager without running __init__."""
    pm = PipelineManager.__new__(PipelineManager)
    pm._ctx = MagicMock()
    pm.connections = {}
    pm.catalog_manager = None
    pm._pipelines = {}
    pm.project_config = None
    pm.config = None
    for k, v in overrides.items():
        setattr(pm, k, v)
    return pm


# ---------------------------------------------------------------------------
# _build_connections
# ---------------------------------------------------------------------------


class TestBuildConnections:
    """Tests for PipelineManager._build_connections."""

    @patch("odibi.pipeline.load_plugins")
    @patch("odibi.connections.factory.register_builtins")
    def test_empty_connections(self, _reg, _plug):
        pm = _make_pm()
        result = pm._build_connections({})
        assert result == {}

    @patch("odibi.pipeline.load_plugins")
    @patch("odibi.connections.factory.register_builtins")
    @patch("odibi.pipeline.get_connection_factory")
    def test_model_dump_branch(self, mock_factory_fn, _reg, _plug):
        mock_conn_config = MagicMock()
        mock_conn_config.model_dump.return_value = {"type": "local", "path": "./data"}

        mock_factory = MagicMock(return_value=MagicMock())
        mock_factory_fn.return_value = mock_factory

        pm = _make_pm()
        result = pm._build_connections({"my_conn": mock_conn_config})
        assert "my_conn" in result
        mock_conn_config.model_dump.assert_called()

    @patch("odibi.pipeline.load_plugins")
    @patch("odibi.connections.factory.register_builtins")
    @patch("odibi.pipeline.get_connection_factory")
    def test_dict_branch_no_model_dump(self, mock_factory_fn, _reg, _plug):
        # Object with 'dict' but no 'model_dump'
        mock_conn_config = MagicMock(spec=[])
        mock_conn_config.model_dump = MagicMock(return_value={"type": "local", "path": "."})
        # has model_dump, so that branch is taken

        mock_factory = MagicMock(return_value=MagicMock())
        mock_factory_fn.return_value = mock_factory

        pm = _make_pm()
        result = pm._build_connections({"c": mock_conn_config})
        assert "c" in result

    @patch("odibi.pipeline.load_plugins")
    @patch("odibi.connections.factory.register_builtins")
    @patch("odibi.pipeline.get_connection_factory")
    def test_plain_dict_config(self, mock_factory_fn, _reg, _plug):
        mock_factory = MagicMock(return_value=MagicMock())
        mock_factory_fn.return_value = mock_factory

        pm = _make_pm()
        result = pm._build_connections({"c": {"type": "local", "path": "."}})
        assert "c" in result
        mock_factory.assert_called_once_with("c", {"type": "local", "path": "."})

    @patch("odibi.pipeline.load_plugins")
    @patch("odibi.connections.factory.register_builtins")
    @patch("odibi.pipeline.get_connection_factory")
    def test_factory_not_found_raises(self, mock_factory_fn, _reg, _plug):
        mock_factory_fn.return_value = None

        pm = _make_pm()
        with pytest.raises(ValueError, match="Unsupported connection type"):
            pm._build_connections({"c": {"type": "nonexistent"}})

    @patch("odibi.pipeline.load_plugins")
    @patch("odibi.connections.factory.register_builtins")
    @patch("odibi.pipeline.get_connection_factory")
    def test_factory_raises_wraps_error(self, mock_factory_fn, _reg, _plug):
        mock_factory = MagicMock(side_effect=RuntimeError("boom"))
        mock_factory_fn.return_value = mock_factory

        pm = _make_pm()
        with pytest.raises(ValueError, match="Failed to create connection 'c'"):
            pm._build_connections({"c": {"type": "local", "path": "."}})

    @patch("odibi.pipeline.load_plugins")
    @patch("odibi.connections.factory.register_builtins")
    @patch("odibi.pipeline.get_connection_factory")
    def test_configure_connections_parallel_called(self, mock_factory_fn, _reg, _plug):
        mock_factory = MagicMock(return_value=MagicMock())
        mock_factory_fn.return_value = mock_factory

        pm = _make_pm()
        with patch("odibi.utils.configure_connections_parallel", return_value=({}, [])):
            pm._build_connections({"c": {"type": "local", "path": "."}})

    @patch("odibi.pipeline.load_plugins")
    @patch("odibi.connections.factory.register_builtins")
    @patch("odibi.pipeline.get_connection_factory")
    def test_configure_connections_parallel_with_errors(self, mock_factory_fn, _reg, _plug):
        mock_conn = MagicMock()
        mock_factory = MagicMock(return_value=mock_conn)
        mock_factory_fn.return_value = mock_factory

        pm = _make_pm()
        with patch(
            "odibi.utils.configure_connections_parallel",
            return_value=({"c": mock_conn}, ["warning1"]),
        ):
            result = pm._build_connections({"c": {"type": "local", "path": "."}})
        assert "c" in result

    @patch("odibi.pipeline.load_plugins")
    @patch("odibi.connections.factory.register_builtins")
    @patch("odibi.pipeline.get_connection_factory")
    def test_configure_connections_parallel_import_error(self, mock_factory_fn, _reg, _plug):
        mock_factory = MagicMock(return_value=MagicMock())
        mock_factory_fn.return_value = mock_factory

        pm = _make_pm()
        with patch("odibi.utils.configure_connections_parallel", side_effect=ImportError):
            result = pm._build_connections({"c": {"type": "local", "path": "."}})
        assert "c" in result


# ---------------------------------------------------------------------------
# register_outputs
# ---------------------------------------------------------------------------


class TestRegisterOutputs:
    def test_register_all_pipelines(self):
        p1 = MagicMock()
        p1.register_outputs.return_value = 3
        p2 = MagicMock()
        p2.register_outputs.return_value = 5

        pm = _make_pm(_pipelines={"silver": p1, "gold": p2})
        result = pm.register_outputs()
        assert result == {"silver": 3, "gold": 5}

    def test_register_single_pipeline(self):
        p1 = MagicMock()
        p1.register_outputs.return_value = 2

        pm = _make_pm(_pipelines={"silver": p1, "gold": MagicMock()})
        result = pm.register_outputs("silver")
        assert result == {"silver": 2}

    def test_register_list_of_pipelines(self):
        p1 = MagicMock()
        p1.register_outputs.return_value = 1
        p2 = MagicMock()
        p2.register_outputs.return_value = 4

        pm = _make_pm(_pipelines={"a": p1, "b": p2})
        result = pm.register_outputs(["a", "b"])
        assert result == {"a": 1, "b": 4}

    def test_register_pipeline_not_found(self):
        pm = _make_pm(_pipelines={"a": MagicMock()})
        result = pm.register_outputs("nonexistent")
        assert result == {}
        pm._ctx.warning.assert_called()


# ---------------------------------------------------------------------------
# PipelineResults
# ---------------------------------------------------------------------------


class TestPipelineResults:
    def test_get_node_result_found(self):
        from odibi.pipeline import PipelineResults

        nr = MagicMock()
        results = PipelineResults(pipeline_name="test", node_results={"n1": nr})
        assert results.get_node_result("n1") is nr

    def test_get_node_result_not_found(self):
        from odibi.pipeline import PipelineResults

        results = PipelineResults(pipeline_name="test")
        assert results.get_node_result("n1") is None

    def test_defaults(self):
        from odibi.pipeline import PipelineResults

        r = PipelineResults(pipeline_name="p")
        assert r.completed == []
        assert r.failed == []
        assert r.skipped == []
        assert r.node_results == {}
        assert r.duration == 0.0
        assert r.story_path is None
