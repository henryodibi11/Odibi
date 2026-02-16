"""Tests for pipeline.py - Pipeline and PipelineManager coverage gaps.

Covers: PipelineResults, Pipeline init/run/validate, PipelineManager methods,
batch writes, alerting, flush_stories, deploy, list/query, path resolution.
Avoids Spark/Delta in test names per Windows filtering rules.
"""

import logging  # noqa: I001

logging.getLogger("odibi").propagate = False

import os  # noqa: E402
from unittest.mock import MagicMock, PropertyMock, patch  # noqa: E402

import pandas as pd  # noqa: E402
import pytest  # noqa: E402

from odibi.config import (  # noqa: E402
    AlertConfig,
    ErrorStrategy,
    NodeConfig,
    PipelineConfig,
)
from odibi.node import NodeResult  # noqa: E402
from odibi.pipeline import Pipeline, PipelineManager, PipelineResults  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def node_cfg_a():
    return NodeConfig(
        name="node_a",
        read={"connection": "src", "format": "csv", "path": "a.csv"},
        write={"connection": "dst", "format": "csv", "path": "out_a.csv"},
    )


@pytest.fixture
def node_cfg_b():
    return NodeConfig(
        name="node_b",
        depends_on=["node_a"],
        read={"connection": "src", "format": "csv", "path": "b.csv"},
        write={"connection": "dst", "format": "csv", "path": "out_b.csv"},
    )


@pytest.fixture
def simple_pipeline_config(node_cfg_a):
    return PipelineConfig(pipeline="test_pipe", nodes=[node_cfg_a])


@pytest.fixture
def multi_node_config(node_cfg_a, node_cfg_b):
    return PipelineConfig(pipeline="multi_pipe", nodes=[node_cfg_a, node_cfg_b])


@pytest.fixture
def mock_engine():
    engine = MagicMock()
    engine.name = "pandas"
    type(engine).spark = PropertyMock(return_value=None)
    return engine


@pytest.fixture
def connections():
    src = MagicMock()
    dst = MagicMock()
    dst.get_path.side_effect = lambda p: f"/data/{p}"
    return {"src": src, "dst": dst}


def _make_pipeline(config, connections, **kwargs):
    """Helper to build a Pipeline with heavy internals mocked."""
    with (
        patch("odibi.pipeline.get_engine_class") as mock_gec,
        patch("odibi.pipeline.create_context") as mock_cc,
        patch("odibi.pipeline.DependencyGraph") as mock_dg,
        patch("odibi.pipeline.StoryGenerator"),
    ):
        mock_engine = MagicMock()
        type(mock_engine).spark = PropertyMock(return_value=None)
        mock_gec.return_value = lambda **kw: mock_engine

        mock_ctx = MagicMock()
        mock_cc.return_value = mock_ctx

        mock_graph = MagicMock()
        nodes_dict = {n.name: n for n in config.nodes}
        mock_graph.nodes = nodes_dict
        mock_graph.topological_sort.return_value = list(nodes_dict.keys())
        mock_graph.get_execution_layers.return_value = [list(nodes_dict.keys())]
        mock_graph.get_dependents.return_value = set()
        mock_graph.get_dependencies.return_value = set()
        mock_graph.to_dict.return_value = {"nodes": [], "edges": []}
        mock_dg.return_value = mock_graph

        pipeline = Pipeline(
            pipeline_config=config,
            engine="pandas",
            connections=connections,
            **kwargs,
        )
        pipeline._mock_graph = mock_graph
        pipeline._mock_engine = mock_engine
        return pipeline


# ===========================================================================
# PipelineResults
# ===========================================================================


class TestPipelineResultsExtended:
    def test_to_dict_includes_node_count(self):
        nr = NodeResult(node_name="n1", success=True, duration=1.0)
        r = PipelineResults(
            pipeline_name="p",
            completed=["n1"],
            node_results={"n1": nr},
        )
        d = r.to_dict()
        assert d["node_count"] == 1

    def test_debug_summary_no_story_path(self):
        r = PipelineResults(
            pipeline_name="p",
            failed=["n1"],
            story_path=None,
        )
        s = r.debug_summary()
        assert "logs" in s.lower()
        assert "odibi doctor" in s

    def test_debug_summary_with_story_path_and_error(self):
        nr = NodeResult(
            node_name="bad",
            success=False,
            duration=0.5,
            error=Exception("boom"),
        )
        r = PipelineResults(
            pipeline_name="p",
            failed=["bad"],
            node_results={"bad": nr},
            story_path="/tmp/story.json",
        )
        s = r.debug_summary()
        assert "boom" in s
        assert "odibi story show /tmp/story.json" in s
        assert "odibi story last --node bad" in s

    def test_debug_summary_failed_node_without_result(self):
        r = PipelineResults(pipeline_name="p", failed=["orphan"])
        s = r.debug_summary()
        assert "orphan" in s

    def test_get_node_result_missing(self):
        r = PipelineResults(pipeline_name="p")
        assert r.get_node_result("x") is None


# ===========================================================================
# Pipeline __init__ / context manager
# ===========================================================================


class TestPipelineInit:
    def test_basic_init(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        assert p.config is simple_pipeline_config
        assert p.engine_type == "pandas"

    def test_context_manager(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        with p:
            pass

    def test_cleanup_connections_closes(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        p._cleanup_connections()
        for conn in connections.values():
            if hasattr(conn, "close"):
                conn.close.assert_called()

    def test_cleanup_connections_empty(self, simple_pipeline_config):
        p = _make_pipeline(simple_pipeline_config, {})
        p._cleanup_connections()

    def test_cleanup_connections_close_error(self, simple_pipeline_config, connections):
        connections["src"].close.side_effect = RuntimeError("fail")
        p = _make_pipeline(simple_pipeline_config, connections)
        p._cleanup_connections()

    def test_init_with_performance_config_model_dump(self, simple_pipeline_config, connections):
        perf = MagicMock()
        perf.model_dump.return_value = {"batch_size": 1000}
        p = _make_pipeline(simple_pipeline_config, connections, performance_config=perf)
        assert p.performance_config is perf

    def test_init_with_catalog_manager(self, simple_pipeline_config, connections):
        cat = MagicMock()
        p = _make_pipeline(simple_pipeline_config, connections, catalog_manager=cat)
        assert p.catalog_manager is cat


# ===========================================================================
# Pipeline.register_outputs
# ===========================================================================


class TestRegisterOutputs:
    def test_no_catalog(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        assert p.register_outputs() == 0

    def test_with_catalog(self, simple_pipeline_config, connections):
        cat = MagicMock()
        cat.register_outputs_from_config.return_value = 3
        p = _make_pipeline(simple_pipeline_config, connections, catalog_manager=cat)
        assert p.register_outputs() == 3


# ===========================================================================
# Pipeline.validate
# ===========================================================================


class TestPipelineValidate:
    def test_validate_passes(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        v = p.validate()
        assert v["valid"] is True
        assert v["node_count"] == 1

    def test_validate_missing_connection_warning(self, connections):
        cfg = PipelineConfig(
            pipeline="p",
            nodes=[
                NodeConfig(
                    name="n",
                    read={"connection": "missing_conn", "format": "csv", "path": "x.csv"},
                )
            ],
        )
        p = _make_pipeline(cfg, connections)
        v = p.validate()
        assert any("missing_conn" in w for w in v["warnings"])

    def test_validate_dependency_error(self, simple_pipeline_config, connections):
        from odibi.exceptions import DependencyError

        p = _make_pipeline(simple_pipeline_config, connections)
        p.graph.topological_sort.side_effect = DependencyError("cycle")
        v = p.validate()
        assert v["valid"] is False
        assert any("cycle" in e for e in v["errors"])


# ===========================================================================
# Pipeline.get_execution_layers / visualize
# ===========================================================================


class TestPipelineGraphMethods:
    def test_get_execution_layers(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        p.graph.get_execution_layers.return_value = [["node_a"]]
        assert p.get_execution_layers() == [["node_a"]]

    def test_visualize(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        p.graph.visualize.return_value = "graph viz"
        assert p.visualize() == "graph viz"


# ===========================================================================
# Pipeline.run_node
# ===========================================================================


class TestRunNode:
    def test_run_node_not_found(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        with pytest.raises(ValueError, match="not found"):
            p.run_node("nonexistent")

    @patch("odibi.pipeline.Node")
    def test_run_node_success(self, mock_node_cls, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.5
        )
        mock_node_cls.return_value = mock_instance

        result = p.run_node("node_a")
        assert result.success is True

    @patch("odibi.pipeline.Node")
    def test_run_node_with_mock_data(self, mock_node_cls, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.1
        )
        mock_node_cls.return_value = mock_instance

        result = p.run_node("node_a", mock_data={"upstream": pd.DataFrame({"x": [1]})})
        assert result.success is True
        p.context.register.assert_called_once()


# ===========================================================================
# Pipeline._send_alerts
# ===========================================================================


class TestSendAlerts:
    def test_send_alert_on_start(self, simple_pipeline_config, connections):
        alert = MagicMock(spec=AlertConfig)
        alert.on_events = ["on_start"]
        p = _make_pipeline(simple_pipeline_config, connections, alerts=[alert])
        results = PipelineResults(pipeline_name="p")
        with patch("odibi.pipeline.send_alert") as mock_send:
            p._send_alerts("on_start", results)
            mock_send.assert_called_once()
            ctx = mock_send.call_args[0][2]
            assert ctx["status"] == "STARTED"

    def test_send_alert_on_failure(self, simple_pipeline_config, connections):
        alert = MagicMock(spec=AlertConfig)
        alert.on_events = ["on_failure"]
        p = _make_pipeline(simple_pipeline_config, connections, alerts=[alert])
        results = PipelineResults(pipeline_name="p", failed=["n1"])
        with patch("odibi.pipeline.send_alert") as mock_send:
            p._send_alerts("on_failure", results)
            mock_send.assert_called_once()

    def test_send_alert_skips_non_matching_event(self, simple_pipeline_config, connections):
        alert = MagicMock(spec=AlertConfig)
        alert.on_events = ["on_success"]
        p = _make_pipeline(simple_pipeline_config, connections, alerts=[alert])
        results = PipelineResults(pipeline_name="p")
        with patch("odibi.pipeline.send_alert") as mock_send:
            p._send_alerts("on_start", results)
            mock_send.assert_not_called()


# ===========================================================================
# Pipeline.flush_stories
# ===========================================================================


class TestFlushStories:
    def test_no_pending_story(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        assert p.flush_stories() is None

    def test_pending_story_success(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        future = MagicMock()
        future.result.return_value = "/path/story.json"
        p._story_future = future
        p._story_executor = MagicMock()

        result = p.flush_stories(timeout=5.0)
        assert result == "/path/story.json"
        assert p._story_future is None

    def test_pending_story_failure(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        future = MagicMock()
        future.result.side_effect = RuntimeError("gen failed")
        p._story_future = future
        p._story_executor = MagicMock()

        result = p.flush_stories()
        assert result is None
        assert p._story_future is None


# ===========================================================================
# Pipeline.buffer_* and _flush_batch_writes
# ===========================================================================


class TestBatchBuffers:
    def test_buffer_lineage_record(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        p.buffer_lineage_record({"source_table": "a", "target_table": "b"})
        assert len(p._pending_lineage_records) == 1

    def test_buffer_asset_record(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        p.buffer_asset_record({"table_name": "t"})
        assert len(p._pending_asset_records) == 1

    def test_buffer_hwm_update(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        p.buffer_hwm_update("hwm_key", "2024-01-01")
        assert len(p._pending_hwm_updates) == 1
        assert p._pending_hwm_updates[0] == {"key": "hwm_key", "value": "2024-01-01"}

    def test_flush_no_catalog(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        p._pending_lineage_records = [{"a": 1}]
        p._flush_batch_writes()
        assert p._pending_lineage_records == [{"a": 1}]

    def test_flush_lineage_success(self, simple_pipeline_config, connections):
        cat = MagicMock()
        p = _make_pipeline(simple_pipeline_config, connections, catalog_manager=cat)
        p._pending_lineage_records = [{"source_table": "a"}]
        p._flush_batch_writes()
        cat.record_lineage_batch.assert_called_once()
        assert p._pending_lineage_records == []

    def test_flush_lineage_error(self, simple_pipeline_config, connections):
        cat = MagicMock()
        cat.record_lineage_batch.side_effect = RuntimeError("fail")
        p = _make_pipeline(simple_pipeline_config, connections, catalog_manager=cat)
        p._pending_lineage_records = [{"a": 1}]
        p._flush_batch_writes()
        assert p._pending_lineage_records == []

    def test_flush_assets_success(self, simple_pipeline_config, connections):
        cat = MagicMock()
        p = _make_pipeline(simple_pipeline_config, connections, catalog_manager=cat)
        p._pending_asset_records = [{"table_name": "t"}]
        p._flush_batch_writes()
        cat.register_assets_batch.assert_called_once()
        assert p._pending_asset_records == []

    def test_flush_assets_error(self, simple_pipeline_config, connections):
        cat = MagicMock()
        cat.register_assets_batch.side_effect = RuntimeError("fail")
        p = _make_pipeline(simple_pipeline_config, connections, catalog_manager=cat)
        p._pending_asset_records = [{"table_name": "t"}]
        p._flush_batch_writes()
        assert p._pending_asset_records == []

    @patch("odibi.pipeline.create_state_backend")
    @patch("odibi.pipeline.StateManager")
    def test_flush_hwm_success(self, mock_sm, mock_csb, simple_pipeline_config, connections):
        cat = MagicMock()
        p = _make_pipeline(simple_pipeline_config, connections, catalog_manager=cat)
        p.project_config = MagicMock()
        p._pending_hwm_updates = [{"key": "k", "value": "v"}]
        p._flush_batch_writes()
        mock_sm.return_value.set_hwm_batch.assert_called_once()
        assert p._pending_hwm_updates == []

    def test_flush_hwm_no_project_config(self, simple_pipeline_config, connections):
        cat = MagicMock()
        p = _make_pipeline(simple_pipeline_config, connections, catalog_manager=cat)
        p.project_config = None
        p._pending_hwm_updates = [{"key": "k", "value": "v"}]
        p._flush_batch_writes()
        assert p._pending_hwm_updates == []


# ===========================================================================
# Pipeline._get_databricks_*
# ===========================================================================


class TestDatabricksHelpers:
    def test_get_cluster_id_no_engine(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        assert p._get_databricks_cluster_id() is None

    def test_get_job_id_no_engine(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        assert p._get_databricks_job_id() is None

    def test_get_workspace_id_from_env(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        with patch.dict(os.environ, {"DATABRICKS_WORKSPACE_ID": "ws-123"}):
            assert p._get_databricks_workspace_id() == "ws-123"

    def test_get_workspace_id_from_host_url(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        with patch.dict(
            os.environ,
            {"DATABRICKS_HOST": "https://adb-12345.1.azuredatabricks.net"},
            clear=False,
        ):
            os.environ.pop("DATABRICKS_WORKSPACE_ID", None)
            result = p._get_databricks_workspace_id()
            assert result == "adb-12345"

    def test_get_workspace_id_none(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        with patch.dict(os.environ, {}, clear=True):
            assert p._get_databricks_workspace_id() is None


# ===========================================================================
# Pipeline._sync_catalog_if_configured
# ===========================================================================


class TestSyncCatalog:
    def test_no_catalog(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        p._sync_catalog_if_configured()

    def test_no_project_config(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        p.catalog_manager = MagicMock()
        p.project_config = None
        p._sync_catalog_if_configured()

    def test_no_sync_to(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        p.catalog_manager = MagicMock()
        p.project_config = MagicMock()
        p.project_config.system.sync_to = None
        p._sync_catalog_if_configured()

    def test_sync_not_after_run(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        p.catalog_manager = MagicMock()
        p.project_config = MagicMock()
        p.project_config.system.sync_to.on = "manual"
        p._sync_catalog_if_configured()

    @patch("odibi.pipeline.CatalogSyncer", create=True)
    def test_sync_connection_not_found(self, mock_syncer, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        p.catalog_manager = MagicMock()
        p.project_config = MagicMock()
        p.project_config.system.sync_to.on = "after_run"
        p.project_config.system.sync_to.connection = "missing_conn"
        with patch("odibi.pipeline.CatalogSyncer"):
            p._sync_catalog_if_configured()


# ===========================================================================
# Pipeline.run - serial execution paths
# ===========================================================================


class TestPipelineRun:
    @patch("odibi.pipeline.Node")
    def test_run_serial_success(self, mock_node_cls, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections, generate_story=False)
        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.5
        )
        mock_node_cls.return_value = mock_instance

        results = p.run()
        assert results.pipeline_name == "test_pipe"
        assert "node_a" in results.completed
        assert results.failed == []

    @patch("odibi.pipeline.Node")
    def test_run_serial_failure_default(self, mock_node_cls, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections, generate_story=False)
        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=False, duration=0.5
        )
        mock_node_cls.return_value = mock_instance

        results = p.run()
        assert "node_a" in results.failed

    @patch("odibi.pipeline.Node")
    def test_run_serial_fail_fast(self, mock_node_cls, multi_node_config, connections):
        p = _make_pipeline(multi_node_config, connections, generate_story=False)
        p.graph.nodes["node_a"].on_error = ErrorStrategy.FAIL_FAST

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=False, duration=0.1
        )
        mock_node_cls.return_value = mock_instance

        results = p.run(on_error="fail_fast")
        assert "node_a" in results.failed

    @patch("odibi.pipeline.Node")
    def test_run_node_exception(self, mock_node_cls, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections, generate_story=False)
        mock_node_cls.side_effect = RuntimeError("kaboom")

        results = p.run()
        assert "node_a" in results.failed

    @patch("odibi.pipeline.Node")
    def test_run_with_tag_filter(self, mock_node_cls, connections):
        cfg = PipelineConfig(
            pipeline="p",
            nodes=[
                _make_node_config("tagged", tags=["gold"]),
                _make_node_config("untagged"),
            ],
        )
        p = _make_pipeline(cfg, connections, generate_story=False)
        p.graph.nodes["tagged"].tags = ["gold"]
        p.graph.nodes["untagged"].tags = []

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="tagged", success=True, duration=0.1
        )
        mock_node_cls.return_value = mock_instance

        results = p.run(tag="gold")
        assert "tagged" in results.completed or "tagged" in results.node_results

    @patch("odibi.pipeline.Node")
    def test_run_with_node_filter(self, mock_node_cls, multi_node_config, connections):
        p = _make_pipeline(multi_node_config, connections, generate_story=False)

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.1
        )
        mock_node_cls.return_value = mock_instance

        results = p.run(node="node_a")
        assert "node_a" in results.completed

    def test_run_with_node_filter_missing(self, multi_node_config, connections):
        p = _make_pipeline(multi_node_config, connections, generate_story=False)
        with pytest.raises(ValueError, match="not found"):
            p.run(node="nonexistent")

    @patch("odibi.pipeline.Node")
    def test_run_disabled_node(self, mock_node_cls, connections):
        cfg = PipelineConfig(
            pipeline="p",
            nodes=[
                _make_node_config("active"),
                _make_node_config("inactive", enabled=False),
            ],
        )
        p = _make_pipeline(cfg, connections, generate_story=False)
        p.graph.nodes["inactive"].enabled = False
        p.graph.nodes["active"].enabled = True

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="active", success=True, duration=0.1
        )
        mock_node_cls.return_value = mock_instance

        results = p.run()
        assert "inactive" in results.skipped

    @patch("odibi.pipeline.Node")
    def test_run_skipped_dependency(self, mock_node_cls, multi_node_config, connections):
        p = _make_pipeline(multi_node_config, connections, generate_story=False)
        p.graph.nodes["node_b"].depends_on = ["node_a"]

        call_count = [0]

        def mock_execute():
            call_count[0] += 1
            if call_count[0] == 1:
                return NodeResult(node_name="node_a", success=False, duration=0.1)
            return NodeResult(
                node_name="node_b",
                success=False,
                duration=0.0,
                metadata={"skipped": True, "reason": "dependency_failed"},
            )

        mock_instance = MagicMock()
        mock_instance.execute.side_effect = mock_execute
        mock_node_cls.return_value = mock_instance

        results = p.run()
        assert "node_a" in results.failed


# ===========================================================================
# Pipeline.run - batch catalog writes at end
# ===========================================================================


class TestRunCatalogEndWrites:
    @patch("odibi.pipeline.Node")
    def test_run_batch_logs_run_records(self, mock_node_cls, simple_pipeline_config, connections):
        cat = MagicMock()
        p = _make_pipeline(
            simple_pipeline_config,
            connections,
            catalog_manager=cat,
            generate_story=False,
        )
        mock_instance = MagicMock()
        nr = NodeResult(
            node_name="node_a",
            success=True,
            duration=0.5,
            metadata={"_run_record": {"node_name": "node_a"}},
        )
        mock_instance.execute.return_value = nr
        mock_node_cls.return_value = mock_instance

        p.run()
        cat.log_runs_batch.assert_called_once()

    @patch("odibi.pipeline.Node")
    def test_run_batch_output_records(self, mock_node_cls, simple_pipeline_config, connections):
        cat = MagicMock()
        p = _make_pipeline(
            simple_pipeline_config,
            connections,
            catalog_manager=cat,
            generate_story=False,
        )
        mock_instance = MagicMock()
        nr = NodeResult(
            node_name="node_a",
            success=True,
            duration=0.5,
            metadata={"_output_record": {"table": "out_a"}},
        )
        mock_instance.execute.return_value = nr
        mock_node_cls.return_value = mock_instance

        p.run()
        cat.register_outputs_batch.assert_called_once()

    @patch("odibi.pipeline.Node")
    def test_run_skip_run_logging(self, mock_node_cls, simple_pipeline_config, connections):
        cat = MagicMock()
        perf = MagicMock()
        perf.skip_run_logging = True
        p = _make_pipeline(
            simple_pipeline_config,
            connections,
            catalog_manager=cat,
            performance_config=perf,
            generate_story=False,
        )
        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.1
        )
        mock_node_cls.return_value = mock_instance

        p.run()
        cat.log_runs_batch.assert_not_called()


# ===========================================================================
# PipelineManager - lightweight method tests
# ===========================================================================


def _make_node_config(name, **kwargs):
    """Create a NodeConfig that passes validation (requires at least one action)."""
    defaults = {"read": {"connection": "src", "format": "csv", "path": f"{name}.csv"}}
    defaults.update(kwargs)
    return NodeConfig(name=name, **defaults)


def _make_manager(pipelines=None, catalog_manager=None, connections=None):
    """Build a PipelineManager with mocked internals."""
    project_config = MagicMock()
    project_config.project = "test_proj"
    project_config.engine = "pandas"
    project_config.pipelines = pipelines or []

    mock_logging = MagicMock()
    mock_logging.structured = False
    mock_logging.level.value = "INFO"
    project_config.logging = mock_logging

    project_config.lineage = MagicMock()
    project_config.system = None

    mock_story = MagicMock()
    mock_story.connection = "dst"
    mock_story.path = "stories"
    mock_story.auto_generate = True
    mock_story.max_sample_rows = 10
    mock_story.async_generation = False
    mock_story.docs = None
    mock_story.generate_lineage = False
    project_config.story = mock_story

    project_config.retry = None
    project_config.alerts = []
    project_config.performance = None

    conns = connections or {"dst": MagicMock()}
    if "dst" in conns:
        conns["dst"].get_path.side_effect = lambda p: f"/data/{p}"
    if hasattr(conns.get("dst"), "pandas_storage_options"):
        conns["dst"].pandas_storage_options.return_value = {}

    with (
        patch("odibi.pipeline.configure_logging"),
        patch("odibi.pipeline.OpenLineageAdapter"),
        patch("odibi.pipeline.Pipeline") as mock_pipe_cls,
    ):
        mock_pipes = {}
        for pc in pipelines or []:
            mp = MagicMock()
            mp.config = pc
            mp.run.return_value = PipelineResults(pipeline_name=pc.pipeline)
            mp.flush_stories.return_value = None
            mock_pipes[pc.pipeline] = mp

        mock_pipe_cls.side_effect = lambda **kw: mock_pipes.get(
            kw["pipeline_config"].pipeline, MagicMock()
        )

        mgr = PipelineManager(project_config, conns)
        mgr.catalog_manager = catalog_manager
        return mgr, mock_pipes


class TestPipelineManagerMethods:
    def test_list_pipelines_empty(self):
        mgr, _ = _make_manager()
        assert mgr.list_pipelines() == []

    def test_list_pipelines_with_data(self):
        pc = PipelineConfig(
            pipeline="p1",
            nodes=[_make_node_config("n1")],
        )
        mgr, _ = _make_manager(pipelines=[pc])
        assert "p1" in mgr.list_pipelines()

    def test_get_pipeline_not_found(self):
        mgr, _ = _make_manager()
        with pytest.raises(ValueError, match="not found"):
            mgr.get_pipeline("missing")

    def test_get_pipeline_found(self):
        pc = PipelineConfig(pipeline="p1", nodes=[_make_node_config("n1")])
        mgr, _ = _make_manager(pipelines=[pc])
        p = mgr.get_pipeline("p1")
        assert p is not None

    def test_deploy_no_catalog(self):
        mgr, _ = _make_manager()
        assert mgr.deploy() is False

    def test_deploy_with_catalog(self):
        pc = PipelineConfig(pipeline="p1", nodes=[_make_node_config("n1")])
        cat = MagicMock()
        mgr, _ = _make_manager(pipelines=[pc], catalog_manager=cat)
        result = mgr.deploy()
        assert result is True
        cat.register_pipeline.assert_called_once()

    def test_deploy_no_matching_pipelines(self):
        pc = PipelineConfig(pipeline="p1", nodes=[_make_node_config("n1")])
        cat = MagicMock()
        mgr, _ = _make_manager(pipelines=[pc], catalog_manager=cat)
        result = mgr.deploy(pipelines="nonexistent")
        assert result is False

    def test_deploy_failure(self):
        pc = PipelineConfig(pipeline="p1", nodes=[_make_node_config("n1")])
        cat = MagicMock()
        cat.bootstrap.side_effect = RuntimeError("fail")
        mgr, _ = _make_manager(pipelines=[pc], catalog_manager=cat)
        result = mgr.deploy()
        assert result is False


class TestPipelineManagerRun:
    def test_run_no_pipelines(self):
        mgr, _ = _make_manager()
        assert mgr.run() == {}

    def test_run_single_pipeline(self):
        pc = PipelineConfig(pipeline="p1", nodes=[_make_node_config("n1")])
        mgr, mocks = _make_manager(pipelines=[pc])
        result = mgr.run("p1")
        assert isinstance(result, PipelineResults)
        assert result.pipeline_name == "p1"

    def test_run_multiple_pipelines(self):
        pc1 = PipelineConfig(pipeline="p1", nodes=[_make_node_config("n1")])
        pc2 = PipelineConfig(pipeline="p2", nodes=[_make_node_config("n2")])
        mgr, _ = _make_manager(pipelines=[pc1, pc2])
        result = mgr.run(["p1", "p2"])
        assert isinstance(result, dict)
        assert "p1" in result and "p2" in result

    def test_run_pipeline_not_found(self):
        mgr, _ = _make_manager()
        with pytest.raises(ValueError, match="not found"):
            mgr.run("missing")

    def test_run_with_catalog_invalidates_cache(self):
        pc = PipelineConfig(pipeline="p1", nodes=[_make_node_config("n1")])
        cat = MagicMock()
        mgr, _ = _make_manager(pipelines=[pc], catalog_manager=cat)
        mgr.run("p1")
        cat.invalidate_cache.assert_called()


class TestPipelineManagerFlushStories:
    def test_flush_stories_none_pending(self):
        pc = PipelineConfig(pipeline="p1", nodes=[_make_node_config("n1")])
        mgr, _ = _make_manager(pipelines=[pc])
        result = mgr.flush_stories()
        assert result == {} or isinstance(result, dict)


class TestPipelineManagerRegisterOutputs:
    def test_register_outputs_all(self):
        pc = PipelineConfig(pipeline="p1", nodes=[_make_node_config("n1")])
        mgr, mocks = _make_manager(pipelines=[pc])
        mocks["p1"].register_outputs.return_value = 2
        result = mgr.register_outputs()
        assert result == {"p1": 2}

    def test_register_outputs_specific(self):
        pc = PipelineConfig(pipeline="p1", nodes=[_make_node_config("n1")])
        mgr, mocks = _make_manager(pipelines=[pc])
        mocks["p1"].register_outputs.return_value = 1
        result = mgr.register_outputs("p1")
        assert result == {"p1": 1}

    def test_register_outputs_missing_pipeline(self):
        mgr, _ = _make_manager()
        result = mgr.register_outputs("nope")
        assert result == {}


# ===========================================================================
# PipelineManager - list/query methods
# ===========================================================================


class TestPipelineManagerListQuery:
    def test_list_registered_pipelines_no_catalog(self):
        mgr, _ = _make_manager()
        df = mgr.list_registered_pipelines()
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_list_registered_nodes_no_catalog(self):
        mgr, _ = _make_manager()
        df = mgr.list_registered_nodes()
        assert isinstance(df, pd.DataFrame)

    def test_list_runs_no_catalog(self):
        mgr, _ = _make_manager()
        df = mgr.list_runs()
        assert isinstance(df, pd.DataFrame)

    def test_list_tables_no_catalog(self):
        mgr, _ = _make_manager()
        df = mgr.list_tables()
        assert isinstance(df, pd.DataFrame)

    def test_get_pipeline_status_no_catalog(self):
        mgr, _ = _make_manager()
        assert mgr.get_pipeline_status("p1") == {}

    def test_get_node_stats_no_catalog(self):
        mgr, _ = _make_manager()
        assert mgr.get_node_stats("n1") == {}

    def test_get_state_no_catalog(self):
        mgr, _ = _make_manager()
        assert mgr.get_state("key") is None

    def test_get_all_state_no_catalog(self):
        mgr, _ = _make_manager()
        df = mgr.get_all_state()
        assert isinstance(df, pd.DataFrame)

    def test_clear_state_no_catalog(self):
        mgr, _ = _make_manager()
        assert mgr.clear_state("key") is False

    def test_get_schema_history_no_catalog(self):
        mgr, _ = _make_manager()
        df = mgr.get_schema_history("table")
        assert isinstance(df, pd.DataFrame)

    def test_get_lineage_no_catalog(self):
        mgr, _ = _make_manager()
        df = mgr.get_lineage("table")
        assert isinstance(df, pd.DataFrame)


# ===========================================================================
# PipelineManager - path resolution
# ===========================================================================


class TestPathResolution:
    def test_is_full_path_abfss(self):
        mgr, _ = _make_manager()
        assert mgr._is_full_path("abfss://container@account.dfs.core.windows.net/path")

    def test_is_full_path_s3(self):
        mgr, _ = _make_manager()
        assert mgr._is_full_path("s3://bucket/path")

    def test_is_full_path_local(self):
        mgr, _ = _make_manager()
        assert mgr._is_full_path("C:/data/file.csv")
        assert mgr._is_full_path("/data/file.csv")

    def test_is_full_path_relative(self):
        mgr, _ = _make_manager()
        assert not mgr._is_full_path("bronze/table")

    def test_resolve_table_path_full(self):
        mgr, _ = _make_manager()
        assert mgr._resolve_table_path("s3://bucket/table") == "s3://bucket/table"

    def test_lookup_in_catalog_no_catalog(self):
        mgr, _ = _make_manager()
        assert mgr._lookup_in_catalog("table") is None

    def test_lookup_in_catalog_with_match(self):
        cat = MagicMock()
        cat._read_local_table.return_value = pd.DataFrame(
            {"table_name": ["my_table"], "path": ["/data/my_table"]}
        )
        mgr, _ = _make_manager(catalog_manager=cat)
        result = mgr._lookup_in_catalog("my_table")
        assert result == "/data/my_table"

    def test_lookup_in_catalog_dotted_name(self):
        cat = MagicMock()
        cat._read_local_table.return_value = pd.DataFrame(
            {"table_name": ["vw_table"], "path": ["/data/vw_table"]}
        )
        mgr, _ = _make_manager(catalog_manager=cat)
        result = mgr._lookup_in_catalog("schema.vw_table")
        assert result == "/data/vw_table"

    def test_lookup_in_catalog_no_match(self):
        cat = MagicMock()
        cat._read_local_table.return_value = pd.DataFrame(
            {"table_name": ["other"], "path": ["/data/other"]}
        )
        mgr, _ = _make_manager(catalog_manager=cat)
        assert mgr._lookup_in_catalog("missing") is None

    def test_lookup_in_catalog_empty_df(self):
        cat = MagicMock()
        cat._read_local_table.return_value = pd.DataFrame()
        mgr, _ = _make_manager(catalog_manager=cat)
        assert mgr._lookup_in_catalog("table") is None

    def test_lookup_in_catalog_exception(self):
        cat = MagicMock()
        cat._read_local_table.side_effect = RuntimeError("read fail")
        mgr, _ = _make_manager(catalog_manager=cat)
        assert mgr._lookup_in_catalog("table") is None


# ===========================================================================
# PipelineManager._auto_register_pipelines
# ===========================================================================


class TestAutoRegister:
    def test_no_catalog(self):
        mgr, _ = _make_manager()
        mgr._auto_register_pipelines(["p1"])

    def test_with_catalog_new_pipeline(self):
        pc = PipelineConfig(pipeline="p1", nodes=[_make_node_config("n1")])
        cat = MagicMock()
        cat.get_all_registered_pipelines.return_value = {}
        cat.get_all_registered_nodes.return_value = {}
        mgr, _ = _make_manager(pipelines=[pc], catalog_manager=cat)
        mgr._auto_register_pipelines(["p1"])
        cat.register_pipelines_batch.assert_called_once()
        cat.register_nodes_batch.assert_called_once()

    def test_with_catalog_unchanged_pipeline(self):
        pc = PipelineConfig(pipeline="p1", nodes=[_make_node_config("n1")])
        cat = MagicMock()
        import hashlib
        import json

        dump = pc.model_dump(mode="json")
        dump_str = json.dumps(dump, sort_keys=True)
        pipeline_hash = hashlib.md5(dump_str.encode("utf-8")).hexdigest()
        cat.get_all_registered_pipelines.return_value = {"p1": pipeline_hash}

        node_dump = pc.nodes[0].model_dump(
            mode="json", exclude={"description", "tags", "log_level"}
        )
        node_dump_str = json.dumps(node_dump, sort_keys=True)
        node_hash = hashlib.md5(node_dump_str.encode("utf-8")).hexdigest()
        cat.get_all_registered_nodes.return_value = {"p1": {"n1": node_hash}}

        mgr, _ = _make_manager(pipelines=[pc], catalog_manager=cat)
        mgr._auto_register_pipelines(["p1"])
        cat.register_pipelines_batch.assert_not_called()
        cat.register_nodes_batch.assert_not_called()

    def test_auto_register_exception(self):
        pc = PipelineConfig(pipeline="p1", nodes=[_make_node_config("n1")])
        cat = MagicMock()
        cat.get_all_registered_pipelines.side_effect = RuntimeError("fail")
        mgr, _ = _make_manager(pipelines=[pc], catalog_manager=cat)
        mgr._auto_register_pipelines(["p1"])


# ===========================================================================
# Pipeline.__init__ edge cases (DocsConfig, perf fallbacks)
# ===========================================================================


class TestPipelineInitEdgeCases:
    def test_init_with_docs_config_dict(self, connections):
        cfg = PipelineConfig(
            pipeline="p",
            nodes=[
                NodeConfig(
                    name="n",
                    read={"connection": "src", "format": "csv", "path": "x.csv"},
                )
            ],
        )
        with (
            patch("odibi.pipeline.get_engine_class") as mock_gec,
            patch("odibi.pipeline.create_context"),
            patch("odibi.pipeline.DependencyGraph"),
            patch("odibi.pipeline.StoryGenerator"),
        ):
            mock_engine = MagicMock()
            type(mock_engine).spark = PropertyMock(return_value=None)
            mock_gec.return_value = lambda **kw: mock_engine

            p = Pipeline(
                pipeline_config=cfg,
                engine="pandas",
                connections=connections,
                story_config={"docs": {"enabled": True}},
            )
            assert p is not None

    def test_init_perf_config_no_model_dump(self, connections):
        cfg = PipelineConfig(
            pipeline="p",
            nodes=[
                NodeConfig(
                    name="n",
                    read={"connection": "src", "format": "csv", "path": "x.csv"},
                )
            ],
        )
        perf = {"batch_size": 500}
        with (
            patch("odibi.pipeline.get_engine_class") as mock_gec,
            patch("odibi.pipeline.create_context"),
            patch("odibi.pipeline.DependencyGraph"),
            patch("odibi.pipeline.StoryGenerator"),
        ):
            mock_engine = MagicMock()
            type(mock_engine).spark = PropertyMock(return_value=None)
            mock_gec.return_value = lambda **kw: mock_engine

            p = Pipeline(
                pipeline_config=cfg,
                engine="pandas",
                connections=connections,
                performance_config=perf,
            )
            assert p.performance_config == perf


# ===========================================================================
# Pipeline.run - parallel execution
# ===========================================================================


class TestPipelineRunParallel:
    @patch("odibi.pipeline.Node")
    def test_parallel_success(self, mock_node_cls, multi_node_config, connections):
        p = _make_pipeline(multi_node_config, connections, generate_story=False)
        p.graph.get_execution_layers.return_value = [["node_a"], ["node_b"]]

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.5
        )
        mock_node_cls.return_value = mock_instance

        results = p.run(parallel=True, max_workers=2)
        assert isinstance(results, PipelineResults)
        assert len(results.completed) >= 1

    @patch("odibi.pipeline.Node")
    def test_parallel_failure_fail_fast(self, mock_node_cls, multi_node_config, connections):
        p = _make_pipeline(multi_node_config, connections, generate_story=False)
        p.graph.get_execution_layers.return_value = [["node_a", "node_b"]]
        p.graph.nodes["node_a"].on_error = ErrorStrategy.FAIL_FAST

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=False, duration=0.1
        )
        mock_node_cls.return_value = mock_instance

        results = p.run(parallel=True, on_error="fail_fast")
        assert len(results.failed) >= 1

    @patch("odibi.pipeline.Node")
    def test_parallel_skipped_node(self, mock_node_cls, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections, generate_story=False)
        p.graph.get_execution_layers.return_value = [["node_a"]]

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a",
            success=False,
            duration=0.0,
            metadata={"skipped": True, "reason": "dependency_failed"},
        )
        mock_node_cls.return_value = mock_instance

        results = p.run(parallel=True)
        assert "node_a" in results.skipped

    @patch("odibi.pipeline.Node")
    def test_parallel_completed_skipped_metadata(
        self, mock_node_cls, simple_pipeline_config, connections
    ):
        p = _make_pipeline(simple_pipeline_config, connections, generate_story=False)
        p.graph.get_execution_layers.return_value = [["node_a"]]

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a",
            success=True,
            duration=0.1,
            metadata={"skipped": True, "reason": "resume_from_failure"},
        )
        mock_node_cls.return_value = mock_instance

        results = p.run(parallel=True)
        assert "node_a" in results.completed

    @patch("odibi.pipeline.Node")
    def test_parallel_future_exception(self, mock_node_cls, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections, generate_story=False)
        p.graph.get_execution_layers.return_value = [["node_a"]]

        mock_node_cls.side_effect = RuntimeError("init failed")

        results = p.run(parallel=True)
        assert "node_a" in results.failed


# ===========================================================================
# Pipeline.run - console progress paths
# ===========================================================================


class TestPipelineRunConsole:
    @patch("odibi.pipeline.PipelineProgress")
    @patch("odibi.pipeline.Node")
    def test_console_progress_success(
        self, mock_node_cls, mock_progress_cls, simple_pipeline_config, connections
    ):
        p = _make_pipeline(simple_pipeline_config, connections, generate_story=False)
        mock_progress = MagicMock()
        mock_progress_cls.return_value = mock_progress

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.5
        )
        mock_node_cls.return_value = mock_instance

        p.run(console=True)
        mock_progress.start.assert_called_once()
        mock_progress.finish.assert_called_once()
        mock_progress.update_node.assert_called()

    @patch("odibi.pipeline.PipelineProgress")
    @patch("odibi.pipeline.Node")
    def test_console_progress_failure(
        self, mock_node_cls, mock_progress_cls, simple_pipeline_config, connections
    ):
        p = _make_pipeline(simple_pipeline_config, connections, generate_story=False)
        mock_progress = MagicMock()
        mock_progress_cls.return_value = mock_progress

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=False, duration=0.3
        )
        mock_node_cls.return_value = mock_instance

        p.run(console=True)
        mock_progress.update_node.assert_called()


# ===========================================================================
# Pipeline.run - drift detection branches
# ===========================================================================


class TestDriftDetection:
    @patch("odibi.pipeline.Node")
    def test_drift_detected(self, mock_node_cls, simple_pipeline_config, connections):
        cat = MagicMock()
        cat.get_pipeline_hash.return_value = "different_hash"
        cat.register_outputs_from_config.return_value = 0
        p = _make_pipeline(
            simple_pipeline_config, connections, catalog_manager=cat, generate_story=False
        )

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.1
        )
        mock_node_cls.return_value = mock_instance

        results = p.run()
        assert results.pipeline_name == "test_pipe"

    @patch("odibi.pipeline.Node")
    def test_no_remote_hash(self, mock_node_cls, simple_pipeline_config, connections):
        cat = MagicMock()
        cat.get_pipeline_hash.return_value = None
        cat.register_outputs_from_config.return_value = 0
        p = _make_pipeline(
            simple_pipeline_config, connections, catalog_manager=cat, generate_story=False
        )

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.1
        )
        mock_node_cls.return_value = mock_instance

        results = p.run()
        assert results.pipeline_name == "test_pipe"

    @patch("odibi.pipeline.Node")
    def test_drift_check_passes(self, mock_node_cls, simple_pipeline_config, connections):
        import hashlib
        import json

        dump = simple_pipeline_config.model_dump(mode="json")
        dump_str = json.dumps(dump, sort_keys=True)
        local_hash = hashlib.md5(dump_str.encode("utf-8")).hexdigest()

        cat = MagicMock()
        cat.get_pipeline_hash.return_value = local_hash
        cat.register_outputs_from_config.return_value = 0
        p = _make_pipeline(
            simple_pipeline_config, connections, catalog_manager=cat, generate_story=False
        )

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.1
        )
        mock_node_cls.return_value = mock_instance

        results = p.run()
        assert results.pipeline_name == "test_pipe"

    @patch("odibi.pipeline.Node")
    def test_drift_check_exception(self, mock_node_cls, simple_pipeline_config, connections):
        cat = MagicMock()
        cat.get_pipeline_hash.side_effect = RuntimeError("catalog down")
        cat.register_outputs_from_config.return_value = 0
        p = _make_pipeline(
            simple_pipeline_config, connections, catalog_manager=cat, generate_story=False
        )

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.1
        )
        mock_node_cls.return_value = mock_instance

        results = p.run()
        assert "node_a" in results.completed


# ===========================================================================
# Pipeline.run - resume from failure
# ===========================================================================


class TestResumeFromFailure:
    @patch("odibi.pipeline.StateManager")
    @patch("odibi.pipeline.create_state_backend")
    @patch("odibi.pipeline.Node")
    def test_resume_skip_unchanged_node(
        self, mock_node_cls, mock_csb, mock_sm_cls, simple_pipeline_config, connections
    ):
        p = _make_pipeline(simple_pipeline_config, connections, generate_story=False)
        p.project_config = MagicMock()

        mock_sm = MagicMock()
        mock_sm.get_last_run_info.return_value = {
            "success": True,
            "metadata": {"version_hash": "abc123"},
        }
        mock_sm_cls.return_value = mock_sm

        mock_instance = MagicMock()
        mock_instance.restore.return_value = True
        mock_node_cls.return_value = mock_instance

        with patch("odibi.utils.hashing.calculate_node_hash", return_value="abc123"):
            results = p.run(resume_from_failure=True)
            assert "node_a" in results.completed

    @patch("odibi.pipeline.StateManager")
    @patch("odibi.pipeline.create_state_backend")
    @patch("odibi.pipeline.Node")
    def test_resume_rerun_changed_hash(
        self, mock_node_cls, mock_csb, mock_sm_cls, simple_pipeline_config, connections
    ):
        p = _make_pipeline(simple_pipeline_config, connections, generate_story=False)
        p.project_config = MagicMock()

        mock_sm = MagicMock()
        mock_sm.get_last_run_info.return_value = {
            "success": True,
            "metadata": {"version_hash": "old_hash"},
        }
        mock_sm_cls.return_value = mock_sm

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.5
        )
        mock_node_cls.return_value = mock_instance

        with patch("odibi.utils.hashing.calculate_node_hash", return_value="new_hash"):
            results = p.run(resume_from_failure=True)
            assert "node_a" in results.completed

    @patch("odibi.pipeline.StateManager")
    @patch("odibi.pipeline.create_state_backend")
    @patch("odibi.pipeline.Node")
    def test_resume_no_previous_run(
        self, mock_node_cls, mock_csb, mock_sm_cls, simple_pipeline_config, connections
    ):
        p = _make_pipeline(simple_pipeline_config, connections, generate_story=False)
        p.project_config = MagicMock()

        mock_sm = MagicMock()
        mock_sm.get_last_run_info.return_value = None
        mock_sm_cls.return_value = mock_sm

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.2
        )
        mock_node_cls.return_value = mock_instance

        results = p.run(resume_from_failure=True)
        assert "node_a" in results.completed

    @patch("odibi.pipeline.StateManager")
    @patch("odibi.pipeline.create_state_backend")
    @patch("odibi.pipeline.Node")
    def test_resume_restore_fails(
        self, mock_node_cls, mock_csb, mock_sm_cls, simple_pipeline_config, connections
    ):
        p = _make_pipeline(simple_pipeline_config, connections, generate_story=False)
        p.project_config = MagicMock()

        mock_sm = MagicMock()
        mock_sm.get_last_run_info.return_value = {
            "success": True,
            "metadata": {"version_hash": "abc123"},
        }
        mock_sm_cls.return_value = mock_sm

        mock_instance = MagicMock()
        mock_instance.restore.return_value = False
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.3
        )
        mock_node_cls.return_value = mock_instance

        with patch("odibi.utils.hashing.calculate_node_hash", return_value="abc123"):
            results = p.run(resume_from_failure=True)
            assert "node_a" in results.completed

    @patch("odibi.pipeline.StateManager")
    @patch("odibi.pipeline.create_state_backend")
    @patch("odibi.pipeline.Node")
    def test_resume_restore_exception(
        self, mock_node_cls, mock_csb, mock_sm_cls, simple_pipeline_config, connections
    ):
        p = _make_pipeline(simple_pipeline_config, connections, generate_story=False)
        p.project_config = MagicMock()

        mock_sm = MagicMock()
        mock_sm.get_last_run_info.return_value = {
            "success": True,
            "metadata": {"version_hash": "abc123"},
        }
        mock_sm_cls.return_value = mock_sm

        mock_instance = MagicMock()
        mock_instance.restore.side_effect = RuntimeError("restore boom")
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.2
        )
        mock_node_cls.return_value = mock_instance

        with patch("odibi.utils.hashing.calculate_node_hash", return_value="abc123"):
            results = p.run(resume_from_failure=True)
            assert "node_a" in results.completed

    @patch("odibi.pipeline.create_state_backend")
    @patch("odibi.pipeline.Node")
    def test_resume_no_project_config(
        self, mock_node_cls, mock_csb, simple_pipeline_config, connections
    ):
        p = _make_pipeline(simple_pipeline_config, connections, generate_story=False)
        p.project_config = None

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.1
        )
        mock_node_cls.return_value = mock_instance

        results = p.run(resume_from_failure=True)
        assert "node_a" in results.completed

    @patch("odibi.pipeline.create_state_backend")
    @patch("odibi.pipeline.Node")
    def test_resume_state_backend_fails(
        self, mock_node_cls, mock_csb, simple_pipeline_config, connections
    ):
        p = _make_pipeline(simple_pipeline_config, connections, generate_story=False)
        p.project_config = MagicMock()
        mock_csb.side_effect = RuntimeError("no backend")

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.1
        )
        mock_node_cls.return_value = mock_instance

        results = p.run(resume_from_failure=True)
        assert "node_a" in results.completed

    @patch("odibi.pipeline.StateManager")
    @patch("odibi.pipeline.create_state_backend")
    @patch("odibi.pipeline.Node")
    def test_resume_no_write_config(self, mock_node_cls, mock_csb, mock_sm_cls, connections):
        cfg = PipelineConfig(
            pipeline="p",
            nodes=[
                NodeConfig(
                    name="transform_only",
                    read={"connection": "src", "format": "csv", "path": "x.csv"},
                )
            ],
        )
        p = _make_pipeline(cfg, connections, generate_story=False)
        p.project_config = MagicMock()
        p.graph.nodes["transform_only"].write = None

        mock_sm = MagicMock()
        mock_sm.get_last_run_info.return_value = {
            "success": True,
            "metadata": {"version_hash": "abc123"},
        }
        mock_sm_cls.return_value = mock_sm

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="transform_only", success=True, duration=0.1
        )
        mock_node_cls.return_value = mock_instance

        with patch("odibi.utils.hashing.calculate_node_hash", return_value="abc123"):
            results = p.run(resume_from_failure=True)
            assert "transform_only" in results.completed


# ===========================================================================
# Pipeline.run - leverage summary tables / catalog logging
# ===========================================================================


class TestLeverageSummaryTables:
    @patch("odibi.derived_updater.DerivedUpdater")
    @patch("odibi.pipeline.Node")
    def test_catalog_pipeline_run_logging(
        self, mock_node_cls, mock_du_cls, simple_pipeline_config, connections
    ):
        cat = MagicMock()
        cat.register_outputs_from_config.return_value = 0
        cat.get_pipeline_hash.return_value = None
        p = _make_pipeline(
            simple_pipeline_config, connections, catalog_manager=cat, generate_story=False
        )
        p.project_config = MagicMock()
        p.project_config.owner = "test_owner"
        p.project_config.system = MagicMock()
        p.project_config.system.cost_per_compute_hour = 10.0

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a",
            success=True,
            duration=1.0,
            rows_written=100,
        )
        mock_node_cls.return_value = mock_instance

        p.run()
        cat.log_pipeline_run.assert_called_once()
        cat.log_node_runs_batch.assert_called_once()

    @patch("odibi.derived_updater.DerivedUpdater")
    @patch("odibi.pipeline.Node")
    def test_catalog_log_pipeline_run_fails(
        self, mock_node_cls, mock_du_cls, simple_pipeline_config, connections
    ):
        cat = MagicMock()
        cat.register_outputs_from_config.return_value = 0
        cat.get_pipeline_hash.return_value = None
        cat.log_pipeline_run.side_effect = RuntimeError("write fail")
        p = _make_pipeline(
            simple_pipeline_config, connections, catalog_manager=cat, generate_story=False
        )

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.5
        )
        mock_node_cls.return_value = mock_instance

        results = p.run()
        assert "node_a" in results.completed

    @patch("odibi.derived_updater.DerivedUpdater")
    @patch("odibi.pipeline.Node")
    def test_catalog_node_run_batch_fails(
        self, mock_node_cls, mock_du_cls, simple_pipeline_config, connections
    ):
        cat = MagicMock()
        cat.register_outputs_from_config.return_value = 0
        cat.get_pipeline_hash.return_value = None
        cat.log_node_runs_batch.side_effect = RuntimeError("batch fail")
        p = _make_pipeline(
            simple_pipeline_config, connections, catalog_manager=cat, generate_story=False
        )

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.5
        )
        mock_node_cls.return_value = mock_instance

        results = p.run()
        assert "node_a" in results.completed

    @patch("odibi.derived_updater.DerivedUpdater")
    @patch("odibi.pipeline.Node")
    def test_derived_updater_runs(
        self, mock_node_cls, mock_du_cls, simple_pipeline_config, connections
    ):
        cat = MagicMock()
        cat.register_outputs_from_config.return_value = 0
        cat.get_pipeline_hash.return_value = None
        p = _make_pipeline(
            simple_pipeline_config, connections, catalog_manager=cat, generate_story=False
        )

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.5
        )
        mock_node_cls.return_value = mock_instance

        p.run()
        mock_du_cls.assert_called_once_with(cat)

    @patch("odibi.derived_updater.DerivedUpdater")
    @patch("odibi.pipeline.Node")
    def test_derived_updater_exception(
        self, mock_node_cls, mock_du_cls, simple_pipeline_config, connections
    ):
        cat = MagicMock()
        cat.register_outputs_from_config.return_value = 0
        cat.get_pipeline_hash.return_value = None
        mock_du_cls.side_effect = RuntimeError("updater boom")
        p = _make_pipeline(
            simple_pipeline_config, connections, catalog_manager=cat, generate_story=False
        )

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.5
        )
        mock_node_cls.return_value = mock_instance

        results = p.run()
        assert "node_a" in results.completed

    @patch("odibi.derived_updater.DerivedUpdater")
    @patch("odibi.pipeline.Node")
    def test_terminal_nodes_rows_processed(
        self, mock_node_cls, mock_du_cls, multi_node_config, connections
    ):
        cat = MagicMock()
        cat.register_outputs_from_config.return_value = 0
        cat.get_pipeline_hash.return_value = None
        p = _make_pipeline(
            multi_node_config, connections, catalog_manager=cat, generate_story=False
        )

        def make_result(name):
            return NodeResult(node_name=name, success=True, duration=0.5, rows_written=50)

        mock_instance = MagicMock()
        call_count = [0]

        def side_effect():
            call_count[0] += 1
            name = "node_a" if call_count[0] == 1 else "node_b"
            return make_result(name)

        mock_instance.execute.side_effect = side_effect
        mock_node_cls.return_value = mock_instance

        p.run()
        call_args = cat.log_pipeline_run.call_args[0][0]
        assert call_args["rows_processed"] is not None

    @patch("odibi.derived_updater.DerivedUpdater")
    @patch("odibi.pipeline.Node")
    def test_first_error_summary(
        self, mock_node_cls, mock_du_cls, simple_pipeline_config, connections
    ):
        cat = MagicMock()
        cat.register_outputs_from_config.return_value = 0
        cat.get_pipeline_hash.return_value = None
        p = _make_pipeline(
            simple_pipeline_config, connections, catalog_manager=cat, generate_story=False
        )

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a",
            success=False,
            duration=0.1,
            error=Exception("something broke"),
        )
        mock_node_cls.return_value = mock_instance

        p.run()
        call_args = cat.log_pipeline_run.call_args[0][0]
        assert "something broke" in call_args["error_summary"]


# ===========================================================================
# Pipeline.run - story generation (sync and async)
# ===========================================================================


class TestStoryGeneration:
    @patch("odibi.pipeline.Node")
    def test_story_sync_success(self, mock_node_cls, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections, generate_story=True)
        p.story_config = {"async_generation": False}
        p.story_generator.generate.return_value = "/path/to/story.json"

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.1
        )
        mock_node_cls.return_value = mock_instance

        results = p.run()
        assert results.story_path == "/path/to/story.json"

    @patch("odibi.pipeline.Node")
    def test_story_sync_failure(self, mock_node_cls, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections, generate_story=True)
        p.story_config = {"async_generation": False}
        p.story_generator.generate.side_effect = RuntimeError("story fail")

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.1
        )
        mock_node_cls.return_value = mock_instance

        results = p.run()
        assert results.story_path is None

    @patch("odibi.pipeline.Node")
    def test_story_async(self, mock_node_cls, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections, generate_story=True)
        p.story_config = {"async_generation": True}
        p.story_generator.generate.return_value = "/path/async_story.json"

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.1
        )
        mock_node_cls.return_value = mock_instance

        p.run()
        assert p._story_future is not None
        story_path = p.flush_stories(timeout=5.0)
        assert story_path == "/path/async_story.json"

    @patch("odibi.pipeline.Node")
    def test_story_with_project_config(self, mock_node_cls, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections, generate_story=True)
        p.story_config = {"async_generation": False}
        p.project_config = MagicMock()
        p.project_config.model_dump.return_value = {
            "project": "my_proj",
            "plant": "P1",
            "asset": None,
            "business_unit": "BU1",
            "layer": "silver",
        }
        p.story_generator.generate.return_value = "/story.json"

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.1
        )
        mock_node_cls.return_value = mock_instance

        results = p.run()
        assert results.story_path == "/story.json"


# ===========================================================================
# Pipeline.run - state saving at end
# ===========================================================================


class TestStateSaving:
    @patch("odibi.pipeline.StateManager")
    @patch("odibi.pipeline.create_state_backend")
    @patch("odibi.pipeline.Node")
    def test_state_saved_after_run(
        self, mock_node_cls, mock_csb, mock_sm_cls, simple_pipeline_config, connections
    ):
        p = _make_pipeline(simple_pipeline_config, connections, generate_story=False)
        p.project_config = MagicMock()

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.1
        )
        mock_node_cls.return_value = mock_instance

        p.run()
        mock_sm_cls.return_value.save_pipeline_run.assert_called()

    @patch("odibi.pipeline.create_state_backend")
    @patch("odibi.pipeline.Node")
    def test_state_save_backend_fails(
        self, mock_node_cls, mock_csb, simple_pipeline_config, connections
    ):
        p = _make_pipeline(simple_pipeline_config, connections, generate_story=False)
        p.project_config = MagicMock()
        mock_csb.side_effect = RuntimeError("no backend")

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.1
        )
        mock_node_cls.return_value = mock_instance

        results = p.run()
        assert "node_a" in results.completed


# ===========================================================================
# Pipeline.run - lineage emission
# ===========================================================================


class TestLineageEmission:
    @patch("odibi.pipeline.Node")
    def test_lineage_emitted(self, mock_node_cls, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections, generate_story=False)
        p.lineage = MagicMock()
        p.lineage.emit_pipeline_start.return_value = "parent-run-id"
        p.lineage.emit_node_start.return_value = "node-run-id"

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a", success=True, duration=0.1
        )
        mock_node_cls.return_value = mock_instance

        p.run()
        p.lineage.emit_pipeline_start.assert_called_once()
        p.lineage.emit_node_start.assert_called()
        p.lineage.emit_node_complete.assert_called()
        p.lineage.emit_pipeline_complete.assert_called_once()


# ===========================================================================
# Pipeline.run - output register batch failure
# ===========================================================================


class TestOutputRegisterBatchFailure:
    @patch("odibi.pipeline.Node")
    def test_register_outputs_batch_exception(
        self, mock_node_cls, simple_pipeline_config, connections
    ):
        cat = MagicMock()
        cat.register_outputs_batch.side_effect = RuntimeError("batch fail")
        p = _make_pipeline(
            simple_pipeline_config, connections, catalog_manager=cat, generate_story=False
        )

        mock_instance = MagicMock()
        mock_instance.execute.return_value = NodeResult(
            node_name="node_a",
            success=True,
            duration=0.5,
            metadata={"_output_record": {"table": "out"}},
        )
        mock_node_cls.return_value = mock_instance

        results = p.run()
        assert "node_a" in results.completed


# ===========================================================================
# Pipeline.validate - transformer and step validation
# ===========================================================================


class TestValidateTransformers:
    def test_validate_transformer_in_registry(self, connections):
        from odibi.transformers import register_standard_library

        register_standard_library()

        cfg = PipelineConfig(
            pipeline="p",
            nodes=[
                NodeConfig(
                    name="n",
                    read={"connection": "src", "format": "csv", "path": "x.csv"},
                    transformer="rename_columns",
                    params={"mapping": {"a": "b"}},
                )
            ],
        )
        p = _make_pipeline(cfg, connections)
        p.graph.nodes["n"].transformer = "rename_columns"
        p.graph.nodes["n"].params = {"mapping": {"a": "b"}}
        v = p.validate()
        assert v["valid"] is True

    def test_validate_transformer_invalid_params(self, connections):
        cfg = PipelineConfig(
            pipeline="p",
            nodes=[
                NodeConfig(
                    name="n",
                    read={"connection": "src", "format": "csv", "path": "x.csv"},
                    transformer="rename_columns",
                    params={"columns": {"a": "b"}},
                )
            ],
        )
        p = _make_pipeline(cfg, connections)
        p.graph.nodes["n"].transformer = "nonexistent_transformer_xyz"
        p.graph.nodes["n"].params = {}

        from odibi.registry import FunctionRegistry

        with patch.object(
            FunctionRegistry, "validate_params", side_effect=ValueError("bad params")
        ):
            v = p.validate()
            assert v["valid"] is False
            assert any("bad params" in e for e in v["errors"])

    def test_validate_write_connection_warning(self, connections):
        cfg = PipelineConfig(
            pipeline="p",
            nodes=[
                NodeConfig(
                    name="n",
                    read={"connection": "src", "format": "csv", "path": "x.csv"},
                    write={"connection": "missing_write", "format": "csv", "path": "y.csv"},
                )
            ],
        )
        p = _make_pipeline(cfg, connections)
        v = p.validate()
        assert any("missing_write" in w for w in v["warnings"])


# ===========================================================================
# Pipeline._sync_catalog_if_configured - sync/async paths
# ===========================================================================


class TestSyncCatalogPaths:
    def test_sync_synchronous(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        p.catalog_manager = MagicMock()
        p.project_config = MagicMock()
        p.project_config.system.sync_to.on = "after_run"
        p.project_config.system.sync_to.connection = "dst"
        p.project_config.system.sync_to.async_sync = False
        p.project_config.system.environment = "dev"
        connections["dst"] = MagicMock()
        p.connections = connections

        with patch("odibi.catalog_sync.CatalogSyncer", create=True) as mock_syncer_cls:
            mock_syncer = MagicMock()
            mock_syncer.sync.return_value = {"t1": {"success": True}}
            mock_syncer_cls.return_value = mock_syncer
            p._sync_catalog_if_configured()
            mock_syncer.sync.assert_called_once()

    def test_sync_async(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        p.catalog_manager = MagicMock()
        p.project_config = MagicMock()
        p.project_config.system.sync_to.on = "after_run"
        p.project_config.system.sync_to.connection = "dst"
        p.project_config.system.sync_to.async_sync = True
        p.project_config.system.environment = "dev"
        connections["dst"] = MagicMock()
        p.connections = connections

        with patch("odibi.catalog_sync.CatalogSyncer", create=True) as mock_syncer_cls:
            mock_syncer = MagicMock()
            mock_syncer_cls.return_value = mock_syncer
            p._sync_catalog_if_configured()
            mock_syncer.sync_async.assert_called_once()

    def test_sync_exception(self, simple_pipeline_config, connections):
        p = _make_pipeline(simple_pipeline_config, connections)
        p.catalog_manager = MagicMock()
        p.project_config = MagicMock()
        p.project_config.system.sync_to.on = "after_run"
        p.project_config.system.sync_to.connection = "dst"
        p.connections = connections

        with patch(
            "odibi.catalog_sync.CatalogSyncer", create=True, side_effect=RuntimeError("fail")
        ):
            p._sync_catalog_if_configured()


# ===========================================================================
# PipelineManager - list/query methods WITH catalog data
# ===========================================================================


class TestPipelineManagerListQueryWithCatalog:
    def test_list_registered_pipelines_with_data(self):
        cat = MagicMock()
        cat._read_local_table.return_value = pd.DataFrame(
            {"pipeline_name": ["p1"], "version_hash": ["abc"]}
        )
        mgr, _ = _make_manager(catalog_manager=cat)
        df = mgr.list_registered_pipelines()
        assert not df.empty
        assert "p1" in df["pipeline_name"].values

    def test_list_registered_pipelines_exception(self):
        cat = MagicMock()
        cat._read_local_table.side_effect = RuntimeError("fail")
        mgr, _ = _make_manager(catalog_manager=cat)
        df = mgr.list_registered_pipelines()
        assert df.empty

    def test_list_registered_nodes_with_filter(self):
        cat = MagicMock()
        cat._read_local_table.return_value = pd.DataFrame(
            {"pipeline_name": ["p1", "p2"], "node_name": ["n1", "n2"]}
        )
        mgr, _ = _make_manager(catalog_manager=cat)
        df = mgr.list_registered_nodes(pipeline="p1")
        assert len(df) == 1

    def test_list_registered_nodes_exception(self):
        cat = MagicMock()
        cat._read_local_table.side_effect = RuntimeError("fail")
        mgr, _ = _make_manager(catalog_manager=cat)
        df = mgr.list_registered_nodes()
        assert df.empty

    def test_list_runs_with_filters(self):
        cat = MagicMock()
        cat._read_local_table.return_value = pd.DataFrame(
            {
                "pipeline_name": ["p1", "p1", "p2"],
                "node_name": ["n1", "n2", "n1"],
                "status": ["SUCCESS", "FAILURE", "SUCCESS"],
                "timestamp": pd.to_datetime(["2026-01-10", "2026-01-11", "2026-01-12"]),
            }
        )
        mgr, _ = _make_manager(catalog_manager=cat)
        df = mgr.list_runs(pipeline="p1", status="SUCCESS", limit=5)
        assert len(df) == 1

    def test_list_runs_empty(self):
        cat = MagicMock()
        cat._read_local_table.return_value = pd.DataFrame()
        mgr, _ = _make_manager(catalog_manager=cat)
        df = mgr.list_runs()
        assert df.empty

    def test_list_runs_exception(self):
        cat = MagicMock()
        cat._read_local_table.side_effect = RuntimeError("fail")
        mgr, _ = _make_manager(catalog_manager=cat)
        df = mgr.list_runs()
        assert df.empty

    def test_list_tables_with_data(self):
        cat = MagicMock()
        cat._read_local_table.return_value = pd.DataFrame(
            {"table_name": ["t1"], "path": ["/data/t1"]}
        )
        mgr, _ = _make_manager(catalog_manager=cat)
        df = mgr.list_tables()
        assert not df.empty

    def test_list_tables_exception(self):
        cat = MagicMock()
        cat._read_local_table.side_effect = RuntimeError("fail")
        mgr, _ = _make_manager(catalog_manager=cat)
        df = mgr.list_tables()
        assert df.empty

    def test_get_pipeline_status_never_run(self):
        cat = MagicMock()
        cat._read_local_table.return_value = pd.DataFrame()
        mgr, _ = _make_manager(catalog_manager=cat)
        status = mgr.get_pipeline_status("p1")
        assert status["status"] == "never_run"

    def test_get_pipeline_status_with_run(self):
        cat = MagicMock()
        cat._read_local_table.return_value = pd.DataFrame(
            {
                "pipeline_name": ["p1"],
                "status": ["SUCCESS"],
                "timestamp": pd.to_datetime(["2026-01-10"]),
                "duration_ms": [5000],
                "node_name": ["n1"],
            }
        )
        mgr, _ = _make_manager(catalog_manager=cat)
        status = mgr.get_pipeline_status("p1")
        assert status["last_status"] == "SUCCESS"

    def test_get_pipeline_status_exception(self):
        cat = MagicMock()
        cat._read_local_table.side_effect = RuntimeError("fail")
        mgr, _ = _make_manager(catalog_manager=cat)
        result = mgr.get_pipeline_status("p1")
        assert result.get("status") == "never_run" or result == {}

    def test_get_node_stats_with_data(self):
        cat = MagicMock()
        cat.get_average_duration.return_value = 2.5
        cat._read_local_table.return_value = pd.DataFrame(
            {
                "node_name": ["n1", "n1"],
                "status": ["SUCCESS", "FAILURE"],
                "rows_processed": [100, 50],
                "timestamp": pd.to_datetime(["2026-02-10", "2026-02-11"]),
            }
        )
        mgr, _ = _make_manager(catalog_manager=cat)
        stats = mgr.get_node_stats("n1", days=7)
        assert stats["runs"] == 2
        assert stats["success_rate"] == 0.5

    def test_get_node_stats_no_matching_node(self):
        cat = MagicMock()
        cat.get_average_duration.return_value = None
        cat._read_local_table.return_value = pd.DataFrame(
            {
                "node_name": ["other"],
                "status": ["SUCCESS"],
                "rows_processed": [100],
                "timestamp": pd.to_datetime(["2026-02-10"]),
            }
        )
        mgr, _ = _make_manager(catalog_manager=cat)
        stats = mgr.get_node_stats("n1")
        assert stats["runs"] == 0

    def test_get_node_stats_exception(self):
        cat = MagicMock()
        cat.get_average_duration.side_effect = RuntimeError("fail")
        mgr, _ = _make_manager(catalog_manager=cat)
        assert mgr.get_node_stats("n1") == {}

    def test_get_state_found(self):
        cat = MagicMock()
        cat._read_table.return_value = pd.DataFrame({"key": ["hwm_test"], "value": ["2026-01-01"]})
        mgr, _ = _make_manager(catalog_manager=cat)
        result = mgr.get_state("hwm_test")
        assert result is not None
        assert result["key"] == "hwm_test"

    def test_get_state_not_found(self):
        cat = MagicMock()
        cat._read_table.return_value = pd.DataFrame({"key": ["other"], "value": ["x"]})
        mgr, _ = _make_manager(catalog_manager=cat)
        assert mgr.get_state("missing") is None

    def test_get_state_empty(self):
        cat = MagicMock()
        cat._read_table.return_value = pd.DataFrame()
        mgr, _ = _make_manager(catalog_manager=cat)
        assert mgr.get_state("key") is None

    def test_get_state_exception(self):
        cat = MagicMock()
        cat._read_table.side_effect = RuntimeError("fail")
        mgr, _ = _make_manager(catalog_manager=cat)
        assert mgr.get_state("key") is None

    def test_get_all_state_with_prefix(self):
        cat = MagicMock()
        cat._read_table.return_value = pd.DataFrame(
            {"key": ["hwm_a", "hwm_b", "other"], "value": ["1", "2", "3"]}
        )
        mgr, _ = _make_manager(catalog_manager=cat)
        df = mgr.get_all_state(prefix="hwm_")
        assert len(df) == 2

    def test_get_all_state_exception(self):
        cat = MagicMock()
        cat._read_table.side_effect = RuntimeError("fail")
        mgr, _ = _make_manager(catalog_manager=cat)
        df = mgr.get_all_state()
        assert df.empty

    def test_clear_state_success(self):
        cat = MagicMock()
        cat.clear_state_key.return_value = True
        mgr, _ = _make_manager(catalog_manager=cat)
        assert mgr.clear_state("key") is True

    def test_clear_state_exception(self):
        cat = MagicMock()
        cat.clear_state_key.side_effect = RuntimeError("fail")
        mgr, _ = _make_manager(catalog_manager=cat)
        assert mgr.clear_state("key") is False

    def test_get_schema_history_with_catalog(self):
        cat = MagicMock()
        cat.get_schema_history.return_value = [{"version": 1}]
        mgr, _ = _make_manager(catalog_manager=cat)
        mgr._resolve_table_path = MagicMock(return_value="/data/table")
        df = mgr.get_schema_history("table")
        assert not df.empty

    def test_get_schema_history_exception(self):
        cat = MagicMock()
        cat.get_schema_history.side_effect = RuntimeError("fail")
        mgr, _ = _make_manager(catalog_manager=cat)
        mgr._resolve_table_path = MagicMock(return_value="/data/table")
        df = mgr.get_schema_history("table")
        assert df.empty

    def test_get_lineage_upstream(self):
        cat = MagicMock()
        cat.get_upstream.return_value = [{"source": "a"}]
        cat.get_downstream.return_value = []
        mgr, _ = _make_manager(catalog_manager=cat)
        mgr._resolve_table_path = MagicMock(return_value="/data/table")
        df = mgr.get_lineage("table", direction="upstream")
        assert len(df) == 1

    def test_get_lineage_downstream(self):
        cat = MagicMock()
        cat.get_upstream.return_value = []
        cat.get_downstream.return_value = [{"target": "b"}]
        mgr, _ = _make_manager(catalog_manager=cat)
        mgr._resolve_table_path = MagicMock(return_value="/data/table")
        df = mgr.get_lineage("table", direction="downstream")
        assert len(df) == 1

    def test_get_lineage_both(self):
        cat = MagicMock()
        cat.get_upstream.return_value = [{"source": "a"}]
        cat.get_downstream.return_value = [{"target": "b"}]
        mgr, _ = _make_manager(catalog_manager=cat)
        mgr._resolve_table_path = MagicMock(return_value="/data/table")
        df = mgr.get_lineage("table", direction="both")
        assert len(df) == 2

    def test_get_lineage_exception(self):
        cat = MagicMock()
        cat.get_upstream.side_effect = RuntimeError("fail")
        mgr, _ = _make_manager(catalog_manager=cat)
        mgr._resolve_table_path = MagicMock(return_value="/data/table")
        df = mgr.get_lineage("table")
        assert df.empty


# ===========================================================================
# PipelineManager.run - lineage generation path
# ===========================================================================


class TestPipelineManagerRunLineage:
    def test_run_with_lineage_generation(self):
        pc = PipelineConfig(pipeline="p1", nodes=[_make_node_config("n1")])
        mgr, mocks = _make_manager(pipelines=[pc])
        mgr.project_config.story.generate_lineage = True
        mocks["p1"].run.return_value = PipelineResults(pipeline_name="p1", story_path="/story.json")

        with patch("odibi.pipeline.generate_lineage") as mock_gl:
            mock_result = MagicMock()
            mock_result.nodes = ["a"]
            mock_result.edges = [("a", "b")]
            mock_result.json_path = "/lineage.json"
            mock_gl.return_value = mock_result
            mgr.run("p1")
            mock_gl.assert_called_once()

    def test_run_lineage_generation_returns_none(self):
        pc = PipelineConfig(pipeline="p1", nodes=[_make_node_config("n1")])
        mgr, mocks = _make_manager(pipelines=[pc])
        mgr.project_config.story.generate_lineage = True

        with patch("odibi.pipeline.generate_lineage", return_value=None):
            result = mgr.run("p1")
            assert isinstance(result, PipelineResults)

    def test_run_lineage_generation_exception(self):
        pc = PipelineConfig(pipeline="p1", nodes=[_make_node_config("n1")])
        mgr, mocks = _make_manager(pipelines=[pc])
        mgr.project_config.story.generate_lineage = True

        with patch("odibi.pipeline.generate_lineage", side_effect=RuntimeError("fail")):
            result = mgr.run("p1")
            assert isinstance(result, PipelineResults)


# ===========================================================================
# PipelineManager._resolve_table_path - node write fallback
# ===========================================================================


class TestResolveTablePathNodeFallback:
    def test_resolve_via_node_write(self):
        pc = PipelineConfig(
            pipeline="p1",
            nodes=[
                NodeConfig(
                    name="my_node",
                    read={"connection": "src", "format": "csv", "path": "in.csv"},
                    write={"connection": "dst", "format": "csv", "path": "out.csv"},
                )
            ],
        )
        mgr, _ = _make_manager(pipelines=[pc])
        mgr._pipelines["p1"].config = pc
        result = mgr._resolve_table_path("my_node")
        assert result == "/data/out.csv"

    def test_resolve_fallback_to_system_connection(self):
        mgr, _ = _make_manager()
        mgr.project_config.system = MagicMock()
        mgr.project_config.system.connection = "dst"
        result = mgr._resolve_table_path("some/relative/path")
        assert "/data/" in result
