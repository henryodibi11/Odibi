"""Extended coverage tests for odibi.pipeline — utility methods and edge paths."""

import threading
from concurrent.futures import Future
from unittest.mock import MagicMock, patch


from odibi.node import NodeResult
from odibi.pipeline import Pipeline, PipelineResults


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pipeline(**overrides):
    """Create a Pipeline without running __init__."""
    p = Pipeline.__new__(Pipeline)
    p._ctx = MagicMock()
    p.config = MagicMock()
    p.config.pipeline = "test_pipeline"
    p.engine = MagicMock()
    p.engine.spark = None  # no Spark by default
    p.connections = {}
    p.catalog_manager = None
    p.lineage = None
    p.story_generator = MagicMock()
    p.story_config = {}
    p.generate_story = True
    p.project_config = None
    p.alerts = []
    p.performance_config = None
    p._pending_lineage_records = []
    p._pending_asset_records = []
    p._pending_hwm_updates = []
    p._batch_mode_enabled = True
    p._buffer_lock = threading.Lock()
    p._story_future = None
    p._story_executor = None
    p._sync_thread = None
    for k, v in overrides.items():
        setattr(p, k, v)
    return p


def _make_node_result(name, success=True, error=None, duration=1.0, rows=10):
    return NodeResult(
        node_name=name,
        success=success,
        duration=duration,
        rows_processed=rows,
        error=error,
    )


# ===========================================================================
# PipelineResults.debug_summary
# ===========================================================================


class TestDebugSummaryNoFailures:
    def test_returns_empty_string(self):
        r = PipelineResults(pipeline_name="p", completed=["a", "b"], failed=[])
        assert r.debug_summary() == ""


class TestDebugSummaryWithErrors:
    def test_failed_node_with_error_message(self):
        nr = _make_node_result("node_x", success=False, error=ValueError("boom"))
        r = PipelineResults(
            pipeline_name="p",
            failed=["node_x"],
            node_results={"node_x": nr},
        )
        summary = r.debug_summary()
        assert "node_x" in summary
        assert "boom" in summary

    def test_failed_node_without_error(self):
        nr = _make_node_result("node_y", success=False, error=None)
        r = PipelineResults(
            pipeline_name="p",
            failed=["node_y"],
            node_results={"node_y": nr},
        )
        summary = r.debug_summary()
        assert "node_y" in summary
        # Should not contain a colon after the node name (no error detail)
        assert "  • node_y\n" in summary

    def test_failed_node_not_in_results(self):
        r = PipelineResults(
            pipeline_name="p",
            failed=["ghost"],
            node_results={},
        )
        summary = r.debug_summary()
        assert "  • ghost\n" in summary

    def test_multiple_failed_nodes(self):
        nr1 = _make_node_result("a", success=False, error=RuntimeError("err1"))
        nr2 = _make_node_result("b", success=False, error=TypeError("err2"))
        r = PipelineResults(
            pipeline_name="p",
            failed=["a", "b"],
            node_results={"a": nr1, "b": nr2},
        )
        summary = r.debug_summary()
        assert "err1" in summary
        assert "err2" in summary


class TestDebugSummaryStoryPath:
    def test_with_story_path(self):
        nr = _make_node_result("n", success=False, error=ValueError("x"))
        r = PipelineResults(
            pipeline_name="p",
            failed=["n"],
            node_results={"n": nr},
            story_path="/tmp/story.html",
        )
        summary = r.debug_summary()
        assert "odibi story show /tmp/story.html" in summary
        assert "odibi story last --node n" in summary

    def test_without_story_path(self):
        nr = _make_node_result("n", success=False, error=ValueError("x"))
        r = PipelineResults(
            pipeline_name="p",
            failed=["n"],
            node_results={"n": nr},
            story_path=None,
        )
        summary = r.debug_summary()
        assert "Check the logs" in summary
        assert "odibi story show" not in summary

    def test_contains_odibi_doctor(self):
        nr = _make_node_result("n", success=False, error=ValueError("x"))
        r = PipelineResults(
            pipeline_name="p",
            failed=["n"],
            node_results={"n": nr},
        )
        summary = r.debug_summary()
        assert "odibi doctor" in summary


# ===========================================================================
# PipelineResults.to_dict
# ===========================================================================


class TestToDict:
    def test_structure_keys(self):
        r = PipelineResults(
            pipeline_name="my_pipe",
            completed=["a"],
            failed=["b"],
            skipped=["c"],
            node_results={"a": _make_node_result("a"), "b": _make_node_result("b", False)},
            duration=12.5,
            start_time="2025-01-01T00:00:00",
            end_time="2025-01-01T00:00:12",
        )
        d = r.to_dict()
        assert d["pipeline_name"] == "my_pipe"
        assert d["completed"] == ["a"]
        assert d["failed"] == ["b"]
        assert d["skipped"] == ["c"]
        assert d["duration"] == 12.5
        assert d["start_time"] == "2025-01-01T00:00:00"
        assert d["end_time"] == "2025-01-01T00:00:12"
        assert d["node_count"] == 2

    def test_empty_results(self):
        r = PipelineResults(pipeline_name="empty")
        d = r.to_dict()
        assert d["node_count"] == 0
        assert d["completed"] == []
        assert d["failed"] == []
        assert d["skipped"] == []

    def test_node_count_matches_results_len(self):
        nr = {f"n{i}": _make_node_result(f"n{i}") for i in range(5)}
        r = PipelineResults(pipeline_name="p", node_results=nr)
        assert r.to_dict()["node_count"] == 5


# ===========================================================================
# Pipeline._cleanup_connections
# ===========================================================================


class TestCleanupConnections:
    def test_no_connections(self):
        p = _make_pipeline(connections={})
        p._cleanup_connections()
        # No error raised

    def test_empty_connections_dict(self):
        p = _make_pipeline(connections={})
        p._cleanup_connections()
        p._ctx.debug.assert_not_called()

    def test_connection_with_close(self):
        conn = MagicMock()
        p = _make_pipeline(connections={"db": conn})
        p._cleanup_connections()
        conn.close.assert_called_once()

    def test_close_raises_exception(self):
        conn = MagicMock()
        conn.close.side_effect = RuntimeError("close failed")
        p = _make_pipeline(connections={"db": conn})
        p._cleanup_connections()
        p._ctx.warning.assert_called_once()

    def test_connection_without_close(self):
        conn = object()  # no close method
        p = _make_pipeline(connections={"raw": conn})
        p._cleanup_connections()
        # No error raised


# ===========================================================================
# Pipeline.flush_stories
# ===========================================================================


class TestFlushStories:
    def test_no_future_returns_none(self):
        p = _make_pipeline()
        assert p.flush_stories() is None

    def test_future_success(self):
        future = Future()
        future.set_result("/tmp/story.html")
        executor = MagicMock()
        p = _make_pipeline(_story_future=future, _story_executor=executor)
        result = p.flush_stories()
        assert result == "/tmp/story.html"
        executor.shutdown.assert_called_once_with(wait=False)
        assert p._story_future is None
        assert p._story_executor is None

    def test_future_exception_returns_none(self):
        future = Future()
        future.set_exception(RuntimeError("generation failed"))
        executor = MagicMock()
        p = _make_pipeline(_story_future=future, _story_executor=executor)
        result = p.flush_stories()
        assert result is None
        executor.shutdown.assert_called_once_with(wait=False)

    def test_future_clears_refs(self):
        future = Future()
        future.set_result("path")
        p = _make_pipeline(_story_future=future, _story_executor=MagicMock())
        p.flush_stories()
        assert p._story_future is None
        assert p._story_executor is None


# ===========================================================================
# Pipeline.flush_sync
# ===========================================================================


class TestFlushSync:
    def test_no_thread_returns(self):
        p = _make_pipeline(_sync_thread=None)
        p.flush_sync()
        # No error, no join call

    def test_thread_not_alive_returns(self):
        t = MagicMock()
        t.is_alive.return_value = False
        p = _make_pipeline(_sync_thread=t)
        p.flush_sync()
        t.join.assert_not_called()

    def test_thread_alive_joins(self):
        t = MagicMock()
        t.is_alive.side_effect = [True, False]
        p = _make_pipeline(_sync_thread=t)
        p.flush_sync()
        t.join.assert_called_once()
        assert p._sync_thread is None

    def test_thread_does_not_finish_in_time(self):
        t = MagicMock()
        t.is_alive.side_effect = [True, True]
        p = _make_pipeline(_sync_thread=t)
        p.flush_sync(timeout=0.01)
        p._ctx.warning.assert_called()
        assert p._sync_thread is None

    def test_join_raises(self):
        t = MagicMock()
        t.is_alive.return_value = True
        t.join.side_effect = RuntimeError("thread error")
        p = _make_pipeline(_sync_thread=t)
        p.flush_sync()
        p._ctx.warning.assert_called()
        assert p._sync_thread is None


# ===========================================================================
# Pipeline._send_alerts
# ===========================================================================


class TestSendAlerts:
    @patch("odibi.pipeline.send_alert")
    def test_matching_event_triggers_alert(self, mock_send):
        alert = MagicMock()
        alert.on_events = ["on_failure"]
        p = _make_pipeline(alerts=[alert], generate_story=False)
        results = PipelineResults(
            pipeline_name="p",
            failed=["node_a"],
            node_results={"node_a": _make_node_result("node_a", False, RuntimeError("e"))},
        )
        p._send_alerts("on_failure", results)
        mock_send.assert_called_once()

    @patch("odibi.pipeline.send_alert")
    def test_non_matching_event_skips(self, mock_send):
        alert = MagicMock()
        alert.on_events = ["on_success"]
        p = _make_pipeline(alerts=[alert], generate_story=False)
        results = PipelineResults(pipeline_name="p")
        p._send_alerts("on_failure", results)
        mock_send.assert_not_called()

    @patch("odibi.pipeline.send_alert")
    def test_on_start_sets_status_started(self, mock_send):
        alert = MagicMock()
        alert.on_events = ["on_start"]
        p = _make_pipeline(alerts=[alert], generate_story=False)
        results = PipelineResults(pipeline_name="p")
        p._send_alerts("on_start", results)
        mock_send.assert_called_once()
        context = mock_send.call_args[0][1]
        assert "STARTED" in context

    @patch("odibi.pipeline.send_alert")
    def test_no_alerts_configured(self, mock_send):
        p = _make_pipeline(alerts=[])
        results = PipelineResults(pipeline_name="p")
        p._send_alerts("on_failure", results)
        mock_send.assert_not_called()

    @patch("odibi.pipeline.send_alert")
    def test_alert_event_with_enum_value(self, mock_send):
        event_mock = MagicMock()
        event_mock.value = "on_failure"
        alert = MagicMock()
        alert.on_events = [event_mock]
        p = _make_pipeline(alerts=[alert], generate_story=False)
        results = PipelineResults(pipeline_name="p", failed=["x"], node_results={})
        p._send_alerts("on_failure", results)
        mock_send.assert_called_once()


# ===========================================================================
# Pipeline buffer methods
# ===========================================================================


class TestBufferMethods:
    def test_buffer_lineage_record(self):
        p = _make_pipeline()
        rec = {"source_table": "a", "target_table": "b"}
        p.buffer_lineage_record(rec)
        assert p._pending_lineage_records == [rec]

    def test_buffer_lineage_multiple(self):
        p = _make_pipeline()
        p.buffer_lineage_record({"a": 1})
        p.buffer_lineage_record({"b": 2})
        assert len(p._pending_lineage_records) == 2

    def test_buffer_asset_record(self):
        p = _make_pipeline()
        rec = {"table_name": "t1", "format": "parquet"}
        p.buffer_asset_record(rec)
        assert p._pending_asset_records == [rec]

    def test_buffer_asset_multiple(self):
        p = _make_pipeline()
        p.buffer_asset_record({"x": 1})
        p.buffer_asset_record({"y": 2})
        assert len(p._pending_asset_records) == 2

    def test_buffer_hwm_update(self):
        p = _make_pipeline()
        p.buffer_hwm_update("key1", "val1")
        assert p._pending_hwm_updates == [{"key": "key1", "value": "val1"}]

    def test_buffer_hwm_multiple(self):
        p = _make_pipeline()
        p.buffer_hwm_update("k1", 1)
        p.buffer_hwm_update("k2", 2)
        assert len(p._pending_hwm_updates) == 2
        assert p._pending_hwm_updates[1] == {"key": "k2", "value": 2}


# ===========================================================================
# Pipeline._get_databricks_*
# ===========================================================================


class TestGetDatabricksIds:
    def test_cluster_id_no_session(self):
        p = _make_pipeline()
        p.engine.spark = None
        assert p._get_databricks_cluster_id() is None

    def test_job_id_no_session(self):
        p = _make_pipeline()
        p.engine.spark = None
        assert p._get_databricks_job_id() is None

    def test_workspace_id_no_session_no_env(self):
        p = _make_pipeline()
        p.engine.spark = None
        with patch.dict("os.environ", {}, clear=True):
            assert p._get_databricks_workspace_id() is None

    def test_workspace_id_from_env_plain(self):
        p = _make_pipeline()
        p.engine.spark = None
        with patch.dict("os.environ", {"DATABRICKS_WORKSPACE_ID": "ws-123"}, clear=True):
            assert p._get_databricks_workspace_id() == "ws-123"

    def test_workspace_id_from_env_url(self):
        p = _make_pipeline()
        p.engine.spark = None
        url = "https://adb-1234567890.1.azuredatabricks.net"
        with patch.dict("os.environ", {"DATABRICKS_HOST": url}, clear=True):
            result = p._get_databricks_workspace_id()
            assert result == "adb-1234567890"

    def test_cluster_id_conf_raises(self):
        p = _make_pipeline()
        p.engine.spark = MagicMock()
        p.engine.spark.conf.get.side_effect = Exception("no conf")
        assert p._get_databricks_cluster_id() is None

    def test_job_id_conf_raises(self):
        p = _make_pipeline()
        p.engine.spark = MagicMock()
        p.engine.spark.conf.get.side_effect = Exception("no conf")
        assert p._get_databricks_job_id() is None


# ===========================================================================
# Pipeline._flush_batch_writes
# ===========================================================================


class TestFlushBatchWrites:
    def test_no_catalog_manager_returns(self):
        p = _make_pipeline(catalog_manager=None)
        p._pending_lineage_records = [{"a": 1}]
        p._flush_batch_writes()
        # Records are NOT cleared because we returned early
        assert p._pending_lineage_records == [{"a": 1}]

    def test_lineage_records_flushed(self):
        cm = MagicMock()
        p = _make_pipeline(catalog_manager=cm)
        recs = [{"src": "a", "tgt": "b"}]
        p._pending_lineage_records = recs
        p._flush_batch_writes()
        cm.record_lineage_batch.assert_called_once_with(recs)
        assert p._pending_lineage_records == []

    def test_asset_records_flushed(self):
        cm = MagicMock()
        p = _make_pipeline(catalog_manager=cm)
        recs = [{"table": "t1"}]
        p._pending_asset_records = recs
        p._flush_batch_writes()
        cm.register_assets_batch.assert_called_once_with(recs)
        assert p._pending_asset_records == []

    @patch("odibi.pipeline.create_state_backend")
    @patch("odibi.pipeline.StateManager")
    def test_hwm_updates_flushed(self, mock_sm_cls, mock_backend):
        cm = MagicMock()
        pc = MagicMock()
        p = _make_pipeline(catalog_manager=cm, project_config=pc)
        updates = [{"key": "k", "value": "v"}]
        p._pending_hwm_updates = updates
        p._flush_batch_writes()
        mock_sm_cls.return_value.set_hwm_batch.assert_called_once_with(updates)
        assert p._pending_hwm_updates == []

    def test_hwm_no_project_config(self):
        cm = MagicMock()
        p = _make_pipeline(catalog_manager=cm, project_config=None)
        p._pending_hwm_updates = [{"key": "k", "value": "v"}]
        p._flush_batch_writes()
        # No error, records cleared
        assert p._pending_hwm_updates == []

    def test_lineage_exception_non_fatal(self):
        cm = MagicMock()
        cm.record_lineage_batch.side_effect = RuntimeError("db error")
        p = _make_pipeline(catalog_manager=cm)
        p._pending_lineage_records = [{"x": 1}]
        p._flush_batch_writes()
        p._ctx.warning.assert_called()
        assert p._pending_lineage_records == []

    def test_asset_exception_non_fatal(self):
        cm = MagicMock()
        cm.register_assets_batch.side_effect = RuntimeError("db error")
        p = _make_pipeline(catalog_manager=cm)
        p._pending_asset_records = [{"x": 1}]
        p._flush_batch_writes()
        p._ctx.warning.assert_called()
        assert p._pending_asset_records == []

    @patch("odibi.pipeline.create_state_backend", side_effect=RuntimeError("fail"))
    def test_hwm_exception_non_fatal(self, _):
        cm = MagicMock()
        pc = MagicMock()
        p = _make_pipeline(catalog_manager=cm, project_config=pc)
        p._pending_hwm_updates = [{"key": "k", "value": "v"}]
        p._flush_batch_writes()
        p._ctx.warning.assert_called()
        assert p._pending_hwm_updates == []

    def test_empty_buffers_no_calls(self):
        cm = MagicMock()
        p = _make_pipeline(catalog_manager=cm)
        p._flush_batch_writes()
        cm.record_lineage_batch.assert_not_called()
        cm.register_assets_batch.assert_not_called()


# ===========================================================================
# Pipeline._sync_catalog_if_configured
# ===========================================================================


class TestSyncCatalogIfConfigured:
    def test_no_catalog_manager(self):
        p = _make_pipeline(catalog_manager=None)
        p._sync_catalog_if_configured()
        # No error

    def test_no_project_config(self):
        cm = MagicMock()
        p = _make_pipeline(catalog_manager=cm, project_config=None)
        p._sync_catalog_if_configured()

    def test_no_system_attr(self):
        cm = MagicMock()
        pc = MagicMock()
        pc.system = None
        p = _make_pipeline(catalog_manager=cm, project_config=pc)
        p._sync_catalog_if_configured()

    def test_no_sync_to(self):
        cm = MagicMock()
        pc = MagicMock()
        pc.system.sync_to = None
        p = _make_pipeline(catalog_manager=cm, project_config=pc)
        p._sync_catalog_if_configured()

    def test_sync_on_not_after_run(self):
        cm = MagicMock()
        pc = MagicMock()
        pc.system.sync_to.on = "manual"
        p = _make_pipeline(catalog_manager=cm, project_config=pc)
        p._sync_catalog_if_configured()

    @patch("odibi.pipeline.CatalogSyncer", create=True)
    def test_sync_connection_not_found(self, _):
        cm = MagicMock()
        pc = MagicMock()
        pc.system.sync_to.on = "after_run"
        pc.system.sync_to.connection = "missing_conn"
        p = _make_pipeline(catalog_manager=cm, project_config=pc, connections={})
        with patch("odibi.catalog_sync.CatalogSyncer", create=True):
            p._sync_catalog_if_configured()
        p._ctx.warning.assert_called()

    @patch("odibi.catalog_sync.CatalogSyncer", create=True)
    def test_sync_sync_mode(self, mock_syncer_cls):
        cm = MagicMock()
        pc = MagicMock()
        pc.system.sync_to.on = "after_run"
        pc.system.sync_to.connection = "target"
        pc.system.sync_to.async_sync = False
        target_conn = MagicMock()
        mock_syncer_cls.return_value.sync.return_value = {"t1": {"success": True}}
        p = _make_pipeline(
            catalog_manager=cm,
            project_config=pc,
            connections={"target": target_conn},
        )
        p._sync_catalog_if_configured()
        mock_syncer_cls.return_value.sync.assert_called_once()

    @patch("odibi.catalog_sync.CatalogSyncer", create=True)
    def test_sync_async_mode(self, mock_syncer_cls):
        cm = MagicMock()
        pc = MagicMock()
        pc.system.sync_to.on = "after_run"
        pc.system.sync_to.connection = "target"
        pc.system.sync_to.async_sync = True
        target_conn = MagicMock()
        mock_thread = MagicMock()
        mock_syncer_cls.return_value.sync_async.return_value = mock_thread
        p = _make_pipeline(
            catalog_manager=cm,
            project_config=pc,
            connections={"target": target_conn},
        )
        p._sync_catalog_if_configured()
        assert p._sync_thread is mock_thread

    @patch("odibi.catalog_sync.CatalogSyncer", side_effect=RuntimeError("import fail"), create=True)
    def test_sync_exception_non_fatal(self, _):
        cm = MagicMock()
        pc = MagicMock()
        pc.system.sync_to.on = "after_run"
        pc.system.sync_to.connection = "target"
        p = _make_pipeline(
            catalog_manager=cm,
            project_config=pc,
            connections={"target": MagicMock()},
        )
        p._sync_catalog_if_configured()
        p._ctx.warning.assert_called()


# ===========================================================================
# PipelineResults.get_node_result
# ===========================================================================


class TestGetNodeResult:
    def test_existing_node(self):
        nr = _make_node_result("a")
        r = PipelineResults(pipeline_name="p", node_results={"a": nr})
        assert r.get_node_result("a") is nr

    def test_missing_node(self):
        r = PipelineResults(pipeline_name="p", node_results={})
        assert r.get_node_result("missing") is None


# ===========================================================================
# Pipeline context manager
# ===========================================================================


class TestContextManager:
    def test_enter_returns_self(self):
        p = _make_pipeline()
        assert p.__enter__() is p

    def test_exit_calls_cleanup(self):
        conn = MagicMock()
        p = _make_pipeline(connections={"db": conn})
        p.__exit__(None, None, None)
        conn.close.assert_called_once()


# ===========================================================================
# Debug summary — long error truncation
# ===========================================================================


class TestDebugSummaryTruncation:
    def test_error_message_truncated_to_200_chars(self):
        long_msg = "x" * 500
        nr = _make_node_result("n", success=False, error=ValueError(long_msg))
        r = PipelineResults(
            pipeline_name="p",
            failed=["n"],
            node_results={"n": nr},
        )
        summary = r.debug_summary()
        # The error in the summary should be at most 200 chars of the original
        for line in summary.split("\n"):
            if "• n:" in line:
                error_part = line.split(": ", 1)[1]
                assert len(error_part) <= 200


# ===========================================================================
# PipelineResults defaults
# ===========================================================================


class TestPipelineResultsDefaults:
    def test_default_duration(self):
        r = PipelineResults(pipeline_name="p")
        assert r.duration == 0.0

    def test_default_story_path(self):
        r = PipelineResults(pipeline_name="p")
        assert r.story_path is None

    def test_default_start_end_times(self):
        r = PipelineResults(pipeline_name="p")
        assert r.start_time is None
        assert r.end_time is None
