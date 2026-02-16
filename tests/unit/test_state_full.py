"""Comprehensive unit tests for odibi/state/__init__.py.

Covers: _retry_delta_operation, LocalJSONStateBackend, CatalogStateBackend,
SqlServerSystemBackend, StateManager, create_state_backend,
create_sync_source_backend, sync_system_data, _sync_runs, _sync_state.
"""

import os
import tempfile
from unittest.mock import MagicMock, Mock, patch

import pytest

from odibi.state import (
    CatalogStateBackend,
    LocalJSONStateBackend,
    SqlServerSystemBackend,
    StateManager,
    _retry_delta_operation,
    _sync_runs,
    _sync_state,
    create_state_backend,
    create_sync_source_backend,
    sync_system_data,
)


# ---------------------------------------------------------------------------
# _retry_delta_operation
# ---------------------------------------------------------------------------
class TestRetryOperation:
    def test_success_first_try(self):
        assert _retry_delta_operation(lambda: 99) == 99

    def test_retry_on_concurrent_error(self):
        call_count = 0

        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("ConcurrentAppendException: conflict")
            return "done"

        with patch("odibi.state.time.sleep"):
            result = _retry_delta_operation(flaky, max_retries=5, base_delay=0.01)
        assert result == "done"
        assert call_count == 3

    def test_non_concurrent_raises_immediately(self):
        with pytest.raises(TypeError, match="bad type"):
            _retry_delta_operation(lambda: (_ for _ in ()).throw(TypeError("bad type")))

    def test_max_retries_exceeded(self):
        def always_fails():
            raise Exception("concurrent conflict oops")

        with patch("odibi.state.time.sleep"):
            with pytest.raises(Exception, match="concurrent"):
                _retry_delta_operation(always_fails, max_retries=2, base_delay=0.01)


# ---------------------------------------------------------------------------
# LocalJSONStateBackend
# ---------------------------------------------------------------------------
class TestLocalJSONStateBackend:
    def _make(self, tmpdir):
        return LocalJSONStateBackend(os.path.join(tmpdir, "state.json"))

    def test_load_state_empty(self):
        with tempfile.TemporaryDirectory() as d:
            b = self._make(d)
            assert b.load_state() == {"pipelines": {}, "hwm": {}}

    def test_save_pipeline_run_and_load(self):
        with tempfile.TemporaryDirectory() as d:
            b = self._make(d)
            b.save_pipeline_run("p1", {"nodes": {"n1": {"success": True}}})
            assert b.load_state()["pipelines"]["p1"]["nodes"]["n1"]["success"] is True

    def test_get_last_run_info_found(self):
        with tempfile.TemporaryDirectory() as d:
            b = self._make(d)
            b.save_pipeline_run("p1", {"nodes": {"n1": {"success": True, "ts": 1}}})
            info = b.get_last_run_info("p1", "n1")
            assert info == {"success": True, "ts": 1}

    def test_get_last_run_info_not_found(self):
        with tempfile.TemporaryDirectory() as d:
            b = self._make(d)
            assert b.get_last_run_info("p1", "n1") is None

    def test_get_last_run_status_delegates(self):
        with tempfile.TemporaryDirectory() as d:
            b = self._make(d)
            b.save_pipeline_run("p1", {"nodes": {"n1": {"success": False}}})
            assert b.get_last_run_status("p1", "n1") is False

    def test_get_last_run_status_missing(self):
        with tempfile.TemporaryDirectory() as d:
            b = self._make(d)
            assert b.get_last_run_status("p1", "n1") is None

    def test_get_set_hwm(self):
        with tempfile.TemporaryDirectory() as d:
            b = self._make(d)
            b.set_hwm("k1", "2024-01-01")
            assert b.get_hwm("k1") == "2024-01-01"

    def test_get_hwm_missing(self):
        with tempfile.TemporaryDirectory() as d:
            b = self._make(d)
            assert b.get_hwm("missing") is None

    def test_corrupt_json_fallback(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "state.json")
            with open(path, "w") as f:
                f.write("{{{broken json")
            b = LocalJSONStateBackend(path)
            assert b.load_state() == {"pipelines": {}, "hwm": {}}

    def test_set_hwm_batch(self):
        with tempfile.TemporaryDirectory() as d:
            b = self._make(d)
            b.set_hwm_batch([{"key": "a", "value": 1}, {"key": "b", "value": 2}])
            assert b.get_hwm("a") == 1
            assert b.get_hwm("b") == 2


# ---------------------------------------------------------------------------
# CatalogStateBackend
# ---------------------------------------------------------------------------
class TestCatalogStateBackend:
    def _make(self, **kwargs):
        defaults = {
            "meta_runs_path": "/tmp/runs",
            "meta_state_path": "/tmp/state",
        }
        defaults.update(kwargs)
        return CatalogStateBackend(**defaults)

    def test_load_state_returns_empty(self):
        b = self._make()
        assert b.load_state() == {"pipelines": {}}

    def test_save_pipeline_run_is_noop(self):
        b = self._make()
        b.save_pipeline_run("p1", {"nodes": {}})  # should not raise

    def test_get_last_run_status_delegates(self):
        b = self._make()
        b.get_last_run_info = Mock(return_value={"success": True})
        assert b.get_last_run_status("p1", "n1") is True

    def test_get_last_run_status_none(self):
        b = self._make()
        b.get_last_run_info = Mock(return_value=None)
        assert b.get_last_run_status("p1", "n1") is None

    # ---- _get_last_run_local ----
    @patch("odibi.state.DeltaTable", None)
    def test_get_last_run_local_no_lib(self):
        b = self._make()
        assert b._get_last_run_local("p1", "n1") is None

    @patch("odibi.state.DeltaTable")
    def test_get_last_run_local_success(self, mock_dt_cls):
        import pandas as pd

        df = pd.DataFrame(
            {
                "pipeline_name": ["p1"],
                "node_name": ["n1"],
                "status": ["SUCCESS"],
                "metadata": ['{"rows": 100}'],
                "timestamp": [pd.Timestamp("2024-01-01")],
            }
        )
        mock_table = MagicMock()
        mock_table.num_rows = 1
        mock_table.to_pandas.return_value = df

        mock_ds = MagicMock()
        mock_ds.to_table.return_value = mock_table

        mock_dt = MagicMock()
        mock_dt.to_pyarrow_dataset.return_value = mock_ds
        mock_dt_cls.return_value = mock_dt

        b = self._make()
        result = b._get_last_run_local("p1", "n1")
        assert result is not None
        assert result["success"] is True
        assert result["metadata"] == {"rows": 100}

    # ---- get_hwm / set_hwm local path ----
    @patch("odibi.state.DeltaTable", None)
    def test_get_hwm_local_no_lib(self):
        b = self._make()
        assert b.get_hwm("key1") is None

    @patch("odibi.state.DeltaTable")
    def test_get_hwm_local_found(self, mock_dt_cls):
        mock_col = MagicMock()
        mock_col.__getitem__ = Mock(return_value=MagicMock(as_py=Mock(return_value='"hello"')))

        mock_table = MagicMock()
        mock_table.num_rows = 1
        mock_table.column.return_value = mock_col

        mock_ds = MagicMock()
        mock_ds.to_table.return_value = mock_table

        mock_dt = MagicMock()
        mock_dt.to_pyarrow_dataset.return_value = mock_ds
        mock_dt_cls.return_value = mock_dt

        b = self._make()
        result = b._get_hwm_local("key1")
        assert result == "hello"

    # ---- get_hwm / set_hwm session path ----
    def test_get_hwm_with_session(self):
        mock_row = MagicMock()
        mock_row.value = '"42"'
        mock_session = MagicMock()
        mock_session.read.format.return_value.load.return_value.filter.return_value.select.return_value.first.return_value = (  # noqa: E501
            mock_row
        )

        b = self._make(spark_session=mock_session)

        mock_F = MagicMock()
        with patch.dict(
            "sys.modules",
            {"pyspark": MagicMock(), "pyspark.sql": MagicMock(), "pyspark.sql.functions": mock_F},
        ):  # noqa: E501
            mock_F.col.return_value = "key"
            result = b._get_hwm_spark("k")
        assert result == "42"

    @patch("odibi.state.time.sleep")
    def test_set_hwm_with_session(self, mock_sleep):
        mock_session = MagicMock()
        mock_session.read.format.return_value.load.return_value.count.return_value = 0

        b = self._make(spark_session=mock_session)
        b.set_hwm("k", "v")
        mock_session.createDataFrame.assert_called_once()

    @patch("odibi.state.time.sleep")
    @patch("odibi.state.DeltaTable")
    @patch("odibi.state.write_deltalake")
    @patch("odibi.state.pd")
    def test_set_hwm_local(self, mock_pd, mock_write, mock_dt_cls, mock_sleep):
        mock_df = MagicMock()
        mock_pd.DataFrame.return_value = mock_df
        mock_pd.to_datetime = MagicMock()

        mock_dt = MagicMock()
        mock_dt_cls.return_value = mock_dt

        b = self._make()
        b.set_hwm("k", "v")
        mock_dt.merge.assert_called_once()


# ---------------------------------------------------------------------------
# SqlServerSystemBackend
# ---------------------------------------------------------------------------
class TestSqlServerSystemBackend:
    def _make(self, **kwargs):
        conn = MagicMock()
        defaults = {"connection": conn, "schema_name": "odibi_sys", "environment": "test"}
        defaults.update(kwargs)
        return SqlServerSystemBackend(**defaults), conn

    def test_ensure_tables_creates_once(self):
        b, conn = self._make()
        b._ensure_tables()
        b._ensure_tables()
        assert conn.execute.call_count == 3  # schema + runs DDL + state DDL

    def test_load_state(self):
        b, _ = self._make()
        assert b.load_state() == {"pipelines": {}}

    def test_save_pipeline_run_noop(self):
        b, conn = self._make()
        b.save_pipeline_run("p1", {})
        conn.execute.assert_not_called()

    def test_get_last_run_info_found(self):
        b, conn = self._make()
        conn.execute.return_value = [("SUCCESS", '{"rows": 10}')]
        info = b.get_last_run_info("p1", "n1")
        assert info["success"] is True
        assert info["metadata"] == {"rows": 10}

    def test_get_last_run_info_not_found(self):
        b, conn = self._make()
        conn.execute.return_value = []
        assert b.get_last_run_info("p1", "n1") is None

    def test_get_last_run_info_exception(self):
        b, conn = self._make()
        conn.execute.side_effect = [None, None, None, Exception("db down")]
        assert b.get_last_run_info("p1", "n1") is None

    def test_get_hwm_found_json(self):
        b, conn = self._make()
        conn.execute.return_value = [("42",)]
        result = b.get_hwm("k")
        assert result == 42

    def test_get_hwm_found_plain(self):
        b, conn = self._make()
        conn.execute.return_value = [("not-json",)]
        result = b.get_hwm("k")
        assert result == "not-json"

    def test_get_hwm_not_found(self):
        b, conn = self._make()
        conn.execute.return_value = []
        assert b.get_hwm("k") is None

    def test_set_hwm(self):
        b, conn = self._make()
        b.set_hwm("k", {"v": 1})
        # 3 from _ensure_tables + 1 from set_hwm
        assert conn.execute.call_count == 4

    def test_log_run(self):
        b, conn = self._make()
        b.log_run("r1", "p1", "n1", "SUCCESS", rows_processed=100, duration_ms=500)
        # 3 from _ensure_tables + 1 from log_run
        assert conn.execute.call_count == 4

    def test_log_runs_batch(self):
        b, conn = self._make()
        records = [
            {
                "run_id": "r1",
                "pipeline_name": "p1",
                "node_name": "n1",
                "status": "SUCCESS",
                "rows_processed": 10,
                "duration_ms": 100,
                "metrics_json": "{}",
            },
            {
                "run_id": "r2",
                "pipeline_name": "p1",
                "node_name": "n2",
                "status": "FAILED",
            },
        ]
        b.log_runs_batch(records)
        # 3 from first _ensure_tables + 1 insert + 1 insert = 5
        assert conn.execute.call_count == 5

    def test_set_hwm_batch(self):
        b, conn = self._make()
        b.set_hwm_batch([{"key": "a", "value": 1}, {"key": "b", "value": 2}])
        # 3 (ensure) + 2 (two set_hwm calls)
        assert conn.execute.call_count == 5


# ---------------------------------------------------------------------------
# StateManager
# ---------------------------------------------------------------------------
class TestStateManagerFull:
    def test_requires_backend(self):
        with pytest.raises(ValueError, match="StateBackend must be provided"):
            StateManager(backend=None)

    def test_save_pipeline_run_dict(self):
        backend = MagicMock()
        backend.load_state.return_value = {"pipelines": {}}
        mgr = StateManager(backend=backend)
        mgr.save_pipeline_run("p1", {"end_time": "2024-01-01"})
        backend.save_pipeline_run.assert_called_once()

    def test_get_set_hwm_proxies(self):
        backend = MagicMock()
        backend.load_state.return_value = {"pipelines": {}}
        backend.get_hwm.return_value = 42
        mgr = StateManager(backend=backend)
        mgr.set_hwm("k", 42)
        assert mgr.get_hwm("k") == 42
        backend.set_hwm.assert_called_once_with("k", 42)
        backend.get_hwm.assert_called_once_with("k")

    def test_set_hwm_batch_proxies(self):
        backend = MagicMock()
        backend.load_state.return_value = {"pipelines": {}}
        mgr = StateManager(backend=backend)
        updates = [{"key": "a", "value": 1}]
        mgr.set_hwm_batch(updates)
        backend.set_hwm_batch.assert_called_once_with(updates)

    def test_get_last_run_info_proxies(self):
        backend = MagicMock()
        backend.load_state.return_value = {"pipelines": {}}
        backend.get_last_run_info.return_value = {"success": True}
        mgr = StateManager(backend=backend)
        assert mgr.get_last_run_info("p", "n") == {"success": True}

    def test_get_last_run_status_proxies(self):
        backend = MagicMock()
        backend.load_state.return_value = {"pipelines": {}}
        backend.get_last_run_status.return_value = False
        mgr = StateManager(backend=backend)
        assert mgr.get_last_run_status("p", "n") is False


# ---------------------------------------------------------------------------
# create_state_backend
# ---------------------------------------------------------------------------
class TestCreateStateBackend:
    def test_no_system_config_returns_local_json(self):
        config = Mock()
        config.system = None
        backend = create_state_backend(config, project_root="/tmp")
        assert isinstance(backend, LocalJSONStateBackend)

    def test_local_connection(self):
        config = Mock()
        config.system.connection = "local_conn"
        config.system.path = "_system"
        config.system.environment = "dev"
        config.connections = {
            "local_conn": {"type": "local", "base_path": "/data"},
        }
        backend = create_state_backend(config, project_root="/tmp")
        assert isinstance(backend, CatalogStateBackend)

    def test_azure_blob_connection(self):
        config = Mock()
        config.system.connection = "az"
        config.system.path = "_system"
        config.system.environment = "prod"
        config.connections = {
            "az": {
                "type": "azure_blob",
                "account_name": "acct",
                "container": "ctr",
                "auth": {"mode": "account_key", "account_key": "k1"},
            },
        }
        backend = create_state_backend(config, project_root="/tmp")
        assert isinstance(backend, CatalogStateBackend)
        assert backend.storage_options["account_key"] == "k1"

    @patch("odibi.connections.factory.create_connection", create=True)
    def test_sql_server_connection(self, mock_create_conn):
        mock_conn = MagicMock()
        mock_create_conn.return_value = mock_conn
        config = Mock()
        config.system.connection = "sql"
        config.system.environment = "prod"
        config.system.schema_name = "my_schema"
        config.connections = {"sql": {"type": "sql_server"}}
        backend = create_state_backend(config, project_root="/tmp")
        assert isinstance(backend, SqlServerSystemBackend)
        assert backend.schema_name == "my_schema"

    def test_missing_connection_raises(self):
        config = Mock()
        config.system.connection = "missing"
        config.connections = {}
        with pytest.raises(ValueError, match="not found"):
            create_state_backend(config, project_root="/tmp")


# ---------------------------------------------------------------------------
# create_sync_source_backend
# ---------------------------------------------------------------------------
class TestCreateSyncSourceBackend:
    def test_local_connection(self):
        sync_cfg = {"connection": "local_conn", "path": "_sys"}
        conns = {"local_conn": {"type": "local", "base_path": "/data"}}
        backend = create_sync_source_backend(sync_cfg, conns, project_root="/tmp")
        assert isinstance(backend, CatalogStateBackend)

    def test_azure_blob_connection_sas(self):
        sync_cfg = {"connection": "az", "path": "_sys"}
        conns = {
            "az": {
                "type": "azure_blob",
                "account_name": "acct",
                "container": "ctr",
                "auth": {"mode": "sas", "sas_token": "tok"},
            },
        }
        backend = create_sync_source_backend(sync_cfg, conns, project_root="/tmp")
        assert isinstance(backend, CatalogStateBackend)
        assert backend.storage_options["sas_token"] == "tok"

    @patch("odibi.connections.factory.create_connection", create=True)
    def test_sql_server_connection(self, mock_create_conn):
        mock_conn = MagicMock()
        mock_create_conn.return_value = mock_conn
        sync_cfg = {"connection": "sql", "schema_name": "s1"}
        conns = {"sql": {"type": "sql_server"}}
        backend = create_sync_source_backend(sync_cfg, conns, project_root="/tmp")
        assert isinstance(backend, SqlServerSystemBackend)

    def test_missing_connection_raises(self):
        sync_cfg = {"connection": "nope"}
        with pytest.raises(ValueError, match="not found"):
            create_sync_source_backend(sync_cfg, {}, project_root="/tmp")

    def test_fallback_local_path(self):
        sync_cfg = {"connection": "c"}
        conns = {"c": {"type": "unknown_type"}}
        backend = create_sync_source_backend(sync_cfg, conns, project_root="/tmp")
        assert isinstance(backend, CatalogStateBackend)


# ---------------------------------------------------------------------------
# sync_system_data, _sync_runs, _sync_state
# ---------------------------------------------------------------------------
class TestSyncSystemData:
    def test_sync_system_data_both(self):
        source = MagicMock(spec=SqlServerSystemBackend)
        target = MagicMock(spec=SqlServerSystemBackend)

        with (
            patch("odibi.state._sync_runs", return_value=5) as mock_runs,
            patch("odibi.state._sync_state", return_value=3) as mock_state,
        ):
            result = sync_system_data(source, target)

        assert result == {"runs": 5, "state": 3}
        mock_runs.assert_called_once_with(source, target)
        mock_state.assert_called_once_with(source, target)

    def test_sync_system_data_runs_only(self):
        source = MagicMock()
        target = MagicMock()
        with patch("odibi.state._sync_runs", return_value=2):
            result = sync_system_data(source, target, tables=["runs"])
        assert result == {"runs": 2, "state": 0}

    def test_sync_system_data_state_only(self):
        source = MagicMock()
        target = MagicMock()
        with patch("odibi.state._sync_state", return_value=4):
            result = sync_system_data(source, target, tables=["state"])
        assert result == {"runs": 0, "state": 4}


class TestSyncRuns:
    def test_sql_server_source_to_sql_server_target(self):
        source = MagicMock(spec=SqlServerSystemBackend)
        source.schema_name = "s"
        source.connection = MagicMock()
        source.connection.execute.return_value = [
            ("r1", "p1", "n1", "SUCCESS", 10, 100, "{}"),
        ]

        target = MagicMock(spec=SqlServerSystemBackend)

        count = _sync_runs(source, target)
        assert count == 1
        target.log_runs_batch.assert_called_once()

    def test_sql_server_source_empty(self):
        source = MagicMock(spec=SqlServerSystemBackend)
        source.schema_name = "s"
        source.connection = MagicMock()
        source.connection.execute.return_value = []

        target = MagicMock(spec=SqlServerSystemBackend)
        count = _sync_runs(source, target)
        assert count == 0

    def test_sql_server_source_exception(self):
        source = MagicMock(spec=SqlServerSystemBackend)
        source.schema_name = "s"
        source.connection = MagicMock()
        source.connection.execute.side_effect = Exception("db error")

        target = MagicMock(spec=SqlServerSystemBackend)
        count = _sync_runs(source, target)
        assert count == 0


class TestSyncState:
    def test_sql_server_source_to_target(self):
        source = MagicMock(spec=SqlServerSystemBackend)
        source.schema_name = "s"
        source.connection = MagicMock()
        source.connection.execute.return_value = [
            ("key1", '"val1"'),
            ("key2", "plain_val"),
        ]

        target = MagicMock()
        count = _sync_state(source, target)
        assert count == 2
        target.set_hwm_batch.assert_called_once()
        batch = target.set_hwm_batch.call_args[0][0]
        assert batch[0] == {"key": "key1", "value": "val1"}
        assert batch[1] == {"key": "key2", "value": "plain_val"}

    def test_sql_server_source_empty(self):
        source = MagicMock(spec=SqlServerSystemBackend)
        source.schema_name = "s"
        source.connection = MagicMock()
        source.connection.execute.return_value = []

        target = MagicMock()
        count = _sync_state(source, target)
        assert count == 0

    def test_sql_server_source_exception(self):
        source = MagicMock(spec=SqlServerSystemBackend)
        source.schema_name = "s"
        source.connection = MagicMock()
        source.connection.execute.side_effect = Exception("fail")

        target = MagicMock()
        count = _sync_state(source, target)
        assert count == 0

    def test_null_keys_skipped(self):
        source = MagicMock(spec=SqlServerSystemBackend)
        source.schema_name = "s"
        source.connection = MagicMock()
        source.connection.execute.return_value = [
            (None, "val"),
            ("key1", '"v1"'),
        ]

        target = MagicMock()
        count = _sync_state(source, target)
        assert count == 1
