"""Tests for odibi.state — StateBackend, LocalJSON, Catalog, SqlServer, StateManager, factories."""

import json
import logging
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from odibi.state import (
    CatalogStateBackend,
    LocalJSONStateBackend,
    SqlServerSystemBackend,
    StateManager,
    _sync_runs,
    _sync_state,
    _write_runs_to_catalog,
    create_state_backend,
    create_sync_source_backend,
    sync_system_data,
)

logging.getLogger("odibi").propagate = False


# ===========================================================================
# LocalJSONStateBackend
# ===========================================================================


class TestLocalJSONInit:
    def test_init_nonexistent_file(self, tmp_path):
        path = str(tmp_path / "state.json")
        backend = LocalJSONStateBackend(path)
        assert backend.state == {"pipelines": {}, "hwm": {}}

    def test_init_valid_json(self, tmp_path):
        path = tmp_path / "state.json"
        path.write_text(json.dumps({"pipelines": {"p1": {}}, "hwm": {"k": "v"}}))
        backend = LocalJSONStateBackend(str(path))
        assert backend.state["pipelines"] == {"p1": {}}
        assert backend.state["hwm"]["k"] == "v"

    def test_init_corrupted_json(self, tmp_path):
        path = tmp_path / "state.json"
        path.write_text("not valid json{{{")
        backend = LocalJSONStateBackend(str(path))
        assert backend.state == {"pipelines": {}, "hwm": {}}


class TestLocalJSONOperations:
    def test_load_state(self, tmp_path):
        path = str(tmp_path / "state.json")
        backend = LocalJSONStateBackend(path)
        assert backend.load_state() == {"pipelines": {}, "hwm": {}}

    def test_save_pipeline_run(self, tmp_path):
        path = str(tmp_path / "state.json")
        backend = LocalJSONStateBackend(path)
        backend.save_pipeline_run("pipe1", {"last_run": "2026-01-01", "nodes": {}})
        assert backend.state["pipelines"]["pipe1"]["last_run"] == "2026-01-01"
        # Verify persisted to disk
        with open(path) as f:
            data = json.load(f)
        assert "pipe1" in data["pipelines"]

    def test_save_pipeline_creates_pipelines_key(self, tmp_path):
        path = str(tmp_path / "state.json")
        backend = LocalJSONStateBackend(path)
        backend.state = {}  # Remove pipelines key
        backend.save_pipeline_run("p", {"nodes": {}})
        assert "p" in backend.state["pipelines"]

    def test_get_last_run_info_found(self, tmp_path):
        path = str(tmp_path / "state.json")
        backend = LocalJSONStateBackend(path)
        backend.state = {"pipelines": {"p": {"nodes": {"n": {"success": True, "ts": "1"}}}}}
        info = backend.get_last_run_info("p", "n")
        assert info["success"] is True

    def test_get_last_run_info_not_found(self, tmp_path):
        path = str(tmp_path / "state.json")
        backend = LocalJSONStateBackend(path)
        assert backend.get_last_run_info("p", "n") is None

    def test_get_last_run_status_true(self, tmp_path):
        path = str(tmp_path / "state.json")
        backend = LocalJSONStateBackend(path)
        backend.state = {"pipelines": {"p": {"nodes": {"n": {"success": True}}}}}
        assert backend.get_last_run_status("p", "n") is True

    def test_get_last_run_status_none(self, tmp_path):
        path = str(tmp_path / "state.json")
        backend = LocalJSONStateBackend(path)
        assert backend.get_last_run_status("p", "n") is None

    def test_get_hwm(self, tmp_path):
        path = str(tmp_path / "state.json")
        backend = LocalJSONStateBackend(path)
        backend.state = {"hwm": {"key1": "val1"}}
        assert backend.get_hwm("key1") == "val1"
        assert backend.get_hwm("missing") is None

    def test_set_hwm(self, tmp_path):
        path = str(tmp_path / "state.json")
        backend = LocalJSONStateBackend(path)
        backend.set_hwm("k1", "v1")
        assert backend.state["hwm"]["k1"] == "v1"

    def test_set_hwm_creates_hwm_key(self, tmp_path):
        path = str(tmp_path / "state.json")
        backend = LocalJSONStateBackend(path)
        backend.state = {}
        backend.set_hwm("k", "v")
        assert backend.state["hwm"]["k"] == "v"

    def test_save_to_disk_creates_directories(self, tmp_path):
        path = str(tmp_path / "sub" / "dir" / "state.json")
        backend = LocalJSONStateBackend(path)
        backend.set_hwm("k", "v")
        assert os.path.exists(path)


# ===========================================================================
# CatalogStateBackend
# ===========================================================================


class TestCatalogStateInit:
    def test_init_sets_properties(self):
        backend = CatalogStateBackend(
            meta_runs_path="/runs",
            meta_state_path="/state",
            spark_session=None,
            storage_options={"key": "val"},
            environment="prod",
        )
        assert backend.meta_runs_path == "/runs"
        assert backend.meta_state_path == "/state"
        assert backend.spark is None
        assert backend.storage_options == {"key": "val"}
        assert backend.environment == "prod"

    def test_load_state_returns_empty(self):
        backend = CatalogStateBackend("/r", "/s")
        assert backend.load_state() == {"pipelines": {}}

    def test_save_pipeline_run_noop(self):
        backend = CatalogStateBackend("/r", "/s")
        backend.save_pipeline_run("p", {"data": True})  # Should not raise


class TestCatalogStateLastRunLocal:
    def test_get_last_run_local_no_table(self, tmp_path):
        backend = CatalogStateBackend(
            meta_runs_path=str(tmp_path / "nonexistent"),
            meta_state_path=str(tmp_path / "state"),
        )
        result = backend.get_last_run_info("p", "n")
        assert result is None

    def test_get_last_run_local_with_data(self, tmp_path):
        runs_path = str(tmp_path / "runs")
        # Code uses row.get("metadata") not "metrics_json"
        table = pa.table(
            {
                "pipeline_name": pa.array(["p", "p"], type=pa.string()),
                "node_name": pa.array(["n", "n"], type=pa.string()),
                "status": pa.array(["SUCCESS", "FAILURE"], type=pa.string()),
                "timestamp": pa.array(
                    [
                        datetime(2026, 1, 2, tzinfo=timezone.utc),
                        datetime(2026, 1, 1, tzinfo=timezone.utc),
                    ],
                    type=pa.timestamp("us", tz="UTC"),
                ),
                "metadata": pa.array(['{"k": 1}', "{}"], type=pa.string()),
            }
        )
        write_deltalake(runs_path, table)

        backend = CatalogStateBackend(meta_runs_path=runs_path, meta_state_path="/s")
        result = backend.get_last_run_info("p", "n")
        assert result is not None
        assert result["success"] is True
        assert result["metadata"] == {"k": 1}

    def test_get_last_run_local_no_matching_rows(self, tmp_path):
        runs_path = str(tmp_path / "runs")
        df = pd.DataFrame(
            {
                "pipeline_name": ["other"],
                "node_name": ["n"],
                "status": ["SUCCESS"],
                "timestamp": pd.to_datetime(["2026-01-01"]),
                "metrics_json": ["{}"],
            }
        )
        write_deltalake(runs_path, df)

        backend = CatalogStateBackend(meta_runs_path=runs_path, meta_state_path="/s")
        result = backend.get_last_run_info("p", "n")
        assert result is None

    def test_get_last_run_status_delegates(self, tmp_path):
        backend = CatalogStateBackend(str(tmp_path / "runs"), str(tmp_path / "state"))
        assert backend.get_last_run_status("p", "n") is None

    def test_get_last_run_info_spark_dispatch(self):
        spark = MagicMock()
        row = MagicMock()
        row.status = "SUCCESS"
        row.metrics_json = '{"x": 1}'
        spark.read.format.return_value.load.return_value.filter.return_value.select.return_value.orderBy.return_value.first.return_value = row

        backend = CatalogStateBackend("/runs", "/state", spark_session=spark)
        result = backend.get_last_run_info("p", "n")
        assert result["success"] is True
        assert result["metadata"] == {"x": 1}


class TestCatalogStateHWM:
    def test_get_hwm_local_no_table(self, tmp_path):
        backend = CatalogStateBackend("/r", str(tmp_path / "noexist"))
        assert backend.get_hwm("k") is None

    def test_get_hwm_local_with_data(self, tmp_path):
        state_path = str(tmp_path / "state")
        df = pd.DataFrame(
            {
                "key": ["test_key"],
                "value": [json.dumps({"a": 1})],
                "environment": ["dev"],
                "updated_at": pd.to_datetime(["2026-01-01"]),
            }
        )
        write_deltalake(state_path, df)

        backend = CatalogStateBackend("/r", state_path)
        result = backend.get_hwm("test_key")
        assert result == {"a": 1}

    def test_get_hwm_local_no_match(self, tmp_path):
        state_path = str(tmp_path / "state")
        df = pd.DataFrame(
            {
                "key": ["other"],
                "value": ["1"],
                "environment": ["dev"],
                "updated_at": pd.to_datetime(["2026-01-01"]),
            }
        )
        write_deltalake(state_path, df)

        backend = CatalogStateBackend("/r", state_path)
        assert backend.get_hwm("missing") is None

    def test_get_hwm_local_non_json_value(self, tmp_path):
        state_path = str(tmp_path / "state")
        table = pa.table(
            {
                "key": pa.array(["k"], type=pa.string()),
                "value": pa.array(["not-json"], type=pa.string()),
                "environment": pa.array(["dev"], type=pa.string()),
                "updated_at": pa.array(
                    [datetime(2026, 1, 1, tzinfo=timezone.utc)], type=pa.timestamp("us", tz="UTC")
                ),
            }
        )
        write_deltalake(state_path, table)

        backend = CatalogStateBackend("/r", state_path)
        result = backend.get_hwm("k")
        assert result == "not-json"

    def test_get_hwm_spark_dispatch(self):
        spark = MagicMock()
        row = MagicMock()
        row.value = '{"x": 2}'
        spark.read.format.return_value.load.return_value.filter.return_value.select.return_value.first.return_value = row

        backend = CatalogStateBackend("/r", "/s", spark_session=spark)
        result = backend.get_hwm("k")
        assert result == {"x": 2}

    def test_get_hwm_spark_path_not_found(self):
        spark = MagicMock()
        spark.read.format.return_value.load.side_effect = Exception("PATH_NOT_FOUND")

        backend = CatalogStateBackend("/r", "/s", spark_session=spark)
        assert backend.get_hwm("k") is None

    def test_set_hwm_local_new_table(self, tmp_path):
        state_path = str(tmp_path / "state")
        backend = CatalogStateBackend("/r", state_path, environment="test")
        backend.set_hwm("key1", {"val": 42})
        # Verify written
        dt = DeltaTable(state_path)
        result = dt.to_pandas()
        assert len(result) == 1
        assert result.iloc[0]["key"] == "key1"

    def test_set_hwm_local_merge_existing(self, tmp_path):
        state_path = str(tmp_path / "state")
        table = pa.table(
            {
                "key": pa.array(["key1"], type=pa.string()),
                "value": pa.array([json.dumps("old")], type=pa.string()),
                "environment": pa.array(["test"], type=pa.string()),
                "updated_at": pa.array(
                    [datetime(2026, 1, 1, tzinfo=timezone.utc)], type=pa.timestamp("us", tz="UTC")
                ),
            }
        )
        write_deltalake(state_path, table)

        backend = CatalogStateBackend("/r", state_path, environment="test")
        backend.set_hwm("key1", "new_value")

        dt = DeltaTable(state_path)
        result = dt.to_pandas()
        assert len(result) == 1
        assert json.loads(result.iloc[0]["value"]) == "new_value"

    def test_set_hwm_batch_empty(self):
        backend = CatalogStateBackend("/r", "/s")
        backend.set_hwm_batch([])  # Should not raise

    def test_set_hwm_batch_local(self, tmp_path):
        state_path = str(tmp_path / "state")
        backend = CatalogStateBackend("/r", state_path, environment="test")
        backend.set_hwm_batch(
            [
                {"key": "k1", "value": "v1"},
                {"key": "k2", "value": "v2"},
            ]
        )
        dt = DeltaTable(state_path)
        result = dt.to_pandas()
        assert len(result) == 2


class TestCatalogStateSpark:
    def test_spark_table_exists_true(self):
        spark = MagicMock()
        spark.read.format.return_value.load.return_value.count.return_value = 5
        backend = CatalogStateBackend("/r", "/s", spark_session=spark)
        assert backend._spark_table_exists("/path") is True

    def test_spark_table_exists_false(self):
        spark = MagicMock()
        spark.read.format.return_value.load.side_effect = Exception("not found")
        backend = CatalogStateBackend("/r", "/s", spark_session=spark)
        assert backend._spark_table_exists("/path") is False


# ===========================================================================
# SqlServerSystemBackend
# ===========================================================================


class TestSqlServerBackendInit:
    def test_init_defaults(self):
        conn = MagicMock()
        backend = SqlServerSystemBackend(connection=conn)
        assert backend.connection is conn
        assert backend.schema_name == "odibi_system"
        assert backend.environment is None
        assert backend._tables_created is False

    def test_init_custom(self):
        conn = MagicMock()
        backend = SqlServerSystemBackend(conn, schema_name="custom", environment="prod")
        assert backend.schema_name == "custom"
        assert backend.environment == "prod"


class TestSqlServerEnsureTables:
    def test_ensure_tables_creates(self):
        conn = MagicMock()
        backend = SqlServerSystemBackend(conn, "test_schema")
        backend._ensure_tables()
        assert conn.execute.call_count == 3  # schema + 2 tables
        assert backend._tables_created is True

    def test_ensure_tables_skips_if_done(self):
        conn = MagicMock()
        backend = SqlServerSystemBackend(conn)
        backend._tables_created = True
        backend._ensure_tables()
        conn.execute.assert_not_called()

    def test_ensure_tables_handles_error(self):
        conn = MagicMock()
        conn.execute.side_effect = Exception("DB error")
        backend = SqlServerSystemBackend(conn)
        backend._ensure_tables()  # Should not raise
        assert backend._tables_created is False


class TestSqlServerOperations:
    def test_load_state_returns_empty(self):
        conn = MagicMock()
        backend = SqlServerSystemBackend(conn)
        assert backend.load_state() == {"pipelines": {}}

    def test_save_pipeline_run_noop(self):
        conn = MagicMock()
        backend = SqlServerSystemBackend(conn)
        backend.save_pipeline_run("p", {})  # Should not raise

    def test_get_last_run_info_found(self):
        conn = MagicMock()
        conn.execute.return_value = [("SUCCESS", '{"k": 1}')]
        backend = SqlServerSystemBackend(conn)
        backend._tables_created = True
        result = backend.get_last_run_info("p", "n")
        assert result["success"] is True
        assert result["metadata"] == {"k": 1}

    def test_get_last_run_info_not_found(self):
        conn = MagicMock()
        conn.execute.return_value = []
        backend = SqlServerSystemBackend(conn)
        backend._tables_created = True
        result = backend.get_last_run_info("p", "n")
        assert result is None

    def test_get_last_run_info_error(self):
        conn = MagicMock()
        conn.execute.side_effect = Exception("error")
        backend = SqlServerSystemBackend(conn)
        backend._tables_created = True
        assert backend.get_last_run_info("p", "n") is None

    def test_get_last_run_info_invalid_json(self):
        conn = MagicMock()
        conn.execute.return_value = [("SUCCESS", "not-json")]
        backend = SqlServerSystemBackend(conn)
        backend._tables_created = True
        result = backend.get_last_run_info("p", "n")
        assert result["success"] is True
        assert result["metadata"] == {}

    def test_get_last_run_status(self):
        conn = MagicMock()
        conn.execute.return_value = [("SUCCESS", "{}")]
        backend = SqlServerSystemBackend(conn)
        backend._tables_created = True
        assert backend.get_last_run_status("p", "n") is True

    def test_get_last_run_status_none(self):
        conn = MagicMock()
        conn.execute.return_value = []
        backend = SqlServerSystemBackend(conn)
        backend._tables_created = True
        assert backend.get_last_run_status("p", "n") is None

    def test_get_hwm_json(self):
        conn = MagicMock()
        conn.execute.return_value = [('{"val": 42}',)]
        backend = SqlServerSystemBackend(conn)
        backend._tables_created = True
        result = backend.get_hwm("k")
        assert result == {"val": 42}

    def test_get_hwm_raw(self):
        conn = MagicMock()
        conn.execute.return_value = [("not-json",)]
        backend = SqlServerSystemBackend(conn)
        backend._tables_created = True
        assert backend.get_hwm("k") == "not-json"

    def test_get_hwm_not_found(self):
        conn = MagicMock()
        conn.execute.return_value = []
        backend = SqlServerSystemBackend(conn)
        backend._tables_created = True
        assert backend.get_hwm("k") is None

    def test_get_hwm_error(self):
        conn = MagicMock()
        conn.execute.side_effect = Exception("err")
        backend = SqlServerSystemBackend(conn)
        backend._tables_created = True
        assert backend.get_hwm("k") is None

    def test_set_hwm(self):
        conn = MagicMock()
        backend = SqlServerSystemBackend(conn)
        backend._tables_created = True
        backend.set_hwm("k", {"v": 1})
        assert conn.execute.call_count == 1

    def test_set_hwm_error(self):
        conn = MagicMock()
        conn.execute.side_effect = Exception("err")
        backend = SqlServerSystemBackend(conn)
        backend._tables_created = True
        backend.set_hwm("k", "v")  # Should not raise

    def test_set_hwm_batch(self):
        conn = MagicMock()
        backend = SqlServerSystemBackend(conn)
        backend._tables_created = True
        backend.set_hwm_batch([{"key": "k1", "value": "v1"}, {"key": "k2", "value": "v2"}])
        assert conn.execute.call_count == 2

    def test_log_run(self):
        conn = MagicMock()
        backend = SqlServerSystemBackend(conn, environment="dev")
        backend._tables_created = True
        backend.log_run("r1", "p1", "n1", "SUCCESS", 100, 500)
        assert conn.execute.call_count == 1

    def test_log_run_error(self):
        conn = MagicMock()
        conn.execute.side_effect = Exception("err")
        backend = SqlServerSystemBackend(conn)
        backend._tables_created = True
        backend.log_run("r1", "p1", "n1", "SUCCESS")  # Should not raise

    def test_log_runs_batch(self):
        conn = MagicMock()
        backend = SqlServerSystemBackend(conn)
        backend._tables_created = True
        records = [
            {"run_id": "r1", "pipeline_name": "p", "node_name": "n", "status": "SUCCESS"},
            {"run_id": "r2", "pipeline_name": "p", "node_name": "n", "status": "FAILURE"},
        ]
        backend.log_runs_batch(records)
        assert conn.execute.call_count == 2


# ===========================================================================
# StateManager
# ===========================================================================


class TestStateManager:
    def test_init_with_backend(self):
        backend = MagicMock()
        backend.load_state.return_value = {"pipelines": {}}
        mgr = StateManager(backend=backend)
        assert mgr.state == {"pipelines": {}}

    def test_init_no_backend_raises(self):
        with pytest.raises(ValueError, match="StateBackend must be provided"):
            StateManager(backend=None)

    def test_save_pipeline_run_dict(self):
        backend = MagicMock()
        backend.load_state.return_value = {"pipelines": {}}
        mgr = StateManager(backend=backend)
        mgr.save_pipeline_run("p", {"end_time": "t"})
        backend.save_pipeline_run.assert_called_once()

    def test_save_pipeline_run_with_results_object(self):
        backend = MagicMock()
        backend.load_state.return_value = {"pipelines": {}}
        mgr = StateManager(backend=backend)

        results = MagicMock()
        results.to_dict.return_value = {"end_time": "t"}
        node_res = MagicMock()
        node_res.success = True
        node_res.metadata = {"timestamp": "ts"}
        results.node_results = {"n1": node_res}

        mgr.save_pipeline_run("p", results)
        backend.save_pipeline_run.assert_called_once()
        call_args = backend.save_pipeline_run.call_args[0]
        assert call_args[0] == "p"
        assert "n1" in call_args[1]["nodes"]

    def test_get_last_run_info(self):
        backend = MagicMock()
        backend.load_state.return_value = {}
        backend.get_last_run_info.return_value = {"success": True}
        mgr = StateManager(backend=backend)
        assert mgr.get_last_run_info("p", "n") == {"success": True}

    def test_get_last_run_status(self):
        backend = MagicMock()
        backend.load_state.return_value = {}
        backend.get_last_run_status.return_value = True
        mgr = StateManager(backend=backend)
        assert mgr.get_last_run_status("p", "n") is True

    def test_get_set_hwm(self):
        backend = MagicMock()
        backend.load_state.return_value = {}
        mgr = StateManager(backend=backend)
        mgr.set_hwm("k", "v")
        backend.set_hwm.assert_called_once_with("k", "v")
        mgr.get_hwm("k")
        backend.get_hwm.assert_called_once_with("k")

    def test_set_hwm_batch(self):
        backend = MagicMock()
        backend.load_state.return_value = {}
        mgr = StateManager(backend=backend)
        updates = [{"key": "k1", "value": "v1"}]
        mgr.set_hwm_batch(updates)
        backend.set_hwm_batch.assert_called_once_with(updates)


# ===========================================================================
# create_state_backend factory
# ===========================================================================


class TestCreateStateBackend:
    def test_no_system_config(self, tmp_path):
        config = MagicMock()
        config.system = None
        backend = create_state_backend(config, project_root=str(tmp_path))
        assert isinstance(backend, LocalJSONStateBackend)

    def test_connection_not_found(self):
        config = MagicMock()
        config.system = MagicMock()
        config.system.connection = "missing"
        config.connections = {}
        with pytest.raises(ValueError, match="not found"):
            create_state_backend(config)

    def test_local_type(self, tmp_path):
        config = MagicMock()
        config.system = MagicMock()
        config.system.connection = "local_conn"
        config.system.path = "test_system"
        config.system.environment = "dev"
        config.connections = {
            "local_conn": {"type": "local", "base_path": str(tmp_path)},
        }
        backend = create_state_backend(config, project_root=str(tmp_path))
        assert isinstance(backend, CatalogStateBackend)
        assert "test_system" in backend.meta_state_path

    def test_azure_blob_account_key(self):
        config = MagicMock()
        config.system = MagicMock()
        config.system.connection = "blob"
        config.system.path = "sys"
        config.system.environment = None
        config.connections = {
            "blob": {
                "type": "azure_blob",
                "account_name": "acct",
                "container": "cont",
                "auth": {"mode": "account_key", "account_key": "key123"},
            },
        }
        backend = create_state_backend(config)
        assert isinstance(backend, CatalogStateBackend)
        assert "abfss://" in backend.meta_runs_path
        assert backend.storage_options["account_key"] == "key123"

    def test_azure_blob_sas(self):
        config = MagicMock()
        config.system = MagicMock()
        config.system.connection = "blob"
        config.system.path = "sys"
        config.system.environment = None
        config.connections = {
            "blob": {
                "type": "azure_blob",
                "account_name": "acct",
                "container": "cont",
                "auth": {"mode": "sas", "sas_token": "tok"},
            },
        }
        backend = create_state_backend(config)
        assert backend.storage_options["sas_token"] == "tok"

    def test_sql_server_type(self):
        mock_conn = MagicMock()
        mock_create = MagicMock(return_value=mock_conn)
        config = MagicMock()
        config.system = MagicMock()
        config.system.connection = "sql"
        config.system.schema_name = "my_schema"
        config.system.environment = "prod"
        config.connections = {"sql": {"type": "sql_server"}}
        import odibi.connections.factory as factory_mod

        factory_mod.create_connection = mock_create
        try:
            backend = create_state_backend(config)
            assert isinstance(backend, SqlServerSystemBackend)
            assert backend.schema_name == "my_schema"
        finally:
            if hasattr(factory_mod, "create_connection"):
                del factory_mod.create_connection

    def test_unsupported_type_fallback(self, tmp_path):
        config = MagicMock()
        config.system = MagicMock()
        config.system.connection = "other"
        config.system.path = "sys"
        config.system.environment = None
        config.connections = {"other": {"type": "delta"}}
        backend = create_state_backend(config, project_root=str(tmp_path))
        assert isinstance(backend, CatalogStateBackend)


# ===========================================================================
# create_sync_source_backend factory
# ===========================================================================


class TestCreateSyncSourceBackend:
    def test_connection_not_found(self):
        sync_cfg = MagicMock()
        sync_cfg.connection = "missing"
        with pytest.raises(ValueError, match="not found"):
            create_sync_source_backend(sync_cfg, {})

    def test_sql_server(self):
        mock_conn = MagicMock()
        mock_create = MagicMock(return_value=mock_conn)
        sync_cfg = MagicMock()
        sync_cfg.connection = "sql"
        sync_cfg.schema_name = "sch"
        connections = {"sql": {"type": "sql_server"}}
        import odibi.connections.factory as factory_mod

        factory_mod.create_connection = mock_create
        try:
            backend = create_sync_source_backend(sync_cfg, connections)
            assert isinstance(backend, SqlServerSystemBackend)
        finally:
            if hasattr(factory_mod, "create_connection"):
                del factory_mod.create_connection

    def test_local_type(self, tmp_path):
        sync_cfg = {"connection": "local", "path": "sys"}
        connections = {"local": {"type": "local", "base_path": str(tmp_path)}}
        backend = create_sync_source_backend(sync_cfg, connections, str(tmp_path))
        assert isinstance(backend, CatalogStateBackend)

    def test_azure_blob_account_key(self):
        sync_cfg = {"connection": "blob", "path": "sys"}
        connections = {
            "blob": {
                "type": "azure_blob",
                "account_name": "a",
                "container": "c",
                "auth": {"mode": "account_key", "account_key": "k"},
            },
        }
        backend = create_sync_source_backend(sync_cfg, connections)
        assert isinstance(backend, CatalogStateBackend)
        assert backend.storage_options["account_key"] == "k"

    def test_azure_blob_sas(self):
        sync_cfg = {"connection": "blob", "path": "sys"}
        connections = {
            "blob": {
                "type": "azure_blob",
                "account_name": "a",
                "container": "c",
                "auth": {"mode": "sas", "sas_token": "t"},
            },
        }
        backend = create_sync_source_backend(sync_cfg, connections)
        assert backend.storage_options["sas_token"] == "t"

    def test_fallback_no_base_uri(self, tmp_path):
        sync_cfg = {"connection": "x", "path": "sys"}
        connections = {"x": {"type": "unknown"}}
        backend = create_sync_source_backend(sync_cfg, connections, str(tmp_path))
        assert isinstance(backend, CatalogStateBackend)


# ===========================================================================
# sync_system_data, _sync_runs, _sync_state
# ===========================================================================


class TestSyncSystemData:
    def test_default_syncs_both(self):
        source = MagicMock(spec=LocalJSONStateBackend)
        target = MagicMock(spec=LocalJSONStateBackend)
        with (
            patch("odibi.state._sync_runs", return_value=5) as mock_runs,
            patch("odibi.state._sync_state", return_value=3) as mock_state,
        ):
            result = sync_system_data(source, target)
        assert result == {"runs": 5, "state": 3}
        mock_runs.assert_called_once()
        mock_state.assert_called_once()

    def test_sync_only_runs(self):
        source = MagicMock()
        target = MagicMock()
        with (
            patch("odibi.state._sync_runs", return_value=2),
            patch("odibi.state._sync_state") as mock_state,
        ):
            result = sync_system_data(source, target, tables=["runs"])
        assert result["runs"] == 2
        assert result["state"] == 0
        mock_state.assert_not_called()

    def test_sync_only_state(self):
        source = MagicMock()
        target = MagicMock()
        with (
            patch("odibi.state._sync_runs") as mock_runs,
            patch("odibi.state._sync_state", return_value=4),
        ):
            result = sync_system_data(source, target, tables=["state"])
        assert result["state"] == 4
        assert result["runs"] == 0
        mock_runs.assert_not_called()


class TestSyncRuns:
    def test_catalog_source_with_data(self, tmp_path):
        runs_path = str(tmp_path / "runs")
        df = pd.DataFrame(
            {
                "run_id": ["r1"],
                "pipeline_name": ["p"],
                "node_name": ["n"],
                "status": ["SUCCESS"],
                "rows_processed": [100],
                "duration_ms": [500],
                "metrics_json": ["{}"],
            }
        )
        write_deltalake(runs_path, df)

        source = CatalogStateBackend(meta_runs_path=runs_path, meta_state_path="/s")
        target = MagicMock(spec=SqlServerSystemBackend)
        target.log_runs_batch = MagicMock()

        count = _sync_runs(source, target)
        assert count == 1
        target.log_runs_batch.assert_called_once()

    def test_catalog_source_empty(self, tmp_path):
        runs_path = str(tmp_path / "runs")
        df = pd.DataFrame(
            {
                "run_id": [],
                "pipeline_name": [],
                "node_name": [],
                "status": [],
                "rows_processed": [],
                "duration_ms": [],
                "metrics_json": [],
            }
        )
        write_deltalake(runs_path, df)

        source = CatalogStateBackend(meta_runs_path=runs_path, meta_state_path="/s")
        target = MagicMock()
        count = _sync_runs(source, target)
        assert count == 0

    def test_sql_source(self):
        source = MagicMock(spec=SqlServerSystemBackend)
        source._ensure_tables = MagicMock()
        source.schema_name = "sch"
        source.connection = MagicMock()
        source.connection.execute.return_value = [
            ("r1", "p", "n", "SUCCESS", 10, 100, "{}"),
        ]
        target = MagicMock(spec=SqlServerSystemBackend)
        target.log_runs_batch = MagicMock()

        count = _sync_runs(source, target)
        assert count == 1

    def test_sql_source_error(self):
        source = MagicMock(spec=SqlServerSystemBackend)
        source._ensure_tables = MagicMock()
        source.schema_name = "sch"
        source.connection = MagicMock()
        source.connection.execute.side_effect = Exception("err")

        target = MagicMock()
        count = _sync_runs(source, target)
        assert count == 0

    def test_no_records(self):
        source = MagicMock(spec=SqlServerSystemBackend)
        source._ensure_tables = MagicMock()
        source.schema_name = "sch"
        source.connection = MagicMock()
        source.connection.execute.return_value = None

        target = MagicMock()
        count = _sync_runs(source, target)
        assert count == 0


class TestSyncState:
    def test_catalog_source_with_data(self, tmp_path):
        state_path = str(tmp_path / "state")
        df = pd.DataFrame(
            {
                "key": ["k1", "k2"],
                "value": [json.dumps("v1"), "plain"],
            }
        )
        write_deltalake(state_path, df)

        source = CatalogStateBackend(meta_runs_path="/r", meta_state_path=state_path)
        target = MagicMock()
        target.set_hwm_batch = MagicMock()

        count = _sync_state(source, target)
        assert count == 2
        target.set_hwm_batch.assert_called_once()

    def test_catalog_source_empty(self, tmp_path):
        state_path = str(tmp_path / "state")
        df = pd.DataFrame({"key": [], "value": []})
        write_deltalake(state_path, df)

        source = CatalogStateBackend("/r", state_path)
        target = MagicMock()
        count = _sync_state(source, target)
        assert count == 0

    def test_sql_source(self):
        source = MagicMock(spec=SqlServerSystemBackend)
        source._ensure_tables = MagicMock()
        source.schema_name = "sch"
        source.connection = MagicMock()
        source.connection.execute.return_value = [("k1", json.dumps("v1"))]

        target = MagicMock()
        target.set_hwm_batch = MagicMock()

        count = _sync_state(source, target)
        assert count == 1

    def test_sql_source_error(self):
        source = MagicMock(spec=SqlServerSystemBackend)
        source._ensure_tables = MagicMock()
        source.schema_name = "sch"
        source.connection = MagicMock()
        source.connection.execute.side_effect = Exception("err")

        target = MagicMock()
        count = _sync_state(source, target)
        assert count == 0


class TestWriteRunsToCatalog:
    def test_writes_records(self, tmp_path):
        runs_path = str(tmp_path / "runs")
        target = CatalogStateBackend(
            meta_runs_path=runs_path, meta_state_path="/s", environment="test"
        )
        records = [
            {
                "run_id": "r1",
                "pipeline_name": "p",
                "node_name": "n",
                "status": "SUCCESS",
                "rows_processed": 10,
                "duration_ms": 100,
                "metrics_json": "{}",
            },
        ]
        _write_runs_to_catalog(target, records)
        dt = DeltaTable(runs_path)
        result = dt.to_pandas()
        assert len(result) == 1
        assert result.iloc[0]["run_id"] == "r1"
