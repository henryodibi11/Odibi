"""Tests for odibi.state — StateBackend, LocalJSON, Catalog, SqlServer, StateManager, factories."""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from types import ModuleType
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError, TableNotFoundError

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


class _SparkConditionError(Exception):
    def __init__(self, condition, message=None):
        super().__init__(message or f"[{condition}] Spark operation failed")
        self.condition = condition

    def getErrorClass(self):
        return self.condition


@pytest.fixture
def mocked_pyspark_modules():
    """Install minimal PySpark modules so state tests exercise Odibi, not a live JVM."""

    class _SparkType:
        def __init__(self, *args):
            self.args = args

    functions_module = ModuleType("pyspark.sql.functions")
    functions_module.col = MagicMock(return_value=MagicMock())

    types_module = ModuleType("pyspark.sql.types")
    types_module.StringType = _SparkType
    types_module.StructField = _SparkType
    types_module.StructType = _SparkType
    types_module.TimestampType = _SparkType

    sql_module = ModuleType("pyspark.sql")
    sql_module.functions = functions_module
    sql_module.types = types_module

    pyspark_module = ModuleType("pyspark")
    pyspark_module.sql = sql_module

    with patch.dict(
        sys.modules,
        {
            "pyspark": pyspark_module,
            "pyspark.sql": sql_module,
            "pyspark.sql.functions": functions_module,
            "pyspark.sql.types": types_module,
        },
    ):
        yield functions_module


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
        backend = CatalogStateBackend("/runs", "/state", spark_session=spark)
        expected = {"success": True, "metadata": {"x": 1}}

        with patch.object(backend, "_get_last_run_spark", return_value=expected) as getter:
            result = backend.get_last_run_info("p", "n")

        assert result == expected
        getter.assert_called_once_with("p", "n")


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

    @pytest.mark.parametrize("failure_point", ["open", "dataset", "scan"])
    def test_get_hwm_local_operational_failure_is_visible(self, failure_point):
        backend = CatalogStateBackend("/r", "/state")
        error = RuntimeError(f"{failure_point} failed")
        delta_table = MagicMock()
        dataset = MagicMock()
        delta_table.to_pyarrow_dataset.return_value = dataset

        if failure_point == "open":
            delta_table_factory = MagicMock(side_effect=error)
        else:
            delta_table_factory = MagicMock(return_value=delta_table)
            if failure_point == "dataset":
                delta_table.to_pyarrow_dataset.side_effect = error
            else:
                dataset.to_table.side_effect = error

        with (
            patch("odibi.state.DeltaTable", delta_table_factory),
            pytest.raises(RuntimeError, match=f"{failure_point} failed"),
        ):
            backend.get_hwm("k")

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
        backend = CatalogStateBackend("/r", "/s", spark_session=spark)

        with patch.object(backend, "_get_hwm_spark", return_value={"x": 2}) as getter:
            result = backend.get_hwm("k")

        assert result == {"x": 2}
        getter.assert_called_once_with("k")

    def test_get_hwm_spark_json_value(self, mocked_pyspark_modules):
        spark = MagicMock()
        row = MagicMock()
        row.value = '{"x": 2}'
        spark.read.format.return_value.load.return_value.filter.return_value.select.return_value.first.return_value = row

        backend = CatalogStateBackend("/r", "/s", spark_session=spark)
        assert backend.get_hwm("k") == {"x": 2}

    @pytest.mark.parametrize(
        "error",
        [
            _SparkConditionError("PATH_NOT_FOUND"),
            RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] missing state table"),
        ],
    )
    def test_get_hwm_spark_true_missing_state(self, error, mocked_pyspark_modules):
        spark = MagicMock()
        spark.read.format.return_value.load.side_effect = error

        backend = CatalogStateBackend("/r", "/s", spark_session=spark)
        assert backend.get_hwm("k") is None

    def test_get_hwm_spark_absent_key(self, mocked_pyspark_modules):
        spark = MagicMock()
        spark.read.format.return_value.load.return_value.filter.return_value.select.return_value.first.return_value = None

        backend = CatalogStateBackend("/r", "/s", spark_session=spark)
        assert backend.get_hwm("missing") is None

    @pytest.mark.parametrize(
        "failure_point", ["format", "load", "expression", "filter", "select", "first"]
    )
    def test_get_hwm_spark_operational_failure_is_visible(
        self, failure_point, mocked_pyspark_modules
    ):
        spark = MagicMock()
        error = RuntimeError(f"{failure_point} credential does not exist")
        loaded = spark.read.format.return_value.load.return_value
        filtered = loaded.filter.return_value
        selected = filtered.select.return_value

        if failure_point == "format":
            spark.read.format.side_effect = error
        elif failure_point == "load":
            spark.read.format.return_value.load.side_effect = error
        elif failure_point == "expression":
            mocked_pyspark_modules.col.side_effect = error
        elif failure_point == "filter":
            loaded.filter.side_effect = error
        elif failure_point == "select":
            filtered.select.side_effect = error
        else:
            selected.first.side_effect = error

        backend = CatalogStateBackend("/r", "/s", spark_session=spark)
        with pytest.raises(RuntimeError, match=failure_point) as raised:
            backend.get_hwm("k")

        assert raised.value is error

    def test_set_hwm_local_new_table(self, tmp_path):
        state_path = str(tmp_path / "state")
        with pytest.raises(TableNotFoundError):
            DeltaTable(state_path)

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

    def test_set_hwm_local_open_failure_is_visible_and_preserves_table(self, tmp_path):
        state_path = str(tmp_path / "state")
        table = pa.table(
            {
                "key": pa.array(["unrelated"], type=pa.string()),
                "value": pa.array([json.dumps("keep")], type=pa.string()),
                "environment": pa.array(["test"], type=pa.string()),
                "updated_at": pa.array(
                    [datetime(2026, 1, 1, tzinfo=timezone.utc)],
                    type=pa.timestamp("us", tz="UTC"),
                ),
            }
        )
        write_deltalake(state_path, table)
        before = DeltaTable(state_path).to_pandas()
        backend = CatalogStateBackend("/r", state_path, environment="test")

        with (
            patch("odibi.state.DeltaTable", side_effect=RuntimeError("open failed")),
            patch("odibi.state.write_deltalake") as fallback_write,
            pytest.raises(RuntimeError, match="open failed"),
        ):
            backend.set_hwm("target", "new")

        fallback_write.assert_not_called()
        after = DeltaTable(state_path).to_pandas()
        pd.testing.assert_frame_equal(after, before)

    def test_set_hwm_local_retries_typed_first_create_conflict(self, tmp_path):
        state_path = str(tmp_path / "state")
        backend = CatalogStateBackend("/r", state_path, environment="test")
        competing_row = pd.DataFrame(
            {
                "key": ["competing"],
                "value": [json.dumps("survives")],
                "environment": ["test"],
                "updated_at": [datetime.now(timezone.utc)],
            }
        )
        real_write = write_deltalake
        create_attempts = 0

        def lose_first_create(path, _df, **kwargs):
            nonlocal create_attempts
            create_attempts += 1
            real_write(
                path,
                competing_row,
                mode="append",
                storage_options=kwargs.get("storage_options"),
            )
            raise CommitFailedError("version 0 already exists")

        with (
            patch("odibi.state.write_deltalake", side_effect=lose_first_create),
            patch("odibi.state.time.sleep") as retry_sleep,
        ):
            backend.set_hwm("requested", "committed-after-retry")

        result = DeltaTable(state_path).to_pandas()
        values = {row["key"]: json.loads(row["value"]) for _, row in result.iterrows()}
        assert values == {
            "competing": "survives",
            "requested": "committed-after-retry",
        }
        assert create_attempts == 1
        retry_sleep.assert_called_once()

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
        values = {row["key"]: json.loads(row["value"]) for _, row in result.iterrows()}
        assert values == {"k1": "v1", "k2": "v2"}

    def test_set_hwm_batch_merge_failure_preserves_all_rows_and_is_retryable(self, tmp_path):
        state_path = str(tmp_path / "state")
        table = pa.table(
            {
                "key": pa.array(["target", "unrelated"], type=pa.string()),
                "value": pa.array([json.dumps("old"), json.dumps("keep")], type=pa.string()),
                "environment": pa.array(["test", "test"], type=pa.string()),
                "updated_at": pa.array(
                    [
                        datetime(2026, 1, 1, tzinfo=timezone.utc),
                        datetime(2026, 1, 2, tzinfo=timezone.utc),
                    ],
                    type=pa.timestamp("us", tz="UTC"),
                ),
            }
        )
        write_deltalake(state_path, table)
        before = DeltaTable(state_path).to_pandas().sort_values("key").reset_index(drop=True)
        backend = CatalogStateBackend("/r", state_path, environment="test")
        updates = [
            {"key": "target", "value": "new"},
            {"key": "inserted", "value": "added"},
        ]
        failing_table = MagicMock()
        failing_table.merge.side_effect = RuntimeError("merge failed")

        with (
            patch("odibi.state.DeltaTable", return_value=failing_table),
            patch("odibi.state.write_deltalake") as fallback_write,
            pytest.raises(RuntimeError, match="merge failed"),
        ):
            backend.set_hwm_batch(updates)

        fallback_write.assert_not_called()
        unchanged = DeltaTable(state_path).to_pandas().sort_values("key").reset_index(drop=True)
        pd.testing.assert_frame_equal(unchanged, before)

        backend.set_hwm_batch(updates)

        result = DeltaTable(state_path).to_pandas()
        values = {row["key"]: json.loads(row["value"]) for _, row in result.iterrows()}
        assert values == {"target": "new", "unrelated": "keep", "inserted": "added"}


class TestCatalogStateSpark:
    def test_spark_table_exists_true(self):
        spark = MagicMock()
        spark.read.format.return_value.load.return_value.count.return_value = 5
        backend = CatalogStateBackend("/r", "/s", spark_session=spark)
        assert backend._spark_table_exists("/path") is True

    def test_spark_table_exists_false_for_structured_missing(self):
        spark = MagicMock()
        spark.read.format.return_value.load.side_effect = _SparkConditionError("PATH_NOT_FOUND")
        backend = CatalogStateBackend("/r", "/s", spark_session=spark)
        assert backend._spark_table_exists("/path") is False

    @pytest.mark.parametrize("failure_point", ["load", "count"])
    def test_spark_table_exists_operational_failure_is_visible(self, failure_point):
        spark = MagicMock()
        error = RuntimeError(f"{failure_point} failed")
        if failure_point == "load":
            spark.read.format.return_value.load.side_effect = error
        else:
            spark.read.format.return_value.load.return_value.count.side_effect = error
        backend = CatalogStateBackend("/r", "/s", spark_session=spark)

        with pytest.raises(RuntimeError, match=f"{failure_point} failed") as raised:
            backend._spark_table_exists("/path")

        assert raised.value is error

    @pytest.mark.parametrize("batch", [False, True])
    def test_spark_setter_probe_failure_performs_zero_writes(self, batch, mocked_pyspark_modules):
        spark = MagicMock()
        updates_df = MagicMock()
        spark.createDataFrame.return_value = updates_df
        error = RuntimeError("authorization failed")
        spark.read.format.return_value.load.side_effect = error
        backend = CatalogStateBackend("/r", "/s", spark_session=spark)

        with pytest.raises(RuntimeError, match="authorization failed") as raised:
            if batch:
                backend.set_hwm_batch([{"key": "k", "value": "v"}])
            else:
                backend.set_hwm("k", "v")

        assert raised.value is error
        updates_df.write.format.assert_not_called()
        updates_df.createOrReplaceTempView.assert_not_called()
        spark.sql.assert_not_called()

    @pytest.mark.parametrize("batch", [False, True])
    def test_spark_missing_state_creation_is_error_if_exists(self, batch, mocked_pyspark_modules):
        spark = MagicMock()
        updates_df = MagicMock()
        spark.createDataFrame.return_value = updates_df
        spark.read.format.return_value.load.side_effect = _SparkConditionError("PATH_NOT_FOUND")
        backend = CatalogStateBackend("/r", "/s", spark_session=spark)

        if batch:
            backend.set_hwm_batch([{"key": "k", "value": "v"}])
        else:
            backend.set_hwm("k", "v")

        writer = updates_df.write.format.return_value
        updates_df.write.format.assert_called_once_with("delta")
        writer.mode.assert_called_once_with("errorifexists")
        writer.mode.return_value.save.assert_called_once_with("/s")
        spark.sql.assert_not_called()

    def test_spark_first_create_operational_write_failure_is_not_retried(
        self, mocked_pyspark_modules
    ):
        spark = MagicMock()
        updates_df = MagicMock()
        spark.createDataFrame.return_value = updates_df
        write_error = RuntimeError("write authorization failed")
        updates_df.write.format.return_value.mode.return_value.save.side_effect = write_error
        backend = CatalogStateBackend("/r", "/s", spark_session=spark)

        with (
            patch.object(backend, "_spark_table_exists", return_value=False) as exists,
            patch("odibi.state.time.sleep") as retry_sleep,
            pytest.raises(RuntimeError, match="write authorization failed") as raised,
        ):
            backend.set_hwm("k", "v")

        assert raised.value is write_error
        exists.assert_called_once_with("/s")
        retry_sleep.assert_not_called()
        updates_df.write.format.return_value.mode.return_value.save.assert_called_once_with("/s")
        spark.sql.assert_not_called()

    def test_spark_first_create_race_reprobes_and_merges(self, mocked_pyspark_modules):
        spark = MagicMock()
        updates_df = MagicMock()
        spark.createDataFrame.return_value = updates_df
        updates_df.write.format.return_value.mode.return_value.save.side_effect = (
            _SparkConditionError("DELTA_PATH_EXISTS")
        )
        backend = CatalogStateBackend("/r", "/s", spark_session=spark)

        with (
            patch.object(backend, "_spark_table_exists", side_effect=[False, True]) as exists,
            patch("odibi.state.time.sleep") as retry_sleep,
        ):
            backend.set_hwm("k", "v")

        assert exists.call_count == 2
        retry_sleep.assert_not_called()
        updates_df.write.format.return_value.mode.assert_called_once_with("errorifexists")
        updates_df.createOrReplaceTempView.assert_called_once()
        view_name = updates_df.createOrReplaceTempView.call_args.args[0]
        assert view_name.startswith("_odibi_hwm_updates_")
        spark.sql.assert_called_once()
        spark.catalog.dropTempView.assert_called_once_with(view_name)

    def test_spark_first_create_race_without_table_fails_closed(self, mocked_pyspark_modules):
        spark = MagicMock()
        updates_df = MagicMock()
        spark.createDataFrame.return_value = updates_df
        create_error = _SparkConditionError("DELTA_PATH_EXISTS")
        updates_df.write.format.return_value.mode.return_value.save.side_effect = create_error
        backend = CatalogStateBackend("/r", "/s", spark_session=spark)

        with (
            patch.object(backend, "_spark_table_exists", side_effect=[False, False]) as exists,
            pytest.raises(_SparkConditionError) as raised,
        ):
            backend.set_hwm("k", "v")

        assert raised.value is create_error
        assert exists.call_count == 2
        updates_df.createOrReplaceTempView.assert_not_called()
        spark.sql.assert_not_called()

    def test_spark_merge_failure_remains_authoritative_when_cleanup_fails(
        self, mocked_pyspark_modules
    ):
        spark = MagicMock()
        updates_df = MagicMock()
        spark.createDataFrame.return_value = updates_df
        merge_error = RuntimeError("merge failed")
        spark.sql.side_effect = merge_error
        spark.catalog.dropTempView.side_effect = RuntimeError("cleanup failed")
        backend = CatalogStateBackend("/r", "/s", spark_session=spark)

        with (
            patch.object(backend, "_spark_table_exists", return_value=True),
            pytest.raises(RuntimeError, match="merge failed") as raised,
        ):
            backend.set_hwm("k", "v")

        assert raised.value is merge_error
        updates_df.createOrReplaceTempView.assert_called_once()
        view_name = updates_df.createOrReplaceTempView.call_args.args[0]
        spark.catalog.dropTempView.assert_called_once_with(view_name)


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
        with pytest.raises(Exception, match="err"):
            backend.get_hwm("k")

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
        with pytest.raises(Exception, match="err"):
            backend.set_hwm("k", "v")

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
