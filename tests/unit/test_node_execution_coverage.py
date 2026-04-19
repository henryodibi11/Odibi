"""Tests targeting specific uncovered execution paths in odibi/node.py (non-Spark)."""

import json
from unittest.mock import MagicMock

import pandas as pd
import pytest

from odibi.config import IncrementalConfig, IncrementalMode, NodeConfig
from odibi.exceptions import ExecutionContext, NodeExecutionError
from odibi.node import NodeExecutor


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_executor(**overrides):
    """Create a NodeExecutor with mock dependencies for non-Spark testing."""
    engine = MagicMock()
    engine.name = "pandas"
    engine.count_rows.return_value = 5
    engine.get_sample.return_value = [{"a": 1}]

    ctx = MagicMock()
    connections = overrides.pop("connections", {"src": MagicMock(), "dst": MagicMock()})
    state_manager = overrides.pop("state_manager", MagicMock())

    executor = NodeExecutor(
        context=ctx,
        engine=engine,
        connections=connections,
        catalog_manager=overrides.pop("catalog_manager", None),
        config_file=overrides.pop("config_file", "test.yaml"),
        state_manager=state_manager,
        **overrides,
    )
    return executor


def _simple_config(**overrides):
    """Create a minimal NodeConfig for testing."""
    defaults = {
        "name": "test_node",
        "read": {"connection": "src", "format": "csv", "path": "data.csv"},
    }
    defaults.update(overrides)
    return NodeConfig(**defaults)


# ===========================================================================
# Target 1: Error handling in execute (lines 393-460)
# ===========================================================================


class TestExecuteErrorHandling:
    """Tests for exception handling block in execute()."""

    def test_execute_returns_failure_on_read_error(self):
        executor = _make_executor()
        config = _simple_config()
        executor.engine.read.side_effect = RuntimeError("connection timeout")

        result = executor.execute(config)

        assert not result.success
        assert result.error is not None
        assert isinstance(result.error, NodeExecutionError)
        assert "connection timeout" in result.error.message

    def test_error_metadata_contains_steps_and_traceback(self):
        executor = _make_executor()
        config = _simple_config()
        executor.engine.read.side_effect = ValueError("bad value")

        result = executor.execute(config)

        assert "steps" in result.metadata
        assert "error_traceback" in result.metadata
        assert "error_traceback_cleaned" in result.metadata
        assert isinstance(result.metadata["error_traceback"], str)

    def test_error_metadata_contains_config_snapshot(self):
        executor = _make_executor()
        config = _simple_config()
        executor.engine.read.side_effect = ValueError("oops")

        result = executor.execute(config)

        assert "config_snapshot" in result.metadata
        assert "connection_health" in result.metadata

    def test_error_metadata_contains_phase_timings(self):
        executor = _make_executor()
        config = _simple_config()
        executor.engine.read.side_effect = ValueError("oops")

        result = executor.execute(config)

        assert "phase_timings_ms" in result.metadata
        assert isinstance(result.metadata["phase_timings_ms"], dict)

    def test_error_wraps_non_node_execution_error(self):
        executor = _make_executor()
        config = _simple_config()
        executor.engine.read.side_effect = TypeError("wrong type")

        result = executor.execute(config)

        assert isinstance(result.error, NodeExecutionError)
        assert result.error.original_error is not None

    def test_error_preserves_node_execution_error(self):
        executor = _make_executor()
        config = _simple_config()
        nee = NodeExecutionError(
            message="already wrapped",
            context=ExecutionContext(node_name="test_node"),
            original_error=RuntimeError("inner"),
        )
        executor.engine.read.side_effect = nee

        result = executor.execute(config)

        assert result.error is nee

    def test_suggestions_logged_when_not_suppressed(self):
        executor = _make_executor()
        config = _simple_config()
        executor.engine.read.side_effect = RuntimeError("fail")

        result = executor.execute(config, suppress_error_log=False)

        assert not result.success
        assert result.duration >= 0

    def test_error_logging_suppressed_during_retries(self):
        executor = _make_executor()
        config = _simple_config()
        executor.engine.read.side_effect = RuntimeError("fail")

        result = executor.execute(config, suppress_error_log=True)

        assert not result.success

    def test_persisted_dfs_cleaned_up_on_error(self):
        executor = _make_executor()
        config = _simple_config()
        mock_df = MagicMock()
        mock_df.unpersist = MagicMock()

        # Inject a persisted df before the error

        def side_effect_read(**kwargs):
            executor._persisted_dfs.append(mock_df)
            raise RuntimeError("boom")

        executor.engine.read.side_effect = side_effect_read

        result = executor.execute(config)

        assert not result.success
        mock_df.unpersist.assert_called_once()
        assert len(executor._persisted_dfs) == 0

    def test_error_result_has_correct_node_name(self):
        executor = _make_executor()
        config = _simple_config(name="my_node")
        executor.engine.read.side_effect = RuntimeError("fail")

        result = executor.execute(config)

        assert result.node_name == "my_node"

    def test_error_metadata_rows_read_is_none_when_read_fails(self):
        executor = _make_executor()
        config = _simple_config()
        executor.engine.read.side_effect = RuntimeError("fail")

        result = executor.execute(config)

        assert result.metadata["rows_read"] is None

    def test_error_metadata_sample_data_none_when_no_sample(self):
        executor = _make_executor()
        config = _simple_config()
        executor.engine.read.side_effect = RuntimeError("fail")

        result = executor.execute(config)

        assert result.metadata["sample_data_in"] is None


# ===========================================================================
# Target 2: first_run_query (lines 559-567)
# ===========================================================================


class TestFirstRunQuery:
    """Tests for first_run_query logic in _execute_read_phase."""

    def test_first_run_query_used_when_target_missing(self):
        executor = _make_executor()
        executor.engine.table_exists.return_value = False
        executor.engine.read.return_value = pd.DataFrame({"a": [1]})

        config = _simple_config(
            write={
                "connection": "dst",
                "format": "csv",
                "path": "out.csv",
                "first_run_query": "SELECT 1 AS bootstrap",
            },
        )

        executor._execute_read_phase(config, hwm_state=None)

        call_kwargs = executor.engine.read.call_args
        assert call_kwargs.kwargs["options"]["query"] == "SELECT 1 AS bootstrap"

    def test_first_run_query_not_used_when_target_exists(self):
        executor = _make_executor()
        executor.engine.table_exists.return_value = True
        executor.engine.read.return_value = pd.DataFrame({"a": [1]})

        config = _simple_config(
            write={
                "connection": "dst",
                "format": "csv",
                "path": "out.csv",
                "first_run_query": "SELECT 1 AS bootstrap",
            },
        )

        executor._execute_read_phase(config, hwm_state=None)

        call_kwargs = executor.engine.read.call_args
        assert call_kwargs.kwargs["options"].get("query") is None

    def test_first_run_query_skipped_when_no_write_config(self):
        executor = _make_executor()
        executor.engine.read.return_value = pd.DataFrame({"a": [1]})

        config = _simple_config()  # no write

        executor._execute_read_phase(config, hwm_state=None)

        call_kwargs = executor.engine.read.call_args
        assert call_kwargs.kwargs["options"].get("query") is None


# ===========================================================================
# Target 3: archive_options (lines 570-578)
# ===========================================================================


class TestArchiveOptions:
    """Tests for archive_options merging into read_options."""

    def test_archive_options_merged_into_read_options(self):
        executor = _make_executor()
        executor.engine.read.return_value = pd.DataFrame({"a": [1]})

        config = _simple_config(
            read={
                "connection": "src",
                "format": "csv",
                "path": "data.csv",
                "archive_options": {"badRecordsPath": "/tmp/bad"},
            },
        )

        executor._execute_read_phase(config, hwm_state=None)

        call_kwargs = executor.engine.read.call_args
        assert call_kwargs.kwargs["options"]["badRecordsPath"] == "/tmp/bad"

    def test_archive_options_step_recorded(self):
        executor = _make_executor()
        executor.engine.read.return_value = pd.DataFrame({"a": [1]})

        config = _simple_config(
            read={
                "connection": "src",
                "format": "csv",
                "path": "data.csv",
                "archive_options": {"opt1": "val1", "opt2": "val2"},
            },
        )

        executor._execute_read_phase(config, hwm_state=None)

        assert any("archive_options" in s for s in executor._execution_steps)


# ===========================================================================
# Target 4: incremental SQL pushdown (lines 581-596)
# ===========================================================================


class TestIncrementalSqlPushdown:
    """Tests for incremental SQL filter generation in read phase."""

    def test_incremental_filter_added_to_options(self):
        executor = _make_executor()
        executor.engine.table_exists.return_value = True
        executor.engine.read.return_value = pd.DataFrame({"a": [1]})

        config = _simple_config(
            read={
                "connection": "src",
                "format": "sql_server",
                "table": "dbo.events",
                "incremental": {
                    "mode": "rolling_window",
                    "column": "updated_at",
                    "lookback": 3,
                    "unit": "day",
                },
            },
            write={"connection": "dst", "format": "csv", "path": "out.csv"},
        )

        executor._execute_read_phase(config, hwm_state=None)

        call_kwargs = executor.engine.read.call_args
        assert "filter" in call_kwargs.kwargs["options"]
        assert "updated_at" in call_kwargs.kwargs["options"]["filter"]

    def test_incremental_filter_combined_with_existing(self):
        executor = _make_executor()
        executor.engine.table_exists.return_value = True
        executor.engine.read.return_value = pd.DataFrame({"a": [1]})

        config = _simple_config(
            read={
                "connection": "src",
                "format": "sql_server",
                "table": "dbo.events",
                "options": {"filter": "status = 'active'"},
                "incremental": {
                    "mode": "rolling_window",
                    "column": "updated_at",
                    "lookback": 1,
                    "unit": "day",
                },
            },
            write={"connection": "dst", "format": "csv", "path": "out.csv"},
        )

        executor._execute_read_phase(config, hwm_state=None)

        call_kwargs = executor.engine.read.call_args
        filt = call_kwargs.kwargs["options"]["filter"]
        assert "status = 'active'" in filt
        assert "AND" in filt

    def test_incremental_step_recorded(self):
        executor = _make_executor()
        executor.engine.table_exists.return_value = True
        executor.engine.read.return_value = pd.DataFrame({"a": [1]})

        config = _simple_config(
            read={
                "connection": "src",
                "format": "sql_server",
                "table": "dbo.events",
                "incremental": {
                    "mode": "rolling_window",
                    "column": "ts",
                    "lookback": 2,
                    "unit": "hour",
                },
            },
            write={"connection": "dst", "format": "csv", "path": "out.csv"},
        )

        executor._execute_read_phase(config, hwm_state=None)

        assert any("Incremental SQL pushdown" in s for s in executor._execution_steps)


# ===========================================================================
# Target 5: sql_file resolution (lines 599-623)
# ===========================================================================


class TestSqlFileResolution:
    """Tests for sql_file resolving to query in read phase."""

    def test_sql_file_resolved_to_query(self, tmp_path):
        sql_content = "SELECT id, name FROM users"
        sql_file = tmp_path / "query.sql"
        sql_file.write_text(sql_content, encoding="utf-8")

        config_file = str(tmp_path / "pipeline.yaml")
        executor = _make_executor(config_file=config_file)
        executor.engine.read.return_value = pd.DataFrame({"id": [1]})

        config = _simple_config(
            read={
                "connection": "src",
                "format": "csv",
                "path": "data.csv",
                "sql_file": "query.sql",
            },
        )

        executor._execute_read_phase(config, hwm_state=None)

        call_kwargs = executor.engine.read.call_args
        assert call_kwargs.kwargs["options"]["query"] == sql_content

    def test_sql_file_wrapped_with_incremental_filter(self, tmp_path):
        sql_content = "SELECT * FROM orders"
        sql_file = tmp_path / "orders.sql"
        sql_file.write_text(sql_content, encoding="utf-8")

        config_file = str(tmp_path / "pipeline.yaml")
        executor = _make_executor(config_file=config_file)
        executor.engine.table_exists.return_value = True
        executor.engine.read.return_value = pd.DataFrame({"a": [1]})

        config = _simple_config(
            read={
                "connection": "src",
                "format": "sql_server",
                "table": "orders",
                "sql_file": "orders.sql",
                "incremental": {
                    "mode": "rolling_window",
                    "column": "created_at",
                    "lookback": 7,
                    "unit": "day",
                },
            },
            write={"connection": "dst", "format": "csv", "path": "out.csv"},
        )

        executor._execute_read_phase(config, hwm_state=None)

        call_kwargs = executor.engine.read.call_args
        query = call_kwargs.kwargs["options"]["query"]
        assert "_sql_file_subquery" in query
        assert "WHERE" in query
        # filter should be removed from options since it's in the query
        assert "filter" not in call_kwargs.kwargs["options"]

    def test_sql_file_step_recorded(self, tmp_path):
        sql_file = tmp_path / "q.sql"
        sql_file.write_text("SELECT 1", encoding="utf-8")

        config_file = str(tmp_path / "pipeline.yaml")
        executor = _make_executor(config_file=config_file)
        executor.engine.read.return_value = pd.DataFrame({"a": [1]})

        config = _simple_config(
            read={
                "connection": "src",
                "format": "csv",
                "path": "data.csv",
                "sql_file": "q.sql",
            },
        )

        executor._execute_read_phase(config, hwm_state=None)

        assert any("Resolved sql_file" in s for s in executor._execution_steps)

    def test_sql_file_not_found_raises(self, tmp_path):
        config_file = str(tmp_path / "pipeline.yaml")
        executor = _make_executor(config_file=config_file)

        config = _simple_config(
            read={
                "connection": "src",
                "format": "csv",
                "path": "data.csv",
                "sql_file": "missing.sql",
            },
        )

        with pytest.raises(FileNotFoundError, match="SQL file not found"):
            executor._execute_read_phase(config, hwm_state=None)

    def test_sql_file_no_config_file_raises(self):
        executor = _make_executor(config_file=None)

        config = _simple_config(
            read={
                "connection": "src",
                "format": "csv",
                "path": "data.csv",
                "sql_file": "query.sql",
            },
        )

        with pytest.raises(ValueError, match="Cannot resolve sql_file"):
            executor._execute_read_phase(config, hwm_state=None)


# ===========================================================================
# Target 6: Simulation state capture (lines 725-767)
# ===========================================================================


class TestSimulationStateCapture:
    """Tests for HWM, random walk, and scheduled event state from simulation."""

    def test_simulation_hwm_captured_from_attrs(self):
        executor = _make_executor()
        df = pd.DataFrame({"ts": [1, 2, 3]})
        df.attrs["_simulation_max_timestamp"] = "2024-01-15T12:00:00"
        executor.engine.read.return_value = df

        config = _simple_config(
            read={
                "connection": "src",
                "format": "simulation",
                "incremental": {
                    "mode": "stateful",
                    "column": "ts",
                },
            },
        )

        _, pending_hwm = executor._execute_read_phase(config, hwm_state=None)

        assert pending_hwm is not None
        assert pending_hwm[1] == "2024-01-15T12:00:00"

    def test_simulation_hwm_state_key_defaults_to_node_name(self):
        executor = _make_executor()
        df = pd.DataFrame({"ts": [1]})
        df.attrs["_simulation_max_timestamp"] = "2024-06-01"
        executor.engine.read.return_value = df

        config = _simple_config(
            name="my_sim_node",
            read={
                "connection": "src",
                "format": "simulation",
                "incremental": {
                    "mode": "stateful",
                    "column": "ts",
                },
            },
        )

        _, pending_hwm = executor._execute_read_phase(config, hwm_state=None)

        assert pending_hwm[0] == "my_sim_node_hwm"

    def test_simulation_random_walk_state_persisted(self):
        state_mgr = MagicMock()
        executor = _make_executor(state_manager=state_mgr)
        df = pd.DataFrame({"ts": [1]})
        df.attrs["_simulation_max_timestamp"] = "2024-01-01"
        df.attrs["_simulation_random_walk_state"] = {"col_a": 42.5}
        executor.engine.read.return_value = df

        config = _simple_config(
            name="rw_node",
            read={
                "connection": "src",
                "format": "simulation",
                "incremental": {"mode": "stateful", "column": "ts"},
            },
        )

        executor._execute_read_phase(config, hwm_state=None)

        state_mgr.set_hwm.assert_any_call("rw_node_rw_state", json.dumps({"col_a": 42.5}))

    def test_simulation_scheduled_event_state_persisted(self):
        state_mgr = MagicMock()
        executor = _make_executor(state_manager=state_mgr)
        df = pd.DataFrame({"ts": [1]})
        df.attrs["_simulation_max_timestamp"] = "2024-01-01"
        df.attrs["_simulation_scheduled_event_state"] = {"event_x": "pending"}
        executor.engine.read.return_value = df

        config = _simple_config(
            name="se_node",
            read={
                "connection": "src",
                "format": "simulation",
                "incremental": {"mode": "stateful", "column": "ts"},
            },
        )

        executor._execute_read_phase(config, hwm_state=None)

        state_mgr.set_hwm.assert_any_call("se_node_se_state", json.dumps({"event_x": "pending"}))

    def test_simulation_no_hwm_when_no_max_timestamp(self):
        executor = _make_executor()
        df = pd.DataFrame({"ts": [1]})
        # No _simulation_max_timestamp in attrs
        executor.engine.read.return_value = df

        config = _simple_config(
            read={
                "connection": "src",
                "format": "simulation",
                "incremental": {"mode": "stateful", "column": "ts"},
            },
        )

        _, pending_hwm = executor._execute_read_phase(config, hwm_state=None)

        assert pending_hwm is None

    def test_simulation_rw_state_not_persisted_without_state_manager(self):
        executor = _make_executor(state_manager=None)
        df = pd.DataFrame({"ts": [1]})
        df.attrs["_simulation_max_timestamp"] = "2024-01-01"
        df.attrs["_simulation_random_walk_state"] = {"col": 1.0}
        executor.engine.read.return_value = df

        config = _simple_config(
            read={
                "connection": "src",
                "format": "simulation",
                "incremental": {"mode": "stateful", "column": "ts"},
            },
        )

        # Should not raise even without state_manager
        executor._execute_read_phase(config, hwm_state=None)


# ===========================================================================
# Target 7: _generate_incremental_sql_filter stateful mode (lines 1046-1081)
# ===========================================================================


class TestGenerateIncrementalSqlFilterStateful:
    """Tests for stateful mode in _generate_incremental_sql_filter."""

    def test_stateful_basic_filter(self):
        executor = _make_executor()
        executor.state_manager.get_hwm.return_value = "100"
        executor.engine.table_exists.return_value = True

        inc = IncrementalConfig(
            mode=IncrementalMode.STATEFUL,
            column="id",
        )
        config = _simple_config(
            read={"connection": "src", "format": "sql_server", "table": "t"},
            write={"connection": "dst", "format": "csv", "path": "out.csv"},
        )

        result = executor._generate_incremental_sql_filter(inc, config)

        assert result is not None
        assert "[id]" in result
        assert "> '100'" in result

    def test_stateful_with_watermark_lag(self):
        executor = _make_executor()
        executor.state_manager.get_hwm.return_value = "2024-06-15T10:00:00"
        executor.engine.table_exists.return_value = True

        inc = IncrementalConfig(
            mode=IncrementalMode.STATEFUL,
            column="updated_at",
            watermark_lag="2h",
        )
        config = _simple_config(
            read={"connection": "src", "format": "sql_server", "table": "t"},
            write={"connection": "dst", "format": "csv", "path": "out.csv"},
        )

        result = executor._generate_incremental_sql_filter(inc, config)

        assert result is not None
        # HWM should be shifted back by 2 hours
        assert "2024-06-15 08:00:00" in result

    def test_stateful_with_fallback_column(self):
        executor = _make_executor()
        executor.state_manager.get_hwm.return_value = "500"
        executor.engine.table_exists.return_value = True

        inc = IncrementalConfig(
            mode=IncrementalMode.STATEFUL,
            column="updated_at",
            fallback_column="created_at",
        )
        config = _simple_config(
            read={"connection": "src", "format": "sql_server", "table": "t"},
            write={"connection": "dst", "format": "csv", "path": "out.csv"},
        )

        result = executor._generate_incremental_sql_filter(inc, config)

        assert result is not None
        assert "COALESCE" in result
        assert "[updated_at]" in result
        assert "[created_at]" in result

    def test_stateful_returns_none_on_first_run(self):
        executor = _make_executor()
        executor.engine.table_exists.return_value = False

        inc = IncrementalConfig(
            mode=IncrementalMode.STATEFUL,
            column="id",
        )
        config = _simple_config(
            read={"connection": "src", "format": "sql_server", "table": "t"},
            write={"connection": "dst", "format": "csv", "path": "out.csv"},
        )

        result = executor._generate_incremental_sql_filter(inc, config)

        assert result is None

    def test_stateful_returns_none_when_no_hwm(self):
        executor = _make_executor()
        executor.state_manager.get_hwm.return_value = None
        executor.engine.table_exists.return_value = True

        inc = IncrementalConfig(
            mode=IncrementalMode.STATEFUL,
            column="id",
        )
        config = _simple_config(
            read={"connection": "src", "format": "sql_server", "table": "t"},
            write={"connection": "dst", "format": "csv", "path": "out.csv"},
        )

        result = executor._generate_incremental_sql_filter(inc, config)

        assert result is None

    def test_stateful_iso_datetime_hwm_formatted(self):
        executor = _make_executor()
        executor.state_manager.get_hwm.return_value = "2024-03-20T14:30:45.123456"
        executor.engine.table_exists.return_value = True

        inc = IncrementalConfig(
            mode=IncrementalMode.STATEFUL,
            column="event_time",
        )
        config = _simple_config(
            read={"connection": "src", "format": "sql_server", "table": "t"},
            write={"connection": "dst", "format": "csv", "path": "out.csv"},
        )

        result = executor._generate_incremental_sql_filter(inc, config)

        assert result is not None
        # ISO datetime with T should be reformatted to space-separated
        assert "2024-03-20 14:30:45" in result
        assert "T" not in result.split(">")[1]  # no T after the > sign

    def test_stateful_watermark_lag_with_invalid_hwm_passes(self):
        executor = _make_executor()
        executor.state_manager.get_hwm.return_value = "not-a-date"
        executor.engine.table_exists.return_value = True

        inc = IncrementalConfig(
            mode=IncrementalMode.STATEFUL,
            column="col",
            watermark_lag="1h",
        )
        config = _simple_config(
            read={"connection": "src", "format": "sql_server", "table": "t"},
            write={"connection": "dst", "format": "csv", "path": "out.csv"},
        )

        # Should not raise — the ValueError is caught and lag is not applied
        result = executor._generate_incremental_sql_filter(inc, config)

        assert result is not None
        assert "not-a-date" in result

    def test_stateful_custom_state_key(self):
        executor = _make_executor()
        executor.state_manager.get_hwm.return_value = "42"
        executor.engine.table_exists.return_value = True

        inc = IncrementalConfig(
            mode=IncrementalMode.STATEFUL,
            column="id",
            state_key="custom_key",
        )
        config = _simple_config(
            read={"connection": "src", "format": "sql_server", "table": "t"},
            write={"connection": "dst", "format": "csv", "path": "out.csv"},
        )

        executor._generate_incremental_sql_filter(inc, config)

        executor.state_manager.get_hwm.assert_called_with("custom_key")
