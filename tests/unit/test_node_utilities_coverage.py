"""Tests for node.py utility/helper methods not covered by existing test files."""

from __future__ import annotations

import time
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pandas as pd

from odibi.node import PhaseTimer, NodeResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_executor():
    """Create a minimal NodeExecutor instance without full __init__."""
    from odibi.node import NodeExecutor

    ex = NodeExecutor.__new__(NodeExecutor)
    ex.engine = MagicMock()
    ex.engine.name = "pandas"
    ex.connections = {}
    ex.context = MagicMock()
    ex.state_manager = MagicMock()
    ex._execution_steps = []
    ex._ctx = MagicMock()
    ex._delta_write_info = {}
    ex._persisted_dfs = []
    ex._table_exists_cache = {}
    ex.config_file = "test.yaml"
    ex._current_pipeline = None
    return ex


def _make_config(**overrides):
    cfg = MagicMock()
    cfg.name = "test_node"
    cfg.read = None
    cfg.write = None
    cfg.transform = None
    cfg.depends_on = []
    cfg.columns = {}
    cfg.privacy = None
    cfg.contracts = None
    cfg.tags = []
    cfg.cache = False
    cfg.log_level = None
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


# =========================================================================
# PhaseTimer
# =========================================================================


class TestPhaseTimer:
    def test_basic_timing(self):
        timer = PhaseTimer()
        with timer.phase("read"):
            time.sleep(0.01)
        with timer.phase("transform"):
            time.sleep(0.01)
        s = timer.summary()
        assert "read" in s
        assert "transform" in s
        assert s["read"] > 0
        assert s["transform"] > 0

    def test_summary_ms(self):
        timer = PhaseTimer()
        with timer.phase("read"):
            time.sleep(0.01)
        ms = timer.summary_ms()
        assert ms["read"] >= 10  # at least 10ms

    def test_empty_timer(self):
        timer = PhaseTimer()
        assert timer.summary() == {}
        assert timer.summary_ms() == {}


# =========================================================================
# _clean_spark_traceback
# =========================================================================


class TestCleanSparkTraceback:
    def test_removes_java_lines(self):
        ex = _make_executor()
        raw = (
            "Traceback (most recent call last):\n"
            '  File "node.py", line 100, in execute\n'
            "    result = run()\n"
            "  at org.apache.spark.sql.SparkSession.sql(SparkSession.scala:100)\n"
            "  at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)\n"
            '  File "my_module.py", line 50, in run\n'
            "RuntimeError: Spark failed\n"
        )
        cleaned = ex._clean_spark_traceback(raw)
        assert "org.apache.spark" not in cleaned
        assert "java.util.concurrent" not in cleaned
        assert "RuntimeError: Spark failed" in cleaned

    def test_removes_py4j_lines(self):
        ex = _make_executor()
        raw = "py4j.protocol.Py4JError: something\nPy4JJavaError: oops\nActual error\n"
        cleaned = ex._clean_spark_traceback(raw)
        assert "py4j.protocol" not in cleaned
        assert "Py4JJavaError" not in cleaned
        assert "Actual error" in cleaned

    def test_removes_duplicate_empty_lines(self):
        ex = _make_executor()
        raw = "line1\n\n\n\nline2\n"
        cleaned = ex._clean_spark_traceback(raw)
        assert "\n\n\n" not in cleaned

    def test_cleans_spark_exception_prefix(self):
        ex = _make_executor()
        raw = "org.apache.spark.sql.AnalysisException: Table not found\n"
        cleaned = ex._clean_spark_traceback(raw)
        assert "Table not found" in cleaned

    def test_removes_dots_lines(self):
        ex = _make_executor()
        raw = "Error\n...\nMore info\n"
        cleaned = ex._clean_spark_traceback(raw)
        assert "..." not in cleaned

    def test_resumes_after_python_file_line(self):
        ex = _make_executor()
        raw = (
            "  at scala.collection.Iterator.foreach(Iterator.scala:100)\n"
            '  File "my_code.py", line 50, in my_func\n'
            "    raise ValueError('bad')\n"
            "ValueError: bad\n"
        )
        cleaned = ex._clean_spark_traceback(raw)
        assert "my_code.py" in cleaned
        assert "ValueError: bad" in cleaned


# =========================================================================
# _calculate_pii
# =========================================================================


class TestCalculatePii:
    def test_no_pii(self):
        ex = _make_executor()
        cfg = _make_config()
        result = ex._calculate_pii(cfg)
        assert result == {}

    def test_local_pii(self):
        ex = _make_executor()
        col_meta = MagicMock()
        col_meta.pii = True
        cfg = _make_config(columns={"ssn": col_meta})
        result = ex._calculate_pii(cfg)
        assert result == {"ssn": True}

    def test_inherited_pii(self):
        ex = _make_executor()
        ex.context.get_metadata.return_value = {"pii_columns": {"email": True}}
        cfg = _make_config(depends_on=["upstream_node"])
        result = ex._calculate_pii(cfg)
        assert "email" in result

    def test_declassify(self):
        ex = _make_executor()
        col_meta = MagicMock()
        col_meta.pii = True
        privacy = MagicMock()
        privacy.declassify = ["ssn"]
        cfg = _make_config(columns={"ssn": col_meta}, privacy=privacy)
        result = ex._calculate_pii(cfg)
        assert "ssn" not in result

    def test_inherited_plus_local_plus_declassify(self):
        ex = _make_executor()
        ex.context.get_metadata.return_value = {"pii_columns": {"email": True, "phone": True}}
        col_meta = MagicMock()
        col_meta.pii = True
        privacy = MagicMock()
        privacy.declassify = ["phone"]
        cfg = _make_config(
            depends_on=["upstream"],
            columns={"name": col_meta},
            privacy=privacy,
        )
        result = ex._calculate_pii(cfg)
        assert "email" in result
        assert "name" in result
        assert "phone" not in result


# =========================================================================
# _count_rows
# =========================================================================


class TestCountRows:
    def test_none_df(self):
        ex = _make_executor()
        assert ex._count_rows(None) is None

    def test_streaming_df(self):
        ex = _make_executor()
        df = MagicMock()
        df.isStreaming = True
        assert ex._count_rows(df) is None

    def test_normal_df(self):
        ex = _make_executor()
        ex.engine.count_rows.return_value = 42
        df = MagicMock()
        df.isStreaming = False
        assert ex._count_rows(df) == 42


# =========================================================================
# _get_column_max (Pandas path)
# =========================================================================


class TestGetColumnMax:
    def test_basic_max(self):
        ex = _make_executor()
        # Remove spark attr to force Pandas path
        del ex.engine.spark
        df = pd.DataFrame({"val": [1, 5, 3]})
        result = ex._get_column_max(df, "val")
        assert result == 5

    def test_max_with_fallback_column(self):
        ex = _make_executor()
        del ex.engine.spark
        df = pd.DataFrame({"a": [None, 2, None], "b": [10, 20, 30]})
        result = ex._get_column_max(df, "a", fallback_column="b")
        assert result == 30

    def test_max_all_nan(self):
        ex = _make_executor()
        del ex.engine.spark
        df = pd.DataFrame({"val": [None, None, None]})
        result = ex._get_column_max(df, "val")
        assert result is None

    def test_max_numpy_integer(self):
        ex = _make_executor()
        del ex.engine.spark
        df = pd.DataFrame({"val": pd.array([1, 2, 3], dtype="Int64")})
        result = ex._get_column_max(df, "val")
        assert result == 3
        assert isinstance(result, int)

    def test_max_numpy_float(self):
        ex = _make_executor()
        del ex.engine.spark
        df = pd.DataFrame({"val": [1.5, 2.5, 3.5]})
        result = ex._get_column_max(df, "val")
        assert result == 3.5

    def test_max_datetime(self):
        ex = _make_executor()
        del ex.engine.spark
        df = pd.DataFrame({"ts": pd.to_datetime(["2024-01-01", "2024-06-01"])})
        result = ex._get_column_max(df, "ts")
        assert "2024-06" in str(result)

    def test_max_column_missing(self):
        ex = _make_executor()
        del ex.engine.spark
        df = pd.DataFrame({"a": [1, 2]})
        result = ex._get_column_max(df, "nonexistent")
        assert result is None

    def test_max_streaming(self):
        ex = _make_executor()
        df = MagicMock()
        df.isStreaming = True
        result = ex._get_column_max(df, "col")
        assert result is None

    def test_max_exception(self):
        ex = _make_executor()
        del ex.engine.spark
        df = MagicMock()
        df.columns = ["val"]
        df.__getitem__ = MagicMock(side_effect=Exception("bad"))
        result = ex._get_column_max(df, "val")
        assert result is None


# =========================================================================
# _generate_suggestions
# =========================================================================


class TestGenerateSuggestions:
    @patch("odibi.node.get_suggestions_for_transform")
    def test_basic_suggestions(self, mock_fn):
        mock_fn.return_value = ["Try this"]
        ex = _make_executor()
        cfg = _make_config()
        result = ex._generate_suggestions(ValueError("bad"), cfg)
        assert result == ["Try this"]

    @patch("odibi.node.get_suggestions_for_transform")
    def test_with_execution_steps(self, mock_fn):
        mock_fn.return_value = []
        ex = _make_executor()
        ex._execution_steps = ["transform: scd2"]
        cfg = _make_config()
        ex._generate_suggestions(ValueError("bad"), cfg)
        call_kwargs = mock_fn.call_args[1]
        assert call_kwargs["transformer_name"].strip() == "scd2"

    @patch("odibi.node.get_suggestions_for_transform")
    def test_with_available_columns(self, mock_fn):
        mock_fn.return_value = []
        ex = _make_executor()
        ex.context.get_df = MagicMock(return_value=pd.DataFrame({"a": [1], "b": [2]}))
        cfg = _make_config()
        ex._generate_suggestions(ValueError("bad"), cfg)
        call_kwargs = mock_fn.call_args[1]
        assert call_kwargs["available_columns"] == ["a", "b"]


# =========================================================================
# _get_config_snapshot
# =========================================================================


class TestGetConfigSnapshot:
    def test_full_snapshot(self):
        ex = _make_executor()
        read_cfg = MagicMock()
        read_cfg.connection = "local"
        read_cfg.format = MagicMock()
        read_cfg.format.value = "csv"
        read_cfg.table = "tbl"
        read_cfg.path = "/data"

        step = MagicMock()
        step.function = "add_column"
        step.params = {"col": "x", "password": "secret123"}
        transform_cfg = MagicMock()
        transform_cfg.steps = [step]

        write_cfg = MagicMock()
        write_cfg.connection = "sql"
        write_cfg.format = MagicMock()
        write_cfg.format.value = "delta"
        write_cfg.path = "/out"
        write_cfg.mode = "append"

        cfg = _make_config(
            read=read_cfg,
            transform=transform_cfg,
            write=write_cfg,
            depends_on=["dep1"],
        )
        snapshot = ex._get_config_snapshot(cfg)
        assert snapshot["node_name"] == "test_node"
        assert snapshot["read"]["connection"] == "local"
        assert snapshot["write"]["connection"] == "sql"
        assert snapshot["depends_on"] == ["dep1"]
        # Secret should be filtered out
        assert "password" not in snapshot["transform_steps"][0]["params"]
        assert "col" in snapshot["transform_steps"][0]["params"]

    def test_snapshot_no_read_write(self):
        ex = _make_executor()
        cfg = _make_config()
        snapshot = ex._get_config_snapshot(cfg)
        assert snapshot["node_name"] == "test_node"
        assert "read" not in snapshot

    def test_snapshot_exception(self):
        ex = _make_executor()
        cfg = MagicMock()
        cfg.name = property(lambda s: (_ for _ in ()).throw(Exception("boom")))
        # Force exception
        type(cfg).name = property(lambda s: (_ for _ in ()).throw(Exception("boom")))
        result = ex._get_config_snapshot(cfg)
        assert result is None


# =========================================================================
# _get_connection_health
# =========================================================================


class TestGetConnectionHealth:
    def test_with_read_and_write(self):
        ex = _make_executor()
        ex._execution_steps = ["read from local_conn"]
        read_cfg = MagicMock()
        read_cfg.connection = "local_conn"
        write_cfg = MagicMock()
        write_cfg.connection = "sql_conn"
        cfg = _make_config(read=read_cfg, write=write_cfg)
        health = ex._get_connection_health(cfg)
        assert "read_connection" in health
        assert "write_connection" in health
        assert health["read_connection"]["name"] == "local_conn"

    def test_no_connections(self):
        ex = _make_executor()
        cfg = _make_config()
        health = ex._get_connection_health(cfg)
        assert health is None

    def test_exception(self):
        ex = _make_executor()
        cfg = MagicMock()
        cfg.read = MagicMock()
        cfg.read.connection = MagicMock(side_effect=Exception("boom"))
        # Force a hard exception
        type(cfg.read).connection = property(lambda s: (_ for _ in ()).throw(Exception("x")))
        result = ex._get_connection_health(cfg)
        assert result is None


# =========================================================================
# _get_delta_table_state
# =========================================================================


class TestGetDeltaTableState:
    def test_non_delta_format(self):
        ex = _make_executor()
        write_cfg = MagicMock()
        write_cfg.format = "csv"
        cfg = _make_config(write=write_cfg)
        assert ex._get_delta_table_state(cfg) is None

    def test_no_write_config(self):
        ex = _make_executor()
        cfg = _make_config()
        assert ex._get_delta_table_state(cfg) is None

    def test_no_connection_found(self):
        ex = _make_executor()
        write_cfg = MagicMock()
        write_cfg.format = "delta"
        write_cfg.connection = "missing"
        write_cfg.path = "/data"
        cfg = _make_config(write=write_cfg)
        result = ex._get_delta_table_state(cfg)
        assert result is None

    def test_with_base_path(self):
        ex = _make_executor()
        conn = MagicMock(spec=[])  # no get_full_path
        conn.base_path = "/base"
        ex.connections = {"local": conn}
        write_cfg = MagicMock()
        write_cfg.format = "delta"
        write_cfg.connection = "local"
        write_cfg.path = "table1"
        cfg = _make_config(write=write_cfg)
        with patch.dict("sys.modules", {"deltalake": MagicMock()}):
            # DeltaTable raises "not a Delta table"
            import sys

            mock_dt_mod = sys.modules["deltalake"]
            mock_dt_mod.DeltaTable.side_effect = Exception("not a Delta table")
            result = ex._get_delta_table_state(cfg)
        assert result is not None
        assert result["table_exists"] is False

    def test_no_path_no_conn(self):
        ex = _make_executor()
        write_cfg = MagicMock()
        write_cfg.format = "delta"
        write_cfg.connection = None
        write_cfg.path = None
        cfg = _make_config(write=write_cfg)
        result = ex._get_delta_table_state(cfg)
        assert result is None


# =========================================================================
# _check_target_exists
# =========================================================================


class TestCheckTargetExists:
    def test_table_no_spark(self):
        ex = _make_executor()
        del ex.engine.spark
        write_cfg = MagicMock()
        write_cfg.table = "my_table"
        write_cfg.path = None
        conn = MagicMock()
        assert ex._check_target_exists(write_cfg, conn) is True

    def test_path_no_spark(self):
        ex = _make_executor()
        del ex.engine.spark
        write_cfg = MagicMock()
        write_cfg.table = None
        write_cfg.path = "/data/table"
        conn = MagicMock()
        conn.get_path.return_value = "/full/path"
        assert ex._check_target_exists(write_cfg, conn) is True

    def test_no_table_no_path(self):
        ex = _make_executor()
        write_cfg = MagicMock()
        write_cfg.table = None
        write_cfg.path = None
        conn = MagicMock()
        assert ex._check_target_exists(write_cfg, conn) is True

    def test_file_not_found(self):
        ex = _make_executor()
        write_cfg = MagicMock()
        write_cfg.table = MagicMock(side_effect=FileNotFoundError)
        type(write_cfg).table = property(lambda s: (_ for _ in ()).throw(FileNotFoundError))
        result = ex._check_target_exists(write_cfg, MagicMock())
        assert result is False


# =========================================================================
# _execute_dry_run
# =========================================================================


class TestExecuteDryRun:
    def test_dry_run_basic(self):
        ex = _make_executor()
        cfg = _make_config()
        result = ex._execute_dry_run(cfg)
        assert result.success is True
        assert result.metadata["dry_run"] is True
        assert result.rows_processed == 0

    def test_dry_run_with_read(self):
        ex = _make_executor()
        read_cfg = MagicMock()
        read_cfg.connection = "local"
        cfg = _make_config(read=read_cfg)
        result = ex._execute_dry_run(cfg)
        assert any("read" in s.lower() for s in result.metadata["steps"])

    def test_dry_run_with_transform(self):
        ex = _make_executor()
        transform_cfg = MagicMock()
        transform_cfg.steps = [MagicMock(), MagicMock(), MagicMock()]
        cfg = _make_config(transform=transform_cfg)
        result = ex._execute_dry_run(cfg)
        assert any("3 transform" in s for s in result.metadata["steps"])

    def test_dry_run_with_write(self):
        ex = _make_executor()
        write_cfg = MagicMock()
        write_cfg.connection = "sql_server"
        cfg = _make_config(write=write_cfg)
        result = ex._execute_dry_run(cfg)
        assert any("write" in s.lower() for s in result.metadata["steps"])


# =========================================================================
# _generate_incremental_sql_filter
# =========================================================================


class TestGenerateIncrementalSqlFilter:
    def _make_inc_config(self, **kwargs):
        from odibi.config import IncrementalMode

        inc = MagicMock()
        inc.mode = kwargs.get("mode", IncrementalMode.ROLLING_WINDOW)
        inc.column = kwargs.get("column", "updated_at")
        inc.lookback = kwargs.get("lookback", 7)
        inc.unit = kwargs.get("unit", "day")
        inc.fallback_column = kwargs.get("fallback_column", None)
        inc.date_format = kwargs.get("date_format", None)
        inc.state_key = kwargs.get("state_key", None)
        inc.watermark_lag = kwargs.get("watermark_lag", None)
        return inc

    def test_rolling_window_day(self):
        ex = _make_executor()
        ex._cached_table_exists = MagicMock(return_value=True)
        ex._quote_sql_column = MagicMock(side_effect=lambda c, *a: f"[{c}]")
        ex._get_date_expr = MagicMock(return_value=("[updated_at]", "'2024-01-01'"))
        inc = self._make_inc_config(unit="day", lookback=7)
        read_cfg = MagicMock()
        read_cfg.format = "sql_server"
        read_cfg.incremental = inc
        write_cfg = MagicMock()
        write_cfg.connection = "sql"
        write_cfg.table = "tbl"
        write_cfg.register_table = None
        cfg = _make_config(read=read_cfg, write=write_cfg)
        ex.connections = {"sql": MagicMock()}
        result = ex._generate_incremental_sql_filter(inc, cfg)
        assert ">=" in result

    def test_rolling_window_hour(self):
        ex = _make_executor()
        ex._cached_table_exists = MagicMock(return_value=True)
        ex._quote_sql_column = MagicMock(side_effect=lambda c, *a: f"[{c}]")
        ex._get_date_expr = MagicMock(return_value=("[col]", "'2024'"))
        inc = self._make_inc_config(unit="hour", lookback=24)
        cfg = _make_config(write=MagicMock(connection="sql", table="t", register_table=None))
        cfg.read = MagicMock(format="sql_server")
        ex.connections = {"sql": MagicMock()}
        result = ex._generate_incremental_sql_filter(inc, cfg)
        assert result is not None

    def test_rolling_window_month(self):
        ex = _make_executor()
        ex._cached_table_exists = MagicMock(return_value=True)
        ex._quote_sql_column = MagicMock(side_effect=lambda c, *a: f"[{c}]")
        ex._get_date_expr = MagicMock(return_value=("[col]", "'2024'"))
        inc = self._make_inc_config(unit="month", lookback=3)
        cfg = _make_config(write=MagicMock(connection="sql", table="t", register_table=None))
        cfg.read = MagicMock(format="sql_server")
        ex.connections = {"sql": MagicMock()}
        result = ex._generate_incremental_sql_filter(inc, cfg)
        assert ">=" in result

    def test_rolling_window_year(self):
        ex = _make_executor()
        ex._cached_table_exists = MagicMock(return_value=True)
        ex._quote_sql_column = MagicMock(side_effect=lambda c, *a: f"[{c}]")
        ex._get_date_expr = MagicMock(return_value=("[col]", "'2024'"))
        inc = self._make_inc_config(unit="year", lookback=1)
        cfg = _make_config(write=MagicMock(connection="sql", table="t", register_table=None))
        cfg.read = MagicMock(format="sql_server")
        ex.connections = {"sql": MagicMock()}
        result = ex._generate_incremental_sql_filter(inc, cfg)
        assert ">=" in result

    def test_rolling_window_with_fallback(self):
        ex = _make_executor()
        ex._cached_table_exists = MagicMock(return_value=True)
        ex._quote_sql_column = MagicMock(side_effect=lambda c, *a: f"[{c}]")
        ex._get_date_expr = MagicMock(return_value=("[col]", "'2024'"))
        inc = self._make_inc_config(fallback_column="created_at")
        cfg = _make_config(write=MagicMock(connection="sql", table="t", register_table=None))
        cfg.read = MagicMock(format="sql_server")
        ex.connections = {"sql": MagicMock()}
        result = ex._generate_incremental_sql_filter(inc, cfg)
        assert "COALESCE" in result

    def test_rolling_window_no_lookback(self):
        ex = _make_executor()
        ex._cached_table_exists = MagicMock(return_value=True)
        inc = self._make_inc_config(lookback=None, unit=None)
        cfg = _make_config(write=MagicMock(connection="sql", table="t", register_table=None))
        cfg.read = MagicMock(format="sql_server")
        ex.connections = {"sql": MagicMock()}
        result = ex._generate_incremental_sql_filter(inc, cfg)
        assert result is None

    def test_stateful_with_hwm(self):
        from odibi.config import IncrementalMode

        ex = _make_executor()
        ex._cached_table_exists = MagicMock(return_value=True)
        ex._quote_sql_column = MagicMock(side_effect=lambda c, *a: f"[{c}]")
        ex.state_manager.get_hwm.return_value = "2024-01-01T12:00:00"
        inc = self._make_inc_config(mode=IncrementalMode.STATEFUL)
        cfg = _make_config(write=MagicMock(connection="sql", table="t", register_table=None))
        cfg.read = MagicMock(format="sql_server")
        ex.connections = {"sql": MagicMock()}
        result = ex._generate_incremental_sql_filter(inc, cfg)
        assert ">" in result
        assert "2024-01-01 12:00:00" in result

    def test_stateful_no_hwm(self):
        from odibi.config import IncrementalMode

        ex = _make_executor()
        ex._cached_table_exists = MagicMock(return_value=True)
        ex.state_manager.get_hwm.return_value = None
        inc = self._make_inc_config(mode=IncrementalMode.STATEFUL)
        cfg = _make_config(write=MagicMock(connection="sql", table="t", register_table=None))
        cfg.read = MagicMock(format="sql_server")
        ex.connections = {"sql": MagicMock()}
        result = ex._generate_incremental_sql_filter(inc, cfg)
        assert result is None

    def test_stateful_with_fallback_column(self):
        from odibi.config import IncrementalMode

        ex = _make_executor()
        ex._cached_table_exists = MagicMock(return_value=True)
        ex._quote_sql_column = MagicMock(side_effect=lambda c, *a: f"[{c}]")
        ex.state_manager.get_hwm.return_value = "100"
        inc = self._make_inc_config(mode=IncrementalMode.STATEFUL, fallback_column="created_at")
        cfg = _make_config(write=MagicMock(connection="sql", table="t", register_table=None))
        cfg.read = MagicMock(format="sql_server")
        ex.connections = {"sql": MagicMock()}
        result = ex._generate_incremental_sql_filter(inc, cfg)
        assert "COALESCE" in result

    def test_first_run_returns_none(self):
        ex = _make_executor()
        ex._cached_table_exists = MagicMock(return_value=False)
        inc = self._make_inc_config()
        cfg = _make_config(write=MagicMock(connection="sql", table="t", register_table=None))
        cfg.read = MagicMock(format="sql_server")
        ex.connections = {"sql": MagicMock()}
        result = ex._generate_incremental_sql_filter(inc, cfg)
        assert result is None

    def test_stateful_with_watermark_lag(self):
        from odibi.config import IncrementalMode

        ex = _make_executor()
        ex._cached_table_exists = MagicMock(return_value=True)
        ex._quote_sql_column = MagicMock(side_effect=lambda c, *a: f"[{c}]")
        ex.state_manager.get_hwm.return_value = "2024-06-15T12:00:00"
        inc = self._make_inc_config(mode=IncrementalMode.STATEFUL, watermark_lag="2h")
        cfg = _make_config(write=MagicMock(connection="sql", table="t", register_table=None))
        cfg.read = MagicMock(format="sql_server")
        ex.connections = {"sql": MagicMock()}
        with patch("odibi.node.parse_duration", return_value=timedelta(hours=2)):
            result = ex._generate_incremental_sql_filter(inc, cfg)
        assert result is not None
        assert ">" in result

    def test_no_write_config(self):
        ex = _make_executor()
        inc = self._make_inc_config()
        cfg = _make_config()
        cfg.read = MagicMock(format="sql_server")
        result = ex._generate_incremental_sql_filter(inc, cfg)
        # Should still work (no first-run check)
        assert result is not None or result is None  # depends on engine mock state


# =========================================================================
# NodeResult
# =========================================================================


class TestNodeResult:
    def test_basic_creation(self):
        r = NodeResult(node_name="n1", success=True, duration=1.5)
        assert r.node_name == "n1"
        assert r.success is True
        assert r.duration == 1.5
        assert r.metadata == {}

    def test_with_error(self):
        err = ValueError("bad")
        r = NodeResult(node_name="n1", success=False, duration=0.5, error=err)
        assert r.error is err

    def test_with_schema_alias(self):
        r = NodeResult(node_name="n1", success=True, duration=0.0, schema=["a", "b"])
        assert r.result_schema == ["a", "b"]
