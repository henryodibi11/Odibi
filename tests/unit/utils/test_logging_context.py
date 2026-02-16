"""Comprehensive unit tests for odibi/utils/logging_context.py."""

import json
from unittest.mock import patch

import pytest

from odibi.utils.logging_context import (
    LoggingContext,
    OperationMetrics,
    OperationType,
    StructuredLogger,
    create_logging_context,
    get_logging_context,
    set_logging_context,
)


# ---------------------------------------------------------------------------
# OperationMetrics
# ---------------------------------------------------------------------------
class TestOperationMetrics:
    """Tests for OperationMetrics dataclass."""

    def test_elapsed_ms_without_end_time(self):
        m = OperationMetrics(start_time=100.0)
        assert m.elapsed_ms is None

    def test_elapsed_ms_with_end_time(self):
        m = OperationMetrics(start_time=100.0, end_time=100.5)
        assert m.elapsed_ms == pytest.approx(500.0)

    def test_row_delta_both_set(self):
        m = OperationMetrics(rows_in=100, rows_out=80)
        assert m.row_delta == -20

    def test_row_delta_missing_rows_in(self):
        m = OperationMetrics(rows_out=80)
        assert m.row_delta is None

    def test_row_delta_missing_rows_out(self):
        m = OperationMetrics(rows_in=100)
        assert m.row_delta is None

    def test_to_dict_empty(self):
        m = OperationMetrics(start_time=1.0)
        assert m.to_dict() == {}

    def test_to_dict_full(self):
        m = OperationMetrics(
            start_time=1.0,
            end_time=2.0,
            rows_in=100,
            rows_out=90,
            schema_before={"a": "int", "b": "str"},
            schema_after={"a": "int"},
            partition_count=4,
            memory_bytes=2 * 1024 * 1024,
            extra={"custom": "value"},
        )
        d = m.to_dict()
        assert d["elapsed_ms"] == pytest.approx(1000.0)
        assert d["rows_in"] == 100
        assert d["rows_out"] == 90
        assert d["row_delta"] == -10
        assert d["columns_before"] == 2
        assert d["columns_after"] == 1
        assert d["partitions"] == 4
        assert d["memory_mb"] == pytest.approx(2.0)
        assert d["custom"] == "value"


# ---------------------------------------------------------------------------
# StructuredLogger
# ---------------------------------------------------------------------------
class TestStructuredLogger:
    """Tests for StructuredLogger."""

    def test_register_secret_and_redact(self):
        logger = StructuredLogger(structured=True, level="DEBUG")
        logger.register_secret("my-password-123")
        assert logger._redact("conn=my-password-123") == "conn=[REDACTED]"

    def test_register_empty_secret_ignored(self):
        logger = StructuredLogger(structured=True, level="DEBUG")
        logger.register_secret("")
        logger.register_secret("   ")
        assert len(logger._secrets) == 0

    def test_redact_no_secrets_registered(self):
        logger = StructuredLogger(structured=True, level="DEBUG")
        assert logger._redact("plain text") == "plain text"

    def test_redact_none_text(self):
        logger = StructuredLogger(structured=True, level="DEBUG")
        logger.register_secret("secret")
        assert logger._redact(None) is None

    def test_structured_mode_outputs_json(self, capsys):
        logger = StructuredLogger(structured=True, level="INFO")
        logger.info("hello world", key="val")
        captured = capsys.readouterr().out.strip()
        data = json.loads(captured)
        assert data["message"] == "hello world"
        assert data["level"] == "INFO"
        assert data["key"] == "val"
        assert "timestamp" in data

    def test_non_structured_mode_human_readable(self, caplog):
        logger = StructuredLogger(structured=False, level="DEBUG")
        import logging

        with caplog.at_level(logging.DEBUG, logger="odibi"):
            logger.info("test msg", extra_key="extra_val")
        assert any("test msg" in r.message for r in caplog.records)

    def test_level_filtering_debug_not_logged_at_info(self, capsys):
        logger = StructuredLogger(structured=True, level="INFO")
        logger.debug("should not appear")
        captured = capsys.readouterr().out.strip()
        assert captured == ""

    def test_warning_logged_at_info_level(self, capsys):
        logger = StructuredLogger(structured=True, level="INFO")
        logger.warning("warn msg")
        captured = capsys.readouterr().out.strip()
        data = json.loads(captured)
        assert data["level"] == "WARNING"

    def test_error_logged(self, capsys):
        logger = StructuredLogger(structured=True, level="INFO")
        logger.error("err msg")
        captured = capsys.readouterr().out.strip()
        data = json.loads(captured)
        assert data["level"] == "ERROR"

    def test_redact_in_kwargs(self, capsys):
        logger = StructuredLogger(structured=True, level="INFO")
        logger.register_secret("s3cret")
        logger.info("connecting", password="s3cret")
        captured = capsys.readouterr().out.strip()
        data = json.loads(captured)
        assert data["password"] == "[REDACTED]"
        assert "s3cret" not in captured


# ---------------------------------------------------------------------------
# LoggingContext
# ---------------------------------------------------------------------------
class TestLoggingContext:
    """Tests for LoggingContext."""

    def _make_ctx(self, **overrides):
        defaults = dict(
            logger=StructuredLogger(structured=True, level="DEBUG"),
            pipeline_id="pipe1",
            node_id="node1",
            engine="pandas",
        )
        defaults.update(overrides)
        return LoggingContext(**defaults)

    # -- with_context -------------------------------------------------------
    def test_with_context_creates_new_context(self):
        ctx = self._make_ctx()
        child = ctx.with_context(node_id="node2")
        assert child.node_id == "node2"
        assert child.pipeline_id == "pipe1"
        assert child is not ctx

    # -- __enter__ / __exit__ -----------------------------------------------
    def test_enter_returns_self(self):
        ctx = self._make_ctx()
        with ctx as c:
            assert c is ctx

    def test_exit_logs_exception(self, capsys):
        ctx = self._make_ctx()
        try:
            with ctx:
                raise ValueError("boom")
        except ValueError:
            pass
        captured = capsys.readouterr().out
        assert "ValueError" in captured
        assert "boom" in captured

    # -- operation context manager ------------------------------------------
    def test_operation_normal(self, capsys):
        ctx = self._make_ctx()
        with ctx.operation(OperationType.TRANSFORM, "filter") as metrics:
            metrics.rows_in = 100
            metrics.rows_out = 50
        captured = capsys.readouterr().out
        assert "Completed transform: filter" in captured

    def test_operation_exception(self, capsys):
        ctx = self._make_ctx()
        with pytest.raises(RuntimeError, match="fail"):
            with ctx.operation(OperationType.READ, "load") as metrics:
                metrics.rows_in = 10
                raise RuntimeError("fail")
        captured = capsys.readouterr().out
        assert "RuntimeError" in captured

    # -- log_operation_start / log_operation_end ----------------------------
    def test_log_operation_start_end(self, capsys):
        ctx = self._make_ctx()
        metrics = ctx.log_operation_start(OperationType.WRITE, "save")
        metrics.rows_in = 200
        metrics.rows_out = 200
        ctx.log_operation_end(metrics, success=True)
        captured = capsys.readouterr().out
        assert "Completed write: save" in captured

    def test_log_operation_end_failure(self, capsys):
        ctx = self._make_ctx()
        metrics = ctx.log_operation_start(OperationType.VALIDATE, "check")
        ctx.log_operation_end(metrics, success=False)
        captured = capsys.readouterr().out
        assert "Failed validate: check" in captured

    def test_log_operation_end_no_stack(self, capsys):
        ctx = self._make_ctx()
        ctx.log_operation_end(success=True)
        captured = capsys.readouterr().out
        assert "execute" in captured

    # -- log_exception ------------------------------------------------------
    def test_log_exception(self, capsys):
        ctx = self._make_ctx()
        ctx.log_exception(ValueError("oops"), operation="transform")
        captured = capsys.readouterr().out
        assert "ValueError" in captured
        assert "oops" in captured

    def test_log_exception_with_traceback(self, capsys):
        ctx = self._make_ctx()
        ctx.log_exception(
            RuntimeError("tb_test"),
            operation="load",
            include_traceback=True,
        )
        captured = capsys.readouterr().out
        assert "RuntimeError" in captured

    # -- log_schema_change --------------------------------------------------
    def test_log_schema_change_added_removed(self, capsys):
        ctx = self._make_ctx()
        before = {"a": "int", "b": "str"}
        after = {"a": "int", "c": "float"}
        ctx.log_schema_change(before, after, operation="join")
        captured = capsys.readouterr().out
        assert "Schema change in join" in captured

    # -- log_row_count_change -----------------------------------------------
    def test_log_row_count_change(self, capsys):
        ctx = self._make_ctx()
        ctx.log_row_count_change(100, 80, operation="filter")
        captured = capsys.readouterr().out
        assert "100 -> 80" in captured
        assert "-20" in captured

    def test_log_row_count_change_zero_before(self, capsys):
        ctx = self._make_ctx()
        ctx.log_row_count_change(0, 10, operation="insert")
        captured = capsys.readouterr().out
        assert "0 -> 10" in captured

    # -- log_spark_metrics (name avoids Windows conftest "spark" filter) ----
    def test_log_distributed_engine_metrics(self, capsys):
        ctx = self._make_ctx()
        ctx.log_spark_metrics(
            partition_count=8,
            shuffle_partitions=200,
            broadcast_size_mb=1.5,
            cached=True,
        )
        captured = capsys.readouterr().out
        assert "Spark metrics" in captured

    def test_log_distributed_engine_metrics_empty(self, capsys):
        ctx = self._make_ctx()
        ctx.log_spark_metrics()
        captured = capsys.readouterr().out
        assert captured.strip() == ""

    # -- log_pandas_metrics -------------------------------------------------
    def test_log_pandas_metrics(self, capsys):
        ctx = self._make_ctx()
        ctx.log_pandas_metrics(
            memory_mb=50.0,
            dtypes={"a": "int64", "b": "object"},
            chunked=True,
            chunk_size=1000,
        )
        captured = capsys.readouterr().out
        assert "Pandas metrics" in captured

    def test_log_pandas_metrics_high_memory_warning(self, capsys):
        ctx = self._make_ctx()
        ctx.log_pandas_metrics(memory_mb=1500.0)
        captured = capsys.readouterr().out
        assert "High memory usage" in captured

    def test_log_pandas_metrics_empty(self, capsys):
        ctx = self._make_ctx()
        ctx.log_pandas_metrics()
        captured = capsys.readouterr().out
        assert captured.strip() == ""

    # -- log_validation_result ----------------------------------------------
    def test_log_validation_result_passed(self, capsys):
        ctx = self._make_ctx()
        ctx.log_validation_result(passed=True, rule_name="not_null_check")
        captured = capsys.readouterr().out
        assert "Validation passed: not_null_check" in captured

    def test_log_validation_result_failed(self, capsys):
        ctx = self._make_ctx()
        ctx.log_validation_result(
            passed=False,
            rule_name="range_check",
            failures=["value out of range"],
        )
        captured = capsys.readouterr().out
        assert "Validation failed: range_check" in captured

    # -- log_connection -----------------------------------------------------
    def test_log_connection(self, capsys):
        ctx = self._make_ctx()
        ctx.log_connection("azure_blob", "my_storage", action="connect")
        captured = capsys.readouterr().out
        assert "Connection connect: my_storage" in captured

    # -- log_file_io --------------------------------------------------------
    def test_log_file_io(self, capsys):
        ctx = self._make_ctx()
        ctx.log_file_io(
            path="/data/out.parquet",
            format="parquet",
            mode="write",
            rows=500,
            size_mb=12.345,
            partitions=["date"],
        )
        captured = capsys.readouterr().out
        assert "File I/O: write parquet" in captured

    # -- log_graph_operation ------------------------------------------------
    def test_log_graph_operation(self, capsys):
        ctx = self._make_ctx()
        ctx.log_graph_operation(
            "resolve",
            node_count=5,
            edge_count=8,
            layer_count=3,
        )
        captured = capsys.readouterr().out
        assert "Graph resolve" in captured


# ---------------------------------------------------------------------------
# Domain loggers: get / set / create
# ---------------------------------------------------------------------------
class TestDomainLoggers:
    """Tests for get_logging_context, set_logging_context, create_logging_context."""

    def setup_method(self):
        import odibi.utils.logging_context as _mod

        self._mod = _mod
        self._original = _mod._global_context
        _mod._global_context = None

    def teardown_method(self):
        self._mod._global_context = self._original

    def test_set_and_get_logging_context(self):
        ctx = LoggingContext(
            logger=StructuredLogger(structured=True, level="DEBUG"),
            pipeline_id="test_pipe",
        )
        set_logging_context(ctx)
        assert get_logging_context() is ctx

    def test_get_logging_context_creates_default(self):
        result = get_logging_context()
        assert isinstance(result, LoggingContext)

    @patch("odibi.utils.logging_context.logger", create=True)
    def test_create_logging_context(self, mock_logger):
        ctx = create_logging_context(
            pipeline_id="p1",
            node_id="n1",
            engine="spark",
        )
        assert ctx.pipeline_id == "p1"
        assert ctx.node_id == "n1"
        assert ctx.engine == "spark"
