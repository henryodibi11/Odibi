import json
import logging
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


@pytest.fixture(autouse=True)
def suppress_odibi_logging():
    logger = logging.getLogger("odibi")
    old_propagate = logger.propagate
    logger.propagate = False
    yield
    logger.propagate = old_propagate


class TestOperationMetrics:
    def test_elapsed_ms_none_when_no_end_time(self):
        m = OperationMetrics()
        assert m.elapsed_ms is None

    def test_elapsed_ms_correct_value(self):
        m = OperationMetrics(start_time=10.0, end_time=10.5)
        assert m.elapsed_ms == pytest.approx(500.0)

    def test_row_change_none_when_rows_in_none(self):
        m = OperationMetrics(rows_out=100)
        assert m.row_delta is None

    def test_row_change_none_when_rows_out_none(self):
        m = OperationMetrics(rows_in=100)
        assert m.row_delta is None

    def test_row_change_correct_value(self):
        m = OperationMetrics(rows_in=100, rows_out=80)
        assert m.row_delta == -20

    def test_to_dict_only_non_none_fields(self):
        m = OperationMetrics()
        d = m.to_dict()
        assert "rows_in" not in d
        assert "rows_out" not in d
        assert "row_delta" not in d
        assert "elapsed_ms" not in d
        assert "columns_before" not in d
        assert "columns_after" not in d
        assert "partitions" not in d
        assert "memory_mb" not in d

    def test_to_dict_all_fields_populated(self):
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


class TestStructuredLogger:
    def test_init_default(self):
        logger = StructuredLogger()
        assert logger.structured is False
        assert logger.level == logging.INFO
        assert isinstance(logger._secrets, set)
        assert len(logger._secrets) == 0

    def test_init_structured(self):
        logger = StructuredLogger(structured=True)
        assert logger.structured is True

    def test_register_secret(self):
        logger = StructuredLogger(structured=True)
        logger.register_secret("my-secret-key")
        assert "my-secret-key" in logger._secrets

    @pytest.mark.parametrize("value", [None, "", "   "])
    def test_register_secret_ignores_invalid(self, value):
        logger = StructuredLogger(structured=True)
        logger.register_secret(value)
        assert len(logger._secrets) == 0

    def test_redact_replaces_secrets(self):
        logger = StructuredLogger(structured=True)
        logger.register_secret("password123")
        result = logger._redact("connecting with password123 to host")
        assert result == "connecting with [REDACTED] to host"

    def test_redact_no_secrets_returns_original(self):
        logger = StructuredLogger(structured=True)
        result = logger._redact("nothing secret here")
        assert result == "nothing secret here"

    def test_redact_empty_text(self):
        logger = StructuredLogger(structured=True)
        logger.register_secret("secret")
        assert logger._redact("") == ""

    def test_structured_info_prints_json(self, capsys):
        logger = StructuredLogger(structured=True, level="DEBUG")
        logger.info("test message", key="val")
        captured = capsys.readouterr()
        parsed = json.loads(captured.out.strip())
        assert parsed["level"] == "INFO"
        assert parsed["message"] == "test message"
        assert parsed["key"] == "val"
        assert "timestamp" in parsed

    def test_structured_warning_prints_json(self, capsys):
        logger = StructuredLogger(structured=True, level="DEBUG")
        logger.warning("warn msg")
        parsed = json.loads(capsys.readouterr().out.strip())
        assert parsed["level"] == "WARNING"
        assert parsed["message"] == "warn msg"

    def test_structured_error_prints_json(self, capsys):
        logger = StructuredLogger(structured=True, level="DEBUG")
        logger.error("err msg")
        parsed = json.loads(capsys.readouterr().out.strip())
        assert parsed["level"] == "ERROR"

    def test_structured_debug_prints_json(self, capsys):
        logger = StructuredLogger(structured=True, level="DEBUG")
        logger.debug("dbg msg")
        parsed = json.loads(capsys.readouterr().out.strip())
        assert parsed["level"] == "DEBUG"

    def test_structured_redacts_secrets_in_message(self, capsys):
        logger = StructuredLogger(structured=True)
        logger.register_secret("s3cr3t")
        logger.info("token is s3cr3t")
        parsed = json.loads(capsys.readouterr().out.strip())
        assert "[REDACTED]" in parsed["message"]
        assert "s3cr3t" not in parsed["message"]

    def test_structured_redacts_secrets_in_kwargs(self, capsys):
        logger = StructuredLogger(structured=True)
        logger.register_secret("s3cr3t")
        logger.info("connecting", conn_str="host;password=s3cr3t")
        parsed = json.loads(capsys.readouterr().out.strip())
        assert "s3cr3t" not in parsed["conn_str"]
        assert "[REDACTED]" in parsed["conn_str"]

    def test_human_readable_info(self):
        logger = StructuredLogger(structured=False)
        logger.logger.propagate = False
        with patch.object(logger.logger, "info") as mock_info:
            logger.info("hello world")
            mock_info.assert_called_once_with("hello world")

    def test_human_readable_warning(self):
        logger = StructuredLogger(structured=False)
        logger.logger.propagate = False
        with patch.object(logger.logger, "warning") as mock_warn:
            logger.warning("watch out")
            mock_warn.assert_called_once_with("[WARN] watch out")

    def test_human_readable_error(self):
        logger = StructuredLogger(structured=False)
        logger.logger.propagate = False
        with patch.object(logger.logger, "error") as mock_err:
            logger.error("broken")
            mock_err.assert_called_once_with("[ERROR] broken")

    def test_human_readable_debug(self):
        logger = StructuredLogger(structured=False, level="DEBUG")
        logger.logger.propagate = False
        with patch.object(logger.logger, "debug") as mock_dbg:
            logger.debug("trace info")
            mock_dbg.assert_called_once_with("[DEBUG] trace info")

    def test_human_readable_with_kwargs(self):
        logger = StructuredLogger(structured=False)
        logger.logger.propagate = False
        with patch.object(logger.logger, "info") as mock_info:
            logger.info("msg", key="val", num=42)
            call_arg = mock_info.call_args[0][0]
            assert "msg" in call_arg
            assert "key=val" in call_arg
            assert "num=42" in call_arg

    def test_log_below_level_skipped(self, capsys):
        logger = StructuredLogger(structured=True, level="WARNING")
        logger.info("should not appear")
        assert capsys.readouterr().out == ""


class TestModuleLevelFunctions:
    def setup_method(self):
        import odibi.utils.logging_context as mod

        mod._global_context = None

    def test_get_logging_context_returns_logging_context(self):
        ctx = get_logging_context()
        assert isinstance(ctx, LoggingContext)

    def test_set_and_get_logging_context_roundtrip(self):
        custom = LoggingContext(
            logger=StructuredLogger(structured=True),
            pipeline_id="test_pipe",
        )
        set_logging_context(custom)
        retrieved = get_logging_context()
        assert retrieved is custom
        assert retrieved.pipeline_id == "test_pipe"

    def test_create_logging_context_with_params(self):
        ctx = create_logging_context(
            pipeline_id="p1",
            node_id="n1",
            engine="pandas",
        )
        assert isinstance(ctx, LoggingContext)
        assert ctx.pipeline_id == "p1"
        assert ctx.node_id == "n1"
        assert ctx.engine == "pandas"


class TestLoggingContext:
    @pytest.fixture
    def ctx(self):
        logger = StructuredLogger(structured=True, level="DEBUG")
        return LoggingContext(logger=logger, pipeline_id="pipe1", node_id="node1", engine="pandas")

    @pytest.fixture
    def ctx_minimal(self):
        logger = StructuredLogger(structured=True, level="DEBUG")
        return LoggingContext(logger=logger)

    def test_logger_property_returns_injected(self, ctx):
        assert ctx.logger is ctx._logger

    def test_logger_property_fallback_when_none(self):
        ctx = LoggingContext(logger=None)
        result = ctx.logger
        assert result is not None
        assert hasattr(result, "info")

    def test_base_context_with_all_fields(self, ctx):
        bc = ctx._base_context()
        assert bc["pipeline_id"] == "pipe1"
        assert bc["node_id"] == "node1"
        assert bc["engine"] == "pandas"
        assert "timestamp" in bc

    def test_base_context_minimal(self, ctx_minimal):
        bc = ctx_minimal._base_context()
        assert "pipeline_id" not in bc
        assert "node_id" not in bc
        assert "engine" not in bc

    def test_with_context_creates_new(self, ctx):
        new_ctx = ctx.with_context(node_id="node2")
        assert new_ctx.node_id == "node2"
        assert new_ctx.pipeline_id == "pipe1"
        assert new_ctx is not ctx

    def test_context_manager_enter_exit(self, ctx):
        with ctx as c:
            assert c is ctx

    def test_context_manager_logs_exception(self, ctx, capsys):
        try:
            with ctx:
                raise ValueError("boom")
        except ValueError:
            pass
        out = capsys.readouterr().out
        assert "boom" in out

    def test_info_logs_with_context(self, ctx, capsys):
        ctx.info("hello")
        parsed = json.loads(capsys.readouterr().out.strip())
        assert parsed["message"] == "hello"
        assert parsed["pipeline_id"] == "pipe1"

    def test_warning_logs_with_context(self, ctx, capsys):
        ctx.warning("watch out")
        parsed = json.loads(capsys.readouterr().out.strip())
        assert parsed["level"] == "WARNING"

    def test_error_logs_with_context(self, ctx, capsys):
        ctx.error("bad")
        parsed = json.loads(capsys.readouterr().out.strip())
        assert parsed["level"] == "ERROR"

    def test_debug_logs_with_context(self, ctx, capsys):
        ctx.debug("trace")
        parsed = json.loads(capsys.readouterr().out.strip())
        assert parsed["level"] == "DEBUG"

    def test_operation_context_manager_success(self, ctx, capsys):
        with ctx.operation(OperationType.READ, "read csv") as metrics:
            metrics.rows_in = 100
            metrics.rows_out = 100
        lines = [json.loads(line) for line in capsys.readouterr().out.strip().split("\n")]
        assert any("Starting" in entry["message"] for entry in lines)
        assert any("Completed" in entry["message"] for entry in lines)

    def test_operation_context_manager_exception(self, ctx, capsys):
        with pytest.raises(RuntimeError):
            with ctx.operation(OperationType.TRANSFORM, "fail"):
                raise RuntimeError("oops")

    def test_log_operation_start_returns_metrics(self, ctx, capsys):
        metrics = ctx.log_operation_start(OperationType.WRITE, "write parquet")
        assert isinstance(metrics, OperationMetrics)
        capsys.readouterr()

    def test_log_schema_change(self, ctx, capsys):
        ctx.log_schema_change(
            {"a": "int", "b": "str"},
            {"a": "int", "c": "float"},
        )
        out = capsys.readouterr().out
        assert "Schema" in out

    def test_log_row_count_change(self, ctx, capsys):
        ctx.log_row_count_change(100, 80, operation="filter")
        out = capsys.readouterr().out
        assert "100" in out
        assert "80" in out

    def test_log_validation_result_passed(self, ctx, capsys):
        ctx.log_validation_result(True, "not_null")
        out = capsys.readouterr().out
        assert "passed" in out

    def test_log_validation_result_failed(self, ctx, capsys):
        ctx.log_validation_result(False, "range_check", failures=["too high"])
        out = capsys.readouterr().out
        assert "failed" in out

    def test_log_connection(self, ctx, capsys):
        ctx.log_connection("azure_blob", "my_storage", action="connect")
        out = capsys.readouterr().out
        assert "my_storage" in out

    def test_log_file_io(self, ctx, capsys):
        ctx.log_file_io("/data/test.parquet", "parquet", "read", rows=500)
        out = capsys.readouterr().out
        assert "parquet" in out

    def test_log_graph_operation(self, ctx, capsys):
        ctx.log_graph_operation("resolve", node_count=5, edge_count=3)
        out = capsys.readouterr().out
        assert "resolve" in out

    def test_log_spark_metrics(self, ctx, capsys):
        ctx.log_spark_metrics(partition_count=8, cached=True)
        out = capsys.readouterr().out
        assert "8" in out

    def test_log_spark_metrics_empty(self, ctx, capsys):
        ctx.log_spark_metrics()
        assert capsys.readouterr().out == ""

    def test_log_pandas_metrics(self, ctx, capsys):
        ctx.log_pandas_metrics(memory_mb=50.5, chunked=True, chunk_size=1000)
        out = capsys.readouterr().out
        assert "50.5" in out

    def test_log_pandas_metrics_high_memory_warns(self, ctx, capsys):
        ctx.log_pandas_metrics(memory_mb=2000.0)
        out = capsys.readouterr().out
        assert "High memory" in out

    def test_log_pandas_metrics_empty(self, ctx, capsys):
        ctx.log_pandas_metrics()
        assert capsys.readouterr().out == ""
