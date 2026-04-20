"""Tests for CatalogManager observability tables: run logging, pipeline runs, failures.

Covers: log_run, _log_run_sql_server, log_runs_batch, log_pipeline_run,
        _log_pipeline_run_sql_server, _sql_server_table_exists,
        log_node_runs_batch, _log_node_runs_batch_sql_server,
        log_failure, _log_failure_sql_server  (issue #292).

NOTE: Test names avoid 'spark' and 'delta' to prevent conftest.py from skipping on Windows.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from odibi.catalog import CatalogManager
from odibi.config import SystemConfig
from odibi.engine.pandas_engine import PandasEngine


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def catalog_manager(tmp_path):
    """CatalogManager in Pandas mode, bootstrapped, with project set."""
    config = SystemConfig(connection="local", path="_odibi_system")
    base_path = str(tmp_path / "_odibi_system")
    engine = PandasEngine(config={})
    cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=engine)
    cm.bootstrap()
    cm.project = "test_project"
    return cm


@pytest.fixture
def bare_catalog(tmp_path):
    """CatalogManager with no engine and no spark (all operations should no-op)."""
    config = SystemConfig(connection="local", path="_odibi_system")
    base_path = str(tmp_path / "_odibi_system")
    cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=None)
    return cm


@pytest.fixture
def sql_server_catalog(tmp_path):
    """CatalogManager configured with a mock SQL Server connection."""
    config = SystemConfig(connection="local", path="_odibi_system")
    base_path = str(tmp_path / "_odibi_system")
    engine = PandasEngine(config={})
    cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=engine)
    # Mock connection with AzureSQL class name to trigger is_sql_server_mode
    mock_conn = MagicMock()
    mock_conn.__class__ = type("AzureSQL", (), {})
    cm.connection = mock_conn
    cm.project = "test_project"
    return cm


def _read_table(cm, table_key):
    """Read a catalog table and return a pandas DataFrame."""
    from deltalake import DeltaTable

    dt = DeltaTable(cm.tables[table_key])
    return dt.to_pandas()


def _make_pipeline_run(**overrides):
    """Build a pipeline_run dict with sensible defaults."""
    base = {
        "run_id": "run-001",
        "pipeline_name": "etl_customers",
        "owner": "data_team",
        "layer": "silver",
        "run_start_at": datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
        "run_end_at": datetime(2025, 1, 1, 10, 5, 0, tzinfo=timezone.utc),
        "duration_ms": 300000,
        "status": "SUCCESS",
        "nodes_total": 3,
        "nodes_succeeded": 3,
        "nodes_failed": 0,
        "nodes_skipped": 0,
        "rows_processed": 1000,
        "error_summary": None,
        "terminal_nodes": "node_c",
        "environment": "prod",
        "created_at": datetime(2025, 1, 1, 10, 5, 1, tzinfo=timezone.utc),
    }
    base.update(overrides)
    return base


def _make_node_result(**overrides):
    """Build a node result dict for log_node_runs_batch."""
    base = {
        "run_id": "run-001",
        "node_id": "node-001",
        "pipeline_name": "etl_customers",
        "node_name": "load_customers",
        "status": "SUCCESS",
        "run_start_at": datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
        "run_end_at": datetime(2025, 1, 1, 10, 1, 0, tzinfo=timezone.utc),
        "duration_ms": 60000,
        "rows_processed": 500,
        "metrics_json": '{"rows_in": 500}',
        "environment": "prod",
        "created_at": datetime(2025, 1, 1, 10, 1, 1, tzinfo=timezone.utc),
    }
    base.update(overrides)
    return base


# ===========================================================================
# log_run()
# ===========================================================================


class TestLogRun:
    """Tests for CatalogManager.log_run()."""

    def test_pandas_engine_appends_record(self, catalog_manager):
        """log_run writes a row to meta_runs via Pandas engine."""
        catalog_manager.log_run(
            run_id="run-001",
            pipeline_name="etl_orders",
            node_name="load_orders",
            status="SUCCESS",
            rows_processed=100,
            duration_ms=5000,
            metrics_json='{"rows_in": 100}',
        )
        df = _read_table(catalog_manager, "meta_runs")
        assert len(df) == 1
        assert df.iloc[0]["run_id"] == "run-001"
        assert df.iloc[0]["pipeline_name"] == "etl_orders"
        assert df.iloc[0]["node_name"] == "load_orders"
        assert df.iloc[0]["status"] == "SUCCESS"
        assert df.iloc[0]["project"] == "test_project"

    def test_appends_multiple_records(self, catalog_manager):
        """Multiple log_run calls append separate rows."""
        catalog_manager.log_run("r1", "pipe", "n1", "SUCCESS", 10, 100)
        catalog_manager.log_run("r2", "pipe", "n2", "FAILED", 0, 50)
        df = _read_table(catalog_manager, "meta_runs")
        assert len(df) == 2
        assert set(df["run_id"]) == {"r1", "r2"}

    def test_no_engine_no_op(self, bare_catalog):
        """log_run with no engine is a silent no-op."""
        bare_catalog.log_run("r1", "pipe", "node", "SUCCESS")  # Should not raise

    def test_retry_failure_logs_warning(self, catalog_manager):
        """If the write fails, a warning is logged (not raised)."""
        with patch("odibi.catalog.logger") as mock_logger:
            with patch.object(catalog_manager.engine, "write", side_effect=IOError("disk full")):
                catalog_manager.log_run("r1", "pipe", "node", "SUCCESS")
        mock_logger.warning.assert_called_once()
        assert "Failed to log run" in mock_logger.warning.call_args[0][0]

    def test_sql_server_mode_delegates(self, sql_server_catalog):
        """log_run in SQL Server mode calls _log_run_sql_server."""
        with patch.object(sql_server_catalog, "_log_run_sql_server") as mock_sql:
            sql_server_catalog.log_run("r1", "pipe", "node", "SUCCESS", 10, 100, "{}")
        mock_sql.assert_called_once()
        args = mock_sql.call_args[0]
        assert args[0] == "r1"  # run_id
        assert args[2] == "pipe"  # pipeline_name

    def test_default_optional_params(self, catalog_manager):
        """log_run with default optional params writes correctly."""
        catalog_manager.log_run("r1", "pipe", "node", "SUCCESS")
        df = _read_table(catalog_manager, "meta_runs")
        assert df.iloc[0]["rows_processed"] == 0
        assert df.iloc[0]["duration_ms"] == 0
        assert df.iloc[0]["metrics_json"] == "{}"

    def test_environment_from_config(self, tmp_path):
        """log_run reads environment from config."""
        config = SystemConfig(connection="local", path="_odibi_system")
        config.environment = "production"
        base_path = str(tmp_path / "_odibi_system")
        engine = PandasEngine(config={})
        cm = CatalogManager(spark=None, config=config, base_path=base_path, engine=engine)
        cm.bootstrap()
        cm.log_run("r1", "pipe", "node", "SUCCESS")
        df = _read_table(cm, "meta_runs")
        assert df.iloc[0]["environment"] == "production"


# ===========================================================================
# _log_run_sql_server()
# ===========================================================================


class TestLogRunSqlServer:
    """Tests for CatalogManager._log_run_sql_server()."""

    def test_executes_insert(self, sql_server_catalog):
        """SQL Server log_run executes INSERT statement via connection."""
        sql_server_catalog._log_run_sql_server(
            run_id="r1",
            project="proj",
            pipeline_name="pipe",
            node_name="node",
            status="SUCCESS",
            rows_processed=100,
            duration_ms=5000,
            metrics_json='{"rows": 100}',
            environment="prod",
        )
        sql_server_catalog.connection.execute.assert_called_once()
        call_args = sql_server_catalog.connection.execute.call_args
        assert "INSERT INTO" in call_args[0][0]
        params = call_args[0][1]
        assert params["run_id"] == "r1"
        assert params["rows"] == 100

    def test_failure_logs_warning(self, sql_server_catalog):
        """SQL Server log_run failure logs warning."""
        sql_server_catalog.connection.execute.side_effect = Exception("conn error")
        with patch("odibi.catalog.logger") as mock_logger:
            sql_server_catalog._log_run_sql_server(
                "r1", "proj", "pipe", "node", "SUCCESS", 0, 0, "{}", None
            )
        mock_logger.warning.assert_called_once()
        assert "Failed to log run to SQL Server" in mock_logger.warning.call_args[0][0]

    def test_defaults_none_params(self, sql_server_catalog):
        """SQL Server log_run handles None for optional params."""
        sql_server_catalog._log_run_sql_server(
            "r1", None, "pipe", "node", "SUCCESS", None, None, None, None
        )
        params = sql_server_catalog.connection.execute.call_args[0][1]
        assert params["rows"] == 0
        assert params["duration"] == 0
        assert params["metrics"] == "{}"

    def test_custom_schema_name(self, sql_server_catalog):
        """SQL Server log_run uses schema_name from config."""
        sql_server_catalog.config.schema_name = "custom_schema"
        sql_server_catalog._log_run_sql_server(
            "r1", "proj", "pipe", "node", "SUCCESS", 10, 100, "{}", "dev"
        )
        sql_text = sql_server_catalog.connection.execute.call_args[0][0]
        assert "custom_schema" in sql_text


# ===========================================================================
# log_runs_batch()
# ===========================================================================


class TestLogRunsBatch:
    """Tests for CatalogManager.log_runs_batch()."""

    def test_empty_records_no_op(self, catalog_manager):
        """Empty records list is a silent no-op."""
        catalog_manager.log_runs_batch([])  # Should not raise

    def test_pandas_engine_appends_records(self, catalog_manager):
        """Batch log writes multiple rows to meta_runs."""
        records = [
            {
                "run_id": "r1",
                "pipeline_name": "pipe",
                "node_name": "n1",
                "status": "SUCCESS",
                "rows_processed": 100,
                "duration_ms": 1000,
                "metrics_json": "{}",
            },
            {
                "run_id": "r1",
                "pipeline_name": "pipe",
                "node_name": "n2",
                "status": "FAILED",
                "rows_processed": 0,
                "duration_ms": 500,
                "metrics_json": "{}",
            },
        ]
        catalog_manager.log_runs_batch(records)
        df = _read_table(catalog_manager, "meta_runs")
        assert len(df) == 2
        assert set(df["node_name"]) == {"n1", "n2"}
        # project should be filled from catalog_manager.project
        assert all(df["project"] == "test_project")

    def test_no_engine_no_op(self, bare_catalog):
        """Batch log with no engine is a no-op."""
        bare_catalog.log_runs_batch(
            [{"run_id": "r1", "pipeline_name": "p", "node_name": "n", "status": "S"}]
        )

    def test_sql_server_mode_iterates(self, sql_server_catalog):
        """SQL Server mode calls _log_run_sql_server for each record."""
        records = [
            {"run_id": "r1", "pipeline_name": "p", "node_name": "n1", "status": "SUCCESS"},
            {"run_id": "r1", "pipeline_name": "p", "node_name": "n2", "status": "FAILED"},
        ]
        with patch.object(sql_server_catalog, "_log_run_sql_server") as mock_sql:
            sql_server_catalog.log_runs_batch(records)
        assert mock_sql.call_count == 2

    def test_retry_failure_logs_warning(self, catalog_manager):
        """Batch log failure logs warning."""
        with patch("odibi.catalog.logger") as mock_logger:
            with patch.object(catalog_manager.engine, "write", side_effect=IOError("fail")):
                catalog_manager.log_runs_batch(
                    [{"run_id": "r1", "pipeline_name": "p", "node_name": "n", "status": "S"}]
                )
        mock_logger.warning.assert_called_once()
        assert "Failed to batch log runs" in mock_logger.warning.call_args[0][0]

    def test_defaults_for_missing_keys(self, catalog_manager):
        """Batch log uses defaults for optional keys not present in records."""
        records = [
            {"run_id": "r1", "pipeline_name": "p", "node_name": "n", "status": "S"},
        ]
        catalog_manager.log_runs_batch(records)
        df = _read_table(catalog_manager, "meta_runs")
        assert df.iloc[0]["rows_processed"] == 0
        assert df.iloc[0]["duration_ms"] == 0
        assert df.iloc[0]["metrics_json"] == "{}"


# ===========================================================================
# log_pipeline_run()
# ===========================================================================


class TestLogPipelineRun:
    """Tests for CatalogManager.log_pipeline_run()."""

    def test_pandas_engine_appends_record(self, catalog_manager):
        """log_pipeline_run writes a row to meta_pipeline_runs."""
        run = _make_pipeline_run()
        catalog_manager.log_pipeline_run(run)
        df = _read_table(catalog_manager, "meta_pipeline_runs")
        assert len(df) == 1
        assert df.iloc[0]["run_id"] == "run-001"
        assert df.iloc[0]["pipeline_name"] == "etl_customers"
        assert df.iloc[0]["status"] == "SUCCESS"
        assert df.iloc[0]["project"] == "test_project"

    def test_no_engine_no_op(self, bare_catalog):
        """log_pipeline_run with no engine is a no-op."""
        bare_catalog.log_pipeline_run(_make_pipeline_run())

    def test_sql_server_mode_delegates(self, sql_server_catalog):
        """SQL Server mode calls _log_pipeline_run_sql_server."""
        with patch.object(sql_server_catalog, "_log_pipeline_run_sql_server") as mock_sql:
            sql_server_catalog.log_pipeline_run(_make_pipeline_run())
        mock_sql.assert_called_once()

    def test_retry_failure_logs_warning(self, catalog_manager):
        """Pipeline run write failure logs warning."""
        with patch("odibi.catalog.logger") as mock_logger:
            with patch.object(catalog_manager.engine, "write", side_effect=IOError("fail")):
                catalog_manager.log_pipeline_run(_make_pipeline_run())
        mock_logger.warning.assert_called_once()
        assert "Failed to log pipeline run" in mock_logger.warning.call_args[0][0]

    def test_optional_fields_are_none(self, catalog_manager):
        """log_pipeline_run handles optional fields being None."""
        run = _make_pipeline_run(
            owner=None,
            layer=None,
            error_summary=None,
            terminal_nodes=None,
            databricks_cluster_id=None,
            databricks_job_id=None,
            databricks_workspace_id=None,
            estimated_cost_usd=None,
            actual_cost_usd=None,
            cost_source=None,
        )
        catalog_manager.log_pipeline_run(run)
        df = _read_table(catalog_manager, "meta_pipeline_runs")
        assert len(df) == 1
        assert df.iloc[0]["owner"] is None

    def test_multiple_pipeline_runs(self, catalog_manager):
        """Multiple log_pipeline_run calls append separate rows."""
        catalog_manager.log_pipeline_run(_make_pipeline_run(run_id="r1"))
        catalog_manager.log_pipeline_run(_make_pipeline_run(run_id="r2"))
        df = _read_table(catalog_manager, "meta_pipeline_runs")
        assert len(df) == 2


# ===========================================================================
# _log_pipeline_run_sql_server()
# ===========================================================================


class TestLogPipelineRunSqlServer:
    """Tests for CatalogManager._log_pipeline_run_sql_server()."""

    def test_table_not_exists_raises(self, sql_server_catalog):
        """Raises NotImplementedError when meta_pipeline_runs doesn't exist."""
        with patch.object(sql_server_catalog, "_sql_server_table_exists", return_value=False):
            with pytest.raises(NotImplementedError, match="meta_pipeline_runs"):
                sql_server_catalog._log_pipeline_run_sql_server(_make_pipeline_run())

    def test_nan_rows_processed_becomes_none(self, sql_server_catalog):
        """NaN rows_processed is converted to None for SQL Server compatibility."""
        run = _make_pipeline_run(rows_processed=float("nan"))
        with patch.object(sql_server_catalog, "_sql_server_table_exists", return_value=True):
            sql_server_catalog._log_pipeline_run_sql_server(run)
        params = sql_server_catalog.connection.execute.call_args[0][1]
        assert params["rows_processed"] is None

    def test_execute_success(self, sql_server_catalog):
        """Successful MERGE statement execution."""
        with patch.object(sql_server_catalog, "_sql_server_table_exists", return_value=True):
            sql_server_catalog._log_pipeline_run_sql_server(_make_pipeline_run())
        sql_server_catalog.connection.execute.assert_called_once()
        sql_text = sql_server_catalog.connection.execute.call_args[0][0]
        assert "MERGE INTO" in sql_text

    def test_execute_failure_logs_warning(self, sql_server_catalog):
        """Execution failure logs a warning."""
        sql_server_catalog.connection.execute.side_effect = Exception("timeout")
        with patch("odibi.catalog.logger") as mock_logger:
            with patch.object(sql_server_catalog, "_sql_server_table_exists", return_value=True):
                sql_server_catalog._log_pipeline_run_sql_server(_make_pipeline_run())
        mock_logger.warning.assert_called_once()
        assert "Failed to log pipeline run to SQL Server" in mock_logger.warning.call_args[0][0]

    def test_normal_rows_processed_preserved(self, sql_server_catalog):
        """Normal int rows_processed is passed through unchanged."""
        run = _make_pipeline_run(rows_processed=5000)
        with patch.object(sql_server_catalog, "_sql_server_table_exists", return_value=True):
            sql_server_catalog._log_pipeline_run_sql_server(run)
        params = sql_server_catalog.connection.execute.call_args[0][1]
        assert params["rows_processed"] == 5000


# ===========================================================================
# _sql_server_table_exists()
# ===========================================================================


class TestSqlServerTableExists:
    """Tests for CatalogManager._sql_server_table_exists()."""

    def test_not_sql_server_mode_returns_false(self, catalog_manager):
        """Non-SQL-Server mode always returns False."""
        assert catalog_manager._sql_server_table_exists("meta_runs") is False

    def test_table_exists_returns_true(self, sql_server_catalog):
        """Returns True when INFORMATION_SCHEMA query finds a row."""
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (1,)
        sql_server_catalog.connection.execute.return_value = mock_result
        assert sql_server_catalog._sql_server_table_exists("meta_runs") is True

    def test_table_not_exists_returns_false(self, sql_server_catalog):
        """Returns False when INFORMATION_SCHEMA query finds no rows."""
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        sql_server_catalog.connection.execute.return_value = mock_result
        assert sql_server_catalog._sql_server_table_exists("meta_runs") is False

    def test_exception_returns_false(self, sql_server_catalog):
        """Returns False on any exception."""
        sql_server_catalog.connection.execute.side_effect = Exception("conn lost")
        assert sql_server_catalog._sql_server_table_exists("meta_runs") is False

    def test_uses_schema_from_config(self, sql_server_catalog):
        """Uses schema_name from config in query."""
        sql_server_catalog.config.schema_name = "my_schema"
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        sql_server_catalog.connection.execute.return_value = mock_result
        sql_server_catalog._sql_server_table_exists("meta_runs")
        params = sql_server_catalog.connection.execute.call_args[0][1]
        assert params["schema"] == "my_schema"


# ===========================================================================
# log_node_runs_batch()
# ===========================================================================


class TestLogNodeRunsBatch:
    """Tests for CatalogManager.log_node_runs_batch()."""

    def test_empty_results_no_op(self, catalog_manager):
        """Empty list is a silent no-op."""
        catalog_manager.log_node_runs_batch([])

    def test_pandas_engine_appends_records(self, catalog_manager):
        """Batch node run logging writes rows to meta_node_runs."""
        results = [
            _make_node_result(node_id="n1", node_name="load_a"),
            _make_node_result(node_id="n2", node_name="load_b"),
        ]
        catalog_manager.log_node_runs_batch(results)
        df = _read_table(catalog_manager, "meta_node_runs")
        assert len(df) == 2
        assert set(df["node_name"]) == {"load_a", "load_b"}
        assert all(df["project"] == "test_project")

    def test_no_engine_no_op(self, bare_catalog):
        """No engine → no-op."""
        bare_catalog.log_node_runs_batch([_make_node_result()])

    def test_sql_server_mode_delegates(self, sql_server_catalog):
        """SQL Server mode calls _log_node_runs_batch_sql_server."""
        with patch.object(sql_server_catalog, "_log_node_runs_batch_sql_server") as mock_sql:
            sql_server_catalog.log_node_runs_batch([_make_node_result()])
        mock_sql.assert_called_once()

    def test_retry_failure_logs_warning(self, catalog_manager):
        """Write failure logs warning."""
        with patch("odibi.catalog.logger") as mock_logger:
            with patch.object(catalog_manager.engine, "write", side_effect=IOError("fail")):
                catalog_manager.log_node_runs_batch([_make_node_result()])
        mock_logger.warning.assert_called_once()
        assert "Failed to batch log node runs" in mock_logger.warning.call_args[0][0]

    def test_optional_fields_default(self, catalog_manager):
        """Missing optional fields use None defaults."""
        result = {
            "run_id": "r1",
            "node_id": "n1",
            "pipeline_name": "pipe",
            "node_name": "node",
        }
        catalog_manager.log_node_runs_batch([result])
        df = _read_table(catalog_manager, "meta_node_runs")
        assert len(df) == 1
        assert df.iloc[0]["status"] is None


# ===========================================================================
# _log_node_runs_batch_sql_server()
# ===========================================================================


class TestLogNodeRunsBatchSqlServer:
    """Tests for CatalogManager._log_node_runs_batch_sql_server()."""

    def test_table_not_exists_raises(self, sql_server_catalog):
        """Raises NotImplementedError when table doesn't exist."""
        with patch.object(sql_server_catalog, "_sql_server_table_exists", return_value=False):
            with pytest.raises(NotImplementedError, match="meta_node_runs"):
                sql_server_catalog._log_node_runs_batch_sql_server([_make_node_result()])

    def test_nan_rows_processed_becomes_none(self, sql_server_catalog):
        """NaN rows_processed is converted to None."""
        result = _make_node_result(rows_processed=float("nan"))
        with patch.object(sql_server_catalog, "_sql_server_table_exists", return_value=True):
            sql_server_catalog._log_node_runs_batch_sql_server([result])
        params = sql_server_catalog.connection.execute.call_args[0][1]
        assert params["rows_processed"] is None

    def test_execute_success(self, sql_server_catalog):
        """Successful execution for multiple results."""
        results = [
            _make_node_result(node_id="n1"),
            _make_node_result(node_id="n2"),
        ]
        with patch.object(sql_server_catalog, "_sql_server_table_exists", return_value=True):
            sql_server_catalog._log_node_runs_batch_sql_server(results)
        assert sql_server_catalog.connection.execute.call_count == 2
        sql_text = sql_server_catalog.connection.execute.call_args[0][0]
        assert "MERGE INTO" in sql_text

    def test_execute_failure_logs_warning(self, sql_server_catalog):
        """Execution failure logs a warning."""
        sql_server_catalog.connection.execute.side_effect = Exception("err")
        with patch("odibi.catalog.logger") as mock_logger:
            with patch.object(sql_server_catalog, "_sql_server_table_exists", return_value=True):
                sql_server_catalog._log_node_runs_batch_sql_server([_make_node_result()])
        mock_logger.warning.assert_called_once()
        assert "Failed to batch log node runs" in mock_logger.warning.call_args[0][0]

    def test_normal_rows_processed_preserved(self, sql_server_catalog):
        """Normal int rows_processed passes through unchanged."""
        result = _make_node_result(rows_processed=999)
        with patch.object(sql_server_catalog, "_sql_server_table_exists", return_value=True):
            sql_server_catalog._log_node_runs_batch_sql_server([result])
        params = sql_server_catalog.connection.execute.call_args[0][1]
        assert params["rows_processed"] == 999


# ===========================================================================
# log_failure()
# ===========================================================================


class TestLogFailure:
    """Tests for CatalogManager.log_failure()."""

    def test_pandas_engine_appends_record(self, catalog_manager):
        """log_failure writes a row to meta_failures."""
        catalog_manager.log_failure(
            failure_id="f-001",
            run_id="run-001",
            pipeline_name="etl_orders",
            node_name="load_orders",
            error_type="ValueError",
            error_message="Bad data in column X",
            stack_trace="Traceback...",
        )
        df = _read_table(catalog_manager, "meta_failures")
        assert len(df) == 1
        assert df.iloc[0]["failure_id"] == "f-001"
        assert df.iloc[0]["error_type"] == "ValueError"
        assert df.iloc[0]["project"] == "test_project"

    def test_truncates_long_error_message(self, catalog_manager):
        """Error messages longer than 1000 chars are truncated."""
        long_msg = "X" * 2000
        catalog_manager.log_failure("f-001", "r1", "pipe", "node", "Error", long_msg, None)
        df = _read_table(catalog_manager, "meta_failures")
        assert len(df.iloc[0]["error_message"]) == 1000

    def test_truncates_long_stack_trace(self, catalog_manager):
        """Stack traces longer than 2000 chars are truncated."""
        long_trace = "Y" * 5000
        catalog_manager.log_failure("f-001", "r1", "pipe", "node", "Error", "msg", long_trace)
        df = _read_table(catalog_manager, "meta_failures")
        assert len(df.iloc[0]["stack_trace"]) == 2000

    def test_none_message_and_trace(self, catalog_manager):
        """log_failure handles None error_message and stack_trace."""
        catalog_manager.log_failure("f-001", "r1", "pipe", "node", "Error", None, None)
        df = _read_table(catalog_manager, "meta_failures")
        assert len(df) == 1
        assert df.iloc[0]["error_message"] is None
        assert df.iloc[0]["stack_trace"] is None

    def test_no_engine_no_op(self, bare_catalog):
        """log_failure with no engine is a no-op."""
        bare_catalog.log_failure("f1", "r1", "p", "n", "E", "msg")

    def test_sql_server_mode_delegates(self, sql_server_catalog):
        """SQL Server mode calls _log_failure_sql_server."""
        with patch.object(sql_server_catalog, "_log_failure_sql_server") as mock_sql:
            sql_server_catalog.log_failure("f1", "r1", "p", "n", "E", "msg", "trace")
        mock_sql.assert_called_once()

    def test_retry_failure_logs_warning(self, catalog_manager):
        """Write failure logs warning."""
        with patch("odibi.catalog.logger") as mock_logger:
            with patch.object(catalog_manager.engine, "write", side_effect=IOError("fail")):
                catalog_manager.log_failure("f1", "r1", "p", "n", "E", "msg")
        mock_logger.warning.assert_called_once()
        assert "Failed to log failure" in mock_logger.warning.call_args[0][0]

    def test_error_code_is_none(self, catalog_manager):
        """error_code field is always None (future taxonomy)."""
        catalog_manager.log_failure("f-001", "r1", "pipe", "node", "Error", "msg")
        df = _read_table(catalog_manager, "meta_failures")
        assert df.iloc[0]["error_code"] is None

    def test_multiple_failures_append(self, catalog_manager):
        """Multiple failures are appended as separate rows."""
        catalog_manager.log_failure("f1", "r1", "pipe", "n1", "E1", "msg1")
        catalog_manager.log_failure("f2", "r1", "pipe", "n2", "E2", "msg2")
        df = _read_table(catalog_manager, "meta_failures")
        assert len(df) == 2


# ===========================================================================
# _log_failure_sql_server()
# ===========================================================================


class TestLogFailureSqlServer:
    """Tests for CatalogManager._log_failure_sql_server()."""

    def test_table_not_exists_raises(self, sql_server_catalog):
        """Raises NotImplementedError when table doesn't exist."""
        with patch.object(sql_server_catalog, "_sql_server_table_exists", return_value=False):
            with pytest.raises(NotImplementedError, match="meta_failures"):
                sql_server_catalog._log_failure_sql_server(
                    "f1", "r1", "pipe", "node", "E", "msg", None
                )

    def test_execute_success(self, sql_server_catalog):
        """Successful INSERT execution."""
        with patch.object(sql_server_catalog, "_sql_server_table_exists", return_value=True):
            sql_server_catalog._log_failure_sql_server(
                "f1", "r1", "pipe", "node", "ValueError", "bad data", "Traceback..."
            )
        sql_server_catalog.connection.execute.assert_called_once()
        sql_text = sql_server_catalog.connection.execute.call_args[0][0]
        assert "INSERT INTO" in sql_text

    def test_execute_failure_logs_warning(self, sql_server_catalog):
        """Execution failure logs warning."""
        sql_server_catalog.connection.execute.side_effect = Exception("err")
        with patch("odibi.catalog.logger") as mock_logger:
            with patch.object(sql_server_catalog, "_sql_server_table_exists", return_value=True):
                sql_server_catalog._log_failure_sql_server(
                    "f1", "r1", "pipe", "node", "E", "msg", None
                )
        mock_logger.warning.assert_called_once()
        assert "Failed to log failure to SQL Server" in mock_logger.warning.call_args[0][0]

    def test_truncation_in_sql_server(self, sql_server_catalog):
        """SQL Server log_failure truncates long messages."""
        long_msg = "A" * 2000
        long_trace = "B" * 5000
        with patch.object(sql_server_catalog, "_sql_server_table_exists", return_value=True):
            sql_server_catalog._log_failure_sql_server(
                "f1", "r1", "pipe", "node", "E", long_msg, long_trace
            )
        params = sql_server_catalog.connection.execute.call_args[0][1]
        assert len(params["error_message"]) == 1000
        assert len(params["stack_trace"]) == 2000

    def test_none_message_and_trace_sql_server(self, sql_server_catalog):
        """SQL Server handles None error_message and stack_trace."""
        with patch.object(sql_server_catalog, "_sql_server_table_exists", return_value=True):
            sql_server_catalog._log_failure_sql_server("f1", "r1", "pipe", "node", "E", None, None)
        params = sql_server_catalog.connection.execute.call_args[0][1]
        assert params["error_message"] is None
        assert params["stack_trace"] is None
