"""Unit tests for NodeExecutor incremental SQL filter generation.

Covers issue #283 — lines 844-1116 of node.py.
Tests _quote_sql_column, _get_date_expr, and _generate_incremental_sql_filter.
"""

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from odibi.config import NodeConfig
from odibi.node import NodeExecutor


@pytest.fixture
def mock_context():
    return MagicMock()


@pytest.fixture
def mock_engine():
    engine = MagicMock()
    engine.name = "pandas"
    engine.table_exists.return_value = True
    return engine


@pytest.fixture
def connections():
    return {"src": MagicMock(), "dst": MagicMock()}


def _make_executor(mock_context, mock_engine, connections, **kwargs):
    return NodeExecutor(mock_context, mock_engine, connections, **kwargs)


def _make_config(incremental_dict, name="test_node", read_format="csv"):
    return NodeConfig(
        name=name,
        read={
            "connection": "src",
            "format": read_format,
            "path": "input.csv",
            "incremental": incremental_dict,
        },
        write={"connection": "dst", "format": "csv", "path": "output.csv"},
    )


# ============================================================
# _quote_sql_column
# ============================================================


class TestQuoteSqlColumn:
    """Test SQL column quoting per engine dialect."""

    def test_sql_server_format_uses_brackets(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        result = executor._quote_sql_column("updated_at", "sql_server", "pandas")
        assert result == "[updated_at]"

    def test_azure_sql_format_uses_brackets(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        result = executor._quote_sql_column("col", "azure_sql", "pandas")
        assert result == "[col]"

    def test_mssql_format_uses_brackets(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        result = executor._quote_sql_column("col", "mssql", "pandas")
        assert result == "[col]"

    def test_cluster_engine_uses_backticks(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        # format="delta" + engine contains "spark" → backticks
        result = executor._quote_sql_column("col", "delta", "SparkEngine")
        assert result == "`col`"

    def test_pandas_engine_uses_double_quotes(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        result = executor._quote_sql_column("col", "csv", "PandasEngine")
        assert result == '"col"'

    def test_duckdb_engine_uses_double_quotes(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        result = executor._quote_sql_column("col", "parquet", "DuckDBEngine")
        assert result == '"col"'

    def test_polars_engine_uses_double_quotes(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        result = executor._quote_sql_column("col", "parquet", "PolarsEngine")
        assert result == '"col"'

    def test_unknown_engine_returns_bare_column(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        result = executor._quote_sql_column("col", "csv", None)
        assert result == "col"


# ============================================================
# _get_date_expr
# ============================================================


class TestGetDateExpr:
    """Test SQL date expression generation for various formats."""

    def _cutoff(self):
        return datetime(2024, 6, 15, 10, 30, 45)

    def test_default_format(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        col_expr, cutoff_expr = executor._get_date_expr('"col"', self._cutoff(), None)
        assert col_expr == '"col"'
        assert cutoff_expr == "'2024-06-15 10:30:45'"

    def test_oracle_format(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        col_expr, cutoff_expr = executor._get_date_expr('"col"', self._cutoff(), "oracle")
        assert "TO_TIMESTAMP" in col_expr
        assert "DD-MON-RR" in col_expr
        assert "TO_TIMESTAMP" in cutoff_expr

    def test_oracle_sqlserver_format(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        col_expr, cutoff_expr = executor._get_date_expr('"col"', self._cutoff(), "oracle_sqlserver")
        assert "TRY_CAST" in col_expr
        assert "SUBSTRING" in col_expr
        assert cutoff_expr == "'2024-06-15 10:30:45'"

    def test_sql_server_format(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        col_expr, cutoff_expr = executor._get_date_expr("[col]", self._cutoff(), "sql_server")
        assert "CONVERT(DATETIME, [col], 120)" == col_expr
        assert cutoff_expr == "'2024-06-15 10:30:45'"

    def test_us_format(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        col_expr, cutoff_expr = executor._get_date_expr('"col"', self._cutoff(), "us")
        assert "MM/DD/YYYY" in col_expr
        assert "06/15/2024" in cutoff_expr

    def test_eu_format(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        col_expr, cutoff_expr = executor._get_date_expr('"col"', self._cutoff(), "eu")
        assert "DD/MM/YYYY" in col_expr
        assert "15/06/2024" in cutoff_expr

    def test_iso_format(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        col_expr, cutoff_expr = executor._get_date_expr('"col"', self._cutoff(), "iso")
        assert "TO_TIMESTAMP" in col_expr
        assert "2024-06-15T10:30:45" in cutoff_expr


# ============================================================
# _generate_incremental_sql_filter — rolling_window
# ============================================================


class TestIncrementalRollingWindow:
    """Test rolling_window mode filter generation."""

    def test_rolling_window_day(self, mock_context, mock_engine, connections):
        mock_engine.table_exists.return_value = True
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "mode": "rolling_window",
                "column": "updated_at",
                "lookback": 3,
                "unit": "day",
            }
        )

        result = executor._generate_incremental_sql_filter(config.read.incremental, config)

        assert result is not None
        assert ">=" in result
        assert "updated_at" in result

    def test_rolling_window_hour(self, mock_context, mock_engine, connections):
        mock_engine.table_exists.return_value = True
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "mode": "rolling_window",
                "column": "ts",
                "lookback": 6,
                "unit": "hour",
            }
        )

        result = executor._generate_incremental_sql_filter(config.read.incremental, config)
        assert result is not None
        assert "ts" in result

    def test_rolling_window_month(self, mock_context, mock_engine, connections):
        mock_engine.table_exists.return_value = True
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "mode": "rolling_window",
                "column": "created",
                "lookback": 2,
                "unit": "month",
            }
        )

        result = executor._generate_incremental_sql_filter(config.read.incremental, config)
        assert result is not None

    def test_rolling_window_year(self, mock_context, mock_engine, connections):
        mock_engine.table_exists.return_value = True
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "mode": "rolling_window",
                "column": "yr",
                "lookback": 1,
                "unit": "year",
            }
        )

        result = executor._generate_incremental_sql_filter(config.read.incremental, config)
        assert result is not None

    def test_rolling_window_with_fallback_column(self, mock_context, mock_engine, connections):
        mock_engine.table_exists.return_value = True
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "mode": "rolling_window",
                "column": "updated_at",
                "fallback_column": "created_at",
                "lookback": 3,
                "unit": "day",
            }
        )

        result = executor._generate_incremental_sql_filter(config.read.incremental, config)
        assert result is not None
        assert "COALESCE" in result
        assert "updated_at" in result
        assert "created_at" in result

    def test_rolling_window_with_date_format(self, mock_context, mock_engine, connections):
        mock_engine.table_exists.return_value = True
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "mode": "rolling_window",
                "column": "event_start",
                "lookback": 3,
                "unit": "day",
                "date_format": "sql_server",
            }
        )

        result = executor._generate_incremental_sql_filter(config.read.incremental, config)
        assert result is not None
        assert "CONVERT" in result

    def test_first_run_returns_none(self, mock_context, mock_engine, connections):
        """First run (target doesn't exist) should return None for full load."""
        mock_engine.table_exists.return_value = False
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "mode": "rolling_window",
                "column": "updated_at",
                "lookback": 3,
                "unit": "day",
            }
        )

        result = executor._generate_incremental_sql_filter(config.read.incremental, config)
        assert result is None


# ============================================================
# _generate_incremental_sql_filter — stateful
# ============================================================


class TestIncrementalStateful:
    """Test stateful mode filter generation."""

    def test_stateful_with_string_hwm(self, mock_context, mock_engine, connections):
        mock_engine.table_exists.return_value = True
        state_mgr = MagicMock()
        state_mgr.get_hwm.return_value = "2024-06-15 10:00:00"
        executor = _make_executor(mock_context, mock_engine, connections, state_manager=state_mgr)
        config = _make_config(
            {
                "mode": "stateful",
                "column": "updated_at",
            }
        )

        result = executor._generate_incremental_sql_filter(config.read.incremental, config)

        assert result is not None
        assert ">" in result
        assert "updated_at" in result
        assert "2024-06-15" in result

    def test_stateful_with_iso_hwm_formats_correctly(self, mock_context, mock_engine, connections):
        """ISO format HWM (with T) should be converted to space-separated."""
        mock_engine.table_exists.return_value = True
        state_mgr = MagicMock()
        state_mgr.get_hwm.return_value = "2024-06-15T10:00:00"
        executor = _make_executor(mock_context, mock_engine, connections, state_manager=state_mgr)
        config = _make_config(
            {
                "mode": "stateful",
                "column": "updated_at",
            }
        )

        result = executor._generate_incremental_sql_filter(config.read.incremental, config)

        assert result is not None
        assert "T" not in result.split("'")[1]

    def test_stateful_with_watermark_lag(self, mock_context, mock_engine, connections):
        mock_engine.table_exists.return_value = True
        state_mgr = MagicMock()
        state_mgr.get_hwm.return_value = "2024-06-15T12:00:00"
        executor = _make_executor(mock_context, mock_engine, connections, state_manager=state_mgr)
        config = _make_config(
            {
                "mode": "stateful",
                "column": "updated_at",
                "watermark_lag": "2h",
            }
        )

        result = executor._generate_incremental_sql_filter(config.read.incremental, config)

        assert result is not None
        # HWM was 12:00, lag is 2h, so filter should use 10:00
        assert "10:00:00" in result

    def test_stateful_no_hwm_returns_none(self, mock_context, mock_engine, connections):
        mock_engine.table_exists.return_value = True
        state_mgr = MagicMock()
        state_mgr.get_hwm.return_value = None
        executor = _make_executor(mock_context, mock_engine, connections, state_manager=state_mgr)
        config = _make_config(
            {
                "mode": "stateful",
                "column": "updated_at",
            }
        )

        result = executor._generate_incremental_sql_filter(config.read.incremental, config)
        assert result is None

    def test_stateful_no_state_manager_returns_none(self, mock_context, mock_engine, connections):
        mock_engine.table_exists.return_value = True
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "mode": "stateful",
                "column": "updated_at",
            }
        )

        result = executor._generate_incremental_sql_filter(config.read.incremental, config)
        assert result is None

    def test_stateful_with_fallback_column(self, mock_context, mock_engine, connections):
        mock_engine.table_exists.return_value = True
        state_mgr = MagicMock()
        state_mgr.get_hwm.return_value = "2024-01-01 00:00:00"
        executor = _make_executor(mock_context, mock_engine, connections, state_manager=state_mgr)
        config = _make_config(
            {
                "mode": "stateful",
                "column": "updated_at",
                "fallback_column": "created_at",
            }
        )

        result = executor._generate_incremental_sql_filter(config.read.incremental, config)

        assert result is not None
        assert "COALESCE" in result
        assert "created_at" in result

    def test_stateful_with_custom_state_key(self, mock_context, mock_engine, connections):
        mock_engine.table_exists.return_value = True
        state_mgr = MagicMock()
        state_mgr.get_hwm.return_value = "100"
        executor = _make_executor(mock_context, mock_engine, connections, state_manager=state_mgr)
        config = _make_config(
            {
                "mode": "stateful",
                "column": "id",
                "state_key": "my_custom_key",
            }
        )

        result = executor._generate_incremental_sql_filter(config.read.incremental, config)

        assert result is not None
        state_mgr.get_hwm.assert_called_with("my_custom_key")

    def test_stateful_integer_hwm(self, mock_context, mock_engine, connections):
        """Non-datetime HWM (e.g., integer ID) should work."""
        mock_engine.table_exists.return_value = True
        state_mgr = MagicMock()
        state_mgr.get_hwm.return_value = 42
        executor = _make_executor(mock_context, mock_engine, connections, state_manager=state_mgr)
        config = _make_config(
            {
                "mode": "stateful",
                "column": "id",
            }
        )

        result = executor._generate_incremental_sql_filter(config.read.incremental, config)

        assert result is not None
        assert "42" in result
