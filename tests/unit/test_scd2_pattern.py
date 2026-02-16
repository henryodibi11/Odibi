"""Unit tests for SCD2Pattern."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.config import NodeConfig
from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.patterns.scd2 import SCD2Pattern


def create_pandas_context(df, engine=None):
    """Helper to create a PandasContext with a DataFrame."""
    pandas_context = PandasContext()
    ctx = EngineContext(
        context=pandas_context,
        df=df,
        engine_type=EngineType.PANDAS,
        engine=engine,
    )
    return ctx


@pytest.fixture
def mock_engine():
    """Create a mock engine."""
    engine = MagicMock()
    engine.connections = {}
    return engine


@pytest.fixture
def mock_config():
    """Create a basic mock NodeConfig."""
    config = MagicMock(spec=NodeConfig)
    config.name = "test_node"
    config.params = {}
    return config


class TestSCD2PatternValidation:
    """Test SCD2Pattern validation."""

    def test_validate_requires_keys(self, mock_engine, mock_config):
        """Test that keys are required."""
        mock_config.params = {"target": "dim_customer"}

        pattern = SCD2Pattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="keys"):
            pattern.validate()

    def test_validate_requires_target(self, mock_engine, mock_config):
        """Test that target is required."""
        mock_config.params = {"keys": ["customer_id"]}

        pattern = SCD2Pattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="target"):
            pattern.validate()

    def test_validate_empty_keys_fails(self, mock_engine, mock_config):
        """Test that empty keys list fails."""
        mock_config.params = {"keys": [], "target": "dim_customer"}

        pattern = SCD2Pattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="keys"):
            pattern.validate()

    def test_validate_success_with_required_params(self, mock_engine, mock_config):
        """Test validation passes with required params."""
        mock_config.params = {
            "keys": ["customer_id"],
            "target": "dim_customer",
        }

        pattern = SCD2Pattern(mock_engine, mock_config)
        pattern.validate()

    def test_validate_success_with_all_params(self, mock_engine, mock_config):
        """Test validation passes with all params."""
        mock_config.params = {
            "keys": ["customer_id"],
            "target": "dim_customer",
            "valid_from_col": "start_date",
            "valid_to_col": "end_date",
            "is_current_col": "current_flag",
            "track_cols": ["name", "email"],
        }

        pattern = SCD2Pattern(mock_engine, mock_config)
        pattern.validate()


class TestSCD2PatternExecution:
    """Test SCD2Pattern execution."""

    @patch("odibi.patterns.scd2.scd2")
    def test_execute_calls_scd2_transformer(self, mock_scd2, mock_engine, mock_config):
        """Test that execute calls the scd2 transformer."""
        mock_result_ctx = MagicMock()
        mock_result_ctx.df = pd.DataFrame({"customer_id": [1]})
        mock_scd2.return_value = mock_result_ctx

        mock_config.params = {
            "keys": ["customer_id"],
            "target": "dim_customer",
            "track_cols": ["name"],
            "effective_time_col": "updated_at",
        }

        source_df = pd.DataFrame(
            {
                "customer_id": [1, 2],
                "name": ["Alice", "Bob"],
                "updated_at": ["2024-01-01", "2024-01-02"],
            }
        )
        context = create_pandas_context(source_df, mock_engine)

        pattern = SCD2Pattern(mock_engine, mock_config)
        result = pattern.execute(context)

        mock_scd2.assert_called_once()
        assert result is mock_result_ctx.df

    @patch("odibi.patterns.scd2.scd2")
    def test_execute_uses_default_column_names(self, mock_scd2, mock_engine, mock_config):
        """Test that default column names are used for valid_to and is_current."""
        mock_result_ctx = MagicMock()
        mock_result_ctx.df = pd.DataFrame({"customer_id": [1]})
        mock_scd2.return_value = mock_result_ctx

        mock_config.params = {
            "keys": ["customer_id"],
            "target": "dim_customer",
            "track_cols": ["name"],
            "effective_time_col": "updated_at",
        }

        source_df = pd.DataFrame(
            {
                "customer_id": [1, 2],
                "name": ["Alice", "Bob"],
                "updated_at": ["2024-01-01", "2024-01-02"],
            }
        )
        context = create_pandas_context(source_df, mock_engine)

        pattern = SCD2Pattern(mock_engine, mock_config)
        pattern.execute(context)

        mock_scd2.assert_called_once()
        call_args = mock_scd2.call_args
        scd_params = call_args[0][1]
        assert scd_params.end_time_col == "valid_to"
        assert scd_params.current_flag_col == "is_current"

    @patch("odibi.patterns.scd2.scd2")
    def test_execute_filters_valid_params(self, mock_scd2, mock_engine, mock_config):
        """Test that only valid SCD2Params are passed."""
        mock_result_ctx = MagicMock()
        mock_result_ctx.df = pd.DataFrame({"customer_id": [1]})
        mock_scd2.return_value = mock_result_ctx

        mock_config.params = {
            "keys": ["customer_id"],
            "target": "dim_customer",
            "track_cols": ["name"],
            "effective_time_col": "updated_at",
            "invalid_param": "should_be_filtered",
        }

        source_df = pd.DataFrame(
            {
                "customer_id": [1, 2],
                "name": ["Alice", "Bob"],
                "updated_at": ["2024-01-01", "2024-01-02"],
            }
        )
        context = create_pandas_context(source_df, mock_engine)

        pattern = SCD2Pattern(mock_engine, mock_config)
        pattern.execute(context)

        call_args = mock_scd2.call_args
        scd_params = call_args[0][1]
        assert not hasattr(scd_params, "invalid_param")

    @patch("odibi.patterns.scd2.scd2")
    def test_execute_handles_spark_context(self, mock_scd2, mock_engine, mock_config):
        """Test execution with Spark context type."""
        mock_result_ctx = MagicMock()
        mock_result_df = MagicMock()
        mock_result_df.count.return_value = 5
        mock_result_ctx.df = mock_result_df
        mock_scd2.return_value = mock_result_ctx

        mock_config.params = {
            "keys": ["customer_id"],
            "target": "dim_customer",
            "track_cols": ["name"],
            "effective_time_col": "updated_at",
        }

        mock_spark_df = MagicMock()
        mock_spark_df.count.return_value = 10

        pandas_context = PandasContext()
        context = EngineContext(
            context=pandas_context,
            df=mock_spark_df,
            engine_type=EngineType.SPARK,
            engine=mock_engine,
        )

        pattern = SCD2Pattern(mock_engine, mock_config)
        result = pattern.execute(context)

        mock_spark_df.count.assert_called()
        assert result is mock_result_df

    @patch("odibi.patterns.scd2.scd2")
    def test_execute_handles_count_failure(self, mock_scd2, mock_engine, mock_config):
        """Test execution continues even if count fails."""
        mock_result_ctx = MagicMock()
        mock_result_ctx.df = pd.DataFrame({"customer_id": [1]})
        mock_scd2.return_value = mock_result_ctx

        mock_config.params = {
            "keys": ["customer_id"],
            "target": "dim_customer",
            "track_cols": ["name"],
            "effective_time_col": "updated_at",
        }

        source_df = pd.DataFrame(
            {
                "customer_id": [1, 2],
                "name": ["Alice", "Bob"],
                "updated_at": ["2024-01-01", "2024-01-02"],
            }
        )
        context = create_pandas_context(source_df, mock_engine)
        context._df = MagicMock()
        context._df.__len__ = MagicMock(side_effect=Exception("Count failed"))

        pattern = SCD2Pattern(mock_engine, mock_config)
        pattern.execute(context)

        mock_scd2.assert_called_once()


class TestSCD2PatternErrorHandling:
    """Test SCD2Pattern error handling."""

    def test_execute_raises_on_missing_required_params(self, mock_engine, mock_config):
        """Test that missing required params raise ValueError."""
        mock_config.params = {
            "keys": ["customer_id"],
            "target": "dim_customer",
        }

        source_df = pd.DataFrame({"customer_id": [1, 2], "name": ["Alice", "Bob"]})
        context = create_pandas_context(source_df, mock_engine)

        pattern = SCD2Pattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="Invalid SCD2 parameters"):
            pattern.execute(context)

    @patch("odibi.patterns.scd2.scd2")
    def test_execute_raises_on_scd2_failure(self, mock_scd2, mock_engine, mock_config):
        """Test that scd2 transformer failures are re-raised."""
        mock_scd2.side_effect = ValueError("SCD2 failed")

        mock_config.params = {
            "keys": ["customer_id"],
            "target": "dim_customer",
            "track_cols": ["name"],
            "effective_time_col": "updated_at",
        }

        source_df = pd.DataFrame(
            {
                "customer_id": [1, 2],
                "name": ["Alice", "Bob"],
                "updated_at": ["2024-01-01", "2024-01-02"],
            }
        )
        context = create_pandas_context(source_df, mock_engine)

        pattern = SCD2Pattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="SCD2 failed"):
            pattern.execute(context)

    @patch("odibi.patterns.scd2.scd2")
    @patch("odibi.patterns.scd2.get_logging_context")
    def test_execute_logs_error_on_failure(self, mock_get_ctx, mock_scd2, mock_engine, mock_config):
        """Test that errors are logged before re-raising."""
        mock_ctx = MagicMock()
        mock_get_ctx.return_value = mock_ctx
        mock_scd2.side_effect = ValueError("SCD2 failed")

        mock_config.params = {
            "keys": ["customer_id"],
            "target": "dim_customer",
            "track_cols": ["name"],
            "effective_time_col": "updated_at",
        }

        source_df = pd.DataFrame(
            {
                "customer_id": [1, 2],
                "name": ["Alice", "Bob"],
                "updated_at": ["2024-01-01", "2024-01-02"],
            }
        )
        context = create_pandas_context(source_df, mock_engine)

        pattern = SCD2Pattern(mock_engine, mock_config)

        with pytest.raises(ValueError):
            pattern.execute(context)

        mock_ctx.error.assert_called()


class TestSCD2PatternLogging:
    """Test SCD2Pattern logging."""

    @patch("odibi.patterns.scd2.scd2")
    @patch("odibi.patterns.scd2.get_logging_context")
    def test_execute_logs_start_and_complete(
        self, mock_get_ctx, mock_scd2, mock_engine, mock_config
    ):
        """Test that execution logs start and completion."""
        mock_ctx = MagicMock()
        mock_get_ctx.return_value = mock_ctx

        mock_result_ctx = MagicMock()
        mock_result_ctx.df = pd.DataFrame({"customer_id": [1]})
        mock_scd2.return_value = mock_result_ctx

        mock_config.params = {
            "keys": ["customer_id"],
            "target": "dim_customer",
            "track_cols": ["name"],
            "effective_time_col": "updated_at",
        }

        source_df = pd.DataFrame(
            {
                "customer_id": [1, 2],
                "name": ["Alice", "Bob"],
                "updated_at": ["2024-01-01", "2024-01-02"],
            }
        )
        context = create_pandas_context(source_df, mock_engine)

        pattern = SCD2Pattern(mock_engine, mock_config)
        pattern.execute(context)

        assert mock_ctx.debug.call_count >= 1
        mock_ctx.info.assert_called_once()

        info_call = mock_ctx.info.call_args
        assert "elapsed_ms" in info_call[1]
        assert info_call[1]["pattern"] == "SCD2Pattern"

    @patch("odibi.patterns.scd2.scd2")
    @patch("odibi.patterns.scd2.get_logging_context")
    def test_execute_logs_row_counts(self, mock_get_ctx, mock_scd2, mock_engine, mock_config):
        """Test that row counts are logged."""
        mock_ctx = MagicMock()
        mock_get_ctx.return_value = mock_ctx

        mock_result_ctx = MagicMock()
        result_df = pd.DataFrame({"customer_id": [1, 2, 3]})
        mock_result_ctx.df = result_df
        mock_scd2.return_value = mock_result_ctx

        mock_config.params = {
            "keys": ["customer_id"],
            "target": "dim_customer",
            "track_cols": ["name"],
            "effective_time_col": "updated_at",
        }

        source_df = pd.DataFrame(
            {
                "customer_id": [1, 2],
                "name": ["Alice", "Bob"],
                "updated_at": ["2024-01-01", "2024-01-02"],
            }
        )
        context = create_pandas_context(source_df, mock_engine)

        pattern = SCD2Pattern(mock_engine, mock_config)
        pattern.execute(context)

        info_call = mock_ctx.info.call_args
        assert info_call[1]["source_rows"] == 2
        assert info_call[1]["result_rows"] == 3


class TestSCD2PatternRegistration:
    """Test SCD2Pattern registration."""

    def test_pattern_registered(self):
        """Test that SCD2Pattern is registered in patterns registry."""
        from odibi.patterns import get_pattern_class

        pattern_class = get_pattern_class("scd2")
        assert pattern_class == SCD2Pattern


class TestSCD2PatternConfiguration:
    """Test SCD2Pattern configuration options."""

    @patch("odibi.patterns.scd2.scd2")
    def test_custom_column_names(self, mock_scd2, mock_engine, mock_config):
        """Test custom column names are passed through."""
        mock_result_ctx = MagicMock()
        mock_result_ctx.df = pd.DataFrame({"customer_id": [1]})
        mock_scd2.return_value = mock_result_ctx

        mock_config.params = {
            "keys": ["customer_id"],
            "target": "dim_customer",
            "track_cols": ["name"],
            "effective_time_col": "effective_date",
            "end_time_col": "expiry_date",
            "current_flag_col": "active_flag",
        }

        source_df = pd.DataFrame(
            {
                "customer_id": [1, 2],
                "name": ["Alice", "Bob"],
                "effective_date": ["2024-01-01", "2024-01-02"],
            }
        )
        context = create_pandas_context(source_df, mock_engine)

        pattern = SCD2Pattern(mock_engine, mock_config)
        pattern.execute(context)

        mock_scd2.assert_called_once()
        call_args = mock_scd2.call_args
        scd_params = call_args[0][1]
        assert scd_params.end_time_col == "expiry_date"
        assert scd_params.current_flag_col == "active_flag"

    @patch("odibi.patterns.scd2.scd2")
    def test_track_cols_passed(self, mock_scd2, mock_engine, mock_config):
        """Test track_cols are passed through."""
        mock_result_ctx = MagicMock()
        mock_result_ctx.df = pd.DataFrame({"customer_id": [1]})
        mock_scd2.return_value = mock_result_ctx

        mock_config.params = {
            "keys": ["customer_id"],
            "target": "dim_customer",
            "track_cols": ["name", "email", "address"],
            "effective_time_col": "updated_at",
        }

        source_df = pd.DataFrame(
            {
                "customer_id": [1, 2],
                "name": ["Alice", "Bob"],
                "email": ["a@example.com", "b@example.com"],
                "address": ["NYC", "LA"],
                "updated_at": ["2024-01-01", "2024-01-02"],
            }
        )
        context = create_pandas_context(source_df, mock_engine)

        pattern = SCD2Pattern(mock_engine, mock_config)
        pattern.execute(context)

        mock_scd2.assert_called_once()
        call_args = mock_scd2.call_args
        scd_params = call_args[0][1]
        assert scd_params.track_cols == ["name", "email", "address"]
