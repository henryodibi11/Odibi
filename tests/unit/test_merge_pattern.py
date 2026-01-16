"""Unit tests for MergePattern."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.config import NodeConfig
from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.patterns.merge import MergePattern


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
    config.params = {}
    return config


class TestMergePatternValidation:
    """Test MergePattern validation."""

    def test_validate_requires_target(self, mock_engine, mock_config):
        """Test that target is required."""
        mock_config.params = {"keys": ["id"]}

        pattern = MergePattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="target.*path"):
            pattern.validate()

    def test_validate_accepts_path_as_target(self, mock_engine, mock_config):
        """Test that 'path' can be used instead of 'target'."""
        mock_config.params = {"path": "/some/path", "keys": ["id"]}

        pattern = MergePattern(mock_engine, mock_config)
        pattern.validate()

    def test_validate_requires_keys(self, mock_engine, mock_config):
        """Test that keys are required."""
        mock_config.params = {"target": "/some/path"}

        pattern = MergePattern(mock_engine, mock_config)

        with pytest.raises((ValueError, AttributeError)):
            pattern.validate()

    def test_validate_success_with_target_and_keys(self, mock_engine, mock_config):
        """Test validation passes with target and keys."""
        mock_config.params = {
            "target": "/some/path",
            "keys": ["id"],
        }

        pattern = MergePattern(mock_engine, mock_config)
        pattern.validate()

    def test_validate_success_with_path_and_keys(self, mock_engine, mock_config):
        """Test validation passes with path and keys."""
        mock_config.params = {
            "path": "/some/path",
            "keys": ["id"],
        }

        pattern = MergePattern(mock_engine, mock_config)
        pattern.validate()

    def test_validate_success_with_strategy(self, mock_engine, mock_config):
        """Test validation passes with strategy."""
        mock_config.params = {
            "target": "/some/path",
            "keys": ["id"],
            "strategy": "upsert",
        }

        pattern = MergePattern(mock_engine, mock_config)
        pattern.validate()


class TestMergePatternExecution:
    """Test MergePattern execution."""

    @patch("odibi.patterns.merge.merge")
    def test_execute_calls_merge_transformer(self, mock_merge, mock_engine, mock_config):
        """Test that execute calls the merge transformer."""
        mock_config.params = {
            "target": "/some/path",
            "keys": ["id"],
            "strategy": "upsert",
        }

        source_df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
        context = create_pandas_context(source_df, mock_engine)

        pattern = MergePattern(mock_engine, mock_config)
        result = pattern.execute(context)

        mock_merge.assert_called_once()
        assert result is source_df

    @patch("odibi.patterns.merge.merge")
    def test_execute_filters_valid_params(self, mock_merge, mock_engine, mock_config):
        """Test that only valid MergeParams are passed to merge."""
        mock_config.params = {
            "target": "/some/path",
            "keys": ["id"],
            "strategy": "upsert",
            "invalid_param": "should_be_filtered",
        }

        source_df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
        context = create_pandas_context(source_df, mock_engine)

        pattern = MergePattern(mock_engine, mock_config)
        pattern.execute(context)

        call_kwargs = mock_merge.call_args[1]
        assert "invalid_param" not in call_kwargs

    @patch("odibi.patterns.merge.merge")
    def test_execute_uses_default_strategy(self, mock_merge, mock_engine, mock_config):
        """Test that default strategy is 'upsert'."""
        mock_config.params = {
            "target": "/some/path",
            "keys": ["id"],
        }

        source_df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
        context = create_pandas_context(source_df, mock_engine)

        pattern = MergePattern(mock_engine, mock_config)
        pattern.execute(context)

        mock_merge.assert_called_once()

    @patch("odibi.patterns.merge.merge")
    def test_execute_handles_spark_context(self, mock_merge, mock_engine, mock_config):
        """Test execution with Spark context type."""
        mock_config.params = {
            "target": "/some/path",
            "keys": ["id"],
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

        pattern = MergePattern(mock_engine, mock_config)
        result = pattern.execute(context)

        mock_spark_df.count.assert_called()
        assert result is mock_spark_df

    @patch("odibi.patterns.merge.merge")
    def test_execute_handles_count_failure(self, mock_merge, mock_engine, mock_config):
        """Test execution continues even if count fails."""
        mock_config.params = {
            "target": "/some/path",
            "keys": ["id"],
        }

        source_df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
        context = create_pandas_context(source_df, mock_engine)
        context._df = MagicMock()
        context._df.__len__ = MagicMock(side_effect=Exception("Count failed"))

        pattern = MergePattern(mock_engine, mock_config)
        pattern.execute(context)

        mock_merge.assert_called_once()


class TestMergePatternErrorHandling:
    """Test MergePattern error handling."""

    @patch("odibi.patterns.merge.merge")
    def test_execute_raises_on_merge_failure(self, mock_merge, mock_engine, mock_config):
        """Test that merge failures are re-raised."""
        mock_merge.side_effect = ValueError("Merge failed")

        mock_config.params = {
            "target": "/some/path",
            "keys": ["id"],
        }

        source_df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
        context = create_pandas_context(source_df, mock_engine)

        pattern = MergePattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="Merge failed"):
            pattern.execute(context)

    @patch("odibi.patterns.merge.merge")
    @patch("odibi.patterns.merge.get_logging_context")
    def test_execute_logs_error_on_failure(
        self, mock_get_ctx, mock_merge, mock_engine, mock_config
    ):
        """Test that errors are logged before re-raising."""
        mock_ctx = MagicMock()
        mock_get_ctx.return_value = mock_ctx
        mock_merge.side_effect = ValueError("Merge failed")

        mock_config.params = {
            "target": "/some/path",
            "keys": ["id"],
        }

        source_df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
        context = create_pandas_context(source_df, mock_engine)

        pattern = MergePattern(mock_engine, mock_config)

        with pytest.raises(ValueError):
            pattern.execute(context)

        mock_ctx.error.assert_called()


class TestMergePatternLogging:
    """Test MergePattern logging."""

    @patch("odibi.patterns.merge.merge")
    @patch("odibi.patterns.merge.get_logging_context")
    def test_execute_logs_start_and_complete(
        self, mock_get_ctx, mock_merge, mock_engine, mock_config
    ):
        """Test that execution logs start and completion."""
        mock_ctx = MagicMock()
        mock_get_ctx.return_value = mock_ctx

        mock_config.params = {
            "target": "/some/path",
            "keys": ["id"],
            "strategy": "upsert",
        }

        source_df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
        context = create_pandas_context(source_df, mock_engine)

        pattern = MergePattern(mock_engine, mock_config)
        pattern.execute(context)

        assert mock_ctx.debug.call_count >= 1
        mock_ctx.info.assert_called_once()

        info_call = mock_ctx.info.call_args
        assert "elapsed_ms" in info_call[1]
        assert info_call[1]["pattern"] == "MergePattern"


class TestMergePatternRegistration:
    """Test MergePattern registration."""

    def test_pattern_registered(self):
        """Test that MergePattern is registered in patterns registry."""
        from odibi.patterns import get_pattern_class

        pattern_class = get_pattern_class("merge")
        assert pattern_class == MergePattern
