"""Unit tests for Pattern base class."""

import time
from unittest.mock import MagicMock, patch

import pytest

from odibi.config import NodeConfig
from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.patterns.base import Pattern


class ConcretePattern(Pattern):
    """Concrete implementation of Pattern for testing."""

    def execute(self, context: EngineContext):
        return context.df


class FailingPattern(Pattern):
    """Pattern that raises an error during execution."""

    def execute(self, context: EngineContext):
        raise ValueError("Test error")


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
    config.params = {"key1": "value1", "key2": "value2"}
    return config


class TestPatternInitialization:
    """Test Pattern initialization."""

    def test_init_stores_engine(self, mock_engine, mock_config):
        """Test that engine is stored correctly."""
        pattern = ConcretePattern(mock_engine, mock_config)
        assert pattern.engine is mock_engine

    def test_init_stores_config(self, mock_engine, mock_config):
        """Test that config is stored correctly."""
        pattern = ConcretePattern(mock_engine, mock_config)
        assert pattern.config is mock_config

    def test_init_stores_params(self, mock_engine, mock_config):
        """Test that params are extracted from config."""
        pattern = ConcretePattern(mock_engine, mock_config)
        assert pattern.params == {"key1": "value1", "key2": "value2"}

    def test_init_empty_params(self, mock_engine):
        """Test initialization with empty params."""
        config = MagicMock(spec=NodeConfig)
        config.params = {}
        pattern = ConcretePattern(mock_engine, config)
        assert pattern.params == {}


class TestPatternValidation:
    """Test Pattern validation."""

    def test_validate_default_passes(self, mock_engine, mock_config):
        """Test that default validation passes."""
        pattern = ConcretePattern(mock_engine, mock_config)
        pattern.validate()

    @patch("odibi.patterns.base.get_logging_context")
    def test_validate_logs_pattern_name(self, mock_get_ctx, mock_engine, mock_config):
        """Test that validation logs the pattern name."""
        mock_ctx = MagicMock()
        mock_get_ctx.return_value = mock_ctx

        pattern = ConcretePattern(mock_engine, mock_config)
        pattern.validate()

        mock_ctx.debug.assert_called()
        call_args = mock_ctx.debug.call_args_list
        assert any("ConcretePattern" in str(c) for c in call_args)


class TestPatternLogging:
    """Test Pattern logging methods."""

    @patch("odibi.patterns.base.get_logging_context")
    def test_log_execution_start_returns_time(self, mock_get_ctx, mock_engine, mock_config):
        """Test that _log_execution_start returns start time."""
        mock_ctx = MagicMock()
        mock_get_ctx.return_value = mock_ctx

        pattern = ConcretePattern(mock_engine, mock_config)
        start_time = pattern._log_execution_start()

        assert isinstance(start_time, float)
        assert start_time > 0

    @patch("odibi.patterns.base.get_logging_context")
    def test_log_execution_start_logs_debug(self, mock_get_ctx, mock_engine, mock_config):
        """Test that _log_execution_start logs debug message."""
        mock_ctx = MagicMock()
        mock_get_ctx.return_value = mock_ctx

        pattern = ConcretePattern(mock_engine, mock_config)
        pattern._log_execution_start(custom_key="custom_value")

        mock_ctx.debug.assert_called_once()
        call_kwargs = mock_ctx.debug.call_args[1]
        assert call_kwargs["pattern"] == "ConcretePattern"
        assert call_kwargs["custom_key"] == "custom_value"

    @patch("odibi.patterns.base.get_logging_context")
    def test_log_execution_complete_logs_elapsed(self, mock_get_ctx, mock_engine, mock_config):
        """Test that _log_execution_complete logs elapsed time."""
        mock_ctx = MagicMock()
        mock_get_ctx.return_value = mock_ctx

        pattern = ConcretePattern(mock_engine, mock_config)
        start_time = time.time() - 0.1

        pattern._log_execution_complete(start_time, rows_processed=100)

        mock_ctx.info.assert_called_once()
        call_kwargs = mock_ctx.info.call_args[1]
        assert call_kwargs["pattern"] == "ConcretePattern"
        assert call_kwargs["rows_processed"] == 100
        assert "elapsed_ms" in call_kwargs
        assert call_kwargs["elapsed_ms"] >= 100

    @patch("odibi.patterns.base.get_logging_context")
    def test_log_error_logs_error_details(self, mock_get_ctx, mock_engine, mock_config):
        """Test that _log_error logs error details."""
        mock_ctx = MagicMock()
        mock_get_ctx.return_value = mock_ctx

        pattern = ConcretePattern(mock_engine, mock_config)
        error = ValueError("Test error message")

        pattern._log_error(error, target="test_table")

        mock_ctx.error.assert_called_once()
        call_args = mock_ctx.error.call_args
        assert "Test error message" in call_args[0][0]
        assert call_args[1]["pattern"] == "ConcretePattern"
        assert call_args[1]["error_type"] == "ValueError"
        assert call_args[1]["target"] == "test_table"


class TestPatternAbstractMethods:
    """Test Pattern abstract method requirements."""

    def test_pattern_is_abstract(self):
        """Test that Pattern cannot be instantiated directly."""
        mock_engine = MagicMock()
        mock_config = MagicMock(spec=NodeConfig)
        mock_config.params = {}

        with pytest.raises(TypeError):
            Pattern(mock_engine, mock_config)

    def test_execute_must_be_implemented(self):
        """Test that subclasses must implement execute."""

        class IncompletePattern(Pattern):
            pass

        mock_engine = MagicMock()
        mock_config = MagicMock(spec=NodeConfig)
        mock_config.params = {}

        with pytest.raises(TypeError):
            IncompletePattern(mock_engine, mock_config)


class TestPatternExecution:
    """Test Pattern execution behavior."""

    def test_execute_returns_dataframe(self, mock_engine, mock_config):
        """Test that execute returns a DataFrame."""
        import pandas as pd

        pattern = ConcretePattern(mock_engine, mock_config)

        pandas_context = PandasContext()
        df = pd.DataFrame({"col1": [1, 2, 3]})
        context = EngineContext(
            context=pandas_context,
            df=df,
            engine_type=EngineType.PANDAS,
            engine=mock_engine,
        )

        result = pattern.execute(context)
        assert result is df

    def test_failing_pattern_raises_error(self, mock_engine, mock_config):
        """Test that failing patterns raise errors."""
        import pandas as pd

        pattern = FailingPattern(mock_engine, mock_config)

        pandas_context = PandasContext()
        df = pd.DataFrame({"col1": [1, 2, 3]})
        context = EngineContext(
            context=pandas_context,
            df=df,
            engine_type=EngineType.PANDAS,
            engine=mock_engine,
        )

        with pytest.raises(ValueError, match="Test error"):
            pattern.execute(context)
