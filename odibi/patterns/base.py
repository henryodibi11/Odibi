import time
from abc import ABC, abstractmethod
from typing import Any, Optional

from odibi.config import NodeConfig
from odibi.context import EngineContext
from odibi.engine.base import Engine
from odibi.enums import EngineType
from odibi.utils.logging_context import get_logging_context


class Pattern(ABC):
    """Base class for Execution Patterns."""

    def __init__(self, engine: Engine, config: NodeConfig):
        self.engine = engine
        self.config = config
        self.params = config.params

    @abstractmethod
    def execute(self, context: EngineContext) -> Any:
        """
        Execute the pattern logic.

        Args:
            context: EngineContext containing current DataFrame and helpers.

        Returns:
            The transformed DataFrame.
        """
        pass

    def validate(self) -> None:
        """
        Validate pattern configuration.
        Raises ValueError if invalid.
        """
        ctx = get_logging_context()
        pattern_name = self.__class__.__name__
        ctx.debug(
            f"{pattern_name} validation starting",
            pattern=pattern_name,
            params=self.params,
        )
        ctx.debug(f"{pattern_name} validation passed", pattern=pattern_name)

    def _log_execution_start(self, **kwargs) -> float:
        """
        Log pattern execution start. Returns start time for elapsed calculation.

        Args:
            **kwargs: Additional key-value pairs to log.

        Returns:
            Start time in seconds.
        """
        ctx = get_logging_context()
        pattern_name = self.__class__.__name__
        ctx.debug(f"{pattern_name} execution starting", pattern=pattern_name, **kwargs)
        return time.time()

    def _log_execution_complete(self, start_time: float, **kwargs) -> None:
        """
        Log pattern execution completion with elapsed time.

        Args:
            start_time: Start time from _log_execution_start.
            **kwargs: Additional key-value pairs to log (e.g., row counts).
        """
        ctx = get_logging_context()
        pattern_name = self.__class__.__name__
        elapsed_ms = (time.time() - start_time) * 1000
        ctx.info(
            f"{pattern_name} execution completed",
            pattern=pattern_name,
            elapsed_ms=round(elapsed_ms, 2),
            **kwargs,
        )

    def _log_error(self, error: Exception, **kwargs) -> None:
        """
        Log error context before raising exceptions.

        Args:
            error: The exception that occurred.
            **kwargs: Additional context to log.
        """
        ctx = get_logging_context()
        pattern_name = self.__class__.__name__
        ctx.error(
            f"{pattern_name} execution failed: {error}",
            pattern=pattern_name,
            error_type=type(error).__name__,
            **kwargs,
        )

    def _get_row_count(self, df, engine_type) -> Optional[int]:
        """Get row count from a DataFrame, handling both Spark and Pandas."""
        try:
            if engine_type == EngineType.SPARK:
                return df.count()
            else:
                return len(df)
        except Exception:
            return None
