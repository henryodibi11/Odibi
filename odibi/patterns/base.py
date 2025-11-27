from abc import ABC, abstractmethod
from typing import Any

from odibi.config import NodeConfig
from odibi.context import EngineContext
from odibi.engine.base import Engine


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
        pass
