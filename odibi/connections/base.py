"""Base connection interface."""

from abc import ABC, abstractmethod


class BaseConnection(ABC):
    """Abstract base class for connections."""

    @abstractmethod
    def get_path(self, relative_path: str) -> str:
        """Get full path for a relative path.

        Args:
            relative_path: Relative path or table name

        Returns:
            Full path to resource
        """
        pass

    @abstractmethod
    def validate(self) -> None:
        """Validate connection configuration.

        Raises:
            ConnectionError: If validation fails
        """
        pass
