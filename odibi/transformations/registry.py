"""
Transformation Registry
=======================

Global registry for all transformations (built-in and user-defined).
"""

from typing import Dict, Callable, Optional, Any


class TransformationRegistry:
    """
    Central registry for all transformations.

    The registry stores transformation functions and enables lookup by name.
    It maintains metadata about each transformation including version,
    documentation, and explanation methods.
    """

    def __init__(self):
        self._transformations: Dict[str, Callable] = {}
        self._metadata: Dict[str, Dict[str, Any]] = {}

    def register(
        self,
        name: str,
        func: Callable,
        version: str = "1.0.0",
        category: Optional[str] = None,
        tags: Optional[list] = None,
    ) -> Callable:
        """
        Register a transformation function.

        Args:
            name: Unique name for the transformation
            func: The transformation function
            version: Semantic version (default: "1.0.0")
            category: Optional category (e.g., "aggregation", "filtering")
            tags: Optional list of tags for discovery

        Returns:
            The registered function (for decorator chaining)

        Raises:
            ValueError: If name already registered
        """
        if name in self._transformations:
            raise ValueError(
                f"Transformation '{name}' already registered. "
                f"Use a unique name or unregister the existing transformation."
            )

        # Validate function has docstring
        if not func.__doc__ or len(func.__doc__.strip()) < 10:
            raise ValueError(
                f"Transformation '{name}' must have a docstring (minimum 10 characters).\n"
                f"Documentation is mandatory in Odibi."
            )

        self._transformations[name] = func
        self._metadata[name] = {
            "version": version,
            "category": category,
            "tags": tags or [],
            "docstring": func.__doc__,
        }

        return func

    def get(self, name: str) -> Optional[Callable]:
        """
        Get a transformation by name.

        Args:
            name: Transformation name

        Returns:
            The transformation function, or None if not found
        """
        return self._transformations.get(name)

    def get_metadata(self, name: str) -> Optional[Dict[str, Any]]:
        """Get metadata for a transformation."""
        return self._metadata.get(name)

    def list_all(self) -> Dict[str, Dict[str, Any]]:
        """List all registered transformations with metadata."""
        return {
            name: {"func": func, **self._metadata[name]}
            for name, func in self._transformations.items()
        }

    def unregister(self, name: str) -> bool:
        """
        Unregister a transformation.

        Args:
            name: Transformation name

        Returns:
            True if unregistered, False if not found
        """
        if name in self._transformations:
            del self._transformations[name]
            del self._metadata[name]
            return True
        return False

    def clear(self):
        """Clear all registrations (primarily for testing)."""
        self._transformations.clear()
        self._metadata.clear()


# Global registry instance
_global_registry = TransformationRegistry()


def get_registry() -> TransformationRegistry:
    """Get the global transformation registry."""
    return _global_registry
