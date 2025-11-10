"""
Transformation Decorators
=========================

Decorators for registering transformations.
"""

from typing import Callable, Optional
from .registry import get_registry


def transformation(
    name: str,
    version: str = "1.0.0",
    category: Optional[str] = None,
    tags: Optional[list] = None,
):
    """
    Decorator to register a transformation.

    Usage:
        @transformation("my_transform", category="filtering")
        def my_transform(df, threshold):
            '''Filter records above threshold.'''
            return df[df.value > threshold]

    Args:
        name: Unique name for transformation
        version: Semantic version
        category: Optional category
        tags: Optional tags for discovery

    Returns:
        Decorated function (registered in global registry)
    """

    def decorator(func: Callable) -> Callable:
        registry = get_registry()
        registry.register(name=name, func=func, version=version, category=category, tags=tags)
        return func

    return decorator
