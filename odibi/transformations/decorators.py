"""
Transformation Decorators
=========================

Decorators for registering transformations.
"""

from typing import Callable, Optional
from .registry import get_registry
from .explanation import wrap_with_explanation


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

        @my_transform.explain
        def explain(threshold, **context):
            return f"Filter records above {threshold}"

    Args:
        name: Unique name for transformation
        version: Semantic version
        category: Optional category
        tags: Optional tags for discovery

    Returns:
        Decorated function (registered in global registry) with .explain() capability
    """

    def decorator(func: Callable) -> Callable:
        # Wrap with explanation capability
        wrapped = wrap_with_explanation(func)

        # Register in registry
        registry = get_registry()
        registry.register(name=name, func=wrapped, version=version, category=category, tags=tags)

        return wrapped

    return decorator
