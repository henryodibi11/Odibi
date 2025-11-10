"""
Transformations Module
=====================

This module provides the transformation registry and decorators for
defining custom data transformations.

Example:
    from odibi import transformation

    @transformation("filter_high_values")
    def filter_high_values(df, threshold=100):
        '''Filter records above threshold.'''
        return df[df.value > threshold]
"""

from .registry import TransformationRegistry, get_registry
from .decorators import transformation
from .context import TransformationContext, build_context_from_config
from . import templates

__all__ = [
    "TransformationRegistry",
    "get_registry",
    "transformation",
    "TransformationContext",
    "build_context_from_config",
    "templates",
]
__version__ = "1.3.0-alpha.1"
