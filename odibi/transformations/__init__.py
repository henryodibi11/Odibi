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

__all__ = ["TransformationRegistry", "get_registry", "transformation"]
__version__ = "1.3.0-alpha.1"
