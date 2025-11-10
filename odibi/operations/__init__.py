"""
Built-in Operations for Odibi Pipelines
========================================

This module contains framework-provided operations that users can reference
in their pipeline configurations.

Operations (Phase 3A):
- pivot: Convert long-format to wide-format
- unpivot: Convert wide-format to long-format
- join: Merge two datasets
- sql: Execute SQL transformations

Each operation provides:
- execute() method: Perform the transformation
- explain() method: Generate context-aware documentation
"""

from . import pivot, unpivot, join, sql

__all__ = ["pivot", "unpivot", "join", "sql"]
__version__ = "1.3.0-alpha.1"
