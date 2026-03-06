"""Discovery and profiling module for Odibi.

Provides data source discovery, schema inference, and profiling capabilities
for both humans and AI agents.

This module extracts discovery logic from the MCP layer into reusable core APIs.
"""

from odibi.discovery.types import (
    CatalogSummary,
    DatasetRef,
    Schema,
    Column,
    TableProfile,
    Relationship,
    FreshnessResult,
    PartitionInfo,
)

__all__ = [
    "CatalogSummary",
    "DatasetRef",
    "Schema",
    "Column",
    "TableProfile",
    "Relationship",
    "FreshnessResult",
    "PartitionInfo",
]
