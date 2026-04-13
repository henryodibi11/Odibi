"""Type definitions for discovery responses.

All discovery methods return Pydantic models for consistency, validation,
and easy serialization (JSON, YAML, dict).
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field


class DatasetRef(BaseModel):
    """Reference to a dataset (table, file, folder)."""

    name: str = Field(description="Dataset name (table name or file path)")
    namespace: Optional[str] = Field(default=None, description="Schema or folder prefix")
    kind: Literal["table", "view", "file", "folder"] = Field(description="Dataset type")
    path: Optional[str] = Field(default=None, description="Full path or qualified name")
    format: Optional[str] = Field(default=None, description="File format (csv, parquet, etc.)")
    size_bytes: Optional[int] = Field(default=None, description="Size in bytes")
    row_count: Optional[int] = Field(default=None, description="Approximate row count")
    modified_at: Optional[datetime] = Field(default=None, description="Last modified timestamp")


class Column(BaseModel):
    """Column metadata."""

    name: str
    dtype: str = Field(description="Data type (string, int64, datetime, etc.)")
    nullable: Optional[bool] = Field(default=None, description="Whether column accepts nulls")
    description: Optional[str] = Field(default=None, description="Column description/comment")

    # Optional profiling data
    null_count: Optional[int] = None
    null_pct: Optional[float] = None
    cardinality: Optional[str] = Field(default=None, description="high, medium, low, unique")
    distinct_count: Optional[int] = None
    sample_values: List[Any] = Field(default_factory=list, description="Sample values")
    detected_pattern: Optional[str] = Field(
        default=None, description="date:YYYY-MM-DD, email, uuid, etc."
    )


class Schema(BaseModel):
    """Schema information for a dataset."""

    dataset: DatasetRef
    columns: List[Column]
    primary_key: Optional[List[str]] = Field(default=None, description="Primary key columns")
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class TableProfile(BaseModel):
    """Detailed profiling statistics for a table."""

    dataset: DatasetRef
    rows_sampled: int = Field(description="Number of rows sampled for profiling")
    total_rows: Optional[int] = Field(default=None, description="Total rows in table")
    columns: List[Column] = Field(description="Columns with profiling stats")

    # Candidate columns for pipeline configuration
    candidate_keys: List[str] = Field(
        default_factory=list, description="Likely primary key columns"
    )
    candidate_watermarks: List[str] = Field(
        default_factory=list, description="Likely incremental timestamp columns"
    )

    # Quality metrics
    completeness: Optional[float] = Field(
        default=None, description="Overall non-null rate (0.0-1.0)"
    )

    # Suggestions
    suggestions: List[str] = Field(
        default_factory=list, description="Suggested transformations or config"
    )
    warnings: List[str] = Field(default_factory=list, description="Data quality warnings")


class PreviewResult(BaseModel):
    """Preview of sample rows from a dataset."""

    dataset: DatasetRef
    columns: List[str] = Field(default_factory=list, description="Column names")
    rows: List[Dict[str, Any]] = Field(default_factory=list, description="Sample rows as dicts")
    total_rows: Optional[int] = Field(default=None, description="Total row count if known")
    truncated: bool = Field(default=False, description="True if more rows exist than returned")
    format: Optional[str] = Field(default=None, description="Source format (csv, parquet, etc.)")


class Relationship(BaseModel):
    """Foreign key or inferred relationship between datasets."""

    parent: DatasetRef = Field(description="Parent/dimension table")
    child: DatasetRef = Field(description="Child/fact table")
    keys: List[tuple[str, str]] = Field(description="Column pairs (parent_col, child_col)")
    source: Literal["declared", "heuristic"] = Field(description="How relationship was discovered")
    confidence: float = Field(default=1.0, description="Confidence score (0.0-1.0)")
    details: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")


class FreshnessResult(BaseModel):
    """Data freshness information."""

    dataset: DatasetRef
    last_updated: Optional[datetime] = Field(default=None, description="Last update timestamp")
    source: Literal["metadata", "data"] = Field(
        description="metadata=table stats, data=MAX(timestamp_col)"
    )
    details: Dict[str, Any] = Field(default_factory=dict)
    is_stale: Optional[bool] = Field(
        default=None, description="True if data is older than threshold"
    )
    age_hours: Optional[float] = Field(default=None, description="Age in hours")


class PartitionInfo(BaseModel):
    """Partition structure information for file-based datasets."""

    root: str = Field(description="Root path")
    keys: List[str] = Field(
        default_factory=list, description="Partition key names (year, month, date, etc.)"
    )
    example_values: Dict[str, List[str]] = Field(
        default_factory=dict, description="Sample partition values"
    )
    format: Optional[str] = Field(default=None, description="File format in partitions")
    partition_count: Optional[int] = Field(
        default=None, description="Number of partitions detected"
    )


class CatalogSummary(BaseModel):
    """High-level catalog summary for a connection."""

    connection_name: str
    connection_type: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # SQL connections
    schemas: Optional[List[str]] = Field(default=None, description="Database schemas")
    tables: Optional[List[DatasetRef]] = Field(default=None, description="Tables/views")

    # Storage connections
    folders: Optional[List[DatasetRef]] = Field(default=None, description="Folders")
    files: Optional[List[DatasetRef]] = Field(default=None, description="Files")

    # Summary stats
    total_datasets: int = Field(default=0, description="Total datasets found")
    formats: Dict[str, int] = Field(default_factory=dict, description="File formats → count")

    # Recommendations
    next_step: str = Field(default="", description="Suggested next action")
    suggestions: List[str] = Field(default_factory=list)
