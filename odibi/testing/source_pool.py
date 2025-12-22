"""
SourcePool: Deterministic, frozen test data sources for Odibi testing.

Phase 7.B.1 - Preparation only, NO runtime logic.

This module defines the schema and metadata structures for deterministic,
replayable data sources that exercise all supported Odibi data types and
ingestion paths.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ============================================
# Enums for SourcePool Configuration
# ============================================


class FileFormat(str, Enum):
    """Supported file formats for source pools."""

    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    AVRO = "avro"
    DELTA = "delta"


class SourceType(str, Enum):
    """Supported source types for ingestion testing."""

    LOCAL = "local"
    ADLS_EMULATED = "adls_emulated"
    AZURE_BLOB_EMULATED = "azure_blob_emulated"
    SQL_JDBC_LOCAL = "sql_jdbc_local"
    CLOUDFILES = "cloudfiles"


class DataQuality(str, Enum):
    """Classification of data cleanliness."""

    CLEAN = "clean"  # No nulls, no duplicates, valid types
    MESSY = "messy"  # Contains nulls, edge cases, type issues
    MIXED = "mixed"  # Combination of clean and messy partitions


class PoolStatus(str, Enum):
    """Lifecycle status of a source pool."""

    DRAFT = "draft"  # Schema defined, data not yet prepared
    FROZEN = "frozen"  # Data prepared and hash-verified
    DEPRECATED = "deprecated"  # Marked for removal


# ============================================
# Schema Definitions (explicit, no inference)
# ============================================


class ColumnSchema(BaseModel):
    """Explicit column definition - NO runtime inference."""

    name: str = Field(description="Column name")
    dtype: str = Field(description="Data type (string, int64, float64, bool, datetime, etc.)")
    nullable: bool = Field(default=False, description="Whether column allows nulls")
    primary_key: bool = Field(default=False, description="Part of primary key")
    description: Optional[str] = Field(default=None, description="Column documentation")
    sample_values: Optional[List[Any]] = Field(
        default=None,
        description="Example values for documentation (not used at runtime)",
    )


class TableSchema(BaseModel):
    """Complete table schema definition."""

    columns: List[ColumnSchema] = Field(description="Ordered list of columns")
    primary_keys: Optional[List[str]] = Field(
        default=None, description="List of primary key column names"
    )
    partition_columns: Optional[List[str]] = Field(
        default=None, description="Partition columns (for Delta/Parquet)"
    )

    @model_validator(mode="after")
    def validate_pk_columns_exist(self):
        """Ensure all primary key columns exist in schema."""
        if self.primary_keys:
            col_names = {c.name for c in self.columns}
            for pk in self.primary_keys:
                if pk not in col_names:
                    raise ValueError(f"Primary key column '{pk}' not in schema")
        return self


# ============================================
# Data Characteristics Metadata
# ============================================


class DataCharacteristics(BaseModel):
    """Metadata about data characteristics for test coverage."""

    row_count: int = Field(ge=0, description="Exact row count (deterministic)")
    has_nulls: bool = Field(default=False, description="Contains null values")
    has_duplicates: bool = Field(default=False, description="Contains duplicate keys")
    has_unicode: bool = Field(default=False, description="Contains non-ASCII characters")
    has_special_chars: bool = Field(default=False, description="Contains newlines, quotes, etc.")
    has_empty_strings: bool = Field(default=False, description="Contains empty string values")
    has_whitespace_issues: bool = Field(
        default=False, description="Leading/trailing whitespace in strings"
    )
    has_type_coercion_cases: bool = Field(
        default=False, description="Values that may coerce unexpectedly"
    )
    date_range: Optional[Dict[str, str]] = Field(
        default=None,
        description="Date range {min: ISO date, max: ISO date}",
    )
    numeric_ranges: Optional[Dict[str, Dict[str, float]]] = Field(
        default=None,
        description="Numeric column ranges {column: {min: v, max: v}}",
    )


# ============================================
# Integrity & Hashing
# ============================================


class IntegrityManifest(BaseModel):
    """Cryptographic integrity manifest for frozen source pools."""

    algorithm: Literal["sha256"] = "sha256"
    file_hashes: Dict[str, str] = Field(description="Map of relative file path -> SHA256 hash")
    manifest_hash: str = Field(
        description="SHA256 hash of sorted file_hashes for quick verification"
    )
    frozen_at: datetime = Field(description="Timestamp when pool was frozen")
    frozen_by: str = Field(default="system", description="User/system that froze the pool")


# ============================================
# Source Pool Definition (Main Schema)
# ============================================


class SourcePoolConfig(BaseModel):
    """
    Complete SourcePool definition.

    This is the primary schema for defining deterministic test data sources.

    Invariants:
    - All data is disk-backed and hashable
    - Schemas are explicit (no runtime inference)
    - Metadata is complete and machine-readable
    - Sources are immutable once frozen
    """

    # === Identification ===
    pool_id: str = Field(
        description="Unique identifier (e.g., 'nyc_taxi_csv_clean')",
        pattern=r"^[a-z][a-z0-9_]*$",
    )
    version: str = Field(
        default="1.0.0",
        description="Semantic version for tracking pool evolution",
    )
    name: str = Field(description="Human-readable name")
    description: str = Field(description="Detailed description of the dataset")

    # === Source Configuration ===
    file_format: FileFormat = Field(description="File format")
    source_type: SourceType = Field(description="Source/ingestion type to test")
    data_quality: DataQuality = Field(description="Clean/messy/mixed classification")

    # === Schema (Explicit, No Inference) ===
    schema: TableSchema = Field(description="Explicit schema definition")

    # === Disk Location (relative to .odibi/source_cache/) ===
    cache_path: str = Field(
        description="Relative path under .odibi/source_cache/ (e.g., 'nyc_taxi/csv/clean/')"
    )

    # === Data Characteristics ===
    characteristics: DataCharacteristics = Field(
        description="Metadata about data properties for test coverage"
    )

    # === Status & Integrity ===
    status: PoolStatus = Field(
        default=PoolStatus.DRAFT,
        description="Current lifecycle status",
    )
    integrity: Optional[IntegrityManifest] = Field(
        default=None,
        description="Integrity manifest (required when status=frozen)",
    )

    # === Provenance ===
    original_source: Optional[str] = Field(
        default=None,
        description="URL or reference to original public dataset",
    )
    license: Optional[str] = Field(
        default=None,
        description="Data license (e.g., 'CC0', 'MIT', 'Public Domain')",
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When this pool definition was created",
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Last modification timestamp",
    )

    # === Test Coverage Hints ===
    tests_coverage: List[str] = Field(
        default_factory=list,
        description="List of test scenarios this pool covers (e.g., 'null_handling', 'unicode_support')",
    )
    compatible_pipelines: List[str] = Field(
        default_factory=list,
        description="Pipeline patterns this pool is designed for (e.g., 'bronze_ingestion', 'silver_dedup')",
    )

    @model_validator(mode="after")
    def validate_frozen_has_integrity(self):
        """Frozen pools must have integrity manifest."""
        if self.status == PoolStatus.FROZEN and not self.integrity:
            raise ValueError(f"Pool '{self.pool_id}': frozen status requires integrity manifest")
        return self

    @field_validator("cache_path")
    @classmethod
    def validate_cache_path(cls, v: str) -> str:
        """Ensure cache_path is relative and safe."""
        if v.startswith("/") or v.startswith("\\") or ".." in v:
            raise ValueError(f"cache_path must be relative without '..': {v}")
        return v.replace("\\", "/")


# ============================================
# Source Pool Index (Registry)
# ============================================


class SourcePoolIndex(BaseModel):
    """
    Index of all registered source pools.

    Stored at: .odibi/source_metadata/pool_index.yaml
    """

    version: str = Field(default="1.0.0", description="Index schema version")
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    pools: Dict[str, str] = Field(
        default_factory=dict,
        description="Map of pool_id -> metadata file path (relative to source_metadata/)",
    )

    def add_pool(self, pool_id: str, metadata_path: str) -> None:
        """Register a pool in the index."""
        self.pools[pool_id] = metadata_path
        self.updated_at = datetime.now(timezone.utc)

    def remove_pool(self, pool_id: str) -> None:
        """Remove a pool from the index."""
        if pool_id in self.pools:
            del self.pools[pool_id]
            self.updated_at = datetime.now(timezone.utc)
