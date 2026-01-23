# odibi_mcp/discovery/limits.py
"""Discovery limits configuration."""

from pydantic import BaseModel, Field


class DiscoveryLimits(BaseModel):
    """
    Configuration limits for source discovery operations.
    Enforces deny-by-default and resource caps.
    """

    max_files_per_request: int = Field(default=1000, ge=1, le=10000)
    max_tables_per_request: int = Field(default=500, ge=1, le=5000)
    max_schema_columns: int = Field(default=500, ge=1, le=2000)
    max_preview_rows: int = Field(default=100, ge=1, le=1000)
    max_preview_bytes: int = Field(default=1_000_000, ge=1024)  # 1MB default
    max_path_depth: int = Field(default=5, ge=1, le=20)
    schema_inference_bytes: int = Field(default=10_000_000, ge=1024)  # 10MB for inference

    def validate_file_count(self, count: int) -> bool:
        """Check if file count is within limits."""
        return count <= self.max_files_per_request

    def validate_table_count(self, count: int) -> bool:
        """Check if table count is within limits."""
        return count <= self.max_tables_per_request


# Default limits instance
DEFAULT_LIMITS = DiscoveryLimits()
