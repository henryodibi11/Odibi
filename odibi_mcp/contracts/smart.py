"""
Smart Discovery Tool contracts for the Odibi MCP server.

These dataclasses follow the SmartResponse pattern where every response includes:
- data (the actual result)
- confidence (0.0-1.0)
- warnings (list of strings)
- errors (list of strings)
- next_step (suggested next tool to call)
- ready_for (pre-filled params for next tool)
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class FolderInfo:
    """Information about a folder in storage connection."""

    path: str
    file_count: int
    pattern: Optional[str] = None
    formats: Dict[str, int] = field(default_factory=dict)
    sample_files: List[str] = field(default_factory=list)


@dataclass
class SchemaInfo:
    """Information about a schema in SQL connection."""

    name: str
    table_count: int
    sample_tables: List[str] = field(default_factory=list)
    estimated_rows: Optional[str] = None


@dataclass
class MapEnvironmentResponse:
    """Response from mapping a connection environment (storage or SQL)."""

    connection: str
    connection_type: str
    scanned_at: datetime
    summary: Dict[str, Any]
    structure: List[Any]
    recommendations: List[str] = field(default_factory=list)
    next_step: str = ""
    suggested_sources: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


@dataclass
class FileOptions:
    """Detected file reading options for CSV/text files."""

    encoding: str = "utf-8"
    delimiter: str = ","
    quote_char: str = '"'
    skip_rows: int = 0
    header_row: Optional[int] = 0
    line_terminator: str = "\n"


@dataclass
class ColumnInfo:
    """Information about a single column in a source."""

    name: str
    dtype: str
    nullable: bool = True
    null_count: Optional[int] = None
    null_pct: Optional[float] = None  # Percentage of nulls (0.0-1.0)
    cardinality: Optional[str] = None  # "high", "medium", "low", "unique"
    distinct_count: Optional[int] = None
    sample_values: List[Any] = field(default_factory=list)
    detected_pattern: Optional[str] = None  # e.g., "date:YYYY-MM-DD", "email", "uuid"
    suggested_transform: Optional[str] = None  # e.g., "cast_columns", "rename_columns"


@dataclass
class TransformerSuggestion:
    """A suggested transformer based on column analysis."""

    transformer: str
    reason: str
    columns: List[str] = field(default_factory=list)
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DecisionHint:
    """A decision the AI needs to make about ambiguous data."""

    question: str
    options: List[str] = field(default_factory=list)
    default: Optional[str] = None
    column: Optional[str] = None


@dataclass
class OdibiSuggestions:
    """Odibi-aware suggestions based on profile analysis."""

    suggested_pattern: Optional[str] = None  # "dimension", "fact", "scd2", "aggregation"
    pattern_reason: Optional[str] = None
    suggested_transformers: List[TransformerSuggestion] = field(default_factory=list)
    incremental_hint: Optional[
        Dict[str, str]
    ] = None  # {"column": "ModifiedDate", "type": "timestamp"}
    partition_hint: Optional[List[str]] = None  # Columns good for partitioning
    decisions_needed: List[DecisionHint] = field(default_factory=list)


@dataclass
class FixInstruction:
    """Actionable fix instruction when test fails."""

    action: str  # "edit_yaml", "change_encoding", "adjust_skiprows", etc.
    description: str
    path: Optional[str] = None  # YAML path like "source.options.encoding"
    current_value: Optional[str] = None
    suggested_value: Optional[str] = None
    retry_with: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationResult:
    """Results from validating source data quality."""

    column_count_consistent: bool = True
    mojibake_detected: bool = False
    empty_columns: List[str] = field(default_factory=list)
    type_consistency: Dict[str, float] = field(default_factory=dict)


@dataclass
class ProfileSourceResponse:
    """Response from profiling a data source (file or table)."""

    connection: str
    path: str
    source_type: str
    attempts: int = 1
    confidence: float = 1.0
    file_options: Optional[FileOptions] = None
    schema: List[ColumnInfo] = field(default_factory=list)
    sample_rows: List[Dict[str, Any]] = field(default_factory=list)
    validation: ValidationResult = field(default_factory=ValidationResult)
    candidate_keys: List[str] = field(default_factory=list)
    candidate_watermarks: List[str] = field(default_factory=list)
    # AI-friendly enhancements
    suggestions: Optional[OdibiSuggestions] = None
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    next_step: str = ""
    ready_for: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GenerateBronzeNodeResponse:
    """Response from generating a bronze layer node YAML."""

    node_name: str
    yaml_content: str
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    next_step: str = ""
    ready_for: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TestNodeResponse:
    """Response from testing a generated node configuration."""

    status: str
    rows_read: int = 0
    sample_rows: List[Dict[str, Any]] = field(default_factory=list)
    schema: List[ColumnInfo] = field(default_factory=list)
    validation: ValidationResult = field(default_factory=ValidationResult)
    errors: List[str] = field(default_factory=list)
    suggestions: List[str] = field(default_factory=list)
    # AI-friendly fix instructions
    fixes: List[FixInstruction] = field(default_factory=list)
    ready_to_save: bool = False


@dataclass
class FileProfileSummary:
    """Summary of a single file's profile result."""

    path: str
    source_type: str
    confidence: float
    options: Dict[str, Any] = field(default_factory=dict)
    column_count: int = 0
    sample_columns: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    ready_for: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OptionsGroup:
    """Group of files that share the same read options."""

    options: Dict[str, Any]
    files: List[str] = field(default_factory=list)
    count: int = 0


@dataclass
class ProfileFolderResponse:
    """Response from batch profiling a folder of files."""

    connection: str
    folder_path: str
    pattern: str
    total_files: int
    profiled_count: int
    success_count: int
    error_count: int
    confidence: float
    options_groups: List[OptionsGroup] = field(default_factory=list)
    file_profiles: List[FileProfileSummary] = field(default_factory=list)
    consistent_options: bool = False
    recommended_options: Dict[str, Any] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    next_step: str = ""
    ready_for: Dict[str, Any] = field(default_factory=dict)
