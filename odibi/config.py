"""Configuration models for ODIBI framework."""

from typing import Optional, List, Dict, Any, Union
from pydantic import BaseModel, Field, field_validator, model_validator
from enum import Enum


class EngineType(str, Enum):
    """Supported execution engines."""

    SPARK = "spark"
    PANDAS = "pandas"


class ConnectionType(str, Enum):
    """Supported connection types."""

    LOCAL = "local"
    AZURE_BLOB = "azure_blob"
    DELTA = "delta"
    SQL_SERVER = "sql_server"


class WriteMode(str, Enum):
    """Write modes for output operations."""

    OVERWRITE = "overwrite"
    APPEND = "append"
    UPSERT = "upsert"
    APPEND_ONCE = "append_once"


class LogLevel(str, Enum):
    """Logging levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class AlertType(str, Enum):
    """Types of alerting channels."""

    WEBHOOK = "webhook"
    SLACK = "slack"
    TEAMS = "teams"


class AlertConfig(BaseModel):
    """Configuration for alerts."""

    type: AlertType
    url: str = Field(description="Webhook URL")
    on_events: List[str] = Field(
        default=["on_failure"],
        description="Events to trigger alert: on_start, on_success, on_failure",
    )
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Extra metadata for alert")


class ErrorStrategy(str, Enum):
    """Strategy for handling node failures."""

    FAIL_FAST = "fail_fast"  # Stop pipeline immediately
    FAIL_LATER = "fail_later"  # Continue pipeline (dependents skipped) - DEFAULT
    IGNORE = "ignore"  # Treat as success (warning) - Dependents run


# ============================================
# Connection Configurations
# ============================================


class BaseConnectionConfig(BaseModel):
    """Base configuration for all connections."""

    type: ConnectionType
    validation_mode: str = "lazy"  # 'lazy' or 'eager'


class LocalConnectionConfig(BaseConnectionConfig):
    """Local filesystem connection."""

    type: ConnectionType = ConnectionType.LOCAL
    base_path: str = Field(default="./data", description="Base directory path")


class AzureBlobConnectionConfig(BaseConnectionConfig):
    """Azure Blob Storage connection."""

    type: ConnectionType = ConnectionType.AZURE_BLOB
    account_name: str
    container: str
    auth: Dict[str, str] = Field(default_factory=dict)


class DeltaConnectionConfig(BaseConnectionConfig):
    """Delta Lake connection."""

    type: ConnectionType = ConnectionType.DELTA
    catalog: str
    schema_name: str = Field(alias="schema")


class SQLServerConnectionConfig(BaseConnectionConfig):
    """SQL Server connection."""

    type: ConnectionType = ConnectionType.SQL_SERVER
    host: str
    database: str
    port: int = 1433
    auth: Dict[str, str] = Field(default_factory=dict)


class HttpConnectionConfig(BaseConnectionConfig):
    """HTTP connection."""

    type: str = "http"
    base_url: str
    headers: Dict[str, str] = Field(default_factory=dict)
    auth: Dict[str, str] = Field(default_factory=dict)


# Connection config discriminated union
ConnectionConfig = Union[
    LocalConnectionConfig,
    AzureBlobConnectionConfig,
    DeltaConnectionConfig,
    SQLServerConnectionConfig,
    HttpConnectionConfig,
]


# ============================================
# Node Configurations
# ============================================


class ReadConfig(BaseModel):
    """Configuration for reading data."""

    connection: str = Field(description="Connection name from project.yaml")
    format: str = Field(description="Data format (csv, parquet, delta, etc.)")
    table: Optional[str] = Field(default=None, description="Table name for SQL/Delta")
    path: Optional[str] = Field(default=None, description="Path for file-based sources")
    options: Dict[str, Any] = Field(default_factory=dict, description="Format-specific options")

    @model_validator(mode="after")
    def check_table_or_path(self):
        """Ensure either table or path is provided."""
        # Allow query in options to substitute for table/path
        has_query = self.options and "query" in self.options
        if not self.table and not self.path and not has_query:
            raise ValueError(
                "Either 'table' or 'path' must be provided for read config (or 'query' in options)"
            )
        return self


class TransformStep(BaseModel):
    """Single transformation step."""

    sql: Optional[str] = None
    function: Optional[str] = None
    operation: Optional[str] = None
    params: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def check_step_type(self):
        """Ensure exactly one step type is provided."""
        step_types = [self.sql, self.function, self.operation]
        if sum(x is not None for x in step_types) != 1:
            raise ValueError("Exactly one of 'sql', 'function', or 'operation' must be provided")
        return self


class TransformConfig(BaseModel):
    """Configuration for transforming data."""

    steps: List[Union[str, TransformStep]] = Field(
        description="List of transformation steps (SQL strings or TransformStep configs)"
    )


class ValidationConfig(BaseModel):
    """Configuration for data validation."""

    schema_validation: Optional[Dict[str, Any]] = Field(
        default=None, alias="schema", description="Schema validation rules"
    )
    not_empty: bool = Field(default=False, description="Ensure result is not empty")
    no_nulls: List[str] = Field(
        default_factory=list, description="Columns that must not have nulls"
    )
    ranges: Dict[str, Dict[str, float]] = Field(
        default_factory=dict, description="Value ranges {col: {min: 0, max: 100}}"
    )
    allowed_values: Dict[str, List[Any]] = Field(
        default_factory=dict, description="Allowed values {col: [val1, val2]}"
    )


class WriteConfig(BaseModel):
    """Configuration for writing data."""

    connection: str = Field(description="Connection name from project.yaml")
    format: str = Field(description="Output format (csv, parquet, delta, etc.)")
    table: Optional[str] = Field(default=None, description="Table name for SQL/Delta")
    path: Optional[str] = Field(default=None, description="Path for file-based outputs")
    mode: WriteMode = Field(default=WriteMode.OVERWRITE, description="Write mode")
    options: Dict[str, Any] = Field(default_factory=dict, description="Format-specific options")

    @model_validator(mode="after")
    def check_table_or_path(self):
        """Ensure either table or path is provided."""
        if not self.table and not self.path:
            raise ValueError("Either 'table' or 'path' must be provided for write config")
        return self


class NodeConfig(BaseModel):
    """Configuration for a single node."""

    name: str = Field(description="Unique node name")
    description: Optional[str] = Field(default=None, description="Human-readable description")
    depends_on: List[str] = Field(default_factory=list, description="List of node dependencies")

    # Operations (at least one required)
    read: Optional[ReadConfig] = None
    transform: Optional[TransformConfig] = None
    write: Optional[WriteConfig] = None
    transformer: Optional[str] = Field(default=None, description="Name of transformer to apply")
    params: Dict[str, Any] = Field(default_factory=dict, description="Parameters for transformer")

    # Optional features
    cache: bool = Field(default=False, description="Cache result for reuse")
    log_level: Optional[LogLevel] = Field(
        default=None, description="Override log level for this node"
    )
    on_error: ErrorStrategy = Field(
        default=ErrorStrategy.FAIL_LATER, description="Failure handling strategy"
    )
    validation: Optional[ValidationConfig] = None
    sensitive: Union[bool, List[str]] = Field(
        default=False, description="If true or list of columns, masks sample data in stories"
    )

    @model_validator(mode="after")
    def check_at_least_one_operation(self):
        """Ensure at least one operation is defined."""
        if not any([self.read, self.transform, self.write, self.transformer]):
            raise ValueError(
                f"Node '{self.name}' must have at least one of: read, transform, write, transformer"
            )
        return self


# ============================================
# Pipeline Configuration
# ============================================


class PipelineConfig(BaseModel):
    """Configuration for a pipeline."""

    pipeline: str = Field(description="Pipeline name")
    description: Optional[str] = Field(default=None, description="Pipeline description")
    layer: Optional[str] = Field(default=None, description="Logical layer (bronze/silver/gold)")
    nodes: List[NodeConfig] = Field(description="List of nodes in this pipeline")

    @field_validator("nodes")
    @classmethod
    def check_unique_node_names(cls, nodes: List[NodeConfig]) -> List[NodeConfig]:
        """Ensure all node names are unique within the pipeline."""
        names = [node.name for node in nodes]
        if len(names) != len(set(names)):
            duplicates = [name for name in names if names.count(name) > 1]
            raise ValueError(f"Duplicate node names found: {set(duplicates)}")
        return nodes


# ============================================
# Project Configuration
# ============================================


class RetryConfig(BaseModel):
    """Retry configuration."""

    enabled: bool = True
    max_attempts: int = Field(default=3, ge=1, le=10)
    backoff: str = Field(default="exponential", pattern="^(exponential|linear|constant)$")


class LoggingConfig(BaseModel):
    """Logging configuration."""

    level: LogLevel = LogLevel.INFO
    structured: bool = Field(default=False, description="Output JSON logs")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Extra metadata in logs")


class StoryConfig(BaseModel):
    """Story generation configuration.

    Stories are ODIBI's core value - execution reports with lineage.
    They must use a connection for consistent, traceable output.
    """

    connection: str = Field(
        description="Connection name for story output (uses connection's path resolution)"
    )
    path: str = Field(description="Path for stories (relative to connection base_path)")
    max_sample_rows: int = Field(default=10, ge=0, le=100)
    auto_generate: bool = True
    retention_days: Optional[int] = Field(default=30, ge=1, description="Days to keep stories")
    retention_count: Optional[int] = Field(
        default=100, ge=1, description="Max number of stories to keep"
    )


# DefaultsConfig deleted - settings moved to top-level ProjectConfig
# PipelineDiscoveryConfig deleted - Phase 2 feature (file discovery)


class ProjectConfig(BaseModel):
    """Complete project configuration from YAML.

    Represents the entire YAML structure with validation.
    All settings are top-level (no nested defaults).
    """

    # === MANDATORY ===
    project: str = Field(description="Project name")
    engine: EngineType = Field(default=EngineType.PANDAS, description="Execution engine")
    connections: Dict[str, Dict[str, Any]] = Field(
        description="Named connections (at least one required)"
    )
    pipelines: List[PipelineConfig] = Field(
        description="Pipeline definitions (at least one required)"
    )
    story: StoryConfig = Field(description="Story generation configuration (mandatory)")

    # === OPTIONAL (with sensible defaults) ===
    description: Optional[str] = Field(default=None, description="Project description")
    version: str = Field(default="1.0.0", description="Project version")
    owner: Optional[str] = Field(default=None, description="Project owner/contact")

    # Global settings (optional with defaults in Pydantic)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    alerts: List[AlertConfig] = Field(default_factory=list, description="Alert configurations")

    # === PHASE 3 ===
    environments: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description="Environment-specific overrides",
    )

    @model_validator(mode="after")
    def validate_story_connection_exists(self):
        """Ensure story.connection is defined in connections."""
        if self.story.connection not in self.connections:
            available = ", ".join(self.connections.keys())
            raise ValueError(
                f"Story connection '{self.story.connection}' not found. "
                f"Available connections: {available}"
            )
        return self

    @model_validator(mode="after")
    def check_environments_not_implemented(self):
        """Check environments implementation."""
        # Implemented in Phase 3
        return self
