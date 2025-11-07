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


class LogLevel(str, Enum):
    """Logging levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


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


# Connection config discriminated union
ConnectionConfig = Union[
    LocalConnectionConfig,
    AzureBlobConnectionConfig,
    DeltaConnectionConfig,
    SQLServerConnectionConfig,
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
        if not self.table and not self.path:
            raise ValueError("Either 'table' or 'path' must be provided for read config")
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

    # Optional features
    cache: bool = Field(default=False, description="Cache result for reuse")
    validation: Optional[ValidationConfig] = None

    @model_validator(mode="after")
    def check_at_least_one_operation(self):
        """Ensure at least one operation is defined."""
        if not any([self.read, self.transform, self.write]):
            raise ValueError(
                f"Node '{self.name}' must have at least one of: read, transform, write"
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
    """Story generation configuration."""

    auto_generate: bool = True
    max_sample_rows: int = Field(default=10, ge=0, le=100)
    output_path: str = Field(default="stories/", description="Directory for story output")


class DefaultsConfig(BaseModel):
    """Global default settings."""

    retry: RetryConfig = Field(default_factory=RetryConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    story: StoryConfig = Field(default_factory=StoryConfig)


class PipelineDiscoveryConfig(BaseModel):
    """Pipeline discovery configuration."""

    path: str = Field(description="Glob pattern for pipeline files")
    layer: Optional[str] = Field(default=None, description="Assign layer to discovered pipelines")


class ProjectConfig(BaseModel):
    """Root project configuration."""

    project: str = Field(description="Project name")
    version: str = Field(default="1.0.0", description="Project version")
    description: Optional[str] = Field(default=None, description="Project description")
    owner: Optional[str] = Field(default=None, description="Project owner/contact")
    engine: EngineType = Field(default=EngineType.PANDAS, description="Default execution engine")

    # Infrastructure
    connections: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict, description="Named connections"
    )

    # Pipeline discovery
    pipelines: Optional[List[PipelineDiscoveryConfig]] = Field(
        default=None, description="Patterns for discovering pipeline files"
    )

    # Global settings
    defaults: DefaultsConfig = Field(default_factory=DefaultsConfig)

    # Environment overrides
    environments: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None, description="Environment-specific overrides"
    )
