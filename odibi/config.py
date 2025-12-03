"""Configuration models for ODIBI framework."""

from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

try:
    from typing import Annotated
except ImportError:
    from typing_extensions import Annotated

from pydantic import BaseModel, Field, field_validator, model_validator


class EngineType(str, Enum):
    """Supported execution engines."""

    SPARK = "spark"
    PANDAS = "pandas"
    POLARS = "polars"


class ConnectionType(str, Enum):
    """Supported connection types."""

    LOCAL = "local"
    AZURE_BLOB = "azure_blob"
    DELTA = "delta"
    SQL_SERVER = "sql_server"
    HTTP = "http"


class WriteMode(str, Enum):
    """Write modes for output operations."""

    OVERWRITE = "overwrite"
    APPEND = "append"
    UPSERT = "upsert"
    APPEND_ONCE = "append_once"


class DeleteDetectionMode(str, Enum):
    """
    Delete detection strategies for Silver layer processing.

    Values:
    * `none` - No delete detection (default). Use for append-only facts.
    * `snapshot_diff` - Compare Delta version N vs N-1 keys. Use for full snapshot sources only.
    * `sql_compare` - LEFT ANTI JOIN Silver keys against live source. Recommended for HWM ingestion.
    """

    NONE = "none"
    SNAPSHOT_DIFF = "snapshot_diff"
    SQL_COMPARE = "sql_compare"


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


class AlertEvent(str, Enum):
    """Events that trigger alerts."""

    ON_START = "on_start"
    ON_SUCCESS = "on_success"
    ON_FAILURE = "on_failure"
    ON_QUARANTINE = "on_quarantine"
    ON_GATE_BLOCK = "on_gate_block"
    ON_THRESHOLD_BREACH = "on_threshold_breach"


class AlertConfig(BaseModel):
    """
    Configuration for alerts with throttling support.

    Supports Slack, Teams, and generic webhooks with event-specific payloads.

    **Available Events:**
    - `on_start` - Pipeline started
    - `on_success` - Pipeline completed successfully
    - `on_failure` - Pipeline failed
    - `on_quarantine` - Rows were quarantined
    - `on_gate_block` - Quality gate blocked the pipeline
    - `on_threshold_breach` - A threshold was exceeded

    Example:
    ```yaml
    alerts:
      - type: slack
        url: "${SLACK_WEBHOOK_URL}"
        on_events:
          - on_failure
          - on_quarantine
          - on_gate_block
        metadata:
          throttle_minutes: 15
          max_per_hour: 10
          channel: "#data-alerts"
    ```
    """

    type: AlertType
    url: str = Field(description="Webhook URL")
    on_events: List[AlertEvent] = Field(
        default=[AlertEvent.ON_FAILURE],
        description="Events to trigger alert: on_start, on_success, on_failure, on_quarantine, on_gate_block, on_threshold_breach",
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Extra metadata: throttle_minutes, max_per_hour, channel, etc.",
    )


class ErrorStrategy(str, Enum):
    """Strategy for handling node failures."""

    FAIL_FAST = "fail_fast"  # Stop pipeline immediately
    FAIL_LATER = "fail_later"  # Continue pipeline (dependents skipped) - DEFAULT
    IGNORE = "ignore"  # Treat as success (warning) - Dependents run


class ValidationMode(str, Enum):
    """Validation execution mode."""

    LAZY = "lazy"
    EAGER = "eager"


class ThresholdBreachAction(str, Enum):
    """Action to take when delete threshold is exceeded."""

    WARN = "warn"
    ERROR = "error"
    SKIP = "skip"


class FirstRunBehavior(str, Enum):
    """Behavior when no previous version exists for snapshot_diff."""

    SKIP = "skip"
    ERROR = "error"


# ============================================
# Delete Detection Configuration
# ============================================


class DeleteDetectionConfig(BaseModel):
    """
    Configuration for delete detection in Silver layer.

    ### 🔍 "CDC Without CDC" Guide

    **Business Problem:**
    "Records are deleted in our Azure SQL source, but our Silver tables still show them."

    **The Solution:**
    Use delete detection to identify and flag records that no longer exist in the source.

    **Recipe 1: SQL Compare (Recommended for HWM)**
    ```yaml
    transform:
      steps:
        - operation: detect_deletes
          params:
            mode: sql_compare
            keys: [customer_id]
            source_connection: azure_sql
            source_table: dbo.Customers
    ```

    **Recipe 2: Snapshot Diff (For Full Snapshot Sources)**
    Use ONLY with full snapshot ingestion, NOT with HWM incremental.
    ```yaml
    transform:
      steps:
        - operation: detect_deletes
          params:
            mode: snapshot_diff
            keys: [customer_id]
    ```

    **Recipe 3: Conservative Threshold**
    ```yaml
    transform:
      steps:
        - operation: detect_deletes
          params:
            mode: sql_compare
            keys: [customer_id]
            source_connection: erp
            source_table: dbo.Customers
            max_delete_percent: 20.0
            on_threshold_breach: error
    ```

    **Recipe 4: Hard Delete (Remove Rows)**
    ```yaml
    transform:
      steps:
        - operation: detect_deletes
          params:
            mode: sql_compare
            keys: [customer_id]
            source_connection: azure_sql
            source_table: dbo.Customers
            soft_delete_col: null  # removes rows instead of flagging
    ```
    """

    mode: DeleteDetectionMode = Field(
        default=DeleteDetectionMode.NONE,
        description="Delete detection strategy: none, snapshot_diff, sql_compare",
    )

    keys: List[str] = Field(
        default_factory=list,
        description="Business key columns for comparison",
    )

    soft_delete_col: Optional[str] = Field(
        default="_is_deleted",
        description="Column to flag deletes (True = deleted). Set to null for hard-delete (removes rows).",
    )

    source_connection: Optional[str] = Field(
        default=None,
        description="For sql_compare: connection name to query live source",
    )
    source_table: Optional[str] = Field(
        default=None,
        description="For sql_compare: table to query for current keys",
    )
    source_query: Optional[str] = Field(
        default=None,
        description="For sql_compare: custom SQL query for keys (overrides source_table)",
    )

    snapshot_column: Optional[str] = Field(
        default=None,
        description="For snapshot_diff on non-Delta: column to identify snapshots. "
        "If None, uses Delta time travel (default).",
    )

    on_first_run: FirstRunBehavior = Field(
        default=FirstRunBehavior.SKIP,
        description="Behavior when no previous version exists for snapshot_diff",
    )

    max_delete_percent: Optional[float] = Field(
        default=50.0,
        ge=0.0,
        le=100.0,
        description="Safety threshold: warn/error if more than X% of rows would be deleted",
    )

    on_threshold_breach: ThresholdBreachAction = Field(
        default=ThresholdBreachAction.WARN,
        description="Behavior when delete percentage exceeds max_delete_percent",
    )

    @model_validator(mode="after")
    def validate_mode_requirements(self):
        """Validate that required fields are present for each mode."""
        if self.mode == DeleteDetectionMode.NONE:
            return self

        if not self.keys:
            raise ValueError(f"delete_detection: 'keys' required for mode='{self.mode.value}'")

        if self.mode == DeleteDetectionMode.SQL_COMPARE:
            if not self.source_connection:
                raise ValueError(
                    "delete_detection: 'source_connection' required for sql_compare mode"
                )
            if not self.source_table and not self.source_query:
                raise ValueError(
                    "delete_detection: 'source_table' or 'source_query' required for sql_compare mode"
                )

        return self


# ============================================
# Write Metadata Configuration
# ============================================


class WriteMetadataConfig(BaseModel):
    """
    Configuration for metadata columns added during Bronze writes.

    ### 📋 Bronze Metadata Guide

    **Business Problem:**
    "We need lineage tracking and debugging info for our Bronze layer data."

    **The Solution:**
    Add metadata columns during ingestion for traceability.

    **Recipe 1: Add All Metadata (Recommended)**
    ```yaml
    write:
      connection: bronze
      table: customers
      mode: append
      add_metadata: true  # adds all applicable columns
    ```

    **Recipe 2: Selective Metadata**
    ```yaml
    write:
      connection: bronze
      table: customers
      mode: append
      add_metadata:
        extracted_at: true
        source_file: true
        source_connection: false
        source_table: false
    ```

    **Available Columns:**
    - `_extracted_at`: Pipeline execution timestamp (all sources)
    - `_source_file`: Source filename/path (file sources only)
    - `_source_connection`: Connection name used (all sources)
    - `_source_table`: Table or query name (SQL sources only)
    """

    extracted_at: bool = Field(
        default=True,
        description="Add _extracted_at column with pipeline execution timestamp",
    )
    source_file: bool = Field(
        default=True,
        description="Add _source_file column with source filename (file sources only)",
    )
    source_connection: bool = Field(
        default=False,
        description="Add _source_connection column with connection name",
    )
    source_table: bool = Field(
        default=False,
        description="Add _source_table column with table/query name (SQL sources only)",
    )


# ============================================
# Connection Configurations
# ============================================


class BaseConnectionConfig(BaseModel):
    """Base configuration for all connections."""

    type: ConnectionType
    validation_mode: ValidationMode = ValidationMode.LAZY


class LocalConnectionConfig(BaseConnectionConfig):
    """
    Local filesystem connection.

    Example:
    ```yaml
    local_data:
      type: "local"
      base_path: "./data"
    ```
    """

    type: Literal[ConnectionType.LOCAL] = ConnectionType.LOCAL
    base_path: str = Field(default="./data", description="Base directory path")


# --- Azure Blob Auth ---


class AzureBlobAuthMode(str, Enum):
    ACCOUNT_KEY = "account_key"
    SAS = "sas"
    CONNECTION_STRING = "connection_string"
    KEY_VAULT = "key_vault"
    AAD_MSI = "aad_msi"


class AzureBlobKeyVaultAuth(BaseModel):
    mode: Literal[AzureBlobAuthMode.KEY_VAULT] = AzureBlobAuthMode.KEY_VAULT
    key_vault: str
    secret: str


class AzureBlobAccountKeyAuth(BaseModel):
    mode: Literal[AzureBlobAuthMode.ACCOUNT_KEY] = AzureBlobAuthMode.ACCOUNT_KEY
    account_key: str


class AzureBlobSasAuth(BaseModel):
    mode: Literal[AzureBlobAuthMode.SAS] = AzureBlobAuthMode.SAS
    sas_token: str


class AzureBlobConnectionStringAuth(BaseModel):
    mode: Literal[AzureBlobAuthMode.CONNECTION_STRING] = AzureBlobAuthMode.CONNECTION_STRING
    connection_string: str


class AzureBlobMsiAuth(BaseModel):
    mode: Literal[AzureBlobAuthMode.AAD_MSI] = AzureBlobAuthMode.AAD_MSI
    client_id: Optional[str] = None


AzureBlobAuthConfig = Annotated[
    Union[
        AzureBlobKeyVaultAuth,
        AzureBlobAccountKeyAuth,
        AzureBlobSasAuth,
        AzureBlobConnectionStringAuth,
        AzureBlobMsiAuth,
    ],
    Field(discriminator="mode"),
]


class AzureBlobConnectionConfig(BaseConnectionConfig):
    """
    Azure Blob Storage connection.

    Scenario 1: Prod with Key Vault-managed key
    ```yaml
    adls_bronze:
      type: "azure_blob"
      account_name: "myaccount"
      container: "bronze"
      auth:
        mode: "key_vault"
        key_vault: "kv-data"
        secret: "adls-account-key"
    ```

    Scenario 2: Local dev with inline account key
    ```yaml
    adls_dev:
      type: "azure_blob"
      account_name: "devaccount"
      container: "sandbox"
      auth:
        mode: "account_key"
        account_key: "${ADLS_ACCOUNT_KEY}"
    ```

    Scenario 3: MSI (no secrets)
    ```yaml
    adls_msi:
      type: "azure_blob"
      account_name: "myaccount"
      container: "bronze"
      auth:
        mode: "aad_msi"
        # optional: client_id for user-assigned identity
        client_id: "00000000-0000-0000-0000-000000000000"
    ```
    """

    type: Literal[ConnectionType.AZURE_BLOB] = ConnectionType.AZURE_BLOB
    account_name: str
    container: str
    auth: AzureBlobAuthConfig = Field(
        default_factory=lambda: AzureBlobMsiAuth(mode=AzureBlobAuthMode.AAD_MSI)
    )


class DeltaConnectionConfig(BaseConnectionConfig):
    """
    Delta Lake connection.

    Scenario 1: Delta via metastore
    ```yaml
    delta_silver:
      type: "delta"
      catalog: "spark_catalog"
      schema: "silver_db"
    ```

    Scenario 2: Direct path + Node usage
    ```yaml
    delta_local:
      type: "local"
      base_path: "dbfs:/mnt/delta"

    # In pipeline:
    # read:
    #   connection: "delta_local"
    #   format: "delta"
    #   path: "bronze/orders"
    ```
    """

    type: Literal[ConnectionType.DELTA] = ConnectionType.DELTA
    catalog: str = Field(description="Spark catalog name (e.g. 'spark_catalog')")
    schema_name: str = Field(alias="schema", description="Database/schema name")
    table: Optional[str] = Field(
        default=None,
        description="Optional default table name for this connection (used by story/pipeline helpers)",
    )


# --- SQL Server Auth ---


class SQLServerAuthMode(str, Enum):
    AAD_MSI = "aad_msi"
    AAD_PASSWORD = "aad_password"
    SQL_LOGIN = "sql_login"
    CONNECTION_STRING = "connection_string"


class SQLLoginAuth(BaseModel):
    mode: Literal[SQLServerAuthMode.SQL_LOGIN] = SQLServerAuthMode.SQL_LOGIN
    username: str
    password: str


class SQLAadPasswordAuth(BaseModel):
    mode: Literal[SQLServerAuthMode.AAD_PASSWORD] = SQLServerAuthMode.AAD_PASSWORD
    tenant_id: str
    client_id: str
    client_secret: str


class SQLMsiAuth(BaseModel):
    mode: Literal[SQLServerAuthMode.AAD_MSI] = SQLServerAuthMode.AAD_MSI
    client_id: Optional[str] = None


class SQLConnectionStringAuth(BaseModel):
    mode: Literal[SQLServerAuthMode.CONNECTION_STRING] = SQLServerAuthMode.CONNECTION_STRING
    connection_string: str


SQLServerAuthConfig = Annotated[
    Union[SQLLoginAuth, SQLAadPasswordAuth, SQLMsiAuth, SQLConnectionStringAuth],
    Field(discriminator="mode"),
]


class SQLServerConnectionConfig(BaseConnectionConfig):
    """
    SQL Server connection.

    Scenario 1: Managed identity (AAD MSI)
    ```yaml
    sql_dw_msi:
      type: "sql_server"
      host: "server.database.windows.net"
      database: "dw"
      auth:
        mode: "aad_msi"
    ```

    Scenario 2: SQL login
    ```yaml
    sql_dw_login:
      type: "sql_server"
      host: "server.database.windows.net"
      database: "dw"
      auth:
        mode: "sql_login"
        username: "dw_writer"
        password: "${DW_PASSWORD}"
    ```
    """

    type: Literal[ConnectionType.SQL_SERVER] = ConnectionType.SQL_SERVER
    host: str
    database: str
    port: int = 1433
    auth: SQLServerAuthConfig = Field(
        default_factory=lambda: SQLMsiAuth(mode=SQLServerAuthMode.AAD_MSI)
    )


# --- HTTP Auth ---


class HttpAuthMode(str, Enum):
    NONE = "none"
    BASIC = "basic"
    BEARER = "bearer"
    API_KEY = "api_key"


class HttpBasicAuth(BaseModel):
    mode: Literal[HttpAuthMode.BASIC] = HttpAuthMode.BASIC
    username: str
    password: str


class HttpBearerAuth(BaseModel):
    mode: Literal[HttpAuthMode.BEARER] = HttpAuthMode.BEARER
    token: str


class HttpApiKeyAuth(BaseModel):
    mode: Literal[HttpAuthMode.API_KEY] = HttpAuthMode.API_KEY
    header_name: str = "Authorization"
    value_template: str = "Bearer {token}"


class HttpNoAuth(BaseModel):
    mode: Literal[HttpAuthMode.NONE] = HttpAuthMode.NONE


HttpAuthConfig = Annotated[
    Union[HttpNoAuth, HttpBasicAuth, HttpBearerAuth, HttpApiKeyAuth],
    Field(discriminator="mode"),
]


class HttpConnectionConfig(BaseConnectionConfig):
    """
    HTTP connection.

    Scenario: Bearer token via env var
    ```yaml
    api_source:
      type: "http"
      base_url: "https://api.example.com"
      headers:
        User-Agent: "odibi-pipeline"
      auth:
        mode: "bearer"
        token: "${API_TOKEN}"
    ```
    """

    type: Literal[ConnectionType.HTTP] = ConnectionType.HTTP
    base_url: str
    headers: Dict[str, str] = Field(default_factory=dict)
    auth: HttpAuthConfig = Field(default_factory=lambda: HttpNoAuth(mode=HttpAuthMode.NONE))


class CustomConnectionConfig(BaseModel):
    """
    Configuration for custom/plugin connections.
    Allows any fields.
    """

    type: str
    validation_mode: ValidationMode = ValidationMode.LAZY
    # Allow extra fields
    model_config = {"extra": "allow"}


# Connection config discriminated union
ConnectionConfig = Union[
    LocalConnectionConfig,
    AzureBlobConnectionConfig,
    DeltaConnectionConfig,
    SQLServerConnectionConfig,
    HttpConnectionConfig,
    CustomConnectionConfig,
]


# ============================================
# Node Configurations
# ============================================


class ReadFormat(str, Enum):
    CSV = "csv"
    PARQUET = "parquet"
    DELTA = "delta"
    JSON = "json"
    SQL = "sql"


class TimeTravelConfig(BaseModel):
    """
    Configuration for time travel reading (Delta/Iceberg).

    Example:
    ```yaml
    time_travel:
      as_of_version: 10
      # OR
      as_of_timestamp: "2023-10-01T12:00:00Z"
    ```
    """

    as_of_version: Optional[int] = Field(
        default=None, description="Version number to time travel to"
    )
    as_of_timestamp: Optional[str] = Field(
        default=None, description="Timestamp string to time travel to"
    )

    @model_validator(mode="after")
    def check_one_method(self):
        if self.as_of_version is not None and self.as_of_timestamp is not None:
            raise ValueError("Specify either 'as_of_version' or 'as_of_timestamp', not both.")
        return self


class IncrementalUnit(str, Enum):
    """
    Time units for incremental lookback.

    Values:
    * `hour`
    * `day`
    * `month`
    * `year`
    """

    HOUR = "hour"
    DAY = "day"
    MONTH = "month"
    YEAR = "year"


class IncrementalMode(str, Enum):
    """Mode for incremental loading."""

    ROLLING_WINDOW = "rolling_window"  # Current default: WHERE col >= NOW() - lookback
    STATEFUL = "stateful"  # New: WHERE col > last_hwm


class IncrementalConfig(BaseModel):
    """
    Configuration for automatic incremental loading.

    Modes:
    1. **Rolling Window** (Default): Uses a time-based lookback from NOW().
       Good for: Stateless loading where you just want "recent" data.
       Args: `lookback`, `unit`

    2. **Stateful**: Tracks the High-Water Mark (HWM) of the key column.
       Good for: Exact incremental ingestion (e.g. CDC-like).
       Args: `state_key` (optional), `watermark_lag` (optional)

    Generates SQL:
    - Rolling: `WHERE column >= NOW() - lookback`
    - Stateful: `WHERE column > :last_hwm`

    Example (Rolling Window):
    ```yaml
    incremental:
      mode: "rolling_window"
      column: "updated_at"
      lookback: 3
      unit: "day"
    ```

    Example (Stateful HWM):
    ```yaml
    incremental:
      mode: "stateful"
      column: "id"
      # Optional: track separate column for HWM state
      state_key: "last_processed_id"
    ```

    Example (Stateful with Watermark Lag):
    ```yaml
    incremental:
      mode: "stateful"
      column: "updated_at"
      # Handle late-arriving data: look back 2 hours from HWM
      watermark_lag: "2h"
    ```
    """

    model_config = {"populate_by_name": True}

    mode: IncrementalMode = Field(
        default=IncrementalMode.ROLLING_WINDOW,
        description="Incremental strategy: 'rolling_window' or 'stateful'",
    )

    # Columns
    column: str = Field(
        alias="key_column", description="Primary column to filter on (e.g., updated_at)"
    )
    fallback_column: Optional[str] = Field(
        default=None,
        description="Backup column if primary is NULL (e.g., created_at). Generates COALESCE(col, fallback) >= ...",
    )

    # Rolling Window Args
    lookback: Optional[int] = Field(
        default=None, description="Time units to look back (Rolling Window only)"
    )
    unit: Optional[IncrementalUnit] = Field(
        default=None,
        description="Time unit for lookback (Rolling Window only). Options: 'hour', 'day', 'month', 'year'",
    )

    # Stateful Args
    state_key: Optional[str] = Field(
        default=None,
        description="Unique ID for state tracking. Defaults to node name if not provided.",
    )
    watermark_lag: Optional[str] = Field(
        default=None,
        description=(
            "Safety buffer for late-arriving data in stateful mode. "
            "Subtracts this duration from the stored HWM when filtering. "
            "Format: '<number><unit>' where unit is 's', 'm', 'h', or 'd'. "
            "Examples: '2h' (2 hours), '30m' (30 minutes), '1d' (1 day). "
            "Use when source has replication lag or eventual consistency."
        ),
    )

    @model_validator(mode="after")
    def check_mode_args(self):
        if self.mode == IncrementalMode.ROLLING_WINDOW:
            # Apply defaults if missing (Backward Compatibility)
            if self.lookback is None:
                self.lookback = 1
            if self.unit is None:
                self.unit = IncrementalUnit.DAY
        return self


class ReadConfig(BaseModel):
    """
    Configuration for reading data.

    ### 📖 "Universal Reader" Guide

    **Business Problem:**
    "I need to read from files, databases, streams, and even travel back in time to see how data looked yesterday."

    **Recipe 1: The Time Traveler (Delta/Iceberg)**
    *Reproduce a bug by seeing the data exactly as it was.*
    ```yaml
    read:
      connection: "silver_lake"
      format: "delta"
      table: "fact_sales"
      time_travel:
        as_of_timestamp: "2023-10-25T14:00:00Z"
    ```

    **Recipe 2: The Streamer**
    *Process data in real-time.*
    ```yaml
    read:
      connection: "event_hub"
      format: "json"
      streaming: true
    ```

    **Recipe 3: The SQL Query**
    *Push down filtering to the source database.*
    ```yaml
    read:
      connection: "enterprise_dw"
      format: "sql"
      # Use the query option to filter at source!
      query: "SELECT * FROM huge_table WHERE date >= '2024-01-01'"
    ```

    **Recipe 4: Archive Bad Records (Spark)**
    *Capture malformed records for later inspection.*
    ```yaml
    read:
      connection: "landing"
      format: "json"
      path: "events/*.json"
      archive_options:
        badRecordsPath: "/mnt/quarantine/bad_records"
    ```

    **Recipe 5: Optimize JDBC Parallelism (Spark)**
    *Control partition count for SQL sources to reduce task overhead.*
    ```yaml
    read:
      connection: "enterprise_dw"
      format: "sql"
      table: "small_lookup_table"
      options:
        numPartitions: 1  # Single partition for small tables
    ```

    **Performance Tip:** For small tables (<100K rows), use `numPartitions: 1` to avoid
    excessive Spark task scheduling overhead. For large tables, increase partitions
    to enable parallel reads (requires partitionColumn, lowerBound, upperBound).
    """

    connection: str = Field(description="Connection name from project.yaml")
    format: Union[ReadFormat, str] = Field(description="Data format (csv, parquet, delta, etc.)")
    table: Optional[str] = Field(default=None, description="Table name for SQL/Delta")
    path: Optional[str] = Field(default=None, description="Path for file-based sources")
    streaming: bool = Field(default=False, description="Enable streaming read (Spark only)")
    schema_ddl: Optional[str] = Field(
        default=None,
        description=(
            "Schema for streaming reads from file sources (required for Avro, JSON, CSV). "
            "Use Spark DDL format: 'col1 STRING, col2 INT, col3 TIMESTAMP'. "
            "Not required for Delta (schema is inferred from table metadata)."
        ),
    )
    query: Optional[str] = Field(
        default=None,
        description="SQL query to filter at source (pushdown). Mutually exclusive with table/path if supported by connector.",
    )
    incremental: Optional[IncrementalConfig] = Field(
        default=None,
        description="Automatic incremental loading strategy (CDC-like). If set, generates query based on target state (HWM).",
    )
    time_travel: Optional[TimeTravelConfig] = Field(
        default=None, description="Time travel options (Delta only)"
    )
    archive_options: Dict[str, Any] = Field(
        default_factory=dict,
        description="Options for archiving bad records (e.g. badRecordsPath for Spark)",
    )
    options: Dict[str, Any] = Field(default_factory=dict, description="Format-specific options")

    @model_validator(mode="after")
    def move_query_to_options(self):
        """Move top-level query to options."""
        if self.query:
            if "query" in self.options and self.options["query"] != self.query:
                raise ValueError("Cannot specify 'query' in both top-level and options")
            self.options["query"] = self.query
        return self

    @model_validator(mode="after")
    def check_table_or_path(self):
        """Ensure either table or path is provided."""
        # 1. Can't set both path and table
        if self.table and self.path:
            raise ValueError("ReadConfig: 'table' and 'path' are mutually exclusive.")

        # 2. Format-specific rules
        has_query = self.options and "query" in self.options

        if self.format == ReadFormat.SQL:
            if not (self.table or self.query or has_query):
                raise ValueError("ReadConfig: For format='sql', specify either 'table' or 'query'.")
        elif self.format in [ReadFormat.CSV, ReadFormat.PARQUET, ReadFormat.JSON]:
            if not self.path:
                # Some users might read from table/catalog even for parquet?
                # But usually file formats need path.
                pass

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
    """
    Configuration for transforming data.

    ### 🔧 "Transformation Pipeline" Guide

    **Business Problem:**
    "I have complex logic that mixes SQL for speed and Python for complex calculations."

    **The Solution:**
    Chain multiple steps together. Output of Step 1 becomes input of Step 2.

    **Function Registry:**
    The `function` step type looks up functions registered with `@transform` (or `@register`).
    This allows you to use the *same* registered functions as both top-level Transformers and steps in a chain.

    **Recipe: The Mix-and-Match**
    ```yaml
    transform:
      steps:
        # Step 1: SQL Filter (Fast)
        - sql: "SELECT * FROM df WHERE status = 'ACTIVE'"

        # Step 2: Custom Python Function (Complex Logic)
        # Looks up 'calculate_lifetime_value' in the registry
        - function: "calculate_lifetime_value"
          params: { discount_rate: 0.05 }

        # Step 3: Built-in Operation (Standard)
        - operation: "drop_duplicates"
          params: { subset: ["user_id"] }
    ```
    """

    steps: List[Union[str, TransformStep]] = Field(
        description="List of transformation steps (SQL strings or TransformStep configs)"
    )


class ValidationAction(str, Enum):
    FAIL = "fail"
    WARN = "warn"


class OnFailAction(str, Enum):
    ALERT = "alert"
    IGNORE = "ignore"


class TestType(str, Enum):
    NOT_NULL = "not_null"
    UNIQUE = "unique"
    ACCEPTED_VALUES = "accepted_values"
    ROW_COUNT = "row_count"
    CUSTOM_SQL = "custom_sql"
    RANGE = "range"
    REGEX_MATCH = "regex_match"
    VOLUME_DROP = "volume_drop"  # Phase 4.1: History-Aware
    SCHEMA = "schema"
    DISTRIBUTION = "distribution"
    FRESHNESS = "freshness"


class ContractSeverity(str, Enum):
    WARN = "warn"
    FAIL = "fail"
    QUARANTINE = "quarantine"


class BaseTestConfig(BaseModel):
    type: TestType
    name: Optional[str] = Field(default=None, description="Optional name for the check")
    on_fail: ContractSeverity = Field(
        default=ContractSeverity.FAIL, description="Action on failure"
    )


class VolumeDropTest(BaseTestConfig):
    """
    Checks if row count dropped significantly compared to history.
    Formula: (current - avg) / avg < -threshold
    """

    type: Literal[TestType.VOLUME_DROP] = TestType.VOLUME_DROP
    threshold: float = Field(default=0.5, description="Max allowed drop (0.5 = 50% drop)")
    lookback_days: int = Field(default=7, description="Days of history to average")


class NotNullTest(BaseTestConfig):
    """
    Ensures specified columns contain no null values.

    ```yaml
    contracts:
      - type: not_null
        columns: [customer_id, order_date]
    ```
    """

    type: Literal[TestType.NOT_NULL] = TestType.NOT_NULL
    columns: List[str] = Field(description="Columns that must not contain nulls")


class UniqueTest(BaseTestConfig):
    """
    Ensures specified columns (or combination) contain unique values.

    ```yaml
    contracts:
      - type: unique
        columns: [order_id]
    ```
    """

    type: Literal[TestType.UNIQUE] = TestType.UNIQUE
    columns: List[str] = Field(
        description="Columns that must be unique (composite key if multiple)"
    )


class AcceptedValuesTest(BaseTestConfig):
    """
    Ensures a column only contains values from an allowed list.

    ```yaml
    contracts:
      - type: accepted_values
        column: status
        values: [pending, approved, rejected]
    ```
    """

    type: Literal[TestType.ACCEPTED_VALUES] = TestType.ACCEPTED_VALUES
    column: str = Field(description="Column to check")
    values: List[Any] = Field(description="Allowed values")


class RowCountTest(BaseTestConfig):
    """
    Validates that row count falls within expected bounds.

    ```yaml
    contracts:
      - type: row_count
        min: 1000
        max: 100000
    ```
    """

    type: Literal[TestType.ROW_COUNT] = TestType.ROW_COUNT
    min: Optional[int] = Field(default=None, description="Minimum row count")
    max: Optional[int] = Field(default=None, description="Maximum row count")


class CustomSQLTest(BaseTestConfig):
    """
    Runs a custom SQL condition and fails if too many rows violate it.

    ```yaml
    contracts:
      - type: custom_sql
        condition: "amount > 0"
        threshold: 0.01  # Allow up to 1% failures
    ```
    """

    type: Literal[TestType.CUSTOM_SQL] = TestType.CUSTOM_SQL
    condition: str = Field(description="SQL condition that should be true for valid rows")
    threshold: float = Field(
        default=0.0, description="Failure rate threshold (0.0 = strictly no failures allowed)"
    )


class RangeTest(BaseTestConfig):
    """
    Ensures column values fall within a specified range.

    ```yaml
    contracts:
      - type: range
        column: age
        min: 0
        max: 150
    ```
    """

    type: Literal[TestType.RANGE] = TestType.RANGE
    column: str = Field(description="Column to check")
    min: Optional[Union[int, float, str]] = Field(
        default=None, description="Minimum value (inclusive)"
    )
    max: Optional[Union[int, float, str]] = Field(
        default=None, description="Maximum value (inclusive)"
    )


class RegexMatchTest(BaseTestConfig):
    """
    Ensures column values match a regex pattern.

    ```yaml
    contracts:
      - type: regex_match
        column: email
        pattern: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
    ```
    """

    type: Literal[TestType.REGEX_MATCH] = TestType.REGEX_MATCH
    column: str = Field(description="Column to check")
    pattern: str = Field(description="Regex pattern to match")


class SchemaContract(BaseTestConfig):
    """
    Validates that the DataFrame schema matches expected columns.

    Uses the `columns` metadata from NodeConfig to verify schema.

    ```yaml
    contracts:
      - type: schema
        strict: true  # Fail if extra columns present
    ```
    """

    type: Literal[TestType.SCHEMA] = TestType.SCHEMA
    strict: bool = Field(default=True, description="If true, fail on unexpected columns")
    on_fail: ContractSeverity = ContractSeverity.FAIL


class DistributionContract(BaseTestConfig):
    """
    Checks if a column's statistical distribution is within expected bounds.

    ```yaml
    contracts:
      - type: distribution
        column: price
        metric: mean
        threshold: ">100"  # Mean must be > 100
        on_fail: warn
    ```
    """

    type: Literal[TestType.DISTRIBUTION] = TestType.DISTRIBUTION
    column: str = Field(description="Column to analyze")
    metric: Literal["mean", "min", "max", "null_percentage"] = Field(
        description="Statistical metric to check"
    )
    threshold: str = Field(description="Threshold expression (e.g., '>100', '<0.05')")
    on_fail: ContractSeverity = ContractSeverity.WARN


class FreshnessContract(BaseTestConfig):
    """
    Ensures data is not stale by checking a timestamp column.

    ```yaml
    contracts:
      - type: freshness
        column: updated_at
        max_age: "24h"  # Data must be less than 24 hours old
    ```
    """

    type: Literal[TestType.FRESHNESS] = TestType.FRESHNESS
    column: str = Field(default="updated_at", description="Timestamp column to check")
    max_age: str = Field(description="Maximum allowed age (e.g., '24h', '7d')")
    on_fail: ContractSeverity = ContractSeverity.FAIL


TestConfig = Annotated[
    Union[
        NotNullTest,
        UniqueTest,
        AcceptedValuesTest,
        RowCountTest,
        CustomSQLTest,
        RangeTest,
        RegexMatchTest,
        VolumeDropTest,
        SchemaContract,
        DistributionContract,
        FreshnessContract,
    ],
    Field(discriminator="type"),
]


# ============================================
# Quarantine Configuration
# ============================================


class QuarantineColumnsConfig(BaseModel):
    """
    Columns added to quarantined rows for debugging and reprocessing.

    Example:
    ```yaml
    quarantine:
      connection: silver
      path: customers_quarantine
      add_columns:
        _rejection_reason: true
        _rejected_at: true
        _source_batch_id: true
        _failed_tests: true
        _original_node: false
    ```
    """

    rejection_reason: bool = Field(
        default=True,
        description="Add _rejection_reason column with test failure description",
    )
    rejected_at: bool = Field(
        default=True,
        description="Add _rejected_at column with UTC timestamp",
    )
    source_batch_id: bool = Field(
        default=True,
        description="Add _source_batch_id column with run ID for traceability",
    )
    failed_tests: bool = Field(
        default=True,
        description="Add _failed_tests column with comma-separated list of failed test names",
    )
    original_node: bool = Field(
        default=False,
        description="Add _original_node column with source node name",
    )


class QuarantineConfig(BaseModel):
    """
    Configuration for quarantine table routing.

    Routes rows that fail validation tests to a quarantine table
    with rejection metadata for later analysis/reprocessing.

    Example:
    ```yaml
    validation:
      tests:
        - type: not_null
          columns: [customer_id]
          on_fail: quarantine
      quarantine:
        connection: silver
        path: customers_quarantine
        add_columns:
          _rejection_reason: true
          _rejected_at: true
    ```
    """

    connection: str = Field(description="Connection for quarantine writes")
    path: Optional[str] = Field(default=None, description="Path for quarantine data")
    table: Optional[str] = Field(default=None, description="Table name for quarantine")
    add_columns: QuarantineColumnsConfig = Field(
        default_factory=QuarantineColumnsConfig,
        description="Metadata columns to add to quarantined rows",
    )
    retention_days: Optional[int] = Field(
        default=90,
        ge=1,
        description="Days to retain quarantined data (auto-cleanup)",
    )

    @model_validator(mode="after")
    def validate_destination(self):
        """Ensure either path or table is specified."""
        if not self.path and not self.table:
            raise ValueError("QuarantineConfig requires either 'path' or 'table'")
        return self


# ============================================
# Quality Gate Configuration
# ============================================


class GateOnFail(str, Enum):
    """
    Action when quality gate fails.

    Values:
    * `abort` - Stop pipeline, write nothing (default)
    * `warn_and_write` - Log warning, write all rows anyway
    * `write_valid_only` - Write only rows that passed validation
    """

    ABORT = "abort"
    WARN_AND_WRITE = "warn_and_write"
    WRITE_VALID_ONLY = "write_valid_only"


class GateThreshold(BaseModel):
    """
    Per-test threshold configuration for quality gates.

    Allows setting different pass rate requirements for specific tests.

    Example:
    ```yaml
    gate:
      thresholds:
        - test: not_null
          min_pass_rate: 0.99
        - test: unique
          min_pass_rate: 1.0
    ```
    """

    test: str = Field(description="Test name or type to apply threshold to")
    min_pass_rate: float = Field(
        ge=0.0,
        le=1.0,
        description="Minimum pass rate required (0.0-1.0, e.g., 0.99 = 99%)",
    )


class RowCountGate(BaseModel):
    """
    Row count anomaly detection for quality gates.

    Validates that batch size falls within expected bounds and
    detects significant changes from previous runs.

    Example:
    ```yaml
    gate:
      row_count:
        min: 100
        max: 1000000
        change_threshold: 0.5
    ```
    """

    min: Optional[int] = Field(default=None, ge=0, description="Minimum expected row count")
    max: Optional[int] = Field(default=None, ge=0, description="Maximum expected row count")
    change_threshold: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Max allowed change vs previous run (e.g., 0.5 = 50% change triggers failure)",
    )


class GateConfig(BaseModel):
    """
    Quality gate configuration for batch-level validation.

    Gates evaluate the entire batch before writing, ensuring
    data quality thresholds are met.

    Example:
    ```yaml
    gate:
      require_pass_rate: 0.95
      on_fail: abort
      thresholds:
        - test: not_null
          min_pass_rate: 0.99
      row_count:
        min: 100
        change_threshold: 0.5
    ```
    """

    require_pass_rate: float = Field(
        default=0.95,
        ge=0.0,
        le=1.0,
        description="Minimum percentage of rows passing ALL tests",
    )
    on_fail: GateOnFail = Field(
        default=GateOnFail.ABORT,
        description="Action when gate fails",
    )
    thresholds: List[GateThreshold] = Field(
        default_factory=list,
        description="Per-test thresholds (overrides global require_pass_rate)",
    )
    row_count: Optional[RowCountGate] = Field(
        default=None,
        description="Row count anomaly detection",
    )


class ValidationConfig(BaseModel):
    """
    Configuration for data validation (Quality Gate).

    ### 🛡️ "The Indestructible Pipeline" Pattern

    **Business Problem:**
    "Bad data polluted our Gold reports, causing executives to make wrong decisions. We need to stop it *before* it lands."

    **The Solution:**
    A Quality Gate that runs *after* transformation but *before* writing.

    **Recipe: The Quality Gate**
    ```yaml
    validation:
      mode: "fail"          # fail (stop pipeline) or warn (log only)
      on_fail: "alert"      # alert or ignore

      tests:
        # 1. Completeness
        - type: "not_null"
          columns: ["transaction_id", "customer_id"]

        # 2. Integrity
        - type: "unique"
          columns: ["transaction_id"]

        - type: "accepted_values"
          column: "status"
          values: ["PENDING", "COMPLETED", "FAILED"]

        # 3. Ranges & Patterns
        - type: "range"
          column: "age"
          min: 18
          max: 120

        - type: "regex_match"
          column: "email"
          pattern: "^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$"

        # 4. Business Logic (SQL)
        - type: "custom_sql"
          name: "dates_ordered"
          condition: "created_at <= completed_at"
          threshold: 0.01   # Allow 1% failure
    ```

    **Recipe: Quarantine + Gate**
    ```yaml
    validation:
      tests:
        - type: not_null
          columns: [customer_id]
          on_fail: quarantine
      quarantine:
        connection: silver
        path: customers_quarantine
      gate:
        require_pass_rate: 0.95
        on_fail: abort
    ```
    """

    mode: ValidationAction = Field(
        default=ValidationAction.FAIL,
        description="Execution mode: 'fail' (stop pipeline) or 'warn' (log only)",
    )
    on_fail: OnFailAction = Field(
        default=OnFailAction.ALERT,
        description="Action on failure: 'alert' (send notification) or 'ignore'",
    )
    tests: List[TestConfig] = Field(default_factory=list, description="List of validation tests")
    quarantine: Optional[QuarantineConfig] = Field(
        default=None,
        description="Quarantine configuration for failed rows",
    )
    gate: Optional[GateConfig] = Field(
        default=None,
        description="Quality gate configuration for batch-level validation",
    )


class AutoOptimizeConfig(BaseModel):
    """
    Configuration for Delta Lake automatic optimization.

    Example:
    ```yaml
    auto_optimize:
      enabled: true
      vacuum_retention_hours: 168
    ```
    """

    enabled: bool = Field(default=True, description="Enable auto optimization")
    vacuum_retention_hours: int = Field(
        default=168,
        description="Hours to retain history for VACUUM (default 7 days). Set to 0 to disable VACUUM.",
    )


class TriggerConfig(BaseModel):
    """
    Configuration for streaming trigger intervals.

    Specify exactly one of the trigger options.

    Example:
    ```yaml
    trigger:
      processing_time: "10 seconds"
    ```

    Or for one-time processing:
    ```yaml
    trigger:
      once: true
    ```
    """

    processing_time: Optional[str] = Field(
        default=None,
        description="Trigger interval as duration string (e.g., '10 seconds', '1 minute')",
    )
    once: Optional[bool] = Field(
        default=None,
        description="Process all available data once and stop",
    )
    available_now: Optional[bool] = Field(
        default=None,
        description="Process all available data in multiple batches, then stop",
    )
    continuous: Optional[str] = Field(
        default=None,
        description="Continuous processing with checkpoint interval (e.g., '1 second')",
    )

    @model_validator(mode="after")
    def check_exactly_one_trigger(self):
        """Ensure exactly one trigger type is specified."""
        triggers = [
            self.processing_time is not None,
            self.once is True,
            self.available_now is True,
            self.continuous is not None,
        ]
        if sum(triggers) > 1:
            raise ValueError(
                "TriggerConfig: specify exactly one of 'processing_time', 'once', "
                "'available_now', or 'continuous'"
            )
        return self


class StreamingWriteConfig(BaseModel):
    """
    Configuration for Spark Structured Streaming writes.

    ### 🚀 "Real-Time Pipeline" Guide

    **Business Problem:**
    "I need to process data continuously as it arrives from Kafka/Event Hubs
    and write it to Delta Lake in near real-time."

    **The Solution:**
    Configure streaming write with checkpoint location for fault tolerance
    and trigger interval for processing frequency.

    **Recipe: Streaming Ingestion**
    ```yaml
    write:
      connection: "silver_lake"
      format: "delta"
      table: "events_stream"
      streaming:
        output_mode: append
        checkpoint_location: "/checkpoints/events_stream"
        trigger:
          processing_time: "10 seconds"
    ```

    **Recipe: One-Time Streaming (Batch-like)**
    ```yaml
    write:
      connection: "silver_lake"
      format: "delta"
      table: "events_batch"
      streaming:
        output_mode: append
        checkpoint_location: "/checkpoints/events_batch"
        trigger:
          available_now: true
    ```
    """

    output_mode: Literal["append", "update", "complete"] = Field(
        default="append",
        description=(
            "Output mode for streaming writes. "
            "'append' - Only new rows. 'update' - Updated rows only. "
            "'complete' - Entire result table (requires aggregation)."
        ),
    )
    checkpoint_location: str = Field(
        description=(
            "Path for streaming checkpoints. Required for fault tolerance. "
            "Must be a reliable storage location (e.g., cloud storage, DBFS)."
        ),
    )
    trigger: Optional[TriggerConfig] = Field(
        default=None,
        description=(
            "Trigger configuration. If not specified, processes data as fast as possible. "
            "Use 'processing_time' for micro-batch intervals, 'once' for single batch, "
            "'available_now' for processing all available data then stopping."
        ),
    )
    query_name: Optional[str] = Field(
        default=None,
        description="Name for the streaming query (useful for monitoring and debugging)",
    )
    await_termination: Optional[bool] = Field(
        default=False,
        description=(
            "Wait for the streaming query to terminate. "
            "Set to True for batch-like streaming with 'once' or 'available_now' triggers."
        ),
    )
    timeout_seconds: Optional[int] = Field(
        default=None,
        description=(
            "Timeout in seconds when await_termination is True. If None, waits indefinitely."
        ),
    )


class WriteConfig(BaseModel):
    """
    Configuration for writing data.

    ### 🚀 "Big Data Performance" Guide

    **Business Problem:**
    "My dashboards are slow because the query scans terabytes of data just to find one day's sales."

    **The Solution:**
    Use **Partitioning** for coarse filtering (skipping huge chunks) and **Z-Ordering** for fine-grained skipping (colocating related data).

    **Recipe: Lakehouse Optimized**
    ```yaml
    write:
      connection: "gold_lake"
      format: "delta"
      table: "fact_sales"
      mode: "append"

      # 1. Partitioning: Physical folders.
      # Use for low-cardinality columns often used in WHERE clauses.
      # WARNING: Do NOT partition by high-cardinality cols like ID or Timestamp!
      partition_by: ["country_code", "txn_year_month"]

      # 2. Z-Ordering: Data clustering.
      # Use for high-cardinality columns often used in JOINs or predicates.
      zorder_by: ["customer_id", "product_id"]

      # 3. Table Properties: Engine tuning.
      table_properties:
        "delta.autoOptimize.optimizeWrite": "true"
        "delta.autoOptimize.autoCompact": "true"
    ```
    """

    connection: str = Field(description="Connection name from project.yaml")
    format: Union[ReadFormat, str] = Field(description="Output format (csv, parquet, delta, etc.)")
    table: Optional[str] = Field(default=None, description="Table name for SQL/Delta")
    path: Optional[str] = Field(default=None, description="Path for file-based outputs")
    register_table: Optional[str] = Field(
        default=None, description="Register file output as external table (Spark/Delta only)"
    )
    mode: WriteMode = Field(
        default=WriteMode.OVERWRITE,
        description="Write mode. Options: 'overwrite', 'append', 'upsert', 'append_once'",
    )
    partition_by: List[str] = Field(
        default_factory=list,
        description="List of columns to physically partition the output by (folder structure). Use for low-cardinality columns (e.g. date, country).",
    )
    zorder_by: List[str] = Field(
        default_factory=list,
        description="List of columns to Z-Order by. Improves read performance for high-cardinality columns used in filters/joins (Delta only).",
    )
    table_properties: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Delta table properties. Overrides global performance.delta_table_properties. "
            "Example: {'delta.columnMapping.mode': 'name'} to allow special characters in column names."
        ),
    )
    merge_schema: bool = Field(
        default=False, description="Allow schema evolution (mergeSchema option in Delta)"
    )
    first_run_query: Optional[str] = Field(
        default=None,
        description=(
            "SQL query for full-load on first run (High Water Mark pattern). "
            "If set, uses this query when target table doesn't exist, then switches to incremental. "
            "Only applies to SQL reads."
        ),
    )
    options: Dict[str, Any] = Field(default_factory=dict, description="Format-specific options")
    auto_optimize: Optional[Union[bool, AutoOptimizeConfig]] = Field(
        default=None,
        description="Auto-run OPTIMIZE and VACUUM after write (Delta only)",
    )
    add_metadata: Optional[Union[bool, WriteMetadataConfig]] = Field(
        default=None,
        description=(
            "Add metadata columns for Bronze layer lineage. "
            "Set to `true` to add all applicable columns, or provide a WriteMetadataConfig for selective columns. "
            "Columns: _extracted_at, _source_file (file sources), _source_connection, _source_table (SQL sources)."
        ),
    )
    skip_if_unchanged: bool = Field(
        default=False,
        description=(
            "Skip write if DataFrame content is identical to previous write. "
            "Computes SHA256 hash of entire DataFrame and compares to stored hash in Delta table metadata. "
            "Useful for snapshot tables without timestamps to avoid redundant appends. "
            "Only supported for Delta format."
        ),
    )
    skip_hash_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "Columns to include in hash computation for skip_if_unchanged. "
            "If None, all columns are used. Specify a subset to ignore volatile columns like timestamps."
        ),
    )
    skip_hash_sort_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "Columns to sort by before hashing for deterministic comparison. "
            "Required if row order may vary between runs. Typically your business key columns."
        ),
    )
    streaming: Optional[StreamingWriteConfig] = Field(
        default=None,
        description=(
            "Streaming write configuration for Spark Structured Streaming. "
            "When set, uses writeStream instead of batch write. "
            "Requires a streaming DataFrame from a streaming read source."
        ),
    )

    @model_validator(mode="after")
    def check_table_or_path(self):
        """Ensure either table or path is provided."""
        if not self.table and not self.path:
            raise ValueError("Either 'table' or 'path' must be provided for write config")
        if self.table and self.path:
            raise ValueError("WriteConfig: 'table' and 'path' are mutually exclusive.")
        return self


class ColumnMetadata(BaseModel):
    """Metadata for a column in the data dictionary."""

    description: Optional[str] = Field(default=None, description="Column description")
    pii: bool = Field(default=False, description="Contains PII?")
    tags: List[str] = Field(
        default_factory=list, description="Tags (e.g. 'business_key', 'measure')"
    )


class SchemaMode(str, Enum):
    ENFORCE = "enforce"
    EVOLVE = "evolve"


class OnNewColumns(str, Enum):
    IGNORE = "ignore"
    FAIL = "fail"
    ADD_NULLABLE = "add_nullable"


class OnMissingColumns(str, Enum):
    FAIL = "fail"
    FILL_NULL = "fill_null"


class PrivacyMethod(str, Enum):
    """Supported privacy anonymization methods."""

    HASH = "hash"  # SHA256 hash
    MASK = "mask"  # Mask all but last 4 chars
    REDACT = "redact"  # Replace with [REDACTED]


class PrivacyConfig(BaseModel):
    """
    Configuration for PII anonymization.

    Example:
    ```yaml
    privacy:
      method: "hash"
      salt: "my_secret_salt"
    ```
    """

    method: PrivacyMethod = Field(
        ..., description="Anonymization method. Options: 'hash', 'mask', 'redact'"
    )
    salt: Optional[str] = Field(
        default=None,
        description="Salt for hashing (optional but recommended). Combined with value before hashing.",
    )
    declassify: List[str] = Field(
        default_factory=list,
        description="List of columns to declassify (remove from PII inheritance).",
    )


class SchemaPolicyConfig(BaseModel):
    """
    Configuration for Schema Management (Drift Handling).

    Controls how the node handles differences between input data and target table schema.
    """

    mode: SchemaMode = Field(
        default=SchemaMode.ENFORCE, description="Schema evolution mode: 'enforce' or 'evolve'"
    )
    on_new_columns: Optional[OnNewColumns] = Field(
        default=None,
        description="Action for new columns in input: 'ignore', 'fail', 'add_nullable'",
    )
    on_missing_columns: OnMissingColumns = Field(
        default=OnMissingColumns.FILL_NULL,
        description="Action for missing columns in input: 'fail', 'fill_null'",
    )

    @model_validator(mode="after")
    def set_defaults(self):
        if self.mode == SchemaMode.EVOLVE:
            if self.on_new_columns is None:
                self.on_new_columns = OnNewColumns.ADD_NULLABLE
        else:  # ENFORCE
            if self.on_new_columns is None:
                self.on_new_columns = OnNewColumns.IGNORE
        return self


class NodeConfig(BaseModel):
    """
    Configuration for a single node.

    ### 🧠 "The Smart Node" Pattern

    **Business Problem:**
    "We need complex dependencies, caching for heavy computations, and the ability to run only specific parts of the pipeline."

    **The Solution:**
    Nodes are the building blocks. They handle dependencies (`depends_on`), execution control (`tags`, `enabled`), and performance (`cache`).

    ### 🕸️ DAG & Dependencies
    **The Glue of the Pipeline.**
    Nodes don't run in isolation. They form a Directed Acyclic Graph (DAG).

    *   **`depends_on`**: Critical! If Node B reads from Node A (in memory), you MUST list `["Node A"]`.
        *   *Implicit Data Flow*: If a node has no `read` block, it automatically picks up the DataFrame from its first dependency.

    ### 🧠 Smart Read & Incremental Loading

    **Automated History Management.**

    Odibi intelligently determines whether to perform a **Full Load** or an **Incremental Load** based on the state of the target.

    **The "Smart Read" Logic:**
    1.  **First Run (Full Load):** If the target table (defined in `write`) does **not exist**:
        *   Incremental filtering rules are **ignored**.
        *   The entire source dataset is read.
        *   Use `write.first_run_query` (optional) to override the read query for this initial bootstrap (e.g., to backfill only 1 year of history instead of all time).

    2.  **Subsequent Runs (Incremental Load):** If the target table **exists**:
        *   **Rolling Window:** Filters source data where `column >= NOW() - lookback`.
        *   **Stateful:** Filters source data where `column > last_high_water_mark`.

    This ensures you don't need separate "init" and "update" pipelines. One config handles both lifecycle states.

    ### 🏷️ Orchestration Tags
    **Run What You Need.**
    Tags allow you to execute slices of your pipeline.
    *   `odibi run --tag daily` -> Runs all nodes with "daily" tag.
    *   `odibi run --tag critical` -> Runs high-priority nodes.

    ### 🤖 Choosing Your Logic: Transformer vs. Transform

    **1. The "Transformer" (Top-Level)**
    *   **What it is:** A pre-packaged, heavy-duty operation that defines the *entire purpose* of the node.
    *   **When to use:** When applying a standard Data Engineering pattern (e.g., SCD2, Merge, Deduplicate).
    *   **Analogy:** "Run this App."
    *   **Syntax:** `transformer: "scd2"` + `params: {...}`

    **2. The "Transform Steps" (Process Chain)**
    *   **What it is:** A sequence of smaller steps (SQL, functions, operations) executed in order.
    *   **When to use:** For custom business logic, data cleaning, or feature engineering pipelines.
    *   **Analogy:** "Run this Script."
    *   **Syntax:** `transform: { steps: [...] }`

    *Note: You can use both! The `transformer` runs first, then `transform` steps refine the result.*

    ### 🔗 Chaining Operations
    **You can mix and match!**
    The execution order is always:
    1.  **Read** (or Dependency Injection)
    2.  **Transformer** (The "App" logic, e.g., Deduplicate)
    3.  **Transform Steps** (The "Script" logic, e.g., cleanup)
    4.  **Validation**
    5.  **Write**

    *Constraint:* You must define **at least one** of `read`, `transformer`, `transform`, or `write`.

    ### ⚡ Example: App vs. Script

    **Scenario 1: The Full ETL Flow (Chained)**
    *Shows explicit Read, Transform Chain, and Write.*

    ```yaml
    # 1. Ingest (The Dependency)
    - name: "load_raw_users"
      read: { connection: "s3_landing", format: "json", path: "users/*.json" }
      write: { connection: "bronze", format: "parquet", path: "users_raw" }

    # 2. Process (The Consumer)
    - name: "clean_users"
      depends_on: ["load_raw_users"]

      # "clean_text" is a registered function from the Transformer Catalog
      transform:
        steps:
          - sql: "SELECT * FROM df WHERE email IS NOT NULL"
          - function: "clean_text"
            params: { columns: ["email"], case: "lower" }

      write: { connection: "silver", format: "delta", table: "dim_users" }
    ```

    **Scenario 2: The "App" Node (Top-Level Transformer)**
    *Shows a node that applies a pattern (Deduplicate) to incoming data.*

    ```yaml
    - name: "deduped_users"
      depends_on: ["clean_users"]

      # The "App": Deduplication (From Transformer Catalog)
      transformer: "deduplicate"
      params:
        keys: ["user_id"]
        order_by: "updated_at DESC"

      write: { connection: "gold", format: "delta", table: "users_unique" }
    ```

    **Scenario 3: The Tagged Runner (Reporting)**
    *Shows how tags allow running specific slices (e.g., `odibi run --tag daily`).*

    ```yaml
    - name: "daily_report"
      tags: ["daily", "reporting"]
      depends_on: ["deduped_users"]

      # Ad-hoc aggregation script
      transform:
        steps:
          - sql: "SELECT date_trunc('day', updated_at) as day, count(*) as total FROM df GROUP BY 1"

      write: { connection: "local_data", format: "csv", path: "reports/daily_stats.csv" }
    ```

    **Scenario 4: The "Kitchen Sink" (All Operations)**
    *Shows Read -> Transformer -> Transform -> Write execution order.*

    **Why this works:**
    1.  **Internal Chaining (`df`):** In every step (Transformer or SQL), `df` refers to the output of the *previous* step.
    2.  **External Access (`depends_on`):** If you added `depends_on: ["other_node"]`, you could also run `SELECT * FROM other_node` in your SQL steps!

    ```yaml
    - name: "complex_flow"
      # 1. Read -> Creates initial 'df'
      read: { connection: "bronze", format: "parquet", path: "users" }

      # 2. Transformer (The "App": Deduplicate first)
      # Takes 'df' (from Read), dedups it, returns new 'df'
      transformer: "deduplicate"
      params: { keys: ["user_id"], order_by: "updated_at DESC" }

      # 3. Transform Steps (The "Script": Filter AFTER deduplication)
      # SQL sees the deduped data as 'df'
      transform:
        steps:
          - sql: "SELECT * FROM df WHERE status = 'active'"

      # 4. Write -> Saves the final filtered 'df'
      write: { connection: "silver", format: "delta", table: "active_unique_users" }
    ```

    ### 📚 Transformer Catalog

    These are the built-in functions you can use in two ways:

    1.  **As a Top-Level Transformer:** `transformer: "name"` (Defines the node's main logic)
    2.  **As a Step in a Chain:** `transform: { steps: [{ function: "name" }] }` (Part of a sequence)

    *Note: `merge` and `scd2` are special "Heavy Lifters" and should generally be used as Top-Level Transformers.*

    **Data Engineering Patterns**
    *   `merge`: Upsert/Merge into target (Delta/SQL). *([Params](#mergeparams))*
    *   `scd2`: Slowly Changing Dimensions Type 2. *([Params](#scd2params))*
    *   `deduplicate`: Remove duplicates using window functions. *([Params](#deduplicateparams))*

    **Relational Algebra**
    *   `join`: Join two datasets. *([Params](#joinparams))*
    *   `union`: Stack datasets vertically. *([Params](#unionparams))*
    *   `pivot`: Rotate rows to columns. *([Params](#pivotparams))*
    *   `unpivot`: Rotate columns to rows (melt). *([Params](#unpivotparams))*
    *   `aggregate`: Group by and sum/count/avg. *([Params](#aggregateparams))*

    **Data Quality & Cleaning**
    *   `validate_and_flag`: Check rules and flag invalid rows. *([Params](#validateandflagparams))*
    *   `clean_text`: Trim and normalize case. *([Params](#cleantextparams))*
    *   `filter_rows`: SQL-based filtering. *([Params](#filterrowsparams))*
    *   `fill_nulls`: Replace NULLs with defaults. *([Params](#fillnullsparams))*

    **Feature Engineering**
    *   `derive_columns`: Create new cols via SQL expressions. *([Params](#derivecolumnsparams))*
    *   `case_when`: Conditional logic (if-else). *([Params](#casewhenparams))*
    *   `generate_surrogate_key`: Create MD5 keys from columns. *([Params](#surrogatekeyparams))*
    *   `date_diff`, `date_add`, `date_trunc`: Date arithmetic.

    **Scenario 1: The Full ETL Flow**
    *(Show two nodes: one loader, one processor)*

    ```yaml
    # 1. Ingest (The Dependency)
    - name: "load_raw_users"
      read: { connection: "s3_landing", format: "json", path: "users/*.json" }
      write: { connection: "bronze", format: "parquet", path: "users_raw" }

    # 2. Process (The Consumer)
    - name: "clean_users"
      depends_on: ["load_raw_users"]  # <--- Explicit dependency

      # Explicit Transformation Steps
      transform:
        steps:
          - sql: "SELECT * FROM df WHERE email IS NOT NULL"
          - function: "clean_text"
            params: { columns: ["email"], case: "lower" }

      write: { connection: "silver", format: "delta", table: "dim_users" }
    ```

    **Scenario 2: The "App" Node (Transformer)**
    *(Show a node that is a Transformer, no read needed if it picks up from dependency)*

    ```yaml
    - name: "deduped_users"
      depends_on: ["clean_users"]

      # The "App": Deduplication
      transformer: "deduplicate"
      params:
        keys: ["user_id"]
        order_by: "updated_at DESC"

      write: { connection: "gold", format: "delta", table: "users_unique" }
    ```

    **Scenario 3: The Tagged Runner**
    *Run only this with `odibi run --tag daily`*
    ```yaml
    - name: "daily_report"
      tags: ["daily", "reporting"]
      # ...
    ```

    **Scenario 4: Pre/Post SQL Hooks**
    *Setup and cleanup with SQL statements.*
    ```yaml
    - name: "optimize_sales"
      depends_on: ["load_sales"]
      pre_sql:
        - "SET spark.sql.shuffle.partitions = 200"
        - "CREATE TEMP VIEW staging AS SELECT * FROM bronze.raw_sales"
      transform:
        steps:
          - sql: "SELECT * FROM staging WHERE amount > 0"
      post_sql:
        - "OPTIMIZE gold.fact_sales ZORDER BY (customer_id)"
        - "VACUUM gold.fact_sales RETAIN 168 HOURS"
      write:
        connection: "gold"
        format: "delta"
        table: "fact_sales"
    ```

    **Scenario 5: Materialization Strategies**
    *Choose how output is persisted.*
    ```yaml
    # Option 1: View (no physical storage, logical model)
    - name: "vw_active_customers"
      materialized: "view"  # Creates SQL view instead of table
      transform:
        steps:
          - sql: "SELECT * FROM customers WHERE status = 'active'"
      write:
        connection: "gold"
        table: "vw_active_customers"

    # Option 2: Incremental (append to existing Delta table)
    - name: "fact_events"
      materialized: "incremental"  # Uses APPEND mode
      read:
        connection: "bronze"
        table: "raw_events"
        incremental:
          mode: "stateful"
          column: "event_time"
      write:
        connection: "silver"
        format: "delta"
        table: "fact_events"

    # Option 3: Table (default - full overwrite)
    - name: "dim_products"
      materialized: "table"  # Default behavior
      # ...
    ```
    """

    name: str = Field(description="Unique node name")
    description: Optional[str] = Field(default=None, description="Human-readable description")
    enabled: bool = Field(default=True, description="If False, node is skipped during execution")
    tags: List[str] = Field(
        default_factory=list,
        description="Operational tags for selective execution (e.g., 'daily', 'critical'). Use with `odibi run --tag`.",
    )
    depends_on: List[str] = Field(
        default_factory=list,
        description="List of parent nodes that must complete before this node runs. The output of these nodes is available for reading.",
    )

    columns: Dict[str, ColumnMetadata] = Field(
        default_factory=dict,
        description="Data Dictionary defining the output schema. Used for documentation, PII tagging, and validation.",
    )

    # Operations (at least one required)
    read: Optional[ReadConfig] = Field(
        default=None,
        description="Input operation (Load). If missing, data is taken from the first dependency.",
    )
    transform: Optional[TransformConfig] = Field(
        default=None,
        description="Chain of fine-grained transformation steps (SQL, functions). Runs after 'transformer' if both are present.",
    )
    write: Optional[WriteConfig] = Field(
        default=None, description="Output operation (Save to file/table)."
    )
    streaming: bool = Field(
        default=False, description="Enable streaming execution for this node (Spark only)"
    )
    transformer: Optional[str] = Field(
        default=None,
        description="Name of the 'App' logic to run (e.g., 'deduplicate', 'scd2'). See Transformer Catalog for options.",
    )
    params: Dict[str, Any] = Field(default_factory=dict, description="Parameters for transformer")

    # Optional features
    pre_sql: List[str] = Field(
        default_factory=list,
        description=(
            "List of SQL statements to execute before node runs. "
            "Use for setup: temp tables, variable initialization, grants. "
            "Example: ['SET spark.sql.shuffle.partitions=200', "
            "'CREATE TEMP VIEW src AS SELECT * FROM raw']"
        ),
    )
    post_sql: List[str] = Field(
        default_factory=list,
        description=(
            "List of SQL statements to execute after node completes. "
            "Use for cleanup, optimization, or audit logging. "
            "Example: ['OPTIMIZE gold.fact_sales', 'VACUUM gold.fact_sales RETAIN 168 HOURS']"
        ),
    )
    materialized: Optional[Literal["table", "view", "incremental"]] = Field(
        default=None,
        description=(
            "Materialization strategy. Options: "
            "'table' (default physical write), "
            "'view' (creates SQL view instead of table), "
            "'incremental' (uses append mode for Delta tables). "
            "Views are useful for Gold layer logical models."
        ),
    )

    cache: bool = Field(default=False, description="Cache result for reuse")
    log_level: Optional[LogLevel] = Field(
        default=None, description="Override log level for this node"
    )
    on_error: ErrorStrategy = Field(
        default=ErrorStrategy.FAIL_LATER, description="Failure handling strategy"
    )
    validation: Optional[ValidationConfig] = None
    contracts: List[TestConfig] = Field(
        default_factory=list,
        description="Pre-condition contracts (Circuit Breakers). Runs on input data before transformation.",
    )
    schema_policy: Optional[SchemaPolicyConfig] = Field(
        default=None, description="Schema drift handling policy"
    )
    privacy: Optional[PrivacyConfig] = Field(
        default=None, description="Privacy Suite: PII anonymization settings"
    )
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

    @model_validator(mode="after")
    def check_transformer_params(self):
        if self.transformer and not self.params:
            raise ValueError(
                f"Node '{self.name}': 'transformer' is set but 'params' is empty. "
                "Either remove transformer or provide matching params."
            )
        return self


# ============================================
# Pipeline Configuration
# ============================================


class PipelineConfig(BaseModel):
    """
    Configuration for a pipeline.

    Example:
    ```yaml
    pipelines:
      - pipeline: "user_onboarding"
        description: "Ingest and process new users"
        layer: "silver"
        nodes:
          - name: "node1"
            ...
    ```
    """

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


class BackoffStrategy(str, Enum):
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    CONSTANT = "constant"


class RetryConfig(BaseModel):
    """
    Retry configuration.

    Example:
    ```yaml
    retry:
      enabled: true
      max_attempts: 3
      backoff: "exponential"
    ```
    """

    enabled: bool = True
    max_attempts: int = Field(default=3, ge=1, le=10)
    backoff: BackoffStrategy = Field(default=BackoffStrategy.EXPONENTIAL)


class LoggingConfig(BaseModel):
    """
    Logging configuration.

    Example:
    ```yaml
    logging:
      level: "INFO"
      structured: true
    ```
    """

    level: LogLevel = LogLevel.INFO
    structured: bool = Field(default=False, description="Output JSON logs")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Extra metadata in logs")


class PerformanceConfig(BaseModel):
    """
    Performance tuning configuration.

    Example:
    ```yaml
    performance:
      use_arrow: true
      spark_config:
        "spark.sql.shuffle.partitions": "200"
        "spark.sql.adaptive.enabled": "true"
        "spark.databricks.delta.optimizeWrite.enabled": "true"
      delta_table_properties:
        "delta.columnMapping.mode": "name"
    ```

    **Spark Config Notes:**
    - Configs are applied via `spark.conf.set()` at runtime
    - For existing sessions (e.g., Databricks), only runtime-settable configs will take effect
    - Session-level configs (e.g., `spark.executor.memory`) require session restart
    - Common runtime-safe configs: shuffle partitions, adaptive query execution, Delta optimizations
    """

    use_arrow: bool = Field(
        default=True,
        description="Use Apache Arrow-backed DataFrames (Pandas only). Reduces memory and speeds up I/O.",
    )
    spark_config: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Spark configuration settings applied at runtime via spark.conf.set(). "
            "Example: {'spark.sql.shuffle.partitions': '200', 'spark.sql.adaptive.enabled': 'true'}. "
            "Note: Some configs require session restart and cannot be set at runtime."
        ),
    )
    delta_table_properties: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Default table properties applied to all Delta writes. "
            "Example: {'delta.columnMapping.mode': 'name'} to allow special characters in column names."
        ),
    )
    skip_null_profiling: bool = Field(
        default=False,
        description=(
            "Skip null profiling in metadata collection phase. "
            "Reduces execution time for large DataFrames by avoiding an additional Spark job."
        ),
    )


class StoryConfig(BaseModel):
    """
    Story generation configuration.

    Stories are ODIBI's core value - execution reports with lineage.
    They must use a connection for consistent, traceable output.

    Example:
    ```yaml
    story:
      connection: "local_data"
      path: "stories/"
      retention_days: 30
      failure_sample_size: 100
      max_failure_samples: 500
      max_sampled_validations: 5
    ```

    **Failure Sample Settings:**
    - `failure_sample_size`: Number of failed rows to capture per validation (default: 100)
    - `max_failure_samples`: Total failed rows across all validations (default: 500)
    - `max_sampled_validations`: After this many validations, show only counts (default: 5)
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

    # Failure sample settings (troubleshooting)
    failure_sample_size: int = Field(
        default=100,
        ge=0,
        le=1000,
        description="Number of failed rows to capture per validation rule",
    )
    max_failure_samples: int = Field(
        default=500,
        ge=0,
        le=5000,
        description="Maximum total failed rows across all validations",
    )
    max_sampled_validations: int = Field(
        default=5,
        ge=1,
        le=20,
        description="After this many validations, show only counts (no samples)",
    )

    @model_validator(mode="after")
    def check_retention_policy(self):
        if self.retention_days is None and self.retention_count is None:
            raise ValueError(
                "StoryConfig: Specify at least one of 'retention_days' or 'retention_count'."
            )
        return self


class SystemConfig(BaseModel):
    """
    Configuration for the Odibi System Catalog (The Brain).

    Stores metadata, state, and pattern configurations.
    """

    connection: str = Field(description="Connection to store system tables (e.g., 'adls_bronze')")
    path: str = Field(default="_odibi_system", description="Path relative to connection root")


class LineageConfig(BaseModel):
    """
    Configuration for OpenLineage integration.

    Example:
    ```yaml
    lineage:
      url: "http://localhost:5000"
      namespace: "my_project"
    ```
    """

    url: Optional[str] = Field(default=None, description="OpenLineage API URL")
    namespace: str = Field(default="odibi", description="Namespace for jobs")
    api_key: Optional[str] = Field(default=None, description="API Key")


class ProjectConfig(BaseModel):
    """
    Complete project configuration from YAML.

    ### 🏢 "Enterprise Setup" Guide

    **Business Problem:**
    "We need a robust production environment with alerts, retries, and proper logging."

    **Recipe: Production Ready**
    ```yaml
    project: "Customer360"
    engine: "spark"

    # 1. Resilience
    retry:
        enabled: true
        max_attempts: 3
        backoff: "exponential"

    # 2. Observability
    logging:
        level: "INFO"
        structured: true  # JSON logs for Splunk/Datadog

    # 3. Alerting
    alerts:
        - type: "slack"
        url: "${SLACK_WEBHOOK_URL}"
        on_events: ["on_failure"]

    # ... connections and pipelines ...
    ```
    """

    # === MANDATORY ===
    project: str = Field(description="Project name")
    engine: EngineType = Field(default=EngineType.PANDAS, description="Execution engine")
    connections: Dict[str, ConnectionConfig] = Field(
        description="Named connections (at least one required)"
    )
    pipelines: List[PipelineConfig] = Field(
        description="Pipeline definitions (at least one required)"
    )
    story: StoryConfig = Field(description="Story generation configuration (mandatory)")
    system: SystemConfig = Field(description="System Catalog configuration (mandatory)")

    # === OPTIONAL (with sensible defaults) ===
    lineage: Optional["LineageConfig"] = Field(
        default=None, description="OpenLineage configuration"
    )
    description: Optional[str] = Field(default=None, description="Project description")
    version: str = Field(default="1.0.0", description="Project version")
    owner: Optional[str] = Field(default=None, description="Project owner/contact")
    vars: Dict[str, Any] = Field(
        default_factory=dict, description="Global variables for substitution (e.g. ${vars.env})"
    )

    # Global settings (optional with defaults in Pydantic)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    alerts: List[AlertConfig] = Field(default_factory=list, description="Alert configurations")
    performance: PerformanceConfig = Field(
        default_factory=PerformanceConfig, description="Performance tuning"
    )

    # === PHASE 3 ===
    environments: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description="Structure: same as ProjectConfig but with only overridden fields. Not yet validated strictly.",
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
    def ensure_system_config(self):
        """
        Validate system config connection exists.
        """
        if self.system is None:
            raise ValueError("System config is mandatory")

        # Ensure the system connection exists
        if self.system.connection not in self.connections:
            available = ", ".join(self.connections.keys())
            raise ValueError(
                f"System connection '{self.system.connection}' not found. "
                f"Available connections: {available}"
            )

        return self

    @model_validator(mode="after")
    def check_environments_not_implemented(self):
        """Check environments implementation."""
        # Implemented in Phase 3
        return self


def load_config_from_file(path: str) -> ProjectConfig:
    """
    Load and validate configuration from file.

    Args:
        path: Path to YAML file

    Returns:
        ProjectConfig
    """
    from odibi.utils import load_yaml_with_env

    config_dict = load_yaml_with_env(path)
    return ProjectConfig(**config_dict)
