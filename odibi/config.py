"""Configuration models for ODIBI framework."""

from enum import Enum
from typing import Any, Dict, List, Optional, Union, Literal

try:
    from typing import Annotated
except ImportError:
    from typing_extensions import Annotated

from pydantic import BaseModel, Field, field_validator, model_validator


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
    HTTP = "http"


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


class AlertEvent(str, Enum):
    """Events that trigger alerts."""

    ON_START = "on_start"
    ON_SUCCESS = "on_success"
    ON_FAILURE = "on_failure"


class AlertConfig(BaseModel):
    """
    Configuration for alerts.

    Example:
    ```yaml
    alerts:
      - type: "slack"
        url: "https://hooks.slack.com/..."
        on_events: ["on_failure"]
    ```
    """

    type: AlertType
    url: str = Field(description="Webhook URL")
    on_events: List[AlertEvent] = Field(
        default=[AlertEvent.ON_FAILURE],
        description="Events to trigger alert: on_start, on_success, on_failure",
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Extra metadata for alert (must be JSON-serializable, e.g. simple strings/numbers)",
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


# Connection config discriminated union
ConnectionConfig = Annotated[
    Union[
        LocalConnectionConfig,
        AzureBlobConnectionConfig,
        DeltaConnectionConfig,
        SQLServerConnectionConfig,
        HttpConnectionConfig,
    ],
    Field(discriminator="type"),
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
    """

    connection: str = Field(description="Connection name from project.yaml")
    format: ReadFormat = Field(description="Data format (csv, parquet, delta, etc.)")
    table: Optional[str] = Field(default=None, description="Table name for SQL/Delta")
    path: Optional[str] = Field(default=None, description="Path for file-based sources")
    streaming: bool = Field(default=False, description="Enable streaming read (Spark only)")
    query: Optional[str] = Field(default=None, description="SQL query (shortcut for options.query)")
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


class ValidationConfig(BaseModel):
    """
    Configuration for data validation.

    ### 🛡️ "The Indestructible Pipeline" Pattern

    **Business Problem:**
    "Bad data polluted our Gold reports, causing executives to make wrong decisions. We need to stop it *before* it lands."

    **The Solution:**
    A Quality Gate that runs *after* transformation but *before* writing.

    **Recipe: The Quality Gate**
    ```yaml
    validation:
      # BLOCKING: Stop the pipeline if data is bad.
      severity: "error"

      # 1. Completeness
      not_empty: true
      no_nulls: ["transaction_id", "customer_id"]

      # 2. Integrity
      allowed_values:
        status: ["PENDING", "COMPLETED", "FAILED"]
        currency: ["USD", "EUR", "GBP"]

      # 3. Business Logic (SQL Expressions)
      custom_sql:
        positive_amount: "amount > 0"
        valid_tax_rate: "tax_rate BETWEEN 0 AND 0.5"
        dates_ordered: "created_at <= completed_at"
    ```
    """

    severity: Literal["error", "warning"] = Field(
        default="error",
        description="If 'error', fails pipeline. If 'warning', logs alert but continues.",
    )
    schema_validation: Optional[Dict[str, Any]] = Field(
        default=None, alias="schema", description="Schema validation rules"
    )
    custom_sql: Dict[str, str] = Field(
        default_factory=dict,
        description="Custom SQL checks. Key is check name, value is SQL expression returning boolean.",
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
    format: ReadFormat = Field(description="Output format (csv, parquet, delta, etc.)")
    table: Optional[str] = Field(default=None, description="Table name for SQL/Delta")
    path: Optional[str] = Field(default=None, description="Path for file-based outputs")
    register_table: Optional[str] = Field(
        default=None, description="Register file output as external table (Spark/Delta only)"
    )
    mode: WriteMode = Field(default=WriteMode.OVERWRITE, description="Write mode")
    partition_by: List[str] = Field(
        default_factory=list, description="Columns to partition output by"
    )
    zorder_by: List[str] = Field(
        default_factory=list, description="Columns to Z-Order by (Delta only)"
    )
    table_properties: Dict[str, str] = Field(
        default_factory=dict, description="Table properties (e.g. comments, retention)"
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
    """

    name: str = Field(description="Unique node name")
    description: Optional[str] = Field(default=None, description="Human-readable description")
    enabled: bool = Field(default=True, description="If False, node is skipped during execution")
    tags: List[str] = Field(
        default_factory=list, description="Operational tags for selective execution"
    )
    depends_on: List[str] = Field(default_factory=list, description="List of node dependencies")

    columns: Dict[str, ColumnMetadata] = Field(
        default_factory=dict, description="Data Dictionary: Metadata for output columns"
    )

    # Operations (at least one required)
    read: Optional[ReadConfig] = Field(
        default=None,
        description="Input operation. If missing, data is taken from the first dependency.",
    )
    transform: Optional[TransformConfig] = Field(
        default=None, description="Chain of fine-grained transformation steps (SQL, functions)."
    )
    write: Optional[WriteConfig] = Field(
        default=None, description="Output operation (save to file/table)."
    )
    streaming: bool = Field(default=False, description="Enable streaming execution for this node")
    transformer: Optional[str] = Field(
        default=None,
        description="High-level pattern (App) to apply. Valid value from Transformer Catalog.",
    )
    params: Dict[str, Any] = Field(default_factory=dict, description="Parameters for transformer")

    # Optional features
    pre_sql: List[str] = Field(default_factory=list, description="SQL to run before node execution")
    post_sql: List[str] = Field(default_factory=list, description="SQL to run after node execution")
    materialized: Optional[Literal["table", "view", "incremental"]] = Field(
        default=None, description="Materialization strategy (Gold layer)"
    )

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
    """Performance tuning configuration."""

    use_arrow: bool = Field(
        default=True,
        description="Use Apache Arrow-backed DataFrames (Pandas only). Reduces memory and speeds up I/O.",
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
    ```
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

    @model_validator(mode="after")
    def check_retention_policy(self):
        if self.retention_days is None and self.retention_count is None:
            raise ValueError(
                "StoryConfig: Specify at least one of 'retention_days' or 'retention_count'."
            )
        return self


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

    # === OPTIONAL (with sensible defaults) ===
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
    def check_environments_not_implemented(self):
        """Check environments implementation."""
        # Implemented in Phase 3
        return self
