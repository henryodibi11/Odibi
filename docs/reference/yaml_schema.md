# Odibi Configuration Reference

This manual details the YAML configuration schema for Odibi projects.
*Auto-generated from Pydantic models.*

## Project Structure

### `ProjectConfig`
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

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **project** | str | Yes | - | Project name |
| **engine** | EngineType | No | `EngineType.PANDAS` | Execution engine |
| **connections** | Dict[str, [LocalConnectionConfig](#localconnectionconfig) | [AzureBlobConnectionConfig](#azureblobconnectionconfig) | [DeltaConnectionConfig](#deltaconnectionconfig) | [SQLServerConnectionConfig](#sqlserverconnectionconfig) | [HttpConnectionConfig](#httpconnectionconfig) | [CustomConnectionConfig](#customconnectionconfig)] | Yes | - | Named connections (at least one required)<br>**Options:** [LocalConnectionConfig](#localconnectionconfig), [AzureBlobConnectionConfig](#azureblobconnectionconfig), [DeltaConnectionConfig](#deltaconnectionconfig), [SQLServerConnectionConfig](#sqlserverconnectionconfig), [HttpConnectionConfig](#httpconnectionconfig) |
| **pipelines** | List[[PipelineConfig](#pipelineconfig)] | Yes | - | Pipeline definitions (at least one required) |
| **story** | [StoryConfig](#storyconfig) | Yes | - | Story generation configuration (mandatory) |
| **system** | [SystemConfig](#systemconfig) | Yes | - | System Catalog configuration (mandatory) |
| **lineage** | Optional[[LineageConfig](#lineageconfig)] | No | - | OpenLineage configuration |
| **description** | Optional[str] | No | - | Project description |
| **version** | str | No | `1.0.0` | Project version |
| **owner** | Optional[str] | No | - | Project owner/contact |
| **vars** | Dict[str, Any] | No | `PydanticUndefined` | Global variables for substitution (e.g. ${vars.env}) |
| **retry** | [RetryConfig](#retryconfig) | No | `PydanticUndefined` | - |
| **logging** | [LoggingConfig](#loggingconfig) | No | `PydanticUndefined` | - |
| **alerts** | List[[AlertConfig](#alertconfig)] | No | `PydanticUndefined` | Alert configurations |
| **performance** | [PerformanceConfig](#performanceconfig) | No | `PydanticUndefined` | Performance tuning |
| **environments** | Optional[Dict[str, Dict[str, Any]]] | No | - | Structure: same as ProjectConfig but with only overridden fields. Not yet validated strictly. |

---

### `PipelineConfig`
> *Used in: [ProjectConfig](#projectconfig)*

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

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **pipeline** | str | Yes | - | Pipeline name |
| **description** | Optional[str] | No | - | Pipeline description |
| **layer** | Optional[str] | No | - | Logical layer (bronze/silver/gold) |
| **nodes** | List[[NodeConfig](#nodeconfig)] | Yes | - | List of nodes in this pipeline |

---

### `NodeConfig`
> *Used in: [PipelineConfig](#pipelineconfig)*

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

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **name** | str | Yes | - | Unique node name |
| **description** | Optional[str] | No | - | Human-readable description |
| **enabled** | bool | No | `True` | If False, node is skipped during execution |
| **tags** | List[str] | No | `PydanticUndefined` | Operational tags for selective execution (e.g., 'daily', 'critical'). Use with `odibi run --tag`. |
| **depends_on** | List[str] | No | `PydanticUndefined` | List of parent nodes that must complete before this node runs. The output of these nodes is available for reading. |
| **columns** | Dict[str, [ColumnMetadata](#columnmetadata)] | No | `PydanticUndefined` | Data Dictionary defining the output schema. Used for documentation, PII tagging, and validation. |
| **read** | Optional[[ReadConfig](#readconfig)] | No | - | Input operation (Load). If missing, data is taken from the first dependency. |
| **transform** | Optional[[TransformConfig](#transformconfig)] | No | - | Chain of fine-grained transformation steps (SQL, functions). Runs after 'transformer' if both are present. |
| **write** | Optional[[WriteConfig](#writeconfig)] | No | - | Output operation (Save to file/table). |
| **streaming** | bool | No | `False` | Enable streaming execution for this node (Spark only) |
| **transformer** | Optional[str] | No | - | Name of the 'App' logic to run (e.g., 'deduplicate', 'scd2'). See Transformer Catalog for options. |
| **params** | Dict[str, Any] | No | `PydanticUndefined` | Parameters for transformer |
| **pre_sql** | List[str] | No | `PydanticUndefined` | SQL to run before node execution |
| **post_sql** | List[str] | No | `PydanticUndefined` | SQL to run after node execution |
| **materialized** | Optional[Literal['table', 'view', 'incremental']] | No | - | Materialization strategy (Gold layer) |
| **cache** | bool | No | `False` | Cache result for reuse |
| **log_level** | Optional[LogLevel] | No | - | Override log level for this node |
| **on_error** | ErrorStrategy | No | `ErrorStrategy.FAIL_LATER` | Failure handling strategy |
| **validation** | Optional[[ValidationConfig](#validationconfig)] | No | - | - |
| **contracts** | List[[TestConfig](#contracts-data-quality-gates)] | No | `PydanticUndefined` | Pre-condition contracts (Circuit Breakers). Runs on input data before transformation.<br>**Options:** [NotNullTest](#notnulltest), [UniqueTest](#uniquetest), [AcceptedValuesTest](#acceptedvaluestest), [RowCountTest](#rowcounttest), [CustomSQLTest](#customsqltest), [RangeTest](#rangetest), [RegexMatchTest](#regexmatchtest), [VolumeDropTest](#volumedroptest), [SchemaContract](#schemacontract), [DistributionContract](#distributioncontract), [FreshnessContract](#freshnesscontract) |
| **schema_policy** | Optional[[SchemaPolicyConfig](#schemapolicyconfig)] | No | - | Schema drift handling policy |
| **privacy** | Optional[[PrivacyConfig](#privacyconfig)] | No | - | Privacy Suite: PII anonymization settings |
| **sensitive** | bool | List[str] | No | `False` | If true or list of columns, masks sample data in stories |

---

### `ColumnMetadata`
> *Used in: [NodeConfig](#nodeconfig)*

Metadata for a column in the data dictionary.

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **description** | Optional[str] | No | - | Column description |
| **pii** | bool | No | `False` | Contains PII? |
| **tags** | List[str] | No | `PydanticUndefined` | Tags (e.g. 'business_key', 'measure') |

---

### `SystemConfig`
> *Used in: [ProjectConfig](#projectconfig)*

Configuration for the Odibi System Catalog (The Brain).

Stores metadata, state, and pattern configurations.

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **connection** | str | Yes | - | Connection to store system tables (e.g., 'adls_bronze') |
| **path** | str | No | `_odibi_system` | Path relative to connection root |

---

## Connections

### `LocalConnectionConfig`
> *Used in: [ProjectConfig](#projectconfig)*

Local filesystem connection.

Example:
```yaml
local_data:
  type: "local"
  base_path: "./data"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **type** | Literal['local'] | No | `ConnectionType.LOCAL` | - |
| **validation_mode** | ValidationMode | No | `ValidationMode.LAZY` | - |
| **base_path** | str | No | `./data` | Base directory path |

---

### `DeltaConnectionConfig`
> *Used in: [ProjectConfig](#projectconfig)*

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

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **type** | Literal['delta'] | No | `ConnectionType.DELTA` | - |
| **validation_mode** | ValidationMode | No | `ValidationMode.LAZY` | - |
| **catalog** | str | Yes | - | Spark catalog name (e.g. 'spark_catalog') |
| **schema_name** | str | Yes | - | Database/schema name |
| **table** | Optional[str] | No | - | Optional default table name for this connection (used by story/pipeline helpers) |

---

### `AzureBlobConnectionConfig`
> *Used in: [ProjectConfig](#projectconfig)*

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

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **type** | Literal['azure_blob'] | No | `ConnectionType.AZURE_BLOB` | - |
| **validation_mode** | ValidationMode | No | `ValidationMode.LAZY` | - |
| **account_name** | str | Yes | - | - |
| **container** | str | Yes | - | - |
| **auth** | AzureBlobAuthConfig | No | `PydanticUndefined` | **Options:** [AzureBlobKeyVaultAuth](#azureblobkeyvaultauth), [AzureBlobAccountKeyAuth](#azureblobaccountkeyauth), [AzureBlobSasAuth](#azureblobsasauth), [AzureBlobConnectionStringAuth](#azureblobconnectionstringauth), [AzureBlobMsiAuth](#azureblobmsiauth) |

---

### `SQLServerConnectionConfig`
> *Used in: [ProjectConfig](#projectconfig)*

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

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **type** | Literal['sql_server'] | No | `ConnectionType.SQL_SERVER` | - |
| **validation_mode** | ValidationMode | No | `ValidationMode.LAZY` | - |
| **host** | str | Yes | - | - |
| **database** | str | Yes | - | - |
| **port** | int | No | `1433` | - |
| **auth** | SQLServerAuthConfig | No | `PydanticUndefined` | **Options:** [SQLLoginAuth](#sqlloginauth), [SQLAadPasswordAuth](#sqlaadpasswordauth), [SQLMsiAuth](#sqlmsiauth), [SQLConnectionStringAuth](#sqlconnectionstringauth) |

---

### `HttpConnectionConfig`
> *Used in: [ProjectConfig](#projectconfig)*

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

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **type** | Literal['http'] | No | `ConnectionType.HTTP` | - |
| **validation_mode** | ValidationMode | No | `ValidationMode.LAZY` | - |
| **base_url** | str | Yes | - | - |
| **headers** | Dict[str, str] | No | `PydanticUndefined` | - |
| **auth** | HttpAuthConfig | No | `PydanticUndefined` | **Options:** [HttpNoAuth](#httpnoauth), [HttpBasicAuth](#httpbasicauth), [HttpBearerAuth](#httpbearerauth), [HttpApiKeyAuth](#httpapikeyauth) |

---

## Node Operations

### `ReadConfig`
> *Used in: [NodeConfig](#nodeconfig)*

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

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **connection** | str | Yes | - | Connection name from project.yaml |
| **format** | ReadFormat | str | Yes | - | Data format (csv, parquet, delta, etc.) |
| **table** | Optional[str] | No | - | Table name for SQL/Delta |
| **path** | Optional[str] | No | - | Path for file-based sources |
| **streaming** | bool | No | `False` | Enable streaming read (Spark only) |
| **query** | Optional[str] | No | - | SQL query to filter at source (pushdown). Mutually exclusive with table/path if supported by connector. |
| **incremental** | Optional[[IncrementalConfig](#incrementalconfig)] | No | - | Automatic incremental loading strategy (CDC-like). If set, generates query based on target state (HWM). |
| **time_travel** | Optional[[TimeTravelConfig](#timetravelconfig)] | No | - | Time travel options (Delta only) |
| **archive_options** | Dict[str, Any] | No | `PydanticUndefined` | Options for archiving bad records (e.g. badRecordsPath for Spark) |
| **options** | Dict[str, Any] | No | `PydanticUndefined` | Format-specific options |

---

### `IncrementalConfig`
> *Used in: [ReadConfig](#readconfig)*

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

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **mode** | IncrementalMode | No | `IncrementalMode.ROLLING_WINDOW` | Incremental strategy: 'rolling_window' or 'stateful' |
| **column** | str | Yes | - | Primary column to filter on (e.g., updated_at) |
| **fallback_column** | Optional[str] | No | - | Backup column if primary is NULL (e.g., created_at). Generates COALESCE(col, fallback) >= ... |
| **lookback** | Optional[int] | No | - | Time units to look back (Rolling Window only) |
| **unit** | Optional[IncrementalUnit] | No | - | Time unit for lookback (Rolling Window only). Options: 'hour', 'day', 'month', 'year' |
| **state_key** | Optional[str] | No | - | Unique ID for state tracking. Defaults to node name if not provided. |
| **watermark_lag** | Optional[str] | No | - | Safety buffer to handle late-arriving data. Subtracts this duration (e.g., '2h', '30m') from the stored High Water Mark when generating the query. Useful if your source system has eventual consistency or replication lag. |

---

### `TimeTravelConfig`
> *Used in: [ReadConfig](#readconfig)*

Configuration for time travel reading (Delta/Iceberg).

Example:
```yaml
time_travel:
  as_of_version: 10
  # OR
  as_of_timestamp: "2023-10-01T12:00:00Z"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **as_of_version** | Optional[int] | No | - | Version number to time travel to |
| **as_of_timestamp** | Optional[str] | No | - | Timestamp string to time travel to |

---

### `TransformConfig`
> *Used in: [NodeConfig](#nodeconfig)*

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

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **steps** | List[str | [TransformStep](#transformstep)] | Yes | - | List of transformation steps (SQL strings or TransformStep configs) |

---

### `ValidationConfig`
> *Used in: [NodeConfig](#nodeconfig)*

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
      pattern: "^[\w\.-]+@[\w\.-]+\.\w+$"

    # 4. Business Logic (SQL)
    - type: "custom_sql"
      name: "dates_ordered"
      condition: "created_at <= completed_at"
      threshold: 0.01   # Allow 1% failure
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **mode** | ValidationAction | No | `ValidationAction.FAIL` | Execution mode: 'fail' (stop pipeline) or 'warn' (log only) |
| **on_fail** | OnFailAction | No | `OnFailAction.ALERT` | Action on failure: 'alert' (send notification) or 'ignore' |
| **tests** | List[[TestConfig](#contracts-data-quality-gates)] | No | `PydanticUndefined` | List of validation tests<br>**Options:** [NotNullTest](#notnulltest), [UniqueTest](#uniquetest), [AcceptedValuesTest](#acceptedvaluestest), [RowCountTest](#rowcounttest), [CustomSQLTest](#customsqltest), [RangeTest](#rangetest), [RegexMatchTest](#regexmatchtest), [VolumeDropTest](#volumedroptest), [SchemaContract](#schemacontract), [DistributionContract](#distributioncontract), [FreshnessContract](#freshnesscontract) |

---

### `WriteConfig`
> *Used in: [NodeConfig](#nodeconfig)*

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

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **connection** | str | Yes | - | Connection name from project.yaml |
| **format** | ReadFormat | str | Yes | - | Output format (csv, parquet, delta, etc.) |
| **table** | Optional[str] | No | - | Table name for SQL/Delta |
| **path** | Optional[str] | No | - | Path for file-based outputs |
| **register_table** | Optional[str] | No | - | Register file output as external table (Spark/Delta only) |
| **mode** | WriteMode | No | `WriteMode.OVERWRITE` | Write mode. Options: 'overwrite', 'append', 'upsert', 'append_once' |
| **partition_by** | List[str] | No | `PydanticUndefined` | List of columns to physically partition the output by (folder structure). Use for low-cardinality columns (e.g. date, country). |
| **zorder_by** | List[str] | No | `PydanticUndefined` | List of columns to Z-Order by. Improves read performance for high-cardinality columns used in filters/joins (Delta only). |
| **table_properties** | Dict[str, str] | No | `PydanticUndefined` | Table properties (e.g. comments, retention) |
| **merge_schema** | bool | No | `False` | Allow schema evolution (mergeSchema option in Delta) |
| **first_run_query** | Optional[str] | No | - | SQL query for full-load on first run (High Water Mark pattern). If set, uses this query when target table doesn't exist, then switches to incremental. Only applies to SQL reads. |
| **options** | Dict[str, Any] | No | `PydanticUndefined` | Format-specific options |
| **auto_optimize** | bool | [AutoOptimizeConfig](#autooptimizeconfig) | No | - | Auto-run OPTIMIZE and VACUUM after write (Delta only) |

---

## Contracts (Data Quality Gates)

### Pre-Condition Circuit Breakers

Contracts are **fail-fast data quality checks** that run on input data **before** transformation.
Unlike validation (which runs after transforms and can warn), contracts always halt execution on failure.

**Use Cases:**
- Ensure source data meets minimum quality standards before processing
- Prevent bad data from propagating through the pipeline
- Fail early to save compute resources

**Example:**
```yaml
- name: "process_orders"
  contracts:
    - type: not_null
      columns: [order_id, customer_id]
    - type: row_count
      min: 100
    - type: freshness
      column: created_at
      max_age: "24h"
  read:
    source: raw_orders
  transform:
    steps:
      - function: filter
        params:
          condition: "status != 'cancelled'"
```

---

### `AcceptedValuesTest`
> *Used in: [NodeConfig](#nodeconfig), [ValidationConfig](#validationconfig)*

Ensures a column only contains values from an allowed list.

```yaml
contracts:
  - type: accepted_values
    column: status
    values: [pending, approved, rejected]
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **type** | Literal['accepted_values'] | No | `TestType.ACCEPTED_VALUES` | - |
| **name** | Optional[str] | No | - | Optional name for the check |
| **on_fail** | ContractSeverity | No | `ContractSeverity.FAIL` | Action on failure |
| **column** | str | Yes | - | Column to check |
| **values** | List[Any] | Yes | - | Allowed values |

---

### `CustomSQLTest`
> *Used in: [NodeConfig](#nodeconfig), [ValidationConfig](#validationconfig)*

Runs a custom SQL condition and fails if too many rows violate it.

```yaml
contracts:
  - type: custom_sql
    condition: "amount > 0"
    threshold: 0.01  # Allow up to 1% failures
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **type** | Literal['custom_sql'] | No | `TestType.CUSTOM_SQL` | - |
| **name** | Optional[str] | No | - | Optional name for the check |
| **on_fail** | ContractSeverity | No | `ContractSeverity.FAIL` | Action on failure |
| **condition** | str | Yes | - | SQL condition that should be true for valid rows |
| **threshold** | float | No | `0.0` | Failure rate threshold (0.0 = strictly no failures allowed) |

---

### `DistributionContract`
> *Used in: [NodeConfig](#nodeconfig), [ValidationConfig](#validationconfig)*

Checks if a column's statistical distribution is within expected bounds.

```yaml
contracts:
  - type: distribution
    column: price
    metric: mean
    threshold: ">100"  # Mean must be > 100
    on_fail: warn
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **type** | Literal['distribution'] | No | `TestType.DISTRIBUTION` | - |
| **name** | Optional[str] | No | - | Optional name for the check |
| **on_fail** | ContractSeverity | No | `ContractSeverity.WARN` | - |
| **column** | str | Yes | - | Column to analyze |
| **metric** | Literal['mean', 'min', 'max', 'null_percentage'] | Yes | - | Statistical metric to check |
| **threshold** | str | Yes | - | Threshold expression (e.g., '>100', '<0.05') |

---

### `FreshnessContract`
> *Used in: [NodeConfig](#nodeconfig), [ValidationConfig](#validationconfig)*

Ensures data is not stale by checking a timestamp column.

```yaml
contracts:
  - type: freshness
    column: updated_at
    max_age: "24h"  # Data must be less than 24 hours old
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **type** | Literal['freshness'] | No | `TestType.FRESHNESS` | - |
| **name** | Optional[str] | No | - | Optional name for the check |
| **on_fail** | ContractSeverity | No | `ContractSeverity.FAIL` | - |
| **column** | str | No | `updated_at` | Timestamp column to check |
| **max_age** | str | Yes | - | Maximum allowed age (e.g., '24h', '7d') |

---

### `NotNullTest`
> *Used in: [NodeConfig](#nodeconfig), [ValidationConfig](#validationconfig)*

Ensures specified columns contain no null values.

```yaml
contracts:
  - type: not_null
    columns: [customer_id, order_date]
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **type** | Literal['not_null'] | No | `TestType.NOT_NULL` | - |
| **name** | Optional[str] | No | - | Optional name for the check |
| **on_fail** | ContractSeverity | No | `ContractSeverity.FAIL` | Action on failure |
| **columns** | List[str] | Yes | - | Columns that must not contain nulls |

---

### `RangeTest`
> *Used in: [NodeConfig](#nodeconfig), [ValidationConfig](#validationconfig)*

Ensures column values fall within a specified range.

```yaml
contracts:
  - type: range
    column: age
    min: 0
    max: 150
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **type** | Literal['range'] | No | `TestType.RANGE` | - |
| **name** | Optional[str] | No | - | Optional name for the check |
| **on_fail** | ContractSeverity | No | `ContractSeverity.FAIL` | Action on failure |
| **column** | str | Yes | - | Column to check |
| **min** | int | float | str | No | - | Minimum value (inclusive) |
| **max** | int | float | str | No | - | Maximum value (inclusive) |

---

### `RegexMatchTest`
> *Used in: [NodeConfig](#nodeconfig), [ValidationConfig](#validationconfig)*

Ensures column values match a regex pattern.

```yaml
contracts:
  - type: regex_match
    column: email
    pattern: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **type** | Literal['regex_match'] | No | `TestType.REGEX_MATCH` | - |
| **name** | Optional[str] | No | - | Optional name for the check |
| **on_fail** | ContractSeverity | No | `ContractSeverity.FAIL` | Action on failure |
| **column** | str | Yes | - | Column to check |
| **pattern** | str | Yes | - | Regex pattern to match |

---

### `RowCountTest`
> *Used in: [NodeConfig](#nodeconfig), [ValidationConfig](#validationconfig)*

Validates that row count falls within expected bounds.

```yaml
contracts:
  - type: row_count
    min: 1000
    max: 100000
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **type** | Literal['row_count'] | No | `TestType.ROW_COUNT` | - |
| **name** | Optional[str] | No | - | Optional name for the check |
| **on_fail** | ContractSeverity | No | `ContractSeverity.FAIL` | Action on failure |
| **min** | Optional[int] | No | - | Minimum row count |
| **max** | Optional[int] | No | - | Maximum row count |

---

### `SchemaContract`
> *Used in: [NodeConfig](#nodeconfig), [ValidationConfig](#validationconfig)*

Validates that the DataFrame schema matches expected columns.

Uses the `columns` metadata from NodeConfig to verify schema.

```yaml
contracts:
  - type: schema
    strict: true  # Fail if extra columns present
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **type** | Literal['schema'] | No | `TestType.SCHEMA` | - |
| **name** | Optional[str] | No | - | Optional name for the check |
| **on_fail** | ContractSeverity | No | `ContractSeverity.FAIL` | - |
| **strict** | bool | No | `True` | If true, fail on unexpected columns |

---

### `UniqueTest`
> *Used in: [NodeConfig](#nodeconfig), [ValidationConfig](#validationconfig)*

Ensures specified columns (or combination) contain unique values.

```yaml
contracts:
  - type: unique
    columns: [order_id]
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **type** | Literal['unique'] | No | `TestType.UNIQUE` | - |
| **name** | Optional[str] | No | - | Optional name for the check |
| **on_fail** | ContractSeverity | No | `ContractSeverity.FAIL` | Action on failure |
| **columns** | List[str] | Yes | - | Columns that must be unique (composite key if multiple) |

---

### `VolumeDropTest`
> *Used in: [NodeConfig](#nodeconfig), [ValidationConfig](#validationconfig)*

Checks if row count dropped significantly compared to history.
Formula: (current - avg) / avg < -threshold

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **type** | Literal['volume_drop'] | No | `TestType.VOLUME_DROP` | - |
| **name** | Optional[str] | No | - | Optional name for the check |
| **on_fail** | ContractSeverity | No | `ContractSeverity.FAIL` | Action on failure |
| **threshold** | float | No | `0.5` | Max allowed drop (0.5 = 50% drop) |
| **lookback_days** | int | No | `7` | Days of history to average |

---

## Global Settings

### `LineageConfig`
> *Used in: [ProjectConfig](#projectconfig)*

Configuration for OpenLineage integration.

Example:
```yaml
lineage:
  url: "http://localhost:5000"
  namespace: "my_project"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **url** | Optional[str] | No | - | OpenLineage API URL |
| **namespace** | str | No | `odibi` | Namespace for jobs |
| **api_key** | Optional[str] | No | - | API Key |

---

### `AlertConfig`
> *Used in: [ProjectConfig](#projectconfig)*

Configuration for alerts.

Example:
```yaml
alerts:
  - type: "slack"
    url: "https://hooks.slack.com/..."
    on_events: ["on_failure"]
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **type** | AlertType | Yes | - | - |
| **url** | str | Yes | - | Webhook URL |
| **on_events** | List[AlertEvent] | No | `[<AlertEvent.ON_FAILURE: 'on_failure'>]` | Events to trigger alert: on_start, on_success, on_failure |
| **metadata** | Dict[str, Any] | No | `PydanticUndefined` | Extra metadata for alert (must be JSON-serializable, e.g. simple strings/numbers) |

---

### `LoggingConfig`
> *Used in: [ProjectConfig](#projectconfig)*

Logging configuration.

Example:
```yaml
logging:
  level: "INFO"
  structured: true
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **level** | LogLevel | No | `LogLevel.INFO` | - |
| **structured** | bool | No | `False` | Output JSON logs |
| **metadata** | Dict[str, Any] | No | `PydanticUndefined` | Extra metadata in logs |

---

### `PerformanceConfig`
> *Used in: [ProjectConfig](#projectconfig)*

Performance tuning configuration.

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **use_arrow** | bool | No | `True` | Use Apache Arrow-backed DataFrames (Pandas only). Reduces memory and speeds up I/O. |

---

### `RetryConfig`
> *Used in: [ProjectConfig](#projectconfig)*

Retry configuration.

Example:
```yaml
retry:
  enabled: true
  max_attempts: 3
  backoff: "exponential"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **enabled** | bool | No | `True` | - |
| **max_attempts** | int | No | `3` | - |
| **backoff** | BackoffStrategy | No | `BackoffStrategy.EXPONENTIAL` | - |

---

### `StoryConfig`
> *Used in: [ProjectConfig](#projectconfig)*

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

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **connection** | str | Yes | - | Connection name for story output (uses connection's path resolution) |
| **path** | str | Yes | - | Path for stories (relative to connection base_path) |
| **max_sample_rows** | int | No | `10` | - |
| **auto_generate** | bool | No | `True` | - |
| **retention_days** | Optional[int] | No | `30` | Days to keep stories |
| **retention_count** | Optional[int] | No | `100` | Max number of stories to keep |

---

## Transformation Reference

### How to Use Transformers

You can use any transformer in two ways:

**1. As a Top-Level Transformer ("The App")**
Use this for major operations that define the node's purpose (e.g. Merge, SCD2).
```yaml
- name: "my_node"
  transformer: "<transformer_name>"
  params:
    <param_name>: <value>
```

**2. As a Step in a Chain ("The Script")**
Use this for smaller operations within a `transform` block (e.g. clean_text, filter).
```yaml
- name: "my_node"
  transform:
    steps:
      - function: "<transformer_name>"
         params:
           <param_name>: <value>
```

**Available Transformers:**
The models below describe the `params` required for each transformer.

---

### 📂 Common Operations

#### CaseWhenCase
[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **condition** | str | Yes | - | - |
| **value** | str | Yes | - | - |

---

#### `case_when` (CaseWhenParams)
Implements structured CASE WHEN logic.

Configuration for conditional logic.

Example:
```yaml
case_when:
  output_col: "age_group"
  default: "'Adult'"
  cases:
    - condition: "age < 18"
      value: "'Minor'"
    - condition: "age > 65"
      value: "'Senior'"
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **cases** | List[CaseWhenCase] | Yes | - | List of conditional branches |
| **default** | str | No | `NULL` | Default value if no condition met |
| **output_col** | str | Yes | - | Name of the resulting column |

---

#### `cast_columns` (CastColumnsParams)
Casts specific columns to new types while keeping others intact.

Configuration for column type casting.

Example:
```yaml
cast_columns:
  casts:
    age: "int"
    salary: "DOUBLE"
    created_at: "TIMESTAMP"
    tags: "ARRAY<STRING>"  # Raw SQL types allowed
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **casts** | Dict[str, SimpleType | str] | Yes | - | Map of column to target SQL type |

---

#### `clean_text` (CleanTextParams)
Applies string cleaning operations (Trim/Case) via SQL.

Configuration for text cleaning.

Example:
```yaml
clean_text:
  columns: ["email", "username"]
  trim: true
  case: "lower"
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **columns** | List[str] | Yes | - | List of columns to clean |
| **trim** | bool | No | `True` | Apply TRIM() |
| **case** | Literal['lower', 'upper', 'preserve'] | No | `preserve` | Case conversion |

---

#### `concat_columns` (ConcatColumnsParams)
Concatenates multiple columns into one string.
NULLs are skipped (treated as empty string) using CONCAT_WS behavior.

Configuration for string concatenation.

Example:
```yaml
concat_columns:
  columns: ["first_name", "last_name"]
  separator: " "
  output_col: "full_name"
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **columns** | List[str] | Yes | - | Columns to concatenate |
| **separator** | str | No | - | Separator string |
| **output_col** | str | Yes | - | Resulting column name |

---

#### `convert_timezone` (ConvertTimezoneParams)
Converts a timestamp from one timezone to another.
Assumes the input column is a naive timestamp representing time in source_tz,
or a timestamp with timezone.

Configuration for timezone conversion.

Example:
```yaml
convert_timezone:
  col: "utc_time"
  source_tz: "UTC"
  target_tz: "America/New_York"
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **col** | str | Yes | - | Timestamp column to convert |
| **source_tz** | str | No | `UTC` | Source timezone (e.g., 'UTC', 'America/New_York') |
| **target_tz** | str | Yes | - | Target timezone (e.g., 'America/Los_Angeles') |
| **output_col** | Optional[str] | No | - | Name of the result column (default: {col}_{target_tz}) |

---

#### `date_add` (DateAddParams)
Adds an interval to a date/timestamp column.

Configuration for date addition.

Example:
```yaml
date_add:
  col: "created_at"
  value: 1
  unit: "day"
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **col** | str | Yes | - | - |
| **value** | int | Yes | - | - |
| **unit** | Literal['day', 'month', 'year', 'hour', 'minute', 'second'] | Yes | - | - |

---

#### `date_diff` (DateDiffParams)
Calculates difference between two dates/timestamps.
Returns the elapsed time in the specified unit (as float for sub-day units).

Configuration for date difference.

Example:
```yaml
date_diff:
  start_col: "created_at"
  end_col: "updated_at"
  unit: "day"
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **start_col** | str | Yes | - | - |
| **end_col** | str | Yes | - | - |
| **unit** | Literal['day', 'hour', 'minute', 'second'] | No | `day` | - |

---

#### `date_trunc` (DateTruncParams)
Truncates a date/timestamp to the specified precision.

Configuration for date truncation.

Example:
```yaml
date_trunc:
  col: "created_at"
  unit: "month"
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **col** | str | Yes | - | - |
| **unit** | Literal['year', 'month', 'day', 'hour', 'minute', 'second'] | Yes | - | - |

---

#### `derive_columns` (DeriveColumnsParams)
Appends new columns based on SQL expressions.

Design:
- Uses projection to add fields.
- Keeps all existing columns via `*`.

Configuration for derived columns.

Example:
```yaml
derive_columns:
  derivations:
    total_price: "quantity * unit_price"
    full_name: "concat(first_name, ' ', last_name)"
```

Note: Engine will fail if expressions reference non-existent columns.

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **derivations** | Dict[str, str] | Yes | - | Map of column name to SQL expression |

---

#### `distinct` (DistinctParams)
Returns unique rows (SELECT DISTINCT).

Configuration for distinct rows.

Example:
```yaml
distinct:
  columns: ["category", "status"]
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **columns** | Optional[List[str]] | No | - | Columns to project (if None, keeps all columns unique) |

---

#### `extract_date_parts` (ExtractDateParams)
Extracts date parts using ANSI SQL extract/functions.

Configuration for extracting date parts.

Example:
```yaml
extract_date_parts:
  source_col: "created_at"
  prefix: "created"
  parts: ["year", "month"]
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **source_col** | str | Yes | - | - |
| **prefix** | Optional[str] | No | - | - |
| **parts** | Literal[typing.Literal['year', 'month', 'day', 'hour']] | No | `['year', 'month', 'day']` | - |

---

#### `fill_nulls` (FillNullsParams)
Replaces null values with specified defaults using COALESCE.

Configuration for filling null values.

Example:
```yaml
fill_nulls:
  values:
    count: 0
    description: "N/A"
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **values** | Dict[str, str | int | float | bool] | Yes | - | Map of column to fill value |

---

#### `filter_rows` (FilterRowsParams)
Filters rows using a standard SQL WHERE clause.

Design:
- SQL-First: Pushes filtering to the engine's optimizer.
- Zero-Copy: No data movement to Python.

Configuration for filtering rows.

Example:
```yaml
filter_rows:
  condition: "age > 18 AND status = 'active'"
```

Example (Null Check):
```yaml
filter_rows:
  condition: "email IS NOT NULL AND email != ''"
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **condition** | str | Yes | - | SQL WHERE clause (e.g., 'age > 18 AND status = "active"') |

---

#### `limit` (LimitParams)
Limits result size.

Configuration for result limiting.

Example:
```yaml
limit:
  n: 100
  offset: 0
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **n** | int | Yes | - | Number of rows to return |
| **offset** | int | No | `0` | Number of rows to skip |

---

#### `normalize_schema` (NormalizeSchemaParams)
Structural transformation to rename, drop, and reorder columns.

Note: This is one of the few that might behave better with native API in some cases,
but SQL projection handles it perfectly and is consistent.

Configuration for schema normalization.

Example:
```yaml
normalize_schema:
  rename:
    old_col: "new_col"
  drop: ["unused_col"]
  select_order: ["id", "new_col", "created_at"]
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **rename** | Optional[Dict[str, str]] | No | `PydanticUndefined` | old_name -> new_name |
| **drop** | Optional[List[str]] | No | `PydanticUndefined` | Columns to remove; ignored if not present |
| **select_order** | Optional[List[str]] | No | - | Final column order; any missing columns appended after |

---

#### `sample` (SampleParams)
Samples data using random filtering.

Configuration for random sampling.

Example:
```yaml
sample:
  fraction: 0.1
  seed: 42
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **fraction** | float | Yes | - | Fraction of rows to return (0.0 to 1.0) |
| **seed** | Optional[int] | No | - | - |

---

#### `sort` (SortParams)
Sorts the dataset.

Configuration for sorting.

Example:
```yaml
sort:
  by: ["created_at", "id"]
  ascending: false
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **by** | str | List[str] | Yes | - | Column(s) to sort by |
| **ascending** | bool | No | `True` | Sort order |

---

#### `split_part` (SplitPartParams)
Extracts the Nth part of a string after splitting by a delimiter.

Configuration for splitting strings.

Example:
```yaml
split_part:
  col: "email"
  delimiter: "@"
  index: 2  # Extracts domain
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **col** | str | Yes | - | Column to split |
| **delimiter** | str | Yes | - | Delimiter to split by |
| **index** | int | Yes | - | 1-based index of the token to extract |

---

### 📂 Relational Algebra

#### `aggregate` (AggregateParams)
Performs grouping and aggregation via SQL.

Configuration for aggregation.

Example:
```yaml
aggregate:
  group_by: ["department", "region"]
  aggregations:
    salary: "sum"
    employee_id: "count"
    age: "avg"
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **group_by** | List[str] | Yes | - | Columns to group by |
| **aggregations** | Dict[str, AggFunc] | Yes | - | Map of column to aggregation function (sum, avg, min, max, count) |

---

#### `join` (JoinParams)
Joins the current dataset with another dataset from the context.

Configuration for joining datasets.

Scenario 1: Simple Left Join
```yaml
join:
  right_dataset: "customers"
  on: "customer_id"
  how: "left"
```

Scenario 2: Join with Prefix (avoid collisions)
```yaml
join:
  right_dataset: "orders"
  on: ["user_id"]
  how: "inner"
  prefix: "ord"  # Result cols: ord_date, ord_amount...
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **right_dataset** | str | Yes | - | Name of the node/dataset to join with |
| **on** | str | List[str] | Yes | - | Column(s) to join on |
| **how** | Literal['inner', 'left', 'right', 'full', 'cross'] | No | `left` | Join type |
| **prefix** | Optional[str] | No | - | Prefix for columns from right dataset to avoid collisions |

---

#### `pivot` (PivotParams)
Pivots row values into columns.

Configuration for pivoting data.

Example:
```yaml
pivot:
  group_by: ["product_id", "region"]
  pivot_col: "month"
  agg_col: "sales"
  agg_func: "sum"
```

Example (Optimized for Spark):
```yaml
pivot:
  group_by: ["id"]
  pivot_col: "category"
  values: ["A", "B", "C"]  # Explicit values avoid extra pass
  agg_col: "amount"
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **group_by** | List[str] | Yes | - | - |
| **pivot_col** | str | Yes | - | - |
| **agg_col** | str | Yes | - | - |
| **agg_func** | Literal['sum', 'count', 'avg', 'max', 'min', 'first'] | No | `sum` | - |
| **values** | Optional[List[str]] | No | - | Specific values to pivot (for Spark optimization) |

---

#### `union` (UnionParams)
Unions current dataset with others.

Configuration for unioning datasets.

Example (By Name - Default):
```yaml
union:
  datasets: ["sales_2023", "sales_2024"]
  by_name: true
```

Example (By Position):
```yaml
union:
  datasets: ["legacy_data"]
  by_name: false
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **datasets** | List[str] | Yes | - | List of node names to union with current |
| **by_name** | bool | No | `True` | Match columns by name (UNION ALL BY NAME) |

---

#### `unpivot` (UnpivotParams)
Unpivots columns into rows (Melt/Stack).

Configuration for unpivoting (melting) data.

Example:
```yaml
unpivot:
  id_cols: ["product_id"]
  value_vars: ["jan_sales", "feb_sales", "mar_sales"]
  var_name: "month"
  value_name: "sales"
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **id_cols** | List[str] | Yes | - | - |
| **value_vars** | List[str] | Yes | - | - |
| **var_name** | str | No | `variable` | - |
| **value_name** | str | No | `value` | - |

---

### 📂 Data Quality

#### `cross_check` (CrossCheckParams)
Perform cross-node validation checks.

Does not return a DataFrame (returns None).
Raises ValidationError on failure.

Configuration for cross-node validation checks.

Example (Row Count Mismatch):
```yaml
transformer: "cross_check"
params:
  type: "row_count_diff"
  inputs: ["node_a", "node_b"]
  threshold: 0.05  # Allow 5% difference
```

Example (Schema Match):
```yaml
transformer: "cross_check"
params:
  type: "schema_match"
  inputs: ["staging_orders", "prod_orders"]
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **type** | str | Yes | - | Check type: 'row_count_diff', 'schema_match' |
| **inputs** | List[str] | Yes | - | List of node names to compare |
| **threshold** | float | No | `0.0` | Threshold for diff (0.0-1.0) |

---

### 📂 Warehousing Patterns

#### AuditColumnsConfig
[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **created_col** | Optional[str] | No | - | Column to set only on first insert |
| **updated_col** | Optional[str] | No | - | Column to update on every merge |

---

#### `merge` (MergeParams)
Merge transformer implementation.
Handles Upsert, Append-Only, and Delete-Match strategies.

Configuration for Merge transformer (Upsert/Append).

### ⚖️ "GDPR & Compliance" Guide

**Business Problem:**
"A user exercised their 'Right to be Forgotten'. We need to remove them from our Silver tables immediately."

**The Solution:**
Use the `delete_match` strategy. The source dataframe contains the IDs to be deleted, and the transformer removes them from the target.

**Recipe 1: Right to be Forgotten (Delete)**
```yaml
transformer: "merge"
params:
  target: "silver.customers"
  keys: ["customer_id"]
  strategy: "delete_match"
```

**Recipe 2: Conditional Update (SCD Type 1)**
"Only update if the source record is newer than the target record."
```yaml
transformer: "merge"
params:
  target: "silver.products"
  keys: ["product_id"]
  strategy: "upsert"
  update_condition: "source.updated_at > target.updated_at"
```

**Recipe 3: Safe Insert (Filter Bad Records)**
"Only insert records that are not marked as deleted."
```yaml
transformer: "merge"
params:
  target: "silver.orders"
  keys: ["order_id"]
  strategy: "append_only"
  insert_condition: "source.is_deleted = false"
```

**Recipe 4: Audit Columns**
"Track when records were created or updated."
```yaml
transformer: "merge"
params:
  target: "silver.users"
  keys: ["user_id"]
  audit_cols:
    created_col: "dw_created_at"
    updated_col: "dw_updated_at"
```

**Recipe 5: Full Sync (Insert + Update + Delete)**
"Sync target with source: insert new, update changed, and remove soft-deleted."
```yaml
transformer: "merge"
params:
  target: "silver.customers"
  keys: ["id"]
  strategy: "upsert"
  # 1. Delete if source says so
  delete_condition: "source.is_deleted = true"
  # 2. Update if changed (and not deleted)
  update_condition: "source.hash != target.hash"
  # 3. Insert new (and not deleted)
  insert_condition: "source.is_deleted = false"
```

**Strategies:**
*   **upsert** (Default): Update existing records, insert new ones.
*   **append_only**: Ignore duplicates, only insert new keys.
*   **delete_match**: Delete records in target that match keys in source.

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **target** | str | Yes | - | Target table name or path |
| **keys** | List[str] | Yes | - | List of join keys |
| **strategy** | MergeStrategy | No | `MergeStrategy.UPSERT` | Merge behavior: 'upsert', 'append_only', 'delete_match' |
| **audit_cols** | Optional[AuditColumnsConfig] | No | - | {'created_col': '...', 'updated_col': '...'} |
| **optimize_write** | bool | No | `False` | Run OPTIMIZE after write (Spark) |
| **zorder_by** | Optional[List[str]] | No | - | Columns to Z-Order by |
| **cluster_by** | Optional[List[str]] | No | - | Columns to Liquid Cluster by (Delta) |
| **update_condition** | Optional[str] | No | - | SQL condition for update clause (e.g. 'source.ver > target.ver') |
| **insert_condition** | Optional[str] | No | - | SQL condition for insert clause (e.g. 'source.status != "deleted"') |
| **delete_condition** | Optional[str] | No | - | SQL condition for delete clause (e.g. 'source.status = "deleted"') |

---

#### `scd2` (SCD2Params)
Implements SCD Type 2 Logic.

Returns the FULL history dataset (to be written via Overwrite).

Parameters for SCD Type 2 (Slowly Changing Dimensions) transformer.

### 🕰️ The "Time Machine" Pattern

**Business Problem:**
"I need to know what the customer's address was *last month*, not just where they live now."

**The Solution:**
SCD Type 2 tracks the full history of changes. Each record has an "effective window" (start/end dates) and a flag indicating if it is the current version.

**Recipe:**
```yaml
transformer: "scd2"
params:
  target: "gold/customers"         # Path to existing history
  keys: ["customer_id"]            # How we identify the entity
  track_cols: ["address", "tier"]  # What changes we care about
  effective_time_col: "txn_date"   # When the change actually happened
  end_time_col: "valid_to"         # (Optional) Name of closing timestamp
  current_flag_col: "is_active"    # (Optional) Name of current flag
```

**How it works:**
1. **Match**: Finds existing records using `keys`.
2. **Compare**: Checks `track_cols` to see if data changed.
3. **Close**: If changed, updates the old record's `end_time_col` to the new `effective_time_col`.
4. **Insert**: Adds a new record with `effective_time_col` as start and open-ended end date.

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **target** | str | Yes | - | Target table name or path containing history |
| **keys** | List[str] | Yes | - | Natural keys to identify unique entities |
| **track_cols** | List[str] | Yes | - | Columns to monitor for changes |
| **effective_time_col** | str | Yes | - | Source column indicating when the change occurred. |
| **end_time_col** | str | No | `valid_to` | Name of the end timestamp column |
| **current_flag_col** | str | No | `is_current` | Name of the current record flag column |
| **delete_col** | Optional[str] | No | - | Column indicating soft deletion (boolean) |

---

### 📂 Advanced & Feature Engineering

#### `deduplicate` (DeduplicateParams)
Deduplicates data using Window functions.

Configuration for deduplication.

Scenario: Keep latest record
```yaml
deduplicate:
  keys: ["id"]
  order_by: "updated_at DESC"
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **keys** | List[str] | Yes | - | List of columns to partition by (columns that define uniqueness) |
| **order_by** | Optional[str] | No | - | SQL Order by clause (e.g. 'updated_at DESC') to determine which record to keep (first one is kept) |

---

#### `dict_based_mapping` (DictMappingParams)
Configuration for dictionary mapping.

Scenario: Map status codes to labels
```yaml
dict_based_mapping:
  column: "status_code"
  mapping:
    "1": "Active"
    "0": "Inactive"
  default: "Unknown"
  output_column: "status_desc"
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **column** | str | Yes | - | Column to map values from |
| **mapping** | Dict[str, str | int | float | bool] | Yes | - | Dictionary of source value -> target value |
| **default** | str | int | float | bool | No | - | Default value if source value is not found in mapping |
| **output_column** | Optional[str] | No | - | Name of output column. If not provided, overwrites source column. |

---

#### `explode_list_column` (ExplodeParams)
Configuration for exploding lists.

Scenario: Flatten list of items per order
```yaml
explode_list_column:
  column: "items"
  outer: true  # Keep orders with empty items list
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **column** | str | Yes | - | Column containing the list/array to explode |
| **outer** | bool | No | `False` | If True, keep rows with empty lists (explode_outer behavior). If False, drops them. |

---

#### `generate_surrogate_key` (SurrogateKeyParams)
Generates a deterministic surrogate key (MD5) from a combination of columns.
Handles NULLs by treating them as empty strings to ensure consistency.

Configuration for surrogate key generation.

Example:
```yaml
generate_surrogate_key:
  columns: ["region", "product_id"]
  separator: "-"
  output_col: "unique_id"
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **columns** | List[str] | Yes | - | Columns to combine for the key |
| **separator** | str | No | `-` | Separator between values |
| **output_col** | str | No | `surrogate_key` | Name of the output column |

---

#### `geocode` (GeocodeParams)
Geocoding Stub.

Configuration for geocoding.

Example:
```yaml
geocode:
  address_col: "full_address"
  output_col: "lat_long"
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **address_col** | str | Yes | - | Column containing the address to geocode |
| **output_col** | str | No | `lat_long` | Name of the output column for coordinates |

---

#### `hash_columns` (HashParams)
Hashes columns for PII/Anonymization.

Configuration for column hashing.

Example:
```yaml
hash_columns:
  columns: ["email", "ssn"]
  algorithm: "sha256"
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **columns** | List[str] | Yes | - | List of columns to hash |
| **algorithm** | HashAlgorithm | No | `HashAlgorithm.SHA256` | Hashing algorithm. Options: 'sha256', 'md5' |

---

#### `normalize_json` (NormalizeJsonParams)
Flattens a nested JSON/Struct column.

Configuration for JSON normalization.

Example:
```yaml
normalize_json:
  column: "json_data"
  sep: "_"
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **column** | str | Yes | - | Column containing nested JSON/Struct |
| **sep** | str | No | `_` | Separator for nested fields (e.g., 'parent_child') |

---

#### `parse_json` (ParseJsonParams)
Parses a JSON string column into a Struct/Map column.

Configuration for JSON parsing.

Example:
```yaml
parse_json:
  column: "raw_json"
  json_schema: "id INT, name STRING"
  output_col: "parsed_struct"
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **column** | str | Yes | - | String column containing JSON |
| **json_schema** | str | Yes | - | DDL schema string (e.g. 'a INT, b STRING') or Spark StructType DDL |
| **output_col** | Optional[str] | No | - | - |

---

#### `regex_replace` (RegexReplaceParams)
SQL-based Regex replacement.

Configuration for regex replacement.

Example:
```yaml
regex_replace:
  column: "phone"
  pattern: "[^0-9]"
  replacement: ""
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **column** | str | Yes | - | Column to apply regex replacement on |
| **pattern** | str | Yes | - | Regex pattern to match |
| **replacement** | str | Yes | - | String to replace matches with |

---

#### `sessionize` (SessionizeParams)
Assigns session IDs based on inactivity threshold.

Configuration for sessionization.

Example:
```yaml
sessionize:
  timestamp_col: "event_time"
  user_col: "user_id"
  threshold_seconds: 1800
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **timestamp_col** | str | Yes | - | Timestamp column to calculate session duration from |
| **user_col** | str | Yes | - | User identifier to partition sessions by |
| **threshold_seconds** | int | No | `1800` | Inactivity threshold in seconds (default: 30 minutes). If gap > threshold, new session starts. |
| **session_col** | str | No | `session_id` | Output column name for the generated session ID |

---

#### `unpack_struct` (UnpackStructParams)
Flattens a struct/dict column into top-level columns.

Configuration for unpacking structs.

Example:
```yaml
unpack_struct:
  column: "user_info"
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **column** | str | Yes | - | Struct/Dictionary column to unpack/flatten into individual columns |

---

#### `validate_and_flag` (ValidateAndFlagParams)
Validates rules and appends a column with a list/string of failed rule names.

Configuration for validation flagging.

Example:
```yaml
validate_and_flag:
  flag_col: "data_issues"
  rules:
    age_check: "age >= 0"
    email_format: "email LIKE '%@%'"
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **rules** | Dict[str, str] | Yes | - | Map of rule name to SQL condition (must be TRUE) |
| **flag_col** | str | No | `_issues` | Name of the column to store failed rules |

---

#### `window_calculation` (WindowCalculationParams)
Generic wrapper for Window functions.

Configuration for window functions.

Example:
```yaml
window_calculation:
  target_col: "cumulative_sales"
  function: "sum(sales)"
  partition_by: ["region"]
  order_by: "date ASC"
```

[Back to Catalog](#nodeconfig)

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **target_col** | str | Yes | - | - |
| **function** | str | Yes | - | Window function e.g. 'sum(amount)', 'rank()' |
| **partition_by** | List[str] | No | `PydanticUndefined` | - |
| **order_by** | Optional[str] | No | - | - |

---
