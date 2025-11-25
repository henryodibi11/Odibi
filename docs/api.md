# Odibi Configuration Reference

This manual details the YAML configuration schema for Odibi projects.
*Auto-generated from Pydantic models.*

## Project Structure

### `NodeConfig`
Configuration for a single node.

Example:
```yaml
- name: "process_users"
  description: "Clean and enrich user data"
  depends_on: ["raw_users"]
  read: ...
  transform: ...
  write: ...
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **name** | `<class 'str'>` | Yes | - | Unique node name |
| **description** | `Optional[str]` | No | - | Human-readable description |
| **depends_on** | `List[str]` | No | `PydanticUndefined` | List of node dependencies |
| **read** | `Optional[ReadConfig]` | No | - | - |
| **transform** | `Optional[TransformConfig]` | No | - | - |
| **write** | `Optional[WriteConfig]` | No | - | - |
| **streaming** | `<class 'bool'>` | No | `False` | Enable streaming execution for this node |
| **transformer** | `Optional[str]` | No | - | Name of transformer to apply |
| **params** | `Dict[str, Any]` | No | `PydanticUndefined` | Parameters for transformer |
| **cache** | `<class 'bool'>` | No | `False` | Cache result for reuse |
| **log_level** | `Optional[LogLevel]` | No | - | Override log level for this node |
| **on_error** | `<enum 'ErrorStrategy'>` | No | `ErrorStrategy.FAIL_LATER` | Failure handling strategy |
| **validation** | `Optional[ValidationConfig]` | No | - | - |
| **sensitive** | `Union[bool, List[str]]` | No | `False` | If true or list of columns, masks sample data in stories |

---

### `PipelineConfig`
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
| **pipeline** | `<class 'str'>` | Yes | - | Pipeline name |
| **description** | `Optional[str]` | No | - | Pipeline description |
| **layer** | `Optional[str]` | No | - | Logical layer (bronze/silver/gold) |
| **nodes** | `List[NodeConfig]` | Yes | - | List of nodes in this pipeline |

---

### `ProjectConfig`
Complete project configuration from YAML.

Represents the entire YAML structure with validation.
All settings are top-level (no nested defaults).

Example:
```yaml
project: "MyDataProject"
engine: "pandas"
connections: ...
story: ...
pipelines: ...
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **project** | `<class 'str'>` | Yes | - | Project name |
| **engine** | `<enum 'EngineType'>` | No | `EngineType.PANDAS` | Execution engine |
| **connections** | `Dict[str, Dict[str, Any]]` | Yes | - | Named connections (at least one required) |
| **pipelines** | `List[PipelineConfig]` | Yes | - | Pipeline definitions (at least one required) |
| **story** | `<class 'StoryConfig'>` | Yes | - | Story generation configuration (mandatory) |
| **description** | `Optional[str]` | No | - | Project description |
| **version** | `<class 'str'>` | No | `1.0.0` | Project version |
| **owner** | `Optional[str]` | No | - | Project owner/contact |
| **retry** | `<class 'RetryConfig'>` | No | `PydanticUndefined` | - |
| **logging** | `<class 'LoggingConfig'>` | No | `PydanticUndefined` | - |
| **alerts** | `List[AlertConfig]` | No | `PydanticUndefined` | Alert configurations |
| **performance** | `<class 'PerformanceConfig'>` | No | `PydanticUndefined` | Performance tuning |
| **environments** | `Optional[Dict[str, Dict[str, Any]]]` | No | - | Environment-specific overrides |

---

## Connections

### `AzureBlobConnectionConfig`
Azure Blob Storage connection.

Example:
```yaml
adls_bronze:
  type: "azure_blob"
  account_name: "myaccount"
  container: "bronze"
  auth:
    mode: "key_vault"
    key_vault: "kv-name"
    secret: "adls-key"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **type** | `<enum 'ConnectionType'>` | No | `ConnectionType.AZURE_BLOB` | - |
| **validation_mode** | `<class 'str'>` | No | `lazy` | - |
| **account_name** | `<class 'str'>` | Yes | - | - |
| **container** | `<class 'str'>` | Yes | - | - |
| **auth** | `Dict[str, str]` | No | `PydanticUndefined` | - |

---

### `DeltaConnectionConfig`
Delta Lake connection.

Example:
```yaml
delta_silver:
  type: "delta"
  catalog: "spark_catalog"
  schema: "silver_db"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **type** | `<enum 'ConnectionType'>` | No | `ConnectionType.DELTA` | - |
| **validation_mode** | `<class 'str'>` | No | `lazy` | - |
| **catalog** | `<class 'str'>` | Yes | - | - |
| **schema_name** | `<class 'str'>` | Yes | - | - |

---

### `HttpConnectionConfig`
HTTP connection.

Example:
```yaml
api_source:
  type: "http"
  base_url: "https://api.example.com"
  headers:
    Authorization: "Bearer ${API_TOKEN}"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **type** | `<class 'str'>` | No | `http` | - |
| **validation_mode** | `<class 'str'>` | No | `lazy` | - |
| **base_url** | `<class 'str'>` | Yes | - | - |
| **headers** | `Dict[str, str]` | No | `PydanticUndefined` | - |
| **auth** | `Dict[str, str]` | No | `PydanticUndefined` | - |

---

### `LocalConnectionConfig`
Local filesystem connection.

Example:
```yaml
local_data:
  type: "local"
  base_path: "./data"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **type** | `<enum 'ConnectionType'>` | No | `ConnectionType.LOCAL` | - |
| **validation_mode** | `<class 'str'>` | No | `lazy` | - |
| **base_path** | `<class 'str'>` | No | `./data` | Base directory path |

---

### `SQLServerConnectionConfig`
SQL Server connection.

Example:
```yaml
sql_dw:
  type: "sql_server"
  host: "server.database.windows.net"
  database: "dw"
  auth:
    mode: "aad_msi"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **type** | `<enum 'ConnectionType'>` | No | `ConnectionType.SQL_SERVER` | - |
| **validation_mode** | `<class 'str'>` | No | `lazy` | - |
| **host** | `<class 'str'>` | Yes | - | - |
| **database** | `<class 'str'>` | Yes | - | - |
| **port** | `<class 'int'>` | No | `1433` | - |
| **auth** | `Dict[str, str]` | No | `PydanticUndefined` | - |

---

## Node Operations

### `ReadConfig`
Configuration for reading data.

Example (File):
```yaml
read:
  connection: "local_data"
  format: "csv"
  path: "input/users.csv"
  options:
    header: true
```

Example (SQL):
```yaml
read:
  connection: "sql_dw"
  format: "sql"
  table: "users"  # OR query: "SELECT * FROM users"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **connection** | `<class 'str'>` | Yes | - | Connection name from project.yaml |
| **format** | `<class 'str'>` | Yes | - | Data format (csv, parquet, delta, etc.) |
| **table** | `Optional[str]` | No | - | Table name for SQL/Delta |
| **path** | `Optional[str]` | No | - | Path for file-based sources |
| **streaming** | `<class 'bool'>` | No | `False` | Enable streaming read (Spark only) |
| **query** | `Optional[str]` | No | - | SQL query (shortcut for options.query) |
| **options** | `Dict[str, Any]` | No | `PydanticUndefined` | Format-specific options |

---

### `TransformConfig`
Configuration for transforming data.

Example (SQL Mix):
```yaml
transform:
  steps:
    - sql: "SELECT * FROM df WHERE active = true"
    - function: "my_custom_func"
      params: { multiplier: 2 }
    - operation: "pivot"
      params: { index: "id", columns: "year", values: "sales" }
```

Example (Shortcuts):
```yaml
transform:
  steps:
    - "SELECT * FROM df"  # String is implied SQL
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **steps** | `List[Union[str, TransformStep]]` | Yes | - | List of transformation steps (SQL strings or TransformStep configs) |

---

### `ValidationConfig`
Configuration for data validation.

Example:
```yaml
validation:
  not_empty: true
  no_nulls: ["id", "email"]
  schema:
    id: "int"
    email: "string"
  allowed_values:
    status: ["active", "inactive"]
  ranges:
    age: { min: 0, max: 120 }
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **schema_validation** | `Optional[Dict[str, Any]]` | No | - | Schema validation rules |
| **not_empty** | `<class 'bool'>` | No | `False` | Ensure result is not empty |
| **no_nulls** | `List[str]` | No | `PydanticUndefined` | Columns that must not have nulls |
| **ranges** | `Dict[str, Dict[str, float]]` | No | `PydanticUndefined` | Value ranges {col: {min: 0, max: 100}} |
| **allowed_values** | `Dict[str, List[Any]]` | No | `PydanticUndefined` | Allowed values {col: [val1, val2]} |

---

### `WriteConfig`
Configuration for writing data.

Example:
```yaml
write:
  connection: "adls_silver"
  format: "delta"
  path: "silver/users"
  mode: "overwrite"
  register_table: "silver_users"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **connection** | `<class 'str'>` | Yes | - | Connection name from project.yaml |
| **format** | `<class 'str'>` | Yes | - | Output format (csv, parquet, delta, etc.) |
| **table** | `Optional[str]` | No | - | Table name for SQL/Delta |
| **path** | `Optional[str]` | No | - | Path for file-based outputs |
| **register_table** | `Optional[str]` | No | - | Register file output as external table (Spark/Delta only) |
| **mode** | `<enum 'WriteMode'>` | No | `WriteMode.OVERWRITE` | Write mode |
| **first_run_query** | `Optional[str]` | No | - | SQL query for full-load on first run (High Water Mark pattern). If set, uses this query when target table doesn't exist, then switches to incremental. Only applies to SQL reads. |
| **options** | `Dict[str, Any]` | No | `PydanticUndefined` | Format-specific options |

---

## Global Settings

### `AlertConfig`
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
| **type** | `<enum 'AlertType'>` | Yes | - | - |
| **url** | `<class 'str'>` | Yes | - | Webhook URL |
| **on_events** | `List[str]` | No | `['on_failure']` | Events to trigger alert: on_start, on_success, on_failure |
| **metadata** | `Dict[str, Any]` | No | `PydanticUndefined` | Extra metadata for alert |

---

### `LoggingConfig`
Logging configuration.

Example:
```yaml
logging:
  level: "INFO"
  structured: true
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **level** | `<enum 'LogLevel'>` | No | `LogLevel.INFO` | - |
| **structured** | `<class 'bool'>` | No | `False` | Output JSON logs |
| **metadata** | `Dict[str, Any]` | No | `PydanticUndefined` | Extra metadata in logs |

---

### `PerformanceConfig`
Performance tuning configuration.

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **use_arrow** | `<class 'bool'>` | No | `True` | Use Apache Arrow-backed DataFrames (Pandas only). Reduces memory and speeds up I/O. |

---

### `RetryConfig`
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
| **enabled** | `<class 'bool'>` | No | `True` | - |
| **max_attempts** | `<class 'int'>` | No | `3` | - |
| **backoff** | `<class 'str'>` | No | `exponential` | - |

---

### `StoryConfig`
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
| **connection** | `<class 'str'>` | Yes | - | Connection name for story output (uses connection's path resolution) |
| **path** | `<class 'str'>` | Yes | - | Path for stories (relative to connection base_path) |
| **max_sample_rows** | `<class 'int'>` | No | `10` | - |
| **auto_generate** | `<class 'bool'>` | No | `True` | - |
| **retention_days** | `Optional[int]` | No | `30` | Days to keep stories |
| **retention_count** | `Optional[int]` | No | `100` | Max number of stories to keep |

---

## Transformation Reference

### `AggregateParams`
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

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **group_by** | `List[str]` | Yes | - | Columns to group by |
| **aggregations** | `Dict[str, str]` | Yes | - | Map of column to aggregation function (sum, avg, min, max, count) |

---

### `CaseWhenParams`
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

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **cases** | `List[Dict[str, str]]` | Yes | - | List of {condition: ..., value: ...} |
| **default** | `<class 'str'>` | No | `NULL` | Default value if no condition met |
| **output_col** | `<class 'str'>` | Yes | - | Name of the resulting column |

---

### `CastColumnsParams`
Configuration for column type casting.

Example:
```yaml
cast_columns:
  casts:
    age: "int"
    salary: "double"
    created_at: "timestamp"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **casts** | `Dict[str, str]` | Yes | - | Map of column to target SQL type |

---

### `CleanTextParams`
Configuration for text cleaning.

Example:
```yaml
clean_text:
  columns: ["email", "username"]
  trim: true
  case: "lower"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **columns** | `List[str]` | Yes | - | List of columns to clean |
| **trim** | `<class 'bool'>` | No | `True` | Apply TRIM() |
| **case** | `Literal['lower', 'upper', 'preserve']` | No | `preserve` | Case conversion |

---

### `ConcatColumnsParams`
Configuration for string concatenation.

Example:
```yaml
concat_columns:
  columns: ["first_name", "last_name"]
  separator: " "
  output_col: "full_name"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **columns** | `List[str]` | Yes | - | Columns to concatenate |
| **separator** | `<class 'str'>` | No | - | Separator string |
| **output_col** | `<class 'str'>` | Yes | - | Resulting column name |

---

### `ConvertTimezoneParams`
Configuration for timezone conversion.

Example:
```yaml
convert_timezone:
  col: "utc_time"
  source_tz: "UTC"
  target_tz: "America/New_York"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **col** | `<class 'str'>` | Yes | - | Timestamp column to convert |
| **source_tz** | `<class 'str'>` | No | `UTC` | Source timezone (e.g., 'UTC', 'America/New_York') |
| **target_tz** | `<class 'str'>` | Yes | - | Target timezone (e.g., 'America/Los_Angeles') |
| **output_col** | `Optional[str]` | No | - | Name of the result column (default: {col}_{target_tz}) |

---

### `DateAddParams`
Configuration for date addition.

Example:
```yaml
date_add:
  col: "created_at"
  value: 1
  unit: "day"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **col** | `<class 'str'>` | Yes | - | - |
| **value** | `<class 'int'>` | Yes | - | - |
| **unit** | `Literal['day', 'month', 'year', 'hour', 'minute', 'second']` | Yes | - | - |

---

### `DateDiffParams`
Configuration for date difference.

Example:
```yaml
date_diff:
  start_col: "created_at"
  end_col: "updated_at"
  unit: "day"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **start_col** | `<class 'str'>` | Yes | - | - |
| **end_col** | `<class 'str'>` | Yes | - | - |
| **unit** | `Literal['day', 'hour', 'minute', 'second']` | No | `day` | - |

---

### `DateTruncParams`
Configuration for date truncation.

Example:
```yaml
date_trunc:
  col: "created_at"
  unit: "month"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **col** | `<class 'str'>` | Yes | - | - |
| **unit** | `Literal['year', 'month', 'day', 'hour', 'minute', 'second']` | Yes | - | - |

---

### `DeduplicateParams`
Configuration for deduplication.

Example:
```yaml
deduplicate:
  keys: ["id"]
  order_by: "updated_at DESC"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **keys** | `List[str]` | Yes | - | - |
| **order_by** | `Optional[str]` | No | - | SQL Order by clause (e.g. 'updated_at DESC') |

---

### `DeriveColumnsParams`
Configuration for derived columns.

Example:
```yaml
derive_columns:
  derivations:
    total_price: "quantity * unit_price"
    full_name: "concat(first_name, ' ', last_name)"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **derivations** | `Dict[str, str]` | Yes | - | Map of column name to SQL expression |

---

### `DictMappingParams`
Configuration for dictionary mapping.

Example:
```yaml
dict_based_mapping:
  column: "status_code"
  mapping:
    1: "Active"
    0: "Inactive"
  default: "Unknown"
  output_column: "status_desc"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **column** | `<class 'str'>` | Yes | - | - |
| **mapping** | `Dict[Any, Any]` | Yes | - | - |
| **default** | `Optional[Any]` | No | - | - |
| **output_column** | `Optional[str]` | No | - | - |

---

### `DistinctParams`
Configuration for distinct rows.

Example:
```yaml
distinct:
  columns: ["category", "status"]
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **columns** | `Optional[List[str]]` | No | - | Columns to project (if None, keeps all columns unique) |

---

### `ExplodeParams`
Configuration for exploding lists.

Example:
```yaml
explode_list_column:
  column: "items"
  outer: true
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **column** | `<class 'str'>` | Yes | - | - |
| **outer** | `<class 'bool'>` | No | `False` | If True, keep rows with empty lists (explode_outer) |

---

### `ExtractDateParams`
Configuration for extracting date parts.

Example:
```yaml
extract_date_parts:
  source_col: "created_at"
  prefix: "created"
  parts: ["year", "month"]
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **source_col** | `<class 'str'>` | Yes | - | - |
| **prefix** | `Optional[str]` | No | - | - |
| **parts** | `List[Literal['year', 'month', 'day', 'hour']]` | No | `['year', 'month', 'day']` | - |

---

### `FillNullsParams`
Configuration for filling null values.

Example:
```yaml
fill_nulls:
  values:
    count: 0
    description: "N/A"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **values** | `Dict[str, Union[str, int, float, bool]]` | Yes | - | Map of column to fill value |

---

### `FilterRowsParams`
Configuration for filtering rows.

Example:
```yaml
filter_rows:
  condition: "age > 18 AND status = 'active'"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **condition** | `<class 'str'>` | Yes | - | SQL WHERE clause (e.g., 'age > 18 AND status = "active"') |

---

### `HashParams`
Configuration for column hashing.

Example:
```yaml
hash_columns:
  columns: ["email", "ssn"]
  algorithm: "sha256"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **columns** | `List[str]` | Yes | - | - |
| **algorithm** | `Literal['sha256', 'md5']` | No | `sha256` | - |

---

### `JoinParams`
Configuration for joining datasets.

Example:
```yaml
join:
  right_dataset: "customers"
  on: "customer_id"
  how: "left"
  prefix: "cust"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **right_dataset** | `<class 'str'>` | Yes | - | Name of the node/dataset to join with |
| **on** | `Union[str, List[str]]` | Yes | - | Column(s) to join on |
| **how** | `Literal['inner', 'left', 'right', 'full', 'cross']` | No | `left` | Join type |
| **prefix** | `Optional[str]` | No | - | Prefix for columns from right dataset to avoid collisions |

---

### `LimitParams`
Configuration for result limiting.

Example:
```yaml
limit:
  n: 100
  offset: 0
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **n** | `<class 'int'>` | Yes | - | Number of rows to return |
| **offset** | `<class 'int'>` | No | `0` | Number of rows to skip |

---

### `MergeParams`
Configuration for Merge transformer (Upsert/Append).

Example:
```yaml
transformer: "merge"
params:
  target: "silver/customers"
  keys: ["customer_id"]
  strategy: "upsert"
  audit_cols:
    created_col: "created_at"
    updated_col: "updated_at"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **target** | `<class 'str'>` | Yes | - | Target table name or path |
| **keys** | `List[str]` | Yes | - | List of join keys |
| **strategy** | `<class 'str'>` | No | `upsert` | 'upsert', 'append_only', 'delete_match' |
| **audit_cols** | `Optional[Dict[str, str]]` | No | - | {'created_col': '...', 'updated_col': '...'} |
| **optimize_write** | `<class 'bool'>` | No | `False` | Run OPTIMIZE after write (Spark) |
| **zorder_by** | `Optional[List[str]]` | No | - | Columns to Z-Order by |
| **cluster_by** | `Optional[List[str]]` | No | - | Columns to Liquid Cluster by (Delta) |

---

### `NormalizeSchemaParams`
Configuration for schema normalization.

Example:
```yaml
normalize_schema:
  rename:
    old_col: "new_col"
  drop: ["unused_col"]
  select_order: ["id", "new_col", "created_at"]
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **rename** | `Optional[Dict[str, str]]` | No | `PydanticUndefined` | - |
| **drop** | `Optional[List[str]]` | No | `PydanticUndefined` | - |
| **select_order** | `Optional[List[str]]` | No | - | - |

---

### `ParseJsonParams`
Configuration for JSON parsing.

Example:
```yaml
parse_json:
  column: "raw_json"
  json_schema: "id INT, name STRING"
  output_col: "parsed_struct"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **column** | `<class 'str'>` | Yes | - | String column containing JSON |
| **json_schema** | `<class 'str'>` | Yes | - | DDL schema string (e.g. 'a INT, b STRING') or Spark StructType DDL |
| **output_col** | `Optional[str]` | No | - | - |

---

### `PivotParams`
Configuration for pivoting data.

Example:
```yaml
pivot:
  group_by: ["product_id", "region"]
  pivot_col: "month"
  agg_col: "sales"
  agg_func: "sum"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **group_by** | `List[str]` | Yes | - | - |
| **pivot_col** | `<class 'str'>` | Yes | - | - |
| **agg_col** | `<class 'str'>` | Yes | - | - |
| **agg_func** | `Literal['sum', 'count', 'avg', 'max', 'min', 'first']` | No | `sum` | - |
| **values** | `Optional[List[str]]` | No | - | Specific values to pivot (for Spark optimization) |

---

### `RegexReplaceParams`
Configuration for regex replacement.

Example:
```yaml
regex_replace:
  column: "phone"
  pattern: "[^0-9]"
  replacement: ""
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **column** | `<class 'str'>` | Yes | - | - |
| **pattern** | `<class 'str'>` | Yes | - | - |
| **replacement** | `<class 'str'>` | Yes | - | - |

---

### `SampleParams`
Configuration for random sampling.

Example:
```yaml
sample:
  fraction: 0.1
  seed: 42
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **fraction** | `<class 'float'>` | Yes | - | Fraction of rows to return (0.0 to 1.0) |
| **seed** | `Optional[int]` | No | - | - |

---

### `SortParams`
Configuration for sorting.

Example:
```yaml
sort:
  by: ["created_at", "id"]
  ascending: false
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **by** | `Union[str, List[str]]` | Yes | - | Column(s) to sort by |
| **ascending** | `<class 'bool'>` | No | `True` | Sort order |

---

### `SplitPartParams`
Configuration for splitting strings.

Example:
```yaml
split_part:
  col: "email"
  delimiter: "@"
  index: 2  # Extracts domain
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **col** | `<class 'str'>` | Yes | - | Column to split |
| **delimiter** | `<class 'str'>` | Yes | - | Delimiter to split by |
| **index** | `<class 'int'>` | Yes | - | 1-based index of the token to extract |

---

### `SurrogateKeyParams`
Configuration for surrogate key generation.

Example:
```yaml
generate_surrogate_key:
  columns: ["region", "product_id"]
  separator: "-"
  output_col: "unique_id"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **columns** | `List[str]` | Yes | - | Columns to combine for the key |
| **separator** | `<class 'str'>` | No | `-` | Separator between values |
| **output_col** | `<class 'str'>` | No | `surrogate_key` | Name of the output column |

---

### `UnionParams`
Configuration for unioning datasets.

Example:
```yaml
union:
  datasets: ["sales_2023", "sales_2024"]
  by_name: true
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **datasets** | `List[str]` | Yes | - | List of node names to union with current |
| **by_name** | `<class 'bool'>` | No | `True` | Match columns by name (UNION ALL BY NAME) |

---

### `UnpackStructParams`
Configuration for unpacking structs.

Example:
```yaml
unpack_struct:
  column: "user_info"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **column** | `<class 'str'>` | Yes | - | - |

---

### `UnpivotParams`
Configuration for unpivoting (melting) data.

Example:
```yaml
unpivot:
  id_cols: ["product_id"]
  value_vars: ["jan_sales", "feb_sales", "mar_sales"]
  var_name: "month"
  value_name: "sales"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **id_cols** | `List[str]` | Yes | - | - |
| **value_vars** | `List[str]` | Yes | - | - |
| **var_name** | `<class 'str'>` | No | `variable` | - |
| **value_name** | `<class 'str'>` | No | `value` | - |

---

### `ValidateAndFlagParams`
Configuration for validation flagging.

Example:
```yaml
validate_and_flag:
  flag_col: "data_issues"
  rules:
    age_check: "age >= 0"
    email_format: "email LIKE '%@%'"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **rules** | `Dict[str, str]` | Yes | - | Map of rule name to SQL condition (must be TRUE) |
| **flag_col** | `<class 'str'>` | No | `_issues` | Name of the column to store failed rules |

---

### `WindowCalculationParams`
Configuration for window functions.

Example:
```yaml
window_calculation:
  target_col: "cumulative_sales"
  function: "sum(sales)"
  partition_by: ["region"]
  order_by: "date ASC"
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| **target_col** | `<class 'str'>` | Yes | - | - |
| **function** | `<class 'str'>` | Yes | - | Window function e.g. 'sum(amount)', 'rank()' |
| **partition_by** | `List[str]` | No | `PydanticUndefined` | - |
| **order_by** | `Optional[str]` | No | - | - |

---
