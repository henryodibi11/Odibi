# ODIBI Configuration Hierarchy

This document details the complete hierarchy of configuration objects available in the ODIBI framework.

## 🌳 Visual Hierarchy

```
ProjectConfig
├── project: str (Required)
├── engine: "pandas" | "spark" (Default: "pandas")
├── description: str (Optional)
├── version: str (Default: "1.0.0")
├── owner: str (Optional)
├── connections: Dict[str, ConnectionConfig]
│   ├── type: "local"
│   │   ├── validation_mode: "lazy" | "eager"
│   │   └── base_path: str (Default: "./data")
│   ├── type: "azure_blob"
│   │   ├── validation_mode: "lazy" | "eager"
│   │   ├── account_name: str
│   │   ├── container: str
│   │   └── auth: Dict[str, str]
│   ├── type: "delta"
│   │   ├── validation_mode: "lazy" | "eager"
│   │   ├── catalog: str
│   │   └── schema: str
│   └── type: "sql_server"
│       ├── validation_mode: "lazy" | "eager"
│       ├── host: str
│       ├── database: str
│       ├── port: int (Default: 1433)
│       └── auth: Dict[str, str]
├── pipelines: List[PipelineConfig]
│   └── PipelineConfig
│       ├── pipeline: str (Required)
│       ├── description: str (Optional)
│       ├── layer: str (Optional)
│       └── nodes: List[NodeConfig]
│           └── NodeConfig
│               ├── name: str (Required)
│               ├── description: str (Optional)
│               ├── depends_on: List[str] (Default: [])
│               ├── cache: bool (Default: False)
│               ├── read: ReadConfig (Optional)
│               │   ├── connection: str
│               │   ├── format: str
│               │   ├── path: str (File based)
│               │   ├── table: str (SQL/Delta based)
│               │   └── options: Dict[str, Any]
│               ├── transform: TransformConfig (Optional)
│               │   └── steps: List[TransformStep | str]
│               │       ├── str (SQL Query)
│               │       └── TransformStep
│               │           ├── sql: str
│               │           ├── function: str
│               │           ├── operation: str
│               │           └── params: Dict[str, Any]
│               ├── write: WriteConfig (Optional)
│               │   ├── connection: str
│               │   ├── format: str
│               │   ├── mode: "overwrite" | "append"
│               │   ├── path: str (File based)
│               │   ├── table: str (SQL/Delta based)
│               │   └── options: Dict[str, Any]
│               └── validation: ValidationConfig (Optional)
│                   ├── schema: Dict[str, Any]
│                   ├── not_empty: bool (Default: False)
│                   └── no_nulls: List[str] (Default: [])
├── story: StoryConfig (Required)
│   ├── connection: str
│   ├── path: str
│   ├── max_sample_rows: int (Default: 10)
│   └── auto_generate: bool (Default: True)
├── retry: RetryConfig (Optional)
│   ├── enabled: bool (Default: True)
│   ├── max_attempts: int (Default: 3)
│   └── backoff: "exponential" | "linear" | "constant"
└── logging: LoggingConfig (Optional)
    ├── level: "DEBUG" | "INFO" | "WARNING" | "ERROR"
    ├── structured: bool (Default: False)
    └── metadata: Dict[str, Any]
```

---

## 📚 Detailed Reference

### 1. Project Configuration (`ProjectConfig`)
The top-level configuration object.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `project` | `str` | Yes | - | Name of the project. |
| `engine` | `str` | No | `"pandas"` | Execution engine: `"spark"` or `"pandas"`. |
| `connections` | `Dict` | Yes | - | Dictionary of connection definitions. |
| `pipelines` | `List` | Yes | - | List of pipeline definitions. |
| `story` | `StoryConfig` | Yes | - | Configuration for story generation. |
| `description` | `str` | No | `None` | Project description. |
| `version` | `str` | No | `"1.0.0"` | Project version. |
| `owner` | `str` | No | `None` | Project owner/contact. |
| `retry` | `RetryConfig` | No | `{}` | Global retry settings. |
| `logging` | `LoggingConfig` | No | `{}` | Global logging settings. |

### 2. Connection Configurations
Configured in the `connections` dictionary. The `type` field determines the class used.

#### Common Fields
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `str` | Required | `"local"`, `"azure_blob"`, `"delta"`, or `"sql_server"` |
| `validation_mode` | `str` | `"lazy"` | `"lazy"` or `"eager"` connection validation |

#### Local Connection (`type: local`)
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_path` | `str` | `"./data"` | Base directory for local files. |

#### Azure Blob Connection (`type: azure_blob`)
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `account_name` | `str` | Required | Azure storage account name. |
| `container` | `str` | Required | Container name. |
| `auth` | `Dict` | `{}` | Authentication details (e.g., SAS token, account key). |

#### Delta Lake Connection (`type: delta`)
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `catalog` | `str` | Required | Delta catalog name. |
| `schema` | `str` | Required | Schema (database) name. (Alias for `schema_name`) |

#### SQL Server Connection (`type: sql_server`)
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `host` | `str` | Required | Server hostname. |
| `database` | `str` | Required | Database name. |
| `port` | `int` | `1433` | Port number. |
| `auth` | `Dict` | `{}` | Authentication (username, password). |

### 3. Pipeline Configuration (`PipelineConfig`)
Defines a sequence of data processing nodes.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `pipeline` | `str` | Yes | - | Unique name for the pipeline. |
| `nodes` | `List[NodeConfig]` | Yes | - | List of processing nodes. |
| `description` | `str` | No | `None` | Description of the pipeline's purpose. |
| `layer` | `str` | No | `None` | Logical layer (e.g., `"bronze"`, `"silver"`). |

### 4. Node Configuration (`NodeConfig`)
A single unit of work (Read -> Transform -> Write).

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | `str` | Yes | - | Unique name for the node. |
| `depends_on` | `List[str]` | No | `[]` | List of node names this node waits for. |
| `read` | `ReadConfig` | No | `None` | Configuration for reading data. |
| `transform` | `TransformConfig` | No | `None` | Configuration for data transformation. |
| `write` | `WriteConfig` | No | `None` | Configuration for writing data. |
| `cache` | `bool` | No | `False` | Whether to cache the result in memory. |
| `validation` | `ValidationConfig` | No | `None` | Data quality rules. |
| `description` | `str` | No | `None` | Node description. |

*(Note: At least one of `read`, `transform`, or `write` is required.)*

### 5. Operation Configurations

#### Read Configuration (`ReadConfig`)
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `connection` | `str` | Yes | Name of a connection defined in `connections`. |
| `format` | `str` | Yes | format (e.g., `"csv"`, `"parquet"`, `"delta"`). |
| `path` | `str` | * | File path (relative to connection base). |
| `table` | `str` | * | Table name (for SQL/Delta). |
| `options` | `Dict` | No | Engine-specific options (e.g., `header=True`). |

*(* Either `path` or `table` is required)*

#### Transform Configuration (`TransformConfig`)
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `steps` | `List` | Yes | List of SQL strings or `TransformStep` objects. |

**Transform Step (`TransformStep`)**
| Field | Type | Description |
|-------|------|-------------|
| `sql` | `str` | SQL query string. |
| `function` | `str` | Registered Python function name. |
| `operation` | `str` | Built-in operation name. |
| `params` | `Dict` | Parameters passed to the function/operation. |

*(* Exactly one of `sql`, `function`, or `operation` is required per step)*

#### Write Configuration (`WriteConfig`)
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `connection` | `str` | Yes | Name of a connection defined in `connections`. |
| `format` | `str` | Yes | format. |
| `mode` | `str` | No | `"overwrite"` (default) or `"append"`. |
| `path` | `str` | * | Output file path. |
| `table` | `str` | * | Output table name. |
| `options` | `Dict` | No | Engine-specific write options. |

*(* Either `path` or `table` is required)*

#### Validation Configuration (`ValidationConfig`)
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `schema` | `Dict` | `None` | Schema definition for validation. |
| `not_empty` | `bool` | `False` | Fail if result dataframe is empty. |
| `no_nulls` | `List[str]` | `[]` | List of columns that cannot contain nulls. |

### 6. Support Configurations

#### Story Configuration (`StoryConfig`)
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection` | `str` | Required | Connection name to store stories. |
| `path` | `str` | Required | Path to store story files. |
| `max_sample_rows` | `int` | `10` | Max rows to show in data samples. |
| `auto_generate` | `bool` | `True` | Whether to generate stories automatically. |

#### Retry Configuration (`RetryConfig`)
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `True` | Enable/disable retries. |
| `max_attempts` | `int` | `3` | Maximum retry attempts. |
| `backoff` | `str` | `"exponential"` | Backoff strategy (`"exponential"`, `"linear"`, `"constant"`). |

#### Logging Configuration (`LoggingConfig`)
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `level` | `str` | `"INFO"` | `"DEBUG"`, `"INFO"`, `"WARNING"`, `"ERROR"`. |
| `structured` | `bool` | `False` | Enable JSON structured logging. |
| `metadata` | `Dict` | `{}` | Extra static metadata for logs. |
