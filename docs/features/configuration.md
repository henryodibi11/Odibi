# Configuration System

YAML-based configuration for defining projects, pipelines, and nodes with built-in validation, environment variable support, and environment-specific overrides.

## Overview

Odibi's configuration system provides:
- **YAML-based**: Human-readable, version-controllable configuration files
- **Pydantic validation**: Type-safe configuration with helpful error messages
- **Environment variables**: Secure secret injection with `${VAR}` syntax
- **Environment overrides**: Dev/staging/prod configurations in a single file
- **Hierarchical structure**: Project → Pipelines → Nodes

## Project Configuration

`ProjectConfig` is the root configuration defining the entire Odibi project.

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `project` | string | Project name |
| `connections` | object | Named connections (at least one required) |
| `pipelines` | list | Pipeline definitions (at least one required) |
| `story` | object | Story generation configuration |
| `system` | object | System Catalog configuration |

### Optional Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `engine` | string | `pandas` | Execution engine: `spark`, `pandas` |
| `version` | string | `1.0.0` | Project version |
| `description` | string | - | Project description |
| `owner` | string | - | Project owner/contact |
| `vars` | object | `{}` | Global variables for substitution |
| `retry` | object | See below | Retry configuration |
| `logging` | object | See below | Logging configuration |
| `alerts` | list | `[]` | Alert configurations |
| `performance` | object | See below | Performance tuning |
| `lineage` | object | - | OpenLineage configuration |
| `environments` | object | - | Environment-specific overrides |

### Basic Example

```yaml
project: "Customer360"
engine: "spark"
version: "1.0.0"

connections:
  bronze:
    type: "local"
    base_path: "./data/bronze"
  silver:
    type: "delta"
    catalog: "spark_catalog"
    schema: "silver_db"

story:
  connection: "bronze"
  path: "stories/"
  retention_days: 30

system:
  connection: "bronze"
  path: "_odibi_system"

pipelines:
  - pipeline: "customer_ingestion"
    nodes:
      - name: "load_customers"
        read: { connection: "bronze", format: "csv", path: "customers.csv" }
        write: { connection: "silver", table: "customers" }
```

### Retry Configuration

```yaml
retry:
  enabled: true
  max_attempts: 3        # 1-10
  backoff: "exponential" # exponential, linear, constant
```

### Logging Configuration

```yaml
logging:
  level: "INFO"          # DEBUG, INFO, WARNING, ERROR
  structured: true       # JSON logs for Splunk/Datadog
  metadata:
    team: "data-platform"
```

### Performance Configuration

```yaml
performance:
  use_arrow: true  # Use Apache Arrow-backed DataFrames (Pandas only)
```

### Story Configuration

```yaml
story:
  connection: "bronze"        # Must exist in connections
  path: "stories/"            # Path relative to connection
  max_sample_rows: 10         # 0-100
  auto_generate: true
  retention_days: 30          # Days to keep stories
  retention_count: 100        # Max stories to keep
```

### System Configuration

```yaml
system:
  connection: "bronze"        # Must exist in connections
  path: "_odibi_system"       # Path relative to connection root
```

### Lineage Configuration

```yaml
lineage:
  url: "http://localhost:5000"
  namespace: "my_project"
  api_key: "${LINEAGE_API_KEY}"
```

## Pipeline Configuration

`PipelineConfig` groups related nodes into a logical unit.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `pipeline` | string | Yes | Pipeline name |
| `description` | string | No | Pipeline description |
| `layer` | string | No | Logical layer: `bronze`, `silver`, `gold` |
| `nodes` | list | Yes | List of nodes (unique names required) |

```yaml
pipelines:
  - pipeline: "user_onboarding"
    description: "Ingest and process new users"
    layer: "silver"
    nodes:
      - name: "load_users"
        # ...
      - name: "clean_users"
        depends_on: ["load_users"]
        # ...
```

## Node Configuration

`NodeConfig` defines individual data processing steps.

### Core Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique node name |
| `description` | string | No | Human-readable description |
| `enabled` | bool | No | If `false`, node is skipped (default: `true`) |
| `tags` | list | No | Tags for selective execution (`odibi run --tag daily`) |
| `depends_on` | list | No | Parent nodes that must complete first |

### Operations (at least one required)

| Field | Type | Description |
|-------|------|-------------|
| `read` | object | Input operation (load data) |
| `transformer` | string | Built-in transformation app (e.g., `deduplicate`, `scd2`) |
| `params` | object | Parameters for transformer |
| `transform` | object | Chain of transformation steps |
| `write` | object | Output operation (save data) |

### Execution Order

1. **Read** (or dependency injection if no read block)
2. **Transformer** (the "App" logic)
3. **Transform Steps** (the "Script" logic)
4. **Validation**
5. **Write**

### Read Configuration

```yaml
read:
  connection: "bronze"
  format: "parquet"           # csv, parquet, delta, json, sql
  path: "customers/"
  # OR for SQL
  query: "SELECT * FROM customers WHERE active = 1"

  # Incremental loading
  incremental:
    mode: "rolling_window"    # or "stateful"
    column: "updated_at"
    lookback: 3
    unit: "day"

  # Time travel (Delta)
  time_travel:
    as_of_version: 10
    # OR as_of_timestamp: "2023-10-01T12:00:00Z"
```

### Transform Configuration

```yaml
transform:
  steps:
    # SQL step
    - sql: "SELECT * FROM df WHERE status = 'active'"

    # Function step
    - function: "clean_text"
      params:
        columns: ["email"]
        case: "lower"

    # Operation step
    - operation: "detect_deletes"
      params:
        mode: "sql_compare"
        keys: ["customer_id"]
```

### Write Configuration

```yaml
write:
  connection: "silver"
  format: "delta"
  table: "customers"
  mode: "upsert"              # overwrite, append, upsert, append_once

  # Metadata columns
  add_metadata: true          # or selective: {extracted_at: true, source_file: false}
```

### Validation Configuration

```yaml
validation:
  tests:
    - type: not_null
      columns: [customer_id, email]
      on_fail: quarantine     # fail, warn, quarantine

    - type: unique
      columns: [customer_id]

    - type: accepted_values
      column: status
      values: ["active", "inactive", "pending"]

    - type: custom_sql
      sql: "COUNT(*) FILTER (WHERE age < 0) = 0"
      message: "Negative ages found"

  quarantine:
    connection: "silver"
    path: "quarantine/customers"

  gate:
    require_pass_rate: 0.95   # Block if < 95% pass
```

### Contracts (Pre-conditions)

```yaml
contracts:
  - type: row_count
    min: 1000
    on_fail: fail

  - type: freshness
    column: "updated_at"
    max_age_hours: 24

  - type: schema
    columns:
      id: "int"
      name: "string"
```

### Privacy Configuration

```yaml
privacy:
  enabled: true
  rules:
    - column: "email"
      method: "hash"          # hash, mask, redact, fake
    - column: "ssn"
      method: "mask"
      params:
        pattern: "XXX-XX-####"
```

### Error Handling

```yaml
on_error: "fail_later"        # fail_fast, fail_later, ignore
```

| Strategy | Description |
|----------|-------------|
| `fail_fast` | Stop pipeline immediately on error |
| `fail_later` | Continue pipeline, skip dependents (default) |
| `ignore` | Treat as success with warning, dependents run |

### Complete Node Example

```yaml
- name: "process_orders"
  description: "Clean and deduplicate orders"
  tags: ["daily", "critical"]
  depends_on: ["load_orders"]

  transformer: "deduplicate"
  params:
    keys: ["order_id"]
    order_by: "updated_at DESC"

  transform:
    steps:
      - sql: "SELECT * FROM df WHERE status != 'cancelled'"

  validation:
    tests:
      - type: not_null
        columns: [order_id, customer_id]
        on_fail: quarantine
    quarantine:
      connection: "silver"
      path: "quarantine/orders"
    gate:
      require_pass_rate: 0.98

  write:
    connection: "gold"
    format: "delta"
    table: "orders_clean"
    mode: "upsert"

  on_error: "fail_fast"
  cache: true
  log_level: "DEBUG"
```

## Environment Variables

Use `${VAR_NAME}` syntax to inject environment variables:

```yaml
connections:
  azure_blob:
    type: "azure_blob"
    account_name: "myaccount"
    container: "data"
    auth:
      mode: "account_key"
      account_key: "${AZURE_STORAGE_KEY}"

alerts:
  - type: slack
    url: "${SLACK_WEBHOOK_URL}"
```

Variables are resolved at configuration load time. Missing variables raise an error.

### Global Variables

Define reusable variables in `vars`:

```yaml
vars:
  env: "production"
  team: "data-platform"

logging:
  metadata:
    environment: "${vars.env}"
    team: "${vars.team}"
```

## Environment Overrides

Define environment-specific configurations that override base settings:

```yaml
project: "Customer360"
engine: "pandas"

connections:
  database:
    type: "sql_server"
    host: "dev-server.database.windows.net"
    database: "dev_db"

environments:
  staging:
    connections:
      database:
        host: "staging-server.database.windows.net"
        database: "staging_db"

  production:
    engine: "spark"
    connections:
      database:
        host: "prod-server.database.windows.net"
        database: "prod_db"
    logging:
      level: "WARNING"
      structured: true
```

Select environment at runtime:
```bash
odibi run --env production
```

## Validation

Odibi uses Pydantic for configuration validation, providing:

### Type Checking

```yaml
# This will fail: max_attempts must be integer 1-10
retry:
  max_attempts: 100  # Error: ensure this value is less than or equal to 10
```

### Required Field Validation

```yaml
# This will fail: 'project' is required
engine: "spark"
pipelines: []
# Error: field required - project
```

### Cross-Field Validation

```yaml
# This will fail: story.connection must exist in connections
connections:
  bronze:
    type: "local"
    base_path: "./data"

story:
  connection: "silver"  # Error: Story connection 'silver' not found
  path: "stories/"
```

### Node Validation

```yaml
# This will fail: node must have at least one operation
- name: "empty_node"
  # Error: Node 'empty_node' must have at least one of: read, transform, write, transformer
```

### Loading Configuration

```python
from odibi.config import load_config_from_file, ProjectConfig

# From file (with env var substitution)
config = load_config_from_file("odibi.yaml")

# From dict (programmatic)
config = ProjectConfig(
    project="MyProject",
    connections={"local": {"type": "local", "base_path": "./data"}},
    pipelines=[...],
    story={"connection": "local", "path": "stories/"},
    system={"connection": "local"},
)
```

## Complete Example

```yaml
project: "E-Commerce Analytics"
version: "2.0.0"
engine: "spark"
owner: "data-team@company.com"

vars:
  env: "production"

# Resilience
retry:
  enabled: true
  max_attempts: 3
  backoff: "exponential"

# Observability
logging:
  level: "INFO"
  structured: true
  metadata:
    environment: "${vars.env}"

# Alerting
alerts:
  - type: slack
    url: "${SLACK_WEBHOOK_URL}"
    on_events:
      - on_failure
      - on_gate_block
    metadata:
      throttle_minutes: 15
      channel: "#data-alerts"

# Performance
performance:
  use_arrow: true

# Lineage
lineage:
  url: "http://marquez:5000"
  namespace: "ecommerce"

# Connections
connections:
  landing:
    type: "azure_blob"
    account_name: "datalake"
    container: "landing"
    auth:
      mode: "aad_msi"

  bronze:
    type: "delta"
    catalog: "spark_catalog"
    schema: "bronze"

  silver:
    type: "delta"
    catalog: "spark_catalog"
    schema: "silver"

  gold:
    type: "delta"
    catalog: "spark_catalog"
    schema: "gold"

# Story output
story:
  connection: "bronze"
  path: "_stories/"
  max_sample_rows: 10
  retention_days: 30

# System catalog
system:
  connection: "bronze"
  path: "_odibi_system"

# Pipelines
pipelines:
  - pipeline: "orders_bronze"
    layer: "bronze"
    nodes:
      - name: "ingest_orders"
        read:
          connection: "landing"
          format: "json"
          path: "orders/*.json"
          incremental:
            mode: "stateful"
            column: "order_date"
        write:
          connection: "bronze"
          table: "raw_orders"
          mode: "append"
          add_metadata: true

  - pipeline: "orders_silver"
    layer: "silver"
    nodes:
      - name: "clean_orders"
        depends_on: ["ingest_orders"]

        transformer: "deduplicate"
        params:
          keys: ["order_id"]
          order_by: "updated_at DESC"

        transform:
          steps:
            - sql: "SELECT * FROM df WHERE order_total > 0"
            - function: "clean_text"
              params:
                columns: ["customer_email"]
                case: "lower"

        validation:
          tests:
            - type: not_null
              columns: [order_id, customer_id]
              on_fail: quarantine
            - type: range
              column: "order_total"
              min: 0
          quarantine:
            connection: "silver"
            path: "quarantine/orders"
          gate:
            require_pass_rate: 0.95

        write:
          connection: "silver"
          table: "orders"
          mode: "upsert"

# Environment overrides
environments:
  dev:
    engine: "pandas"
    logging:
      level: "DEBUG"
    connections:
      landing:
        type: "local"
        base_path: "./test_data/landing"
      bronze:
        type: "local"
        base_path: "./test_data/bronze"
```

## Related

- [YAML Schema Reference](../reference/yaml_schema.md) - Complete field reference
- [Alerting](alerting.md) - Alert configuration details
- [Quality Gates](quality_gates.md) - Validation and gates
- [Quarantine Tables](quarantine.md) - Quarantine configuration
