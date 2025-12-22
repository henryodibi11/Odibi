# Connections

Unified connection system for accessing local filesystems, cloud storage, databases, and HTTP endpoints with pluggable authentication.

## Overview

Odibi's connection system provides:
- **Multiple backends**: Local filesystem, Azure ADLS, Azure SQL, HTTP APIs
- **Flexible authentication**: Service principals, managed identity, Key Vault, connection strings
- **Environment variables**: Secure secret injection via `${VAR}` syntax
- **Plugin architecture**: Register custom connection types via factory pattern

## Built-in Connection Types

| Type | Description |
|------|-------------|
| `local` | Local filesystem or URI-based paths |
| `local_dbfs` | Databricks File System mock for local development |
| `azure_adls` | Azure Data Lake Storage Gen2 |
| `azure_sql` | Azure SQL Database |
| `http` | HTTP/REST API endpoints |
| `delta` | Delta Lake tables (path-based or catalog) |

## Configuration

### Basic Structure

```yaml
connections:
  bronze:
    type: local
    base_path: ./data/bronze

  silver:
    type: azure_adls
    account_name: mystorageaccount
    container: data
    path_prefix: silver
    auth:
      key_vault_name: my-keyvault
      secret_name: storage-key
```

### Connection Config Options

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | Connection type (see table above) |
| `auth` | object | No | Authentication configuration |
| `auth_mode` | string | No | Authentication mode (auto-detected if omitted) |
| `validation_mode` | string | No | `eager` or `lazy` validation (default: `lazy`) |

## Local Connection

Simple filesystem connection for local development or mounted volumes.

```yaml
connections:
  raw_data:
    type: local
    base_path: ./data/raw

  mounted_volume:
    type: local
    base_path: /mnt/storage/data
```

### URI-Based Paths

Supports URI schemes like `file://` or `dbfs:/`:

```yaml
connections:
  dbfs_data:
    type: local
    base_path: dbfs:/FileStore/data
```

### Config Options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_path` | string | `./data` | Base directory for all paths |

## Local DBFS Connection

Mock DBFS for testing Databricks pipelines locally.

```yaml
connections:
  dbfs:
    type: local_dbfs
    root: .dbfs
```

Maps `dbfs:/FileStore/data.csv` to `.dbfs/FileStore/data.csv`.

### Config Options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `root` | string | `.dbfs` | Local directory to use as DBFS root |

## Azure Data Lake Storage (ADLS) Connection

Azure Data Lake Storage Gen2 with multi-mode authentication.

```yaml
connections:
  datalake:
    type: azure_adls
    account_name: mystorageaccount
    container: datalake
    path_prefix: bronze
    auth_mode: key_vault
    auth:
      key_vault_name: my-keyvault
      secret_name: storage-account-key
```

### Config Options

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `account_name` | string | Yes | Storage account name |
| `container` | string | Yes | Container/filesystem name |
| `path_prefix` | string | No | Optional prefix for all paths |
| `auth_mode` | string | No | Authentication mode (auto-detected) |

### Authentication Modes

#### Key Vault (Recommended)

Retrieves storage account key from Azure Key Vault:

```yaml
connections:
  secure_storage:
    type: azure_adls
    account_name: mystorageaccount
    container: data
    auth_mode: key_vault
    auth:
      key_vault_name: my-keyvault
      secret_name: storage-account-key
```

#### Service Principal

OAuth authentication with Azure AD service principal:

```yaml
connections:
  sp_storage:
    type: azure_adls
    account_name: mystorageaccount
    container: data
    auth_mode: service_principal
    auth:
      tenant_id: ${AZURE_TENANT_ID}
      client_id: ${AZURE_CLIENT_ID}
      client_secret: ${AZURE_CLIENT_SECRET}
```

#### Managed Identity

Use Azure Managed Identity (recommended for Azure-hosted workloads):

```yaml
connections:
  msi_storage:
    type: azure_adls
    account_name: mystorageaccount
    container: data
    auth_mode: managed_identity
```

#### SAS Token

Shared Access Signature for time-limited access:

```yaml
connections:
  sas_storage:
    type: azure_adls
    account_name: mystorageaccount
    container: data
    auth_mode: sas_token
    auth:
      sas_token: ${STORAGE_SAS_TOKEN}
```

#### Direct Key (Development Only)

⚠️ **Not recommended for production**

```yaml
connections:
  dev_storage:
    type: azure_adls
    account_name: mystorageaccount
    container: data
    auth_mode: direct_key
    auth:
      account_key: ${STORAGE_ACCOUNT_KEY}
```

### Path Resolution

ADLS connections generate `abfss://` URIs:

```python
conn.get_path("folder/file.parquet")
# Returns: abfss://data@mystorageaccount.dfs.core.windows.net/bronze/folder/file.parquet
```

## Azure SQL Connection

Azure SQL Database with SQL auth, Managed Identity, or Key Vault.

```yaml
connections:
  warehouse:
    type: azure_sql
    host: myserver.database.windows.net
    database: analytics
    auth_mode: aad_msi
```

### Config Options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `host` / `server` | string | Required | SQL Server hostname |
| `database` | string | Required | Database name |
| `driver` | string | `ODBC Driver 18 for SQL Server` | ODBC driver |
| `port` | int | `1433` | SQL Server port |
| `timeout` | int | `30` | Connection timeout (seconds) |
| `auth_mode` | string | Auto | `sql`, `aad_msi`, `key_vault` |

### Authentication Modes

#### SQL Authentication

```yaml
connections:
  sql_auth:
    type: azure_sql
    host: myserver.database.windows.net
    database: mydb
    auth_mode: sql
    auth:
      username: ${SQL_USERNAME}
      password: ${SQL_PASSWORD}
```

#### Managed Identity

```yaml
connections:
  msi_sql:
    type: azure_sql
    host: myserver.database.windows.net
    database: mydb
    auth_mode: aad_msi
```

#### Key Vault

```yaml
connections:
  keyvault_sql:
    type: azure_sql
    host: myserver.database.windows.net
    database: mydb
    auth_mode: key_vault
    auth:
      username: sqladmin
      key_vault_name: my-keyvault
      secret_name: sql-password
```

### Usage

```python
from odibi.connections.azure_sql import AzureSQL

conn = AzureSQL(
    server="myserver.database.windows.net",
    database="analytics",
    auth_mode="aad_msi",
)

# Read data
df = conn.read_sql("SELECT * FROM customers WHERE region = 'US'")

# Read entire table
df = conn.read_table("orders", schema="dbo")

# Write data
conn.write_table(df, "processed_orders", if_exists="replace")

# Execute statements
conn.execute("DELETE FROM staging WHERE processed = 1")
```

## HTTP Connection

Connect to REST APIs with various authentication methods.

```yaml
connections:
  api:
    type: http
    base_url: https://api.example.com/v1/
    auth:
      token: ${API_TOKEN}
```

### Config Options

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `base_url` | string | Yes | Base URL for API |
| `headers` | object | No | Default request headers |
| `auth` | object | No | Authentication configuration |

### Authentication Methods

#### Bearer Token

```yaml
connections:
  bearer_api:
    type: http
    base_url: https://api.example.com/
    auth:
      token: ${API_BEARER_TOKEN}
```

#### Basic Auth

```yaml
connections:
  basic_api:
    type: http
    base_url: https://api.example.com/
    auth:
      username: ${API_USER}
      password: ${API_PASSWORD}
```

#### API Key

```yaml
connections:
  apikey_api:
    type: http
    base_url: https://api.example.com/
    auth:
      api_key: ${API_KEY}
      header_name: X-API-Key  # Optional, defaults to X-API-Key
```

### Custom Headers

```yaml
connections:
  custom_api:
    type: http
    base_url: https://api.example.com/
    headers:
      Content-Type: application/json
      X-Custom-Header: custom-value
    auth:
      token: ${API_TOKEN}
```

## Delta Connection

Delta Lake tables via path or Unity Catalog.

### Path-Based Delta

```yaml
connections:
  delta_lake:
    type: delta
    path: /mnt/delta/tables
```

### Catalog-Based Delta (Spark)

```yaml
connections:
  unity_catalog:
    type: delta
    catalog: main
    schema: analytics
```

## Environment Variables

Use `${VAR}` syntax to inject secrets from environment variables:

```yaml
connections:
  secure:
    type: azure_adls
    account_name: ${STORAGE_ACCOUNT}
    container: data
    auth:
      client_id: ${AZURE_CLIENT_ID}
      client_secret: ${AZURE_CLIENT_SECRET}
      tenant_id: ${AZURE_TENANT_ID}
```

Environment variables are resolved at runtime, keeping secrets out of configuration files.

## Connection Factory

Odibi uses a plugin system for connection types. Built-in types are registered automatically.

### Registering Custom Connections

```python
from odibi.plugins import register_connection_factory
from odibi.connections.base import BaseConnection

class MyCustomConnection(BaseConnection):
    def __init__(self, endpoint: str, api_key: str):
        self.endpoint = endpoint
        self.api_key = api_key

    def get_path(self, relative_path: str) -> str:
        return f"{self.endpoint}/{relative_path}"

    def validate(self) -> None:
        if not self.endpoint:
            raise ValueError("Endpoint is required")

def create_custom_connection(name: str, config: dict):
    return MyCustomConnection(
        endpoint=config["endpoint"],
        api_key=config.get("api_key", ""),
    )

# Register the factory
register_connection_factory("my_custom", create_custom_connection)
```

Then use in YAML:

```yaml
connections:
  custom:
    type: my_custom
    endpoint: https://custom-service.example.com
    api_key: ${CUSTOM_API_KEY}
```

### Built-in Factory Registration

Built-in connections are registered via `register_builtins()`:

| Factory Name | Connection Class |
|--------------|------------------|
| `local` | `LocalConnection` |
| `http` | `HttpConnection` |
| `azure_blob` | `AzureADLS` |
| `azure_adls` | `AzureADLS` |
| `delta` | `LocalConnection` or `DeltaCatalogConnection` |
| `sql_server` | `AzureSQL` |
| `azure_sql` | `AzureSQL` |

## Complete Examples

### Multi-Environment Setup

```yaml
project: DataPipeline
engine: spark

connections:
  # Local development
  local_bronze:
    type: local
    base_path: ./data/bronze

  local_silver:
    type: local
    base_path: ./data/silver

  # Azure production
  azure_bronze:
    type: azure_adls
    account_name: ${STORAGE_ACCOUNT}
    container: datalake
    path_prefix: bronze
    auth_mode: managed_identity

  azure_silver:
    type: azure_adls
    account_name: ${STORAGE_ACCOUNT}
    container: datalake
    path_prefix: silver
    auth_mode: managed_identity

  # SQL database
  warehouse:
    type: azure_sql
    host: ${SQL_SERVER}
    database: analytics
    auth_mode: aad_msi

  # External API
  weather_api:
    type: http
    base_url: https://api.weather.com/v1/
    auth:
      api_key: ${WEATHER_API_KEY}

pipelines:
  - pipeline: ingest_orders
    nodes:
      - name: read_orders
        source:
          connection: azure_bronze
          path: orders/
        # ...
```

### Service Principal Authentication

```yaml
connections:
  adls_sp:
    type: azure_adls
    account_name: mystorageaccount
    container: data
    path_prefix: ingestion
    auth_mode: service_principal
    auth:
      tenant_id: ${AZURE_TENANT_ID}
      client_id: ${AZURE_CLIENT_ID}
      client_secret: ${AZURE_CLIENT_SECRET}

  sql_sp:
    type: azure_sql
    host: myserver.database.windows.net
    database: warehouse
    auth_mode: sql
    auth:
      username: ${SQL_USER}
      password: ${SQL_PASSWORD}
```

### Key Vault Integration

```yaml
connections:
  secure_storage:
    type: azure_adls
    account_name: mystorageaccount
    container: sensitive-data
    auth_mode: key_vault
    auth:
      key_vault_name: my-keyvault
      secret_name: storage-account-key

  secure_sql:
    type: azure_sql
    host: myserver.database.windows.net
    database: secure_db
    auth_mode: key_vault
    auth:
      username: sqladmin
      key_vault_name: my-keyvault
      secret_name: sql-admin-password
```

## Best Practices

1. **Use Managed Identity** - Preferred for Azure-hosted workloads (no secrets to manage)
2. **Use Key Vault** - Store secrets in Key Vault, not config files
3. **Environment variables** - Use `${VAR}` for any sensitive values
4. **Lazy validation** - Default `validation_mode: lazy` defers validation until first use
5. **Separate connections** - Use different connections for different security zones
6. **Register secrets** - Secrets are automatically registered for log redaction

## Related

- [YAML Schema Reference](../reference/yaml_schema.md#connections)
- [Pipeline Configuration](pipelines.md)
- [Secrets Management](../guides/secrets.md)
