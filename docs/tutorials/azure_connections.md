# Azure Connections Tutorial

This tutorial shows how to connect Odibi to Azure data services: Blob Storage, ADLS Gen2, and Azure SQL.

## Prerequisites

- Odibi installed (`pip install odibi`)
- Azure subscription with appropriate permissions
- Azure CLI installed (`az login` completed)

## 1. Azure Blob Storage

### Service Principal Authentication (Recommended for Production)

```yaml
connections:
  azure_landing:
    type: azure_blob
    account_name: mystorageaccount
    container: landing
    auth:
      mode: service_principal
      tenant_id: "${AZURE_TENANT_ID}"
      client_id: "${AZURE_CLIENT_ID}"
      client_secret: "${AZURE_CLIENT_SECRET}"
```

### Managed Identity (Databricks/Synapse)

```yaml
connections:
  azure_landing:
    type: azure_blob
    account_name: mystorageaccount
    container: landing
    auth:
      mode: managed_identity
```

### SAS Token

```yaml
connections:
  azure_landing:
    type: azure_blob
    account_name: mystorageaccount
    container: landing
    auth:
      mode: sas
      token: "${AZURE_SAS_TOKEN}"
```

### Connection String

```yaml
connections:
  azure_landing:
    type: azure_blob
    connection_string: "${AZURE_STORAGE_CONNECTION_STRING}"
    container: landing
```

## 2. ADLS Gen2 (Hierarchical Namespace)

For Delta Lake on Azure, use ADLS Gen2:

```yaml
connections:
  adls_bronze:
    type: azure_blob
    account_name: mydatalake
    container: bronze
    is_adls_gen2: true  # Enable hierarchical namespace
    auth:
      mode: service_principal
      tenant_id: "${AZURE_TENANT_ID}"
      client_id: "${AZURE_CLIENT_ID}"
      client_secret: "${AZURE_CLIENT_SECRET}"
```

### Delta Lake on ADLS

```yaml
connections:
  delta_silver:
    type: delta
    base_path: abfss://silver@mydatalake.dfs.core.windows.net/
    # Auth inherited from Spark session config
```

## 3. Azure SQL Database

### SQL Authentication

```yaml
connections:
  azure_sql:
    type: sqlserver
    server: myserver.database.windows.net
    database: mydb
    auth:
      mode: sql_login
      username: "${SQL_USER}"
      password: "${SQL_PASSWORD}"
```

### Azure AD Authentication

```yaml
connections:
  azure_sql:
    type: sqlserver
    server: myserver.database.windows.net
    database: mydb
    auth:
      mode: aad_password
      username: user@company.com
      password: "${AAD_PASSWORD}"
```

### Managed Identity

```yaml
connections:
  azure_sql:
    type: sqlserver
    server: myserver.database.windows.net
    database: mydb
    auth:
      mode: msi
```

## 4. Complete Azure Project

Here's a full project using Azure services:

```yaml
# project.yaml
project: "azure_data_platform"
engine: spark

connections:
  # Landing zone (raw files)
  landing:
    type: azure_blob
    account_name: "${STORAGE_ACCOUNT}"
    container: landing
    auth:
      mode: service_principal
      tenant_id: "${AZURE_TENANT_ID}"
      client_id: "${AZURE_CLIENT_ID}"
      client_secret: "${AZURE_CLIENT_SECRET}"

  # Bronze layer (Delta)
  bronze:
    type: delta
    base_path: abfss://bronze@${STORAGE_ACCOUNT}.dfs.core.windows.net/

  # Silver layer (Delta)  
  silver:
    type: delta
    base_path: abfss://silver@${STORAGE_ACCOUNT}.dfs.core.windows.net/

  # Source database
  erp_sql:
    type: sqlserver
    server: erp-server.database.windows.net
    database: erp_prod
    auth:
      mode: msi

story:
  connection: landing
  path: _odibi/stories/

system:
  connection: landing
  path: _odibi/catalog/

pipelines:
  - pipeline: bronze_ingest
    layer: bronze
    nodes:
      # Ingest from SQL
      - name: customers_raw
        read:
          connection: erp_sql
          format: sql
          table: dbo.Customers
        write:
          connection: bronze
          table: raw_customers

      # Ingest from files
      - name: orders_raw
        read:
          connection: landing
          format: csv
          path: orders/*.csv
        write:
          connection: bronze
          table: raw_orders
```

## 5. Environment Variables

Store secrets in environment variables or Azure Key Vault:

```bash
# .env (local development - git-ignored!)
AZURE_TENANT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
AZURE_CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
AZURE_CLIENT_SECRET=your-secret-here
STORAGE_ACCOUNT=mydatalake
SQL_USER=odibi_user
SQL_PASSWORD=secure-password
```

### Using Azure Key Vault

```yaml
# Reference Key Vault secrets with ${kv:secret-name}
connections:
  azure_sql:
    type: sqlserver
    server: myserver.database.windows.net
    database: mydb
    auth:
      mode: sql_login
      username: "${kv:sql-username}"
      password: "${kv:sql-password}"
```

## 6. Spark Configuration for Azure

When using Spark with Azure, configure the session:

```python
# conf/spark_azure.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("OdibiAzure") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("fs.azure.account.auth.type", "OAuth") \
    .config("fs.azure.account.oauth.provider.type", 
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") \
    .config("fs.azure.account.oauth2.client.id", os.environ["AZURE_CLIENT_ID"]) \
    .config("fs.azure.account.oauth2.client.secret", os.environ["AZURE_CLIENT_SECRET"]) \
    .config("fs.azure.account.oauth2.client.endpoint", 
            f"https://login.microsoftonline.com/{os.environ['AZURE_TENANT_ID']}/oauth2/token") \
    .getOrCreate()
```

## 7. Running on Azure Databricks

### Cluster Configuration

Add to cluster Spark config:
```
fs.azure.account.auth.type OAuth
fs.azure.account.oauth.provider.type org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
fs.azure.account.oauth2.client.id {{secrets/odibi/client-id}}
fs.azure.account.oauth2.client.secret {{secrets/odibi/client-secret}}
fs.azure.account.oauth2.client.endpoint https://login.microsoftonline.com/{{secrets/odibi/tenant-id}}/oauth2/token
```

### Unity Catalog

With Unity Catalog, use catalog-based connections:

```yaml
connections:
  bronze:
    type: delta
    catalog: main
    schema: bronze

  silver:
    type: delta
    catalog: main
    schema: silver
```

## 8. Running on Azure Synapse

```python
# Synapse notebook
%%pyspark
from odibi import run_project

# Synapse auto-configures ADLS access via linked services
run_project("/synapse/project.yaml", pipelines=["bronze_ingest"])
```

## Common Issues

### "403 Forbidden" on Blob Access

1. Check service principal has "Storage Blob Data Contributor" role
2. Verify container name is correct
3. Check firewall allows your IP

### "Login failed" on Azure SQL

1. Verify Azure AD admin is set on SQL server
2. Check firewall allows Azure services
3. For MSI, ensure the managed identity has db_datareader/db_datawriter

### Slow ADLS Performance

Enable Delta caching:
```yaml
performance:
  delta_table_properties:
    delta.autoOptimize.optimizeWrite: true
```

## Next Steps

- [Spark Engine Tutorial](spark_engine.md) - Spark-specific features
- [Getting Started](getting_started.md) - Basic Odibi concepts
- [Performance Tuning](../guides/performance_tuning.md) - Optimize large pipelines

## See Also

- [AzureBlobConnectionConfig](../reference/yaml_schema.md#azureblobconnectionconfig) - Full Azure Blob options
- [SQLServerConnectionConfig](../reference/yaml_schema.md#sqlserverconnectionconfig) - Azure SQL options
- [DeltaConnectionConfig](../reference/yaml_schema.md#deltaconnectionconfig) - Delta Lake options
