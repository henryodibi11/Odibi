# Bulk Copy for SQL Server

High-performance data loading to SQL Server using ADLS staging and BULK INSERT, achieving 10-50x faster writes compared to JDBC.

## Overview

Traditional JDBC writes insert data row-by-row, which is slow for large datasets. Bulk copy:

1. Stages data as Parquet files in Azure Data Lake Storage (ADLS)
2. Uses SQL Server's `BULK INSERT` with `OPENROWSET` to load data directly from storage
3. Leverages SQL Server's parallel loading capabilities for maximum throughput

This approach is ideal for:
- Loading millions of rows in seconds
- Overwrite operations (full table refresh)
- MERGE operations with large staging datasets
- ETL pipelines running on Databricks with SQL Server targets

## Quick Start (Auto-Setup)

The easiest way to use bulk copy - odibi handles all SQL Server setup automatically:

```yaml
write:
  connection: sql_server_prod
  format: sql_server
  table: dw.fact_sales
  mode: overwrite
  overwrite_options:
    bulk_copy: true
    staging_connection: adls_staging
    auto_setup: true  # Creates external data source automatically
```

That's it! Odibi will:
1. Create the database master key (if needed)
2. Create a credential using your ADLS connection's auth
3. Create an external data source named `odibi_{staging_connection}`
4. Use it for all subsequent bulk operations

**Requirements for auto_setup:**
- SQL user needs elevated permissions: `ALTER ANY EXTERNAL DATA SOURCE`, `CONTROL`
- Staging connection must use: `account_key`, `sas`, `connection_string`, or `aad_msi` auth

## Configuration Options

### Option 1: Auto-Setup (Recommended)

Let odibi create and manage the SQL Server objects:

```yaml
overwrite_options:
  bulk_copy: true
  staging_connection: adls_staging
  auto_setup: true
```

The external data source will be named `odibi_adls_staging` (based on your connection name).

### Option 2: Custom External Data Source Name

Use auto_setup with a custom name:

```yaml
overwrite_options:
  bulk_copy: true
  staging_connection: adls_staging
  external_data_source: MyCustomDataSource
  auto_setup: true
```

### Option 3: Manual Setup (Pre-Created)

If you prefer to create the SQL objects yourself or don't have elevated permissions:

```yaml
overwrite_options:
  bulk_copy: true
  staging_connection: adls_staging
  external_data_source: OdibiBulkStaging  # Must exist in SQL Server
  auto_setup: false  # Default
```

See [Manual SQL Server Setup](#manual-sql-server-setup) below.

## Authentication Methods

When `auto_setup: true`, odibi reads your ADLS connection auth and creates the matching SQL Server credential:

### Account Key

```yaml
connections:
  adls_staging:
    type: azure_blob
    account_name: mystorageaccount
    container: staging
    auth:
      mode: account_key
      account_key: ${STORAGE_ACCOUNT_KEY}
```

Creates credential with the account key as SECRET.

### SAS Token

```yaml
connections:
  adls_staging:
    type: azure_blob
    account_name: mystorageaccount
    container: staging
    auth:
      mode: sas
      sas_token: ${STORAGE_SAS_TOKEN}
```

Creates credential with the SAS token as SECRET.

### Connection String

```yaml
connections:
  adls_staging:
    type: azure_blob
    account_name: mystorageaccount
    container: staging
    auth:
      mode: connection_string
      connection_string: ${STORAGE_CONNECTION_STRING}
```

Extracts AccountKey or SharedAccessSignature from the connection string.

### Managed Identity (Azure SQL Only)

```yaml
connections:
  adls_staging:
    type: azure_blob
    account_name: mystorageaccount
    container: staging
    auth:
      mode: aad_msi
```

Creates external data source **without** a credential - Azure SQL uses its managed identity.

**Prerequisites:**
- Azure SQL Database (not on-premises SQL Server)
- Azure SQL's managed identity has "Storage Blob Data Reader" role on the storage account

## Configuration Reference

### Overwrite Options

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `bulk_copy` | boolean | No | `false` | Enable bulk copy mode |
| `staging_connection` | string | When `bulk_copy=true` | - | Connection name for ADLS staging |
| `staging_path` | string | No | `odibi_staging/bulk` | Path prefix for staging files |
| `external_data_source` | string | No | Auto-generated | SQL Server external data source name |
| `auto_setup` | boolean | No | `false` | Auto-create SQL Server objects |
| `keep_staging_files` | boolean | No | `false` | Retain staging files after load |
| `create_table` | boolean | No | `true` | Auto-create table if missing |
| `schema_evolution` | boolean | No | `false` | Evolve table schema to match source |

### Merge Options

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `bulk_copy` | boolean | No | `false` | Enable bulk copy for staging |
| `staging_connection` | string | When `bulk_copy=true` | - | Connection name for ADLS staging |
| `staging_path` | string | No | `odibi_staging/bulk` | Path prefix for staging files |
| `external_data_source` | string | No | Auto-generated | SQL Server external data source name |
| `auto_setup` | boolean | No | `false` | Auto-create SQL Server objects |
| `keep_staging_files` | boolean | No | `false` | Retain staging files after load |
| `key_columns` | list | Yes | - | Columns for MERGE matching |

## How It Works

### Overwrite Flow

```
┌─────────────┐    ┌──────────────────┐    ┌──────────────────┐
│ Spark       │───▶│ ADLS Staging     │───▶│ SQL Server       │
│ DataFrame   │    │ (Parquet)        │    │ BULK INSERT      │
└─────────────┘    └──────────────────┘    └──────────────────┘
                           │                        │
                           ▼                        ▼
                   Automatic cleanup        Target table
                   after success            populated
```

1. DataFrame written as Parquet to ADLS staging path
2. SQL Server `BULK INSERT` via `OPENROWSET` reads from staging
3. Staging files cleaned up (unless `keep_staging_files: true`)

### Merge Flow

```
┌─────────────┐    ┌──────────────────┐    ┌──────────────────┐
│ Spark       │───▶│ ADLS Staging     │───▶│ SQL Server       │
│ DataFrame   │    │ (Parquet)        │    │ Staging Table    │
└─────────────┘    └──────────────────┘    └──────────────────┘
                                                   │
                                                   ▼
                                           ┌──────────────────┐
                                           │ MERGE INTO       │
                                           │ Target Table     │
                                           └──────────────────┘
```

## Performance Comparison

| Method | 1M Rows | 10M Rows | 100M Rows |
|--------|---------|----------|-----------|
| JDBC (row-by-row) | ~5 min | ~50 min | Hours |
| Bulk Copy | ~10 sec | ~60 sec | ~8 min |

*Actual performance varies based on network, SQL Server resources, and data complexity.*

## Example: Full Pipeline

```yaml
project: sales_analytics

connections:
  source:
    type: azure_adls
    account_name: datalakeprod
    container: raw
    auth:
      mode: account_key
      account_key: ${ADLS_KEY}
      
  staging:
    type: azure_adls
    account_name: datalakeprod
    container: staging
    auth:
      mode: account_key
      account_key: ${ADLS_KEY}
      
  warehouse:
    type: azure_sql
    server: sql-prod.database.windows.net
    database: analytics_dw
    auth:
      method: service_principal

pipelines:
  load_warehouse:
    nodes:
      - name: fact_orders
        read:
          connection: source
          path: orders/2024/
          format: parquet
        write:
          connection: warehouse
          format: sql_server
          table: dw.fact_orders
          mode: overwrite
          overwrite_options:
            bulk_copy: true
            staging_connection: staging
            auto_setup: true
```

## Manual SQL Server Setup

If you can't use `auto_setup: true` (e.g., limited permissions), create the objects manually:

### Option 1: Storage Account Key

```sql
USE your_database;

-- Create master key (one-time)
IF NOT EXISTS (SELECT 1 FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'YourStrongPassword123!';

-- Create credential with account key
CREATE DATABASE SCOPED CREDENTIAL OdibiBulkCredential
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = '<your-storage-account-key>';

-- Create external data source
CREATE EXTERNAL DATA SOURCE OdibiBulkStaging
WITH (
    TYPE = BLOB_STORAGE,
    LOCATION = 'https://<account>.blob.core.windows.net/<container>',
    CREDENTIAL = OdibiBulkCredential
);
```

### Option 2: SAS Token

```sql
USE your_database;

CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'YourStrongPassword123!';

-- Create credential with SAS token (without leading '?')
CREATE DATABASE SCOPED CREDENTIAL OdibiBulkCredential
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = 'sv=2022-11-02&ss=b&srt=co&sp=rl...';

CREATE EXTERNAL DATA SOURCE OdibiBulkStaging
WITH (
    TYPE = BLOB_STORAGE,
    LOCATION = 'https://<account>.blob.core.windows.net/<container>',
    CREDENTIAL = OdibiBulkCredential
);
```

### Option 3: Managed Identity (Azure SQL Only)

```sql
-- No credential needed - Azure SQL uses its managed identity
CREATE EXTERNAL DATA SOURCE OdibiBulkStaging
WITH (
    TYPE = BLOB_STORAGE,
    LOCATION = 'https://<account>.blob.core.windows.net/<container>'
);
```

Then grant the Azure SQL managed identity "Storage Blob Data Reader" role on the storage account.

## Troubleshooting

### "External data source not found"

Either:
1. Set `auto_setup: true` to create it automatically
2. Ensure the `external_data_source` name matches exactly what was created in SQL Server

### "Cannot bulk load - permission denied"

For manual setup, the SQL user needs `ADMINISTER BULK OPERATIONS` permission:

```sql
GRANT ADMINISTER BULK OPERATIONS TO [your_user];
```

For auto_setup, you need elevated permissions:

```sql
GRANT ALTER ANY EXTERNAL DATA SOURCE TO [your_user];
GRANT CONTROL ON DATABASE::your_db TO [your_user];
```

### "SAS token expired"

If using manual setup, update the credential:

```sql
ALTER DATABASE SCOPED CREDENTIAL OdibiBulkCredential
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = '<new-sas-token>';
```

With `auto_setup: true`, just update your connection's `sas_token` value.

### Staging files not cleaned up

If `keep_staging_files: false` but files remain, check:
- The staging connection has delete permissions
- No error occurred during cleanup (check logs)

### Schema mismatch errors

Enable `schema_evolution: true` to auto-add missing columns:

```yaml
overwrite_options:
  bulk_copy: true
  schema_evolution: true
```

## Limitations

- **Spark engine only**: Bulk copy requires PySpark for JDBC connection pooling
- **ADLS/Blob staging required**: Cannot use local filesystem for staging
- **SQL Server 2017+**: Requires external data source support
- **Azure SQL**: Fully supported with all auth methods
- **On-premises SQL Server**: Requires network access to Azure storage

## See Also

- [Connections](connections.md) - Configure ADLS and SQL Server connections
- [Patterns](patterns.md) - Dimension and Fact loading patterns
- [SQL Server Writer Reference](../reference/yaml_schema.md#sqlserveroverwriteoptions) - Full option reference
