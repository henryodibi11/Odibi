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

## Prerequisites

### One-Time SQL Server Setup

Create an external data source pointing to your ADLS staging container:

```sql
-- Create master key (if not exists)
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'YourStrongPassword123!';

-- Create database scoped credential
CREATE DATABASE SCOPED CREDENTIAL OdibiBulkCredential
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = '<your-sas-token>';

-- Create external data source
CREATE EXTERNAL DATA SOURCE OdibiBulkStaging
WITH (
    TYPE = BLOB_STORAGE,
    LOCATION = 'https://<storage-account>.blob.core.windows.net/<container>',
    CREDENTIAL = OdibiBulkCredential
);
```

### Required Permissions

- `ADMINISTER BULK OPERATIONS` permission on SQL Server
- Read access on the external data source
- Write permissions to target table

## Configuration

### Overwrite Mode

Replace an entire table using bulk copy:

```yaml
nodes:
  - name: load_sales_fact
    read:
      connection: adls
      path: sales/processed/
      format: parquet
    write:
      connection: sql_server_prod
      format: sql_server
      table: dw.fact_sales
      mode: overwrite
      overwrite_options:
        bulk_copy: true
        staging_connection: adls_staging
        staging_path: bulk_staging/sales/
        external_data_source: OdibiBulkStaging
```

### Merge Mode

Use bulk copy to stage data for MERGE operations:

```yaml
nodes:
  - name: merge_customer_dim
    read:
      connection: adls
      path: customers/changes/
      format: parquet
    write:
      connection: sql_server_prod
      format: sql_server
      table: dw.dim_customer
      mode: merge
      merge_options:
        key_columns:
          - customer_id
        bulk_copy: true
        staging_connection: adls_staging
        staging_path: bulk_staging/customers/
        external_data_source: OdibiBulkStaging
```

## Configuration Reference

### Overwrite Options

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `bulk_copy` | boolean | No | `false` | Enable bulk copy mode |
| `staging_connection` | string | When `bulk_copy=true` | - | Connection name for ADLS staging |
| `staging_path` | string | No | Auto-generated | Path within staging container for temp files |
| `external_data_source` | string | When `bulk_copy=true` | - | SQL Server external data source name |
| `keep_staging_files` | boolean | No | `false` | Retain staging files after load (for debugging) |
| `create_table` | boolean | No | `true` | Auto-create table if missing |
| `schema_evolution` | boolean | No | `false` | Evolve table schema to match source |

### Merge Options

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `bulk_copy` | boolean | No | `false` | Enable bulk copy for staging |
| `staging_connection` | string | When `bulk_copy=true` | - | Connection name for ADLS staging |
| `staging_path` | string | No | Auto-generated | Path within staging container for temp files |
| `external_data_source` | string | When `bulk_copy=true` | - | SQL Server external data source name |
| `keep_staging_files` | boolean | No | `false` | Retain staging files after load |
| `key_columns` | list | Yes | - | Columns for MERGE matching |
| `update_columns` | list | No | All non-key | Columns to update on match |
| `insert_columns` | list | No | All | Columns to insert on no match |

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

1. DataFrame written as Parquet to ADLS staging path
2. Temporary staging table created in SQL Server
3. `BULK INSERT` loads data into staging table
4. `MERGE` statement executes against target table
5. Staging table and files cleaned up

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
      method: service_principal
      
  staging:
    type: azure_adls
    account_name: datalakeprod
    container: staging
    auth:
      method: service_principal
      
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
            staging_path: bulk/orders/
            external_data_source: OdibiBulkStaging
```

## Troubleshooting

### "External data source not found"

Ensure the `external_data_source` name matches exactly what was created in SQL Server.

### "Cannot bulk load - permission denied"

The SQL user needs `ADMINISTER BULK OPERATIONS` permission:

```sql
GRANT ADMINISTER BULK OPERATIONS TO [your_user];
```

### "SAS token expired"

Update the database scoped credential with a new SAS token:

```sql
ALTER DATABASE SCOPED CREDENTIAL OdibiBulkCredential
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = '<new-sas-token>';
```

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
  ...
```

## Limitations

- **Spark engine only**: Bulk copy requires PySpark for JDBC connection pooling
- **ADLS staging required**: Cannot use local filesystem for staging
- **SQL Server 2017+**: Requires external data source support
- **Azure SQL**: Fully supported with Azure storage
- **On-premises SQL Server**: Requires network access to Azure storage

## See Also

- [Connections](connections.md) - Configure ADLS and SQL Server connections
- [Patterns](patterns.md) - Dimension and Fact loading patterns
- [SQL Server Writer Reference](../reference/yaml_schema.md#sqlserveroverwriteoptions) - Full option reference
