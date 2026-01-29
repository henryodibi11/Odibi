# Bulk Copy for SQL Server

High-performance data loading to SQL Server using ADLS staging and BULK INSERT, achieving 10-50x faster writes compared to standard JDBC.

## Performance Tiers

Odibi provides two levels of SQL Server write optimization:

### 1. JDBC Bulk Copy Protocol (Default - All Databases)

**Enabled automatically** for all SQL Server writes via `useBulkCopyForBatchInsert=true`. No configuration needed.

| Database | Support | Speedup |
|----------|---------|---------|
| Azure SQL Database | ✅ Works | 5-10x |
| Azure SQL Managed Instance | ✅ Works | 5-10x |
| Azure Synapse Analytics | ✅ Works | 5-10x |
| SQL Server (on-prem) | ✅ Works | 5-10x |

This uses Microsoft's JDBC driver bulk copy protocol - faster than row-by-row inserts, no external staging required.

### 2. File-Based Bulk Copy (All Azure SQL Platforms)

For maximum performance with very large datasets, use `bulk_copy: true` to stage data in ADLS and load via `BULK INSERT`.

| Database | Format | Support | Speedup |
|----------|--------|---------|---------|
| **Azure Synapse Analytics** | Parquet | ✅ Full support | 10-50x |
| **SQL Server 2022+** | Parquet | ✅ With PolyBase | 10-50x |
| **Azure SQL Database** | CSV | ✅ Supported | 10-50x |
| **Azure SQL Managed Instance** | CSV | ✅ Supported | 10-50x |

**Auto-detection:** Odibi automatically detects your database type and uses the optimal format:
- **Synapse/SQL Server 2022+**: Parquet (faster, columnar)
- **Azure SQL DB/MI**: CSV with proper escaping and `0x0a` row terminators for Linux compatibility

**First-run optimization:** On initial load (empty target table), odibi skips the staging table and MERGE, doing a direct BULK INSERT to target for maximum speed.

## Overview

Bulk copy works by:

1. Writing data to Azure Data Lake Storage (ADLS) as a staging file
2. Using SQL Server `BULK INSERT` to load directly from blob storage
3. Achieving 10-50x faster writes compared to JDBC

The format is auto-selected based on your database type (Parquet for Synapse, CSV for Azure SQL DB).

This approach is ideal for:
- Loading millions of rows in seconds
- Overwrite operations (full table refresh)
- MERGE operations with large staging datasets
- ETL pipelines running on Databricks with SQL Server targets

## Complete Examples

### Example 1: Overwrite with Bulk Copy

Full table refresh using bulk copy - replaces all data in the target table:

```yaml
project: wideworldimporters

connections:
  # Source data lake
  adls_silver:
    type: azure_blob
    account_name: ${AZURE_STORAGE_ACCOUNT}
    container: datalake
    auth:
      mode: account_key
      account_key: ${AZURE_STORAGE_KEY}

  # Staging for bulk copy (can be same as source)
  adls_staging:
    type: azure_blob
    account_name: ${AZURE_STORAGE_ACCOUNT}
    container: datalake
    auth:
      mode: account_key
      account_key: ${AZURE_STORAGE_KEY}

  # SQL Server target
  sql_warehouse:
    type: sql_server
    server: ${SQL_SERVER}
    database: ${SQL_DATABASE}
    auth:
      mode: sql_login
      username: ${SQL_USER}
      password: ${SQL_PASSWORD}

pipelines:
  - pipeline: load_fact_tables
    nodes:
      - name: fact_sales_to_sql
        read:
          connection: adls_silver
          path: silver/fact_sales/
          format: parquet
        write:
          connection: sql_warehouse
          format: sql_server
          table: dw.fact_sales
          mode: overwrite
          overwrite_options:
            bulk_copy: true
            staging_connection: adls_staging
            external_data_source: OdibiBulkStagingPROD
            # Optional settings
            auto_create_schema: true
            auto_create_table: true
            audit_cols:
              created_col: created_ts
              updated_col: updated_ts
            schema_evolution:
              mode: evolve
              add_columns: true
```

### Example 2: Merge with Bulk Copy

Incremental sync using bulk copy for staging - inserts new rows, updates changed rows:

```yaml
project: wideworldimporters

connections:
  adls_silver:
    type: azure_blob
    account_name: ${AZURE_STORAGE_ACCOUNT}
    container: datalake
    auth:
      mode: account_key
      account_key: ${AZURE_STORAGE_KEY}

  adls_staging:
    type: azure_blob
    account_name: ${AZURE_STORAGE_ACCOUNT}
    container: datalake
    auth:
      mode: account_key
      account_key: ${AZURE_STORAGE_KEY}

  sql_warehouse:
    type: sql_server
    server: ${SQL_SERVER}
    database: ${SQL_DATABASE}
    auth:
      mode: sql_login
      username: ${SQL_USER}
      password: ${SQL_PASSWORD}

pipelines:
  - pipeline: sync_dimensions
    nodes:
      - name: dim_customer_merge
        read:
          connection: adls_silver
          path: silver/dim_customer/
          format: parquet
        write:
          connection: sql_warehouse
          format: sql_server
          table: dw.dim_customer
          mode: merge
          merge_keys:
            - customer_id
          merge_options:
            bulk_copy: true
            staging_connection: adls_staging
            external_data_source: OdibiBulkStagingPROD
            # MERGE behavior
            update_condition: "source._hash_diff != target._hash_diff"
            exclude_columns:
              - _hash_diff
            # Table setup
            auto_create_schema: true
            auto_create_table: true
            audit_cols:
              created_col: created_ts
              updated_col: updated_ts
```

### Example 3: Auto-Setup Mode

Let odibi create the SQL Server objects automatically (requires elevated permissions):

```yaml
nodes:
  - name: fact_orders_to_sql
    read:
      connection: adls_silver
      path: silver/fact_orders/
      format: parquet
    write:
      connection: sql_warehouse
      format: sql_server
      table: dw.fact_orders
      mode: overwrite
      overwrite_options:
        bulk_copy: true
        staging_connection: adls_staging
        auto_setup: true  # Creates external data source automatically
        auto_create_table: true
```

With `auto_setup: true`, odibi will:
1. Create database master key (if needed)
2. Create credential from your ADLS connection auth
3. Create external data source named `odibi_adls_staging`

## Staging Path Organization

Staging files are organized by project/pipeline/node for easy debugging:

```
datalake/odibi_staging/bulk/{project}/{pipeline}/{node}/{uuid}.parquet
```

Example:
```
datalake/odibi_staging/bulk/wideworldimporters/load_fact_tables/fact_sales_to_sql/a1b2c3d4.parquet
```

This prevents conflicts when multiple nodes run in parallel and makes it easy to identify orphaned files.

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
| `auto_create_schema` | boolean | No | `false` | Create schema if missing |
| `auto_create_table` | boolean | No | `false` | Create table if missing |
| `audit_cols` | object | No | - | Add created_ts/updated_ts columns |
| `schema_evolution` | object | No | - | Auto-add new columns |

### Merge Options

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `bulk_copy` | boolean | No | `false` | Enable bulk copy for staging |
| `staging_connection` | string | When `bulk_copy=true` | - | Connection name for ADLS staging |
| `staging_path` | string | No | `odibi_staging/bulk` | Path prefix for staging files |
| `external_data_source` | string | No | Auto-generated | SQL Server external data source name |
| `auto_setup` | boolean | No | `false` | Auto-create SQL Server objects |
| `keep_staging_files` | boolean | No | `false` | Retain staging files after load |
| `update_condition` | string | No | - | SQL condition for WHEN MATCHED UPDATE |
| `delete_condition` | string | No | - | SQL condition for WHEN MATCHED DELETE |
| `exclude_columns` | list | No | - | Columns to exclude from MERGE |
| `auto_create_schema` | boolean | No | `false` | Create schema if missing |
| `auto_create_table` | boolean | No | `false` | Create table if missing |
| `audit_cols` | object | No | - | Add created_ts/updated_ts columns |

## Authentication Methods

When `auto_setup: true`, odibi reads your ADLS connection auth and creates the matching SQL Server credential:

### Account Key

```yaml
connections:
  adls_staging:
    type: azure_blob
    account_name: mystorageaccount
    container: datalake
    auth:
      mode: account_key
      account_key: ${STORAGE_ACCOUNT_KEY}
```

### SAS Token (Recommended for Azure SQL DB)

**Important:** For Azure SQL Database BULK INSERT, use SAS token authentication. The SAS token must have:
- **Resource types:** Container AND Object (both required)
- **Permissions:** Read, List

```yaml
connections:
  adls_staging:
    type: azure_adls  # or azure_blob
    account_name: mystorageaccount
    container: datalake
    auth:
      mode: sas_token
      sas_token: ${STORAGE_SAS_TOKEN}  # Without leading '?' - odibi handles it
```

The external data source location must include the container:
`https://account.blob.core.windows.net/container`

### Managed Identity (Azure SQL Only)

```yaml
connections:
  adls_staging:
    type: azure_blob
    account_name: mystorageaccount
    container: datalake
    auth:
      mode: aad_msi
```

## Manual SQL Server Setup

If you can't use `auto_setup: true`, create the objects manually:

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
CREATE EXTERNAL DATA SOURCE OdibiBulkStagingPROD
WITH (
    TYPE = BLOB_STORAGE,
    LOCATION = 'https://<account>.blob.core.windows.net/<container>',
    CREDENTIAL = OdibiBulkCredential
);
```

Then reference it in your YAML:

```yaml
overwrite_options:
  bulk_copy: true
  staging_connection: adls_staging
  external_data_source: OdibiBulkStagingPROD  # Must match SQL object name
```

## Performance Comparison

| Method | 1M Rows | 10M Rows | 100M Rows |
|--------|---------|----------|-----------|
| JDBC (row-by-row) | ~5 min | ~50 min | Hours |
| Bulk Copy | ~10 sec | ~60 sec | ~8 min |

*Actual performance varies based on network, SQL Server resources, and data complexity.*

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

### Staging files not cleaned up

If `keep_staging_files: false` but files remain:
- Check the staging connection has delete permissions
- Check logs for cleanup errors

## Technical Details

### CSV Format for Azure SQL DB

When targeting Azure SQL Database, odibi writes CSV files with:
- **Quote character:** `"`
- **Escape character:** `"` (doubled quotes)
- **Row terminator:** `0x0a` (hex format for Linux/Databricks compatibility)
- **Encoding:** UTF-8

These settings ensure proper handling of embedded quotes, newlines, and special characters.

### ODBC Driver

For Databricks, use ODBC Driver 18 (preferred over 17):
- TLS 1.3 support
- Better connection resilience
- Note: May require `TrustServerCertificate=yes` for Azure SQL

## Limitations

- **Spark engine only**: Bulk copy requires PySpark for Parquet/CSV writing
- **ADLS/Blob staging required**: Cannot use local filesystem for staging
- **SQL Server 2017+**: Requires external data source support

## See Also

- [Connections](connections.md) - Configure ADLS and SQL Server connections
- [Patterns](patterns.md) - Dimension and Fact loading patterns
- [SQL Server Writer Reference](../reference/yaml_schema.md#sqlserveroverwriteoptions) - Full option reference
