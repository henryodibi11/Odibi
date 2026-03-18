# Engine Parity Table

Comparison of Pandas, Spark, and Polars engine capabilities as of v3.4.3.

## Write Modes

| Mode | Pandas | Spark | Polars | Notes |
| :--- | :---: | :---: | :---: | :--- |
| `overwrite` | ✅ | ✅ | ✅ | Default mode. |
| `append` | ✅ | ✅ | ✅ | |
| `error` | ✅ | ✅ | ✅ | Fails if table exists. |
| `ignore` | ✅ | ✅ | ✅ | Skips if table exists. |
| `upsert` | ✅ | ✅ | ✅ | Requires `keys`. Spark: Delta Lake only. |
| `append_once` | ✅ | ✅ | ✅ | Requires `keys`. Spark: Delta Lake only. |
| `merge` | ✅ | ✅ | ✅ | SQL Server only. T-SQL MERGE via staging table. |

## SQL Server Features (Phase 4)

| Feature | Pandas | Spark | Polars | Notes |
| :--- | :---: | :---: | :---: | :--- |
| SQL Server MERGE | ✅ | ✅ | ✅ | Upsert via staging table pattern. |
| Composite Merge Keys | ✅ | ✅ | ✅ | Multiple columns in ON clause. |
| Merge Conditions | ✅ | ✅ | ✅ | update_condition, delete_condition, insert_condition. |
| Audit Columns | ✅ | ✅ | ✅ | Auto-populate created/updated timestamps. |
| Key Validations | ✅ | ✅ | ✅ | Null/duplicate key checks. |
| Enhanced Overwrite | ✅ | ✅ | ✅ | truncate_insert, drop_create, delete_insert. |
| Auto Schema Creation | ✅ | ✅ | ✅ | CREATE SCHEMA IF NOT EXISTS. |
| Auto Table Creation | ✅ | ✅ | ✅ | Infer schema from DataFrame. |
| Schema Evolution | ✅ | ✅ | ✅ | strict, evolve, ignore modes. |
| Batch Processing | ✅ | ✅ | ✅ | Chunk large writes for memory efficiency. |
| Incremental Merge | ✅ | ✅ | ✅ | Compare hashes, only write changed rows to staging. |

## Core Features

| Feature | Pandas | Spark | Polars | Notes |
| :--- | :---: | :---: | :---: | :--- |
| Reading (CSV, Parquet, JSON) | ✅ | ✅ | ✅ | |
| Reading (Delta Lake) | ✅ | ✅ | ✅ | Pandas/Polars require `deltalake` package. |
| Reading (SQL Server) | ✅ | ✅ | ✅ | |
| Writing (SQL Server) | ✅ | ✅ | ✅ | Including merge and enhanced overwrite. |
| SQL Transformations | ✅ | ✅ | ✅ | Pandas uses DuckDB/SQLite. Polars uses native SQL context. |
| PII Anonymization | ✅ | ✅ | ✅ | Hash, Mask, Redact. |
| Schema Validation | ✅ | ✅ | ✅ | |
| Data Contracts | ✅ | ✅ | ✅ | |
| Incremental Loading | ✅ | ✅ | ✅ | Rolling Window & Stateful. |
| Time Travel | ✅ | ✅ | ✅ | Delta Lake only. |

## Execution Context

| Property | Pandas | Spark | Polars | Notes |
| :--- | :---: | :---: | :---: | :--- |
| `df` | ✅ | ✅ | ✅ | Native DataFrame. |
| `columns` | ✅ | ✅ | ✅ | |
| `schema` | ✅ | ✅ | ✅ | Dictionary of types. |
| `pii_metadata` | ✅ | ✅ | ✅ | Active PII columns. |
