# Engine Parity Table

Comparison of Pandas and Spark engine capabilities as of V3.

## Write Modes

| Mode | Pandas | Spark | Notes |
| :--- | :---: | :---: | :--- |
| `overwrite` | ✅ | ✅ | Default mode. |
| `append` | ✅ | ✅ | |
| `error` | ✅ | ✅ | Fails if table exists. |
| `ignore` | ✅ | ✅ | Skips if table exists. |
| `upsert` | ✅ | ✅ | Requires `keys`. Spark: Delta Lake only. |
| `append_once` | ✅ | ✅ | Requires `keys`. Spark: Delta Lake only. |

## Core Features

| Feature | Pandas | Spark | Notes |
| :--- | :---: | :---: | :--- |
| Reading (CSV, Parquet, JSON) | ✅ | ✅ | |
| Reading (Delta Lake) | ✅ | ✅ | Pandas requires `deltalake` package. |
| Reading (SQL Server) | ✅ | ✅ | |
| SQL Transformations | ✅ | ✅ | Pandas uses DuckDB/SQLite. |
| PII Anonymization | ✅ | ✅ | Hash, Mask, Redact. |
| Schema Validation | ✅ | ✅ | |
| Data Contracts | ✅ | ✅ | |
| Incremental Loading | ✅ | ✅ | Rolling Window & Stateful. |
| Time Travel | ✅ | ✅ | Delta Lake only. |

## Execution Context

| Property | Pandas | Spark | Notes |
| :--- | :---: | :---: | :--- |
| `df` | ✅ | ✅ | Native DataFrame. |
| `columns` | ✅ | ✅ | |
| `schema` | ✅ | ✅ | Dictionary of types. |
| `pii_metadata` | ✅ | ✅ | Active PII columns. |
