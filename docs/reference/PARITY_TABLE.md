# Engine Parity Table

Comparison of Pandas, Spark, and Polars engine capabilities as of V3.

## Write Modes

| Mode | Pandas | Spark | Polars | Notes |
| :--- | :---: | :---: | :---: | :--- |
| `overwrite` | ✅ | ✅ | ✅ | Default mode. |
| `append` | ✅ | ✅ | ✅ | |
| `error` | ✅ | ✅ | ✅ | Fails if table exists. |
| `ignore` | ✅ | ✅ | ✅ | Skips if table exists. |
| `upsert` | ✅ | ✅ | ✅ | Requires `keys`. Spark: Delta Lake only. |
| `append_once` | ✅ | ✅ | ✅ | Requires `keys`. Spark: Delta Lake only. |

## Core Features

| Feature | Pandas | Spark | Polars | Notes |
| :--- | :---: | :---: | :---: | :--- |
| Reading (CSV, Parquet, JSON) | ✅ | ✅ | ✅ | |
| Reading (Delta Lake) | ✅ | ✅ | ✅ | Pandas/Polars require `deltalake` package. |
| Reading (SQL Server) | ✅ | ✅ | ✅ | |
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
