# Odibi Logging Audit Report

**Date:** 2025-12-01  
**Status:** Complete  
**Test Results:** 655 passed, 0 failed, 20 skipped

## Executive Summary

This audit enhanced the Odibi framework with comprehensive, structured logging across all major modules. The goal was to eliminate silent failures, provide better observability, and enable effective debugging in production environments.

### Key Achievements

- Created new `LoggingContext` infrastructure for context-aware logging
- Added structured logging with timing, row counts, and schema tracking
- Implemented secret redaction to prevent credential leaks
- Enhanced ~25 modules with detailed operation logging
- Fixed all silent failure patterns identified

---

## New Infrastructure

### `odibi/utils/logging_context.py`

A new module providing context-based logging with automatic correlation:

| Component | Description |
|-----------|-------------|
| `OperationType` | Enum categorizing operations (READ, WRITE, TRANSFORM, VALIDATE, etc.) |
| `OperationMetrics` | Dataclass tracking timing, row counts, schema changes, memory |
| `StructuredLogger` | Base logger supporting human-readable and JSON output with secret redaction |
| `LoggingContext` | Context-aware wrapper with pipeline/node/engine tracking |

**Key Features:**
- Automatic context injection (pipeline_id, node_id, engine)
- Operation timing with millisecond precision
- Row count tracking with delta calculations
- Schema change detection (columns added/removed/type changes)
- Secret redaction for connection strings and credentials

**Helper Methods:**
- `log_file_io()` - File read/write operations
- `log_spark_metrics()` - Spark-specific metrics (partitions, broadcasts)
- `log_pandas_metrics()` - Pandas-specific metrics (memory, dtypes)
- `log_validation_result()` - Validation pass/fail with details
- `log_graph_operation()` - Dependency graph operations
- `log_connection()` - Connection lifecycle events
- `log_schema_change()` - Schema transformation tracking
- `log_row_count_change()` - Row count deltas with warnings

---

## Enhanced Modules

### Core Pipeline Components

#### `odibi/graph.py`
- **Added:** Node count, edge count, layer count logging
- **Added:** Cycle detection with node names in error messages
- **Added:** Resolution phase timing
- **Fixed:** Silent failures in dependency resolution

#### `odibi/node.py`
- **Added:** Per-phase logging (source_read, transform, validation, sink_write)
- **Added:** Row count tracking through execution phases
- **Added:** Schema change logging between transformations
- **Added:** Timing metrics for each phase

#### `odibi/pipeline.py`
- **Added:** Pipeline start/end logging with run IDs
- **Added:** Layer-by-layer execution progress
- **Added:** Parallel node execution tracking
- **Added:** Summary metrics (total nodes, duration, failures)

### Configuration & Utilities

#### `odibi/utils/config_loader.py`
- **Added:** YAML file loading with path logging
- **Added:** Environment variable substitution tracking
- **Added:** Missing env var warnings with variable names
- **Added:** Schema validation result logging
- **Fixed:** Silent env var substitution failures

### Validation Engine

#### `odibi/validation/engine.py`
- **Added:** Rule-by-rule validation logging
- **Added:** Pass/fail counts per rule
- **Added:** Failure row details (configurable limit)
- **Added:** Timing for expensive validations
- **Fixed:** Silent validation failures now logged with details

### Engine Layer

#### `odibi/engine/pandas_engine.py`
- **Added:** File I/O logging with format, path, row counts
- **Added:** Memory usage warnings (>1GB threshold)
- **Added:** SQL execution logging with query previews
- **Added:** Chunk processing progress for large files
- **Fixed:** Silent read failures now include file path and error details

#### `odibi/engine/spark_engine.py`
- **Added:** Partition count logging for reads/writes
- **Added:** JDBC connection logging (redacted credentials)
- **Added:** Delta Lake operation metrics (merge, optimize, vacuum)
- **Added:** Streaming batch progress logging
- **Added:** Broadcast join size tracking
- **Fixed:** Silent JDBC failures now include connection details

### Connection Layer

#### `odibi/connections/factory.py`
- **Added:** Connection type resolution logging
- **Added:** Connection creation/reuse tracking
- **Added:** Failed connection attempts with error details

#### `odibi/connections/local.py`
- **Added:** File system path resolution logging
- **Added:** Directory creation logging
- **Added:** File existence checks with paths

#### `odibi/connections/azure_adls.py`
- **Added:** Storage account connection logging
- **Added:** Container/path resolution logging
- **Added:** Credential type detection (SAS, OAuth, Key)
- **Added:** Secret redaction for connection strings

#### `odibi/connections/azure_sql.py`
- **Added:** Server/database connection logging
- **Added:** Authentication method logging (SQL Auth, AAD)
- **Added:** JDBC/ODBC driver selection logging
- **Added:** Query execution timing

### Transformer Layer

#### `odibi/transformers/relational.py`
- **Added:** Join operation logging with key columns
- **Added:** Row count before/after joins
- **Added:** Filter operation logging with predicate info
- **Added:** Aggregation logging with group-by columns

#### `odibi/transformers/advanced.py`
- **Added:** Window function logging
- **Added:** Pivot/unpivot operation tracking
- **Added:** Complex expression evaluation logging

#### `odibi/transformers/scd.py`
- **Added:** SCD Type 1/2 operation logging
- **Added:** Row version tracking
- **Added:** Effective date range logging

#### `odibi/transformers/merge_transformer.py`
- **Added:** Merge key logging
- **Added:** Insert/update/delete counts
- **Added:** Conflict resolution logging

#### `odibi/transformers/validation.py`
- **Added:** Validation rule application logging
- **Added:** Quarantine row tracking

#### `odibi/transformers/sql_core.py`
- **Added:** SQL generation logging
- **Added:** Query execution timing
- **Added:** Result set size logging

### Pattern Layer

#### `odibi/patterns/base.py`
- **Added:** Pattern instantiation logging
- **Added:** Configuration validation logging
- **Added:** Execution phase logging

#### `odibi/patterns/scd2.py`
- **Added:** Slowly changing dimension processing logging
- **Added:** New/changed/expired row counts
- **Added:** Surrogate key generation logging

#### `odibi/patterns/merge.py`
- **Added:** Merge pattern execution logging
- **Added:** Match condition logging
- **Added:** Update/insert clause logging

### Story/Documentation Layer

#### `odibi/story/generator.py`
- **Added:** Story generation logging
- **Added:** Template rendering logging
- **Added:** Output file creation logging

#### `odibi/story/renderers.py`
- **Added:** Renderer selection logging
- **Added:** Format conversion logging
- **Added:** Asset embedding logging

#### `odibi/story/metadata.py`
- **Added:** Metadata extraction logging
- **Added:** Schema inference logging
- **Added:** Lineage tracking logging

---

## Silent Failure Patterns Fixed

| Module | Issue | Fix |
|--------|-------|-----|
| `config_loader` | Missing env vars silently returned empty string | Now logs warning with variable name |
| `pandas_engine` | File read errors lost path context | Error now includes full path and format |
| `spark_engine` | JDBC failures had no connection context | Errors include server/database (redacted creds) |
| `validation/engine` | Failed rules returned False silently | Now logs rule name, failure count, sample failures |
| `graph` | Cycle detection raised generic error | Now includes node names in cycle |
| `connections/factory` | Unknown connection type silent | Now logs attempted type and available types |
| `transformers/*` | Transform failures lost input context | Now log input row count, schema before failure |

---

## Log Output Examples

### Human-Readable Mode
```
[15:42:33] Starting READ: orders.parquet (pipeline_id=etl_daily, node_id=load_orders)
[15:42:34] Completed READ: orders.parquet (elapsed_ms=1234.56, rows=50000, partitions=8)
[15:42:34] [DEBUG] Schema change in transform (columns_before=12, columns_after=15, columns_added=['calculated_total', 'tax_amount', 'discount'])
[15:42:35] [WARN] Validation failed: not_null_check (rule=not_null_check, passed=False, failures=['customer_id is null: 23 rows'])
```

### JSON/Structured Mode
```json
{"timestamp": "2025-12-01T15:42:33.123Z", "level": "INFO", "message": "Starting READ: orders.parquet", "pipeline_id": "etl_daily", "node_id": "load_orders", "operation": "read"}
{"timestamp": "2025-12-01T15:42:34.456Z", "level": "INFO", "message": "Completed READ: orders.parquet", "pipeline_id": "etl_daily", "node_id": "load_orders", "operation": "read", "elapsed_ms": 1234.56, "rows": 50000, "partitions": 8}
```

---

## Recommendations

### Short-term
1. **Add sampling for large failure sets** - Currently logs up to 10 failure examples; consider making configurable
2. **Add correlation IDs** - Pass run_id through all operations for distributed tracing
3. **Memory threshold configuration** - Make the 1GB warning threshold configurable

### Medium-term
1. **Metrics export** - Add Prometheus/StatsD exporters for `OperationMetrics`
2. **Log aggregation** - Consider structured logging to ELK/Splunk/Datadog
3. **Performance dashboards** - Build Grafana dashboards from structured logs

### Long-term
1. **OpenTelemetry integration** - Add spans for distributed tracing
2. **Automatic alerting** - Trigger alerts on validation failure rates
3. **Log-based debugging** - Replay failed operations from logs

---

## Files Modified

```
odibi/utils/logging_context.py  (NEW)
odibi/utils/logging.py
odibi/utils/__init__.py
odibi/utils/config_loader.py
odibi/graph.py
odibi/node.py
odibi/pipeline.py
odibi/validation/engine.py
odibi/engine/pandas_engine.py
odibi/engine/spark_engine.py
odibi/connections/factory.py
odibi/connections/local.py
odibi/connections/azure_adls.py
odibi/connections/azure_sql.py
odibi/transformers/relational.py
odibi/transformers/advanced.py
odibi/transformers/scd.py
odibi/transformers/merge_transformer.py
odibi/transformers/validation.py
odibi/transformers/sql_core.py
odibi/patterns/base.py
odibi/patterns/scd2.py
odibi/patterns/merge.py
odibi/story/generator.py
odibi/story/renderers.py
odibi/story/metadata.py
```

---

## Verification

All tests pass after the logging enhancements:

```bash
pytest tests/ -x -q --tb=short
# Result: 655 passed, 20 skipped
```

Lint checks pass:

```bash
ruff check .
ruff format .
# Result: All checks passed
```
