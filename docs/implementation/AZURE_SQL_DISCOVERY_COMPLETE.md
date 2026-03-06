# Azure SQL Discovery Methods - Implementation Complete ✅

## Summary

Successfully implemented 6 discovery methods for the `AzureSQL` connection class in `odibi/connections/azure_sql.py`.

## Methods Implemented

### 1. `list_schemas() -> List[str]`
- Queries `INFORMATION_SCHEMA.SCHEMATA`
- Filters out system schemas (sys, INFORMATION_SCHEMA, etc.)
- Returns list of user schema names

### 2. `list_tables(schema: str = "dbo") -> List[Dict]`
- Queries `INFORMATION_SCHEMA.TABLES`
- Returns list of dicts: `{name, type, schema}`
- Distinguishes between tables and views

### 3. `get_table_info(table: str) -> Dict`
- Parses `schema.table` or defaults to `dbo`
- Queries `INFORMATION_SCHEMA.COLUMNS` for schema
- Optionally gets row count from `sys.dm_db_partition_stats`
- Returns `Schema` model dict with dataset and columns

### 4. `discover_catalog(include_schema, include_stats, limit) -> Dict`
- Calls `list_schemas()` to get all schemas
- For each schema, calls `list_tables()`
- Builds `CatalogSummary` dict
- Supports:
  - `include_schema`: Get columns for each table
  - `include_stats`: Get row counts
  - `limit`: Limit datasets per schema

### 5. `profile(dataset, sample_rows, columns) -> Dict`
- Reads sample using `SELECT TOP (sample_rows)`
- Calculates:
  - Null counts and percentages
  - Distinct counts and cardinality (unique/high/medium/low)
  - Sample values (top 5 non-null)
  - Overall completeness
- Detects:
  - **Candidate keys**: Columns with unique values
  - **Candidate watermarks**: Datetime columns for incremental loading
- Returns `TableProfile` dict

### 6. `get_freshness(dataset, timestamp_column) -> Dict`
- If `timestamp_column` provided: `SELECT MAX(column)`
- Else: Queries `sys.tables.modify_date`
- Calculates age in hours
- Returns `FreshnessResult` dict with source indicator (data vs metadata)

## Implementation Details

### Error Handling
- All methods gracefully handle errors
- Return empty dicts/lists on failure
- Log errors via `get_logging_context()`
- Optional operations (row counts) wrapped in try/except

### SQL Queries
- Use parameterized queries to prevent SQL injection
- Leverage `self.read_sql()` for SELECT queries
- Use `INFORMATION_SCHEMA` for portability
- Use `sys.tables` and `sys.partitions` for row counts

### Return Types
- Import from `odibi.discovery.types`
- Use Pydantic models: `DatasetRef`, `Column`, `Schema`, `TableProfile`, `CatalogSummary`, `FreshnessResult`
- Return dicts via `model.model_dump()`

### Schema Parsing
- All methods accept `schema.table` or just `table`
- Default schema is `dbo` if not specified
- Consistent parsing logic across methods

## Testing

Created comprehensive test suite in `tests/integration/test_azure_sql_discovery.py`:

✅ 8 passing tests
- `test_list_schemas` - Verifies schema listing
- `test_list_tables` - Verifies table listing
- `test_get_table_info` - Verifies schema retrieval with row count
- `test_discover_catalog` - Verifies full catalog discovery
- `test_profile` - Verifies profiling with candidate detection
- `test_get_freshness_with_timestamp` - Verifies freshness from data
- `test_get_freshness_from_metadata` - Verifies freshness from metadata
- `test_schema_parsing` - Verifies schema.table parsing

All tests use mocks (no real database required for CI).

## Example Usage

```python
from odibi.connections.azure_sql import AzureSQL

# Connect to Azure SQL
conn = AzureSQL(
    server="myserver.database.windows.net",
    database="mydb",
    auth_mode="sql",
    username="user",
    password="pass"
)

# List schemas
schemas = conn.list_schemas()
# ['dbo', 'staging', 'warehouse']

# List tables in schema
tables = conn.list_tables("dbo")
# [{'name': 'customers', 'type': 'table', 'schema': 'dbo'}, ...]

# Get table schema
info = conn.get_table_info("dbo.customers")
# {'dataset': {...}, 'columns': [{...}, ...]}

# Discover full catalog
catalog = conn.discover_catalog(include_schema=True, limit=10)
# {'schemas': [...], 'tables': [...], 'total_datasets': 25, ...}

# Profile a table
profile = conn.profile("dbo.orders", sample_rows=5000)
# {
#   'candidate_keys': ['order_id'],
#   'candidate_watermarks': ['order_date', 'created_at'],
#   'completeness': 0.98,
#   'rows_sampled': 5000,
#   'total_rows': 1000000,
#   ...
# }

# Check data freshness
freshness = conn.get_freshness("dbo.orders", timestamp_column="order_date")
# {
#   'last_updated': '2024-03-15 10:30:00',
#   'source': 'data',
#   'age_hours': 2.5,
#   ...
# }

conn.close()
```

## Key Features

✅ **Logging**: All methods use `get_logging_context()` for structured logging
✅ **Error Recovery**: Graceful fallbacks (e.g., metadata freshness if data query fails)
✅ **Performance**: Fast queries using INFORMATION_SCHEMA and sys tables
✅ **Type Safety**: Pydantic models for validation and serialization
✅ **Flexibility**: Optional parameters for include_schema, include_stats, limit
✅ **Smart Detection**: Automatically identifies candidate keys and watermark columns
✅ **Profiling**: Statistical analysis with null counts, cardinality, completeness

## Files Modified

1. `odibi/connections/azure_sql.py` - Added 6 discovery methods (473 lines)
2. `tests/integration/test_azure_sql_discovery.py` - Added comprehensive test suite (8 tests)

## Next Steps

These discovery methods enable:
- **Auto-configuration**: Suggest pipeline configs based on profiling
- **Data quality**: Identify completeness issues and missing values
- **Incremental loading**: Auto-detect watermark columns
- **Documentation**: Generate data catalogs and lineage
- **Monitoring**: Track data freshness and staleness

The implementation follows the same patterns as other connection types (ADLS, Local) for consistency across the framework.
