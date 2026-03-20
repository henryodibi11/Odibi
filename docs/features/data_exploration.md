# Data Exploration

Explore and understand your data sources — local files, object storage, and SQL databases — without writing a pipeline.

## Overview

Odibi's exploration API lets you answer the questions every data engineer asks when starting a project:

- **What data do I have?** → `discover()`
- **What does it look like?** → `preview()`
- **What's the schema?** → `discover(conn, dataset=...)`
- **Is the data any good?** → `discover(conn, dataset=..., profile=True)`
- **Is it fresh?** → `freshness()`
- **How are tables related?** → `relationships()`

All methods are available on `PipelineManager` (via `from_yaml`) and directly on connection objects.

## Quick Start

```python
from odibi.pipeline import Pipeline

manager = Pipeline.from_yaml("config.yaml")

# What connections do I have?
catalog = manager.discover()

# What's in my SQL database?
manager.discover("crm_db")

# Peek at the data
manager.preview("crm_db", "dbo.Orders", rows=10)

# Profile data quality
manager.discover("crm_db", dataset="dbo.Orders", profile=True)

# Check freshness
manager.freshness("crm_db", "dbo.Orders", timestamp_column="order_date")

# Discover foreign keys
manager.relationships("crm_db")
```

## Methods

### `discover()` — Catalog Discovery

Discover what datasets exist in one or all connections.

```python
# Discover ALL connections at once
result = manager.discover()
# Returns: {"connections_scanned": 3, "results": {"local": {...}, "crm_db": {...}, ...}}

# Discover a single connection
result = manager.discover("crm_db")

# Filter by pattern
result = manager.discover("raw_data", pattern="*.csv")
result = manager.discover("crm_db", pattern="fact_*")

# Include column schemas
result = manager.discover("crm_db", include_schema=True)

# Get schema for a specific dataset
result = manager.discover("crm_db", dataset="dbo.Orders")

# Profile a specific dataset
result = manager.discover("crm_db", dataset="dbo.Orders", profile=True, sample_rows=5000)
```

#### What it returns

| Connection Type | Discovers |
|---|---|
| **Local / ADLS** | Files, folders, formats (CSV, Parquet, Delta, JSON), partition structure |
| **Azure SQL** | Schemas, tables, views, row counts |

### `preview()` — Sample Rows

See actual data without loading the full dataset. Returns up to 100 rows.

```python
# Preview a SQL table (default: 5 rows)
result = manager.preview("crm_db", "dbo.Customers")
for row in result["rows"]:
    print(row)
# {'customer_id': 1, 'name': 'Alice', 'email': 'alice@example.com'}
# {'customer_id': 2, 'name': 'Bob', 'email': 'bob@example.com'}
# ...

# Preview more rows
result = manager.preview("crm_db", "dbo.Customers", rows=20)

# Preview specific columns only
result = manager.preview("crm_db", "dbo.Customers", columns=["customer_id", "name"])

# Preview a CSV file
result = manager.preview("raw_data", "sales/2024.csv")

# Preview a Parquet file on ADLS
result = manager.preview("datalake", "bronze/orders/part-00000.parquet")
```

#### Response structure

```python
{
    "dataset": {"name": "Customers", "namespace": "dbo", "kind": "table", "row_count": 50000},
    "columns": ["customer_id", "name", "email", "created_at"],
    "rows": [
        {"customer_id": 1, "name": "Alice", "email": "alice@example.com", "created_at": "2024-01-15"},
        # ...
    ],
    "total_rows": 50000,
    "truncated": True,
}
```

### `freshness()` — Data Freshness

Check when data was last updated.

```python
# Check file freshness (uses file modification time)
result = manager.freshness("raw_data", "sales/2024.csv")
print(f"Last updated: {result['last_updated']}")
print(f"Age: {result['age_hours']:.1f} hours")

# Check SQL table freshness (uses sys.tables metadata)
result = manager.freshness("crm_db", "dbo.Orders")

# Check using a specific timestamp column (more accurate for SQL)
result = manager.freshness("crm_db", "dbo.Orders", timestamp_column="order_date")
```

#### Response structure

```python
{
    "dataset": {"name": "Orders", "namespace": "dbo", "kind": "table"},
    "last_updated": "2024-03-15T10:30:00",
    "source": "data",          # "data" = from timestamp column, "metadata" = from sys.tables
    "age_hours": 12.5,
}
```

### `relationships()` — Foreign Key Discovery

Discover foreign key relationships in SQL databases. Useful for understanding how tables relate before building dimensional models.

```python
# Discover all foreign keys
rels = manager.relationships("crm_db")
for rel in rels:
    parent = f"{rel['parent']['namespace']}.{rel['parent']['name']}"
    child = f"{rel['child']['namespace']}.{rel['child']['name']}"
    keys = ", ".join(f"{p}→{c}" for p, c in rel["keys"])
    print(f"  {child} → {parent}  ({keys})")
# dbo.Orders → dbo.Customers  (customer_id→customer_id)
# dbo.OrderItems → dbo.Orders  (order_id→order_id)
# dbo.OrderItems → dbo.Products  (product_id→product_id)

# Filter by schema
rels = manager.relationships("crm_db", schema="sales")
```

#### Response structure

```python
[
    {
        "parent": {"name": "Customers", "namespace": "dbo", "kind": "table"},
        "child": {"name": "Orders", "namespace": "dbo", "kind": "table"},
        "keys": [["customer_id", "customer_id"]],
        "source": "declared",     # From actual FK constraints
        "confidence": 1.0,
        "details": {"constraint_name": "FK_Orders_Customers"},
    }
]
```

!!! tip "Use relationships to plan your star schema"
    The output from `relationships()` maps directly to dimension/fact table design.
    Parent tables are typically dimensions, child tables are typically facts.

## Supported Connection Types

| Method | Local FS | ADLS | Azure SQL |
|---|:---:|:---:|:---:|
| `discover()` | ✅ | ✅ | ✅ |
| `preview()` | ✅ | ✅ | ✅ |
| `freshness()` | ✅ | ✅ | ✅ |
| `relationships()` | — | — | ✅ |
| `profile()` (via discover) | ✅ | ✅ | ✅ |

## Direct Connection Access

You can also use the discovery API directly on connection objects:

```python
from odibi.connections.local import LocalConnection
from odibi.connections.azure_sql import AzureSQL

# Local filesystem
conn = LocalConnection(base_path="./data")
conn.list_files(pattern="*.csv")
conn.list_folders()
conn.discover_catalog(recursive=True)
conn.get_schema("sales.csv")
conn.profile("sales.csv", sample_rows=1000)
conn.preview("sales.csv", rows=10)
conn.get_freshness("sales.csv")
conn.detect_partitions("orders/")

# Azure SQL
sql = AzureSQL(server="myserver.database.windows.net", database="mydb", auth_mode="aad_msi")
sql.list_schemas()
sql.list_tables("dbo")
sql.get_table_info("dbo.Orders")
sql.discover_catalog(include_schema=True)
sql.profile("dbo.Orders", sample_rows=5000)
sql.preview("dbo.Orders", rows=10)
sql.get_freshness("dbo.Orders", timestamp_column="order_date")
sql.relationships(schema="dbo")
```

## Typical Workflow

A common exploration workflow when starting a new project:

```python
from odibi.pipeline import Pipeline

manager = Pipeline.from_yaml("config.yaml")

# 1. What connections do I have?
overview = manager.discover()
print(f"Scanned {overview['connections_scanned']} connections")

# 2. What's in the SQL database?
catalog = manager.discover("warehouse")
for table in catalog.get("tables", []):
    print(f"  {table['namespace']}.{table['name']} ({table.get('row_count', '?')} rows)")

# 3. How are tables related?
rels = manager.relationships("warehouse")
for rel in rels:
    print(f"  {rel['child']['name']} → {rel['parent']['name']}")

# 4. Preview interesting tables
preview = manager.preview("warehouse", "dbo.Orders", rows=5)
for row in preview["rows"]:
    print(row)

# 5. Profile for data quality
profile = manager.discover("warehouse", dataset="dbo.Orders", profile=True)
print(f"Completeness: {profile.get('completeness', 'N/A')}")
print(f"Candidate keys: {profile.get('candidate_keys', [])}")
print(f"Watermark columns: {profile.get('candidate_watermarks', [])}")

# 6. Check freshness
fresh = manager.freshness("warehouse", "dbo.Orders", timestamp_column="order_date")
print(f"Last updated: {fresh.get('last_updated')} ({fresh.get('age_hours', '?')} hours ago)")
```

## Error Handling

All methods return structured error dicts instead of raising exceptions:

```python
# Connection not found
result = manager.preview("nonexistent", "table")
# {"error": {"code": "CONNECTION_NOT_FOUND", "message": "...", "available_connections": [...]}}

# Feature not supported
result = manager.relationships("local_data")
# {"error": {"code": "NOT_SUPPORTED", "message": "...", "fix": "Only SQL connections support FK discovery"}}
```

## Related

- [Connections](connections.md) — Connection configuration and authentication
- [Patterns](patterns.md) — Loading patterns for building pipelines
- [Python API Guide](../guides/python_api_guide.md) — Full programmatic usage
- [Catalog](catalog.md) — System catalog for metadata tracking
