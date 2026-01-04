# Example 2: Incremental SQL Ingestion (Database → Raw with HWM)

Load data from a SQL Server database incrementally using high-water mark (HWM).

## When to Use

- Source table has millions of rows
- Source has an `updated_at` or `created_at` timestamp column
- You want to load only new/changed rows after the first full load

## How It Works

1. **First run**: No state exists → Full load (all rows)
2. **Subsequent runs**: State exists → Only rows where `updated_at > last_hwm`
3. State is persisted to `_system` catalog automatically

---

## Full Config

```yaml
# odibi.yaml
project: incremental_orders

engine: spark  # Use Spark for JDBC

connections:
  source_db:
    type: sql_server
    host: ${SQL_SERVER_HOST}
    database: production
    username: ${SQL_USER}
    password: ${SQL_PASSWORD}
  
  lake:
    type: local
    base_path: ./data/lake

story:
  connection: lake
  path: stories

system:
  connection: lake
  path: _system

pipelines:
  - pipeline: bronze_orders
    layer: bronze
    nodes:
      - name: ingest_orders
        read:
          connection: source_db
          format: jdbc
          table: dbo.orders
        incremental:
          mode: stateful
          column: updated_at
          # Optional: limit first load to recent data
          # first_run_lookback: "365d"
        write:
          connection: lake
          format: delta
          path: bronze/orders
          mode: append
```

---

## Environment Variables

Set these before running:

```bash
export SQL_SERVER_HOST=your-server.database.windows.net
export SQL_USER=reader
export SQL_PASSWORD=your-password
```

---

## Run

```bash
# First run: Full load
odibi run odibi.yaml

# Second run: Incremental (only new rows)
odibi run odibi.yaml
```

---

## Check State

```bash
odibi catalog state odibi.yaml
```

Output:
```
bronze_orders.ingest_orders:
  hwm: 2025-01-03T12:00:00
  last_run: 2025-01-03T12:05:32
```

---

## Schema Reference

| Key | Docs |
|-----|------|
| `connections[].type: sql_server` | [SQLServerConnectionConfig](../../reference/yaml_schema.md#sqlserverconnectionconfig) |
| `incremental.mode: stateful` | [IncrementalConfig](../../reference/yaml_schema.md#incrementalconfig) |
| `write.mode: append` | [WriteConfig](../../reference/yaml_schema.md#writeconfig) |

---

## Common Patterns

**Choose `rolling_window` instead if:**
- You don't need exact row-level tracking
- Source rows can be updated without changing `updated_at`
- You want simpler state management

```yaml
incremental:
  mode: rolling_window
  column: created_at
  lookback: "7d"  # Always load last 7 days
```

[→ Pattern: Incremental Stateful](../../patterns/incremental_stateful.md)
