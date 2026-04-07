---
title: Unity Catalog Setup Guide
roles: [sr-de]
tags: [guide:platform, topic:databricks, topic:unity-catalog, topic:delta-lake]
prereqs: [setup_azure.md, ../tutorials/spark_engine.md]
next: [performance_tuning.md, production_deployment.md]
related: [../features/connections.md, ../tutorials/azure_connections.md]
time: 15m
---

# Unity Catalog Setup Guide

This guide covers connecting Odibi to **Databricks Unity Catalog (UC)** — the 3-level namespace model (`catalog.schema.table`) that manages data governance, access control, and storage credentials centrally.

---

## Why Unity Catalog?

| Without UC | With UC |
|------------|---------|
| Mount points (`/mnt/...`) or ADLS paths | Governed table names (`catalog.schema.table`) |
| Manual credential config per connection | Storage credentials managed centrally |
| Hive metastore (single `default` catalog) | Multiple catalogs, schemas, fine-grained ACLs |
| `CREATE TABLE ... LOCATION` for registration | Managed tables — auto-registered |

**Key insight:** UC handles storage and credentials for you. Odibi connections become lightweight pointers to a `catalog.schema` pair — no account keys, no mount paths.

---

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- A UC catalog and schema created (e.g., `my_catalog.bronze`, `my_catalog.silver`)
- Odibi installed with Spark extras: `pip install "odibi[spark]"`

---

## 1. Connection Setup

Use the `delta` connection type with `catalog` and `schema` fields. This maps directly to UC's 3-level namespace:

```yaml
# project.yaml
project: "my_uc_project"
engine: spark

connections:
  bronze:
    type: delta
    catalog: my_catalog
    schema: bronze

  silver:
    type: delta
    catalog: my_catalog
    schema: silver

  gold:
    type: delta
    catalog: my_catalog
    schema: gold
```

Each connection owns **tier 1** (catalog) and **tier 2** (schema). Your pipeline nodes supply **tier 3** (table).

!!! info "How it works under the hood"
    The `DeltaCatalogConnection.get_path(table)` method returns `my_catalog.silver.dim_customers`.
    This flows through `DeltaTable.forName()`, `spark.table()`, and `saveAsTable()` — all UC-native APIs.

---

## 2. Reading Data

Use `table:` with the table name only — the connection resolves the full 3-level path:

```yaml
nodes:
  load_customers:
    read:
      connection: bronze
      format: delta
      table: raw_customers    # → my_catalog.bronze.raw_customers
```

### Time Travel (works with UC)

```yaml
nodes:
  debug_snapshot:
    read:
      connection: silver
      format: delta
      table: dim_customers
      time_travel:
        as_of_version: 5
```

---

## 3. Writing Data

Same pattern — `connection` provides catalog + schema, `table:` provides the table name:

```yaml
nodes:
  write_customers:
    write:
      connection: silver
      format: delta
      table: dim_customers    # → my_catalog.silver.dim_customers
      mode: overwrite
```

### Upsert / Merge

```yaml
nodes:
  upsert_orders:
    write:
      connection: silver
      format: delta
      table: fact_orders
      mode: upsert
      options:
        keys: [order_id]
```

### Optimized Writes

All Delta write features work with UC managed tables:

```yaml
nodes:
  write_sales:
    write:
      connection: gold
      format: delta
      table: fact_sales
      mode: append
      partition_by: [sale_year, sale_month]
      zorder_by: [customer_id, product_id]
      table_properties:
        "delta.autoOptimize.optimizeWrite": "true"
        "delta.autoOptimize.autoCompact": "true"
```

---

## 4. Patterns with UC

All Odibi patterns work with UC table names.

### SCD2

```yaml
nodes:
  scd2_customers:
    read:
      connection: bronze
      format: delta
      table: raw_customers

    transformer: scd2
    params:
      target: "my_catalog.silver.dim_customers"
      keys: [customer_id]
      track_cols: [name, address, tier]
      effective_time_col: updated_at

    # No write: block needed — SCD2 is self-contained
```

!!! tip "SCD2 target format"
    The `target` param in SCD2 needs the **full 3-level name** since it bypasses the connection resolution.
    Use `"catalog.schema.table"` format directly.

### Merge Transformer

```yaml
nodes:
  merge_products:
    read:
      connection: bronze
      format: delta
      table: raw_products

    transformer: merge
    params:
      target: "my_catalog.silver.dim_products"
      keys: [product_id]
      strategy: upsert
```

### Dimension Pattern

```yaml
nodes:
  build_dim_customer:
    read:
      connection: bronze
      format: delta
      table: raw_customers

    pattern: dimension
    params:
      natural_key: customer_id
      surrogate_key: customer_sk
      scd_type: 2
      track_cols: [name, address, phone]
      target: "my_catalog.silver.dim_customers"

    write:
      connection: silver
      format: delta
      table: dim_customers
      mode: overwrite
```

---

## 5. Mixed Connections (UC + SQL Server)

A common real-world setup: read from SQL Server, write to UC.

```yaml
connections:
  erp_source:
    type: sql_server
    server: erp-server.database.windows.net
    database: production
    auth:
      mode: aad_msi

  silver:
    type: delta
    catalog: my_catalog
    schema: silver

nodes:
  ingest_orders:
    read:
      connection: erp_source
      format: sql
      table: dbo.SalesOrders

    write:
      connection: silver
      format: delta
      table: raw_orders
      mode: append
```

---

## 6. Databricks Notebook Integration

On Databricks, pass the existing `spark` session — it already has UC configured:

```python
from odibi import Pipeline

# Databricks provides a pre-configured spark session
# No need for ADLS keys, mount points, or credential setup
pipeline = Pipeline.from_yaml(
    "project.yaml",
    "pipeline.yaml",
    spark_session=spark,  # The Databricks-provided session
)

pipeline.run()
```

---

## What You DON'T Need

| Feature | Why not needed with UC |
|---------|----------------------|
| `register_table:` | UC managed tables are auto-registered |
| ADLS connection for lakehouse | UC manages storage credentials |
| `base_path:` / mount points | UC abstracts storage location |
| `configure_spark()` calls | The Databricks session is pre-configured |

---

## Migrating from Hive Metastore

If you have existing Odibi projects using `spark_catalog`:

```yaml
# BEFORE (Hive metastore)
connections:
  silver:
    type: delta
    catalog: spark_catalog
    schema: silver_db

# AFTER (Unity Catalog)
connections:
  silver:
    type: delta
    catalog: my_catalog      # ← Your UC catalog name
    schema: silver            # ← Your UC schema name
```

That's it — change two strings. All pipeline YAML stays the same.

---

## Troubleshooting

### "Table or view not found"

- Verify the catalog and schema exist: `SHOW SCHEMAS IN my_catalog`
- Check permissions: `SHOW GRANTS ON SCHEMA my_catalog.silver`

### "Cannot create managed table with LOCATION"

- You're using `register_table:` which creates external tables. Remove it — UC managed tables don't need it.

### SCD2 target not resolving

- The `target` param in SCD2/Merge transformers needs the full 3-level name (`catalog.schema.table`), not just the table name. The connection isn't used for target resolution in these transformers.
