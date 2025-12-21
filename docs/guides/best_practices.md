# Odibi Best Practices Guide

**Version:** 2.4.0  
**Last Updated:** 2025-12-03  
**Audience:** Data Engineers, Analytics Engineers, Team Leads

This guide covers recommended patterns for building maintainable, scalable, and production-ready Odibi pipelines.

---

## Table of Contents

1. [Project Organization](#1-project-organization)
2. [Pipeline Design](#2-pipeline-design)
3. [Node Design](#3-node-design)
4. [Naming Conventions](#4-naming-conventions)
5. [Configuration Management](#5-configuration-management)
6. [Performance](#6-performance)
7. [Data Quality](#7-data-quality)
8. [Cross-Pipeline Dependencies](#8-cross-pipeline-dependencies)
9. [Security](#9-security)
10. [Version Control](#10-version-control)

---

## 1. Project Organization

### Recommended Folder Structure

```
my-odibi-project/
├── project.yaml                    # Core config (connections, settings)
├── pipelines/
│   ├── bronze/
│   │   └── read_bronze.yaml        # Bronze layer pipeline
│   ├── silver/
│   │   └── transform_silver.yaml   # Silver layer pipeline
│   └── gold/
│       └── build_gold.yaml         # Gold layer pipeline
├── transformations/
│   ├── __init__.py
│   └── custom_transforms.py        # Custom Python transformations
├── sql/
│   └── complex_queries.sql         # Complex SQL (optional)
├── tests/
│   └── test_pipelines.py           # Pipeline tests
├── .env                            # Local secrets (git-ignored)
├── .gitignore
└── README.md
```

### Separation of Concerns

| File | Contains | Does NOT Contain |
|------|----------|------------------|
| `project.yaml` | Connections, system config, story config, imports | Pipeline definitions |
| `pipelines/*.yaml` | Pipeline and node definitions | Connection details |
| `transformations/` | Custom Python logic | YAML configuration |

### Example `project.yaml`

```yaml
project: OEE
description: "OEE Analytics Platform"
engine: spark
version: "1.0.0"
owner: "Data Team"

# === Connections (defined once, used everywhere) ===
connections:
  source_db:
    type: sql_server
    host: ${DB_HOST}
    database: ${DB_NAME}
    auth:
      mode: sql_login
      username: ${DB_USER}
      password: ${DB_PASS}

  lakehouse:
    type: azure_blob
    account_name: ${STORAGE_ACCOUNT}
    container: datalake
    auth:
      mode: account_key
      account_key: ${STORAGE_KEY}

# === System Catalog ===
system:
  connection: lakehouse
  path: _odibi_system

# === Story Configuration ===
story:
  connection: lakehouse
  path: stories/
  retention_days: 30
  auto_generate: true

# === Global Settings ===
performance:
  use_arrow: true
  skip_null_profiling: true

retry:
  enabled: true
  max_attempts: 3
  backoff: exponential

logging:
  level: INFO
  structured: true

# === Import Pipelines ===
imports:
  - pipelines/bronze/read_bronze.yaml
  - pipelines/silver/transform_silver.yaml
  - pipelines/gold/build_gold.yaml
```

### Example Pipeline File

**pipelines/bronze/read_bronze.yaml:**
```yaml
pipelines:
  - pipeline: read_bronze
    description: "Ingest raw data from source systems"
    layer: bronze
    nodes:
      - name: orders
        description: "Raw orders from ERP"
        read:
          connection: source_db
          format: sql
          table: sales.orders
          incremental:
            mode: stateful
            column: updated_at
        write:
          connection: lakehouse
          format: delta
          path: "bronze/orders"
          mode: append
          add_metadata: true

      - name: customers
        description: "Customer master data"
        read:
          connection: source_db
          format: sql
          table: sales.customers
        write:
          connection: lakehouse
          format: delta
          path: "bronze/customers"
          mode: append
          add_metadata: true
          skip_if_unchanged: true
          skip_hash_columns: [customer_id]
```

---

## 2. Pipeline Design

### One Pipeline Per Layer Per Domain

✅ **Good:**
```
pipelines/
├── bronze/
│   └── read_bronze.yaml           # All bronze ingestion
├── silver/
│   └── transform_silver.yaml      # All silver transformations
└── gold/
    ├── gold_sales.yaml            # Sales domain aggregates
    └── gold_inventory.yaml        # Inventory domain aggregates
```

❌ **Avoid:**
```
pipelines/
├── orders_bronze_silver_gold.yaml  # Too many concerns in one file
└── everything.yaml                 # Unmaintainable
```

### Pipeline Sizing Guidelines

| Node Count | Recommendation |
|------------|----------------|
| 1-20 nodes | Single pipeline file |
| 20-50 nodes | Consider splitting by sub-domain |
| 50+ nodes | Split into multiple pipelines |

### Keep Nodes with Their Pipeline

❌ **Don't split nodes into separate files:**
```yaml
# nodes/orders.yaml - BAD: nodes scattered across files
pipelines:
  - pipeline: read_bronze
    nodes:
      - name: orders
```

✅ **Keep nodes together:**
```yaml
# read_bronze.yaml - GOOD: all nodes in one place
pipelines:
  - pipeline: read_bronze
    nodes:
      - name: orders
        # ...
      - name: customers
        # ...
      - name: products
        # ...
```

**Why?**
- `depends_on` relationships are visible in one file
- Easier to understand the full pipeline flow
- One file = one pipeline = one commit for changes

---

## 3. Node Design

### Single Responsibility

Each node should do **one thing well**:

✅ **Good:**
```yaml
- name: load_orders
  read: ...
  write: ...

- name: clean_orders
  depends_on: [load_orders]
  transform:
    steps:
      - sql: "SELECT * FROM load_orders WHERE status IS NOT NULL"
  write: ...

- name: enrich_orders
  depends_on: [clean_orders, customers]
  transform:
    steps:
      - operation: join
        left: clean_orders
        right: customers
        on: [customer_id]
  write: ...
```

❌ **Avoid:**
```yaml
- name: do_everything
  read: ...
  transform:
    steps:
      - sql: "..."  # 500 lines of SQL doing everything
  write: ...
```

### Use Descriptions

Always add descriptions for documentation and debugging:

```yaml
- name: calculate_daily_revenue
  description: "Aggregates order amounts by day for finance reporting"
  tags: [daily, finance, critical]
```

### Cache Strategically

Use `cache: true` for nodes that are:
- Read by multiple downstream nodes
- Expensive to compute
- Small enough to fit in memory

```yaml
- name: dimension_products
  description: "Product dimension - cached for multiple joins"
  read: ...
  cache: true  # Multiple nodes will join to this
```

---

## 4. Naming Conventions

### Pipeline Names

Use `snake_case` with layer prefix:

| Pattern | Example |
|---------|---------|
| `{action}_{layer}` | `read_bronze`, `transform_silver`, `build_gold` |
| `{layer}_{domain}` | `bronze_sales`, `silver_inventory` |

### Node Names

Use descriptive `snake_case`:

| Pattern | Example |
|---------|---------|
| Source nodes | `orders`, `customers`, `products` |
| Transformed nodes | `clean_orders`, `enriched_customers` |
| Aggregated nodes | `daily_sales`, `monthly_revenue` |
| Dimension nodes | `dim_product`, `dim_customer` |
| Fact nodes | `fact_orders`, `fact_inventory` |

### Connection Names

Use environment + purpose:

```yaml
connections:
  prod_source_db:    # Production source database
  prod_lakehouse:    # Production data lake
  dev_lakehouse:     # Development data lake
```

---

## 5. Configuration Management

### Environment Variables for Secrets

✅ **Always use environment variables for sensitive data:**
```yaml
connections:
  database:
    host: ${DB_HOST}
    username: ${DB_USER}
    password: ${DB_PASSWORD}
```

❌ **Never hardcode secrets:**
```yaml
connections:
  database:
    password: "my_secret_password"  # NEVER DO THIS
```

### Use `.env` for Local Development

```bash
# .env (git-ignored)
DB_HOST=localhost
DB_USER=dev_user
DB_PASSWORD=dev_password
STORAGE_ACCOUNT=devaccount
STORAGE_KEY=abc123...
```

### Environment-Specific Overrides

```yaml
# In project.yaml
environments:
  dev:
    connections:
      lakehouse:
        container: dev-datalake
  prod:
    logging:
      level: WARNING
    connections:
      lakehouse:
        container: prod-datalake
```

Run with: `odibi run project.yaml --env prod`

---

## 6. Performance

### Enable Arrow for Pandas

```yaml
performance:
  use_arrow: true  # Major speedup for Parquet I/O
```

### Use Incremental Loading

Don't reload full tables every time:

```yaml
read:
  connection: source_db
  table: orders
  incremental:
    mode: stateful
    column: updated_at
    watermark_lag: "1d"
```

### Skip Unchanged Data

For dimension tables that rarely change:

```yaml
write:
  mode: append
  skip_if_unchanged: true
  skip_hash_columns: [id]
```

### Optimize Delta Writes (Spark)

```yaml
write:
  format: delta
  options:
    optimize_write: true
    cluster_by: [date, region]
```

### Skip Null Profiling for Large Tables

```yaml
performance:
  skip_null_profiling: true  # Faster for very large DataFrames
```

---

## 7. Data Quality & Validation

### Validation Strategy Overview

Odibi provides three validation mechanisms for different use cases:

| Mechanism | When Executed | Purpose | On Failure |
|-----------|---------------|---------|------------|
| **Contracts** | Before processing | Input validation | Always stops pipeline |
| **Validation** | After transformation | Output checks | Configurable (warn/error) |
| **Gates** | Before write | Critical path checks | Blocks downstream nodes |

### Use Contracts for Input Validation

Fail fast if source data is bad:

```yaml
- name: process_orders
  contracts:
    - type: not_null
      columns: [order_id, customer_id]
    - type: row_count
      min: 100
    - type: freshness
      column: created_at
      max_age: "24h"
  read: ...
  transform: ...
```

### Use Validation for Output Checks

Warn (or fail) if output doesn't meet expectations:

```yaml
- name: daily_revenue
  transform: ...
  validation:
    tests:
      - type: not_null
        columns: [date, revenue]
      - type: unique
        columns: [date]
      - type: range
        column: revenue
        min: 0
    on_failure: warn  # or "error" to fail the pipeline
```

### Available Validation Types

| Type | Description | Example |
|------|-------------|---------|
| `not_null` | Check for null values | `columns: [id, name]` |
| `unique` | Check for duplicates | `columns: [id]` |
| `row_count` | Validate row counts | `min: 100, max: 1000000` |
| `freshness` | Check data recency | `column: updated_at, max_age: "24h"` |
| `range` | Numeric bounds | `column: amount, min: 0, max: 10000` |
| `regex` | Pattern matching | `column: email, pattern: "^.+@.+$"` |
| `referential` | FK validation | `column: customer_id, reference: dim_customer.id` |
| `custom` | Custom Python function | `function: my_validation_func` |

### Use Quality Gates for Critical Paths

```yaml
- name: load_orders
  gate:
    - type: row_count
      min: 1000
      on_failure: block  # Stops pipeline if < 1000 rows
```

### FK Validation for Fact Tables

Ensure referential integrity before loading fact tables:

```yaml
- name: fact_orders
  depends_on: [dim_customer, dim_product]
  read:
    connection: staging
    path: orders
  validation:
    tests:
      - type: referential
        column: customer_id
        reference: dim_customer.customer_id
        on_orphan: warn
      - type: referential
        column: product_id
        reference: dim_product.product_id
        on_orphan: filter  # Remove orphan rows
  write:
    connection: warehouse
    path: fact_orders
```

### Custom Validation Functions

Register custom validation logic:

```python
from odibi import transform

@transform("validate_business_rules")
def validate_business_rules(context, current):
    """Custom business rule validation."""
    errors = []
    
    # Rule 1: Order amount must match line items
    mismatched = current[current['total'] != current['line_items_sum']]
    if len(mismatched) > 0:
        errors.append(f"{len(mismatched)} orders with mismatched totals")
    
    # Rule 2: Future dates not allowed
    future_orders = current[current['order_date'] > pd.Timestamp.now()]
    if len(future_orders) > 0:
        errors.append(f"{len(future_orders)} orders with future dates")
    
    if errors:
        context.log_warning(f"Validation issues: {'; '.join(errors)}")
    
    return current
```

Use in YAML:
```yaml
transform:
  steps:
    - function: validate_business_rules
```

### Quarantine Bad Records

Separate bad data for review instead of failing:

```yaml
- name: process_orders
  validation:
    tests:
      - type: not_null
        columns: [order_id, amount]
    on_failure: quarantine
    quarantine:
      connection: warehouse
      path: quarantine/orders
      include_reason: true  # Adds _quarantine_reason column
```

---

## 8. Cross-Pipeline Dependencies

### Use `$pipeline.node` References

When silver needs bronze outputs:

```yaml
# pipelines/silver/transform_silver.yaml
pipelines:
  - pipeline: transform_silver
    nodes:
      - name: enriched_orders
        inputs:
          orders: $read_bronze.orders           # Cross-pipeline reference
          customers: $read_bronze.customers
        transform:
          steps:
            - operation: join
              left: orders
              right: customers
              on: [customer_id]
        write:
          connection: lakehouse
          format: delta
          path: "silver/enriched_orders"
```

### Run Pipelines in Order

```bash
# Bronze first
odibi run project.yaml --pipeline read_bronze

# Then silver (references bronze outputs)
odibi run project.yaml --pipeline transform_silver
```

### Best Practices for References

1. **Always use `path:` in write config** — ensures cross-engine compatibility
2. **Run source pipeline first** — references require catalog entries
3. **Use meaningful node names** — `$read_bronze.orders` is clearer than `$p1.n1`

---

## 9. Security

### Mask Sensitive Columns in Stories

```yaml
- name: process_users
  sensitive: [email, ssn, phone]  # Masked in Data Stories
```

### Full Node Masking for PII-Heavy Nodes

```yaml
- name: medical_records
  sensitive: true  # Entire sample redacted
```

### Use Key Vault in Production

```yaml
connections:
  lakehouse:
    auth:
      mode: key_vault
      key_vault: my-key-vault
      secret: storage-account-key
```

### Never Log Secrets

Odibi automatically redacts values that look like secrets, but be careful in custom transformations:

```python
@transform
def my_transform(context, params):
    # ❌ NEVER do this
    print(f"Using password: {params['password']}")

    # ✅ Do this instead
    logger.info("Connecting to database...")
```

---

## 10. Version Control

### Git Ignore List

```gitignore
# .gitignore
.env
*.pyc
__pycache__/
.odibi/
stories/
*.log
.venv/
```

### Commit Guidelines

| Change Type | Commit Message |
|-------------|----------------|
| New pipeline | `feat(bronze): add customer ingestion pipeline` |
| New node | `feat(silver): add order enrichment node` |
| Bug fix | `fix(gold): correct revenue calculation` |
| Config change | `chore: update retry settings` |

### Branch Strategy

```
main           # Production-ready pipelines
├── develop    # Integration branch
├── feature/*  # New pipelines/nodes
└── fix/*      # Bug fixes
```

### PR Checklist

- [ ] Pipeline runs locally without errors
- [ ] Node descriptions added
- [ ] Sensitive columns marked
- [ ] Incremental config for large tables
- [ ] Tests pass

---

## Quick Reference

### Project Organization Cheat Sheet

```
project.yaml          → Connections, settings, imports (NO pipelines)
pipelines/{layer}/    → One YAML per pipeline
transformations/      → Custom Python code
.env                  → Local secrets (git-ignored)
```

### Node Checklist

- [ ] Descriptive name (`clean_orders` not `node_1`)
- [ ] Description explaining purpose
- [ ] Tags for filtering (`daily`, `critical`)
- [ ] `cache: true` if used by multiple nodes
- [ ] `sensitive` for PII columns
- [ ] Incremental config for large tables

### Performance Checklist

- [ ] `use_arrow: true` for Pandas
- [ ] Incremental loading for large sources
- [ ] `skip_if_unchanged` for dimensions
- [ ] `skip_null_profiling` for very large tables
- [ ] `cluster_by` for Spark/Delta

---

## Related Documentation

- [The Definitive Guide](the_definitive_guide.md) — Deep dive into architecture
- [Performance Tuning](performance_tuning.md) — Optimization details
- [Production Deployment](production_deployment.md) — Going to production
- [Cross-Pipeline Dependencies](../features/cross-pipeline-dependencies.md) — `$pipeline.node` references
- [Configuration Reference](../reference/configuration.md) — Full YAML schema
