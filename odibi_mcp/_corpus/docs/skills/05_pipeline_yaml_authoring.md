# Skill 05 — Pipeline YAML Authoring

> **Layer:** Building
> **When:** Writing, generating, or debugging odibi pipeline YAML configs.
> **Source of truth:** `odibi/config.py` — all Pydantic models that validate YAML.

---

## Purpose

Odibi pipelines are defined in YAML. Agents frequently generate invalid YAML by hallucinating field names, using wrong write modes, or violating node naming rules. This skill encodes the exact rules.

---

## Pipeline Structure

```yaml
pipeline: my_pipeline          # Required. Alphanumeric + underscore only.
layer: silver                  # Optional. bronze | silver | gold
engine: pandas                 # Optional. pandas | spark | polars (default: pandas)

connections:                   # Required. Named connection configs.
  my_source:
    type: local                # local | azure_blob | sql_server | http | delta
    base_path: ./data/raw

  my_target:
    type: local
    base_path: ./data/silver

nodes:                         # Required. List of processing nodes.
  - name: ingest_customers     # Required. Alphanumeric + underscore only.
    read:                      # Required (except date_dimension pattern).
      connection: my_source
      path: customers.csv
      format: csv

    transform:                 # Optional. List of transformation steps.
      steps:
        - function: clean_text
          params:
            columns: [name, email]
            operations: [trim, lower]
        - function: derive_columns
          params:
            derivations:
              full_name: "concat(first_name, ' ', last_name)"

    validation:                # Optional. Data quality tests.
      tests:
        - type: not_null
          column: customer_id
        - type: unique
          column: email

    write:                     # Required. Output configuration.
      connection: my_target
      path: customers
      format: delta
      mode: overwrite
```

---

## Critical Field Names

### ✅ Correct Field Names
| Field | Used In | Purpose |
|-------|---------|---------|
| `read:` | Node | Input configuration |
| `write:` | Node | Output configuration |
| `transform:` | Node | Transformation steps |
| `validation:` | Node | Data quality tests |
| `connection:` | Read/Write | Connection reference |
| `path:` | Read/Write | File/table path |
| `format:` | Read/Write | Data format |
| `query:` | Read | SQL query (for SQL sources) |
| `table:` | Read | SQL table name |
| `mode:` | Write | Write mode |
| `options:` | Write | Additional write options |

### ❌ Never Use These
| Wrong | Correct | Notes |
|-------|---------|-------|
| `source:` | `read:` | Common hallucination |
| `sink:` | `write:` | Common hallucination |
| `target:` | `write:` | Use only in pattern params |
| `inputs:` | `read:` | Not an odibi concept |
| `outputs:` | `write:` | Not an odibi concept |
| `sql:` | `query:` | For SQL sources |
| `steps:` (top-level) | `transform: steps:` | Must be nested |

---

## Write Modes

| Mode | Behavior | Required Options |
|------|----------|------------------|
| `overwrite` | Replace all data | None |
| `append` | Add rows (may duplicate) | None |
| `upsert` | Update by key, insert new | `options: {keys: [col1]}` |
| `append_once` | Insert only new keys (idempotent) | `options: {keys: [col1]}` |
| `merge` | SQL Server MERGE statement | `merge_keys: [col1]` |

### ⚠️ Common Errors

**Missing keys for upsert/append_once → RUNTIME FAILURE:**
```yaml
# ❌ WRONG — will fail at runtime
write:
  mode: upsert

# ✅ CORRECT
write:
  mode: upsert
  options:
    keys: [customer_id]
```

**Wrong key field for merge:**
```yaml
# ❌ WRONG — merge uses merge_keys, not options.keys
write:
  mode: merge
  options:
    keys: [id]

# ✅ CORRECT
write:
  mode: merge
  merge_keys: [customer_id]
```

---

## Node Naming Rules

- ✅ `ingest_customers`, `dim_product`, `fact_sales_2024`
- ❌ `ingest-customers` (hyphens)
- ❌ `dim.product` (dots)
- ❌ `fact sales` (spaces)
- Pattern: `^[a-zA-Z0-9_]+$`
- Spark registers node outputs as temp views — names must be valid SQL identifiers

---

## Connection Types

### Local
```yaml
connections:
  local_data:
    type: local
    base_path: ./data
```

### Azure Blob / ADLS
```yaml
connections:
  adls:
    type: azure_blob
    account_name: mystorageaccount
    container: data
    auth:
      mode: key                # key | sas | service_principal | msi
      key: "${AZURE_STORAGE_KEY}"
```

### SQL Server / Azure SQL
```yaml
connections:
  sql:
    type: sql_server
    host: myserver.database.windows.net
    database: mydb
    auth_mode: sql             # sql | aad_msi | aad_sp
    username: "${SQL_USER}"
    password: "${SQL_PASS}"
```

### HTTP API
```yaml
connections:
  api:
    type: http
    base_url: https://api.example.com
    headers:
      Authorization: "Bearer ${API_TOKEN}"
```

---

## Read Configuration

### File sources
```yaml
read:
  connection: local_data
  path: customers.csv          # Relative to connection base_path
  format: csv                  # csv | parquet | json | delta | avro | excel
  options:                     # Format-specific options
    encoding: utf-8
    delimiter: ","
    header: true
```

### SQL sources
```yaml
read:
  connection: sql
  format: sql
  table: dbo.Customers         # OR use query:

read:
  connection: sql
  format: sql
  query: "SELECT * FROM dbo.Customers WHERE active = 1"
```

### Incremental loading
```yaml
read:
  connection: sql
  format: sql
  table: dbo.Orders
  incremental:
    column: modified_date      # High-water mark column
    mode: append               # append | replace
```

---

## Transform Configuration

```yaml
transform:
  steps:
    - function: filter_rows            # Registered function name
      params:
        condition: "status = 'active'"

    - function: derive_columns
      params:
        derivations:
          year: "EXTRACT(YEAR FROM order_date)"

    - function: deduplicate
      params:
        keys: [order_id]
        order_by: [modified_at]
        keep: last
```

**Rules:**
- `function:` must match a name in `FunctionRegistry`
- Use `odibi list transformers` to see all available names
- `params:` must match the transformer's Pydantic model fields
- Steps execute in order — output of step N is input to step N+1

---

## Validation Configuration

```yaml
validation:
  tests:
    - type: not_null
      column: customer_id
    - type: unique
      column: email
    - type: accepted_values
      column: status
      values: [active, inactive, pending]
    - type: range
      column: age
      min: 0
      max: 150
    - type: row_count
      min: 1
    - type: freshness
      column: updated_at
      max_age: "24h"

  quarantine:                  # Optional. Route failed rows.
    enabled: true
    path: quarantine/customers
    format: parquet

  gate:                        # Optional. Batch-level threshold.
    min_pass_rate: 0.95
    on_fail: abort             # abort | warn | skip
```

---

## Pattern Nodes

Patterns use `transformer:` and `params:` instead of `transform: steps:`:

### Dimension
```yaml
- name: dim_customer
  read:
    connection: sql
    format: sql
    table: dbo.DimCustomer
  transformer: dimension
  params:
    natural_key: customer_id
    surrogate_key: customer_sk
    scd_type: 1
    target: gold/dim_customer
  write:
    connection: local
    path: gold/dim_customer
    format: delta
    mode: overwrite
```

### SCD2
```yaml
- name: scd2_employee
  read:
    connection: sql
    format: sql
    table: dbo.Employee
  transformer: scd2
  params:
    keys: [employee_id]
    tracked_columns: [name, email, department]
    target: gold/dim_employee
  write:
    connection: local
    path: gold/dim_employee
    format: delta
    mode: append
```

### Fact
```yaml
- name: fact_orders
  read:
    connection: sql
    format: sql
    table: dbo.FactOrders
  transformer: fact
  params:
    keys: [order_id]
    dimension_lookups:
      customer_sk:
        dimension: gold/dim_customer
        natural_key: customer_id
        surrogate_key: customer_sk
  write:
    connection: local
    path: gold/fact_orders
    format: delta
    mode: upsert
    options:
      keys: [order_id]
```

---

## Validation Checklist

Before saving YAML:
- [ ] `pipeline:` name is alphanumeric + underscore
- [ ] All `name:` fields are alphanumeric + underscore
- [ ] All connections are defined in `connections:` section
- [ ] `read:` has `connection:` and either `path:` or `query:`/`table:`
- [ ] `write:` has `connection:`, `path:`, `format:`, and `mode:`
- [ ] `upsert`/`append_once` modes have `options: {keys: [...]}`
- [ ] `merge` mode has `merge_keys: [...]`
- [ ] `transform: steps:` functions are registered names (check with `odibi list transformers`)
- [ ] Run `odibi validate <file>.yaml` to catch errors before running
