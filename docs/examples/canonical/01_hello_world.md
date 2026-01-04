# Example 1: Hello World (CSV → Parquet)

The simplest possible Odibi pipeline. Read a CSV, write Parquet.

## When to Use

- First-time users learning Odibi
- Quick local data conversions
- Prototyping before production setup

## Expected Output

- `data/bronze/customers/*.parquet` — Your data in columnar format
- `data/bronze/stories/*.html` — Audit report with row counts and schema

---

## Full Config

```yaml
# odibi.yaml
project: hello_world

connections:
  landing:
    type: local
    base_path: ./data/landing
  bronze:
    type: local
    base_path: ./data/bronze

story:
  connection: bronze
  path: stories

system:
  connection: bronze
  path: _system

pipelines:
  - pipeline: ingest
    layer: bronze
    nodes:
      - name: ingest_customers
        read:
          connection: landing
          format: csv
          path: customers.csv
          options:
            header: true
        write:
          connection: bronze
          format: parquet
          path: customers
          mode: overwrite
```

---

## Sample Data

Copy from `docs/examples/canonical/sample_data/customers.csv` or create `data/landing/customers.csv`:

```csv
customer_id,name,email,tier,city,updated_at
1,Alice,alice@example.com,Gold,NYC,2025-01-01
2,Bob,bob@example.com,Silver,LA,2025-01-01
3,Charlie,charlie@example.com,Bronze,Chicago,2025-01-01
```

---

## Run

```bash
odibi run odibi.yaml
```

---

## Schema Reference

| Key | Docs |
|-----|------|
| `connections[].type: local` | [LocalConnectionConfig](../../reference/yaml_schema.md#localconnectionconfig) |
| `read.format: csv` | [ReadConfig](../../reference/yaml_schema.md#readconfig) |
| `write.format: parquet` | [WriteConfig](../../reference/yaml_schema.md#writeconfig) |
| `story` | [StoryConfig](../../reference/yaml_schema.md#storyconfig) |
