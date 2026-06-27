---
name: pipeline-yaml-authoring
description: "Use when writing or debugging Odibi pipeline YAML — node structure (read/transformer/transform/validation/write), applying patterns, and the field names the strict validator rejects. Pairs get_schema + validate_yaml."
requires: [odibi]
---

# pipeline-yaml-authoring Skill

Author Odibi pipeline YAML that passes the strict Pydantic validator on the first or
second try. **Source of truth is `get_schema` (the config contract), not memory** — the
validator hard-errors on unknown keys with a did-you-mean hint.

## When to Load This Skill

- Writing a new pipeline / node / transform chain.
- A `validate_yaml` call returned did-you-mean or hallucinated-field errors.
- You need to apply a pattern (dimension/fact/scd2/merge/aggregation/date_dimension).

## Top-Level Structure

```yaml
project: my_project          # required, alphanumeric + underscore
engine: pandas               # optional: pandas | spark | polars (default pandas)

connections: { ... }         # required — see add-a-connection skill

pipelines:                   # required: list of pipelines
  - pipeline: build_silver   # alphanumeric + underscore
    layer: silver            # optional label: bronze | silver | gold
    nodes: [ ... ]

story:                       # MANDATORY
  connection: gold
  path: stories
system:                      # MANDATORY
  connection: gold
  path: _system
```

## Node Structure

A node runs in this fixed order: **read → transformer → transform.steps → validation → write.**
At least one of `read` / `inputs` / `transformer` / `transform` / `write` is required.

```yaml
- name: clean_customers          # alphanumeric + underscore (valid SQL identifier)
  depends_on: [load_raw]         # parents that must run first (their output is readable)
  read:
    connection: bronze
    format: parquet              # csv | parquet | json | delta | sql
    path: customers              # relative to connection base_path
  transform:
    steps:                       # ordered; each step gets prior step's df
      - sql: "SELECT * FROM df WHERE email IS NOT NULL"
      - function: clean_text     # a registered transformer (list_transformers)
        params: { columns: [name, email], operations: [trim, lower] }
  validation:                    # optional — see validation-workflow skill
    tests:
      - { type: not_null, column: customer_id }
  write:
    connection: silver
    format: delta
    table: dim_customers         # or path: ...
    mode: upsert                 # overwrite | append | upsert | append_once | merge
    options: { keys: [customer_id] }
```

### transformer vs transform — pick the right one

| Use `transformer:` (+ `params:`) | Use `transform: { steps: [...] }` |
|---|---|
| One heavy, pre-packaged operation | A chain of small steps |
| Patterns: `dimension`, `fact`, `scd2`, `merge`, `aggregation`, `date_dimension` | SQL filters, registered `function:` calls |
| `deduplicate`, `merge`, `scd2` heavy lifters | custom business logic / cleanup |

Both can appear on one node — `transformer` runs first, then `transform.steps` refine it.
A `transform` step must specify **exactly one** of `sql`, `sql_file`, `function`, `operation`.

## Applying a Pattern

Patterns are invoked exactly like transformers: `transformer: <pattern>` + `params:`.
Use `list_patterns` to see them and `apply_pattern_template` to get a filled stub, then
`get_schema` for the params contract.

```yaml
- name: dim_customer
  read: { connection: source, format: csv, path: customers.csv }
  transformer: dimension
  params:
    natural_key: customer_id
    surrogate_key: customer_sk
    scd_type: 1
    track_cols: [name, email, tier]
    unknown_member: true
  write: { connection: gold, format: parquet, path: dim_customer, mode: overwrite }

- name: fact_sales
  depends_on: [dim_customer]
  read: { connection: source, format: csv, path: orders.csv }
  transformer: fact
  params:
    grain: [order_id, line_item_id]
    dimensions:
      - { source_column: customer_id, dimension_table: dim_customer,
          dimension_key: customer_id, surrogate_key: customer_sk }
    orphan_handling: unknown
    measures: [quantity, amount]
  write: { connection: gold, format: parquet, path: fact_sales, mode: overwrite }
```

If a node has `transformer:` it **must** have non-empty `params:` (validator enforces it),
and known patterns enforce their required params with a clear error listing what's missing.

## Write Modes

| Mode | Behavior | Required |
|---|---|---|
| `overwrite` | Replace all data | — |
| `append` | Add rows (may duplicate) | — |
| `upsert` | Update by key, insert new | `options: { keys: [...] }` |
| `append_once` | Insert only new keys (idempotent) — best for Bronze | `options: { keys: [...] }` |
| `merge` | SQL Server T-SQL MERGE via staging | `options: { keys: [...] }` (merge keys) |

## Common Mistakes the Strict Validator Rejects

| Wrong | Correct | Why |
|---|---|---|
| `source:` / `sink:` / `outputs:` | `read:` / `write:` | hallucinated node fields → hard error |
| `sql:` at node top level | `read: { query: ... }` or a `transform` step `sql:` | not a node field |
| `pattern: { type: dimension }` | `transformer: dimension` + `params:` | there is **no** `pattern:` field |
| `dim-customer`, `dim.customer` | `dim_customer` | names must match `^[a-zA-Z0-9_]+$` |
| `mode: upsert` with no keys | add `options: { keys: [...] }` | required for upsert/append_once/merge |
| misspelled key (e.g. `base_pth`) | fix it | strict models hard-error with a suggestion |
| `transformer:` with empty `params` | provide params or drop transformer | validator rejects |

## Authoring Loop

1. `get_schema` → the exact contract for the section you're writing.
2. `list_transformers` / `list_patterns` → valid `function:` / `transformer:` names.
3. `apply_pattern_template` + `get_example` → a working starting point.
4. `validate_yaml` → fix every did-you-mean / required-param error.
5. `test_pipeline` → run it; then `node_sample` / `story` to inspect.
