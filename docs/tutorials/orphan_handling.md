# Orphan Handling Tutorial — Fact Pattern `orphan_handling`

> **GitHub Issue:** #222
> **Prerequisite:** Familiarity with odibi pipeline basics (see [Getting Started](getting_started.md)) and dimension tables. For validation-engine quarantine (`on_fail: quarantine`), see [Quarantine & Orphan Handling Tutorial](quarantine_workflow.md); this tutorial covers the Fact Pattern's `orphan_handling` parameter instead.

---

## 1. Problem Statement

When you build a fact table that joins to dimension tables, some fact rows may reference dimension keys that do not exist. These are **orphans**.

For example, if `fact_orders.customer_id` is `999` but `dim_customer` only contains IDs 1–5, that order is an orphan: it cannot be joined to a customer, so the surrogate key lookup returns no match. Left unhandled, orphans either break the pipeline (join drops them silently) or produce fact rows with missing surrogate keys, corrupting downstream analytics.

You need to decide, up front, what happens to orphans:

- **Silently drop them** — you lose data without knowing why.
- **Fail the pipeline** — safe, but stops all processing until upstream is fixed.
- **Quarantine them** — good rows flow to the target, orphan rows are written to a separate output with metadata explaining why they were rejected.

This tutorial shows all three Fact Pattern strategies, with a runnable pipeline that verifies the quarantine strategy end to end.

---

## 2. Three Strategies for Orphan Handling

The Fact Pattern's `orphan_handling` parameter accepts three values:

| Strategy | Behavior | When to use |
|---|---|---|
| `unknown` (default) | Orphan rows get surrogate key `0` (the unknown member) and pass through. | You want to keep all fact rows and tag orphans with a known unknown-member SK. |
| `reject` | Raises a `ValueError` when any orphan is found, stopping the pipeline. | You need strict data quality and cannot tolerate any orphan rows. |
| `quarantine` | Orphan rows are split out and written to a separate quarantine output with metadata columns; valid rows are written normally. | You want to keep the pipeline running while isolating bad rows for review. |

### Strategy 1: `unknown` (SK = 0)

```yaml
orphan_handling: unknown
```

Orphan fact rows receive surrogate key `0`, which maps to the dimension table's unknown member row. The pipeline completes, but orphaned facts are grouped under the unknown member. Downstream reports should filter out SK `0` if you do not want orphans included.

### Strategy 2: `reject`

```yaml
orphan_handling: reject
```

If any orphan row is found, `odibi run` raises a `ValueError` and exits non-zero. No fact rows are written. Use this when orphan data is a critical error that must be fixed upstream before the pipeline can proceed.

> **Note:** This strategy is **not runnable** in the tutorial pipeline below because `odibi run` would exit non-zero on the orphan rows in the sample data. It is described here for completeness; the runnable YAML uses `quarantine`.

### Strategy 3: `quarantine`

```yaml
orphan_handling: quarantine
quarantine:
  connection: quarantine_store
  path: fact_orders_orphans
  add_columns:
    _rejection_reason: true
    _rejected_at: true
    _source_dimension: true
```

Valid rows (those whose dimension keys resolve) are written to the fact table. Orphan rows are written to the quarantine output (`fact_orders_orphans`) with metadata columns explaining which dimension lookup failed. The pipeline completes with exit code 0.

This is the strategy verified in this tutorial: 7 source rows → 5 valid fact rows + 2 quarantined orphans.

---

## 3. Step-by-Step YAML

The complete pipeline below builds three dimension tables (`dim_customer`, `dim_product`, `dim_date`) and one fact table (`fact_orders`) using the Fact Pattern with `orphan_handling: quarantine`.

Save this as `docs/tutorials/orphan_handling/pipeline.yaml`:

```yaml
project: orphan_handling_tutorial
engine: pandas

connections:
  raw:
    type: local
    base_path: ./data
  warehouse:
    type: local
    base_path: ./output/warehouse
  quarantine_store:
    type: local
    base_path: ./output/quarantine

story:
  connection: warehouse
  path: stories/

system:
  connection: warehouse
  path: _odibi_system

pipelines:
  - pipeline: build_fact_with_orphans
    layer: silver
    nodes:
      - name: dim_customer
        read:
          connection: raw
          format: csv
          path: dim_customer.csv
        transformer: dimension
        params:
          natural_key: customer_id
          surrogate_key: customer_sk
          scd_type: 1
          track_cols: [customer_name, region]
          unknown_member: true
        write:
          connection: warehouse
          format: parquet
          path: dim_customer
          mode: overwrite

      - name: dim_product
        read:
          connection: raw
          format: csv
          path: dim_product.csv
        transformer: dimension
        params:
          natural_key: product_id
          surrogate_key: product_sk
          scd_type: 1
          track_cols: [product_name, category]
          unknown_member: true
        write:
          connection: warehouse
          format: parquet
          path: dim_product
          mode: overwrite

      - name: dim_date
        read:
          connection: raw
          format: csv
          path: dim_date.csv
        transformer: dimension
        params:
          natural_key: date_id
          surrogate_key: date_sk
          scd_type: 1
          track_cols: [full_date, month, year]
          unknown_member: true
        write:
          connection: warehouse
          format: parquet
          path: dim_date
          mode: overwrite

      - name: fact_orders
        depends_on: [dim_customer, dim_product, dim_date]
        read:
          connection: raw
          format: csv
          path: fact_orders.csv
        transformer: fact
        params:
          grain: [order_id]
          dimensions:
            - source_column: customer_id
              dimension_table: dim_customer
              dimension_key: customer_id
              surrogate_key: customer_sk
            - source_column: product_id
              dimension_table: dim_product
              dimension_key: product_id
              surrogate_key: product_sk
          orphan_handling: quarantine
          quarantine:
            connection: quarantine_store
            path: fact_orders_orphans
            add_columns:
              _rejection_reason: true
              _rejected_at: true
              _source_dimension: true
          measures:
            - quantity
            - amount
        write:
          connection: warehouse
          format: parquet
          path: fact_orders
          mode: overwrite
```

### How it works

1. **Dimension nodes** (`dim_customer`, `dim_product`, `dim_date`) read CSVs from the `raw` connection (`./data`) and write to the `warehouse` connection (`./output/warehouse`). Each uses `unknown_member: true` so the dimension includes a catch-all unknown row.
2. **Fact node** (`fact_orders`) reads `fact_orders.csv`, looks up each dimension by foreign key, and writes valid rows to `fact_orders` in the warehouse. Orphan rows are written to `fact_orders_orphans` with metadata columns, resolved against the `quarantine_store` connection's `base_path` (`./output/quarantine`), so the file lands at `./output/quarantine/fact_orders_orphans`.
3. **`depends_on: [dim_customer, dim_product, dim_date]`** ensures dimensions are built before the fact table, so their outputs are available for lookup.

### Important: where to run from

All relative paths (`./data`, `./output/warehouse`, `./output/quarantine`) resolve against your **current working directory**, not the YAML file location. You must run `odibi validate` and `odibi run` from `docs/tutorials/orphan_handling/`:

```bash
cd docs/tutorials/orphan_handling
odibi validate pipeline.yaml
odibi run pipeline.yaml
```

Running from the repository root would resolve `./data` to an absent `data/` folder at the repo root and fail.

---

## 4. Quarantine Deep Dive

When `orphan_handling: quarantine` is set, the Fact Pattern splits the fact rows into two groups:

- **Valid rows** — every dimension key resolved successfully. Written to the fact table (`fact_orders`) with `mode: overwrite`.
- **Orphan rows** — at least one dimension key did not resolve. Written to the quarantine output (`fact_orders_orphans`) with metadata columns added.

### Quarantine output

The quarantine path `fact_orders_orphans` has no file extension, so odibi writes it as Parquet to `./output/quarantine/fact_orders_orphans`, resolved from the `quarantine_store` connection's `base_path` (`./output/quarantine`). The output includes all original fact columns plus the metadata columns from `quarantine.add_columns`:

| Column | Description |
|---|---|
| `_rejection_reason` | Which dimension lookup failed (e.g., "customer", "product") |
| `_rejected_at` | UTC timestamp when the row was quarantined |
| `_source_dimension` | The dimension table name that caused the rejection |

With the sample data, 7 source rows produce 5 valid fact rows and 2 quarantined orphans:

- Order 6: `customer_id=999` — not in `dim_customer` → quarantined
- Order 7: `product_id=999` — not in `dim_product` → quarantined

---

## 5. Reviewing Quarantined Data

After a run, inspect the quarantine output to decide how to handle orphans:

```python
import pandas as pd

# Read the quarantine output
quarantined = pd.read_parquet("./output/quarantine/fact_orders_orphans")
print(quarantined)
```

Typical next steps:

1. **Fix upstream data** — add the missing dimension members (customer 999, product 999) to the source CSVs, then re-run.
2. **Correct the fact data** — change the orphan fact rows to reference valid dimension keys, then re-run.
3. **Accept the orphans** — if the orphans are legitimate, switch to `orphan_handling: unknown` so they pass through with surrogate key 0.

### Re-running the tutorial

The quarantine output path `fact_orders_orphans` has no extension, so odibi's quarantine writer uses **append mode**: each re-run appends the orphan rows to the existing file (2 → 4 → 6 …). The fact table uses `mode: overwrite`, so it is idempotent.

To get the expected `7 → 5 + 2` count on a re-run, clear the quarantine output first:

```bash
rm ./output/quarantine/fact_orders_orphans
```

or delete `./output/quarantine/fact_orders_orphans` before re-running.

---

## 6. Sample Data

The tutorial uses four sample CSV files committed under `docs/tutorials/orphan_handling/data/`:

| File | Rows | Purpose |
|---|---|---|
| `dim_customer.csv` | 5 | Customer dimension (IDs 1–5) |
| `dim_product.csv` | 5 | Product dimension (IDs 101–105) |
| `dim_date.csv` | 5 | Date dimension (IDs 1–5) |
| `fact_orders.csv` | 7 | Fact table with 2 orphan rows |

`fact_orders.csv` contains two deliberate orphans:

- Order 6: `customer_id=999` (not in `dim_customer`)
- Order 7: `product_id=999` (not in `dim_product`)

These two rows exercise the quarantine path when you run the pipeline with `orphan_handling: quarantine`.

---

## Related

- [Quarantine & Orphan Handling Tutorial](quarantine_workflow.md) — covers validation-engine quarantine (`on_fail: quarantine`) for GitHub issue #222. This tutorial covers the complementary Fact Pattern `orphan_handling` mechanism.
