# Task 21 — End-to-End Star Schema Build

End-to-end integration test for the odibi pattern stack against real Delta
tables in Unity Catalog. Builds a date-conformed star schema
(`dim_date`, `dim_customer`, `dim_product`, `fact_orders`) from
programmatically generated source data, then verifies row counts,
surrogate-key sequencing, FK integrity, SCD2 history, unknown-member
handling, derived-measure consistency, and audit columns.

## Files

| File | Purpose |
|------|---------|
| `config.yaml` | Declarative ProjectConfig-validated description of the pipeline (4 nodes, 3 dimensions + 1 fact). Validates against `odibi.config.ProjectConfig`. |
| `star_schema_e2e.py` | Executable integration test (Spark / Databricks Connect). Generates source data, drives the patterns, verifies, cleans up. |

## Run

```powershell
& "C:\Users\HOdibi\Repos\.venv\Scripts\python.exe" `
    examples\star_schema_e2e\star_schema_e2e.py
```

Requires Databricks Connect 17.3+ with the cluster online (`1121-215743-ak1cop0m`,
DBR 17.3 LTS, Spark 4.0.0). Sandbox catalog: `eaai_dev.hardening_scratch`.

## What it does

1. Connects via Databricks Connect and resets the sandbox schema.
2. Generates source data:
   - 100 customers with mixed segments / countries
   - 50 products with random prices / categories
   - 10,000 orders + 25 deliberate orphan rows (unknown customer + product)
3. Writes raw source data to `src_customer`, `src_product`, `src_orders`.
4. Builds dimensions in dependency order:
   - `dim_date` via `DateDimensionPattern` (`unknown_member: true`)
   - `dim_customer` via `DimensionPattern` SCD1 (`unknown_member: true`)
   - `dim_product` via direct `scd2` transformer (see Bug Notes); first load
     of 50 products, then a second load mutating prices on 10 products to
     produce SCD2 history.
5. Builds `fact_orders` via `FactPattern` with FK lookups to dim_customer
   and dim_product, `orphan_handling=unknown`, `grain=[order_id]`,
   `deduplicate=true`, and a calculated measure
   `extended_amount = quantity * unit_price`.
6. Verifies via direct SQL against the Unity Catalog tables (see "What it
   verifies" below).
7. Drops every table created.

## What it verifies

| Check | How |
|-------|-----|
| Row counts | `dim_date=367`, `dim_customer=101`, `fact_orders=10025`, `dim_product>=51` |
| Surrogate keys unique | `len(SKs) == len(set(SKs))` per dim |
| Surrogate keys include `0` | unknown-member row present in every dim |
| FK integrity | `LEFT JOIN ... WHERE dim.sk IS NULL` returns 0 for every fact FK |
| Unknown-member usage | 25 fact rows hit `customer_sk=0 OR product_sk=0` |
| SCD2 history | ≥5 product_ids with multiple versions in dim_product |
| `is_current` invariant | exactly one `is_current=true` row per product_id |
| Calculated measure | `extended_amount == quantity * unit_price` for all rows |
| Audit columns | `load_timestamp` and `source_system` non-null on every fact row |

## Bug Notes (P-009)

`DimensionPattern` with `scd_type=2` is **not usable as-is** on Spark
Connect when combined with surrogate-key generation. Two distinct issues:

1. **`_ensure_unknown_member` (line 602):** calls
   `spark.createDataFrame([row_values], cols)` with a `valid_to`
   value of `None`. Spark Connect fails with
   `[CANNOT_DETERMINE_TYPE] Some of types cannot be determined after
   inferring.` because the all-`None` column has no inferable type.
   Workaround: build the unknown-member row with explicit `cast()`
   per column (see `_inject_unknown_member_scd2` in the test).
2. **SK + history mismatch:** with the default `use_delta_merge=True`,
   the `scd2` transformer writes history via `MERGE` and returns only
   the new/changed rows. `DimensionPattern` adds `product_sk` to that
   partial DataFrame and the caller overwrites the target — destroying
   the merged history. Setting `use_delta_merge=False` triggers the
   legacy path, which then fails in `unionByName` because
   `rows_to_insert` (source schema) has no `product_sk` column while
   `final_target` (target schema) does. **Workaround:** drive the
   `scd2` transformer directly, then read the target back, assign
   sequential `product_sk` to any rows missing one, inject the unknown
   member, and overwrite.

`FactPattern` dimension lookup also has a column-aliasing bug when
`dimension_key == surrogate_key` (e.g., `date_sk` → `date_sk`). Both
columns get aliased to `_dim_date_sk`, producing `[AMBIGUOUS_REFERENCE]`
on Spark. **Workaround:** skip date-dim from the fact's `dimensions`
list (the source already carries `order_date_sk`) and verify the FK via
direct SQL.

These three issues are captured as P-009 in `docs/LESSONS_LEARNED.md`.

## Config validation

```powershell
& "C:\Users\HOdibi\Repos\.venv\Scripts\python.exe" -c `
    "from odibi.config import load_config_from_file; `
     cfg = load_config_from_file('examples/star_schema_e2e/config.yaml'); `
     print('OK', cfg.project, len(cfg.pipelines[0].nodes), 'nodes')"
```

Expected: `OK star_schema_e2e 4 nodes`.
