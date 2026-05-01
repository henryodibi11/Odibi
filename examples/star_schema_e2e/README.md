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

## Bug Notes (P-009) — ✅ RESOLVED

All three Spark Connect bugs documented as P-009 are now fixed (Task 26a):

1. **`_ensure_unknown_member`** — replaced `spark.createDataFrame()` with
   `spark.range(1).select(F.lit().cast(dtype).alias())` using `df.schema.fields`.
2. **SCD2 SK + history mismatch** — after `scd2()` on Spark, re-read the target
   to get complete history before surrogate-key assignment.
3. **FactPattern `dim_key == sk_col`** — project the column once when
   `dim_key == sk_col` to avoid `AMBIGUOUS_REFERENCE`.

All workarounds removed from this test. The test now uses standard
`DimensionPattern` for dim_product SCD2 and includes dim_date in `FactPattern`.

## Config validation

```powershell
& "C:\Users\HOdibi\Repos\.venv\Scripts\python.exe" -c `
    "from odibi.config import load_config_from_file; `
     cfg = load_config_from_file('examples/star_schema_e2e/config.yaml'); `
     print('OK', cfg.project, len(cfg.pipelines[0].nodes), 'nodes')"
```

Expected: `OK star_schema_e2e 4 nodes`.
