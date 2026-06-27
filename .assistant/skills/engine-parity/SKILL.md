---
name: engine-parity
description: "Use when choosing or switching the Odibi engine (pandas/spark/polars) — what works on which engine, the SQL-first parity rule, and the Polars gaps that raise errors. Points to the parity table doc."
requires: [odibi]
---

# engine-parity Skill

Odibi runs the same YAML on three engines. **They are NOT equally complete.** Pick the
engine before authoring, because some transformers/patterns simply don't exist on Polars
and will raise at runtime.

Set the engine once, at project top level:
```yaml
engine: pandas        # pandas | spark | polars  (default: pandas)
```

## When to Load This Skill

- Choosing an engine for a new pipeline, or switching one.
- A node fails with `ValueError`/`NotImplementedError` mentioning an engine.
- You used a transformer/pattern on Polars and got a runtime error.

## TL;DR — Engine Status

| Engine | Status | Use when |
|---|---|---|
| **pandas** | **Fully implemented + tested** (DuckDB SQL backend) | Default. Files up to ~1GB, dev, anything correctness-critical. |
| **spark** | Implemented (all code paths exist), unit tests run on Pandas only | Big data / Databricks production. Code is correct Spark API but lightly tested — validate outputs. |
| **polars** | **Partial.** SQL transformers work; many native transformers + **all patterns** raise errors | Fast single-node CSV/Parquet wrangling with SQL-shaped transforms only. |

Authoritative, regenerated-from-source detail: read `docs/reference/ENGINE_PARITY.md`
(legend ✅ tested / ⚠️ implemented-untested / ❌ missing). `docs/reference/PARITY_TABLE.md`
is the higher-level feature view but reads more optimistic than ENGINE_PARITY — trust
ENGINE_PARITY for the per-transformer truth.

## The Parity Rule (why SQL-first wins)

SQL-based transformers dispatch through `context.sql()` (DuckDB for Pandas, Spark SQL,
Polars SQL) and get **automatic cross-engine parity**. Native-API transformers must be
ported per engine — and several aren't ported to Polars.

- **Works everywhere (SQL-based):** `filter_rows`, `derive_columns`, `cast_columns`,
  `clean_text`, `select_columns`, `drop_columns`, `rename_columns`, `sort`, `limit`,
  `distinct`, `fill_nulls`, `case_when`, date functions, `concat_columns`, `coalesce_columns`,
  `aggregate`, `deduplicate`, `regex_replace`, `window_calculation`.
- **Prefer plain `transform: { steps: [{ sql: ... }] }`** when you want guaranteed parity.

## Polars Gaps — These Raise on Polars (❌)

If `engine: polars`, these raise `ValueError` (tracked in issue #212). Switch to Pandas/Spark
or rework with SQL:

- Transformers: `pivot`, `unpivot`, `explode_list_column`, `dict_based_mapping`,
  `unpack_struct`, `hash_columns`, `normalize_json`, `sessionize`,
  `split_events_by_period`, `scd2`, `merge`, `detect_deletes`.
- **All 6 patterns** (`dimension`, `fact`, `scd2`, `merge`, `aggregation`, `date_dimension`)
  have **no Polars path**. Building a star schema → use **pandas** (or spark).
- Validation: 8/9 tests work on Polars; `custom_sql` is partial (logs a warning + skips).

Cross-engine working transformers on Polars: domain ones (`unit_convert`,
`fluid_properties`, `psychrometrics`, `detect_sequential_phases`) have explicit Polars paths.

## Choosing an Engine — Decision

```
Building dimensions/facts/SCD2/merge (patterns)?
  └── pandas (default) or spark.  NOT polars.

Big data / running on Databricks?
  └── spark.  Validate outputs — Spark paths are lightly unit-tested.

Single-node, large-ish file, only SQL-shaped transforms?
  └── polars is fine — but verify no ❌ transformer is in the node.

Anything else / not sure?
  └── pandas. It is the fully-tested reference engine.
```

## Cross-Engine Caveats (when comparing outputs)

- Column **order** may differ (sort before comparing).
- Int width / NaN-vs-None / string-vs-object dtypes differ — compare with `check_dtype=False`.
- Polars datetimes are timezone-aware (UTC); use UTC consistently to avoid offsets.

## Workflow

1. Decide engine from the table above; set `engine:` at project top level.
2. For each node, confirm its transformers/patterns aren't in the Polars ❌ list.
3. Use `explain <transformer>` / `list_transformers` to confirm support before authoring.
4. On Spark, run `test_pipeline` and inspect outputs (`node_sample`) — paths are under-tested.
