# Aggregation Pattern — Real Data Stress Test

> **Campaign:** Task 19 (Phase 5: Pattern Stress Testing)
> **Pattern Source:** [odibi/patterns/aggregation.py](../../odibi/patterns/aggregation.py)
> **Coverage Note:** Pandas paths are exercised at 63%+ via unit tests; this script
> validates the **Spark path** end-to-end against a real Databricks cluster and proves
> Pandas/Spark engine parity.

## What This Verifies

The script `stress_test.py` runs **8 scenarios** through `AggregationPattern.execute()`
on **both** the Pandas (DuckDB) and Spark engines, then asserts the two engines
produced identical results (row count + every measure column).

| # | Scenario | Asserts |
|---|----------|---------|
| 1 | Simple aggregation (`SUM/AVG/COUNT/MIN/MAX` on `region`) | Sums match source, parity |
| 2 | Multi-grain `GROUP BY [order_date, category, region]` | 264 groups, parity |
| 3 | Incremental merge — `merge_strategy: replace` | Untouched keys preserved |
| 4 | Incremental merge — `merge_strategy: sum` (additive) | Outer-join sum semantics |
| 5 | Audit columns — `audit: {load_timestamp: true}` | Column present, recent |
| 6 | Large data — 100k rows → 1k groups | Completes in <60s, both engines |
| 7 | Null grain values | NULL bucket present, parity |
| 8 | Empty groups via `having` | 0 rows returned |

Scenarios 3 & 4 call `_merge_replace` / `_merge_sum` directly, since exercising
the full incremental-target round-trip would require writing/reading a Delta table
(out of scope for a 250-LOC stress test).

## Run It

```powershell
& "C:\Users\HOdibi\Repos\.venv\Scripts\python.exe" examples\aggregation_stress\stress_test.py
```

Requires Databricks Connect (already configured for cluster
`1121-215743-ak1cop0m`, DBR 17.3 LTS, Spark 4.0.0).

## Result on April 30, 2026

```
[2026-04-30T21:31:31+00:00] All 8 scenarios passed (Pandas + Spark parity verified).
  100k rows -> 1k groups in 3.4s (Pandas + Spark combined).
```

No bugs were filed — the pattern's Pandas and Spark paths produce identical
aggregates across all 8 scenarios, including:

- Null grain values (NULL becomes its own group on both engines).
- Empty result sets (`HAVING SUM(amount) < 0` returns 0 rows).
- Outer-join semantics in `_merge_sum` (NULL → 0 fallback).

## Why This Lives Outside `tests/unit/`

The stress test requires a real Spark Connect session to a Databricks cluster, so
running it in the standard `pytest tests/unit/` suite would fail on machines
without `DATABRICKS_CONFIG_PROFILE` set. It is preserved here as a manually
re-runnable parity check whenever `aggregation.py` changes.
