# Date Dimension Pattern — Full Feature Test

> **Campaign:** Task 20 (Phase 5: Pattern Stress Testing)
> **Pattern Source:** [odibi/patterns/date_dimension.py](../../odibi/patterns/date_dimension.py)
> **Coverage Note:** Pandas paths are exercised at 57% via unit tests; this script
> validates the **Spark path** end-to-end against a real Databricks cluster and proves
> Pandas/Spark engine parity, plus exhaustively tests fiscal-year configurations.

## Scenarios

| # | Scenario | Asserts |
|---|----------|---------|
| 1 | Pandas — all 19 columns populated for full 2024 (366 days) | Spot-check 2024-03-15 = Friday, Q1, March, sk=20240315 |
| 2 | Pandas vs Spark parity (90 days, 2024-01-01..2024-03-31) | Every column matches row-for-row |
| 3 | Fiscal year start months 1, 4, 7, 10 (8 boundary cases) | Calendar→FY conversion, FQ rollover at month boundary |
| 4 | Unknown member row (Pandas) — `unknown_member: true` | sk=0, day_of_week='Unknown', date=1900-01-01 |
| 4b | Unknown member row (Spark) | sk=0 row prepended; sorted output starts at 0 |
| 5 | Single-day range | start_date == end_date → 1 row |
| 6 | Leap-year boundary 2024-02-29 vs non-leap 2023 | Feb 29 present in 2024, absent in 2023; day_of_year=60 |
| 7 | 10-year range performance (2015–2024 = 3653 days) | Generates in <30s, exact day count |

## Run It

```powershell
& "C:\Users\HOdibi\Repos\.venv\Scripts\python.exe" examples\date_dimension_test\full_test.py
```

Requires Databricks Connect (cluster `1121-215743-ak1cop0m`, DBR 17.3 LTS, Spark 4.0.0).

## Result on April 30, 2026

```
All scenarios passed (Pandas + Spark validated).
  10-year range: 3653 rows in 0.19s.
```

### Fiscal Year Cases Verified

| Fiscal Start | Sample Date | Expected FY | Expected FQ | Result |
|:---:|:---:|:---:|:---:|:---:|
| 1 (Jan) | 2024-03-15 | 2024 | 1 | ✅ |
| 4 (Apr) | 2024-03-15 | 2024 | 4 | ✅ |
| 4 (Apr) | 2024-04-01 | 2025 | 1 | ✅ |
| 4 (Apr) | 2024-12-31 | 2025 | 3 | ✅ |
| 7 (Jul) | 2024-06-30 | 2024 | 4 | ✅ |
| 7 (Jul) | 2024-07-01 | 2025 | 1 | ✅ |
| 10 (Oct) | 2024-09-30 | 2024 | 4 | ✅ |
| 10 (Oct) | 2024-10-01 | 2025 | 1 | ✅ |

## Why This Lives Outside `tests/unit/`

The script requires a real Spark Connect session, so it is preserved here as a
manually re-runnable parity check whenever `date_dimension.py` changes.
