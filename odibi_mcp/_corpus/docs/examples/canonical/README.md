# Canonical YAML Examples

Copy-paste ready configs for the 5 most common use cases.

Each example is a **complete, runnable config**—not a fragment.

## Sample Data

Download sample datasets from [sample_data/](sample_data/) or copy them:

```bash
mkdir -p data/landing
cp docs/examples/canonical/sample_data/*.csv data/landing/
```

| Example | Use Case | Key Features |
|---------|----------|--------------|
| [1. Hello World](01_hello_world.md) | Local CSV → Parquet | Minimal viable config |
| [2. Incremental SQL](02_incremental_sql.md) | Database → Raw (HWM) | Stateful high-water mark |
| [3. SCD2 Dimension](03_scd2_dimension.md) | Track history | Surrogate keys, versioning |
| [4. Fact Table](04_fact_table.md) | Star schema fact | SK lookups, grain validation |
| [5. Full Pipeline](05_full_pipeline.md) | Validation + Quarantine + Alerting | Production-ready |

---

## When to Use Each

```
I need to...
│
├─► Get started fast → Example 1 (Hello World)
├─► Load from a database → Example 2 (Incremental SQL)
├─► Build dimension with history → Example 3 (SCD2)
├─► Build fact table for BI → Example 4 (Fact Table)
└─► Production pipeline with alerts → Example 5 (Full Pipeline)
```
