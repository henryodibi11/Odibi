# Quarantine Workflow Example

> **GitHub Issue:** #222
> **Tutorial:** [docs/tutorials/quarantine_workflow.md](../../docs/tutorials/quarantine_workflow.md)

## Quick Start

```bash
cd examples/quarantine_workflow
odibi run config.yaml
```

## Scenario

Building a fact table (`fact_orders`) that references a dimension (`dim_customer`).
Some orders reference non-existent customers, and some have data quality issues.

### Data

| File | Rows | Description |
|------|:---:|-------------|
| `dim_customer.csv` | 20 | Customers with IDs 1-20 |
| `fact_orders.csv` | 100 | Orders — 10 orphan FKs, 3 null amounts, 2 negative amounts, 3 invalid status |
| `dim_customer_fixed.csv` | 25 | Original 20 + 5 new customers (IDs 21-25) that fix orphan references |

### Data Quality Issues

| Issue | Rows | IDs |
|-------|:---:|-----|
| Orphan customer_id (21-25) | 10 | Orders 91-100 |
| Null amount | 3 | Orders 85-87 |
| Negative amount | 2 | Orders 88-89 |
| Invalid status ("CANCELLED") | 3 | Orders 82-84 |

## Directory Structure

```
quarantine_workflow/
├── config.yaml              # Pipeline config with validation + quarantine
├── README.md                # This file
└── data/
    ├── dim_customer.csv     # 20 customers (IDs 1-20)
    ├── fact_orders.csv      # 100 orders (10 orphans + quality issues)
    └── dim_customer_fixed.csv  # 25 customers (fixes orphans)
```

## Features Covered

| Feature | Config/Code | Description |
|---------|:-----------:|-------------|
| Quarantine split | `on_fail: quarantine` | Route failed rows to quarantine table |
| Metadata columns | `add_columns` | `_rejection_reason`, `_rejected_at`, `_source_batch_id`, `_failed_tests` |
| Quality gate | `gate` | Batch-level pass rate check (90% threshold) |
| Gate actions | `on_fail` | `abort`, `warn_and_write`, `write_valid_only` |
| FK validation | `FKValidator` | Detect orphan foreign keys |
| FK actions | `on_violation` | `error` (reject), `warn`, `quarantine` (filter) |
| Sampling | `max_rows` | Cap quarantine output for high-volume failures |
| Re-processing | Tutorial | Fix upstream, re-validate quarantined data |

## Campaign Notebook

Full E2E testing with assertions: `campaign/12_quarantine_e2e`

**Tests:**
- Quarantine split (valid/invalid)
- Quarantine metadata columns
- FK validation: reject, warn, filter
- Quality gate: abort, warn_and_write
- Quarantine sampling (max_rows)
- Re-processing after fix
- Config YAML validation
