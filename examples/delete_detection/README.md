# Delete Detection Example

Demonstrates odibi's `detect_deletes` transformer for CDC-like behavior
without native Change Data Capture feeds.

## Quick Start

```bash
cd examples/delete_detection
odibi run config.yaml
```

## Scenario

A customer management system where records appear and disappear:

| Day | File | Customers | What Happened |
|-----|------|-----------|---------------|
| 1 | day1_customers.csv | 100 | Initial load |
| 2 | day2_customers.csv | 95 | 5 deleted (IDs 12, 27, 45, 68, 91) |
| 3 | day3_customers.csv | 100 | 2 returned (12, 45) + 3 new (101–103) |

## Directory Structure

```
delete_detection/
├── config.yaml               # Snapshot diff pipeline config
├── README.md                 # This file
└── data/
    ├── day1_customers.csv    # 100 customers (initial load)
    ├── day2_customers.csv    # 95 customers (5 deleted)
    └── day3_customers.csv    # 100 customers (2 returned + 3 new)
```

## What Gets Tested

| Feature | Verified |
|---------|----------|
| snapshot_diff mode | Day 1→2 detects 5 deleted keys |
| sql_compare mode | Simulated via Pandas left-anti join |
| Soft delete flag | `_is_deleted` column added correctly |
| Reappearing records | Day 3: IDs 12, 45 return with `_is_deleted=False` |
| Hard delete | Deleted rows physically removed |
| Safety threshold | `max_delete_percent` triggers warning/error |
| First run behavior | No previous version → skip gracefully |
| New records | Day 3: IDs 101–103 appear with `_is_deleted=False` |

## Modes

- **snapshot_diff** — Compares Delta version N vs N-1. Best for full extracts.
- **sql_compare** — LEFT ANTI JOIN against live SQL source. Best for HWM/incremental.

## Full Tutorial

See [docs/tutorials/delete_detection.md](../../docs/tutorials/delete_detection.md).

## Campaign Notebook

For Databricks E2E testing, see `campaign/11_delete_detection_e2e`.
