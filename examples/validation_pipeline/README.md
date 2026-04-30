# Validation Pipeline Example

A complete Bronze → Silver pipeline demonstrating odibi's data quality system.

## Quick Start

```bash
# From the examples/validation_pipeline/ directory:
odibi run config.yaml
```

## Structure

```
validation_pipeline/
├── config.yaml              # Pipeline config with all 11 validation tests
├── data/
│   ├── good_data.csv        # 20 clean rows (passes all tests)
│   ├── bad_data.csv         # 20 rows with 8 known quality issues
│   └── edge_data.csv        # 3 rows with unicode names
├── expected/
│   ├── valid_output.csv     # 12 rows expected to pass quarantine
│   └── quarantine_output.csv # 8 rows expected to be quarantined
└── README.md
```

## Data Quality Issues in bad_data.csv

| Row | Issue | Test Type | Expected Result |
|-----|-------|-----------|-----------------|
| 3 | `customer_id` is NULL | `not_null` | Quarantined |
| 6 | `email` is NULL | `not_null` | Quarantined |
| 1,8 | Duplicate `customer_id=1` | `unique` | Quarantined |
| 10 | `status='deleted'` | `accepted_values` | Quarantined |
| 12 | `age=-5` | `range` | Quarantined |
| 15 | `age=200` | `range` | Quarantined |
| 17 | `email='not-an-email'` | `regex_match` | Quarantined |
| 19 | `created_at > updated_at` | `custom_sql` | Logged (fail severity) |

## Validation Tests Demonstrated

All 11 odibi test types are configured:

1. **not_null** — Required fields (customer_id, name, email)
2. **unique** — Primary key (customer_id)
3. **accepted_values** — Status enum validation
4. **range** — Age bounds [0, 150]
5. **regex_match** — Email format validation
6. **row_count** — Batch size bounds [1, 1M]
7. **freshness** — Data must be within 30 days
8. **volume_drop** — Detect sudden row count drops
9. **schema** — Column presence validation
10. **distribution** — Mean age > 18 (statistical check)
11. **custom_sql** — Business rule: created_at <= updated_at

## Running on Databricks

See `campaign/10_validation_e2e.py` for cross-engine verification
(Pandas vs Spark parity tests).

## Tutorial

See `docs/tutorials/validation_pipeline.md` for the full walkthrough.
