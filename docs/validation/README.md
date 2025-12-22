# Data Validation

Odibi provides a comprehensive validation framework to ensure data quality at every stage of your pipeline.

## Validation Layers

| Layer | When it Runs | Purpose |
|-------|--------------|---------|
| **Contracts** | Before transform | Fail-fast checks on input data |
| **Validation Tests** | After transform | Row-level data quality checks |
| **Quality Gates** | After validation | Batch-level thresholds and pass rates |
| **FK Validation** | Post-pipeline | Referential integrity between tables |

## Quick Links

### Core Documentation

- [Quality Gates](../features/quality_gates.md) - Batch-level validation with pass rates and row count checks
- [FK Validation](fk.md) - Foreign key validation between fact and dimension tables
- [Quarantine](../features/quarantine.md) - Capture and review invalid records

### Configuration Reference

- [Contracts Reference](../reference/yaml_schema.md#contracts-data-quality-gates) - Pre-transform fail-fast checks
- [Validation Tests Reference](../reference/yaml_schema.md#validationconfig) - Row-level tests (not_null, unique, range, etc.)

## Choosing the Right Validation

```
┌─────────────────────────────────────────────────────────────┐
│                    INPUT DATA                               │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  CONTRACTS (Pre-Transform)                                  │
│  • not_null on required columns                             │
│  • row_count min/max                                        │
│  • freshness checks                                         │
│  → ALWAYS FAILS on violation                                │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  TRANSFORMATION                                             │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  VALIDATION TESTS (Post-Transform)                          │
│  • Range checks, format validation                          │
│  • Custom SQL conditions                                    │
│  → Can WARN, QUARANTINE, or FAIL                            │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  QUALITY GATES (Batch-Level)                                │
│  • Pass rate thresholds (e.g., 95%)                         │
│  • Row count anomaly detection                              │
│  → Can ABORT, WARN, or WRITE_VALID_ONLY                     │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    OUTPUT DATA                              │
└─────────────────────────────────────────────────────────────┘
```

## Example: Complete Validation Setup

```yaml
nodes:
  - name: process_orders
    read:
      connection: staging
      path: orders

    # 1. Contracts - Fail fast on bad input
    contracts:
      - type: not_null
        columns: [order_id, customer_id]
      - type: row_count
        min: 100

    # 2. Transformation
    transform:
      steps:
        - sql: "SELECT * FROM df WHERE amount > 0"

    # 3. Validation Tests - Check output quality
    validation:
      tests:
        - type: range
          column: amount
          min: 0
          max: 1000000
        - type: unique
          columns: [order_id]
      on_fail: quarantine  # Route bad rows to quarantine

      # 4. Quality Gate - Batch-level threshold
      gate:
        require_pass_rate: 0.95
        on_fail: abort

    write:
      connection: warehouse
      path: fact_orders
```

## See Also

- [Getting Started: Validation](../tutorials/getting_started.md#7-add-data-validation) - Tutorial walkthrough
- [Fact Pattern](../patterns/fact.md) - Orphan handling in fact tables
