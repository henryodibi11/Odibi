# Odibi Glossary

Standard terminology used across Odibi documentation.

## Data Quality Terms

| Term | Definition | YAML Key |
|------|------------|----------|
| **Contracts** | Pre-transform checks that always fail on violation. Use for input data validation. | `contracts:` |
| **Validation Tests** | Post-transform row-level checks with configurable actions (fail/warn/quarantine). | `validation.tests:` |
| **Quality Gates** | Batch-level thresholds (pass rate, row counts) evaluated after validation. | `validation.gate:` |
| **Quarantine** | Routing invalid rows to a separate table for review instead of failing. | `validation.quarantine:` |

## Pipeline Terms

| Term | Definition | YAML Key |
|------|------------|----------|
| **Pipeline** | A collection of nodes that execute together as a logical unit. | `pipelines:` |
| **Node** | A single unit of work: read → transform → validate → write. | `nodes:` |
| **Transformer** | A pre-built "app" for major operations (scd2, merge, deduplicate). | `transformer:` |
| **Transform Steps** | A chain of smaller operations (SQL, functions) for custom logic. | `transform.steps:` |
| **Pattern** | A declarative dimensional modeling template (dimension, fact, aggregation). | `pattern:` |

## Dimensional Modeling Terms

| Term | Definition |
|------|------------|
| **Natural Key** | Business identifier from source system (e.g., `customer_id`). |
| **Surrogate Key** | System-generated integer key for joins (e.g., `customer_sk`). |
| **SCD Type 1** | Overwrite dimension changes (no history). |
| **SCD Type 2** | Track dimension changes with versioned rows (`is_current`, `valid_from`, `valid_to`). |
| **Grain** | The level of detail in a fact table (e.g., one row per order). |
| **Orphan** | A fact row with no matching dimension record. |

## Execution Terms

| Term | Definition |
|------|------------|
| **Story** | Execution report with lineage, metrics, and validation results. |
| **Connection** | Named data source/destination (local, Azure, Delta, SQL Server). |
| **Context** | Runtime environment holding registered DataFrames and engine state. |

## Actions on Failure

| Term | Usage Context | Behavior |
|------|---------------|----------|
| `fail` | Contracts, Validation | Stop execution immediately |
| `warn` | Validation | Log warning, continue processing |
| `quarantine` | Validation | Route bad rows to quarantine table |
| `abort` | Quality Gates | Stop pipeline, write nothing |
| `warn_and_write` | Quality Gates | Log warning, write all rows |
| `write_valid_only` | Quality Gates | Write only rows that passed |

## See Also

- [YAML Configuration Reference](yaml_schema.md) - Complete configuration options
- [Validation Overview](../validation/README.md) - Data quality framework
