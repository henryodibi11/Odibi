# The Odibi Playbook

**Find your problem. Get the solution.**

---

## Most Common Flows

| I need to... | Go here |
|--------------|---------|
| **Start from zero** | [Golden Path](../golden_path.md) |
| **Copy a working config** | [Canonical Examples](../examples/canonical/README.md) |
| **Load only new rows** | [Incremental Stateful](../patterns/incremental_stateful.md) |
| **Track dimension history** | [SCD2 Pattern](../patterns/scd2.md) |
| **Validate my data** | [Quality Gates](../features/quality_gates.md) |

---

## If You Only Read 3 Pages...

1. **[Golden Path](../golden_path.md)** — Zero to running in 10 minutes
2. **[Patterns Overview](../patterns/README.md)** — Common solutions to common problems
3. **[YAML Schema](../reference/yaml_schema.md)** — All configuration options

---

## Find Your Problem

### Bronze Layer: Ingestion

*"Get data from sources into your lakehouse reliably."*

| Problem | Pattern | Docs |
|---------|---------|------|
| Load all files from a folder | Append-only | [Pattern](../patterns/append_only_raw.md) |
| Only process new files since last run | Rolling window | [Pattern](../patterns/incremental_stateful.md) |
| Track exact high-water mark | Stateful HWM | [Pattern](../patterns/incremental_stateful.md) |
| Fail if source is empty or stale | Contracts | [YAML](../reference/yaml_schema.md#contractconfig) |
| Handle malformed records | Bad records path | [YAML](../reference/yaml_schema.md#readconfig) |
| Extract from SQL Server | JDBC read | [Example](../examples/canonical/02_incremental_sql.md) |

### Silver Layer: Transformation

*"Clean, deduplicate, and model your data."*

| Problem | Pattern | Docs |
|---------|---------|------|
| Remove duplicates | Deduplicate transformer | [YAML](../reference/yaml_schema.md#deduplicateparams) |
| Keep latest record per key | Dedupe with ordering | [YAML](../reference/yaml_schema.md#deduplicateparams) |
| Track dimension changes over time | SCD2 | [Pattern](../patterns/scd2.md) |
| Upsert into target table | Merge | [Pattern](../patterns/merge_upsert.md) |
| Validate output data quality | Validation tests | [Feature](../features/quality_gates.md) |
| Route bad rows for review | Quarantine | [Feature](../features/quarantine.md) |

### Gold Layer: Analytics

*"Build fact tables, aggregations, and semantic layers."*

| Problem | Pattern | Docs |
|---------|---------|------|
| Build fact table with SK lookups | Fact pattern | [Pattern](../patterns/fact.md) |
| Handle orphan records | Orphan handling | [Pattern](../patterns/fact.md#orphan-handling) |
| Pre-aggregate metrics | Aggregation pattern | [Pattern](../patterns/aggregation.md) |
| Generate date dimension | Date dimension | [Pattern](../patterns/date_dimension.md) |

---

## Decision Trees

### Choose Your Engine

```
Data size?
├─► < 1GB → engine: pandas
├─► 1-10GB → engine: polars
└─► > 10GB or Delta Lake → engine: spark
```

### Choose Your Incremental Mode

```
Source has timestamps?
├─► Yes → mode: stateful (exact HWM tracking)
└─► No
    └─► Data arrives daily? → mode: rolling_window (lookback)
    └─► Unknown pattern? → write.skip_if_unchanged: true
```

### Choose Your Validation Approach

```
When to check?
├─► Before processing (source quality) → contracts:
└─► After processing (output quality) → validation.tests:
    └─► Need to stop pipeline? → gate.on_failure: fail
    └─► Soft warning OK? → gate.on_failure: warn
```

### Choose Your SCD Type

```
Need historical state?
├─► No → scd_type: 1 (overwrite)
└─► Yes → scd_type: 2 (versioned)
    └─► Storage concerns? → Consider snapshots instead
```

---

## Quick Links by Role

### Data Engineer (Daily Work)

- [CLI Master Guide](../guides/cli_master_guide.md) — Run, debug, diagnose
- [Cheatsheet](../reference/cheatsheet.md) — Quick syntax reference
- [Troubleshooting](../troubleshooting.md) — Common errors and fixes

### Data Engineer (Building Pipelines)

- [Canonical Examples](../examples/canonical/README.md) — Copy-paste configs
- [Patterns Overview](../patterns/README.md) — Standard solutions
- [Writing Transformations](../guides/writing_transformations.md) — Custom logic

### Data Engineer (Production)

- [Production Deployment](../guides/production_deployment.md) — Going to prod
- [Alerting](../features/alerting.md) — Slack/email notifications
- [Performance Tuning](../guides/performance_tuning.md) — Optimize speed

---

## CLI Quick Reference

| Task | Command |
|------|---------|
| Run pipeline | `odibi run config.yaml` |
| Run specific node | `odibi run config.yaml --node name` |
| Dry run (no writes) | `odibi run config.yaml --dry-run` |
| Validate config | `odibi validate config.yaml` |
| View DAG | `odibi graph config.yaml` |
| Check state | `odibi catalog state config.yaml` |
| Diagnose issues | `odibi doctor config.yaml` |
| List stories | `odibi story list` |

---

## All 40+ Transformers

| Category | Transformers |
|----------|--------------|
| **Filtering** | `filter_rows`, `distinct`, `sample`, `limit` |
| **Columns** | `derive_columns`, `select_columns`, `drop_columns`, `rename_columns`, `cast_columns` |
| **Text** | `clean_text`, `trim_whitespace`, `regex_replace`, `split_part`, `concat_columns` |
| **Dates** | `extract_date_parts`, `date_add`, `date_trunc`, `date_diff`, `convert_timezone` |
| **Nulls** | `fill_nulls`, `coalesce_columns` |
| **Relational** | `join`, `union`, `aggregate`, `pivot`, `unpivot` |
| **Window** | `window_calculation` (rank, sum, lag, lead) |
| **JSON** | `parse_json`, `normalize_json`, `explode_list_column`, `unpack_struct` |
| **Keys** | `generate_surrogate_key`, `hash_columns` |
| **Patterns** | `scd2`, `merge`, `deduplicate`, `dimension`, `fact`, `aggregation` |

**Full reference:** [YAML Schema - Transformers](../reference/yaml_schema.md#transformer-catalog)

---

## See Also

- [Golden Path](../golden_path.md) — Start here if new
- [Canonical Examples](../examples/canonical/README.md) — Working configs
- [YAML Schema](../reference/yaml_schema.md) — Complete reference
