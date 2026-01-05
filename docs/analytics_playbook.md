# Odibi Analytics Playbook

**Author:** Henry Odibi  
**Last Updated:** January 2026  
**Purpose:** Standards and conventions for data pipelines

---

## 1. Project Structure

```
analytics-project/
├── configs/
│   ├── env/
│   │   ├── dev.yaml          # Pandas, local paths
│   │   └── prod.yaml         # Spark, production storage
│   └── connections.yaml      # Secrets via env vars
├── domains/
│   ├── sales/
│   │   ├── pipelines/
│   │   │   ├── 01_raw/
│   │   │   ├── 02_staging/
│   │   │   └── 03_marts/
│   │   ├── udfs/             # Python helpers
│   │   ├── tests/
│   │   └── docs/
│   └── logistics/
│       └── ...
├── data_stories/             # Audit reports by env/date
├── quarantine/               # Bad data by env/domain/table
└── docs/
    ├── analytics_playbook.md
    └── glossary.md
```

**Key conventions:**
- One domain folder per business area (sales, logistics, finance)
- Layers: `01_raw` → `02_staging` → `03_marts`
- One major business concept per YAML file

---

## 2. Naming Conventions

### Files
| Layer | Pattern | Example |
|-------|---------|---------|
| Raw | `r_<source>_<entity>.yaml` | `r_erp_orders.yaml` |
| Staging | `stg_<entity>.yaml` | `stg_orders.yaml` |
| Dimension | `dim_<entity>.yaml` | `dim_customer.yaml` |
| Fact | `fact_<process>.yaml` | `fact_sales.yaml` |

### Pipelines & Nodes
- Pipeline: `<verb>_<object>_<frequency>` → `build_sales_marts_daily`
- Node: Match the table name → `dim_customer`, `fact_sales`

### Required Metadata
```yaml
pipelines:
  - pipeline: build_sales_marts
    description: "Daily sales dimension and fact tables"
    owner: "data_engineering"
    schedule: "0 6 * * *"
    nodes:
      - name: dim_customer
        description: "Type 1 customer dimension"
```

---

## 3. Alerting Strategy

### Channels (Teams)
| Channel | Events | Who Monitors |
|---------|--------|--------------|
| `#data-critical` | `on_failure`, `on_gate_block` | You (immediate) |
| `#data-quality` | `on_quarantine`, `on_threshold_breach` | You (daily review) |
| `#data-runs` | `on_success` | Audit trail only |

### Configuration
```yaml
alerts:
  - type: teams
    url: "${TEAMS_CRITICAL_WEBHOOK}"
    on_events: [on_failure, on_gate_block]
    metadata:
      throttle_minutes: 15
      
  - type: teams
    url: "${TEAMS_QUALITY_WEBHOOK}"
    on_events: [on_quarantine, on_threshold_breach]
    metadata:
      throttle_minutes: 30
```

### Rules
- No alerts for dev environment
- Quarantine < 0.1% → log only, no alert
- Alert only after final retry failure

---

## 4. Environment Management

### Dev vs Prod
| Aspect | Dev | Prod |
|--------|-----|------|
| Engine | Pandas | Spark |
| Data | Sample/subset | Full |
| Storage | Local filesystem | Delta Lake |
| Alerts | Off | On |

### Promotion Workflow
1. Develop in feature branch
2. Run locally: `odibi run <yaml> --config configs/env/dev.yaml`
3. Review Data Story
4. PR → self-review with checklist
5. Merge to main = ready for prod

---

## 5. Data Quality & Quarantine

### Standard Rules (all pipelines)
- **Primary keys:** Not null, unique
- **Foreign keys:** Must resolve; orphans → "unknown" surrogate + log
- **Critical fields:** Not null, valid ranges

### Example Config
```yaml
validation:
  primary_key:
    columns: [order_id, line_item_id]
    on_violation: quarantine
  foreign_keys:
    - column: customer_id
      dimension_table: dim_customer
      on_missing: "unknown"
  rules:
    - name: non_negative_quantity
      condition: "quantity >= 0"
      on_violation: quarantine
```

### Quarantine Management
- Location: `quarantine/<env>/<domain>/<table>/<yyyy-mm-dd>.parquet`
- Weekly review: 1-2 hours blocked
- Retention: **90 days** then delete
- Track decisions in `docs/quarantine_log.md`

---

## 6. Retention Policies

| Data Type | Retention |
|-----------|-----------|
| Raw zone | 180 days |
| Staging tables | 90 days |
| Marts (dim/fact) | 2-3 years |
| Data Stories | 90 days |
| Quarantine | 90 days |
| Logs | 30 days |

---

## 7. Documentation Standards

### Per Domain README
- Purpose of domain
- Key pipelines (links to YAML)
- Main tables with:
  - 1-2 line description
  - Primary key / grain
  - Upstream sources

### Per Table Spec
```markdown
### fact_orders
- **Grain:** order_id + line_item_id
- **Used by:** Sales dashboard, margin report
- **SLA:** Available by 08:00

| Column | Type | Description |
|--------|------|-------------|
| order_id | string | Business order ID |
| customer_sk | int | FK to dim_customer |
| quantity | int | Units ordered (>=0) |
```

### Rule
If you add a new dim/fact, update:
1. YAML description fields
2. Domain README
3. Glossary (if new term)

Keep each update < 15 minutes.

---

## 8. Stakeholder Communication

### Cadences
- **Monthly:** Data Release Notes in Teams
  - New tables/dashboards
  - Changed logic
  - Known issues
- **Incidents:** Within 1 hour
  - What happened (plain language)
  - Impacted reports
  - ETA for fix

### Use Data Stories
- Share screenshots for audits/disputes
- Shows row counts, filters, orphan handling
- Builds credibility without technical deep-dives

---

## 9. Testing Checklist

### Before Promoting to Prod
- [ ] YAML has `description`, `owner`, `schedule`
- [ ] Dev run succeeded
- [ ] Data Story reviewed
- [ ] Key metrics validated against known numbers
- [ ] Documentation updated
- [ ] Data quality rules defined

### Testing Levels
1. **Unit tests** for Python helpers in `udfs/`
2. **Dev runs** with schema/rule validation
3. **Spot checks** against known metrics
4. **Backfill tests** on limited historical window

---

## 10. Operational Best Practices

### Scheduling
- One standard start time (e.g., 06:00 daily)
- Document SLA per pipeline
- Define retry behavior (e.g., 2x with 10 min delay)

### Idempotency
- Pipelines safe to rerun for same date
- Use merge/upsert or partition overwrite
- Include `run_date` partition

### Backfills
- Dev first for subset of dates
- Prod in small batches (month by month)
- Monitor Data Stories per batch

### Versioning
- All changes through Git
- Tag major data model changes
- Pin Odibi/Pandas/Spark versions in `pyproject.toml`

---

## Quick Reference

```bash
# Dev run
odibi run domains/sales/pipelines/03_marts/fact_sales.yaml --config configs/env/dev.yaml

# Prod run
odibi run domains/sales/pipelines/03_marts/fact_sales.yaml --config configs/env/prod.yaml
```

---

*Last reviewed: January 2026*
