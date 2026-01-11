# Observability Tables

Auto-populating observability tables for leadership dashboards with zero manual effort.

## Overview

Odibi automatically populates observability tables on every pipeline run, enabling Power BI dashboards for leadership without manual intervention.

### What Leadership Gets

| Dashboard | Source Table | Auto-Updated |
|-----------|--------------|--------------|
| Platform Health | `meta_pipeline_health` | ✅ Every run |
| Cost Trends | `meta_daily_stats` | ✅ Every run |
| SLA Compliance | `meta_sla_status` | ✅ Every run |
| Failure Analysis | `meta_failures` | ✅ On failure |

### Key Features

- **Zero-touch**: Tables auto-populate on pipeline completion
- **Exactly-once**: Guard table prevents duplicate updates
- **Engine parity**: Works on Spark, Pandas/delta-rs, and SQL Server
- **Failure-resilient**: Observability errors never fail pipelines

## Table Taxonomy

Observability tables are divided into two categories:

### Fact Tables (Append-Only)

Immutable records that capture what happened. Never modified after initial write.

| Table | Purpose | Granularity |
|-------|---------|-------------|
| `meta_pipeline_runs` | Pipeline execution log | One row per pipeline execution |
| `meta_node_runs` | Node execution log | One row per node execution |
| `meta_failures` | Failure details | One row per failure event |
| `meta_observability_errors` | Observability system failures | One row per observability failure |
| `meta_derived_applied_runs` | Idempotency guard | One row per (derived_table, run_id) |

### Derived Tables (Incrementally Maintained)

Aggregated views that are upserted on each pipeline completion.

| Table | Purpose | Update Trigger |
|-------|---------|----------------|
| `meta_daily_stats` | Daily aggregates | Upsert on pipeline completion |
| `meta_pipeline_health` | Current health snapshot | Upsert on pipeline completion |
| `meta_sla_status` | Freshness compliance | Upsert on pipeline completion |

## Configuration

### Pipeline-Level Config

Enable SLA tracking by adding `owner` and `freshness_sla` to your pipeline:

```yaml
pipelines:
  - pipeline: orders_silver
    description: "Transform orders to silver layer"
    layer: silver
    owner: "data-team@company.com"      # Pipeline owner for SLA alerts
    freshness_sla: "6h"                  # Expected freshness (6 hours)
    freshness_anchor: run_completion     # Default: when pipeline last ran
    nodes:
      # ...
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `owner` | string | No | Pipeline owner (email or name). Shown in health/SLA dashboards |
| `freshness_sla` | string | No | Expected freshness: `30m`, `6h`, `1d`, `1w` |
| `freshness_anchor` | string | No | What defines freshness. Default: `run_completion` |

!!! note "Freshness SLA Required for SLA Tracking"
    The `meta_sla_status` table is only updated if `freshness_sla` is configured.

### System-Level Config

Configure cost tracking and retention in the `system` section:

```yaml
system:
  connection: catalog_storage
  path: _odibi_system
  cost_per_compute_hour: 2.50           # Estimated cost per compute hour (USD)
  retention_days:
    daily_stats: 365                    # Keep daily stats for 1 year
    failures: 90                        # Keep failure records for 90 days
    observability_errors: 90            # Keep observability errors for 90 days
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `cost_per_compute_hour` | float | None | Estimated cost per compute hour (USD) for cost tracking |
| `retention_days.daily_stats` | int | 365 | Days to retain daily stats |
| `retention_days.failures` | int | 90 | Days to retain failure records |
| `retention_days.observability_errors` | int | 90 | Days to retain observability errors |

## Schema Reference

### meta_pipeline_runs

Pipeline execution log. One row per pipeline execution.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | STRING | Primary key (UUID) |
| `pipeline_name` | STRING | Pipeline name |
| `owner` | STRING | Pipeline owner (nullable) |
| `layer` | STRING | Medallion layer (nullable) |
| `run_start_at` | TIMESTAMP | Execution start time |
| `run_end_at` | TIMESTAMP | Execution end time |
| `duration_ms` | BIGINT | Duration in milliseconds |
| `status` | STRING | `SUCCESS` or `FAILURE` |
| `nodes_total` | INT | Total nodes in pipeline |
| `nodes_succeeded` | INT | Nodes that succeeded |
| `nodes_failed` | INT | Nodes that failed |
| `nodes_skipped` | INT | Nodes that were skipped |
| `rows_processed` | BIGINT | Sum of terminal node rows (nullable) |
| `error_summary` | STRING | First 500 chars of error (nullable) |
| `terminal_nodes` | STRING | Comma-separated terminal node names (nullable) |
| `environment` | STRING | Environment tag (nullable) |
| `created_at` | TIMESTAMP | Record creation time |

### meta_node_runs

Node execution log. One row per node execution.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | STRING | FK to pipeline run |
| `node_id` | STRING | UUID for this node execution |
| `pipeline_name` | STRING | Pipeline name |
| `node_name` | STRING | Node name |
| `status` | STRING | `SUCCESS`, `FAILURE`, or `SKIPPED` |
| `run_start_at` | TIMESTAMP | Node execution start time |
| `run_end_at` | TIMESTAMP | Node execution end time |
| `duration_ms` | BIGINT | Duration in milliseconds |
| `rows_processed` | BIGINT | Rows processed (nullable) |
| `metrics_json` | STRING | Flat dict of metrics (scalars only) |
| `environment` | STRING | Environment tag (nullable) |
| `created_at` | TIMESTAMP | Record creation time |

### meta_failures

Failure details. One row per failure event.

| Column | Type | Description |
|--------|------|-------------|
| `failure_id` | STRING | Primary key (UUID) |
| `run_id` | STRING | FK to pipeline run |
| `pipeline_name` | STRING | Pipeline name |
| `node_name` | STRING | Node name |
| `error_type` | STRING | Exception class name |
| `error_message` | STRING | Error message (max 1000 chars) |
| `error_code` | STRING | Error code for taxonomy (nullable) |
| `stack_trace` | STRING | Stack trace (max 2000 chars, nullable) |
| `timestamp` | TIMESTAMP | When failure occurred |
| `date` | DATE | For partitioning |

### meta_observability_errors

Observability system failures. Self-heals by logging its own errors.

| Column | Type | Description |
|--------|------|-------------|
| `error_id` | STRING | Primary key (UUID) |
| `run_id` | STRING | Pipeline run ID (nullable) |
| `pipeline_name` | STRING | Pipeline name (nullable) |
| `component` | STRING | Component that failed (e.g., `catalog_update`, `derived_updates`) |
| `error_message` | STRING | Error message (max 500 chars) |
| `timestamp` | TIMESTAMP | When error occurred |
| `date` | DATE | For partitioning |

### meta_derived_applied_runs (Guard Table)

Idempotency guard for derived table updates. Ensures exactly-once semantics.

| Column | Type | Description |
|--------|------|-------------|
| `derived_table` | STRING | PK (with run_id): Derived table name |
| `run_id` | STRING | PK (with derived_table): Pipeline run ID |
| `claim_token` | STRING | UUID of claiming process |
| `status` | STRING | `CLAIMED`, `APPLIED`, or `FAILED` |
| `claimed_at` | TIMESTAMP | When claim was acquired |
| `applied_at` | TIMESTAMP | When update completed (nullable) |
| `error_message` | STRING | Error if failed (max 500 chars, nullable) |

### meta_daily_stats

Daily aggregates. Primary key: `(date, pipeline_name)`.

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Stats date |
| `pipeline_name` | STRING | Pipeline name |
| `runs` | BIGINT | Total runs on this day |
| `successes` | BIGINT | Successful runs |
| `failures` | BIGINT | Failed runs |
| `total_rows` | BIGINT | Total rows processed |
| `total_duration_ms` | BIGINT | Total execution time |
| `estimated_cost_usd` | DOUBLE | Estimated cost (nullable) |
| `actual_cost_usd` | DOUBLE | Actual cost from billing (nullable) |
| `cost_source` | STRING | `configured_rate`, `databricks_billing`, `none`, or `mixed` |
| `cost_is_actual` | BOOLEAN | Whether cost is from billing |

### meta_pipeline_health

Current health snapshot. Primary key: `pipeline_name`.

| Column | Type | Description |
|--------|------|-------------|
| `pipeline_name` | STRING | Pipeline name |
| `owner` | STRING | Pipeline owner (nullable) |
| `layer` | STRING | Medallion layer (nullable) |
| `total_runs` | BIGINT | Lifetime total runs |
| `total_successes` | BIGINT | Lifetime successes |
| `total_failures` | BIGINT | Lifetime failures |
| `success_rate_7d` | DOUBLE | 7-day success rate (nullable) |
| `success_rate_30d` | DOUBLE | 30-day success rate (nullable) |
| `avg_duration_ms_7d` | DOUBLE | 7-day average duration (nullable) |
| `total_rows_30d` | BIGINT | 30-day total rows (nullable) |
| `estimated_cost_30d` | DOUBLE | 30-day estimated cost (nullable) |
| `last_success_at` | TIMESTAMP | Last successful run (nullable) |
| `last_failure_at` | TIMESTAMP | Last failed run (nullable) |
| `last_run_at` | TIMESTAMP | Most recent run |
| `updated_at` | TIMESTAMP | Record update time |

### meta_sla_status

Freshness compliance. Primary key: `pipeline_name`.

| Column | Type | Description |
|--------|------|-------------|
| `pipeline_name` | STRING | Pipeline name |
| `owner` | STRING | Pipeline owner (nullable) |
| `freshness_sla` | STRING | SLA string (e.g., `6h`) |
| `freshness_anchor` | STRING | `run_completion`, `table_max_timestamp`, or `watermark_state` |
| `freshness_sla_minutes` | INT | SLA in minutes |
| `last_success_at` | TIMESTAMP | Last successful run (nullable) |
| `minutes_since_success` | INT | Minutes since last success (nullable) |
| `sla_met` | BOOLEAN | Whether SLA is currently met |
| `hours_overdue` | DOUBLE | Hours overdue if SLA breached (nullable) |
| `updated_at` | TIMESTAMP | Record update time |

## How Auto-Population Works

When a pipeline completes, the following sequence occurs:

```
Pipeline Execution
        │
        ▼
┌───────────────────────────────────────────┐
│ 1. Write Facts (append-only)              │
│    • meta_pipeline_runs                   │
│    • meta_node_runs                       │
│    • meta_failures (if any)               │
└───────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────┐
│ 2. Update Derived Tables (with guard)     │
│    For each derived table:                │
│    • Try to claim via guard table         │
│    • If claimed, update derived table     │
│    • Mark applied or failed               │
└───────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────┐
│ 3. On Any Error                           │
│    • Log to meta_observability_errors     │
│    • Continue pipeline (never fail)       │
└───────────────────────────────────────────┘
```

## Guard Semantics (Exactly-Once)

The guard table (`meta_derived_applied_runs`) ensures each derived table update happens exactly once per run.

### Status Values

| Status | Meaning | Can Reclaim? |
|--------|---------|--------------|
| `CLAIMED` | Update in progress | Yes, if stale (>60 min) |
| `APPLIED` | Update completed successfully | No (terminal) |
| `FAILED` | Update failed | Yes (always) |

### Claim Lifecycle

1. **Try Claim**: Insert `CLAIMED` row with unique token
2. **On Success**: Update to `APPLIED` (terminal state)
3. **On Failure**: Update to `FAILED` (reclaimable)
4. **Stale Claims**: `CLAIMED` entries older than 60 minutes are reclaimable

!!! warning "APPLIED is Terminal"
    Once a row reaches `APPLIED` status, it can never be reclaimed. This prevents double-counting in derived tables.

### Concurrent Safety

The guard table uses atomic operations (MERGE in Spark/SQL Server, append+verify in Pandas) to handle concurrent updates safely.

## CLI Commands

### rebuild-summaries

Recompute derived tables from fact tables. Use after failures or when derived tables become inconsistent.

```bash
# Rebuild specific pipeline since a date
odibi system rebuild-summaries config.yaml --pipeline orders_silver --since 2024-01-01

# Rebuild all pipelines since a date
odibi system rebuild-summaries config.yaml --all --since 2024-01-01

# Custom stale claim threshold (default: 60 minutes)
odibi system rebuild-summaries config.yaml --all --since 2024-01-01 --max-age-minutes 30
```

| Option | Required | Description |
|--------|----------|-------------|
| `--pipeline` | No* | Specific pipeline to rebuild |
| `--all` | No* | Rebuild all pipelines |
| `--since` | Yes | Start date (YYYY-MM-DD) |
| `--max-age-minutes` | No | Max age for stale CLAIMED entries (default: 60) |
| `--env` | No | Environment override |

*Must specify either `--pipeline` or `--all`

**When to use:**

- After system outages that left updates incomplete
- After fixing bugs in derived table logic
- When derived tables show incorrect aggregates
- To backfill historical data

### cleanup

Delete old records based on retention configuration.

```bash
# Preview what would be deleted
odibi system cleanup config.yaml --dry-run

# Actually delete old records
odibi system cleanup config.yaml

# With environment override
odibi system cleanup config.yaml --env prod
```

| Option | Required | Description |
|--------|----------|-------------|
| `--dry-run` | No | Preview without deleting |
| `--env` | No | Environment override |

**Tables affected:**

| Table | Retention | Default |
|-------|-----------|---------|
| `meta_daily_stats` | `retention_days.daily_stats` | 365 days |
| `meta_failures` | `retention_days.failures` | 90 days |
| `meta_observability_errors` | `retention_days.observability_errors` | 90 days |

## Engine Parity

All operations work across Spark, Pandas/delta-rs, and SQL Server with semantic equivalence.

| Operation | Spark | Pandas/delta-rs | SQL Server |
|-----------|-------|-----------------|------------|
| try_claim | Atomic MERGE | Append + verify | Atomic MERGE |
| mark_applied | UPDATE | Read-modify-write | UPDATE |
| mark_failed | UPDATE | Read-modify-write | UPDATE |
| daily_stats | MERGE + deltas | Groupby + overwrite | MERGE |
| pipeline_health | MERGE + window | Filter + overwrite | MERGE + CTE |
| sla_status | SQL CTE | Python datetime | DATEDIFF |

!!! note "Pandas Mode Limitations"
    Pandas/delta-rs mode uses optimistic concurrency with retries. Under very high concurrency, some operations may need multiple attempts.

## Troubleshooting

### Derived Updates Failing

**Symptoms:** `meta_derived_applied_runs` has `FAILED` entries.

**Check status:**

```sql
-- Find failed updates
SELECT derived_table, run_id, error_message, claimed_at
FROM meta_derived_applied_runs
WHERE status = 'FAILED'
ORDER BY claimed_at DESC
LIMIT 20
```

**Resolution:**

```bash
# Rebuild failed updates
odibi system rebuild-summaries config.yaml --all --since 2024-01-01
```

### Stale CLAIMED Entries

**Symptoms:** Updates stuck in `CLAIMED` status for >60 minutes.

**Cause:** Process crashed or was killed before completing.

**Resolution:**

```bash
# Rebuild with shorter stale threshold
odibi system rebuild-summaries config.yaml --all --since 2024-01-01 --max-age-minutes 30
```

### Derived Tables Out of Sync

**Symptoms:** `meta_daily_stats` doesn't match `meta_pipeline_runs` aggregates.

**Cause:** Failed updates, race conditions, or bug in derived logic.

**Resolution:**

```bash
# Full rebuild from fact tables
odibi system rebuild-summaries config.yaml --all --since 2024-01-01
```

### Guard Table Full Scan Performance

**For large guard tables**, consider partitioning by date or periodic cleanup of old `APPLIED` entries.

### Observability Errors

Check `meta_observability_errors` for internal issues:

```sql
SELECT component, error_message, COUNT(*) as count
FROM meta_observability_errors
WHERE timestamp > current_date - 7
GROUP BY component, error_message
ORDER BY count DESC
```

## Complete Example

```yaml
project: SalesAnalytics
engine: spark

system:
  connection: catalog_storage
  path: _odibi_system
  environment: prod
  cost_per_compute_hour: 2.50
  retention_days:
    daily_stats: 365
    failures: 90
    observability_errors: 90

connections:
  catalog_storage:
    type: adls
    account: "${STORAGE_ACCOUNT}"
    container: metadata

  bronze:
    type: adls
    account: "${STORAGE_ACCOUNT}"
    container: bronze

  silver:
    type: adls
    account: "${STORAGE_ACCOUNT}"
    container: silver

pipelines:
  - pipeline: orders_silver
    description: "Transform orders to silver layer"
    layer: silver
    owner: "data-team@company.com"
    freshness_sla: "6h"
    nodes:
      - name: read_orders
        type: read
        connection: bronze
        path: raw/orders
        format: delta

      - name: transform
        type: transform
        input: read_orders
        transform: |
          SELECT * FROM {input}
          WHERE order_date >= '2024-01-01'

      - name: write_orders
        type: write
        input: transform
        connection: silver
        path: orders
        format: delta
        mode: merge
        merge_keys: [order_id]
```

After running this pipeline:

- `meta_pipeline_runs`: New row with execution details
- `meta_node_runs`: 3 rows (one per node)
- `meta_daily_stats`: Upserted with today's aggregates
- `meta_pipeline_health`: Upserted with lifetime stats
- `meta_sla_status`: Upserted with freshness compliance

## Executive Dashboard Views

When using [Catalog Sync](catalog_sync.md) to replicate data to SQL Server, Odibi automatically creates pre-built views optimized for visualization tools.

### Available Views

| View | Purpose | Key Metrics |
|------|---------|-------------|
| `vw_pipeline_health_status` | RAG status per pipeline | `health_status` (RED/AMBER/GREEN), `health_reason` |
| `vw_exec_overview` | Executive summary by project | Success rates (7d/30d/90d), cost trends, reliability score |
| `vw_table_freshness` | Data staleness monitoring | `freshness_status`, `hours_since_update` |
| `vw_pipeline_sla_status` | SLA compliance dashboard | `sla_met`, `hours_overdue`, `sla_rag` |
| `vw_exec_current_issues` | What's broken now | Failed pipelines with error details, priority order |
| `vw_pipeline_risk` | Risk scoring | `risk_score`, `risk_level` (CRITICAL/HIGH/MEDIUM/LOW) |
| `vw_cost_summary` | Cost tracking | 7d/30d costs, runtime hours, cost trends |

### Health Status Logic

The `vw_pipeline_health_status` view uses this RAG logic:

| Status | Condition |
|--------|-----------|
| RED | Last run failed, success rate <90%, or no runs in 7 days |
| AMBER | Success rate <100% or no run in 48+ hours |
| GREEN | 100% success rate and recent runs |

### SLA Status Logic

The `vw_pipeline_sla_status` view combines SLA tracking with business context:

| RAG | Condition |
|-----|-----------|
| GREEN | SLA met |
| AMBER | SLA breached by ≤1 hour |
| RED | SLA breached by >1 hour or high-criticality pipeline not successful |

### Risk Scoring

The `vw_pipeline_risk` view calculates risk as:

```
risk_score = criticality_weight × (failure_rate × 100 + log10(runtime_hours) × 5)
```

Where `criticality_weight` is 3 (High), 2 (Medium), or 1 (Low).

### Business Context (dim_pipeline_context)

The `dim_pipeline_context` table is **manually populated** to add business metadata to your pipelines. This enriches the executive views with ownership and criticality information.

| Column | Type | Description |
|--------|------|-------------|
| `project` | STRING | Project name (PK) |
| `pipeline_name` | STRING | Pipeline name (PK) |
| `environment` | STRING | Environment (PK) |
| `business_criticality` | STRING | `High`, `Medium`, or `Low` |
| `business_owner` | STRING | Business stakeholder name/email |
| `business_process` | STRING | Business process this pipeline supports |
| `notes` | STRING | Optional notes |

**Example:**

```sql
INSERT INTO [odibi_system].[dim_pipeline_context] 
    (project, pipeline_name, environment, business_criticality, business_owner, business_process)
VALUES 
    ('SalesAnalytics', 'orders_silver', 'prod', 'High', 'Jane Smith', 'Daily Sales Reporting'),
    ('SalesAnalytics', 'inventory_bronze', 'prod', 'Medium', 'Bob Jones', 'Inventory Sync');
```

!!! note "Optional but Recommended"
    Views work without business context (columns will be NULL), but populating this table enables priority-based alerting and risk scoring.

## Best Practices

1. **Set owners** - Configure `owner` on all pipelines for accountability
2. **Define SLAs** - Set `freshness_sla` for business-critical pipelines
3. **Monitor health** - Build dashboards on `meta_pipeline_health`
4. **Periodic cleanup** - Run `odibi system cleanup` weekly/monthly
5. **Check observability errors** - Review `meta_observability_errors` regularly
6. **Use rebuild sparingly** - Only when derived tables are actually inconsistent

## Related

- [System Catalog](catalog.md) - Core catalog tables and configuration
- [Alerting](alerting.md) - Notifications for pipeline events
- [CLI Reference](cli.md) - Full CLI command reference
- [Diagnostics](diagnostics.md) - Pipeline debugging tools
