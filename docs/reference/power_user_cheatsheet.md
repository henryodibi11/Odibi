# Odibi Power User Cheatsheet

**Fill this out before you write a single line of YAML. When every blank is filled, the config writes itself.**

**How to use this document:**
1. **Section 1** — Fill once per project (connections, story, system, etc.)
2. **Section 2** — Fill once per pipeline (name, layer, error strategy)
3. **Section 3** — Print and fill once PER NODE (this is the page you photocopy)
4. **Section 4** — Advanced options (flip here when a checkbox says "→ see Section 4.X")
5. **Section 5** — Decision trees (quick reference when you're stuck on a choice)

---

# Section 1: Project Setup (Fill Once)

## 1.1 Project Identity

```
Project name:     ____________________
Description:      ____________________
Version:          __________ (default: 1.0.0)
Owner:            ____________________ (email)
Engine:           [ ] pandas    [ ] spark    [ ] polars
```

**How to choose engine → see Decision Tree 5.1**

```yaml
# YOUR YAML
project: "___"
engine: "___"
description: "___"
version: "___"
owner: "___"
```

---

## 1.2 Connections

**List every connection your project needs. You may have multiple of the same type.**
**Auth details → see Connection Type Cards below (reference once, then just fill the table).**

### Connection Roster (fill one row per connection)

```
 #  Name                 Type          Purpose                    Auth card
 ── ──────────────────── ──────────── ──────────────────────────── ─────────
 1  ___________________  ___________  ___________________________  → Card __
 2  ___________________  ___________  ___________________________  → Card __
 3  ___________________  ___________  ___________________________  → Card __
 4  ___________________  ___________  ___________________________  → Card __
 5  ___________________  ___________  ___________________________  → Card __
 6  ___________________  ___________  ___________________________  → Card __
```

**Example filled in:**
```
 #  Name                 Type          Purpose                    Auth card
 ── ──────────────────── ──────────── ──────────────────────────── ─────────
 1  landing_zone         local         Raw files from vendors       → Card A
 2  bronze_lake          azure_blob    Bronze layer (ADLS)          → Card B
 3  silver_lake          azure_blob    Silver layer (same account)  → Card B
 4  gold_lake            azure_blob    Gold layer (same account)    → Card B
 5  source_erp           sql_server    Source ERP database          → Card C
 6  target_analytics     sql_server    Target reporting database    → Card C
 7  vendor_api           http          REST API for pricing data    → Card D
```

### Connection Detail Sheets (fill one per connection)

```
Connection #___: ____________________
  Type:          ____________________
  Auth card:     ____________________

  Field 1:       ____________________    (see card for field names)
  Field 2:       ____________________
  Field 3:       ____________________
  Auth field:    ${____________________}  (env var for secrets)
```

---

### Card A: Local Connection

```
Fields needed:
  base_path:     ____________________    (e.g., ./data, /mnt/data)
```

```yaml
  my_local:
    type: local
    base_path: "./data"
```

---

### Card B: Azure Blob / ADLS Gen2

```
Fields needed:
  account_name:  ____________________
  container:     ____________________

Auth mode (pick one):
  [ ] account_key        → account_key: ${ENV_VAR}
  [ ] sas                → sas_token: ${ENV_VAR}
  [ ] connection_string  → connection_string: ${ENV_VAR}
  [ ] key_vault          → key_vault: ___  secret: ___
  [ ] aad_msi            → (no extra fields, uses managed identity)
```

```yaml
  my_blob:
    type: azure_blob
    account_name: "${STORAGE_ACCOUNT}"
    container: "my-container"
    auth:
      mode: account_key
      account_key: "${STORAGE_KEY}"
```

**Tip:** Multiple azure_blob connections can share the same account but use different containers:
```yaml
  bronze_lake:
    type: azure_blob
    account_name: "${STORAGE_ACCOUNT}"
    container: "bronze"
    auth: { mode: account_key, account_key: "${STORAGE_KEY}" }

  silver_lake:
    type: azure_blob
    account_name: "${STORAGE_ACCOUNT}"
    container: "silver"
    auth: { mode: account_key, account_key: "${STORAGE_KEY}" }
```

---

### Card C: SQL Server / Azure SQL

```
Fields needed:
  host:          ____________________
  database:      ____________________

Auth mode (pick one):
  [ ] sql_login          → username: ${ENV_VAR}  password: ${ENV_VAR}
  [ ] aad_msi            → (no extra fields, Databricks/Azure)
```

```yaml
  my_sql:
    type: sql_server
    host: "${DB_SERVER}"
    database: "${DB_NAME}"
    auth:
      mode: sql_login
      username: "${DB_USER}"
      password: "${DB_PASS}"
```

**Tip:** Separate source and target connections even if same server — different permissions/purposes:
```yaml
  source_erp:
    type: sql_server
    host: "${ERP_SERVER}"
    database: "ERP_Production"
    auth: { mode: sql_login, username: "${ERP_USER}", password: "${ERP_PASS}" }

  target_analytics:
    type: sql_server
    host: "${ANALYTICS_SERVER}"
    database: "Analytics_DW"
    auth: { mode: sql_login, username: "${DW_USER}", password: "${DW_PASS}" }
```

---

### Card D: HTTP / REST API

```
Fields needed:
  base_url:      ____________________

Auth (pick one):
  [ ] none
  [ ] bearer     → token: ${ENV_VAR}
  [ ] basic      → username: ${ENV_VAR}  password: ${ENV_VAR}
  [ ] api_key    → key: ${ENV_VAR}  header: ____________________

Pagination:
  [ ] none
  [ ] offset     → page_size: ______
  [ ] cursor     → cursor_field: ______
  [ ] link_header

Rate limit:      ______ requests per second
Retry attempts:  ______
```

```yaml
  my_api:
    type: http
    base_url: "https://api.example.com/v1"
    auth:
      mode: bearer
      token: "${API_TOKEN}"
    pagination:
      type: offset
      page_size: 100
    rate_limit:
      requests_per_second: 5
    retry:
      max_attempts: 3
      backoff: exponential
```

---

## 1.3 Story Configuration

```
Connection:            ____________________ (must match a connection name above)
Path:                  ____________________ (e.g., stories/)
Auto generate:         [ ] yes (default)    [ ] no
Max sample rows:       __________ (default: 10, set 0 for prod security)
Retention days:        __________ (default: 30)
Retention count:       __________ (default: 100)
Async generation:      [ ] yes    [ ] no    (faster for multi-pipeline)
Generate lineage:      [ ] yes    [ ] no

Failure troubleshooting:
  Failure sample size:     __________ (default: 100, failed rows per validation)
  Max failure samples:     __________ (default: 500, total across all validations)
  Max sampled validations: __________ (default: 5, after this show counts only)

Auto-generate docs:    [ ] yes    [ ] no
  If yes:
  Output path:         ____________________ (e.g., docs/generated/)
  Outputs:
    [ ] README           [ ] TECHNICAL_DETAILS
    [ ] NODE_CARDS        [ ] RUN_MEMO (per-run)
  Include:
    [ ] SQL              [ ] config snapshots    [ ] schema tables
```

```yaml
story:
  connection: "___"
  path: "___"
  max_sample_rows: 10
  retention_days: 30
  # failure_sample_size: 100
  # max_failure_samples: 500
  # max_sampled_validations: 5
  # async_generation: true
  # generate_lineage: true
  # docs:
  #   enabled: true
  #   output_path: "docs/generated/"
  #   outputs:
  #     readme: true
  #     technical_details: true
  #     node_cards: true
  #     run_memo: true
```

---

## 1.4 System Catalog

```
Connection:            ____________________ (must be storage, NOT sql_server)
Path:                  ____________________ (default: _odibi_system)
Environment tag:       ____________________ (dev / qat / prod)

Sync to secondary?     [ ] yes    [ ] no    → see Section 4.7
Sync from source?      [ ] yes    [ ] no    → see Section 4.7

Cost tracking:         [ ] yes    [ ] no
  Cost per hour:       $________
  Databricks billing:  [ ] yes    [ ] no    (query Databricks billing tables)

Retention:
  Daily stats:              __________ days (default: 365)
  Failures:                 __________ days (default: 90)
  Observability errors:     __________ days (default: 90)
```

```yaml
system:
  connection: "___"
  path: "_odibi_system"
  environment: "___"
  # cost_per_compute_hour: 2.50
  # retention_days:
  #   daily_stats: 365
  #   failures: 90
  #   observability_errors: 90
```

---

## 1.5 Global Settings

### Retry
```
Enabled:             [ ] yes    [ ] no
Max attempts:        __________ (default: 3, max: 10)
Backoff:             [ ] exponential    [ ] linear    [ ] constant
```
```yaml
retry:
  enabled: true
  max_attempts: 3
  backoff: exponential
```

### Logging
```
Level:               [ ] DEBUG    [ ] INFO    [ ] WARNING    [ ] ERROR
Structured (JSON):   [ ] yes    [ ] no
Extra metadata:      ____________________    (key-value pairs added to all log entries)
```
```yaml
logging:
  level: INFO
  structured: false
  # metadata:
  #   team: "data-engineering"
  #   cost_center: "analytics"
```

### Alerts

**List all alert destinations. You may have multiple (e.g., Slack for failures, email for success).**

```
 #  Type              URL/Destination            Events
 ── ────────────────  ────────────────────────── ──────────────────────────
 1  _______________   ${_______________}         ______________________________
 2  _______________   ${_______________}         ______________________________
 3  _______________   ${_______________}         ______________________________

Available types:   slack | teams | teams_workflow | webhook
Available events:  on_start | on_success | on_failure | on_quarantine
                   on_gate_block | on_threshold_breach

Throttling (per alert):
  Alert #___: throttle_minutes: ______  max_per_hour: ______  channel: ______
  Alert #___: throttle_minutes: ______  max_per_hour: ______  channel: ______
```
```yaml
alerts:
  - type: slack
    url: "${SLACK_WEBHOOK_URL}"
    on_events: [on_failure, on_quarantine, on_gate_block]
    metadata:
      throttle_minutes: 15
      max_per_hour: 10
      channel: "#data-alerts"
  # - type: teams
  #   url: "${TEAMS_WEBHOOK_URL}"
  #   on_events: [on_failure]
```

### Performance
```
Use Arrow (Pandas):          [ ] yes    [ ] no    (default: yes)
Skip null profiling:         [ ] yes    [ ] no    (faster for large DFs)
Skip catalog writes:         [ ] yes    [ ] no    (faster Bronze ingestion)
Skip run logging:            [ ] yes    [ ] no    (saves 10-20s per run)

Spark config:                ____________________
Delta table properties:      ____________________
```
```yaml
performance:
  use_arrow: true
  skip_null_profiling: false
  # spark_config:
  #   "spark.sql.shuffle.partitions": "200"
  #   "spark.sql.adaptive.enabled": "true"
```

### OpenLineage (optional)
```
Enabled:             [ ] yes    [ ] no
URL:                 ____________________
Namespace:           ____________________ (default: odibi)
API key:             ${___}
```

---

## 1.6 Variables & Substitution

**Three substitution types are available anywhere in YAML:**

| Syntax | What it does | Example |
|--------|-------------|---------|
| `${ENV_VAR}` | OS environment variable | `${STORAGE_KEY}` |
| `${ENV_VAR:default}` | Env var with fallback | `${ODIBI_ENV:dev}` |
| `${vars.xxx}` | Project variable (from vars: block) | `${vars.base_path}` |
| `${date:expr}` | Date expression | `${date:today}`, `${date:-7d}` |

### Project Variables (defined once, reused everywhere)
```
Variable name          Value                        Usage
____________________   ____________________         ${vars.___}
____________________   ____________________         ${vars.___}
____________________   ____________________         ${vars.___}
____________________   ____________________         ${vars.___}
```
```yaml
vars:
  base_path: "data"
  env: "${ODIBI_ENV:dev}"
  schema: "analytics"
```

### Environment Variables Used (list all secrets/env vars your project needs)
```
Env var name           Purpose                      Where used
____________________   ____________________         ____________________
____________________   ____________________         ____________________
____________________   ____________________         ____________________
____________________   ____________________         ____________________
```

**Date expressions:** `${date:today}`, `${date:-1d}`, `${date:-7d}`, `${date:+1d}`, `${date:-1w}`

---

## 1.7 Environment Strategy

```
Strategy:    [ ] No environments (single config)
             [ ] Inline environments: block
             [ ] External env.{env}.yaml files
             [ ] Per-environment pipelines (env files import different pipeline YAMLs)

Environments needed:
  [ ] dev     [ ] test/qat     [ ] staging     [ ] prod

What changes per environment?
[ ] Engine (pandas → spark)
[ ] Connections (local → cloud)
[ ] Story settings (sampling → no sampling)
[ ] Retry/alerts (off → on)
[ ] Pipeline definitions (different pipelines per env)
[ ] System catalog environment tag
[ ] Performance settings
[ ] Logging level
[ ] Lineage config
[ ] Variables (vars)

Overrideable fields (inline environments: block):
engine, connections, system, performance, logging, retry,
alerts, story, lineage, vars, pipelines
(external env files can override ANYTHING — no restrictions)

Env files support imports:
env.dev.yaml can use imports:, ${vars.xxx}, ${date:...}
```

**How to choose → see Decision Tree 5.6**

---

## 1.8 YAML Organization

```
Project size:    [ ] Small (< 5 nodes)       → single project.yaml
                 [ ] Medium (5-20 nodes)     → project.yaml + pipeline imports
                 [ ] Large (20+ nodes)       → per-layer pipeline files + env files

File layout:
  project.yaml              → connections, system, story, settings
  pipelines/bronze/__.yaml  → bronze pipeline(s)
  pipelines/silver/__.yaml  → silver pipeline(s)
  pipelines/gold/__.yaml    → gold pipeline(s)
  env.dev.yaml              → dev overrides
  env.prod.yaml             → prod overrides
  sql/                      → external SQL files
  transformations/          → custom Python functions
```

```yaml
# project.yaml — imports pull in pipelines
imports:
  - pipelines/bronze/ingest.yaml
  - pipelines/silver/transform.yaml
  - pipelines/gold/aggregate.yaml
```

---

# Section 2: Pipeline Worksheet (Fill Per Pipeline)

```
════════════════════════════════════════════════════════════════
 PIPELINE: ____________________
════════════════════════════════════════════════════════════════

Name:              ____________________
Description:       ____________________
Layer:             [ ] bronze    [ ] silver    [ ] gold
Owner:             ____________________ (email)
Freshness SLA:     ____________________ (format: '<number><unit>' where unit is s/m/h/d. e.g., 6h, 1d, or leave blank)
Freshness anchor:  [ ] run_completion (default)    [ ] table_max_timestamp    [ ] watermark_state

How many nodes?    __________

Node list:
  1. ____________________
  2. ____________________
  3. ____________________
  4. ____________________
  5. ____________________
  6. ____________________
  7. ____________________
  8. ____________________
```

```yaml
pipelines:
  - pipeline: "___"
    description: "___"
    layer: "___"
    owner: "___"
    # freshness_sla: "6h"
    nodes:
      # ... see Section 3 for each node
```

---

# Section 3: Node Worksheet (Print Copies — Fill Per Node)

**✂️ ─── PHOTOCOPY THIS PAGE ─── ✂️**

```
════════════════════════════════════════════════════════════════
 NODE: ____________________     Pipeline: ____________________
════════════════════════════════════════════════════════════════
```

## 3.1 Identity

```
Name:              ____________________
Description:       ____________________
Depends on:        ____________________
Tags:              ____________________ (e.g., daily, critical, hourly)
Enabled:           [ ] yes    [ ] no
Cache result:      [ ] yes    [ ] no    (keep in memory for downstream)
Materialized:      [ ] table (default)    [ ] view    [ ] incremental
On error:          [ ] fail_fast    [ ] fail_later (default)    [ ] ignore
Streaming node:    [ ] yes    [ ] no    (Spark only, enables streaming execution)
Log level override:[ ] none    [ ] DEBUG    [ ] INFO    [ ] WARNING
```

```yaml
- name: "___"
  description: "___"
  depends_on: [___]
  # tags: [daily]
  # cache: true
  # on_error: fail_later
```

---

## 3.2 Read Phase — "Where does the data come from?"

```
Source type:      [ ] File/storage (csv, parquet, delta, json)
                 [ ] Database (sql)
                 [ ] API/HTTP
                 [ ] No read — gets data from dependency (skip this section)
                 [ ] Multi-source inputs → see Section 4.1

Connection:      ____________________
Format:          [ ] csv    [ ] parquet    [ ] delta    [ ] json    [ ] sql
Path:            ____________________    (for files)
  — OR —
Table:           ____________________    (for database/catalog)
  — OR —
Query:           ____________________    (inline SQL, pushdown to source)
  — OR —
SQL file:        ____________________    (external .sql file, relative to YAML)

Filter:          ____________________    (WHERE clause pushed to source)
Streaming:       [ ] yes    [ ] no       → if yes, see Section 4.2
Archive bad records: [ ] yes    [ ] no   → if yes, see Section 4.6

Format options:  ____________________    (e.g., header: true, delimiter: "|")
```

### Incremental Loading — "Should I reload everything or just new data?"

```
Incremental:     [ ] No — full load every time (skip below)
                 [ ] rolling_window — lookback period
                 [ ] stateful — high water mark (remembers last position)

  Column:        ____________________    (timestamp/date column to filter on)
  Fallback col:  ____________________    (backup column if primary missing)

  If rolling_window:
    Lookback:    __________
    Unit:        [ ] hours    [ ] days    [ ] weeks

  If stateful:
    State key:     ____________________  (custom key, default: node name)
    Watermark lag: ____________________  (format: '<number><unit>' where unit is s/m/h/d. e.g., "5m" safety buffer)
```

### Time Travel (Delta only)
```
Time travel:     [ ] No
                 [ ] as_of_timestamp: ____________________
                 [ ] version: __________
```

```yaml
  read:
    connection: "___"
    format: "___"
    path: "___"            # or table: "___"
    # query: "SELECT ..."  # or sql_file: "queries/my_query.sql"
    # filter: "date > '2024-01-01'"
    # options:
    #   header: "true"
    #   delimiter: "|"
    # incremental:
    #   mode: stateful
    #   column: updated_at
    # time_travel:
    #   as_of_timestamp: "2024-01-01T00:00:00Z"
```

---

## 3.3 Transform Phase — "What do I do with the data?"

### Choose your approach:

```
Approach:        [ ] No transformation (pass-through, e.g., Bronze ingestion)
                 [ ] Transformer — one heavy operation ("App" mode)
                 [ ] Transform steps — chain of lighter operations ("Script" mode)
                 [ ] Both — transformer first, then steps refine the result
```

**How to choose pattern/transformer → see Decision Tree 5.2**

### If using a Transformer ("App"):

```
Transformer:     [ ] scd2            [ ] merge          [ ] deduplicate
                 [ ] dimension       [ ] fact            [ ] aggregation
                 [ ] date_dimension  [ ] join            [ ] pivot
                 [ ] unpivot         [ ] union           [ ] filter_rows
                 [ ] derive_columns  [ ] case_when       [ ] fill_nulls
                 [ ] clean_text      [ ] cast_columns    [ ] generate_surrogate_key
                 [ ] hash_columns    [ ] rename_columns  [ ] drop_columns
                 [ ] select_columns  [ ] sort            [ ] explode
                 [ ] normalize_json  [ ] validate_and_flag
                 [ ] detect_deletes  [ ] date_diff       [ ] date_add
                 [ ] date_trunc      [ ] regex_replace   [ ] split_column
                 [ ] unit_conversion [ ] cross_check
                 [ ] other: ____________________

Params:
  Key                    Value
  ____________________   ____________________
  ____________________   ____________________
  ____________________   ____________________
  ____________________   ____________________
  ____________________   ____________________
```

```yaml
  transformer: "___"
  params:
    ___: "___"
```

### If using Transform Steps ("Script"):

```
Step 1:  [ ] sql       query: ____________________
         [ ] function  name: __________  params: ____________________
         [ ] operation name: __________  params: ____________________

Step 2:  [ ] sql       query: ____________________
         [ ] function  name: __________  params: ____________________
         [ ] operation name: __________  params: ____________________

Step 3:  [ ] sql       query: ____________________
         [ ] function  name: __________  params: ____________________
         [ ] operation name: __________  params: ____________________
```

```yaml
  transform:
    steps:
      - sql: "SELECT * FROM df WHERE ___"
      - function: "___"
        params: { ___: ___ }
```

### Pre/Post SQL (optional)

```
Pre-SQL:   ____________________    (runs before transform)
Post-SQL:  ____________________    (runs after write, e.g., OPTIMIZE, VACUUM)
```

```yaml
  # pre_sql: ["SET spark.sql.shuffle.partitions = 200"]
  # post_sql: ["OPTIMIZE gold.fact_sales ZORDER BY (customer_id)"]
```

---

## 3.4 Contracts — "Is the SOURCE data safe to process?"

**Contracts are pre-transform circuit breakers. They run on input data BEFORE transformation.**

**All 11 test types work as contracts. Common choices for contracts: freshness, schema, row_count, volume_drop.**

```
Contracts needed?  [ ] No (skip)    [ ] Yes (check applicable):

Source quality (most common for contracts):
[ ] freshness      column: ____________  max_age: ________ (format: '<number><unit>' where unit is s/m/h/d. e.g., 24h, 7d)
                   name: ____________  on_fail: [ ] fail  [ ] warn

[ ] schema         strict: [ ] yes  [ ] no
                   name: ____________  on_fail: [ ] fail  [ ] warn

[ ] row_count      min: ________  max: ________
                   name: ____________  on_fail: [ ] fail  [ ] warn

[ ] volume_drop    threshold: ________ (0.5 = 50% drop)  lookback_days: ________
                   name: ____________  on_fail: [ ] fail  [ ] warn

[ ] distribution   column: ________ metric: [ ]mean [ ]min [ ]max [ ]null_percentage
                   threshold: ________ (e.g., ">100", "<0.05")
                   name: ____________  on_fail: [ ] fail  [ ] warn

Row-level checks (can also be contracts):
[ ] not_null       columns: ____________________
                   name: ____________  on_fail: [ ] fail  [ ] warn  [ ] quarantine

[ ] unique         columns: ____________________
                   name: ____________  on_fail: [ ] fail  [ ] warn  [ ] quarantine

[ ] accepted_values column: ____________  values: ____________________
                   name: ____________  on_fail: [ ] fail  [ ] warn  [ ] quarantine

[ ] range          column: ____________  min: ________  max: ________
                   name: ____________  on_fail: [ ] fail  [ ] warn  [ ] quarantine

[ ] regex_match    column: ____________  pattern: ____________________
                   name: ____________  on_fail: [ ] fail  [ ] warn  [ ] quarantine

[ ] custom_sql     condition: ____________________  threshold: ________
                   name: ____________  on_fail: [ ] fail  [ ] warn  [ ] quarantine
```

```yaml
  contracts:
    - type: freshness
      column: updated_at
      max_age: "24h"
    - type: schema
      strict: true
    - type: row_count
      min: 100
    - type: not_null
      columns: [id, name]
```

---

## 3.5 Validation — "Is the OUTPUT data good enough to write?"

**Validation runs on transformed data AFTER transformation but BEFORE writing.**

```
Validation needed? [ ] No (skip)    [ ] Yes

Mode:              [ ] fail (stop pipeline)    [ ] warn (log only)
On fail:           [ ] alert    [ ] ignore
Fail fast:         [ ] yes (stop on first failure)    [ ] no (run all tests)
Cache DF (Spark):  [ ] yes    [ ] no
```

### Tests (all 11 types available — check all that apply):

```
Row-level quality:
[ ] not_null        columns: ____________________
                    name: ____________  on_fail: [ ] fail  [ ] warn  [ ] quarantine

[ ] unique          columns: ____________________
                    name: ____________  on_fail: [ ] fail  [ ] warn  [ ] quarantine

[ ] accepted_values column: ____________  values: ____________________
                    name: ____________  on_fail: [ ] fail  [ ] warn  [ ] quarantine

[ ] range           column: ____________  min: ________  max: ________
                    name: ____________  on_fail: [ ] fail  [ ] warn  [ ] quarantine

[ ] regex_match     column: ____________  pattern: ____________________
                    name: ____________  on_fail: [ ] fail  [ ] warn  [ ] quarantine

[ ] custom_sql      condition: ____________________
                    threshold: ________ (0.0 = zero tolerance, 0.01 = 1% allowed)
                    name: ____________  on_fail: [ ] fail  [ ] warn  [ ] quarantine

Batch-level checks:
[ ] row_count       min: ________  max: ________
                    name: ____________  on_fail: [ ] fail  [ ] warn

[ ] volume_drop     threshold: ________  lookback_days: ________
                    name: ____________  on_fail: [ ] fail  [ ] warn

[ ] freshness       column: ____________  max_age: ________
                    name: ____________  on_fail: [ ] fail  [ ] warn

[ ] schema          strict: [ ] yes  [ ] no
                    name: ____________  on_fail: [ ] fail  [ ] warn

[ ] distribution    column: ____________  metric: [ ]mean [ ]min [ ]max [ ]null_pct
                    threshold: ________ (e.g., ">100")
                    name: ____________  on_fail: [ ] fail  [ ] warn
```

```yaml
  validation:
    mode: fail
    on_fail: alert
    tests:
      - type: not_null
        columns: [___]
      - type: unique
        columns: [___]
      - type: range
        column: "___"
        min: 0
        max: 999999
```

### Quality Gate (batch-level pass/fail):

```
Quality gate?      [ ] No    [ ] Yes

  Pass rate:       ________% (default: 95)
  On fail:         [ ] abort    [ ] warn_and_write    [ ] write_valid_only

  Per-test thresholds:
    Test             Min pass rate
    ______________   __________%
    ______________   __________%

  Row count gate:
    Min rows:        __________
    Max rows:        __________
    Change threshold: ________% (vs previous run)
```

```yaml
    gate:
      require_pass_rate: 0.95
      on_fail: abort
      thresholds:
        - test: not_null
          min_pass_rate: 0.99
      # row_count:
      #   min: 100
      #   change_threshold: 0.5
```

### Quarantine (route bad rows to separate table):

```
Quarantine?        [ ] No    [ ] Yes

  Connection:      ____________________
  Path/table:      ____________________
  Retention days:  __________ (default: 90)
  Max rows:        __________ (cap per run)
  Sample fraction: __________ (0.0-1.0, for sampling large invalid sets)

  Add columns:
    [ ] _rejection_reason  (default: yes)
    [ ] _rejected_at       (default: yes)
    [ ] _source_batch_id   (default: yes)
    [ ] _failed_tests      (default: yes)
    [ ] _original_node     (default: no)
```

```yaml
    quarantine:
      connection: "___"
      path: "___"
      retention_days: 90
      # max_rows: 10000
```

---

## 3.6 Write Phase — "How do I save the output?"

```
Write needed?    [ ] No (in-memory only, downstream nodes read from context)
                 [ ] Yes

Connection:      ____________________
Format:          [ ] csv    [ ] parquet    [ ] delta    [ ] json    [ ] sql_server
Path:            ____________________    (for files)
  — OR —
Table:           ____________________    (for database/catalog)
```

### Write Mode — "How do I handle existing data?"

**How to choose → see Decision Tree 5.3**

```
Mode:            [ ] overwrite     — replace everything (full refresh, dimensions)
                 [ ] append        — add rows, no duplicate check (logs, events)
                 [ ] upsert        — update by key or insert (Silver/Gold updates)
                 [ ] append_once   — skip existing keys, insert new (idempotent Bronze)
                 [ ] merge         — SQL Server MERGE via staging → see Section 4.3

If upsert/append_once:
  Keys (options):  ____________________    (columns identifying unique records)

Format options:    ____________________    (e.g., header: true, quoteAll: true)
```

```yaml
  write:
    connection: "___"
    format: "___"
    path: "___"          # or table: "___"
    mode: "___"
    # options:
    #   keys: [order_id]  # for upsert/append_once
    #   header: "true"    # format-specific options
```

### Delta Lake Options (if format = delta):

```
Partition by:      ____________________    (low-cardinality: date, country)
Z-order by:        ____________________    (high-cardinality: customer_id)
Merge schema:      [ ] yes    [ ] no       (allow new columns)
Overwrite schema:  [ ] yes    [ ] no       (allow schema replacement)
Table properties:  ____________________    (e.g., delta.columnMapping.mode: name)
Register table:    ____________________    (register as external Spark table)

Auto optimize:     [ ] no    [ ] yes
  Vacuum hours:    __________ (default: 168 = 7 days)
```

```yaml
    # partition_by: [order_year, order_month]
    # zorder_by: [customer_id]
    # merge_schema: true
    # auto_optimize:
    #   enabled: true
    #   vacuum_retention_hours: 168
```

### Bronze Metadata (if Bronze layer):

```
Add metadata:      [ ] no    [ ] yes (all)    [ ] selective
  [ ] _extracted_at        (pipeline timestamp)
  [ ] _source_file         (source filename)
  [ ] _source_connection   (connection name)
  [ ] _source_table        (table/query name)
```

```yaml
    # add_metadata: true
```

### Skip-If-Unchanged (for slow-changing sources):

```
Skip if unchanged: [ ] no    [ ] yes
  Hash columns:    ____________________    (subset, or blank = all columns)
  Sort columns:    ____________________    (for deterministic hash)
```

```yaml
    # skip_if_unchanged: true
    # skip_hash_columns: [customer_id, name, email]
    # skip_hash_sort_columns: [customer_id]
```

### First Run Query (HWM bootstrap):

```
First run query:   [ ] no    [ ] yes: ____________________
```

```yaml
    # first_run_query: "SELECT * FROM orders WHERE date >= '2023-01-01'"
```

### SQL Server options?
```
[ ] merge_options   → see Section 4.3
[ ] overwrite_options → see Section 4.4
```

### Streaming write?
```
[ ] streaming write → see Section 4.2
```

---

## 3.7 Schema & Privacy

### Schema Policy — "How do I handle schema drift?"

```
Schema policy?   [ ] No (skip)    [ ] Yes

  Mode:          [ ] enforce (strict — fail on mismatch)
                 [ ] evolve (adapt to changes)

  On new columns:    [ ] ignore    [ ] fail    [ ] add_nullable
  On missing columns:[ ] fail      [ ] fill_null
```

```yaml
  # schema_policy:
  #   mode: evolve
  #   on_new_columns: add_nullable
  #   on_missing_columns: fill_null
```

### Privacy — "Does this node handle PII?"

```
Has PII?         [ ] No (skip)    [ ] Yes

  PII columns:  ____________________
  Method:        [ ] hash (SHA256)    [ ] mask (last 4 chars)    [ ] redact ([REDACTED])
  Salt:          ${___}
  Declassify:    ____________________    (columns to un-mark as PII)

  Sensitive columns (masked in stories): ____________________
```

```yaml
  # columns:
  #   email:
  #     pii: true
  #   phone:
  #     pii: true
  # privacy:
  #   method: hash
  #   salt: "${PRIVACY_SALT}"
  # sensitive: [email, phone, ssn]
```

### Data Dictionary (optional documentation):

```
Column               Description                    Tags
__________________   ____________________________   ____________________
__________________   ____________________________   ____________________
__________________   ____________________________   ____________________
__________________   ____________________________   ____________________
```

```yaml
  # columns:
  #   customer_id:
  #     description: "Unique customer identifier"
  #     tags: [business_key]
  #   revenue:
  #     description: "Total revenue in USD"
  #     tags: [measure]
```

---

## 3.8 Documentation (optional)

```
Explanation:     [ ] none
                 [ ] inline:     ____________________
                 [ ] external file: ____________________  (relative to YAML)

Runbook URL:     ____________________    (link to troubleshooting guide)
```

```yaml
  # explanation: "Joins orders with customers and filters active only"
  # explanation_file: "docs/nodes/clean_orders.md"
  # runbook_url: "https://wiki.example.com/runbooks/clean_orders"
```

**✂️ ─── END OF NODE WORKSHEET ─── ✂️**

---

# Section 4: Advanced Options

## 4.1 Multi-Source Inputs (alternative to `read:`)

**When:** A node needs data from multiple pipelines or multiple sources.
**Rule:** Cannot be used together with `read:`. Use one or the other.

```
Input name           Source reference
__________________   $pipeline_name.node_name
__________________   $pipeline_name.node_name
__________________   { connection: ___, path: ___ }
```

```yaml
  inputs:
    events: "$read_bronze.shift_events"
    calendar: "$read_bronze.calendar"
    # OR explicit read config:
    # lookup: { connection: "local", path: "reference/codes.csv", format: csv }
```

---

## 4.2 Streaming Configuration

### Streaming Read:
```
Streaming read:    [ ] yes
Schema DDL:        ____________________    (required for JSON/CSV/Avro streaming)
                   (e.g., "col1 STRING, col2 INT, col3 TIMESTAMP")
```

```yaml
  read:
    connection: "___"
    format: "___"
    streaming: true
    schema_ddl: "event_id STRING, payload STRING, event_time TIMESTAMP"
```

### Streaming Write:
```
Output mode:       [ ] append    [ ] update    [ ] complete
Checkpoint:        ____________________    (required, reliable storage path)
Query name:        ____________________    (for monitoring)
Await termination: [ ] yes    [ ] no
Timeout seconds:   __________

Trigger:           [ ] processing_time: __________  (e.g., "10 seconds")
                   [ ] once: true                   (process all, stop)
                   [ ] available_now: true           (process all in batches, stop)
                   [ ] continuous: __________        (e.g., "1 second")
```

```yaml
  write:
    connection: "___"
    format: delta
    table: "___"
    streaming:
      output_mode: append
      checkpoint_location: "/checkpoints/___"
      trigger:
        processing_time: "10 seconds"
```

---

## 4.3 SQL Server Merge Options

**When:** `write.mode: merge` with a SQL Server connection.

```
Merge keys:             ____________________    (ON clause columns)

Update condition:       ____________________    (e.g., source._hash != target._hash)
Delete condition:       ____________________    (e.g., source._is_deleted = 1)
Insert condition:       ____________________    (e.g., source.is_valid = 1)
Exclude columns:        ____________________    (don't write to target)
Staging schema:         ____________________ (default: staging)

Audit columns:
  Created col:          ____________________    (set on INSERT, GETUTCDATE())
  Updated col:          ____________________    (set on INSERT+UPDATE)

Pre-merge validations:
  Check null keys:      [ ] yes    [ ] no
  Check duplicate keys: [ ] yes    [ ] no
  Fail on error:        [ ] yes    [ ] no

Auto-create:
  Auto-create schema:   [ ] yes    [ ] no
  Auto-create table:    [ ] yes    [ ] no
  Primary key on merge keys: [ ] yes    [ ] no
  Index on merge keys:  [ ] yes    [ ] no

Schema evolution:
  Mode:                 [ ] strict    [ ] evolve    [ ] ignore
  Add columns:          [ ] yes    [ ] no

Incremental merge:      [ ] yes    [ ] no    (only write changed rows to staging)
  Hash column:          ____________________    (e.g., _hash_diff)
  Change detection cols:____________________

Batch size:             __________

Bulk copy?              [ ] no    [ ] yes (10-50x faster for large datasets)
  Staging connection:   ____________________    (ADLS/blob for staging files)
  Staging path:         ____________________    (default: odibi_staging/bulk)
  External data source: ____________________    (SQL Server object name)
  Auto setup:           [ ] yes    [ ] no
  Force recreate:       [ ] yes    [ ] no
  Keep staging files:   [ ] yes    [ ] no
  CSV options:          ____________________    (quote, escape, encoding overrides)
```

```yaml
  write:
    connection: azure_sql
    format: sql_server
    table: sales.fact_orders
    mode: merge
    merge_keys: [order_id]
    merge_options:
      update_condition: "source._hash_diff != target._hash_diff"
      exclude_columns: [_hash_diff]
      audit_cols:
        created_col: created_ts
        updated_col: updated_ts
      validations:
        check_null_keys: true
        check_duplicate_keys: true
      # incremental: true
      # bulk_copy: true
      # staging_connection: adls_staging
```

---

## 4.4 SQL Server Overwrite Options

**When:** `write.mode: overwrite` with a SQL Server connection.

```
Strategy:           [ ] truncate_insert (fastest, needs TRUNCATE permission)
                    [ ] drop_create     (refreshes schema)
                    [ ] delete_insert   (no special permissions needed)

Audit columns:
  Created col:      ____________________
  Updated col:      ____________________

Auto-create:
  Schema:           [ ] yes    [ ] no
  Table:            [ ] yes    [ ] no

Schema evolution:
  Mode:             [ ] strict    [ ] evolve    [ ] ignore

Batch size:         __________

Bulk copy?          [ ] no    [ ] yes
  (same sub-fields as Section 4.3)
```

```yaml
  write:
    connection: azure_sql
    format: sql_server
    table: fact.combined_downtime
    mode: overwrite
    overwrite_options:
      strategy: truncate_insert
      audit_cols:
        created_col: created_ts
        updated_col: updated_ts
```

---

## 4.5 Delete Detection

**When:** Source system hard-deletes records and you need Silver to reflect those deletes.

```
Mode:                [ ] none    [ ] snapshot_diff    [ ] sql_compare

Keys:                ____________________    (business key columns)
Soft delete column:  ____________________    (default: _is_deleted, null = hard delete)

If snapshot_diff:
  Connection:        ____________________    (target Delta table connection)
  Path:              ____________________    (target Delta table path)
  Snapshot column:   ____________________    (for non-Delta)
  On first run:      [ ] skip    [ ] error

If sql_compare:
  Source connection:  ____________________   (live source database)
  Source table:       ____________________
  — OR —
  Source query:       ____________________

Safety:
  Max delete %:       __________ (default: 50%)
  On breach:          [ ] warn    [ ] error    [ ] skip
```

```yaml
  transform:
    steps:
      - operation: detect_deletes
        params:
          mode: sql_compare
          keys: [customer_id]
          source_connection: azure_sql
          source_table: dbo.Customers
          max_delete_percent: 20.0
          on_threshold_breach: error
```

---

## 4.6 Archive Options (Spark Bad Records)

```
Bad records path:  ____________________    (e.g., /mnt/quarantine/bad_records)
```

```yaml
  read:
    archive_options:
      badRecordsPath: "/mnt/quarantine/bad_records"
```

---

## 4.7 System Catalog Sync

### Sync To (replicate catalog to SQL Server or another blob):
```
Target connection:  ____________________
Schema name:        ____________________ (default: odibi_system, for SQL Server)
Path:               ____________________ (for blob targets)
Mode:               [ ] incremental    [ ] full
Trigger:            [ ] after_run      [ ] manual
Async:              [ ] yes    [ ] no
Tables to sync:     ____________________    (blank = default high-priority tables)
Sync last N days:   __________
```

```yaml
system:
  connection: adls_prod
  path: _odibi_system
  environment: prod
  sync_to:
    connection: sql_server_prod
    schema_name: odibi_system
    mode: incremental
    on: after_run
```

### Sync From (pull local dev data into centralized catalog):
```
Source connection:   ____________________
Source path:         ____________________
Source schema:       ____________________    (for SQL Server sources)
```

---

# Section 5: Decision Trees

## 5.1 Engine Selection

```
What's your situation?
│
├─► Local development, files < 1 GB
│   └─► pandas ✅ (fast startup, zero dependencies)
│
├─► Local development, files 1-10 GB
│   └─► polars ✅ (faster than Pandas, lazy evaluation)
│
├─► Production, Delta Lake, > 10 GB
│   └─► spark ✅ (distributed processing, Delta native)
│
├─► Databricks
│   └─► spark ✅ (native integration)
│
└─► Not sure?
    └─► Start with pandas, switch to spark for production
```

**Rule:** Use `pandas` locally, `spark` in production. Override per environment:
```yaml
engine: pandas
environments:
  prod:
    engine: spark
```

---

## 5.2 Pattern & Transformer Selection

```
What kind of data are you processing?
│
├─► Reference/lookup data (customers, products, stores)
│   │
│   ├─► Do attributes change over time?
│   │   ├─► YES — Do you need history of every change?
│   │   │   ├─► YES → scd2 pattern
│   │   │   └─► NO  → dimension pattern (overwrites with latest)
│   │   └─► NO — Static reference data
│   │       └─► dimension pattern
│   │
│   └─► Is it a date/calendar table?
│       └─► date_dimension pattern (auto-generates)
│
├─► Transactional data (orders, events, measurements)
│   │
│   ├─► Does it have foreign keys to dimension tables?
│   │   └─► YES → fact pattern (FK lookup + surrogate keys)
│   │
│   └─► Is it raw event/log data?
│       └─► No pattern needed — just read → write
│
├─► Need to merge incoming rows against existing target?
│   └─► merge pattern (or write mode: upsert/merge)
│
├─► Need to roll up / summarize / aggregate?
│   └─► aggregation pattern
│
├─► Need to clean / reshape / enrich?
│   │
│   ├─► Remove duplicates → deduplicate transformer
│   ├─► Join two datasets → join transformer
│   ├─► Pivot rows to columns → pivot transformer
│   ├─► Unpivot columns to rows → unpivot transformer
│   ├─► Flatten nested JSON → normalize_json transformer
│   ├─► Clean text (trim/case) → clean_text transformer
│   ├─► Fill nulls → fill_nulls transformer
│   ├─► Add computed columns → derive_columns transformer
│   ├─► Conditional logic → case_when transformer
│   ├─► Generate hash keys → generate_surrogate_key transformer
│   ├─► Type conversion → cast_columns transformer
│   ├─► Detect deleted records → detect_deletes transformer
│   └─► Custom SQL logic → transform.steps with sql
│
└─► None of the above?
    └─► Use transform.steps with custom SQL or Python functions
```

---

## 5.3 Write Mode Selection

```
How should existing data be handled?
│
├─► First load / full refresh / recalculated output
│   └─► overwrite ✅
│
├─► Events, logs, immutable records (never updated)
│   └─► append ✅
│
├─► Incoming rows may update existing rows (by key)
│   ├─► Target is Delta Lake → upsert ✅
│   └─► Target is SQL Server → merge ✅
│
├─► Bronze ingestion (safe to retry/rerun without duplicates)
│   └─► append_once ✅ (requires keys in options)
│
├─► SCD2 transformer output
│   └─► Transformer handles write internally — no write: block needed
│
└─► Aggregation / Gold layer recalculation
    └─► overwrite ✅ (idempotent)
```

**Quick reference:**

| Mode | Existing keys | New keys | Use case |
|------|--------------|----------|----------|
| `overwrite` | Deleted | Inserted | Full refresh |
| `append` | Duplicated | Inserted | Logs, events |
| `upsert` | Updated | Inserted | Silver/Gold updates |
| `append_once` | Skipped | Inserted | Idempotent Bronze |
| `merge` | Updated | Inserted | SQL Server targets |

### Bronze `append_once` Strategy Guide

**Maturity progression — don't start with `append_once`:**

```
New source / exploratory phase
│
├─► Start with append — understand the data, find the grain
│   (duplicates are OK here, you're learning)
│
└─► Once you know the natural keys
    └─► Switch to append_once — production-safe, idempotent reruns
```

**Choosing keys for `append_once`:**

```
Does the source have a natural business key (order_id, invoice_number)?
│
├─► YES → Use the natural key(s)
│   keys: [order_id]
│
├─► YES, but composite → Use the composite key
│   keys: [invoice_number, line_item_id]
│
└─► NO natural key → Consider a content hash
    Create a _row_hash from non-key columns (see below)
    keys: [_row_hash]
    ⚠️ Caution: truly duplicate rows (same content, different meaning)
       will be collapsed — add a positional element if needed
```

**Advanced: Bronze as a change log (capture every version of a row):**

Use both the business key AND a content hash as composite `append_once` keys.
Same key + different data = different hash = new row inserted.

```yaml
# Bronze change log pattern
transform:
  steps:
    - function: add_hash_column
      params:
        columns: [customer_name, email, status]   # non-key columns
        output_column: _row_hash
write:
  mode: append_once
  add_metadata: true            # adds _extracted_at automatically
  options:
    keys: [customer_id, _row_hash]   # identity + state
```

**How this flows into Silver:**

```
Bronze (append_once)                    Silver (upsert / SCD2)
┌──────────────────────────────┐       ┌──────────────────────────────┐
│ Every distinct version of a  │       │ Deduplicate by business key  │
│ row is stored as a new row.  │──────►│ (keep latest via             │
│                              │       │  _extracted_at DESC).        │
│ _row_hash = change detection │       │                              │
│ _extracted_at = ordering     │       │ Compare _row_hash to detect  │
│                              │       │ if data actually changed.    │
└──────────────────────────────┘       └──────────────────────────────┘
```

| Concern | Solved by |
|---------|-----------|
| Prevent duplicate ingestion | `append_once` with business keys |
| Capture every data version | Add `_row_hash` to keys |
| Order versions in Silver | `_extracted_at` (added by `add_metadata: true`) |
| Detect changes in Silver | Compare `_row_hash` (no need to diff all columns) |
| Legitimate source duplicates | Add positional element to hash input |

---

## 5.4 Incremental Loading Strategy

```
Should I reload everything each run?
│
├─► Source has a reliable timestamp column (updated_at, created_at)?
│   │
│   ├─► Need exact tracking (CDC-like, never miss a row)?
│   │   └─► stateful (high water mark) ✅
│   │       Remembers last value, filters column > last_hwm
│   │
│   └─► OK with some overlap / reprocessing?
│       └─► rolling_window ✅
│           Filters column >= NOW() - lookback
│
├─► Source is append-only immutable files?
│   └─► No incremental needed — just append write mode
│
├─► Source changes unpredictably / no timestamp?
│   ├─► Small dataset (< 1M rows)
│   │   └─► Full overwrite each run
│   └─► Large dataset
│       └─► skip_if_unchanged on write (hash comparison)
│
└─► Source rarely changes?
    └─► skip_if_unchanged: true ✅
```

---

## 5.5 Validation Strategy

```
How strict should data quality be?
│
├─► Development / testing
│   └─► validation.mode: warn
│       No quarantine, no gates
│
├─► Production, non-critical data
│   └─► validation.mode: warn + alerting
│       gate: { on_fail: warn_and_write }
│
├─► Production, critical data
│   └─► validation.mode: fail
│       gate: { require_pass_rate: 0.95, on_fail: abort }
│       Add quarantine for review
│
└─► Compliance / regulatory
    └─► validation.mode: fail
        gate: { require_pass_rate: 1.0, on_fail: abort }
        quarantine: yes (audit trail)
        FK validation across dimensions
        contracts: freshness + schema + volume_drop
```

**Layer-specific guidance:**

| Layer | Contracts (pre) | Validation (post) | Gate | Quarantine |
|-------|----------------|-------------------|------|------------|
| Bronze | freshness, row_count, schema | None or minimal | No | No |
| Silver | None (already validated input) | not_null, unique, range, accepted_values | Yes (95%) | Yes |
| Gold | None | Custom SQL, FK validation | Yes (99%) | Optional |

---

## 5.6 Environment Strategy

```
How complex is your deployment?
│
├─► Single machine, no environments needed
│   └─► No environment setup needed ✅
│
├─► Same pipelines, different infrastructure (local vs cloud)
│   └─► Inline environments: block ✅
│       Override: engine, connections
│
├─► Same pipelines, significantly different config
│   └─► External env.{env}.yaml files ✅
│       Full override power, supports imports
│
├─► Different pipelines per environment (dev sandbox)
│   └─► Per-environment pipelines ✅
│       Base config: shared connections (no pipelines)
│       env.dev.yaml: imports dev pipeline YAMLs
│       env.prod.yaml: imports prod pipeline YAMLs
│       --env flag is REQUIRED
│
└─► Multi-team, shared infrastructure
    └─► Per-environment pipelines + per-domain pipeline files
```

---

## 5.7 Layer Assignment

```
What layer does this logic belong in?
│
├─► Raw data ingestion (no transformation)
│   └─► Bronze
│       Write mode: append or append_once
│       Add metadata: yes
│       Format: parquet or delta
│
├─► Cleaning, deduplication, type casting, standardization
│   └─► Silver
│       Write mode: overwrite or upsert
│       Validation: yes
│       Quality gates: yes
│
├─► Business logic, aggregation, joining, reporting
│   └─► Gold
│       Write mode: overwrite
│       Pattern: fact, aggregation, dimension
│       Materialized: table or view
│
└─► Not sure?
    └─► Does it change the SEMANTIC MEANING of the data?
        ├─► YES → Gold
        └─► NO → Silver (cleaning) or Bronze (raw)
```

---

## 5.8 File Format Selection

```
What format should I use?
│
├─► Source files from external systems
│   └─► csv (read as-is, write to better format)
│
├─► Local analytics, single machine, no versioning needed
│   └─► parquet (fast, compressed, columnar)
│
├─► Production, need ACID / time travel / schema evolution
│   └─► delta (parquet + transaction log)
│
├─► API responses, nested data
│   └─► json (read), then write as parquet/delta
│
├─► SQL Server target
│   └─► sql_server (format for SQL Server writes)
│
└─► Not sure?
    └─► delta for anything that matters, parquet for throwaway
```

---

## 5.9 Connection + Auth Selection

```
Where is my data?
│
├─► Local filesystem
│   └─► type: local, base_path: ./data
│
├─► Azure Blob Storage / ADLS Gen2
│   │
│   ├─► Production with Key Vault?
│   │   └─► auth.mode: key_vault ✅ (most secure)
│   │
│   ├─► Production with Managed Identity?
│   │   └─► auth.mode: aad_msi ✅ (no secrets to manage)
│   │
│   ├─► Development with account key?
│   │   └─► auth.mode: account_key, key in env var ✅
│   │
│   └─► Temporary / external sharing?
│       └─► auth.mode: sas ✅
│
├─► SQL Server / Azure SQL
│   ├─► auth.mode: sql_login (username/password in env vars)
│   └─► auth.mode: aad_msi (Databricks/Azure)
│
└─► REST API
    ├─► auth: bearer / basic / api_key
    ├─► pagination: offset / cursor / link_header
    └─► rate_limit: requests_per_second
```

---

# Section 6: Feature Coverage Checklist

**Before shipping to production, review this list. Check everything you should be using.**

### Ingestion (Bronze)
```
[ ] add_metadata: true on all Bronze writes
[ ] append_once mode for idempotent reruns (with keys)
[ ] Contracts: freshness, row_count, schema on source nodes
[ ] skip_if_unchanged for slow-changing reference data
[ ] Incremental loading for large/growing sources
[ ] Archive options for bad records (Spark)
[ ] Format options set (header, delimiter, etc.)
```

### Transformation (Silver)
```
[ ] Validation tests on all critical output columns
[ ] Quality gates with appropriate pass rates
[ ] Quarantine for bad rows (with retention policy)
[ ] FK validation across dimension/fact tables
[ ] Delete detection if source hard-deletes records
[ ] Schema policy: evolve for unstable sources
[ ] Privacy/PII masking for sensitive columns
[ ] Deduplication where needed
```

### Business Logic (Gold)
```
[ ] Appropriate pattern (fact, aggregation, dimension)
[ ] Surrogate key generation where needed
[ ] Materialized views for logical models
[ ] Partitioning on large tables (low-cardinality columns)
[ ] Z-ordering on query columns (high-cardinality)
[ ] Cross-pipeline dependencies via inputs: where needed
```

### Operations
```
[ ] Environment configs (dev/prod at minimum)
[ ] Secrets in environment variables (never hardcoded)
[ ] All env vars documented (Section 1.6 env var roster)
[ ] Retry enabled with exponential backoff
[ ] Alerts on failure (Slack/Teams/webhook)
[ ] Alert throttling configured (avoid alert storms)
[ ] Stories enabled for audit trail
[ ] Story max_sample_rows: 0 in production (security)
[ ] System catalog with environment tags
[ ] System catalog sync_to for production observability
[ ] Logging at INFO level (structured for log aggregators)
[ ] Tags for selective execution (daily, hourly, critical)
[ ] Pipeline owners set (shows in alerts/stories)
[ ] Freshness SLAs on critical pipelines
```

### Performance
```
[ ] Delta format for production tables
[ ] Partitioning on large tables
[ ] Auto-optimize on frequently written tables
[ ] skip_null_profiling for large DataFrames
[ ] skip_catalog_writes for high-throughput Bronze
[ ] skip_run_logging if catalog overhead is too high
[ ] Caching for heavily-reused datasets
[ ] Bulk copy for large SQL Server writes
[ ] Spark config tuned (shuffle partitions, adaptive query)
```

### Documentation
```
[ ] Pipeline descriptions and layer tags
[ ] Node descriptions
[ ] Explanations on complex transformations
[ ] Runbook URLs on critical nodes
[ ] Column metadata (data dictionary) on Silver + Gold tables
[ ] Sensitive column marking for stories
[ ] Story docs generation enabled (README, node cards)
[ ] Variables documented (Section 1.6 variables roster)
```

---

# Section 7: Quick YAML Templates

## 7.1 Minimal Bronze Node

```yaml
- name: raw_orders
  description: "Ingest orders from landing zone"
  read:
    connection: source
    format: csv
    path: landing/orders.csv
  write:
    connection: lake
    format: delta
    path: bronze/orders
    mode: append_once
    add_metadata: true
    options:
      keys: [order_id]
```

## 7.2 Silver Node with Validation

```yaml
- name: clean_orders
  description: "Clean and validate orders"
  depends_on: [raw_orders]
  transform:
    steps:
      - sql: "SELECT * FROM raw_orders WHERE order_id IS NOT NULL"
      - function: clean_text
        params: { columns: [customer_name], case: title }
  validation:
    mode: fail
    tests:
      - type: not_null
        columns: [order_id, customer_id]
      - type: unique
        columns: [order_id]
      - type: range
        column: amount
        min: 0
    gate:
      require_pass_rate: 0.95
      on_fail: abort
    quarantine:
      connection: lake
      path: quarantine/orders
  write:
    connection: lake
    format: delta
    path: silver/orders
    mode: overwrite
```

## 7.3 SCD2 Dimension

```yaml
- name: dim_customer
  description: "Customer dimension with history tracking"
  depends_on: [clean_customers]
  transformer: scd2
  params:
    keys: [customer_id]
    tracked_columns: [name, email, address, tier]
    target_connection: lake
    target_path: gold/dim_customer
```

## 7.4 Fact Table

```yaml
- name: fact_sales
  description: "Sales fact with dimension lookups"
  depends_on: [clean_orders, dim_customer, dim_product, dim_date]
  transformer: fact
  params:
    measures: [quantity, amount, discount]
    foreign_keys:
      customer_sk:
        dimension: dim_customer
        key: customer_id
      product_sk:
        dimension: dim_product
        key: product_id
      date_sk:
        dimension: dim_date
        key: order_date
        dimension_key: date_key
  write:
    connection: lake
    format: delta
    path: gold/fact_sales
    mode: overwrite
    partition_by: [order_year]
    zorder_by: [customer_sk]
```

## 7.5 Aggregation (Gold)

```yaml
- name: daily_sales_summary
  description: "Daily revenue by region"
  depends_on: [fact_sales]
  transformer: aggregation
  params:
    group_by: [order_date, region]
    aggregations:
      total_orders: { column: order_id, function: count }
      total_revenue: { column: amount, function: sum }
      avg_order_value: { column: amount, function: avg }
  write:
    connection: lake
    format: delta
    path: gold/daily_sales_summary
    mode: overwrite
```

## 7.6 SQL Server Merge

```yaml
- name: sync_to_sql
  description: "Merge fact table to SQL Server"
  depends_on: [fact_sales]
  write:
    connection: azure_sql
    format: sql_server
    table: analytics.fact_sales
    mode: merge
    merge_keys: [order_id]
    merge_options:
      update_condition: "source._hash_diff != target._hash_diff"
      audit_cols:
        created_col: created_ts
        updated_col: updated_ts
      auto_create_table: true
      auto_create_schema: true
```

## 7.7 Complete Minimal Project

```yaml
project: "MyProject"
engine: pandas

connections:
  local:
    type: local
    base_path: ./data

story:
  connection: local
  path: stories

system:
  connection: local
  path: _odibi_system

pipelines:
  - pipeline: bronze
    layer: bronze
    nodes:
      - name: raw_data
        read:
          connection: local
          format: csv
          path: landing/data.csv
        write:
          connection: local
          format: parquet
          path: bronze/data
          mode: overwrite
```

---

# Section 8: Pipeline Planning Worksheet (Blank)

**Print this page. Fill in with pencil. One per node.**

```
┌──────────────────────────────────────────────────────────────────────┐
│ NODE:  ________________________    PIPELINE: ______________________ │
│ LAYER: [ ] bronze  [ ] silver  [ ] gold                            │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│ DEPENDS ON: _______________________________________________________ │
│                                                                      │
│ ─── READ ───────────────────────────────────────────────────────────│
│ Connection: ________________  Format: ________________              │
│ Path/Table: ________________________________________________        │
│ Incremental: [ ] no  [ ] rolling_window  [ ] stateful               │
│   Column: ________________  Lookback: ______ [ ]h [ ]d [ ]w        │
│                                                                      │
│ ─── TRANSFORM ──────────────────────────────────────────────────────│
│ Transformer: ________________  OR  SQL: ________________________    │
│ Params: ____________________________________________________________│
│ Step 2: ____________________________________________________________│
│ Step 3: ____________________________________________________________│
│                                                                      │
│ ─── CONTRACTS (pre-transform) ──────────────────────────────────────│
│ [ ] freshness  col: ____________ max_age: ________                  │
│ [ ] schema     [ ] row_count min: ____ max: ____                    │
│ [ ] volume_drop  [ ] distribution  [ ] not_null  [ ] unique         │
│                                                                      │
│ ─── VALIDATE (post-transform) ─────────────────────────────────────│
│ [ ] not_null  cols: ________________________________________________│
│ [ ] unique    cols: ________________________________________________│
│ [ ] range     col: ____________ min: ________ max: ________         │
│ [ ] accepted  col: ____________ values: ________________________    │
│ [ ] regex     col: ____________ pattern: _______________________    │
│ [ ] custom_sql condition: _______________________________________   │
│ [ ] freshness  [ ] schema  [ ] distribution  [ ] row_count          │
│ [ ] volume_drop                                                      │
│ Gate pass rate: ________%  On fail: [ ] abort  [ ] warn  [ ] valid  │
│ Quarantine: [ ] no  [ ] yes  path: _____________________________    │
│                                                                      │
│ ─── WRITE ──────────────────────────────────────────────────────────│
│ Connection: ________________  Format: ________________              │
│ Path/Table: ________________________________________________        │
│ Mode: [ ] overwrite  [ ] append  [ ] upsert  [ ] append_once       │
│       [ ] merge (→ Section 4.3)                                     │
│ Partition by: __________________________________________________    │
│ Z-order by:   __________________________________________________    │
│ [ ] add_metadata  [ ] skip_if_unchanged  [ ] merge_schema           │
│ [ ] auto_optimize                                                    │
│                                                                      │
│ ─── EXTRAS ─────────────────────────────────────────────────────────│
│ [ ] schema_policy (§3.7)  [ ] privacy/PII (§3.7)                    │
│ [ ] streaming (§4.2)  [ ] delete_detection (§4.5)                   │
│ [ ] multi-source inputs (§4.1)  [ ] archive bad records (§4.6)      │
│ [ ] SQL Server merge (§4.3)  [ ] SQL Server overwrite (§4.4)        │
│ Tags: __________________________  Cache: [ ] yes  [ ] no            │
│                                                                      │
│ NOTES: _____________________________________________________________│
│ ____________________________________________________________________│
│ ____________________________________________________________________│
└──────────────────────────────────────────────────────────────────────┘
```

---

*Built for the solo data engineer who wants to make pipeline building mundane.*
*Fill it out → type it in → run it → move on to what matters.*
