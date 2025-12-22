# The Odibi Playbook

A problem-first guide to data engineering with Odibi. Find your problem, get the solution.

---

## New to Odibi?

If you're just getting started:

1. **[Getting Started Tutorial](../tutorials/getting_started.md)** — Build your first pipeline in 10 minutes
2. **[Bronze Layer Tutorial](../tutorials/bronze_layer.md)** — Common ingestion problems solved
3. **[Silver Layer Tutorial](../tutorials/silver_layer.md)** — Cleaning and deduplication
4. **[Gold Layer Tutorial](../tutorials/gold_layer.md)** — Building star schemas

Then come back here when you need to solve a specific problem.

---

## How to Use This Playbook

1. **Find your layer** (Bronze → Silver → Gold)
2. **Find your problem**
3. **Follow the link** to implementation details

---

## Bronze Layer: Ingestion

*"Get data from sources into your lakehouse reliably."*

### File-Based Ingestion

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| Load all files from a folder | Append-only raw ingestion | `read` → `write` | [Append-Only Raw](../patterns/append_only_raw.md) |
| Only process new files since last run | Time-based lookback | `incremental.mode: rolling_window` | [Incremental Loading](../patterns/incremental_stateful.md) |
| Track exact high-water mark | Stateful HWM tracking | `incremental.mode: stateful` | [Incremental Loading](../patterns/incremental_stateful.md) |
| Files have inconsistent schemas | Schema evolution handling | `write.options.mergeSchema: true` | [Schema Tracking](../features/schema_tracking.md) |
| Malformed records break the pipeline | Bad record archiving | `read.options.badRecordsPath` | [YAML Reference](../reference/yaml_schema.md#readconfig) |
| Need to reprocess a time window | Windowed reprocessing | Rolling window config | [Windowed Reprocess](../patterns/windowed_reprocess.md) |

### Database Ingestion

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| Full table extract from SQL | SQL read | `read.format: jdbc` | [Getting Started](../tutorials/getting_started.md) |
| Only extract new/changed rows | Stateful HWM incremental | `incremental.mode: stateful` | [Incremental Loading](../patterns/incremental_stateful.md) |
| CDC / change data capture | Merge incoming changes | `transformer: merge` | [Merge/Upsert](../patterns/merge_upsert.md) |
| Source sends duplicates | Deduplicate on ingest | `transformer: deduplicate` | [YAML Reference](../reference/yaml_schema.md#deduplicateparams) |

### Ingestion Quality (Contracts)

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| Fail fast if source is empty | Row count contract | `contracts: [{type: row_count, min: 1}]` | [YAML Reference](../reference/yaml_schema.md#rowcounttest) |
| Detect unexpected volume drops | Volume drop check | `contracts: [{type: volume_drop, threshold: 0.5}]` | [YAML Reference](../reference/yaml_schema.md#volumedroptest) |
| Ensure required columns exist | Schema contract | `contracts: [{type: schema, strict: true}]` | [YAML Reference](../reference/yaml_schema.md#schemacontract) |
| Ensure no NULL values | Not-null check | `contracts: [{type: not_null, columns: [...]}]` | [YAML Reference](../reference/yaml_schema.md#notnulltest) |
| Ensure unique keys | Uniqueness check | `contracts: [{type: unique, columns: [...]}]` | [YAML Reference](../reference/yaml_schema.md#uniquetest) |
| Validate enum values | Accepted values | `contracts: [{type: accepted_values, column: status, values: [A,B,C]}]` | [YAML Reference](../reference/yaml_schema.md#acceptedvaluestest) |
| Validate numeric range | Range check | `contracts: [{type: range, column: age, min: 0, max: 150}]` | [YAML Reference](../reference/yaml_schema.md#rangetest) |
| Validate format (email, ID) | Regex match | `contracts: [{type: regex_match, column: email, pattern: "..."}]` | [YAML Reference](../reference/yaml_schema.md#regexmatchtest) |
| Custom SQL validation | Custom SQL | `contracts: [{type: custom_sql, condition: "amount > 0"}]` | [YAML Reference](../reference/yaml_schema.md#customsqltest) |
| Detect data drift | Distribution check | `contracts: [{type: distribution, column: price, metric: mean}]` | [YAML Reference](../reference/yaml_schema.md#distributioncontract) |
| Ensure data freshness | Freshness check | `contracts: [{type: freshness, column: updated_at, max_age: "24h"}]` | [YAML Reference](../reference/yaml_schema.md#freshnesscontract) |

---

## Silver Layer: Transformation & Conforming

*"Clean, deduplicate, and model your data."*

### Deduplication

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| Remove duplicates within a batch | Deduplicate transformer | `transformer: deduplicate` | [YAML Reference](../reference/yaml_schema.md#deduplicateparams) |
| Keep latest record per key | Dedupe with ordering | `params: {keys: [...], order_by: "updated_at DESC"}` | [YAML Reference](../reference/yaml_schema.md#deduplicateparams) |

### Slowly Changing Dimensions (SCD)

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| Overwrite dimension changes (no history) | SCD Type 1 | `transformer: dimension` + `params.scd_type: 1` | [Dimension Pattern](../patterns/dimension.md) |
| Track dimension changes over time | SCD Type 2 | `transformer: dimension` + `params.scd_type: 2` | [SCD2 Pattern](../patterns/scd2.md) |
| Generate surrogate keys | Dimension pattern | `params.surrogate_key: customer_sk` | [Dimension Pattern](../patterns/dimension.md) |
| Handle unknown dimension members | Unknown member row | `params.unknown_member: true` | [Dimension Pattern](../patterns/dimension.md) |

### Data Quality

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| Validate data after transformation | Validation tests | `validation.tests: [...]` | [Quality Gates](../features/quality_gates.md) |
| Route bad rows for review | Quarantine | `validation.quarantine: {...}` | [Quarantine](../features/quarantine.md) |
| Require 95% pass rate | Quality gate threshold | `validation.gate.require_pass_rate: 0.95` | [Quality Gates](../features/quality_gates.md) |
| Check referential integrity | FK validation | FK Validator API | [FK Validation](../validation/fk.md) |

### Merge & Upsert

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| Upsert into target table | Merge transformer | `transformer: merge` | [Merge/Upsert](../patterns/merge_upsert.md) |
| Detect deleted records | Delete detection | `delete_detection.mode: sql_compare` | [YAML Reference](../reference/yaml_schema.md#deletedetectionconfig) |
| Soft delete instead of hard delete | Soft delete flag | `delete_detection.soft_delete_col: is_deleted` | [YAML Reference](../reference/yaml_schema.md#deletedetectionconfig) |

### Custom Transformations

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| Apply SQL transformations | SQL steps | `transform.steps: [{sql: "..."}]` | [Writing Transformations](../guides/writing_transformations.md) |
| Apply Python functions | Function steps | `transform.steps: [{function: "..."}]` | [Writing Transformations](../guides/writing_transformations.md) |
| Chain multiple operations | Transform pipeline | Multiple steps in sequence | [Writing Transformations](../guides/writing_transformations.md) |

### Built-in Transformers (40+)

Odibi has 40+ built-in transformers. Use them in `transform.steps`:

```yaml
transform:
  steps:
    - transformer: filter_rows
      params: {condition: "amount > 0"}
    - transformer: derive_columns
      params: {columns: {total: "qty * price"}}
```

| Category | Transformers |
|----------|--------------|
| **Filtering** | `filter_rows`, `distinct`, `sample`, `limit` |
| **Columns** | `derive_columns`, `select_columns`, `drop_columns`, `rename_columns`, `cast_columns` |
| **Text** | `clean_text`, `trim_whitespace`, `regex_replace`, `split_part`, `concat_columns` |
| **Dates** | `extract_date_parts`, `date_add`, `date_trunc`, `date_diff`, `convert_timezone` |
| **Nulls** | `fill_nulls`, `coalesce_columns` |
| **Relational** | `join`, `union`, `aggregate`, `pivot`, `unpivot` |
| **Window** | `window_calculation` (rank, sum, lag, lead, etc.) |
| **JSON/Nested** | `parse_json`, `normalize_json`, `explode_list_column`, `unpack_struct` |
| **Keys/Hashing** | `generate_surrogate_key`, `hash_columns` |
| **Advanced** | `sessionize`, `validate_and_flag`, `dict_based_mapping`, `split_events_by_period` |
| **Validation** | `cross_check` (compare two DataFrames) |
| **Schema** | `normalize_schema`, `normalize_column_names`, `add_prefix`, `add_suffix` |
| **Sorting** | `sort` |
| **Values** | `replace_values`, `case_when` |

**Full reference:** [YAML Schema - Transformers](../reference/yaml_schema.md#transformer-catalog)

---

## Gold Layer: Consumption & Analytics

*"Build fact tables, aggregations, and semantic layers for BI."*

### Fact Tables

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| Build fact table with SK lookups | Fact pattern | `transformer: fact` | [Fact Pattern](../patterns/fact.md) |
| Handle orphan records (missing dims) | Orphan handling | `params.orphan_handling: unknown` | [Fact Pattern](../patterns/fact.md) |
| Quarantine orphans for review | Orphan quarantine | `params.orphan_handling: quarantine` | [Fact Pattern](../patterns/fact.md) |
| Validate grain (no duplicates) | Grain validation | `params.grain: [order_id]` | [Fact Pattern](../patterns/fact.md) |

### Aggregations

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| Pre-aggregate metrics | Aggregation pattern | `transformer: aggregation` | [Aggregation Pattern](../patterns/aggregation.md) |
| Incremental aggregation | Merge aggregates | `params.incremental: true` | [Aggregation Pattern](../patterns/aggregation.md) |

### Reference Data

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| Generate date dimension | Date dimension pattern | `transformer: date_dimension` | [Date Dimension](../patterns/date_dimension.md) |
| Skip unchanged reference data | Skip-if-unchanged | `write.skip_if_unchanged: true` | [Skip If Unchanged](../patterns/skip_if_unchanged.md) |

### Semantic Layer

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| Define reusable metrics | Semantic config | `semantic.metrics: [...]` | [Semantics](../semantics/index.md) |
| Define dimensions for BI | Semantic config | `semantic.dimensions: [...]` | [Semantics](../semantics/index.md) |
| Query metrics dynamically | Semantic Query | `SemanticQuery.execute("revenue BY region")` | [Query API](../semantics/query.md) |
| Pre-compute aggregations | Materialization | `Materializer.execute("monthly_revenue")` | [Materialization](../semantics/materialize.md) |
| Incremental materialization | Incremental materializer | `IncrementalMaterializer` | [Materialization](../semantics/materialize.md) |
| Schedule materializations | Materialization schedules | `materializations[].schedule: "@daily"` | [Materialization](../semantics/materialize.md) |
| List defined materializations | Materializer API | `materializer.list_materializations()` | [Materialization](../semantics/materialize.md) |

---

## Cross-Cutting Concerns

### Performance

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| Pipeline is slow | Performance tuning | `performance` config | [Performance Tuning](../guides/performance_tuning.md) |
| Cache hot DataFrames | Caching | `cache: true` | [YAML Reference](../reference/yaml_schema.md#nodeconfig) |
| Skip expensive profiling | Skip null profiling | `skip_null_profiling: true` | [Performance Tuning](../guides/performance_tuning.md) |

### Operations

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| Run only specific nodes | Tag-based execution | `tags: [daily]` + `odibi run --tag daily` | [CLI Guide](../guides/cli_master_guide.md) |
| Retry on transient failures | Retry config | `retry: {enabled: true}` | [YAML Reference](../reference/yaml_schema.md#retryconfig) |
| Alert on failure | Alerting | `alerts: [{type: slack, ...}]` | [Alerting](../features/alerting.md) |
| Track data lineage | OpenLineage | `lineage: {url: "..."}` | [Lineage](../features/lineage.md) |
| Export traces to observability platform | OpenTelemetry | `OTEL_EXPORTER_OTLP_ENDPOINT` env var | [Observability](../features/observability.md) |

### Multi-Pipeline

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| Reference output from another pipeline | Cross-pipeline deps | `$pipeline.node` syntax | [Cross-Pipeline Dependencies](../features/cross-pipeline-dependencies.md) |
| Environment-specific config | Environments | `environments: {prod: {...}}` | [Environments](../guides/environments.md) |

### Documentation & Observability

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| Track pipeline execution history | Stories | `story: {connection: ..., path: stories/}` | [Stories](../features/stories.md) |
| Generate HTML/JSON/Markdown reports | Story renderers | Automatic after each run | [Stories](../features/stories.md) |
| See row counts, duration, schema changes | Story metadata | Auto-captured metrics | [Stories](../features/stories.md) |
| Track schema evolution over time | System Catalog | `system: {connection: ..., path: _system}` | [Catalog](../features/catalog.md) |
| Query run history and stats | Catalog API | `odibi catalog runs`, `odibi catalog stats` | [Catalog](../features/catalog.md) |
| View HWM state | Catalog state | `odibi catalog state config.yaml` | [Catalog](../features/catalog.md) |
| Audit config changes | Version hashing | Auto-detects config drift | [Catalog](../features/catalog.md) |
| Compare Delta table versions | Delta diagnostics | `get_delta_diff(table, v1, v2)` | [Diagnostics](../features/diagnostics.md) |
| Detect data/schema drift | Drift detection | `detect_drift(table, threshold)` | [Diagnostics](../features/diagnostics.md) |
| Compare pipeline runs | Run comparison | `diff_runs(run_a, run_b)` | [Diagnostics](../features/diagnostics.md) |

### Orchestration

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| Export to Airflow | Airflow DAG generation | `AirflowExporter` | [Orchestration](../features/orchestration.md) |
| Export to Dagster | Dagster asset generation | `DagsterExporter` | [Orchestration](../features/orchestration.md) |
| CLI for orchestration export | Export command | `odibi export airflow config.yaml` | [Orchestration](../features/orchestration.md) |

### CLI Commands

| Problem | Solution | Command | Docs |
|---------|----------|---------|------|
| **Scaffold & Setup** ||||
| Create new project | Init wizard | `odibi init-pipeline my_project` | [CLI Guide](../guides/cli_master_guide.md) |
| Use a template | Template init | `odibi init-pipeline my_project --template local-medallion` | [CLI Guide](../guides/cli_master_guide.md) |
| Generate API docs | Docs generator | `odibi docs` | [CLI Guide](../guides/cli_master_guide.md) |
| **Execution** ||||
| Run a pipeline | Execute nodes | `odibi run config.yaml` | [CLI Guide](../guides/cli_master_guide.md) |
| Run specific pipeline | Filter by name | `odibi run config.yaml --pipeline orders` | [CLI Guide](../guides/cli_master_guide.md) |
| Run specific node | Single node | `odibi run config.yaml --node transform_orders` | [CLI Guide](../guides/cli_master_guide.md) |
| Dry run (no writes) | Simulate | `odibi run config.yaml --dry-run` | [CLI Guide](../guides/cli_master_guide.md) |
| Resume from failure | Skip successful nodes | `odibi run config.yaml --resume` | [CLI Guide](../guides/cli_master_guide.md) |
| Run nodes in parallel | Parallel execution | `odibi run config.yaml --parallel --workers 8` | [CLI Guide](../guides/cli_master_guide.md) |
| **Validation & Testing** ||||
| Validate config syntax | Config check | `odibi validate config.yaml` | [CLI Guide](../guides/cli_master_guide.md) |
| Run transform unit tests | Test runner | `odibi test tests/` | [Testing](../guides/testing.md) |
| Update test snapshots | Snapshot mode | `odibi test --snapshot` | [Testing](../guides/testing.md) |
| Diagnose issues | Doctor check | `odibi doctor config.yaml` | [CLI Guide](../guides/cli_master_guide.md) |
| **Visualization & Inspection** ||||
| Visualize dependencies | DAG graph | `odibi graph config.yaml` | [CLI Guide](../guides/cli_master_guide.md) |
| Output as Mermaid | Mermaid format | `odibi graph config.yaml --format mermaid` | [CLI Guide](../guides/cli_master_guide.md) |
| View schema history | Schema diff | `odibi schema history table --config config.yaml` | [Schema Tracking](../features/schema_tracking.md) |
| Compare schema versions | Schema diff | `odibi schema diff table --from-version 3 --to-version 5` | [Schema Tracking](../features/schema_tracking.md) |
| **Stories & Reporting** ||||
| Generate story report | Story gen | `odibi story generate config.yaml` | [Stories](../features/stories.md) |
| Compare two runs | Story diff | `odibi story diff run1.json run2.json` | [Stories](../features/stories.md) |
| List story files | Story list | `odibi story list` | [Stories](../features/stories.md) |
| **Lineage** ||||
| Emit OpenLineage events | Lineage emit | `odibi lineage emit config.yaml` | [Lineage](../features/lineage.md) |
| View lineage graph | Lineage view | `odibi lineage view config.yaml` | [Lineage](../features/lineage.md) |
| **Deployment & Export** ||||
| Deploy to System Catalog | Catalog deploy | `odibi deploy config.yaml` | [Catalog](../features/catalog.md) |
| Export to Airflow | Airflow DAG | `odibi export airflow config.yaml` | [Orchestration](../features/orchestration.md) |
| Export to Dagster | Dagster assets | `odibi export dagster config.yaml` | [Orchestration](../features/orchestration.md) |
| **Secrets & Config** ||||
| Manage secrets | Secret commands | `odibi secrets set KEY VALUE` | [Secrets](../guides/secrets.md) |
| List secrets | Secret list | `odibi secrets list` | [Secrets](../guides/secrets.md) |

### Connections

| Problem | Solution | Connection Type | Docs |
|---------|----------|-----------------|------|
| Read/write local files | Local filesystem | `type: local` | [Connections](../features/connections.md) |
| Read/write Azure Blob/ADLS | Azure storage | `type: azure_blob` | [Azure Setup](../guides/setup_azure.md) |
| Read/write Delta tables | Delta Lake | `type: delta` | [Connections](../features/connections.md) |
| Read from SQL Server | JDBC connection | `type: sql_server` | [Connections](../features/connections.md) |
| Call HTTP APIs | REST endpoints | `type: http` | [Connections](../features/connections.md) |

### Advanced Features

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| Query old data versions | Delta Time Travel | `read.time_travel.version` or `timestamp` | [YAML Reference](../reference/yaml_schema.md#timetravelconfig) |
| Add load metadata columns | Write metadata | `write.add_metadata: true` | [YAML Reference](../reference/yaml_schema.md#writemetadataconfig) |
| Add _extracted_at, _source_file | Selective metadata | `write.add_metadata: {extracted_at: true}` | [YAML Reference](../reference/yaml_schema.md#writemetadataconfig) |
| Partition output for performance | Partition by columns | `write.partition_by: [year, month]` | [YAML Reference](../reference/yaml_schema.md#writeconfig) |
| Run SQL before transform | Pre-SQL hooks | `pre_sql: ["CREATE TEMP VIEW..."]` | [YAML Reference](../reference/yaml_schema.md#nodeconfig) |
| Run SQL after write | Post-SQL hooks | `post_sql: ["OPTIMIZE table", "VACUUM"]` | [YAML Reference](../reference/yaml_schema.md#nodeconfig) |
| Control error handling | Error strategy | `on_error: fail_fast|fail_later|ignore` | [YAML Reference](../reference/yaml_schema.md#nodeconfig) |
| Depend on other nodes | Node dependencies | `depends_on: [node1, node2]` | [Pipelines](../features/pipelines.md) |
| Enable/disable nodes | Conditional execution | `enabled: false` | [Pipelines](../features/pipelines.md) |
| Route bad rows to quarantine | Quarantine config | `validation.quarantine: {connection: ..., path: ...}` | [Quarantine](../features/quarantine.md) |
| Compare two DataFrames | Cross-check validation | `transformer: cross_check` | [YAML Reference](../reference/yaml_schema.md) |

---

## Quick Decision Tree

```
I need to...
│
├─► INGEST data
│   ├─► From files → read + write (append mode)
│   ├─► From database → read.format: jdbc
│   ├─► Only new data → incremental.mode: stateful
│   └─► Skip if unchanged → write.skip_if_unchanged: true
│
├─► CLEAN/TRANSFORM data
│   ├─► Remove duplicates → transformer: deduplicate
│   ├─► Track history → transformer: dimension (scd_type: 2)
│   ├─► Upsert/merge → transformer: merge
│   ├─► Validate quality → validation.tests: [...]
│   ├─► Custom logic → transform.steps: [{sql: ...}]
│   └─► Built-in ops → filter_rows, derive_columns, join, etc.
│
├─► BUILD ANALYTICS
│   ├─► Fact table → transformer: fact
│   ├─► Aggregation → transformer: aggregation
│   ├─► Date table → transformer: date_dimension
│   └─► Metrics layer → semantic.metrics: [...]
│
├─► MONITOR & DEBUG
│   ├─► Execution history → story config (auto HTML reports)
│   ├─► Compare runs → odibi story diff run1.json run2.json
│   ├─► Schema history → system catalog (auto tracking)
│   ├─► Run stats → odibi catalog stats
│   ├─► Diagnose → odibi doctor
│   └─► View lineage → odibi lineage view
│
├─► OPERATE
│   ├─► Run subset → odibi run --tag daily
│   ├─► Resume → odibi run --resume
│   ├─► Parallelize → odibi run --parallel
│   ├─► Handle failures → retry + alerts
│   ├─► Deploy to catalog → odibi deploy config.yaml
│   └─► Export to Airflow → odibi export airflow
│
├─► EXTEND
│   ├─► Custom connection → register_connection_factory()
│   ├─► Custom transform → @transform decorator
│   └─► Package plugins → pyproject.toml entry points
│
├─► TEST
│   ├─► Unit test node → pipeline.run_node(mock_data=...)
│   ├─► CLI test runner → odibi test tests/
│   ├─► Assert equality → assert_frame_equal()
│   ├─► Generate data → generate_sample_data()
│   └─► Deterministic tests → SourcePoolConfig
│
├─► AUTOMATE
│   ├─► Python API → PipelineManager.from_yaml()
│   ├─► Web dashboard → odibi ui config.yaml
│   └─► Access state → StateManager API
│
└─► GET STARTED
    ├─► New project → odibi init-pipeline my_project
    ├─► From template → odibi init-pipeline --template local-medallion
    └─► Generate docs → odibi docs
```

---

## Developer Experience

*"Extend, test, and automate Odibi."*

### Python API

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| Run pipelines programmatically | Python execution | `PipelineManager.from_yaml()` | [Python API](../guides/python_api_guide.md) |
| Run a specific pipeline | Select pipeline | `manager.run("pipeline_name")` | [Python API](../guides/python_api_guide.md) |
| Dry run without writing | Simulation | `manager.run(dry_run=True)` | [Python API](../guides/python_api_guide.md) |
| Resume from failure | Skip completed | `manager.run(resume_from_failure=True)` | [Python API](../guides/python_api_guide.md) |
| Access story metadata in code | Story API | `result.story_path` | [Python API](../guides/python_api_guide.md) |
| Compare pipeline runs | Diff API | `diff_runs(run_a, run_b)` | [Python API](../guides/python_api_guide.md) |
| Compare Delta table versions | Delta diff | `get_delta_diff(path, v1, v2)` | [Python API](../guides/python_api_guide.md) |

### Plugins & Extensibility

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| Need custom connection (Snowflake, Postgres) | Connection plugin | `register_connection_factory()` | [Plugins](../features/plugins.md) |
| Need custom transformation | Transform plugin | `@transform` decorator | [Plugins](../features/plugins.md) |
| Validate transform parameters | Pydantic models | `@transform(param_model=MyParams)` | [Plugins](../features/plugins.md) |
| Auto-load custom code | Auto-discovery | `transforms.py`, `plugins.py` files | [Plugins](../features/plugins.md) |
| Package plugins for distribution | Entry points | `pyproject.toml` entry points | [Plugins](../features/plugins.md) |
| List registered functions | Registry API | `FunctionRegistry.list_functions()` | [Plugins](../features/plugins.md) |

### Testing

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| Unit test a single node | Mock input data | `pipeline.run_node(mock_data=...)` | [Python API](../guides/python_api_guide.md#unit-testing-nodes) |
| Assert DataFrame equality | Test assertions | `assert_frame_equal(left, right)` | [Testing](../guides/testing.md) |
| Assert schema match | Schema assertions | `assert_schema_equal(left, right)` | [Testing](../guides/testing.md) |
| Generate sample test data | Fixtures | `generate_sample_data(rows=100)` | [Testing](../guides/testing.md) |
| Create temp directories | Fixtures | `with temp_directory() as d:` | [Testing](../guides/testing.md) |
| Deterministic replay tests | Source Pools | `SourcePoolConfig` | [Source Pools](../source_pools_design.md) |
| Test data with nulls/unicode | Data quality pools | `DataQuality.MESSY` | [Source Pools](../source_pools_design.md) |
| Hash-verify frozen test data | Integrity manifest | `IntegrityManifest` | [Source Pools](../source_pools_design.md) |

### Web UI

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| View pipeline status in browser | Web dashboard | `odibi ui config.yaml` | [UI](../features/ui.md) |
| Browse story reports visually | Stories viewer | `/stories` endpoint | [UI](../features/ui.md) |
| View current config | Config viewer | `/config` endpoint | [UI](../features/ui.md) |

---

## Advanced Internals

*"Low-level APIs for power users and contributors."*

### State Management

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| Choose state backend | Backend config | `LocalJSONStateBackend` vs `CatalogStateBackend` | [State](../features/state.md) |
| Access HWM programmatically | State API | `state_mgr.get_hwm(key)` | [State](../features/state.md) |
| Set HWM programmatically | State API | `state_mgr.set_hwm(key, value)` | [State](../features/state.md) |
| Batch HWM updates (parallel pipelines) | Efficient writes | `state_mgr.set_hwm_batch(updates)` | [State](../features/state.md) |
| Handle Delta concurrent writes | Retry logic | Auto-retry with exponential backoff | [State](../features/state.md) |

### Context API

| Problem | Solution | Odibi Feature | Docs |
|---------|----------|---------------|------|
| Access engine-specific context | Context classes | `PandasContext`, `SparkContext`, `PolarsContext` | [Context API](../api/context.md) |
| Get current DataFrame in transform | Context get | `context.get("node_name")` | [Context API](../api/context.md) |
| Register DataFrame for downstream | Context set | `context.set("name", df)` | [Context API](../api/context.md) |
| Access Spark session | Engine context | `context.engine_context.spark` | [Context API](../api/context.md) |
| Execute SQL in context | SQL execution | `context.sql("SELECT ...")` | [Context API](../api/context.md) |

---

## Engines

Odibi supports 3 execution engines with feature parity:

| Engine | Best For | Config |
|--------|----------|--------|
| `pandas` | Small data (<1GB), local dev | `engine: pandas` |
| `polars` | Medium data (1-10GB), fast local | `engine: polars` |
| `spark` | Big data (>10GB), Delta Lake, Databricks | `engine: spark` |

**All patterns and transformers work on all engines.**

See: [Engine Parity Table](../reference/PARITY_TABLE.md), [Spark Tutorial](../tutorials/spark_engine.md)

---

## Gaps / Coming Soon

<!-- TODO: Add docs for these -->
| Pattern | Status | Notes |
|---------|--------|-------|
| Streaming ingestion | Partial | See [Spark Tutorial](../tutorials/spark_engine.md#streaming-ingestion) |
| Full CDC patterns | Partial | Merge covers basics, need CDC-specific guide |
| Data vault patterns | Not yet | Hub/Link/Satellite patterns |
| Slowly Changing Dimension Type 3 | Not yet | Current + previous value |

---

## See Also

- [Getting Started](../tutorials/getting_started.md) — First pipeline tutorial
- [YAML Reference](../reference/yaml_schema.md) — Full configuration options
- [Python API Guide](../guides/python_api_guide.md) — Programmatic usage
- [Plugins & Extensibility](../features/plugins.md) — Custom connections & transforms
- [Source Pools Design](../source_pools_design.md) — Deterministic test data
- [Glossary](../reference/glossary.md) — Terminology reference
- [Best Practices](../guides/best_practices.md) — Production patterns
