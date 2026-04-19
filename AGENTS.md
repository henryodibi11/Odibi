# Odibi - Agent Guide

## AI Quick Start

**Read [docs/THREAD_QUICK_START.md](docs/THREAD_QUICK_START.md) FIRST** — it has the full source layout with line counts, environment facts, test rules, and coverage commands. This saves 5-10 minutes of exploration per thread.

**For deep framework documentation, read [docs/ODIBI_DEEP_CONTEXT.md](docs/ODIBI_DEEP_CONTEXT.md) only when you need feature-level details.**

This 2,200+ line document covers:
- Runtime behavior (Spark temp views, Pandas/DuckDB, Polars)
- All 6 patterns (Dimension, Fact, SCD2, Merge, Aggregation, Date Dimension)
- 52+ transformers with examples
- Validation, quarantine, quality gates
- Connections (Local, ADLS, Azure SQL, HTTP, DBFS)
- Delta Lake features (partitioning, Z-ordering, VACUUM, schema evolution)
- System Catalog, OpenLineage, FK validation
- SQL Server writer, incremental loading modes
- Manufacturing transformers, testing utilities
- CLI reference, anti-patterns, extension points

---

## About This Project

**Creator:** Solo data engineer on an analytics team (only DE among data analysts)

**The Story:** I work in operations, not IT. I know the gaps and pain points of doing data work alone while spearheading change in my company. Odibi was born from needing unique solutions due to IT roadblocks and working without support. I've proven invaluable and more will be asked of me - I need tools that buy back time and freedom.

**Goals:**
- **Short term:** Make odibi stable and bug-free
- **Medium term:** Find gaps and improve coverage
- **Ultimate goal:** A framework so easy and powerful it gives me time back to focus on what matters

**Success Test:** Can you hand the docs to a business analyst and have them build a working pipeline without your help? If yes, the framework is truly self-service.

## Project Structure

```
odibi/              # Core framework - THE PRODUCT
├── engine/         # Spark, Pandas, Polars engines (must maintain parity)
├── patterns/       # SCD2, Merge, Aggregation, Dimension, Fact patterns
├── validation/     # Data quality engine, quarantine, FK validation
├── transformers/   # SCD, merge, delete detection, advanced transforms
├── semantics/      # Metrics, dimensions, materialization
tests/              # Test suite
stories/            # Business context and use cases
examples/           # Usage examples
docs/               # Documentation
.odibi/source_cache # Sample datasets for testing
```

## Commands

### Testing
```bash
pytest tests/unit/ -v               # Run unit tests (may hang — see below)
pytest tests/unit/test_X.py -v      # Run specific test file (preferred)
pytest --tb=short                   # Shorter tracebacks
python scripts/run_test_campaign.py # End-to-end pattern validation
.\scripts\run_coverage.ps1          # Run coverage in safe batches (recommended)
```

### Test Coverage Notes

**Do NOT flag Spark engine coverage as a problem.** The 4% coverage is misleading:
- Spark is tested via mock-based tests in `tests/integration/test_patterns_spark_mock.py`
- Spark is validated in production on Databricks
- CI skips real Spark tests (no JVM available)

The test strategy is: **mock logic in CI, validate behavior in Databricks prod.**

**Spark branch testing via mocks:** For modules with dual Spark/Pandas paths (e.g., `catalog.py`),
use `unittest.mock.MagicMock` for `SparkSession` to cover Spark branches without a JVM.
Set `spark=MagicMock()` and wire up chained return values (e.g., `spark.read.format().load().filter()...`).
This ensures all conditional branches are exercised in CI.

### Current Coverage Status (April 2026)
- `catalog.py` — **85% covered** (126 tests across 5 files). Both Pandas and Spark branches tested via mocks. Remaining uncovered: SQL Server queries, cleanup_orphans Spark paths, parquet fallback in _ensure_table. Not worth covering further (diminishing returns).
- ~25 pre-existing test failures in older catalog test files due to caplog logging capture issues (logger.warning → stderr, caplog doesn't capture). NOT caused by recent changes.

- `engine/pandas_engine.py` — **74% covered** (118 new tests across 3 files + existing tests). Covers read/write core, CSV retries, Excel patterns, generic upsert, write-to-target, all utility methods (anonymize, harmonize_schema, validate_schema/data, filter_greater_than, filter_coalesce, etc.). Remaining uncovered: Excel fastexcel/openpyxl closures, simulation reading, Delta maintenance ops (vacuum/history/restore/maintain), execute_sql, execute_operation. Diminishing returns.
  - `tests/unit/engine/test_pe_readwrite.py` (42 tests) — read(), write(), _write_iterator(), _handle_generic_upsert(), _write_to_target()
  - `tests/unit/engine/test_pe_utilities.py` (59 tests) — add_write_metadata, harmonize_schema, anonymize, count_nulls, validate_schema, _infer_avro_schema, validate_data, get_source_files, profile_nulls, filter_greater_than, filter_coalesce
  - `tests/unit/engine/test_pe_format_handling.py` (17 tests) — CSV retries, parquet list-of-files, JSON glob, Delta→DuckDB fallback, avro local/remote, Excel sheet patterns
- ~94 pre-existing test failures across `test_pandas_engine_core.py` and `test_pandas_engine_full_coverage.py` due to logging context pollution (tests pass individually but fail when run together). NOT caused by recent changes.

- `validation/engine.py` — **67% covered** (76 tests). Bug fixed: `isinstance(df, pyspark.sql.DataFrame)` → `except (ImportError, AttributeError)`. All 11 test types tested for both Pandas and Polars (eager + lazy). Remaining: Spark `_validate_spark` path (lines 366-570).
  - `tests/unit/validation/test_validation_engine.py` (76 tests) — NOT_NULL, UNIQUE, ACCEPTED_VALUES, ROW_COUNT, RANGE, REGEX_MATCH, CUSTOM_SQL, SCHEMA, FRESHNESS × Pandas + Polars, fail_fast, warn severity, edge cases
- `validation/quarantine.py` — **69% covered** (49 tests). All Pandas + Polars mask evaluation, split_valid_invalid, add_quarantine_metadata, _apply_sampling, write_quarantine, has_quarantine_tests. Remaining: Spark paths.
  - `tests/unit/validation/test_quarantine_coverage.py` (49 tests) — _evaluate_test_mask Pandas/Polars, split_valid_invalid, metadata columns, sampling, write_quarantine
- `validation/gate.py` — **94% covered** (32 tests). evaluate_gate, _check_row_count, _get_previous_row_count all covered. Remaining: Spark detection branch (3 lines).
  - `tests/unit/validation/test_gate_coverage.py` (32 tests) — pass rates, thresholds, row count checks, on_fail actions, catalog mocks

- `testing/source_pool.py` — **100% covered** (44 tests). All Pydantic models, enums, validators fully tested.
  - `tests/unit/testing/test_source_pool_coverage.py` (44 tests) — all enums, schemas, validators, SourcePoolIndex methods

- `cli/graph.py` — **66% covered** (16 tests). _generate_dot and _generate_mermaid fully covered. Remaining: graph_command (needs PipelineManager).
  - `cli/test.py` — **97% covered** (10+31 tests). load_test_files, slugify, run_test_case core + edge cases, test_command loop (path not found, no files, pass/fail, YAML errors, snapshot, parent traversal). Remaining: ~5 lines.
    - `tests/unit/cli/test_cli_test_coverage.py` (31 tests) — slugify, load_test_files edge cases, run_test_case (CSV, missing transforms, TypeError, snapshots), test_command full loop
  - `cli/init_pipeline.py` — **97% covered** (38 tests). Nearly fully covered including interactive prompts, force overwrite, all generated files.
  - `cli/secrets.py` — **63% covered** (15 tests). extract_env_vars, init_command, SimpleConnection, secrets_command dispatcher. Remaining: check_keyvault_access, validate_command Key Vault paths.
  - `cli/lineage.py` — **100% covered** (10+17 tests). All functions fully tested: _get_catalog_manager (all error paths + success), _lineage_upstream/downstream/impact (no data, json format, tree format, with affected tables/pipelines).
    - `tests/unit/cli/test_cli_lineage_coverage.py` (17 tests) — _get_catalog_manager, upstream, downstream, impact
  - `cli/schema.py` — **100% covered** (5+22 tests). All functions fully tested: _get_catalog_manager, _schema_history (no config, no catalog, no history, table/json format), _schema_diff (no versions, additions/removals/type changes).
    - `tests/unit/cli/test_cli_schema_coverage.py` (22 tests) — _get_catalog_manager, _schema_history, _schema_diff
  - All CLI test files:
    - `tests/unit/cli/test_cli_test_graph.py` (35 tests)
    - `tests/unit/cli/test_cli_schema_secrets_lineage.py` (45 tests)
    - `tests/unit/cli/test_cli_init_pipeline.py` (38 tests)

- `cli/main.py` — **98% covered** (37+13+20 tests). format_output, print_table, cmd_validate, cmd_doctor, cmd_doctor_path, cmd_discover, cmd_scaffold_project, cmd_scaffold_sql_pipeline, scaffold dispatch, all deferred-import command branches. Remaining: ~6 lines.
  - `tests/unit/cli/test_cli_main_coverage.py` (37 tests) — all pure functions, mock-based command tests
  - `tests/unit/cli/test_cli_main_remaining_coverage.py` (13 tests) — cmd_discover, cmd_scaffold_project, cmd_scaffold_sql_pipeline
  - `tests/unit/cli/test_cli_main_scaffold_coverage.py` (20 tests) — scaffold dispatch + deferred import branches
- `cli/catalog.py` — **94% covered** (51+26 tests). _format_table, _format_output, catalog_command dispatcher, all 7 query commands + _stats_command + _sync_command, _sync_status_command, _sync_purge_command. Remaining: ~27 lines (error paths in query commands).
  - `tests/unit/cli/test_cli_catalog_coverage.py` (51 tests) — formatters, dispatcher, all catalog queries with mock DataFrames
  - `tests/unit/cli/test_cli_catalog_sync_coverage.py` (26 tests) — sync (no system/sync_to, invalid tables, mode override, dry run, success, partial failure), sync-status, sync-purge
- `cli/system.py` — **89% covered** (42 tests). system_command dispatcher, _sync_command, _rebuild_summaries_command, _optimize_command, _cleanup_command, _count_records_to_delete, _count_records_pandas, _count_records_sql_server, _delete_old_records, _delete_records_pandas, _delete_records_sql_server.
  - `tests/unit/cli/test_cli_system_coverage.py` (42 tests) — dispatcher, all 4 commands, all 6 helper functions
- `cli/list_cmd.py` — **97% covered** (41 tests). _get_first_line, _serialize_default, _extract_pattern_params (docstring parsing + all fallback patterns), _ensure_initialized, list_command dispatcher, list_transformers/patterns/connections/recipes (table + json), explain_command (transformer, pattern, connection, recipe, not found).
  - `tests/unit/cli/test_cli_list_cmd_coverage.py` (41 tests)
- `cli/story.py` — **96% covered** (44 tests). story_command dispatcher, generate_command (YAML loading, DocStoryGenerator, themes, error paths), diff_command (basic + detailed with node comparison, time/row diffs, status changes), list_command (directory listing, size formatting, limits), last_command (file search, browser opening, node filtering), show_command (HTML/JSON/other), add_story_parser. Remaining: diff verbose traceback, list error path.
  - `tests/unit/cli/test_cli_story_coverage.py` (44 tests) — all commands with mock args, file I/O via tmp_path
- `story/generator.py` — **83% covered** (100 tests). StoryGenerator init, _infer_layer, _compute_anomalies, _convert_result, _get_error_*, _build_graph_data, _compare_with_last_success, generate (local + remote), cleanup (local + remote), retention, _write_remote (fsspec + dbutils fallback), _cleanup_remote (count/time retention, empty dirs), _generate_pipeline_index (full local flow), _render_index_html, _find_last_successful_run_remote, _clean_config_for_dump, _get_git_info, get_alert_summary, _generate_docs. Remaining: some fallback graph building paths, config snapshot diff paths.
  - `tests/unit/story/test_story_generator_coverage.py` (100 tests) — all pure logic, graph building, anomaly detection, cross-run comparison, full generate integration, remote write/cleanup/index

- `story/lineage_utils.py` — **100% covered** (62 tests). All 4 functions fully tested: get_full_stories_path (all 9 conn types + edge cases), get_storage_options (all auth modes — direct, nested, dict, MSI, key_vault), get_write_file (all cloud closures with mock fsspec), generate_lineage (mock LineageGenerator).
  - `tests/unit/story/test_lineage_utils_coverage.py` (62 tests) — get_full_stories_path, get_storage_options, get_write_file, write closure bodies, generate_lineage
- `story/lineage.py` — **98% covered** (70 new tests). LineageGenerator: generate (all node types, edge formats, layer inference), save (local + remote + write_file), render_json/html, _find_story_files (local), _find_remote_story_files (mock fsspec), _load_story (local + remote with retry), _extract_layer_info (all statuses), _stitch_cross_layer_edges, _inherit_layers_from_matches, _generate_mermaid_diagram, _generate_layers_table, _get_layer_class, all helper methods. Remaining: 6 lines (retry sleep paths).
  - `tests/unit/story/test_lineage_coverage.py` (70 tests) — all LineageGenerator methods, dataclass serialization, edge cases
- `lineage.py` — **96% covered** (71 tests). OpenLineageAdapter disabled paths, enabled paths (mock OL types injected at module level), _create_dataset_from_config, init with URL. LineageTracker: record_lineage, record_dependency_lineage, _resolve_table_path, get_upstream/downstream, get_impact_analysis. Remaining uncovered: lines 9-28 (OpenLineage import block — not installed), lines 51-52/58-59 (init config early-return branches already covered by disabled tests).
  - `tests/unit/test_lineage_coverage.py` (71 tests) — all OpenLineageAdapter + LineageTracker paths
- `story/doc_generator.py` — **90% covered** (92 new tests + 25 existing). All rendering helpers, node cards, readme, technical details, run memo, pipeline state. Remaining: _write_remote, _read_remote (fsspec/dbutils paths).
  - `tests/unit/story/test_doc_generator_coverage.py` (92 tests)
- `utils/progress.py` — **100% covered** (60 tests). All Rich and plain text paths, phase timing reports.
  - `tests/unit/utils/test_progress_coverage.py` (60 tests)
- `connections/api_fetcher.py` — **94% covered** (88 new tests + 38 existing). All pagination strategies, response extractor, date variables, rate limiter, factory. Remaining: HTTP retry paths.
  - `tests/unit/connections/test_api_fetcher_coverage.py` (88 tests)

- `patterns/aggregation.py` — **63% covered** (32 tests). validate(), _aggregate_pandas() with DuckDB SQL, incremental merge strategies (replace/sum/min/max), audit columns, end-to-end. Remaining: Spark paths, error handling in execute().
  - `tests/unit/patterns/test_aggregation_coverage.py` (32 tests) — validate, aggregate, incremental merge, audit, end-to-end
- `patterns/fact.py` — **74% covered** (47 tests). validate(), _deduplicate(), dimension lookups (unknown/reject/quarantine), _apply_measures(), _is_expression(), _validate_grain(), quarantine metadata + write (CSV/JSON/parquet). Remaining: Spark paths.
  - `tests/unit/patterns/test_fact_coverage.py` (47 tests) — validate, dedup, dimension lookup, measures, grain validation, quarantine, audit, end-to-end
- `patterns/dimension.py` — **70% covered** (41 tests). validate(), _get_max_sk(), _generate_surrogate_keys(), SCD0/SCD1 full Pandas paths, SCD2 (mocked scd2 transformer), audit columns (dimension override), _ensure_unknown_member(). Remaining: Spark paths, _load_existing_spark.
  - `tests/unit/patterns/test_dimension_coverage.py` (41 tests) — validate, SK generation, SCD0/1/2, audit, unknown member

- `writers/sql_server_writer.py` — **67% covered** (208 tests). All non-Spark paths: dataclasses, helpers, hash (Pandas+Polars), filter changed rows, validate keys, schema/DDL, schema_evolution (all modes), merge_pandas (auto-create PK/index, audit, incremental, schema evo, cleanup), merge_polars (all paths), overwrite_pandas/polars (all strategies + batch), bulk_copy helpers (_execute_bulk_insert, _validate_bulk_insert_schema, _is_azure_sql_database, setup_bulk_copy_external_source, credentials, cleanup). Remaining: Spark-only paths (~466 stmts).
  - `tests/unit/writers/test_sql_server_writer_coverage.py` (208 tests)
- `derived_updater.py` — **74% covered** (109 tests). All utility functions, DerivedUpdater dispatch, full Pandas claim lifecycle, apply_derived_update (fail-safe wrapper), apply_derived_updates_batch (batch mode), SQL Server claim lifecycle + derived updaters, **Pandas derived updater methods** (_update_daily_stats_pandas, _update_pipeline_health_pandas, _update_sla_status_pandas) with full coverage of cost source logic, window metrics, SLA met/violated, run cache, existing row replacement. Remaining: Spark paths (~200 lines).
  - `tests/unit/test_derived_updater_coverage.py` (109 tests) — utilities, dispatch, Pandas Delta claim/mark/reclaim, apply_derived_update, batch mode, SQL Server claim lifecycle + derived updaters, Pandas daily stats/pipeline health/SLA status
- `introspect.py` — **88% covered** (34+21 tests). FieldDoc, ModelDoc, clean_type_str, format_type_hint, get_docstring, get_summary, get_pydantic_fields, discover_modules, scan_module_for_models, constants, render_markdown (empty/core/connection/transformation groups, cross-linking, type alias expansion, used_in, code fence handling), generate_docs pipeline. Remaining: some edge paths in transformation rendering.
  - `tests/unit/test_introspect_coverage.py` (34 tests) — data models, type formatting, docstring extraction, module discovery, model scanning
  - `tests/unit/test_introspect_render_coverage.py` (21 tests) — render_markdown all groups, cross-linking, generate_docs

- `tools/adf_profiler.py` — **99% covered** (70 tests). All REST API methods, pagination, caching, all activity types, all trigger types, full_profile orchestration, parallel run stats, markdown report, JSON output, Excel output (10 sheets), lineage, _auto_fit_columns. Remaining: 1 line (secret redaction edge case).
  - `tests/unit/tools/test_adf_profiler_coverage.py` (70 tests) — all AdfProfiler methods with mocked requests/credentials
- `utils/setup_helpers.py` — **94% covered** (22 tests). KeyVaultFetchResult, fetch_keyvault_secret (success/ImportError/exception), fetch_keyvault_secrets_parallel (kv/non-kv connections, verbose), configure_connections_parallel (prefetch on/off, cache injection, errors), validate_databricks_environment. Remaining: Spark active session check, IPython dbutils check.
  - `tests/unit/utils/test_setup_helpers_coverage.py` (22 tests)
- `connections/factory.py` — **81% covered** (25 tests). All factory functions: create_local/http/azure_blob/delta/sql_server/postgres connections. Auth auto-detection for all modes. Missing host/account/database validation errors. register_builtins.
  - `tests/unit/connections/test_connection_factory_coverage.py` (25 tests)
- `patterns/date_dimension.py` — **57% covered** (17 tests). _calc_fiscal_year, _calc_fiscal_quarter, _generate_pandas, _add_unknown_member Pandas, _get_row_count, execute Pandas path with/without unknown_member. Remaining: Spark paths.
  - `tests/unit/patterns/test_date_dimension_coverage.py` (17 tests)
- CLI small commands — **35 tests** covering cli/templates.py (list/show/transformer/schema/dispatch), cli/validate.py (valid/invalid/exception), cli/ui.py (import error, parser), cli/export.py (airflow/dagster/errors), cli/run.py (success/failure/exception), cli/deploy.py (success/no catalog/exception).
  - `tests/unit/cli/test_cli_small_commands_coverage.py` (35 tests)
- `pipeline.py` — **35% covered** (16+69 tests). PipelineResults (debug_summary, to_dict, get_node_result, defaults), Pipeline utility methods (_cleanup_connections, flush_stories, flush_sync, _send_alerts, buffer_lineage/asset/hwm, _get_databricks_*_id, _flush_batch_writes, _sync_catalog_if_configured, context manager), PipelineManager (_build_connections, register_outputs). Remaining: Pipeline.run() execution paths, PipelineManager.from_yaml, __init__, validate, run_node (~1000 lines, mostly integration-level).
  - `tests/unit/test_pipeline_coverage.py` (16 tests) — _build_connections, register_outputs, PipelineResults basics
  - `tests/unit/test_pipeline_extended_coverage.py` (69 tests) — debug_summary, to_dict, cleanup, flush, alerts, buffers, databricks IDs, batch writes, sync
- `node.py` — **81% covered** (50+39 utility/execution tests). PhaseTimer, _clean_spark_traceback, _calculate_pii, _count_rows, _get_column_max, _generate_suggestions, _get_config_snapshot, _get_connection_health, _get_delta_table_state, _check_target_exists, _execute_dry_run, _generate_incremental_sql_filter (rolling window + stateful + watermark_lag), NodeResult. **Plus execution paths**: error handling (NodeExecutionError wrapping, traceback capture, suggestions, error metadata), first_run_query, archive_options, incremental SQL pushdown, sql_file resolution with incremental filter wrapping, simulation state capture (HWM, random walk, scheduled events), stateful incremental filter with watermark_lag/fallback_column.
  - `tests/unit/test_node_utilities_coverage.py` (50 tests)
  - `tests/unit/test_node_execution_coverage.py` (39 tests) — error handling, first_run_query, archive_options, incremental SQL pushdown, sql_file, simulation state, stateful filter

- `state/__init__.py` — **80% covered** (86 tests). LocalJSONStateBackend (init/load/save/get/set all paths), CatalogStateBackend (local Delta read/write for runs+HWM, Spark dispatch, set_hwm merge+fallback, batch operations), SqlServerSystemBackend (_ensure_tables, get_last_run_info, get/set HWM, log_run, log_runs_batch), StateManager (init, delegation), create_state_backend factory (local JSON fallback, local/azure_blob/sql_server types), create_sync_source_backend factory, sync_system_data + _sync_runs + _sync_state. Remaining: Spark HWM batch paths, _retry_delta_operation, some sync edge cases.
  - `tests/unit/test_state_coverage.py` (86 tests) — all backends, manager, factories, sync operations

- `catalog_sync.py` — **81% covered** (88 tests). CatalogSyncer init, _get_target_type (all detection paths), get_tables_to_sync, sync dispatch (SQL Server + Delta), sync_async, _sync_to_sql_server (full/incremental/env injection/empty/error), _sync_to_delta (spark/engine/error/path validation), _read_source_table, _get_time_column, _normalize_tz, _apply_incremental_filter (last_sync/sync_last_days/date), _get_last_sync_timestamp (spark/engine/error), _update_sync_state (spark/engine/append/update), _ensure_sql_schema/table/dim_tables/views, _apply_column_mappings, _df_to_records, _sanitize_value (all types), _insert_to_sql_server (MERGE/INSERT/deadlock retry), purge_sql_tables. Remaining: Spark _apply_incremental_filter_spark, some _sync_to_delta Spark paths.
  - `tests/unit/test_catalog_sync_coverage.py` (88 tests)
- `connections/azure_adls.py` — **89% covered** (61 new + 65 existing = 126 total tests). All discovery methods (list_files, list_folders, discover_catalog recursive/non-recursive, get_schema, profile, preview, detect_partitions, get_freshness), get_storage_key edge cases (cached/key_vault/timeout/ImportError), validate edge cases (unsupported auth_mode, production warning, sas/sp failures), _get_fs, get_client_secret. Remaining: configure_spark internal paths (~100 lines).
  - `tests/unit/connections/test_azure_adls_coverage.py` (61 tests)
  - `tests/unit/connections/test_azure_adls.py` (65 tests)
- `connections/local.py` — **98% covered** (64 tests). All init/get_path/validate paths, list_files/folders (including error paths), discover_catalog (recursive/non-recursive/pattern/limit/error/formats), get_schema (parquet/csv/json/unsupported/error), profile (all cardinalities/candidate keys+watermarks/columns filter/empty df/sample cap), preview (all formats/columns/cap), detect_partitions (URI/subpath/error), get_freshness (URI/exists/missing/error). Remaining: 5 lines (edge cases).
  - `tests/unit/connections/test_local_coverage.py` (64 tests)
- `connections/postgres.py` — **96% covered** (85 tests). All methods: init, get_path, validate, get_engine (cached/new/ImportError/connection error), read_sql, read_table, read_sql_query, write_table, execute/execute_sql, SQL helpers (quote_identifier, qualify_table, build_select_query), list_schemas, list_tables, get_table_info, discover_catalog (path/pattern/limit/include_schema), profile (all cardinalities), preview, relationships, get_freshness (data/metadata/1970/error), get_spark_options (sslmode), close, _sanitize_error, _get_error_suggestions (all patterns). Remaining: ~15 lines (edge cases in profile/discover).
  - `tests/unit/connections/test_postgres_coverage.py` (85 tests)
- `connections/azure_sql.py` — **93% covered** (87 tests). All methods: init, get_password (direct/cached/key_vault/ImportError/no-password), odbc_dsn (sql/aad_msi/aad_sp), get_path, validate (all paths), get_engine (cached/new/error), read_sql, read_table, read_sql_query, write_table, execute/execute_sql, SQL helpers, list_schemas, list_tables, get_table_info, discover_catalog, profile, preview, relationships, get_freshness, close, _sanitize_error, _get_error_suggestions, get_spark_options (all auth modes). Remaining: ~30 lines (ImportError in get_engine, get_spark_options edge paths).
  - `tests/unit/connections/test_azure_sql_coverage.py` (87 tests)

- `transformers/` module coverage:
  - `__init__.py` — **100%** (register_standard_library fully tested)
  - `validation.py` — **100%**
  - `advanced.py` — **80%** (Pandas paths for sessionize, split_events_by_period all covered. Remaining: Spark SQL paths — diminishing returns)
  - `units.py` — **78%** (Pandas + Polars paths covered, _convert_value, _parse_gauge_offset, error handlers. Remaining: Spark UDF lines 276-325)
  - `thermodynamics.py` — **71%** (CoolProp installed, fluid_properties + psychrometrics Pandas/Polars tested. Remaining: Spark UDF paths)
  - `manufacturing.py` — **67%** (Polars paths fully covered including phase detection, status tracking, metadata extraction. Remaining: Spark paths lines 549-946)
  - `delete_detection.py` — **64%** (50 tests, all Pandas paths covered — Spark paths skipped)
  - `relational.py` — **67%** (30 tests). Join full/right/cross, union by_name/by_position/missing dataset, pivot all agg funcs, unsupported engine errors. Remaining: Spark paths.
    - `tests/unit/transformers/test_relational_coverage.py` (30 tests) — join types, union variants, pivot/unpivot engine errors
  - `sql_core.py` — **93%** (31 tests). extract_date_parts, normalize_schema, sort, limit, sample, distinct, fill_nulls, split_part, date_add, date_trunc, date_diff, case_when, convert_timezone, concat_columns, normalize_column_names, coalesce_columns drop_source. Remaining: Spark-only paths.
    - `tests/unit/transformers/test_sql_core_coverage.py` (31 tests) — all 16 previously uncovered SQL transformers
  - `merge_transformer.py` — **61%** (Pandas + DuckDB merge paths fully covered. Remaining: _merge_spark lines 424-631)
  - `scd.py` — **49%** (36 tests, all Pandas + DuckDB paths covered. Remaining: _scd2_spark and Delta MERGE lines 289-719)
  - Test files:
    - `tests/unit/transformers/test_delete_detect.py` (50 tests)
    - `tests/unit/transformers/test_transformer_registration.py` (95 tests)
    - `tests/unit/transformers/test_advanced_coverage.py` (20 tests) — sessionize Pandas, split_by_day/hour/shift Pandas
    - `tests/unit/transformers/test_merge_pandas_deep.py` (18 tests) — DuckDB merge, Pandas fallback, connection resolution
    - `tests/unit/transformers/test_units_coverage.py` (37 tests) — Polars conversion, _convert_value, _parse_gauge_offset, error paths, list_units, unit_convert
    - `tests/unit/transformers/test_scd_coverage.py` (36 tests) — SCD2 Pandas + DuckDB paths, _safe_isna, params validation, bug fix test
    - `tests/unit/transformers/test_thermodynamics_coverage.py` (60 tests) — fluid_properties, saturation_properties, psychrometrics with real CoolProp
    - `tests/unit/transformers/test_manufacturing_coverage.py` (30 tests) — Polars phase detection, status tracking, metadata extraction, fill_null

### 🎯 Coverage Roadmap: 66% → 80% (April–May 2026)

**Baseline (April 17, 2026):** 34,363 stmts, 11,807 missed → **66% covered**
**Target:** 80% → need to cover **~4,816 more statements**
**Budget:** ~$10/day Amp credits, ~5 weeks

#### ⚠️ BLOCKER: Fix validation/engine.py First
`validation/engine.py` line 53 does `isinstance(df, pyspark.sql.DataFrame)` — PySpark IS installed but `pyspark.sql.DataFrame` is not accessible without JVM, causing `AttributeError`. This makes **261 tests fail** (all validation/engine + quarantine + gate tests). Fix with:
```python
def _is_spark_dataframe(df) -> bool:
    try:
        from pyspark.sql import DataFrame as SparkDataFrame
        return isinstance(df, SparkDataFrame)
    except Exception:
        return False
```
Apply the same pattern in `quarantine.py` and `gate.py` if they have the same check.

#### Phase 1: Unblock + Cheap Wins (Week 1) → Target: ~70%
| Day | Module(s) | Missed Stmts | Strategy |
|-----|-----------|-------------|----------|
| 1 | Fix `validation/engine.py` Spark detection bug | 412 | Fix bug, add Pandas/Polars dispatch tests |
| 2 | `validation/engine.py` Pandas+Polars matrix | (continued) | All 11 test types × 2 engines |
| 3 | `validation/quarantine.py` + `testing/source_pool.py` | 216 + 101 | Restored tests + new coverage |
| 4 | `cli/graph.py` + `cli/test.py` + `cli/schema.py` | 95 + 152 + 114 | CliRunner + monkeypatch |
| 5 | `cli/init_pipeline.py` + `cli/secrets.py` + `cli/lineage.py` | 80 + 85 + 133 | CliRunner + monkeypatch |

#### Phase 2: CLI + Story Sweep (Week 2) → Target: ~73%
| Day | Module(s) | Missed Stmts | Strategy |
|-----|-----------|-------------|----------|
| 1 | `cli/main.py` + `cli/catalog.py` | 166 + 258 | CliRunner |
| 2 | `story/generator.py` | 308 | Pure logic/rendering |
| 3 | `story/doc_generator.py` | 292 | Pure logic/rendering |
| 4 | `story/lineage_utils.py` + `lineage.py` | 97 + 107 | Graph/tree logic |
| 5 | `utils/progress.py` + `connections/api_fetcher.py` | 86 + 121 | Mock HTTP |

#### Phase 3: Patterns + Transforms (Week 3) → Target: ~75%
| Day | Module(s) | Missed Stmts | Strategy |
|-----|-----------|-------------|----------|
| 1 | `patterns/aggregation.py` + `patterns/fact.py` | 112 + 117 | Pandas tests |
| 2 | `patterns/dimension.py` + `relational.py` | 82 + 91 | Pandas tests |
| 3 | `transformers/scd.py` + `merge_transformer.py` | 158 + 134 | Pandas/DuckDB paths |
| 4 | `manufacturing.py` + `thermodynamics.py` | 176 + 127 | Polars/CoolProp |
| 5 | Start `derived_updater.py` | 444 | Core logic branches |

#### Phase 4: Medium Complexity (Week 4) → Target: ~77%
| Day | Module(s) | Missed Stmts | Strategy |
|-----|-----------|-------------|----------|
| 1-2 | Finish `derived_updater.py` | (continued) | Mock-based |
| 3 | `catalog_sync.py` | 237 | Mock storage |
| 4 | `connections/azure_adls.py` | 251 | Mock Azure SDK |
| 5 | `connections/local.py` or remaining Tier 1 | 74 | Filesystem mocks |

#### Phase 5: Closers (Week 5) → Target: **80%**
Pick based on actual numbers after Week 4:
- **Option A:** `sql_server_writer.py` (767 missed, 46% cov) — biggest single closer
- **Option B:** `introspect.py` (329 missed) + `postgres.py` (321 missed)
- **Option C:** `adf_profiler.py` (590 missed) — if it's mockable

#### Checkpoints
- **After Week 2:** If still <70%, move `sql_server_writer.py` earlier
- **After Week 4:** If still <76%, plan two heavy closers instead of one

#### Hard Skip (Don't Waste Credits)
- `engine/spark_engine.py` (1,262 missed) — tested in Databricks prod
- Spark-specific branches everywhere — diminishing returns
- `diagnostics/delta.py` (171 missed) — Delta/Spark-bound
- Files already >85% (`pandas_engine`, `polars_engine`, `pipeline`, `node`, `azure_sql`, `tools/templates`)

#### Thread Start Protocol
Every new coverage thread should:
1. Read this section of AGENTS.md first
2. Check which phase we're in (update the ✅ below as phases complete)
3. Pick the next uncompleted module from that phase
4. Run `python -m coverage run --source=odibi.<module> -m pytest tests/unit/<test_file> -q --tb=no` then `python -m coverage report --show-missing` to get current baseline (**NOT** `pytest --cov` — it breaks Rich logging)
5. Write tests, verify passing, update coverage % in this file

#### Progress Tracker
- [x] Phase 1: Unblock + Cheap Wins (319 new tests)
- [x] Phase 2: CLI + Story Sweep (526 tests — 37 cli/main, 51 cli/catalog, 65 story/generator, 92 doc_generator, 62 lineage_utils, 71 lineage, 60 progress, 88 api_fetcher)
- [ ] Phase 3: Patterns + Transforms — DONE for patterns (aggregation 63%, fact 74%, dimension 70%). SCD 60%, merge 59% — entry points + Pandas fallback covered. Manufacturing/thermodynamics already at 67%/71%.
- [x] Phase 4: Medium Complexity — DONE (derived_updater 49%✓, introspect ~30%✓, sql_server_writer 117 tests✓, catalog_sync 81%✓, azure_adls 89%✓, local 98%✓)
- [ ] Phase 5: Closers — IN PROGRESS (sql_server_writer 68%/208 tests, postgres 96%/85 tests, azure_sql 93%/87 tests, adf_profiler 99%/70 tests, node.py 74%/50 new tests, setup_helpers 94%/22 tests, date_dimension 57%/17 tests, factory.py 81%/25 tests, cli small commands 35 tests + prior: derived_updater 54%/85, state 80%/86, story/generator 83%/100, cli/story 96%/44)
  - **Current: 80% overall (34,363 stmts, 6,854 missed) — TARGET REACHED!**
  - New test files this round: test_cli_system_coverage (42), test_cli_lineage_coverage (17), test_cli_list_cmd_coverage (41), test_cli_catalog_sync_coverage (26), test_cli_schema_coverage (22), test_cli_main_remaining_coverage (13), test_cli_main_scaffold_coverage (20), test_cli_test_coverage (31), test_introspect_render_coverage (21), test_config_coverage (52), test_quarantine_remaining_coverage (25), test_node_execution_coverage (39), test_pipeline_coverage (16), test_derived_updater_coverage +24 Pandas methods = **474 tests** (previous session)
  - New test files this session: test_sql_core_coverage (31), test_relational_coverage (30), test_pipeline_extended_coverage (69), test_lineage_coverage (70) = **200 new tests**
  - Coverage measurement note: `coverage run` hangs when combining test_pandas_engine_core.py + azure_adls.py + simulation tests with other files. Must run in separate batches and use `coverage run -a` to append.
- [x] **80% reached 🎉**

### Linting & Formatting
```bash
ruff check .                        # Check for issues
ruff check . --fix                  # Auto-fix issues
ruff format .                       # Format code
pre-commit run --all-files          # Run all checks
```

### Documentation Generation
```bash
python odibi/introspect.py          # Regenerate yaml_schema.md from Pydantic models
```
Auto-generates `docs/reference/yaml_schema.md` by introspecting config classes.
The docstrings in `odibi/config.py` are the source of truth for documentation.

### Introspection (AI-Friendly CLI)
```bash
odibi list transformers             # List all 54 transformers
odibi list patterns                 # List all 6 patterns
odibi list connections              # List all connection types
odibi explain <name>                # Get detailed docs for any feature
odibi list transformers --format json  # JSON output for parsing
```
Use these commands to discover available features before generating YAML configs.

### Template Generator
```bash
odibi templates list                # List all template types
odibi templates show azure_blob     # Show YAML template with all auth options
odibi templates show sql_server     # Show connection template
odibi templates show validation     # Show all 11 validation test types
odibi templates transformer scd2    # Show transformer params + example YAML
odibi templates schema              # Generate JSON schema for VS Code
```
Templates are generated directly from Pydantic models—always in sync with code.

### WSL/Spark Testing (Windows)
Tests requiring Spark must run through WSL:
```bash
wsl -d Ubuntu-22.04 -- bash -c "cd /mnt/d/odibi && pip install -e '.[dev]' && python3 -m pytest tests/"
```

## Testing Gotchas & Lessons Learned

**Read this before writing mock-based Spark tests. These pitfalls cost significant time to figure out.**

### Mock PySpark Setup (Critical Order)
PySpark 4.1.1 IS installed in this environment, so `catalog.py`'s try/except imports succeed and the fallback type stubs (lines 22-137) are NOT defined. These stubs are NOT dead code — they're essential for Pandas-only deployments. **Do not remove them.**

When mocking PySpark for Spark branch testing:
1. **ALWAYS `import odibi.catalog` (or `CatalogManager`) FIRST**, then install mock pyspark modules in `sys.modules`. If you install mocks before importing, the fallback type stubs never get defined, and all Spark-branch code breaks silently.
2. **Use `sys.modules["pyspark"] = mock_module`** (direct assignment), NOT `sys.modules.setdefault(...)`. `setdefault` won't override the real pyspark that's already loaded.
3. **Mock `pyspark.sql.types` must use the real fallback classes** from `odibi.catalog` (e.g., `StringType`, `StructField`, `StructType`), NOT `MagicMock()`. Otherwise `StructType(fields)` creates a MagicMock where `.fields` is auto-generated garbage.
4. **When multiple test files install mock pyspark at module level**, the last file loaded wins. The writes file should reuse the reads file's `_MockFunctions` via `sys.modules.get("pyspark.sql.functions")` to avoid clobbering `col()`, `avg()`, etc.

### MagicMock Dunder Methods
**Setting `mock_obj.__getitem__ = ...` on a MagicMock INSTANCE does NOT work.** Python resolves dunder methods on the TYPE, not the instance. Use plain tuples instead (e.g., `mock_stats = (100.0,)` for code that does `stats[0]`).

### _MockColumn Completeness
If you build a `_MockColumn` helper for Spark filter expressions, it must support **all** operators the code uses: `__eq__`, `__ge__`, `__and__`, `__rand__`, `__hash__`, `__invert__`, and `desc()`. Missing `__invert__` causes `remove_node` tests to fail silently.

### Logging Context Pollution in Tests
The `get_logging_context()` singleton from `odibi.utils.logging_context` causes test-ordering failures. Tests that call engine methods (read/write) trigger structured logging calls (ctx.info, ctx.error, ctx.debug) that fail with `AttributeError` or `TypeError` when tests run in certain orders. **Tests pass individually but fail in batch.** This affects `test_pandas_engine_core.py` and `test_pandas_engine_full_coverage.py`. New test files should NOT use caplog and should only assert on return values/behavior, not log output.

### SCD2 Pandas Bug: flag_col Required in Target — FIXED
`_scd2_pandas` line 973 previously accessed `final_target[flag_col]` unconditionally after the `changed_records` merge. If the target DataFrame lacked the `flag_col` column AND there were changed records, this threw a `KeyError`. **Fixed:** Now checks if `flag_col` exists; if not, creates it as `True` and casts to `bool` before setting `False`. Test: `test_target_without_flag_col_with_changes`.

### CoolProp and Polars Are Installed
CoolProp 7.2.0 and Polars are available in this environment. The thermodynamics and manufacturing Polars tests run directly — no mocking needed. Don't skip these in new test files.

### NodeResult.result_schema Uses Field Alias
`NodeResult.result_schema` has `Field(alias="schema")`. You must pass it as `schema=["a","b"]` in the constructor, NOT `result_schema=["a","b"]` — the latter silently sets it to None. Read via `.result_schema`.

### Polars Freshness Tests: Use UTC Timestamps
Polars adds `+00:00` timezone to datetime values. The validation engine's `_validate_polars` freshness check compares with `datetime.now(timezone.utc)` when the timestamp has tzinfo. Use `datetime.now(timezone.utc) - timedelta(...)` in test data, not `datetime.now()`, or the 5-hour UTC offset makes "fresh" data appear stale.

### pytest --cov Breaks Tests With Rich Logging
Running `pytest --cov=odibi.validation.engine` causes tests to FAIL because `--cov` changes import/execution order, triggering the Rich `Text` object vs `str` mismatch in `get_logging_context()` / `Highlighter`. Tests pass without `--cov`. **Workaround:** Use `python -m coverage run --source=... -m pytest ...` followed by `python -m coverage report --show-missing`. This gives the same coverage data without the ordering issue.

### Test Suite Hangs When Running Full `tests/unit/`
Running the full `tests/unit/` suite in a single process **will hang**. This is caused by interactions between:
1. **ThreadPoolExecutor + coverage tracing** — `azure_adls.get_storage_key` and `adf_profiler.full_profile` use thread pools that deadlock under coverage's `sys.settrace` hook.
2. **Rich logging singleton contamination** — The `get_logging_context()` singleton accumulates state across tests. When certain files run in sequence, Rich's highlighter receives a `Text` object instead of `str`, triggering a blocking `TypeError`.
3. **Mock pyspark `sys.modules` contamination** — Test files that install mock pyspark at module level (`test_catalog_mock_engine_reads.py`, `test_catalog_mock_engine_writes.py`) modify `sys.modules` permanently. When combined with later tests that do real `import pyspark`, the process can deadlock.
4. **Polars `repeat_by` + `list.join`** — The anonymize "mask" method in Polars hangs in certain test contexts.

**The fix:** Always run tests in batches. Use `scripts/run_coverage.ps1` or run batches manually:
```powershell
# Batch 1 — main suite (exclude known hang sources)
python -m coverage run --source=odibi -m pytest tests/unit/ -q --tb=no `
    --ignore=tests/unit/engine/test_pandas_engine_core.py `
    --ignore=tests/unit/connections/test_azure_adls_coverage.py `
    --ignore=tests/unit/test_simulation.py `
    --ignore=tests/unit/test_simulation_random_walk.py `
    --ignore=tests/unit/test_simulation_fixes.py `
    --ignore=tests/unit/test_simulation_daily_profile.py `
    --ignore=tests/unit/test_simulation_coverage_gaps.py `
    --ignore=tests/unit/test_stateful_simulation.py `
    --ignore=tests/unit/tools/test_adf_profiler_coverage.py `
    -k "not test_failure_logs_warning"

# Batch 2-5 — isolated files (use -a to append coverage)
python -m coverage run -a --source=odibi -m pytest tests/unit/engine/test_pandas_engine_core.py -q --tb=no
python -m coverage run -a --source=odibi -m pytest tests/unit/connections/test_azure_adls_coverage.py -q --tb=no
python -m coverage run -a --source=odibi -m pytest tests/unit/test_simulation*.py tests/unit/test_stateful_simulation.py -q --tb=no
python -m coverage run -a --source=odibi -m pytest tests/unit/tools/test_adf_profiler_coverage.py -q --tb=no

# Report
python -m coverage report --show-missing
```

### builtins.__import__ Mocking Is Dangerous
Patching `builtins.__import__` to simulate missing libraries (e.g., `fsspec`, `pyarrow`) can accidentally catch internal imports for Rich, logging, or coverage — causing `ImportError` during unrelated code. **Use `sys.modules` manipulation instead:**
```python
import sys
original = sys.modules.get("fsspec")
sys.modules["fsspec"] = None
try:
    # run test
finally:
    if original is not None:
        sys.modules["fsspec"] = original
    else:
        sys.modules.pop("fsspec", None)
```

### Pre-Existing Test Failures (Not Regressions)
These failures exist in older test files and are NOT caused by recent changes:
- **~25 caplog failures** in `test_catalog_batch_and_utils.py`, `test_catalog_bootstrap.py`, `test_catalog_observability.py`, `test_catalog_lineage_schema.py` — `logger.warning` goes to stderr, `caplog` doesn't capture it.
- **~94 failures** in `test_pandas_engine_core.py` and `test_pandas_engine_full_coverage.py` when run in batch — logging context pollution (pass individually).
- **~70-93 failures** in `test_adf_profiler_coverage.py` — Excel report generation and caching issues (environment-specific).

### Pattern Test Construction
When testing patterns (AggregationPattern, FactPattern, DimensionPattern), NodeConfig's Pydantic validation checks required params at construction time. For tests that intentionally pass invalid/empty params to test pattern-level validation, **use a MagicMock config instead of NodeConfig**:
```python
from unittest.mock import MagicMock
cfg = MagicMock()
cfg.name = "test_node"
cfg.params = {"grain": []}  # intentionally invalid
pattern = AggregationPattern(engine=engine, config=cfg)
pattern.validate()  # now tests the pattern's own validation
```
For aggregation _aggregate_pandas(), patterns use `context.sql()` which needs a DuckDB-based `sql_executor`. Set up:
```python
import duckdb
def sql_executor(query, context):
    conn = duckdb.connect()
    for name in context.list_names():
        conn.register(name, context.get(name))
    return conn.execute(query).fetchdf()
```

### State Module: create_connection Import
`odibi/state/__init__.py` does `from odibi.connections.factory import create_connection` inside `create_state_backend()` and `create_sync_source_backend()`. However, `create_connection` does NOT exist in `odibi.connections.factory` — only typed functions like `create_sql_server_connection` exist. To test these SQL Server factory paths, inject the mock directly: `import odibi.connections.factory as m; m.create_connection = MagicMock()` and clean up in a `finally` block.

### CatalogStateBackend: Null Type Columns
When testing `CatalogStateBackend` set_hwm/set_hwm_batch with real Delta writes, always provide a non-None `environment` value (e.g., `environment="test"`). If environment is None, Pandas creates a Null-type column that Delta Lake rejects with `SchemaMismatchError: Invalid data type for Delta Lake: Null`. Similarly, seed Delta test data using `pa.table()` with explicit types instead of `pd.DataFrame` to avoid Null type inference.

### Test File Naming
**Do NOT use "spark" or "delta" in test file names.** The `conftest.py` skip filter catches them on Windows and skips the entire file. Use names like `test_catalog_mock_engine_reads.py` instead of `test_catalog_spark_reads.py`.

### Standard Test Fixture
```python
cat = CatalogManager(
    spark=None,
    config=SystemConfig(connection="local", path="_odibi_system"),
    base_path=str(tmp_path / "_odibi_system"),
    engine=PandasEngine(config={}),
)
```
Seed data with `write_deltalake` + PyArrow, read with `DeltaTable(path).to_pandas()`.

### Catalog Test Files Reference
- `test_catalog_outputs_and_config.py` (22 tests) — get_node_output, register_outputs_from_config, _extract_node_output_info, _get_all_outputs_cached
- `test_catalog_pipeline_run_and_optimize.py` (26 tests) — get_pipeline_run, optimize, _bootstrapped guard, _prepare_pipeline_record, _prepare_node_record
- `test_catalog_deprecated_skip.py` (19 tests) — register_pipeline/register_node skip_if_unchanged, _get_storage_options, _table_exists cloud paths
- `test_catalog_mock_engine_reads.py` (27 tests) — Spark read branches via mock
- `test_catalog_mock_engine_writes.py` (32 tests) — Spark write branches via mock

## Key Patterns

1. **Engine Parity:** If Pandas has it, Spark and Polars should too
2. **Use `get_logging_context()`** for structured logging
3. **Pydantic models** for config validation
4. **Tests in `tests/unit/`** matching source structure

## Continuous Learning — UPDATE THIS FILE

**Every Amp thread MUST update AGENTS.md before finishing if it discovered something that would save future threads time or money.**

### What to capture
- **Gotchas/pitfalls** that took more than one attempt to figure out
- **Mock/test patterns** that work (or don't) for specific modules
- **Coverage updates** — when you add tests, update the "Current Coverage Status" section with new percentages and what's covered
- **Import order issues**, silent failures, or environment-specific quirks
- **Naming constraints** (e.g., conftest skip filters, reserved words)
- **Working code patterns** that future threads should reuse rather than reinvent

### How to capture
1. Add gotchas to the **"Testing Gotchas & Lessons Learned"** section (or create a new subsection if it's not test-related)
2. Update **"Current Coverage Status"** with new numbers when you add tests
3. Keep entries **concise and actionable** — state the problem, the wrong approach, and the fix
4. **Do NOT remove existing entries** — only append or update percentages

### Why this matters
Each lesson captured here saves ~10-30 minutes and real money on future threads. The compounding effect is significant. If you hit a wall, solved it, and think "another agent would hit this too" — write it down.

## What NOT to Do

- Don't rebuild agent/chat infrastructure - use Amp/Cursor
- Don't add features without tests
- Don't break engine parity

## Archived Code

The `_archive/` folder contains experimental agent code that was deprioritized.
Focus is now on making odibi core rock-solid.
