# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.13.0] - 2026-01-23

### Added - Schema Comparison & New Recipes

- **New MCP Tool**: `compare_schemas` - Compare schemas between two data sources
  - Returns differences in columns, types, and nullability
  - Identifies breaking vs non-breaking changes
  - Supports Excel sheets via `source_sheet`/`target_sheet` params
  - Provides compatibility summary for quick assessment

- **New AI Workflow Recipes** (4 new recipes, 14 total):
  - Recipe 11: "Analyze Unknown Connection" - Full discovery workflow for storage/database
  - Recipe 12: "Compare Source vs Target Schema" - Schema compatibility check
  - Recipe 13: "Catalog a Database" - AI-powered data cataloging
  - Recipe 14: "Suggest Pipeline from Discovered Data" - End-to-end pipeline generation

### Changed

- Total MCP tools: 45 → 46
- Documentation updates across mcp_guide.md, mcp_recipes.md, mcp_system_prompt.md, rules.md

## [2.12.0] - 2026-01-23

### Added - Exploration Mode & Discovery Tools

- **Exploration Mode**: Minimal config format for data exploration without full pipeline setup
  - Only requires `connections:` block - no pipelines, story, or system needed
  - Enables AI to explore data sources before building pipelines

- **New Discovery Tools**:
  - `discover_database` - Crawl SQL database with schemas and samples in one call
  - `discover_storage` - Crawl file storage recursively with schemas and samples
  - Both tools support recursive scanning for nested folders (Excel files in subdirs)

- **Enhanced `list_sheets`**: List all sheet names in Excel files for discovery

### Fixed

- `FileInfo` contract: Fixed field names (`logical_name`/`physical_path` not `name`/`path`)

## [2.11.0] - 2026-01-23

### Added - MCP Facade for AI Assistants

- **MCP Facade**: Complete Model Context Protocol implementation for AI assistant integration
  - Read-only interface ensuring AI cannot mutate data or trigger runs
  - Single-project scoping with deny-by-default access controls
  - Typed Pydantic responses (no Dict[str, Any])

- **Core Contracts** (`odibi_mcp/contracts/`):
  - `TruncatedReason` enum for typed truncation responses
  - `MCPEnvelope`, `PolicyApplied` for standardized responses
  - `RunSelector` for flexible run targeting (latest, by ID, time-based)
  - `ResourceRef` with gated physical path exposure
  - `AccessContext`, `ConnectionPolicy` for unified access control
  - `TimeWindow`, `SchemaResponse`, `GraphData`, `DiffSummary`, etc.

- **Access Enforcement** (`odibi_mcp/access/`):
  - Project scoping in `CatalogManager._apply_project_scope()`
  - Path validation with allow/deny prefix lists
  - Physical ref gating (3 conditions must pass)
  - Audit logging with sensitive key redaction

- **MCP Tools**:
  - **Story**: `story_read`, `story_diff`, `node_describe`
  - **Sample**: `node_sample`, `node_sample_in`, `node_failed_rows`
  - **Catalog**: `node_stats`, `pipeline_stats`, `failure_summary`, `schema_history`
  - **Lineage**: `lineage_upstream`, `lineage_downstream`, `lineage_graph`
  - **Schema**: `output_schema`, `list_outputs`
  - **Discovery**: `list_files`, `list_tables`, `infer_schema`, `describe_table`, `preview_source`

- **Error Handling**: Standardized `MCPError` with typed error codes

- **Documentation**:
  - `odibi_mcp/README.md` with full tool reference
  - `mcp_config.example.yaml` configuration template
  - `docs/mcp/examples/basic_usage.py` usage examples

### Changed

- `CatalogManager`: Added `set_access_context()` and `_apply_project_scope()` for MCP integration

## [2.10.0] - 2026-01-22

### Added - Documentation Generation from Story Artifacts

- **DocGenerator Phase 1**: New documentation generation system that creates LLM-ready markdown from Story artifacts
  - `README.md`: Project overview with all pipelines, node counts, run status, duration
  - `TECHNICAL_DETAILS.md`: Engineer-facing exhaustive details, output schemas, SQL queries
  - `NODE_CARDS/*.md`: Per-node transformation documentation with schemas, SQL, config
  - `RUN_HISTORY.md`: Consolidated run memos with newest first

- **DocGenerator Phase 2 - Full Metadata Surfacing**: All tracked Story metadata now surfaced for AI consumption
  - **README.md enhancements**: Run Health Summary, Data Freshness indicator, Key Metrics (rows in/out/dropped), Git info, Pipeline DAG (Mermaid)
  - **Node Cards enhancements**: Sample Data, Null Profile, Column Statistics, Failed Rows Samples, Source Files, Delta Write Info, Duration History, Cross-run Comparison, Execution Steps, Retry History, Environment
  - **New helper methods**: `_render_mermaid_dag()`, `_render_sample_data_table()`, `_render_null_profile()`, `_render_column_statistics()`, `_render_failed_rows_samples()`

- **Centralized Error Suggestions Engine**: Rich contextual error suggestions with story integration

- **Remote Storage Support**: DocGenerator now writes to ADLS when connection is configured

- **Semantic Layer Docs**: Semantic layer now included in project-level documentation

### Changed

- **TECHNICAL_DETAILS.md**: Now shows all output schemas instead of just final node
- **RUN_HISTORY.md**: Multiple runs consolidated into single file (newest first)
- **Project-level Aggregation**: Docs now aggregate across all pipelines in a project

## [2.9.0] - 2026-01-20

### Added - Engine Parity & Spark Improvements

- **Polars Azure SQL Read Support**: Polars engine now supports reading from Azure SQL/SQL Server databases, completing engine parity across all three engines (Pandas, Spark, Polars)
  - Supports `azure_sql`, `sql_server`, and `sql` format types
  - Automatic schema.table parsing with `dbo` default schema
  - Returns Polars LazyFrame for lazy evaluation

### Fixed

- **Spark cloudFiles.schemaLocation**: Schema location path now correctly resolves through write connection for Auto Loader streaming
- **Spark Delta Streaming Properties**: Delta table properties (optimizeWrite, autoCompact) now correctly applied for streaming writes
- **Spark Column Name Escaping**: Column names with special characters now escaped with backticks in Spark engine
- **regex_replace Transformer**: Fixed duplicate column bug when replacing values

### Changed

- **Python 3.9 Compatibility**: Multiple fixes for CI compatibility including black version pinning and conditional imports

## [2.8.0] - 2026-01-18

### Added - AI Assistant MCP Server Enhancements

- **odibi-knowledge MCP Server** (`odibi_mcp`): Now includes 21 tools for AI assistants
  - Integrated into main package via `pip install odibi[mcp]` or `pip install odibi[mcp-rag]`

- **New Decision Support Tools**:
  - `suggest_pattern` - Recommends the right pattern (scd2, merge, dimension, fact, aggregation, date_dimension) based on use case description with keyword matching and confidence scoring
  - `get_engine_differences` - Documents critical differences between Spark, Pandas/DuckDB, and Polars SQL dialects (type names, array indexing, date functions, etc.)
  - `get_validation_rules` - Complete reference for all 10 validation rule types with YAML examples

- **Code Generation Tools** (from previous session):
  - `generate_transformer` - Generates complete Python code for custom transformers with correct signature
  - `generate_pipeline_yaml` - Generates complete valid YAML pipeline configs
  - `validate_yaml` - Validates YAML configs and returns specific errors before runtime
  - `diagnose_error` - Diagnoses common odibi errors and suggests fixes
  - `get_example` - Dynamically loads examples from 150+ documentation files

- **AI Assistant Configuration**:
  - Updated `.clinerules` with all 21 MCP tools organized by category
  - Updated `.continue/rules.md` for Continue IDE integration
  - Updated `docs/guides/ai-assistant-setup.md` with complete tool reference

### Testing

- **MCP Knowledge Tests**: Added 29 unit tests for all MCP knowledge tools (`tests/unit/test_mcp_knowledge.py`)

### Documentation

- Complete MCP tool reference in `docs/guides/ai-assistant-setup.md`

## [2.7.0] - 2026-01-14

### Added - Excel Reading Enhancements

- **Excel Glob Patterns**: Read multiple Excel files with glob patterns like `"Sales/*.xlsx"` or `"data/**/*.xlsx"`
- **Sheet Pattern Matching**: New `sheet_pattern` option filters sheets by pattern (e.g., `["*powerbi*", "*power bi*"]`)
- **Source File Tracking**: New `add_source_file` option adds `_source_file` and `_source_sheet` columns for data lineage
- **Cloud Excel Support**: Excel reading now works with Azure Blob Storage and S3 via fsspec integration

### Fixed

- **SQL Server Overwrite**: Fixed `auto_create_schema` and `overwrite_options` passthrough in SQL Server overwrite mode

## [2.6.6] - 2026-01-14

### Fixed

- **Derived metrics now appear at end of views**: Simple metrics are emitted first in YAML order, then derived metrics at the end. This ensures predictable column ordering matching source tables.

## [2.6.5] - 2026-01-14

### Fixed

- **View columns now preserve YAML order**: Metrics appear in the order listed in `views.metrics`, not alphabetically sorted

## [2.6.4] - 2026-01-14

### Added

- **Semantic Layer: Display Labels** for metrics and dimensions:
  - New `label` field on `MetricDefinition` and `DimensionDefinition`
  - Controls column alias in generated views (e.g., `AS [Total Calendar Hours]`)
  - Defaults to `name` if not specified
  - Column aliases now wrapped in `[]` brackets for SQL Server compatibility with spaces

### Fixed

- **Version now read dynamically** from package metadata via `importlib.metadata`:
  - `odibi.__version__` now matches `pyproject.toml` automatically
  - Removed hardcoded `__version__` from submodules (`validation`, `testing`, `story`, `semantics`)
  - Stories and all version references now show the correct installed version

## [2.6.1] - 2026-01-11

### Added - Executive Dashboard Views & SLA Tracking

- **Executive Dashboard Views** (SQL Server sync):
  - `vw_pipeline_health_status` - RAG status per pipeline with health reasons
  - `vw_exec_overview` - Project-level summary with 7d/30d/90d success rates and trends
  - `vw_table_freshness` - Data staleness monitoring with freshness RAG
  - `vw_pipeline_sla_status` - SLA compliance dashboard with breach detection
  - `vw_exec_current_issues` - Currently failing pipelines with error details
  - `vw_pipeline_risk` - Risk scoring based on failure rate and criticality
  - `vw_cost_summary` - Cost tracking with 7d/30d trends

- **Business Context Table** (`dim_pipeline_context`):
  - Manually populated dimension for business metadata
  - Fields: `business_owner`, `business_process`, `business_criticality`
  - Enhances executive views with priority-based alerting

- **Cost Tracking Columns**:
  - Added `estimated_cost_usd`, `actual_cost_usd`, `cost_source` to `meta_pipeline_runs`
  - Added `estimated_cost_usd` to `meta_node_runs`
  - Cost aggregation in `meta_daily_stats`

- **SLA Tracking Enhancement**:
  - Added `environment` column to `meta_sla_status` for multi-environment support
  - SLA sync now properly replicates to SQL Server

### Fixed

- Schema mismatch in test fixtures for `meta_pipeline_runs` (21 vs 24 fields)
- SQL Server DDL for `meta_sla_status` missing `environment` column
- `cost_trend` in `vw_cost_summary` now correctly typed as FLOAT

### Documentation

- Added Executive Dashboard Views section to `docs/features/observability.md`
- Documented `dim_pipeline_context` schema and usage
- Added SQL examples for populating business context

## [2.6.0] - 2026-01-10

### Added - Developer Experience: Golden Path & Debugging

- **Golden Path Templates** (`odibi init`):
  - Templates now aligned with `docs/golden_path.md` canonical examples
  - Three templates: `hello`, `scd2`, `star-schema`
  - Each template includes sample data from canonical examples
  - Improved interactive prompt with descriptions

- **Frictionless Debugging** (`PipelineResults.debug_summary()`):
  - New method generates actionable next steps when pipelines fail
  - Shows failed nodes with truncated error messages
  - Points to story path and specific debugging commands
  - Automatically printed by CLI on failure

- **Story Commands** (`odibi story last`, `odibi story show`):
  - `odibi story last` - Opens most recent story (HTML in browser, JSON summary)
  - `odibi story last --node <name>` - Filter to specific node details
  - `odibi story show <path>` - View any story file

- **Improved CLI Output**:
  - Success: Shows story path and view command
  - Failure: Shows debug summary with next steps
  - CLI epilog updated with Golden Path quick start

### Changed

- `odibi init` now copies sample data from canonical examples
- README template updated with Golden Path workflow and debugging guide
- CLI help reorganized into "Golden Path", "Core Commands", and "Debugging" sections

### Fixed

- **Validation bug**: Patterns (`dimension`, `fact`, `scd2`, etc.) were incorrectly failing validation with "not registered" error
- **Story search**: `odibi story last` now recursively searches nested story directories

## [2.5.0] - 2026-01-09

### Added - System Environments & SQL Server Backend

- **Environment Tagging** (`system.environment`):
  - Tag all `meta_runs` and `meta_state` records with environment (dev, qat, prod)
  - Enables cross-environment querying from a single catalog location
  - Environment column added to both Delta and SQL Server schemas

- **SQL Server System Backend** (`SqlServerSystemBackend`):
  - Store system catalog tables in SQL Server instead of Delta
  - Auto-creates schema and tables (`[odibi_system].[meta_runs]`, `[odibi_system].[meta_state]`)
  - Configure via `system.connection` pointing to sql_server type connection
  - `system.schema_name` controls target schema (default: `odibi_system`)

- **System Sync** (`odibi system sync`):
  - New CLI command to sync system data between backends
  - Push local development runs/state to centralized SQL Server
  - `sync_from` config specifies source backend
  - Options: `--env`, `--tables runs|state`, `--dry-run`

- **SyncFromConfig** model:
  - `connection`: Source connection name
  - `path`: Source path (for file-based backends)
  - `schema_name`: Source schema (for SQL Server)

### Documentation
- Updated `docs/features/catalog.md` with SQL Server backend, environment tagging, and sync sections
- Updated `docs/guides/environments.md` with system environment use cases
- Regenerated `docs/reference/yaml_schema.md` with new config models

## [2.4.2] - 2026-01-09

### Testing
- **FK Validation Coverage**: Added 51 new tests for `fk.py` (32% → 88% coverage)
  - Full RelationshipConfig and RelationshipRegistry tests
  - FKValidator Pandas engine tests with orphan detection
  - validate_fk_on_load convenience function tests (error, warn, filter modes)
  - parse_relationships_config tests
- **Explanation Linter Coverage**: Added 42 new tests for `explanation_linter.py` (40% → 100% coverage)
  - All lint rules tested (E001-E004, W001)
  - All lazy phrases and required sections validated
  - format_issues and has_errors methods tested

## [2.4.1] - 2026-01-09

### Fixed
- **Freshness Validation Bug**: Fixed `TypeError` in freshness check when datetime conversion fails (returns `None` instead of `pd.NaT`)

### Testing
- **Validation Module Coverage**: Added 54 new tests for validation module (109 → 163 total)
  - Full Polars engine coverage for all test types (NOT_NULL, UNIQUE, RANGE, ACCEPTED_VALUES, REGEX_MATCH, FRESHNESS, SCHEMA)
  - Polars LazyFrame support tested
  - `write_quarantine()` function fully tested
  - Exception handling paths covered
  - Missing column edge cases for all engines

## [2.4.0] - 2026-01-09

### Open Source Release

- **License**: Changed from MIT to Apache 2.0
- **PyPI**: Published to PyPI (`pip install odibi`)
- **Documentation**: Deployed docs site to GitHub Pages
- **Community**: Added GitHub issue templates and enabled Discussions
- **Cleanup**: Removed internal/experimental modules (agents, internal docs)
- **Public Docs**: Added `docs/philosophy.md` with project principles

## [2.2.0] - 2026-01-01

### Added - SQL Server Merge Enhancements

- **Incremental Merge Optimization** (`merge_options.incremental`):
  - `incremental: true` - Reads target hashes, compares in Spark/Pandas/Polars, only writes changed rows to staging
  - `hash_column: _hash_diff` - Use existing hash column for change detection
  - `change_detection_columns: [col1, col2]` - Compute hash from specified columns
  - Auto-detects `_hash_diff` if present, otherwise computes from all non-key columns

- **Audit Columns** (`merge_options.audit_cols`):
  - `created_col` - Set to GETUTCDATE() on INSERT only
  - `updated_col` - Set to GETUTCDATE() on INSERT and UPDATE
  - Auto-added to table on `auto_create_table`

- **Spark Engine Parity**:
  - `merge_pandas` now supports `auto_create_schema`, `auto_create_table`, `primary_key_on_merge_keys`
  - All engines (Spark, Pandas, Polars) now have feature parity for SQL Server merge

## [2.1.1] - 2025-12-10

### Performance - Delta Write Optimization
- **Skip Redundant Table Registration**: Tables using `register_table` now check `catalog.tableExists()` before running `CREATE TABLE IF NOT EXISTS`, saving 10-20s per incremental write.
- **Batch Table Properties**: `ALTER TABLE SET TBLPROPERTIES` now batches all properties into a single SQL statement instead of one-per-property, saving 3-6s per node.
- **Cache Table Existence Checks**: `NodeExecutor` now caches table existence checks per execution, avoiding repeated Delta table open + limit(1).collect() operations (3-5s each).

These optimizations target the ~96% write phase overhead identified in Bronze pipeline profiling, with expected cumulative savings of 15-30s per node for incremental Delta writes.

## [2.1.0] - 2025-11-21

### Added - Developer Experience (DX)
- **Unified Key Vault Support**: Fetch credentials for ANY connection (ADLS, Azure SQL) from Key Vault using `key_vault_name` and `secret_name`, regardless of authentication mode (SAS, Account Key, SQL Auth).
- **Master Template (`odibi create`)**: Comprehensive, verifiable "Kitchen Sink" template documenting every feature with inline comments.
- **Auto-Detect Auth**: Azure ADLS connections now auto-detect auth mode (`sas_token`, `key_vault`, `service_principal`, or `managed_identity`) based on provided config.
- **Spark Engine Hardening**:
  - Fixed SQL transformation support (`execute_sql`) on Spark context.
  - Added native PySpark implementations for data validation (`ranges`, `allowed_values`).
  - Enabled proper JDBC connection string generation for Azure SQL on Spark.
- **Delta Connection Support**: Native support for `type: delta` in configuration, creating `LocalConnection` (path-based) or `DeltaCatalogConnection` (catalog-based) automatically.
- **Documentation**: Explanatory comments in templates for Retry Strategies, Validation Modes, and Security best practices.

### Fixed
- **Spark SQL Crash**: Fixed `SparkEngine.execute_sql` trying to call `.items()` on SparkContext.
- **Validation Logic**: Decoupled validation logic from Node execution, allowing engine-specific implementations (preventing Pandas logic from running on Spark DataFrames).
- **Configuration Gaps**: Resolved discrepancies between `odibi create` template and actual codebase capabilities.

## [2.0.0] - 2025-11-20

### Added - Production Hardening (Phase 4)
- **Retry Logic**: Configurable exponential backoff for node failures (`retry` config section).
- **Checkpointing**: Resume failed pipelines from last successful node with `--resume-from-failure`.
- **Parallel Execution**: Execute independent nodes concurrently using execution layers.
- **Alerting System**: Webhook integration for Slack/Teams notifications on start/success/failure.
- **PII Redaction**: `sensitive: true` flag on nodes to mask data in generated stories.
- **Structured Logging**: JSON logging support for integration with log aggregators (Splunk, Datadog).
- **Error Strategies**: Configurable failure handling (`fail_fast`, `fail_later`, `ignore`).
- **State Management**: `StateManager` to track execution status for checkpointing.

### Changed
- **Performance**: optimized execution graph resolution.
- **Doctor**: Enhanced system diagnostics to check for new production features.

## [1.3.0] - 2025-11-15

### Added - CLI & Connectivity (Phase 3)
- **Full CLI Suite**:
  - `odibi doctor`: System diagnostics.
  - `odibi graph`: Visualization of pipeline dependencies.
  - `odibi validate`: Schema and logic validation.
- **Azure SQL Connector**: Production-ready SQLAlchemy/ODBC connection with Managed Identity support.
- **Story Themes**: Custom styling for HTML reports (Corporate, Dark, Minimal).
- **Story Renderers**: Pluggable renderers for HTML, Markdown, and JSON outputs.
- **Documentation**: Comprehensive CLI reference.

## [1.2.0-alpha.4-phase2.5] - 2025-11-10

### Added
- **CLI Module (`odibi/cli/`)**: Dedicated command-line interface
  - `odibi run config.yaml` - Execute pipelines
  - `odibi validate config.yaml` - Validate configuration
  - Entry point via `python -m odibi` or `odibi` command
- **Phase 3 Scaffolding**: Empty modules with comprehensive documentation
  - `odibi/operations/` - Built-in operations (pivot, join, etc.)
  - `odibi/transformations/` - Transformation registry
  - `odibi/validation/` - Quality enforcement
  - `odibi/testing/` - Testing utilities
- **Dependencies for Phase 3**:
  - `markdown2>=2.4.0` - Story markdown rendering
  - `Jinja2>=3.1.0` - HTML templating
  - `pyodbc>=5.0.0` (optional) - Azure SQL support
  - `sqlalchemy>=2.0.0` (optional) - SQL toolkit
- **87 new tests** - Comprehensive coverage increase
  - 11 CLI unit tests
  - 21 module structure tests
  - 14 CLI integration tests
  - 41 pandas_engine tests (21% → 94% coverage!)
- **CI/CD Enhancement**: New test-sql job for SQL dependencies

### Changed
- **Test coverage**: 68% → 79% (+16% improvement!)
- **Test count**: 137 → 224 tests (+87 tests, +63%)
- **Project structure**: Reorganized for Phase 3 readiness
- **README.md**: Updated with CLI usage examples
- **PROJECT_STRUCTURE.md**: Complete restructure documentation
- **Version**: 1.2.0-alpha.3 → 1.2.0-alpha.4-phase2.5

### Technical
- Zero breaking changes to public API
- Fully backward compatible with Phase 2C
- All Python versions (3.9-3.12) validated
- CLI fully functional and tested
- Phase 3 modules importable but empty (v0.0.0)

### Documentation
- Created PHASE3_ROADMAP.md
- Updated PROJECT_STRUCTURE.md
- Enhanced module docstrings
- Clear separation between implemented and planned features

---

## [1.2.0-alpha.3-phase2c] - 2025-11-10

### Added - Performance & Setup Utilities (Phase 2C)
- **Parallel Key Vault fetching** - 3x+ faster startup with `configure_connections_parallel()`
- **Timeout protection** - 30s default timeout for Key Vault operations prevents hanging
- **Setup utilities module** - `odibi.utils.setup_helpers` with programmatic configuration
- **Databricks validation** - `validate_databricks_environment()` checks runtime/Spark/dbutils
- **Interactive setup notebook** - `setup/databricks_setup.ipynb` with step-by-step guide
- **Performance walkthrough** - `walkthroughs/phase2c_performance_keyvault.ipynb`
- **Multi-account test notebook** - `walkthroughs/databricks_multiaccount_test.ipynb`
- **Complete Databricks test** - `walkthroughs/databricks_complete_test.ipynb`
- **15 new tests** for setup utilities (`tests/test_setup_helpers.py`)
- `KeyVaultFetchResult` dataclass for detailed error reporting
- `pyarrow>=10.0.0` added to dependencies for Delta Lake support

### Fixed
- **CRITICAL:** `SparkEngine.execute_sql()` now registers context DataFrames as temp views
  - Was causing "table not found" errors in SQL transformations
  - All SQL queries with context now work correctly
- **CRITICAL:** `SparkEngine` now properly exported from `odibi.engine` module
  - Can now import with `from odibi.engine import SparkEngine`
- Added timeout protection to `AzureADLS.get_storage_key()` to prevent indefinite hanging

### Changed
- CI workflow now installs `.[all,dev]` for complete test coverage
- Version bumped to `1.2.0-alpha.3`
- Dev dependencies now include PySpark, Delta Spark, and Azure packages
- Updated PHASES.md and STATUS.md to reflect Phase 2C completion

### Databricks Validation
- ✅ Multi-account ADLS (2 storage accounts configured and verified)
- ✅ Cross-account data transfer (medallion architecture: Bronze → Silver)
- ✅ Delta Lake time travel (versionAsOf tested)
- ✅ Schema introspection (get_schema, get_shape, count_rows)
- ✅ SQL transformations with temp view registration
- ✅ Complete pipeline execution
- ✅ All Phase 2 features validated in production Databricks environment

### Performance
- 3x faster startup with 3 Key Vault connections
- 4.4x faster startup with 5 Key Vault connections
- Timeout protection prevents indefinite waits
- Detailed per-connection timing metrics

### Tests
- 137 total tests passing (was 122)
- 15 new setup utility tests
- All Phase 2A and 2B tests still pass
- Zero breaking changes

## [1.2.0-alpha.2-phase2b] - 2025-11-09

### Added - Delta Lake Support (Phase 2B)
- **Delta Lake read/write** support in PandasEngine using `deltalake` package
- **Delta Lake read/write** support in SparkEngine using `delta-spark` package
- **Time travel** with `versionAsOf` option for reading specific Delta versions
- **VACUUM operation** (`vacuum_delta()`) to clean old files and save storage
- **History tracking** (`get_delta_history()`) to list all Delta table versions
- **Restore operation** (`restore_delta()`) to rollback to previous versions
- **Partitioning support** with performance anti-pattern warnings
- **12 comprehensive Delta tests** in `tests/test_delta_pandas.py`
- `delta-spark>=2.3.0` dependency added to `spark` extras

### Changed
- SparkEngine now auto-configures Delta Lake support when `delta-spark` is available
- Both engines now emit warnings when using partitioning to prevent performance issues
- Updated STATUS.md to reflect Phase 2B completion
- Updated PHASES.md with Phase 2B deliverables marked complete

### Notes
- Delta Lake is now fully integrated with both Pandas and Spark engines
- All Delta operations work seamlessly with ADLS connections
- Tests pass with 122 total tests (84 core + 26 ADLS + 12 Delta)
- Phase 2B complete - moving to Phase 2C (Performance & Polish)

## [1.2.0-alpha.1-phase2a] - 2025-11-09

### Added
- **Avro format support** in PandasEngine for read/write operations
- **PipelineManager** class for multi-pipeline orchestration from YAML
- **Pipeline.from_yaml()** now returns PipelineManager (backward-compatible)
- Comprehensive `examples/template_full.yaml` documenting all YAML options
- `fastavro>=1.8.0` dependency for Avro support
- `pandasql>=0.7.3` dependency for SQL fallback
- **Story configuration** now requires explicit connection reference
- Validation that story connection exists in connections section
- Complete documentation: `docs/CONFIGURATION_EXPLAINED.md` (500+ lines)

### Changed
- **BREAKING**: `ProjectConfig` now requires `story`, `connections`, and `pipelines` fields (no defaults)
- **BREAKING**: Stories now use connection pattern - `story.connection` is mandatory
- **BREAKING**: Removed `DefaultsConfig` and `PipelineDiscoveryConfig` classes
- **BREAKING**: Settings flattened to top-level (retry, logging, story instead of nested defaults)
- **API Improvement**: `Pipeline.from_yaml()` returns manager with `run()`, `run('name')`, `run(['names'])` methods
- Run all pipelines by default, specify by name (not index) for clarity
- Single source of truth: `ProjectConfig` represents entire YAML (no raw dict parsing)
- Updated PHASES.md to include Avro in SparkEngine Phase 3 roadmap

### Migration Guide (v1.0 → v1.1)

**Before (v1.0):**
```yaml
connections:
  local:
    type: local
    base_path: ./data

# Story path was implicit/floating
```

**After (v1.1):**
```yaml
connections:
  data:
    type: local
    base_path: ./data
  outputs:
    type: local
    base_path: ./outputs

story:
  connection: outputs  # Required: explicit connection
  path: stories/       # Resolved to ./outputs/stories/
  enabled: true

retry:
  max_attempts: 3
  backoff_seconds: 2.0

logging:
  level: INFO

pipelines:
  - name: my_pipeline
    # ... rest of config
```

**Key Changes:**
1. Add `story.connection` field pointing to an existing connection
2. Move retry/logging settings to top-level (remove `defaults` wrapper)
3. All three sections (`story`, `connections`, `pipelines`) are now mandatory

**Why:** Stories now follow the same explicit connection pattern as data, providing clear traceability and single source of truth.

### Fixed
- Dependency scanning across entire odibi module (all external deps captured)
- Config validation now prevents missing story connections
- Eliminated dual parsing (ProjectConfig is single source of truth)

## [1.1.0-alpha.1-ci-setup] - 2025-11-06

### Added
- Continuous Integration workflow (`.github/workflows/ci.yml`) for multi-Python (3.9–3.12) testing
- Pre-commit configuration (`.pre-commit-config.yaml`) for Black, Ruff, and file hygiene checks
- Pip caching and Codecov coverage integration for faster, visible builds

### Improved
- All 89 tests passing (78 Pandas core, 8 Azure connection, 3 Spark imports)
- Codebase reformatted and linted automatically
- Code quality gates now active on every commit (local) and push (remote)

### Notes
- Phase 1D complete: CI/CD infrastructure now active on every commit and push
- CI runs on Ubuntu with Python 3.9, 3.10, 3.11, 3.12
- Pre-commit hooks enforce Black formatting, Ruff linting, YAML/TOML validation

## [1.1.0-alpha.1] - TBD

### Added
- Scaffolded Spark engine with import guards (Phase 1 - stubs only)
- Scaffolded Azure connections: ADLS Gen2, Azure SQL, local DBFS mock
- Open-source governance files: CONTRIBUTING.md, CODE_OF_CONDUCT.md, SECURITY.md, CODEOWNERS
- CHANGELOG.md following Keep a Changelog format
- Import guard tests for optional dependencies
- Connection path resolution tests (no network I/O)
- Type distribution support (`py.typed` marker file)

### Changed
- Flattened optional dependencies in `pyproject.toml` (removed self-referencing `[all]` extra)
- Commented out CLI entry point (not implemented yet)

### Fixed
- Package configuration for better dependency resolution

## [1.0.0] - 2025-11-05

### Added
- Initial release: Pandas MVP with 78 passing tests
- Core framework components:
  - Context manager for pipeline execution
  - Registry for managing nodes and connections
  - DAG-based dependency graph
  - Pipeline orchestration
- Local filesystem connection
- Pydantic-based configuration system
- YAML pipeline definitions
- Comprehensive examples and documentation
- Getting started walkthrough notebook
- Development setup with Black, Ruff, mypy
- Test suite with pytest

### Documentation
- README with quickstart guide
- Example pipelines and configurations
- API reference for core components

[Unreleased]: https://github.com/henryodibi11/Odibi/compare/v1.1.0-alpha.1...HEAD
[1.1.0-alpha.1]: https://github.com/henryodibi11/Odibi/compare/v1.0.0...v1.1.0-alpha.1
[1.0.0]: https://github.com/henryodibi11/Odibi/releases/tag/v1.0.0
