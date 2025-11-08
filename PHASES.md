# ODIBI Framework - Evolution Phases

**Version Strategy:** Semantic Versioning (SemVer)  
**Current Version:** v1.0.0 (Pandas MVP Complete)  
**Status:** Phase 1 in progress

---

## Overview

ODIBI will evolve through 5 distinct phases, each building on the previous foundation. Each phase has clear deliverables, acceptance criteria, and version targets.

**Principles:**
- **Non-breaking:** New engines and connectors are opt-in via extras
- **Tested:** All features require tests before promotion to stable
- **Documented:** New capabilities include examples and setup guides
- **Transparent:** CHANGELOG.md tracks all changes

---

## Phase 1 — Spark Engine + Azure Integrations (Scaffolding)

**Target Version:** v1.1.0-alpha.1 → v1.1.0  
**Status:** ✅ Complete (v1.1.0-alpha.2-walkthroughs)  
**Completed:** November 2025

### Goals
- ✅ Scaffold Spark engine and Azure connections **without breaking Pandas**
- ✅ Provide structure, docs, and examples to enable contributions
- ✅ Establish open-source governance and community standards

### Deliverables

#### Code Scaffolding
- [x] **Engine:** `odibi/engine/spark_engine.py`
  - Class `SparkEngine` implementing `Engine` interface
  - Import-guarded (raises helpful error if `pyspark` not installed)
  - Methods stubbed with `NotImplementedError` and PHASES.md references
  - Basic introspection methods implemented (`get_schema`, `get_shape`, `count_rows`)

- [x] **Connections:** Azure and mock DBFS
  - `odibi/connections/azure_adls.py` - Azure Data Lake Storage Gen2 path resolver
  - `odibi/connections/azure_sql.py` - Azure SQL connection config
  - `odibi/connections/local_dbfs.py` - Mock DBFS for local testing
  - All implement `BaseConnection` interface
  - No network I/O in validation phase

#### Documentation
- [x] **Setup Guides:**
  - `docs/setup_databricks.md` - Databricks cluster setup, notebook integration
  - `docs/setup_azure.md` - Authentication options, permissions, connection templates

- [x] **Examples:**
  - `examples/example_spark.yaml` - Spark engine template with ADLS/DBFS
  - `examples/example_local.yaml` - Simplified local Pandas example

#### Open-Source Standards
- [x] **Governance:**
  - `CONTRIBUTING.md` - Contribution workflow, coding standards, testing
  - `CODE_OF_CONDUCT.md` - Contributor Covenant v2.1
  - `SECURITY.md` - Vulnerability reporting process
  - `CODEOWNERS` - Maintainer assignments
  - `CHANGELOG.md` - Version history (Keep a Changelog format)

- [x] **GitHub Templates:**
  - `.github/ISSUE_TEMPLATE/bug_report.md`
  - `.github/ISSUE_TEMPLATE/feature_request.md`
  - `.github/PULL_REQUEST_TEMPLATE.md`

- [x] **CI/CD:**
  - `.github/workflows/ci.yml` - Multi-Python version testing
  - Job: `test-base` (required) - Pandas tests on Python 3.9–3.12
  - Job: `test-extras` (optional) - Spark/Azure import tests
  - `.pre-commit-config.yaml` - black, ruff, trailing whitespace

#### README Updates
- [x] Add badges (build status, Python versions, license, PyPI)
- [x] Document installation with extras (`pip install "odibi[spark]"`)
- [x] Update roadmap section to reference PHASES.md
- [x] Link to CONTRIBUTING.md and CODE_OF_CONDUCT.md

#### Testing
- [x] Import tests for new modules (skip if extras not installed)
- [x] Path resolution tests for Azure connections (no network calls)
- [x] All 78 existing Pandas tests still pass

#### Walkthroughs (Phase 1F)
- [x] **6 Jupyter notebooks** covering all core features
  - `00_setup_environment.ipynb` - Setup and mental model
  - `01_local_pipeline_pandas.ipynb` - Full pipeline example with explanations
  - `02_cli_and_testing.ipynb` - Testing patterns and CLI preview
  - `03_spark_preview_stub.ipynb` - Spark architecture overview
  - `04_ci_cd_and_precommit.ipynb` - Code quality automation
  - `05_build_new_pipeline.ipynb` - Build from scratch tutorial
- [x] **Concept explanations:** Config vs Runtime, SQL-over-Pandas
- [x] **Troubleshooting sections:** Common errors with solutions
- [x] **All notebooks tested** and verified to run cell-by-cell

#### Release
- [x] Git tag: `v1.1.0-alpha.1-ci-setup` (scaffolding release)
- [x] Git tag: `v1.1.0-alpha.2-walkthroughs` (walkthroughs complete)
- [x] Update CHANGELOG.md with scaffolding notes
- [x] GitHub Release with clear "experimental" status

### Acceptance Criteria
- [x] All 78 Pandas tests pass with zero modifications
- [x] New modules raise clear errors when extras not installed
- [x] CI green on base job (Pandas); extras job can succeed
- [x] Examples and docs clearly mark Spark/Azure as experimental
- [x] No breaking changes to existing Pandas pipelines

---

## Phase 2 — Spark Engine + Azure ADLS (Production-Ready)

**Target Version:** v1.2.0  
**Status:** 📋 Planning Complete → Ready for Implementation  
**Timeline:** Q1 2026

**Design Document:** [docs/PHASE2_DESIGN_DECISIONS.md](docs/PHASE2_DESIGN_DECISIONS.md)

### Goals
- Implement production-ready Spark engine (read, write, transform)
- Complete Azure ADLS integration with Key Vault authentication
- Enable multi-account storage scenarios (Databricks production use case)
- Provide seamless Databricks onboarding experience

### Deliverables

#### Spark Engine Implementation
- [ ] `SparkEngine.read()` - Parquet, CSV, Delta from ADLS/DBFS
- [ ] `SparkEngine.write()` - Parquet, CSV, Delta with modes (overwrite/append)
- [ ] `SparkEngine.execute_sql()` - SQL transforms with temp view registration
- [ ] Connection configuration at engine init (all storage accounts)
- [ ] Integration tests with local Spark session

#### Azure ADLS Authentication
- [ ] **Two auth modes:** `key_vault` (default) and `direct_key` (fallback)
- [ ] Key Vault integration using `DefaultAzureCredential` (Databricks managed identity)
- [ ] Credential caching (fetch once per connection)
- [ ] Storage options injection for Pandas (`pandas_storage_options()`)
- [ ] Spark config setup for all accounts (`configure_spark()`)
- [ ] Eager validation (fail fast on connection init)
- [ ] Production warning when using `direct_key` mode

#### User Onboarding Tools
- [ ] `setup/databricks_setup.ipynb` - Interactive Databricks + Key Vault setup
- [ ] `odibi/utils/setup_helpers.py` - Programmatic setup utilities
  - `get_databricks_identity_info()` - Get workspace managed identity
  - `print_keyvault_setup_instructions()` - Generate setup commands
- [ ] Automated identity detection and command generation

#### PandasEngine Enhancements
- [ ] `_merge_storage_options()` - Inject connection credentials
- [ ] Update `read()` to support ADLS with storage_options
- [ ] Update `write()` to support ADLS with storage_options
- [ ] Skip `mkdir` for remote URIs (abfss://, s3://, etc.)

#### Documentation
- [ ] `docs/PHASE2_DESIGN_DECISIONS.md` - Complete design rationale ✅
- [ ] `docs/databricks_setup.md` - Databricks + Key Vault setup guide
- [ ] Update `examples/template_full.yaml` - Show Key Vault auth
- [ ] Update `README.md` - Databricks quick start section
- [ ] Update `docs/CONFIGURATION_EXPLAINED.md` - Auth modes section

#### Examples
- [ ] `examples/example_spark_databricks.yaml` - Multi-account Spark pipeline
- [ ] `examples/example_azure_pandas.yaml` - Pandas with ADLS

### Acceptance Criteria
- [ ] Spark engine executes real pipelines in Databricks
- [ ] Multi-account storage works (3+ storage accounts simultaneously)
- [ ] Key Vault auth works with workspace managed identity
- [ ] Setup notebook guides users through one-time setup (< 5 minutes)
- [ ] All tests pass (Spark integration tests require pyspark installed)
- [ ] Documentation clear and tested in real Databricks environment

---

## Phase 3 — CLI Tools + Advanced Features

**Target Version:** v1.3.0  
**Status:** ⏳ Planned  
**Timeline:** Q2 2026

### Goals
- Improve developer experience with polished CLI tools
- Enhance story/report generation for pipeline runs
- Add more cloud connectors (AWS S3, GCP)
- Add testing utilities and fixtures

### Deliverables

#### CLI Enhancement
- [ ] `odibi validate <config>` - Validate YAML without execution
- [ ] `odibi run <config>` - Execute pipeline from CLI
- [ ] `odibi graph <config>` - Visualize dependency graph (ASCII art or export to DOT)
- [ ] `odibi config doctor <config>` - Lint YAML, show resolved connections
- [ ] Rich error messages with suggestions and context
- [ ] `--log-level` flag (DEBUG, INFO, WARNING, ERROR)
- [ ] `--dry-run` mode

#### Testing Utilities
- [ ] `odibi.testing.fixtures` - Temporary directories, sample data generators
- [ ] `odibi.testing.spark` - Mock Spark session factory (skipped if missing)
- [ ] `odibi.testing.assertions` - DataFrame equality helpers (engine-agnostic)
- [ ] Example datasets compatible with both Pandas and Spark

#### Story Generator Enhancements
- [ ] `odibi.story` module enhancement
- [ ] Capture run metadata: timestamps, node durations, success/failure
- [ ] Sample data snapshots (first N rows per node)
- [ ] Schema tracking (detect schema changes between nodes)
- [ ] Export formats: Markdown, JSON, HTML (basic)
- [ ] Configurable verbosity levels

#### New Connectors (Scaffolded → Implemented)
- [ ] `connections/s3.py` - AWS S3 using `boto3` (scaffold + docs)
- [ ] `connections/gcs.py` - Google Cloud Storage using `gcsfs` (scaffold + docs)
- [ ] `connections/azure_sql.py` - Implement read/write via SQLAlchemy + ODBC
- [ ] Connection factory: YAML `type` field → class instantiation

#### Examples
- [ ] `examples/spark_sql_pipeline/` - End-to-end Spark SQL transformation
- [ ] `examples/azure_etl/` - ADLS read → transform → Azure SQL write
- [ ] `examples/story_demo/` - Pipeline showcasing story generation

### Acceptance Criteria
- [ ] Stories generated for Pandas runs; Spark marked experimental
- [ ] At least one connector fully implemented with tests
- [ ] Spark engine can execute simple SQL transforms
- [ ] ~20 new tests for stories and connectors

---

## Phase 4 — Performance + Production Hardening

**Target Version:** v1.4.0  
**Status:** ⏳ Planned  
**Timeline:** Q3 2026

### Goals
- Stabilize API contracts and error semantics
- Optimize performance for production workloads
- Add retry logic and idempotency

### Deliverables

#### Performance
- [ ] Engine benchmarks (Pandas vs Spark for common operations)
- [ ] Lazy evaluation strategies where applicable
- [ ] Parallel node execution (use layers from graph analysis)
- [ ] Benchmark suite in `tests/benchmarks/`
- [ ] Performance guide (`docs/performance.md`)

#### Reliability
- [ ] Retry/backoff for connection failures (configurable)
- [ ] Idempotent write modes (append-once, upsert patterns)
- [ ] Schema validation improvements (consistent across engines)
- [ ] Structured logging (JSON output option)
- [ ] Log level controls per node

#### Error Handling
- [ ] Standardized error types across engines
- [ ] Error context preservation through stack
- [ ] Detailed error messages with actionable suggestions
- [ ] Error recovery strategies (skip, retry, fail-fast)

#### Documentation
- [ ] Production deployment guide (`docs/production.md`)
- [ ] Monitoring and observability guide
- [ ] Performance tuning recommendations

### Acceptance Criteria
- [ ] Benchmarks published and documented
- [ ] Retry logic tested with simulated failures
- [ ] Error messages consistently helpful across engines
- [ ] Zero regressions in test suite

---

## Phase 5 — Community + Ecosystem

**Target Version:** v2.0.0  
**Status:** ⏳ Planned  
**Timeline:** 2026

### Goals
- Grow community contributions and extensions
- Establish plugin ecosystem
- Create comprehensive documentation site

### Deliverables

#### Documentation Site
- [ ] MkDocs setup with versioning
- [ ] API reference (auto-generated)
- [ ] Tutorials and cookbook
- [ ] Architecture deep-dive
- [ ] Deploy to GitHub Pages

#### Extensibility
- [ ] Plugin system design (entry points for engines/connections)
- [ ] Extension guide (`docs/extending.md`)
- [ ] Example third-party engine/connector
- [ ] Registry for community plugins

#### Community
- [ ] First external contributor PRs merged
- [ ] Regular release cadence (monthly/quarterly)
- [ ] Release automation (Release Drafter, PyPI publish on tag)
- [ ] Discussion forum or GitHub Discussions enabled
- [ ] Examples gallery with community contributions

#### Advanced Features
- [ ] YAML schema validation (JSON Schema for autocomplete in IDEs)
- [ ] Pipeline templating and composition
- [ ] Environment-based configuration (dev/staging/prod)
- [ ] Secret management integration (Azure Key Vault, AWS Secrets Manager)

### Acceptance Criteria
- [ ] Docs site deployed and comprehensive
- [ ] At least 3 external contributors with merged PRs
- [ ] Plugin system validated with real extension
- [ ] Community engagement metrics tracked

---

## Version History

| Version | Status | Description | Date |
|---------|--------|-------------|------|
| v1.0.0 | ✅ Released | Pandas MVP - Core framework complete | 2025-11-05 |
| v1.1.0-alpha.1 | 🚧 In Progress | Phase 1 scaffolding (Spark/Azure stubs) | 2025-Q4 |
| v1.1.0 | ⏳ Planned | Phase 1 stable + Phase 2 CLI tools | 2026-Q1 |
| v1.2.0 | ⏳ Planned | Phase 2 stable | 2026-Q1 |
| v1.3.0 | ⏳ Planned | Phase 3 - Stories + Connectors | 2026-Q2 |
| v1.4.0 | ⏳ Planned | Phase 4 - Performance + Production | 2026-Q3 |
| v2.0.0 | ⏳ Planned | Phase 5 - Community + Ecosystem | 2026 |

---

## Contributing to Phases

See [CONTRIBUTING.md](CONTRIBUTING.md) for:
- How to pick up phase deliverables
- Branch naming conventions (`phase-1/spark-engine`)
- Testing requirements for new features
- Documentation standards

**Current Focus:** Phase 1 scaffolding  
**Next Up:** CLI tools and testing utilities

---

**Last Updated:** 2025-11-06  
**Maintainer:** @henryodibi11
