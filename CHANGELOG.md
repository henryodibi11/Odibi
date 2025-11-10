# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
