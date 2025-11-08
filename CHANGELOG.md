# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
