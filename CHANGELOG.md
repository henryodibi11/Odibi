# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
