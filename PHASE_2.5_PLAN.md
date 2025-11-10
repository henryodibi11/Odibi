# Phase 2.5: Reorganization & Foundation

**Status:** Planning  
**Duration:** 2 weeks  
**Purpose:** Prepare codebase for Phase 3 growth without breaking existing functionality

---

## Overview

Phase 2.5 ensures we have a stable, organized foundation before adding Phase 3 features. This phase focuses on:
- Reorganizing existing code for clarity
- Creating scaffolding for Phase 3 modules
- Adding necessary dependencies
- Ensuring all tests remain green throughout

**Principle:** No feature development, only structural improvements.

---

## Week 1: Audit & Scaffolding

### Day 1: Baseline & Audit

**Tasks:**
1. Run full test suite and document baseline
2. Generate test coverage report
3. Audit current folder structure
4. Identify files that should be reorganized

**Commands:**
```bash
# Baseline tests
cd Odibi
pytest -v --tb=short > test_baseline.txt
pytest --cov=odibi --cov-report=html

# Review structure
tree odibi/ -L 3 > structure_current.txt

# Check dependencies
poetry show --tree > dependencies_current.txt
```

**Deliverables:**
- `test_baseline.txt` - All tests passing (137/137)
- `structure_current.txt` - Current folder layout
- `dependencies_current.txt` - Current dependency tree
- Coverage report in `htmlcov/index.html`

**Acceptance Criteria:**
- ✅ All 137 tests pass
- ✅ Test coverage documented (current: 78%)
- ✅ Structure documented for comparison

---

### Day 2: CLI Module Reorganization

**Current State:**
```
odibi/
├── __main__.py  (CLI entry point)
├── pipeline.py  (may have CLI code mixed in)
```

**Target State:**
```
odibi/
├── cli/
│   ├── __init__.py
│   ├── main.py      # CLI entry point
│   ├── run.py       # odibi run command
│   └── validate.py  # odibi validate command
```

**Tasks:**
1. Create `odibi/cli/` folder structure
2. Extract CLI logic into dedicated files
3. Update `__main__.py` to import from `cli.main`
4. Run tests after each change

**Implementation:**
```bash
# Create structure
mkdir -p odibi/cli
touch odibi/cli/__init__.py
```

```python
# odibi/cli/__init__.py
"""
Command-line interface for Odibi.

Available commands:
- run: Execute a pipeline from YAML config
- validate: Validate YAML config without execution
"""
from .main import main

__all__ = ['main']
```

```python
# odibi/cli/main.py
"""Main CLI entry point."""
import sys
import argparse
from .run import run_command
from .validate import validate_command

def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description='Odibi Data Pipeline Framework')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # odibi run
    run_parser = subparsers.add_parser('run', help='Execute pipeline')
    run_parser.add_argument('config', help='Path to YAML config file')
    run_parser.add_argument('--env', default='development', help='Environment (development/production)')
    
    # odibi validate
    validate_parser = subparsers.add_parser('validate', help='Validate config')
    validate_parser.add_argument('config', help='Path to YAML config file')
    
    args = parser.parse_args()
    
    if args.command == 'run':
        return run_command(args)
    elif args.command == 'validate':
        return validate_command(args)
    else:
        parser.print_help()
        return 1

if __name__ == '__main__':
    sys.exit(main())
```

```python
# odibi/cli/run.py
"""Run command implementation."""
from odibi import Pipeline

def run_command(args):
    """Execute pipeline from config file."""
    try:
        pipeline = Pipeline(args.config)
        pipeline.run()
        print(f"✅ Pipeline completed successfully")
        return 0
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        return 1
```

```python
# odibi/cli/validate.py
"""Validate command implementation."""
from odibi import Config

def validate_command(args):
    """Validate config file."""
    try:
        config = Config.load(args.config)
        print(f"✅ Config is valid")
        return 0
    except Exception as e:
        print(f"❌ Config validation failed: {e}")
        return 1
```

```python
# odibi/__main__.py (updated)
"""Entry point for python -m odibi."""
from odibi.cli.main import main
import sys

if __name__ == '__main__':
    sys.exit(main())
```

**Testing:**
```bash
# Test imports
python -c "from odibi.cli import main; print('OK')"

# Test CLI still works
python -m odibi --help

# Run full test suite
pytest -v

# Manual integration test
python -m odibi run examples/example_local.yaml
```

**Deliverables:**
- `odibi/cli/` module created
- All CLI logic extracted
- Tests still passing (137/137)
- CLI commands still work

**Acceptance Criteria:**
- ✅ `odibi run` works
- ✅ `odibi validate` works
- ✅ All tests pass
- ✅ No breaking changes

---

### Day 3: Create Phase 3 Scaffolding

**Tasks:**
1. Create empty folders for Phase 3 modules
2. Add `__init__.py` with documentation
3. Update project structure documentation

**Implementation:**
```bash
# Create folders
mkdir -p odibi/operations
mkdir -p odibi/transformations
mkdir -p odibi/story
mkdir -p odibi/validation
mkdir -p odibi/testing

# Create __init__.py files
touch odibi/operations/__init__.py
touch odibi/transformations/__init__.py
touch odibi/story/__init__.py
touch odibi/validation/__init__.py
touch odibi/testing/__init__.py
```

```python
# odibi/operations/__init__.py
"""
Built-in Operations for Odibi Pipelines
========================================

This module contains framework-provided operations that users can reference
in their pipeline configurations.

Planned operations (Phase 3):
- pivot: Convert long-format to wide-format
- unpivot: Convert wide-format to long-format
- join: Merge two datasets
- sql: Execute SQL transformations
- aggregate: Common aggregations

Each operation provides:
- execute() method: Perform the transformation
- explain() method: Generate context-aware documentation
"""

__all__ = []
__version__ = "0.0.0"  # Will be 1.3.0 when implemented
```

```python
# odibi/transformations/__init__.py
"""
Transformation Registry and Decorators
=======================================

This module provides the infrastructure for users to define and register
custom transformations.

Planned features (Phase 3):
- @transformation decorator: Register user-defined transformations
- TransformationRegistry: Global registry of all transformations
- Context passing: Enable transformations to receive pipeline metadata

Example (future):
    from odibi import transformation
    
    @transformation("my_custom_calc")
    def my_custom_calc(df, threshold):
        '''Filter records above threshold.'''
        return df[df.value > threshold]
    
    @my_custom_calc.explain
    def explain(threshold, **context):
        plant = context.get('plant')
        return f"Filter {plant} records above {threshold}"
"""

__all__ = []
__version__ = "0.0.0"
```

```python
# odibi/story/__init__.py
"""
Story Generation System
=======================

This module provides automatic documentation and audit trail generation
for pipeline executions.

Planned features (Phase 3):
- Run Stories: Auto-generated execution logs (timing, row counts, errors)
- Doc Stories: Stakeholder-ready documentation with explanations
- Story Diffing: Compare two pipeline runs for troubleshooting
- Themes: Customizable branding and styling

Two story types:
1. Run Story (auto-generated every run):
   - What happened during execution
   - Performance metrics, row counts, schema changes
   - Errors and stack traces
   
2. Doc Story (generated on-demand):
   - How the pipeline works
   - Step-by-step explanations
   - Business context and formulas
   - Reference tables
"""

__all__ = []
__version__ = "0.0.0"
```

```python
# odibi/validation/__init__.py
"""
Quality Enforcement and Validation
===================================

This module enforces Odibi's quality standards through automated validation.

Planned features (Phase 3):
- Explanation linting: Ensure transformations are documented
- Quality scoring: Detect generic/lazy documentation
- Schema validation: Verify config structure
- Pre-run validation: Catch errors before execution

Principle: Enforce excellence, don't hope for it.
"""

__all__ = []
__version__ = "0.0.0"
```

```python
# odibi/testing/__init__.py
"""
Testing Utilities and Fixtures
===============================

This module provides utilities to help users test their pipelines.

Planned features (Phase 3):
- Test fixtures: Sample data generators, temp directories
- Assertions: DataFrame equality checks (engine-agnostic)
- Mock objects: Spark sessions, connections
- Test helpers: Common testing patterns

Example (future):
    from odibi.testing import fixtures, assertions
    
    def test_my_pipeline():
        with fixtures.temp_workspace() as ws:
            df = fixtures.sample_data(rows=100)
            result = my_transform(df)
            assertions.assert_df_equal(result, expected)
"""

__all__ = []
__version__ = "0.0.0"
```

**Testing:**
```bash
# Test imports don't break
python -c "import odibi.operations; print('OK')"
python -c "import odibi.transformations; print('OK')"
python -c "import odibi.story; print('OK')"
python -c "import odibi.validation; print('OK')"
python -c "import odibi.testing; print('OK')"

# Run full test suite
pytest -v
```

**Deliverables:**
- 5 new folders with documented `__init__.py` files
- PROJECT_STRUCTURE.md updated
- Tests still passing

**Acceptance Criteria:**
- ✅ All folders created
- ✅ Documentation clear about future plans
- ✅ No import errors
- ✅ All tests pass

---

### Day 4-5: Add Dependencies

**Tasks:**
1. Add Phase 3 dependencies to `pyproject.toml`
2. Test dependency resolution
3. Update lock file
4. Verify CI/CD still works

**Dependencies to add:**
```toml
# Add to pyproject.toml

[tool.poetry.dependencies]
python = "^3.9"
pandas = "^2.0.0"
pyyaml = "^6.0"
networkx = "^3.1"

# NEW - Phase 3 dependencies
markdown2 = "^2.4.0"     # Story generation: render markdown
Jinja2 = "^3.1.0"        # Story generation: HTML templating

# OPTIONAL - Phase 3D (Azure SQL)
[tool.poetry.group.sql.dependencies]
pyodbc = { version = "^5.0.0", optional = true }
sqlalchemy = { version = "^2.0.0", optional = true }

# Keep existing extras
[tool.poetry.extras]
spark = ["pyspark", "delta-spark"]
azure = ["azure-identity", "azure-keyvault-secrets", "azure-storage-file-datalake", "deltalake", "pyarrow"]
sql = ["pyodbc", "sqlalchemy"]
all = ["pyspark", "delta-spark", "azure-identity", "azure-keyvault-secrets", "azure-storage-file-datalake", "deltalake", "pyarrow", "pyodbc", "sqlalchemy"]
```

**Implementation:**
```bash
# Update pyproject.toml (manual edit)

# Install new dependencies
poetry add markdown2 Jinja2

# Install SQL dependencies as optional
poetry add --optional pyodbc sqlalchemy
poetry add -G sql pyodbc sqlalchemy

# Update lock file
poetry lock --no-update

# Install all dependencies
poetry install --all-extras

# Verify
poetry show | grep markdown2
poetry show | grep jinja2
poetry show | grep pyodbc
```

**Testing:**
```bash
# Test new imports
python -c "import markdown2; print('markdown2 OK')"
python -c "import jinja2; print('Jinja2 OK')"
python -c "import sqlalchemy; print('SQLAlchemy OK')" || echo "Optional - OK if not installed"

# Test that existing code isn't affected
pytest -v

# Test in fresh environment
poetry env remove python
poetry install
pytest -v
```

**Update CI/CD:**
```yaml
# .github/workflows/ci.yml (add SQL job)

jobs:
  test-base:
    # ... existing base job ...
  
  test-extras:
    # ... existing extras job ...
  
  test-sql:  # NEW
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.9", "3.12"]
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: ${{ matrix.python-version }}
      - name: Install dependencies
        run: |
          pip install poetry
          poetry install --extras sql
      - name: Run SQL import tests
        run: |
          poetry run python -c "from odibi.connections.azure_sql import AzureSQLConnection"
```

**Deliverables:**
- `pyproject.toml` updated with Phase 3 dependencies
- `poetry.lock` updated
- CI/CD updated to test SQL extras
- All tests passing

**Acceptance Criteria:**
- ✅ `poetry install` works
- ✅ `poetry install --all-extras` works
- ✅ All tests pass (137/137)
- ✅ CI/CD green on all jobs

---

## Week 2: Testing & Documentation

### Day 6-7: Comprehensive Testing

**Tasks:**
1. Add tests for new CLI structure
2. Add tests for empty module imports
3. Verify test coverage hasn't decreased
4. Add integration tests for reorganized code

**New Tests:**
```python
# tests/unit/test_cli.py
"""Tests for CLI module."""
import pytest
from odibi.cli import main
from odibi.cli.run import run_command
from odibi.cli.validate import validate_command

def test_cli_import():
    """CLI module should be importable."""
    assert main is not None
    assert run_command is not None
    assert validate_command is not None

def test_cli_main_no_args(capsys):
    """CLI should show help when no args provided."""
    with pytest.raises(SystemExit):
        main()
    captured = capsys.readouterr()
    assert "usage:" in captured.out.lower() or "usage:" in captured.err.lower()

# More CLI tests...
```

```python
# tests/unit/test_module_structure.py
"""Tests for Phase 3 scaffolding."""
import importlib

def test_operations_module_exists():
    """Operations module should be importable."""
    import odibi.operations
    assert odibi.operations.__version__ == "0.0.0"
    assert odibi.operations.__all__ == []

def test_transformations_module_exists():
    """Transformations module should be importable."""
    import odibi.transformations
    assert odibi.transformations.__version__ == "0.0.0"

def test_story_module_exists():
    """Story module should be importable."""
    import odibi.story
    assert odibi.story.__version__ == "0.0.0"

def test_validation_module_exists():
    """Validation module should be importable."""
    import odibi.validation
    assert odibi.validation.__version__ == "0.0.0"

def test_testing_module_exists():
    """Testing module should be importable."""
    import odibi.testing
    assert odibi.testing.__version__ == "0.0.0"
```

```python
# tests/integration/test_cli_integration.py
"""Integration tests for CLI."""
import subprocess
import tempfile
import os

def test_cli_run_example_local():
    """CLI should run example_local.yaml successfully."""
    result = subprocess.run(
        ['python', '-m', 'odibi', 'run', 'examples/example_local.yaml'],
        capture_output=True,
        text=True
    )
    assert result.returncode == 0
    assert "completed successfully" in result.stdout.lower()

def test_cli_validate_example():
    """CLI should validate example configs."""
    result = subprocess.run(
        ['python', '-m', 'odibi', 'validate', 'examples/example_local.yaml'],
        capture_output=True,
        text=True
    )
    assert result.returncode == 0
    assert "valid" in result.stdout.lower()
```

**Testing:**
```bash
# Run new tests
pytest tests/unit/test_cli.py -v
pytest tests/unit/test_module_structure.py -v
pytest tests/integration/test_cli_integration.py -v

# Run all tests
pytest -v

# Check coverage
pytest --cov=odibi --cov-report=html
# Should maintain or improve 78% coverage
```

**Deliverables:**
- 15+ new tests for reorganized code
- Test coverage maintained or improved
- All 152+ tests passing (137 original + new tests)

**Acceptance Criteria:**
- ✅ CLI tests pass
- ✅ Module structure tests pass
- ✅ Integration tests pass
- ✅ Coverage ≥ 78%

---

### Day 8-9: Documentation Updates

**Tasks:**
1. Update PROJECT_STRUCTURE.md
2. Update README.md with new CLI commands
3. Update CONTRIBUTING.md for new structure
4. Create PHASE3_ROADMAP.md

**Documentation:**

**PROJECT_STRUCTURE.md:**
```markdown
# Odibi Project Structure

Updated: 2025-11-10 (Phase 2.5)

## Directory Layout

```
odibi/
├── __init__.py              # Main package exports
├── __main__.py              # CLI entry point
├── config.py                # Configuration loading
├── pipeline.py              # Pipeline orchestration
├── project_config.py        # ProjectConfig model
│
├── cli/                     # NEW (Phase 2.5)
│   ├── __init__.py
│   ├── main.py              # CLI entry point
│   ├── run.py               # odibi run command
│   └── validate.py          # odibi validate command
│
├── engine/                  # Execution engines
│   ├── __init__.py
│   ├── base.py              # BaseEngine interface
│   ├── pandas_engine.py     # Pandas implementation
│   └── spark_engine.py      # Spark implementation
│
├── connections/             # Data connectors
│   ├── __init__.py
│   ├── base.py              # BaseConnection interface
│   ├── azure_adls.py        # Azure Data Lake Storage
│   ├── azure_sql.py         # Azure SQL (stub - Phase 3D)
│   └── local_dbfs.py        # Local DBFS mock
│
├── operations/              # NEW (Phase 2.5) - Scaffolding for Phase 3
│   └── __init__.py          # Built-in operations (pivot, join, etc.)
│
├── transformations/         # NEW (Phase 2.5) - Scaffolding for Phase 3
│   └── __init__.py          # Transformation registry
│
├── story/                   # NEW (Phase 2.5) - Scaffolding for Phase 3
│   └── __init__.py          # Story generation system
│
├── validation/              # NEW (Phase 2.5) - Scaffolding for Phase 3
│   └── __init__.py          # Quality enforcement
│
├── testing/                 # NEW (Phase 2.5) - Scaffolding for Phase 3
│   └── __init__.py          # Testing utilities
│
├── utils/                   # Utilities
│   ├── __init__.py
│   └── setup_helpers.py     # Setup utilities
│
└── tests/                   # Test suite
    ├── unit/
    ├── integration/
    └── fixtures/
```

## Module Responsibilities

### Core Modules (Phase 1-2)

**cli/**
- Command-line interface
- User-facing commands (run, validate)
- Argument parsing and dispatch

**engine/**
- Data processing engines
- Pandas and Spark implementations
- Read, write, execute operations

**connections/**
- Data source/sink connectors
- Azure, local, and cloud connections
- Path resolution and authentication

### Phase 3 Modules (Scaffolding Only)

**operations/**
- Built-in transformations (pivot, unpivot, join)
- Each operation includes execute() and explain() methods
- Self-documenting operations

**transformations/**
- User transformation registry
- @transformation decorator
- Context passing infrastructure

**story/**
- Run story generation (auto-capture)
- Doc story generation (explanations)
- Story diffing and comparison

**validation/**
- Explanation quality linting
- Config validation
- Pre-run checks

**testing/**
- Test fixtures and helpers
- DataFrame assertions
- Mock objects

## Import Patterns

**User imports (stable):**
```python
from odibi import Pipeline
from odibi.engine import PandasEngine, SparkEngine
from odibi.connections import AzureADLS
```

**Phase 3 imports (coming soon):**
```python
from odibi import transformation
from odibi.operations import pivot, unpivot
from odibi.story import generate_story
from odibi.validation import validate
```

## Development Guidelines

1. **Module independence**: Each module should have minimal dependencies
2. **Clear interfaces**: Use abstract base classes for extensibility
3. **Test coverage**: Maintain ≥78% coverage
4. **Documentation**: Every module needs comprehensive docstrings
```

**README.md updates:**
```markdown
# Odibi

[... existing content ...]

## Installation

```bash
# Core framework (Pandas only)
pip install odibi

# With Spark support
pip install "odibi[spark]"

# With Azure support
pip install "odibi[azure]"

# With Azure SQL support (Phase 3)
pip install "odibi[sql]"

# Everything
pip install "odibi[all]"
```

## Command-Line Interface

```bash
# Run a pipeline
odibi run config.yaml

# Validate a config
odibi validate config.yaml

# Get help
odibi --help
```

[... rest of README ...]
```

**PHASE3_ROADMAP.md:**
```markdown
# Phase 3 Roadmap

**Target:** Q1 2026  
**Status:** Planning (Phase 2.5 in progress)

## Overview

Phase 3 completes Odibi's transparency and documentation capabilities with:
- Automatic story generation (run + documentation)
- Transformation registry with enforced documentation
- Quality linting and validation
- Azure SQL connector
- Enhanced CLI tools

## Sub-Phases

### Phase 3A: Foundation (4 weeks)
- Transformation registry
- Explanation system
- Quality enforcement
- Built-in operations

### Phase 3B: Stories (3 weeks)
- Run story engine
- Doc story generator
- Theme system

### Phase 3C: CLI + Diffing (2 weeks)
- CLI commands
- Story diffing
- Batch operations

### Phase 3D: Azure SQL (1 week)
- Azure SQL connector
- Read/write operations
- Examples

### Phase 3E: Documentation (1 week)
- User guides
- API documentation
- Best practices

**Total: 11 weeks**

See PHASE_3A_PLAN.md for detailed implementation plan.
```

**Deliverables:**
- PROJECT_STRUCTURE.md updated
- README.md updated
- PHASE3_ROADMAP.md created
- All docs clear and accurate

**Acceptance Criteria:**
- ✅ Structure documented
- ✅ Future plans clear
- ✅ No outdated information

---

### Day 10: Final Validation & Release

**Tasks:**
1. Run full test suite on all Python versions
2. Verify CI/CD passes
3. Create git tag
4. Update CHANGELOG
5. Create GitHub release

**Final Testing:**
```bash
# Local test all Python versions (if using pyenv)
for version in 3.9 3.10 3.11 3.12; do
  pyenv shell $version
  poetry env use python
  poetry install
  pytest -v
done

# Or rely on CI/CD for multi-version testing

# Final test suite
poetry install --all-extras
pytest -v --cov=odibi --cov-report=html
pytest -v --cov=odibi --cov-report=term

# Manual integration tests
python -m odibi run examples/example_local.yaml
python -m odibi validate examples/example_local.yaml
```

**CHANGELOG.md update:**
```markdown
# Changelog

All notable changes to this project will be documented in this file.

## [1.2.0-alpha.4-phase2.5] - 2025-11-10

### Added
- Dedicated CLI module (`odibi/cli/`) for better organization
- Scaffolding for Phase 3 modules (operations, transformations, story, validation, testing)
- markdown2 and Jinja2 dependencies for upcoming story generation
- pyodbc and SQLAlchemy as optional dependencies for Azure SQL support
- Comprehensive documentation in new module `__init__.py` files
- 15+ new tests for reorganized code structure

### Changed
- Reorganized CLI code into dedicated `cli/` module (no breaking changes)
- Updated PROJECT_STRUCTURE.md to reflect new organization
- Enhanced README with new CLI commands
- CI/CD updated to test optional SQL dependencies

### Technical
- Test count increased from 137 to 152+
- Test coverage maintained at 78%
- All Python versions (3.9-3.12) validated in CI/CD
- No breaking changes to public API
- Fully backward compatible with Phase 2C

### Documentation
- Created PHASE3_ROADMAP.md
- Updated PROJECT_STRUCTURE.md
- Enhanced module docstrings
- Clear separation between implemented and planned features

---

## [1.2.0-alpha.3-phase2c] - 2025-11-09
[... previous changelog entries ...]
```

**Git workflow:**
```bash
# Commit all changes
git add .
git commit -m "Phase 2.5: Reorganization and Phase 3 scaffolding"

# Tag release
git tag -a v1.2.0-alpha.4-phase2.5 -m "Phase 2.5: Foundation for Phase 3"

# Push
git push origin main --tags
```

**GitHub Release:**
```markdown
# Phase 2.5: Reorganization & Foundation

Preparation release for Phase 3 development.

## What's New

✅ **CLI Module**: Dedicated `odibi/cli/` module for better code organization
✅ **Phase 3 Scaffolding**: Empty modules ready for Phase 3 implementation
✅ **Dependencies Added**: markdown2, Jinja2, and optional SQL libraries
✅ **Comprehensive Documentation**: Every module documents future plans
✅ **Enhanced Testing**: 15+ new tests, all 152 tests passing

## Breaking Changes

None - fully backward compatible with Phase 2C.

## Migration

No migration needed. Existing code continues to work.

## What's Next

Phase 3A begins with transformation registry and explanation system.
See PHASE3_ROADMAP.md for details.

## Testing

- ✅ 152/152 tests passing
- ✅ 78% test coverage maintained
- ✅ All Python versions (3.9-3.12) validated
- ✅ CI/CD green across all jobs
```

**Deliverables:**
- Git tag: `v1.2.0-alpha.4-phase2.5`
- GitHub release published
- CHANGELOG.md updated
- All tests passing

**Acceptance Criteria:**
- ✅ All 152+ tests pass
- ✅ CI/CD green
- ✅ Git tag created
- ✅ Release published
- ✅ Documentation complete

---

## Phase 2.5 Acceptance Criteria

**Before moving to Phase 3A:**

### Code Quality
- [ ] All 152+ tests passing
- [ ] Test coverage ≥ 78%
- [ ] No linter errors (`ruff check odibi/`)
- [ ] No type errors (if using mypy)
- [ ] CI/CD green on all Python versions (3.9-3.12)

### Structure
- [ ] CLI module created and tested
- [ ] Phase 3 scaffolding in place (5 empty modules)
- [ ] All modules have documented `__init__.py`
- [ ] No files in wrong locations

### Dependencies
- [ ] markdown2 installed and importable
- [ ] Jinja2 installed and importable
- [ ] Optional SQL dependencies work
- [ ] `poetry install --all-extras` succeeds
- [ ] No dependency conflicts

### Documentation
- [ ] PROJECT_STRUCTURE.md updated
- [ ] README.md updated
- [ ] PHASE3_ROADMAP.md created
- [ ] CHANGELOG.md updated
- [ ] All docstrings complete

### Backward Compatibility
- [ ] Existing imports still work
- [ ] `from odibi import Pipeline` works
- [ ] Example configs still run
- [ ] No breaking changes

### Release
- [ ] Git tag created
- [ ] GitHub release published
- [ ] Version bumped to v1.2.0-alpha.4-phase2.5
- [ ] All commits pushed

---

## Risks & Mitigations

**Risk: Reorganization breaks imports**
- Mitigation: Test after each change, maintain backward compatibility

**Risk: New dependencies cause conflicts**
- Mitigation: Test in fresh environment, lock file updated

**Risk: Tests become flaky**
- Mitigation: Run tests multiple times, fix before proceeding

**Risk: Documentation becomes outdated**
- Mitigation: Update docs as part of each change, not at the end

---

## Next Steps

After Phase 2.5 completes:
1. Review and merge Phase 2.5 branch
2. Begin Phase 3A: Foundation
3. See PHASE_3A_PLAN.md for detailed implementation

---

**Last Updated:** 2025-11-10  
**Maintainer:** Henry Odibi
