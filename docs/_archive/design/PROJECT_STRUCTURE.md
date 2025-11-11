# Odibi Project Structure

**Updated:** 2025-11-10 (Phase 2.5)  
**Version:** v1.2.0-alpha.3-phase2c → v1.2.0-alpha.4-phase2.5

---

## Directory Layout

```
odibi/
├── __init__.py              # Main package exports
├── __main__.py              # CLI entry point (NEW - Phase 2.5)
├── config.py                # Configuration loading (ProjectConfig, PipelineConfig, etc.)
├── pipeline.py              # Pipeline orchestration (Pipeline, PipelineManager)
├── context.py               # Context for DataFrame management
├── registry.py              # Transform function registry
├── graph.py                 # Dependency graph
├── node.py                  # Node execution
├── story.py                 # Story generation
├── exceptions.py            # Custom exceptions
├── py.typed                 # Type hints marker
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
│   ├── local.py             # Local filesystem
│   ├── local_dbfs.py        # Local DBFS mock
│   ├── azure_adls.py        # Azure Data Lake Storage
│   └── azure_sql.py         # Azure SQL (stub - Phase 3D)
│
├── utils/                   # Utilities
│   ├── __init__.py
│   └── setup_helpers.py     # Setup utilities (parallel Key Vault, etc.)
│
├── operations/              # NEW (Phase 2.5) - Scaffolding for Phase 3
│   └── __init__.py          # Built-in operations (pivot, join, etc.)
│
├── transformations/         # NEW (Phase 2.5) - Scaffolding for Phase 3
│   └── __init__.py          # Transformation registry
│
├── validation/              # NEW (Phase 2.5) - Scaffolding for Phase 3
│   └── __init__.py          # Quality enforcement
│
└── testing/                 # NEW (Phase 2.5) - Scaffolding for Phase 3
    └── __init__.py          # Testing utilities


tests/                       # Test suite (125 passing + 12 skipped)
├── test_config.py           # Configuration tests
├── test_context.py          # Context tests
├── test_registry.py         # Registry tests
├── test_graph.py            # Dependency graph tests
├── test_pipeline.py         # Pipeline tests
├── test_azure_adls_auth.py  # Azure ADLS authentication tests
├── test_connections_paths.py # Connection path resolution
├── test_delta_pandas.py     # Delta Lake tests (skipped - optional dep)
├── test_extras_imports.py   # Import guard tests
└── test_setup_helpers.py    # Setup utilities tests


examples/                    # Example configurations
├── example_local.yaml       # Local Pandas pipeline
├── example_spark.yaml       # Spark pipeline template
├── example_delta_pipeline.yaml  # Delta Lake example
├── template_full.yaml       # Full template
├── template_full_adls.yaml  # Azure ADLS template
├── getting_started/         # Getting started examples
└── README.md


setup/                       # Setup notebooks
└── databricks_setup.ipynb   # Databricks setup guide


walkthroughs/                # Learning walkthroughs
├── 00_setup_environment.ipynb
├── 01_local_pipeline_pandas.ipynb
├── 02_cli_and_testing.ipynb
├── 03_spark_preview_stub.ipynb
├── 04_ci_cd_and_precommit.ipynb
└── 05_build_new_pipeline.ipynb


docs/                        # Documentation
├── setup_databricks.md
└── setup_azure.md


.github/workflows/           # CI/CD
└── ci.yml                   # GitHub Actions
```

---

## Module Responsibilities

### Core Modules (Phase 1-2)

**cli/**
- Command-line interface (Phase 2.5)
- User-facing commands (`run`, `validate`)
- Argument parsing and dispatch

**config.py**
- Pydantic models for YAML configuration
- `ProjectConfig`, `PipelineConfig`, `NodeConfig`
- `ReadConfig`, `WriteConfig`, `TransformConfig`
- Validation and type safety

**pipeline.py**
- `Pipeline`: Single pipeline executor
- `PipelineManager`: Multi-pipeline orchestrator from YAML
- Story generation integration
- Execution orchestration

**engine/**
- Data processing engines
- `PandasEngine`: Pandas implementation (read, write, execute SQL)
- `SparkEngine`: Spark implementation
- Delta Lake support (VACUUM, history, restore)

**connections/**
- Data source/sink connectors
- `LocalConnection`: Local filesystem
- `AzureADLS`: Azure Data Lake Storage with Key Vault auth
- `AzureSQLConnection`: Azure SQL (stub)
- Path resolution and authentication

**context.py**
- DataFrame context management
- Register, retrieve, and cache DataFrames
- Isolation between pipelines

**registry.py**
- `@transform` decorator for user functions
- Function registry and parameter validation
- Function metadata and introspection

**graph.py**
- Dependency graph construction
- Topological sorting for execution order
- Cycle detection
- Execution layer computation

**node.py**
- Node execution logic
- Read, transform, write operations
- Error handling and reporting

**story.py**
- Story generation for pipeline runs
- Execution documentation
- Sample data capture

**utils/**
- `setup_helpers.py`: Parallel Key Vault fetching, Databricks validation

### Phase 3 Modules (Scaffolding Only - Phase 2.5)

**operations/**
- Built-in transformations (pivot, unpivot, join, sql, aggregate)
- Each operation includes `execute()` and `explain()` methods
- Self-documenting operations
- **Status:** Scaffolding only (v0.0.0)

**transformations/**
- User transformation registry
- `@transformation` decorator
- Context passing infrastructure
- Explanation system
- **Status:** Scaffolding only (v0.0.0)

**validation/**
- Explanation quality linting
- Config validation
- Pre-run checks
- Quality scoring
- **Status:** Scaffolding only (v0.0.0)

**testing/**
- Test fixtures and helpers
- DataFrame assertions (engine-agnostic)
- Mock objects
- Common testing patterns
- **Status:** Scaffolding only (v0.0.0)

---

## Import Patterns

### User Imports (Stable)
```python
from odibi import Pipeline, PipelineManager
from odibi import transform, Context
from odibi.engine import PandasEngine, SparkEngine
from odibi.connections import AzureADLS, LocalConnection
```

### CLI Usage
```bash
# Validate configuration
python -m odibi validate config.yaml
odibi validate config.yaml  # After install

# Run pipeline
python -m odibi run config.yaml
odibi run config.yaml  # After install

# Get help
python -m odibi --help
```

### Phase 3 Imports (Coming Soon)
```python
from odibi import transformation
from odibi.operations import pivot, unpivot, join
from odibi.story import generate_story
from odibi.validation import validate_explanations
from odibi.testing import fixtures, assertions
```

---

## Development Guidelines

1. **Module Independence**: Each module should have minimal dependencies
2. **Clear Interfaces**: Use abstract base classes for extensibility
3. **Test Coverage**: Maintain ≥68% coverage (target: 100%)
4. **Documentation**: Every module needs comprehensive docstrings
5. **Backward Compatibility**: No breaking changes without major version bump

---

## Test Coverage (Baseline: Phase 2.5 Day 1)

| Module | Statements | Miss | Cover |
|--------|------------|------|-------|
| odibi/__init__.py | 12 | 7 | 42% |
| odibi/config.py | 144 | 5 | 97% |
| odibi/connections/azure_adls.py | 69 | 5 | 93% |
| odibi/graph.py | 120 | 4 | 97% |
| odibi/registry.py | 60 | 2 | 97% |
| odibi/story.py | 106 | 5 | 95% |
| odibi/pipeline.py | 168 | 73 | 57% |
| odibi/node.py | 156 | 40 | 74% |
| odibi/context.py | 68 | 24 | 65% |
| odibi/engine/pandas_engine.py | 220 | 174 | 21% |
| odibi/engine/spark_engine.py | 95 | 62 | 35% |
| **TOTAL** | **1534** | **487** | **68%** |

**Goal:** Increase to 100% coverage by end of Phase 3

---

## Version History

### v1.2.0-alpha.4-phase2.5 (In Progress)
- ✅ CLI module created (`odibi/cli/`)
- ✅ Phase 3 scaffolding (4 empty modules)
- ✅ All tests passing (125/137)
- ✅ PROJECT_STRUCTURE.md updated

### v1.2.0-alpha.3-phase2c (Complete)
- Parallel Key Vault fetching
- Setup utilities
- Databricks validation

### v1.2.0-alpha.2-phase2b (Complete)
- Delta Lake support
- VACUUM, history, restore operations

### v1.2.0-alpha.1-phase2a (Complete)
- Azure ADLS with Key Vault auth
- Multi-account storage support

---

**For detailed roadmap, see [IMPLEMENTATION_ROADMAP.md](IMPLEMENTATION_ROADMAP.md)**
