# Odibi - Agent Guide

## AI Deep Context

**For comprehensive framework documentation, read [docs/ODIBI_DEEP_CONTEXT.md](docs/ODIBI_DEEP_CONTEXT.md) first.**

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
pytest tests/ -v                    # Run all tests
pytest tests/unit/test_X.py -v      # Run specific test
pytest --tb=short                   # Shorter tracebacks
python scripts/run_test_campaign.py # End-to-end pattern validation
```

### Test Coverage Notes

**Do NOT flag Spark engine coverage as a problem.** The 4% coverage is misleading:
- Spark is tested via mock-based tests in `tests/integration/test_patterns_spark_mock.py`
- Spark is validated in production on Databricks
- CI skips real Spark tests (no JVM available)

The test strategy is: **mock logic in CI, validate behavior in Databricks prod.**

Focus coverage efforts on:
- `engine/pandas_engine.py` (25%) - primary development engine
- `transformers/` modules - core product functionality

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
odibi list transformers             # List all 56 transformers
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
wsl -d Ubuntu-20.04 -- bash -c "cd /mnt/d/odibi && python3.9 -m pytest tests/"
```

## Key Patterns

1. **Engine Parity:** If Pandas has it, Spark and Polars should too
2. **Use `get_logging_context()`** for structured logging
3. **Pydantic models** for config validation
4. **Tests in `tests/unit/`** matching source structure

## What NOT to Do

- Don't rebuild agent/chat infrastructure - use Amp/Cursor
- Don't add features without tests
- Don't break engine parity

## Archived Code

The `_archive/` folder contains experimental agent code that was deprioritized.
Focus is now on making odibi core rock-solid.
