# Odibi - Agent Guide

## About This Project

**Creator:** Solo data engineer on an analytics team (only DE among data analysts)

**The Story:** I work in operations, not IT. I know the gaps and pain points of doing data work alone while spearheading change in my company. Odibi was born from needing unique solutions due to IT roadblocks and working without support. I've proven invaluable and more will be asked of me - I need tools that buy back time and freedom.

**Goals:**
- **Short term:** Make odibi stable and bug-free
- **Medium term:** Find gaps and improve coverage
- **Ultimate goal:** A framework so easy and powerful it gives me time back to focus on what matters

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
```

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
