# Contributing to ODIBI

Thank you for your interest in contributing to ODIBI! This document provides guidelines and standards for contributions.

## 🎯 Development Philosophy

- **Explicit over implicit**: Every behavior should be clear and documented
- **Test-driven**: All features require tests before merging
- **Non-breaking**: New engines and connectors are opt-in via extras
- **Documented**: Changes include examples and documentation updates

## 🚀 Getting Started

### 1. Fork and Clone

```bash
git clone https://github.com/henryodibi11/Odibi.git
cd Odibi
```

### 2. Set Up Development Environment

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in editable mode with dev dependencies
pip install -e .[dev]

# Install pre-commit hooks
pre-commit install
```

### 3. Verify Installation

```bash
# Run tests
pytest -v

# All 78 base tests should pass
```

## 📋 How to Contribute

### Picking Up Work

1. Check [PHASES.md](PHASES.md) for current phase deliverables
2. Look for unchecked items in the current phase (Phase 1)
3. Check existing issues or create a new one
4. Comment on the issue to claim it

### Branch Naming Convention

Use descriptive branch names with phase prefix:

```bash
# For Phase 1 features
git checkout -b phase-1/spark-engine
git checkout -b phase-1/azure-adls-connection

# For bug fixes
git checkout -b fix/context-cache-issue

# For documentation
git checkout -b docs/setup-databricks
```

## 🧪 Testing Requirements

### Required Tests

All new features must include tests:

- **Unit tests**: Test individual components in isolation
- **Integration tests**: Test component interactions
- **Import tests**: For optional dependencies (Spark, Azure)

### Test Structure

```python
# tests/test_new_feature.py
import pytest

def test_basic_functionality():
    """Test the happy path."""
    # Arrange
    config = {...}

    # Act
    result = do_something(config)

    # Assert
    assert result.success is True

@pytest.mark.extras
def test_spark_feature():
    """Test Spark-specific functionality (skip if pyspark not installed)."""
    pytest.importorskip("pyspark")
    # Test code here
```

### Running Tests

```bash
# Run all tests
pytest -v

# Run specific test file
pytest tests/test_config.py -v

# Run with coverage
pytest --cov=odibi --cov-report=term-missing

# Run only extras tests
pytest -k "extras" -v

# Skip slow tests
pytest -m "not slow" -v
```

### Test Coverage

- Core modules: **90%+** coverage required
- New features: Must include tests before merging
- No decrease in overall coverage

## 🎨 Coding Standards

### Python Style

We use **Black** for formatting and **Ruff** for linting:

```bash
# Format code
black odibi/ tests/

# Lint code
ruff check odibi/ tests/

# Type check (on core modules)
mypy odibi/
```

### Code Conventions

1. **Follow existing patterns**: Look at similar code before implementing
2. **Type hints**: All public functions should have type annotations
3. **Docstrings**: Use Google-style docstrings for classes and public methods
4. **Import organization**: Standard library → third-party → local
5. **Line length**: 100 characters (Black enforced)

### Example

```python
from typing import Dict, Any
from pydantic import BaseModel


class NewFeature:
    """Brief description of the feature.

    Longer explanation if needed, including:
    - Key behaviors
    - Usage patterns
    - Limitations

    Args:
        config: Configuration dictionary with required fields
        engine: Execution engine (pandas or spark)

    Raises:
        ValueError: If config is invalid
    """

    def __init__(self, config: Dict[str, Any], engine: str = "pandas") -> None:
        self.config = config
        self.engine = engine

    def execute(self) -> Dict[str, Any]:
        """Execute the feature and return results."""
        # Implementation
        pass
```

## 📝 Documentation Standards

### Required Documentation

When adding features, update:

1. **Code comments**: Only for complex logic, not obvious operations
2. **Docstrings**: All public classes and methods
3. **README.md**: If adding user-facing features
4. **PHASES.md**: Check off completed deliverables
5. **CHANGELOG.md**: Add entry under "Unreleased"

### Documentation Files

- **Setup guides**: For new connections (e.g., `docs/setup_azure.md`)
- **Examples**: Working YAML configs in `examples/`
- **API docs**: Inline docstrings (auto-generated later)

## 🔧 Phase-Specific Guidelines

### Phase 1: Spark & Azure Scaffolding (Current)

**Goal**: Create structure without breaking existing Pandas functionality

**Requirements**:
- ✅ Import guards for optional dependencies
- ✅ Stub methods with `NotImplementedError` and PHASES.md references
- ✅ No changes to existing Pandas code
- ✅ All 78 existing tests must pass
- ✅ New modules importable without extras installed

**Example - Spark Engine**:

```python
from odibi.engine.base import Engine

class SparkEngine(Engine):
    """Spark execution engine (Phase 1: Scaffolding).

    Status: Experimental - Basic introspection only.
    Full implementation planned for Phase 3.
    """

    def __init__(self, session=None, config=None):
        try:
            from pyspark.sql import SparkSession
        except ImportError as e:
            raise ImportError(
                "Spark support requires 'pip install odibi[spark]'. "
                "See docs/setup_databricks.md for setup instructions."
            ) from e

        from pyspark.sql import SparkSession
        self.spark = session or SparkSession.builder.getOrCreate()
        self.config = config

    def get_schema(self, df):
        """Get DataFrame schema."""
        return [(f.name, f.dataType.simpleString()) for f in df.schema]

    def read(self, *args, **kwargs):
        raise NotImplementedError(
            "SparkEngine.read() will be implemented in Phase 3. "
            "See PHASES.md line 171 for details."
        )
```

## 🔍 Pull Request Process

### Before Submitting

- [ ] Tests pass locally (`pytest -v`)
- [ ] Code formatted (`black odibi/ tests/`)
- [ ] Linting passes (`ruff check odibi/ tests/`)
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
- [ ] No decrease in test coverage

### PR Template

When you open a PR, include:

1. **What**: Brief description of changes
2. **Why**: Problem being solved or feature being added
3. **How**: Implementation approach
4. **Testing**: What tests were added
5. **Phase**: Which phase deliverable this addresses

### Review Process

1. Automated checks run (CI tests, linting)
2. Maintainer review (usually within 2-3 days)
3. Address feedback
4. Approval and merge

## 🐛 Reporting Bugs

### Bug Report Template

Use the GitHub issue template and include:

1. **ODIBI version**: `pip show odibi`
2. **Python version**: `python --version`
3. **OS**: Windows/Linux/macOS
4. **Installed extras**: `pip show pyspark azure-storage-blob`
5. **Minimal reproduction**: Smallest config that reproduces the bug
6. **Expected vs. Actual**: What should happen vs. what does happen
7. **Error messages**: Full traceback if applicable

## 💡 Feature Requests

### Feature Request Template

1. **Problem**: What problem does this solve?
2. **Proposed solution**: How should it work?
3. **Alternatives**: Other approaches considered
4. **Phase alignment**: Which phase does this fit into?

## 🏗️ Architecture Patterns

### Engine Pattern

All engines implement the `Engine` interface:

- `read()`, `write()`, `execute_sql()`, `execute_transform()`
- `get_schema()`, `get_shape()`, `count_rows()`

### Connection Pattern

All connections implement `BaseConnection`:

- Configuration validation in `__init__`
- Path/URI resolution methods
- No network I/O in validation phase (Phase 1)

### Context Pattern

The `Context` object passes data between nodes:

- `register(name, data)`: Store node output
- `get(name)`: Retrieve node output
- Engine-agnostic interface

## 📊 Project Structure

```
odibi/
├── engine/          # Execution engines (pandas, spark)
├── connections/     # Data connections (local, azure, s3)
├── config.py        # Pydantic configuration models
├── context.py       # Context API for data passing
├── node.py          # Node execution logic
├── graph.py         # Dependency graph analysis
├── pipeline.py      # Pipeline orchestration
└── registry.py      # Transform function registry
```

## 🤝 Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you agree to uphold this code.

## 📜 License

By contributing, you agree that your contributions will be licensed under the MIT License.

## ❓ Questions?

- Check [PHASES.md](PHASES.md) for roadmap and current work
- Review [docs/](docs/) for technical documentation
- Open an issue for clarification
- Reach out to maintainers: @henryodibi11

## 🙏 Thank You!

Every contribution helps make ODIBI better. Whether it's code, documentation, bug reports, or feedback, we appreciate your involvement in the project!

---

**Current Focus**: Phase 1 - Spark & Azure Scaffolding  
**Next Up**: Phase 2 - CLI Tools & Testing Utilities
