# Skill 04 — Write a Pattern

> **Layer:** Building
> **When:** Adding a new warehouse pattern to odibi.
> **Prerequisite:** Complete Skill 01 and Skill 02. Confirm no existing pattern covers your need.

---

## Overview

Odibi patterns are classes that extend `Pattern` ABC. They implement common data warehouse loading strategies (dimensions, facts, SCD2, etc.). Patterns receive an engine and config, validate parameters, execute logic, and return a DataFrame.

**Key files:**
- `odibi/patterns/base.py` — `Pattern` ABC (the base class)
- `odibi/patterns/dimension.py` — canonical exemplar
- `odibi/patterns/__init__.py` — pattern registry (`_PATTERNS` dict)
- `odibi/config.py` — `NodeConfig` Pydantic model

---

## Step-by-Step

### Step 1: Define the Pattern Class

```python
from typing import Any, ClassVar, List

from odibi.context import EngineContext
from odibi.enums import EngineType
from odibi.patterns.base import Pattern
from odibi.utils.logging_context import get_logging_context


class MyPattern(Pattern):
    """
    My Pattern: One-line description.

    Features:
    - Feature 1
    - Feature 2

    Configuration Options (via params dict):
        - **key_param** (str): Description (required)
        - **optional_param** (int): Description (default: 10)
    """

    required_params: ClassVar[List[str]] = ["key_param"]
    optional_params: ClassVar[List[str]] = ["optional_param"]

    def validate(self) -> None:
        """Validate pattern configuration."""
        ctx = get_logging_context()

        if not self.params.get("key_param"):
            ctx.error(
                "MyPattern validation failed: 'key_param' is required",
                pattern="MyPattern",
                node=self.config.name,
            )
            raise ValueError(
                f"MyPattern (node '{self.config.name}'): 'key_param' is required. "
                "Provide it in the node's params section."
            )

        super().validate()

    def execute(self, context: EngineContext) -> Any:
        """Execute the pattern logic."""
        self.validate()
        start_time = self._log_execution_start(
            key_param=self.params.get("key_param"),
        )

        try:
            if context.engine_type == EngineType.SPARK:
                result = self._execute_spark(context)
            else:
                result = self._execute_pandas(context)

            row_count = self._get_row_count(result, context.engine_type)
            self._log_execution_complete(start_time, rows=row_count)
            return result

        except Exception as e:
            self._log_error(e)
            raise

    def _execute_pandas(self, context: EngineContext) -> Any:
        """Pandas/DuckDB implementation."""
        df = context.df
        key_param = self.params["key_param"]
        # ... your logic here ...
        return df

    def _execute_spark(self, context: EngineContext) -> Any:
        """Spark implementation."""
        df = context.df
        key_param = self.params["key_param"]
        # ... your logic here ...
        return df
```

**Conventions:**
- Extend `Pattern` from `odibi.patterns.base`
- Define `required_params` and `optional_params` as ClassVars
- `validate()` checks params and raises `ValueError` with actionable messages
- `execute()` dispatches to engine-specific methods
- Use `self._log_execution_start()`, `self._log_execution_complete()`, `self._log_error()`
- Use `self._get_row_count()` for engine-agnostic row counting
- Use `self._load_existing_target()` to load existing target data
- Use `self._add_audit_columns()` for load_timestamp/source_system
- **Return the DataFrame — never write it directly**

### Step 2: Register in `__init__.py`

```python
# odibi/patterns/__init__.py
from odibi.patterns.my_pattern import MyPattern

_PATTERNS = {
    # ... existing patterns ...
    "my_pattern": MyPattern,
}
```

### Step 3: Write Tests

Create `tests/unit/patterns/test_my_pattern_coverage.py`:

```python
import pandas as pd
import pytest
from unittest.mock import MagicMock

from odibi.engine.pandas_engine import PandasEngine
from odibi.context import PandasContext


@pytest.fixture
def engine():
    return PandasEngine(config={})


@pytest.fixture
def make_config():
    """Create a mock config. Use MagicMock to bypass Pydantic validation."""
    def _make(name="test_node", params=None):
        cfg = MagicMock()
        cfg.name = name
        cfg.params = params or {}
        return cfg
    return _make


def test_validate_missing_required(engine, make_config):
    from odibi.patterns.my_pattern import MyPattern
    cfg = make_config(params={})
    pattern = MyPattern(engine=engine, config=cfg)
    with pytest.raises(ValueError, match="key_param"):
        pattern.validate()


def test_execute_pandas(engine, make_config):
    from odibi.patterns.my_pattern import MyPattern
    cfg = make_config(params={"key_param": "test_value"})
    pattern = MyPattern(engine=engine, config=cfg)

    df = pd.DataFrame({"col1": [1, 2, 3]})
    context = PandasContext(df=df)
    result = pattern.execute(context)

    assert result is not None
    assert len(result) > 0
```

**Testing rules:**
- Use `MagicMock` for `NodeConfig` to bypass Pydantic validation in tests
- Test `validate()` for missing/invalid params
- Test `execute()` with Pandas DataFrames
- Test audit columns if your pattern adds them
- Do NOT use caplog — assert on return values
- File name: `test_<pattern>_coverage.py` (no "spark" or "delta")

### Step 4: Verify

```bash
pytest tests/unit/patterns/test_my_pattern_coverage.py -v

# Verify registration
python -c "from odibi.patterns import _PATTERNS; print('my_pattern' in _PATTERNS)"
```

---

## Inherited Methods from Pattern ABC

These are available via `self.*` — don't reimplement them:

| Method | Purpose |
|--------|---------|
| `_log_execution_start(**kwargs)` | Log start, returns start_time |
| `_log_execution_complete(start_time, **kwargs)` | Log completion with elapsed time |
| `_log_error(exception, **kwargs)` | Log error context |
| `_get_row_count(df, engine_type)` | Engine-agnostic row count |
| `_load_existing_target(context, target)` | Load existing table (multi-format) |
| `_load_existing_pandas(context, target)` | Pandas target loading |
| `_load_existing_spark(context, target)` | Spark target loading |
| `_add_audit_columns(context, df, audit_config)` | Add load_timestamp/source_system |

---

## Engine Parity Checklist

- [ ] `_execute_pandas()` works with Pandas DataFrames
- [ ] `_execute_spark()` works with Spark DataFrames (test via mock)
- [ ] Both paths produce identical output schemas
- [ ] Audit columns added consistently across engines
- [ ] Validation errors are identical regardless of engine

---

## Exemplar: DimensionPattern Structure

The `dimension.py` pattern demonstrates the full lifecycle:

1. **validate()** — checks `natural_key`, `surrogate_key`, `scd_type`, `target`, `track_cols`
2. **execute()** — dispatches to `_execute_pandas()` or `_execute_spark()`
3. **_generate_surrogate_keys()** — MAX(existing) + ROW_NUMBER for new rows
4. **_ensure_unknown_member()** — inserts SK=0 row for orphan FK handling
5. **_add_audit_columns()** — load_timestamp, source_system (uses base class method)
6. **SCD dispatch** — SCD0 (static), SCD1 (overwrite), SCD2 (delegates to `scd2` transformer)

---

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| Writing directly to storage in `execute()` | Return the DataFrame — caller writes |
| Using `NodeConfig()` in tests | Use `MagicMock()` to bypass Pydantic |
| Missing `validate()` override | Users get no error messages for bad config |
| Not logging with `_log_*` methods | Use inherited methods, not raw logging |
| Not handling both engines | Add `_execute_spark()` even if placeholder |
| Forgetting to register in `__init__.py` | Pattern won't be available to pipeline runner |
