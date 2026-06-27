# Skill 14 — Code Standards

> **Layer:** Standards
> **When:** Writing or modifying ANY odibi source code. Load this BEFORE writing production code.
> **Goal:** Agent produces code that passes linting, follows conventions, and needs zero edits.

---

## Purpose

Odibi has strict conventions. When an agent follows them precisely, PRs are merge-ready on first submission. This skill encodes every convention as a copy-paste recipe.

---

## Function Signature Standards

### Transformer Functions
```python
def my_transform(context: EngineContext, params: MyTransformParams) -> EngineContext:
    """One-line description in imperative mood.

    Design:
    - SQL-First: Pushes logic to the engine's optimizer.
    - Zero-Copy: No data movement to Python.
    """
    ctx = get_logging_context()
    start_time = time.time()

    # ... implementation ...

    elapsed_ms = (time.time() - start_time) * 1000
    ctx.debug("MyTransform completed", elapsed_ms=round(elapsed_ms, 2))
    return result
```

**Signature is always:** `(context: EngineContext, params: ParamsModel) -> EngineContext`

### Pattern Methods
```python
class MyPattern(Pattern):
    required_params: ClassVar[List[str]] = ["key_param"]
    optional_params: ClassVar[List[str]] = ["optional_param"]

    def validate(self) -> None:
        ...
        super().validate()

    def execute(self, context: EngineContext) -> Any:
        self.validate()
        start_time = self._log_execution_start(...)
        ...
        self._log_execution_complete(start_time, rows=row_count)
        return result
```

### Standalone Functions
```python
def calculate_thing(df: pd.DataFrame, column: str, threshold: float = 0.5) -> pd.DataFrame:
    """Calculate thing for the given column.

    Args:
        df: Input DataFrame.
        column: Column to process.
        threshold: Minimum value to include. Defaults to 0.5.

    Returns:
        DataFrame with calculated column added.

    Raises:
        ValueError: If column not found in DataFrame.
    """
```

---

## Import Order

Always this exact order with blank lines between groups:

```python
# 1. Standard library
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, ClassVar, Dict, List, Optional

# 2. Third-party
import pandas as pd
from pydantic import BaseModel, Field

# 3. odibi
from odibi.context import EngineContext
from odibi.enums import EngineType
from odibi.utils.logging_context import get_logging_context
```

**Within each group:** sort alphabetically by module name.

---

## Logging

### ✅ Correct (production code)
```python
from odibi.utils.logging_context import get_logging_context

ctx = get_logging_context()
ctx.debug("Operation starting", column=col, rows=len(df))
ctx.info("Operation completed", elapsed_ms=round(elapsed, 2))
ctx.warning("Unexpected condition", detail="...")
ctx.error("Operation failed", error=str(e))
```

### ❌ Never Do
```python
print("debugging")                           # Never print
import logging; logger = logging.getLogger()  # Never bare logging
logger.info("starting")                       # Not in transforms/patterns
```

**Exception:** `validation/quarantine.py` and `validation/gate.py` use `logging.getLogger(__name__)` — legacy convention, acceptable in validation module only.

---

## Engine Dispatch Pattern

When code must branch by engine, use this exact pattern:

### Standard Dispatch (in a transformer)
```python
def my_transform(context: EngineContext, params: MyParams) -> EngineContext:
    if context.engine_type == EngineType.PANDAS:
        return _my_transform_pandas(context, params)
    elif context.engine_type == EngineType.SPARK:
        return _my_transform_spark(context, params)
    elif context.engine_type == EngineType.POLARS:
        return _my_transform_polars(context, params)
    else:
        raise ValueError(f"Unsupported engine type: {context.engine_type}")
```

### Standard Dispatch (in a pattern)
```python
def execute(self, context: EngineContext) -> Any:
    self.validate()
    start_time = self._log_execution_start()

    if context.engine_type == EngineType.SPARK:
        result = self._execute_spark(context)
    else:
        result = self._execute_pandas(context)

    row_count = self._get_row_count(result, context.engine_type)
    self._log_execution_complete(start_time, rows=row_count)
    return result
```

### SQL-First (preferred when SQL suffices)
```python
def my_transform(context: EngineContext, params: MyParams) -> EngineContext:
    """Use context.sql() — auto-dispatches to DuckDB (Pandas) or Spark SQL."""
    sql_query = f'SELECT *, UPPER("{params.column}") AS "{params.output}" FROM df'
    return context.sql(sql_query)
```

**Prefer SQL-first.** Only add engine-specific branches when SQL cannot express the logic.

---

## Pydantic Params Model

Every transformer/configurable function gets a Pydantic model:

```python
from pydantic import BaseModel, Field
from typing import List, Optional

class MyTransformParams(BaseModel):
    """Configuration for my_transform.

    Example:
    ```yaml
    my_transform:
      column: source_col
      output: target_col
      threshold: 0.5
    ```
    """

    column: str = Field(..., description="Source column to transform")
    output: Optional[str] = Field(
        default=None, description="Output column name (default: overwrite source)"
    )
    threshold: float = Field(
        default=0.5, description="Minimum threshold", ge=0.0, le=1.0
    )
```

**Rules:**
- `Field(...)` = required, `Field(default=X)` = optional
- Include YAML example in docstring (used by `odibi explain`)
- Flat structure — no nested models unless absolutely needed
- Use `ge=`, `le=`, `min_length=` validators where appropriate

---

## Error Messages

Error messages must be **actionable** — tell the user exactly what to fix:

### ✅ Good Error Messages
```python
raise ValueError(
    f"AggregationPattern (node '{self.config.name}'): 'grain' is required. "
    "Provide a list of columns in the node's params section: params: {grain: [col1, col2]}"
)

raise ValueError(
    f"Connection '{name}' missing 'host'. "
    f"Expected 'host' in config, got keys: {list(config.keys())}"
)
```

### ❌ Bad Error Messages
```python
raise ValueError("Invalid config")       # Useless — what's wrong?
raise ValueError("Missing parameter")    # Which one?
raise Exception("Error occurred")        # No actionable info
```

### Error Message Template
```python
raise ValueError(
    f"<Component> (node '{self.config.name}'): <what's wrong>. "
    "<how to fix it>"
)
```

---

## Registration

### Transformer Registration (in `odibi/transformers/__init__.py`)
```python
from odibi.transformers import my_module

# In register_standard_library():
registry.register(my_module.my_transform, "my_transform", my_module.MyTransformParams)
```

### Pattern Registration (in `odibi/patterns/__init__.py`)
```python
from odibi.patterns.my_pattern import MyPattern

_PATTERNS = {
    # ... existing ...
    "my_pattern": MyPattern,
}
```

### Connection Registration (in `odibi/connections/factory.py`)
```python
# Factory function (deferred import inside function body)
def create_my_source_connection(name: str, config: Dict[str, Any]) -> Any:
    from odibi.connections.my_source import MySourceConnection
    ...

# In register_builtins():
register_connection_factory("my_source", create_my_source_connection)
```

---

## Type Hints

### Required On
- All public functions (every function without `_` prefix)
- All method parameters and return types
- Class attributes with ClassVar

### Style
```python
# Simple types
def count_rows(df: pd.DataFrame) -> int:

# Optional
def get_name(config: dict, default: Optional[str] = None) -> Optional[str]:

# Complex
def process(
    items: List[Dict[str, Any]],
    engine_type: EngineType,
    callback: Optional[Callable[[str], None]] = None,
) -> pd.DataFrame:

# ClassVar
required_params: ClassVar[List[str]] = ["grain"]
```

---

## Docstrings

### Google Style (odibi convention)
```python
def my_function(df: pd.DataFrame, key: str, mode: str = "append") -> pd.DataFrame:
    """One-line summary in imperative mood.

    Longer description if needed. Explain design decisions
    or non-obvious behavior.

    Args:
        df: Input DataFrame.
        key: Column name to use as key.
        mode: Write mode. One of 'append', 'overwrite'. Defaults to 'append'.

    Returns:
        DataFrame with transformation applied.

    Raises:
        ValueError: If key column not found in DataFrame.
    """
```

### When to Include
- **Always:** Public functions, class `__init__`, pattern `execute`/`validate`
- **Optional:** Private helpers (`_helper()`) — add if logic is non-obvious
- **Never:** Test functions (the test name IS the documentation)

---

## File Template (New Source Module)

```python
"""<Module name> — one-line description.

<Longer description if needed.>
"""

import time
from typing import Any, Dict, List, Optional

import pandas as pd
from pydantic import BaseModel, Field

from odibi.context import EngineContext
from odibi.enums import EngineType
from odibi.utils.logging_context import get_logging_context


class MyParams(BaseModel):
    """Configuration for my_function.

    Example:
    ```yaml
    my_function:
      column: name
    ```
    """

    column: str = Field(..., description="Column to process")


def my_function(context: EngineContext, params: MyParams) -> EngineContext:
    """Process the specified column.

    Args:
        context: Engine context with DataFrame.
        params: Validated parameters.

    Returns:
        EngineContext with processed DataFrame.
    """
    ctx = get_logging_context()
    start_time = time.time()

    sql_query = f'SELECT *, UPPER("{params.column}") AS "{params.column}" FROM df'
    result = context.sql(sql_query)

    elapsed_ms = (time.time() - start_time) * 1000
    ctx.debug("my_function completed", elapsed_ms=round(elapsed_ms, 2))
    return result
```

---

## Linting (Must Pass Before PR)

```bash
ruff check .                # Check for issues
ruff check . --fix          # Auto-fix
ruff format .               # Format
```

All PRs must pass `ruff check` with zero errors.

---

## What NOT to Do

| Don't | Do Instead |
|-------|------------|
| Classes with mutable state | Standalone functions (except Pattern subclasses) |
| `print()` | `get_logging_context().debug()` |
| `logging.getLogger()` in transforms | `get_logging_context()` |
| Write output inside a function | Return the DataFrame |
| Nested Pydantic models | Flat params with `Field()` |
| Hyphens in registered names | Underscore only: `my_transform` |
| New dependencies without checking | Check `pyproject.toml` first |
| Comments explaining obvious code | Let the code speak; comment only "why" |
