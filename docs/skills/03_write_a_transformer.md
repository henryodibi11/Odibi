# Skill 03 — Write a Transformer

> **Layer:** Building
> **When:** Adding a new transformer to the odibi registry.
> **Prerequisite:** Complete Skill 01 (Think/Plan/Critique) and Skill 02 (Odibi-First Lookup) to confirm no existing transformer covers your need.

---

## Overview

Odibi transformers are registered functions that operate on `EngineContext` objects. They receive data via context, transform it using SQL (DuckDB for Pandas, Spark SQL for Spark), and return a new context. All transformers follow the same pattern.

**Key files:**
- `odibi/transformers/sql_core.py` — canonical exemplar (26 transformers)
- `odibi/transformers/__init__.py` — registration site
- `odibi/registry.py` — `FunctionRegistry` class
- `odibi/context.py` — `EngineContext` class

---

## Step-by-Step

### Step 1: Define the Pydantic Params Model

Every transformer has a Pydantic model that validates its YAML parameters.

```python
from pydantic import BaseModel, Field
from typing import List, Optional

class MyTransformParams(BaseModel):
    """
    Configuration for my_transform.

    Example:
    ```yaml
    my_transform:
      column: source_col
      output: target_col
    ```
    """

    column: str = Field(..., description="Source column to transform")
    output: Optional[str] = Field(
        default=None, description="Output column name (default: overwrite source)"
    )
```

**Rules:**
- Use `Field(...)` for required params, `Field(default=X)` for optional
- Include a YAML example in the docstring — agents and `odibi explain` use it
- Keep params flat — no nested models unless absolutely necessary

### Step 2: Implement the Transform Function

```python
import time
from odibi.context import EngineContext
from odibi.utils.logging_context import get_logging_context


def my_transform(context: EngineContext, params: MyTransformParams) -> EngineContext:
    """
    One-line description of what this transformer does.

    Design:
    - SQL-First: Pushes logic to the engine's optimizer.
    - Zero-Copy: No data movement to Python.
    """
    ctx = get_logging_context()
    start_time = time.time()

    output_col = params.output or params.column

    ctx.debug(
        "MyTransform starting",
        column=params.column,
        output=output_col,
    )

    # SQL-first approach — works with both DuckDB (Pandas) and Spark SQL
    sql_query = f'SELECT *, UPPER("{params.column}") AS "{output_col}" FROM df'
    result = context.sql(sql_query)

    elapsed_ms = (time.time() - start_time) * 1000
    ctx.debug(
        "MyTransform completed",
        elapsed_ms=round(elapsed_ms, 2),
    )

    return result
```

**Conventions:**
- Signature is always `(context: EngineContext, params: ParamsModel) -> EngineContext`
- Use `context.sql(query)` for SQL-based transforms — this auto-dispatches to DuckDB or Spark SQL
- Use `get_logging_context()` for structured logging — never `print()` or `logging.getLogger()`
- Time the operation and log elapsed_ms
- Return the result of `context.sql()` — never modify `context.df` directly

### Step 3: Register in `__init__.py`

Add registration to `odibi/transformers/__init__.py`:

```python
# In the imports at top
from odibi.transformers import my_module

# In register_standard_library():
registry.register(my_module.my_transform, "my_transform", my_module.MyTransformParams)
```

**Rules:**
- The string name `"my_transform"` is what users write in YAML
- It must be unique across the entire registry
- Use snake_case, no hyphens or dots

### Step 4: Write Tests

Create `tests/unit/transformers/test_my_transform.py`:

```python
import pandas as pd
import pytest

from odibi.context import PandasContext
from odibi.transformers.my_module import MyTransformParams, my_transform


@pytest.fixture
def sample_context():
    df = pd.DataFrame({"name": ["alice", "bob"], "age": [30, 25]})
    return PandasContext(df=df)


def test_basic_transform(sample_context):
    params = MyTransformParams(column="name")
    result = my_transform(sample_context, params)
    assert list(result.df["name"]) == ["ALICE", "BOB"]


def test_with_output_column(sample_context):
    params = MyTransformParams(column="name", output="upper_name")
    result = my_transform(sample_context, params)
    assert "upper_name" in result.df.columns
    assert "name" in result.df.columns  # original preserved


def test_invalid_params():
    with pytest.raises(Exception):
        MyTransformParams()  # missing required 'column'
```

**Testing rules:**
- Test with Pandas DataFrames (no Spark/JVM needed in CI)
- Test happy path, edge cases, and param validation
- Do NOT use caplog — assert on return values only
- Do NOT put "spark" or "delta" in the test file name
- Place in `tests/unit/transformers/`

### Step 5: Verify

```bash
# Run your tests
pytest tests/unit/transformers/test_my_transform.py -v

# Verify registration
python -c "from odibi.transformers import register_standard_library; register_standard_library(); from odibi.registry import FunctionRegistry; print('my_transform' in FunctionRegistry.list_functions())"

# Check it appears in CLI
odibi list transformers | grep my_transform
```

---

## Engine Parity Checklist

If your transformer uses engine-specific logic (not pure SQL):

- [ ] Pandas path works with DuckDB SQL via `context.sql()`
- [ ] Spark path works with Spark SQL via `context.sql()`
- [ ] Polars path handled (or raises `NotImplementedError` with clear message)
- [ ] All paths tested

**When SQL is sufficient** (most cases), `context.sql()` handles parity automatically. Only add engine-specific paths when SQL cannot express the logic (e.g., UDFs, window functions with complex frames, or library-specific operations like CoolProp).

---

## Exemplar: `filter_rows` (Simplest Transformer)

```python
class FilterRowsParams(BaseModel):
    condition: str = Field(..., description="SQL WHERE clause")

def filter_rows(context: EngineContext, params: FilterRowsParams) -> EngineContext:
    sql_query = f"SELECT * FROM df WHERE {params.condition}"
    return context.sql(sql_query)
```

This is the gold standard — 2 lines of logic, SQL-first, works on all engines.

---

## Exemplar: `derive_columns` (Multiple Outputs)

```python
class DeriveColumnsParams(BaseModel):
    derivations: Dict[str, str] = Field(..., description="Map of column name to SQL expression")

def derive_columns(context: EngineContext, params: DeriveColumnsParams) -> EngineContext:
    projections = ", ".join(
        f'({expr}) AS "{col}"' for col, expr in params.derivations.items()
    )
    sql_query = f"SELECT *, {projections} FROM df"
    return context.sql(sql_query)
```

---

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| Using `context.df` directly instead of SQL | Use `context.sql()` — it handles engine dispatch |
| Forgetting to register in `__init__.py` | Transformer won't appear in `odibi list transformers` |
| Using `logging.getLogger()` | Use `get_logging_context()` |
| Adding engine-specific branches unnecessarily | If SQL works, use SQL only |
| Creating a class-based transformer | Use standalone functions |
| Not adding Pydantic params model | Users won't get validation or `odibi explain` docs |
| Using hyphens in the registered name | Node names must be alphanumeric + underscore |
