# Skill 15 — Engine Parity Standards

> **Layer:** Standards
> **When:** Writing ANY code that touches DataFrames or engine-specific logic.
> **Rule:** If Pandas has it, Spark and Polars must be planned. No silent gaps.

---

## Purpose

Odibi supports three engines: Pandas (via DuckDB SQL), Spark, and Polars. When a feature works on one engine but silently fails on another, users get runtime surprises. This skill ensures every engine path is explicitly handled — either implemented, skipped with `NotImplementedError`, or guarded with a skip marker in tests.

---

## Decision Tree: Which Approach?

```
Does your logic fit in SQL?
├── YES → Use context.sql() — automatic parity across all engines
│         (DuckDB for Pandas, Spark SQL for Spark, DuckDB for Polars)
│         DONE. No engine-specific code needed.
│
└── NO → You need engine-specific branches
         │
         ├── Can you implement all 3 engines now?
         │   ├── YES → Implement all 3 + test all 3
         │   └── NO → Implement what you can + raise NotImplementedError for the rest
         │             with a message like:
         │             raise NotImplementedError(
         │                 f"my_transform does not yet support {context.engine_type}. "
         │                 "Contributions welcome — see docs/skills/03_write_a_transformer.md"
         │             )
         │
         └── File a GitHub issue for each missing engine path
```

---

## Implementation Patterns

### Pattern 1: SQL-First (Preferred)

```python
def my_transform(context: EngineContext, params: MyParams) -> EngineContext:
    """Works on all engines via context.sql()."""
    sql = f'SELECT *, UPPER("{params.column}") AS upper_{params.column} FROM df'
    return context.sql(sql)
```

**Use for:** filter, derive, cast, rename, select, drop, sort, limit, distinct, fill_nulls, case_when, date functions, concat, coalesce — most sql_core transformers.

### Pattern 2: Engine Dispatch with Private Helpers

```python
def my_transform(context: EngineContext, params: MyParams) -> EngineContext:
    """Requires engine-specific logic."""
    if context.engine_type == EngineType.PANDAS:
        return _my_transform_pandas(context, params)
    elif context.engine_type == EngineType.SPARK:
        return _my_transform_spark(context, params)
    elif context.engine_type == EngineType.POLARS:
        return _my_transform_polars(context, params)
    else:
        raise ValueError(f"Unsupported engine type: {context.engine_type}")


def _my_transform_pandas(context: EngineContext, params: MyParams) -> EngineContext:
    df = context.df
    # Pandas-specific logic
    return EngineContext(context=context.context, df=result, ...)


def _my_transform_spark(context: EngineContext, params: MyParams) -> EngineContext:
    df = context.df
    # PySpark-specific logic
    return EngineContext(context=context.context, df=result, ...)


def _my_transform_polars(context: EngineContext, params: MyParams) -> EngineContext:
    df = context.df
    # Polars-specific logic
    return EngineContext(context=context.context, df=result, ...)
```

**Use for:** join, merge, SCD2, delete_detection, sessionize, manufacturing — anything needing native DataFrame APIs.

### Pattern 3: Implemented + NotImplementedError Stub

```python
def my_transform(context: EngineContext, params: MyParams) -> EngineContext:
    if context.engine_type == EngineType.PANDAS:
        return _my_transform_pandas(context, params)
    elif context.engine_type == EngineType.SPARK:
        return _my_transform_spark(context, params)
    elif context.engine_type == EngineType.POLARS:
        raise NotImplementedError(
            f"my_transform does not yet support Polars. "
            f"See GitHub issue #NNN for tracking."
        )
    else:
        raise ValueError(f"Unsupported engine type: {context.engine_type}")
```

**Use for:** When you can't implement all engines yet. Always file a GitHub issue.

---

## Testing All Engines

### Test File Structure

```python
"""Tests for my_transform — all engines."""

import pandas as pd
import pytest

# Skip guards
try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    pl = None
    HAS_POLARS = False

requires_polars = pytest.mark.skipif(not HAS_POLARS, reason="Polars not installed")

from odibi.context import EngineContext, PandasContext
from odibi.engine.pandas_engine import PandasEngine
from odibi.enums import EngineType
from odibi.transformers.my_module import my_transform, MyParams


# ---------------------------------------------------------------------------
# Pandas Tests (always run)
# ---------------------------------------------------------------------------

def test_pandas_basic():
    df = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
    ctx = PandasContext()
    engine_ctx = EngineContext(
        context=ctx, df=df, engine_type=EngineType.PANDAS,
        engine=PandasEngine(config={}),
    )
    result = my_transform(engine_ctx, MyParams(column="value"))
    assert result.df["value"].tolist() == [10, 20, 30]


# ---------------------------------------------------------------------------
# Polars Tests (skipped if Polars not installed)
# ---------------------------------------------------------------------------

@requires_polars
def test_polars_basic():
    from odibi.context import PolarsContext
    df = pl.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
    engine_ctx = EngineContext(
        context=PolarsContext(), df=df, engine_type=EngineType.POLARS,
        engine=None,
    )
    result = my_transform(engine_ctx, MyParams(column="value"))
    assert result.df["value"].to_list() == [10, 20, 30]


# ---------------------------------------------------------------------------
# NotImplementedError Tests (for engines not yet supported)
# ---------------------------------------------------------------------------

def test_unsupported_engine_raises():
    """If Polars is not implemented, verify clear error."""
    from unittest.mock import MagicMock
    ctx = MagicMock()
    ctx.engine_type = EngineType.POLARS
    with pytest.raises(NotImplementedError, match="Polars"):
        my_transform(ctx, MyParams(column="value"))


# ---------------------------------------------------------------------------
# Cross-Engine Comparison (when all engines are implemented)
# ---------------------------------------------------------------------------

@requires_polars
def test_pandas_polars_parity():
    """Verify Pandas and Polars produce identical results."""
    data = {"id": [1, 2, 3], "value": [10, 20, 30]}

    # Pandas
    pd_ctx = EngineContext(
        context=PandasContext(), df=pd.DataFrame(data),
        engine_type=EngineType.PANDAS, engine=PandasEngine(config={}),
    )
    pd_result = my_transform(pd_ctx, MyParams(column="value"))

    # Polars
    from odibi.context import PolarsContext
    pl_ctx = EngineContext(
        context=PolarsContext(), df=pl.DataFrame(data),
        engine_type=EngineType.POLARS, engine=None,
    )
    pl_result = my_transform(pl_ctx, MyParams(column="value"))

    # Compare (convert Polars to Pandas for comparison)
    pd.testing.assert_frame_equal(
        pd_result.df.sort_values("id").reset_index(drop=True),
        pl_result.df.to_pandas().sort_values("id").reset_index(drop=True),
        check_dtype=False,  # Engine dtype differences are OK
    )
```

### Cross-Engine Comparison Helper

```python
def assert_engine_parity(pandas_df: pd.DataFrame, other_df, sort_by: str = None):
    """Compare a Pandas result with Spark or Polars result.

    Args:
        pandas_df: Reference Pandas DataFrame.
        other_df: Spark DataFrame (.toPandas()) or Polars DataFrame (.to_pandas()).
        sort_by: Column to sort by before comparison. None = sort by all columns.
    """
    import polars as pl

    # Convert to Pandas if needed
    if hasattr(other_df, "toPandas"):
        other_pd = other_df.toPandas()
    elif isinstance(other_df, pl.DataFrame):
        other_pd = other_df.to_pandas()
    else:
        other_pd = other_df

    # Sort
    sort_cols = [sort_by] if sort_by else list(pandas_df.columns)
    pd_sorted = pandas_df.sort_values(sort_cols).reset_index(drop=True)
    other_sorted = other_pd.sort_values(sort_cols).reset_index(drop=True)

    pd.testing.assert_frame_equal(pd_sorted, other_sorted, check_dtype=False)
```

---

## Polars-Specific Conventions

### DateTime Handling
```python
# Polars uses timezone-aware datetimes — ALWAYS use UTC in tests
from datetime import datetime, timezone, timedelta

# ✅ Correct
df = pl.DataFrame({
    "ts": [datetime.now(timezone.utc) - timedelta(hours=1)]
})

# ❌ Wrong — naive timestamps cause 5h offset
df = pl.DataFrame({
    "ts": [datetime.now() - timedelta(hours=1)]
})
```

### Polars API Differences
```python
# Pandas → Polars equivalents
df.columns           # Both: same
len(df)              # Both: same
df["col"].tolist()   # Pandas
df["col"].to_list()  # Polars (note: to_list, not tolist)
df.empty             # Pandas
df.is_empty()        # Polars (note: is_empty(), not empty)
df.shape[0]          # Both: same
df.sort_values("x")  # Pandas
df.sort("x")         # Polars
pd.isna(val)         # Pandas
val is None           # Polars (Polars uses None, not NaN for nulls)
```

### Lazy vs Eager
```python
# Some Polars code may use lazy frames
if isinstance(df, pl.LazyFrame):
    df = df.collect()
# Always work with eager DataFrames in tests
```

---

## Spark-Specific Conventions

### Mock Spark in CI
```python
# CI has no JVM — mock Spark for branch coverage
# See Skill 13 for the full mock pattern

# CRITICAL: import odibi FIRST, then mock pyspark
import odibi.catalog
# ... then install mocks in sys.modules
```

### Real Spark on Databricks
```python
# On Databricks, spark is available as a global
# Use Skill 12 (Databricks Notebook Protocol) for setup

from odibi.engine.spark_engine import SparkEngine
engine = SparkEngine(config={"spark": spark})
```

---

## Engine Parity Checklist

Before submitting a PR that adds or modifies engine-specific code:

- [ ] **Pandas path** implemented and tested
- [ ] **Spark path** implemented OR `NotImplementedError` with GitHub issue
- [ ] **Polars path** implemented OR `NotImplementedError` with GitHub issue
- [ ] **Cross-engine comparison** test (if all engines implemented)
- [ ] **Skip guards** on Polars/Spark tests (`requires_polars`, `requires_pyspark`)
- [ ] **Same output schema** across all engines (column names and order match)
- [ ] **Same row count** across all engines for identical input
- [ ] **Same values** (use `check_dtype=False` for numeric type differences)
- [ ] **NotImplementedError** tests for any stub engines

---

## Common Parity Bugs

| Bug | Root Cause | Fix |
|-----|-----------|-----|
| Different column order | Spark/Polars may reorder | Sort columns before comparing |
| Int vs Int64 vs int32 | Engine default int types differ | `check_dtype=False` in comparison |
| NaN vs None | Pandas uses NaN, Polars uses None | Use `isna()` / `is_null()` generically |
| String vs object | Pandas stores strings as object dtype | `check_dtype=False` |
| Timezone-naive vs UTC | Polars always adds UTC | Use `timezone.utc` everywhere |
| Row order different | Engines may not preserve insertion order | Sort before comparing |
