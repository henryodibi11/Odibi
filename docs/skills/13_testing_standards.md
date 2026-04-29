# Skill 13 — Testing Standards

> **Layer:** Standards
> **When:** Writing ANY test file for odibi. Load this BEFORE writing a single line of test code.
> **Goal:** Agent produces test files that pass on first run with zero edits.

---

## Purpose

Odibi tests have 3 environments: local Windows (no Spark JVM), CI (no Spark JVM), and Databricks (everything available). Tests must **gracefully skip** when dependencies are missing — never fail. This skill encodes every convention so precisely that the output requires no human editing.

---

## File Structure

### Naming
```
tests/unit/<module>/test_<module>_<suffix>.py
```

**Forbidden words in filenames:** `spark`, `delta` — conftest.py auto-skips them on Windows.

| Good ✅ | Bad ❌ | Why |
|---------|--------|-----|
| `test_catalog_mock_engine_reads.py` | `test_catalog_spark_reads.py` | "spark" triggers skip filter |
| `test_catalog_sync_coverage.py` | `test_delta_sync.py` | "delta" triggers skip filter |
| `test_scd_coverage.py` | `test_scd_spark_path.py` | "spark" triggers skip filter |

### Directory Mapping
```
odibi/transformers/sql_core.py    → tests/unit/transformers/test_sql_core_coverage.py
odibi/patterns/dimension.py      → tests/unit/patterns/test_dimension_coverage.py
odibi/validation/engine.py       → tests/unit/validation/test_validation_engine.py
odibi/connections/postgres.py     → tests/unit/connections/test_postgres_coverage.py
odibi/cli/main.py                → tests/unit/cli/test_cli_main_coverage.py
odibi/story/generator.py         → tests/unit/story/test_story_generator_coverage.py
odibi/catalog.py                 → tests/unit/test_catalog_coverage.py
```

---

## Import Block Template

Every test file starts with this exact structure:

```python
"""Tests for odibi.<module>.<submodule> — <what's tested>."""

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta, timezone

# --- Conditional imports (MUST use these exact patterns) ---

# Polars: use pytest.importorskip at module level
# This SKIPS the entire file if Polars is missing — tests never fail.
pl = pytest.importorskip("polars")

# OR: use try/except with a flag for per-test skipping
try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    pl = None
    HAS_POLARS = False

# DuckDB: use pytest.importorskip
duckdb = pytest.importorskip("duckdb")

# CoolProp: use pytest.importorskip
pytest.importorskip("CoolProp")

# SQLAlchemy: use try/except flag
try:
    import sqlalchemy
    _has_sqlalchemy = True
except ImportError:
    _has_sqlalchemy = False

# --- odibi imports (AFTER conditional imports) ---
from odibi.context import EngineContext, PandasContext
from odibi.engine.pandas_engine import PandasEngine
from odibi.enums import EngineType
```

---

## Skip Guard Reference

Use these exact patterns. Do NOT invent new ones.

### Module-Level Skip (entire file skipped)
```python
# Use when the ENTIRE file needs the dependency
pl = pytest.importorskip("polars")
duckdb = pytest.importorskip("duckdb")
pytest.importorskip("CoolProp")
```

### Per-Test Skip (individual tests skipped)
```python
# Use when SOME tests need the dependency but others don't

# Pattern A: flag variable
try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    pl = None
    HAS_POLARS = False

@pytest.mark.skipif(not HAS_POLARS, reason="Polars not installed")
def test_polars_path():
    ...

# Pattern B: None check (for libraries that assign to a variable)
try:
    import polars as pl
except ImportError:
    pl = None

@pytest.mark.skipif(pl is None, reason="polars not installed")
def test_polars_feature():
    ...

# Pattern C: inline importorskip (for one-off tests)
def test_needs_duckdb():
    duckdb = pytest.importorskip("duckdb")
    conn = duckdb.connect()
    ...
```

### Standard Skip Guards (copy-paste these)

```python
# --- Polars ---
try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    pl = None
    HAS_POLARS = False

requires_polars = pytest.mark.skipif(not HAS_POLARS, reason="Polars not installed")

# --- PySpark ---
try:
    import pyspark
    _has_pyspark = True
except ImportError:
    _has_pyspark = False

requires_pyspark = pytest.mark.skipif(not _has_pyspark, reason="pyspark not installed")

# --- SQLAlchemy ---
try:
    import sqlalchemy
    _has_sqlalchemy = True
except ImportError:
    _has_sqlalchemy = False

requires_sqlalchemy = pytest.mark.skipif(not _has_sqlalchemy, reason="sqlalchemy not installed")

# --- DuckDB ---
try:
    import duckdb
    _has_duckdb = True
except ImportError:
    _has_duckdb = False

requires_duckdb = pytest.mark.skipif(not _has_duckdb, reason="duckdb not installed")

# --- Delta Lake ---
try:
    from deltalake import DeltaTable
    _has_delta = True
except ImportError:
    _has_delta = False

requires_delta = pytest.mark.skipif(not _has_delta, reason="deltalake not installed")

# --- CoolProp ---
try:
    import CoolProp
    _has_coolprop = True
except ImportError:
    _has_coolprop = False

requires_coolprop = pytest.mark.skipif(not _has_coolprop, reason="CoolProp not installed")
```

**Usage:**
```python
@requires_polars
def test_polars_validation():
    ...

@requires_duckdb
def test_duckdb_aggregate():
    ...
```

---

## Fixture Standards

### PandasEngine (most common)
```python
@pytest.fixture
def engine():
    return PandasEngine(config={})
```

### EngineContext with DuckDB SQL
```python
def make_context(df: pd.DataFrame) -> EngineContext:
    """Create a Pandas EngineContext with DuckDB SQL executor."""
    import duckdb

    def sql_executor(query, context):
        conn = duckdb.connect()
        for name in context.list_names():
            conn.register(name, context.get(name))
        return conn.execute(query).fetchdf()

    ctx = PandasContext()
    engine = PandasEngine(config={})
    return EngineContext(
        context=ctx,
        df=df,
        engine_type=EngineType.PANDAS,
        sql_executor=sql_executor,
        engine=engine,
    )
```

### Mock NodeConfig (for patterns)
```python
def make_config(name="test_node", params=None):
    cfg = MagicMock()
    cfg.name = name
    cfg.params = params or {}
    return cfg
```

### CatalogManager
```python
@pytest.fixture
def catalog(tmp_path):
    from odibi.catalog import CatalogManager
    from odibi.config import SystemConfig
    return CatalogManager(
        spark=None,
        config=SystemConfig(connection="local", path="_odibi_system"),
        base_path=str(tmp_path / "_odibi_system"),
        engine=PandasEngine(config={}),
    )
```

### Delta Table Seeding
```python
def seed_delta(path, data_dict, schema=None):
    """Seed a Delta table. Use pa.table — NEVER pd.DataFrame (Null type issue)."""
    import pyarrow as pa
    from deltalake import write_deltalake
    table = pa.table(data_dict, schema=schema)
    write_deltalake(str(path), table, mode="overwrite")
```

---

## Assertion Standards

### ✅ Good Assertions (concrete values)
```python
# Exact row count
assert len(result.df) == 3

# Exact column values
assert result.df["name"].tolist() == ["Alice", "Bob", "Charlie"]

# Exact numeric result
assert result.df["total"].sum() == 150.0

# Exact schema
assert list(result.df.columns) == ["id", "name", "amount"]

# Error message content
with pytest.raises(ValueError, match="key_param"):
    pattern.validate()

# Empty DataFrame
assert result.df.empty

# Null handling
assert result.df["nullable_col"].isna().sum() == 2
```

### ❌ Bad Assertions (too weak)
```python
# NEVER DO THESE — they pass even when the result is wrong
assert result is not None          # Useless — almost never None
assert len(result) > 0             # Doesn't verify content
assert result.df.shape[0] >= 1     # Doesn't verify content
assert "error" in str(errors)      # Fragile string matching
assert result.status == "success"  # Doesn't verify data
```

### Assertion Patterns by Test Type

**Transformer tests:**
```python
def test_filter_rows_basic():
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    ctx = make_context(df)
    params = FilterRowsParams(condition="id > 1")
    result = filter_rows(ctx, params)
    assert result.df["id"].tolist() == [2, 3]  # Exact values
    assert len(result.df) == 2                  # Exact count
```

**Pattern tests:**
```python
def test_dimension_scd1_update():
    # ... setup ...
    result = pattern.execute(context)
    assert len(result) == 10                                      # Row count
    assert result["customer_sk"].max() == 10                      # SK generation
    assert result.loc[result["id"] == 1, "name"].iloc[0] == "Updated"  # Value check
```

**Validation tests:**
```python
def test_not_null_fail():
    df = pd.DataFrame({"a": [1, None, 3]})
    cfg = ValidationConfig(tests=[NotNullTest(columns=["a"])])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1           # Exact error count
    assert "NULLs" in errors[0]       # Error message content
```

**Negative tests (always include at least one):**
```python
def test_missing_required_param():
    with pytest.raises(ValueError, match="grain"):
        pattern.validate()

def test_empty_dataframe():
    df = pd.DataFrame({"id": [], "name": []})
    result = transform(make_context(df), params)
    assert result.df.empty
```

---

## Test Organization

### Section Comments
```python
# ── Pandas NOT_NULL ──────────────────────────────────────────────

def test_pandas_not_null_pass():
    ...

def test_pandas_not_null_fail():
    ...

# ── Polars NOT_NULL ──────────────────────────────────────────────

@requires_polars
def test_polars_not_null_pass():
    ...
```

### Grouping by Behavior
```python
# Group 1: Happy path
# Group 2: Edge cases (empty, null, single row)
# Group 3: Error cases (bad params, missing columns)
# Group 4: Engine parity (Polars, marked with skip guard)
```

---

## Coverage Measurement

```bash
# Single module
python -m coverage run --source=odibi.<module> -m pytest tests/unit/<path>/test_file.py -q --tb=no
python -m coverage report --show-missing

# NEVER use pytest --cov (breaks Rich logging)
```

---

## What NOT to Do

| Don't | Why | Do Instead |
|-------|-----|------------|
| `pytest --cov` | Breaks Rich logging singleton | `python -m coverage run` |
| `caplog` | Logging context pollution → batch failures | Assert return values |
| `"spark"` in filename | conftest.py skips it | Use `mock_engine` or `coverage` in name |
| `builtins.__import__` patch | Catches Rich/logging imports | `sys.modules["pkg"] = None` |
| `assert result is not None` | Passes on garbage output | Assert concrete values |
| `pd.DataFrame` for Delta seeding | Null-type columns rejected | `pa.table()` with explicit schema |
| Bare `logging.getLogger` | Conflicts with `get_logging_context()` | Don't assert on logs at all |
| Running full `tests/unit/` | Deadlocks | Run in batches |

---

## Complete Test File Template

```python
"""Tests for odibi.<module>.<submodule> — <description>."""

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from odibi.context import EngineContext, PandasContext
from odibi.engine.pandas_engine import PandasEngine
from odibi.enums import EngineType

# Optional dependency guards
try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    pl = None
    HAS_POLARS = False

requires_polars = pytest.mark.skipif(not HAS_POLARS, reason="Polars not installed")

# Module under test
from odibi.<module> import TargetFunction, TargetParams


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def engine():
    return PandasEngine(config={})


def make_context(df: pd.DataFrame) -> EngineContext:
    ctx = PandasContext()
    return EngineContext(
        context=ctx, df=df, engine_type=EngineType.PANDAS, engine=PandasEngine(config={})
    )


# ---------------------------------------------------------------------------
# Happy Path
# ---------------------------------------------------------------------------

def test_basic_operation():
    df = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
    ctx = make_context(df)
    params = TargetParams(column="value")
    result = TargetFunction(ctx, params)
    assert result.df["value"].tolist() == [10, 20, 30]  # Concrete values


# ---------------------------------------------------------------------------
# Edge Cases
# ---------------------------------------------------------------------------

def test_empty_dataframe():
    df = pd.DataFrame({"id": [], "value": []})
    ctx = make_context(df)
    params = TargetParams(column="value")
    result = TargetFunction(ctx, params)
    assert result.df.empty


def test_single_row():
    df = pd.DataFrame({"id": [1], "value": [42]})
    ctx = make_context(df)
    params = TargetParams(column="value")
    result = TargetFunction(ctx, params)
    assert len(result.df) == 1


def test_null_values():
    df = pd.DataFrame({"id": [1, 2], "value": [None, 30]})
    ctx = make_context(df)
    params = TargetParams(column="value")
    result = TargetFunction(ctx, params)
    assert result.df["value"].isna().sum() == 1


# ---------------------------------------------------------------------------
# Error Cases
# ---------------------------------------------------------------------------

def test_invalid_params():
    with pytest.raises((ValueError, Exception)):
        TargetParams()  # Missing required field


# ---------------------------------------------------------------------------
# Polars Parity
# ---------------------------------------------------------------------------

@requires_polars
def test_polars_basic_operation():
    from odibi.context import PolarsContext
    df = pl.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
    ctx = EngineContext(
        context=PolarsContext(), df=df,
        engine_type=EngineType.POLARS, engine=None,
    )
    params = TargetParams(column="value")
    result = TargetFunction(ctx, params)
    assert result.df["value"].to_list() == [10, 20, 30]
```
