# Testing & Pytest — A Comprehensive Guide Using Odibi

> **Purpose:** Learn everything about pytest and Python testing using Odibi as the working codebase.
> **Audience:** You (a developer who built the framework but wants to deeply understand the testing that was built for it).
> **Method:** Every example uses real Odibi code. No toy examples — every concept is grounded in your own codebase.
> **Prerequisite:** You can run `pytest tests/unit/validation/test_validation_engine.py -v` and see tests pass.

---

## Table of Contents

1. [Why Test?](#1-why-test)
2. [How Pytest Works (The Mental Model)](#2-how-pytest-works-the-mental-model)
3. [Your First Test — Anatomy](#3-your-first-test--anatomy)
4. [Assertions — The Heart of Every Test](#4-assertions--the-heart-of-every-test)
5. [Fixtures — Setup Without Repetition](#5-fixtures--setup-without-repetition)
6. [Markers and Skip Guards](#6-markers-and-skip-guards)
7. [Parametrize — One Test, Many Inputs](#7-parametrize--one-test-many-inputs)
8. [Mocking — Testing Without Real Dependencies](#8-mocking--testing-without-real-dependencies)
9. [Testing for Errors (Negative Tests)](#9-testing-for-errors-negative-tests)
10. [conftest.py — Shared Infrastructure](#10-conftestpy--shared-infrastructure)
11. [Test Organization — How to Structure Files](#11-test-organization--how-to-structure-files)
12. [Coverage — Measuring What's Tested](#12-coverage--measuring-whats-tested)
13. [The Odibi Traps — Lessons That Cost Real Time](#13-the-odibi-traps--lessons-that-cost-real-time)
14. [Patterns Cookbook — Real Recipes from Odibi Tests](#14-patterns-cookbook--real-recipes-from-odibi-tests)
15. [The "Break It" Methodology](#15-the-break-it-methodology)
16. [Exercises](#16-exercises)
17. [Quick Reference Card](#17-quick-reference-card)

---

## 1. Why Test?

Testing isn't about proving code works — it's about **defining what "works" means** so you can change code later without fear.

### The Real Payoff

You have 34,000+ statements in Odibi. Without tests, every change is a gamble. With 80% coverage:
- You can refactor `sql_core.py` (1,182 lines) and know in 5 seconds if you broke anything
- You can upgrade Polars and immediately see which transformers fail
- You can hand Odibi to another developer and the tests serve as executable documentation

### The Testing Pyramid (Odibi Edition)

```
         ┌──────────────┐
         │  E2E / Smoke  │   campaign/ notebooks (run on Databricks)
         │  (slow, few)  │   examples/star_schema_e2e/
         ├──────────────┤
         │  Integration  │   tests/integration/ (mock Spark, real DuckDB)
         │  (medium)     │   tests/engine/test_parity.py
         ├──────────────┤
         │  Unit Tests   │   tests/unit/ (fast, many, your bread & butter)
         │  (fast, many) │   208 tests for sql_server_writer alone
         └──────────────┘
```

**Odibi's approach:** Unit tests in CI (fast, no JVM), integration/E2E on Databricks (real Spark). This is a pragmatic split driven by real constraints (no JVM in Windows CI).

---

## 2. How Pytest Works (The Mental Model)

Pytest does three things:

1. **Discovers** test files and functions
2. **Runs** them
3. **Reports** pass/fail

### Discovery Rules

Pytest finds tests by convention:

| What | Rule | Example |
|------|------|---------|
| **Files** | Names start with `test_` or end with `_test.py` | `test_validation_engine.py` ✅ |
| **Functions** | Names start with `test_` | `def test_pandas_not_null_pass()` ✅ |
| **Classes** | Names start with `Test` | `class TestExtractDateParts:` ✅ |
| **Methods** | Names start with `test_` inside a `Test` class | `def test_all_parts_default_prefix(self)` ✅ |

**No registration needed.** Just name things right and pytest finds them.

### Running Tests

```bash
# Run one file
pytest tests/unit/validation/test_validation_engine.py -v

# Run one test function
pytest tests/unit/validation/test_validation_engine.py::test_pandas_not_null_pass -v

# Run one class
pytest tests/unit/transformers/test_sql_core_coverage.py::TestExtractDateParts -v

# Run tests matching a keyword
pytest tests/unit/ -k "not_null" -v

# Short output (just pass/fail counts)
pytest tests/unit/validation/test_validation_engine.py -q

# Stop on first failure
pytest tests/unit/validation/test_validation_engine.py -x

# Show print() output (normally hidden)
pytest tests/unit/validation/test_validation_engine.py -s
```

### Key Flags Cheat Sheet

| Flag | What it does | When to use |
|------|-------------|-------------|
| `-v` | Verbose — shows each test name | Debugging, seeing what ran |
| `-q` | Quiet — just counts | CI, coverage runs |
| `-x` | Stop on first failure | Debugging a cascade |
| `-s` | Show stdout/stderr | Debugging print statements |
| `--tb=short` | Shorter tracebacks | Readability |
| `--tb=no` | No tracebacks | Coverage runs (speed) |
| `-k "expr"` | Filter by keyword expression | Run subset by name |
| `--lf` | Rerun only last-failed tests | Iterating on fixes |

---

## 3. Your First Test — Anatomy

Let's look at a real test from `test_validation_engine.py`:

```python
def test_pandas_not_null_pass(validator):
    df = pd.DataFrame({"a": [1, 2, 3]})                          # 1. ARRANGE
    cfg = ValidationConfig(tests=[NotNullTest(columns=["a"])])    # 2. ARRANGE
    assert validator.validate(df, cfg) == []                      # 3. ACT + ASSERT
```

Every test follows **Arrange → Act → Assert**:

| Step | What | This test |
|------|------|-----------|
| **Arrange** | Set up the data and configuration | Create DataFrame, create ValidationConfig |
| **Act** | Call the function you're testing | `validator.validate(df, cfg)` |
| **Assert** | Check the result is correct | `== []` (no errors means pass) |

### The Naming Convention

```
test_pandas_not_null_pass
│     │       │        │
│     │       │        └── outcome: what should happen
│     │       └─────────── feature: what's being tested
│     └─────────────────── engine: which engine path
└───────────────────────── prefix: required by pytest
```

Good names read like sentences: "test that pandas not_null validation passes when data is clean."

---

## 4. Assertions — The Heart of Every Test

An assertion is a statement that must be `True` or the test fails. Pytest uses Python's built-in `assert`.

### Basic Assertions

```python
# Equality
assert result == expected
assert len(errors) == 1
assert result.df["id"].tolist() == [1, 2, 3]

# Membership
assert "NULLs" in errors[0]
assert "name" in result.df.columns

# Boolean
assert result.df.empty
assert not errors

# Numeric
assert result.df["total"].sum() == 150.0
assert result.df["value"].isna().sum() == 2
```

### ✅ Good Assertions (from Odibi's standards)

```python
# Exact row count — you know the answer
assert len(result.df) == 3

# Exact column values — you know the data
assert result.df["name"].tolist() == ["Alice", "Bob", "Charlie"]

# Exact schema — you know the columns
assert list(result.df.columns) == ["id", "name", "amount"]

# Error message content — you know the error
with pytest.raises(ValueError, match="key_param"):
    pattern.validate()
```

### ❌ Bad Assertions (too weak — they pass even when the result is wrong)

```python
# These are USELESS — they almost never fail
assert result is not None          # Almost nothing is None
assert len(result) > 0             # Doesn't verify WHAT's in there
assert result.df.shape[0] >= 1     # Could have garbage data
assert "error" in str(errors)      # Fragile string matching
```

**The rule:** An assertion should fail if the code is broken. If you can imagine broken code that still passes your assertion, the assertion is too weak.

### Pandas-Specific Assertion Patterns

```python
# Compare DataFrames (order-independent)
from odibi.testing import assert_frame_equal
assert_frame_equal(result_df, expected_df)

# Check specific cells
assert result.df.loc[result.df["id"] == 1, "name"].iloc[0] == "Updated"

# Check nulls
assert result.df["nullable_col"].isna().sum() == 2

# Check dtypes
assert result.df["amount"].dtype == "float64"
```

### What Makes a Failing Assertion Useful?

Pytest shows you what went wrong:

```
>       assert result.df["id"].tolist() == [1, 2, 3]
E       AssertionError: assert [1, 3] == [1, 2, 3]
E         Right contains one more item: 3
```

This tells you *exactly* which row is missing. Compare with:

```
>       assert len(result.df) > 0
E       AssertionError: assert 0 > 0
```

This only tells you "it's empty" — not *why*.

---

## 5. Fixtures — Setup Without Repetition

A **fixture** is a function that provides setup for tests. Instead of repeating setup code in every test, you write it once as a fixture.

### The Problem (Without Fixtures)

```python
def test_not_null_pass():
    validator = Validator()  # repeated
    df = pd.DataFrame({"a": [1, 2, 3]})
    ...

def test_not_null_fail():
    validator = Validator()  # repeated again
    df = pd.DataFrame({"a": [1, None, 3]})
    ...
```

### The Solution (With Fixtures)

```python
@pytest.fixture
def validator():
    return Validator()

def test_not_null_pass(validator):       # pytest injects it
    df = pd.DataFrame({"a": [1, 2, 3]})
    ...

def test_not_null_fail(validator):       # same fixture, fresh instance
    df = pd.DataFrame({"a": [1, None, 3]})
    ...
```

**How it works:** Pytest sees `validator` in the function signature, finds the `@pytest.fixture` with that name, calls it, and passes the result to your test. **Every test gets a fresh instance.**

### Real Odibi Fixtures

#### Simple Fixture — Engine

```python
# From tests/unit/patterns/test_dimension_coverage.py

@pytest.fixture
def engine():
    mock = MagicMock()
    mock.connections = {}
    return mock
```

#### Fixture With Dependencies — Catalog

```python
# From docs/skills/07_testing.md

@pytest.fixture
def catalog(tmp_path):                    # tmp_path is a BUILT-IN fixture
    return CatalogManager(
        spark=None,
        config=SystemConfig(connection="local", path="_odibi_system"),
        base_path=str(tmp_path / "_odibi_system"),  # unique temp dir per test
        engine=PandasEngine(config={}),
    )
```

`tmp_path` is a **built-in pytest fixture** that gives each test a unique temporary directory. It's cleaned up automatically.

#### Factory Fixture — Make Multiple Configs

```python
# From tests/unit/patterns/test_dimension_coverage.py

@pytest.fixture
def cfg():
    c = MagicMock(spec=NodeConfig)
    c.name = "test_dim"
    c.params = {}
    return c
```

#### Helper Function vs Fixture

Not everything needs to be a fixture. Use a **plain function** when you need different data per test:

```python
# This is a HELPER, not a fixture — called directly in each test
def _make_context(df, engine=None):
    return EngineContext(
        context=PandasContext(),
        df=df,
        engine_type=EngineType.PANDAS,
        engine=engine,
    )

def test_basic():
    ctx = _make_context(pd.DataFrame({"id": [1, 2]}))  # custom data
    ...
```

**When to use a fixture vs a helper:**

| Use a Fixture | Use a Helper Function |
|---|---|
| Same setup for many tests | Different data per test |
| Needs cleanup (`yield`) | No cleanup needed |
| Can be shared via `conftest.py` | Local to one file |

### Fixture Scope

By default, fixtures run once per test. You can change this:

```python
@pytest.fixture(scope="module")    # once per file (shared across all tests in file)
def expensive_connection():
    conn = create_connection()
    yield conn
    conn.close()

@pytest.fixture(scope="session")   # once per entire test run
def shared_engine():
    return PandasEngine(config={})
```

| Scope | When it runs | When to use |
|---|---|---|
| `"function"` (default) | Before each test | Most tests — clean state |
| `"module"` | Once per file | Expensive setup, read-only |
| `"session"` | Once per run | Very expensive, truly shared |

### The `yield` Pattern — Setup AND Teardown

```python
# From odibi/testing/fixtures.py
@contextmanager
def temp_directory():
    temp_dir = tempfile.mkdtemp()     # SETUP
    try:
        yield temp_dir                # test runs here
    finally:
        shutil.rmtree(temp_dir)       # TEARDOWN (always runs)
```

In pytest fixtures:

```python
@pytest.fixture
def temp_catalog(tmp_path):
    cat = CatalogManager(
        spark=None,
        config=SystemConfig(connection="local", path="_odibi_system"),
        base_path=str(tmp_path / "_odibi_system"),
        engine=PandasEngine(config={}),
    )
    yield cat              # test runs here
    # cleanup happens automatically (tmp_path is cleaned by pytest)
```

### Built-in Fixtures You Should Know

| Fixture | What it gives you | Example |
|---|---|---|
| `tmp_path` | Unique temp directory (Path object) | `str(tmp_path / "data.csv")` |
| `tmp_path_factory` | Multiple temp dirs | `tmp_path_factory.mktemp("data")` |
| `monkeypatch` | Safely override things | `monkeypatch.setattr(module, "func", mock)` |
| `capsys` | Capture stdout/stderr | `out = capsys.readouterr().out` |

> ⚠️ **Odibi rule: Never use `caplog`.** It pollutes the structured logging singleton and causes batch failures. Assert on return values instead. (See Trap T-001.)

---

## 6. Markers and Skip Guards

Markers let you **tag** tests and control which ones run.

### `@pytest.mark.skipif` — Conditional Skipping

This is Odibi's most important pattern. The framework runs on 3 environments (Windows local, CI, Databricks), and not all dependencies are available everywhere.

```python
# Pattern: try/except → flag → skipif decorator
try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    pl = None
    HAS_POLARS = False

requires_polars = pytest.mark.skipif(not HAS_POLARS, reason="Polars not installed")

@requires_polars
def test_polars_validation():
    df = pl.DataFrame({"a": [1, 2, 3]})
    ...
```

**What happens:**
- If Polars is installed: test runs normally
- If Polars is missing: test shows as **SKIPPED** (not FAILED) in output

### `pytest.importorskip` — Module-Level Skip

When the **entire file** needs a dependency:

```python
# At the top of the file — skips ALL tests if polars is missing
pl = pytest.importorskip("polars")
```

This is the first line in `test_validation_engine.py`:

```python
pl = pytest.importorskip("polars")
```

### Inline `importorskip` — One-Off Tests

```python
def test_needs_duckdb():
    duckdb = pytest.importorskip("duckdb")   # skip just this test
    conn = duckdb.connect()
    ...
```

### Standard Skip Guards (Copy-Paste These)

Odibi defines these patterns — reuse them:

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

# --- DuckDB ---
try:
    import duckdb
    _has_duckdb = True
except ImportError:
    _has_duckdb = False
requires_duckdb = pytest.mark.skipif(not _has_duckdb, reason="duckdb not installed")

# --- CoolProp ---
try:
    import CoolProp
    _has_coolprop = True
except ImportError:
    _has_coolprop = False
requires_coolprop = pytest.mark.skipif(not _has_coolprop, reason="CoolProp not installed")
```

### `@pytest.mark.skip` — Always Skip

```python
@pytest.mark.skip(reason="Bug #248 — fix pending")
def test_broken_feature():
    ...
```

### Custom Markers

You can register custom markers in `pytest.ini` or `pyproject.toml`:

```ini
# pyproject.toml
[tool.pytest.ini_options]
markers = [
    "slow: marks tests as slow (deselect with '-m \"not slow\"')",
    "spark: requires Spark JVM",
]
```

Then use:

```python
@pytest.mark.slow
def test_full_pipeline():
    ...
```

```bash
# Run only fast tests
pytest -m "not slow"
```

---

## 7. Parametrize — One Test, Many Inputs

`@pytest.mark.parametrize` runs the same test with different inputs. This eliminates copy-paste test variants.

### Basic Parametrize

```python
@pytest.mark.parametrize("input_val, expected", [
    ([1, 2, 3], []),          # all valid → no errors
    ([1, None, 3], 1),        # one null → one error
    ([None, None], 2),        # two nulls → two errors in multi-col
])
def test_not_null_variants(validator, input_val, expected):
    df = pd.DataFrame({"a": input_val})
    cfg = ValidationConfig(tests=[NotNullTest(columns=["a"])])
    errors = validator.validate(df, cfg)
    if isinstance(expected, list):
        assert errors == expected
    else:
        assert len(errors) == expected
```

### Real Odibi Example — Multiple Engines

```python
@pytest.mark.parametrize("engine_type, make_df", [
    ("pandas", lambda data: pd.DataFrame(data)),
    ("polars", lambda data: pl.DataFrame(data)),
])
def test_not_null_cross_engine(validator, engine_type, make_df):
    df = make_df({"a": [1, None, 3]})
    cfg = ValidationConfig(tests=[NotNullTest(columns=["a"])])
    errors = validator.validate(df, cfg)
    assert len(errors) == 1
```

### When to Parametrize vs Separate Tests

| Use Parametrize | Use Separate Tests |
|---|---|
| Same logic, different inputs | Different logic per case |
| Input/output matrix (e.g., engine × test type) | Complex setup differs per case |
| Edge cases of the same function | Different functions being tested |

---

## 8. Mocking — Testing Without Real Dependencies

**Mocking** replaces a real object with a fake one that you control. This lets you test code that depends on databases, APIs, or Spark — without having those running.

### Why Mock?

Odibi's `CatalogManager` needs Spark or Delta Lake. In CI (no JVM), you can't create a real Spark session. Solution: **mock it**.

### `MagicMock` — The Swiss Army Knife

```python
from unittest.mock import MagicMock

# Create a fake object
mock_engine = MagicMock()

# It accepts ANY attribute or method call
mock_engine.read("table")          # doesn't crash
mock_engine.write(df, "path")      # doesn't crash
mock_engine.anything.you.want()    # doesn't crash
```

### Real Odibi Example — Mock NodeConfig

```python
# From test_dimension_coverage.py
from unittest.mock import MagicMock

# Instead of building a real NodeConfig (which requires valid YAML params),
# create a mock that looks like one:
cfg = MagicMock(spec=NodeConfig)
cfg.name = "test_dim"
cfg.params = {"natural_key": "id", "surrogate_key": "sk", "scd_type": 1}

pattern = DimensionPattern(engine=engine, config=cfg)
pattern.validate()  # works! It only reads cfg.params, not the full Pydantic model
```

### `patch` — Temporarily Replace Something

```python
from unittest.mock import patch

def test_catalog_bootstrap(catalog, tmp_path):
    with patch.object(catalog, "_write_delta") as mock_write:
        catalog.bootstrap()
        mock_write.assert_called_once()   # verify it tried to write
```

### `patch` as a Decorator

```python
@patch("odibi.connections.factory.create_sql_server_connection")
def test_state_backend(mock_create):
    mock_create.return_value = MagicMock()
    backend = create_state_backend("sql_server", connection_string="...")
    mock_create.assert_called_once()
```

### Simulating Missing Libraries (The Safe Way)

```python
import sys

def test_handles_missing_fsspec():
    original = sys.modules.get("fsspec")
    sys.modules["fsspec"] = None        # makes `import fsspec` raise ImportError
    try:
        result = function_that_imports_fsspec()
        assert result.used_fallback is True
    finally:
        if original is not None:
            sys.modules["fsspec"] = original
        else:
            sys.modules.pop("fsspec", None)
```

> ⚠️ **Odibi Trap T-008:** Never use `builtins.__import__` patching — it catches Rich, logging, and coverage internal imports. Always use `sys.modules` manipulation.

### Mock Chaining (for Spark)

```python
# Mock a Spark read chain: spark.read.format("delta").load(path).filter(...)
mock_spark = MagicMock()
mock_df = MagicMock()
mock_spark.read.format.return_value.load.return_value = mock_df
mock_df.filter.return_value = mock_df
mock_df.toPandas.return_value = pd.DataFrame({"id": [1, 2]})

# Now any code that does spark.read.format("delta").load(path) gets mock_df
```

### MagicMock Gotchas

#### Dunder Methods Don't Work on Instances (Trap T-007)

```python
# ❌ Python resolves dunders on the TYPE, not the instance
mock_obj = MagicMock()
mock_obj.__getitem__ = lambda self, key: key  # IGNORED!

# ✅ Use plain tuples/dicts instead
mock_stats = (100.0,)  # for code that does stats[0]
```

#### `spec=` Restricts What a Mock Accepts

```python
# Without spec: mock accepts anything
mock = MagicMock()
mock.nonexistent_method()  # no error — silent

# With spec: mock only accepts real methods
mock = MagicMock(spec=PandasEngine)
mock.nonexistent_method()  # AttributeError! catches typos
```

### `assert_called` Family

```python
mock_func = MagicMock()
mock_func("hello", count=3)

mock_func.assert_called_once()                         # was it called exactly once?
mock_func.assert_called_with("hello", count=3)         # with these exact args?
mock_func.assert_called_once_with("hello", count=3)    # both combined

# Check it was NOT called
mock_func.assert_not_called()   # fails here because we called it
```

---

## 9. Testing for Errors (Negative Tests)

**Negative tests** verify that code fails correctly — the right exception, the right message.

### `pytest.raises` — Expect an Exception

```python
def test_missing_natural_key(engine, cfg):
    cfg.params = {"surrogate_key": "sk"}                # missing natural_key
    with pytest.raises(ValueError, match="natural_key"):
        _pattern(engine, cfg).validate()
```

**What this does:**
1. Runs the code inside the `with` block
2. Asserts that `ValueError` is raised
3. Asserts that the error message matches the regex `"natural_key"`
4. If no exception is raised → **test FAILS**

### Multiple Exception Types

```python
def test_invalid_params():
    with pytest.raises((ValueError, TypeError)):
        TargetParams()  # might raise either
```

### Checking the Exception Object

```python
def test_gate_failure_details():
    with pytest.raises(GateFailedError) as exc_info:
        evaluate_gate(results, config)
    
    assert exc_info.value.pass_rate < 0.95
    assert exc_info.value.required_rate == 0.95
```

### Always Include At Least One Negative Test

For every feature, ask: **"What should happen when the input is wrong?"**

```python
# Happy path
def test_dimension_scd1_update():
    ...  # normal data → correct result

# Negative: missing required param
def test_dimension_missing_natural_key():
    with pytest.raises(ValueError, match="natural_key"):
        ...

# Edge case: empty input
def test_dimension_empty_dataframe():
    df = pd.DataFrame({"id": [], "name": []})
    result = pattern.execute(context)
    assert result.df.empty
```

---

## 10. conftest.py — Shared Infrastructure

`conftest.py` is a special file that pytest loads automatically. It provides:
- Fixtures shared across all tests in that directory (and subdirectories)
- Hooks that modify test collection and execution
- No need to import it — pytest finds it automatically

### Odibi's conftest.py — What It Does

```python
# tests/conftest.py — let's break it down

# 1. SKIP SPARK ON WINDOWS
# On Windows, there's no Spark JVM. Instead of letting tests crash,
# conftest replaces SparkEngine.__init__ with pytest.skip().
if sys.platform == "win32":
    try:
        from odibi.engine import spark_engine
        spark_engine.SparkEngine.__init__ = lambda self, *args, **kwargs: pytest.skip(
            "Skipping Spark tests on Windows due to missing winutils"
        )
    except Exception:
        pass

# 2. RESET LOGGING SINGLETON
# The logging context is a global singleton that accumulates state.
# Without this reset, tests pass individually but fail in batch.
@pytest.fixture(autouse=True)
def _reset_logging_context():
    import odibi.utils.logging_context as lc
    original = lc._global_context
    lc._global_context = None
    yield
    lc._global_context = original

# 3. SKIP FILES WITH "spark" OR "delta" IN THE NAME
def pytest_ignore_collect(collection_path, config):
    if sys.platform == "win32" and _is_spark_or_delta_name(collection_path.name):
        return True
```

### Key Concepts in conftest.py

#### `autouse=True` — Runs For Every Test Automatically

```python
@pytest.fixture(autouse=True)
def _reset_logging_context():
    ...
```

You don't need to include it in test function signatures — it runs automatically for every test in the directory.

#### `pytest_ignore_collect` — Skip Files During Collection

This is a **pytest hook** — a function with a special name that pytest calls at specific points:

```python
def pytest_ignore_collect(collection_path, config):
    # Return True to skip this file entirely
    if sys.platform == "win32" and "spark" in collection_path.name.lower():
        return True
```

This is why you can't put "spark" or "delta" in test filenames on Windows.

#### `pytest_collection_modifyitems` — Filter Tests After Collection

```python
def pytest_collection_modifyitems(config, items):
    if sys.platform == "win32":
        items[:] = [item for item in items if not _is_spark_or_delta_name(item.fspath.basename)]
```

### conftest.py Hierarchy

You can have conftest.py at multiple levels:

```
tests/
├── conftest.py                      # shared by ALL tests
├── unit/
│   ├── conftest.py                  # shared by all unit tests
│   └── validation/
│       ├── conftest.py              # shared by validation tests only
│       └── test_validation_engine.py
```

Fixtures defined in a parent conftest.py are available to all tests below it.

---

## 11. Test Organization — How to Structure Files

### Mirror the Source Structure

```
odibi/transformers/sql_core.py     → tests/unit/transformers/test_sql_core_coverage.py
odibi/patterns/dimension.py       → tests/unit/patterns/test_dimension_coverage.py
odibi/validation/engine.py        → tests/unit/validation/test_validation_engine.py
odibi/connections/postgres.py      → tests/unit/connections/test_postgres_coverage.py
```

### Within a File — Group by Behavior

```python
# ── Happy Path ──────────────────────────────────────────────────

def test_filter_rows_basic():
    ...

def test_filter_rows_multiple_conditions():
    ...

# ── Edge Cases ──────────────────────────────────────────────────

def test_filter_rows_empty_dataframe():
    ...

def test_filter_rows_single_row():
    ...

def test_filter_rows_null_values():
    ...

# ── Error Cases ─────────────────────────────────────────────────

def test_filter_rows_invalid_column():
    ...

def test_filter_rows_bad_sql_syntax():
    ...

# ── Polars Parity ───────────────────────────────────────────────

@requires_polars
def test_polars_filter_rows_basic():
    ...
```

### Using Classes for Grouping

```python
class TestExtractDateParts:
    @pytest.fixture
    def date_df(self):
        return pd.DataFrame(
            {"created_at": pd.to_datetime(["2024-03-15 10:30:00"])}
        )
    
    def test_all_parts_default_prefix(self, date_df):
        ...
    
    def test_custom_prefix(self, date_df):
        ...

class TestNormalizeSchema:
    def test_lowercase_columns(self):
        ...
```

**Why classes?** They group related tests AND can have their own fixtures (like `date_df` above).

### The Complete Test File Template

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
    assert result.df["value"].tolist() == [10, 20, 30]


# ---------------------------------------------------------------------------
# Edge Cases
# ---------------------------------------------------------------------------

def test_empty_dataframe():
    df = pd.DataFrame({"id": [], "value": []})
    ctx = make_context(df)
    result = TargetFunction(ctx, TargetParams(column="value"))
    assert result.df.empty


def test_null_values():
    df = pd.DataFrame({"id": [1, 2], "value": [None, 30]})
    ctx = make_context(df)
    result = TargetFunction(ctx, TargetParams(column="value"))
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
    ...
```

---

## 12. Coverage — Measuring What's Tested

Coverage tells you which lines of your code are exercised by tests. Odibi targets 80%.

### ⚠️ Odibi Rule: Never Use `pytest --cov`

```bash
# ❌ BREAKS — Rich logging causes import order issues
pytest --cov=odibi.validation.engine tests/unit/validation/

# ✅ WORKS — same data, no issues
python -m coverage run --source=odibi.validation.engine -m pytest tests/unit/validation/test_validation_engine.py -q --tb=no
python -m coverage report --show-missing
```

### Reading Coverage Output

```
Name                              Stmts   Miss  Cover   Missing
---------------------------------------------------------------
odibi/validation/engine.py         412    136    67%    366-370, 380-385, ...
```

| Column | What it means |
|--------|---------------|
| **Stmts** | Total executable statements |
| **Miss** | Statements never executed during tests |
| **Cover** | Percentage of statements executed |
| **Missing** | Line numbers that weren't hit |

### How to Use Missing Lines

The `Missing` column tells you exactly where to focus:

```
Missing: 366-370, 380-385, 450-570
```

1. Open `odibi/validation/engine.py` at line 366
2. You'll see: it's a Spark-specific path (`_validate_spark`)
3. Decision: Skip it (tested in Databricks) or mock Spark and test it

### Batched Coverage (Odibi's Approach)

The full test suite hangs if run at once. Run in batches:

```powershell
# Batch 1 — main suite
python -m coverage run --source=odibi -m pytest tests/unit/ -q --tb=no `
    --ignore=tests/unit/engine/test_pandas_engine_core.py `
    --ignore=tests/unit/connections/test_azure_adls_coverage.py `
    --ignore=tests/unit/test_simulation*.py

# Batch 2 — isolated (use -a to APPEND coverage data)
python -m coverage run -a --source=odibi -m pytest tests/unit/engine/test_pandas_engine_core.py -q --tb=no

# Final report
python -m coverage report --show-missing
```

The `-a` flag **appends** to the existing `.coverage` data file instead of overwriting it.

### What Coverage Numbers Mean

| Coverage | What it tells you |
|---|---|
| **100%** | Every statement executed — but NOT every scenario tested |
| **80%** | Good target — remaining 20% is usually error paths, platform branches |
| **50%** | Major features untested — risky to refactor |
| **<30%** | Tests exist but barely exercise the code |

### Coverage Doesn't Mean Correctness

```python
def add(a, b):
    return a + b

def test_add():
    add(1, 2)  # 100% coverage! But... no assertion. Useless test.
```

**Coverage measures execution, not verification.** You need both coverage AND good assertions.

---

## 13. The Odibi Traps — Lessons That Cost Real Time

These are hard-won lessons from `docs/LESSONS_LEARNED.md`. Each one was discovered by hitting a wall and spending time (= money) to figure it out. **Read these before writing any test.**

### T-001: caplog Does Not Capture Structured Logging

**What looks right:**
```python
def test_something(caplog):
    with caplog.at_level(logging.WARNING):
        do_thing()
    assert "expected" in caplog.text
```

**What happens:** Tests pass individually but fail in batch. The `get_logging_context()` singleton accumulates state. Rich's highlighter receives a `Text` object instead of `str`, causing a blocking `TypeError`.

**The fix:** Never use `caplog`. Assert on return values only.

```python
def test_something():
    result = do_thing()
    assert result.status == "warning"
```

### T-002: Mock PySpark Order Matters

**What looks right:**
```python
# Mock pyspark first, then import odibi
sys.modules["pyspark"] = MagicMock()
from odibi.catalog import CatalogManager  # too late!
```

**What happens:** `catalog.py` has a try/except that imports pyspark. If real pyspark is installed, the try succeeds. But the fallback type stubs never get defined because the except block is skipped.

**The fix:** Import odibi FIRST, then install mocks:

```python
# Step 1: Import odibi (triggers real pyspark import + fallback stubs)
import odibi.catalog

# Step 2: THEN override with mocks
mock_pyspark = MagicMock()
sys.modules["pyspark"] = mock_pyspark
```

### T-003: "spark" or "delta" in Test Filenames → Skipped

**What looks right:** Naming a file `test_catalog_spark_reads.py`

**What happens:** `conftest.py` checks filenames and skips anything with "spark" or "delta" on Windows. Your tests silently never run.

**The fix:** Use names like `test_catalog_mock_engine_reads.py`.

### T-008: builtins.__import__ Patching Is Dangerous

**What looks right:**
```python
@patch("builtins.__import__", side_effect=ImportError)
def test_missing_library(mock_import):
    ...
```

**What happens:** This catches ALL imports, including Rich, logging, and coverage internals. Random `ImportError` explosions.

**The fix:** Use `sys.modules` manipulation:

```python
sys.modules["fsspec"] = None  # simulates ImportError for just this module
```

### T-023: coverage --source Triggers NumPy Double-Import

**What looks right:**
```bash
python -m coverage run --source=odibi.transformers.delete_detection -m pytest ...
```

**What happens:** On Databricks, `--source` activates import hooks that cause NumPy to double-import, breaking `pandas.merge(..., indicator=True)`.

**The fix:** Use `--include` instead:
```bash
python -m coverage run --include=odibi/transformers/delete_detection.py -m pytest ...
```

### T-026: TestConfig Is a Union Type

**What looks right:**
```python
cfg = TestConfig(type=TestType.NOT_NULL, column="amount")
```

**What happens:** `TypeError: Cannot instantiate typing.Union`

**The fix:** Use the specific classes:
```python
cfg = NotNullTest(columns=["amount"])
cfg = RangeTest(column="amount", min=0)
```

### T-028: Gate Pass Rate Depends on Unique Row Failures

**What looks right:** Creating validation results where all failures are at the same row positions.

**What happens:** `evaluate_gate` counts unique failing rows across all tests. Overlapping failures = fewer unique failures = higher pass rate than expected.

**The fix:** Spread failures across non-overlapping positions.

---

## 14. Patterns Cookbook — Real Recipes from Odibi Tests

### Recipe 1: Testing a Transformer

```python
def setup_context(df):
    """Standard helper — creates a Pandas EngineContext."""
    pandas_ctx = PandasContext()
    pandas_ctx.register("df", df.copy())
    engine = PandasEngine()
    return EngineContext(
        pandas_ctx, df.copy(), EngineType.PANDAS,
        sql_executor=engine.execute_sql, engine=engine,
    )

def test_filter_rows_basic():
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    ctx = setup_context(df)
    params = FilterRowsParams(condition="id > 1")
    result = filter_rows(ctx, params)
    assert result.df["id"].tolist() == [2, 3]
    assert len(result.df) == 2
```

### Recipe 2: Testing a Pattern with MagicMock Config

```python
def test_dimension_scd1_update():
    engine = MagicMock()
    engine.connections = {}
    cfg = MagicMock(spec=NodeConfig)
    cfg.name = "dim_customer"
    cfg.params = {
        "natural_key": "id",
        "surrogate_key": "customer_sk",
        "scd_type": 1,
        "track_cols": ["name", "email"],
    }
    
    pattern = DimensionPattern(engine=engine, config=cfg)
    pattern.validate()  # should not raise
    
    # Test with real data
    source_df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    context = _make_context(source_df, engine)
    result = pattern.execute(context)
    assert len(result) == 2
```

### Recipe 3: Testing Validation with Multiple Test Types

```python
def test_combined_validation(validator):
    df = pd.DataFrame({
        "id": [1, 1, 3],               # not unique
        "amount": [-5, 10, 20],         # below range
        "status": ["A", "X", "B"],      # invalid value
    })
    cfg = ValidationConfig(tests=[
        UniqueTest(columns=["id"]),
        RangeTest(column="amount", min=0),
        AcceptedValuesTest(column="status", values=["A", "B"]),
    ])
    errors = validator.validate(df, cfg)
    assert len(errors) == 3  # one error per failing test
```

### Recipe 4: DuckDB SQL Executor for Patterns

```python
import duckdb

def sql_executor(query, context):
    """DuckDB-based SQL executor for testing patterns that use context.sql()."""
    conn = duckdb.connect()
    for name in context.list_names():
        conn.register(name, context.get(name))
    return conn.execute(query).fetchdf()

def test_aggregation_pandas():
    df = pd.DataFrame({"region": ["A", "A", "B"], "sales": [10, 20, 30]})
    ctx = PandasContext()
    ctx.register("df", df)
    engine_ctx = EngineContext(
        context=ctx, df=df, engine_type=EngineType.PANDAS,
        sql_executor=sql_executor, engine=PandasEngine(config={}),
    )
    # ... test aggregation pattern
```

### Recipe 5: Testing with `monkeypatch`

```python
def test_cli_command(monkeypatch, capsys):
    """Test a CLI command by monkeypatching sys.argv."""
    monkeypatch.setattr("sys.argv", ["odibi", "list", "transformers"])
    
    from odibi.cli.main import main
    main()
    
    captured = capsys.readouterr()
    assert "filter_rows" in captured.out
```

### Recipe 6: Testing File I/O with `tmp_path`

```python
def test_write_and_read(tmp_path):
    path = tmp_path / "test.csv"
    
    # Write
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    df.to_csv(path, index=False)
    
    # Read
    result = pd.read_csv(path)
    assert len(result) == 2
    assert list(result.columns) == ["id", "name"]
```

---

## 15. The "Break It" Methodology

From your simulation notebooks — apply the same thinking to testing:

### The Question

For every test, ask: **"What is the gap between what this test checks and what could go wrong in production?"**

### Example: Testing a SCD2 Merge

**What the test checks:**
```python
def test_scd2_basic():
    result = scd2_merge(source, target, keys=["id"])
    assert len(result) == 5
```

**What could go wrong in production:**
- What if `id` has NaN values? (Trap T-009 — NaN != NaN in IEEE 754)
- What if source and target have different column orders?
- What if there are duplicate keys in the source?
- What if the target table is empty (first run)?

**Better tests (covering the gaps):**
```python
def test_scd2_nan_in_key():
    source = pd.DataFrame({"id": [1, float("nan")], "name": ["A", "B"]})
    target = pd.DataFrame({"id": [1, float("nan")], "name": ["A", "B"]})
    result = scd2_merge(source, target, keys=["id"])
    assert len(result) == 2  # NaN should match NaN, not create new versions

def test_scd2_first_run_empty_target():
    source = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
    target = pd.DataFrame(columns=["id", "name"])  # empty
    result = scd2_merge(source, target, keys=["id"])
    assert len(result) == 2  # all rows inserted
```

### The Break-It Checklist

For any function, ask:

1. **Empty input** — what happens with no rows?
2. **Single row** — boundary condition
3. **Nulls** — in keys, in values, all nulls
4. **Duplicates** — duplicate keys, duplicate rows
5. **Type mismatches** — string "123" vs int 123
6. **Large data** — does it scale? (usually a benchmark, not a unit test)
7. **Concurrency** — what if two processes call this? (relevant for catalog/state)
8. **Missing dependency** — what if Polars/DuckDB/fsspec isn't installed?

---

## 16. Exercises

### Exercise 1: Read and Understand (15 min)

Open `tests/unit/validation/test_validation_engine.py`. Answer:
1. How many test functions are there?
2. What does the `validator` fixture do?
3. Why does line 6 use `pytest.importorskip("polars")` instead of `import polars`?
4. Find one positive test and one negative test. What makes them different?

### Exercise 2: Write Your First Test (20 min)

Create a new file `tests/unit/test_my_first_test.py`:

```python
"""My first pytest tests — learning the basics."""

import pandas as pd
import pytest


def test_dataframe_creation():
    """Test that I can create a DataFrame and check its shape."""
    df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
    assert len(df) == 2
    assert list(df.columns) == ["name", "age"]


def test_dataframe_filter():
    """Test filtering a DataFrame."""
    df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "value": [10, 20, 30, 40, 50]})
    filtered = df[df["value"] > 25]
    assert len(filtered) == 3
    assert filtered["id"].tolist() == [3, 4, 5]


def test_empty_dataframe():
    """Test edge case — empty DataFrame."""
    df = pd.DataFrame({"id": [], "value": []})
    assert df.empty
    assert len(df) == 0
```

Run it:
```bash
pytest tests/unit/test_my_first_test.py -v
```

### Exercise 3: Add a Fixture (15 min)

Modify your test file to use a fixture:

```python
@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "score": [85, 92, 78, 95, 88],
    })

def test_row_count(sample_df):
    assert len(sample_df) == 5

def test_high_scores(sample_df):
    high = sample_df[sample_df["score"] > 90]
    assert high["name"].tolist() == ["Bob", "Diana"]
```

### Exercise 4: Test a Real Odibi Function (30 min)

Pick ONE of these simple transformers and write 3 tests for it:

1. **`limit`** — Limits rows. Test: correct count, limit > rows, limit = 0
2. **`distinct`** — Removes duplicates. Test: with dupes, without dupes, empty df
3. **`sort`** — Sorts rows. Test: ascending, descending, by multiple columns

Use the `setup_context` helper pattern from Recipe 1.

### Exercise 5: Write a Negative Test (15 min)

Pick a Pydantic model from `odibi/config.py` (e.g., `RangeTest`, `AcceptedValuesTest`).
Write a test that creates it with invalid parameters and uses `pytest.raises`:

```python
def test_range_test_missing_column():
    with pytest.raises(Exception):
        RangeTest()  # missing required 'column' field
```

### Exercise 6: Mock an Object (30 min)

Create a test that uses `MagicMock` to test a pattern's `validate()` method without needing real config:

```python
from unittest.mock import MagicMock
from odibi.patterns.aggregation import AggregationPattern

def test_aggregation_validate_missing_grain():
    engine = MagicMock()
    cfg = MagicMock()
    cfg.name = "test_agg"
    cfg.params = {"measures": [{"column": "amount", "function": "sum"}]}  # no grain!
    
    pattern = AggregationPattern(engine=engine, config=cfg)
    with pytest.raises(ValueError, match="grain"):
        pattern.validate()
```

### Exercise 7: Coverage (20 min)

Pick any test file and measure its coverage:

```bash
python -m coverage run --source=odibi.validation.engine -m pytest tests/unit/validation/test_validation_engine.py -q --tb=no
python -m coverage report --show-missing
```

Look at the `Missing` lines. Open the source file at those line numbers. Can you see why they're not covered? (Usually Spark paths or edge cases.)

---

## 17. Quick Reference Card

### Running Tests

```bash
pytest path/to/test.py -v                  # one file, verbose
pytest path/to/test.py::test_name -v       # one test
pytest path/to/test.py -k "not_null" -v    # keyword filter
pytest path/to/test.py -x                  # stop on first failure
pytest path/to/test.py --lf                # rerun last failures
```

### Coverage (Odibi Way)

```bash
python -m coverage run --source=odibi.module -m pytest tests/unit/path.py -q --tb=no
python -m coverage report --show-missing
```

### Common Assertions

```python
assert x == y                           # equality
assert x != y                           # inequality
assert x in collection                  # membership
assert "text" in string                 # substring
assert len(items) == 3                  # length
assert df.empty                         # empty DataFrame
assert df["col"].isna().sum() == 2      # null count
assert df["col"].tolist() == [1, 2, 3]  # exact values
```

### Exception Testing

```python
with pytest.raises(ValueError, match="message"):
    function_that_should_fail()
```

### Skip Guards

```python
# Module-level (skips entire file)
pl = pytest.importorskip("polars")

# Per-test
@pytest.mark.skipif(not HAS_POLARS, reason="Polars not installed")
def test_polars_feature():
    ...
```

### Fixture Pattern

```python
@pytest.fixture
def thing():
    obj = create_thing()    # setup
    yield obj               # test runs
    obj.cleanup()           # teardown
```

### Mock Pattern

```python
from unittest.mock import MagicMock, patch

mock = MagicMock()
mock.method.return_value = "result"

with patch("module.function") as mock_fn:
    mock_fn.return_value = "mocked"
```

### Odibi-Specific Don'ts

| ❌ Don't | ✅ Do Instead |
|----------|--------------|
| `pytest --cov` | `python -m coverage run` |
| `caplog` | Assert on return values |
| `"spark"` in filename | `"mock_engine"` or `"coverage"` |
| `builtins.__import__` | `sys.modules["pkg"] = None` |
| `assert result is not None` | Assert concrete values |
| Run full `tests/unit/` at once | Run in batches |

---

## Glossary

| Term | Definition |
|------|-----------|
| **Fixture** | A function decorated with `@pytest.fixture` that provides setup for tests |
| **Marker** | A tag on a test (e.g., `@pytest.mark.skipif`) that controls execution |
| **Parametrize** | Running the same test with multiple input sets |
| **Mock** | A fake object that replaces a real dependency |
| **MagicMock** | A mock that automatically handles any attribute access or method call |
| **Coverage** | Percentage of code lines executed during tests |
| **conftest.py** | Auto-loaded pytest configuration file for shared fixtures and hooks |
| **Assertion** | A statement that must be True or the test fails |
| **Negative test** | A test that verifies code fails correctly (right error, right message) |
| **Skip guard** | A pattern that skips tests when a dependency is missing |
| **Monkeypatch** | Safely override attributes/environment variables during a test |
| **AAA** | Arrange-Act-Assert — the standard test structure |
