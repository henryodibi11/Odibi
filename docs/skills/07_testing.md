# Skill 07 — Testing

> **Layer:** Quality
> **When:** Writing tests for any odibi module.
> **Critical:** Read this before writing your first test. These rules prevent failures that cost real time and money.

---

## Purpose

Odibi's test suite has hard-won constraints due to PySpark's JVM dependency, Rich logging singletons, and coverage tooling quirks. This skill encodes every rule and pattern you need.

---

## Golden Rules

### 1. Never Run the Full Suite at Once
```bash
# ❌ WILL HANG — deadlocks from ThreadPoolExecutor + coverage + mock contamination
pytest tests/unit/ -v

# ✅ Run in batches
pytest tests/unit/transformers/ -v
pytest tests/unit/validation/ -v
pytest tests/unit/connections/ -v
```

Use `scripts/run_coverage.ps1` for full coverage measurement — it runs safe batches automatically.

### 2. Never Use `pytest --cov`
```bash
# ❌ Breaks Rich logging — tests FAIL
pytest --cov=odibi.validation.engine tests/unit/validation/

# ✅ Use coverage run instead
python -m coverage run --source=odibi.validation.engine -m pytest tests/unit/validation/test_validation_engine.py -q --tb=no
python -m coverage report --show-missing
```

### 3. Never Use caplog
```python
# ❌ Logging context pollution causes batch failure
def test_something(caplog):
    with caplog.at_level(logging.WARNING):
        do_thing()
    assert "expected" in caplog.text

# ✅ Assert on return values and behavior
def test_something():
    result = do_thing()
    assert result.status == "warning"
```

### 4. Never Put "spark" or "delta" in Test File Names
```
# ❌ conftest.py skip filter catches these on Windows
test_catalog_spark_reads.py
test_delta_operations.py

# ✅ Use descriptive names without trigger words
test_catalog_mock_engine_reads.py
test_catalog_sync_coverage.py
```

### 5. Always Assert on Return Values, Not Logs
```python
# ❌ Fragile — log format changes break tests
assert "completed" in caplog.text

# ✅ Stable — behavior is the contract
assert result.rows_processed == 100
assert result.status == "success"
```

---

## Test File Location

Tests mirror the source structure:
```
odibi/patterns/dimension.py     → tests/unit/patterns/test_dimension_coverage.py
odibi/validation/engine.py      → tests/unit/validation/test_validation_engine.py
odibi/connections/postgres.py   → tests/unit/connections/test_postgres_coverage.py
odibi/transformers/sql_core.py  → tests/unit/transformers/test_sql_core_coverage.py
odibi/cli/main.py               → tests/unit/cli/test_cli_main_coverage.py
odibi/story/generator.py        → tests/unit/story/test_story_generator_coverage.py
```

---

## Standard Fixtures

### Pandas Engine
```python
from odibi.engine.pandas_engine import PandasEngine

@pytest.fixture
def engine():
    return PandasEngine(config={})
```

### Catalog Manager
```python
from odibi.catalog import CatalogManager
from odibi.config import SystemConfig
from odibi.engine.pandas_engine import PandasEngine

@pytest.fixture
def catalog(tmp_path):
    return CatalogManager(
        spark=None,
        config=SystemConfig(connection="local", path="_odibi_system"),
        base_path=str(tmp_path / "_odibi_system"),
        engine=PandasEngine(config={}),
    )
```

### Mock NodeConfig (for Patterns)
```python
from unittest.mock import MagicMock

@pytest.fixture
def make_config():
    def _make(name="test_node", params=None):
        cfg = MagicMock()
        cfg.name = name
        cfg.params = params or {}
        return cfg
    return _make
```

### DuckDB SQL Executor (for Aggregation Pattern)
```python
import duckdb

def sql_executor(query, context):
    conn = duckdb.connect()
    for name in context.list_names():
        conn.register(name, context.get(name))
    return conn.execute(query).fetchdf()
```

### Delta Table Seeding
```python
import pyarrow as pa
from deltalake import write_deltalake, DeltaTable

def seed_delta(path, data_dict, schema=None):
    table = pa.table(data_dict, schema=schema)  # Use pa.table, not pd.DataFrame
    write_deltalake(str(path), table, mode="overwrite")

def read_delta(path):
    return DeltaTable(str(path)).to_pandas()
```

**⚠️ Always use `pa.table()` with explicit types for Delta seeding.** Pandas DataFrames with None values create Null-type columns that Delta rejects.

---

## Mock PySpark Pattern (Critical Order)

When testing Spark branches without JVM:

```python
import sys
from unittest.mock import MagicMock

# Step 1: Import the odibi module FIRST
import odibi.catalog  # This triggers real pyspark import + fallback stubs

# Step 2: THEN install mock pyspark
mock_pyspark = MagicMock()
mock_sql = MagicMock()
mock_types = MagicMock()

# Step 3: Use real fallback types from odibi.catalog (not MagicMock)
mock_types.StringType = odibi.catalog.StringType
mock_types.StructField = odibi.catalog.StructField
mock_types.StructType = odibi.catalog.StructType

mock_sql.types = mock_types
mock_pyspark.sql = mock_sql

# Step 4: Override sys.modules (NOT setdefault)
sys.modules["pyspark"] = mock_pyspark
sys.modules["pyspark.sql"] = mock_sql
sys.modules["pyspark.sql.types"] = mock_types
```

**Why this order matters:**
- If you mock pyspark BEFORE importing odibi, the fallback type stubs never get defined
- If you use `setdefault()`, the real pyspark (which is installed) won't be overridden
- If you use `MagicMock()` for types, `StructType(fields).fields` returns garbage

---

## Simulating Missing Libraries

```python
import sys

# ✅ Safe pattern
original = sys.modules.get("fsspec")
sys.modules["fsspec"] = None  # Simulates ImportError
try:
    result = function_that_imports_fsspec()
finally:
    if original is not None:
        sys.modules["fsspec"] = original
    else:
        sys.modules.pop("fsspec", None)
```

**❌ Never use `builtins.__import__` patching** — it accidentally catches Rich, logging, and coverage internal imports.

---

## Coverage Measurement

### Single Module
```bash
python -m coverage run --source=odibi.validation.engine -m pytest tests/unit/validation/test_validation_engine.py -q --tb=no
python -m coverage report --show-missing
```

### Full Project (Batched)
```powershell
# Batch 1 — main suite (exclude known hang sources)
python -m coverage run --source=odibi -m pytest tests/unit/ -q --tb=no `
    --ignore=tests/unit/engine/test_pandas_engine_core.py `
    --ignore=tests/unit/connections/test_azure_adls_coverage.py `
    --ignore=tests/unit/test_simulation.py `
    --ignore=tests/unit/test_simulation_random_walk.py `
    --ignore=tests/unit/test_simulation_fixes.py `
    --ignore=tests/unit/test_simulation_daily_profile.py `
    --ignore=tests/unit/test_simulation_coverage_gaps.py `
    --ignore=tests/unit/test_stateful_simulation.py `
    --ignore=tests/unit/tools/test_adf_profiler_coverage.py `
    -k "not test_failure_logs_warning"

# Batch 2-5 — isolated (use -a to append)
python -m coverage run -a --source=odibi -m pytest tests/unit/engine/test_pandas_engine_core.py -q --tb=no
python -m coverage run -a --source=odibi -m pytest tests/unit/connections/test_azure_adls_coverage.py -q --tb=no
python -m coverage run -a --source=odibi -m pytest tests/unit/test_simulation*.py tests/unit/test_stateful_simulation.py -q --tb=no
python -m coverage run -a --source=odibi -m pytest tests/unit/tools/test_adf_profiler_coverage.py -q --tb=no

python -m coverage report --show-missing
```

---

## Known Pre-Existing Failures (Not Regressions)

Do NOT try to fix these — they are known issues:
- **~25 caplog failures** in catalog test files — `logger.warning` goes to stderr
- **~94 failures** in `test_pandas_engine_core.py` when run in batch — logging context pollution
- **~70-93 failures** in `test_adf_profiler_coverage.py` — environment-specific Excel/caching issues

---

## MagicMock Gotchas

### Dunder Methods Don't Work on Instances
```python
# ❌ Python resolves dunders on the TYPE, not instance
mock_obj = MagicMock()
mock_obj.__getitem__ = lambda self, key: key  # Ignored!

# ✅ Use plain tuples/dicts instead
mock_stats = (100.0,)  # For code that does stats[0]
```

### _MockColumn Must Be Complete
If building a mock Spark Column, implement ALL operators:
- `__eq__`, `__ge__`, `__and__`, `__rand__`, `__hash__`, `__invert__`, `desc()`
- Missing `__invert__` causes silent test failures

---

## Polars Tests: UTC Timestamps

```python
from datetime import datetime, timezone, timedelta

# ❌ Polars adds +00:00, making naive timestamps look stale
df = pl.DataFrame({"ts": [datetime.now() - timedelta(hours=1)]})

# ✅ Use UTC-aware timestamps
df = pl.DataFrame({"ts": [datetime.now(timezone.utc) - timedelta(hours=1)]})
```

---

## NodeResult Field Alias Trap

```python
from odibi.node import NodeResult

# ❌ Silently sets result_schema to None
result = NodeResult(result_schema=["a", "b"])

# ✅ Use the alias
result = NodeResult(schema=["a", "b"])
# Read via: result.result_schema
```

---

## Checklist Before Submitting Tests

- [ ] Tests pass individually: `pytest tests/unit/my_test.py -v`
- [ ] No "spark" or "delta" in test file name
- [ ] No caplog usage
- [ ] Assertions on return values, not log output
- [ ] `MagicMock` for NodeConfig in pattern tests
- [ ] `pa.table()` (not `pd.DataFrame`) for Delta seeding
- [ ] Non-None environment values in CatalogStateBackend tests
- [ ] UTC timestamps for Polars freshness tests
- [ ] Coverage check: `python -m coverage run --source=odibi.module -m pytest ... && python -m coverage report`
