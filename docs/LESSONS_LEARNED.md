# Odibi — Lessons Learned System

> **Purpose:** Structured, searchable memory that persists across all agent sessions.
> **Rule:** Every session that discovers something new MUST add an entry here.
> **Read this file at the start of every session.** It prevents repeat mistakes.

---

## How This System Works

Previously, lessons were buried in AGENTS.md's "Testing Gotchas" section — a growing blob that mixed coverage numbers, mock patterns, and design decisions. This file separates **persistent knowledge** into four searchable categories:

| Category | What Goes Here | Example |
|----------|---------------|---------|
| **Decisions** | Why we chose X over Y | "Use DuckDB SQL, not raw Pandas" |
| **Traps** | Things that look right but break | "caplog doesn't capture structured logging" |
| **Patterns** | Working code recipes | "Mock PySpark setup order" |
| **Discoveries** | Behavior we verified empirically | "Polars adds +00:00 to all datetimes" |

AGENTS.md keeps its existing role: **coverage tracker + test infrastructure notes.** This file handles the **why** and **how** that agents need to make correct decisions.

---

## Decisions Log

> Design choices with rationale. Search here before proposing alternatives.

### D-001: SQL-First Transformers
**Date:** 2026-01  
**Context:** Building transformer system for engine parity  
**Decision:** Use `context.sql()` (DuckDB for Pandas, Spark SQL for Spark) as the primary transform mechanism  
**Rationale:** One SQL query works on all engines. Engine-specific code only when SQL cannot express the logic (UDFs, CoolProp, complex window frames)  
**Affected:** All transformers in `odibi/transformers/`

### D-002: Pydantic for Config, Plain Dicts for Pattern Params
**Date:** 2026-01  
**Context:** Pattern validation needs flexible params without strict Pydantic at the pattern layer  
**Decision:** YAML → Pydantic (config.py) → `params: dict` passed to patterns. Patterns validate params themselves in `validate()`  
**Rationale:** NodeConfig validates structure. Pattern.validate() validates semantics. Decoupled.  
**Affected:** All patterns in `odibi/patterns/`

### D-003: No %run on Databricks — lib/ Pattern Only
**Date:** 2026-04  
**Context:** %run hangs and gets stuck in Databricks notebooks  
**Decision:** All new self-contained projects use the validation_e2e lib/ pattern: `lib/` folder with `setup.py` (auto-installs deps, exports API), `config.py`, `__init__.py`. Each notebook does `sys.path.insert(0, PROJECT_ROOT)` then `from lib.setup import *`  
**Rationale:** Eliminates %run dependency chains. Each notebook is self-contained.  
**Affected:** All Databricks notebooks going forward

### D-004: Coverage via `coverage run`, Never `pytest --cov`
**Date:** 2026-04  
**Context:** `pytest --cov` changes import/execution order, triggering Rich `Text` vs `str` mismatch  
**Decision:** Always use `python -m coverage run --source=... -m pytest ...` then `python -m coverage report --show-missing`  
**Rationale:** Same data, no ordering issue  
**Affected:** All coverage measurement

### D-005: Test Batching Required
**Date:** 2026-04  
**Context:** Full `tests/unit/` suite hangs due to ThreadPoolExecutor + coverage tracing + mock contamination + Polars anonymize mask hang  
**Decision:** Always run tests in batches. Use `scripts/run_coverage.ps1` or manual batches.  
**Affected:** All CI and local test runs

### D-006: AGENTS.md as Living Coverage Tracker
**Date:** 2026-04  
**Context:** Need to track per-module coverage across sessions  
**Decision:** AGENTS.md "Current Coverage Status" section is the authoritative record. Every session that adds tests updates it.  
**Rationale:** Single location, searchable, always current  
**Affected:** All test-writing sessions

### D-007: Standalone Functions Over Stateful Classes
**Date:** 2026-01  
**Context:** Framework architecture decision  
**Decision:** Transformers are standalone functions `(context, params) -> context`. Patterns are the only classes (extend Pattern ABC). No classes with mutable state.  
**Rationale:** Simpler to test, compose, and reason about. Patterns need class hierarchy for polymorphism.  
**Affected:** All odibi code

---

## Traps (Known Pitfalls)

> Things that look correct but break. Search here before attempting a new approach.

### T-001: caplog Does Not Capture Structured Logging
**Symptom:** Tests using `caplog` pass individually but fail in batch  
**Root Cause:** `get_logging_context()` singleton accumulates state. Rich highlighter receives `Text` object instead of `str`, triggering blocking `TypeError`  
**Fix:** Never use caplog. Assert on return values and behavior only.  
**Modules:** All modules using `get_logging_context()`

### T-002: Mock PySpark Order Matters
**Symptom:** Spark branch tests fail silently — fallback type stubs undefined  
**Root Cause:** If you mock pyspark BEFORE importing odibi, `catalog.py`'s try/except succeeds (real pyspark exists) but the fallback stubs never get defined  
**Fix:** Always `import odibi.catalog` FIRST, then install mocks in `sys.modules`  
**Modules:** `catalog.py`, any Spark branch testing

### T-003: "spark" or "delta" in Test Filenames → Skipped
**Symptom:** Test file exists but never runs  
**Root Cause:** `conftest.py` skip filter on Windows  
**Fix:** Use names like `test_catalog_mock_engine_reads.py`, not `test_catalog_spark_reads.py`  
**Modules:** All test files

### T-004: Delta Lake Rejects Null-Type Columns
**Symptom:** `SchemaMismatchError: Invalid data type for Delta Lake: Null`  
**Root Cause:** Pandas DataFrames with None values create Null-type columns  
**Fix:** Use `pa.table()` with explicit types for Delta seeding. Always provide non-None `environment` values.  
**Modules:** `state/__init__.py`, any Delta Lake tests

### T-005: NodeResult.result_schema Field Alias
**Symptom:** `result_schema` is silently None despite being passed  
**Root Cause:** Pydantic `Field(alias="schema")` — must construct with `schema=["a","b"]`, not `result_schema=["a","b"]`  
**Fix:** Pass as `schema=[...]` in constructor, read via `.result_schema`  
**Modules:** `node.py`

### T-006: Polars Freshness Tests Need UTC
**Symptom:** "Fresh" data appears stale in Polars tests  
**Root Cause:** Polars adds `+00:00` timezone. Validation compares with `datetime.now(timezone.utc)`. Naive timestamps are 5h off (Central).  
**Fix:** Use `datetime.now(timezone.utc) - timedelta(...)` in test data  
**Modules:** `validation/engine.py`

### T-007: MagicMock Dunder Methods Don't Work on Instances
**Symptom:** `mock_obj.__getitem__` is ignored  
**Root Cause:** Python resolves dunder methods on the TYPE, not the instance  
**Fix:** Use plain tuples/dicts instead. `mock_stats = (100.0,)` for code that does `stats[0]`  
**Modules:** Any mock-heavy tests

### T-008: builtins.__import__ Patching Is Dangerous
**Symptom:** `ImportError` during unrelated code when simulating missing libraries  
**Root Cause:** Patching `builtins.__import__` catches internal imports for Rich, logging, coverage  
**Fix:** Use `sys.modules` manipulation: set `sys.modules["pkg"] = None`, restore in finally  
**Modules:** Any tests simulating missing optional dependencies

### T-009: SCD2 Float/NaN Comparison Unreliable
**Symptom:** SCD2 change detection false-positives on float columns  
**Root Cause:** NaN != NaN in Python. float comparison precision issues.  
**Status:** Open issue #248. Not yet fixed.  
**Modules:** `transformers/scd.py`

### T-010: Polars `repeat_by` + `list.join` Hangs
**Symptom:** Test suite hangs when anonymize "mask" method runs in Polars  
**Root Cause:** Polars expression evaluation deadlock in certain test contexts  
**Fix:** Isolate Polars anonymize tests in separate batch  
**Modules:** `engine/polars_engine.py` anonymize method

---

## Patterns (Working Recipes)

> Copy-paste these. They're battle-tested.

### P-001: Standard CatalogManager Test Fixture
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

### P-002: Mock PySpark for Spark Branch Testing
```python
import sys
from unittest.mock import MagicMock

# Step 1: Import odibi FIRST (triggers real pyspark + fallback stubs)
import odibi.catalog

# Step 2: Build mock hierarchy
mock_pyspark = MagicMock()
mock_sql = MagicMock()
mock_types = MagicMock()

# Step 3: Use real fallback types
mock_types.StringType = odibi.catalog.StringType
mock_types.StructField = odibi.catalog.StructField
mock_types.StructType = odibi.catalog.StructType
mock_sql.types = mock_types
mock_pyspark.sql = mock_sql

# Step 4: Override (NOT setdefault)
sys.modules["pyspark"] = mock_pyspark
sys.modules["pyspark.sql"] = mock_sql
sys.modules["pyspark.sql.types"] = mock_types
```

### P-003: DuckDB SQL Executor for Pattern Tests
```python
import duckdb

def sql_executor(query, context):
    conn = duckdb.connect()
    for name in context.list_names():
        conn.register(name, context.get(name))
    return conn.execute(query).fetchdf()
```

### P-004: Delta Table Seeding (No Null Types)
```python
import pyarrow as pa
from deltalake import write_deltalake, DeltaTable

def seed_delta(path, data_dict, schema=None):
    table = pa.table(data_dict, schema=schema)  # NOT pd.DataFrame
    write_deltalake(str(path), table, mode="overwrite")

def read_delta(path):
    return DeltaTable(str(path)).to_pandas()
```

### P-005: Simulating Missing Libraries
```python
import sys

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

### P-006: Mock NodeConfig for Pattern Tests
```python
from unittest.mock import MagicMock

def make_config(name="test_node", params=None):
    cfg = MagicMock()
    cfg.name = name
    cfg.params = params or {}
    return cfg
```

### P-007: Databricks Notebook lib/ Pattern
```python
# Every notebook starts with:
import sys, os
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit("/", 1)[0]
sys.path.insert(0, PROJECT_ROOT)
from lib.setup import *
```

---

## Discoveries (Verified Behavior)

> Empirical findings that inform implementation decisions.

### V-001: Polars Adds +00:00 to All Datetime Columns
**Verified:** 2026-04  
**Finding:** Polars always stores timezone-aware datetimes. When comparing with Python `datetime.now()` (naive), the offset causes false results.  
**Implication:** All Polars datetime tests must use `timezone.utc`

### V-002: CoolProp and Polars Are Installed in CI
**Verified:** 2026-04  
**Finding:** CoolProp 7.2.0 and Polars are available. No mocking needed for thermodynamics or manufacturing Polars tests.  
**Implication:** Run these tests directly, don't skip them

### V-003: PySpark 4.1.1 Is Installed — Fallback Stubs Are Not Dead Code
**Verified:** 2026-04  
**Finding:** Because PySpark IS installed, `catalog.py`'s try/except imports succeed. The fallback type stubs (lines 22-137) are NOT defined at runtime. But they ARE essential for Pandas-only deployments.  
**Implication:** Do not remove fallback stubs. They're not dead code.

### V-004: `create_connection` Does Not Exist in Factory
**Verified:** 2026-04  
**Finding:** `state/__init__.py` imports `create_connection` from `odibi.connections.factory`, but only typed functions like `create_sql_server_connection` exist.  
**Implication:** To test SQL Server state backend factory paths, inject mock directly: `m.create_connection = MagicMock()`

---

## Session Log Template

Copy-paste this at the end of your session:

```markdown
## Session: [Date] — [Brief Description]

### Work Done
- [bullet list of changes]

### Verification Report
- Tests written: N
- Tests passing: N/N
- Command: `pytest tests/unit/path/test_file.py -v`

### New Entries Added
- [ ] Decision: D-NNN — [title]
- [ ] Trap: T-NNN — [title]
- [ ] Pattern: P-NNN — [title]
- [ ] Discovery: V-NNN — [title]
- [ ] AGENTS.md coverage updated

### No New Entries Needed
- [ ] Confirmed: no new gotchas discovered this session
```
