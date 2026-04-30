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

### T-011: Spark Connect Temp View Lifecycle — Views Dropped Before Lazy Eval
**Date:** 2026-04  
**Symptom:** Spark Connect queries fail with `INVALID_HANDLE.Format: OPERATION_HANDLE` or return empty results  
**Root Cause:** `EngineContext.sql()` used a `finally` block to unregister temp views after SQL execution. On classic Spark this works because DataFrames eagerly resolve. On Spark Connect, plan resolution is deferred — the view is dropped before the lazy DataFrame materializes.  
**Fix:** Only unregister temp views on error (`except` block). Successful results keep views alive. Unique view names (`_odibi_view_{uuid}`) prevent conflicts; session cleanup handles the rest.  
**Modules:** `odibi/context.py`

### T-012: `bool(LazyFrame)` TypeError in Row Counting
**Date:** 2026-04  
**Symptom:** `TypeError: the truth value of a LazyFrame is ambiguous` when running transformers with Polars  
**Root Cause:** Row-counting logic used `if rows_before and rows_after:` which calls `bool()` on the result of `.count()`. Polars `.count()` returns a LazyFrame (not an int), and `bool(LazyFrame)` raises TypeError.  
**Fix:** Replace `if rows_before and rows_after:` with `if isinstance(rows_before, (int, float)) and isinstance(rows_after, (int, float)):` — explicit type checks instead of truthiness.  
**Modules:** `odibi/transformers/sql_core.py`, `odibi/transformers/advanced.py`, `odibi/transformers/relational.py`

### T-013: Polars SQL Dialect Gaps — EXCEPT vs EXCLUDE, No Subqueries
**Date:** 2026-04  
**Symptom:** `polars.exceptions.SQLInterfaceError` on deduplicate: "EXCEPT syntax not supported" and "subqueries not supported"  
**Root Cause:** Polars SQL uses `EXCLUDE` for column exclusion (like DuckDB/Pandas), not `EXCEPT` (Spark/SQL Server). Polars SQL also does not support subqueries like `ORDER BY (SELECT NULL)`.  
**Fix:** Added Polars to the EXCLUDE dialect alongside Pandas/DuckDB. Use `ORDER BY 1` for Polars when no explicit `order_by` is specified.  
**Modules:** `odibi/transformers/advanced.py` (deduplicate)

---

## Patterns (Working Recipes)

> Copy-paste these. They're battle-tested.


### T-014: Databricks `editAsset` for Files Replaces Entire Content
**Date:** 2026-04  
**Symptom:** CHANGELOG.md (61,524 chars) truncated to 5,984 chars after using `editAsset` with `type: file`  
**Root Cause:** The `editAsset` tool for workspace files replaces the **entire file body** with the provided content — it does NOT patch or append. When the content parameter only contained the new [Unreleased] section + a version header, all 50+ historical version entries were lost.  
**Fix:** For large files, use Python `open(path, 'a')` to append, or read the full file first and include ALL content in the editAsset body. Never use editAsset for partial updates to large files.  
**Modules:** Any workspace file editing via Databricks assistant tools

### T-015: External Internet Blocked on Databricks Cluster — No GitHub Recovery
**Date:** 2026-04  
**Symptom:** `SSLError(SSLEOFError(8, '[SSL: UNEXPECTED_EOF_WHILE_READING]'))` when calling `raw.githubusercontent.com`, `api.github.com`, or any external HTTPS endpoint from the `shared-173-LTS` cluster.  
**Root Cause:** Corporate proxy or firewall blocks outbound HTTPS to non-approved hosts. PyPI works (via allowed proxy), but GitHub, raw content, and API endpoints do not.  
**Impact:** Cannot recover workspace files from GitHub when accidentally overwritten. Cannot `pip install` from GitHub URLs. Cannot use `requests` or `urllib` for external data.  
**Fix:** Never rely on external HTTP for file recovery. Keep critical content in the conversation cache or workspace. For package installs, only use PyPI-hosted packages.  
**Modules:** Any agent workflow that assumes internet access

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

### V-005: Module Caching on Shared Databricks Clusters
**Verified:** 2026-04  
**Finding:** On shared Databricks clusters, Python caches imported modules across cell executions. If you modify odibi source files and re-import, you get the stale cached version. Clearing just `odibi.*` from `sys.modules` is insufficient if your project has its own `lib/` package — both must be cleared.  
**Implication:** Every campaign/test notebook must clear both `lib.*` and `odibi.*` from `sys.modules` before importing. Pattern: `for m in [k for k in sys.modules if k.startswith('lib') or k.startswith('odibi')]: del sys.modules[m]`

### V-006: Spark Connect `.collect()` Returns List of Rows, Not DataFrame
**Verified:** 2026-04  
**Finding:** `hasattr(df, 'collect')` is True for both Polars LazyFrames AND Spark DataFrames. But Spark's `.collect()` returns `list[Row]`, while Polars' `.collect()` returns a `pl.DataFrame`. Using `hasattr` to detect "needs .collect()" will mis-route Spark DataFrames.  
**Implication:** Always use `isinstance(df, pl.LazyFrame)` to detect Polars lazy frames. Never use `hasattr(df, 'collect')` as a Polars check.

### V-007: Databricks Repos — No `.git` Directory, No Local Git Commands
**Verified:** 2026-04  
**Finding:** Databricks Repos are managed externally — there is no `.git` directory on disk. `git log`, `git diff`, `git checkout` all fail with "not a git repository". Branch operations must use the Repos REST API (`PATCH /api/2.0/repos/{id}`). Switching branches via the API does a full checkout but does NOT restore files that were modified through the workspace API (`editAsset`) — workspace file changes appear to propagate across branches or persist beyond the checkout.  
**Implication:** If you overwrite a file via `editAsset`, you cannot recover it with `git checkout -- file`. The only recovery options are: (1) conversation cache from a prior `readAssetById` call, (2) a separate backup copy, or (3) manual reconstruction. Always read and cache large files before editing them.


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

## Session: 2026-04-30 — Odibi Hardening Campaign (Spark Connect + Polars Parity)

### Work Done
- Fixed `context.py` temp view lifecycle for Spark Connect lazy eval
- Fixed row-counting `bool(LazyFrame)` TypeError across 4 transformer files
- Fixed Polars deduplicate SQL dialect (EXCLUDE + ORDER BY 1)
- Updated `test_context.py` to match new view lifecycle behavior
- Built campaign test harness (01_smoke_test, 02_engine_matrix)
- Validated: 7,440 unit tests pass, 0 regressions in changed modules

### Verification Report
- Tests written: 0 new (1 updated: test_sql_keeps_view_registered_after_successful_execution)
- Tests passing: 7,440/7,440 (excluding 49 pre-existing failures from missing deltalake + /tmp env issues)
- Engine parity: 11/11 parity tests pass
- Campaign: 9/9 engine matrix tests pass (3 transformers × 3 engines)
- Command: `pytest tests/unit/ -q --no-header`

### New Entries Added
- [x] Trap: T-011 — Spark Connect Temp View Lifecycle
- [x] Trap: T-012 — bool(LazyFrame) TypeError in Row Counting
- [x] Trap: T-013 — Polars SQL Dialect Gaps
- [x] Discovery: V-005 — Module Caching on Shared Clusters
- [x] Discovery: V-006 — Spark Connect .collect() Returns List of Rows

## Session: 2026-04-29 — CHANGELOG Catch-Up (#228)

### Work Done
- Updated CHANGELOG.md [Unreleased] section with all post-3.9.0 changes
- Documented: skills system (15 docs), CUSTOM_INSTRUCTIONS.md, LESSONS_LEARNED.md, AGENT_CAMPAIGN.md, campaign workspace, bug audit (43/46 fixed)
- Recovered CHANGELOG.md after accidental truncation from editAsset file replacement
- Reconstructed 67,192-char file from conversation cache in 6 append chunks

### Verification Report
- Tests written: 0 (docs-only change)
- All 52 version entries preserved
- 12/12 success criteria passing
- Command: `python -c "import re; content=open('CHANGELOG.md').read(); versions=re.findall(r'## \[([^\]]+)\]', content); print(f'{len(versions)} versions found')"`

### New Entries Added
- [x] Trap: T-014 — Databricks editAsset for Files Replaces Entire Content
- [x] Trap: T-015 — External Internet Blocked on Databricks Cluster
- [x] Discovery: V-007 — Databricks Repos No .git Directory, No Local Git
- [ ] AGENTS.md coverage updated (no test changes)
