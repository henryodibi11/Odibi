# Odibi v3.9.0 — 80% Test Coverage Milestone

**Release Date:** April 20, 2026

## 🎯 The Big Number: 66% → 80% Coverage

This release is the result of a focused 5-week coverage campaign that added **2,000+ new unit tests** across **60+ test files**, covering every major module in the framework.

| Metric | Before | After |
|--------|--------|-------|
| Statements | 34,363 | 34,363 |
| Missed | 11,807 | 6,854 |
| Coverage | **66%** | **80%** |
| Test files | ~30 | 90+ |
| Total tests | ~7,100 | ~9,200 |

## 🧪 What's Now Tested

### CLI (15 commands, 350+ tests)
Every CLI command — `main`, `catalog`, `system`, `list`, `story`, `schema`, `lineage`, `secrets`, `init`, `test`, `graph`, `export`, `deploy`, `run`, `validate` — has dedicated mock-based tests. Most are at 94-100% coverage.

### Validation Engine (160+ tests)
All 11 validation test types (NOT_NULL, UNIQUE, ACCEPTED_VALUES, ROW_COUNT, RANGE, REGEX_MATCH, CUSTOM_SQL, SCHEMA, FRESHNESS) tested across both Pandas and Polars engines. Quarantine and quality gate modules fully covered.

### Patterns (140+ tests)
Aggregation, Fact, Dimension, and Date Dimension patterns — all Pandas/DuckDB paths exercised including surrogate key generation, SCD0/1/2 dispatch, grain validation, and unknown member handling.

### Transformers (250+ tests)
SCD2, merge, delete detection, relational (join/union/pivot/unpivot), 16 sql_core transformers, manufacturing (Polars phase detection), thermodynamics (real CoolProp), units, and advanced (sessionize, split_events).

### Connections (400+ tests)
Local (98%), Azure ADLS (89%), Azure SQL (93%), PostgreSQL (96%), API Fetcher (94%), Factory (81%). All auth modes, discovery methods, and error paths covered.

### Story & Lineage (370+ tests)
Story generator, doc generator, lineage generator, lineage utilities — all rendering, graph building, cross-run comparison, and remote write/cleanup paths.

### Infrastructure (500+ tests)
State backends (LocalJSON, CatalogState, SqlServer), catalog sync, pipeline utilities, node execution paths, derived updater, SQL Server writer, introspect, progress, setup helpers.

## 🐛 Bug Fixes

- **PySpark isinstance check crashing without JVM** — validation engine, quarantine, and gate modules now handle missing JVM gracefully via try/except
- **SCD2 Pandas KeyError on missing flag_col** — target DataFrames without the current-flag column no longer crash during merge
- **SCD2 TimestampNTZType import failure** — graceful fallback for older PySpark versions
- **OpenLineage 1.46.0 API breaking changes** — updated to new `sourceCode` parameter name and removed deprecated `api_key`
- **SparkEngine.__init__** — no longer touches `SparkSession.builder` when a session is already provided
- **Deprecated `datetime.utcnow()`** — replaced across codebase to suppress Python 3.12 deprecation warnings
- **`pd.concat` FutureWarning** — fixed future-incompatible concat calls

## 🔧 CI Hardening

- **Python 3.9–3.12 green** — all tests pass across 4 Python versions
- **Simplified conftest.py** — spark/delta skip filter checks filename only; removed 3 redundant pytest hooks
- **Ruff lint clean** — zero violations (E712, E741, F841, F811, E402)
- **Coverage batch script** (`scripts/run_coverage.ps1`) — runs tests in safe batches to avoid hangs from thread pool + coverage tracing interactions
- **AGENTS.md** — comprehensive documentation of test patterns, mock gotchas, known hangs, and coverage roadmap for AI-assisted development

## 📋 Test Strategy

The test strategy remains: **mock logic in CI, validate behavior in Databricks prod.**

- Spark-specific branches are tested via `MagicMock` sessions — no JVM required
- Pandas and Polars paths are tested with real engines
- CoolProp thermodynamics tests use the real library
- Delta Lake operations use `deltalake` (Python-native) for reads, mocks for Spark writes

## ⬆️ Upgrade

```bash
pip install --upgrade odibi
```

No breaking changes. All existing pipelines and YAML configs work unchanged.

---

**Full Changelog:** See [CHANGELOG.md](../CHANGELOG.md)

**Installation:** `pip install --upgrade odibi`

**Report Issues:** https://github.com/henryodibi11/Odibi/issues
