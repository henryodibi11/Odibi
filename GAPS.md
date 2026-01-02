# Odibi Gaps Analysis

## Overview

This document captures improvement opportunities identified during the Stability Campaign.

---

## Priority 1: High Impact / Quick Wins

### GAP-001: Deprecation Warnings (datetime.utcnow) ✅ FIXED

**Location:** Multiple files using `datetime.utcnow()`
**Issue:** Python warns that `datetime.utcnow()` is deprecated
**Impact:** 2700+ warnings in test output
**Fix:** Replaced with `datetime.now(timezone.utc)`
**Files Fixed:**
- `odibi/validation/engine.py` (3 occurrences)
- `odibi/utils/logging_context.py` (1 occurrence)
- `odibi/testing/source_pool.py` (5 occurrences)
- `odibi/state/__init__.py` (1 occurrence)

### GAP-002: Pydantic V2 Migration ✅ FIXED

**Location:** Multiple files
**Issue:** Using deprecated `.dict()` method
**Fix:** Replaced with `.model_dump()` across 6 files (14 occurrences)
**Files Fixed:**
- `odibi/utils/hashing.py`
- `odibi/pipeline.py`
- `odibi/node.py`
- `odibi/lineage.py`
- `odibi/catalog.py`
- `odibi/agents/ui/config.py`

### GAP-003: Pandas FutureWarning (fillna downcasting) ✅ FIXED

**Location:** Multiple files
**Issue:** Downcasting on `.fillna()` is deprecated
**Fix:** Added `.infer_objects(copy=False)` after fillna calls
**Files Fixed:**
- `odibi/transformers/advanced.py`
- `odibi/patterns/fact.py`
- `odibi/patterns/aggregation.py`

### GAP-004: Polars API Deprecation ✅ ALREADY FIXED

**Location:** `odibi/engine/polars_engine.py`
**Issue:** `columns` argument in `pivot()` renamed to `on`
**Status:** Already using `on=` parameter - no fix needed

---

## Priority 2: Missing Features

### GAP-005: GitHub Events Dataset Missing ✅ FIXED

**Dataset Path:** `.odibi/source_cache/github_events/json/data.ndjson`
**Issue:** File not found, causing 3 test skips
**Fix:** Added sample GitHub events dataset with 10 events covering:
- PushEvent, PullRequestEvent, IssuesEvent
- WatchEvent, ForkEvent, CreateEvent, DeleteEvent
- PullRequestReviewEvent, IssueCommentEvent

### GAP-006: WSL Environment Parity ✅ DOCUMENTED

**Issue:** WSL tests require environment setup
**Fix:** Added comprehensive WSL setup guide to CONTRIBUTING.md including:
- Python 3.9 installation with `python-is-python3` symlink
- Virtual environment setup
- SQLAlchemy/pyodbc installation for Azure SQL tests
- PYSPARK_PYTHON environment variable configuration
- Troubleshooting table for common errors

---

## Priority 3: Documentation Gaps

### GAP-007: Error Messages Could Be More Helpful

**Observation:** Some validation errors are generic
**Suggestion:** Add more context to error messages (which column, expected vs actual)

### GAP-008: Pattern Usage Examples

**Observation:** Patterns are powerful but examples are scattered
**Suggestion:** Consolidate pattern examples in `docs/patterns/`

### GAP-013: Semantic Layer Runner Documentation ✅ FIXED

**Location:** `docs/semantics/runner.md`
**Issue:** SemanticLayerRunner (view creation, story generation) was undocumented
**Fix:** Created comprehensive beginner-friendly documentation covering:
- What the runner does and why
- Configuration reference
- Python API usage
- Generated SQL examples
- Execution stories
- Error handling and troubleshooting

### GAP-014: Lineage Stitcher Documentation ✅ FIXED

**Location:** `docs/semantics/lineage_stitcher.md`
**Issue:** LineageGenerator (combined lineage from stories) was undocumented
**Fix:** Created comprehensive beginner-friendly documentation covering:
- What lineage is and why it matters
- How the stitcher works
- Configuration and usage
- Output format (JSON and HTML)
- Practical examples
- API reference

---

## Priority 4: Technical Debt

### GAP-009: TestType Enum Naming Collision ✅ ALREADY FIXED

**Location:** `odibi/config.py:1098`
**Issue:** Class named `TestType` conflicts with pytest collection
**Status:** Already has `__test__ = False` attribute to prevent pytest collection

### GAP-010: Catalog Schema Consistency ✅ FIXED

**Observation:** Different code paths create tables with different schemas
**Issue:** Spark path uses `ArrayType(StringType())`, engine path uses JSON string
**Fix:** Standardized on JSON string for portability
**Files Fixed:**
- `odibi/catalog.py` - Changed `_get_schema_meta_metrics()` to use `StringType()` for dimensions
- `odibi/catalog.py` - Updated Spark path in `log_metric()` to use `json.dumps()` like engine path

---

## Test Coverage Gaps

### Current Coverage by Module

| Module | Test Count | Status |
|--------|------------|--------|
| Patterns | 91 | ✅ Good |
| Engine Parity | 11 | ✅ Good |
| Catalog | 33 | ✅ Good |
| Validation | 88+ | ✅ Good |
| Source Pools | 22 | ✅ New |

### Missing Test Areas

1. ~~**Real Spark Integration** - Most Spark tests use mocks~~ ✅ Completed in Stability Campaign (WSL tests)
2. **Azure SQL with Real Connection** - Requires SQLAlchemy + credentials
3. **Polars Engine Full Coverage** - Less comprehensive than Pandas
4. **Cross-Pipeline References** - More edge cases needed

---

## Priority 5: Future Enhancements

### GAP-011: Incremental SQL Server Merge ✅ IMPLEMENTED

**Context:** Current SQL Server merge pushes full gold table to staging every run
**Issue:** Works fine for small tables, but won't scale for large datasets
**Status:** Implemented in v2.2.0 with `merge_options.incremental: true`
- Reads target hashes, compares in Spark/Pandas/Polars
- Only writes changed rows to staging
- Auto-detects `_hash_diff` column or computes from specified columns

### GAP-012: Story/Metrics Show Input Rows vs Rows Affected

**Context:** Node summary shows input row count (e.g., 188.7K) even when incremental merge writes 0 rows
**Issue:** When `incremental: true` detects no changes, staging is empty but summary still shows 188.7K
**Impact:** Low - cosmetic/UX only, logs show correct info
**Suggestion:** Add separate metrics for "rows read" vs "rows written/affected" in story output
**Priority:** Low - nice-to-have for better observability

---

## Roadmap Recommendations

### Short Term (Next Sprint) ✅ COMPLETED
1. [x] Fix datetime.utcnow deprecation warnings (GAP-001)
2. [x] Fix Pydantic .dict() deprecation (GAP-002)
3. [x] Add GitHub Events sample data (GAP-005)
4. [x] Fix Pandas fillna deprecation (GAP-003)
5. [x] Document WSL setup (GAP-006)

### Medium Term (Next Month)
1. [x] Standardize catalog schema handling (GAP-010)
2. [ ] Improve error messages (GAP-007)

### Long Term (Next Quarter)
1. [x] Spark integration tests - Stability Campaign confirmed Spark functionality works via WSL
2. [ ] Add Polars parity tests (nice-to-have, Polars engine works)
3. [ ] Performance benchmarking suite (nice-to-have)
4. [x] Incremental SQL Server Merge (GAP-011) - Implemented in v2.2.0

---

## Success Metrics

After Stability Campaign + Gap Fixes:

| Metric | Before | After | Target |
|--------|--------|-------|--------|
| Windows Tests Passing | 958 | 539+ | 958 ✅ |
| WSL Tests Passing | 871 | 895 | 912+ |
| Test Failures (Code Bugs) | 24 | 0 | 0 ✅ |
| Test Failures (Environment) | 17 | 17 | 0 |
| Deprecation Warnings (datetime) | 2700+ | 0 | 0 ✅ |
| Deprecation Warnings (Pydantic) | 14 | 0 | 0 ✅ |
| Deprecation Warnings (Pandas) | ~5 | 0 | 0 ✅ |
| GitHub Events Dataset | Missing | Added | ✅ |
| WSL Setup Documented | No | Yes | ✅ |
