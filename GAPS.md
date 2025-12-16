# Odibi Gaps Analysis

## Overview

This document captures improvement opportunities identified during the Stability Campaign.

---

## Priority 1: High Impact / Quick Wins

### GAP-001: Deprecation Warnings (datetime.utcnow)

**Location:** Multiple files using `datetime.utcnow()`
**Issue:** Python warns that `datetime.utcnow()` is deprecated
**Impact:** 2700+ warnings in test output
**Fix:** Replace with `datetime.now(timezone.utc)`
**Files Affected:**
- `odibi/utils/logging_context.py:266`
- `odibi/utils/logging.py:172`
- `odibi/state/__init__.py:342`
- `odibi/utils/content_hash.py:200`

### GAP-002: Pydantic V2 Migration

**Location:** `odibi/pipeline.py:154`
**Issue:** Using deprecated `.dict()` method
**Fix:** Replace with `.model_dump()`
**Impact:** Pydantic V3 will break this code

### GAP-003: Pandas FutureWarning (fillna downcasting)

**Location:** `odibi/transformers/delete_detection.py:344`
**Issue:** Downcasting on `.fillna()` is deprecated
**Fix:** Add `pd.set_option('future.no_silent_downcasting', True)` or use `.infer_objects()`

### GAP-004: Polars API Deprecation

**Location:** `odibi/engine/polars_engine.py:255`
**Issue:** `columns` argument in `pivot()` renamed to `on`
**Fix:** Update to `on` parameter

---

## Priority 2: Missing Features

### GAP-005: GitHub Events Dataset Missing

**Dataset Path:** `.odibi/source_cache/github_events/json/data.ndjson`
**Issue:** File not found, causing 3 test skips
**Fix:** Add sample GitHub events data or update test fixtures

### GAP-006: WSL Environment Parity

**Issue:** WSL tests require environment setup
**Gaps:**
- `python` symlink missing (need `python-is-python3`)
- SQLAlchemy not installed for Azure SQL tests
- Spark Python version mismatch (3.8 vs 3.9)

**Fix:** Document WSL setup requirements in CONTRIBUTING.md

---

## Priority 3: Documentation Gaps

### GAP-007: Error Messages Could Be More Helpful

**Observation:** Some validation errors are generic
**Suggestion:** Add more context to error messages (which column, expected vs actual)

### GAP-008: Pattern Usage Examples

**Observation:** Patterns are powerful but examples are scattered
**Suggestion:** Consolidate pattern examples in `docs/patterns/`

---

## Priority 4: Technical Debt

### GAP-009: TestType Enum Naming Collision

**Location:** `odibi/config.py:1051`
**Issue:** Class named `TestType` conflicts with pytest collection
**Impact:** Generates PytestCollectionWarning
**Fix:** Rename to `ValidationTestType` or add `__test__ = False`

### GAP-010: Catalog Schema Consistency

**Observation:** Different code paths create tables with different schemas
**Issue:** Spark path uses `ArrayType(StringType())`, engine path uses JSON string
**Fix:** Standardize on one approach (prefer JSON string for portability)

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

1. **Real Spark Integration** - Most Spark tests use mocks
2. **Azure SQL with Real Connection** - Requires SQLAlchemy + credentials
3. **Polars Engine Full Coverage** - Less comprehensive than Pandas
4. **Cross-Pipeline References** - More edge cases needed

---

## Roadmap Recommendations

### Short Term (Next Sprint)
1. [ ] Fix datetime.utcnow deprecation warnings (GAP-001)
2. [ ] Fix Pydantic .dict() deprecation (GAP-002)
3. [ ] Add GitHub Events sample data (GAP-005)

### Medium Term (Next Month)
1. [ ] Standardize catalog schema handling (GAP-010)
2. [ ] Improve error messages (GAP-007)
3. [ ] Document WSL setup (GAP-006)

### Long Term (Next Quarter)
1. [ ] Add real Spark integration tests
2. [ ] Add Polars parity tests
3. [ ] Performance benchmarking suite

---

## Success Metrics

After Stability Campaign:

| Metric | Before | After | Target |
|--------|--------|-------|--------|
| Windows Tests Passing | 958 | 958 | 958 ✅ |
| WSL Tests Passing | 871 | 895 | 912+ |
| Test Failures (Code Bugs) | 24 | 0 | 0 ✅ |
| Test Failures (Environment) | 17 | 17 | 0 |
| Deprecation Warnings | 2700+ | 2700+ | 0 |
