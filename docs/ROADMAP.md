# Odibi Roadmap

## Current State (v2.16.0)

*Last updated: 2026-02-22*

| Metric | Value |
|--------|-------|
| Tests | 4,832 collected, 4,694 passed, 33 skipped, 0 failures |
| **Test Coverage** | **62%** (target: 80%) |
| Transformers | 56 |
| Patterns | 6 (Dimension, Fact, SCD2, Merge, Aggregation, Date Dimension) |
| Engines | 3 (Pandas/DuckDB, Spark, Polars) |
| Open Issues | 18 (3 bugs, 6 docs, 5 enhancements, 4 feature requests) |
| Python Support | 3.9-3.12 |

### Coverage by Module (Low Priority = >80%, Medium = 50-80%, High = <50%)

**Note on Spark Coverage:** The 3% for Spark is a CI artifact — Spark tests are skipped in CI (no JVM). Spark is tested via mock-based tests in `tests/integration/test_patterns_spark_mock.py` and validated in production on Databricks.

| Module | Coverage | Priority | Notes |
|--------|----------|----------|-------|
| `engine/spark_engine.py` | 3% | OK | CI-skipped; mock-tested + Databricks validated |
| `diagnostics/delta.py` | 13% | HIGH | Needs mock tests |
| `catalog_sync.py` | 43% | HIGH | |
| `cli/catalog.py` | 46% | HIGH | |
| `catalog.py` | 47% | HIGH | Core module, biggest blind spot |
| `node.py` | 51% | HIGH | Core module, second biggest blind spot |
| `derived_updater.py` | 52% | MEDIUM | |
| `story/generator.py` | 54% | MEDIUM | |
| `story/doc_generator.py` | 55% | MEDIUM | |
| `engine/polars_engine.py` | 57% | MEDIUM | Up from 7% — major improvement |
| `transformers/merge_transformer.py` | 61% | MEDIUM | Up from 36% |
| `engine/pandas_engine.py` | 62% | MEDIUM | Up from 25% — primary engine |
| `connections/factory.py` | 69% | LOW | |
| `validation/engine.py` | 69% | LOW | |
| `state/__init__.py` | 73% | LOW | Up from 33% |
| `connections/azure_adls.py` | 76% | LOW | Up from 10% |
| `context.py` | 77% | LOW | Up from 62% |
| `connections/api_fetcher.py` | 79% | LOW | |
| `transformers/advanced.py` | 79% | LOW | Up from 44% |
| `pipeline.py` | 89% | LOW | |
| `diagnostics/manager.py` | 95% | ✅ | |
| `graph.py` | 98% | ✅ | |
| `utils/logging_context.py` | 98% | ✅ | |
| `diagnostics/diff.py` | 99% | ✅ | |

---

## Priority 1: Stability & Defect Triage ✅ LARGELY COMPLETE

**Goal:** Zero critical bugs, reliable cross-engine behavior

### 1.1 Bug Audit (Feb 2026) — DONE

Completed a comprehensive audit filing 46 bug issues (#238–#280). **43 bugs fixed and closed.**

Remaining open bugs (3):
- [x] ~~All critical and high-priority bugs~~ — Fixed
- [ ] #268 SECURITY: MCP discovery tool vulnerable to SQL injection (deferred — MCP not in use)
- [ ] #265 SECURITY: MCP execute.py allows arbitrary code execution (deferred — MCP not in use)
- [ ] #199 AggregationPattern._load_existing_spark lacks multi-format support (low-pri, depends on #192)

Unlabeled open issue to triage:
- [ ] #248 SCD2 Pandas change detection unreliable for float/NaN comparisons

### 1.2 Test Infrastructure — IN PROGRESS

- [x] Run `pytest --cov=odibi --cov-report=html` and identify modules < 80% coverage ✓
- [x] Establish strict 5-point fix checklist (ruff check, ruff format, pytest, no conftest changes, correct @patch targets) ✓
- [ ] Add parametrized tests for transformers across all 3 engines
- [ ] Add edge-case tests: empty DataFrames, null-only columns, Unicode data
- [ ] Increase `catalog.py` coverage from 47% to 80%+
- [ ] Increase `node.py` coverage from 51% to 80%+

---

## Priority 2: Engine Parity ✅ FUNCTIONALLY COMPLETE

**Goal:** Every feature works identically on Pandas, Spark, and Polars

Engine parity is **achieved** — all features work across all 3 engines. See `docs/reference/PARITY_TABLE.md` for the full matrix (all ✅).

### 2.1 Parity Status

| Transformer | Pandas | Spark | Polars | Notes |
|------------|--------|-------|--------|-------|
| `scd2` | ✓ | ✓ | ✓ | Verified |
| `pivot` | ✓ | ✓ | ✓ | Verified |
| `window_calculation` | ✓ | ✓ | ✓ | Verified |
| `normalize_json` | ✓ | ✓ | ✓ | Verified |

### 2.2 Ongoing Maintenance

Engine parity is now a **maintain-as-you-go** concern, not a dedicated effort:
- When adding a feature to one engine, add the matching implementation to the others
- Polars coverage improved from 7% → 57% organically
- [ ] #212 Missing Polars branches in some transformers (low-pri)
- [ ] Document Spark SQL vs DuckDB SQL differences

---

## Priority 3: Error Diagnostics & CLI

**Goal:** Misconfiguration is obvious, actionable errors

### 3.1 YAML Validation

- [x] Ensure `odibi validate config.yaml` catches ALL common mistakes ✓
- [ ] Add validation for node name format (alphanumeric + underscore only)
- [ ] Add validation for missing `format:` in inputs/outputs
- [ ] Improve error messages with line numbers and suggestions

### 3.2 Runtime Diagnostics

- [ ] Expand `diagnose_error` MCP tool with more error patterns
- [ ] Add `odibi doctor` checks for environment issues
- [x] Improve traceback cleaning for node execution errors ✓

---

## Priority 4: Documentation & Examples

**Goal:** Golden path is clear, pitfalls are documented

### 4.1 Examples

- [ ] Verify all examples in `examples/` run successfully
- [ ] Add end-to-end example for each pattern
- [ ] Add "migration from raw SQL" example

### 4.2 Pitfall Documentation

- [ ] Document anti-patterns (e.g., using hyphens in node names)
- [ ] Document engine-specific gotchas
- [ ] Add troubleshooting section to AI assistant setup guide

---

## Priority 5: Feature Gaps

**Goal:** Cover common use cases, stay focused

### 5.1 Potential Additions (Evaluate Need First)

- [ ] `apply_mapping` transformer for lookup-based value replacement
- [ ] `flatten_struct` for deeply nested JSON (beyond single level)
- [ ] `row_number` as standalone transformer (simpler than `window_calculation`)
- [ ] CDC (Change Data Capture) pattern variant

### 5.2 NOT Adding

- Complex orchestration (use Dagster/Airflow)
- Agent/chat infrastructure (use Amp/Cursor/Cline)
- GUI/web interface

---

## Next Actions

### Immediate: Core Coverage Gaps

1. **catalog.py** (47%) — Core data backbone, biggest blind spot
   - System catalog registration, lineage tracking, metadata queries
   - Target: 80%+

2. **node.py** (51%) — Core execution unit, second biggest blind spot
   - Node execution paths, error handling, retry logic
   - Target: 80%+

### Then: Remaining Coverage

3. **catalog_sync.py** (43%) — Catalog synchronization logic
4. **derived_updater.py** (52%) — Derived metric updates
5. **story/generator.py** (54%) — Story generation paths

### Completed (previously planned)

- ~~Pandas engine tests (25% → 62%)~~ ✅
- ~~Polars engine tests (7% → 57%)~~ ✅
- ~~Merge transformer (36% → 61%)~~ ✅
- ~~Advanced transformers (44% → 79%)~~ ✅
- ~~State management (33% → 73%)~~ ✅
- ~~Review GitHub issues and close stale ones~~ ✅ (43 bugs closed)

---

## Existing Test Infrastructure (Not Reflected in Coverage)

The `scripts/run_test_campaign.py` runs end-to-end validation that pytest coverage doesn't capture:

| Phase | What It Tests | Engine |
|-------|---------------|--------|
| Phase 1 | CSV read, Parquet write, schema validation | Pandas |
| Phase 3 | State/HWM persistence | Pandas |
| Phase 4 | Merge pattern (upsert) | Pandas |
| Phase 5 | SCD2 pattern | Pandas |
| Phase 6 | Logical path resolution | Pandas |
| Phase 11 | 10k row scaling | Pandas |

**Production validation:** Spark engine runs in Databricks - not tested in CI but validated in production.

---

## Documentation Gaps to Address

- [x] Add `scripts/run_test_campaign.py` to `docs/guides/testing.md` ✓
- [x] Update `docs/features/engines.md` with engine-specific testing notes ✓
- [ ] Document Spark/Databricks testing approach in `docs/tutorials/spark_engine.md`
- [ ] Add "How to run the test campaign" section to AGENTS.md or CONTRIBUTING.md
- [ ] #229 Add engine parity table for transformers
- [ ] #228 CHANGELOG missing entries for recent bug fixes
- [ ] #225 Add Delta Lake troubleshooting section
- [ ] #224 Add tutorial for validation and contracts workflow
- [ ] #223 Add tutorial for delete detection workflow
- [ ] #222 Add tutorial for quarantine/orphan handling workflow

---

## Success Metrics

| Goal | Target | Current | Status |
|------|--------|---------|--------|
| Test coverage | 80%+ | 62% | 🔶 In progress (+16% from 46%) |
| CI pass rate | 100% | 100% | ✅ Done |
| Engine parity | 100% | 100% | ✅ Done (all features ✅ across engines) |
| Bug backlog | 0 critical | 0 critical | ✅ Done (3 low-pri/deferred remain) |
| Open issues | < 10 | 18 | 🔶 Reduced from 46+ filed |

---

## Long-term Vision

Odibi should be:
- **Declarative**: YAML-first, SQL-based transformations
- **Portable**: Works on laptop (Pandas), cluster (Spark), or serverless (Polars)
- **Stable**: Comprehensive tests, predictable behavior
- **Documented**: AI assistants can generate correct configs without trial and error

The ultimate test: Can you hand a business analyst the docs and have them build a working pipeline without your help?
