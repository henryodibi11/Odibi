# Odibi Roadmap

## Current State (v2.8.0)

| Metric | Value |
|--------|-------|
| Tests | 2,118 collected (1,627 unit tests) |
| **Test Coverage** | **46%** (target: 80%) |
| Transformers | 52 |
| Patterns | 6 (Dimension, Fact, SCD2, Merge, Aggregation, Date Dimension) |
| Engines | 3 (Pandas/DuckDB, Spark, Polars) |
| MCP Tools | 21 |
| Python Support | 3.9-3.12 |

### Coverage by Module (Low Priority = >80%, Medium = 50-80%, High = <50%)

**Note on Engine Coverage:** The low coverage for Spark (4%) and Polars (7%) is **misleading**. These engines are tested via:
- Mock-based tests that validate logic without executing engine code
- Production validation (Spark runs in Databricks)
- Skip markers when dependencies aren't installed

The coverage numbers reflect CI environment limitations, not actual test gaps.

| Module | Coverage | Priority | Notes |
|--------|----------|----------|-------|
| `engine/spark_engine.py` | 4% | OK | Mock-tested + Databricks validated |
| `engine/polars_engine.py` | 7% | MEDIUM | Needs more mock tests |
| `engine/pandas_engine.py` | 25% | HIGH | Primary engine, needs coverage |
| `connections/azure_adls.py` | 10% | HIGH |
| `diagnostics/delta.py` | 13% | HIGH |
| `state/__init__.py` | 33% | HIGH |
| `story/generator.py` | 25% | HIGH |
| `transformers/merge_transformer.py` | 36% | HIGH |
| `transformers/advanced.py` | 44% | MEDIUM |
| `node.py` | 44% | MEDIUM |
| `derived_updater.py` | 49% | MEDIUM |
| `context.py` | 62% | MEDIUM |
| `validation/engine.py` | 68% | LOW |
| `validation/gate.py` | 96% | LOW |
| `graph.py` | 96% | LOW |

---

## Priority 1: Stability & Defect Triage

**Goal:** Zero critical bugs, reliable cross-engine behavior

### 1.1 Known Issues to Investigate

- [ ] Audit edge cases in SCD2 pattern (null handling, late-arriving records)
- [ ] Validate complex pattern interactions (e.g., SCD2 + FK validation)
- [ ] Test YAML validation for all error paths
- [ ] Review engine-specific SQL syntax divergences

### 1.2 Test Infrastructure

- [ ] Run `pytest --cov=odibi --cov-report=html` and identify modules < 80% coverage
- [ ] Add parametrized tests for transformers across all 3 engines
- [ ] Add edge-case tests: empty DataFrames, null-only columns, Unicode data

---

## Priority 2: Engine Parity

**Goal:** Every feature works identically on Pandas, Spark, and Polars

### 2.1 Parity Audit

Run `tests/engine/test_parity.py` and expand coverage:

| Transformer | Pandas | Spark | Polars | Notes |
|------------|--------|-------|--------|-------|
| `scd2` | ✓ | ✓ | ? | Verify Polars impl |
| `pivot` | ✓ | ✓ | ? | Polars syntax differs |
| `window_calculation` | ✓ | ✓ | ? | Frame spec differences |
| `normalize_json` | ✓ | ✓ | ? | Struct handling |

### 2.2 SQL Abstraction

- [ ] Document Spark SQL vs DuckDB SQL differences in `get_engine_differences` MCP tool
- [ ] Add compatibility layer for common divergences (e.g., `REGEXP_REPLACE` syntax)
- [ ] Test all 52 transformers on Polars engine

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
- [ ] Improve traceback cleaning for node execution errors

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

## Immediate Actions (Next Sprint)

### Week 1: Core Coverage Gaps

1. **Pandas engine tests** (`engine/pandas_engine.py` at 25%)
   - Cover DuckDB SQL execution paths
   - Test chunked reading/writing
   - This is the primary development engine

2. **Polars engine tests** (`engine/polars_engine.py` at 7%)
   - Add mock-based tests for SQL operations
   - Verify lazy vs eager execution

3. ~~**Spark engine tests**~~ - Already covered via mocks + Databricks validation

### Week 2: Integration & Transformers

4. **Merge transformer** (`transformers/merge_transformer.py` at 36%)
   - Test all merge modes (insert, update, upsert)
   - Test edge cases (empty source, all updates)

5. **Advanced transformers** (44% coverage)
   - Parameterized tests across engines

### Week 3: State & Node

6. **State management** (`state/__init__.py` at 33%)
   - HWM persistence, checkpointing

7. **Node execution** (`node.py` at 44%)
   - Error handling, retry logic, traceback cleaning

### Week 4: Polish

8. **Expand `diagnose_error`** MCP tool with 10+ error patterns
9. **Review GitHub issues** and close stale ones
10. **Validate all examples** in `examples/` directory

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
- [ ] Document Spark/Databricks testing approach in `docs/tutorials/spark_engine.md`
- [x] Update `docs/features/engines.md` with engine-specific testing notes ✓
- [ ] Add "How to run the test campaign" section to AGENTS.md or CONTRIBUTING.md

---

## Success Metrics

| Goal | Target | Measurement |
|------|--------|-------------|
| Test coverage | 80%+ | `pytest --cov` |
| CI pass rate | 100% | GitHub Actions |
| Polars parity | 100% | Engine parity tests |
| Example validation | 100% | All examples run |
| Issue backlog | < 10 open | GitHub Issues |

---

## Long-term Vision

Odibi should be:
- **Declarative**: YAML-first, SQL-based transformations
- **Portable**: Works on laptop (Pandas), cluster (Spark), or serverless (Polars)
- **Stable**: Comprehensive tests, predictable behavior
- **Documented**: AI assistants can generate correct configs without trial and error

The ultimate test: Can you hand a business analyst the docs and have them build a working pipeline without your help?
