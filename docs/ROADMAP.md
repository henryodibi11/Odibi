# Odibi Roadmap

## Current State (v3.4.3)

*Last updated: 2026-04-29*

| Metric | Value |
|--------|-------|
| Tests | 4,832+ collected (expanding — see AGENTS.md for per-module counts) |
| **Test Coverage** | **80%** (34,363 stmts, 6,854 missed) |
| Transformers | 54 |
| Patterns | 6 (Dimension, Fact, SCD2, Merge, Aggregation, Date Dimension) |
| Engines | 3 (Pandas/DuckDB, Spark, Polars) |
| Open Issues | 18 (3 bugs, 6 docs, 5 enhancements, 4 feature requests) |
| Python Support | 3.9-3.12 |

### Coverage by Module (Low Priority = >80%, Medium = 50-80%, High = <50%)

**Note on Spark Coverage:** The 4% for Spark is a CI artifact — Spark tests are skipped in CI (no JVM). Spark is tested via mock-based tests in `tests/integration/test_patterns_spark_mock.py` and validated in production on Databricks.

| Module | Coverage | Priority | Notes |
|--------|----------|----------|-------|
| `engine/spark_engine.py` | 4% | OK | CI-skipped; mock-tested + Databricks validated |
| `diagnostics/delta.py` | 13% | OK | Hard skip — Delta/Spark-bound |
| `pipeline.py` | 35% | HIGH | Utility methods covered; run() + PipelineManager uncovered (~1000 lines) |
| `transformers/scd.py` | 49% | HIGH | Pandas/DuckDB paths covered; Spark/Delta MERGE uncovered |
| `patterns/date_dimension.py` | 57% | MEDIUM | Pandas paths covered; Spark paths remaining |
| `engine/polars_engine.py` | 57% | MEDIUM | Up from 7% — major improvement |
| `transformers/merge_transformer.py` | 61% | MEDIUM | Pandas/DuckDB merge covered; Spark uncovered |
| `patterns/aggregation.py` | 63% | MEDIUM | validate + Pandas aggregate covered; Spark paths remaining |
| `transformers/delete_detection.py` | 64% | MEDIUM | Pandas paths covered; Spark skipped |
| `validation/engine.py` | 67% | MEDIUM | Bug fixed; 11 test types × Pandas + Polars; Spark `_validate_spark` remaining |
| `writers/sql_server_writer.py` | 67% | MEDIUM | 208 tests; all non-Spark paths covered |
| `transformers/manufacturing.py` | 67% | MEDIUM | Polars paths fully covered; Spark remaining |
| `transformers/relational.py` | 67% | MEDIUM | Join/union/pivot covered; Spark paths remaining |
| `validation/quarantine.py` | 69% | MEDIUM | Pandas + Polars covered; Spark remaining |
| `patterns/dimension.py` | 70% | MEDIUM | SCD0/1/2 Pandas covered; Spark remaining |
| `transformers/thermodynamics.py` | 71% | MEDIUM | CoolProp Pandas/Polars tested; Spark UDF remaining |
| `derived_updater.py` | 74% | LOW | Up from 52%; Pandas lifecycle + SQL Server covered; Spark remaining |
| `engine/pandas_engine.py` | 74% | LOW | Up from 62% — 118 new tests; Delta maintenance ops remaining |
| `patterns/fact.py` | 74% | LOW | Dimension lookups + quarantine covered; Spark remaining |
| `context.py` | 77% | LOW | Up from 62% |
| `transformers/units.py` | 78% | LOW | Pandas + Polars covered; Spark UDF remaining |
| `transformers/advanced.py` | 80% | ✅ | Up from 44%; Pandas sessionize/split fully covered |
| `state/__init__.py` | 80% | ✅ | Up from 33%; all backends + factories + sync covered |
| `catalog_sync.py` | 81% | ✅ | Up from 43%; 88 tests; SQL Server + Delta sync covered |
| `connections/factory.py` | 81% | ✅ | Up from 69%; all factory functions + auth auto-detection |
| `node.py` | 81% | ✅ | Up from 51%; 89 tests; execution paths + error handling covered |
| `story/generator.py` | 83% | ✅ | Up from 54%; 100 tests; generate + cleanup + remote paths |
| `catalog.py` | 85% | ✅ | Up from 47%; 126 tests; Pandas + Spark branches via mocks |
| `introspect.py` | 88% | ✅ | 55 tests; all rendering + module discovery covered |
| `connections/azure_adls.py` | 89% | ✅ | Up from 10%; 126 total tests; discovery + auth covered |
| `cli/system.py` | 89% | ✅ | 42 tests; all commands + helper functions covered |
| `story/doc_generator.py` | 90% | ✅ | Up from 55%; 117 tests; all rendering helpers covered |
| `connections/azure_sql.py` | 93% | ✅ | 87 tests; all methods + auth modes covered |
| `transformers/sql_core.py` | 93% | ✅ | 31 tests; 16 SQL transformers covered |
| `connections/api_fetcher.py` | 94% | ✅ | Up from 79%; 126 total tests; all pagination strategies |
| `cli/catalog.py` | 94% | ✅ | Up from 46%; 77 tests; all query commands + sync covered |
| `validation/gate.py` | 94% | ✅ | 32 tests; evaluate_gate + row count checks covered |
| `utils/setup_helpers.py` | 94% | ✅ | 22 tests; KeyVault + parallel connections covered |
| `diagnostics/manager.py` | 95% | ✅ | |
| `lineage.py` | 96% | ✅ | 71 tests; OpenLineageAdapter + LineageTracker fully covered |
| `connections/postgres.py` | 96% | ✅ | 85 tests; all methods + error suggestions covered |
| `cli/story.py` | 96% | ✅ | 44 tests; all commands covered |
| `cli/test.py` | 97% | ✅ | 41 tests; slugify + run_test_case + test_command loop |
| `cli/init_pipeline.py` | 97% | ✅ | 38 tests; interactive prompts + force overwrite |
| `cli/list_cmd.py` | 97% | ✅ | 41 tests; all list/explain commands |
| `cli/main.py` | 98% | ✅ | 70 tests; all commands + scaffold dispatch |
| `connections/local.py` | 98% | ✅ | 64 tests; all methods covered |
| `story/lineage.py` | 98% | ✅ | 70 tests; LineageGenerator fully covered |
| `graph.py` | 98% | ✅ | |
| `utils/logging_context.py` | 98% | ✅ | |
| `diagnostics/diff.py` | 99% | ✅ | |
| `tools/adf_profiler.py` | 99% | ✅ | 70 tests; all REST API + report generation covered |
| `utils/progress.py` | 100% | ✅ | 60 tests; Rich + plain text paths |
| `cli/lineage.py` | 100% | ✅ | 27 tests; all functions fully covered |
| `cli/schema.py` | 100% | ✅ | 27 tests; all functions fully covered |
| `story/lineage_utils.py` | 100% | ✅ | 62 tests; all 4 functions fully covered |
| `testing/source_pool.py` | 100% | ✅ | 44 tests; all Pydantic models + validators |
| `transformers/__init__.py` | 100% | ✅ | register_standard_library fully tested |
| `transformers/validation.py` | 100% | ✅ | |

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

### 1.2 Test Infrastructure — ✅ TARGET REACHED (80%)

- [x] Run `pytest --cov=odibi --cov-report=html` and identify modules < 80% coverage ✓
- [x] Establish strict 5-point fix checklist (ruff check, ruff format, pytest, no conftest changes, correct @patch targets) ✓
- [ ] Add parametrized tests for transformers across all 3 engines
- [ ] Add edge-case tests: empty DataFrames, null-only columns, Unicode data
- [x] Increase `catalog.py` coverage from 47% to 80%+ ✓ (now 85%, 126 tests)
- [x] Increase `node.py` coverage from 51% to 80%+ ✓ (now 81%, 89 tests)

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

## Priority 6: Agent Hardening Campaign

**Goal:** Ensure AI agents produce correct, first-attempt code when working on odibi

See [`docs/AGENT_CAMPAIGN.md`](AGENT_CAMPAIGN.md) for the full campaign plan — structured prompts, validation tasks, and acceptance criteria for agent reliability.

---

## Next Actions

### Remaining Coverage Gaps (Spark paths dominate)

Most modules below 80% are there because Spark-specific branches are untested in CI (no JVM). The remaining coverage work is primarily:

1. **pipeline.py** (35%) — `Pipeline.run()` execution paths, `PipelineManager.from_yaml/__init__/validate/run_node` (~1000 lines, mostly integration-level)
2. **transformers/scd.py** (49%) — `_scd2_spark` and Delta MERGE paths
3. **patterns/date_dimension.py** (57%) — Spark generation paths
4. **writers/sql_server_writer.py** (67%) — Spark-only paths (~466 stmts)

### Completed (previously planned)

- ~~Pandas engine tests (25% → 74%)~~ ✅
- ~~Polars engine tests (7% → 57%)~~ ✅
- ~~Merge transformer (36% → 61%)~~ ✅
- ~~Advanced transformers (44% → 80%)~~ ✅
- ~~State management (33% → 80%)~~ ✅
- ~~Review GitHub issues and close stale ones~~ ✅ (43 bugs closed)
- ~~catalog.py (47% → 85%)~~ ✅ (126 tests)
- ~~node.py (51% → 81%)~~ ✅ (89 tests)
- ~~catalog_sync.py (43% → 81%)~~ ✅ (88 tests)
- ~~story/generator.py (54% → 83%)~~ ✅ (100 tests)
- ~~story/doc_generator.py (55% → 90%)~~ ✅ (117 tests)
- ~~connections/azure_adls.py (10% → 89%)~~ ✅ (126 tests)
- ~~connections/api_fetcher.py (79% → 94%)~~ ✅ (126 tests)
- ~~cli/catalog.py (46% → 94%)~~ ✅ (77 tests)
- ~~connections/factory.py (69% → 81%)~~ ✅ (25 tests)

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
| Test coverage | 80%+ | 80% (34,363 stmts, 6,854 missed) | ✅ Done |
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
