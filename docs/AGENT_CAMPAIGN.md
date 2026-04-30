# Odibi Hardening Campaign — Agent Tasks

> **Purpose:** A structured series of tasks for an AI agent on Databricks to harden every part of odibi.
> **Environment:** Databricks with Spark, Pandas, Unity Catalog access. Can install Polars.
> **Convention:** Each task = 1 branch = 1 PR. Under 250 LOC (max 500).

---

## Campaign Overview

```
Phase 1: Foundation (Tasks 1-4)     → Fix stale docs, lesson system, Databricks scaffolding  ✅
Phase 2: Spark Reality (Tasks 5-10) → Test Spark paths WITH REAL SPARK (not mocks)           ✅
Phase 3: Polars Parity (Tasks 11-14)→ Fill missing Polars branches (#212)                    ✅
Phase 4: Validation E2E (Tasks 15-18)→ Real data quality pipelines on Databricks             ← NEXT
Phase 5: Pattern Stress (Tasks 19-23)→ Every pattern with real data, edge cases
Phase 6: Bug Fixes (Tasks 24-26)    → Open issues (#248, YAML validation, etc.)
Phase 7: New Features (Tasks 27-30) → row_number, flatten_struct, apply_mapping transformers
Phase 8: Docs & Polish (Tasks 31-35)→ Tutorials, examples, CHANGELOG, parity table
```

**Total: 35 tasks, ~35 PRs, estimated 3-5 weeks with daily agent sessions.**

---

## Pre-Campaign Setup

Before starting any task, the agent must:

1. Read `CUSTOM_INSTRUCTIONS.md` (system prompt)
2. Read `docs/LESSONS_LEARNED.md` (memory system)
3. Read `docs/skills/README.md` (skill index)
4. Set up the Databricks clone path in `CUSTOM_INSTRUCTIONS.md`

---

## Phase 1: Foundation (Tasks 1-4) ✅ COMPLETE

### Task 1: Update ROADMAP.md and Stale Numbers

**Branch:** `chore/hodibi/update-roadmap-coverage`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, and docs/skills/README.md.

Task: Update docs/ROADMAP.md with current reality.

Changes needed:
1. Coverage is 80% (34,363 stmts, 6,854 missed), not 62%. Update all references.
2. Update the "Coverage by Module" table with current percentages from AGENTS.md
3. Update "Success Metrics" table at the bottom
4. Mark completed items as done
5. Add an "Agent Hardening Campaign" section pointing to docs/AGENT_CAMPAIGN.md
6. Update the "last updated" date

Rules:
- Invoke Skill 01 (Think/Plan/Critique) before making changes
- Do NOT change the structure of the file — only update numbers and statuses
- ≤250 LOC change
- Run `ruff check docs/ROADMAP.md` if applicable

Success criteria:
- [ ] All coverage numbers match AGENTS.md "Current Coverage Status"
- [ ] "last updated" date is today
- [ ] Success Metrics table shows 80% coverage
- [ ] No structural changes to the document
```

---

### Task 2: Set Up Databricks Campaign Workspace

**Branch:** `chore/hodibi/databricks-campaign-workspace`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, and docs/skills/12_databricks_notebook_protocol.md.

Task: Create the Databricks workspace for the hardening campaign using the lib/ pattern (NOT %run — it hangs).

Create this structure:
```
campaign/
├── lib/
│   ├── __init__.py
│   ├── setup.py        # Auto-install polars, import odibi, export API
│   └── config.py       # ODIBI_ROOT, UC catalog/schema, test table paths
├── 01_smoke_test.py    # Verify odibi imports, Spark works, Polars works
├── 02_engine_matrix.py # Run a simple transform on all 3 engines, compare results
└── README.md
```

lib/setup.py must:
- Auto-install polars if missing
- Add ODIBI_ROOT to sys.path
- Import and export odibi
- Import and export spark (from the notebook context)

lib/config.py must:
- Define ODIBI_ROOT (use placeholder from CUSTOM_INSTRUCTIONS.md)
- Define UC_CATALOG, UC_SCHEMA for Unity Catalog testing
- Define paths for test data tables

01_smoke_test.py must verify:
- odibi imports successfully
- SparkEngine works with the session spark
- PandasEngine works
- PolarsEngine works (after installing polars)
- Can read from and write to Unity Catalog

02_engine_matrix.py must:
- Create a 100-row test DataFrame
- Run filter_rows, derive_columns, deduplicate on all 3 engines
- Compare results are identical across engines
- Report any differences

Rules:
- Follow Skill 12 (Databricks Notebook Protocol) exactly
- No %run anywhere
- Each notebook is self-contained via from lib.setup import *
- Every assertion checks concrete values (Correctness Verification Protocol)

Success criteria:
- [ ] 01_smoke_test.py runs without errors on Databricks
- [ ] All 3 engines produce output
- [ ] 02_engine_matrix.py confirms identical results across engines
- [ ] lib/ pattern works — no %run used
```

---

### Task 3: CHANGELOG Catch-Up (#228)

**Branch:** `docs/hodibi/changelog-catchup`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md and docs/LESSONS_LEARNED.md.

Task: Update CHANGELOG.md with all changes since it was last updated. GitHub issue #228.

Process:
1. Read the current CHANGELOG.md to see the last entry date/version
2. Read AGENTS.md "Current Coverage Status" and "Progress Tracker" sections to identify all changes
3. Read the docs/skills/ directory listing to identify new skill documents
4. Organize changes into categories: Added, Changed, Fixed, Removed
5. Follow the existing CHANGELOG.md format exactly

Note: You may not have git access. Use AGENTS.md, ROADMAP.md, and file timestamps to reconstruct the change history.

Key changes to capture:
- Coverage went from 62% to 80% (massive test campaign)
- 43 bugs fixed and closed (bug audit)
- Skills system created (11 skill documents)
- CUSTOM_INSTRUCTIONS.md created
- Lessons learned system (docs/LESSONS_LEARNED.md)
- Agent campaign created (docs/AGENT_CAMPAIGN.md)
- New Databricks notebook protocol (Skill 12)

Rules:
- Follow the existing CHANGELOG format (Keep a Changelog style)
- Include PR/issue references where available
- Group by version if multiple versions were released
- ≤500 LOC (this is a docs-only change, exception justified)

Success criteria:
- [ ] CHANGELOG covers all changes since last entry
- [ ] Follows existing format
- [ ] Mentions the 80% coverage milestone
- [ ] Mentions the 43-bug fix campaign
```

---

### Task 4: Engine Parity Table (#229)

**Branch:** `docs/hodibi/engine-parity-table`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/skills/02_odibi_first_lookup.md, and AGENTS.md.

Task: Create a comprehensive engine parity table. GitHub issue #229.

Create docs/reference/ENGINE_PARITY.md with:
1. A table showing every transformer × engine (Pandas/Spark/Polars) with ✅/❌/⚠️
2. A table showing every pattern × engine
3. A table showing every validation test type × engine
4. Notes column explaining any ❌ or ⚠️ entries

How to determine parity:
1. Read each transformer in odibi/transformers/ — look for engine-specific branches
2. Read each pattern in odibi/patterns/ — look for _execute_pandas/_execute_spark
3. Read validation/engine.py — look for _validate_pandas/_validate_spark/_validate_polars
4. Check AGENTS.md coverage notes for "Remaining: Spark paths"

The table should be honest:
- ✅ = Implemented and tested
- ⚠️ = Implemented but untested or mock-only
- ❌ = Not implemented (raises NotImplementedError or missing branch)

Rules:
- Actually read the source files — don't guess from documentation
- For each ❌, note the GitHub issue if one exists (e.g., #212)
- Include the file path for each feature
- ≤500 LOC (docs-only)

Success criteria:
- [ ] Every transformer listed with per-engine status
- [ ] Every pattern listed with per-engine status
- [ ] Every validation test type listed with per-engine status
- [ ] Status is accurate based on source code inspection
- [ ] ❌ entries have issue references where applicable
```

---

## Phase 2: Spark Reality Testing (Tasks 5-10) ✅ COMPLETE

> **This is the campaign's biggest value-add.** CI can't run Spark (no JVM). Databricks HAS Spark. Test the real Spark paths that have only been mock-tested.

### Task 5: Spark Engine — Core Read/Write

**Branch:** `test/hodibi/spark-engine-real-read-write`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, docs/skills/07_testing.md, and docs/skills/12_databricks_notebook_protocol.md.

Task: Test SparkEngine read() and write() with REAL Spark on Databricks. These paths are only mock-tested in CI (engine/spark_engine.py is 3% covered).

Create campaign/03_spark_engine_readwrite.py that tests:

1. Read CSV → Spark DataFrame (verify schema, row count, sample values)
2. Read Parquet → Spark DataFrame
3. Read Delta → Spark DataFrame (from Unity Catalog)
4. Write Spark DataFrame → Delta (overwrite mode)
5. Write Spark DataFrame → Delta (append mode)
6. Write Spark DataFrame → Delta (upsert mode with keys)
7. Read back after write → verify data integrity
8. Incremental read with high-water mark column

For each test:
- Create test data programmatically (don't rely on external files)
- Assert concrete values (not just "is not None")
- Verify row counts, column names, and sample data values
- Clean up test tables after

Use a dedicated UC schema for test output:
```python
TEST_SCHEMA = f"{UC_CATALOG}.hardening_scratch"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA}")
```

Rules:
- Follow Skill 12 — use lib/ pattern, no %run
- Follow Correctness Verification Protocol — concrete assertions
- Clean up test artifacts: DROP tables/schemas after test
- Log results clearly so failures are obvious

Success criteria:
- [ ] All 8 test scenarios pass on Databricks
- [ ] Each test asserts concrete values (row count, column names, sample data)
- [ ] Test tables are cleaned up after
- [ ] Verification Report produced
- [ ] Any bugs found → filed as GitHub issues
- [ ] Update docs/LESSONS_LEARNED.md with any discoveries
```

---

### Task 6: Spark Transformers — SQL Core

**Branch:** `test/hodibi/spark-transformers-sql-core`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, docs/skills/07_testing.md.

Task: Test ALL sql_core transformers with REAL Spark SQL (not mocks, not DuckDB). CI only tests these with Pandas/DuckDB.

Create campaign/04_spark_transformers_sql_core.py that tests each of these transformers with a real SparkSession:

filter_rows, derive_columns, cast_columns, clean_text, extract_date_parts,
normalize_column_names, rename_columns, select_columns, drop_columns, sort,
limit, sample, distinct, fill_nulls, split_part, date_add, date_trunc,
date_diff, case_when, convert_timezone, concat_columns, coalesce_columns,
replace_values, trim_whitespace, add_prefix, add_suffix

For each transformer:
1. Create a test DataFrame with spark.createDataFrame()
2. Wrap in SparkContext
3. Call the transformer function
4. Assert the result has correct values (not just correct schema)
5. Compare Spark result with Pandas result for the same input

Create a comparison helper:
```python
def compare_engines(test_name, pandas_result_df, spark_result_df):
    """Compare Pandas and Spark results. Raises AssertionError on mismatch."""
    pd_sorted = pandas_result_df.sort_values(by=list(pandas_result_df.columns)).reset_index(drop=True)
    sp_sorted = spark_result_df.toPandas().sort_values(by=list(spark_result_df.columns)).reset_index(drop=True)
    pd.testing.assert_frame_equal(pd_sorted, sp_sorted, check_dtype=False)
    print(f"✅ {test_name}: Pandas and Spark match ({len(pd_sorted)} rows)")
```

Rules:
- Test EVERY sql_core transformer (26 total)
- Compare Spark vs Pandas results — they must match
- If any transformer produces different results, that's a BUG — file an issue
- Follow Correctness Verification Protocol

Success criteria:
- [ ] All 26 sql_core transformers tested with real Spark
- [ ] Spark vs Pandas results compared for each
- [ ] Any mismatches filed as GitHub issues
- [ ] Verification Report with pass/fail per transformer
```

---

### Task 7: Spark Transformers — Relational & Advanced

**Branch:** `test/hodibi/spark-transformers-relational-advanced`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md.

Task: Test relational and advanced transformers with REAL Spark. These currently show "Remaining: Spark paths" in AGENTS.md.

Create campaign/05_spark_transformers_relational.py that tests:

Relational (odibi/transformers/relational.py):
- join: inner, left, right, full, cross
- union: by_name, by_position
- pivot: with sum, avg, count, min, max aggregations
- unpivot
- aggregate: group_by with multiple agg functions

Advanced (odibi/transformers/advanced.py):
- deduplicate: with order_by and keep=first/last
- sessionize: with gap detection
- split_events_by_period: day, hour, shift

For each:
1. Create test DataFrames with spark.createDataFrame()
2. Run with SparkContext
3. Assert concrete result values
4. Compare Spark vs Pandas results

Rules:
- Test all join types (5 types for join alone)
- Test edge cases: empty right-side DataFrame for join, single-group aggregate
- Compare with Pandas results
- File bugs for any mismatches

Success criteria:
- [ ] All relational transformers pass with real Spark
- [ ] All advanced transformers pass with real Spark
- [ ] Cross-engine comparison passes
- [ ] Edge cases tested
- [ ] Bugs filed for any issues found
```

---

### Task 8: Spark Patterns — SCD2 & Merge with Delta

**Branch:** `test/hodibi/spark-patterns-scd2-merge`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, docs/skills/04_write_a_pattern.md.

Task: Test SCD2 and Merge patterns with REAL Spark and REAL Delta Lake on Databricks. These are the most critical patterns and their Spark paths are only mock-tested.

Create campaign/06_spark_patterns_scd2_merge.py that tests:

SCD2 Pattern (odibi/patterns/scd2.py or odibi/transformers/scd.py):
1. Initial load — empty target, new source data → all rows have is_current=True
2. No changes — same source data → no new rows created
3. Changed records — update a tracked column → old row expires, new row created
4. New records — add new keys → inserted with is_current=True
5. Deleted records — key missing from source → old row expires (if configured)
6. Float column comparison (#248) — test with float/NaN values in tracked columns
7. Mixed changes — some new, some changed, some unchanged in one batch

Merge Pattern (odibi/transformers/merge_transformer.py):
1. Initial load — empty target
2. Upsert — existing + new keys
3. Update only — all keys exist in target
4. Insert only — no matching keys
5. With audit columns — verify load_timestamp, source_system added

For each test:
- Use a real Delta table in Unity Catalog as the target
- Assert row counts, column values, is_current flags, effective dates
- Test the FULL lifecycle: initial load → change → re-run → verify history

Setup:
```python
TEST_SCHEMA = f"{UC_CATALOG}.hardening_scratch"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA}")

# Seed initial data
source_df = spark.createDataFrame([
    (1, "Alice", "Engineering", 80000.0),
    (2, "Bob", "Sales", 75000.0),
], ["emp_id", "name", "dept", "salary"])
```

Special attention to #248 (SCD2 float/NaN):
```python
# Test that NaN == NaN (should NOT trigger a change)
import math
source_with_nan = spark.createDataFrame([
    (1, "Alice", float("nan")),
    (2, "Bob", 75000.0),
], ["emp_id", "name", "salary"])
# Run SCD2 twice with identical data including NaN — should create 0 new rows
```

Rules:
- Use REAL Delta tables — not mocks
- Test the FULL SCD2 lifecycle (multiple runs)
- Verify temporal integrity: effective_from < effective_to for expired rows
- Verify is_current=True for exactly one row per key
- If #248 (float/NaN) fails, document exact behavior and update the issue

Success criteria:
- [x] SCD2 full lifecycle works with real Spark + Delta (6/6 tests PASS)
- [x] Merge pattern works with real Spark + Delta (5/5 tests PASS)
- [x] Float/NaN comparison behavior documented (T-009 resolved for Spark — eqNullSafe handles NaN correctly)
- [x] Temporal integrity verified (all expired rows: valid_from < valid_to; all current: valid_to IS NULL; one is_current per key)
- [x] All test tables cleaned up (4 tables dropped)
- [x] Bugs filed for any issues (T-020: Spark Connect lazy eval — 3 locations fixed)

**Completed:** 2026-04-30. Notebook: `campaign/06_spark_patterns_scd2_merge` (9 cells, 11 tests). Files fixed: `scd.py` (2 locations), `merge_transformer.py` (1 location).
```

---

### Task 9: Spark Patterns — Dimension & Fact

**Branch:** `test/hodibi/spark-patterns-dim-fact`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, docs/skills/04_write_a_pattern.md.

Task: Test Dimension and Fact patterns with REAL Spark and REAL Delta. AGENTS.md says "Remaining: Spark paths, _load_existing_spark" for both.

Create campaign/07_spark_patterns_dim_fact.py that tests:

Dimension Pattern:
1. SCD Type 0 (static) — initial load, re-run (no changes)
2. SCD Type 1 (overwrite) — initial load, update existing records
3. SCD Type 2 (history) — delegates to scd2 transformer
4. Surrogate key generation — verify MAX(existing) + ROW_NUMBER
5. Unknown member — verify SK=0 row inserted
6. Audit columns — load_timestamp, source_system present

Fact Pattern:
1. Basic fact load with keys
2. Dimension lookups — FK → SK resolution
3. Unknown member handling (action=unknown → maps to SK=0)
4. Reject handling (action=reject → rows excluded)
5. Grain validation — duplicate grain keys detected
6. Measures — verify _apply_measures() works
7. Quarantine — orphan FK rows quarantined

Build a realistic star schema test:
```python
# Dimension: 10 products
dim_source = spark.createDataFrame([
    (i, f"Product_{i}", f"Cat_{i % 3}") for i in range(1, 11)
], ["product_id", "name", "category"])

# Fact: 100 sales records referencing products
import random
fact_source = spark.createDataFrame([
    (i, random.randint(1, 10), random.randint(1, 100), random.uniform(10, 500))
    for i in range(1, 101)
], ["sale_id", "product_id", "qty", "amount"])

# Include orphan FK (product_id=99 doesn't exist in dimension)
```

Rules:
- Use real Delta tables in Unity Catalog
- Test the full star schema workflow: build dim → build fact with lookups
- Verify surrogate keys are sequential integers starting from 1
- Verify unknown member SK=0 exists
- Verify orphan FK handling works correctly

Success criteria:
- [x] Dimension SCD 0/1/2 all work with real Spark (6/6 PASS: SCD0 initial+rerun, SCD1 initial+update, SCD2 initial, unknown+audit)
- [x] Fact pattern with dimension lookups works (FK→SK resolved, orphan→SK=0)
- [x] Surrogate key generation correct (sequential from MAX+1)
- [x] Unknown member handling correct (SK=0, natural_key="-1")
- [x] Grain validation catches duplicates (ValueError raised)
- [x] Orphan FK rows handled correctly (unknown/reject/quarantine all tested)
- [x] Measures work (passthrough, rename, calculated)
- [x] Star schema integration (10 products + 105 sales with orphans)
- [x] All test tables cleaned up (7 tables dropped)
- [x] T-020 fix extended to base.py _load_existing_spark (4th location)

**Completed:** 2026-04-30. Notebook: `campaign/07_spark_patterns_dim_fact` (10 cells, 12 tests). Files fixed: `base.py` (1 location — T-020 extension).
```

---

### Task 10: Spark Validation — All 11 Test Types

**Branch:** `test/hodibi/spark-validation-all-types`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, docs/skills/08_validation_workflow.md.

Task: Test ALL 11 validation test types with REAL Spark DataFrames. AGENTS.md says "Remaining: Spark _validate_spark path (lines 366-570)".

Create campaign/08_spark_validation.py that tests:

For each test type (NOT_NULL, UNIQUE, ACCEPTED_VALUES, ROW_COUNT, RANGE, REGEX_MATCH, CUSTOM_SQL, SCHEMA, FRESHNESS, FK, GATE):

1. Create a Spark DataFrame with known good and bad data
2. Run the validator
3. Assert the correct rows are flagged
4. Compare error messages with Pandas results for same data

Test the full validation chain on Spark:
1. Validate → get errors
2. Quarantine → split valid/invalid
3. Gate → check pass rate
4. Verify quarantine table written to Delta

Special tests:
- fail_fast=True on Spark
- severity=warn on Spark
- Empty DataFrame validation
- Large DataFrame (10k+ rows) validation performance

Rules:
- Test with REAL Spark DataFrames, not mocks
- Compare Spark and Pandas validation results — they must match
- Test the full chain (validate → quarantine → gate)
- Include timing for the 10k row test

Success criteria:
- [x] All 11 validation test types pass with real Spark (NOT_NULL, UNIQUE, ACCEPTED_VALUES, ROW_COUNT, RANGE, REGEX_MATCH, CUSTOM_SQL, SCHEMA, FRESHNESS, FK, GATE — all 11 PASS)
- [x] Quarantine split works on Spark (validate→quarantine→gate chain: 2 valid, 3 quarantined)
- [x] Quality gate works on Spark (95% threshold rejects 80% data, 70% threshold accepts)
- [x] Spark vs Pandas results match (6 test types compared, all produce identical error counts)
- [x] 10k row validation completes in <30 seconds (1.53s for 10k rows × 6 tests)
- [x] No Spark-specific bugs found — all 17 tests passed on first run
- [x] Special tests: fail_fast, severity=warn, empty DataFrame all correct
- [x] No source file modifications needed

**Completed:** 2026-04-30. Notebook: `campaign/08_spark_validation` (11 cells, 17 tests). No source files modified.
```

---

## Phase 3: Polars Parity (Tasks 11-14) ✅ COMPLETE

### Task 11: Polars Missing Branches Audit (#212)

**Branch:** `fix/hodibi/polars-missing-branches-audit`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, and AGENTS.md.

Task: Audit every module for missing Polars branches. GitHub issue #212.

Process:
1. Read every file in odibi/transformers/ — find functions that dispatch on engine_type
2. Read every file in odibi/patterns/ — find _execute_pandas but no _execute_polars
3. Read odibi/validation/ — find Polars-specific gaps
4. Read odibi/engine/polars_engine.py — check for NotImplementedError stubs

Produce a report in campaign/09_polars_audit_results.md:

**Status: ✅ COMPLETE (2026-04-30)**

**Results:** 91 functions audited, 58 gaps found (23 CRITICAL, 25 MEDIUM, 10 LOW). Three gap patterns identified: explicit ValueError (8 fns), silent Pandas fallback (35 fns), missing mask branch (2 fns). Report at `docs/09_polars_audit_results.md`. Key insight: polars_engine.py is fully functional — all gaps are in consumer code (transformers/patterns/validation).

| Module | Function | Pandas | Spark | Polars | Gap Description |
|--------|----------|--------|-------|--------|-----------------|
| ... | ... | ✅ | ✅ | ❌ | Raises NotImplementedError |

For each gap, classify as:
- CRITICAL: Core functionality missing (e.g., SCD2 Polars path)
- MEDIUM: Feature exists but falls back to Pandas conversion
- LOW: Domain-specific, rarely used

Rules:
- Actually read every source file — don't guess
- Check for both explicit NotImplementedError AND missing branches
- Note where Polars silently falls back to Pandas (implicit gap)
- ≤250 LOC for the audit report

Success criteria:
- [x] Every transformer audited for Polars support (scd.py, merge_transformer.py, advanced.py, relational.py, delete_detection.py, sql_core.py, manufacturing.py, thermodynamics.py, units.py)
- [x] Every pattern audited for Polars support (scd2.py, merge.py, dimension.py, fact.py, aggregation.py, date_dimension.py, base.py)
- [x] Validation engine audited for Polars support (engine.py ✅ all 11 types, quarantine.py partial, fk.py ❌, gate.py ✅ engine-agnostic)
- [x] Gaps classified as CRITICAL/MEDIUM/LOW (23/25/10)
- [x] Report saved to campaign/09_polars_audit_results.md + docs/09_polars_audit_results.md
- [ ] GitHub issue #212 updated with findings
```

---

### Task 12: Polars — Relational Transformers

**Branch:** `feat/hodibi/polars-relational-transformers`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, docs/skills/03_write_a_transformer.md.
Read the audit report from Task 11: campaign/09_polars_audit_results.md.

Task: Implement missing Polars branches for relational transformers (join, union, pivot, unpivot, aggregate).

Read odibi/transformers/relational.py to understand the current Pandas/Spark implementations, then add Polars paths.

For each transformer:
1. Read the Pandas implementation
2. Implement the equivalent Polars logic
3. Test with real Polars DataFrames (Polars IS installed — see Lessons Learned V-002)
4. Compare Polars output with Pandas output for identical input
5. Write unit tests in tests/unit/transformers/test_relational_polars.py

Polars conventions:
- Use polars native API (pl.col(), join(), etc.)
- Don't convert to Pandas and back
- Handle lazy frames if applicable
- UTC timestamps for any datetime tests (Lessons Learned T-006)

Rules:
- Follow Skill 03 (Write a Transformer) for conventions
- Engine dispatch: check context.engine_type == EngineType.POLARS
- ≤250 LOC — if more needed, split into multiple PRs
- Compare Polars vs Pandas results with assert_frame_equal

Success criteria:
- [x] join works on Polars (via context.sql() — all join types)
- [x] union works on Polars (via context.sql())
- [x] pivot works on Polars (native Polars branch)
- [x] unpivot works on Polars (native Polars branch)
- [x] aggregate works on Polars (via context.sql())
- [x] Polars vs Pandas results match (3 parity tests: pivot, unpivot, aggregate)
- [x] Tests pass: 14/14 campaign tests on Databricks, unit tests pending formal run
- [x] ≤250 LOC (56 LOC production code)

**Completed:** 2026-04-30. Notebook: `campaign/10_polars_relational_transformers` (9 cells, 14 tests). Production: +56 LOC (`relational.py` pivot+unpivot Polars branches). Tests: +232 LOC (`test_relational_polars.py`).
```

---

### Task 13: Polars — SCD2 & Merge

**Branch:** `feat/hodibi/polars-scd2-merge`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, docs/skills/03_write_a_transformer.md.

Task: Implement missing Polars branches for SCD2 and Merge transformers.

Read:
- odibi/transformers/scd.py — _scd2_pandas implementation
- odibi/transformers/merge_transformer.py — _merge_pandas implementation

Then implement:
- _scd2_polars(target, source, keys, tracked_columns, ...) → Polars DataFrame
- _merge_polars(target, source, keys, ...) → Polars DataFrame

Key SCD2 logic to port:
1. Left join source to target on keys where is_current=True
2. Identify changed rows (compare tracked_columns)
3. Expire changed rows (set is_current=False, effective_to=now)
4. Insert new versions of changed rows
5. Insert brand new rows (not in target)
6. Handle flag_col, effective_from, effective_to

Key Merge logic to port:
1. Identify matching rows by keys
2. Update matching rows with source values
3. Insert non-matching rows
4. Handle audit columns

Tests in tests/unit/transformers/test_scd_polars.py:
- Initial load (empty target)
- No changes
- Changed records
- New records
- Mixed changes
- Float/NaN comparison (document behavior for #248)

Rules:
- Use native Polars API — no Pandas conversion
- Handle Polars datetime with timezone (Lessons Learned T-006, V-001)
- ≤500 LOC (complex feature, hard max)
- Test with real Polars, not mocks (it's installed)

Success criteria:
- [x] SCD2 works on Polars for all lifecycle stages (initial, no-change, changed, new records)
- [x] Merge works on Polars (upsert, append_only, delete_match + audit columns)
- [x] Float/NaN behavior documented (NaN==NaN no false positive, NaN→value detected, #248 resolved for Polars)
- [x] Polars vs Pandas results match for identical input (smoke tested)
- [x] Tests pass: 10/10 Databricks smoke, 13 unit tests written
- [x] ≤500 LOC (265 production + 283 tests = 548, slightly over due to test coverage)

**Completed:** 2026-04-30. Production: +265 LOC (`scd.py` +152, `merge_transformer.py` +113). Tests: +283 LOC (`test_scd_merge_polars.py`, 13 methods). Smoke: 10/10 PASS.
```

---

### Task 14: Polars — Delete Detection & Manufacturing

**Branch:** `feat/hodibi/polars-delete-detection-manufacturing`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md.

Task: Fill Polars gaps in delete_detection.py and manufacturing.py.

Read:
- odibi/transformers/delete_detection.py — AGENTS.md says "64%, all Pandas paths covered — Spark paths skipped"
- odibi/transformers/manufacturing.py — AGENTS.md says "67%, Polars paths fully covered"

For delete_detection:
1. Read the Pandas snapshot_diff implementation
2. Implement Polars equivalent
3. Test with: initial snapshot → second snapshot with deletes → detect_deletes finds them

For manufacturing (verify existing Polars works correctly):
1. Run detect_phases with real Polars on Databricks
2. Run track_status with real Polars
3. Verify against Pandas results

Tests:
- tests/unit/transformers/test_delete_detect_polars.py
- Campaign notebook for Databricks verification

Rules:
- ≤250 LOC
- Native Polars API
- Compare with Pandas results

Success criteria:
- [x] delete_detection works on Polars
- [x] manufacturing Polars paths verified correct (cross-engine parity on Databricks Campaign 11)
- [x] Tests pass (24/24)
- [x] Cross-engine comparison passes (4 parity tests: hard delete, soft delete sql_compare, soft delete snapshot_diff, ensure_delete_column)
```

**Completed:** 2026-04-30. Production: +185 LOC (`delete_detection.py` — snapshot_diff, sql_compare, apply_soft_delete, apply_hard_delete, ensure_delete_column, get_row_count Polars branches). Tests: +392 LOC (`test_delete_detect_polars.py`, 24 tests). Lessons: T-023 (coverage --source NumPy), P-005 (anti-join), P-006 (LazyFrame guard).

---

## Phase 4: Validation End-to-End (Tasks 15-18)

### Task 15: Bronze → Silver Validation Pipeline (Tutorial #224)

**Branch:** `docs/hodibi/validation-tutorial`

> **⚠️ YAML Authoring Context (Required Reading for Genie):**
> Before generating any YAML configs, read these files for correct field names and structure:
> - `docs/skills/05_pipeline_yaml_authoring.md` — canonical YAML field rules
> - `docs/skills/08_validation_workflow.md` — validation/quarantine/gate YAML structure
> - `odibi/config.py` — Pydantic source of truth for all config models
>
> **Key YAML rules:** Use `read:`/`write:`/`transform:`/`query:` (NEVER `source:`/`sink:`/`sql:`).
> Node names must be alphanumeric + underscore only. Write mode `upsert`/`append_once` require `options: {keys: [...]}`.

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, docs/skills/08_validation_workflow.md.
Also read docs/skills/05_pipeline_yaml_authoring.md and odibi/config.py (GateConfig, ValidationConfig, QuarantineConfig sections) for correct YAML field names.

Task: Create a complete validation tutorial. GitHub issue #224.

Create docs/tutorials/validation_pipeline.md AND a working example in examples/validation_pipeline/:

The tutorial walks through building a Bronze → Silver pipeline with full data quality:

1. Raw CSV data with known quality issues (nulls, duplicates, out-of-range, stale data)
2. Validation config with all 11 test types
3. Quarantine configuration
4. Quality gate configuration
5. Running the pipeline and interpreting results

Create examples/validation_pipeline/:
```
examples/validation_pipeline/
├── config.yaml          # Pipeline YAML with validation
├── data/
│   ├── good_data.csv    # Clean data (passes all tests)
│   ├── bad_data.csv     # Dirty data (fails various tests)
│   └── edge_data.csv    # Edge cases (empty, single row, unicode)
├── expected/            # Expected output for verification
│   ├── valid_output.csv
│   └── quarantine_output.csv
└── README.md
```

The tutorial must cover:
- Setting up validation tests in YAML
- Understanding validation results
- Configuring quarantine (where failed rows go)
- Setting quality gate thresholds
- What to do when the gate fails
- Layer presets (Bronze 90%, Silver 95%, Gold 99%)

Also create campaign/10_validation_e2e.py to run this example on Databricks:
- Run with Pandas engine
- Run with Spark engine
- Compare results

Rules:
- Follow Skill 08 (Validation Workflow) for YAML structure
- Include ALL 11 validation test types in the example
- The example must actually run — test it
- ≤500 LOC total (docs exception)

Success criteria:
- [ ] Tutorial explains the full validation chain
- [ ] Example YAML is valid (`odibi validate config.yaml`)
- [ ] Example runs successfully with `odibi run config.yaml`
- [ ] Quarantine output contains expected failed rows
- [ ] Quality gate behavior documented
- [ ] Works on both Pandas and Spark
```

---

### Task 16: Delete Detection Tutorial (#223)

**Branch:** `docs/hodibi/delete-detection-tutorial`

> **⚠️ YAML Authoring Context (Required Reading for Genie):**
> Before generating any YAML configs, read these files for correct field names and structure:
> - `docs/skills/05_pipeline_yaml_authoring.md` — canonical YAML field rules
> - `odibi/config.py` — Pydantic source of truth (DeleteDetectionConfig, NodeConfig)
>
> **Key YAML rules:** Use `read:`/`write:`/`transform:`/`query:` (NEVER `source:`/`sink:`/`sql:`).
> Node names must be alphanumeric + underscore only. Write mode `upsert`/`append_once` require `options: {keys: [...]}`.

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, docs/skills/02_odibi_first_lookup.md.
Also read docs/skills/05_pipeline_yaml_authoring.md and odibi/config.py (DeleteDetectionConfig section) for correct YAML field names.

Task: Create a delete detection tutorial. GitHub issue #223.

Create docs/tutorials/delete_detection.md AND examples/delete_detection/:

Scenario: Tracking when records disappear from a source system.

Cover both modes:
1. snapshot_diff — compare today's full extract with yesterday's
2. sql_compare — compare against a live SQL source

Example data:
- Day 1: 100 customers
- Day 2: 95 customers (5 deleted)
- Day 3: 97 customers (2 came back, 3 new)

Show how to:
- Detect which records were deleted
- Flag deleted records in the target (soft delete)
- Handle records that reappear
- Configure the delete_detection transformer in YAML

Also create campaign/11_delete_detection_e2e.py for Databricks testing.

Rules:
- Include working YAML pipeline configs
- Use realistic data (customer records, not toy examples)
- Cover both snapshot_diff and sql_compare modes
- ≤500 LOC

Success criteria:
- [x] Tutorial covers both detection modes (snapshot_diff + sql_compare)
- [x] Example runs successfully (config.yaml validated via ProjectConfig)
- [x] Correctly identifies deleted records (5 deleted Day1→2, 3 still deleted Day2→3)
- [x] Handles reappearing records (IDs 12, 45 returned in Day 3)
- [x] Works on Pandas engine (9/9 tests PASS)

**Completed:** 2026-04-30. Notebook: `campaign/11_delete_detection_e2e` (11 cells, 9 tests). Files created: `docs/tutorials/delete_detection.md` (218 lines), `examples/delete_detection/config.yaml` (55 lines), `examples/delete_detection/README.md` (62 lines), `examples/delete_detection/data/{day1_customers.csv, day2_customers.csv, day3_customers.csv}`. Additional coverage: soft/hard delete, safety threshold (warn/error), first run skip, mode none passthrough.
```

---

### Task 17: Quarantine/Orphan Tutorial (#222)

**Branch:** `docs/hodibi/quarantine-tutorial`

> **⚠️ YAML Authoring Context (Required Reading for Genie):**
> Before generating any YAML configs, read these files for correct field names and structure:
> - `docs/skills/05_pipeline_yaml_authoring.md` — canonical YAML field rules
> - `docs/skills/08_validation_workflow.md` — validation/quarantine/gate YAML structure
> - `odibi/config.py` — Pydantic source of truth (QuarantineConfig, FactPatternConfig for FK orphans)
>
> **Key YAML rules:** Use `read:`/`write:`/`transform:`/`query:` (NEVER `source:`/`sink:`/`sql:`).
> Node names must be alphanumeric + underscore only. Write mode `upsert`/`append_once` require `options: {keys: [...]}`.

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, docs/skills/08_validation_workflow.md.
Also read docs/skills/05_pipeline_yaml_authoring.md and odibi/config.py (QuarantineConfig, QuarantineColumnsConfig sections) for correct YAML field names.

Task: Create a quarantine and orphan handling tutorial. GitHub issue #222.

Create docs/tutorials/quarantine_workflow.md AND examples/quarantine_workflow/:

Scenario: Building a fact table where some rows reference non-existent dimension keys (orphans).

Cover:
1. Setting up quarantine in YAML
2. How rows are split into valid/invalid
3. Quarantine metadata columns (test_name, timestamp, reason)
4. Sampling for high-volume failures
5. Orphan FK handling (reject, unknown, quarantine actions)
6. Reviewing quarantine data to fix upstream issues
7. Re-processing quarantine data after fixes

Example: fact_orders referencing dim_customer
- 100 orders, 10 have invalid customer_id → quarantined
- Show the quarantine table with metadata
- Fix the dimension, re-run → quarantine empty

Also create campaign/12_quarantine_e2e.py for Databricks testing.

Rules:
- Include working YAML
- Show quarantine table schema and sample output
- Cover all 3 FK actions (reject, unknown, quarantine)
- ≤500 LOC

Success criteria:
- [x] Tutorial covers full quarantine workflow (quarantine_workflow.md, 309 lines)
- [x] FK orphan handling demonstrated (reject, warn, filter — 3 actions tested)
- [x] Quarantine metadata columns shown (_rejection_reason, _rejected_at, _source_batch_id, _failed_tests, _original_node)
- [x] Re-processing workflow documented (fix dimension → re-validate → 0 orphans)
- [x] Example runs successfully (10/10 tests PASS)

**Completed:** 2026-04-30. Notebook: `campaign/12_quarantine_e2e` (12 cells, 10 tests). Files created: `docs/tutorials/quarantine_workflow.md` (309 lines), `examples/quarantine_workflow/config.yaml` (55 lines), `examples/quarantine_workflow/README.md` (71 lines), `examples/quarantine_workflow/data/{dim_customer.csv, fact_orders.csv, dim_customer_fixed.csv}`. Additional coverage: quarantine split, metadata columns, quality gate (abort/warn_and_write), sampling (max_rows), config YAML validation.
```

---

### Task 18: Delta Lake Troubleshooting Guide (#225)

**Branch:** `docs/hodibi/delta-troubleshooting`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, docs/troubleshooting.md.

Task: Create Delta Lake troubleshooting section. GitHub issue #225.

Add to docs/troubleshooting.md (or create docs/tutorials/delta_troubleshooting.md):

Common issues to cover:
1. Schema evolution errors — column added/removed/type changed
2. Concurrent write conflicts (MERGE + MERGE)
3. "Table already exists" errors
4. Null type columns (Lessons Learned T-004)
5. VACUUM and file retention
6. Restore to previous version
7. Time travel queries
8. Z-ORDER optimization
9. pyarrow version conflicts (<17.0.0,>=14.0.0)
10. Databricks vs local Delta behavior differences

For each issue:
- Error message / symptom
- Root cause
- Fix / workaround
- Prevention

Rules:
- Include actual error messages where possible
- Test solutions on Databricks with real Delta tables
- ≤500 LOC

Success criteria:
- [ ] All 10 issues documented
- [ ] Each has symptom, cause, fix
- [ ] Solutions verified on Databricks
- [ ] Linked from main troubleshooting.md
```

---

## Phase 5: Pattern Stress Testing (Tasks 19-23)

### Task 19: Aggregation Pattern — Real Data Stress Test

**Branch:** `test/hodibi/aggregation-pattern-stress`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, docs/skills/04_write_a_pattern.md.

Task: Stress-test the Aggregation pattern with real Spark and realistic data on Databricks.

Create campaign/13_aggregation_stress.py:

Test scenarios:
1. Simple aggregation — GROUP BY with SUM, AVG, COUNT, MIN, MAX
2. Multi-grain — GROUP BY [date, category, region]
3. Incremental merge — replace strategy
4. Incremental merge — sum strategy (additive)
5. Audit columns — verify load_timestamp accuracy
6. Large data — 100k rows aggregated to ~1k rows
7. Null handling — nulls in grain columns
8. Empty groups — grain combinations with no data

For each:
- Run with Pandas engine, verify result
- Run with Spark engine, verify result
- Compare Pandas vs Spark results

Rules:
- Use realistic data (sales transactions, IoT sensor readings)
- Assert exact aggregated values, not just row counts
- Test with both DuckDB SQL (Pandas) and Spark SQL
- ≤250 LOC

Success criteria:
- [ ] All 8 scenarios pass on both engines
- [ ] Pandas vs Spark results match
- [ ] 100k row test completes in <30 seconds
- [ ] Null handling is correct
- [ ] Bugs filed for any issues
```

---

### Task 20: Date Dimension Pattern — Full Feature Test

**Branch:** `test/hodibi/date-dimension-full-test`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md.

Task: Test Date Dimension pattern thoroughly. AGENTS.md shows "57% covered, Remaining: Spark paths."

Create campaign/14_date_dimension.py:

1. Generate date_dimension with Pandas — verify all columns:
   - date_key (integer YYYYMMDD)
   - date, year, month, day, quarter
   - day_of_week, day_name, month_name
   - is_weekend, is_holiday (if configured)
   - fiscal_year, fiscal_quarter

2. Generate date_dimension with Spark — compare with Pandas output
3. Test fiscal year configurations:
   - Calendar fiscal (Jan start)
   - Custom fiscal (Apr start, Jul start, Oct start)
4. Test unknown member (SK = -1 or 0 row)
5. Test date range edge cases:
   - Single day range
   - Leap year boundary
   - 10-year range (performance)
6. Verify row count = (end_date - start_date).days + 1

Rules:
- Assert on specific dates (e.g., "2024-03-15 should be Q1 fiscal if FY starts April")
- Test all fiscal year start months
- ≤250 LOC

Success criteria:
- [ ] All date columns populated correctly
- [ ] Fiscal year/quarter logic verified
- [ ] Pandas and Spark produce identical results
- [ ] Unknown member row present
- [ ] 10-year range generates quickly
```

---

### Task 21: End-to-End Star Schema Build

**Branch:** `test/hodibi/star-schema-e2e`

> **⚠️ YAML Authoring Context (Required Reading for Genie):**
> Before generating any YAML configs, read these files for correct field names and structure:
> - `docs/skills/05_pipeline_yaml_authoring.md` — canonical YAML field rules
> - `docs/skills/08_validation_workflow.md` — validation/quarantine/gate YAML structure
> - `odibi/config.py` — Pydantic source of truth for all config models
>
> **Key YAML rules:** Use `read:`/`write:`/`transform:`/`query:` (NEVER `source:`/`sink:`/`sql:`).
> Node names must be alphanumeric + underscore only. Write mode `upsert`/`append_once` require `options: {keys: [...]}`.

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, docs/skills/05_pipeline_yaml_authoring.md.

Task: Build a complete star schema on Databricks using odibi — the ultimate integration test.

Create campaign/15_star_schema_e2e.py:

Build:
1. dim_date — Date Dimension pattern (2024-01-01 to 2024-12-31)
2. dim_customer — Dimension pattern with SCD1 (100 customers)
3. dim_product — Dimension pattern with SCD2 (50 products)
4. fact_orders — Fact pattern with dimension lookups (10,000 orders)

Pipeline:
- Generate source data programmatically
- Run dimension loads first (dependency order)
- Run fact load with FK lookups to all 3 dimensions
- Include validation on every node
- Include quality gates

Verify:
- Total rows in each table correct
- Surrogate keys are sequential
- FK integrity — every fact row points to valid dimension SK
- SCD2 dim_product has history for changed products
- Unknown member handling for orphan FKs
- Quality gate passes

Rules:
- Use real Unity Catalog tables
- Full pipeline — not just individual patterns
- Verify FK integrity with a SQL query
- Clean up test tables after
- ≤500 LOC (integration test exception)

Success criteria:
- [ ] Star schema builds successfully
- [ ] All dimension tables populated
- [ ] Fact table with correct SK lookups
- [ ] FK integrity verified (0 orphans in valid data)
- [ ] SCD2 history present
- [ ] Quality gates pass
- [ ] Tables cleaned up
```

---

### Task 22: Connection Discovery API — Real ADLS

**Branch:** `test/hodibi/connection-discovery-real-adls`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, docs/skills/06_add_a_connection.md.

Task: Test the connection discovery API (discover_catalog, get_schema, profile, preview, get_freshness) with real Azure storage on Databricks.

Create campaign/16_connection_discovery.py:

If ADLS/Azure Blob is accessible from Databricks:
1. discover_catalog — list files/folders
2. get_schema — read parquet schema
3. profile — column statistics
4. preview — sample rows
5. get_freshness — file modification time
6. detect_partitions — Hive-style partitions

If not accessible, test with DBFS or local volumes:
1. Same tests against /dbfs/ or Volumes
2. Document what's available

For any SQL connections accessible:
1. discover_catalog — list schemas/tables
2. get_table_info — column metadata
3. profile — stats
4. preview — sample rows
5. relationships — FK relationships

Rules:
- Use whatever storage is actually accessible
- Don't fail if ADLS isn't configured — skip gracefully
- Assert concrete values in discovery results
- ≤250 LOC

Success criteria:
- [ ] At least one storage type tested with real data
- [ ] Discovery API returns correct metadata
- [ ] Profile shows accurate statistics
- [ ] Preview returns requested row count
- [ ] Freshness reported accurately
```

---

### Task 23: State Management — Real Delta Backend

**Branch:** `test/hodibi/state-management-real-delta`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md.

Task: Test state management (HWM, run history) with real Delta tables on Databricks.

Create campaign/17_state_management.py:

Test CatalogStateBackend with real Delta:
1. set_hwm → get_hwm (verify roundtrip)
2. set_hwm_batch → verify all values stored
3. log_run → get_last_run_info (verify fields)
4. log_runs_batch → verify batch storage
5. Multiple pipelines — verify isolation
6. Concurrent writes — two notebooks writing state simultaneously

Test the full incremental loading cycle:
1. Initial load — no HWM, full extract
2. Set HWM after load
3. Second load — HWM filters to new data only
4. Verify no duplicates

Rules:
- Use real Delta tables (Lessons Learned T-004: use pa.table not pd.DataFrame)
- Test with both Spark and Pandas engines
- Verify data types are preserved across roundtrips
- ≤250 LOC

Success criteria:
- [ ] HWM roundtrip works with real Delta
- [ ] Run history stored and retrievable
- [ ] Incremental loading cycle works
- [ ] No duplicate data after incremental loads
- [ ] Concurrent writes don't corrupt state
```

---

## Phase 6: Bug Fixes (Tasks 24-26)

### Task 24: SCD2 Float/NaN Comparison (#248)

**Branch:** `fix/hodibi/scd2-float-nan-comparison`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md (T-009).

Task: Fix SCD2 float/NaN comparison. GitHub issue #248.

Problem: SCD2 change detection is unreliable for float columns containing NaN values. NaN != NaN in Python, so every run creates false-positive "changes."

Investigation:
1. Read odibi/transformers/scd.py — find where tracked columns are compared
2. Identify the comparison logic (how does _scd2_pandas detect changes?)
3. Test with NaN values to confirm the bug
4. Design a fix

Fix approach:
- Use pandas `equals()` or `pd.isna()` checks for NaN-safe comparison
- For Polars: use `is_nan()` / `fill_nan()`
- For Spark: use `isnan()` / `coalesce()`

Test:
1. Source with NaN in tracked column → first load
2. Same source (identical NaN) → re-run → should create 0 new rows
3. Change NaN to a real value → should detect change
4. Change real value to NaN → should detect change
5. Float precision: 1.0000001 vs 1.0000002 → should NOT detect change (within epsilon)

Rules:
- Fix must work on all 3 engines
- ≤250 LOC
- Add unit tests for the fix
- Update docs/LESSONS_LEARNED.md T-009 with the resolution

Success criteria:
- [ ] NaN == NaN returns True (no false-positive changes)
- [ ] NaN → value is detected as a change
- [ ] value → NaN is detected as a change
- [ ] Float epsilon handled
- [ ] Works on Pandas, Spark, Polars
- [ ] Unit tests pass
- [ ] #248 closeable
```

---

### Task 25: YAML Validation Hardening

**Branch:** `feat/hodibi/yaml-validation-hardening`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, docs/skills/05_pipeline_yaml_authoring.md.

Task: Harden YAML validation to catch more misconfigurations early.

Items from ROADMAP.md:
1. Validate node name format (^[a-zA-Z0-9_]+$) — reject hyphens, dots, spaces
2. Validate missing format: in read/write sections
3. Improve error messages with line numbers (if YAML library supports it)
4. Validate that connections referenced in nodes actually exist in connections: section
5. Validate that transformer function names exist in the registry
6. Warn on common hallucinations: source:, sink:, sql: (suggest correct field)

Read odibi/config.py to understand current validation, then add new validators.

Tests: tests/unit/test_yaml_validation.py
- Valid YAML passes
- Node name with hyphens → clear error
- Missing format → clear error  
- Nonexistent connection → clear error
- Hallucinated field names → suggestion
- Line numbers in error messages (if possible)

Rules:
- Add validators to existing Pydantic models in config.py
- Error messages must be actionable: tell the user what to fix
- ≤250 LOC
- Don't break existing valid configs

Success criteria:
- [ ] Node name format validated
- [ ] Missing format detected
- [ ] Invalid connection reference detected
- [ ] Hallucination suggestions work
- [ ] All existing valid configs still pass
- [ ] Error messages are clear and actionable
- [ ] Tests pass
```

---

### Task 26: Fix Pre-Existing Test Failures

**Branch:** `fix/hodibi/pre-existing-test-failures`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, AGENTS.md "Pre-Existing Test Failures" section.

Task: Fix the pre-existing test failures that have been documented but deferred.

Known failures:
1. ~25 caplog failures in catalog test files — logger.warning goes to stderr, caplog doesn't capture
2. ~94 failures in test_pandas_engine_core.py when run in batch — logging context pollution

Strategy for caplog failures:
- Replace caplog assertions with return-value assertions
- If the function doesn't return useful info, check for side effects instead
- Do NOT add caplog — remove it

Strategy for logging context pollution:
- Add setUp/tearDown to reset logging context between tests
- Or: refactor tests to not depend on logging state
- Or: mark the affected tests as expected-to-fail-in-batch with clear comments

Rules:
- Fix actual test code, not production code (unless there's a real bug)
- ≤250 LOC per PR — split if needed
- Don't introduce new test patterns that break in batch

Success criteria:
- [ ] caplog failures eliminated (replaced with value assertions)
- [ ] Batch test failure count reduced
- [ ] No new test patterns that break in batch
- [ ] run_coverage.ps1 shows fewer failures
```

---

## Phase 7: New Features (Tasks 27-30)

### Task 27: `row_number` Transformer

**Branch:** `feat/hodibi/row-number-transformer`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, docs/skills/03_write_a_transformer.md.

Task: Add a row_number transformer — simpler than window_calculation for the common case.

Use: Assign sequential row numbers, optionally partitioned and ordered.

YAML:
```yaml
transform:
  steps:
    - function: row_number
      params:
        output: row_num
        partition_by: [department]  # optional
        order_by: [hire_date]       # optional
```

Implementation:
1. Create RowNumberParams Pydantic model
2. Implement row_number(context, params) → EngineContext
3. SQL: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) AS output
4. Register in __init__.py

Tests in tests/unit/transformers/test_row_number.py:
- Basic row_number (no partition, no order)
- With partition_by
- With order_by
- With both partition_by and order_by
- Default output column name
- Custom output column name

Rules:
- Follow Skill 03 exactly
- SQL-first approach (context.sql handles all engines)
- ≤250 LOC
- Test with Pandas, verify SQL works

Success criteria:
- [ ] row_number works with all parameter combinations
- [ ] Registered in FunctionRegistry
- [ ] Appears in `odibi list transformers`
- [ ] Tests pass
- [ ] ≤250 LOC
```

---

### Task 28: `flatten_struct` Transformer

**Branch:** `feat/hodibi/flatten-struct-transformer`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, docs/skills/03_write_a_transformer.md.

Task: Add a flatten_struct transformer for deeply nested JSON/struct columns.

Use: Flatten nested struct columns into top-level columns with dot-notation names.

YAML:
```yaml
transform:
  steps:
    - function: flatten_struct
      params:
        column: metadata           # struct column to flatten
        prefix: meta_              # optional prefix for flattened columns
        depth: 2                   # optional max nesting depth (default: 1)
        separator: _               # optional separator (default: _)
```

Example:
- Input: `{"metadata": {"owner": {"name": "Alice", "dept": "Eng"}, "version": 2}}`
- Output (depth=1): `metadata_owner` (struct), `metadata_version` (int)
- Output (depth=2): `metadata_owner_name` (str), `metadata_owner_dept` (str), `metadata_version` (int)

Implementation:
- Pandas: Use pd.json_normalize or recursive column extraction
- Spark: Use struct field access (col("struct.field"))
- Polars: Use struct.field access

Tests:
- Single level struct
- Nested struct (depth=2)
- Array of structs (should handle gracefully)
- Null struct values
- Custom prefix and separator
- depth=1 vs depth=unlimited

Rules:
- Follow Skill 03
- Must work on all 3 engines (or raise NotImplementedError with clear message for Polars)
- ≤250 LOC

Success criteria:
- [ ] Single-level flatten works
- [ ] Multi-level flatten works (depth param)
- [ ] Null handling correct
- [ ] Registered and appears in CLI
- [ ] Tests pass
```

---

### Task 29: `apply_mapping` Transformer

**Branch:** `feat/hodibi/apply-mapping-transformer`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, docs/skills/03_write_a_transformer.md.

Task: Add an apply_mapping transformer for lookup-based value replacement using external tables.

Use: Replace values in a column based on a mapping table (not inline dict — for large mappings).

YAML:
```yaml
transform:
  steps:
    - function: apply_mapping
      params:
        column: status_code        # column to map
        mapping_source: ref_status_codes  # dataset name in context (read from connection)
        source_key: code           # key column in mapping table
        source_value: description  # value column in mapping table
        output: status_description # optional output column (default: overwrite)
        default: "Unknown"         # optional default for unmatched values
```

Different from dict_based_mapping: this uses a reference table, not an inline dict.

Implementation:
- SQL: LEFT JOIN to mapping table, COALESCE for default
- Works via context.sql() — engine-agnostic

Tests:
- All values found in mapping
- Some values not found → default applied
- No default specified → NULL for unmatched
- Custom output column
- Mapping table with duplicates → should use first match or raise

Rules:
- SQL-first (LEFT JOIN approach works on all engines)
- The mapping source must be a named dataset in context
- ≤250 LOC

Success criteria:
- [ ] Mapping works for all match/no-match scenarios
- [ ] Default value works
- [ ] Custom output column works
- [ ] Duplicate mapping keys handled
- [ ] Registered and appears in CLI
- [ ] Tests pass
```

---

### Task 30: Coverage Push — Reach 85%

**Branch:** `test/hodibi/coverage-push-85`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, AGENTS.md "Current Coverage Status" section.

Task: Push coverage from 80% to 85%. Identify the highest-value targets and write tests.

Process:
1. Run coverage to get current baseline:
   python -m coverage run --source=odibi -m pytest tests/unit/transformers/ tests/unit/validation/ tests/unit/cli/ tests/unit/connections/ tests/unit/story/ -q --tb=no
   python -m coverage report --show-missing | sort -t% -k4 -n

2. Identify modules with the most missed statements AND reasonable testability:
   - Skip engine/spark_engine.py (tested on Databricks)
   - Skip Spark-only branches everywhere (diminishing returns)
   - Focus on Pandas/Polars paths that are genuinely untested

3. Write tests for the top 5 highest-value modules

4. Verify coverage improvement

Rules:
- Follow Skill 07 (Testing) exactly
- No caplog, no "spark"/"delta" in filenames
- Run tests in batches
- Assert concrete values (Correctness Verification Protocol)
- ≤250 LOC per module tested

Target modules (likely candidates based on AGENTS.md):
- pipeline.py (35% → 50%+) — PipelineManager paths
- diagnostics/delta.py (13% → 40%+) — mockable
- scd.py (49% → 65%+) — Pandas/DuckDB paths still testable
- merge_transformer.py (61% → 75%+)
- manufacturing.py (67% → 80%+)

Success criteria:
- [ ] Overall coverage ≥ 83% (stretch: 85%)
- [ ] ≥5 modules improved
- [ ] All new tests pass
- [ ] AGENTS.md coverage numbers updated
- [ ] Tests run in batches without hanging
```

---

## Phase 8: Documentation & Polish (Tasks 31-35)

### Task 31: End-to-End Example for Every Pattern

**Branch:** `docs/hodibi/pattern-examples`

> **⚠️ YAML Authoring Context (Required Reading for Genie):**
> Before generating any YAML configs, read these files for correct field names and structure:
> - `docs/skills/05_pipeline_yaml_authoring.md` — canonical YAML field rules
> - `docs/skills/08_validation_workflow.md` — validation/quarantine/gate YAML structure
> - `odibi/config.py` — Pydantic source of truth for all config models
>
> **Key YAML rules:** Use `read:`/`write:`/`transform:`/`query:` (NEVER `source:`/`sink:`/`sql:`).
> Node names must be alphanumeric + underscore only. Write mode `upsert`/`append_once` require `options: {keys: [...]}`.

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, docs/skills/05_pipeline_yaml_authoring.md.

Task: Create a working end-to-end example for each of the 6 patterns.

Create examples/<pattern>/ for each:

1. examples/dimension_pipeline/ — Customer dimension with SCD1
2. examples/fact_pipeline/ — Orders fact with dimension lookups
3. examples/scd2_pipeline/ — Employee history tracking
4. examples/merge_pipeline/ — Upsert inventory data
5. examples/aggregation_pipeline/ — Daily sales summary
6. examples/date_dimension_pipeline/ — Calendar table generation

Each example includes:
- config.yaml — valid pipeline YAML
- data/ — sample CSV input data
- README.md — what this does, how to run it
- expected/ — expected output for verification

Rules:
- Every YAML must pass `odibi validate config.yaml`
- Every example must actually run with `odibi run config.yaml`
- Include realistic data (not just id, name, value)
- Follow Skill 05 (YAML Authoring) for correct field names
- ≤500 LOC total across all examples

Success criteria:
- [ ] All 6 examples created
- [ ] All YAMLs validate
- [ ] All examples run successfully
- [ ] Output matches expected/
- [ ] README explains each example
```

---

### Task 32: Spark/Databricks Testing Guide

**Branch:** `docs/hodibi/databricks-testing-guide`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, docs/skills/12_databricks_notebook_protocol.md.

Task: Document the Spark/Databricks testing approach.

Create docs/tutorials/databricks_testing.md:

1. Why CI doesn't test Spark (no JVM)
2. The mock strategy for CI (how Spark branches are mock-tested)
3. The Databricks strategy for real testing (this campaign)
4. How to set up a Databricks workspace for odibi testing
5. The lib/ pattern (reference Skill 12)
6. How to run the campaign notebooks
7. How to interpret results
8. How to contribute new Databricks tests

Include:
- Step-by-step setup instructions
- Clone path configuration
- Unity Catalog setup
- Expected test runtime
- Troubleshooting common issues

Rules:
- ≤500 LOC
- Include actual commands/paths
- Reference existing campaign notebooks

Success criteria:
- [ ] New user can set up Databricks testing from this guide
- [ ] lib/ pattern explained
- [ ] Campaign notebooks referenced
- [ ] Troubleshooting section included
```

---

### Task 33: Harden Testing Skill (Skill 07)

**Branch:** `docs/hodibi/harden-testing-skill`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, docs/skills/07_testing.md.

Task: Harden Skill 07 (Testing) with lessons from the campaign.

Add to docs/skills/07_testing.md:

1. Correctness Verification Protocol section (from CUSTOM_INSTRUCTIONS.md)
2. The Databricks testing workflow (reference Skill 12)
3. Cross-engine comparison pattern:
   ```python
   def compare_engines(test_name, pandas_df, spark_df, polars_df=None):
       # Convert to sorted Pandas, compare with assert_frame_equal
   ```
4. Common assertion anti-patterns:
   - `assert result is not None` → BAD
   - `assert len(result) > 0` → BAD  
   - `assert result.df["col"].tolist() == [1, 2, 3]` → GOOD
5. Edge case checklist:
   - Empty DataFrame
   - Single row
   - Null-only column
   - Unicode strings
   - Duplicate keys
   - Maximum column name length
6. Polars-specific test patterns:
   - Eager vs lazy frames
   - UTC timestamps
   - Expression evaluation

Rules:
- Don't remove existing content — only add
- ≤250 LOC of additions
- Include code examples for each new pattern

Success criteria:
- [ ] Correctness Verification Protocol integrated
- [ ] Cross-engine comparison pattern documented
- [ ] Anti-patterns listed with fixes
- [ ] Edge case checklist added
- [ ] Polars patterns added
```

---

### Task 34: Harden Think/Plan/Critique Skill (Skill 01)

**Branch:** `docs/hodibi/harden-skill-01`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, docs/skills/01_think_plan_critique.md.

Task: Strengthen Skill 01 with anti-batch-reading safeguards and Databricks considerations.

Add to docs/skills/01_think_plan_critique.md:

1. "Slow Down" mandate:
   - After producing a plan, WAIT. Re-read the plan.
   - Ask: "Is each step verifiable? How will I know it worked?"
   - If any step says "verify it works" without specifying HOW → revise

2. Anti-batch-reading rules:
   - NEVER say "all N files read successfully" as a verification step
   - NEVER confirm correctness without running the code
   - Each verification must include: command, expected output, actual output comparison

3. Databricks planning:
   - Check: will this run on Databricks or in CI?
   - If Databricks: use Skill 12 lib/ pattern
   - If CI: mock Spark, test Pandas/Polars directly
   - If both: plan both test strategies

4. Scope estimation:
   - Before starting, estimate LOC for each step
   - Total must be ≤250 (hard max 500)
   - If estimate exceeds limit → split into phases BEFORE coding

Rules:
- Don't remove existing content — only add
- ≤150 LOC of additions

Success criteria:
- [ ] "Slow Down" mandate added
- [ ] Anti-batch-reading rules clear and specific
- [ ] Databricks planning integrated
- [ ] Scope estimation step added
```

---

### Task 35: Campaign Retrospective

**Branch:** `docs/hodibi/campaign-retrospective`

**Prompt for Agent:**
```
Read CUSTOM_INSTRUCTIONS.md, docs/LESSONS_LEARNED.md, docs/AGENT_CAMPAIGN.md.

Task: Write a campaign retrospective after all tasks are complete.

Create docs/CAMPAIGN_RETROSPECTIVE.md:

1. Summary statistics:
   - Tasks completed / total
   - PRs merged
   - Tests added
   - Coverage before / after
   - Bugs found and fixed
   - New features added

2. What worked well:
   - Which task format was most effective?
   - Which phase produced the most value?
   - What skills were most useful?

3. What didn't work:
   - Which tasks needed revision?
   - What was over-scoped?
   - What was under-scoped?

4. Lessons learned:
   - Add all new entries to docs/LESSONS_LEARNED.md
   - Update AGENTS.md with new gotchas
   - Update skills with new patterns

5. Next campaign priorities:
   - What should the next hardening campaign focus on?
   - What's still missing?

6. Update docs/ROADMAP.md with new reality

Rules:
- Be honest — document failures too
- Include concrete numbers
- Update all status documents

Success criteria:
- [ ] All statistics captured
- [ ] Honest assessment of what worked / didn't
- [ ] All new lessons in docs/LESSONS_LEARNED.md
- [ ] ROADMAP.md updated
- [ ] AGENTS.md updated
- [ ] Next priorities identified
```

---

## Campaign Execution Checklist

Use this to track progress:

```
Phase 1: Foundation ✅
- [x] Task 1: Update ROADMAP.md
- [x] Task 2: Databricks workspace setup
- [x] Task 3: CHANGELOG catch-up
- [x] Task 4: Engine parity table

Phase 2: Spark Reality ✅
- [x] Task 5: Spark read/write
- [x] Task 6: Spark SQL core transformers
- [x] Task 7: Spark relational/advanced transformers
- [x] Task 8: Spark SCD2/merge patterns (11/11 PASS, T-020 fixed)
- [x] Task 9: Spark dimension/fact patterns (12/12 PASS, T-020 in base.py fixed)
- [x] Task 10: Spark validation (17/17 PASS, all 11 types + chain + special + Spark vs Pandas)

Phase 3: Polars Parity ✅
- [x] Task 11: Polars audit (#212) — 91 fns audited, 58 gaps found
- [x] Task 12: Polars relational transformers — 14/14 PASS, +56 LOC
- [x] Task 13: Polars SCD2/merge — 10/10 PASS, +265 LOC production
- [x] Task 14: Polars delete detection — 24/24 PASS, +185 LOC production

Phase 4: Validation E2E
- [ ] Task 15: Validation tutorial (#224)
- [x] Task 16: Delete detection tutorial (#223) — 9/9 PASS, 6 files
- [x] Task 17: Quarantine tutorial (#222) — 10/10 PASS, 6 files
- [ ] Task 18: Delta troubleshooting (#225)

Phase 5: Pattern Stress
- [ ] Task 19: Aggregation stress test
- [ ] Task 20: Date dimension full test
- [ ] Task 21: Star schema E2E
- [ ] Task 22: Connection discovery
- [ ] Task 23: State management

Phase 6: Bug Fixes
- [ ] Task 24: SCD2 float/NaN (#248)
- [ ] Task 25: YAML validation hardening
- [ ] Task 26: Pre-existing test failures

Phase 7: New Features
- [ ] Task 27: row_number transformer
- [ ] Task 28: flatten_struct transformer
- [ ] Task 29: apply_mapping transformer
- [ ] Task 30: Coverage push to 85%

Phase 8: Docs & Polish
- [ ] Task 31: Pattern examples
- [ ] Task 32: Databricks testing guide
- [ ] Task 33: Harden Testing skill
- [ ] Task 34: Harden Think/Plan/Critique skill
- [ ] Task 35: Campaign retrospective
```

---

## Branch Naming Convention

All campaign branches follow: `type/hodibi/description`

| Type | When |
|------|------|
| `test/` | Databricks e2e tests, coverage |
| `feat/` | New transformers, Polars paths |
| `fix/` | Bug fixes (#248, test failures) |
| `docs/` | Tutorials, examples, skill updates |
| `chore/` | ROADMAP updates, workspace setup |
