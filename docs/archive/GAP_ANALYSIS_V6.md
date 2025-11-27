# Gap Analysis V6: Orchestration, Performance & Developer Velocity

**Date:** 2023-11-26
**Status:** Draft
**Focus:** Day 4 - Operational Maturity & Developer Experience

## 1. Executive Summary
With Data Integrity (State/Concurrency) and Observability (Alerting/Logging) stabilized, Odibi is now "Safe". The next frontier is **Efficiency** and **Velocity**.
Currently, Odibi executes pipelines reliably but rigidly. Developers lack tools to test logic in isolation (`odibi test`), and operations teams lack fine-grained control over parallelism and recovery (`odibi run --resume --parallel`).

This analysis identifies gaps in **Orchestration Maturity**, **Advanced Validation**, and **Developer Tools**.

---

## 2. Orchestration Maturity (DAGs & Parallelism)

### Gap 2.1: Parallelism Control
*   **Current State:** The codebase supports parallel execution via `ThreadPoolExecutor` (`pipeline.py`), but the CLI (`odibi run`) does not expose `max_workers`. It defaults to 4 threads if `--parallel` is passed.
*   **Problem:** Users cannot tune concurrency based on infrastructure limits (e.g., running on a 64-core machine vs. a small container).
*   **Recommendation:** Add `--workers` argument to `odibi run`.

### Gap 2.2: "Smart" Resume
*   **Current State:** `resume_from_failure` simply checks if a node succeeded in the *last* run using `StateManager`.
*   **Problem:**
    1.  It doesn't detect *code changes*. If I fix a bug in a successful node, "resume" will skip it because it "succeeded" previously.
    2.  It doesn't handle "Cascading Invalidations" (if Node A changes, Node B should re-run).
*   **Recommendation:** Implement hash-based state tracking (already captured in metadata `sql_hash` and `config_snapshot`) to invalidate downstream nodes if upstream logic/config changed.

### Gap 2.3: Independent Branch Execution
*   **Current State:** Partial failure handling is implicit. If Node A fails, Node B (dependent) is skipped. Independent Node C continues.
*   **Problem:** There is no explicit control to *force* a stop on any failure (Fail Fast is node-level, not pipeline-level flag) or *force* continuation of independent branches (which is default but murky).
*   **Recommendation:** Clarify "Fail Fast" vs "Best Effort" modes at the CLI level.

---

## 3. Advanced Validation ("Circuit Breakers")

### Gap 3.1: Pre-condition Contracts
*   **Current State:** Validation runs *after* transformation (`_execute_validation_phase` calls `validator.validate(result_df)`).
*   **Problem:** If source data is corrupted (e.g., empty file, wrong schema), the `Read` -> `Transform` steps still run, potentially wasting compute or causing obscure errors in transformation logic before validation catches it.
*   **Recommendation:** Add a `contracts` (or `pre_validation`) block to Node Config that runs checks on the `input_df` (or raw source) *before* transformation.

### Gap 3.2: Cross-Node Circuit Breakers
*   **Current State:** Validation is scoped to a single node.
*   **Problem:** We cannot express rules like "Stop the *entire* pipeline if the total row count of Source A and Source B differs by > 10%".
*   **Recommendation:** Introduce "Check Nodes" - special nodes dedicated to asserting conditions across multiple datasets without producing output.

---

## 4. Developer Velocity (The "Inner Loop")

### Gap 4.1: The Missing `odibi test`
*   **Current State:** To test a change, a developer must run the pipeline (or a tagged slice) using `odibi run`.
*   **Problem:** This requires connection to real data (or setting up local files manually). There is no standard way to unit test a SQL transformation or Python function in isolation.
*   **Recommendation:** specific `odibi test` command that looks for `tests/` folder and runs unit tests using Odibi's engine context but with mock data.

### Gap 4.2: Fixtures & Mocking
*   **Current State:** No built-in support for defining input dataframes (Fixtures) in YAML or Python for testing.
*   **Problem:** Developers write "hacks" to create test CSVs.
*   **Recommendation:** Add a `fixture` capability to the CLI or Config to generate temporary DuckDB/Pandas dataframes for testing.

---

## 5. Prioritized Roadmap (Day 4)

### P0: Orchestration Basics (Quick Wins)
1.  **CLI Workers:** Update `odibi/cli/run.py` to accept `--workers` and pass it to `PipelineManager`.
2.  **Explicit Resume:** Improve `resume_from_failure` logging to clearly show *why* a node is being skipped or re-run.

### P1: Developer Experience (`odibi test`)
1.  **Test Command:** Create `odibi/cli/test.py`.
2.  **Fixture Support:** Allow defining inline data in `test_config.yaml` for unit testing SQL logic.

### P2: Pre-conditions
1.  **Input Validation:** Add `contracts` list to `NodeConfig`.
2.  **Executor Update:** Run `contracts` on `input_df` before `_execute_transform_phase`.
