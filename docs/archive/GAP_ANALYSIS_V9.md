# Gap Analysis V9: Cloud-Scale & High-Performance Platform (Day 7)

**Date:** November 26, 2025
**Author:** Principal Data Architect (Amp)
**Status:** Draft

## Executive Summary

Odibi has reached "Enterprise Maturity" (Day 6) with solid support for lineage, health checks, and snapshot testing. However, to support **Cloud-Scale** workloads and **High-Performance** single-node processing, we must address critical architectural bottlenecks. This analysis identifies four major gaps that prevent Odibi from competing with modern "Next-Gen" data tools (e.g., dbt + DuckDB, Polars, Dagster).

---

## 1. Execution Modernization: The "Polars" Gap

**Status:** Critical
**Impact:** Performance & Memory Efficiency

### Current Architecture
*   **Pandas Engine (`odibi.engine.pandas_engine`):** Relies on `pandas` for in-memory processing. While we have added `LazyDataset` and DuckDB optimizations, large datasets often cause OOM (Out-Of-Memory) errors.
*   **Spark Engine:** Heavyweight, requires JVM/Cluster, overkill for medium data (1GB - 100GB).

### The Gap
There is no "middle ground" engine that is both **fast** (multi-threaded, Rust-based) and **lightweight** (no JVM). `Pandas` is single-threaded and memory-hungry. `Spark` has high overhead/latency.

### Recommendation: Native Polars Integration (`odibi.engine.polars_engine`)
Implement a `PolarsEngine` that fully leverages Polars' **Lazy API**. Odibi's declarative YAML graph maps 1:1 to Polars' LazyFrames.

*   **Lazy by Default:** Unlike `PandasEngine` which tries to hack laziness with `LazyDataset`, `PolarsEngine` should construct a full LazyFrame query plan from the Odibi node config and only `collect()` at the write/check step.
*   **Streaming Support:** Enable Polars streaming mode (`collect(streaming=True)`) for out-of-core processing on single nodes.
*   **Zero-Copy Interop:** Use Arrow for efficient data hand-off if mixing engines (though typically a pipeline stays in one engine).

**Implementation Plan:**
1.  Create `odibi/engine/polars_engine.py` inheriting from `Engine`.
2.  Map Odibi `transform` steps (filter, select, derive) to Polars expressions.
3.  Update `odibi/engine/registry.py` to support `engine: polars`.

---

## 2. Orchestration Ecosystem: The "Airflow/Dagster" Gap

**Status:** High
**Impact:** Enterprise Adoption

### Current Architecture
*   **Native Runner:** `odibi run` executes the DAG locally using `Pipeline.run()`. It handles dependencies via `DependencyGraph`.
*   **Opaque Execution:** To run in Airflow, users execute `odibi run` as a single monolithic task, losing visibility into individual node status in the Airflow UI.

### The Gap
Enterprises already have orchestrators (Airflow, Dagster, Prefect). They do not want to replace them with Odibi; they want Odibi to **run inside** them with full granularity.

### Recommendation: "Compiler" / Export Strategy
Instead of just *running* the DAG, Odibi should be able to *export* or *compile* the `odibi.yaml` into native orchestrator code.

*   **Command:** `odibi export --target airflow --out dags/my_pipeline.py`
*   **Output:** A Python file defining an Airflow DAG where each Odibi Node becomes a `BashOperator` (running `odibi run-node <node>`) or a `PythonOperator`.
*   **Dagster Integration:** A "Software-Defined Asset" factory that reads `odibi.yaml` and yields Dagster Assets.

**Implementation Plan:**
1.  Create `odibi/orchestration/` module.
2.  Implement `AirflowExporter` (Jinja2 templates for DAG generation).
3.  Implement `DagsterFactory` (Dynamic Asset generation).
4.  Add `odibi export` CLI command.

---

## 3. Advanced Data Quality: The "Contracts" Gap

**Status:** Medium
**Impact:** Reliability & Trust

### Current Architecture
*   **Basic Validation:** `NodeExecutor` runs `_execute_contracts_phase` using `Validator`.
*   **Fail Fast:** Raises `ValidationError` which stops the node.

### The Gap
*   **Lack of Nuance:** It's binary (Pass/Fail). We need policies like "Warn", "Quarantine" (write bad rows to a separate table), or "Halt Pipeline".
*   **Schema Drift:** Current schema validation is manual. We need **Data Contracts** that define:
    *   **Schema Inventory:** Strict vs. Compatible (Evolvable).
    *   **SLA:** Freshness checks (e.g., "Data must be newer than 2 hours").
    *   **Distribution:** "Column 'amount' mean must be within 10% of last run."

### Recommendation: Enhanced Data Contracts
Refactor `contracts` to support a richer configuration model.

```yaml
contracts:
  - type: schema
    strict: true
    on_fail: fail
  - type: distribution
    column: amount
    metric: mean
    threshold: "10%"
    on_fail: warn
  - type: freshness
    max_age: "2h"
```

**Implementation Plan:**
1.  Update `NodeConfig` to support advanced contract schema.
2.  Enhance `odibi/validation/engine.py` to return "Severity" levels.
3.  Implement "Quarantine" logic in `NodeExecutor` (write invalid rows to `_error` table).

---

## 4. State & Backend Scalability: The "SQL" Gap

**Status:** Medium
**Impact:** Concurrency & Distributed Execution

### Current Architecture
*   **Local:** `LocalFileStateBackend` (`state.json`) uses file locking. Flaky at scale.
*   **Spark:** `DeltaStateBackend` works well but requires Spark.

### The Gap
We need a robust, concurrency-safe backend for non-Spark distributed environments (e.g., Odibi running on Kubernetes Pods using Polars). `state.json` doesn't work across pods (unless on shared PVC, which is risky).

### Recommendation: Pluggable SQL Backend
Implement `SQLStateBackend` using `SQLAlchemy`.

*   **Storage:** PostgreSQL, MySQL, SQLite.
*   **Concurrency:** Row-level locking via DB.
*   **Schema:** `odibi_runs`, `odibi_state` (HWM), `odibi_metrics`.

**Implementation Plan:**
1.  Add `sqlalchemy` as an optional dependency.
2.  Create `odibi/state/sql_backend.py`.
3.  Update `ProjectConfig` to accept a database connection string for state storage.

---

## Prioritized Roadmap

1.  **Phase 1: Polars Engine (The Performance Win)** - *Immediate Value*
2.  **Phase 2: SQL State Backend (The Scalability Enabler)** - *Prerequisite for Distributed Polars*
3.  **Phase 3: Airflow/Dagster Export (The Ecosystem Play)** - *Adoption Blocker*
4.  **Phase 4: Advanced Contracts (The "Day 2" Reliability)** - *Long-term Stability*
