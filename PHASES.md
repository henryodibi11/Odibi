# Odibi Roadmap (v2.2+)

**Version Strategy:** Semantic Versioning (SemVer)  
**Current Core Version:** v2.2.0 (Target)  
**Last Updated:** November 24, 2025

Odibi v2.4.0 completes Phases 1–7.
This document tracks active and future development.

For a detailed history of all completed phases, see:
➡️ [`docs/_archive/PHASES_HISTORY.md`](docs/_archive/PHASES_HISTORY.md)

---

## 📊 Snapshot: Where We Are

- **Latest Release:** `v2.4.0` – Deep Observability & Visual Diagnostics
- **Completed Phases:** 1–6, 9, 10, 2.1, 2.2, 2.3, 7
- **Currently Active:** Maintenance & Stability (Pre-Phase 8)
- **Stability:** Production-ready; >500 tests; ~98% coverage; Clean Linting

---

## Phase 2.3 — Deep Observability (COMPLETED)

**Goal:** Transform ODIBI into a platform with first-class troubleshooting, lineage, and drift detection without requiring user instrumentation.

**Status:** **Completed**

**Delivered Features:**
- **Visual Pipeline Lineage:** Mermaid.js integration in HTML reports to visualize node dependencies and data flow direction.
- **Explicit SQL Lineage:** Automatically captures and logs all SQL executed via `context.sql()` in Python transformers, eliminating "black box" transformations.
- **Delta Lake Version Tracking:** Native integration with Delta Lake transaction logs to capture version, timestamp, and operation metrics for every write.
- **Data Drift Diagnostics:**
  - **Row-Level Diffing:** Identifies exactly *which* rows were added, removed, or updated between runs (Pandas & Spark).
  - **Visual Stories:** Generated stories now include a "Data Changes" section showing samples of added/removed/updated rows with schema evolution highlighting.
  - **Null Profiler:** Automated null percentage calculation for all columns, displayed as badges in schema reports.
- **Run Comparison:** `odibi.diagnostics` module to programmatically compare pipeline runs (`run-diff`) for logic and data drift.
- **Rich Metadata:**
  - **Execution Environment:** Automatic capture of User, Host, OS, Python, and Library versions (Odibi, Pandas, PySpark).
  - **Source Tracking:** Traceability of input source files for every node.

## Phase 2.2 — High-Performance Core (COMPLETED)

**Goal:** Apply "First Principles" optimization to the framework's core, leveraging modern memory layouts (Arrow), compilation (mypyc), and engine tuning (Spark AQE) without changing the user API.

### 2.2.1 Arrow-Native Pandas Engine ✅

**Status:** **Completed**

**Delivered Features:**
- **Zero-Copy I/O:** Added `performance.use_arrow` config toggle.
- **Memory Reduction:** Drastic reduction (~50%) in memory usage for CSV/Parquet reads using `dtype_backend="pyarrow"`.
- **Delta Lake Optimization:** Native Arrow-to-Pandas conversion for Delta sources.

### 2.2.2 Core Compilation (mypyc) ✅

**Status:** **Completed**

**Delivered Features:**
- **Automated Build:** `setup.py` and `pyproject.toml` configured to compile core modules (`graph`, `pipeline`, `config`, `context`, `state`) into C extensions upon install.
- **Runtime Robustness:** Enforced type safety at runtime for orchestration logic.
- **Faster Startup:** Reduced import overhead for CLI commands.

### 2.2.3 Spark Engine Tuning ✅

**Status:** **Completed**

**Delivered Features:**
- **Adaptive Query Execution (AQE):** Enabled by default for better skew/shuffle handling.
- **Arrow-PySpark Bridge:** Enabled `spark.sql.execution.arrow.pyspark.enabled` for 10-100x faster data transfer between JVM and Python UDFs/Drivers.

### 2.2.4 Advanced I/O & Maintenance ✅

**Status:** **Completed**

**Delivered Features:**
- **Parallel File I/O (Pandas):** Multi-threaded reading for CSV/JSON glob patterns, leveraging `ThreadPoolExecutor` for linear speedups on multi-core systems.
- **Auto-Optimize (Spark):** Native support for `optimize_write` and `zorder_by` in `write` operations to automatically compact and cluster Delta tables.

### 2.2.5 Standard Transformation Library ✅

**Status:** **Completed**

**Goal:** Provide a comprehensive "Standard Library" of 25+ reusable transformations to reduce boilerplate and ensure SQL-first optimization.

**Delivered Features:**
- **Core SQL:** `filter_rows`, `derive_columns`, `cast_columns`, `clean_text`, `split_part`, `case_when`.
- **Date/Time:** `extract_date_parts`, `date_add`, `date_trunc`, `date_diff`, `convert_timezone`.
- **Multi-Table:** `join`, `union` (engine-agnostic).
- **Reshaping:** `pivot`, `unpivot`, `explode_list_column`.
- **Advanced:** `deduplicate`, `window_calculation`, `dict_based_mapping`, `regex_replace`.
- **Quality & Utils:** `validate_and_flag`, `unpack_struct`, `hash_columns`.
- **Architecture:** Introduced `EngineContext` for unified state management and Pydantic-based validation for all transforms.

---

## Phase 2.1 — The Delta-First Foundation (COMPLETED)

**Goal:** Make Odibi’s **Delta-first** medallion architecture (Landing → Raw → …) the *default, batteries-included* experience across Pandas and Spark.

### 2.1.1 Built-in `merge` / `upsert` Transformer (Spark & Pandas) ✅

**Status:** **Completed**

**Delivered Features:**
- **Unified API:** `transformer: merge` works identically for Spark (Delta) and Pandas (Parquet/Delta).
- **Strategies:** `upsert`, `append_only`, `delete_match`.
- **Audit Columns:** Auto-injection of `created_at` / `updated_at`.
- **Schema Evolution:** Automatic schema merging for Spark Delta operations.
- **Streaming Support:** Seamless `foreachBatch` wrapping for Spark Structured Streaming.

**Implementation:**
- `odibi/transformers/merge_transformer.py`
- Integration into `Node` and `Pipeline`.
- Unit and Integration tests.

### 2.1.2 `odibi init-pipeline` Templates ✅

**Status:** **Completed**

**Delivered Features:**
- New CLI command: `odibi init-pipeline <name> --template {local-medallion,azure-delta,...}`
- Templates:
  1. **Local Medallion:** Pandas + Local Parquet/Delta.
  2. **Azure Delta Medallion:** Spark + ADLS + Key Vault.
  3. **Reference-Lite:** Minimal "Gauntlet" style.

### 2.1.3 `env` Config Structure ✅

**Status:** **Completed**

**Delivered Features:**
- Base config (`odibi.yaml`) + Environment overlays (`env.dev.yaml`, `env.prod.yaml`).
- CLI support for `--env` flag to auto-merge configs.

---

## Phase 7 — Ecosystem & Platform (COMPLETED)

> Source: Consolidated and adapted from the prior `PHASES_NEXT.md`.

**Goal:** Move from a “library” to a “platform” by deepening ecosystem integration and operational tooling.

**Status:** **Completed**

### 7.1 Developer Experience (DX) ✅

**Delivered Features:**
- **Project Templates:** Enhanced `odibi init-pipeline` / `odibi create` with:
  - Opinionated templates for Spark-only, Pandas-only, mixed-engine, and “Gauntlet” style projects.
- **Editor & IDE Support:**
  - Continue VS Code schema/intellisense improvements.
  - Keep JSON/YAML schema in sync with new `merge`/env config.
- **Engine Hardening:**
  - Stabilize public API for engine plugins.
  - **External Table Registration (Spark):** Support auto-registration of file-based writes (e.g., `register_table: my_table`) for easier querying in Databricks/Hive. ✅ (Done in v2.2)

### 7.2 Orchestration & Diagnostics ✅

**Delivered Features:**
- **Rich HTML Stories:** Upgraded NodeStory to produce interactive HTML reports with tabs for Schema, SQL, Data Preview, and Config.
- **Drift Detection:**
  - **Logic Drift:** Automatically detects if SQL or Config changed between runs.
  - **Data Drift:** Uses Delta Lake transaction logs to instantly calculate row insertion/deletion counts without expensive table scans.
- **CLI Tooling:**
  - `odibi diag run-diff`: Compare two pipeline runs to trace the "Ripple Effect" of changes.
  - `odibi diag delta-diff`: Deep inspection of Delta table history.
- **Machine-Readable History:** All runs now produce `.json` artifacts alongside human-readable reports, enabling automated lineage analysis.

### 7.3 Engine Extensibility & Hardening ✅

**Status:** **Completed**

**Goal:** Simplify core engine logic and enable dynamic engine registration to support future backends (e.g., DuckDB, Snowpark) without modifying core orchestration code.

**Delivered Features:**
- **Engine Registry:** Decoupled `Pipeline` from hard-coded engine classes using a dynamic `odibi.engine.registry`.
- **Refactored Write Logic:** Decomposed monolithic `PandasEngine.write` into specialized helpers (`_write_sql`, `_write_delta`, `_write_file`) for better maintainability.
- **Clean Orchestration:** Simplified `Node` execution flow into distinct phases (`_execute_read_phase`, `_execute_transform_phase`, etc.) to improve readability and error context.
- **Remote Cleanup Fix:** Hardened `StoryGenerator` to gracefully handle remote storage paths (S3/ADLS) during cleanup.

### 7.4 “Control Plane” UI (Deferred)

**Goal:** A simple web UI for viewing runs.
**Status:** Deferred in favor of rich CLI diagnostics which proved sufficient.

---

## Phase 8 — Advanced Intelligence (Next)

> Adapted from the “Advanced Intelligence” section in the prior `PHASES_NEXT.md`.

**Goal:** Use LLMs to assist with pipeline creation, debugging, and maintenance without making the core framework dependent on external AI services.

**Status:** Deferred until core platform features (Phase 7) are mature.

### 8.1 “Auto-Heal” (Dev-Mode Only)

- If a node fails with a **SQL syntax error** or a simple, localized config issue:
  - Capture failing SQL/config and error message.
  - Propose a fix using an LLM (pluggable, opt-in).
  - In dev mode, optionally retry automatically with the suggested fix.
- Guardrails:
  - Never auto-apply changes in `prod`.
  - Always log “before vs after” for reproducibility.

### 8.2 Natural Language Querying

- Command-line helper, for example:

  ```bash
  odibi query "Show me the average order value by city over the last 30 days"
  ```

- Behavior:
  - Generates a **temporary pipeline** and SQL based on existing tables/nodes.
  - Executes it and returns a tabular result and/or a story snippet.
- Long-term:
  - Option to export the generated pipeline as a starting template.

---

## Phase History & Contributions

- Full historical details of Phases 1–6, 9, 10 and v1.0.0–v2.0.0:  
  ➡️ [`docs/_archive/PHASES_HISTORY.md`](docs/_archive/PHASES_HISTORY.md)
- Phase 5 detailed roadmap:  
  ➡️ [`docs/_archive/PHASE5_ROADMAP_COMPLETED.md`](docs/_archive/PHASE5_ROADMAP_COMPLETED.md)

For contribution guidelines and how to participate in these roadmap items, see:  
➡️ [`CONTRIBUTING.md`](CONTRIBUTING.md)
