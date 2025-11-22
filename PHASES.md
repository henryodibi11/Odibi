# Odibi Roadmap (v2.1+)

**Version Strategy:** Semantic Versioning (SemVer)  
**Current Core Version:** v2.1.0-alpha (Target)  
**Last Updated:** November 22, 2025  

Odibi v2.0.0 completed Phases 1–6, 9, and 10.  
This document tracks active and future development.

For a detailed history of all completed phases, see:  
➡️ [`docs/_archive/PHASES_HISTORY.md`](docs/_archive/PHASES_HISTORY.md)

---

## 📊 Snapshot: Where We Are

- **Latest Release:** `v2.0.0` – Enterprise + Ecosystem
- **Completed Phases:** 1–6, 9, 10
- **Currently Active:** Phase 2.1 (Delta-First Foundation)
- **Stability:** Production-ready; >500 tests; ~80% coverage

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

### 2.1.2 `odibi init-pipeline` Templates (Next Priority)

**Goal:** Make it trivial to start a **Delta-first** Odibi project with best practices baked in.

**Planned Deliverables:**
- New CLI command: `odibi init-pipeline <name> --template {local-medallion,azure-delta,...}`
- Templates:
  1. **Local Medallion:** Pandas + Local Parquet/Delta.
  2. **Azure Delta Medallion:** Spark + ADLS + Key Vault.
  3. **Reference-Lite:** Minimal "Gauntlet" style.

### 2.1.3 `env` Config Structure

**Goal:** Normalize how environments (`dev`, `test`, `prod`) are expressed and overridden.

**Planned Structure:**
- Base config (`odibi.yaml`) + Environment overlays (`env.dev.yaml`, `env.prod.yaml`).
- CLI support for `--env` flag to auto-merge configs.

---

## Phase 7 — Ecosystem & Platform (Next)

> Source: Consolidated and adapted from the prior `PHASES_NEXT.md`.

**Goal:** Move from a “library” to a “platform” by deepening ecosystem integration and operational tooling.

### 7.1 Developer Experience (DX) – Follow-On Work

Some DX work is already done (e.g. templates, config imports). Follow-ons:

- **Project Templates:** Extend `odibi init-pipeline` / `odibi create` with:
  - Opinionated templates for Spark-only, Pandas-only, mixed-engine, and “Gauntlet” style projects.
- **Editor & IDE Support:**
  - Continue VS Code schema/intellisense improvements.
  - Keep JSON/YAML schema in sync with new `merge`/env config.
- **Engine Hardening:**
  - Close remaining functional gaps discovered via Phase 9 (“Infinite Gauntlet”).
  - Stabilize public API for engine plugins.

### 7.2 Orchestration Generators (Deferred → Next)

**Status:** On hold, but next major ecosystem lever.

**Planned Features:**

- Generators for:
  - **Databricks Jobs** (JSON/YAML spec from Odibi configs).
  - **Azure Data Factory / Synapse Pipelines**.
  - **GitHub Actions** workflows for scheduled runs.
- Philosophy:
  - Generators should be **pure**: no hidden state, idempotent.
  - Treat orchestration as a *view* over Odibi configs, not a second source of truth.

### 7.3 “Control Plane” UI (Deferred)

**Goal:** A simple web UI for:

- Viewing runs, stories, and metrics.
- Triggering pipelines manually.
- Inspecting environment and connection health.

**Constraint:** Only justified if real-world usage shows that CLI + existing observability are insufficient; otherwise this stays deferred.

---

## Phase 8 — Advanced Intelligence (Future)

> Adapted from the “Advanced Intelligence” section in the prior `PHASES_NEXT.md`.

**Goal:** Use LLMs to assist with pipeline creation, debugging, and maintenance without making the core framework dependent on external AI services.

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
