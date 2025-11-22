# Odibi Roadmap (v2.1+)

**Version Strategy:** Semantic Versioning (SemVer)  
**Current Core Version:** v2.0.0  
**Last Updated:** November 22, 2025  

Odibi v2.0.0 completes Phases 1–6, 9, and 10. This document focuses on **what comes next**.  
For a detailed history of all completed phases, see:  
➡️ [`docs/_archive/PHASES_HISTORY.md`](docs/_archive/PHASES_HISTORY.md)

---

## 📊 Snapshot: Where We Are

- **Latest Release:** `v2.0.0` – Enterprise + Ecosystem
- **Completed Phases:** 1–6, 9, 10
- **Stability:** Production-ready; >400 tests; ~80% coverage
- **Focus Shift:** From building the core to **Delta-first pipelines**, ecosystem integration, and intelligent tooling.

---

## Phase 2.1 — The Delta-First Foundation (Immediate Priority)

**Goal:** Make Odibi’s **Delta-first** medallion architecture (Landing → Raw → …) the *default, batteries-included* experience across Pandas and Spark.

### 2.1.1 Built-in `merge` / `upsert` Transformer (Spark & Pandas)

**Problem:**  
Today, upsert/merge patterns are ad-hoc (custom SQL, manual joins, or user-written transforms). For Delta-first pipelines, we need a **first-class, engine-agnostic** merge primitive.

**Design Goals:**

- **Unified API:** One YAML/config shape that works for both **Spark (Delta)** and **Pandas**.
- **Delta-First:** Optimized for Delta Lake tables (Landing → Raw, Raw → Silver).
- **Safe by Default:** Explicit keys and column lists; no silent “update everything”.
- **Predictable Semantics:** Clear behavior for matched vs non-matched rows.

#### a) Configuration Shape (High-Level)

Example (Spark / Delta):

```yaml
- id: customers_raw_merge
  type: transform
  engine: spark           # or "pandas"
  transformer: merge      # NEW: built-in transformer
  params:
    target:
      ref: customers_raw  # existing node or table ref
    source:
      ref: customers_landing_clean
    keys:
      - customer_id
    update_columns:
      - name
      - email
      - updated_at
    insert_columns:
      - customer_id
      - name
      - email
      - created_at
    insert_when_not_matched: true
    delete_when_not_matched: false   # optional (for slowly-changing dimensions, etc.)
    options:
      # engine-specific hints (optional, non-breaking)
      delta:
        condition: "target.customer_id = source.customer_id"
        # reserved for future: "cdc" modes, predicates, etc.
```

Example (Pandas upsert into a file/table):

```yaml
- id: customers_upsert_pandas
  type: transform
  engine: pandas
  transformer: merge
  params:
    target:
      path: data/raw/customers.parquet
      format: parquet
    source:
      ref: customers_staging
    keys: [customer_id]
    update_columns: [name, email, updated_at]
    insert_columns: [customer_id, name, email, created_at]
    insert_when_not_matched: true
```

#### b) Semantics

- **Inputs:**
  - `target`: an existing node or table (Delta table, Parquet/CSV file, or engine-native table).
  - `source`: a node producing a DataFrame to merge into `target`.
- **Keys (`keys`):**
  - Required, list of column names.
  - Must exist in both `target` and `source`.
- **Update behavior:**
  - Only columns in `update_columns` are updated on matches.
  - Columns not listed remain unchanged in `target`.
- **Insert behavior:**
  - If `insert_when_not_matched: true`, rows in `source` with no key match are appended using `insert_columns`.
  - If false, unmatched rows are ignored.
- **Delete behavior (optional / future-friendly):**
  - `delete_when_not_matched: false` by default.
  - If true, target rows with no matching source key can be removed (for full-sync patterns).
- **Engine mapping:**
  - **Spark / Delta:** Translates into a `MERGE INTO` statement using the configured condition/keys.
  - **Pandas:** Implements an in-memory merge/upsert followed by a write, respecting file format and write mode (non-breaking with existing engine semantics).

#### c) Scope for v2.1

- Implemented for:
  - `SparkEngine` with Delta format.
  - `PandasEngine` for supported file/table targets (Parquet, CSV, Delta via delta-rs or equivalent connector as already supported).
- Docs & examples:
  - Landing → Raw medallion examples using `merge`.
  - Reference snippets for common patterns: CDC-style upsert, full reload, “only inserts”.

---

### 2.1.2 `odibi init-pipeline` Templates

**Goal:** Make it trivial to start a **Delta-first** Odibi project with best practices baked in.

**Deliverables:**

- New CLI command:  

  ```bash
  odibi init-pipeline <name> --engine {pandas,spark} --template {local-medallion,azure-delta,...} --env dev
  ```

- Initial template set:
  1. **Local Medallion (Pandas + local Delta/Parquet):**
     - Landing → Raw → Silver with `merge` transformer.
     - Local file storage; minimal secrets.
  2. **Azure Delta Medallion (Spark + ADLS):**
     - ADLS-based Landing/Raw/Silver tables.
     - Pre-wired Key Vault + connection config (building on existing v2.0 capabilities).
  3. **Reference-Project Lite (OdibiFlix-style):**
     - Small “Gauntlet” example with a couple of `merge` nodes.

- Each template includes:
  - `odibi.yaml` (with env-aware structure).
  - `env/` config overrides (see 2.1.3).
  - Minimal README with how to run + how to extend.

---

### 2.1.3 `env` Config Structure

**Goal:** Normalize how environments (`dev`, `test`, `prod`) are expressed and overridden in Odibi configs, building on the existing `--env` capabilities.

**Principles:**

- **Single Source of Truth:** Base config + env overlays; no duplication.
- **CLI-First:** `odibi run --env dev` “just works”.
- **Composable:** Plays nicely with template imports (from Phase 7 DX work).

**Planned Structure (conceptual):**

- Base project config (e.g. `odibi.yaml`) with logical environment slots, for example:

  ```yaml
  envs:
    default: dev
    available: [dev, test, prod]
  ```

- Env-specific overrides in dedicated files (examples):

  ```bash
  config/
    base.yaml          # base pipeline definition
    env.dev.yaml       # dev overrides (local paths, test secrets)
    env.test.yaml      # test/staging overrides
    env.prod.yaml      # prod ADLS/SQL, real Key Vault
  ```

- Resolution rules:
  - Start with `base.yaml` (or imports).
  - Apply `env.<name>.yaml` overlay last.
  - Support `imports: [...]` pattern (from existing Phase 7 DX work).
- CLI behavior:
  - `--env` selects overlay.
  - Env used is recorded in stories / logs for traceability.

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
