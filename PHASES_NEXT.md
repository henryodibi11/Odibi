# ODIBI Future Roadmap (2026+)

This document outlines the next evolution of the Odibi framework, focusing on large-scale real-world validation, ecosystem integration, and developer experience.

---

## Phase 6: The "Flagship" Reference Project (Automated via Phase 9)

**Status:** ✅ Completed (Nov 2025) - Merged into Phase 9 ("The Infinite Gauntlet")
**Outcome:** We pivoted from manually building 10 reference projects to building a **generator** (`odibi generate-project`) and **stress tester** (`odibi stress`) that can generate infinite reference projects. We verified against 50+ datasets (including "OdibiEats" style data).

**Goal:** Prove Odibi's capability to handle messy, high-volume, complex real-world scenarios.

---

## Phase 10: Documentation & Education (COMPLETED)

**Status:** ✅ Completed (Nov 2025)
**Outcome:** Rebuilt documentation suite using Diátaxis framework. Delivered "Getting Started" tutorial, "Master CLI Guide", and "Cheatsheet". Verified against v2.0 features.

---

## Phase 7: Ecosystem & Platform

**Goal:** Move from a "Library" to a "Platform" by integrating with the wider data engineering ecosystem.

### 7.1. Developer Experience (DX) - ✅ COMPLETED
**Status:** ✅ Completed (Nov 2025) - v2.1.0
**Outcome:**
- **VS Code Support:** Implemented `odibi init-vscode` (JSON Schema for IntelliSense).
- **Project Templates:** Implemented `odibi create` with "Template-Driven Development".
- **Modular Configs:** Implemented `imports: [...]` to split large YAML files.
- **Engine Hardening:** Fixed 4 major functional gaps.
- **Unified Key Vault:** Decoupled secrets from auth modes.

### 7.2. Orchestration Generators (DEFERRED)
**Status:** ⏸️ On Hold
**Reason:** Orchestration is not currently a bottleneck. We will let actual usage patterns drive the requirements for this feature later.
**Planned Features:**
- Generators for Databricks Jobs, ADF Pipelines, and GitHub Actions.

### 7.3. The "Control Plane" UI (DEFERRED)
**Status:** ⏸️ On Hold
**Planned Features:** Standalone web interface for observability.

---

## Phase 8: Usage & "The Big Picture" (CURRENT)

**Goal:** Stop building features and start **using** Odibi to reclaim time and freedom. Let real-world pains drive future development.

**Focus Areas:**
1.  **Real-world Implementation:** Deploying Odibi v2.1.0 to actual projects.
2.  **Stability:** Fixing bugs that arise during daily usage.
3.  **Strategic Data Engineering:** Using the time saved to focus on architecture and value.

---

---

## Phase 8: Advanced Intelligence (Experimental)

**Goal:** Leverage LLMs to assist in pipeline creation and maintenance.

### 8.1. "Auto-Heal"
- If a node fails with a SQL syntax error, Odibi attempts to fix the SQL using an LLM and retries (in dev mode).

### 8.2. Natural Language Querying
- `odibi query "Show me the average order value by city"` -> Generates and runs a temporary pipeline.

---

## Phase 9: The "Infinite Gauntlet" (Automated Stress Testing)

**Status:** ✅ Completed (Nov 2025)
**Outcome:** Implemented `odibi generate-project` and `odibi stress` with dynamic Kaggle fuzzing. Verified against 50+ real-world datasets on enterprise hardware.

**Goal:** Move from manual project creation to automated stress testing against 50+ diverse datasets (e.g., Kaggle, HuggingFace).

### 9.1. The "Project Generator" (`odibi-gen`)
**Completed:**
- Implemented `odibi generate-project` command.
- Supports CSV, JSON, Parquet, and Excel schema inference.
- Auto-generates `odibi.yaml` with Bronze/Silver layers.
- Includes heuristics for validation and SQL name sanitization.

### 9.2. Batch Validation
**Completed:**
- Implemented `odibi stress` command with parallel execution.
- **New:** Integrated Kaggle API to automatically download and test against real-world datasets.
- Validated against 50+ datasets with 80%+ success rate out-of-the-box.
- Hardened Pandas engine with robust encoding fallback (UTF-8 -> Latin1) and parsing logic.

**Next Steps:**
- Add HuggingFace dataset support.
- Implement advanced delimiter sniffing for international CSVs.

---

## Summary of Priorities

| Phase | Feature | Value Prop |
|-------|---------|------------|
| **10** | **Docs Overhaul** | **COMPLETED:** Diátaxis restructure, Master CLI Guide, Tutorials. |
| **6** | **Reference Project** | Proof of capability, stress testing, marketing asset. |
| **7** | **Airflow/Dagster Gen** | Enterprise adoption blocker removal. |
| **7** | **Control Plane UI** | Operational visibility for non-engineers. |
| **7** | **VS Code Ext** | Developer stickiness and ease of use. |
