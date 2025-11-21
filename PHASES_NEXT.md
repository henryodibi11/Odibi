# ODIBI Future Roadmap (2026+)

This document outlines the next evolution of the Odibi framework, focusing on large-scale real-world validation, ecosystem integration, and developer experience.

---

## Phase 6: The "Flagship" Reference Project (COMPLETED)

**Status:** ✅ Completed (Nov 2025)
**Outcome:** Delivered 10 reference projects ("The Gauntlet") covering Retail, IoT, Finance, Healthcare, and more. See `examples/` for implementations.

**Goal:** Prove Odibi's capability to handle messy, high-volume, complex real-world scenarios beyond simple tutorials.

**Project Codename:** `OdibiEats` (Simulated Food Delivery Analytics)

### 6.1. Complexity Requirements
- **Volume:** 1GB - 10GB simulated dataset (not "Big Data" but enough to break inefficient Pandas code).
- **Sources:**
  - **API:** Simulated stream of JSON events (User Clicks, Driver Locations).
  - **SQL:** Transactional database dump (Orders, Payments).
  - **CSV:** Third-party lookups (Geo-fencing zones, Restaurant metadata).
- **Architecture:** Medallion (Bronze -> Silver -> Gold).

### 6.2. Key Use Cases to Implement
1.  **Identity Resolution (Graph Logic):**
    - Stitching anonymous `session_id` (browsing) to `user_id` (after login/signup).
    - Requires complex joins and window functions.
2.  **Slowly Changing Dimensions (SCD Type 2):**
    - Tracking restaurant menu price changes over time.
    - "What was the price of a Burger on Nov 1st?" vs "Current Price".
3.  **Sessionization:**
    - Grouping clickstream events into 30-minute sessions.
    - Calculating "Time on Site" and "Conversion Rate per Session".
4.  **Geospatial Analytics:**
    - Mapping driver pings to delivery zones.
    - "Average delivery time per neighborhood".

### 6.3. Deliverables
- `examples/reference_project/`: A self-contained folder.
- `datagen/`: Scripts to generate the synthetic data at scale.
- **Performance Report:** Compare Pandas vs Spark execution times on this dataset.

---

## Phase 10: Documentation & Education (Prioritized)

**Goal:** Transform Odibi from a "tool with a README" to a "Framework with a Learning Platform". Consolidate loose docs, adopt Diátaxis structure, and ensure every feature is teachable.

### 10.1. Documentation Restructuring (The "Diátaxis" Refactor)
- **Audit:** Remove duplicate/conflicting guides (e.g., consolidate 3x WSL guides into one authoritative source).
- **Structure:** Reorganize `docs/` into standard buckets:
  - `tutorials/` (Learning-oriented: "Zero to Hero")
  - `guides/` (Task-oriented: "How to X")
  - `reference/` (Information-oriented: API, Config Spec)
  - `explanation/` (Understanding-oriented: "Why Odibi?", Architecture)

### 10.2. The "Master CLI Guide" (Task-Based)
- Move beyond `odibi --help`. Write task-based guides for every command:
  - "How to debug pipelines with `odibi doctor`"
  - "How to stress-test data with `odibi stress`"
  - "How to generate projects with `odibi generate-project`"

### 10.3. The "Odibi Cheatsheet"
- A high-density PDF/Markdown reference for daily use.
  - `odibi.yaml` syntax map.
  - Top 10 CLI command patterns.
  - Standard project directory structure.

### 10.4. "The Gauntlet" Documentation
- We built 10 reference projects (OdibiFlix, OdibiEats, etc.). Now we must document them.
- Create a "Case Studies" section explaining *why* each example was built and what pattern it demonstrates (e.g., "OdibiFlix: Handling high-volume clickstream data").

---

## Phase 7: Ecosystem & Platform

**Goal:** Move from a "Library" to a "Platform" by integrating with the wider data engineering ecosystem.

### 7.1. Orchestration Generators (`odibi generate`)
Instead of building our own scheduler, generate code for industry standards.
- **Airflow:** Generate a DAG file where each Odibi Node is a `BashOperator` or `PythonOperator`.
- **Dagster:** Generate a Graph where each Node is a Software-Defined Asset.
- **GitHub Actions:** Generate a workflow for CI/CD deployment of pipelines.

### 7.2. The "Control Plane" UI
A standalone web interface for observability.
- **Tech:** FastAPI + React (or Streamlit/NiceGUI for simplicity).
- **Features:**
  - **Pipeline Gallery:** Visual graph of all pipelines in the project.
  - **Run History:** Timeline of past runs (green/red bars).
  - **Story Viewer:** Render the JSON stories as interactive HTML reports.
  - **Drift Detection:** Visual warning if schema or row counts deviate significantly.

### 7.3. Developer Experience (DX)
- **VS Code Extension:**
  - Syntax highlighting for `odibi.yaml`.
  - "Run This Node" code lens button in YAML files.
  - Auto-completion for connection names and table paths.
- **Interactive TUI (Text User Interface):**
  - A `top`-like interface for watching pipeline progress in the terminal.

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
| **10** | **Docs Overhaul** | **PRIORITY:** Ensure users can actually use v2.0 features. |
| **6** | **Reference Project** | Proof of capability, stress testing, marketing asset. |
| **7** | **Airflow/Dagster Gen** | Enterprise adoption blocker removal. |
| **7** | **Control Plane UI** | Operational visibility for non-engineers. |
| **7** | **VS Code Ext** | Developer stickiness and ease of use. |
