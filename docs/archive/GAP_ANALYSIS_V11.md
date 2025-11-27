# Gap Analysis V11 - Optimization & Quality (Azure Stack)

**Date:** 2025-11-27
**Status:** Planned

## 🎯 Goal
Solidify Odibi's foundation as a "Brain-Augmented" orchestrator by optimizing the System Catalog for scale, formalizing Data Quality as a first-class citizen, and polishing the Azure-centric connectivity.

## 📉 1. Catalog Performance & Scalability (The "Fast Brain")
**Context:** The System Catalog (`meta_runs`, `meta_state`) currently uses flat Delta tables. As history grows, queries for "Last 7 Days" or "Last Run" will degrade linearly.
**Status:** 🔴 Critical Gap

### Requirements:
- **Partitioning Strategy:**
    - `meta_runs`: Partition by `pipeline_name` and `date` (derived from timestamp).
    - `meta_state`: Partition by `pipeline_name`.
- **Optimization Routine:** Implement a `VACUUM` and `OPTIMIZE` (Z-Order) routine in `CatalogManager` that runs periodically (e.g., post-pipeline success).
- **Caching:** Cache frequently accessed metadata (like Pipeline Hashes) in memory during a single CLI execution to reduce I/O.

## 🛡️ 2. Data Quality & Integrity (The "Truth")
**Context:** Odibi tracks *if* a job ran, but not *if the data is good*. Validation warnings are currently ephemeral text in stories.
**Status:** 🔴 Critical Gap

### Requirements:
- **Schema Definition:** Create `meta_quality` table in System Catalog.
    - Fields: `run_id`, `node_name`, `assertion_type` (e.g., `unique`, `not_null`), `status`, `violation_count`.
- **YAML Config:** Add `quality` block to Node config (schema verification).
- **Persistence:** Update `Node` execution to log assertion results directly to `meta_quality` via `CatalogManager`.
- **Visualization:** Expose DQ scores in `odibi story` and `odibi graph`.

## 🔌 3. Connectivity (Stable)
**Context:** Azure connectivity (`AzureADLS`, `AzureSQL`) supports multiple authentication modes (`key_vault`, `service_principal`, `managed_identity`, `direct_key`) and handles Spark/Pandas configuration robustly.
**Status:** 🟢 Stable
*(No immediate changes required)*

## 📝 4. Scheduling (Deferred)
**Context:** Deferred in favor of stabilizing core functionality first. External schedulers (Airflow/Cron) are sufficient for now.
**Status:** ⚪ Deferred

## 📅 Execution Plan
1.  **Performance:** Partition and Optimize `meta_runs` / `meta_state`.
2.  **Quality:** Implement `meta_quality` and assertion logic.

## 📡 5. Telemetry Bridge (The "Nerves")
**Context:** Odibi has OpenTelemetry hooks, but they require an external collector (Jaeger/Prometheus). Local users lose this rich data.
**Status:** 🟡 Moderate Gap

### Requirements:
- **Catalog Exporter:** Create a custom `SpanProcessor` or `MetricExporter` that writes telemetry directly to `meta_runs` and `meta_metrics` in the System Catalog.
- **Zero-Config Observability:** If no OTLP endpoint is found, default to the Catalog Exporter so metrics are always captured.
