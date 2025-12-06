# 🏗️ Odibi 2.1 Architecture: Unified Catalog & Pattern Engine

**Status:** Blueprint
**Target Version:** Odibi 2.1
**Philosophy:** "Intent-Based Engineering" backed by a Stateful Catalog.

---

## 1. Executive Summary

### The Vision
Odibi 2.1 unifies the **System Catalog** (the "Brain") with a **Pattern-Driven Execution Engine** (the "Intent").
Instead of just executing tasks, Odibi now understands the *semantics* of the data (e.g., "This is a Fact Table", "This is an SCD2 Dimension") and enforces the appropriate behavior, lineage, and quality checks automatically.

### The Core Shift
| Feature | Odibi 1.0 (Task Runner) | Odibi 2.1 (Data Platform) |
| :--- | :--- | :--- |
| **Configuration** | `transformer: scd2` (Implementation Detail) | `pattern: scd2` (Business Intent) |
| **State** | Local Files / Stateless | **System Catalog** (Delta Tables) |
| **Validation** | Row-level checks (`email != null`) | **History-Aware** & **Pattern-Aware** checks |
| **Lineage** | Implicit / Text Logs | **Queryable Meta-Tables** |
| **Semantics** | Buried in SQL strings | **Metric Registry** (First-class citizen) |

---

## 2. The System Catalog (The Brain)
A set of Delta Tables auto-bootstrapped in `_odibi_system/`.

### 1. `meta_tables` (Inventory)
*Tracks physical assets.*
*   `project_name`: STRING (Partition)
*   `table_name`: STRING (Logical Name, e.g., "gold.orders")
*   `path`: STRING (Physical Location)
*   `format`: STRING
*   **`pattern_type`**: STRING (e.g., "scd2", "merge")
*   `schema_hash`: STRING (Drift Detection)
*   `updated_at`: TIMESTAMP

### 2. `meta_runs` (Observability)
*Tracks execution history.*
*   `run_id`: STRING
*   `pipeline_name`: STRING
*   `node_name`: STRING
*   `status`: STRING
*   `rows_processed`: LONG
*   `duration_ms`: LONG
*   `metrics_json`: STRING (JSON stats)
*   `timestamp`: TIMESTAMP

### 3. `meta_patterns` (Governance)
*Tracks pattern compliance.*
*   `table_name`: STRING
*   `pattern_type`: STRING
*   `configuration`: STRING (JSON: params used)
*   `compliance_score`: DOUBLE (0.0 - 1.0)
    *   *Example:* An SCD2 table missing a `valid_to` column gets a score of 0.5.

### 4. `meta_metrics` (Semantics)
*Tracks business logic.*
*   `metric_name`: STRING
*   `definition_sql`: STRING
*   `dimensions`: ARRAY<STRING>
*   `source_table`: STRING

### 5. `meta_state` (Checkpoints)
*Tracks incremental progress.*
*   `pipeline_name`: STRING
*   `node_name`: STRING
*   `hwm_value`: STRING

---

## 3. Pattern-Driven Execution (The Intent)

Users declare the **Pattern**, and Odibi configures the implementation.

### Supported Patterns
1.  **`scd2`**: Slowly Changing Dimension (History tracking).
2.  **`merge`**: Smart Upsert (Conditional updates).

> **Note:** For simple append or overwrite operations, use `write.mode: append` or `write.mode: overwrite` directly—no pattern needed.

### YAML Example
```yaml
- name: "dim_customers"
  pattern: "scd2"  # <--- The Intent
  params:
    keys: ["customer_id"]
    time_col: "updated_at"

  # Odibi Automatically:
  # 1. Selects the SCD2 Transformer.
  # 2. Configures the 'Merge' writer mode.
  # 3. Validates output has 'is_current' and 'valid_to' columns.
  # 4. Registers 'scd2' pattern in meta_tables.
```

---

## 4. Phased Implementation Roadmap

### Phase 1: The Foundation (Catalog & Schema)
**Goal:** Build the "Brain" that holds state.
*   **1.1 Catalog Manager:** Implement `odibi/catalog.py`. Logic to bootstrap the 5 meta-tables.
*   **1.2 Config Update:** Add `SystemConfig` (`connection`, `path`) to `odibi.yaml`.

### Phase 2: The Wiring (Engine Integration)
**Goal:** Connect the "Muscle" to the "Brain".
*   **2.1 Auto-Registration:** `Engine.write()` upserts to `meta_tables` and `meta_patterns`.
*   **2.2 Smart Read:** `Engine.read()` resolves logical names (`gold.orders`) via `meta_tables`.
*   **2.3 Telemetry:** `Node.execute()` flushes stats to `meta_runs`.
*   **2.4 State Migration:** `StateManager` reads/writes to `meta_state`.

### Phase 3: The Pattern Engine
**Goal:** Implement "Intent-Based" Logic.
*   **3.1 Pattern Module:** Create `odibi/patterns/`. Implement classes for `SCD2`, `Fact`, `Snapshot`.
*   **3.2 Execution Router:** Update `Node` to detect `pattern:` config. If present, delegate to Pattern Class instead of generic Transformer.
*   **3.3 Smart Merge:** Implement conditional logic (`whenMatched...`) within the Merge pattern.

### Phase 4: Intelligence (Guardrails)
**Goal:** Use Catalog data for smart checks.
*   **4.1 History-Aware Validation:** `Validator` queries `meta_runs` for anomalies (Volume Drops).
*   **4.2 Pattern Validation:** Pattern classes enforce schema rules (e.g., "Fact table cannot have duplicates").
*   **4.3 Semantic Module:** Parse `semantic_model`, register to `meta_metrics`, and implement `odibi export-views`.

### Phase 5: Observability
**Goal:** Visualization.
*   **5.1 Dashboard:** Standard Databricks Notebook to query `meta_runs` and show platform health.

---

## 5. Migration Strategy
1.  **Config:** Users add `system` block to `odibi.yaml`.
2.  **Bootstrap:** First run creates `_odibi_system/` tables.
3.  **Adoption:** Users gradually switch from `transformer: scd2` to `pattern: scd2` (backward compatible).
