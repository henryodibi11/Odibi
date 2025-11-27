# Deep Gap Analysis (V3)
**Date:** November 26, 2025
**Focus:** Engine Parity, Type Safety, Architecture, and Test Coverage

---

## 1. Engine Parity (Spark vs. Pandas)
We have established a strong dual-engine foundation, but significant gaps remain in feature parity, particularly regarding schema evolution and advanced transformations.

### Findings
| Feature | SparkEngine | PandasEngine | Status |
| :--- | :--- | :--- | :--- |
| **Harmonize Schema** | ✅ Implemented | ❌ **Missing** | `PandasEngine` lacks `harmonize_schema`, meaning `SchemaPolicyConfig` (e.g., `on_new_columns: add_nullable`) is ignored in Pandas execution. |
| **Execute Transform** | ❌ `NotImplementedError` | ❌ Missing | Both engines lack a generic `execute_transform` method for executing registered transformation functions directly from the engine interface. |
| **Anonymization** | ✅ Native Spark Functions | ⚠️ Basic (Non-Vectorized) | Pandas implementation of `hash_columns` uses `apply(lambda...)` which is slow for large datasets. |
| **Geocode** | ⚠️ Stub | ⚠️ Stub | The `geocode` transformer is a placeholder in both engines. |
| **Delta Lake Support** | ✅ Full (Native) | ✅ Full (via `deltalake`) | Good parity here. `PandasEngine` supports `vacuum`, `history`, and `restore` via the `deltalake` library. |

### Recommendation
*   **High Priority:** Implement `harmonize_schema` in `PandasEngine` to support schema evolution policies locally.
*   **Medium Priority:** Implement vectorized hashing in `PandasEngine` (using `pyarrow.compute` if available) or remove the "Stub" warning if performance is acceptable for local dev.

---

## 2. Type Safety & Configuration
We have moved to Pydantic for internal transformer logic, but the "Edge" (Pipeline Config) remains loosely typed.

### Findings
*   **Loose Config Binding:** `NodeConfig` defines `params` as `Dict[str, Any]`. This bypasses Pydantic validation during `odibi validate` or YAML loading. Validation only occurs at *runtime* when `FunctionRegistry.validate_params` is called by the Node executor.
    *   *Consequence:* Users won't know their YAML is invalid (e.g., missing `keys` for `deduplicate`) until they run the pipeline and it crashes.
*   **Transformer Params:** All built-in transformers (`merge`, `scd2`, `deduplicate`, etc.) have excellent Pydantic models (`MergeParams`, `SCD2Params`). The gap is strictly in *connecting* the YAML config to these models earlier.

### Recommendation
*   **Refactor `odibi validate`:** Update the CLI validation logic to lookup the registered Pydantic model for the specified `transformer` (or `transform.steps`) and validate the `params` dictionary *statically* without running the pipeline.
*   **Schema Generation:** Generate a JSON Schema that includes the schemas for all registered transformers, allowing for IDE autocompletion in YAML files.

---

## 3. Architecture & Coupling
The codebase shows signs of "Organic Growth" in the connection management logic.

### Findings
*   **Connection Factory Antipattern:** `PipelineManager._build_connections` (odibi/pipeline.py:672) contains a massive `if/elif` block handling every connection type (`local`, `http`, `azure_adls`, `azure_sql`, `delta`).
    *   *Risk:* Adding a new connection type requires modifying the core `PipelineManager` class.
*   **Implicit Registration:** `odibi/pipeline.py` imports `odibi.transformers` solely for side-effect registration. This makes it hard to treeshake or test components in isolation without loading all transformers.
*   **Node Execution Mixing:** `Node.py` handles both orchestration (retries, state) and execution details (calling registry, unwrapping context).

### Recommendation
*   **Connection Plugins:** Refactor `_build_connections` to use a true Registry/Factory pattern where connection types register themselves.
*   **Explicit Registry:** Move transformer registration to a dedicated `bootstrap()` or `plugins.load()` phase, decoupling it from `pipeline.py` imports.

---

## 4. Test Coverage
Reviewing the `tests/` directory reveals a critical gap in the Phase 1 deliverable.

### Findings
*   **Missing Catalog Tests:** There are **ZERO** tests for `odibi.catalog.CatalogManager`.
    *   `tests/unit/` has no catalog tests.
    *   `tests/integration/` has no catalog tests.
    *   The new "Local System Catalog" (Phase 1) is completely uncovered.
*   **Coverage Gaps:**
    *   `PandasEngine` schema validation logic.
    *   `MergeTransformer` "delete_match" strategy (GDPR) needs specific integration tests.

### Recommendation
*   **Critical:** Add `tests/unit/test_catalog.py` immediately to verify `CatalogManager` bootstrapping and state persistence in local mode.

---

## Refactoring Plan (Phase 2 & 3)

### Phase 2: Parity & Safety (Next Sprint)
1.  **Implement `PandasEngine.harmonize_schema`**: Ensure local execution respects `on_new_columns` policies.
2.  **Strict Validation CLI**: Update `odibi validate` to check `transformer` params against registered Pydantic models.
3.  **Add Catalog Tests**: Create unit and integration tests for `CatalogManager`.

### Phase 3: Architecture (Future)
1.  **Connection Factory Refactor**: Extract connection creation logic into `odibi.connections.factory`.
2.  **Transformer Registry Decoupling**: Move registration out of module side-effects.
3.  **Vectorized Utils**: Optimize `hash_columns` in Pandas using PyArrow.
