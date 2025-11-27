# Odibi Framework Gap Analysis & Test Plan

## 1. Gap Analysis

### 1.1. Pandas Engine Limitations
The Pandas engine (`odibi/engine/pandas_engine.py`) is a "second-class citizen" compared to Spark.
*   **No Streaming**: Explicitly disabled.
*   **Limited Delta Support**: Relies on `deltalake` library (optional dependency).
*   **In-Memory Only**: Naive implementations for Merge and SCD2 read the entire target dataset into memory, which will fail for large datasets (violating scalability).
*   **Missing Partitioning**: Partitioning logic is referenced but not fully implemented/exposed in the write path for Pandas.

### 1.2. Catalog & Metadata Gaps
The `CatalogManager` (`odibi/catalog.py`) is **strictly coupled to Spark**.
*   Methods check `if not self.spark: return`.
*   **Consequence**: In Pandas mode, no system tables (`meta_tables`, `meta_runs`, `meta_state`) are created or updated.
*   **Broken Observability**: Runs are not logged to `meta_runs`.
*   **Broken Lineage**: Assets are not registered in `meta_tables`.
*   **Broken State Management (Partial)**: While `LocalFileStateBackend` exists, the system defaults/fallbacks are unclear, and `CatalogStateBackend` is unusable in Pandas mode.

### 1.3. Pattern & Transformer Path Resolution
Transformers (`merge`, `scd2`) fail to resolve Logical Table Names to Physical Paths in Pandas mode.
*   **Issue**: `_merge_pandas` and `_scd2_pandas` take `target` parameter and use it directly as a file path (`os.path.exists(path)`).
*   **Impact**: A config like `target: silver.customers` will fail in Pandas mode because it looks for a file named "silver.customers" in the current directory, rather than resolving it to `data/silver/customers/` via the Engine's connection logic.
*   **Violation**: Violates the "Logical > Physical" abstraction principle.

### 1.4. Validation & Contracts
*   **Weak Typing**: Many methods return `Any` or `Dict[str, Any]` instead of strong Pydantic models.
*   **Error Handling**: "Pokemon" exception handling (`except Exception: pass`) is prevalent in `get_table_schema` and existence checks, masking potential configuration errors.

### 1.5. Test Execution Findings (New)
During the execution of the Local Test Campaign, the following additional issues were discovered:

1.  **PyArrow Type Validation Failure**:
    *   The default behavior of `PandasEngine` enables PyArrow backend (`use_arrow=True`).
    *   This results in dtypes like `int64[pyarrow]`.
    *   `PandasEngine.validate_schema` uses a hardcoded string check against standard numpy types (e.g. `int64`), causing valid schemas to fail validation.
    *   *Workaround*: Disabled `use_arrow` in test config.

2.  **NodeConfig Validation Strictness**:
    *   `NodeConfig` requires at least one of `read`, `transform`, `write`, or `transformer`.
    *   Providing `params` for a pattern is not enough; `transformer` must be explicitly set.

3.  **Context Construction**:
    *   `PandasContext` does not accept arguments. `EngineContext` must be manually constructed to wrap `PandasContext` when invoking Patterns directly.
    *   `MergePattern` and `SCD2Pattern` rely on `EngineContext` but the Transformer implementation ignores the context's engine/connection and resolves paths natively using `os.path`, failing if logical names are used.

---

## 2. Structured Test Plan (Local Pandas Campaign)

This test campaign validates the framework using **only local resources** and the **Pandas engine**.

### Phase 1: Core Foundations
*   **Goal**: Validate basic I/O and Configuration.
*   **Tests**:
    *   Load `odibi_config.yml`.
    *   Read CSV/JSON using `PandasEngine`.
    *   Write Parquet using `PandasEngine`.
    *   Validate Schema enforcement (types/required columns).

### Phase 2: Catalog (Skipped)
*   *Note*: Cannot be tested in Pandas mode due to Spark dependency.

### Phase 3: State & HWM
*   **Goal**: Validate Local State Backend.
*   **Tests**:
    *   Initialize `LocalFileStateBackend`.
    *   `set_hwm("test_node", "2023-01-01")`.
    *   `get_hwm("test_node")` matches.
    *   Verify persistence to `.odibi/state.json`.

### Phase 4: Base Patterns (Merge)
*   **Goal**: Validate Upsert Logic.
*   **Tests**:
    *   Create initial Parquet file (Target).
    *   Create "Source" DataFrame with new + updated records.
    *   Run `MergePattern` (Strategy: Upsert).
    *   Validate:
        *   New records added.
        *   Existing records updated.
        *   Unchanged records touched.

### Phase 5: Advanced Patterns (SCD2)
*   **Goal**: Validate History Tracking.
*   **Tests**:
    *   Create initial Target (Customer A at Location 1).
    *   Create Source (Customer A at Location 2).
    *   Run `SCD2Pattern`.
    *   Validate:
        *   Old record closed (`valid_to` updated).
        *   New record added (`is_current=True`).
        *   History preserved.

### Phase 6: Cross-Pipeline Interaction
*   **Goal**: Simulate End-to-End flow.
*   **Tests**:
    *   **Bronze**: Raw CSV -> Parquet (Snapshot).
    *   **Silver**: Bronze Parquet -> Silver Parquet (Merge).
    *   **Gold**: Silver Parquet -> Gold Aggregates (Fact).

### Phase 7: Failure Modes
*   **Goal**: Ensure "Loud Failure".
*   **Tests**:
    *   Missing Input File -> `FileNotFoundError`.
    *   Schema Mismatch -> Validation Error.
    *   Invalid Pattern Config -> `ValueError`.

### Phase 8: Scaling & Performance
*   **Goal**: Stress Test Local Engine.
*   **Tests**:
    *   Process 100k rows.
    *   Measure time.
    *   Verify memory usage (implicit check via no-crash).
