# Gap Analysis V2: Framework Consistency & Local Execution

**Date:** 2025-11-26
**Scope:** Full Framework Audit (Transformers, Catalog, CLI, Config)

## 1. Consistency & Local Execution
The core patterns (`merge`, `scd2`) have been stabilized for local execution, but the supporting infrastructure lags behind.

### 🔴 Critical Gaps
*   **CatalogManager is Spark-Dependent**:
    *   `resolve_table_path` and `get_average_volume` in `odibi/catalog.py` return `None` if `self.spark` is missing.
    *   This breaks "System Catalog" features (lineage resolution, metrics) for local Pandas runs.
    *   `_ensure_table` has a messy fallback for creating local system tables, with inline imports and weak error handling.
*   **CLI `init` is Broken**:
    *   `odibi init` relies on `examples/templates/`, but that directory is missing from the repository. Only `examples/starter.odibi.yaml` exists. Users cannot create new projects.

### 🟡 Medium Gaps
*   **Transformer Validation**:
    *   `NodeConfig` in `config.py` defines `params` as `Dict[str, Any]`. Specific parameter validation (e.g., `JoinParams`, `DeduplicateParams`) only happens at runtime when the function is called. `odibi validate` will not catch invalid parameters.

## 2. Code Quality & Safety

### 🔴 "Pokemon" Exception Handling
Found dangerous `except Exception:` blocks that swallow errors silently:
*   `odibi/transformers/advanced.py`:
    *   `unpack_struct`: Swallows errors if `tolist()` fails.
    *   `normalize_json`: Swallows JSON parsing errors.
*   `odibi/catalog.py`:
    *   `_table_exists`: Swallows all exceptions during read check.

### 🟡 Hardcoded/Inline Logic
*   `odibi/pipeline.py`: `_build_connections` contains a massive block of conditional logic to instantiate connections. This violates the Open/Closed principle. It should rely on a plugin/factory system more heavily (though `get_connection_factory` exists, it falls back to hardcoded logic for standard types).

## 3. Documentation vs. Reality
*   **Status**: Mostly aligned. `yaml_schema.md` accurately reflects the parameters found in transformer implementations.
*   **Gap**: The schema implies strict typing, but the Pydantic models in `config.py` are loose (`Dict[str, Any]`).

---

# Refactoring Plan

## Phase 1: Fix Critical Reliability Issues (Immediate)
1.  **Fix `odibi init`**:
    *   Restore `examples/templates/` directory with `simple_local.yaml` and other templates.
    *   Update `odibi/cli/init_pipeline.py` to robustly handle missing templates.
2.  **CatalogManager Local Support**:
    *   Implement `resolve_table_path` and `get_average_volume` using `self.engine.read()` or direct Delta/Parquet reading for local mode.
    *   Refactor `_ensure_table` to use a consistent `create_table` abstraction available in `PandasEngine`.

## Phase 2: Safety & Validation
3.  **Remove Pokemon Catching**:
    *   Replace `except Exception: pass` in `advanced.py` with specific exception handling or explicit error logging.
4.  **Stronger Validation**:
    *   Create a `validate_params` utility that maps transformer names to their Pydantic models and runs validation during `odibi validate`.

## Phase 3: Architecture Cleanup
5.  **Connection Factory Refactor**:
    *   Move the massive `if/elif` block in `PipelineManager._build_connections` into dedicated factory methods or the `get_connection_factory` mechanism to clean up `pipeline.py`.
