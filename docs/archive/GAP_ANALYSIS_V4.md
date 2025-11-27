# Odibi V4 "Day 2" Gap Analysis

**Date:** November 26, 2025
**Scope:** Framework audit for robustness, parity, and documentation synchronization.

## 🚨 Critical Risks (Priority: High)

These issues can cause runtime crashes or silent failures in production.

### 1. Privacy Feature Crash (Pandas Engine)
- **Issue:** `NodeExecutor` calls `self.engine.anonymize()` when `privacy` config is present.
- **Status:** Implemented in `SparkEngine`, but **MISSING** in `PandasEngine`.
- **Impact:** configuring `privacy` on a Pandas pipeline will raise `AttributeError: 'PandasEngine' object has no attribute 'anonymize'`.
- **Fix:** Implement `anonymize` in `PandasEngine` using standard pandas string/apply methods.

### 2. Dead Code: Schema Policy
- **Issue:** `NodeConfig` defines `schema_policy` (OnMissingColumns, OnNewColumns), and it appears in `yaml_schema.md`.
- **Status:** **Node.py does not reference `schema_policy`**. The configuration is parsed but completely ignored during execution.
- **Impact:** Users expecting schema enforcement or evolution will experience default engine behavior (likely failure or silent drift) instead of configured policy.
- **Fix:** Update `NodeExecutor._execute_write_phase` (or Transform phase) to call `engine.harmonize_schema` using the config.

### 3. "Pokemon" Exception Handling
- **Issue:** Broad `except Exception: pass` blocks mask critical configuration or permission errors.
- **Locations:**
  - `odibi/engine/pandas_engine.py`: `get_table_schema` (lines 767-768) - Masks file access errors.
  - `odibi/engine/pandas_engine.py`: `vacuum_delta` - Masks `deltalake` import errors vs permission errors.
  - `odibi/catalog.py`: `_table_exists` - Masks connection issues.
- **Fix:** Catch specific exceptions (`ImportError`, `FileNotFoundError`, `PermissionError`) and log warnings for others.

---

## ⚖️ Engine Parity & Inconsistencies

Differences between Spark and Pandas engines that affect portability.

| Feature | Spark Engine | Pandas Engine | Status |
| :--- | :--- | :--- | :--- |
| **Anonymize** | ✅ Implemented (`sha2`, `regexp_replace`) | ❌ Missing | **Critical** |
| **Harmonize Schema** | ✅ Implemented | ❌ Missing | **Critical** (for Schema Policy) |
| **Z-Order** | ✅ Supported (`zorder_by`) | ❌ Ignored (Filtered out in `_write_delta`) | Inconsistent |
| **Vacuum** | ✅ Implemented | ✅ Implemented | Parity Achieved |
| **Partitioning** | ✅ Supported | ✅ Supported (Passed to `write_deltalake`) | Parity Achieved |

**Note:** `PandasEngine` explicitly filters write options for Delta, allowing only specific keys. `zorder_by` is not in the allowed list, so it is silently ignored.

---

## 📚 Documentation Gaps

Discrepancies between `docs/` and actual code behavior.

1.  **Smart Read / First Run Logic**:
    - **Behavior:** `NodeExecutor` checks if the target table exists. If not, it forces a Full Load (skips incremental filtering).
    - **Doc Status:** Undocumented in `yaml_schema.md` or `guides/`. Users might be confused why their incremental logic isn't applying on the first run.

2.  **Schema Policy**:
    - **Behavior:** Feature is non-functional (see Critical Risks).
    - **Doc Status:** Documented as a supported feature.

3.  **Privacy**:
    - **Behavior:** Crashes on Pandas.
    - **Doc Status:** Implied as supported for all engines.

---

## 🛠️ Refactoring & Code Quality

1.  **Typing**:
    - `odibi/engine/pandas_engine.py` relies heavily on `Any` and `Dict[str, Any]`.
    - **Recommendation:** Use `PandasContext` (if available) or specific TypeVars for DataFrames to improve IDE support and static analysis.

2.  **Unit Test Coverage**:
    - **Missing:** `tests/unit/test_node_executor.py`. The core orchestration logic (NodeExecutor) is not tested in isolation.
    - **Coverage:** `test_pipeline.py` likely covers happy paths, but edge cases in `NodeExecutor` (like `restore`, `dry_run`, complex `incremental` states) need dedicated unit tests.

3.  **Plugin Discovery**:
    - **Status:** `PipelineManager` correctly implements auto-discovery of `transforms.py`. Verified robust.

## Action Plan

1.  **Hotfix**: Implement `anonymize` in `PandasEngine`.
2.  **Hotfix**: Wire up `schema_policy` in `NodeExecutor` and implement `harmonize_schema` in `PandasEngine`.
3.  **Docs**: Add "Smart Read" section to `guides/incremental_loading.md` (if exists) or `yaml_schema.md`.
4.  **Test**: Create `tests/unit/test_node_executor.py`.
