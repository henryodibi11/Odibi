# Odibi Feature Parity Audit Report

**Generated:** 2024  
**Scope:** Config vs Implementation, Engine Parity, CLI vs API, Docs vs Code

---

## Executive Summary

This audit identifies feature parity gaps across the Odibi framework. Issues are categorized by severity:

| Severity | Count | Description |
|----------|-------|-------------|
| 🔴 **Critical** | 3 | Documented but not implemented, or silently ignored |
| 🟡 **Medium** | 8 | Missing in one engine/mode but present in another |
| 🟢 **Low** | 4 | Minor inconsistencies or missing documentation |

---

## 🔴 Critical Issues

### 1. CLI Missing `--tag` Flag (Documented but Not Implemented)

**Location:** `odibi/cli/main.py` (run command)

**Issue:** The documentation and config reference describe tag-based execution (`odibi run --tag daily`), but the `--tag` flag is **not implemented** in the CLI.

**Evidence:**
- Config: `NodeConfig.tags` field exists ([config.py#L2030](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/odibi/config.py#L2030))
- Docs: "Run only this with `odibi run --tag daily`" ([yaml_schema.md#L127](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/docs/reference/yaml_schema.md#L127))
- CLI: No `--tag` argument in [cli/main.py](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/odibi/cli/main.py#L52-L77)

**Impact:** Users cannot run pipelines by tag despite documentation saying they can.

**Fix:** Add `--tag` argument to the run parser and filter nodes in `Pipeline.run()`.

---

### 2. CLI Missing `--node` and `--pipeline` Flags for Run Command

**Location:** `odibi/cli/main.py` (run command)

**Issue:** Orchestration templates (`odibi/orchestration/airflow.py`, `odibi/orchestration/dagster.py`) generate commands like:
```bash
odibi run --pipeline {{ pipeline_name }} --node {{ node.name }}
```
But these flags **do not exist** in the CLI.

**Evidence:**
- [airflow.py#L39](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/odibi/orchestration/airflow.py#L39): `--pipeline` and `--node` usage
- [dagster.py#L61](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/odibi/orchestration/dagster.py#L61): `--pipeline` and `--node` usage
- CLI main.py: No such flags exist

**Impact:** Generated Airflow/Dagster DAGs will fail to execute.

**Fix:** Add `--pipeline` and `--node` arguments to the run command.

---

### 3. `materialized` Config Option Has No Effect

**Location:** `odibi/config.py` (NodeConfig), `odibi/node.py`

**Issue:** The `materialized` field (`table`, `view`, `incremental`) is defined in config but **never read or used** in node execution.

**Evidence:**
- Config: [config.py#L2018](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/odibi/config.py#L2018) defines `materialized` field
- Node.py: No references to `config.materialized` in execution logic
- Docs: Referenced in [yaml_schema.md#L338](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/docs/reference/yaml_schema.md#L338)

**Impact:** Users can set this option but it's silently ignored.

**Fix:** Either implement materialization strategies or remove/deprecate the field.

---

## 🟡 Medium Issues

### 4. Polars Engine Not Exposed in Config Enum

**Location:** `odibi/config.py`, `odibi/enums.py`

**Issue:** `PolarsEngine` exists and is registered, but `EngineType` enum only has `SPARK` and `PANDAS`.

**Evidence:**
- [polars_engine.py](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/odibi/engine/polars_engine.py): Full implementation exists
- [engine/registry.py#L25](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/odibi/engine/registry.py#L25): Registered as "polars"
- [config.py#L14-L18](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/odibi/config.py#L14-L18): Only SPARK/PANDAS

**Impact:** Users cannot set `engine: polars` in YAML config.

**Fix:** Add `POLARS = "polars"` to `EngineType` enum.

---

### 5. Streaming Write Not Implemented (Read Only)

**Location:** `odibi/engine/spark_engine.py`

**Issue:** Streaming **read** is supported via `readStream`, but streaming **write** is not implemented - `write()` method doesn't handle streaming DataFrames.

**Evidence:**
- Read supports streaming: [spark_engine.py#L506-L507](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/odibi/engine/spark_engine.py#L506-L507)
- Write has no streaming handling (only batch write modes)
- Config allows `streaming: true` at node level

**Impact:** Streaming pipelines will fail on write phase.

**Fix:** Add `writeStream` handling in `write()` method for streaming DataFrames.

---

### 6. `partition_by` Not Applied for Delta Merge Operations

**Location:** `odibi/engine/pandas_engine.py`, `odibi/engine/spark_engine.py`

**Issue:** For `upsert`/`append_once` modes with Delta tables, `partition_by` is extracted but applied **after** the merge logic completes. The merge itself doesn't partition-optimize.

**Evidence:**
- Spark: [spark_engine.py#L800-L805](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/odibi/engine/spark_engine.py#L800-L805) - partition_by applied post-merge
- Pandas: Delta merge at [pandas_engine.py#L773-L810](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/odibi/engine/pandas_engine.py#L773-L810) doesn't use partition_by

**Impact:** Performance not optimal for partitioned upsert operations.

---

### 7. PandasEngine Missing `as_of_timestamp` Time Travel

**Location:** `odibi/engine/pandas_engine.py`

**Issue:** `as_of_timestamp` is accepted but only `as_of_version` (versionAsOf) is implemented for Delta time travel.

**Evidence:**
- Method signature accepts both: [pandas_engine.py#L187](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/odibi/engine/pandas_engine.py#L187)
- Only version is used: [pandas_engine.py#L246-L247](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/odibi/engine/pandas_engine.py#L246-L247)
- SparkEngine supports both: [spark_engine.py#L443-L448](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/odibi/engine/spark_engine.py#L443-L448)

**Impact:** Timestamp-based time travel silently ignored in Pandas engine.

---

### 8. PolarsEngine Missing Several Abstract Methods

**Location:** `odibi/engine/polars_engine.py`

**Issue:** PolarsEngine doesn't implement all abstract methods from `Engine` base class:
- `harmonize_schema()` - Not implemented
- `anonymize()` - Not implemented
- `add_write_metadata()` - Not implemented
- `table_exists()` - Partial (file only)
- `maintain_table()` - Not implemented

**Impact:** Polars pipelines will fail if these features are used.

---

### 9. `first_run_query` Only Works with Target Table Check

**Location:** `odibi/node.py` (NodeExecutor._execute_read_phase)

**Issue:** `first_run_query` is only applied when `write` config exists and target table doesn't exist. If a node has `read` + `transform` but no `write`, the query is never used.

**Evidence:** [node.py#L344-L352](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/odibi/node.py#L344-L352)

---

### 10. `delete_detection` Config Never Read

**Location:** `odibi/config.py`, `odibi/node.py`

**Issue:** `DeleteDetectionConfig` is fully defined with validation, but the node executor never reads or applies delete detection logic.

**Evidence:**
- Config: [config.py#L160-L293](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/odibi/config.py#L160-L293) - full implementation
- Transformer exists: [transformers/delete_detection.py](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/odibi/transformers/delete_detection.py)
- Not wired: Must be used as explicit transform step, not automatic

**Clarification:** This works if user explicitly adds `operation: detect_deletes` in transform steps. The config examples show this is the intended pattern.

---

### 11. `environments` Config Not Validated or Applied

**Location:** `odibi/config.py` (ProjectConfig)

**Issue:** The `environments` field is defined but has a validator that does nothing:
```python
@model_validator(mode="after")
def check_environments_not_implemented(self):
    # Implemented in Phase 3
    return self
```

**Evidence:** [config.py#L980-L984](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/odibi/config.py#L980-L984)

**Impact:** Users can define environments but they're silently ignored.

---

## 🟢 Low Issues

### 12. Docs Reference Non-Existent Commands

**Location:** `docs/guides/cli_master_guide.md`

**Issue:** Docs reference `odibi init-vscode` but this command doesn't exist in CLI.

**Evidence:** [cli_master_guide.md#L56-L59](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/docs/guides/cli_master_guide.md#L56-L59)

---

### 13. `retry.backoff` Types Inconsistent

**Location:** `odibi/config.py`, `odibi/node.py`

**Issue:** Config defines `BackoffStrategy` enum with values `exponential`, `linear`, `constant`, but node retry logic only handles `exponential` and `linear`.

**Evidence:**
- Config: [config.py#L730-L733](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/odibi/config.py#L730-L733) - includes `CONSTANT`
- Node: [node.py#L963-L968](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/odibi/node.py#L963-L968) - no `constant` handling

---

### 14. Pre/Post SQL Not Documented in CLI Docs

**Location:** `docs/features/cli.md`

**Issue:** `pre_sql` and `post_sql` node options are implemented but not mentioned in CLI documentation.

---

### 15. `validation_mode` on Connections Never Used

**Location:** `odibi/config.py` (BaseConnectionConfig)

**Issue:** All connection configs inherit `validation_mode: ValidationMode` but this field is never read during pipeline execution.

---

## Write Mode Support Matrix

| Mode | PandasEngine | SparkEngine | Notes |
|------|--------------|-------------|-------|
| `overwrite` | ✅ | ✅ | Both support all formats |
| `append` | ✅ | ✅ | Both support all formats |
| `upsert` | ✅ Delta | ✅ Delta | Both require `keys` option |
| `append_once` | ✅ Delta | ✅ Delta | Both require `keys` option |
| `partition_by` | ✅ | ✅ | Applied via options dict |
| `merge_keys` | ❌ | ❌ | Use `keys` in options instead |

---

## CLI vs API Feature Matrix

| Feature | CLI | Python API | Notes |
|---------|-----|------------|-------|
| Run pipeline | ✅ | ✅ | |
| Dry run | ✅ | ✅ | |
| Resume | ✅ | ✅ | |
| Parallel | ✅ | ✅ | |
| Filter by tag | ❌ | ❌ | **Not implemented** |
| Filter by node | ❌ | ❌ | **Not implemented** |
| Filter by pipeline | ❌ | ✅ | CLI doesn't support |
| On-error override | ✅ | ✅ | |

---

## Recommendations

### High Priority
1. **Add `--tag`, `--node`, `--pipeline` to CLI run command** - Breaks orchestration exports
2. **Implement or remove `materialized` config** - Silent no-op confuses users
3. **Add POLARS to EngineType enum** - Blocks Polars adoption

### Medium Priority
4. Implement streaming write support in SparkEngine
5. Add `as_of_timestamp` support to PandasEngine
6. Complete PolarsEngine abstract method implementations

### Low Priority
7. Add `constant` backoff handling
8. Document pre_sql/post_sql in CLI docs
9. Remove or implement `validation_mode` on connections
10. Remove or implement `environments` config
