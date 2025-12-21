# New GitHub Issues (Copy-Paste Ready)

---

## Issue #32

**Title:** CLI missing `--tag` flag for running nodes by tag

**Labels:** bug, cli, priority-high

**Body:**

```
The documentation describes tag-based execution (`odibi run --tag daily`), but the `--tag` flag is not implemented in the CLI.

## Evidence

- Config: `NodeConfig.tags` field exists in `odibi/config.py`
- Docs reference: `odibi run --tag daily`
- CLI: No `--tag` argument in `odibi/cli/main.py`

## Expected Behavior

Users should be able to run only nodes with specific tags:

```bash
odibi run pipeline.yaml --tag daily
```

## Fix

Add `--tag` argument to the run parser and filter nodes in `Pipeline.run()`.
```

---

## Issue #33

**Title:** CLI missing `--node` and `--pipeline` flags for orchestration

**Labels:** bug, cli, orchestration, priority-high

**Body:**

```
Orchestration templates generate commands with flags that don't exist in the CLI.

## Evidence

`odibi/orchestration/airflow.py` and `odibi/orchestration/dagster.py` generate:

```bash
odibi run --pipeline {{ pipeline_name }} --node {{ node.name }}
```

But these flags do not exist in `odibi/cli/main.py`.

## Impact

Generated Airflow/Dagster DAGs will fail to execute.

## Fix

Add `--pipeline` and `--node` arguments to the run command.
```

---

## Issue #34

**Title:** `materialized` config option has no effect

**Labels:** bug, config, priority-medium

**Body:**

```
The `materialized` field (`table`, `view`, `incremental`) is defined in `NodeConfig` but never read or used in node execution.

## Evidence

- Config defines `materialized` field in `odibi/config.py`
- No references to `config.materialized` in `odibi/node.py`

## Impact

Users can set this option but it's silently ignored.

## Fix

Either implement materialization strategies or remove/deprecate the field.
```

---

## Issue #35

**Title:** Add `POLARS` to `EngineType` enum

**Labels:** enhancement, engine, priority-medium

**Body:**

```
`PolarsEngine` exists and is registered, but `EngineType` enum only has `SPARK` and `PANDAS`.

## Evidence

- `odibi/engine/polars_engine.py` - Full implementation exists
- `odibi/engine/registry.py` - Registered as "polars"
- `odibi/config.py` - Only SPARK/PANDAS in enum

## Impact

Users cannot set `engine: polars` in YAML config.

## Fix

Add `POLARS = "polars"` to `EngineType` enum in `odibi/config.py`.
```

---

## Issue #36

**Title:** PandasEngine missing `as_of_timestamp` time travel

**Labels:** bug, engine-parity, priority-low

**Body:**

```
`as_of_timestamp` is accepted in read config but only `as_of_version` is implemented for Delta time travel in PandasEngine.

## Evidence

- Method signature accepts both in `odibi/engine/pandas_engine.py`
- Only version is used, timestamp is ignored
- SparkEngine supports both

## Impact

Timestamp-based time travel silently ignored in Pandas engine.

## Fix

Implement `timestampAsOf` option in PandasEngine's Delta read logic.
```

---

## Issue #37

**Title:** PolarsEngine missing abstract methods

**Labels:** enhancement, engine-parity, priority-low

**Body:**

```
PolarsEngine doesn't implement all abstract methods from `Engine` base class.

## Missing Methods

- `harmonize_schema()` - Not implemented
- `anonymize()` - Not implemented
- `add_write_metadata()` - Not implemented
- `table_exists()` - Partial (file only)
- `maintain_table()` - Not implemented

## Impact

Polars pipelines will fail if these features are used.

## Fix

Implement remaining abstract methods or raise `NotImplementedError` with clear message.
```

---

## Issue #38

**Title:** `environments` config not implemented

**Labels:** enhancement, config, priority-low

**Body:**

```
The `environments` field is defined in `ProjectConfig` but does nothing.

## Evidence

The validator in `odibi/config.py` explicitly notes:

```python
@model_validator(mode="after")
def check_environments_not_implemented(self):
    # Implemented in Phase 3
    return self
```

## Impact

Users can define environments but they're silently ignored.

## Fix

Either implement environment switching or remove the field with a deprecation notice.
```

---

## Summary

| Issue | Title | Priority |
|-------|-------|----------|
| #32 | CLI missing `--tag` flag | High |
| #33 | CLI missing `--node`/`--pipeline` flags | High |
| #34 | `materialized` config ignored | Medium |
| #35 | Add POLARS to EngineType enum | Medium |
| #36 | PandasEngine missing timestamp time travel | Low |
| #37 | PolarsEngine missing abstract methods | Low |
| #38 | `environments` config not implemented | Low |
