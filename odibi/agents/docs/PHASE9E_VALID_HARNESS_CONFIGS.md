# Phase 9.E: Valid Learning Harness Configs

**Status:** Complete  
**Depends on:** Phase 9.D (Source Binding)

## Summary

Rewrote learning harness YAML files as minimal, valid `ProjectConfig` files that pass Odibi's schema validation. This enables learning cycles to execute real Odibi pipelines with authentic schema validation and source binding.

## Problem

The previous harness configs used a custom schema with fields like:
- `pipeline_id`, `harness: true`, `learning_only: true`
- `sources`, `transformations`, `outputs` (non-standard)
- `validation`, `metrics` (harness-specific)

This caused `ProjectConfig` validation errors, blocking meaningful execution-based learning.

## Solution

Replaced custom harness schema with minimal valid `ProjectConfig` structure:

```yaml
project: learning_harness_<name>

connections:
  bound_sources:
    type: local
    base_path: "${BOUND_SOURCE_ROOT}"
  artifacts:
    type: local
    base_path: "${ARTIFACTS_ROOT}"

story:
  connection: artifacts
  path: stories/<name>

system:
  connection: artifacts
  path: _system

pipelines:
  - pipeline: <test_pipeline>
    nodes:
      - name: <node>
        read/transform/write: ...
```

## Design Principles

### Minimal but Real
- Uses real `ProjectConfig` schema
- No mock execution or fake runners
- Passes full Pydantic validation

### Single-Concept Pipelines
Each harness tests one stressor:
- `scale_join.odibi.yaml` → Join operations
- `skew_test.odibi.yaml` → Null handling and edge cases
- `schema_drift.odibi.yaml` → Type coercion and schema reconciliation

### Deterministic & Frozen
- Fixed schemas
- All writes use `mode: overwrite`
- No random/timestamp operations
- Version-controlled, not auto-generated

### Source-Binding Compatible
- Reads from `${BOUND_SOURCE_ROOT}/<pool>/...`
- Writes to `${ARTIFACTS_ROOT}/learning_harness/...`
- No hardcoded absolute paths

## What Harness Configs Intentionally Do NOT Cover

| Excluded | Reason |
|----------|--------|
| Production examples | These are microbenchmarks, not templates |
| Multi-pipeline projects | Each config tests one concept |
| Complex business logic | Focus on engine stress, not domain logic |
| External connections | Local filesystem only for isolation |
| Alert/retry configs | Not needed for learning cycles |
| Delta format | Uses parquet for simplicity |

## File Changes

### Updated Harness Configs
- `.odibi/learning_harness/scale_join.odibi.yaml`
- `.odibi/learning_harness/skew_test.odibi.yaml`
- `.odibi/learning_harness/schema_drift.odibi.yaml`

### New Test File
- `agents/core/tests/test_learning_harness_config.py`

## Test Coverage

16 tests validating:
- Schema existence and YAML validity
- ProjectConfig Pydantic validation
- Source binding path placeholders
- Pipeline structure requirements
- Single-stressor focus per config
- Determinism guarantees

## Integration with Learning Cycles

After this phase:
1. Learning cycles discover harness configs
2. Source binder injects `BOUND_SOURCE_ROOT` and `ARTIFACTS_ROOT`
3. Odibi engine loads configs via `load_config_from_file()`
4. Pipelines execute against bound source pools
5. Real execution evidence is captured
6. Errors surface through ObserverAgent

## Safety Invariants Preserved

- `max_improvements = 0` enforcement unchanged
- Disk guard behavior unchanged
- Heartbeat logic unchanged
- GuardedCycleRunner behavior unchanged
- Determinism guarantees unchanged
