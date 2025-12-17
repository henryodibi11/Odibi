# Phase 9.D: Source Binding and Reality Injection

**Status:** Implemented  
**Purpose:** Make learning cycles non-trivial without enabling auto-modification

## Overview

Phase 9.D addresses a critical gap: learning cycles resolve source pools (tier100gb / tier600gb) as metadata only, but no resolved sources are actually bound to execution. This means pipelines read trivial local CSVs, and learning mode is structurally correct but under-exercised.

This phase makes learning cycles non-trivial by:
1. **Binding resolved sources to execution**
2. **Introducing a scale harness**
3. **Detecting trivial cycles**

## Goals

### GOAL 1: Bind Resolved Sources to Execution

Source binding ensures that when a profile selects `tier100gb` / `tier600gb`:
- Resolved source pools are mounted deterministically
- Pipelines can only read from bound source paths
- Binding is read-only
- No network access
- No mutation
- Deterministic paths per cycle

### GOAL 2: Learning Harness

Hand-written Odibi pipelines that stress-test the system at scale:
- Located under `.odibi/learning_harness/`
- NOT auto-generated
- NOT modified by agents
- Version-controlled

Pipelines intentionally exercise:
- Wide schemas (100+ columns)
- Skewed joins (90%+ single value)
- Null-heavy data (50%+ null rates)
- Schema drift across partitions
- Large aggregations (memory pressure)

### GOAL 3: Trivial Cycle Detection

Runtime warning when a learning cycle reads zero bytes from bound source pools:

```
⚠️ Learning cycle executed without exercising bound source pools.
   This cycle observed trivial workloads only.
```

No failure—warning only.

## Architecture

### Source Binder (`source_binder.py`)

```
SourceResolver._resolve_sources()
        ↓
    SourceResolutionResult
        ↓
    SourceBinder.bind()
        ↓
    SourceBindingResult
        ↓
    BoundSourceMap
        ↓
    ExecutionGateway.bind_sources()
        ↓
    BOUND_SOURCE_ROOT injected into environment
```

### Environment Variable Injection

When `ExecutionGateway.bind_sources()` is called, it:
1. Stores the `source_cache_root` from the context
2. Injects `BOUND_SOURCE_ROOT` into subprocess environment
3. Provides both WSL and Windows path variants

Environment variables available to pipelines:

| Variable | Description | Example |
|----------|-------------|---------|
| `BOUND_SOURCE_ROOT` | WSL-formatted bound source path | `/mnt/d/odibi/.odibi/source_cache` |
| `BOUND_SOURCE_ROOT_WIN` | Windows-formatted path | `d:\odibi\.odibi\source_cache` |
| `ARTIFACTS_ROOT` | WSL-formatted artifacts path | `/mnt/d/odibi/.odibi/artifacts` |
| `ARTIFACTS_ROOT_WIN` | Windows-formatted path | `d:\odibi\.odibi\artifacts` |
| `ODIBI_ROOT` | WSL-formatted odibi root | `/mnt/d/odibi` |
| `ODIBI_ROOT_WIN` | Windows-formatted path | `d:\odibi` |

### Key Components

#### `SourceBinder`
- Materializes sources under `.odibi/source_cache/<cycle_id>/<pool_name>/`
- Creates `BoundSourceMap` for path enforcement
- Uses symlinks (Unix) or path references (Windows)

#### `BoundSourceMap`
- Immutable mapping of pool_id → bound path
- Enforces path prefix validation
- Provides pool lookup by path

#### `TrivialCycleDetector`
- Tracks which pools were bound
- Records access from evidence
- Emits warning if zero bytes read from bound pools

### Binding Flow

```
1. GuardedCycleRunner.start_learning_cycle()
2. → CycleRunner.start_cycle()  [creates CycleState with source_resolution]
3. → GuardedCycleRunner._bind_sources_for_cycle()
4.   → SourceBinder.bind(resolution)
5.   → SourceBinder.create_execution_context()
6.   → ExecutionGateway.bind_sources(context)
7.   → TrivialCycleDetector.record_binding()
```

### Trivial Detection Flow

```
1. GuardedCycleRunner.run_learning_cycle()
2. → [run cycle steps]
3. → _update_trivial_detector_from_evidence()
4. → TrivialCycleDetector.get_warning()
5. → [emit warning event if trivial]
```

## Learning Harness Pipelines

Located at `.odibi/learning_harness/`:

| Pipeline | Purpose | Exercises |
|----------|---------|-----------|
| `scale_join.odibi.yaml` | Multi-way joins at scale | Wide schemas, 10M+ rows, memory pressure |
| `schema_drift.odibi.yaml` | Schema evolution handling | Type changes, missing columns, coercion |
| `skew_test.odibi.yaml` | Data skew resilience | 90%+ skew, 50%+ nulls, cardinality explosion |

### Invariants

These pipelines:
- Are **NOT** auto-generated
- Are **NOT** modified by agents
- Reference bound sources via `${BOUND_SOURCE_ROOT}`
- Are deterministic
- Are executable via normal discovery

## LearningCycleResult Extensions

New fields in `LearningCycleResult`:

```python
source_binding: Optional[dict[str, Any]]      # Binding details
trivial_cycle_warning: Optional[dict[str, Any]]  # Warning if trivial
bytes_read_from_sources: int                   # Total bytes read
pools_bound: int                               # Number of pools bound
pools_used: int                                # Number of pools accessed
```

## Safety Invariants

All Phase 9.A invariants continue to hold:

- ✅ `max_improvements = 0` enforced
- ✅ ImprovementAgent mechanically skipped
- ✅ ReviewerAgent mechanically skipped
- ✅ Disk guard still enforced
- ✅ Heartbeat still written
- ✅ Determinism preserved
- ✅ Learning remains observation-only

Additional Phase 9.D constraints:

- ❌ NO YAML generation
- ❌ NO pipeline modification
- ❌ NO ImprovementAgent execution
- ❌ NO auto-testing generated configs
- ❌ NO relaxation of learning-mode guards

## Usage

### Running with Source Binding

```python
from agents.core.autonomous_learning import (
    AutonomousLearningScheduler,
    LearningCycleConfig,
)

scheduler = AutonomousLearningScheduler(odibi_root="d:/odibi")
config = LearningCycleConfig(
    project_root="d:/odibi/examples",
    allowed_tiers=["tier100gb", "tier600gb"],
)
result = scheduler.run_single_cycle(config)

# Check binding
print(f"Pools bound: {result.pools_bound}")
print(f"Pools used: {result.pools_used}")
print(f"Bytes read: {result.bytes_read_from_sources}")

# Check for trivial cycle warning
if result.trivial_cycle_warning:
    print(f"WARNING: {result.trivial_cycle_warning['message']}")
```

### Detecting Trivial Cycles

```python
from agents.core.source_binder import detect_trivial_cycle

warning = detect_trivial_cycle(
    cycle_id="test-cycle",
    bound_map=binding_result.bound_source_map,
    source_usage=evidence.source_usage,
)

if warning:
    logger.warning(warning.message)
```

## Success Criteria

After Phase 9.D:

| Metric | Before | After |
|--------|--------|-------|
| Cycle duration | Seconds | Minutes to hours |
| Evidence includes | Basic metrics | SourceUsageSummary |
| Heartbeat reflects | Minimal activity | Real disk + data activity |
| Silence means | Emptiness | Stability |

## Files Changed

- `agents/core/source_binder.py` - New module
- `agents/core/autonomous_learning.py` - Source binding integration
- `agents/core/tests/test_source_binder.py` - Tests
- `.odibi/learning_harness/` - Harness pipelines
- `agents/docs/PHASE9D_SOURCE_BINDING.md` - This documentation

## Next Steps

System is ready for **Phase 10: Proposal Mode** (not implemented in this phase).
