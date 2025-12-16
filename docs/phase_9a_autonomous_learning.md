# Phase 9.A: Guarded Autonomous Learning (Observation-Only)

## Implementation Summary

Phase 9.A enables unattended, large-scale learning cycles that safely execute real pipelines against frozen datasets while maintaining strict safety guarantees.

### Key Components

| Component | Location | Purpose |
|-----------|----------|---------|
| `autonomous_learning.py` | `agents/core/` | Core module with scheduler, guards, and config |
| `LearningModeGuard` | `agents/core/autonomous_learning.py` | Runtime safety enforcement |
| `GuardedCycleRunner` | `agents/core/autonomous_learning.py` | Extended CycleRunner with learning guards |
| `AutonomousLearningScheduler` | `agents/core/autonomous_learning.py` | Multi-cycle session management |
| Learning Mode Disclaimer | `agents/core/reports.py` | Report section for learning cycles |
| Observation Indexing | `agents/core/indexing.py` | Extended to index learning observations |

### Files Modified

1. **`agents/core/autonomous_learning.py`** (NEW)
   - `LearningCycleConfig`: Configuration with validation for learning mode
   - `LearningModeGuard`: Runtime guard that detects violations
   - `GuardedCycleRunner`: CycleRunner that skips improvement/review steps
   - `AutonomousLearningScheduler`: Session manager for multi-cycle runs
   - `validate_learning_profile()`: YAML profile validation

2. **`agents/core/reports.py`**
   - Added `_generate_learning_mode_disclaimer()` method
   - Reports now include "Learning Mode — No Changes Applied" disclaimer

3. **`agents/core/indexing.py`**
   - Added `is_learning_mode`, `observations`, `patterns_discovered` fields
   - Extended `_build_document()` to extract observations and patterns
   - Added `_extract_observations()` and `_extract_patterns()` methods

4. **`agents/core/__init__.py`**
   - Exported all new Phase 9.A components

5. **`agents/core/tests/test_autonomous_learning.py`** (NEW)
   - 38 comprehensive tests for all invariants

---

## Verification Checklist

### ✅ Phase 9.A Invariants (ALL VERIFIED)

| Invariant | Status | Enforcement Mechanism |
|-----------|--------|----------------------|
| ❌ No ImprovementAgent execution | ✅ | `GUARDED_SKIP_STEPS`, `LearningModeGuard` |
| ❌ No code edits | ✅ | `LearningModeGuard.assert_no_code_edits()` |
| ❌ No file writes outside evidence/reports/artifacts/indexes | ✅ | `ALLOWED_WRITE_PATHS` whitelist |
| ❌ No source downloads | ✅ | Profile `require_frozen=True` enforced |
| ❌ No schema inference | ✅ | Learning mode uses frozen pools only |
| ❌ No agent memory writes outside LEARNING scope | ✅ | `memory_scope: "learning"` in context |
| ❌ No retries or recovery logic | ✅ | Fail-fast on any error |

### ✅ Functional Requirements

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| Execute real pipelines | ✅ | `GuardedCycleRunner.run_learning_cycle()` |
| Collect execution evidence | ✅ | `ExecutionEvidence` artifacts persisted |
| Produce human-readable reports | ✅ | `CycleReportGenerator` with learning disclaimer |
| Accumulate indexed learnings | ✅ | `CycleIndexManager` with observations/patterns |
| Wall-clock limit enforcement | ✅ | `CycleTerminationEnforcer` + scheduler limits |
| Step limit enforcement | ✅ | `max_steps` in `LearningCycleConfig` |
| Clean termination on error | ✅ | Try/except with `LearningCycleResult.status=FAILED` |

### ✅ Profile Validation

The `large_scale_learning.yaml` profile is validated for:

- `mode: learning` ✅
- `max_improvements: 0` ✅
- `require_frozen: true` ✅
- `deterministic: true` ✅
- `allow_messy: true` ✅
- `allow_tier_mixing: false` ✅
- `tiers: tier100gb, tier600gb` ✅

---

## Usage

### Run a Single Learning Cycle

```python
from agents.core import (
    AutonomousLearningScheduler,
    LearningCycleConfig,
)

scheduler = AutonomousLearningScheduler(odibi_root="d:/odibi")

config = LearningCycleConfig(
    project_root="d:/odibi/examples",
    max_runtime_hours=8.0,
)

result = scheduler.run_single_cycle(config)
print(f"Cycle {result.cycle_id}: {result.status.value}")
```

### Run a Multi-Cycle Session

```python
session = scheduler.run_session(
    config=config,
    max_cycles=100,
    max_wall_clock_hours=168.0,  # 1 week
)

print(f"Session completed: {session.cycles_completed} cycles")
print(f"Failed: {session.cycles_failed}")
print(f"Total duration: {session.total_duration_seconds:.2f}s")
```

### Validate a Profile

```python
from agents.core import validate_learning_profile

errors = validate_learning_profile(
    ".odibi/cycle_profiles/large_scale_learning.yaml"
)
if errors:
    for e in errors:
        print(f"ERROR: {e}")
else:
    print("Profile is valid for autonomous learning")
```

---

## Test Coverage

```
agents/core/tests/test_autonomous_learning.py - 38 tests

TestLearningCycleConfig: 6 tests
TestLearningModeGuard: 8 tests
TestGuardedSkipSteps: 5 tests
TestLearningModeViolation: 2 tests
TestGuardedCycleRunnerSkipping: 3 tests
TestLearningCycleResult: 1 test
TestAutonomousLearningSession: 2 tests
TestValidateLearningProfile: 6 tests
TestPhase9AInvariants: 5 tests

All tests passing ✅
```

---

## Safety Confirmation

**This phase is safe to run unattended for days or weeks.**

The implementation guarantees:

1. **No code mutations**: ImprovementAgent and ReviewerAgent are mechanically skipped
2. **No proposals**: `max_improvements=0` is enforced at config validation
3. **Deterministic execution**: Same inputs → same outputs (verified via hashes)
4. **Clean termination**: All cycles terminate via max_steps, timeout, or error
5. **Audit trail**: All cycles produce reports and indexed learnings
6. **Fail-fast behavior**: Any violation raises `LearningModeViolation`

---

## Non-Goals (Explicitly NOT Implemented)

- ❌ New agents
- ❌ New UI
- ❌ New policies
- ❌ Behavior changes to improvement or scheduled modes
- ❌ Auto-advancement from learning to improvement mode
- ❌ Memory promotion to validated curriculum

---

## Next Phase Considerations

When Phase 9.B (Guarded Autonomous Improvement) is implemented:

1. Create separate `ImprovementCycleConfig` with `max_improvements > 0`
2. Require explicit human approval to switch from learning to improvement
3. Add golden project requirements for improvement cycles
4. Maintain learning mode as the default for autonomous execution
