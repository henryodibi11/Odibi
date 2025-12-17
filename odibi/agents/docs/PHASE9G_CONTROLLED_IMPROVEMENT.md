# Phase 9.G: Controlled Improvement Activation

> **Design Intent**: Improvements are earned, not assumed. The system must prove it can fix ONE real problem safely before scaling.

## Overview

Phase 9.G enables controlled, single-shot improvements with strict scope enforcement. This phase builds on the observation-only learning from Phase 9.F and proves that the autonomous improvement mechanism works safely before allowing broader changes.

## Why This Phase Exists

Before allowing freeform autonomous improvements, we need to verify:

1. **The improvement mechanism works** - Can the system actually apply a fix?
2. **Scope enforcement works** - Does the system respect file boundaries?
3. **Rollback works** - Can we safely undo bad changes?
4. **Determinism works** - Do same inputs produce identical outputs?
5. **Regression detection works** - Do we catch when fixes break other things?

Phase 9.G answers all these questions with a single, controlled test case.

## Key Design Decisions

### Why `max_improvements = 1`?

Single-shot improvement ensures:
- **Debuggability**: If something goes wrong, exactly one change to analyze
- **Atomicity**: Success or failure is binary, no partial states
- **Simplicity**: One proposal, one review, one outcome
- **Trust building**: Prove the mechanism works before scaling

### Why File Scope Enforcement?

The `ImprovementScope` class restricts which files can be modified:

```python
scope = ImprovementScope.for_single_file(
    "scale_join.odibi.yaml",
    description="Fix validation error in scale_join"
)
```

This prevents:
- **Unintended modifications**: Only the target file changes
- **Scope creep**: LLM can't "helpfully" refactor unrelated code
- **Regression risks**: Unrelated pipelines remain untouched

### Why Before/After Snapshots?

The `ImprovementSnapshot` class captures file state before any changes:

```python
snapshot = ImprovementSnapshot.capture(["scale_join.odibi.yaml"])
# ... apply improvement ...
if validation_failed:
    snapshot.restore()  # Instant rollback
```

Benefits:
- **Deterministic rollback**: Exact original state restored
- **Audit trail**: Full before/after diff captured
- **Safety net**: Any failure triggers automatic restoration

### Why Deterministic Diffs?

Same inputs + same profile + same cycle → identical diff:

```python
result1 = runner.apply_improvement(proposal, state)
result2 = runner.apply_improvement(proposal, state)
assert result1.diff_hash == result2.diff_hash  # Must be true
```

This ensures:
- **Reproducibility**: Re-running produces same fix
- **Predictability**: No random/creative variations
- **Testability**: Can verify exact expected changes

## Architecture

```
ControlledImprovementRunner (extends GuardedCycleRunner)
├── ImprovementScope         # Defines allowed files/fields
├── ImprovementScopeEnforcer # Runtime enforcement
├── ImprovementSnapshot      # Before/after capture
└── ImprovementResult        # Outcome with audit info
```

### Flow

```
1. Configure scope (allowed files only)
2. Capture snapshot (before state)
3. Run improvement step (single proposal)
4. Validate scope (reject if out of bounds)
5. Apply changes (within scope only)
6. Run validation (re-execute pipeline)
7. Check regressions (golden projects)
8. Check stability (unrelated pipelines)
9. Finalize or Rollback
```

## Usage

### Basic Usage

```python
from agents.core.controlled_improvement import (
    ControlledImprovementConfig,
    ControlledImprovementRunner,
    ImprovementScope,
)

# Define scope
scope = ImprovementScope.for_single_file(
    "scale_join.odibi.yaml",
    description="Fix validation error"
)

# Configure
config = ControlledImprovementConfig(
    project_root="/path/to/project",
    improvement_scope=scope,
    golden_projects=[
        GoldenProjectConfig(name="skew_test", path="skew_test.odibi.yaml"),
        GoldenProjectConfig(name="schema_drift", path="schema_drift.odibi.yaml"),
    ],
    require_validation_pass=True,
    require_regression_check=True,
    rollback_on_failure=True,
)

# Run
runner = ControlledImprovementRunner(odibi_root="/path/to/odibi")
state = runner.start_controlled_improvement_cycle(config)

# ... cycle runs, improvement is proposed ...

result = runner.finalize_improvement(state)
print(f"Status: {result.status}")
print(f"Files modified: {result.files_modified}")
```

### Checking Results

```python
if result.status == "APPLIED":
    print("✅ Improvement applied successfully")
    print(f"Diff hash: {result.diff_hash}")
elif result.status == "REJECTED":
    print(f"❌ Improvement rejected: {result.rejection_reason}")
    print(f"Details: {result.rejection_details}")
elif result.status == "ROLLED_BACK":
    print("⚠️ Improvement was rolled back")
    print(f"Restored files: {result.files_restored}")
elif result.status == "NO_PROPOSAL":
    print("ℹ️ No improvement was proposed")
```

## Safety Invariants

These MUST remain true throughout Phase 9.G:

| Invariant | Enforcement |
|-----------|-------------|
| `require_frozen = true` | Config validation |
| `max_improvements = 1` | ControlledImprovementConfig |
| Disk guard active | DiskUsageGuard |
| Heartbeat writing | HeartbeatWriter |
| Source binding active | SourceBinder |
| No YAML generation outside scope | ImprovementScopeEnforcer |
| No additional pipelines | Scope restriction |
| No CLI changes | Read-only mode |

## Rejection Reasons

An improvement can be rejected for:

| Reason | Description |
|--------|-------------|
| `SCOPE_VIOLATION` | Attempted to modify file outside allowed scope |
| `VALIDATION_FAILED` | Pipeline validation failed after applying change |
| `REGRESSION_DETECTED` | Golden project(s) failed after improvement |
| `NON_MINIMAL_DIFF` | Change exceeded max lines/changes limits |
| `NON_DETERMINISTIC` | Different runs produced different diffs |
| `UNRELATED_CHANGES` | Files outside scope were modified |

## Testing

Run the Phase 9.G tests:

```bash
pytest agents/core/tests/test_controlled_improvement.py -v
```

Key test scenarios:
1. `test_improvement_step_runs_once` - Single-shot enforcement
2. `test_apply_improvement_enforces_scope` - Scope violation detection
3. `test_rollback_on_validation_failure` - Rollback mechanism
4. `test_diff_is_deterministic` - Reproducibility verification
5. `test_none_proposal_returns_no_proposal_status` - NONE handling

## What's NOT in Scope for Phase 9.G

Explicitly excluded:
- ❌ Multi-file refactors
- ❌ Autonomous YAML creation
- ❌ Heuristic optimization
- ❌ Performance tuning
- ❌ Learning loop expansion
- ❌ Multiple improvements per cycle

These require Phase 9.H+ after proving Phase 9.G works safely.

## Success Criteria

Phase 9.G is complete when:

- [x] One improvement can be proposed
- [x] One minimal fix can be applied
- [x] All pipelines pass after fix
- [x] No unrelated changes occur
- [x] The system converges cleanly
- [x] Rollback works on failure
- [x] Scope is enforced at runtime

## Related Documentation

- [Phase 9.A: Guarded Autonomous Learning](./PHASE9A_AUTONOMOUS_LEARNING.md)
- [Phase 9.F: Observation Integrity](./PHASE9F_OBSERVATION_INTEGRITY.md)
- [Cycle Profiles](./CYCLE_PROFILES.md)
