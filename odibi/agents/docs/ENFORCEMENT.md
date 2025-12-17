# Phase 0 Enforcement

This document describes the invariants that are now **mechanically enforced** in the Odibi Agent Suite, as opposed to being merely declared.

## Summary

| Invariant | Status | Enforcement Location |
|-----------|--------|---------------------|
| Permission Enforcement | **ENFORCED** | `ExecutionGateway.run_task()` |
| Wall-Clock Timeout | **ENFORCED** | `CycleRunner.run_next_step()` |
| Review Gating | **ENFORCED** | `CycleRunner._post_process_step()` |
| Cycle Termination | **ENFORCED** | `CycleTerminationEnforcer` |

---

## 1. Permission Enforcement

### What is Enforced

- **`can_execute_tasks`**: Agents without this permission CANNOT invoke `ExecutionGateway.run_task()`. Attempting to do so raises `PermissionDeniedError`.

- **`can_edit_source`**: Agents without this permission CANNOT modify files within Odibi source directories (checked via `PermissionEnforcer.check_source_edit_permission()`).

### How it Works

```python
# ExecutionGateway now REQUIRES agent credentials
gateway.run_task(task, agent_permissions=agent.permissions, agent_role=agent.role)

# If agent lacks can_execute_tasks:
# -> PermissionDeniedError raised immediately
# -> Task is NOT executed
```

### What Happens on Violation

- `PermissionDeniedError` is raised
- Error message includes agent role and missing permission
- Execution is halted (not continued silently)

---

## 2. Wall-Clock Timeout Enforcement

### What is Enforced

- `max_runtime_hours` from `CycleConfig` is converted to seconds and tracked by `WallClockEnforcer`
- Before EVERY step, `CycleRunner` checks if wall-clock time is exceeded
- If exceeded, cycle terminates with `exit_status = "timeout"`

### How it Works

```python
# In CycleRunner.start_cycle():
self._termination_enforcer = CycleTerminationEnforcer(
    max_steps=10,
    max_runtime_seconds=config.max_runtime_hours * 3600,
)

# In CycleRunner.run_next_step():
term_status = self._termination_enforcer.check_termination()
if term_status is not None:
    state.exit_status = term_status.value  # "timeout"
    return state  # Cycle stops immediately
```

### What Happens on Timeout

- Cycle is marked as interrupted
- `state.exit_status = "timeout"`
- `state.interrupt_reason` explains the termination
- Cycle does NOT rely on agent cooperation to stop

---

## 3. Mechanical Review Gating

### What is Enforced

- Improvement proposals generate a cryptographic hash
- ReviewerAgent response is parsed for explicit `APPROVED` or `REJECTED` keyword
- Review decision is tied to the specific proposal via hash
- `ReviewGateEnforcer.check_approval()` raises `ReviewGatingError` if:
  - No review has occurred
  - Review was rejected
  - Hash mismatch (wrong proposal)

### How it Works

```python
# When ImprovementAgent produces a proposal:
proposal_hash = sha256(proposal_content)[:16]
review_gate.register_proposal(proposal_content)

# When ReviewerAgent responds:
decision = ReviewDecision.parse_from_response(response, proposal_hash)
review_gate.register_decision(decision)

# Before applying changes:
review_gate.check_approval()  # Raises if not approved
```

### What Happens Without Approval

- `ReviewGatingError` is raised
- Error message explains whether review was missing or rejected
- Changes are NOT applied

---

## 4. Guaranteed Cycle Termination

### What is Enforced

Every cycle MUST terminate via one of:

1. **max_steps** (10 steps) - `exit_status = "max_steps"`
2. **wall-clock timeout** - `exit_status = "timeout"`
3. **explicit interrupt** - `exit_status = "interrupted"`
4. **normal completion** - `exit_status = "completed"`

### How it Works

```python
class CycleTerminationEnforcer:
    def check_termination(self) -> Optional[CycleExitStatus]:
        if self._interrupted:
            return CycleExitStatus.INTERRUPTED
        if self.wall_clock.is_expired():
            return CycleExitStatus.TIMEOUT
        if self.steps_executed >= self.max_steps:
            return CycleExitStatus.MAX_STEPS
        return None
```

### What This Prevents

- Infinite loops (cannot exceed max_steps)
- Runaway cycles (cannot exceed wall-clock time)
- Unresponsive cycles (interrupt is always available)
- Agent-dependent termination (system enforces, not agent)

---

## What Remains Advisory (NOT Enforced)

The following are still advisory/declared only:

| Item | Status | Notes |
|------|--------|-------|
| `read_only` permission | Advisory | Not currently enforced at file system level |
| `can_access_private_memories` | Advisory | Memory access control not enforced |
| `can_write_memories` | Advisory | Memory write control not enforced |
| `gated_improvements` | N/A | Now superseded by ReviewGateEnforcer |
| `stop_on_convergence` | Advisory | Convergence detection remains heuristic |

---

## Error Types

All enforcement errors inherit from `EnforcementError`:

```python
class EnforcementError(Exception): ...
class PermissionDeniedError(EnforcementError): ...
class WallClockTimeoutError(EnforcementError): ...
class ReviewGatingError(EnforcementError): ...
class CycleTerminationError(EnforcementError): ...
```

---

## Migration Notes

### For Code Using ExecutionGateway

Before (would now fail):
```python
gateway.run_task(task)  # PermissionDeniedError
```

After:
```python
gateway.run_task(
    task,
    agent_permissions=agent.permissions,
    agent_role=agent.role,
)
```

### For Cycle Callers

No changes required. Enforcement is internal to `CycleRunner`. However, callers should now check `state.exit_status` to understand why a cycle ended.
