# Source Binding: Execution Safety

**Phase**: 7.D  
**Status**: Design Complete  
**Last Updated**: 2025-12-14

---

## 1. Overview

Source Binding enforces **hard, auditable boundaries** on what data pipelines can access during execution. This is an execution safety mechanism, NOT a scale feature.

### 1.1 Core Invariants

| Invariant | Enforcement |
|-----------|-------------|
| Pipelines may ONLY read from selected SourcePools | Path validation |
| SourcePools are mounted read-only | Write detection |
| No access to arbitrary filesystem paths | Path enforcement |
| No access to network resources | Network detection |
| All source usage is logged | Access tracking |
| All usage is attributable | Evidence integration |

### 1.2 Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     SOURCE BINDING FLOW                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   ┌──────────────┐    ┌──────────────────────┐                     │
│   │   Selection  │───>│   ExecutionSource    │                     │
│   │   Result     │    │   Context            │                     │
│   └──────────────┘    │   (IMMUTABLE)        │                     │
│                       └──────────────────────┘                     │
│                                │                                    │
│                                ▼                                    │
│                       ┌──────────────────────┐                     │
│                       │   ExecutionGateway   │                     │
│                       │   bind_sources()     │                     │
│                       └──────────────────────┘                     │
│                                │                                    │
│                    ┌───────────┼───────────┐                       │
│                    ▼           ▼           ▼                       │
│              ┌──────────┐ ┌──────────┐ ┌──────────┐                │
│              │ Path     │ │ Network  │ │ Write    │                │
│              │ Check    │ │ Check    │ │ Check    │                │
│              └──────────┘ └──────────┘ └──────────┘                │
│                    │           │           │                       │
│                    ▼           ▼           ▼                       │
│              ┌──────────────────────────────────┐                  │
│              │   ✅ ALLOWED  or  ❌ VIOLATION   │                  │
│              └──────────────────────────────────┘                  │
│                                │                                    │
│                                ▼                                    │
│                       ┌──────────────────────┐                     │
│                       │   ExecutionEvidence  │                     │
│                       │   + SourceUsage      │                     │
│                       └──────────────────────┘                     │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. ExecutionSourceContext

### 2.1 Purpose

The `ExecutionSourceContext` is an **immutable** container of mounted source pools for a cycle execution. Once created, it cannot be modified.

### 2.2 Schema

```python
@dataclass
class ExecutionSourceContext:
    context_id: str           # Unique context identifier
    cycle_id: str             # Associated cycle
    selection_id: str         # Source selection that created this

    mounted_pools: Dict[str, MountedPool]  # pool_id -> MountedPool
    allowed_paths: Set[str]   # Allowed absolute path prefixes

    source_cache_root: str    # Path to .odibi/source_cache/
    created_at: str           # Creation timestamp
    frozen: bool = True       # ALWAYS True after creation

    access_log: List[SourceAccessRecord]  # All access attempts
    integrity_verified: bool  # Whether hashes were verified
```

### 2.3 Creation

```python
from agents.core.source_binding import ExecutionSourceContext
from agents.core.source_selection import SourceSelector, CycleMode

# 1. Select pools
selector = SourceSelector(metadata_path)
selection = selector.select(policy, cycle_id, mode)

# 2. Create execution context
context = ExecutionSourceContext.from_selection(
    selection,
    source_cache_root=".odibi/source_cache/"
)

# 3. Bind to gateway
gateway.bind_sources(context)
```

---

## 3. Path Enforcement

### 3.1 Validation vs Enforcement

| Method | Behavior | Use Case |
|--------|----------|----------|
| `validate_path(path)` | Returns pool_id or None | Soft check |
| `enforce_path(path)` | Returns pool_id or raises | Hard check |

### 3.2 Allowed Paths

A path is allowed if and only if:
1. It starts with a mounted pool's `absolute_path`
2. It does not contain path traversal (`..`)
3. It is a real subpath of the pool directory

### 3.3 Violation Types

| Type | Meaning |
|------|---------|
| `PATH_OUTSIDE_POOLS` | Path is not within any mounted pool |
| `UNKNOWN_POOL_ID` | Referenced pool ID doesn't exist |
| `POOL_NOT_SELECTED` | Pool exists but was not selected |
| `INTEGRITY_FAILURE` | Pool file hash doesn't match manifest |
| `NETWORK_ACCESS_ATTEMPT` | Attempted to access network resource |
| `WRITE_ATTEMPT` | Attempted to write to read-only pool |

---

## 4. ExecutionGateway Integration

### 4.1 Binding Sources

```python
gateway = ExecutionGateway(odibi_root="d:/odibi")

# Bind source context
gateway.bind_sources(context)

# Now all pipeline executions will enforce bounds
result = gateway.run_pipeline(
    config_path="pipeline.yaml",
    agent_permissions=permissions,
    agent_role=role,
)

# Check for violations
if gateway.has_source_violations():
    violations = gateway.get_source_violations()
    for v in violations:
        print(f"VIOLATION: {v.violation_type}: {v.message}")
```

### 4.2 Gateway Methods

| Method | Purpose |
|--------|---------|
| `bind_sources(context)` | Bind context for enforcement |
| `unbind_sources()` | Remove binding (testing only) |
| `is_source_bound()` | Check if sources are bound |
| `validate_source_path(path)` | Soft validation |
| `enforce_source_path(path)` | Hard enforcement |
| `get_source_violations()` | Get all violations |
| `has_source_violations()` | Check for violations |

---

## 5. Evidence Integration

### 5.1 SourceUsageSummary

Every `ExecutionEvidence` can include a `SourceUsageSummary`:

```python
@dataclass
class SourceUsageSummary:
    context_id: str           # Execution context ID
    pools_bound: list[str]    # Pools that were available
    pools_used: list[str]     # Pools actually accessed
    files_accessed: int       # Number of file accesses
    integrity_verified: bool  # Whether hashes matched
    violations_detected: int  # Number of violations
```

### 5.2 Adding to Evidence

```python
evidence = ExecutionEvidence.from_execution_result(result, ...)

evidence.set_source_usage(
    context_id=context.context_id,
    pools_bound=list(context.mounted_pools.keys()),
    pools_used=context.get_pools_used(),
    files_accessed=len(context.access_log),
    integrity_verified=context.integrity_verified,
    violations_detected=len([r for r in context.access_log if not r.success]),
)
```

### 5.3 Report Output

In cycle reports, source usage appears as:

```markdown
## Sources Used

| Property | Value |
|----------|-------|
| Context ID | ctx_cycle-001_sel123 |
| Pools Bound | pool1_csv_clean, pool2_json_clean |
| Pools Used | pool1_csv_clean |
| Files Accessed | 5 |
| Integrity Verified | ✅ Yes |
| Violations | 0 |
```

---

## 6. Agent Permissions

### 6.1 What Agents MAY Do

| Operation | Allowed |
|-----------|---------|
| Execute pipelines against bound pools | ✅ YES |
| Read files within pool directories | ✅ YES |
| List files within pool directories | ✅ YES |
| Query which pools are bound | ✅ YES |
| Check if a path is within bounds | ✅ YES |

### 6.2 What Agents MAY NOT Do

| Operation | Forbidden | Error |
|-----------|-----------|-------|
| Alter source bindings during execution | ❌ NO | Context is frozen |
| Request new sources after binding | ❌ NO | Selection is complete |
| Access paths outside bound pools | ❌ NO | `PATH_OUTSIDE_POOLS` |
| Access network resources | ❌ NO | `NETWORK_ACCESS_ATTEMPT` |
| Write to pool directories | ❌ NO | `WRITE_ATTEMPT` |
| Fabricate source data | ❌ NO | Path enforcement |

---

## 7. Failure Behavior

### 7.1 Unbound Source Reference

If a pipeline references a path outside bound pools:

1. `SourceViolationError` is raised
2. Execution **MUST** fail deterministically
3. Error is logged in access_log
4. Observer flags `SOURCE_VIOLATION`
5. Evidence captures the violation

### 7.2 Error Format

```python
SourceViolationError(
    violation_type=SourceViolationType.PATH_OUTSIDE_POOLS,
    message="Path '/data/external/file.csv' is outside all bound source pools. "
            "Allowed paths: ['/path/to/pool1', '/path/to/pool2']",
    path="/data/external/file.csv",
)
```

### 7.3 Determinism Guarantee

**The same invalid path will ALWAYS produce the same error.**

```python
# This will ALWAYS raise SourceViolationError with PATH_OUTSIDE_POOLS
for _ in range(1000):
    try:
        context.enforce_path("/invalid/path")
    except SourceViolationError as e:
        assert e.violation_type == SourceViolationType.PATH_OUTSIDE_POOLS
```

---

## 8. SourceBindingGuard

### 8.1 Purpose

The `SourceBindingGuard` provides pipeline-level validation before execution:

```python
guard = SourceBindingGuard(context)

# Check all source paths in a pipeline config
violations = guard.check_pipeline_config(
    config_path="pipeline.yaml",
    source_paths=["/path/to/input1.csv", "/path/to/input2.json"],
)

if violations:
    for v in violations:
        print(f"BLOCKED: {v}")
    raise RuntimeError("Pipeline references invalid sources")
```

### 8.2 Guard Methods

| Method | Purpose |
|--------|---------|
| `check_pipeline_config(config, paths)` | Validate all source paths |
| `check_network_access(url)` | Block network access |
| `check_write_access(path)` | Block writes to pools |
| `get_violations()` | Get all detected violations |
| `has_violations()` | Quick check for violations |

---

## 9. Integrity Verification

### 9.1 Hash Verification

Pools can be verified against their SHA256 manifests:

```python
from agents.core.source_binding import verify_pool_integrity

pool = context.get_pool("pool1_csv_clean")
manifest = {"data.csv": "77a0135e3b2512f21a1137e2bcaa74f6683d460207d0dc00e214cc6071499605"}

is_valid, errors = verify_pool_integrity(pool, manifest)

if not is_valid:
    for error in errors:
        print(f"INTEGRITY ERROR: {error}")
```

### 9.2 Verification Errors

- `Missing file: {filename}` - File in manifest not found
- `Hash mismatch for {filename}` - File exists but hash differs
- `Error reading {filename}` - IO error during verification

---

## 10. File Locations

| File | Purpose |
|------|---------|
| `agents/core/source_binding.py` | Core binding logic |
| `agents/core/execution.py` | Gateway with binding support |
| `agents/core/evidence.py` | Evidence with source usage |
| `agents/core/tests/test_source_binding.py` | Guardrail tests |
| `agents/docs/source_binding.md` | This documentation |

---

## 11. Non-Goals

This phase explicitly does **NOT** include:

- ❌ Large data tier implementation (600GB+)
- ❌ New agent creation
- ❌ Selection logic changes
- ❌ Live data access
- ❌ Download capabilities
- ❌ Autonomy modifications
