# Source Selection System

**Phase**: 7.C (Control-Plane Only)  
**Status**: Design Complete  
**Last Updated**: 2025-12-14

---

## 1. Overview

The Source Selection system controls **which SourcePools are used in cycles** without executing data pipelines. It is a control-plane feature that:

- **Selects** pools according to policy
- **Logs** every selection decision with rationale
- **Guarantees** determinism: same inputs → same outputs
- **Enforces** guardrails: agents cannot fabricate or modify sources

### 1.1 Key Concepts

```
┌─────────────────────────────────────────────────────────────────────┐
│                     SOURCE SELECTION FLOW                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐ │
│   │   Policy     │    │   Pool       │    │   Selection         │ │
│   │   (YAML)     │───>│   Index      │───>│   Result            │ │
│   │              │    │   (frozen)   │    │   (deterministic)   │ │
│   └──────────────┘    └──────────────┘    └──────────────────────┘ │
│                                                  │                  │
│                                                  ▼                  │
│                                           ┌──────────────────────┐ │
│                                           │   Cycle Context      │ │
│                                           │   (metadata only)    │ │
│                                           └──────────────────────┘ │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 1.2 What This Is NOT

- ❌ **NOT** data execution (no pipelines run)
- ❌ **NOT** data download (pools are pre-cached)
- ❌ **NOT** schema inference (schemas are explicit)
- ❌ **NOT** agent autonomy expansion

---

## 2. SourcePools vs Selection vs Execution

| Concept | Phase | Purpose | Location |
|---------|-------|---------|----------|
| **SourcePools** | 7.B | Frozen test data with explicit schemas | `.odibi/source_cache/` |
| **Selection** | 7.C | Policy-driven pool selection | `agents/core/source_selection.py` |
| **Execution** | Future | Running pipelines against selected pools | `agents/core/execution.py` |

The separation ensures:
1. Pools exist independently of selection logic
2. Selection is auditable and deterministic
3. Execution can be gated separately

---

## 3. Selection Policy Schema

### 3.1 Core Fields

```yaml
policy_id: learning_default        # Unique identifier (snake_case)
name: "Learning Mode Policy"       # Human-readable name
description: "..."                 # Optional description

# Pool Constraints
eligible_pools: null               # List of allowed pool_ids (null = all)
excluded_pools: [edge_cases]       # Blocklist of pool_ids
max_pools_per_cycle: 4             # 1-10 pools per cycle

# Format & Type Constraints
allowed_formats: [csv, json]       # null = all formats
allowed_source_types: [local]      # null = all types

# Quality Constraints
allow_messy_data: true             # Include messy/mixed pools
clean_vs_messy_ratio: 0.5          # 0.0-1.0 target ratio

# Selection Strategy
selection_strategy: hash_based     # round_robin | hash_based | coverage_first | explicit

# Coverage Preferences
prefer_uncovered_formats: true     # Prioritize underused formats
prefer_uncovered_pools: true       # Prioritize recently unused pools

# Explicit Selection (for EXPLICIT strategy only)
explicit_pool_order:               # Fixed ordered list
  - pool_a
  - pool_b
```

### 3.2 Selection Strategies

| Strategy | Description | Determinism |
|----------|-------------|-------------|
| `hash_based` | Score pools using SHA256(pool_id + cycle_id) | ✅ Fully deterministic |
| `round_robin` | Cycle through pools in order | ✅ Deterministic by index |
| `coverage_first` | Prioritize uncovered formats/pools | ✅ Deterministic with history |
| `explicit` | Use exact `explicit_pool_order` | ✅ Fully deterministic |

---

## 4. Cycle Mode Defaults

### 4.1 Learning Mode

```yaml
# .odibi/selection_policies/learning.yaml
policy_id: learning_default
max_pools_per_cycle: 4
allow_messy_data: true
clean_vs_messy_ratio: 0.5
selection_strategy: coverage_first
prefer_uncovered_formats: true
prefer_uncovered_pools: true
```

**Purpose**: Broad exploration of data patterns and edge cases.

### 4.2 Improvement Mode

```yaml
# .odibi/selection_policies/improvement.yaml
policy_id: improvement_default
max_pools_per_cycle: 2
allow_messy_data: false
clean_vs_messy_ratio: 1.0
selection_strategy: hash_based
prefer_uncovered_formats: false
prefer_uncovered_pools: false
```

**Purpose**: Stable, repeatable selection for code changes.

### 4.3 Scheduled Mode

```yaml
# .odibi/selection_policies/scheduled.yaml
policy_id: scheduled_default
max_pools_per_cycle: 3
allow_messy_data: true
selection_strategy: hash_based
prefer_uncovered_formats: false
prefer_uncovered_pools: false
```

**Purpose**: Fully deterministic for CI/CD environments.

---

## 5. Selection Result

The selection produces a `SourceSelectionResult` with:

```python
@dataclass
class SourceSelectionResult:
    selection_id: str          # Unique selection identifier
    policy_id: str             # Policy used
    cycle_id: str              # Cycle this selection is for
    mode: str                  # learning | improvement | scheduled
    selected_at: str           # ISO timestamp

    selected_pool_ids: List[str]           # Ordered list of selected pools
    selected_pools: List[PoolMetadataSummary]  # Metadata (NOT data)

    rationale: List[SelectionRationale]    # Why each pool was selected/rejected

    input_hash: str            # SHA256(policy + pool_index + cycle_id)
    selection_hash: str        # SHA256(selected_pool_ids)

    pools_considered: int      # Total pools in index
    pools_eligible: int        # Pools passing filters
    pools_excluded: int        # Pools failing filters
```

### 5.1 Determinism Verification

```python
# Same inputs must produce same selection_hash
result1 = selector.select(policy, "cycle-123", CycleMode.SCHEDULED)
result2 = selector.select(policy, "cycle-123", CycleMode.SCHEDULED)

assert result1.selection_hash == result2.selection_hash  # MUST be True
```

---

## 6. Agent Guardrails

### 6.1 What Agents MAY Do

| Operation | Allowed |
|-----------|---------|
| Read pool metadata | ✅ YES |
| Read pool integrity manifest | ✅ YES |
| Query pool index | ✅ YES |
| Request selection with valid policy | ✅ YES |
| Verify selection determinism | ✅ YES |
| Include pool metadata in context | ✅ YES |
| Report on pool coverage | ✅ YES |

### 6.2 What Agents MAY NOT Do

| Operation | Forbidden | Reason |
|-----------|-----------|--------|
| Modify pool metadata | ❌ NO | Pools are frozen |
| Modify pool data | ❌ NO | Integrity would break |
| Download new data | ❌ NO | No network during cycles |
| Fabricate sources | ❌ NO | All sources must be in pool_index |
| Access data files directly | ❌ NO | Metadata only |
| Change pool status | ❌ NO | Lifecycle is controlled |
| Add pools to index | ❌ NO | Requires preparation phase |
| Override policy during execution | ❌ NO | Policy is immutable per cycle |

### 6.3 Failure Modes

| Failure | Response |
|---------|----------|
| No pools match policy | `CYCLE_BLOCKED`: Cycle cannot proceed |
| Invalid policy | `POLICY_REJECTED`: Fix config and retry |
| Pool integrity failure | `POOL_CORRUPTED`: Pool excluded until re-frozen |

---

## 7. Context Integration

Selection results are wired into cycle context:

```python
def wire_selection_to_context(selection, context_metadata):
    context_metadata["source_selection"] = {
        "selection_id": selection.selection_id,
        "policy_id": selection.policy_id,
        "selected_pool_ids": selection.selected_pool_ids,
        "selection_hash": selection.selection_hash,
        "input_hash": selection.input_hash,
        "pools_metadata": [p.to_dict() for p in selection.selected_pools],
    }
    return context_metadata
```

This makes selection available to:
- **Observer Agent**: Can report which pools were tested
- **Reports**: Audit trail of what was selected and why
- **Evidence**: Ties execution artifacts to specific pools

---

## 8. Why Agents Cannot Invent Sources

The Odibi system enforces **source integrity** to ensure:

1. **Reproducibility**: Any cycle can be re-run with identical inputs
2. **Auditability**: Every data source is traceable to a frozen pool
3. **Security**: No external/unapproved data enters the system
4. **Determinism**: Selection is verifiable via hashes

If an agent could fabricate sources:
- Cycles would not be reproducible
- Audit trails would be meaningless
- Malicious data could be injected
- Determinism guarantees would break

**The pool_index is the single source of truth.**

---

## 9. Determinism Guarantees

### 9.1 Invariants

```
INVARIANT 1: selection(policy, pool_index, cycle_id) is a pure function
INVARIANT 2: selection_hash is derived from selected_pool_ids only
INVARIANT 3: input_hash captures all selection inputs
INVARIANT 4: Same input_hash → Same selection_hash (ALWAYS)
```

### 9.2 Verification

```python
# Run selection twice with same inputs
result1 = selector.select(policy, cycle_id, mode)
result2 = selector.select(policy, cycle_id, mode)

# These MUST match
assert result1.input_hash == result2.input_hash
assert result1.selection_hash == result2.selection_hash
assert result1.selected_pool_ids == result2.selected_pool_ids
```

---

## 10. File Locations

| File | Purpose |
|------|---------|
| `agents/core/source_selection.py` | Core selection logic |
| `agents/core/tests/test_source_selection.py` | Determinism tests |
| `.odibi/selection_policies/*.yaml` | Policy definitions |
| `.odibi/source_metadata/pool_index.yaml` | Pool registry |
| `.odibi/source_metadata/pools/*.yaml` | Individual pool metadata |
| `.odibi/source_cache/` | Frozen data files (read-only) |

---

## 11. Non-Goals

This phase explicitly does **NOT** include:

- ❌ ExecutionGateway changes
- ❌ Pipeline execution against pools
- ❌ Agent prompt modifications
- ❌ Agent autonomy expansion
- ❌ New agent creation
- ❌ Live data download
- ❌ 600GB tier implementation
- ❌ Schema inference at runtime
