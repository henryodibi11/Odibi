# Cycle Source Wiring Design

**Phase 8.A - Design + Schema + Guardrails Only**

This document describes the control-plane wiring that connects:
- `SourceSelectionPolicy` (Phase 7.C)
- Source Tiers (`tier0` → `tier600gb` → `tier2tb`) (Phase 7.E)
- `ExecutionSourceContext` (Phase 7.D)
- Cycle execution (`CycleConfig`)

---

## 1. Intent

The goal of Phase 8.A is to design the **configuration schema** and **guardrails** that will eventually wire source selection into cycle execution. This phase is **design-only**:

- ✅ Pydantic schemas defined
- ✅ Guardrails documented and enforced at config-time
- ✅ Report schema extended
- ❌ No runtime execution
- ❌ No data downloads
- ❌ No agent behavior changes

### Why This Design?

1. **Separation of Concerns**: Configuration is separate from execution
2. **Config-Time Validation**: Errors caught before cycles run
3. **Determinism**: All selection is reproducible via hashes
4. **Safety**: Mode-tier guardrails prevent resource exhaustion

---

## 2. Core Schemas

### 2.1 CycleSourceConfig

The primary schema for declaring source requirements in a cycle:

```python
from agents.core.cycle_source_config import CycleSourceConfig, SourceTier

config = CycleSourceConfig(
    selection_policy="learning_default",
    allowed_tiers=[SourceTier.TIER0, SourceTier.TIER20GB],
    max_sources=3,
    require_clean=False,
    allow_messy=True,
    deterministic=True,
    allow_tier_mixing=False,
    require_frozen=True,
)
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `selection_policy` | `str` | `"learning_default"` | Policy name from registry |
| `allowed_tiers` | `list[SourceTier]` | `[tier0]` | Tiers that MAY be used |
| `max_sources` | `int` | `3` | Maximum pools per cycle |
| `require_clean` | `bool` | `False` | Only clean pools allowed |
| `allow_messy` | `bool` | `True` | Messy pools may be included |
| `deterministic` | `bool` | `True` | Selection must be reproducible |
| `allow_tier_mixing` | `bool` | `False` | May mix pools from different tiers |
| `require_frozen` | `bool` | `True` | Only FROZEN pools may be selected |

### 2.2 SourceTier Enum

```python
class SourceTier(str, Enum):
    TIER0 = "tier0"        # ~1GB - minimal testing
    TIER20GB = "tier20gb"  # ~20GB - local development
    TIER100GB = "tier100gb"  # ~100GB - integration testing
    TIER600GB = "tier600gb"  # ~600GB - FineWeb, OSM Planet
    TIER2TB = "tier2tb"    # ~2TB - multiple FineWeb snapshots
```

### 2.3 SourceResolutionMetadata

Report schema extension for source selection information:

```python
@dataclass
class SourceResolutionMetadata:
    selection_policy: str
    tiers_used: list[str]
    source_pool_names: list[str]
    selection_hash: str
    context_id: str
    input_hash: str
    pools_considered: int
    pools_eligible: int
    pools_excluded: int
```

---

## 3. Invariants

These invariants are **mechanically enforced** at config-time:

### 3.1 Mode-Tier Compatibility

| Mode | tier0 | tier20gb | tier100gb | tier600gb | tier2tb |
|------|-------|----------|-----------|-----------|---------|
| LEARNING | ✅ | ✅ | ✅ | ✅ | ✅ |
| IMPROVEMENT | ✅ | ✅ | ✅ | ❌ | ❌ |
| SCHEDULED | ✅ | ✅ | ✅ | ✅ | ✅ |

**Rationale**: Large tiers (600GB+) are not allowed in Improvement mode because change validation requires full execution, which is too expensive at scale.

### 3.2 Scheduled Mode Requires Frozen

```
GUARDRAIL: Scheduled mode MAY ONLY use FROZEN pools
```

Scheduled cycles run unattended. Using non-frozen pools would introduce non-determinism.

### 3.3 No Tier Mixing (Default)

```
GUARDRAIL: Cycles MAY NOT mix tiers unless explicitly allowed
```

Mixing tier0 and tier2tb pools in the same cycle creates inconsistent test coverage. Set `allow_tier_mixing=True` to override.

### 3.4 Cleanliness Consistency

```
GUARDRAIL: require_clean=True and allow_messy=True are mutually exclusive
```

---

## 4. Guardrails

### 4.1 CycleSourceConfigGuard

The `CycleSourceConfigGuard` validates configurations at config-time:

```python
from agents.core.cycle_source_config import (
    CycleSourceConfig,
    CycleSourceConfigGuard,
    SourceTier,
)
from agents.core.source_selection import CycleMode

guard = CycleSourceConfigGuard()

# Valid config
config = CycleSourceConfig(
    allowed_tiers=[SourceTier.TIER0],
    require_frozen=True,
)
errors = guard.validate(config, mode=CycleMode.SCHEDULED)
assert len(errors) == 0

# Invalid config - tier600gb in Improvement mode
config = CycleSourceConfig(
    allowed_tiers=[SourceTier.TIER600GB],
)
errors = guard.validate(config, mode=CycleMode.IMPROVEMENT)
assert len(errors) == 1
assert "tier600gb" in errors[0].message
```

### 4.2 Error Types

| Error | Rule | Message |
|-------|------|---------|
| `SCHEDULED_MODE_FROZEN_ONLY` | Scheduled mode requires frozen | Set `require_frozen=True` |
| `MODE_TIER_COMPATIBILITY` | tier600gb+ not in Improvement | Remove large tier from config |
| `NO_TIER_MIXING` | Multiple non-inclusive tiers | Set `allow_tier_mixing=True` or use single tier |

---

## 5. Source Resolution Flow (Conceptual)

This is the **intended flow** for Phase 8.B+:

```
CycleConfig
  │
  ├─ CycleSourceConfig
  │     │
  │     ├─ selection_policy: "learning_default"
  │     ├─ allowed_tiers: [tier0, tier20gb]
  │     └─ max_sources: 3
  │
  ▼
SourceSelectionPolicy (loaded by name)
  │
  ▼
SourceSelector.select(policy, cycle_id, mode)
  │
  ▼
SourceSelectionResult
  │
  ├─ selected_pool_ids: ["tier0_simplewiki", "tier20gb_github_events"]
  ├─ selection_hash: "a1b2c3d4..."
  └─ input_hash: "e5f6g7h8..."
  │
  ▼
ExecutionSourceContext.from_selection(result, cache_root)
  │
  ▼
ExecutionSourceContext (bound to cycle)
  │
  ├─ context_id: "ctx_cycle_abc123_sel_xyz"
  ├─ mounted_pools: {...}
  └─ allowed_paths: {...}
```

### 5.1 Determinism Guarantee

Given identical:
1. `CycleSourceConfig`
2. `SourceSelectionPolicy`
3. `pool_index` (frozen pool metadata)

The resulting `ExecutionSourceContext` will be **identical**.

This is verified via:
- `input_hash`: SHA256 of (policy + pool_index + cycle_id)
- `selection_hash`: SHA256 of sorted selected_pool_ids
- `context_id`: Derived from selection_id

---

## 6. What This Does NOT Do

Phase 8.A is explicitly **inert**. It does NOT:

| Capability | Phase |
|------------|-------|
| Execute pipelines | ❌ Phase 8.A |
| Download data | ❌ Phase 8.A |
| Modify ExecutionGateway | ❌ Phase 8.A |
| Change agent behavior | ❌ Phase 8.A |
| Wire to CycleRunner | Phase 8.B |
| Load policies from registry | Phase 8.C |
| Check tier availability | Phase 8.D |
| Bind context to gateway | Phase 8.E |

---

## 7. Why Execution is Intentionally Deferred

1. **Safety First**: Config-time validation catches errors before resources are consumed
2. **Incremental Delivery**: Each phase can be tested independently
3. **Clear Contracts**: Schemas define the interface before implementation
4. **Determinism Verification**: Hashes can be checked without execution

---

## 8. Example Configurations

### 8.1 Learning Mode (Default)

```yaml
# cycle_source_learning.yaml
selection_policy: learning_default
allowed_tiers:
  - tier0
  - tier20gb
  - tier100gb
max_sources: 4
require_clean: false
allow_messy: true
deterministic: true
allow_tier_mixing: false
require_frozen: true
```

### 8.2 Improvement Mode

```yaml
# cycle_source_improvement.yaml
selection_policy: improvement_default
allowed_tiers:
  - tier0
  - tier20gb
max_sources: 2
require_clean: true
allow_messy: false
deterministic: true
allow_tier_mixing: false
require_frozen: true
```

### 8.3 Scheduled Mode (Strict)

```yaml
# cycle_source_scheduled.yaml
selection_policy: scheduled_default
allowed_tiers:
  - tier0
max_sources: 3
require_clean: false
allow_messy: true
deterministic: true
allow_tier_mixing: false
require_frozen: true  # REQUIRED for scheduled mode
```

### 8.4 Large-Scale Learning

```yaml
# cycle_source_large_learning.yaml
selection_policy: learning_default
allowed_tiers:
  - tier600gb
  - tier2tb
max_sources: 5
require_clean: false
allow_messy: true
deterministic: true
allow_tier_mixing: true  # Allow mixing large tiers
require_frozen: true
```

---

## 9. Integration with Reports

Cycle reports will include a **Source Resolution** section:

```markdown
## Source Resolution

- **Policy:** `learning_default`
- **Tiers Used:** tier0, tier20gb
- **Pools Selected:** simplewiki, github_events_sample
- **Selection Hash:** `a1b2c3d4e5f6g7h8`
- **Context ID:** `ctx_cycle_abc123_sel_xyz`
- **Pools Considered:** 12
- **Pools Eligible:** 8
- **Pools Excluded:** 4
```

---

## 10. Future Phases

| Phase | Scope |
|-------|-------|
| 8.B | Wire `CycleSourceConfig` to `CycleRunner.start_cycle()` |
| 8.C | Implement policy registry and loading |
| 8.D | Add tier availability checks (disk space, network) |
| 8.E | Bind `ExecutionSourceContext` to `ExecutionGateway` |
| 8.F | Add source resolution to report rendering |

---

## 11. Files Created in Phase 8.A

| File | Description |
|------|-------------|
| `agents/core/cycle_source_config.py` | Core schemas and guardrails |
| `agents/docs/cycle_source_wiring.md` | This document |
| `agents/examples/cycle_source_configs/` | Example YAML configurations |

---

## 12. Success Criteria

✅ Everything is read-only  
✅ Nothing executes  
✅ Nothing downloads  
✅ Nothing mutates  
✅ Determinism is preserved  
✅ Future phases can wire execution without redesign
