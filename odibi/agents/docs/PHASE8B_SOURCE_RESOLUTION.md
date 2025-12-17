# Phase 8.B: Source Resolution at Cycle Start

## Overview

Phase 8.B wires `CycleSourceConfig` into `CycleRunner` so that source resolution occurs **deterministically at cycle start**, without executing pipelines or binding sources to ExecutionGateway.

## Status

**COMPLETE** - All Phase 8.B deliverables implemented:
- ✅ `SourceResolutionResult` dataclass
- ✅ `SourceResolver` class
- ✅ `UnboundSourceContext` for read-only context
- ✅ `CycleRunner.start_cycle()` updated
- ✅ Report integration
- ✅ Unit tests for determinism and guardrails

## Architecture

```
CycleConfig
  ↓
CycleRunner.start_cycle(config, mode, source_config?)
  ↓
_resolve_sources()
  ↓
SourceResolver.resolve()
  ↓
CycleSourceConfig → SourceSelectionPolicy → SourceSelector.select()
  ↓
SourceSelectionResult → SourceResolutionResult
  ↓
CycleState.source_resolution (dict)
  ↓
CycleReportGenerator → "## Source Resolution" section
```

## Key Components

### SourceResolver

The main orchestrator for source resolution. Located in `agents/core/source_resolution.py`.

```python
from agents.core.source_resolution import SourceResolver
from agents.core.source_selection import CycleMode

resolver = SourceResolver(odibi_root="/path/to/odibi")
result = resolver.resolve(
    cycle_id="cycle-001",
    mode=CycleMode.LEARNING,
    source_config=None,  # Uses mode default
)
```

### SourceResolutionResult

Immutable snapshot of source resolution at cycle start:

```python
@dataclass
class SourceResolutionResult:
    resolution_id: str
    cycle_id: str
    mode: str
    selected_pool_names: List[str]
    selected_pool_ids: List[str]
    tiers_used: List[str]
    selection_hash: str  # For determinism verification
    input_hash: str
    context_id: str
    pools_considered: int
    pools_eligible: int
    pools_excluded: int
    # ... config snapshot
```

### UnboundSourceContext

Read-only source context that knows which pools WOULD be used but does NOT bind them:

```python
context = UnboundSourceContext.from_resolution_result(result)
assert context.bound is False  # Always False in Phase 8.B
```

## Determinism Guarantee

Same inputs → Same outputs. Verified via hashes:

```python
# First resolution
result1 = resolver.resolve("cycle-001", CycleMode.LEARNING, config)

# Second resolution with same inputs
result2 = resolver.resolve("cycle-001", CycleMode.LEARNING, config)

assert result1.selection_hash == result2.selection_hash  # ✅ Deterministic
```

Verification function:

```python
from agents.core.source_resolution import verify_resolution_determinism

is_deterministic = verify_resolution_determinism(
    odibi_root=odibi_root,
    cycle_id="cycle-001",
    mode=CycleMode.LEARNING,
    expected_hash="abc123",
)
```

## Guardrails Enforced

All guardrails from Phase 8.A are enforced at config-time:

| Guardrail | Mode | Enforcement |
|-----------|------|-------------|
| Scheduled mode MUST use frozen pools | SCHEDULED | `require_frozen=False` → ERROR |
| tier600gb+ blocked in Improvement mode | IMPROVEMENT | `allowed_tiers=[TIER600GB]` → ERROR |
| Cleanliness constraints consistent | ALL | `require_clean=True, allow_messy=True` → ERROR |

Errors raised as `SourceResolutionError`:

```python
except SourceResolutionError as e:
    print(e.error_type)  # "CONFIG_VALIDATION_FAILED"
    print(e.message)     # "Scheduled mode MAY ONLY use FROZEN pools..."
    print(e.details)     # {"errors": [...], "mode": "scheduled"}
```

## CycleRunner Integration

### start_cycle() Signature Change

```python
def start_cycle(
    self,
    config: CycleConfig,
    mode: AssistantMode = AssistantMode.GUIDED_EXECUTION,
    source_config: Optional[CycleSourceConfig] = None,  # NEW in 8.B
) -> CycleState:
```

### CycleState.source_resolution

The `source_resolution` field is populated with the result dict:

```python
state = runner.start_cycle(config, mode, source_config)

# Access resolution metadata
if state.source_resolution:
    pools = state.source_resolution["selected_pool_names"]
    hash = state.source_resolution["selection_hash"]
```

### Graceful Degradation

If resolution fails (e.g., no metadata), the cycle continues with `source_resolution=None`:

```python
# No failure - just warning event emitted
state = runner.start_cycle(config, mode)
if state.source_resolution is None:
    print("Source resolution not available")
```

## Report Integration

The cycle report now includes a **Source Resolution** section:

```markdown
## Source Resolution

_Source pools resolved at cycle start (Phase 8.B - read-only, no execution)._

### Selected Pools (2)

- Tier0 Clean Pool
- Tier20GB Clean Pool

### Resolution Details

| Property | Value |
|----------|-------|
| Policy | `learning_default` |
| Strategy | `hash_based` |
| Tiers Used | tier0, tier20gb |
| Max Sources | 3 |
| Require Frozen | Yes |
| Allow Messy | Yes |

**Pool Statistics:** 10 considered → 5 eligible → 2 selected (5 excluded)

### Determinism Verification

_Same inputs → Same selection (verified via hashes)._

- **Selection Hash:** `abc123def456`
- **Input Hash:** `789ghi012jkl`
- **Context ID:** `ctx_cycle-001_abc123`
- **Resolved At:** 2024-01-15T10:30:00Z
```

## Hard Constraints (DO NOT VIOLATE)

❌ NO pipeline execution
❌ NO data downloads
❌ NO ExecutionGateway changes
❌ NO agent autonomy changes
❌ NO side effects
❌ NO source binding/mounting

Everything is **READ-ONLY** and **DETERMINISTIC**.

## Testing

Run Phase 8.B tests:

```bash
pytest agents/core/tests/test_source_resolution.py -v
```

Test categories:
- `TestDeterminism` - Same inputs → Same outputs
- `TestGuardrailViolations` - Invalid configs fail fast
- `TestModeBehavior` - Scheduled vs Improvement mode
- `TestCycleRunnerIntegration` - start_cycle() integration
- `TestReportIntegration` - Report generation

## Future Phases

| Phase | Description |
|-------|-------------|
| 8.C | Policy registry and loading |
| 8.D | Tier availability checks |
| 8.E | Context binding to ExecutionGateway |

## Files Changed

- `agents/core/source_resolution.py` - NEW
- `agents/core/cycle.py` - Updated `start_cycle()`, added `CycleState.source_resolution`
- `agents/core/reports.py` - Added `_generate_source_resolution()`
- `agents/core/tests/test_source_resolution.py` - NEW
- `agents/docs/PHASE8B_SOURCE_RESOLUTION.md` - NEW (this file)
