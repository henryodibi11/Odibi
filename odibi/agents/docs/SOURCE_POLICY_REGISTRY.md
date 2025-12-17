# Source Policy Registry

**Phase 8.C** - First-class policy management for source selection.

## Overview

The Source Policy Registry provides a formal system for:
- Loading source selection policies from disk
- Validating policies deterministically
- Making policies discoverable to cycles, reports, and humans
- Capturing policy metadata for audit trails

## Intent

Before Phase 8.C, policies were created inline or via defaults. This led to:
- No central place to discover available policies
- No validation of policy-mode compatibility before execution
- No audit trail of which policy was actually used
- No hash verification for determinism

The registry solves these problems by treating policies as **first-class configuration** that is:
- Stored on disk (version-controllable)
- Loaded and validated at resolution time
- Hashed for determinism verification
- Fully documented in cycle reports

## Directory Structure

```
.odibi/
└── selection_policies/
    ├── learning_default.yaml
    ├── improvement_default.yaml
    ├── scheduled_default.yaml
    └── custom_policy.yaml
```

## Policy Schema

Policies are YAML files with the following schema:

```yaml
# Required fields
policy_id: my_custom_policy      # Unique identifier (lowercase + underscores)
name: My Custom Policy           # Human-readable name

# Optional identification
description: "Policy for specific use case"
version: "1.0.0"
author: "team_name"

# Pool constraints
eligible_pools: null             # Explicit pool list (null = all frozen)
excluded_pools: []               # Blocklist of pool IDs
max_pools_per_cycle: 3           # 1-10

# Format/type constraints
allowed_formats: null            # null = all formats allowed
allowed_source_types: null       # null = all types allowed

# Quality constraints
allow_messy_data: true           # Whether messy pools can be selected
clean_vs_messy_ratio: null       # 0.0-1.0 target ratio

# Selection behavior
selection_strategy: hash_based   # round_robin, hash_based, coverage_first, explicit
prefer_uncovered_formats: true
prefer_uncovered_pools: true

# Registry-specific fields
require_frozen: true             # MUST be true for scheduled mode
require_deterministic: true
allowed_modes:                   # Which modes this policy is valid for
  - learning
  - scheduled
disallowed_tiers:                # Tiers that MUST NOT be used
  - tier600gb
  - tier2tb
```

## Validation Rules

The registry enforces these validation rules:

### 1. Scheduled Mode → require_frozen = true

Scheduled cycles MUST use frozen pools only. This ensures reproducibility.

```yaml
# INVALID for scheduled mode
require_frozen: false

# VALID for scheduled mode
require_frozen: true
```

### 2. Improvement Mode → Disallow tier600gb+

Improvement mode validates changes against test suites. Large tiers (600GB+) are too slow for this.

```yaml
# Recommended for improvement mode
disallowed_tiers:
  - tier600gb
  - tier2tb
```

A warning is issued if these tiers are not explicitly disallowed.

### 3. No Conflicting Flags

The registry detects contradictory settings:

```yaml
# WARNING: Contradictory settings
clean_vs_messy_ratio: 1.0  # "all clean"
allow_messy_data: true     # "messy allowed"
```

### 4. Explicit Defaults Only

Policies should explicitly state their constraints rather than relying on implicit defaults. An INFO-level note is issued when `allowed_modes` is not specified.

## API Reference

### SourcePolicyRegistry

```python
from agents.core.source_policy_registry import SourcePolicyRegistry

# Initialize and load
registry = SourcePolicyRegistry(odibi_root)
count = registry.load_all()

# Discovery
policy_ids = registry.list()       # Sorted list of policy IDs
exists = registry.exists("my_policy")

# Retrieval
policy = registry.get("my_policy")  # Returns RegisteredPolicy
selection_policy = registry.get_selection_policy("my_policy")

# Validation and resolution
issues = registry.validate("my_policy", mode=CycleMode.SCHEDULED)
result = registry.resolve("my_policy", mode=CycleMode.SCHEDULED)
```

### PolicyResolutionResult

```python
@dataclass
class PolicyResolutionResult:
    policy_name: str              # Policy ID
    policy_hash: str              # SHA256 hash (16 chars)
    validation_status: PolicyValidationStatus  # VALID, INVALID, WARNINGS
    resolved_at: str              # ISO timestamp
    warnings: List[str]           # Validation warnings
    policy_version: str           # Policy version
    policy_path: str              # Path to policy file
    mode_validated: str           # Mode used for validation
```

### Convenience Functions

```python
from agents.core.source_policy_registry import (
    load_policy_registry,
    resolve_policy_for_cycle,
)

# Load registry
registry = load_policy_registry(odibi_root)

# Resolve policy for cycle
policy, result = resolve_policy_for_cycle(
    odibi_root,
    "my_policy",
    CycleMode.SCHEDULED
)
```

## Integration with Source Resolution

The registry is integrated into `SourceResolver.resolve()`:

```python
resolver = SourceResolver(odibi_root)
result = resolver.resolve(
    cycle_id="cycle_001",
    mode=CycleMode.SCHEDULED,
    use_registry=True,  # Enable registry lookup
)

# Result now includes policy metadata
print(result.policy_hash)
print(result.policy_validation_status)
print(result.policy_warnings)
```

### Resolution Flow

1. Load `CycleSourceConfig` (determines policy name)
2. Load policy from registry
3. Validate policy against mode
4. **FAIL FAST** if validation fails (no silent fallbacks)
5. Apply policy to selection
6. Capture policy metadata in `SourceResolutionResult`

## Reports

Cycle reports now include a "Source Selection Policy" section:

```markdown
## Source Selection Policy

_Policy configuration validated at cycle start (Phase 8.C)._

### Policy Details

| Property | Value |
|----------|-------|
| Policy ID | `scheduled_default` |
| Version | `1.0.0` |
| Hash | `abc123def456` |
| Validation | valid |

### Constraints

| Constraint | Value |
|------------|-------|
| Max Sources | 3 |
| Require Frozen | Yes |
| Allow Messy | Yes |
| Allow Tier Mixing | No |
| Allowed Tiers | tier0, tier20gb, tier100gb |

### Determinism Guarantee

_Same policy hash + same pool index → same selection.
Re-run with identical inputs to verify._
```

## Invariants

The registry maintains these invariants:

1. **READ-ONLY** - No mutations to policy files
2. **DETERMINISTIC** - Same policy file → same hash, same validation
3. **FAIL FAST** - Invalid policies fail at resolution, not execution
4. **NO SILENT FALLBACKS** - If policy not found or invalid, resolution fails
5. **AUDITABLE** - All policy decisions are captured in reports

## Non-Goals

The registry explicitly does NOT:

- **Execute** policies (that's `SourceSelector`)
- **Bind** sources to execution (that's Phase 8.E)
- **Download** data (registry is metadata-only)
- **Modify** policies (read-only)
- **Provide defaults** for missing policies (fail fast instead)

## Example Policy Files

### Learning Mode Default

```yaml
# .odibi/selection_policies/learning_default.yaml
policy_id: learning_default
name: Learning Mode Default
description: Broad coverage, messy data allowed, exploration-friendly
version: "1.0.0"

max_pools_per_cycle: 4
allow_messy_data: true
clean_vs_messy_ratio: 0.5
selection_strategy: coverage_first
prefer_uncovered_formats: true
prefer_uncovered_pools: true
require_frozen: true

allowed_modes:
  - learning
```

### Scheduled Mode Default

```yaml
# .odibi/selection_policies/scheduled_default.yaml
policy_id: scheduled_default
name: Scheduled Mode Default
description: Fully deterministic, reproducible byte-for-byte
version: "1.0.0"

max_pools_per_cycle: 3
allow_messy_data: true
selection_strategy: hash_based
prefer_uncovered_formats: false
prefer_uncovered_pools: false
require_frozen: true  # REQUIRED for scheduled mode

allowed_modes:
  - scheduled
```

### Improvement Mode Default

```yaml
# .odibi/selection_policies/improvement_default.yaml
policy_id: improvement_default
name: Improvement Mode Default
description: Narrow, stable selection for change validation
version: "1.0.0"

max_pools_per_cycle: 2
allow_messy_data: false
clean_vs_messy_ratio: 1.0
selection_strategy: hash_based
prefer_uncovered_formats: false
prefer_uncovered_pools: false
require_frozen: true

allowed_modes:
  - improvement

disallowed_tiers:  # Large tiers too slow for improvement validation
  - tier600gb
  - tier2tb
```

## Testing

Run tests with:

```bash
pytest agents/core/tests/test_source_policy_registry.py -v
```

Tests cover:
- Deterministic loading
- Invalid policy rejection
- Hash stability
- Scheduled vs Improvement enforcement
- Mode constraint validation
- Registry API (list, get, exists)
