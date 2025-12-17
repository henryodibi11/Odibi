# Cycle Profiles

Cycle profiles are pre-configured, validated YAML files that define deterministic cycle behavior for specific use cases.

## Location

```
.odibi/cycle_profiles/
├── large_scale_learning.yaml
└── (future profiles)
```

## Purpose

Profiles provide:
- **Determinism**: Same profile → same behavior
- **Safety**: Pre-validated against mode guardrails
- **Reviewability**: Human-readable YAML with inline documentation
- **Reusability**: Share configurations across teams and environments

---

## Available Profiles

### `large_scale_learning.yaml`

**Purpose:** Deterministic learning profile for large datasets (100GB–600GB).

#### Intended Use

- Analyzing FineWeb, OSM Planet, or similar production-scale datasets
- Building understanding of data quality patterns at scale
- Generating observations for future improvement cycles
- Testing pipeline throughput on real-world data volumes

#### Configuration Summary

| Setting | Value | Rationale |
|---------|-------|-----------|
| `mode` | `learning` | Observe patterns without code changes |
| `max_improvements` | `0` | Observation-only, no proposals |
| `allowed_tiers` | `tier100gb`, `tier600gb` | Large-scale datasets only |
| `require_frozen` | `true` | Reproducibility and integrity |
| `allow_messy` | `true` | Learn from real-world data quality |
| `allow_tier_mixing` | `false` | Predictable resource planning |
| `deterministic` | `true` | Same inputs → same outputs |

#### Explicit Non-Goals

This profile is **NOT** intended for:

- ❌ Proposing or applying code improvements
- ❌ Downloading new data sources
- ❌ Modifying existing source pools
- ❌ Quick iteration or debugging
- ❌ Small dataset testing (use `tier0`/`tier20gb` profiles)

#### Why `max_improvements: 0`?

At 100GB–600GB scale, we want cycles to:
1. Complete predictably without proposal overhead
2. Generate observations that humans review first
3. Avoid accidental changes to production-adjacent pipelines
4. Focus on pattern discovery, not code modification

#### Why `require_frozen: true`?

Frozen pools provide:
1. **Reproducibility**: Stable hashes for verification
2. **Integrity**: Data validated via `prepare_source_pools.py`
3. **Auditability**: Selection can be verified byte-for-byte
4. **Safety**: No accidental use of corrupted data

#### Why `allow_messy: true`?

Learning mode should observe ALL data patterns:
- Null handling edge cases
- Duplicate detection scenarios
- Schema drift examples
- Real-world data quality issues

Understanding messy data is prerequisite to cleaning it.

---

## Profile Schema

Profiles must include:

```yaml
# Required
profile_id: string          # Unique identifier
profile_name: string        # Human-readable name
mode: learning|improvement|scheduled
max_improvements: integer   # 0 for observation-only

# Source configuration
cycle_source_config:
  selection_policy: string  # Must exist in .odibi/selection_policies/
  allowed_tiers: list       # tier0, tier20gb, tier100gb, tier600gb, tier2tb
  max_sources: integer      # 1-20
  require_frozen: boolean
  require_clean: boolean
  allow_messy: boolean
  deterministic: boolean
  allow_tier_mixing: boolean

# Optional
guardrails:
  allow_execution: boolean
  allow_downloads: boolean
  max_duration_hours: integer

metadata:
  author: string
  created_at: ISO timestamp
  non_goals: list
```

---

## Validation

Profiles are validated against:

1. **CycleSourceConfig schema** - Pydantic model validation
2. **Mode guardrails** - `CycleSourceConfigGuard.validate()`
3. **Policy existence** - Selection policy must be registered

Validation happens at profile load time, not execution time.

---

## Future Profiles

Planned profiles include:

| Profile | Mode | Tiers | Use Case |
|---------|------|-------|----------|
| `quick_iteration.yaml` | improvement | tier0 | Fast local testing |
| `integration_test.yaml` | scheduled | tier20gb | CI/CD validation |
| `production_scheduled.yaml` | scheduled | tier100gb+ | Production runs |

---

## Invariants

All cycle profiles must:

1. Be **READ-ONLY** at runtime
2. Be **deterministic** (same inputs → same outputs)
3. Have **explicit** settings (no implicit defaults)
4. Include **inline documentation** explaining choices
5. Pass **schema validation** before use
