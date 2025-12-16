# Phase 9.C: Cycle Profiles

## Overview

Phase 9.C makes cycle profiles **executable**, **auditable**, and **selectable from the UI**.

Cycle profiles are YAML configuration files stored in `.odibi/cycle_profiles/` that define deterministic learning behavior. They replace hardcoded parameters in the scheduler with versioned, hashable, frozen configurations.

## Goals

- ✅ Profiles loaded from `.odibi/cycle_profiles/`
- ✅ Profile config frozen for entire session
- ✅ Profile name + hash recorded in heartbeat, reports, session metadata
- ✅ UI dropdown to select profiles
- ✅ Same profile + same inputs → same execution

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        cycle_profile.py                         │
├─────────────────────────────────────────────────────────────────┤
│  CycleProfile (Pydantic)    ─────►  FrozenCycleProfile          │
│      ↑                                   ↓                      │
│  YAML File                    LearningCycleConfig               │
│      ↑                                   ↓                      │
│  CycleProfileLoader  ◄──────  AutonomousLearningScheduler       │
└─────────────────────────────────────────────────────────────────┘
```

## Components

### 1. CycleProfile (Pydantic Schema)

Located in `agents/core/cycle_profile.py`.

Validates profile YAML against Phase 9.A learning mode invariants:

```python
from agents.core import CycleProfile

profile = CycleProfile(
    profile_id="large_scale_learning",
    profile_name="Learning at Scale",
    mode="learning",           # Must be "learning"
    max_improvements=0,        # Must be 0
    cycle_source_config=CycleSourceConfigSchema(
        require_frozen=True,   # Must be True
        deterministic=True,    # Must be True
    ),
    guardrails=GuardrailsSchema(
        allow_source_mutation=False,  # Must be False
    ),
)
```

### 2. FrozenCycleProfile (Immutable Runtime Config)

A frozen dataclass that **cannot be modified after loading**:

```python
from agents.core import CycleProfileLoader

loader = CycleProfileLoader(odibi_root="d:/odibi")
frozen = loader.load_profile("large_scale_learning")

# Immutable - this raises an error:
frozen.profile_id = "changed"  # ❌ FrozenInstanceError
```

### 3. Profile Hash

Every profile has a SHA-256 content hash (first 16 chars) for auditability:

```python
from agents.core import compute_profile_hash

content = open("profile.yaml").read()
hash = compute_profile_hash(content)  # e.g., "a1b2c3d4e5f6g7h8"
```

### 4. CycleProfileLoader

Discovers and loads profiles from `.odibi/cycle_profiles/`:

```python
from agents.core import CycleProfileLoader

loader = CycleProfileLoader("d:/odibi")

# List available profiles
profiles = loader.list_profiles()  # ["large_scale_learning", "quick_test"]

# Load and freeze a profile
frozen = loader.load_profile("large_scale_learning")

# Get summary for UI display
summary = loader.get_profile_summary("large_scale_learning")
```

## Usage

### Running a Session with a Profile

```python
from agents.core import AutonomousLearningScheduler

scheduler = AutonomousLearningScheduler(odibi_root="d:/odibi")

# Preferred: Use run_session_with_profile()
session = scheduler.run_session_with_profile(
    profile_name="large_scale_learning",
    project_root="d:/odibi/examples",
    max_cycles=10,
    max_wall_clock_hours=8.0,
)

# Alternative: Create config from frozen profile
frozen = scheduler.load_profile("large_scale_learning")
config = LearningCycleConfig.from_frozen_profile(
    profile=frozen,
    project_root="d:/odibi/examples",
)
session = scheduler.run_session(config=config)
```

### Creating a Profile

Create a YAML file in `.odibi/cycle_profiles/`:

```yaml
# .odibi/cycle_profiles/my_learning.yaml

profile_id: my_learning
profile_name: "My Learning Profile"
profile_version: "1.0.0"
description: |
  Custom learning profile for my use case.

mode: learning
max_improvements: 0

cycle_source_config:
  selection_policy: learning_default
  allowed_tiers:
    - tier100gb
  max_sources: 2
  require_frozen: true
  deterministic: true

guardrails:
  allow_execution: false
  allow_downloads: false
  allow_source_mutation: false
  max_duration_hours: 12

metadata:
  author: you
  intended_use: "Custom learning runs"
```

## Auditability

### Heartbeat

The heartbeat file includes profile info:

```json
{
  "last_cycle_id": "abc123",
  "timestamp": "2024-01-01T12:00:00",
  "profile_id": "large_scale_learning",
  "profile_name": "Learning at Scale",
  "profile_hash": "a1b2c3d4e5f6g7h8"
}
```

### Cycle Reports

Reports include a profile section:

```markdown
## Cycle Metadata

- **Cycle ID:** `abc123`
- **Mode:** learning

### Cycle Profile

- **Profile ID:** `large_scale_learning`
- **Profile Name:** Learning at Scale
- **Profile Hash:** `a1b2c3d4e5f6g7h8`
```

### Session Metadata

```python
session.profile_id     # "large_scale_learning"
session.profile_name   # "Learning at Scale"  
session.profile_hash   # "a1b2c3d4e5f6g7h8"
```

## UI Integration

The Learning Session panel includes:

1. **Profile Dropdown** - Select from available profiles
2. **Profile Info Display** - Shows name, description, and hash
3. **Refresh Button** - Reload available profiles
4. **Status Display** - Shows active profile in session monitor

## Safety Invariants

Phase 9.C maintains all Phase 9.A learning mode safety guards:

- ❌ No ImprovementAgent execution
- ❌ No code edits
- ❌ No file writes outside allowed directories
- ❌ No profile modification at runtime
- ❌ No agent writes to profiles
- ✅ Profile frozen for entire session
- ✅ Deterministic execution
- ✅ Full audit trail

## Error Handling

Profile loading fails fast on:

- **Invalid YAML syntax** → `CycleProfileError`
- **Missing required fields** → `CycleProfileError` with validation errors
- **Learning mode invariant violations** → `ValueError`
- **Profile not found** → `CycleProfileError`

```python
try:
    frozen = loader.load_profile("invalid_profile")
except CycleProfileError as e:
    print(f"Profile error: {e}")
    print(f"Validation errors: {e.errors}")
```

## Testing

```bash
# Run profile tests
pytest agents/core/tests/test_cycle_profile.py -v

# Run all Phase 9 tests
pytest agents/core/tests/test_autonomous_learning.py -v
pytest agents/core/tests/test_disk_guard.py -v
```

## Files Changed

- `agents/core/cycle_profile.py` - New module
- `agents/core/autonomous_learning.py` - Profile integration
- `agents/core/disk_guard.py` - HeartbeatData profile fields
- `agents/core/reports.py` - Profile section in reports
- `agents/core/__init__.py` - Exports
- `agents/ui/components/cycle_panel.py` - UI dropdown
- `agents/core/tests/test_cycle_profile.py` - New tests
