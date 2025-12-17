# Learning Mode vs Improvement Mode

This document explains the two operational modes for cycles and why they differ.

## Overview

Cycles operate in one of two modes based on `max_improvements`:

| Setting | Mode | Behavior |
|---------|------|----------|
| `max_improvements = 0` | **Learning Mode** | Observation-only, deterministic, non-polluting |
| `max_improvements > 0` | **Improvement Mode** | Full cycle with proposals, reviews, and changes |

## Learning Mode (Observation-Only)

Learning mode is activated when `max_improvements == 0`.

### Characteristics

- **Deterministic**: No autonomous decisions
- **Predictable**: Same inputs → same behavior
- **Non-polluting**: Does not contaminate production memory

### Behavior Differences

#### 1. Project Selection is Skipped

In learning mode, ProjectAgent is **bypassed entirely**.

**Why?**
- ProjectAgent autonomously selects projects based on coverage gaps
- This introduces non-determinism in learning runs
- Users want to observe a specific project, not have one chosen

**What happens instead:**
- The user-provided `project_root` from the UI is used directly
- The skip reason is logged: `"Learning mode: using user-provided project '...'"`

#### 2. Memory is Scoped Separately

Memories written during learning cycles go to a **learning scope**, not validated scope.

**Why?**
- Learning runs generate observations that are not yet validated
- Mixing unvalidated observations with production curriculum is risky
- Learning observations may be wrong or misleading

**What happens:**
- Memories are stored in `.odibi/memories/learning/` (separate directory)
- Each memory is tagged with `scope: "learning"`
- Learning memories are never merged into validated curriculum automatically

**Memory Scopes:**

| Scope | Purpose | Location |
|-------|---------|----------|
| `validated` | Durable, production memories from successful cycles | `.odibi/memories/` |
| `learning` | Ephemeral, non-validated observations | `.odibi/memories/learning/` |

#### 3. Improvement Steps are Skipped

With `max_improvements = 0`:
- ImprovementAgent is skipped
- ReviewerAgent is skipped (nothing to review)
- RegressionGuardAgent is skipped (no changes to validate)

This is existing behavior (not changed by learning mode).

## Improvement Mode

Improvement mode is activated when `max_improvements > 0`.

### Behavior

- ProjectAgent may autonomously select projects
- ImprovementAgent proposes source code changes
- ReviewerAgent approves or rejects proposals
- RegressionGuardAgent validates against golden projects
- Memories are stored in validated scope

## How to Detect Mode

### In CycleConfig

```python
config = CycleConfig(project_root="...", max_improvements=0)
config.is_learning_mode  # True
```

### In Agent Context

```python
def process(self, context: AgentContext) -> AgentResponse:
    is_learning = context.metadata.get("is_learning_mode", False)
    memory_scope = context.metadata.get("memory_scope", "validated")
```

### In Cycle Logs

```json
{
  "step": "project_selection",
  "skipped": true,
  "skip_reason": "Learning mode: using user-provided project '...'"
}
```

## Rationale

### Why determinism matters for learning runs

1. **Reproducibility**: Same run → same observations
2. **Debugging**: Easier to diagnose issues when behavior is predictable
3. **Trust**: Users need to trust the system before enabling autonomy
4. **Auditing**: Learning runs should be boring and reviewable

### Why memory separation matters

1. **No pollution**: Unvalidated patterns don't contaminate curriculum
2. **Review before promotion**: Learning observations can be reviewed
3. **Safe discards**: Learning memories can be deleted without impact
4. **Clear provenance**: Every memory shows its validation status

## Summary

| Aspect | Learning Mode | Improvement Mode |
|--------|--------------|------------------|
| `max_improvements` | `0` | `> 0` |
| ProjectAgent | Skipped | Runs |
| ImprovementAgent | Skipped | Runs |
| Memory scope | `learning` | `validated` |
| Source changes | None | Possible |
| Determinism | High | Lower |
