# Memory Query Contract (Phase 5.F)

This document defines which agents may query indexed memory and why.

## Overview

Indexed memory contains validated learnings from past cycles. Access to this memory is strictly controlled to prevent agents from:
- Bypassing human review
- Making autonomous decisions based on past data
- Selecting files or ranking proposals based on memory

## Access Levels

| Level | Description |
|-------|-------------|
| `NONE` | Agent has no memory access |
| `READ_ONLY_ADVISORY` | Agent can read memory for advisory context only (cannot use for decisions) |

## Agent Memory Access

| Agent | Access Level | Allowed Intents | Rationale |
|-------|--------------|-----------------|-----------|
| Observer | NONE | - | Analyzes current execution only. Memory would bias observations. |
| Improvement | NONE | - | Proposes based on current observations. Memory-influenced proposals would bypass review. |
| Reviewer | READ_ONLY_ADVISORY | PATTERN_LOOKUP, RISK_ASSESSMENT, PRECEDENT_SUMMARY, HUMAN_REVIEW_SUPPORT | May see advisory context but MUST NOT use it for approval decisions. |
| Environment | NONE | - | Validates current setup only. |
| Project | NONE | - | Project selection is user-driven. |
| User | NONE | - | Simulates user behavior. |
| Regression Guard | NONE | - | Runs tests deterministically. Memory would introduce non-determinism. |
| Curriculum | NONE | - | Writes to memory but does not query it. |
| Convergence | READ_ONLY_ADVISORY | CONVERGENCE_SIGNAL, PRECEDENT_SUMMARY | May check for convergence signals but cannot influence execution. |

## Component Memory Access

| Component | Access Level | Allowed Intents | Output Constraint |
|-----------|--------------|-----------------|-------------------|
| ScorecardGenerator | READ_ONLY_ADVISORY | PATTERN_LOOKUP, RISK_ASSESSMENT, PRECEDENT_SUMMARY, CONVERGENCE_SIGNAL | Output is advisory only, attached to reports |
| CycleIndexManager | NONE | - | Writes to memory, queries only for stats |

## Critical Constraints

1. **All queries MUST include an explicit `MemoryQueryIntent`**
2. **Advisory context does NOT influence approval decisions**
3. **Memory cannot be used to select files, rank proposals, or bypass review**
4. **Observer and Improvement agents have NO memory access**
5. **Reviewer MUST ignore advisory context when making approval decisions**

## Forbidden Actions by Agent

### Observer Agent
- ❌ Query indexed memory
- ❌ Access previous cycle data
- ❌ Reference past improvements

### Improvement Agent
- ❌ Query indexed memory
- ❌ Copy previous improvements
- ❌ Auto-generate proposals from memory

### Reviewer Agent
- ❌ Use memory to approve/reject proposals
- ❌ Auto-approve based on precedent
- ❌ Bypass review based on similarity

**Explicit Constraint:** Advisory context is for HUMAN REFERENCE ONLY. ReviewerAgent decision logic MUST ignore `advisory_context`.

## Programmatic Validation

```python
from agents.core.memory_access import (
    can_query_memory,
    get_allowed_intents,
    validate_query_for_role,
)
from agents.core.agent_base import AgentRole
from agents.core.memory_guardrails import MemoryQueryIntent

# Check if an agent can query memory
assert can_query_memory(AgentRole.REVIEWER) is True
assert can_query_memory(AgentRole.OBSERVER) is False

# Validate a specific query
allowed, reason = validate_query_for_role(
    AgentRole.REVIEWER,
    MemoryQueryIntent.PATTERN_LOOKUP,
)
assert allowed is True

# This would be blocked
allowed, reason = validate_query_for_role(
    AgentRole.IMPROVEMENT,
    MemoryQueryIntent.PATTERN_LOOKUP,
)
assert allowed is False
```

## Generating Contract Documentation

```python
from agents.core.memory_access import generate_contract_documentation

# Generate markdown documentation
doc = generate_contract_documentation()
print(doc)
```
