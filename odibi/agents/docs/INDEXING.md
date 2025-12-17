# Manual Re-Indexing of Cycle Learnings

**Phase 5.C, 5.D, 5.E & 5.F Documentation**

> **See also:**
> - [Memory Access Contract](MEMORY_ACCESS_CONTRACT.md) - Which agents can query memory
> - [Proposal Scorecard](PROPOSAL_SCORECARD.md) - Advisory scoring for proposals
> - [Review Context](REVIEW_CONTEXT.md) - Advisory context enrichment

## Overview

This document explains when and why to run manual re-indexing of cycle learnings, and the guardrails that protect how indexed memory can be queried.

Re-indexing is a **human-blessed step**, not an automatic side-effect of cycle execution. This preserves:

- **Debuggability**: You know exactly what was indexed and when
- **Trust**: Only validated, reviewed content enters the knowledge base
- **Separation**: Clear boundary between execution and learning

## When to Run Re-Indexing

Run re-indexing when you want agents to learn from completed cycles:

1. **After reviewing cycle reports** - Verify the conclusions make sense
2. **Periodically (e.g., weekly)** - Batch index multiple completed cycles
3. **Before major work sessions** - Ensure agents have latest validated learnings
4. **After a series of successful improvements** - Capture patterns that worked

## When NOT to Run Re-Indexing

- Immediately after every cycle (let reports accumulate for review)
- For partial or failed cycles (automatically filtered out)
- When you haven't reviewed the reports yet

## What Gets Indexed

Only high-signal, validated content:

| Content | Indexed |
|---------|---------|
| Final conclusions | ✅ Yes |
| Approved improvements | ✅ Yes |
| Regression results | ✅ Yes |
| Golden project outcomes | ✅ Yes |
| Cycle metadata | ✅ Yes |
| Raw agent messages | ❌ No |
| Rejected proposals | ❌ No |
| Incomplete cycles | ❌ No |
| Failed cycles | ❌ No |

## Usage

### CLI Command

```bash
# From the Odibi workspace root
python -m agents.pipelines.indexer --cycles

# Or directly
python -c "from agents.pipelines.indexer import run_cycle_indexing_cli; run_cycle_indexing_cli()"
```

### Options

```bash
# Show index statistics
python -m agents.pipelines.indexer --cycles --stats

# Force re-index already indexed cycles
python -m agents.pipelines.indexer --cycles --force

# Clear all indexed cycle data
python -m agents.pipelines.indexer --cycles --clear

# Specify workspace root
python -m agents.pipelines.indexer --cycles --odibi-root /path/to/workspace
```

### Programmatic Usage

```python
from agents.core.indexing import CycleIndexManager

# Initialize with .odibi directory path
manager = CycleIndexManager("/path/to/workspace/.odibi")

# Index all eligible completed cycles
result = manager.index_completed_cycles()

print(f"Indexed: {len(result.indexed)}")
print(f"Skipped (already indexed): {len(result.skipped_already_indexed)}")

# Check statistics
stats = manager.get_index_stats()
print(f"Total indexed cycles: {stats['indexed_cycle_count']}")
```

## Filtering Logic

Cycles are indexed only if they meet ALL criteria:

1. `completed == True` - Cycle ran to completion
2. `exit_status != "FAILURE"` - No critical failures
3. `interrupted == False` - Not user-interrupted
4. Not already indexed (unless `force=True`)

## Idempotency

Re-indexing is idempotent:

- Running twice skips already-indexed cycles
- Use `--force` to re-index everything
- Use `--clear` to start fresh

## Storage Location

Indexed data is stored in:

```
.odibi/
├── cycle_index/           # Vector store (ChromaDB)
├── indexed_cycles.json    # Tracking metadata
└── reports/               # Source Markdown reports
    └── cycle_<id>.md
```

## Best Practices

1. **Review before indexing** - Read the Markdown reports first
2. **Index periodically** - Don't index after every cycle
3. **Monitor the stats** - Use `--stats` to track what's indexed
4. **Clear sparingly** - Only clear when you want to rebuild from scratch

## File Change Summaries (Phase 5.D)

For approved and applied improvements, safe file change summaries are also indexed:

```yaml
change:
  file: odibi/transformers/cast.py
  type: MODIFY
  summary: Replaced deprecated `columns` with `schema`
  issues: [CONFIGURATION_ERROR]
  validated_by_golden_projects: true
  cycle_id: <uuid>
```

These summaries explicitly do NOT contain:
- Full diffs
- Raw code content
- Before/after snapshots
- Executable content

---

## Memory Query Guardrails (Phase 5.D)

Indexed memory is protected by strict query guardrails enforced at the API boundary.

### Allowed Query Intents

All queries to indexed memory MUST include an explicit intent:

| Intent | Purpose |
|--------|---------|
| `PATTERN_LOOKUP` | Look up previously observed patterns or solutions |
| `RISK_ASSESSMENT` | Assess risk based on past regressions or failures |
| `PRECEDENT_SUMMARY` | Summarize precedents for similar situations |
| `CONVERGENCE_SIGNAL` | Check for convergence signals from past cycles |
| `HUMAN_REVIEW_SUPPORT` | Provide context to support human review decisions |

### Allowed Query Examples

```python
from agents.core.memory_guardrails import MemoryQueryValidator, MemoryQueryIntent

validator = MemoryQueryValidator()

# ✅ Valid: Pattern lookup with explicit intent
query = validator.validate_and_create(
    query_text="What patterns exist for handling null values?",
    intent=MemoryQueryIntent.PATTERN_LOOKUP,
)

# ✅ Valid: Risk assessment
query = validator.validate_and_create(
    query_text="Have similar schema changes caused regressions before?",
    intent=MemoryQueryIntent.RISK_ASSESSMENT,
)
```

### Disallowed Queries

The following query types are **MECHANICALLY BLOCKED** at the API boundary:

| Blocked Type | Examples |
|--------------|----------|
| `DECIDE_NEXT_CHANGE` | "What should I change next?" |
| `SELECT_FILES` | "Which files should I modify?" |
| `APPLY_CHANGES` | "Apply the change from yesterday" |
| `BYPASS_REVIEW` | "Skip the review for this fix" |
| `RAW_AGENT_REASONING` | "Show me raw agent thoughts" |

```python
# ❌ BLOCKED: Attempting to decide next change
validator.validate_and_create(
    query_text="What should I change next?",
    intent=MemoryQueryIntent.PATTERN_LOOKUP,
)
# Raises: DisallowedQueryError

# ❌ BLOCKED: Missing intent
validator.validate_and_create(
    query_text="Some query",
    intent=None,
)
# Raises: MissingIntentError
```

### Why These Guardrails Exist

1. **Prevent autonomous decision-making** - Agents cannot use indexed memory to decide what to do next
2. **Preserve human oversight** - All action-directing decisions require human involvement
3. **Block replay attacks** - Agents cannot automatically apply previous changes
4. **Protect review integrity** - Review and regression checks cannot be bypassed

---

## Design Constraints

This implementation intentionally does NOT:

- Automatically index after cycles complete
- Allow agents to trigger indexing
- Index raw agent thoughts or rejected proposals
- Create any learning feedback loops
- Allow action-directing queries

Re-indexing remains a deliberate, human-controlled action.
Memory queries are constrained to informational intents only.
