# Review Context Enrichment (Phase 5.F)

## Overview

Phase 5.F adds advisory context enrichment to reviews and reports. This context is **READ-ONLY** and **DOES NOT influence decisions**.

## What This Phase Adds

### 1. Review Context Enrichment

The `ReviewContextEnricher` builds advisory context containing:
- **Proposal Scorecard** (from Phase 5.E)
- **File Change Summaries** (no code content)
- **Golden Project Coverage** list

### 2. Enhanced Cycle Reports

Cycle reports now include two new sections:

#### Files Affected (Summary)
A table showing files changed without code content:

```markdown
## Files Affected (Summary)

_Advisory only — no impact on approval_

| File | Change | Description |
|------|--------|-------------|
| `src/main.py` | MODIFY | Content modified |
| `src/new.py` | ADD | New content added |
```

#### Memory Signals
A table showing memory-derived signals with explicit disclaimer:

```markdown
## Memory Signals

_⚠️ Advisory only — no impact on approval. This information is contextual
and does not approve, reject, or prioritize changes._

| Signal | Level | Evidence |
|--------|-------|----------|
| Similarity | HIGH | This proposal resembles 3 previously approved improvements |
| Historical Risk | LOW | No regressions found in memory for these files |
| Novelty | MEDIUM | Limited precedent: only 2 similar changes found |
```

## What This Phase Does NOT Do

- ❌ Does NOT change reviewer decisions
- ❌ Does NOT influence approvals
- ❌ Does NOT rank or select proposals
- ❌ Does NOT create new agents
- ❌ Does NOT add autonomy
- ❌ Does NOT expose raw memory or embeddings
- ❌ Does NOT surface diffs or code

## Usage

### Building Advisory Context

```python
from agents.core.review_context import ReviewContextEnricher, AdvisoryContext
from agents.core.proposal_scorecard import ScorecardGenerator

# Create enricher
enricher = ReviewContextEnricher()

# Build advisory context
advisory = enricher.build_advisory_context(
    proposal=improvement_proposal,
    scorecard=scorecard,  # From ScorecardGenerator
    golden_project_names=["smoke_test", "integration_test"],
)

# Attach to metadata (READ-ONLY)
metadata["advisory_context"] = advisory.to_dict()
```

### Attaching to Reports

```python
from agents.core.reports import CycleReportGenerator

generator = CycleReportGenerator(odibi_root)
generator.set_scorecard(scorecard)
generator.set_advisory_context(advisory)

report_content = generator.generate_report(cycle_state)
```

### Safety Flags

The `AdvisoryContext.to_dict()` includes explicit safety flags:

```python
{
    "disclaimer": "⚠️ ADVISORY ONLY: This information is contextual...",
    "is_advisory": True,
    "affects_decision": False,
    "scorecard": {...},
    "file_summaries": [...],
    "golden_projects": [...],
}
```

## Disclaimers

Every advisory output includes visible language:

> ⚠️ ADVISORY ONLY: This information is contextual and does not approve, reject, or prioritize changes. Reviewer decisions must be independent.

Shorter form for tables:

> _Advisory only — no impact on approval_

## File Change Summaries

File summaries contain ONLY safe information:

| Field | Description |
|-------|-------------|
| `file_path` | Path to the file |
| `change_type` | ADD / MODIFY / DELETE |
| `description` | One-line description (no code) |

**Explicitly NOT included:**
- Full diffs
- Raw code content
- Before/after snapshots
- Executable content

## Validation

```python
from agents.core.review_context import validate_advisory_context_is_read_only

# Validate that context has no decision-making methods
is_safe = validate_advisory_context_is_read_only(advisory)
assert is_safe is True
```

## Testing

Run tests with:

```bash
pytest agents/core/tests/test_review_context.py -v
```

Test coverage includes:
- Advisory context creation
- File summary generation
- Human-readable output
- Missing memory handling
- Advisory-only validation
