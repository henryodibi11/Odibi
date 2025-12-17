# Memory-Informed Proposal Scorecard (Phase 5.E)

## Overview

The Proposal Scorecard is an **advisory-only** feature that evaluates improvement proposals using indexed memory. It provides context to humans and reviewers **without influencing control flow or decisions**.

## What the Scorecard Does

- Provides historical context about proposals
- Surfaces relevant precedents from indexed memory
- Helps reviewers understand risk signals
- Shows which golden projects exercise changed files

## What the Scorecard Does NOT Do

❌ **Does NOT approve or reject proposals**  
❌ **Does NOT rank proposals for execution**  
❌ **Does NOT modify agent decision logic**  
❌ **Does NOT suggest what to change next**  
❌ **Does NOT bypass reviewer**  
❌ **Does NOT influence execution flow**

The scorecard is purely informational. All review and approval decisions remain with humans.

## Scoring Dimensions

### 1. Similarity Score

Answers: "Have similar improvements been approved before?"

| Level   | Meaning                                          |
|---------|--------------------------------------------------|
| HIGH    | 3+ similar approved improvements found           |
| MEDIUM  | 1-2 similar approved improvements found          |
| LOW     | No similar improvements in memory                |
| UNKNOWN | Unable to assess (no proposal or memory unavailable) |

### 2. Historical Risk Signal

Answers: "Did similar changes historically cause regressions?"

| Level   | Meaning                                          |
|---------|--------------------------------------------------|
| HIGH    | 3+ past regressions involving similar changes    |
| MEDIUM  | 1-2 past regressions found                       |
| LOW     | No regressions found for these files/areas       |
| UNKNOWN | Unable to assess                                 |

### 3. Golden Project Coverage

Answers: "Which golden projects exercise the changed files?"

Lists the golden projects configured for the cycle. This is informational only.

### 4. Change Novelty

Answers: "Is this proposal similar to previously indexed changes?"

| Level   | Meaning                                          |
|---------|--------------------------------------------------|
| HIGH    | Novel - no similar changes indexed before        |
| MEDIUM  | Limited precedent (1-2 similar changes)          |
| LOW     | Well-established pattern (3+ similar changes)    |
| UNKNOWN | Unable to assess                                 |

## Allowed Memory Query Intents

The scorecard uses **only** these allowed intents when querying indexed memory:

- `PATTERN_LOOKUP` - For similarity scoring
- `RISK_ASSESSMENT` - For risk signal assessment
- `PRECEDENT_SUMMARY` - For novelty assessment
- `CONVERGENCE_SIGNAL` - For trend analysis (if needed)

All queries pass through the existing memory guardrails (Phase 5.D).

## Output Format

### Structured (for tooling)

```yaml
proposal_scorecard:
  proposal_title: "Fix S3 concurrency settings"
  generated_at: "2024-01-15T10:30:00.000000"
  similarity:
    level: HIGH
    evidence: "This proposal resembles 3 previously approved improvements"
    source_cycle_ids: ["cycle-abc", "cycle-def", "cycle-ghi"]
  risk:
    level: LOW
    evidence: "No regressions found in memory for these files"
    source_cycle_ids: []
  golden_coverage:
    projects: ["smoke_test", "test_odibi_local"]
  novelty:
    level: MEDIUM
    evidence: "First time modifying S3 concurrency settings"
    source_cycle_ids: []
```

### Human-Readable (for reports)

```markdown
### Proposal Scorecard (Advisory)

_This scorecard provides context only. It does not approve, reject, or influence execution._

**Similarity:** [HIGH] This proposal resembles 3 previously approved improvements (from cycles: cycle-abc, cycle-def, cycle-ghi)
**Historical Risk:** [LOW] No regressions found in memory for these files
**Golden Coverage:** Changes are exercised by: smoke_test, test_odibi_local
**Novelty:** [MEDIUM] First time modifying S3 concurrency settings
```

## Integration Points

### Cycle Report

The scorecard is attached to cycle reports when an `ImprovementProposal` exists:

```python
from agents.core.proposal_scorecard import ScorecardGenerator
from agents.core.reports import CycleReportGenerator

# Generate scorecard
generator = ScorecardGenerator(vector_store=my_store, embedder=my_embedder)
scorecard = generator.generate(proposal, golden_project_names=["smoke_test"])

# Attach to report
report_generator = CycleReportGenerator(odibi_root)
report_generator.set_scorecard(scorecard)
report_content = report_generator.generate_report(cycle_state)
```

### Review Context (Read-Only)

The scorecard can be included in review context metadata:

```python
review_context = {
    "proposal": proposal.to_dict(),
    "scorecard": scorecard.to_dict(),  # Read-only advisory data
}
```

**Important:** The scorecard in review context is for human reference only. It does not affect ReviewerAgent logic.

## Usage Example

```python
from agents.core.proposal_scorecard import ScorecardGenerator, ProposalScorecard
from agents.core.schemas import ImprovementProposal

# Create generator (with or without memory)
generator = ScorecardGenerator(
    vector_store=cycle_index_store,  # Optional
    embedder=embedder,               # Optional
)

# Generate scorecard for a proposal
proposal = ImprovementProposal(
    proposal="FIX",
    title="Fix null handling",
    rationale="Prevents pipeline failures",
    changes=[...],
    impact=ProposalImpact(risk="LOW", expected_benefit="..."),
)

scorecard = generator.generate(
    proposal,
    golden_project_names=["smoke_test", "test_odibi_local"],
)

# Use structured output
data = scorecard.to_dict()

# Or human-readable summary
print(scorecard.to_human_readable())
```

## Constraints

1. **No Autonomy Increase** - This feature does not grant agents any new capabilities
2. **No Execution Changes** - Review gating, regression checks, and approval rules are unchanged
3. **Memory Guardrails Intact** - All queries go through Phase 5.D guardrails
4. **Read-Only Integration** - Scorecard data is attached as context, not used for decisions

## Testing

Run tests with:

```bash
pytest agents/core/tests/test_proposal_scorecard.py -v
```

Test coverage includes:
- Valid scoring with memory data
- Empty memory handling
- Conflicting signals (high similarity + high risk)
- Query intent compliance
- Advisory-only verification (no decision methods)
