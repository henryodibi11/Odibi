# Skill 09 — Code Review Standards

> **Layer:** Quality
> **When:** Preparing code for review or PR submission.
> **Source:** Enterprise Analytics Code Review Standards.

---

## Purpose

These standards ensure every PR is small, focused, and reviewable. They apply to all agents and humans working on odibi.

---

## PR Size Limits

| Threshold | Action |
|-----------|--------|
| ≤250 LOC | ✅ Ideal — easy to review |
| 251-500 LOC | ⚠️ Acceptable — must be a single concern |
| >500 LOC | ❌ Split required — break into smaller PRs |

LOC = Lines of Code changed (additions + deletions), excluding:
- Auto-generated files (JSON schema, docs)
- Test data files
- Lock files

---

## Branch Naming

```
type/contributor/description
```

| Part | Convention | Examples |
|------|-----------|----------|
| `type` | `feat`, `fix`, `test`, `docs`, `refactor`, `chore` | |
| `contributor` | lowercase name or initials | `hodibi`, `ho` |
| `description` | lowercase, hyphen-separated | `add-pivot-transformer`, `fix-scd2-null-handling` |

**Examples:**
```
feat/hodibi/add-pivot-transformer
fix/hodibi/scd2-null-handling
test/hodibi/validation-engine-coverage
docs/hodibi/transformer-authoring-skill
refactor/hodibi/simplify-pattern-base
```

---

## Commit Messages

### Format
```
<type>: <imperative mood title>

<optional body — what and why, not how>
```

### Rules
- **Imperative mood:** "Add feature" not "Added feature" or "Adds feature"
- **Title ≤72 characters**
- **No period at end of title**
- **Body wraps at 72 characters**

### Examples
```
feat: add pivot transformer with Pandas and Spark support

test: cover validation engine Polars freshness path

fix: handle null flag_col in SCD2 Pandas changed records

docs: add transformer authoring skill for agent system

refactor: extract common audit column logic to Pattern base
```

### Type Prefixes
| Type | Use When |
|------|----------|
| `feat` | New feature or capability |
| `fix` | Bug fix |
| `test` | Adding or updating tests (no production code change) |
| `docs` | Documentation only |
| `refactor` | Code restructure with no behavior change |
| `chore` | Build, CI, deps, tooling |
| `perf` | Performance improvement |

---

## PR Title and Description

### Title
Same format as commit message: `<type>: <imperative mood description>`

### Description Template
```markdown
## What
[One paragraph: what this PR does]

## Why
[One paragraph: why this change is needed]

## How
[Bullet points: key implementation decisions]

## Testing
[How this was tested — commands, coverage numbers]

## Checklist
- [ ] Tests pass: `pytest tests/unit/relevant_tests.py -v`
- [ ] Linting passes: `ruff check .`
- [ ] No new test file names contain "spark" or "delta"
- [ ] Coverage maintained or improved
- [ ] AGENTS.md updated (if gotchas discovered)
```

---

## Layered Decomposition

When a change spans multiple layers, split into separate PRs:

### Layer Order (bottom-up)
1. **Core/config** — Pydantic models, enums, base classes
2. **Engine** — Engine-specific implementations
3. **Pattern/Transformer** — Business logic
4. **CLI** — Command-line interface
5. **Tests** — Can accompany any layer
6. **Docs** — Can accompany any layer

### Example: Adding a New Transformer

Instead of one PR with 600 lines:
```
PR 1 (150 LOC): feat: add Pydantic params model for window_rank transformer
PR 2 (200 LOC): feat: implement window_rank transformer with engine parity
PR 3 (150 LOC): test: add window_rank transformer coverage
PR 4 (50 LOC):  docs: add window_rank to transformer skill lookup table
```

---

## Code Style Rules

### Imports
```python
# Standard library
import time
from typing import Any, Dict, List, Optional

# Third-party
import pandas as pd
from pydantic import BaseModel, Field

# odibi
from odibi.context import EngineContext
from odibi.utils.logging_context import get_logging_context
```

### Logging
```python
# ✅ Correct
from odibi.utils.logging_context import get_logging_context
ctx = get_logging_context()
ctx.debug("MyTransform starting", column=col)

# ❌ Wrong — don't use standard logging in transformers/patterns
import logging
logger = logging.getLogger(__name__)
logger.info("starting")
```

**Exception:** `quarantine.py` and `gate.py` use `logging.getLogger(__name__)` — this is the older convention. New code in `validation/` may follow either pattern but prefer `get_logging_context()`.

### Type Hints
```python
# ✅ All public functions must have type hints
def filter_rows(context: EngineContext, params: FilterRowsParams) -> EngineContext:

# ✅ Include return type
def _get_row_count(self, df: Any, engine_type: EngineType) -> Optional[int]:

# ❌ Missing type hints
def filter_rows(context, params):
```

### Docstrings
```python
def my_function(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """One-line summary in imperative mood.

    Longer description if needed, explaining design decisions
    or non-obvious behavior.

    Args:
        df: Input DataFrame
        key: Column name to use as key

    Returns:
        DataFrame with transformation applied

    Raises:
        ValueError: If key column not found in DataFrame
    """
```

---

## Review Checklist (For Reviewers)

### Correctness
- [ ] Does the code do what it claims?
- [ ] Are edge cases handled (empty DataFrame, null columns, single row)?
- [ ] Does it work on all required engines (Pandas/Spark/Polars)?

### Conventions
- [ ] Uses `get_logging_context()` for structured logging?
- [ ] Pydantic params model for transformers?
- [ ] Type hints on all public functions?
- [ ] Imports follow standard/third-party/odibi order?

### Safety
- [ ] No secrets or keys in code?
- [ ] No `print()` statements?
- [ ] No `import *`?
- [ ] No mutable default arguments?

### Testing
- [ ] Tests cover happy path + edge cases?
- [ ] No caplog usage?
- [ ] No forbidden test file names?
- [ ] Coverage maintained or improved?

### Scope
- [ ] PR is ≤500 LOC?
- [ ] Single concern — not mixing bug fix with feature?
- [ ] No unnecessary refactoring of surrounding code?
