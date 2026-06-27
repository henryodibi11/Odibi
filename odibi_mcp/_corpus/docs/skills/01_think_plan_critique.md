# Skill 01 — Think → Plan → Critique

> **Layer:** Reasoning
> **When:** Before every task. Mandatory pre-code gate.
> **Time:** 2-5 minutes of reasoning before any file edits.

---

## Purpose

This skill prevents wasted effort and rework by forcing structured reasoning before writing code. It is mandatory for all agents working on odibi.

**Rule: No file edits until all three phases are complete.**

---

## Phase 1 — Think (Analyze Unknowns)

Before touching any code, answer these questions explicitly:

### 1.1 What exactly is being asked?
- Restate the task in your own words
- Identify the acceptance criteria — what does "done" look like?
- Flag any ambiguity and resolve it before proceeding

### 1.2 What do I not know?
- Which odibi modules are involved?
- Have I read the relevant source files?
- Are there existing tests I need to understand?
- Does this touch multiple engines (Pandas/Spark/Polars)?

### 1.3 What already exists?
- **Invoke Skill 02 (Odibi-First Lookup)** — check if odibi already has what you need
- Search `FunctionRegistry` for existing transformers
- Check `odibi/patterns/` for existing patterns
- Check `odibi/validation/` for existing quality checks
- Search `tests/unit/` for existing test patterns

### 1.4 What are the constraints?
- pyarrow dependency: `pyarrow<17.0.0,>=14.0.0`
- Engine parity: changes to one engine must be reflected in others
- No Pydantic in patterns/validation (use plain dicts for config)
- Pattern functions return DataFrames — they never write directly
- Standalone functions preferred — no classes with mutable state
- `logging.getLogger(__name__)` — no custom loggers
- Type hints on all public functions

---

## Phase 2 — Plan (Outline Steps)

Write a numbered plan with concrete actions:

### Template
```
Plan:
1. Read [file] to understand [what]
2. Read [file] to understand [what]
3. Create/modify [file] — [specific change]
4. Create/modify [file] — [specific change]
5. Write tests in [file] — [what they cover]
6. Run tests: [exact command]
7. Verify: [how to confirm success]
```

### Planning Rules
- **Maximum scope:** PRs under 250 lines of code (hard max 500)
- **One concern per change:** Don't mix bug fixes with features
- **Tests are part of the plan:** Not an afterthought
- **Name files first:** Decide exact file paths before writing code
- **Check test naming:** No "spark" or "delta" in test file names (conftest.py skips them)

### Dependency Check
Before planning changes, verify:
- [ ] Will this break existing tests?
- [ ] Does this require changes in multiple engines?
- [ ] Does this touch `config.py` Pydantic models? (high risk — many dependents)
- [ ] Does this need a new dependency? (check `pyproject.toml` first)

---

## Phase 3 — Critique (Identify Risks)

Challenge your own plan before executing:

### 3.1 What could go wrong?
- Import cycles (odibi has complex cross-module imports)
- Logging context pollution (the `get_logging_context()` singleton contaminates test ordering)
- Mock PySpark setup order (must import odibi first, then mock pyspark)
- Delta Lake Null type columns (always provide non-None values in test data)

### 3.2 What am I over-engineering?
- Am I adding error handling for scenarios that can't happen?
- Am I creating abstractions for one-time operations?
- Am I adding features beyond what was asked?
- Am I refactoring surrounding code that doesn't need it?

### 3.3 What did I miss?
- Did I check Skill 02 for existing implementations?
- Did I account for all three engines?
- Did I check `AGENTS.md` for known gotchas in the modules I'm touching?
- Will my changes maintain the 80% coverage baseline?

### 3.4 Escape hatch
If critique reveals the plan is wrong:
- **Revise the plan** — don't proceed with a flawed approach
- **Ask the user** — if requirements are ambiguous, clarify before coding
- **Reduce scope** — break large tasks into smaller PRs

---

## Output Format

Before writing any code, produce this summary (in your reasoning or as a message):

```
## Think/Plan/Critique Summary

**Task:** [one-line description]
**Modules involved:** [list of odibi modules]
**Existing code to reuse:** [what Skill 02 found]

**Plan:**
1. [step]
2. [step]
...

**Risks:**
- [risk 1]
- [risk 2]

**Verdict:** PROCEED / REVISE / CLARIFY WITH USER
```

---

## Anti-Patterns (What This Skill Prevents)

| Anti-Pattern | What Happens | This Skill's Fix |
|-------------|--------------|-----------------|
| Dive-in coding | Agent starts editing before understanding the codebase | Phase 1 forces reading first |
| Reinventing the wheel | Agent writes a custom dedup function when `deduplicate` exists | Phase 1.3 invokes Skill 02 |
| Scope creep | Agent "improves" surrounding code while fixing a bug | Phase 3.2 challenges over-engineering |
| Breaking tests | Agent doesn't know about mock ordering or caplog issues | Phase 3.1 checks known gotchas |
| Wrong file structure | Agent puts test in wrong directory or uses forbidden names | Phase 2 planning rules |
