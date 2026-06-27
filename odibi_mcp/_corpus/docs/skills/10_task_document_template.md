# Skill 10 — Task Document Template

> **Layer:** Meta
> **When:** Planning a multi-phase feature or project to hand to an AI agent.
> **Origin:** This format produced 148 tests on 234 files with 99% success (document_profiler) and 931 files with 99% success (validation_e2e).

---

## Purpose

When you give an AI agent a vague instruction, you get vague output. When you give it a structured task document with phases, rules, and success criteria, you get production-quality code. This skill defines the format.

---

## Task Document Structure

```markdown
# Agent Task: [Task Name]

## Goal
[One paragraph. What does success look like? Be specific.]

## Datasets / Files
[Exact paths, table names, or test data the agent should use.]

- Input: `path/to/input/data/`
- Output: `path/to/output/`
- Test data: `tests/fixtures/my_feature/`

## Architecture

### Components
[List the files/modules/functions to create or modify.]

| File | Purpose |
|------|---------|
| `odibi/module/feature.py` | Core logic |
| `odibi/module/models.py` | Data models (if needed) |
| `tests/unit/module/test_feature.py` | Tests |

### Data Flow
[How data moves through the components.]

```
Input → Parse → Transform → Validate → Output
```

### Key Decisions
[Explain non-obvious design choices upfront.]

- Use DuckDB SQL for Pandas, not raw pandas operations (engine parity)
- Plain dataclasses for internal models, Pydantic for config validation
- Return DataFrames — never write inside the function

## Rules for Agent

### Mandatory
1. **Invoke Skill 01** (Think → Plan → Critique) before writing code
2. **Invoke Skill 02** (Odibi-First Lookup) to check for existing implementations
3. **Use odibi conventions:** `get_logging_context()`, type hints, Pydantic params
4. **Write tests alongside code** — not as an afterthought
5. **Log every gotcha** to the Lessons Learned section of AGENTS.md (Skill 11)

### Constraints
- Library constraints: [e.g., "Use python-docx only, no LLM dependency"]
- Max PR size: 250 LOC per PR (500 absolute max)
- No new dependencies unless explicitly approved
- pyarrow constraint: `pyarrow<17.0.0,>=14.0.0`

### Anti-Patterns to Avoid
- Don't add classes with mutable state — use standalone functions
- Don't skip engine parity — if Pandas has it, plan the Spark path
- Don't use `print()` or bare `logging` — use `get_logging_context()`
- Don't write output directly — return DataFrames

## Phases

### Phase 1: Foundation
**Deliverable:** [What exists at end of this phase]
**Files:**
- `odibi/module/models.py` — data models
- `tests/unit/module/test_models.py` — model tests

**Success criteria:**
- [ ] All models serialize/deserialize correctly
- [ ] Tests pass: `pytest tests/unit/module/test_models.py -v`
- [ ] 100% coverage on models file

### Phase 2: Core Logic
**Deliverable:** [What exists at end of this phase]
**Files:**
- `odibi/module/feature.py` — core implementation
- `tests/unit/module/test_feature.py` — core tests

**Success criteria:**
- [ ] Function handles happy path, edge cases, error cases
- [ ] Works with Pandas engine
- [ ] Tests pass: `pytest tests/unit/module/ -v`
- [ ] ≥80% coverage

### Phase 3: Integration
**Deliverable:** [What exists at end of this phase]
**Files:**
- Registration in `__init__.py` (if transformer/pattern)
- YAML config support in `config.py` (if needed)

**Success criteria:**
- [ ] Feature accessible via YAML config
- [ ] `odibi list [type]` shows the new feature
- [ ] End-to-end test with sample YAML

### Phase 4: Polish
**Deliverable:** Production-ready feature
**Tasks:**
- [ ] Docstrings complete (Google style)
- [ ] Error messages are actionable
- [ ] Linting passes: `ruff check . && ruff format .`
- [ ] AGENTS.md updated with gotchas (Skill 11)
- [ ] Coverage target met

## Success Criteria (Overall)

| Criterion | Target |
|-----------|--------|
| Tests passing | 100% |
| Coverage | ≥80% on new code |
| Linting | Zero ruff errors |
| Engine parity | Pandas + Spark paths |
| AGENTS.md | Updated with lessons learned |
| PR size | ≤250 LOC per PR |
```

---

## Minimal Task Document (For Small Tasks)

For tasks under 100 LOC, use this shorter format:

```markdown
# Task: [Name]

## Goal
[One sentence.]

## Files to Change
- `path/to/file.py` — [what to change]

## Rules
- Use odibi conventions (Skill 02)
- Write tests
- Max 250 LOC

## Success Criteria
- [ ] Tests pass
- [ ] Linting passes
- [ ] Coverage maintained
```

---

## Tips for Effective Task Documents

1. **Be specific about file paths** — "create odibi/transformers/window_rank.py" not "add a window rank feature"
2. **Include test data** — agents produce better tests when they have concrete examples
3. **State constraints upfront** — "no new dependencies" prevents the agent from installing random packages
4. **Define phases** — agents handle 100-line phases better than 500-line monoliths
5. **Include anti-patterns** — telling the agent what NOT to do is as valuable as telling it what to do
6. **Specify success criteria** — "tests pass" is vague; "pytest returns 0 with ≥80% coverage on the new module" is precise
