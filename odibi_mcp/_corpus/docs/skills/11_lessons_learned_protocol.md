# Skill 11 — Lessons Learned Protocol

> **Layer:** Meta
> **When:** At the end of every agent session, before finishing.
> **Rule:** If you discovered something that would save a future agent time or money, write it down.

---

## Purpose

Every lesson captured in AGENTS.md saves 10-30 minutes and real money on future sessions. The compounding effect across hundreds of sessions is enormous. This skill defines exactly what to capture and where.

---

## Mandatory End-of-Session Checklist

Before closing any session that modified code or tests, execute this checklist:

### 1. Did I discover a gotcha?
A gotcha is anything that:
- Took more than one attempt to figure out
- Would catch another agent by surprise
- Involves import order, mock setup, or environment quirks
- Caused a test to fail in a non-obvious way

**If yes → add to AGENTS.md "Testing Gotchas & Lessons Learned" section.**

### 2. Did I add or modify tests?
If you added tests, update the "Current Coverage Status" section of AGENTS.md:
- Module name and new coverage percentage
- Number of tests added
- What the tests cover (brief bullet points)
- Test file path

**Format:**
```markdown
- `module.py` — **XX% covered** (N tests). [What's covered]. Remaining: [what's not].
  - `tests/unit/path/test_file.py` (N tests) — [list of tested functions]
```

### 3. Did I find a working mock/test pattern?
If you figured out how to test something that was tricky:
- Add it to the "Testing Gotchas" section
- Include the working code pattern
- Explain what didn't work and why

### 4. Did I modify coverage numbers?
If overall coverage changed, update the Progress Tracker section.

### 5. Did I hit a naming constraint?
If a file name, function name, or config key caused issues, document it.

---

## Where to Write

| What | Where |
|------|-------|
| Design decisions (why X over Y) | `docs/LESSONS_LEARNED.md` → **Decisions Log** (D-NNN) |
| Pitfalls that look right but break | `docs/LESSONS_LEARNED.md` → **Traps** (T-NNN) |
| Working code recipes | `docs/LESSONS_LEARNED.md` → **Patterns** (P-NNN) |
| Empirical behavior findings | `docs/LESSONS_LEARNED.md` → **Discoveries** (V-NNN) |
| Coverage numbers and test counts | `AGENTS.md` → **Current Coverage Status** |
| Coverage roadmap progress | `AGENTS.md` → **Progress Tracker** |

**AGENTS.md** keeps coverage tracking. **docs/LESSONS_LEARNED.md** handles the why/how knowledge.

---

## Format for Gotcha Entries

Use this format — state the problem, the wrong approach, and the fix:

```markdown
### [Short Descriptive Title]
**Problem:** [What happened]
**Wrong approach:** [What didn't work]
**Fix:** [What works]
```

**Example:**
```markdown
### CatalogStateBackend: Null Type Columns
**Problem:** Delta Lake rejects `SchemaMismatchError: Invalid data type for Delta Lake: Null`
when environment column is None.
**Wrong approach:** Using `pd.DataFrame` with None values to seed Delta test data.
**Fix:** Always provide a non-None `environment` value (e.g., `environment="test"`).
Seed Delta test data using `pa.table()` with explicit types instead of `pd.DataFrame`.
```

---

## Format for Coverage Updates

```markdown
- `module.py` — **XX% covered** (N tests). Brief description of what's covered.
  Remaining: brief list of what's not covered.
  - `tests/unit/path/test_file.py` (N tests) — functions tested
```

---

## Rules

1. **Do NOT remove existing entries** — only append or update percentages
2. **Keep entries concise and actionable** — 3-5 lines max per gotcha
3. **Include code examples** when the fix involves specific syntax
4. **Update coverage percentages** even if the change is small
5. **Be honest about what's not covered** — "Remaining: Spark paths" is fine
6. **Date your major updates** — helps track knowledge evolution

---

## Why This Matters

| Without This Skill | With This Skill |
|--------------------|-----------------|
| Next agent hits the same mock ordering bug | Agent reads the gotcha and avoids it |
| Next agent wastes 30 min debugging caplog | Agent knows to use return-value assertions |
| Coverage numbers go stale | Numbers are always current |
| Knowledge is lost between sessions | Knowledge compounds across sessions |

**Each entry you write today saves $1-5 on every future session that touches that module.** Over hundreds of sessions, this compounds to thousands of dollars in avoided waste.

---

## Quick Template

Copy-paste this at the end of your session and fill in what applies:

```markdown
## Session Update — [Date]

### Gotchas Discovered
- [gotcha 1]
- [gotcha 2]

### Coverage Updates
- `module.py` — XX% → YY% (+N tests)

### New Test Patterns
- [pattern description with code example]

### Progress Tracker Update
- [x] Phase X: [description]
```
