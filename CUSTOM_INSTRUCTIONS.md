# Odibi — Custom Instructions for AI Agents

> **Load this file at the start of every conversation.** It is the system prompt for any AI agent working on odibi.

---

## Identity

You are working on **odibi**, a Python data pipeline framework for building enterprise data warehouses. It orchestrates nodes (read → transform → validate → write) with dependency resolution, supports Pandas/Spark/Polars engines, and provides 6 warehouse patterns, 54 transformers, and 11 validation test types.

**Creator:** Solo data engineer building tools that buy back time and freedom.

**Coverage:** 80% (34,363 stmts). Do not regress.

---

## Databricks Environment

### Clone Path
```
ODIBI_CLONE_PATH = "<REPLACE_WITH_YOUR_DATABRICKS_CLONE_PATH>"
# Example: /Workspace/Users/your.email@company.com/odibi
# Example: /Volumes/catalog/schema/repos/odibi
```

Set this once per workspace. All notebooks use:
```python
import sys
ODIBI_ROOT = "<REPLACE_WITH_YOUR_DATABRICKS_CLONE_PATH>"
sys.path.insert(0, ODIBI_ROOT)
```

### ⚠️ %run Is Banned — Use lib/ Pattern Instead

**%run hangs and gets stuck.** For new self-contained projects (e2e tests, validation campaigns, profilers), use the `validation_e2e` lib/ pattern:

```
my_project/
├── lib/
│   ├── __init__.py          # Empty
│   ├── setup.py             # Auto-installs deps, exports public API
│   └── config.py            # Paths, connection strings, project config
├── 01_run_tests.py          # Notebook
├── 02_analyze_results.py    # Notebook
└── README.md
```

**Every notebook starts with:**
```python
import sys, os
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit("/", 1)[0]
sys.path.insert(0, PROJECT_ROOT)
from lib.setup import *  # Installs deps, exports API
```

**lib/setup.py pattern:**
```python
"""Project setup — auto-installs deps and exports API."""
import subprocess, sys

def _install(pkg):
    subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "-q"])

# Install what you need
try:
    import polars
except ImportError:
    _install("polars")
    import polars

# Re-export everything notebooks need
from lib.config import *
```

**No %run. No cross-notebook imports. Each notebook is self-contained via lib/.**

---

## PRE-CODE GATE (Mandatory)

Before writing or modifying **any** file, complete this checklist:

### 1. Read Context
- [ ] Read `docs/THREAD_QUICK_START.md` — source layout, environment facts, test rules
- [ ] Read the relevant section of `AGENTS.md` — gotchas for the modules you'll touch
- [ ] Read `docs/skills/README.md` — identify which skills apply to your task
- [ ] Read `docs/LESSONS_LEARNED.md` — check for prior decisions and traps

### 2. Think → Plan → Critique (Skill 01)
- [ ] **Think:** What exactly is being asked? What don't I know? What already exists?
- [ ] **Plan:** Numbered steps with exact file paths and commands
- [ ] **Critique:** What could go wrong? Am I over-engineering? What did I miss?
- [ ] **Verdict:** PROCEED / REVISE / CLARIFY WITH USER

### 3. Odibi-First Check (Skill 02)
- [ ] Checked `docs/skills/02_odibi_first_lookup.md` for existing implementations
- [ ] Confirmed I'm not reinventing a registered transformer, pattern, or utility

### 4. Scope Check
- [ ] Change is ≤250 LOC (hard max 500)
- [ ] Single concern — not mixing bug fix with feature
- [ ] Tests are part of the plan

**Do not proceed until all boxes are checked.**

---

## Skill Router (Mandatory)

**Do not guess which skills to load. Follow this router.**

### Step 1 — Always Load (every task, no exceptions)
```
→ 01 Think/Plan/Critique     docs/skills/01_think_plan_critique.md
→ 02 Odibi-First Lookup      docs/skills/02_odibi_first_lookup.md
```

### Step 2 — Match Your Task Type (load ALL that match)

```
What are you doing?
│
├─ Writing or modifying SOURCE CODE?
│  → 14 Code Standards              docs/skills/14_code_standards.md
│  │
│  ├─ Adding a transformer?
│  │  → 03 Write a Transformer      docs/skills/03_write_a_transformer.md
│  │  → 15 Engine Parity Standards   docs/skills/15_engine_parity_standards.md
│  │
│  ├─ Adding a pattern?
│  │  → 04 Write a Pattern           docs/skills/04_write_a_pattern.md
│  │  → 15 Engine Parity Standards   docs/skills/15_engine_parity_standards.md
│  │
│  ├─ Adding a connection?
│  │  → 06 Add a Connection          docs/skills/06_add_a_connection.md
│  │
│  ├─ Adding validation logic?
│  │  → 08 Validation Workflow       docs/skills/08_validation_workflow.md
│  │
│  └─ Touching engine-specific code (Pandas/Spark/Polars branches)?
│     → 15 Engine Parity Standards   docs/skills/15_engine_parity_standards.md
│
├─ Writing or modifying TESTS?
│  → 13 Testing Standards            docs/skills/13_testing_standards.md
│  → 07 Testing (gotchas)            docs/skills/07_testing.md
│  │
│  └─ Tests touch engine-specific code?
│     → 15 Engine Parity Standards   docs/skills/15_engine_parity_standards.md
│
├─ Writing or generating YAML configs?
│  → 05 Pipeline YAML Authoring      docs/skills/05_pipeline_yaml_authoring.md
│
├─ Working on DATABRICKS notebooks?
│  → 12 Databricks Notebook Protocol docs/skills/12_databricks_notebook_protocol.md
│
├─ Planning a multi-phase project?
│  → 10 Task Document Template       docs/skills/10_task_document_template.md
│
└─ Preparing a PR for review?
   → 09 Code Review Standards        docs/skills/09_code_review_standards.md
```

### Step 3 — Always Close With
```
→ 11 Lessons Learned Protocol   docs/skills/11_lessons_learned_protocol.md
```

### Example Loadouts

| Task | Skills Loaded |
|------|---------------|
| Add `row_number` transformer | 01, 02, **14**, **03**, **15**, **13**, 07 |
| Fix SCD2 float/NaN bug | 01, 02, **14**, **15**, **13**, 07 |
| Write validation tutorial | 01, 02, **05**, **08** |
| Add Polars branch to merge | 01, 02, **14**, **15**, **13**, 07 |
| Test patterns on Databricks | 01, 02, **12**, **13**, 07, **15** |
| Write unit tests only | 01, 02, **13**, 07 |
| Update YAML docs | 01, 02, **05** |
| Coverage push (tests only) | 01, 02, **13**, 07 |

---

## Critical Rules (Never Violate)

### Code Conventions
1. **Standalone functions** — no classes with mutable state (except Pattern subclasses)
2. **Return, don't write** — patterns and transformers return DataFrames, never persist
3. **Pydantic for config** — all YAML-facing models use Pydantic in `config.py`
4. **`get_logging_context()`** — never `print()` or bare `logging.getLogger()` in transforms/patterns
5. **Type hints** on all public functions
6. **Engine parity** — if Pandas has it, plan the Spark/Polars path

### YAML Rules
7. **Correct fields:** `read:`, `write:`, `transform:`, `query:` — NEVER `source:`, `sink:`, `sql:`
8. **Node names:** alphanumeric + underscore only (`^[a-zA-Z0-9_]+$`)
9. **Write mode keys:** `upsert`/`append_once` require `options: {keys: [...]}`, `merge` requires `merge_keys: [...]`

### Testing Rules
10. **No `pytest --cov`** — use `python -m coverage run` instead (Rich logging breaks)
11. **No caplog** — assert on return values, not log output
12. **No "spark" or "delta" in test file names** — conftest.py skips them
13. **Run tests in batches** — full suite hangs (ThreadPoolExecutor + coverage deadlock)
14. **Mock PySpark order:** import odibi FIRST, then install mocks in sys.modules

### Constraints
15. **pyarrow:** `<17.0.0,>=14.0.0`
16. **PR size:** ≤250 LOC (hard max 500)
17. **Coverage:** 80% baseline — do not regress

---

## Correctness Verification Protocol (Mandatory)

**This protocol prevents the "batch-read-and-confirm" failure mode** where an agent reads test files, confirms they parse, but never checks whether the tests actually validate correct behavior.

### For Every Test You Write:
1. **State the expected behavior** before writing the assertion
2. **Assert on concrete values** — not just `is not None` or `len(result) > 0`
3. **Include at least one negative test** — verify that bad input produces the expected error
4. **Check edge cases explicitly:** empty DataFrames, single-row DataFrames, null-only columns, Unicode strings, duplicate keys
5. **Run the test and read the output** — do not assume passing

### For Every Feature You Build:
1. **Run the code** — do not just read it and say it looks correct
2. **Verify with a concrete example** — create a small DataFrame, run the function, print/assert the result
3. **Test the boundary** — what happens at exactly the limit? (e.g., exactly 1 row, exactly 0 rows)
4. **Test the failure path** — what happens with bad input?

### Anti-Patterns (What This Prevents):
| Anti-Pattern | What Happens | This Protocol's Fix |
|-------------|--------------|---------------------|
| "Looks correct to me" | Agent reads code, doesn't run it | Mandate: run and verify |
| `assert result is not None` | Passes even when result is garbage | Require concrete value assertions |
| No negative tests | Code that silently swallows errors | Require at least one error case |
| Batch-reading test files | "All 50 test files read successfully" = useless | Each test must verify specific behavior |
| Skipping edge cases | Works on happy path, explodes on empty DF | Require edge case assertions |

### Verification Checkpoint Template:
After implementing any feature or writing tests, produce this summary:

```
## Verification Report
- Tests written: N
- Tests passing: N/N
- Concrete value assertions: [list 2-3 key assertions]
- Negative tests: [what error cases were tested]
- Edge cases tested: [empty df, nulls, single row, etc.]
- Command run: `pytest tests/unit/path/test_file.py -v`
- Uncovered paths: [what's left]
```

---

## Automatic Workflows — No Prompt Needed

Execute these workflows automatically at the right time. Do NOT wait for instructions.

### Session Start (every session)
```
1. Read this file (CUSTOM_INSTRUCTIONS.md)
2. Read docs/LESSONS_LEARNED.md — check decisions + traps for your modules
3. Read docs/THREAD_QUICK_START.md — source layout, line counts
4. Read AGENTS.md section for the modules you'll touch
5. Run Skill Router (above) → load matched skills
6. Execute PRE-CODE GATE
```

### Before Non-Trivial Code (any source or test changes)
```
1. Skill 01 — Think → Plan → Critique → Verdict
2. Skill 02 — Odibi-First Lookup (is this already built?)
3. Skill 14 — Code Standards (if writing source)
   OR Skill 13 — Testing Standards (if writing tests)
4. Skill 15 — Engine Parity Standards (if touching DataFrame logic)
5. ONLY after all checks pass → write code
```

### After Any Code Change
```
1. Run the code — do NOT just read it and say it looks correct
2. Run tests — `pytest tests/unit/<path>/test_file.py -v`
3. Verify with concrete values (Correctness Verification Protocol)
4. Check coverage if tests were added:
   python -m coverage run --source=odibi.<module> -m pytest <test_file> -q --tb=no
   python -m coverage report --show-missing
```

### Before Delivering / Finishing
```
1. Skill 09 — Code Review Standards (self-review)
2. Linting — `ruff check . && ruff format .`
3. Produce Verification Report (see Correctness Verification Protocol)
4. Confirm PR size ≤250 LOC (max 500)
```

### Session End (every session)
```
1. Skill 11 — Lessons Learned Protocol
2. Update docs/LESSONS_LEARNED.md with any new D/T/P/V entries
3. Update AGENTS.md with coverage numbers if tests were added
4. Produce final Verification Report
```

---

## Workflow Loops (triggered by task type)

When a task matches one of these triggers, execute ALL skills in the chain in order. Stop only if a gate fails (Think/Plan/Critique says REVISE or CLARIFY).

| Trigger | Skill Chain | Description |
|---------|-------------|-------------|
| **"add transformer"** | 01 → 02 → 14 → 03 → 15 → 13 → 07 → 09 | Full transformer lifecycle |
| **"add pattern"** | 01 → 02 → 14 → 04 → 15 → 13 → 07 → 09 | Full pattern lifecycle |
| **"add connection"** | 01 → 02 → 14 → 06 → 13 → 07 → 09 | Full connection lifecycle |
| **"write tests" / "coverage"** | 01 → 02 → 13 → 07 → 15 | Test-only workflow |
| **"fix bug"** | 01 → 02 → 14 → 15 → 13 → 07 → 09 | Bug fix with tests |
| **"add Polars branch"** | 01 → 02 → 14 → 15 → 13 → 07 → 09 | Engine parity gap fill |
| **"validation pipeline"** | 01 → 02 → 05 → 08 → 13 → 07 | Validation setup |
| **"Databricks test"** | 01 → 02 → 12 → 13 → 07 → 15 | Databricks e2e |
| **"build star schema"** | 01 → 02 → 05 → 04 → 15 → 08 → 13 | Full dimensional model |
| **"plan project"** | 01 → 10 → 02 | Multi-phase planning |
| **"YAML config"** | 01 → 02 → 05 | Config authoring |
| **"debug and fix"** | 01 → 02 → 07 → 13 → 14 → 09 | Investigate + fix + test |
| **"full loop"** | 01 → 02 → 14 → 03/04 → 15 → 05 → 13 → 07 → 08 → 09 | Complete feature end-to-end |

### How Loops Work
1. Start at the first skill in the chain
2. Execute each skill's protocol (read, apply, produce output)
3. If Skill 01 (Think/Plan/Critique) produces REVISE → loop back to Skill 01
4. If Skill 01 produces CLARIFY → stop and ask the user
5. Otherwise, proceed through all skills without waiting for prompts
6. End with Skill 11 (Lessons Learned) at session close

---

## OUTPUT CONTRACT

Every response that involves code changes must include:

1. **THINK** — goal + knowns + unknowns (from Skill 01)
2. **PLAN** — numbered steps with file paths (from Skill 01)
3. **CRITIQUE** — what could go wrong (from Skill 01)
4. **CODE** — the implementation
5. **VERIFY** — test command + results + Verification Report
6. **LESSONS** — any gotchas discovered (for docs/LESSONS_LEARNED.md)

For trivial tasks (docs-only, <20 LOC), compress to: PLAN → CODE → VERIFY.

---

## Quick Links

| Doc | Purpose |
|-----|---------|
| `CUSTOM_INSTRUCTIONS.md` | This file — system prompt (load first) |
| `AGENTS.md` | Test gotchas, coverage tracker, mock patterns |
| `docs/THREAD_QUICK_START.md` | Source layout with line counts |
| `docs/ODIBI_DEEP_CONTEXT.md` | Complete framework reference (2,200 lines) |
| `docs/LESSONS_LEARNED.md` | Structured memory — decisions, traps, patterns |
| `docs/AGENT_CAMPAIGN.md` | Hardening campaign — tasks and prompts for agents |
| `docs/skills/` | 15 skill documents for specific task types |
| `odibi/config.py` | Pydantic models — source of truth for YAML |
| `llms.txt` | Lightweight AI context summary |
