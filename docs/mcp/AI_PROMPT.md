# Odibi MCP Implementation — AI Assistant Prompt

> **Copy this prompt at the start of each session with Continue/GPT 5.2**

---

## Session Start Prompt

```
You are helping implement the Odibi MCP Facade — a read-only AI interface for the Odibi data engineering framework.

## Key Files (Read These First)
- Specification: docs/mcp/SPEC.md (full v4.1 design)
- Implementation Plan: docs/mcp/IMPLEMENTATION_PLAN.md (atomic tasks)
- Progress Checklist: docs/mcp/CHECKLIST.md (track completion)

## Core Principles (MUST Follow)
1. READ-ONLY: MCP never mutates data, triggers runs, or alters state
2. SINGLE-PROJECT: Each request operates within one project context
3. DENY-BY-DEFAULT: Path discovery requires explicit allowlists
4. TYPED RESPONSES: Use Pydantic models, never Dict[str, Any]
5. PHYSICAL REFS GATED: 3 conditions must pass to include physical paths

## Existing Code Context
- odibi_mcp/ already exists with server.py and knowledge.py
- We are ADDING to it, not replacing
- Existing tools (list_transformers, validate_yaml, etc.) stay untouched
- New code goes in subfolders: contracts/, access/, tools/, etc.

## Coding Standards
- Use Pydantic v2 syntax (model_dump, not dict)
- Follow existing Odibi patterns (check odibi/config.py for examples)
- Add type hints to all functions
- No Dict[str, Any] in response models — use typed classes
- Run tests after each task

## Current Task
I am working on task [TASK_ID] from the implementation plan.
[PASTE TASK DETAILS HERE]

Please implement this task following the specification.
```

---

## Task-Specific Prompts

### Starting a New Phase

```
I'm starting Phase [N]: [PHASE_NAME].

Please read:
- docs/mcp/SPEC.md — Section: [RELEVANT_SECTION]
- docs/mcp/IMPLEMENTATION_PLAN.md — Phase [N] tasks

Let's begin with task [N]a.
```

### Implementing a Single Task

```
Implement task [TASK_ID] from docs/mcp/IMPLEMENTATION_PLAN.md.

Task: [TASK_DESCRIPTION]
File: [FILE_PATH]
Dependencies: [LIST_DEPENDENCIES]

Reference the spec at docs/mcp/SPEC.md for the exact model/contract definition.

After implementation, I will run:
[VERIFICATION_COMMAND]
```

### Fixing a Failed Test

```
Task [TASK_ID] failed verification.

Command run:
[VERIFICATION_COMMAND]

Error output:
[PASTE_ERROR]

Please fix the implementation.
```

### Reviewing Before Commit

```
I've completed Phase [N]. Before committing, please review:

1. All files in odibi_mcp/[FOLDER]/ follow the spec
2. All tests pass: pytest tests/unit/mcp/ -v
3. No type errors: python -c "from odibi_mcp import *"
4. Ruff is clean: ruff check odibi_mcp/

List any issues found.
```

---

## Quick Reference: File Locations

| Component | Location |
|-----------|----------|
| Enums | `odibi_mcp/contracts/enums.py` |
| Envelope models | `odibi_mcp/contracts/envelope.py` |
| Run selector | `odibi_mcp/contracts/selectors.py` |
| Resource refs | `odibi_mcp/contracts/resources.py` |
| Access context | `odibi_mcp/contracts/access.py` |
| Time window | `odibi_mcp/contracts/time.py` |
| Schema models | `odibi_mcp/contracts/schema.py` |
| Graph models | `odibi_mcp/contracts/graph.py` |
| Diff models | `odibi_mcp/contracts/diff.py` |
| Stats models | `odibi_mcp/contracts/stats.py` |
| Discovery models | `odibi_mcp/contracts/discovery.py` |
| Access enforcement | `odibi_mcp/access/` |
| Audit logging | `odibi_mcp/audit/` |
| Story loader | `odibi_mcp/loaders/story.py` |
| Discovery limits | `odibi_mcp/discovery/limits.py` |
| Tool implementations | `odibi_mcp/tools/` |
| Utilities | `odibi_mcp/utils/` |
| Unit tests | `tests/unit/mcp/` |
| Integration tests | `tests/integration/mcp/` |

---

## Quick Reference: Verification Commands

```bash
# Check imports work
python -c "from odibi_mcp.contracts.enums import TruncatedReason; print('OK')"

# Run specific test
pytest tests/unit/mcp/test_envelope.py -v

# Run all MCP tests
pytest tests/unit/mcp/ -v

# Check types
python -c "from odibi_mcp import *"

# Lint check
ruff check odibi_mcp/

# Format check
ruff format odibi_mcp/ --check
```

---

## Reminders

- [ ] Mark completed tasks in `CHECKLIST.md`
- [ ] Commit after each phase
- [ ] Run `ruff check .` before pushing
- [ ] Update `CHANGELOG.md` when done
