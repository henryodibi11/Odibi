# Odibi Skills — Agent Instruction Set

> **Purpose:** Teach AI agents (Amp, Cursor, Copilot, Cline, Databricks AI) how to work on the odibi codebase correctly and efficiently.
>
> **How to use:** When starting a task, read the skill(s) relevant to your work. Skills are numbered by layer — reasoning skills (01-02) apply to every task, building skills (03-06) apply when extending odibi, quality skills (07-09) apply when shipping code, and meta skills (10-11) govern how you plan and close sessions.

---

## Skill Index

| # | Skill | Layer | When to Use |
|---|-------|-------|-------------|
| 01 | [Think → Plan → Critique](01_think_plan_critique.md) | Reasoning | **Every task.** Mandatory pre-code gate. |
| 02 | [Odibi-First Lookup](02_odibi_first_lookup.md) | Reasoning | **Every task.** Check before writing new code. |
| 03 | [Write a Transformer](03_write_a_transformer.md) | Building | Adding a new transformer to the registry. |
| 04 | [Write a Pattern](04_write_a_pattern.md) | Building | Adding a new warehouse pattern. |
| 05 | [Pipeline YAML Authoring](05_pipeline_yaml_authoring.md) | Building | Writing or generating pipeline YAML configs. |
| 06 | [Add a Connection](06_add_a_connection.md) | Building | Adding a new connection type. |
| 07 | [Testing](07_testing.md) | Quality | Writing tests for any odibi module. |
| 08 | [Validation Workflow](08_validation_workflow.md) | Quality | Adding data quality checks to a pipeline. |
| 09 | [Code Review Standards](09_code_review_standards.md) | Quality | Preparing code for review or PR. |
| 10 | [Task Document Template](10_task_document_template.md) | Meta | Planning a multi-phase feature or project. |
| 11 | [Lessons Learned Protocol](11_lessons_learned_protocol.md) | Meta | Closing a session — update shared knowledge. |
| 12 | [Databricks Notebook Protocol](12_databricks_notebook_protocol.md) | Building | Creating notebooks or e2e tests on Databricks. |
| 13 | [Testing Standards](13_testing_standards.md) | Standards | Writing any test file. Skip guards, fixtures, assertions. |
| 14 | [Code Standards](14_code_standards.md) | Standards | Writing any source code. Signatures, logging, errors. |
| 15 | [Engine Parity Standards](15_engine_parity_standards.md) | Standards | Any code touching DataFrames. Dispatch, comparison, skip guards. |

---

## Skill Invocation Rules

1. **Skills 01 and 02 are mandatory for every task.** No exceptions. Think before you code. Check what already exists before you build.
2. **Building skills (03-06, 12)** — read the relevant skill before creating new framework components. These encode the exact conventions, base classes, and registration patterns.
3. **Quality skills (07-09)** — read skill 07 before writing any test. Read skill 09 before opening a PR.
4. **Meta skills (10-11)** — read skill 10 when planning a multi-session project. Execute skill 11 at the end of every session.
5. **Standards skills (13-15)** — read skill 13 before writing any test, skill 14 before writing source code, skill 15 when touching engine-specific logic. These are **prescriptive recipes** — follow them exactly to produce merge-ready code on first attempt.

## Quick Reference

- **Framework:** odibi — Python data pipeline framework for enterprise data warehouses
- **Engines:** Pandas (DuckDB SQL), Spark, Polars — must maintain parity
- **Config:** Pydantic models in `odibi/config.py` — source of truth
- **Transforms:** 54 registered in `FunctionRegistry` via `odibi/transformers/__init__.py`
- **Patterns:** 6 warehouse patterns in `odibi/patterns/`
- **Validation:** 11 test types in `odibi/validation/engine.py`
- **Connections:** 5 types in `odibi/connections/`
- **Tests:** `tests/unit/` — run in batches, never the full suite at once
- **Coverage:** 80% (34,363 stmts) — do not regress

## Relationship to Other Docs

| Doc | Purpose | Read When |
|-----|---------|-----------|
| `AGENTS.md` | Agent-specific instructions, test gotchas, coverage tracker | Every thread start |
| `docs/THREAD_QUICK_START.md` | Source layout with line counts, environment facts | First read of every thread |
| `docs/ODIBI_DEEP_CONTEXT.md` | Complete framework reference (2,200 lines) | Need feature-level details |
| `docs/skills/` (this folder) | How to do specific types of work correctly | When doing that type of work |
