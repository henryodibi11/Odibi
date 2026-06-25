# Odibi Architecture Decision & Pattern Codex

> **Purpose:** A clear record of the decisions, principles, tradeoffs, recurring patterns, and anti-patterns present in Odibi — grounded in the actual codebase, not assumptions.
>
> **Audience:** The creator (you), future-you, and any AI agent working on Odibi.
>
> **Last updated:** 2026-05-03
>
> **Source of truth:** This codex was derived by inspecting the full codebase, docs, examples, tests, and simulation patterns. Every claim cites specific files.

---

## Table of Contents

1. [What Odibi Is Optimized For](#1-what-odibi-is-optimized-for)
2. [What Odibi Is Not Optimized For](#2-what-odibi-is-not-optimized-for)
3. [Core Principles Discovered from the Repo](#3-core-principles-discovered-from-the-repo)
4. [Architecture Decisions](#4-architecture-decisions)
5. [Reusable Patterns](#5-reusable-patterns)
6. [Anti-Patterns Odibi Avoids](#6-anti-patterns-odibi-avoids)
7. [Tradeoffs and When to Revisit](#7-tradeoffs-and-when-to-revisit)
8. [Simulation & Learning Pattern Standard](#8-simulation--learning-pattern-standard)
9. [Knowledge System Architecture](#9-knowledge-system-architecture)

---

## 1. What Odibi Is Optimized For

These are not aspirational goals — they are demonstrated by the code.

| Optimized For | Evidence |
|---|---|
| **Solo data engineer building data warehouses** | `AGENTS.md`: "Solo data engineer on an analytics team (only DE among data analysts)" |
| **Declarative pipeline authoring (YAML)** | `config.py` (4,766 lines of Pydantic models); 38+ simulation pattern YAMLs in `examples/simulation_patterns/` |
| **Running the same pipeline on Pandas, Spark, or Polars** | 3 engine implementations in `odibi/engine/`, engine dispatch pattern in every transformer |
| **DWH patterns (SCD2, dims, facts, aggregation)** | 6 patterns in `odibi/patterns/` with full Pandas + Spark paths |
| **Data quality as a first-class concern** | `odibi/validation/` (engine, quarantine, gate, FK validation) — 11 test types × 3 engines |
| **Buying back time through automation** | CLI introspection (`odibi list`, `explain`, `templates`), template generation from Pydantic models, story generation |
| **Learning through simulation** | `odibi/simulation/generator.py` (1,769 lines), `odibi-simulations/` (54 configs, 73 notebooks, 20+ domains) |
| **Self-service for non-engineers** | Success test in `AGENTS.md`: "Can you hand the docs to a business analyst and have them build a working pipeline without your help?" |

---

## 2. What Odibi Is Not Optimized For

These are boundaries the codebase implicitly enforces — things Odibi does not attempt.

| Not Optimized For | Evidence |
|---|---|
| **Real-time / streaming pipelines** | Pipeline model is batch: read → transform → validate → write. No streaming primitives. |
| **Multi-tenant SaaS** | No user management, auth, or tenant isolation. Single-user framework. |
| **Horizontal scaling / distributed orchestration** | DAG runs in a single process (`pipeline.py`). Spark handles scale, not the orchestrator. |
| **ML model training / serving** | No ML abstractions. Focus is on data warehousing, not modeling. |
| **Complex event processing** | Simulation engine generates data but doesn't process events in real-time. |
| **UI-first interaction** | `odibi/ui/app.py` (157 lines) exists but is minimal. CLI and YAML are the primary interfaces. |
| **Plugin marketplace / community ecosystem** | Plugin system exists (`plugins.py`, entry points) but is unused in practice. |

---

## 3. Core Principles Discovered from the Repo

These principles are not documented in a single place — they emerge from reading the code, docs, and decisions consistently.

### P1: YAML First, Code Second

**Where it shows:** Every pipeline is a YAML file validated by Pydantic (`config.py`). The CLI generates templates from Pydantic models so YAML and code are always in sync (`odibi/introspect.py`, `odibi/tools/templates.py`). The JSON schema (`odibi.schema.json`) is auto-generated for VS Code validation.

**Implication:** YAML is the user interface. Code changes that break YAML compatibility are breaking changes.

**Files:** `odibi/config.py`, `odibi/introspect.py`, `odibi/tools/templates.py`, `odibi.schema.json`

### P2: Engine Parity — Same YAML, Same Result

**Where it shows:** `AGENTS.md` Key Pattern #1: "If Pandas has it, Spark and Polars should too." Every transformer dispatches by engine type. Skill 15 (`docs/skills/15_engine_parity_standards.md`) codifies this as a decision tree. Campaign Tasks 11-14 specifically fill Polars gaps. E2E stress tests verify row-for-row identical output across engines (V-011, V-012 in `LESSONS_LEARNED.md`).

**Implication:** A new feature without at least a `NotImplementedError` stub for all engines is incomplete.

**Files:** `docs/skills/15_engine_parity_standards.md`, `odibi/transformers/sql_core.py` (SQL-first path), every file in `odibi/patterns/`

### P3: SQL-First Transformers

**Where it shows:** D-001 in `LESSONS_LEARNED.md`: "Use `context.sql()` (DuckDB for Pandas, Spark SQL for Spark) as the primary transform mechanism." Skill 15 decision tree: "Does your logic fit in SQL? YES → Use context.sql() — automatic parity." The `sql_core.py` module (1,182 lines, 25+ transformers) is entirely SQL-first.

**Implication:** New transformers should default to `context.sql()`. Engine-specific branches are a last resort for UDFs, CoolProp, complex window frames, etc.

**Files:** `odibi/transformers/sql_core.py`, `odibi/context.py` (EngineContext.sql()), `docs/skills/14_code_standards.md`

### P4: Standalone Functions, Not Stateful Classes

**Where it shows:** D-007 in `LESSONS_LEARNED.md`: "Transformers are standalone functions `(context, params) → context`. Patterns are the only classes (extend Pattern ABC). No classes with mutable state." Skill 14 (`docs/skills/14_code_standards.md`): "Don't → Classes with mutable state. Do Instead → Standalone functions."

**Implication:** If something needs state, it's either a Pattern (ABC subclass), a connection (ABC subclass), or it belongs in the engine. Never in the transformer.

**Files:** `odibi/transformers/__init__.py` (54 registered functions), `odibi/patterns/base.py` (Pattern ABC), `CUSTOM_INSTRUCTIONS.md` rule #1

### P5: Pydantic as Single Source of Truth

**Where it shows:** `config.py` (4,766 lines) defines every config model. Templates are generated from Pydantic fields (`odibi/introspect.py`). CLI `explain` commands pull docstrings from Pydantic models. JSON schema generated from models. D-002: "YAML → Pydantic (config.py) → params: dict passed to patterns."

**Implication:** If you change a config option, change it in `config.py` first. Everything else derives from it.

**Files:** `odibi/config.py`, `odibi/introspect.py`, `odibi/tools/templates.py`, `odibi/cli/list_cmd.py`

### P6: Return, Don't Write

**Where it shows:** Skill 14 rule: "Return, don't write — patterns and transformers return DataFrames, never persist." The `Pattern.execute()` returns a DataFrame; the pipeline orchestrator handles writing. Exception: SCD2 is self-contained and writes directly (documented as an explicit anti-pattern exception in §18.2 of `ODIBI_DEEP_CONTEXT.md`).

**Implication:** Data persistence is the orchestrator's job, not the transformer's or pattern's.

**Files:** `odibi/patterns/base.py` (execute → Any), `odibi/node.py` (NodeExecutor handles write phase), `CUSTOM_INSTRUCTIONS.md` rule #2

### P7: Actionable Error Messages

**Where it shows:** `odibi/exceptions.py` — every custom exception formats with context: node name, config file, line number, available columns, input shape, previous steps, and actionable suggestions. `odibi/utils/error_suggestions.py` (671 lines) generates specific fix recommendations based on error patterns.

**Implication:** A `ValueError("Invalid config")` would be a code smell. Every error tells the user what went wrong and how to fix it.

**Files:** `odibi/exceptions.py`, `odibi/utils/error_suggestions.py`, `docs/skills/14_code_standards.md` (Error Messages section)

### P8: Structured Logging via Singleton

**Where it shows:** `get_logging_context()` singleton used everywhere in production code. `CUSTOM_INSTRUCTIONS.md` rule #4: "Never `print()` or bare `logging.getLogger()` in transforms/patterns." `LESSONS_LEARNED.md` T-001: caplog doesn't capture it, causing test-ordering failures.

**Implication:** All runtime logging goes through `get_logging_context()`. Tests must assert on return values, never log output.

**Files:** `odibi/utils/logging_context.py` (627 lines), `CUSTOM_INSTRUCTIONS.md`, `LESSONS_LEARNED.md` T-001

---

## 4. Architecture Decisions

Each decision is grounded in code or documentation with a specific citation.

### AD-01: Three-Engine Architecture with DuckDB Bridging Pandas

**Decision:** Support Pandas, Spark, and Polars engines. Pandas uses DuckDB under the hood for SQL operations (not raw Pandas operations).

**Rationale:** Pandas is the development/CI engine (no JVM needed). Spark is the production engine (Databricks). Polars is the performance alternative. DuckDB gives Pandas engine SQL parity with Spark at zero cost.

**Evidence:** `odibi/context.py` line 105-127 — `EngineContext.sql()` dispatches to DuckDB for Pandas, Spark SQL for Spark. `odibi/engine/pandas_engine.py` (2,655 lines) uses DuckDB internally.

**Tradeoff:** Three code paths to maintain. Mitigated by SQL-first approach (one query, all engines) and `NotImplementedError` stubs.

### AD-02: Pydantic Validates Structure, Patterns Validate Semantics

**Decision:** YAML is validated by Pydantic models in `config.py` for structural correctness. Pattern-level semantics (e.g., "grain must have at least one column") are validated in each pattern's `validate()` method.

**Rationale:** Decouples structural validation (is this valid YAML with correct types?) from semantic validation (does this configuration make logical sense for this pattern?).

**Evidence:** D-002 in `LESSONS_LEARNED.md`. Every pattern in `odibi/patterns/` has a `validate()` method that checks `self.params`.

**Tradeoff:** Validation errors can come from two layers, which can confuse users. Mitigated by actionable error messages with node names.

### AD-03: Global Function Registry for Transformers

**Decision:** All transformers register into a global `FunctionRegistry` class. Registration is explicit (in `odibi/transformers/__init__.py`), not decorator-based.

**Rationale:** Explicit registration makes discovery deterministic. CLI `list transformers` reads the registry. Templates and `explain` commands pull from it.

**Evidence:** `odibi/registry.py` (260 lines) — `FunctionRegistry` with `register()`, `get()`, `validate_params()`. `odibi/transformers/__init__.py` — 54 explicit registration calls.

**Tradeoff:** Adding a transformer requires modifying `__init__.py`. But this is intentional — it's a controlled extension point, not a "drop a file and it works" system. Documented in Skill 03 (`docs/skills/03_write_a_transformer.md`).

### AD-04: Context-Based Data Flow Between Nodes

**Decision:** DataFrames flow between nodes via a `Context` object (dict-like for Pandas/Polars, temp views for Spark). Nodes access upstream outputs by name via `context.get(name)`.

**Rationale:** Uniform API regardless of engine. For Spark, temp views allow SQL access (`spark.sql("SELECT * FROM node_name")`). For Pandas, simple dict lookup.

**Evidence:** `odibi/context.py` — `PandasContext`, `SparkContext`, `PolarsContext` all implement `Context` ABC. `odibi/node.py` — `context.register(node_name, df)` after each node completes.

**Tradeoff:** SparkContext requires alphanumeric+underscore names (validated by `_validate_name()`). Spark Connect has lazy view lifecycle issues (T-011 in `LESSONS_LEARNED.md`).

### AD-05: Mock in CI, Validate in Databricks

**Decision:** Spark branches are tested via `unittest.mock.MagicMock` in CI (no JVM). Real Spark testing happens in production Databricks environment via hardening campaign notebooks.

**Rationale:** JVM setup in CI is expensive and fragile. Mocks cover branch logic. Real Spark Connect on Databricks catches runtime integration issues.

**Evidence:** `AGENTS.md`: "mock logic in CI, validate behavior in Databricks prod." `docs/AGENT_CAMPAIGN.md` — 35 campaign tasks, Phases 2-5 run on real Spark.

**Tradeoff:** Mock tests can't catch Spark Connect API differences (T-011, T-017 in `LESSONS_LEARNED.md`). The hardening campaign compensates.

### AD-06: Coverage-Guided Quality with Pragmatic Skip List

**Decision:** Target 80% overall coverage. Explicitly skip Spark engine branches (4% coverage, diminishing returns) and isolate known-hanging tests.

**Rationale:** 80% catches regressions in core logic. Spark branches are tested in prod. Going higher requires mocking complexity that exceeds the value.

**Evidence:** `AGENTS.md` "Coverage Roadmap: 66% → 80%" — detailed phase plan. "Hard Skip" section lists what not to waste credits on. `scripts/run_coverage.ps1` runs tests in batches to avoid hangs.

**Tradeoff:** Spark-specific bugs may not be caught in CI. Accepted risk — caught by campaign notebooks and production runs.

### AD-07: Node Execution Pipeline with Phase Tracking

**Decision:** Each node executes a fixed phase pipeline: `pre_sql → read → incremental_filter → transform → validate → quarantine → gate → write → post_sql`. Each phase is timed by `PhaseTimer`.

**Rationale:** Predictable execution model. Users know exactly when their transforms, validations, and writes happen. Phase timing enables performance diagnostics.

**Evidence:** `ODIBI_DEEP_CONTEXT.md` §2.1-2.2 (execution flow diagram). `odibi/node.py` (3,501 lines) — `NodeExecutor` implements the phase pipeline.

### AD-08: Execution Stories for Observability

**Decision:** Every pipeline run generates an HTML "story" documenting execution: node statuses, row counts, timing, config snapshots, anomalies, and lineage graphs.

**Rationale:** Without a dedicated orchestrator (Airflow, etc.), the data engineer needs audit trails. Stories serve as both debugging tool and compliance record.

**Evidence:** `odibi/story/generator.py` (1,406 lines), `odibi/story/doc_generator.py` (1,373 lines). `odibi/cli/story.py` — `odibi story last` opens the most recent story.

### AD-09: System Catalog with Delta Lake State

**Decision:** Pipeline metadata (run history, HWM, node outputs) is stored in Delta Lake tables managed by `CatalogManager`. State backends support Local JSON (dev), Delta (prod), and SQL Server (enterprise).

**Rationale:** HWM-based incremental loading requires persistent state. Delta Lake provides ACID guarantees for concurrent pipeline runs.

**Evidence:** `odibi/catalog.py` (3,637 lines), `odibi/state/__init__.py` (80% covered), `ODIBI_DEEP_CONTEXT.md` §9 (System Catalog).

### AD-10: Skill System as Codified Engineering Judgment

**Decision:** 15 markdown skill documents encode repeatable workflows for specific task types (write a transformer, add a connection, testing standards, engine parity, etc.).

**Rationale:** AI agents (and humans) make consistent decisions when the process is explicit. The skill router in `CUSTOM_INSTRUCTIONS.md` maps task types to skill chains.

**Evidence:** `docs/skills/` — 15 skill documents. `CUSTOM_INSTRUCTIONS.md` — "Skill Router" section with decision tree and example loadouts. Workflow loops map triggers to skill chains.

---

## 5. Reusable Patterns

Patterns that recur across the codebase and should be deliberately reused.

### RP-01: Engine Dispatch with Private Helpers

```
public function → if PANDAS → _func_pandas()
                → if SPARK  → _func_spark()
                → if POLARS → _func_polars()
                → else      → raise ValueError
```

**Where used:** Every transformer in `odibi/transformers/` that can't use SQL-first. All 6 patterns.

**Why it works:** Each engine path is independently testable. The public function is a thin dispatcher.

**Files:** `docs/skills/14_code_standards.md` (Engine Dispatch Pattern), `docs/skills/15_engine_parity_standards.md`

### RP-02: SQL-First via `context.sql()`

```python
def my_transform(context, params):
    return context.sql(f'SELECT *, expr AS new_col FROM df')
```

**Where used:** All 25+ transformers in `sql_core.py`. DuckDB handles Pandas, Spark SQL handles Spark.

**Why it works:** One SQL string works on all engines. No branching. Maximum engine parity with minimum code.

**Files:** `odibi/transformers/sql_core.py`, `odibi/context.py` (EngineContext.sql)

### RP-03: Pydantic Params + Registration + CLI Introspection

Every transformer follows: `ParamsModel (Pydantic)` → `function(context, params)` → `registry.register(func, name, ParamsModel)`. This unlocks:
- `odibi list transformers` (discovery)
- `odibi explain <name>` (documentation from docstrings)
- `odibi templates transformer <name>` (YAML example from Pydantic fields)
- JSON schema validation in VS Code

**Files:** `odibi/registry.py`, `odibi/transformers/__init__.py`, `odibi/cli/list_cmd.py`

### RP-04: ABC Base Class + Concrete Implementations

```
BaseConnection → LocalConnection, AzureADLSConnection, AzureSQLConnection, ...
Engine (ABC)   → PandasEngine, SparkEngine, PolarsEngine
Pattern (ABC)  → DimensionPattern, FactPattern, AggregationPattern, ...
Context (ABC)  → PandasContext, SparkContext, PolarsContext
```

**Why it works:** Each base defines the contract (what methods must exist). Implementations are swappable. Factory functions create the right concrete type from config.

**Files:** `odibi/connections/base.py`, `odibi/engine/base.py`, `odibi/patterns/base.py`, `odibi/context.py`

### RP-05: Try/Except Import Guards for Optional Dependencies

```python
try:
    from pyspark.sql import DataFrame as SparkDataFrame
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    SparkDataFrame = None
```

**Where used:** `odibi/context.py`, `odibi/catalog.py` (fallback type stubs), every test file touching Spark.

**Why it works:** Framework runs on Pandas-only environments. Spark is optional. CoolProp is optional. Polars is optional.

**Files:** `odibi/context.py` (Polars guard), `odibi/catalog.py` (lines 22-137 — Spark fallback stubs), `AGENTS.md` CI guard pattern

### RP-06: Structured Memory (Decisions/Traps/Patterns/Discoveries)

The `LESSONS_LEARNED.md` system uses four categories with indexed entries:
- **D-NNN: Decisions** — why we chose X over Y
- **T-NNN: Traps** — things that look right but break
- **P-NNN: Patterns** — working code recipes
- **V-NNN: Discoveries** — behavior verified empirically

**Why it works:** Future agents and future-you can search by category. Each entry has date, context, root cause, and fix.

**Files:** `docs/LESSONS_LEARNED.md`, `docs/skills/11_lessons_learned_protocol.md`

### RP-07: Hardening Campaign Pattern

A structured series of tasks, each one branch and one PR, organized by phase:
1. Foundation → Spark Reality → Polars Parity → Validation E2E → Pattern Stress → Bug Fixes → New Features → Docs

Each task has a prompt, success criteria, and <250 LOC constraint.

**Files:** `docs/AGENT_CAMPAIGN.md` (35 tasks, 8 phases)

---

## 6. Anti-Patterns Odibi Avoids

These are things the codebase deliberately does NOT do, with evidence.

### AP-01: No `print()` or Bare Logging

`CUSTOM_INSTRUCTIONS.md` rule #4. All logging goes through `get_logging_context()`. This is enforced by code review standards (Skill 09) and caught by pattern in Skill 14.

### AP-02: No Side Effects in Transformers

Transformers return DataFrames — they never write to disk, send alerts, or modify global state. Exception: SCD2 writes to its target (explicitly documented as a special case in `ODIBI_DEEP_CONTEXT.md` §18.2).

### AP-03: No Nested Pydantic Models

Skill 14: "Flat params with `Field()`. Don't → Nested Pydantic models." Params models are flat dictionaries with `Field(...)` for required and `Field(default=X)` for optional.

### AP-04: No `pytest --cov`

D-004 in `LESSONS_LEARNED.md`: "`pytest --cov` changes import/execution order, triggering Rich Text vs str mismatch." Always use `coverage run` separately.

### AP-05: No `%run` on Databricks

D-003: "%run hangs and gets stuck." The lib/ pattern replaces it entirely.

### AP-06: No Hyphens or Special Characters in Node Names

Enforced by `SparkContext._validate_name()` regex `^[a-zA-Z0-9_]+$`. Documented in `ODIBI_DEEP_CONTEXT.md` §18.1.

### AP-07: No Classes with Mutable State (Outside Patterns/Connections)

D-007: Transformers are standalone functions. Only Pattern subclasses and Connection subclasses are classes.

### AP-08: No `caplog` in Tests

T-001: caplog doesn't capture structured logging and causes batch failures. Assert on return values and behavior only.

### AP-09: No "Spark" or "Delta" in Test File Names

T-003: `conftest.py` skip filter catches these on Windows.

### AP-10: No Feature Without Tests

`AGENTS.md` "What NOT to Do": "Don't add features without tests." `CUSTOM_INSTRUCTIONS.md` scope check requires tests in the plan.

---

## 7. Tradeoffs and When to Revisit

### T-01: Three Engines = Three Code Paths

**Tradeoff:** Engine parity multiplies development and testing effort.

**Current mitigation:** SQL-first approach reduces branching. `NotImplementedError` stubs allow partial implementation.

**Revisit when:** Polars adoption in the team is confirmed (or not). If Polars is never used in production, consider dropping to two engines. If DuckDB SQL covers all use cases, the Pandas engine could absorb remaining edge cases.

### T-02: Monolithic `config.py` (4,766 Lines)

**Tradeoff:** All Pydantic models in one file. Easy to find everything, but long to read and high merge conflict risk.

**Current mitigation:** Well-organized with clear sections. CLI introspection reads from it.

**Revisit when:** Config models exceed ~6,000 lines, or multiple developers are editing config simultaneously. Consider splitting into `config/connections.py`, `config/nodes.py`, `config/validation.py`.

### T-03: Singleton Logging Context

**Tradeoff:** `get_logging_context()` singleton accumulates state, causing test-ordering failures (T-001, T-010). But it provides structured logging across the entire framework.

**Current mitigation:** Never use caplog. Assert on return values. Run tests in batches.

**Revisit when:** A non-singleton logging approach (e.g., dependency-injected logger) becomes worth the refactor cost.

### T-04: SCD2 Is Self-Contained (Violates "Return, Don't Write")

**Tradeoff:** SCD2 transformer writes directly to its target table on all engines. This violates the "return, don't write" principle but is necessary because SCD2 semantics require reading and writing the same table atomically (especially with Delta MERGE).

**Current mitigation:** Documented as an explicit exception in `ODIBI_DEEP_CONTEXT.md` §18.2.

**Revisit when:** Never (this is inherent to SCD2 semantics).

### T-05: Mock-Based Spark Testing vs. Real Spark

**Tradeoff:** Mocks cover branch logic but miss Spark Connect behavior (T-011, T-017, T-019).

**Current mitigation:** Hardening campaign notebooks run on real Spark. Three distinct Spark Connect bugs were found only via real testing (P-009).

**Revisit when:** CI with real Spark becomes cheap enough (e.g., Spark Connect to a shared cluster from GitHub Actions).

### T-06: Test Suite Hangs Require Batching

**Tradeoff:** Full `tests/unit/` suite hangs due to ThreadPoolExecutor + coverage tracing + mock contamination + Polars issues.

**Current mitigation:** `scripts/run_coverage.ps1` runs tests in batches. Documented in `AGENTS.md`.

**Revisit when:** The root causes (Rich singleton, Polars `repeat_by` hang, mock `sys.modules` pollution) are individually fixed.

### T-07: ~34,000 Lines of Framework Code for a Solo Engineer

**Tradeoff:** Odibi is large for a one-person project. Maintaining it requires significant effort.

**Current mitigation:** 80% test coverage, 15 skill docs, structured memory system, AI agent-assisted development.

**Revisit when:** The framework stabilizes and new feature development slows. At that point, the priority shifts from adding features to maintaining and documenting what exists. **This codex is part of that shift.**

---

## 8. Simulation & Learning Pattern Standard

The simulation engine and companion repo (`odibi-simulations/`) establish a gold standard for learning curricula. This section codifies that standard.

### 8.1 The Gold Standard: One Config, One Notebook

| Component | Requirements |
|---|---|
| **YAML config** | ~100-150 lines. 1-2 entities. 8-12 columns. Uses real-world system names. Must be grounded in actual physics/behavior. |
| **Notebook** | Follows the 5-section structure below. One analytical angle per notebook. Uses Altair for visualization. |

### 8.2 Notebook Structure (5 Sections)

1. **Config Inspection** — Load and display the YAML config. Show columns, entities, generator types, chaos parameters. Let the reader understand what they're about to generate.

2. **Run Simulation** — Execute `odibi run` on the config. Load the resulting Parquet file. Show shape, dtypes, first few rows.

3. **Visualization** — 2-4 Altair charts exploring the generated data. Show the behavior the config is designed to model.

4. **Break It** — The most important section. Identify the simulation's failure mode — the gap between model and reality. This is where the learning happens. Examples:
   - "Independent generators can't model spatial correlation" (notebook 50)
   - "Boolean generators without temporal correlation produce unrealistic sessions" (notebook 46)
   - "Linear degradation misses the bathtub curve" (notebook 39)
   - "Queue systems have memory that outlasts the surge" (notebook 52)
   - "trend vs mean_reversion determines crash vs stabilization" (notebook 53)

5. **Takeaway** — One transferable sentence. This sentence should be useful *outside Odibi*, in any engineering context.

### 8.3 Config Design Principles

From the 54 configs in `odibi-simulations/configs/`:

- **Grounded in real systems:** Every config models something physical (compressors, wastewater, solar farms, hospital EDs). Not toy examples.
- **Named entities, not generic IDs:** `Tank_A`, `Station_North`, `Line_1` — names that carry meaning.
- **Generator types map to physics:** `random_walk` for stochastic processes, `daily_profile` for cyclical behavior, `derived` for coupled variables, `sequential` for batch IDs.
- **Entity overrides encode behavioral differences:** Not all entities are identical. `entity_overrides` lets specific units have different parameters (e.g., one compressor runs hotter).
- **Scheduled events force specific scenarios:** `scheduled_events` inject known conditions (maintenance windows, setpoint changes, load spikes) for testing.
- **Chaos injection tests robustness:** `chaos: {outlier_rate, duplicate_rate, blackout_rate}` introduces realistic data quality issues.
- **`prev()` models state:** For any system where the current value depends on the previous value (tank levels, battery SOC, queue depth), `prev()` in derived expressions enables stateful generation.

### 8.4 Domain Coverage (as of May 2026)

20+ domains across 73 notebooks: Manufacturing, Chemical Engineering, Energy (Solar, Wind, BESS, Grid), Healthcare (ICU, Hospital ED), Logistics (Warehouse, Supply Chain, Container Port), IT Ops (Server Monitor, API Perf), Environmental (Air Quality, Water, Weather), Finance (Market Maker, Credit), Telecom (Cell Tower, Network), Transportation (Traffic, Fleet, EV), Agriculture (Crop, Greenhouse), Aviation (Airport Ops), Cybersecurity (SOC), Insurance (Claims), Mining (Ore), Maritime (Port), Construction (Scheduling).

### 8.5 What Makes a Good "Break It" Section

The "Break It" section is not a bug report — it's a *diagnosis of the model's boundary*. It should:

1. **State a hypothesis** about what the simulation should produce if it modeled reality perfectly
2. **Show evidence** that the simulation doesn't produce that (with data, stats, or charts)
3. **Explain why** — what generator feature or design choice causes the gap
4. **Name the engineering concept** that the gap illustrates (spatial correlation, Jensen's inequality, Liebig's law, positive feedback loops, etc.)

This is what makes the simulation a learning tool, not just a data generator.

---

## 9. Knowledge System Architecture

Odibi has an unusually well-developed knowledge management system for a solo project. This section maps how the pieces fit together.

```
CUSTOM_INSTRUCTIONS.md          ← System prompt. Loaded first every session.
    ├── Pre-Code Gate           ← Mandatory checklist before writing code
    ├── Skill Router            ← Maps task types to skill chains
    ├── Critical Rules          ← 17 never-violate rules
    ├── Correctness Protocol    ← Anti-patterns for verification
    └── Workflow Loops          ← Automated skill chains by trigger

docs/skills/ (15 files)         ← Codified workflows for specific task types
    ├── 01 Think/Plan/Critique  ← Decision framework (always loaded)
    ├── 02 Odibi-First Lookup   ← Check before reinventing (always loaded)
    ├── 03-06 Creation skills   ← Transformer, pattern, YAML, connection
    ├── 07-08 Quality skills    ← Testing gotchas, validation workflow
    ├── 09-10 Process skills    ← Code review, task planning
    ├── 11 Lessons Protocol     ← How to capture new knowledge
    ├── 12 Databricks Protocol  ← Notebook standards
    └── 13-15 Standards         ← Testing, code, engine parity

AGENTS.md                       ← Living operational document
    ├── Coverage Status         ← Per-module coverage with test file refs
    ├── Coverage Roadmap        ← Phase plan with progress tracker
    ├── Testing Gotchas         ← Actionable trap catalog
    └── Continuous Learning     ← Rules for updating this file

docs/LESSONS_LEARNED.md         ← Indexed knowledge base
    ├── Decisions (D-NNN)       ← Why we chose X over Y
    ├── Traps (T-NNN)           ← Things that look right but break
    ├── Patterns (P-NNN)        ← Working code recipes
    └── Discoveries (V-NNN)     ← Empirically verified behavior

docs/ODIBI_DEEP_CONTEXT.md     ← Complete framework reference (2,700 lines)
docs/THREAD_QUICK_START.md     ← Source layout with line counts
docs/AGENT_CAMPAIGN.md          ← 35 hardening tasks in 8 phases
```

### Why This Structure Works

1. **Layered depth:** Quick start → skills → deep context → lessons learned. Agents read only what they need.
2. **Compounding knowledge:** Every session adds to `LESSONS_LEARNED.md` and `AGENTS.md`. The system gets smarter over time.
3. **Skill routing prevents guessing:** The decision tree in `CUSTOM_INSTRUCTIONS.md` tells agents exactly which skills to load for their task type.
4. **Indexed traps prevent repeat mistakes:** T-001 through T-029 are searchable. An agent hitting a caplog issue finds T-001 immediately.
5. **Session boundaries are explicit:** Start protocol (load context), work protocol (skills + verification), end protocol (capture lessons).

---

## Appendix: File Citation Index

| Principle/Decision | Primary Files |
|---|---|
| YAML-first | `odibi/config.py`, `odibi/introspect.py`, `odibi.schema.json` |
| Engine parity | `docs/skills/15_engine_parity_standards.md`, `odibi/transformers/sql_core.py` |
| SQL-first | `odibi/transformers/sql_core.py`, `odibi/context.py` |
| Standalone functions | `odibi/transformers/__init__.py`, `CUSTOM_INSTRUCTIONS.md` |
| Pydantic source of truth | `odibi/config.py`, `odibi/introspect.py`, `odibi/tools/templates.py` |
| Return, don't write | `odibi/patterns/base.py`, `odibi/node.py` |
| Actionable errors | `odibi/exceptions.py`, `odibi/utils/error_suggestions.py` |
| Structured logging | `odibi/utils/logging_context.py` |
| Mock in CI, validate in prod | `AGENTS.md`, `docs/AGENT_CAMPAIGN.md` |
| Execution stories | `odibi/story/generator.py`, `odibi/story/doc_generator.py` |
| System catalog | `odibi/catalog.py`, `odibi/state/__init__.py` |
| Skill system | `docs/skills/`, `CUSTOM_INSTRUCTIONS.md` |
| Lessons learned | `docs/LESSONS_LEARNED.md`, `docs/skills/11_lessons_learned_protocol.md` |
| Simulation gold standard | `odibi/simulation/generator.py`, `odibi-simulations/` (54 configs, 73 notebooks) |
| Hardening campaign | `docs/AGENT_CAMPAIGN.md` (35 tasks, 8 phases) |
