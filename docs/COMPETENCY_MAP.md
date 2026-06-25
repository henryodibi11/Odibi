# Odibi-Based Python & Data Engineering Competency Map

> **Purpose:** Use Odibi as a study system to become a stronger Python programmer and data engineer — by studying the concepts, libraries, and patterns already inside the framework you built.
>
> **Method:** Every concept below was discovered by inspecting the Odibi repo, not assumed. File citations are provided.
>
> **Last updated:** 2026-05-03

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Concepts Odibi Already Teaches Well](#2-concepts-odibi-already-teaches-well)
3. [Concepts Present But Worth Deepening](#3-concepts-present-but-worth-deepening)
4. [Concepts Missing Or Needing External Study](#4-concepts-missing-or-needing-external-study)
5. [Libraries Used In Odibi And What To Learn From Them](#5-libraries-used-in-odibi-and-what-to-learn-from-them)
6. [Recommended Odibi Files/Modules To Study](#6-recommended-odibi-filesmodules-to-study)
7. [Small Practice Exercises Based On Odibi](#7-small-practice-exercises-based-on-odibi)
8. [Prioritized 8–12 Week Review Plan](#8-prioritized-812-week-review-plan)

---

## 1. Executive Summary

Odibi is a ~34,000-statement Python data pipeline framework. By building it, you've already exercised a remarkable range of Python and data engineering concepts — most of them deeply, not superficially. This document maps what's there so you can study your own code intentionally, identify the patterns you use intuitively, name them, and fill gaps.

**What Odibi covers well (study the implementation):**
- Abstract base classes and polymorphism (5 ABC hierarchies)
- Pydantic data validation (4,766-line config.py)
- Factory pattern and registry pattern
- Engine abstraction (3 engines, SQL-first design)
- DWH patterns (SCD2, star schema, aggregation)
- Data quality (validation, quarantine, quality gates)
- CLI tooling (18+ subcommands, introspection)
- Threading and concurrency
- Structured exception hierarchy
- Regex for parsing and validation

**What Odibi uses but you should deepen (present but worth studying more formally):**
- Decorators (custom `@transform` decorator exists, but deeper meta-programming is learnable)
- Generators/yield (used in context managers, API pagination — but more patterns exist)
- `inspect` module (used heavily — worth understanding the full introspection model)
- Protocol/structural typing (one usage — worth expanding)

**What Odibi doesn't cover (study externally):**
- `async`/`await` and asyncio
- Metaclasses
- Descriptors
- `__slots__` for memory optimization
- Generic types (`TypeVar`, `Generic`)
- Match/case (Python 3.10+)

---

## 2. Concepts Odibi Already Teaches Well

Each concept below is deeply implemented in Odibi. Studying these files is equivalent to studying a textbook example — with the advantage that you wrote it.

---

### 2.1 Abstract Base Classes (ABCs) and Polymorphism

**Concept:** Define an interface contract that all implementations must follow. Swap implementations without changing calling code.

**Where in Odibi:**
| ABC | File | Implementations |
|-----|------|----------------|
| `Context` | `odibi/context.py:131-197` | `PandasContext`, `SparkContext`, `PolarsContext` |
| `Engine` | `odibi/engine/base.py:9-334` | `PandasEngine`, `SparkEngine`, `PolarsEngine` |
| `Pattern` | `odibi/patterns/base.py:13-274` | `DimensionPattern`, `FactPattern`, `AggregationPattern`, `DateDimensionPattern`, `SCD2Pattern`, `MergePattern` |
| `BaseConnection` | `odibi/connections/base.py:7-170` | `LocalConnection`, `AzureADLSConnection`, `AzureSQLConnection`, `PostgresConnection`, `HttpConnection` |
| `StateBackend` | `odibi/state/__init__.py` | `LocalJSONStateBackend`, `CatalogStateBackend`, `SqlServerSystemBackend` |

**Why it matters:** ABCs are the backbone of any extensible system. When you add a new engine or connection, you implement the contract and everything else works. This is the Open/Closed Principle in action.

**What to study:** Read `engine/base.py` (269 lines) — it defines 14 abstract methods. Then read `engine/pandas_engine.py` to see how each method is concretely implemented. Notice how `register_format()` is a class method that enables extension without modifying the base.

**Exercise:** Pick one abstract method in `Engine` (e.g., `count_nulls`). Read the base signature, then compare the Pandas, Spark, and Polars implementations. Write down: how do they differ? Why?

**Coverage:** ✅ Odibi covers this deeply. No external study needed.

---

### 2.2 Pydantic Data Validation

**Concept:** Use Python type annotations to validate data at runtime. Define schemas once, get validation, serialization, and documentation automatically.

**Where in Odibi:** `odibi/config.py` (4,766 lines) — the largest file in the codebase. Contains 50+ Pydantic models that validate every YAML config.

**Key features used:**
| Pydantic Feature | Where | Example |
|---|---|---|
| `BaseModel` | Every config model | `NodeConfig`, `WriteConfig`, `ValidationConfig` |
| `Field(...)` / `Field(default=X)` | All param models | Required vs optional fields |
| `field_validator` | `config.py:4773-4808` | Node name validation regex |
| `model_validator` | `config.py` (multiple) | Cross-field validation (e.g., SCD2 needs `target` OR `connection+path`) |
| `Annotated` + `Discriminator` | `config.py:535,728,1774,3110` | Discriminated unions for auth configs, generator configs, test configs |
| `str, Enum` | `config.py:20-160` | `EngineType`, `WriteMode`, `DeleteDetectionMode` — type-safe string enums |

**Why it matters:** Pydantic is the industry standard for config validation in Python. Odibi's usage is a masterclass — every transformer param model, every connection config, every validation test config uses Pydantic.

**What to study:** Read `config.py` lines 1-100 (enums and base models). Then read a transformer params model like `sql_core.py:FilterRowsParams`. Then read a discriminated union like `TestConfig` (line ~3110) to see `Annotated[Union[...], Discriminator("type")]`.

**Exercise:** Write a new Pydantic model for a hypothetical `TimezoneConvertParams` with required `column: str`, required `from_tz: str`, optional `to_tz: str = "UTC"`, and a `field_validator` that checks `from_tz` against `pytz.all_timezones`.

**Coverage:** ✅ Odibi covers this exhaustively. You could study Pydantic entirely through this codebase.

---

### 2.3 Factory and Registry Patterns

**Concept:** Decouple object creation from usage. Register implementations by name, look them up at runtime.

**Where in Odibi:**
| Pattern | File | What it does |
|---|---|---|
| **Function Registry** | `odibi/registry.py` (260 lines) | Global registry of 54 transformer functions with name→function+params mapping |
| **Connection Factory** | `odibi/connections/factory.py` (379 lines) | Creates typed connections from config strings (`"local"` → `LocalConnection`) |
| **Context Factory** | `odibi/context.py:522-544` | `create_context("pandas")` → `PandasContext()` |
| **Pattern Registry** | `odibi/patterns/__init__.py` | `_PATTERNS` dict mapping `"dimension"` → `DimensionPattern` class |
| **Engine Registry** | `odibi/engine/registry.py` (38 lines) | Maps engine type strings to engine classes |

**Why it matters:** These patterns are everywhere in professional software. They enable plugin-like extensibility (add a new transformer by registering it) and decouple configuration from implementation (YAML says `"type: local"`, factory creates the right object).

**What to study:** Read `registry.py` end-to-end (260 lines). It's self-contained and demonstrates: class-level dictionaries, `inspect.signature()` introspection, `@wraps` decorator usage, and param validation against Pydantic models.

**Exercise:** Add a fictional transformer to the registry: write a function `reverse_string(context, params)`, a `ReverseStringParams` model, and register it in `__init__.py`. Don't commit — just verify `FunctionRegistry.get("reverse_string")` works.

**Coverage:** ✅ Odibi covers both Factory and Registry deeply.

---

### 2.4 Enums and Type-Safe Constants

**Concept:** Replace magic strings with typed enumerations that the type checker and IDE can validate.

**Where in Odibi:** `odibi/config.py:20-160` — 10+ enum classes all using `str, Enum` (so they serialize naturally to/from YAML strings).

| Enum | Purpose |
|---|---|
| `EngineType` | `"spark"`, `"pandas"`, `"polars"` |
| `WriteMode` | `"overwrite"`, `"append"`, `"upsert"`, `"append_once"`, `"merge"` |
| `DeleteDetectionMode` | `"none"`, `"snapshot_diff"`, `"sql_compare"` |
| `AlertType` | `"webhook"`, `"slack"`, `"teams"` |
| `ErrorStrategy` | `"fail_fast"`, `"fail_later"`, `"ignore"` |

Also: `sql_core.py:SimpleType`, `relational.py:AggFunc`, `merge_transformer.py:MergeStrategy`, `advanced.py:HashAlgorithm`.

**Why it matters:** Enums prevent typo bugs (`"overwite"` vs `"overwrite"`) and enable IDE autocomplete. The `str, Enum` pattern is specific to Python and essential for YAML/JSON serialization.

**What to study:** Read `config.py:38-63` (`WriteMode`) — notice the docstring explains each mode's behavior and includes a comparison table. This is enum documentation done right.

**Exercise:** Find every place `EngineType` is compared in `odibi/transformers/sql_core.py`. How many engine dispatch branches exist? How many use SQL-first (no branching)?

**Coverage:** ✅ Deeply covered. The `str, Enum` pattern is especially valuable.

---

### 2.5 Context Managers

**Concept:** Guarantee setup/cleanup code runs, even on exceptions. The `with` statement protocol.

**Where in Odibi:**
| Implementation | File | How |
|---|---|---|
| `Pipeline` as context manager | `pipeline.py:264-268` | `__enter__`/`__exit__` for connection cleanup |
| Node phase timing | `node.py:53-58, 96-111` | `@contextmanager` generator for `PhaseTimer` |
| Logging metrics scope | `utils/logging_context.py:312-346` | `@contextmanager` for metrics collection |
| Telemetry spans | `utils/telemetry.py:31-34` | `__enter__`/`__exit__` for OpenTelemetry span lifecycle |
| Temp directory fixture | `testing/fixtures.py:17-32` | `@contextmanager` with `yield` |

**Why it matters:** Context managers are one of Python's most elegant features. They replace try/finally patterns and make resource management declarative.

**What to study:** Compare the two styles in Odibi: class-based (`Pipeline.__enter__/__exit__`) vs generator-based (`@contextmanager` in `node.py`). When would you choose one over the other?

**Exercise:** Read `testing/fixtures.py:17-32`. It's a 15-line `@contextmanager` that creates and cleans up a temp directory. Write a similar one that creates a temp Delta table path and cleans up after.

**Coverage:** ✅ Well covered with both styles.

---

### 2.6 Threading and Concurrency

**Concept:** Run code in parallel, protect shared state with locks, use thread pools for I/O-bound work.

**Where in Odibi:**
| Pattern | File | Usage |
|---|---|---|
| `threading.Lock()` | `context.py:217,244`, `catalog.py:204,257` | Protect shared dictionaries from concurrent access |
| `threading.RLock()` | `context.py:396` | Reentrant lock for SparkContext |
| `threading.local()` | `context.py:20-29` | Thread-local storage for unique temp view names |
| `ThreadPoolExecutor` | `pipeline.py:732-741`, `connections/azure_adls.py:295`, `engine/pandas_engine.py:429`, `tools/adf_profiler.py:793` | Parallel file reads, key vault fetches, ADF API calls |
| `threading.Thread` | `catalog_sync.py:862-864` | Fire-and-forget async catalog sync |

**Why it matters:** Real-world data engineering involves I/O-bound operations (API calls, file reads, key vault fetches). Thread pools make these parallel without the complexity of `asyncio`.

**What to study:** Read `pipeline.py:732-741` — how `ThreadPoolExecutor` parallelizes node execution. Then read `context.py:20-29` — how `threading.local()` prevents temp view name collisions between threads.

**Exercise:** Read `connections/azure_adls.py` and find the `ThreadPoolExecutor` usage. What is it parallelizing? What is the lock protecting? Could this deadlock? (Hint: T-001 in LESSONS_LEARNED.md is related.)

**Coverage:** ✅ Deeply covered. Odibi demonstrates real-world threading patterns with real bugs documented (T-001, T-010).

---

### 2.7 Exception Hierarchy and Error Design

**Concept:** Build a custom exception tree that provides actionable, context-rich error messages.

**Where in Odibi:** `odibi/exceptions.py` (237 lines) + `odibi/utils/error_suggestions.py` (671 lines).

| Exception | Purpose | Rich Info Included |
|---|---|---|
| `OdibiException` | Base | — |
| `ConfigValidationError` | Bad YAML | File path, line number, error message |
| `ConnectionError` | Connection failed | Connection name, reason, suggestions list |
| `DependencyError` | DAG problems | Message, cycle path |
| `NodeExecutionError` | Node failed | Node name, config file, step index, available columns, input shape, previous steps, suggestions, story path |
| `ValidationError` | DQ failed | Node name, failure list |
| `GateFailedError` | Gate failed | Pass rate, required rate, failed/total rows, reasons |

**Why it matters:** Error messages are a user interface. `NodeExecutionError._format_error()` produces a multi-section diagnostic that tells the user exactly where, what, and how to fix it. `error_suggestions.py` pattern-matches error strings to generate specific fix recommendations.

**What to study:** Read `exceptions.py` end-to-end (237 lines). Focus on `NodeExecutionError._format_error()` — it's a template for how to design error messages in any project. Then skim `error_suggestions.py` to see how regex patterns map to suggestions.

**Exercise:** Create a hypothetical `SchemaEvolutionError` that includes: source schema, target schema, columns added, columns removed, and a suggestion to use `merge_schema: true`.

**Coverage:** ✅ Exceptional coverage. This is one of Odibi's strongest design patterns.

---

### 2.8 Regex for Parsing and Validation

**Concept:** Use regular expressions for pattern matching, string parsing, and input validation.

**Where in Odibi:** 14+ modules use `re` for different purposes:

| Purpose | File | What |
|---|---|---|
| Name validation | `config.py:16`, `context.py:414` | `^[a-zA-Z_][a-zA-Z0-9_]*$` for node/pipeline names |
| SQL rewriting | `context.py:118`, `node.py:1866` | Replace `df` references with temp view names using word boundaries |
| Date variable substitution | `connections/api_fetcher.py:226-276` | Replace `{today}`, `{yesterday}` in API URL templates |
| Spark error parsing | `exceptions.py:135-152` | Extract Java exception class and message from Py4J stack traces |
| Type hint formatting | `introspect.py:1014,1185-1195` | Clean up Pydantic type annotations for documentation |
| Hive partition detection | `discovery/utils.py:29-33` | Detect `key=value` partition patterns in file paths |
| Interval parsing | `simulation/generator.py:237,468` | Parse `"5min"`, `"1h"`, `"30s"` into timedelta values |

**Why it matters:** Regex is the universal tool for text processing. Odibi uses it for everything from input validation to SQL rewriting to error message cleanup.

**What to study:** Read `connections/api_fetcher.py:226-276` — the date variable substitution code. It replaces `{today-3d}`, `{yesterday}`, `{month_start}` with actual dates using regex + callback. This is a real-world regex application.

**Exercise:** Find the regex in `context.py:118` that replaces `df` references in SQL queries. Why does it use lookbehind/lookahead assertions (`(?<!...)`, `(?!...)`)? What would break without them?

**Coverage:** ✅ Deeply covered across many use cases.

---

### 2.9 DWH / Kimball Patterns

**Concept:** Star schema design — dimension tables with surrogate keys, fact tables with FK lookups, SCD Type 2 history tracking, conformed date dimensions.

**Where in Odibi:**
| Pattern | File | Key Method |
|---|---|---|
| Dimension + SK | `patterns/dimension.py` (548 lines) | `_generate_surrogate_keys()`, `_ensure_unknown_member()` |
| Fact + FK lookup | `patterns/fact.py` (745 lines) | `_join_dimension_pandas()`, orphan handling (unknown/reject/quarantine) |
| SCD2 history | `transformers/scd.py` (841 lines) | Delta MERGE, Pandas fallback, DuckDB SQL path, NaN-safe comparison |
| Date dimension | `patterns/date_dimension.py` (404 lines) | `_calc_fiscal_year()`, `_calc_fiscal_quarter()`, 19 generated columns |
| Aggregation | `patterns/aggregation.py` (477 lines) | `_aggregate_pandas()` with DuckDB SQL, incremental merge strategies |
| Star schema E2E | `examples/star_schema_e2e/` | Full dim_date + dim_customer(SCD1) + dim_product(SCD2) + fact_orders |

**Why it matters:** These are the patterns you'll use in every data warehouse project. Odibi implements them with full engine parity, which means you understand not just the concept but the implementation differences across Pandas, Spark, and Polars.

**What to study:** Read `examples/star_schema_e2e/star_schema_e2e.py` (415 lines) — it drives all four patterns together against real Unity Catalog tables. Then read `patterns/fact.py:_join_dimension_pandas()` to see how orphan handling works.

**Exercise:** In `examples/star_schema_e2e/config.yaml`, trace the data flow: which node produces dim_customer? Which node consumes it? How does the fact table look up customer_sk? What happens to orders with a customer_id not in the dimension?

**Coverage:** ✅ Excellent. The E2E example ties it all together.

---

### 2.10 Data Quality Engineering

**Concept:** Validate data at pipeline boundaries. Route bad data to quarantine. Gate pipelines on quality thresholds.

**Where in Odibi:**
| Concept | File | Implementation |
|---|---|---|
| 11 test types | `validation/engine.py` (706 lines) | NOT_NULL, UNIQUE, ACCEPTED_VALUES, ROW_COUNT, RANGE, REGEX_MATCH, CUSTOM_SQL, SCHEMA, FRESHNESS, CROSS_CHECK, FK |
| Quarantine routing | `validation/quarantine.py` (501 lines) | `split_valid_invalid()` — mask-based row splitting with metadata columns |
| Quality gates | `validation/gate.py` (208 lines) | `evaluate_gate()` — batch pass/fail with configurable thresholds and actions |
| FK validation | `validation/fk.py` (447 lines) | YAML-declared referential integrity with orphan detection |
| E2E example | `examples/quarantine_workflow/` | Complete quarantine pipeline with config, data, and README |

**Why it matters:** Data quality is the difference between a pipeline and a trusted pipeline. Odibi's validation system is declarative (YAML), multi-engine, and has both row-level (quarantine) and batch-level (gate) controls.

**What to study:** Read `validation/engine.py` — focus on how `_validate_pandas()` dispatches to test-type-specific handlers. Then read `validation/gate.py:evaluate_gate()` — it's only 208 lines and demonstrates the full gate evaluation logic.

**Exercise:** Using the quarantine workflow example (`examples/quarantine_workflow/config.yaml`), trace what happens to a row with `amount = -50` when validation is configured with `type: range, min: 0, on_fail: quarantine`. Which function splits the row? What metadata columns are added?

**Coverage:** ✅ Deeply covered with E2E examples.

---

## 3. Concepts Present But Worth Deepening

These concepts exist in Odibi but are used in limited ways. Studying them more formally would strengthen your Python skills.

---

### 3.1 Decorators and Metaprogramming

**Present in Odibi:** `@property`, `@classmethod`, `@abstractmethod`, `@staticmethod`, `@wraps`, and the custom `@transform` decorator in `registry.py:195-247`.

**What to deepen:** The `@transform` decorator is a good decorator-with-arguments example, but Odibi doesn't use:
- Decorator classes (decorators implemented as classes with `__call__`)
- Decorator stacking for cross-cutting concerns (logging, timing, retry)
- `functools.lru_cache` for memoization

**Study these Odibi files:** `registry.py:195-247` (custom decorator with optional name argument)

**External study:** Real Python's decorator guide. Then try writing a `@timed` decorator that logs execution time for any transformer.

---

### 3.2 Generators and Iterators

**Present in Odibi:** `yield` in context managers (`node.py`, `testing/fixtures.py`), generator function for paginated API pages (`api_fetcher.py:1094-1137`).

**What to deepen:** Odibi doesn't use:
- `yield from` for delegating to sub-generators
- Infinite generators for stream processing
- `itertools` module (combinations, chain, groupby)
- Generator expressions as memory-efficient alternatives to list comprehensions (some exist, but few)

**Study these Odibi files:** `connections/api_fetcher.py:1094-1137` (paginated page generator)

**External study:** Python docs on generator expressions. Try converting a list comprehension in `catalog.py` to a generator expression and measure memory difference.

---

### 3.3 Protocol / Structural Typing

**Present in Odibi:** One instance — `PaginationStrategy(Protocol)` in `connections/api_fetcher.py:429-438`.

**What to deepen:** Protocol is Python's answer to "duck typing with type checker support." Odibi could use it more — for example, anything with a `.df` attribute and `.engine_type` could be a `HasDataFrame(Protocol)`.

**Study this Odibi file:** `connections/api_fetcher.py:429-438`

**External study:** PEP 544 (Protocols: Structural subtyping). Compare Protocol vs ABC — when would you use each?

---

### 3.4 The `inspect` Module

**Present in Odibi:** Used in 7 modules for signature introspection, docstring extraction, member scanning, and class detection. This is one of Odibi's underappreciated strengths.

| Usage | File |
|---|---|
| Transform param introspection | `registry.py:54,177-190` |
| Pagination strategy param filtering | `api_fetcher.py:939-942` |
| Auto-generating config docs | `introspect.py:777-807,953-954` |
| Transform signature checking | `node.py:1681,1993` |
| CLI help text generation | `cli/list_cmd.py:74,90,175,334` |
| Template generation | `tools/templates.py:861,875-876` |

**What to deepen:** You use `inspect.signature()`, `inspect.getdoc()`, `inspect.getmembers()`, `inspect.isclass()`. Worth also learning: `inspect.getsource()`, `inspect.getfile()`, `inspect.stack()`, and the `Signature` object's `bind()` method.

**Study this Odibi file:** `odibi/introspect.py` (1,132 lines) — the entire file is an introspection-powered documentation generator. Read it to understand how Odibi auto-generates YAML docs from Pydantic models.

---

### 3.5 Closures

**Present in Odibi:** 6+ closures in `utils/config_loader.py` (date/var substitution, tree walking) and `engine/pandas_engine.py` (retry-wrapped Delta operations).

**What to deepen:** Closures are used but not named as a concept. Understanding `nonlocal`, cell variables, and closure scope rules would help debug the threading issues documented in T-001.

**Study these Odibi files:** `utils/config_loader.py:130-142` (cleanest closure example), `engine/pandas_engine.py:1817-1885` (closures capturing DataFrames for retry logic).

---

### 3.6 Dataclasses

**Present in Odibi:** 16+ modules use `@dataclass` for result objects, metadata containers, and simulation state. But Pydantic dominates for config validation.

**What to deepen:** Understanding when to use `@dataclass` vs Pydantic `BaseModel`:
- `@dataclass` → simple data containers, no validation needed, used internally
- `BaseModel` → external-facing config, needs validation, serialization, YAML support

**Study these Odibi files:** `simulation/generator.py:33-59` (dataclasses for simulation state), `validation/gate.py:17` (gate result), `diagnostics/diff.py:14,53` (diff results). Compare with any Pydantic model in `config.py`.

---

## 4. Concepts Missing Or Needing External Study

These Python concepts are not present in Odibi. They're worth learning separately.

| Concept | Why It's Missing | Why Learn It | Suggested Resource |
|---|---|---|---|
| **`async`/`await`** | Odibi uses threads, not asyncio. Batch pipelines don't need event loops. | Essential for web APIs, high-concurrency I/O, modern Python services. | FastAPI docs (Odibi has FastAPI in `pyproject.toml` — `odibi/ui/app.py` is minimal) |
| **Metaclasses** | Odibi uses ABCs and Pydantic instead. Metaclasses are rarely needed. | Deepens understanding of Python's object model. Framework internals. | Python docs on `type()` and `__init_subclass__` |
| **Descriptors** | Pydantic handles all descriptor-like behavior (validation on assignment). | Understanding `__get__`, `__set__`, `__delete__` explains how `@property` and Pydantic work under the hood. | Python descriptor HowTo guide |
| **`__slots__`** | Not used. Memory optimization isn't a priority for config objects. | Reduces memory per instance by ~40%. Useful for objects created in millions (e.g., simulation rows). | Python docs on `__slots__` |
| **`TypeVar` / `Generic`** | Not used. Odibi uses `Any` for engine-agnostic DataFrame types. | Enables type-safe generic containers and functions. | Python `typing` module docs |
| **`match`/`case`** | Odibi targets Python 3.9+ (match/case requires 3.10+). | Cleaner pattern matching than if/elif chains. Engine dispatch would benefit. | PEP 634 |
| **Walrus operator (`:=`)** | Not used. | Useful in while-loops and comprehension filters. | PEP 572 |
| **`functools.lru_cache`** | Not used (⚠️ uncertain — may exist in a file I didn't scan). | Automatic memoization. Could benefit `introspect.py` discovery calls. | Python `functools` docs |

---

## 5. Libraries Used In Odibi And What To Learn From Them

Every library below is in `pyproject.toml` and actively used. Each one teaches a specific skill.

### Core Dependencies

| Library | Version | Where Used | What To Learn |
|---|---|---|---|
| **Pydantic** | ≥2.0 | `config.py` (4,766 lines) | Data validation, discriminated unions, field validators, model validators, JSON schema generation |
| **PyYAML** | ≥6.0 | `utils/config_loader.py` | YAML parsing, safe loading, custom constructors |
| **Pandas** | ≥2.0 | `engine/pandas_engine.py` (2,655 lines) | DataFrame operations, read/write formats, DuckDB integration, schema manipulation |
| **NumPy** | ≥1.24 | `simulation/generator.py` | Random walks, statistical distributions, array operations |
| **DuckDB** | ≥0.9 | `engine/pandas_engine.py`, `context.py` | SQL on DataFrames, in-process analytics database |
| **PyArrow** | ≥14.0 | `engine/pandas_engine.py`, `catalog.py`, `state/__init__.py` | Columnar data format, Arrow IPC, Parquet read/write, Delta Lake integration |
| **deltalake** | ≥0.18 | `engine/pandas_engine.py`, `catalog.py`, `state/__init__.py` | Delta Lake without Spark — ACID transactions, time travel, schema evolution |
| **Rich** | ≥13.0 | `utils/logging_context.py`, `utils/progress.py`, `cli/main.py` | Terminal formatting, progress bars, tables, syntax highlighting |
| **Jinja2** | ≥3.1 | `story/renderers.py`, `story/doc_generator.py` | HTML template rendering for execution stories |
| **requests** | ≥2.28 | `connections/api_fetcher.py`, `tools/adf_profiler.py` | HTTP requests, session management, retry patterns |

### Optional Dependencies (Worth Studying)

| Library | Where Used | What To Learn |
|---|---|---|
| **PySpark** | `engine/spark_engine.py` (2,327 lines) | Distributed DataFrames, Spark SQL, Delta MERGE, Spark Connect |
| **Polars** | `engine/polars_engine.py` (1,336 lines) | Rust-backed DataFrames, lazy evaluation, expression API |
| **SQLAlchemy** | `connections/azure_sql.py`, `connections/postgres.py` | ORM-free SQL, connection pooling, engine management |
| **CoolProp** | `transformers/thermodynamics.py` (882 lines) | Scientific computing, fluid property lookups |
| **pint** | `transformers/units.py` (448 lines) | Unit conversion, dimensional analysis |
| **fsspec** | `story/lineage_utils.py`, `odibi_mcp/` | Filesystem abstraction (local, ADLS, S3, GCS) |

### Dev Dependencies

| Library | What To Learn |
|---|---|
| **pytest** | Test discovery, fixtures, parametrize, markers, conftest |
| **ruff** | Fast linting and formatting, replaces flake8+black+isort |
| **coverage** | Branch-aware coverage measurement, `--source` vs `--include` |
| **pre-commit** | Git hook automation for consistent code quality |
| **mkdocs-material** | Documentation site generation from markdown |

---

## 6. Recommended Odibi Files/Modules To Study

Ordered by learning value — start at the top.

### Tier 1: Core Architecture (Read These First)

| File | Lines | What You'll Learn |
|---|---|---|
| `odibi/registry.py` | 260 | Registry pattern, `inspect` module, custom decorator, Pydantic validation dispatch |
| `odibi/context.py` | 544 | ABC hierarchy, factory function, threading, SQL dispatch, three context implementations |
| `odibi/engine/base.py` | 334 | Abstract interface design, 14 abstract methods, extension points |
| `odibi/patterns/base.py` | 274 | Pattern ABC, logging helpers, audit columns, multi-engine target loading |
| `odibi/exceptions.py` | 237 | Exception hierarchy, rich error formatting, Spark error parsing |

### Tier 2: Implementation Depth

| File | Lines | What You'll Learn |
|---|---|---|
| `odibi/transformers/sql_core.py` | 1,182 | SQL-first design, 25+ transformers, engine-aware quoting, Pydantic params |
| `odibi/validation/engine.py` | 706 | Multi-engine validation, 11 test types, vectorized operations |
| `odibi/connections/api_fetcher.py` | 1,091 | Pagination strategies, Protocol typing, retry with backoff, generators |
| `odibi/simulation/generator.py` | 1,769 | Dataclasses, NumPy, stateful generation, scheduled events, cross-entity references |
| `odibi/introspect.py` | 1,132 | `inspect` module mastery, auto-documentation, Pydantic field extraction |

### Tier 3: System-Level Patterns

| File | Lines | What You'll Learn |
|---|---|---|
| `odibi/node.py` | 3,501 | Node execution pipeline, phase tracking, context managers, error handling |
| `odibi/pipeline.py` | 3,157 | DAG resolution, ThreadPoolExecutor, context manager protocol, story generation |
| `odibi/catalog.py` | 3,637 | Delta Lake operations, threading, Spark/Pandas dual paths, caching |
| `odibi/config.py` | 4,766 | Pydantic mastery — discriminated unions, cross-field validators, 50+ models |
| `odibi/writers/sql_server_writer.py` | 3,116 | Multi-engine write strategies, SQL generation, schema evolution |

---

## 7. Small Practice Exercises Based On Odibi

Each exercise uses existing Odibi code. No new features — just study and understand.

### Exercise 1: Trace a Pipeline Execution (30 min)
Read `examples/star_schema_e2e/config.yaml`. Draw the DAG by hand (which nodes depend on which). Then read `odibi/graph.py` to see how the framework does it programmatically. Compare your drawing with `odibi graph examples/star_schema_e2e/config.yaml`.

### Exercise 2: Add a Transformer (Dry Run) (45 min)
Read `docs/skills/03_write_a_transformer.md`. Then read `transformers/sql_core.py:row_number` — it's one of the newest transformers. Follow the same pattern to write a `lag` transformer that adds a LAG window function. Write the `LagParams` model, the function, and the registration line. Don't commit.

### Exercise 3: Break a Simulation (30 min)
Pick any notebook from `odibi-simulations/notebooks/` (e.g., `50_weather_stations.ipynb`). Read the "Break It" section. Can you think of a different failure mode for the same config? Write it down in one sentence.

### Exercise 4: Read an Exception Hierarchy (20 min)
Read `odibi/exceptions.py` (237 lines). For each exception class, answer: What information does it include? What would a user do with this information? Then find one place in `node.py` where `NodeExecutionError` is raised and trace what `suggestions` are generated.

### Exercise 5: Compare Engine Implementations (45 min)
Pick `engine/base.py:count_nulls` (abstract). Read the Pandas implementation (`pandas_engine.py`), Spark implementation (`spark_engine.py`), and Polars implementation (`polars_engine.py`). Write down: how does each engine count nulls? Which is the most concise? Which handles edge cases best?

### Exercise 6: Understand the Registry (30 min)
Read `registry.py` end-to-end. Then write down answers to: How does `validate_params` work? What happens if you register two functions with the same name? How does `get_function_info` extract parameter metadata?

### Exercise 7: Trace a Validation Flow (30 min)
Read `examples/quarantine_workflow/config.yaml`. Find the validation tests. Then trace through `validation/engine.py` — which function runs each test? How does `on_fail: quarantine` route to `validation/quarantine.py`?

### Exercise 8: Study the Config Loader (20 min)
Read `utils/config_loader.py`. Find the closures. How does `_substitute_dates` work? What regex does it use? How does it handle `{today-3d}`?

### Exercise 9: Mock PySpark (45 min)
Read `AGENTS.md` "Mock PySpark Setup (Critical Order)". Then read one of the mock-based test files (e.g., `tests/unit/test_catalog_mock_engine_reads.py`). Why does import order matter? What would break if you reversed it?

### Exercise 10: Introspect a Transformer (20 min)
Run `odibi explain scd2` (or read `cli/list_cmd.py:explain_command`). Then read `introspect.py` to understand how `explain` pulls docstrings and Pydantic fields to generate that output. How would you add a new field to `SCD2Params` and have it appear in `explain`?

---

## 8. Prioritized 8–12 Week Review Plan

Each week focuses on one concept area. The exercises reference specific Odibi files. Budget: ~3-5 hours/week.

### Week 1: Python Fundamentals via Odibi Core

**Focus:** ABCs, Enums, Type Hints, Exceptions

| Day | Activity | Files |
|---|---|---|
| 1 | Read `engine/base.py` (269 lines) — identify all abstract methods | `odibi/engine/base.py` |
| 2 | Read `exceptions.py` (237 lines) — trace error formatting | `odibi/exceptions.py` |
| 3 | Read `config.py:20-160` — study all Enum classes | `odibi/config.py` |
| 4 | Exercise 5 (compare engine implementations) | `engine/pandas_engine.py`, `engine/spark_engine.py` |

### Week 2: Pydantic Mastery

**Focus:** BaseModel, Field, Validators, Discriminated Unions

| Day | Activity | Files |
|---|---|---|
| 1 | Read `config.py:1-500` — base models and enums | `odibi/config.py` |
| 2 | Read a transformer params model (e.g., `sql_core.py:FilterRowsParams`) | `odibi/transformers/sql_core.py` |
| 3 | Read discriminated unions: `TestConfig` (~line 3110) | `odibi/config.py` |
| 4 | Exercise 2 (add a transformer dry run) | `docs/skills/03_write_a_transformer.md` |

### Week 3: Patterns and Architecture

**Focus:** Factory, Registry, Context, Polymorphism

| Day | Activity | Files |
|---|---|---|
| 1 | Read `registry.py` (260 lines) end-to-end | `odibi/registry.py` |
| 2 | Read `context.py` (544 lines) — factory + 3 implementations | `odibi/context.py` |
| 3 | Read `connections/factory.py` (379 lines) | `odibi/connections/factory.py` |
| 4 | Exercise 6 (understand the registry) | `odibi/registry.py` |

### Week 4: Data Warehouse Patterns

**Focus:** SCD2, Dimension, Fact, Star Schema

| Day | Activity | Files |
|---|---|---|
| 1 | Read `patterns/dimension.py` (548 lines) | `odibi/patterns/dimension.py` |
| 2 | Read `patterns/fact.py` (745 lines) — focus on FK lookup | `odibi/patterns/fact.py` |
| 3 | Read `examples/star_schema_e2e/config.yaml` + script | `examples/star_schema_e2e/` |
| 4 | Exercise 1 (trace a pipeline execution) | `examples/star_schema_e2e/` |

### Week 5: Data Quality Engineering

**Focus:** Validation, Quarantine, Quality Gates

| Day | Activity | Files |
|---|---|---|
| 1 | Read `validation/engine.py` (706 lines) | `odibi/validation/engine.py` |
| 2 | Read `validation/quarantine.py` + `gate.py` | `odibi/validation/quarantine.py`, `gate.py` |
| 3 | Read `examples/quarantine_workflow/` | `examples/quarantine_workflow/` |
| 4 | Exercise 7 (trace a validation flow) | Validation files |

### Week 6: SQL and Engine Abstraction

**Focus:** DuckDB bridging, SQL-first transformers, Engine dispatch

| Day | Activity | Files |
|---|---|---|
| 1 | Read `transformers/sql_core.py` — first 500 lines | `odibi/transformers/sql_core.py` |
| 2 | Read `context.py:107-128` — `EngineContext.sql()` dispatch | `odibi/context.py` |
| 3 | Read `transformers/scd.py` — 5 different SQL dialects for NaN-safe comparison | `odibi/transformers/scd.py` |
| 4 | Exercise 10 (introspect a transformer) | `odibi/introspect.py`, CLI |

### Week 7: Testing Patterns

**Focus:** pytest, mocking, coverage, test organization

| Day | Activity | Files |
|---|---|---|
| 1 | Read `AGENTS.md` "Testing Gotchas" section | `AGENTS.md` |
| 2 | Read `tests/unit/validation/test_validation_engine.py` — 76 tests | Test file |
| 3 | Read `tests/unit/writers/test_sql_server_writer_coverage.py` — 208 tests | Test file |
| 4 | Exercise 9 (mock PySpark) | Mock test files |

### Week 8: Simulation and Domain Modeling

**Focus:** YAML-driven simulation, generator types, cross-entity references

| Day | Activity | Files |
|---|---|---|
| 1 | Read `simulation/generator.py:1-200` — data structures and generator types | `odibi/simulation/generator.py` |
| 2 | Read 2-3 simulation configs from `odibi-simulations/configs/oneshot/` | Config files |
| 3 | Run a notebook from `odibi-simulations/notebooks/` | Notebook |
| 4 | Exercise 3 (break a simulation) | Notebook |

### Week 9: CLI and Developer Tooling

**Focus:** argparse, introspection, template generation

| Day | Activity | Files |
|---|---|---|
| 1 | Read `cli/main.py` (391 lines) — subcommand dispatch, lazy imports | `odibi/cli/main.py` |
| 2 | Read `cli/list_cmd.py` (453 lines) — introspection-powered discovery | `odibi/cli/list_cmd.py` |
| 3 | Read `introspect.py` (1,132 lines) — auto-doc generation | `odibi/introspect.py` |
| 4 | Run `odibi list transformers`, `odibi explain scd2`, `odibi templates show validation` | CLI |

### Week 10: Concurrency and State

**Focus:** Threading, locks, state backends, Delta Lake

| Day | Activity | Files |
|---|---|---|
| 1 | Read `state/__init__.py` — 3 state backends | `odibi/state/__init__.py` |
| 2 | Read threading in `pipeline.py:732-741` and `context.py:20-29` | Threading files |
| 3 | Read `LESSONS_LEARNED.md` T-001, T-010, T-011 — real concurrency bugs | `docs/LESSONS_LEARNED.md` |
| 4 | Exercise 8 (study config loader closures) | `odibi/utils/config_loader.py` |

### Week 11: Observability and Documentation

**Focus:** Execution stories, lineage, error suggestions

| Day | Activity | Files |
|---|---|---|
| 1 | Read `story/generator.py` — first 500 lines | `odibi/story/generator.py` |
| 2 | Read `story/lineage.py` — lineage graph stitching | `odibi/story/lineage.py` |
| 3 | Read `utils/error_suggestions.py` (671 lines) — pattern-matched suggestions | `odibi/utils/error_suggestions.py` |
| 4 | Run `odibi story last` on a pipeline to see generated output | CLI |

### Week 12: Synthesis and Gaps

**Focus:** Review gaps, try external concepts

| Day | Activity | Resource |
|---|---|---|
| 1 | Write an async version of one `api_fetcher.py` method using `aiohttp` | External study |
| 2 | Add `__slots__` to one dataclass and measure memory difference | External study |
| 3 | Rewrite one engine dispatch in `sql_core.py` using `match/case` (Python 3.10+) | External study |
| 4 | Write a summary: "What I learned about Python from building Odibi" | Personal reflection |

---

## Appendix: Quick Reference — Where To Find Each Concept

| Concept | Primary File(s) |
|---|---|
| ABCs | `engine/base.py`, `connections/base.py`, `patterns/base.py`, `context.py` |
| Pydantic | `config.py`, all `*Params` models in `transformers/` |
| Factory pattern | `connections/factory.py`, `context.py:522-544`, `engine/registry.py` |
| Registry pattern | `registry.py` |
| Enums | `config.py:20-160`, `enums.py` |
| Decorators | `registry.py:195-247` |
| Context managers | `pipeline.py:264-268`, `node.py:53-111`, `testing/fixtures.py` |
| Threading | `context.py`, `catalog.py`, `pipeline.py`, `azure_adls.py` |
| Generators/yield | `api_fetcher.py:1094-1137`, `node.py` context managers |
| Regex | `config.py:16`, `context.py:118`, `api_fetcher.py:226-276`, `exceptions.py:135` |
| Closures | `utils/config_loader.py:130-179`, `engine/pandas_engine.py:1817-1885` |
| inspect module | `registry.py`, `introspect.py`, `cli/list_cmd.py`, `node.py` |
| Protocol typing | `api_fetcher.py:429-438` |
| Dataclasses | `simulation/generator.py:33-59`, `validation/gate.py:17`, `diagnostics/diff.py` |
| f-strings | Everywhere (896+ occurrences) |
| Comprehensions | Everywhere (337+ occurrences) |
| Star unpacking | `engine/*.py`, `connections/*.py`, `patterns/base.py` |
| Try/except guards | Everywhere (117+ occurrences) |
| Exception hierarchy | `exceptions.py` |
| Error suggestions | `utils/error_suggestions.py` |
| DWH patterns | `patterns/dimension.py`, `patterns/fact.py`, `transformers/scd.py` |
| Data quality | `validation/engine.py`, `validation/quarantine.py`, `validation/gate.py` |
| Simulation | `simulation/generator.py` |
| CLI tooling | `cli/main.py`, `cli/list_cmd.py`, `introspect.py` |
| State management | `state/__init__.py`, `catalog.py` |
| Observability | `story/generator.py`, `story/lineage.py` |
