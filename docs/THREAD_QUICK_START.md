# Thread Quick Start — Read This First

> **Purpose:** Save every Amp thread 5-10 minutes of exploration time and $1-3 of credits.
> Instead of searching the codebase, read this file to understand where everything is.

---

## Project Overview

**odibi** is a Python data pipeline framework for building enterprise data warehouses.
It orchestrates nodes (read → transform → validate → write) with dependency resolution,
supports Pandas/Spark/Polars engines, and provides patterns for common DWH tasks.

---

## Source Layout (with line counts)

```
odibi/                          # Core framework
├── config.py            (4766) # Pydantic config models — source of truth
├── catalog.py           (3637) # System catalog — pipeline/node metadata tracking
├── catalog_sync.py      (1310) # Catalog sync to cloud storage
├── node.py              (3501) # Node execution engine — read/transform/validate/write
├── pipeline.py          (3157) # Pipeline orchestration — DAG resolution, execution
├── context.py            (428) # SparkContext/PandasContext — temp view registration
├── project.py            (464) # Project-level config and discovery
├── derived_updater.py   (2053) # Derived column computation
├── graph.py              (397) # DAG graph utilities
├── lineage.py            (426) # Lineage tracking
├── references.py         (120) # Cross-pipeline references
├── registry.py           (210) # Plugin registry
├── plugins.py             (63) # Plugin loading
├── introspect.py        (1132) # CLI introspection — generates docs from code
├── exceptions.py         (237) # Custom exception hierarchy
│
├── engine/
│   ├── base.py           (269) # Abstract engine interface
│   ├── pandas_engine.py (2655) # Pandas/DuckDB engine — main CI engine
│   ├── spark_engine.py  (2327) # Spark engine — tested in Databricks only
│   ├── polars_engine.py (1336) # Polars engine
│   └── registry.py        (38) # Engine registry
│
├── patterns/
│   ├── base.py           (226) # Pattern base class
│   ├── scd2.py           (165) # SCD Type 2 — dual Spark/Pandas
│   ├── merge.py          (138) # Key-based merge/upsert
│   ├── dimension.py      (548) # Dimension builder with surrogate keys
│   ├── fact.py           (745) # Fact builder with SK lookups
│   ├── aggregation.py    (477) # Declarative aggregation
│   └── date_dimension.py (404) # Date dimension generator
│
├── transformers/
│   ├── __init__.py             # register_standard_library — 54 transformers
│   ├── advanced.py      (1199) # sessionize, split_events, fill_missing
│   ├── sql_core.py      (1182) # SQL-based transforms (DuckDB/Spark SQL)
│   ├── manufacturing.py (1077) # Phase detection, status tracking, metadata
│   ├── thermodynamics.py (882) # CoolProp fluid properties, psychrometrics
│   ├── scd.py            (841) # SCD2 transformer — Pandas/DuckDB/Spark
│   ├── merge_transformer.py(744) # Merge transformer — Pandas/DuckDB/Spark
│   ├── relational.py     (568) # Join, lookup, aggregate transforms
│   ├── delete_detection.py(525) # CDC-like snapshot diff
│   ├── units.py          (448) # Unit conversion (Pandas/Polars)
│   └── validation.py     (142) # Transformer validation helpers
│
├── validation/
│   ├── engine.py         (706) # Core validation — 11 test types × Pandas/Polars/Spark
│   ├── quarantine.py     (501) # Split valid/invalid, quarantine metadata
│   ├── gate.py           (208) # Quality gates — pass/fail/warn
│   ├── fk.py             (447) # Foreign key validation
│   └── explanation_linter.py(126) # Validation explanation linting
│
├── connections/
│   ├── base.py           (135) # Abstract connection
│   ├── factory.py        (379) # Connection factory
│   ├── local.py          (525) # Local filesystem
│   ├── azure_adls.py     (910) # Azure Data Lake Storage
│   ├── azure_sql.py     (1300) # Azure SQL / SQL Server
│   ├── postgres.py       (987) # PostgreSQL
│   ├── api_fetcher.py   (1091) # HTTP API fetcher
│   └── http.py            (61) # Simple HTTP connection
│
├── writers/
│   └── sql_server_writer.py(3116) # SQL Server bulk writer + merge
│
├── cli/                        # Click CLI commands
│   ├── main.py           (391) # CLI entry point + common options
│   ├── catalog.py        (660) # catalog commands
│   ├── system.py         (512) # system commands
│   ├── list_cmd.py       (453) # list commands
│   ├── story.py          (430) # story commands
│   ├── lineage.py        (206) # lineage commands
│   ├── schema.py         (163) # schema commands
│   ├── templates.py      (178) # template commands
│   ├── test.py           (233) # test commands
│   ├── secrets.py        (177) # secrets commands
│   ├── init_pipeline.py  (246) # init-pipeline command
│   ├── graph.py          (122) # graph commands
│   ├── export.py          (54) # export commands
│   └── doctor.py         (127) # doctor diagnostic commands
│
├── semantics/                  # Semantic layer
│   ├── story.py          (431) # Semantic story
│   ├── query.py          (591) # DuckDB-backed semantic queries
│   ├── runner.py         (433) # Semantic runner
│   ├── views.py          (359) # Semantic views
│   ├── materialize.py    (322) # Materialization
│   └── metrics.py        (314) # Metric definitions
│
├── story/                      # Documentation generation
│   ├── generator.py     (1406) # Story generator
│   ├── doc_generator.py (1373) # Doc generator
│   ├── lineage.py        (899) # Story lineage
│   ├── metadata.py       (523) # Story metadata
│   ├── doc_story.py      (462) # Doc story
│   ├── renderers.py      (383) # Markdown/HTML renderers
│   ├── lineage_utils.py  (248) # Lineage utilities
│   └── themes.py         (174) # Rendering themes
│
├── simulation/
│   └── generator.py     (1769) # Synthetic data generation
│
├── utils/
│   ├── logging_context.py(627) # Structured logging singleton
│   ├── error_suggestions.py(671) # Error message suggestions
│   ├── alerting.py       (740) # Alert routing (Teams, email, etc.)
│   ├── config_loader.py  (455) # YAML config loading
│   ├── progress.py       (387) # Progress tracking
│   ├── setup_helpers.py  (252) # Environment setup
│   ├── console.py        (179) # Console output helpers
│   ├── content_hash.py   (161) # Content hashing
│   ├── logging.py        (164) # Logging setup
│   ├── telemetry.py       (97) # Usage telemetry
│   ├── encoding.py        (80) # Encoding detection
│   ├── hashing.py         (45) # Hash helpers
│   ├── duration.py        (35) # Duration formatting
│   └── extensions.py      (23) # Extension loading
│
├── tools/
│   ├── adf_profiler.py  (1226) # Azure Data Factory profiler
│   └── templates.py     (1037) # Template generator
│
├── testing/
│   ├── assertions.py      (59) # DataFrame assertion helpers
│   ├── fixtures.py        (67) # Test fixtures
│   └── source_pool.py    (216) # Test data pools
│
├── diagnostics/
│   ├── delta.py          (423) # Delta table diagnostics
│   ├── diff.py           (127) # DataFrame diff
│   └── manager.py        (143) # Diagnostics manager
│
└── ui/
    └── app.py            (157) # Optional web UI
```

## Test Layout

```
tests/
├── conftest.py                 # ⚠️ Skip filter: files with "spark"/"delta" in name
├── unit/                       # Main test directory
│   ├── engine/                 # Engine tests
│   ├── transformers/           # Transformer tests
│   ├── validation/             # Validation tests
│   ├── connections/            # Connection tests
│   ├── writers/                # Writer tests
│   ├── semantics/              # Semantic layer tests
│   ├── story/                  # Story tests
│   ├── utils/                  # Utility tests
│   └── mcp/                    # MCP tests
├── integration/                # Integration tests
└── benchmarks/                 # Performance benchmarks
```

## Environment Facts

| What | Status |
|------|--------|
| Python | 3.12.10 |
| PySpark | 4.1.1 installed, **no JVM** — import works, runtime doesn't |
| Polars | ✅ Installed |
| CoolProp | ✅ 7.2.0 installed |
| DuckDB | ✅ Available via pandas_engine |
| OS | Windows 10 |

## Critical Rules for Test Files

1. **No "spark" or "delta" in test filenames** — `conftest.py` skips them on Windows
2. **No `caplog`** — logging context pollution causes batch failures
3. **Assert behavior, not logs** — return values, side effects, exceptions
4. **Mock Spark, don't run it** — `unittest.mock.MagicMock` for SparkSession
5. **Test files go in `tests/unit/<module>/`** matching source structure

## How to Run Coverage for a Specific Module

```bash
# Single module
pytest tests/ --cov=odibi.validation.engine --cov-report=term-missing -q

# Module directory
pytest tests/ --cov=odibi.patterns --cov-report=term-missing -q

# Full project
pytest tests/ --ignore=tests/benchmarks --cov=odibi --cov-report=term -q
```

## Current Coverage Roadmap

**See AGENTS.md → "Coverage Roadmap: 66% → 80%"** for the phased plan with progress tracker.

## Key Docs

| Doc | What it covers |
|-----|----------------|
| `AGENTS.md` | Agent instructions, gotchas, coverage plan |
| `docs/ODIBI_DEEP_CONTEXT.md` | Full framework reference (2,200 lines) |
| `docs/THREAD_QUICK_START.md` | This file — structure reference |

## Don't Read These Unless Needed

These are large files. Don't read them "just to understand" — use this doc instead:
- `config.py` (4,766 lines) — only read if working on config
- `node.py` (3,501 lines) — only read if working on node execution
- `pipeline.py` (3,157 lines) — only read if working on pipeline orchestration
- `catalog.py` (3,637 lines) — only read if working on catalog
- `ODIBI_DEEP_CONTEXT.md` (2,200 lines) — only read if you need feature details
