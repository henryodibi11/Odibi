# Odibi Developer Cheatsheet

**The internal map. Where things live, how they connect, and where to look when something breaks.**

---

## Class Chain (Runtime Execution)

```
PipelineManager.from_yaml("project.yaml", env="dev")
    │
    ├── load_yaml_with_env()          # config_loader.py — loads, merges, substitutes
    │       ├── process imports
    │       ├── apply inline environments: block
    │       ├── apply external env.{env}.yaml
    │       ├── substitute ${vars.xxx}
    │       └── substitute ${date:...}
    │
    ├── ProjectConfig(**merged_dict)   # config.py — Pydantic validation
    │
    └── PipelineManager.__init__()     # pipeline.py
            │
            ├── for each PipelineConfig:
            │       │
            │       └── Pipeline.__init__()
            │               ├── EngineClass()           # engine/ — pandas, spark, or polars
            │               ├── create_context(engine)  # context.py → PandasContext/SparkContext/PolarsContext
            │               └── DependencyGraph(nodes)  # graph.py — execution order
            │
            └── Pipeline.run()
                    │
                    └── for each node (in graph order):
                            │
                            └── Node.run()              # node.py — orchestrator (retry, restore)
                                    │
                                    └── NodeExecutor     # node.py — engine room
                                            ├── read_phase()      → connection.read()
                                            ├── transform_phase() → EngineContext wraps df + Context
                                            ├── validate_phase()  → quality gates, quarantine
                                            └── write_phase()     → connection.write()
```

---

## Key Files and What They Own

| File | Responsibility | When to look here |
|---|---|---|
| `config.py` | Pydantic models, YAML schema, validation | Config errors, adding a new config field |
| `utils/config_loader.py` | YAML loading, imports, env merging, var substitution | Merge order issues, `${VAR}` problems |
| `pipeline.py` | PipelineManager, Pipeline.run(), connection building | Pipeline orchestration, `--env` handling |
| `node.py` | Node lifecycle, NodeExecutor (read/transform/write) | Node failures, retry, incremental loading |
| `context.py` | Dataset registry (Context), transform API (EngineContext) | "Dataset not found", SQL in transforms |
| `graph.py` | Dependency resolution, execution layers | Node ordering, circular dependency errors |
| `catalog.py` | System Catalog (run tracking, state, metadata) | Run history, state persistence |
| `engine/pandas_engine.py` | Pandas read/write/SQL execution | Read failures, write issues (local/pandas) |
| `engine/spark_engine.py` | Spark read/write/SQL execution | Databricks issues |
| `engine/polars_engine.py` | Polars read/write/SQL execution | Polars-specific issues |
| `connections/` | Local, ADLS, SQL Server, HTTP, DBFS connections | Auth errors, path resolution |
| `patterns/` | Dimension, Fact, SCD2, Merge, Aggregation | Pattern-specific logic |
| `transformers/` | 56 individual transform functions | Adding/fixing transforms |
| `validation/` | Quality gates, quarantine, FK validation | Data quality issues |

---

## Two Things Named "Context"

```
Context (abstract base)                    EngineContext
├── PandasContext   ← dict of DataFrames   ├── .context  → one of the Context types
├── SparkContext    ← Spark temp views      ├── .df       → current DataFrame
└── PolarsContext   ← dict of DataFrames    ├── .sql()    → run SQL
                                            └── .get()    → access other datasets
```

- **Context** = the global dataset store (holds all named DataFrames in the pipeline)
- **EngineContext** = what your transform function receives (current df + access to other datasets)
- `Pipeline.__init__` creates the Context → nodes register outputs into it → downstream nodes read from it

---

## Environment Override Merge Order

```
1. Load project.yaml
2. Process imports (recursive, deep-merged)
3. Apply inline environments:{env}: block (validated — restricted fields)
4. Load env.{env}.yaml (auto-discovered from same directory)
5. Process env file's imports (recursive)
6. Deep-merge env data on top (pipelines APPENDED, dicts merged, scalars overwritten)
7. Substitute ${vars.xxx}
8. Substitute ${date:...}
9. ProjectConfig(**final_dict) — Pydantic validation
```

**Overrideable fields (inline environments block):**
engine, connections, system, performance, logging, retry, alerts, story, lineage, vars, pipelines

**External env.{env}.yaml:** Can override anything (no field restriction).

---

## Config Hierarchy

```
ProjectConfig
├── project: str                    # Project name
├── engine: str                     # "pandas" | "spark" | "polars"
├── connections: Dict[str, Dict]    # Named connection configs
├── pipelines: List[PipelineConfig] # Pipeline definitions
│       └── PipelineConfig
│           ├── pipeline: str       # Pipeline name
│           ├── nodes: List[NodeConfig]
│           │       └── NodeConfig
│           │           ├── name, source, target
│           │           ├── read: ReadConfig
│           │           ├── write: WriteConfig
│           │           ├── transformer: str
│           │           ├── params: Dict
│           │           ├── validation: List[ValidationTest]
│           │           └── depends_on: List[str]
│           └── on_error: str       # fail_fast | fail_later | ignore
├── story: StoryConfig              # Story generation settings
├── system: SystemConfig            # System Catalog settings
├── vars: Dict[str, Any]            # Variable substitution
├── environments: Dict[str, Dict]   # Inline env overrides
├── retry: RetryConfig              # Retry settings
├── alerts: List[AlertConfig]       # Alert destinations
├── performance: PerformanceConfig  # Tuning options
└── lineage: LineageConfig          # OpenLineage settings
```

---

## Node Execution Phases

```
Node.run()
│
├── 1. READ PHASE
│   ├── connection.read(path, format, ...)
│   ├── Incremental loading (time_travel, watermark)
│   └── Registers output in Context
│
├── 2. TRANSFORM PHASE
│   ├── Built-in transformer (from registry)
│   ├── OR custom function (your Python code)
│   ├── Gets EngineContext(context=Context, df=current_df)
│   └── Can chain: SQL → transform → SQL
│
├── 3. VALIDATE PHASE
│   ├── Quality gates (not_null, unique, range, regex, ...)
│   ├── FK validation (cross-dataset referential integrity)
│   └── Quarantine (bad rows → separate output)
│
└── 4. WRITE PHASE
    ├── connection.write(df, path, mode, ...)
    ├── Modes: overwrite, append, merge, upsert
    └── Delta Lake features: partitioning, Z-order, schema evolution
```

---

## Testing Patterns (90% Coverage)

```python
# Pattern 1: Config validation
def test_invalid_config():
    with pytest.raises(ValidationError):
        ProjectConfig(**bad_dict)

# Pattern 2: Mock a connection
@patch("odibi.node.LocalConnection")
def test_node_read(MockConn):
    mock_conn = MockConn.return_value
    mock_conn.read.return_value = sample_df

# Pattern 3: Temp YAML files
def test_env_override():
    with tempfile.TemporaryDirectory() as tmpdir:
        # create files, load, assert

# Pattern 4: Transformer test
def test_rename():
    df = pd.DataFrame({"old": [1, 2]})
    result = rename_columns(df, mapping={"old": "new"})
    assert "new" in result.columns
```

**Key rule:** `@patch` where it's USED, not where it's DEFINED.

---

## CLI Quick Reference

```bash
# Running pipelines
odibi run project.yaml                    # Run all pipelines
odibi run project.yaml --env prod         # Run with environment
odibi run project.yaml --dry-run          # Validate without executing

# Introspection
odibi list transformers                   # All 56 transformers
odibi list patterns                       # All 6 patterns
odibi list connections                    # All connection types
odibi explain <name>                      # Detailed docs for any feature

# Templates
odibi templates show azure_blob           # YAML template with all options
odibi templates transformer scd2          # Transformer params + example

# Development
pytest tests/ -v                          # Run all tests
ruff check . --fix                        # Lint and fix
python odibi/introspect.py                # Regenerate YAML schema docs
```

---

## Module Index (What's Inside Each File)

### `config.py` — Pydantic Models
```
Key Classes:
  ProjectConfig          — top-level config, holds everything
  PipelineConfig         — pipeline name + list of NodeConfigs
  NodeConfig             — name, source, target, read, write, transformer, params, validation
  ReadConfig             — connection, path, format, time_travel, incremental
  WriteConfig            — connection, path, mode (overwrite/append/merge/upsert)
  ConnectionConfig       — type, base_path, account_name, auth_mode
  SystemConfig           — catalog connection + path
  StoryConfig            — story generation settings
  ValidationTest         — not_null, unique, range, regex, custom_sql, row_count

Key Validators:
  validate_environments_structure()  — checks overrideable fields
  validate_story_connection_exists() — ensures story connection is defined
  ensure_system_config()             — ensures system block exists

Key Function:
  load_config_from_file(path)        — loads YAML → ProjectConfig (no env support)
```

### `utils/config_loader.py` — YAML Loading & Merging
```
Key Functions:
  load_yaml_with_env(path, env)  — THE main loader (imports, env merge, var substitution)
  _deep_merge(base, override)    — merge rules: dicts=recursive, pipelines=append, else=overwrite
  _substitute_vars(data, vars)   — resolves ${vars.xxx} placeholders
  _substitute_dates(data)        — resolves ${date:today}, ${date:-7d}, etc.
  _normalize_pattern_to_transformer(data) — converts pattern: block to transformer: + params:
  _tag_nodes_with_source(data, path)      — tags nodes with source YAML for sql_file resolution
```

### `pipeline.py` — Orchestration
```
PipelineManager (line 1752):
  from_yaml(path, env)       — loads config, builds connections, creates Pipeline instances
  run(pipelines, dry_run)    — runs one/many/all pipelines
  get_pipeline(name)         — get a specific Pipeline instance
  deploy(pipelines)          — registers pipelines in System Catalog
  register_outputs()         — pre-registers node outputs for cross-pipeline refs
  flush_stories()            — waits for async story writes to complete

Pipeline (line 124):
  run(dry_run, parallel, tag, node)  — runs nodes in graph order
  register_outputs()                 — pre-registers this pipeline's outputs
  run_node(name, mock_data)          — run a single node (for debugging)
  flush_stories(timeout)             — flush pending story writes

PipelineResults (line 38):
  get_node_result(name)   — get result for a specific node
  to_dict()               — serialize results
  debug_summary()         — human-readable summary
```

### `node.py` — Node Lifecycle & Execution
```
Node (line 3469) — orchestrator:
  execute()           — full lifecycle: restore → execute → retry
  restore()           — check if node can be restored from state
  get_version_hash()  — hash for change detection

NodeExecutor (line 116) — engine room:
  execute()                        — runs all 4 phases
  _execute_read_phase()            — reads data from connection
  _apply_incremental_filtering()   — watermark/time_travel filtering
  _execute_transform_phase()       — runs transformer or custom function
  _execute_transformer_node()      — looks up transformer in registry
  _execute_transform()             — calls the actual transform function
  _execute_validation_phase()      — quality gates, FK checks
  _execute_write_phase()           — writes to connection
  _resolve_sql_file()              — resolves sql_file: paths relative to YAML source

NodeResult (line 79):
  status, row_count, duration, error, timings
```

### `context.py` — Dataset Store & Transform API
```
Context (abstract, line 128) — global dataset registry:
  register(name, df)      — store a DataFrame by name
  get(name)               — retrieve a DataFrame by name
  list_datasets()         — list all registered names

  Implementations:
    PandasContext   — stores DataFrames in a dict
    SparkContext    — registers as Spark temp views
    PolarsContext   — stores DataFrames in a dict

EngineContext (line 32) — per-transform wrapper:
  .df                     — current DataFrame
  .context                — the Context (access other datasets)
  .columns                — column names of current df
  .schema                 — column types
  get(name)               — shortcut to context.get(name)
  sql(query)              — run SQL, returns new EngineContext
  with_df(df)             — create new context with different df

create_context(engine)    — factory: returns PandasContext/SparkContext/PolarsContext
```

### `graph.py` — Dependency Resolution
```
DependencyGraph (line 12):
  topological_sort()           — returns nodes in execution order
  get_execution_layers()       — groups nodes into parallel-safe layers
  get_dependencies(node)       — what does this node depend on?
  get_dependents(node)         — what depends on this node?
  get_independent_nodes()      — nodes with no dependencies
  check_cross_pipeline_cycles() — validates no circular refs across pipelines
  visualize()                  — ASCII visualization of the graph
```

### `catalog.py` — System Catalog
```
CatalogManager (line 144):
  bootstrap()                     — creates meta tables if they don't exist
  log_run(pipeline, node, ...)    — record a node execution
  log_runs_batch(records)         — batch record multiple runs
  register_pipelines_batch(...)   — register pipeline definitions
  register_nodes_batch(...)       — register node definitions
  register_outputs_batch(...)     — register node outputs for cross-pipeline refs
  get_node_output(pipeline, node) — look up a registered output
  get_registered_pipeline(name)   — get pipeline registration
  get_run_ids(pipeline)           — get run IDs for a pipeline
  record_lineage(...)             — record data lineage
  cleanup_orphans(config)         — remove stale registrations
  invalidate_cache()              — clear cached registrations

Meta Tables:
  meta_runs       — execution history
  meta_state      — node state (last run, watermarks)
  meta_pipelines  — pipeline registrations
  meta_nodes      — node registrations
  meta_outputs    — output registrations
  meta_schemas    — schema tracking
  meta_lineage    — data lineage records
  meta_metrics    — quality metrics
```

---

## Where to Look When Things Break

| Symptom | Start here |
|---|---|
| Config validation error | `config.py` — find the `@model_validator` |
| "Missing environment variable" | `config_loader.py` — `replace_env()` |
| Pipeline not found | `pipeline.py` — `self._pipelines` dict |
| Node read failure | `node.py` — `read_phase()`, then `connections/` |
| Transform error | `node.py` — `transform_phase()`, then `transformers/` |
| "Dataset not found" | `context.py` — check `context.register()` / `context.get()` |
| Wrong execution order | `graph.py` — check `depends_on` in node configs |
| State/run tracking issue | `catalog.py` — `register_run()`, `update_state()` |
| Env override not applying | `config_loader.py` — trace merge order (lines 470-508) |
| Duplicate pipeline warning | `pipeline.py` — last definition wins |
