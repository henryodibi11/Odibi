# Architecture Guide - Odibi System Design

**Visual guide to how Odibi works. See the big picture!**

---

## System Overview

```
┌─────────────────────────────────────────────────────────────┐
│                        USER                                  │
│                          │                                   │
│                          ▼                                   │
│             config.yaml / CLI / Python API                   │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   CONFIG LAYER                               │
│                                                               │
│  config.yaml → Pydantic Models → ProjectConfig               │
│                     ↓                                         │
│              Validation happens here                          │
│  Covers: connections, nodes, transforms, simulation,          │
│          validation, patterns, semantics, scheduling          │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                  PIPELINE LAYER                              │
│                                                               │
│  PipelineManager → Pipeline → DependencyGraph                │
│                                    ↓                          │
│              Topological sort → execution order               │
│              Cross-entity references resolved                 │
│              Incremental loading (rolling/stateful)           │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   NODE LAYER                                 │
│                                                               │
│  Node → Read / Transform / Validate / Write                  │
│           ↓          ↓           ↓         ↓                  │
│     Connections  Transformers  Quality   Engine               │
│                  + Patterns    Gates                          │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   ENGINE LAYER                               │
│                                                               │
│  PandasEngine / SparkEngine / PolarsEngine                   │
│           ↓                                                   │
│  Simulation Generator (stateful data generation)             │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   VALIDATION LAYER                           │
│                                                               │
│  Validation Engine → Quality Gates → Quarantine              │
│  FK Validation → Explanation Linter                          │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   STATE & METADATA LAYER                     │
│                                                               │
│  System Catalog (Delta Tables) ←→ OpenLineage Emitter        │
│  Catalog Sync (cross-catalog)      Diagnostics               │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   STORAGE LAYER                              │
│                                                               │
│  Connections → Local / ADLS / Azure SQL / HTTP / DBFS        │
│  Writers → SQL Server (bulk copy)                            │
│  Formats → CSV / Parquet / Delta / JSON / SQL                │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   STORY LAYER                                │
│                                                               │
│  Run Stories → Doc Stories → Diff Stories                     │
│  Metadata → Renderers → HTML/MD/JSON                         │
│  Themes → Automatic audit trail                              │
└─────────────────────────────────────────────────────────────┘
```

---

## Module Map

The `odibi/` package contains the following modules:

```
odibi/
├── cli/                 # CLI entry point (20+ subcommands)
├── connections/         # Storage backends
│   ├── local.py         #   Local filesystem
│   ├── azure_adls.py    #   Azure Data Lake Storage
│   ├── azure_sql.py     #   Azure SQL Database
│   ├── http.py          #   HTTP/REST API
│   ├── local_dbfs.py    #   Databricks DBFS
│   ├── api_fetcher.py   #   API data fetching
│   └── factory.py       #   Connection factory + DeltaCatalog
├── diagnostics/         # Delta diagnostics, diff analysis
├── discovery/           # Type discovery utilities
├── doctor/              # Health checks
├── engine/              # Execution engines
│   ├── pandas_engine.py #   Pandas (default, local)
│   ├── spark_engine.py  #   PySpark (distributed)
│   └── polars_engine.py #   Polars (alternative local)
├── orchestration/       # External orchestrator integration
│   ├── airflow.py       #   Apache Airflow DAG generation
│   └── dagster.py       #   Dagster asset generation
├── patterns/            # Data modeling patterns
│   ├── dimension.py     #   Dimension tables
│   ├── fact.py          #   Fact tables
│   ├── scd2.py          #   Slowly Changing Dimension Type 2
│   ├── merge.py         #   Merge/upsert
│   ├── aggregation.py   #   Aggregation rollups
│   └── date_dimension.py#   Date dimension generation
├── scaffold/            # Project scaffolding / init
├── semantics/           # Semantic layer
│   ├── metrics.py       #   Metric definitions
│   ├── materialize.py   #   Metric materialization
│   ├── views.py         #   Semantic views
│   └── query.py         #   Semantic queries
├── simulation/          # Data simulation engine
│   └── generator.py     #   Stateful generators (prev, ema, pid, etc.)
├── state/               # State management (catalog backend)
├── story/               # Story generation
│   ├── generator.py     #   Run story generator
│   ├── doc_story.py     #   Documentation stories
│   ├── renderers.py     #   HTML/MD/JSON renderers
│   ├── themes.py        #   Visual themes
│   └── metadata.py      #   Execution metadata
├── testing/             # Test utilities
├── tools/               # Internal tooling
├── transformers/        # 50+ registered transformers
├── ui/                  # Terminal UI
├── utils/               # Shared utilities
├── validate/            # YAML/config validation
├── validation/          # Data quality validation
│   ├── engine.py        #   Validation test runner
│   ├── quarantine.py    #   Bad-row quarantine routing
│   ├── fk.py            #   Foreign key validation
│   └── gate.py          #   Quality gates
├── writers/             # Specialized writers
│   └── sql_server_writer.py  # SQL Server bulk copy
├── config.py            # Pydantic models (5000+ lines)
├── context.py           # DataFrame storage during execution
├── graph.py             # DAG dependency resolution
├── node.py              # Node execution logic
├── pipeline.py          # Pipeline + PipelineManager
├── catalog.py           # System catalog
├── catalog_sync.py      # Cross-catalog sync
├── references.py        # Cross-entity reference resolution
├── derived_updater.py   # Derived column computation
├── registry.py          # Transformer registry (singleton)
├── lineage.py           # OpenLineage integration
├── introspect.py        # Schema introspection for docs
├── plugins.py           # Plugin system
├── project.py           # Project-level operations
├── enums.py             # Shared enums
├── constants.py         # Shared constants
└── exceptions.py        # Custom exceptions
```

---

## Pipeline Execution Flow

### What Happens When You Run `odibi run config.yaml`

```
1. CLI Entry Point (cli/main.py)
   │
   ├─→ Parse arguments (tags, dry-run, engine override, etc.)
   └─→ Call run_command(args)

2. Load Configuration (cli/run.py)
   │
   ├─→ Read YAML file
   ├─→ Variable substitution (${vars.env}, env vars)
   ├─→ Parse to ProjectConfig (Pydantic validation)
   └─→ Create PipelineManager

3. Build Dependency Graph (graph.py)
   │
   ├─→ Extract all nodes across pipelines
   ├─→ Build dependency edges (explicit + auto-inferred)
   ├─→ Resolve cross-entity references ($pipeline.node)
   ├─→ Check for cycles
   └─→ Topological sort → execution order

4. Execute Nodes (pipeline.py → node.py)
   │
   ├─→ For each node in order:
   │   │
   │   ├─→ Check: enabled? tag filter? skip?
   │   ├─→ Smart Read (incremental detection)
   │   │   ├─→ First run? → Full load (or first_run_query)
   │   │   └─→ Subsequent? → Rolling window / stateful filter
   │   │
   │   ├─→ Simulation (if simulation config present)
   │   │   └─→ Generator produces stateful time-series data
   │   │
   │   ├─→ Transformer (if configured) → Pattern execution
   │   │   └─→ SCD2, Merge, Dimension, Fact, Aggregation, etc.
   │   │
   │   ├─→ Transform Steps (if configured)
   │   │   └─→ SQL, operations, custom functions in sequence
   │   │
   │   ├─→ Validation (if configured)
   │   │   ├─→ Run validation tests
   │   │   ├─→ Quality gate check
   │   │   └─→ Route failures to quarantine (if enabled)
   │   │
   │   ├─→ Write → Engine.write(DataFrame)
   │   ├─→ Store result in Context
   │   ├─→ Update state catalog (watermarks, row counts)
   │   └─→ Track metadata (timing, rows, schema)
   │
   └─→ All nodes complete

5. Generate Story (story/generator.py)
   │
   ├─→ Collect all node metadata
   ├─→ Calculate aggregates (success rate, total rows)
   ├─→ Render to HTML/MD/JSON with theme
   └─→ Save to stories/runs/

6. Return to User
   │
   └─→ "Pipeline completed successfully"
```

---

## Module Dependencies

### Core Dependency Chain

```
config.py (no dependencies - pure Pydantic models)
    ↓
context.py (stores DataFrames between nodes)
    ↓
registry.py (transformer registration singleton)
    ↓
transformers/ (50+ operations, registered via @transform)
    ↓
engine/ (PandasEngine / SparkEngine / PolarsEngine)
    ↓
connections/ (Local / ADLS / AzureSQL / HTTP / DBFS)
    ↓
validation/ (quality engine, quarantine, FK checks, gates)
    ↓
patterns/ (SCD2, Merge, Dimension, Fact, Aggregation, DateDimension)
    ↓
simulation/ (stateful data generators)
    ↓
node.py (uses engine + context + patterns + validation)
    ↓
graph.py (orders nodes via topological sort)
    ↓
references.py (cross-entity reference resolution)
    ↓
pipeline.py (orchestrates everything, PipelineManager)
    ↓
state/ + catalog.py (tracks run history, watermarks)
    ↓
lineage.py (OpenLineage event emission)
    ↓
story/ (documents execution as HTML/MD/JSON)
    ↓
cli/ (user interface, 20+ subcommands)
```

### Key Module Relationships

```
transformers/
    ├─→ Used by: node.py, story/doc_story.py
    └─→ Uses: registry.py

patterns/
    ├─→ Used by: node.py
    └─→ Uses: engine/, transformers/

simulation/
    ├─→ Used by: pipeline.py
    └─→ Uses: config.py (SimulationConfig, ScheduledEvent)

validation/
    ├─→ Used by: node.py, pipeline.py
    └─→ Uses: engine/ (for quarantine writes)

connections/
    ├─→ Used by: engine/
    └─→ Uses: Nothing (independent connectors)

engine/
    ├─→ Used by: node.py
    └─→ Uses: connections/, transformers/

writers/
    ├─→ Used by: engine/, node.py
    └─→ Uses: connections/ (SQL Server)

semantics/
    ├─→ Used by: cli/
    └─→ Uses: engine/, config.py

orchestration/
    ├─→ Used by: cli/
    └─→ Uses: config.py (generates Airflow/Dagster DAGs)

state/ + catalog.py
    ├─→ Used by: node.py, pipeline.py
    └─→ Uses: deltalake (local), spark (distributed)

cli/
    ├─→ Used by: Users!
    └─→ Uses: Everything
```

---

## Data Flow

### How Data Moves Through a Pipeline

```
1. User YAML Config
   ↓
2. Parsed to ProjectConfig (in-memory objects)
   ↓
3. PipelineManager.run() starts execution
   ↓
4. For each node:

   ┌─────────────────────────────────────────┐
   │ Node Execution                           │
   │                                          │
   │  1. Read Phase (if configured)           │
   │     ├─→ Incremental check (state lookup) │
   │     ├─→ Engine.read() → DataFrame        │
   │     └─→ Connection.get_path()            │
   │                                          │
   │  2. Simulation Phase (if configured)     │
   │     └─→ Generator → stateful DataFrame   │
   │         (prev, ema, pid, random_walk)     │
   │                                          │
   │  3. Transformer Phase (if configured)    │
   │     └─→ Pattern.execute() → DataFrame    │
   │         (SCD2, Merge, Dimension, etc.)    │
   │                                          │
   │  4. Transform Phase (if configured)      │
   │     ├─→ Get DataFrame from context       │
   │     ├─→ Registry.get(operation)          │
   │     └─→ func(df, **params) → DataFrame   │
   │                                          │
   │  5. Validation Phase (if configured)     │
   │     ├─→ Run tests (not_null, range, etc.)│
   │     ├─→ Quality gate pass/fail           │
   │     └─→ Quarantine bad rows (optional)   │
   │                                          │
   │  6. Write Phase (if configured)          │
   │     └─→ Engine.write(DataFrame)          │
   │         ├─→ Connection + format           │
   │         └─→ SQL Server writer (if SQL)    │
   │                                          │
   │  7. Store Result                         │
   │     └─→ Context.set(node_name, df)       │
   │                                          │
   │  8. Track Metadata                       │
   │     ├─→ Row counts, schema, timing       │
   │     └─→ State catalog update             │
   └─────────────────────────────────────────┘
   ↓
5. All nodes complete
   ↓
6. Generate Story
   └─→ PipelineStoryMetadata
       ├─→ All node metadata
       └─→ Rendered to HTML/MD/JSON
```

---

## Transformation Lifecycle

### Registration (Import Time)

```python
# When Python imports odibi/transformers/unpivot.py:

@transform("unpivot", category="reshaping")  # ← Runs at import
def unpivot(df, ...):
    ...

# What happens:
# 1. transform("unpivot", ...) returns a decorator
# 2. Decorator wraps unpivot function
# 3. Decorator calls registry.register("unpivot", wrapped_unpivot)
# 4. Registry stores it globally
# 5. Function is now available to all pipelines
```

### Lookup (Runtime)

```python
# During pipeline execution:

# 1. Node config says: operation="unpivot"
# 2. Node calls: registry.get("unpivot")
# 3. Registry returns the function
# 4. Node calls: func(df, id_vars="ID", ...)
# 5. Result returned
```

### Explanation (Story Generation)

```python
# During story generation:

# 1. Story generator calls: func.get_explanation(**params, **context)
# 2. ExplainableFunction looks for attached explain_func
# 3. If found: calls explain_func(**params, **context)
# 4. Returns formatted markdown
# 5. Included in HTML story
```

---

## Storage Architecture

### Connection Abstraction

```
BaseConnection (interface)
    ↓
┌───┴──────┬────────┬──────────┬──────┬──────────────┐
│          │        │          │      │              │
Local    ADLS    AzureSQL    HTTP   DBFS    DeltaCatalog
│          │        │          │      │
↓          ↓        ↓          ↓      ↓
./data   Azure   SQL DB    REST    Databricks
         Blob              APIs    FileStore
```

**All connections implement:**
- `get_path(relative_path)` - Resolve full path
- `validate()` - Check configuration

**Storage-specific methods:**
- ADLS: `pandas_storage_options()`, `configure_spark()`
- AzureSQL: `read_sql()`, `write_table()`, `get_engine()`
- HTTP: `fetch()` - REST API data retrieval
- Local/DBFS: Path manipulation

### Engine Abstraction

```
Engine (interface)
    ↓
┌───┴──────────────┬────────────────┐
│                  │                │
PandasEngine   SparkEngine    PolarsEngine
│                  │                │
↓                  ↓                ↓
pd.DataFrame   pyspark.DataFrame  pl.DataFrame
```

**All engines implement:**
- `read(connection, path, format, options)`
- `write(df, connection, path, format, mode, options)`
- `execute_sql(df, query)`

**Why?** Swap Pandas ↔ Spark ↔ Polars without changing config!

---

## Patterns Architecture

### Six Data Modeling Patterns

```
BasePattern (interface)
    ↓
┌───┴────────┬──────────┬──────────┬──────────────┬────────────────┐
│            │          │          │              │                │
Dimension   Fact      SCD2      Merge       Aggregation    DateDimension
│            │          │          │              │                │
↓            ↓          ↓          ↓              ↓                ↓
Surrogate   FK refs   History   Upsert/     Rollup/         Calendar
keys,       + measures tracking  Delete      group-by        table gen
dedup                            detect
```

Patterns are selected via `transformer:` in node config and execute as the primary operation before transform steps.

---

## Simulation Architecture

### Stateful Data Generation

```
SimulationConfig
    ├─→ EntityConfig (columns, generators, validation)
    ├─→ ScheduledEvent (setpoint changes, disturbances)
    └─→ SimulationScope (rows, time range)

Generator Types:
    ├─→ random_walk   (noise, drift, mean reversion)
    ├─→ derived       (expressions using prev, ema, pid)
    ├─→ range         (bounded random)
    ├─→ constant      (fixed value)
    └─→ categorical   (discrete values)

Stateful Functions (available in derived expressions):
    ├─→ prev(col, default)     - Previous row value
    ├─→ ema(col, alpha, default) - Exponential moving average
    └─→ pid(pv, sp, Kp, Ki, Kd, dt, min, max, anti_windup)
```

---

## Story Generation Architecture

### Three Types of Stories

```
1. RUN STORIES (automatic)
   │
   └─→ Generated during pipeline.run()
       ├─→ Captures actual execution
       ├─→ Saved to stories/runs/
       └─→ For audit/debugging

2. DOC STORIES (on-demand)
   │
   └─→ Generated via CLI: odibi story generate
       ├─→ Pulls operation explanations
       ├─→ For stakeholder communication
       └─→ Saved to docs/

3. DIFF STORIES (comparison)
   │
   └─→ Generated via CLI: odibi story diff
       ├─→ Compares two run stories
       ├─→ Shows what changed
       └─→ For troubleshooting
```

---

## The Registry Pattern (Deep Dive)

### Why This Pattern?

**Problem:** How do we make operations available globally?

**Bad Solution 1:** Import everything

```python
from odibi.operations import pivot, unpivot, join, sql, ...
# Breaks as we add more operations
```

**Bad Solution 2:** String-based imports

```python
op_module = __import__(f"odibi.operations.{operation_name}")
# Fragile, hard to debug
```

**Good Solution:** Registry

```python
# Operations register themselves:
@transform("pivot")
def pivot(...): ...

# Look up by name:
func = registry.get("pivot")

# Easy! Scalable! Type-safe!
```

### Registry Singleton Pattern

**One registry for entire process:**

```python
# odibi/registry.py

# Create once at module level
_global_registry = TransformationRegistry()

def get_registry():
    return _global_registry  # Always same instance
```

**Benefits:**
- Single source of truth
- Operations registered once
- Available everywhere
- Easy to test (registry.clear() in tests)

---

## Error Handling Strategy

### Validation Layers

```
Layer 1: Pydantic (config validation)
   ↓
Layer 2: Connection.validate() (connection validation)
   ↓
Layer 3: Graph.validate() (dependency validation)
   ↓
Layer 4: Data validation (quality tests, FK checks)
   ↓
Layer 5: Runtime (execution errors)
```

**Fail fast:** Catch errors before execution starts!

### Error Propagation

```python
try:
    # Node execution
    result = node.execute()
except Exception as e:
    # Caught by node.py
    node_result = NodeResult(
        success=False,
        error=e
    )
    # Stored in metadata
    # Shown in story
    # Pipeline continues (or stops, depending on config)
```

**Stories capture all errors** - makes debugging easy!

---

## Performance Characteristics

### Time Complexity

- **Config loading:** O(1) - just YAML parse
- **Dependency graph:** O(n + e) - n nodes, e edges
- **Node execution:** O(n) - linear in number of nodes
- **Story generation:** O(n) - linear in number of nodes

### Space Complexity

- **Context storage:** O(n × m) - n nodes, m average DataFrame size
- **Metadata:** O(n) - one metadata object per node
- **Stories:** O(n) - proportional to nodes

### Optimization Points

**1. Auto-Caching**
```yaml
# Nodes with 3+ downstream consumers are auto-cached
# Prevents redundant re-reads from ADLS
auto_cache_threshold: 3
```

**2. Incremental Loading**
```yaml
# Already implemented - only process new/changed data
incremental:
  mode: rolling_window   # or stateful (high-water mark)
  column: updated_at
  lookback: "7d"
```

**3. Engine Selection**
```yaml
# Swap engines based on data size
engine: pandas   # Small/medium datasets
engine: spark    # Large/distributed datasets
engine: polars   # Alternative high-performance local
```

---

## Design Patterns Used

### 1. Registry Pattern

**Where:** `odibi/registry.py`

**Purpose:** Centralized operation lookup

```python
@transform("calculate_sum")
def calculate_sum(df, ...): ...
```

### 2. Factory Pattern

**Where:** `odibi/connections/factory.py`

**Purpose:** Create connections by type name

```python
conn = create_connection(config)  # Returns the right connection type
```

### 3. Adapter Pattern (State)

**Where:** `odibi/state/`

**Purpose:** Uniform interface for state management

```python
backend = CatalogStateBackend(...)
# Local: uses delta-rs
# Spark: uses Spark SQL
```

### 4. Observer Pattern (Lineage)

**Where:** `odibi/lineage.py`

**Purpose:** Emit events without coupling execution logic

```python
lineage.emit_start(node)
lineage.emit_complete(node)
```

### 5. Strategy Pattern

**Where:** `engine/` (PandasEngine vs SparkEngine vs PolarsEngine)

**Purpose:** Swap execution strategies

```python
engine = PandasEngine()  # or SparkEngine() or PolarsEngine()
df = engine.read(...)    # Same interface, different implementation
```

### 6. Builder Pattern

**Where:** `story/doc_story.py`

**Purpose:** Construct complex documentation

```python
generator = DocStoryGenerator(config)
generator.generate(output_path="doc.html", format="html", theme=CORPORATE_THEME)
```

### 7. Template Method Pattern

**Where:** `story/renderers.py`, `patterns/base.py`

**Purpose:** Define algorithm skeleton, subclasses fill in steps

---

## Key Abstractions

### 1. Engine Abstraction

**Why?** Support multiple execution backends

```python
# User doesn't care if Pandas or Spark:
df = engine.read(connection, "data.parquet", "parquet")
# PandasEngine: uses pd.read_parquet()
# SparkEngine: uses spark.read.parquet()
# PolarsEngine: uses pl.read_parquet()
# Same interface!
```

### 2. Connection Abstraction

**Why?** Support multiple storage systems

```python
# User writes: path: "data.csv"
# Connection resolves to:
# - Local: ./data/data.csv
# - ADLS: abfss://container@account.dfs.core.windows.net/data.csv
# - DBFS: /dbfs/mnt/data.csv
# - HTTP: https://api.example.com/data
# Same code, different storage!
```

### 3. Transformation Abstraction

**Why?** User-defined operations work same as built-in

```python
# Built-in:
@transform("pivot")
def pivot(...): ...

# User-defined:
@transform("my_custom_op")
def my_custom_op(...): ...

# Both registered the same way, both available in YAML!
```

---

## Extensibility Points

### Where You Can Extend Odibi

**1. Add New Transformers**
```
Location: odibi/transformers/
Pattern: Use @transform decorator
Impact: Available in all pipelines
```

**2. Add New Connections**
```
Location: odibi/connections/
Pattern: Extend BaseConnection
Impact: New storage backends
```

**3. Add New Engines**
```
Location: odibi/engine/
Pattern: Implement Engine interface
Impact: New execution backends
```

**4. Add New Patterns**
```
Location: odibi/patterns/
Pattern: Extend BasePattern
Impact: New data modeling patterns
```

**5. Add New Renderers**
```
Location: odibi/story/renderers.py
Pattern: Implement .render() method
Impact: New story output formats
```

**6. Add New Validators**
```
Location: odibi/validation/
Pattern: Create validator class
Impact: Quality enforcement
```

**7. Add New Simulation Generators**
```
Location: odibi/simulation/generator.py
Pattern: Add generator type handling
Impact: New data generation strategies
```

---

## Configuration Model

### Pydantic Model Hierarchy

```
ProjectConfig (root)
    ├── project: str
    ├── engine: EngineType (pandas/spark/polars)
    ├── connections: Dict[str, ConnectionConfig]
    │       └── LocalConnectionConfig / AzureBlobConnectionConfig /
    │           DeltaConnectionConfig / SQLServerConnectionConfig /
    │           HttpConnectionConfig / CustomConnectionConfig
    ├── story: StoryConfig
    ├── system: SystemConfig
    ├── lineage: LineageConfig (optional)
    ├── retry: RetryConfig
    ├── logging: LoggingConfig
    ├── alerts: List[AlertConfig]
    ├── performance: PerformanceConfig
    ├── environments: Dict (optional overrides)
    └── pipelines: List[PipelineConfig]
            ├── pipeline: str
            ├── layer: str (bronze/silver/gold)
            ├── auto_cache_threshold: int
            └── nodes: List[NodeConfig]
                    ├── name: str
                    ├── read: ReadConfig (optional)
                    │     └── incremental: IncrementalConfig
                    ├── transformer: str (optional, e.g. "scd2")
                    ├── params: Dict (pattern parameters)
                    ├── transform: TransformConfig (optional)
                    │     └── steps: List[TransformStep]
                    ├── validation: ValidationConfig (optional)
                    ├── write: WriteConfig (optional)
                    ├── simulation: SimulationConfig (optional)
                    │     ├── entities: List[EntityConfig]
                    │     ├── scheduled_events: List[ScheduledEvent]
                    │     └── scope: SimulationScope
                    ├── depends_on: List[str]
                    ├── inputs: Dict (cross-entity refs)
                    ├── tags: List[str]
                    └── enabled: bool
```

### Validation Flow

```
YAML file
    ↓
yaml.safe_load() → dict
    ↓
Variable substitution (${vars.env}, env vars)
    ↓
ProjectConfig(**dict)  ← Pydantic validation
    ↓
If valid: ProjectConfig instance
If invalid: ValidationError with helpful message
```

---

## Thread Safety

### Current State: Single-Threaded

**Registry:** Thread-safe (read-only after startup)
**Context:** NOT thread-safe (single pipeline execution)
**Pipeline:** NOT thread-safe (sequential execution)

---

## Security Considerations

### Credential Handling

**Good:**
```yaml
connections:
  azure:
    auth_mode: key_vault
    key_vault_name: myvault
    secret_name: storage-key
```

**Bad:**
```yaml
connections:
  azure:
    account_key: "hardcoded_key_here"  # DON'T!
```

### SQL Injection Protection

Do not interpolate untrusted SQL or identifiers, regardless of engine. Use supported
parameter APIs where a connector implements them; neither DuckDB nor Odibi provides a
universal injection-immunity guarantee. See the project
[security policy](https://github.com/henryodibi11/Odibi/blob/main/SECURITY.md).

For **Azure SQL**, use parameterized queries:

```python
# Safe
conn.read_sql(
    "SELECT * FROM users WHERE id = :user_id",
    params={"user_id": 123}
)
```

---

## Next Steps

Learn how to build on the architecture:
- **[Transformation Guide](../guides/writing_transformations.md)** - Create custom operations
- **[Best Practices](../guides/best_practices.md)** - Production patterns
- **Read the code!** Start with `transformers/` directory
