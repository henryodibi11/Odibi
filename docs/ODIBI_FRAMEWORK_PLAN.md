# ODIBI Framework - Complete Implementation Plan

**Framework Name:** ODIBI  
**Version:** 1.0 (Clean slate, no backward compatibility with v2)  
**Timeline:** ~2 months for Phase 1  
**Philosophy:** Explicit over implicit, Stories over magic, Simple over clever

---

## Table of Contents

1. [Vision & Principles](#vision--principles)
2. [Architecture](#architecture)
3. [Core Concepts](#core-concepts)
4. [Config Structure](#config-structure)
5. [Node Specification](#node-specification)
6. [Execution Model](#execution-model)
7. [What We Keep from v2](#what-we-keep-from-v2)
8. [What We Build New](#what-we-build-new)
9. [What We Drop from v2](#what-we-drop-from-v2)
10. [Phase 1 Scope](#phase-1-scope)
11. [Implementation Details](#implementation-details)
12. [Success Criteria](#success-criteria)
13. [Installation & Usage](#installation--usage)

---

## Vision & Principles

### **The Problem ODIBI Solves**

Data engineering work should be:
- **Transparent**: Know exactly what happened and why
- **Traceable**: Full lineage from source to destination
- **Repeatable**: Same config, same results
- **Debuggable**: When things fail, know exactly where and why
- **Teachable**: New users can understand and use it quickly

### **Core Principles**

1. ✅ **Everything is a Node** (reads → transform → creates)
2. ✅ **Everything explicit** (if it's in the story, it must be in the config)
3. ✅ **Connections centralized** (define once, reference by name)
4. ✅ **Dependencies explicit** (clear dependency graph, no magic)
5. ✅ **Stories automatic** (every node execution documented)
6. ✅ **Engine-agnostic** (same config works on Spark/Pandas)
7. ✅ **Start simple, grow naturally** (explicit configs → templates emerge from patterns)

### **Key Design Decision**

> **"If it appears in the story, it must be in the config."**

This means:
- User controls everything visible in the output
- No hidden optimizations or automatic decisions
- Framework can suggest, but user decides

---

## Architecture

### **Layered Architecture**

```
┌─────────────────────────────────────────┐
│  USER LAYER                             │
│  - project.yaml (manifest)              │
│  - pipelines/*.yaml (configs)           │
│  - transformation functions (Python)    │
└─────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────┐
│  ORCHESTRATION LAYER                    │
│  - Config loader (YAML → Pydantic)      │
│  - Dependency graph builder             │
│  - Pipeline executor (parallel)         │
│  - Story generator                      │
└─────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────┐
│  NODE LAYER                             │
│  - Node (base abstraction)              │
│  - ReadNode                             │
│  - TransformNode                        │
│  - WriteNode                            │
│  - Context (data passing between nodes) │
└─────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────┐
│  ENGINE LAYER (adapt from v2)           │
│  - ReaderProvider (dispatch)            │
│  - SaverProvider (dispatch)             │
│  - SparkWorkflowNode                    │
│  - PandasWorkflowNode (create new)      │
└─────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────┐
│  CONNECTION LAYER (keep from v2)        │
│  - BaseConnection (interface)           │
│  - AzureBlobConnection                  │
│  - DeltaConnection                      │
│  - SQLConnection                        │
│  - LocalConnection                      │
└─────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────┐
│  IMPLEMENTATION LAYER (keep from v2)    │
│  - PandasDataReader/Saver               │
│  - SparkDataReader/Saver                │
│  - Actual I/O operations                │
└─────────────────────────────────────────┘
```

---

## Core Concepts

### **1. Node**

A Node is the fundamental unit of work. Every node follows the pattern:

**reads → transform → creates**

```yaml
- name: node_name           # Required (for lineage)
  depends_on: [...]         # Optional (explicit dependencies)

  read:                     # Optional (what it reads)
    connection: name
    table/path: ...

  transform:                # Optional (what it does)
    steps: [...]

  write:                    # Optional (what it creates)
    connection: name
    table/path: ...

  cache: true/false         # Optional (explicit optimization)
```

**At least ONE of read/transform/write must be present.**

**Node Types:**

- **Read-only**: Loads data into context
- **Transform-only**: Processes data in memory
- **Write-only**: Persists data from context
- **Full pipeline**: Does all three

### **2. Pipeline**

A collection of nodes with explicit dependencies. Each project can have multiple pipelines.

```yaml
pipeline: nkc_dryer_1

nodes:
  - name: aspentags
    read: {...}

  - name: process_data
    depends_on: [aspentags]
    transform: {...}

  - name: save_result
    depends_on: [process_data]
    write: {...}
```

### **3. Project**

A self-contained data engineering project with:
- Connections (infrastructure)
- Multiple pipelines (work definitions)
- Optional layers (execution ordering)
- Global settings

Each project = one git repo = one Python package (like Energy Efficiency).

### **4. Connection**

Infrastructure for accessing data. Defined once in project manifest, referenced by name everywhere.

```yaml
connections:
  azure_bronze:
    type: azure_blob
    account: ${AZURE_ACCOUNT}
    container: bronze

  delta_silver:
    type: delta
    catalog: main
    schema: silver
```

### **5. Dependency Graph**

Explicit graph built from `depends_on` declarations. Enables:
- Topological sort (execution order)
- Parallel execution (independent nodes)
- Lineage tracking (what depends on what)
- Resume capability (skip completed nodes)

### **6. Story**

Automatic documentation generated for every pipeline run showing:
- What happened in each node
- Before/after data samples
- Schema changes
- Rows processed
- Duration
- Success/Failure/Skip status
- Retry attempts

### **7. Context**

Data passing mechanism between nodes:

**Spark:** Temp views registered and referenced by name
```python
context.register_view("aspentags", df)
# Later: spark.sql("SELECT * FROM aspentags")
```

**Pandas:** Dictionary of DataFrames
```python
context.data["aspentags"] = df
# Later: context.data["aspentags"]
```

---

## Config Structure

### **Project Manifest (project.yaml)**

Located at project root. Defines infrastructure and discovery.

```yaml
# ============================================
# PROJECT METADATA
# ============================================
project: Energy Efficiency
version: "1.0.0"
description: "Manufacturing energy efficiency analytics"
owner: henry.odibi@ingredion.com
engine: spark  # Default engine (spark or pandas)

# ============================================
# CONNECTIONS (Infrastructure)
# ============================================
connections:
  azure_bronze:
    type: azure_blob
    account_name: ${AZURE_ACCOUNT}  # Environment variables
    container: bronze
    auth:
      key: ${AZURE_KEY}

  azure_silver:
    type: azure_blob
    account_name: ${AZURE_ACCOUNT}
    container: silver
    auth:
      key: ${AZURE_KEY}

  sql_warehouse:
    type: sql_server
    host: db.company.com
    database: manufacturing
    auth:
      user: ${SQL_USER}
      password: ${SQL_PASSWORD}

# ============================================
# PIPELINE DISCOVERY
# ============================================
pipelines:
  # Glob patterns to find all pipeline configs
  - path: pipelines/bronze/**/*.yaml
    layer: bronze

  - path: pipelines/silver/**/*.yaml
    layer: silver

  - path: pipelines/gold/**/*.yaml
    layer: gold

# ============================================
# EXECUTION LAYERS (Optional - for ordering)
# ============================================
layers:
  bronze:
    depends_on: []
    parallel: true  # All bronze pipelines run in parallel

  silver_1:
    depends_on: [bronze]
    parallel: true

  silver_2:
    depends_on: [silver_1]
    parallel: true

  gold:
    depends_on: [silver_2]
    parallel: false  # Run sequentially

# ============================================
# GLOBAL DEFAULTS
# ============================================
defaults:
  retry:
    enabled: true
    max_attempts: 3
    backoff: exponential  # 1s, 2s, 4s

  logging:
    level: INFO  # DEBUG, INFO, WARNING, ERROR
    structured: false  # JSON logs if true
    metadata:  # Optional: added to all logs
      project: Energy Efficiency
      environment: prod

  story:
    auto_generate: true
    max_sample_rows: 10
    output_path: stories/

# ============================================
# ENVIRONMENTS (Optional)
# ============================================
environments:
  qat:
    database_prefix: qat_
    logging:
      level: DEBUG

  prod:
    database_prefix: prod_
    logging:
      level: WARNING
```

### **Pipeline Config (pipelines/silver/dryer_1.yaml)**

Individual pipeline definition. Can be organized however makes sense for the project.

```yaml
# ============================================
# PIPELINE METADATA
# ============================================
pipeline: nkc_dryer_1
description: "NKC Germ Dryer 1 - Silver layer processing"
layer: silver  # Optional: for organization

# ============================================
# NODES
# ============================================
nodes:
  # Node 1: Load reference data
  - name: aspentags
    description: "Load equipment tag configuration"
    read:
      connection: azure_bronze
      path: delta/aspentags
      format: delta  # Delta is a format, not a connection
    cache: true  # Used by many downstream nodes

  # Node 2: Load bronze data
  - name: dryer_bronze
    description: "Load raw dryer sensor data"
    read:
      connection: azure_bronze
      path: delta/nkc_dryer_1_bronze
      format: delta

  # Node 3: Enrich and transform
  - name: dryer_cleaned
    description: "Join with tags, pivot, calculate metrics"
    depends_on: [aspentags, dryer_bronze]
    transform:
      steps:
        # SQL step
        - "SELECT * FROM dryer_bronze"

        # Pivot operation (config)
        - operation: pivot
          params:
            group_by: [Time_Stamp, Plant, Asset]
            pivot_column: Description
            value_column: Value
            agg_func: first

        # Python function with params (tuple)
        - function: get_asset_constants
          params:
            tags_table: aspentags
            plant: NKC
            asset: "Germ Dryer 1"

        # Another Python function
        - function: calc_efficiency
          params:
            enthalpy_col: Dryer_Steam_h

    # Optional validation
    validate:
      schema:
        required_columns: [Time_Stamp, Plant, Asset, Efficiency]
        types:
          Efficiency: float
      not_empty: true

  # Node 4: Write to silver
  - name: dryer_output
    description: "Persist cleaned data to silver layer"
    depends_on: [dryer_cleaned]
    write:
      connection: azure_silver
      path: delta/nkc_dryer_1_cleaned
      format: delta
      mode: overwrite
```

---

## Node Specification

### **Node Schema (Pydantic)**

```python
from pydantic import BaseModel
from typing import Optional, List, Dict, Any, Union

class ReadConfig(BaseModel):
    connection: str  # References project.yaml connection
    format: str  # Required: csv, parquet, json, delta, avro, sql, etc.
    table: Optional[str]  # For SQL/Delta tables
    path: Optional[str]   # For file-based sources
    options: Dict[str, Any] = {}  # Format-specific options

class TransformConfig(BaseModel):
    steps: List[Union[str, Dict, tuple]]  # SQL, config, or (function, params)

class WriteConfig(BaseModel):
    connection: str
    format: str  # Required: csv, parquet, json, delta, avro, sql, etc.
    table: Optional[str]  # For SQL/Delta tables
    path: Optional[str]   # For file-based destinations
    mode: str = "overwrite"  # overwrite, append
    options: Dict[str, Any] = {}  # Format-specific options

class ValidationConfig(BaseModel):
    schema: Optional[Dict] = None
    not_empty: bool = False
    no_nulls: List[str] = []

class NodeConfig(BaseModel):
    name: str  # Required for lineage
    description: Optional[str] = None
    depends_on: List[str] = []

    # At least one required
    read: Optional[ReadConfig] = None
    transform: Optional[TransformConfig] = None
    write: Optional[WriteConfig] = None

    # Optional
    cache: bool = False
    validate: Optional[ValidationConfig] = None

    @validator('name')
    def validate_at_least_one_operation(cls, v, values):
        if not any([values.get('read'), values.get('transform'), values.get('write')]):
            raise ValueError("Node must have at least one of: read, transform, write")
        return v
```

---

## Execution Model

### **Execution Flow**

```
1. LOAD PROJECT MANIFEST
   ├─ Parse project.yaml
   ├─ Resolve environment variables
   ├─ Initialize connections
   └─ Discover pipeline configs (glob patterns)

2. LOAD PIPELINE CONFIGS
   ├─ Parse all pipeline YAML files
   ├─ Validate with Pydantic schemas
   └─ Build complete list of nodes

3. BUILD DEPENDENCY GRAPH
   ├─ Parse all depends_on declarations
   ├─ Validate all dependencies exist
   ├─ Check for circular dependencies
   └─ Topological sort for execution order

4. EXECUTE NODES
   ├─ Identify nodes ready to run (no pending dependencies)
   ├─ Execute in parallel (ThreadPoolExecutor)
   │  ├─ Resolve connection by name
   │  ├─ Execute read (if present)
   │  ├─ Execute transform (if present)
   │  ├─ Execute write (if present)
   │  ├─ Capture metadata (duration, rows, etc.)
   │  └─ Store result in context
   ├─ Mark completed
   ├─ Find newly ready nodes
   └─ Repeat until all nodes complete or fail

5. HANDLE FAILURES
   ├─ Retry failed node (if configured)
   ├─ If all retries fail: mark as FAILED
   ├─ Skip dependent nodes (mark as SKIPPED)
   └─ Continue with independent nodes

6. GENERATE STORY
   ├─ Collect metadata from all nodes
   ├─ Build before/after snapshots
   ├─ Generate HTML story
   └─ Save to configured location
```

### **Parallel Execution Strategy**

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def execute_pipeline(nodes, context):
    completed = set()
    failed = set()
    skipped = set()

    # Build dependency map
    deps = {node.name: node.depends_on for node in nodes}

    with ThreadPoolExecutor(max_workers=10) as executor:
        while len(completed) + len(failed) + len(skipped) < len(nodes):
            # Find ready nodes
            ready = [
                node for node in nodes
                if node.name not in completed
                and node.name not in failed
                and node.name not in skipped
                and all(dep in completed for dep in node.depends_on)
            ]

            if not ready and not running:
                # No more nodes can run
                break

            # Submit ready nodes
            futures = {
                executor.submit(execute_node, node, context): node
                for node in ready
            }

            # Wait for completion
            for future in as_completed(futures):
                node = futures[future]
                try:
                    result = future.result()
                    if result.success:
                        completed.add(node.name)
                    else:
                        failed.add(node.name)
                        # Skip all dependents
                        skip_dependents(node, deps, skipped)
                except Exception as e:
                    failed.add(node.name)
                    skip_dependents(node, deps, skipped)

    return completed, failed, skipped
```

### **Retry Logic**

```python
def execute_node_with_retry(node, context, max_attempts=3):
    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(f"→ {node.name} (attempt {attempt}/{max_attempts})")
            result = node.execute(context)
            logger.info(f"  ✓ {node.name} completed ({result.duration:.1f}s)")
            return result
        except Exception as e:
            logger.warning(f"  ✗ {node.name} failed (attempt {attempt}): {e}")
            if attempt == max_attempts:
                logger.error(f"  ✗ {node.name} FAILED after {max_attempts} attempts")
                raise
            # Exponential backoff
            time.sleep(2 ** (attempt - 1))
```

### **Resume Capability**

```python
def should_skip_node(node, context, resume=False):
    """Check if node output already exists and is recent"""
    if not resume:
        return False

    if not node.write:
        return False  # Transform-only nodes always run

    # Check if output exists
    if node.write.table:
        exists = context.check_table_exists(
            connection=node.write.connection,
            table=node.write.table
        )
        if exists:
            logger.info(f"  ⊙ {node.name} SKIPPED (output exists)")
            return True

    return False
```

---

## What We Keep from v2

### **Connection Layer (95% as-is)**

✅ **Keep entirely:**
- `BaseConnection` interface
- `AzureBlobConnection`
- `SQLConnection`
- `LocalConnection`

**Why:** Clean abstraction, works perfectly, easy to extend (AWS/GCP).

**Note:** Delta is a **format**, not a connection type. Delta tables are read/written via storage connections (Azure, S3, local) with `format: delta`.

**Location in v2:**
```
odibi_de_v2/connector/
  ├── base_connector.py
  ├── azure/azure_blob_connector.py
  ├── sql/sql_database_connection.py
  └── local/local_connection.py
```

**Delta Format Support:**

Delta tables are handled by readers/savers, not connections:

**Spark (native support):**
```python
# Read
df = spark.read.format("delta").load(path)

# Write
df.write.format("delta").mode("overwrite").save(path)
```

**Pandas (requires deltalake package):**
```python
from deltalake import DeltaTable, write_deltalake

# Read
dt = DeltaTable(path)
df = dt.to_pandas()

# Write
write_deltalake(path, df, mode="overwrite")
```

**Installation:**
```bash
pip install odibi[spark]  # Includes native Delta support
pip install odibi[delta]  # Adds deltalake for Pandas Delta support
```

### **Provider Pattern (100% as-is)**

✅ **Keep entirely:**
- `ReaderProvider` (engine dispatch)
- `SaverProvider` (engine dispatch)

**Why:** Perfect for engine-agnostic node execution.

**Location in v2:**
```
odibi_de_v2/ingestion/reader_provider.py
odibi_de_v2/storage/saver_provider.py
```

### **Reader/Saver Implementations (100% as-is)**

✅ **Keep entirely:**
- `PandasDataReader`
- `SparkDataReader`
- `PandasDataSaver`
- `SparkDataSaver`
- All format support (CSV, Parquet, JSON, AVRO, SQL, etc.)

**Why:** Battle-tested, handles all formats, works.

**Location in v2:**
```
odibi_de_v2/ingestion/pandas/pandas_data_reader.py
odibi_de_v2/ingestion/spark/spark_data_reader.py
odibi_de_v2/storage/pandas/pandas_data_saver.py
odibi_de_v2/storage/spark/spark_data_saver.py
```

### **SparkWorkflowNode (100% as-is)**

✅ **Keep entirely:**
- Accepts SQL strings
- Accepts Python functions
- Accepts config operations
- Temp view management
- Step-by-step execution

**Why:** Core transform pattern that works perfectly.

**Location in v2:**
```
odibi_de_v2/transformer/spark/spark_workflow_node.py
```

### **TransformationStory (Extend)**

✅ **Keep and extend:**
- Before/after snapshots
- Schema change tracking
- HTML generation
- Collapsible sections

**Add:**
- Read node visualization
- Write node visualization
- Full pipeline narrative
- Retry tracking
- Skip tracking

**Location in v2:**
```
odibi_de_v2/transformer/visualization/transformation_story.py
```

### **Core Enums (Keep)**

✅ **Keep:**
- `Framework` (SPARK, PANDAS, LOCAL)
- `DataType` (CSV, PARQUET, JSON, SQL, etc.)

**Location in v2:**
```
odibi_de_v2/core/enums.py
```

### **Utilities (Selective)**

✅ **Keep:**
- `@benchmark` decorator (for timing)
- Type checking utilities
- String utilities

❌ **Drop:**
- `@log_call` decorator (use standard logging)
- `@enforce_types` (use Pydantic + type hints)
- Complex metadata manager (use stories)

**Location in v2:**
```
odibi_de_v2/utils/decorators/
```

---

## What We Build New

### **1. Config System**

**New components:**
- YAML parser
- Pydantic schemas for validation
- Project manifest loader
- Environment variable resolution
- Pipeline discovery (glob patterns)

**Structure:**
```
odibi/config/
  ├── schemas.py          # Pydantic models
  ├── loader.py           # YAML loading + validation
  ├── manifest.py         # Project manifest handling
  └── env.py              # Environment variable resolution
```

### **2. Orchestration Engine**

**New components:**
- Dependency graph builder
- Topological sort
- Parallel executor (ThreadPoolExecutor)
- Retry logic
- Skip logic (on dependency failure)
- Resume logic (skip completed nodes)

**Structure:**
```
odibi/orchestration/
  ├── graph.py            # Dependency graph builder
  ├── executor.py         # Parallel execution engine
  ├── retry.py            # Retry logic
  └── resume.py           # Resume capability
```

### **3. Node Abstraction**

**New components:**
- Base Node class
- ReadNode, TransformNode, WriteNode
- Context (Spark temp views / Pandas dict)
- Metadata capture

**Structure:**
```
odibi/nodes/
  ├── base.py             # Base Node abstraction
  ├── read.py             # ReadNode
  ├── transform.py        # TransformNode
  ├── write.py            # WriteNode
  └── context.py          # Context for data passing
```

### **4. PandasWorkflowNode**

**New component:**
- Same interface as SparkWorkflowNode
- Accepts SQL strings (via **DuckDB** - fast, feature-rich, industry standard)
- Accepts Python functions
- Accepts config operations (pivot, unpivot, derived)
- DataFrame passing

**Why DuckDB:**
- Fast (C++ implementation)
- Full SQL support (window functions, CTEs, complex joins)
- Direct Pandas integration: `duckdb.query("SELECT * FROM df")`
- Matches SparkWorkflowNode feature parity
- Industry momentum (used by dbt, Jupyter, Ibis)

**Structure:**
```
odibi/transform/
  ├── spark_workflow.py   # SparkWorkflowNode (from v2)
  └── pandas_workflow.py  # PandasWorkflowNode (new, uses DuckDB for SQL)
```

### **5. Story Generator**

**Extend v2's TransformationStory:**
- Add pipeline-level story
- Add read node stories
- Add write node stories
- Add retry visualization
- Add skip visualization

**Structure:**
```
odibi/story/
  ├── generator.py        # Story generation orchestration
  ├── templates.py        # HTML templates
  └── formatters.py       # Data formatting for display
```

### **6. Logging**

**New: Simple Python logging integration:**
- Standard logging module
- Configurable levels
- Optional structured (JSON) logging
- Metadata injection

**Structure:**
```
odibi/logging/
  ├── setup.py            # Logger setup
  └── formatters.py       # JSON formatter (optional)
```

---

## What We Drop from v2

### **❌ SQL-based Config System**

**Drop:**
- `ConfigUtils`
- Config tables in databases
- `TransformerOrchestrator` (old orchestration)
- Config UI helpers

**Reason:** YAML is simpler, more portable, git-friendly.

### **❌ Heavy Logging Infrastructure**

**Drop:**
- Singleton logger
- Custom logging decorators (`@log_call`)
- Metadata manager (separate from stories)

**Reason:** Standard Python logging is simpler and sufficient.

### **❌ Specific Transformers (Move to separate package)**

**Drop from core, move to `odibi-transformers`:**
- `ColumnAdder`
- `ColumnDropper`
- `ColumnRenamer`
- `ValueReplacer`
- etc.

**Reason:** Users can write their own, or install optional package.

### **❌ Databricks-specific Bootstrap**

**Drop from core:**
- `init_spark_with_azure_secrets`
- Databricks job helpers
- Specific orchestration patterns

**Reason:** Make it optional plugin/example, not core dependency.

### **❌ Over-engineering**

**Drop:**
- Multiple factory layers
- Unused abstractions
- Complex decorator chains
- Premature optimizations

**Reason:** Simplicity and maintainability.

---

## Phase 1 Scope

### **Deliverables**

**Week 1-2: Foundation**
- [ ] Pydantic schemas (ProjectConfig, PipelineConfig, NodeConfig)
- [ ] YAML loader with validation
- [ ] Environment variable resolution
- [ ] Project manifest loading
- [ ] Connection validation (on load + lazy validation)
- [ ] Format handling (explicit format required)
- [ ] Basic unit tests (schemas, loader, validation)

**Week 3-4: Orchestration**
- [ ] Dependency graph builder
- [ ] Topological sort
- [ ] Circular dependency detection
- [ ] Node abstraction (base class)
- [ ] Context implementation (Spark temp views + Pandas DuckDB)
- [ ] Unit tests (graph, nodes, context)

**Week 5-6: Execution**
- [ ] Node execution logic (read/transform/write)
- [ ] Provider integration (reuse v2 readers/savers)
- [ ] PandasWorkflowNode (with DuckDB for SQL)
- [ ] Parallel executor (ThreadPoolExecutor)
- [ ] Retry logic
- [ ] Skip logic (dependency failures)
- [ ] Resume logic (skip completed)
- [ ] Integration tests (end-to-end pipelines)

**Week 7-8: Stories & Polish**
- [ ] Extend TransformationStory for full pipeline
- [ ] Read/Write node story visualization
- [ ] Retry/Skip visualization in stories
- [ ] Logging integration (Python logging)
- [ ] Error messages (clear, actionable)
- [ ] Python API (complete interface)
- [ ] Test with Energy Efficiency project (full migration)
- [ ] Package setup (setup.py, dependencies)
- [ ] Documentation (README, examples)
- [ ] Final testing (unit + integration)

### **Success Criteria**

✅ **Functional:**
- Energy Efficiency project runs on ODIBI
- Same config works on Spark AND Pandas
- 40+ bronze pipelines execute in parallel
- Dependencies honored correctly
- Retry on failure works
- Resume from failure works

✅ **Quality:**
- Complete story generated for every run
- Clear error messages
- All nodes show success/failure/skip status
- Lineage graph accurate
- Performance acceptable (not slower than v2)

✅ **Usability:**
- Config structure intuitive
- Easy to add new pipeline
- Easy to debug failures
- Documentation clear

### **Out of Scope for Phase 1**

⏸️ **Phase 2+ features:**
- CLI beyond Python API
- Transformer library (separate package)
- SQL config option
- Advanced dry-run
- UI for stories (beyond HTML files)
- Cost tracking
- Advanced monitoring

---

## Implementation Details

### **Project Structure**

```
odibi/
├── config/
│   ├── __init__.py
│   ├── schemas.py          # Pydantic models
│   ├── loader.py           # YAML loading
│   ├── manifest.py         # Project manifest
│   └── env.py              # Env var resolution
│
├── orchestration/
│   ├── __init__.py
│   ├── graph.py            # Dependency graph
│   ├── executor.py         # Execution engine
│   ├── retry.py            # Retry logic
│   └── resume.py           # Resume logic
│
├── nodes/
│   ├── __init__.py
│   ├── base.py             # Base Node
│   ├── read.py             # ReadNode
│   ├── transform.py        # TransformNode
│   ├── write.py            # WriteNode
│   └── context.py          # Context
│
├── engine/
│   ├── __init__.py
│   ├── providers/
│   │   ├── reader.py       # ReaderProvider (from v2)
│   │   └── saver.py        # SaverProvider (from v2)
│   ├── readers/
│   │   ├── pandas.py       # PandasDataReader (from v2)
│   │   └── spark.py        # SparkDataReader (from v2)
│   ├── savers/
│   │   ├── pandas.py       # PandasDataSaver (from v2)
│   │   └── spark.py        # SparkDataSaver (from v2)
│   └── workflow/
│       ├── spark.py        # SparkWorkflowNode (from v2)
│       └── pandas.py       # PandasWorkflowNode (new)
│
├── connections/
│   ├── __init__.py
│   ├── base.py             # BaseConnection (from v2)
│   ├── azure.py            # AzureBlobConnection (from v2)
│   ├── sql.py              # SQLConnection (from v2)
│   └── local.py            # LocalConnection (from v2)
│
├── story/
│   ├── __init__.py
│   ├── generator.py        # Story generation
│   ├── templates.py        # HTML templates
│   └── formatters.py       # Data formatting
│
├── logging/
│   ├── __init__.py
│   ├── setup.py            # Logger configuration
│   └── formatters.py       # JSON formatter
│
├── utils/
│   ├── __init__.py
│   ├── decorators.py       # @benchmark, etc.
│   └── helpers.py          # General utilities
│
├── __init__.py
├── __version__.py
└── api.py                  # Main API
```

### **Key Classes**

**Node Base:**
```python
from abc import ABC, abstractmethod
from typing import Optional, Any

class Node(ABC):
    def __init__(self, config: NodeConfig, context: Context):
        self.config = config
        self.context = context
        self.metadata = {}

    @abstractmethod
    def execute(self) -> NodeResult:
        """Execute node logic"""
        pass

    def capture_metadata(self, result: Any):
        """Capture execution metadata"""
        self.metadata = {
            "name": self.config.name,
            "duration": ...,
            "rows": ...,
            "success": True/False
        }
```

**Context:**
```python
class Context:
    def __init__(self, engine: str):
        self.engine = engine
        self.data = {}  # For Pandas
        self.views = {}  # For Spark
        self.spark = None

    def register(self, name: str, data: Any):
        """Register data for downstream nodes"""
        if self.engine == "spark":
            data.createOrReplaceTempView(name)
            self.views[name] = data
        else:
            self.data[name] = data

    def get(self, name: str) -> Any:
        """Get data by name"""
        if self.engine == "spark":
            return self.spark.table(name)
        else:
            return self.data[name]
```

**Pipeline Executor:**
```python
class PipelineExecutor:
    def __init__(self, pipeline_config, project_config):
        self.pipeline = pipeline_config
        self.project = project_config
        self.context = Context(project_config.engine)

    def execute(self):
        # Build graph
        graph = DependencyGraph(self.pipeline.nodes)

        # Execute
        results = execute_parallel(
            nodes=graph.topological_sort(),
            context=self.context,
            retry_config=self.project.defaults.retry
        )

        # Generate story
        story = StoryGenerator(results).generate()

        return results, story
```

---

## Success Criteria

### **Phase 1 Complete When:**

**Functional:**
1. ✅ Energy Efficiency project runs successfully on ODIBI
2. ✅ All 40+ bronze pipelines execute in parallel
3. ✅ Silver layer pipelines execute with correct dependencies
4. ✅ Gold layer pipelines combine data correctly
5. ✅ Same pipeline config works on both Spark and Pandas
6. ✅ Node failures trigger retries (configurable)
7. ✅ Failed nodes skip dependents, continue with independent nodes
8. ✅ Resume runs skip already-completed nodes

**Quality:**
1. ✅ Story generated for every pipeline run
2. ✅ Story shows all nodes (read/transform/write)
3. ✅ Story shows success/failure/skip/retry status
4. ✅ Story shows before/after data samples
5. ✅ Story shows schema changes
6. ✅ Lineage graph accurate
7. ✅ Error messages clear and actionable

**Performance:**
1. ✅ Parallel execution works (40 pipelines run concurrently)
2. ✅ Not slower than v2 for same workload
3. ✅ Memory usage reasonable

**Usability:**
1. ✅ Config structure intuitive (can explain to others)
2. ✅ Easy to add new pipeline (copy/modify existing)
3. ✅ Easy to debug failures (story + logs)
4. ✅ Installation simple (`pip install odibi`)

---

## Installation & Usage

### **Installation**

```bash
# Basic installation
pip install odibi

# With Spark support
pip install odibi[spark]

# Development installation
git clone https://github.com/henryodibi/odibi.git
cd odibi
pip install -e .
```

### **Project Setup**

```bash
# Create new project
mkdir my-data-project
cd my-data-project

# Create structure
mkdir -p pipelines/bronze pipelines/silver pipelines/gold
touch project.yaml

# Initialize git
git init
```

### **Basic Usage**

**Python API:**
```python
from odibi import Pipeline

# Run pipeline
pipeline = Pipeline.from_yaml("project.yaml")
results = pipeline.run()

# Run specific pipeline
results = pipeline.run("pipelines/silver/dryer_1.yaml")

# Resume from failure
results = pipeline.run(resume=True)

# Run with different engine
results = pipeline.run(engine="pandas")
```

**Command Line (Phase 2):**
```bash
# Run all pipelines
odibi run

# Run specific pipeline
odibi run pipelines/silver/dryer_1.yaml

# Validate configs
odibi validate

# Show dependency graph
odibi graph

# Resume from failure
odibi run --resume
```

### **Example: Minimal Pipeline**

**project.yaml:**
```yaml
project: My Project
engine: pandas

connections:
  local:
    type: local

pipelines:
  - path: pipelines/*.yaml
```

**pipelines/simple.yaml:**
```yaml
pipeline: simple_example

nodes:
  - name: read_csv
    read:
      connection: local
      path: data/input.csv

  - name: filter_data
    depends_on: [read_csv]
    transform:
      steps:
        - "SELECT * FROM read_csv WHERE amount > 0"

  - name: save_result
    depends_on: [filter_data]
    write:
      connection: local
      path: data/output.csv
```

**Run:**
```python
from odibi import Pipeline

pipeline = Pipeline.from_yaml("project.yaml")
results = pipeline.run()

# Story automatically saved to stories/simple_example_<timestamp>.html
```

---

## Package Dependencies

### **Core Dependencies (required)**

```toml
[project]
dependencies = [
    "pydantic>=2.0.0",      # Config validation
    "pyyaml>=6.0.0",        # YAML parsing
    "typing-extensions>=4.0.0",  # Type hints
]
```

### **Optional Dependencies**

```toml
[project.optional-dependencies]
spark = [
    "pyspark>=3.3.0",       # Spark engine
]

pandas = [
    "pandas>=1.5.0",        # Pandas engine
    "duckdb>=0.9.0",        # SQL support for PandasWorkflowNode
]

delta = [
    "deltalake>=0.10.0",    # Delta Lake for Pandas
]

azure = [
    "azure-storage-blob>=12.0.0",  # Azure Blob Storage
    "adlfs>=2023.0.0",             # ADLS support
]

formats = [
    "pyarrow>=10.0.0",      # Parquet support
    "fastavro>=1.7.0",      # Avro support
]

all = [
    "pyspark>=3.3.0",
    "pandas>=1.5.0",
    "duckdb>=0.9.0",
    "deltalake>=0.10.0",
    "azure-storage-blob>=12.0.0",
    "adlfs>=2023.0.0",
    "pyarrow>=10.0.0",
    "fastavro>=1.7.0",
]

dev = [
    "pytest>=7.0.0",
    "pytest-cov>=4.0.0",
    "black>=23.0.0",
    "mypy>=1.0.0",
]
```

### **Installation Examples**

```bash
# Minimal (Pandas only)
pip install odibi

# With Spark
pip install odibi[spark]

# With Pandas + DuckDB
pip install odibi[pandas]

# Everything
pip install odibi[all]

# Development
pip install odibi[dev]
```

---

## Python API Specification

### **Basic Usage**

```python
from odibi import Pipeline

# Load and run entire project
pipeline = Pipeline.from_yaml("project.yaml")
results = pipeline.run()

# Access results
print(f"Completed: {len(results.completed)}")
print(f"Failed: {len(results.failed)}")
print(f"Skipped: {len(results.skipped)}")

# Get story
story_path = results.story_path
print(f"Story saved to: {story_path}")
```

### **Run Specific Pipeline**

```python
# Run one pipeline file
results = pipeline.run_pipeline("pipelines/silver/dryer_1.yaml")

# Run by name
results = pipeline.run_pipeline(name="nkc_dryer_1")
```

### **Run with Options**

```python
# Resume from failure
results = pipeline.run(resume=True)

# Override engine
results = pipeline.run(engine="pandas")

# Run specific node only
results = pipeline.run_node("dryer_cleaned")

# Dry-run (validate only)
validation = pipeline.validate()
print(validation.errors)
```

### **Results Object**

```python
class PipelineResults:
    completed: List[str]        # Node names that succeeded
    failed: List[str]           # Node names that failed
    skipped: List[str]          # Node names that were skipped
    metadata: Dict[str, Dict]   # Metadata per node
    story_path: str             # Path to generated story
    duration: float             # Total execution time

    def get_node_result(self, name: str) -> NodeResult:
        """Get detailed result for specific node"""

    def to_dict(self) -> Dict:
        """Convert to dictionary"""
```

### **Error Handling**

```python
from odibi import Pipeline
from odibi.exceptions import (
    ConfigValidationError,
    ConnectionError,
    DependencyError,
    NodeExecutionError
)

try:
    pipeline = Pipeline.from_yaml("project.yaml")
    results = pipeline.run()
except ConfigValidationError as e:
    print(f"Config error: {e.message}")
    print(f"Location: {e.file}:{e.line}")
except ConnectionError as e:
    print(f"Connection '{e.connection_name}' failed: {e.reason}")
except DependencyError as e:
    print(f"Circular dependency detected: {e.cycle}")
except NodeExecutionError as e:
    print(f"Node '{e.node_name}' failed: {e.error}")
    print(f"See story for details: {e.story_path}")
```

### **Programmatic Pipeline Building**

```python
from odibi import Pipeline, Node, ReadConfig, TransformConfig, WriteConfig

# Build pipeline in Python (alternative to YAML)
pipeline = Pipeline(
    project="My Project",
    engine="pandas",
    connections={
        "local": LocalConnection()
    }
)

# Add nodes
pipeline.add_node(
    Node(
        name="read_data",
        read=ReadConfig(
            connection="local",
            path="data/input.csv",
            format="csv"
        )
    )
)

pipeline.add_node(
    Node(
        name="transform",
        depends_on=["read_data"],
        transform=TransformConfig(
            steps=["SELECT * FROM read_data WHERE amount > 0"]
        )
    )
)

results = pipeline.run()
```

---

## Format Handling

### **Supported Formats**

| Format | Spark | Pandas | Notes |
|--------|-------|--------|-------|
| **csv** | ✅ | ✅ | Native support |
| **parquet** | ✅ | ✅ | Requires pyarrow |
| **json** | ✅ | ✅ | Native support |
| **delta** | ✅ | ✅ | Pandas requires deltalake package |
| **avro** | ✅ | ✅ | Requires fastavro |
| **sql** | ✅ | ✅ | SQL tables/databases |

### **Format Configuration**

**Always explicit (required field):**

```yaml
read:
  connection: azure_bronze
  path: data/sales.csv
  format: csv  # REQUIRED - never inferred
  options:
    delimiter: ","
    header: true
```

**Format-specific options:**

```yaml
# CSV
read:
  format: csv
  options:
    delimiter: ","
    header: true
    encoding: utf-8

# Parquet
read:
  format: parquet
  options:
    compression: snappy

# Delta
read:
  format: delta
  options:
    version: 5  # Time travel
    # OR
    timestamp: "2025-01-01"

# SQL
read:
  format: sql
  table: sales_fact
  options:
    query: "SELECT * FROM sales WHERE year = 2025"
```

### **Why Explicit Format?**

Aligns with core principle: **"If it appears in the story, it must be in the config."**

- ✅ No magic behavior
- ✅ Story shows exactly what user configured
- ✅ Clear for teaching/documentation
- ✅ Handles edge cases (`.dat`, `.txt`, custom extensions)
- ✅ Simple validation

---

## Connection Validation Strategy

### **Two-Phase Validation**

**Phase 1: Config Validation (on load)**
```python
# When loading project.yaml
- Validate YAML structure (Pydantic)
- Check required fields present
- Validate connection types supported
- Check environment variables defined
```

**Phase 2: Connection Validation (lazy)**
```python
# When node first uses connection
- Test connectivity
- Validate credentials
- Check permissions
- Cache validation result
```

### **Validation Behavior**

```yaml
# project.yaml
connections:
  azure_bronze:
    type: azure_blob
    account: ${AZURE_ACCOUNT}
    validate: eager  # Optional: validate on load (default: lazy)
```

**Eager validation:**
- Tests all connections when loading project
- Slower startup
- Fail-fast (catches connection issues before execution)

**Lazy validation (default):**
- Tests connection on first use
- Faster startup
- Fails when node runs

### **Error Messages**

```
✗ Connection validation failed: azure_bronze
  Reason: Unable to authenticate with Azure Blob Storage
  Account: myaccount
  Container: bronze
  Error: AuthenticationError: Invalid credentials

  Check:
  1. Environment variable AZURE_KEY is set
  2. Key has correct permissions for container 'bronze'
  3. Account name 'myaccount' is correct
```

---

## Testing Strategy

### **Unit Tests**

**Config Layer:**
- [ ] Pydantic schema validation
- [ ] YAML parsing (valid + invalid)
- [ ] Environment variable resolution
- [ ] Connection validation

**Orchestration Layer:**
- [ ] Dependency graph builder
- [ ] Topological sort
- [ ] Circular dependency detection
- [ ] Parallel execution logic

**Node Layer:**
- [ ] Node execution (read/transform/write)
- [ ] Context (Spark + Pandas)
- [ ] Retry logic
- [ ] Skip logic

**Engine Layer:**
- [ ] Provider dispatch
- [ ] Format handling
- [ ] Connection resolution

### **Integration Tests**

**End-to-end pipelines:**
```python
def test_simple_pipeline():
    """Test: CSV read → transform → Parquet write"""
    pipeline = Pipeline.from_yaml("tests/fixtures/simple.yaml")
    results = pipeline.run()

    assert len(results.completed) == 3
    assert len(results.failed) == 0
    assert output_file_exists("output.parquet")

def test_parallel_execution():
    """Test: 10 independent nodes run in parallel"""
    start = time.time()
    results = pipeline.run()
    duration = time.time() - start

    # Should take ~1x time, not 10x (parallel)
    assert duration < 2.0  # Each node takes 1s
    assert len(results.completed) == 10

def test_retry_and_skip():
    """Test: Node fails → retries → skips dependents"""
    results = pipeline.run()

    assert "failing_node" in results.failed
    assert "dependent_node" in results.skipped
    assert results.metadata["failing_node"]["attempts"] == 3
```

### **Test Fixtures**

```
tests/
├── fixtures/
│   ├── simple.yaml          # Simple 3-node pipeline
│   ├── parallel.yaml        # 10 parallel nodes
│   ├── dependencies.yaml    # Complex dependency graph
│   ├── data/
│   │   ├── input.csv
│   │   └── reference.json
└── test_*.py
```

### **Mocking**

```python
from unittest.mock import Mock
from odibi.connections import AzureBlobConnection

def test_azure_connection():
    """Test connection without actual Azure credentials"""
    mock_conn = Mock(spec=AzureBlobConnection)
    mock_conn.get_file_path.return_value = "abfss://container/path"

    node = ReadNode(connection=mock_conn, path="data.csv")
    # Test node logic without real Azure
```

---

## Migration from v2 to ODIBI

### **Example: Energy Efficiency Dryer Pipeline**

**v2 (Python function):**
```python
def process_nkc_germ_dryer_1(**kwargs):
    steps = [
        (get_energy_efficiency_data, {
            "source_table": "qat_energy_efficiency.nkc_germ_dryer_1_bronze",
            "tags_table": "qat_energy_efficiency.aspentags",
        }),
        {"step_type": "config", "step_value": "pivot", "params": {...}},
        (get_asset_constants, {...}),
    ]
    return run_standard_transformation(steps=steps, ...)
```

**ODIBI (YAML config):**
```yaml
pipeline: nkc_germ_dryer_1

nodes:
  - name: aspentags
    read:
      connection: azure_bronze
      path: delta/aspentags
      format: delta
    cache: true

  - name: dryer_bronze
    read:
      connection: azure_bronze
      path: delta/nkc_germ_dryer_1_bronze
      format: delta

  - name: dryer_enriched
    depends_on: [aspentags, dryer_bronze]
    transform:
      steps:
        - function: get_energy_efficiency_data
          params:
            source_table: dryer_bronze
            tags_table: aspentags
        - operation: pivot
          params: {...}
        - function: get_asset_constants
          params: {...}

  - name: dryer_output
    depends_on: [dryer_enriched]
    write:
      connection: azure_silver
      path: delta/nkc_germ_dryer_1_cleaned
      format: delta
      mode: overwrite
```

### **Key Changes**

1. ✅ **Python → YAML**: Logic in config, not code
2. ✅ **Explicit dependencies**: `depends_on` instead of implicit flow
3. ✅ **Explicit nodes**: Each operation is a named node
4. ✅ **Connections**: Defined once, referenced by name
5. ✅ **Format**: Always explicit

---

## Next Steps

### **After Phase 1:**

**Phase 2: Polish & Extend**
- CLI tools
- Advanced validation (dry-run)
- Performance optimizations
- More connection types (AWS S3, GCP)

**Phase 3: Ecosystem**
- `odibi-transformers` package (pre-built transformers)
- `odibi-databricks` plugin (Databricks helpers)
- Template generators
- Migration tools (v2 → ODIBI)

**Phase 4: Community**
- Documentation site
- Tutorial videos
- Example projects
- Community contributions

---

## Questions & Decisions Log

**Q: Should connections be nodes?**  
**A:** No. Connections are infrastructure, defined in project.yaml. Nodes reference them by name.

**Q: Should references be auto-available?**  
**A:** No. Everything must be an explicit node with dependencies.

**Q: Should caching be automatic?**  
**A:** No. User decides via `cache: true` in config.

**Q: Should logging be complex?**  
**A:** No. Use Python's standard logging module. Stories capture everything else.

**Q: Should we use events?**  
**A:** No. Direct execution with metadata capture is simpler.

**Q: Backward compatibility with v2?**  
**A:** No. Clean slate. ODIBI is a new framework.

**Q: Should we stop on first failure?**  
**A:** No. Retry, then skip dependents, continue with independent nodes.

**Q: Should everything be in one repo?**  
**A:** Core in one repo. Transformers in separate package later.

**Q: Should we use layers for execution ordering?**  
**A:** No. Node dependencies already provide ordering. Layers are redundant and add complexity. Use folder organization for human readability only.

---

## Implementation Refinements

### **Simplifications Applied**

#### **1. Layers Deprecated (Optional Future Feature)**

**Decision:** Drop layers from v1.0. Node dependencies provide all necessary ordering and parallelization.

**Rationale:**
- Dependency graph already handles execution order
- Layers add second dependency system that can conflict
- Lineage tracking uses node `depends_on`, not layers
- Storage defined per node in `write.connection`/`write.path`

**Keep:** Folder structure for organization (`pipelines/bronze/`, `/silver/`, `/gold/`)  
**Drop:** Layer execution enforcement in project.yaml

If needed later, layers can be added as a visualization/grouping feature without affecting execution.

---

#### **2. Unified Context API**

**Problem:** Different APIs for Spark vs Pandas breaks engine-agnostic transforms.

**Solution:** Single interface that works for both engines:

```python
class Context:
    """Unified data context for node execution."""

    def register(self, name: str, df: Union[SparkDF, PandasDF]) -> None:
        """Register DataFrame for use in downstream nodes."""

    def get(self, name: str) -> Union[SparkDF, PandasDF]:
        """Retrieve registered DataFrame."""

    def has(self, name: str) -> bool:
        """Check if DataFrame exists in context."""
```

**Implementation:**
- **Spark backend:** Uses temp views (`createOrReplaceTempView`)
- **Pandas backend:** Uses dictionary storage
- **Transform functions:** Engine-agnostic by default

```yaml
# Same config works on both engines
transform:
  steps:
    - "SELECT * FROM aspentags WHERE plant = 'NKC'"
    - function: enrich_data
      params:
        reference_table: aspentags  # Works on both engines
```

---

#### **3. Smart Format Inference (v1.1 Feature)**

**v1.0:** Format required (explicit over implicit)
```yaml
read:
  path: data/sales.csv
  format: csv  # REQUIRED
```

**v1.1:** Infer from extension, allow override
```yaml
# Inferred (80% of cases)
read:
  path: data/sales.csv  # format: csv (auto-detected)

# Explicit override (edge cases)
read:
  path: data/export.dat
  format: csv  # .dat file is actually CSV
```

**Story always shows actual format used** (whether inferred or explicit).

---

### **Critical Missing Pieces**

#### **1. Function Registry with Type System**

**Problem:** Users can't discover available functions or required params.

**Solution:** Decorator-based registry with type hints:

```python
# In user's transformation module
from odibi import transform
from odibi.context import Context
from typing import Optional
import pandas as pd

@transform
def get_asset_constants(
    context: Context,
    tags_table: str,
    plant: str,
    asset: str,
    default_efficiency: Optional[float] = 0.85
) -> pd.DataFrame:
    """Get constant values for specific asset from tags table.

    Args:
        tags_table: Name of registered tags DataFrame
        plant: Plant code (e.g., 'NKC')
        asset: Asset name (e.g., 'Germ Dryer 1')
        default_efficiency: Fallback efficiency value

    Returns:
        DataFrame with asset constants
    """
    tags = context.get(tags_table)
    filtered = tags[
        (tags['Plant'] == plant) &
        (tags['Asset'] == asset)
    ]
    return filtered

# Usage in YAML (with validation)
transform:
  steps:
    - function: get_asset_constants
      params:
        tags_table: aspentags
        plant: NKC
        asset: "Germ Dryer 1"
        # default_efficiency: optional, uses 0.85
```

**Benefits:**
- Runtime param validation against function signature
- Auto-generated docs from docstrings
- IDE autocomplete (via JSON schema generation)
- Clear error: `Missing required parameter 'plant' for function 'get_asset_constants'`

**Implementation:**
```python
class FunctionRegistry:
    """Global registry of transform functions."""

    _functions: Dict[str, Callable] = {}

    @classmethod
    def register(cls, func: Callable) -> Callable:
        """Register transform function."""
        cls._functions[func.__name__] = func
        return func

    @classmethod
    def get(cls, name: str) -> Callable:
        """Retrieve registered function."""
        if name not in cls._functions:
            raise ValueError(f"Transform function '{name}' not found")
        return cls._functions[name]

    @classmethod
    def validate_params(cls, name: str, params: Dict) -> None:
        """Validate params against function signature."""
        import inspect
        func = cls.get(name)
        sig = inspect.signature(func)
        # Validate required params, types, etc.
```

---

#### **2. Development Workflow Tools**

**CLI Commands:**

```bash
# Validate config without execution
odibi validate project.yaml

# Run single node with mock data
odibi run-node dryer_cleaned \
  --project project.yaml \
  --mock aspentags=data/test_tags.csv \
  --mock dryer_bronze=data/test_bronze.csv \
  --show-output

# Show dependency graph
odibi graph --pipeline nkc_dryer_1

# List all nodes in pipeline
odibi list --pipeline nkc_dryer_1

# Run with debug logging
odibi run project.yaml --log-level DEBUG
```

**Python API for interactive development:**

```python
from odibi import Pipeline
import pandas as pd

# Load pipeline
p = Pipeline.from_yaml("project.yaml")

# Run single node with test data (bypass dependencies)
result = p.run_node(
    "dryer_cleaned",
    mock_data={
        "aspentags": pd.read_csv("test_tags.csv"),
        "dryer_bronze": pd.read_csv("test_bronze.csv")
    }
)

# Inspect output
print(result.df.head())
print(result.metadata)
print(result.duration)

# Test individual transform function
from my_transforms import get_asset_constants
output = get_asset_constants(
    context=mock_context,
    tags_table="aspentags",
    plant="NKC",
    asset="Germ Dryer 1"
)
```

---

#### **3. Context-Aware Error Messages**

**Problem:** Stack traces through YAML config are confusing.

**Solution:** Capture execution context and provide actionable errors:

```python
class ExecutionContext:
    """Runtime context for error reporting."""
    node_name: str
    config_file: str
    config_line: int
    step_index: int
    total_steps: int
    input_schema: List[str]
    input_shape: Tuple[int, int]
    previous_steps: List[str]
```

**Error output example:**

```
✗ Node execution failed: dryer_cleaned
  Location: pipelines/silver/dryer_1.yaml:382
  Step: 2 of 4 (pivot operation)

  Error: KeyError: 'Description'
  Column 'Description' not found in DataFrame

  Available columns: ['Time_Stamp', 'Plant', 'Asset', 'Value']

  Context:
    Input shape: (1000, 4)
    Previous step: "SELECT * FROM dryer_bronze" ✓
    Failed step:
      operation: pivot
      params:
        pivot_column: Description  ← Missing column

  Suggestions:
    1. Check that dryer_bronze contains 'Description' column
    2. Verify previous step output: odibi run-node dryer_bronze --show-output
    3. Enable debug mode: odibi run --log-level DEBUG

  Story: stories/nkc_dryer_1_2025-11-05_14-30-22.md
```

**Implementation:**
```python
class NodeExecutionError(Exception):
    """Enhanced error with execution context."""

    def __init__(
        self,
        message: str,
        context: ExecutionContext,
        suggestions: List[str] = None
    ):
        self.message = message
        self.context = context
        self.suggestions = suggestions or []
        super().__init__(self._format_error())

    def _format_error(self) -> str:
        """Generate rich error message."""
        # Format with context, suggestions, etc.
```

---

## Updated Phase 1 Scope

### **v1.0 Deliverables (MVP)**

**Core Engine:**
- ✅ Pydantic config schemas (project, pipeline, node)
- ✅ Unified Context API (`register`/`get`)
- ✅ Node execution (read/transform/write)
- ✅ Function registry with type validation
- ✅ Dependency graph builder (topological sort, cycle detection)
- ✅ Pipeline executor (parallel execution, retry logic)
- ✅ Story generator (automatic documentation)
- ✅ Context-aware error messages

**Connections (adapt from v2):**
- ✅ LocalConnection (file system)
- ✅ AzureBlobConnection
- ✅ DeltaConnection

**Engines:**
- ✅ Pandas engine (primary)
- ✅ Spark engine (adapt from v2)

**CLI:**
- ✅ `odibi run project.yaml`
- ✅ `odibi validate project.yaml`
- ✅ `odibi run-node <name> --project project.yaml`

**Testing:**
- ✅ Unit tests (config, graph, node execution)
- ✅ Integration tests (end-to-end pipelines)
- ✅ Test fixtures (simple/parallel/dependencies)

**Documentation:**
- ✅ README with quick start
- ✅ API reference (auto-generated from docstrings)
- ✅ Migration guide (v2 → ODIBI)

### **Deferred to v1.1**

- Smart format inference
- Graph visualization
- Advanced CLI (interactive mode)
- More connection types (AWS S3, GCP, SQL Server)
- Performance optimizations (lazy loading, caching)
- Layer support (if needed for visualization)

---

## Implementation Priority

**Week 1-2: Foundation**
1. Project structure + packaging (pyproject.toml)
2. Pydantic schemas (config validation)
3. Unified Context API
4. Function registry

**Week 3-4: Core Engine**
5. Node base class + execution
6. Dependency graph builder
7. Pipeline executor (serial execution first)
8. Error handling + context

**Week 5-6: I/O Layer**
9. Connection abstraction (adapt v2)
10. ReaderProvider/SaverProvider (adapt v2)
11. Pandas engine
12. Spark engine (adapt v2)

**Week 7-8: Polish**
13. Story generator
14. CLI tools
15. Tests (unit + integration)
16. Documentation

---

**Document Version:** 1.1  
**Last Updated:** 2025-11-05  
**Status:** Ready for implementation (with refinements applied)

---

**Ready to build ODIBI v1.0!** 🚀
