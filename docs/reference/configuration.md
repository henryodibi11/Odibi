# ODIBI Configuration System Explained

**Last Updated:** 2025-11-21
**Author:** Henry Odibi
**Purpose:** Demystify how YAML configs, Python classes, and execution flow together

---

## 📑 Table of Contents

1. [The Big Picture](#the-big-picture)
2. [Configuration Flow (Concept → Execution)](#configuration-flow-concept--execution)
3. [Three Layers of Configuration](#three-layers-of-configuration)
4. [Example: Tracing a Pipeline from YAML to Execution](#example-tracing-a-pipeline-from-yaml-to-execution)
5. [Key Concepts Explained](#key-concepts-explained)
6. [Common Confusion Points](#common-confusion-points)
7. [Decision Trees](#decision-trees)
8. [Quick Reference](#quick-reference)

---

## The Big Picture

ODIBI has **three distinct layers** that work together:

```
┌─────────────────────────────────────────────────────────────┐
│  LAYER 1: YAML Configuration (What You Write)              │
│  - Declarative syntax (project.yaml, pipelines/)            │
│  - Human-readable, version-controlled                       │
│  - Defines WHAT to do, not HOW                              │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       │ Parsed by
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  LAYER 2: Pydantic Models (config.py)                      │
│  - Validates YAML structure                                 │
│  - Enforces required fields, types, constraints             │
│  - Converts YAML → Python objects                           │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       │ Used by
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  LAYER 3: Runtime Classes (pipeline.py, node.py, etc.)     │
│  - Executes the actual work                                 │
│  - Manages context, engines, connections                    │
│  - Generates stories, handles errors                        │
└─────────────────────────────────────────────────────────────┘
```

**Key Insight:**  
You interact with **Layer 1 (YAML)**, but under the hood, Pydantic (**Layer 2**) ensures correctness before Runtime (**Layer 3**) does the work.

---

## Configuration Flow (Concept → Execution)

Let's trace a real pipeline from file to execution:

### Step 1: You Write YAML (User)

```yaml
# examples/templates/example_local.yaml
project: My Pipeline
engine: pandas
connections:
  local:
    type: local
    base_path: ./data
pipelines:
  - pipeline: bronze_to_silver
    nodes:
      - name: load_raw_sales
        read:
          connection: local
          path: bronze/sales.csv
          format: csv
```

### Step 2: YAML → Pydantic Models (Automatic)

When you call `Pipeline.from_yaml("examples/templates/example_local.yaml")`:

```python
# pipeline.py (line 317)
with open(yaml_path, "r") as f:
    config = yaml.safe_load(f)  # Loads YAML as Python dict

# Parse into Pydantic models (config.py)
project_config = ProjectConfig(**config)  # Validates project/engine/connections
pipeline_config = PipelineConfig(**config['pipelines'][0])  # Validates pipeline structure
```

**What Pydantic does:**
- Checks that `project`, `engine`, `connections` exist
- Ensures `engine` is either "pandas" or "spark" (not "panda" or "pands")
- Validates each node has required fields (`name`, at least one operation)
- Converts strings to enums where needed (e.g., `engine: "pandas"` → `EngineType.PANDAS`)

**If validation fails:**
```
ValidationError: 1 validation error for PipelineConfig
nodes.0.read.connection
  field required (type=value_error.missing)
```
This is **Layer 2** catching errors before execution.

### Step 3: Create Connections (Automatic)

```python
# pipeline.py (lines 342-351) - PipelineManager.from_yaml()
connections = {}
for conn_name, conn_config in config.get("connections", {}).items():
    if conn_config.get("type") == "local":
        connections[conn_name] = LocalConnection(
            base_path=conn_config.get("base_path", "./data")
        )
```

**Result:** Python objects ready to use:
```python
connections = {
    "local": LocalConnection(base_path="./data")
}
```

### Step 4: Create PipelineManager (Automatic)

```python
# pipeline.py (lines 280-314)
manager = PipelineManager(
    yaml_config=config,
    engine="pandas",
    connections=connections,
    story_config=config.get("story", {})
)
```

**What PipelineManager does:**
- Creates a `Pipeline` object for **each** pipeline in the YAML
- Stores them in `self._pipelines` dictionary keyed by pipeline name
- Example: `manager._pipelines = {"bronze_to_silver": Pipeline(...), "silver_to_gold": Pipeline(...)}`

### Step 5: Run Pipelines (User)

```python
# You call:
results = manager.run()  # Runs ALL pipelines

# Or:
result = manager.run('bronze_to_silver')  # Runs specific pipeline
```

**What happens:**
1. `PipelineManager.run()` looks up the pipeline by name
2. Calls `Pipeline.run()` (line 134)
3. `Pipeline.run()` orchestrates node execution:
   - Builds dependency graph
   - Topologically sorts nodes
   - Executes each node in order
   - Passes data via Context
   - Generates story

---

## Three Layers of Configuration

### Layer 1: YAML Files (Declarative)

**Purpose:** Human-readable, version-controlled pipeline definitions

**Key Files:**
- **Project-level:** `project.yaml` or any YAML with `project` + `connections` + `pipelines`
- **Pipeline-level:** Individual YAML files with specific pipelines

**What You Define:**
| Section | Purpose | Required |
|---------|---------|----------|
| `project` | Project name | ✅ Yes |
| `engine` | Execution engine (pandas/spark) | ✅ Yes |
| `connections` | Where data lives | ✅ Yes |
| `pipelines` | List of pipelines | ✅ Yes |
| `story` | Story generation config | ✅ Yes |

**Example:**
```yaml
project: Sales Analytics
engine: pandas
connections:
  warehouse:
    type: local
    base_path: /data/warehouse
pipelines:
  - pipeline: daily_sales
    nodes: [...]
```

---

### Layer 2: Pydantic Models (Validation)

**Purpose:** Type-safe, validated Python objects

**Key File:** [`odibi/config.py`](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/odibi/config.py)

**Main Models:**

#### ProjectConfig (Line 266)
```python
class ProjectConfig(BaseModel):
    project: str  # Required
    version: str = "1.0.0"  # Default
    engine: EngineType = EngineType.PANDAS  # Default
    connections: Dict[str, Dict[str, Any]]  # Required
    story: StoryConfig  # Required
    pipelines: List[PipelineConfig]  # Required
    retry: RetryConfig = RetryConfig()  # Default
    logging: LoggingConfig = LoggingConfig()  # Default
```

**Maps to YAML:**
```yaml
project: My Pipeline      # → project
version: "2.0.0"          # → version
engine: pandas            # → engine (validated as EngineType.PANDAS)
connections:
  data:                   # → connections["data"]
    type: local
    base_path: ./data
  outputs:                # → connections["outputs"]
    type: local
    base_path: ./outputs
  api_source:             # → connections["api_source"]
    type: http
    base_url: "https://api.example.com/v1"
    headers:
      Authorization: "Bearer ${API_TOKEN}"
story:
  connection: outputs     # → story.connection (required)
  path: stories/          # → story.path
  auto_generate: true     # → story.auto_generate
  max_sample_rows: 10     # → story.max_sample_rows
  retention_days: 30      # → story.retention_days
  retention_count: 100    # → story.retention_count
retry:
  enabled: true           # → retry.enabled
  max_attempts: 3         # → retry.max_attempts
  backoff: exponential    # → retry.backoff
logging:
  level: INFO             # → logging.level
pipelines:                # → pipelines (list of pipeline configs)
  - pipeline: example
    nodes: [...]
```

#### PipelineConfig (Line 203)
```python
class PipelineConfig(BaseModel):
    pipeline: str  # Required (pipeline name)
    description: Optional[str] = None
    layer: Optional[str] = None
    nodes: List[NodeConfig]  # Required (at least one node)
```

**Maps to YAML:**
```yaml
pipelines:
  - pipeline: bronze_to_silver  # → pipeline
    layer: transformation       # → layer
    description: "Clean data"   # → description
    nodes: [...]               # → nodes
```

#### NodeConfig (Line 172)
```python
class NodeConfig(BaseModel):
    name: str  # Required (unique within pipeline)
    depends_on: List[str] = []
    read: Optional[ReadConfig] = None
    transform: Optional[TransformConfig] = None
    write: Optional[WriteConfig] = None
    cache: bool = False
    sensitive: Union[bool, List[str]] = False  # PII Masking
```

**Maps to YAML:**
```yaml
nodes:
  - name: load_raw_sales         # → name
    depends_on: [prev_node]      # → depends_on
    sensitive: ["email"]         # → sensitive (Redact email in reports)
    read:                        # → read (ReadConfig)
      connection: local
      path: bronze/sales.csv
      format: csv
    cache: true                  # → cache
```

**Validation Rules:**
- Node must have **at least one** of: `read`, `transform`, `write`
- All node names must be **unique** within a pipeline
- Connections referenced in `read.connection` or `write.connection` should exist (warned, not enforced)

---

### Layer 3: Runtime Classes (Execution)

**Purpose:** Execute the actual work

**Key Files:**
- [`odibi/pipeline.py`](file:///c:/Users/hodibi/OneDrive%20-%20Ingredion/Desktop/Repos/Odibi/odibi/pipeline.py) - Pipeline orchestration
- `odibi/node.py` - Individual node execution
- `odibi/context.py` - Data passing between nodes
- `odibi/engine/pandas_engine.py` - Actual read/write/transform logic

**Main Classes:**

#### PipelineManager (Line 280)
```python
class PipelineManager:
    def __init__(self, yaml_config, engine, connections, story_config):
        self._pipelines = {}  # Dict[pipeline_name -> Pipeline]
        for pipeline_config_dict in yaml_config["pipelines"]:
            pipeline_config = PipelineConfig(**pipeline_config_dict)
            self._pipelines[pipeline_config.pipeline] = Pipeline(...)

    def run(self, pipelines=None):
        # Run all or specific pipelines
```

**Responsibilities:**
- Load and validate YAML
- Instantiate connections
- Create Pipeline objects for each pipeline
- Orchestrate multi-pipeline execution
- Return results

#### Pipeline (Line 63)
```python
class Pipeline:
    def __init__(self, pipeline_config, engine, connections, ...):
        self.config = pipeline_config  # PipelineConfig from Layer 2
        self.engine = PandasEngine()   # Or SparkEngine
        self.context = create_context(engine)
        self.graph = DependencyGraph(pipeline_config.nodes)

    def run(self):
        execution_order = self.graph.topological_sort()
        for node_name in execution_order:
            node = Node(...)
            node_result = node.execute()
```

**Responsibilities:**
- Build dependency graph
- Determine execution order
- Execute nodes sequentially (or parallel in future)
- Manage context for data passing
- Generate stories

#### Node (odibi/node.py)
```python
class Node:
    def execute(self):
        # 1. Read data (if read config exists)
        if self.config.read:
            data = self.engine.read(...)
            self.context.register(self.config.name, data)

        # 2. Transform data (if transform config exists)
        if self.config.transform:
            data = self.engine.execute_transform(...)
            self.context.register(self.config.name, data)

        # 3. Write data (if write config exists)
        if self.config.write:
            self.engine.write(...)
```

**Responsibilities:**
- Execute read → transform → write for a single node
- Use engine for actual operations
- Register results in context
- Return NodeResult

---

## Example: Tracing a Pipeline from YAML to Execution

Let's trace this simple YAML:

```yaml
project: Simple Pipeline
engine: pandas
connections:
  local:
    type: local
    base_path: ./data
  outputs:
    type: local
    base_path: ./outputs
story:
  connection: outputs
  path: stories/
  enabled: true
pipelines:
  - pipeline: example
    nodes:
      - name: load_data
        read:
          connection: local
          path: input.csv
          format: csv
        cache: true

      - name: clean_data
        depends_on: [load_data]
        transform:
          steps:
            - "SELECT * FROM load_data WHERE amount > 0"

      - name: save_data
        depends_on: [clean_data]
        write:
          connection: local
          path: output.parquet
          format: parquet
          mode: overwrite
```

### Execution Trace

**1. User calls:**
```python
from odibi.pipeline import Pipeline
manager = Pipeline.from_yaml("simple.yaml")
```

**2. `Pipeline.from_yaml()` delegates to `PipelineManager.from_yaml()` (line 109)**

**3. `PipelineManager.from_yaml()` (line 317):**
```python
# Load YAML
with open("simple.yaml") as f:
    config = yaml.safe_load(f)
# config = {
#     "project": "Simple Pipeline",
#     "engine": "pandas",
#     "connections": {"local": {"type": "local", "base_path": "./data"}},
#     "pipelines": [{"pipeline": "example", "nodes": [...]}]
# }

# Validate project config (entire YAML - single source of truth)
project_config = ProjectConfig(**config)
# ✅ Validation passed - checks:
#    - Required fields: project, connections, story, pipelines
#    - story.connection exists in connections
#    - engine is valid (pandas or spark)

# Create connections
connections = {
    "local": LocalConnection(base_path="./data")
}

# Create PipelineManager
manager = PipelineManager(
    project_config=project_config,
    engine="pandas",
    connections=connections
)
```

**4. `PipelineManager.__init__()` (line 283):**
```python
# Loop through pipelines in YAML
for pipeline_config_dict in config["pipelines"]:
    # Validate pipeline config
    pipeline_config = PipelineConfig(
        pipeline="example",
        nodes=[
            NodeConfig(name="load_data", read=ReadConfig(...), cache=True),
            NodeConfig(name="clean_data", depends_on=["load_data"], transform=TransformConfig(...)),
            NodeConfig(name="save_data", depends_on=["clean_data"], write=WriteConfig(...))
        ]
    )
    # ✅ Validation passed (all nodes have unique names, at least one operation each)

    # Create Pipeline instance
    self._pipelines["example"] = Pipeline(
        pipeline_config=pipeline_config,
        engine="pandas",
        connections={"local": LocalConnection(...)},
        story_config={}
    )
```

**5. User runs:**
```python
results = manager.run()  # Run all pipelines
```

**6. `PipelineManager.run()` (line 363):**
```python
# pipelines=None means run all
pipeline_names = list(self._pipelines.keys())  # ["example"]

# Run each pipeline
for name in pipeline_names:
    results[name] = self._pipelines[name].run()
```

**7. `Pipeline.run()` (line 134):**
```python
# Get execution order from dependency graph
execution_order = self.graph.topological_sort()
# Returns: ["load_data", "clean_data", "save_data"]

# Execute nodes in order
for node_name in execution_order:  # "load_data"
    node_config = self.graph.nodes["load_data"]
    node = Node(
        config=node_config,
        context=self.context,
        engine=self.engine,  # PandasEngine
        connections={"local": LocalConnection(...)}
    )
    node_result = node.execute()
```

**8. `Node.execute()` for "load_data" (odibi/node.py):**
```python
# Node has read config
if self.config.read:
    # Get connection
    conn = self.connections["local"]  # LocalConnection(base_path="./data")
    full_path = conn.get_path("input.csv")  # "./data/input.csv"

    # Read using engine
    data = self.engine.read(
        path=full_path,
        format="csv",
        options={}
    )
    # data = pandas.DataFrame(...)

    # Register in context
    self.context.register("load_data", data)
```

**9. `Node.execute()` for "clean_data":**
```python
# Node has transform config
if self.config.transform:
    sql = "SELECT * FROM load_data WHERE amount > 0"
    data = self.engine.execute_sql(sql, self.context)
    # Engine gets "load_data" DataFrame from context
    # Executes SQL using pandasql or duckdb
    # Returns filtered DataFrame

    self.context.register("clean_data", data)
```

**10. `Node.execute()` for "save_data":**
```python
# Node has write config
if self.config.write:
    data = self.context.get("clean_data")  # Get from previous node
    conn = self.connections["local"]
    full_path = conn.get_path("output.parquet")  # "./data/output.parquet"

    self.engine.write(
        data=data,
        path=full_path,
        format="parquet",
        mode="overwrite",
        options={}
    )
    # Writes DataFrame to ./data/output.parquet
```

**11. Story generation (if enabled):**
```python
story_path = self.story_generator.generate(
    node_results={...},
    completed=["load_data", "clean_data", "save_data"],
    failed=[],
    ...
)
# Generates markdown story in ./stories/
```

**12. Return results to user:**
```python
results = {
    "example": PipelineResults(
        pipeline_name="example",
        completed=["load_data", "clean_data", "save_data"],
        failed=[],
        duration=2.3,
        story_path="./stories/example_20251107_143025.md"
    )
}
```

---

## Key Concepts Explained

### 1. Config vs Runtime

**Config (Layer 1 + 2):**
- What you **declare** in YAML
- Validated by Pydantic
- Immutable once loaded
- Example: `ReadConfig(connection="local", path="input.csv", format="csv")`

**Runtime (Layer 3):**
- What **executes** the work
- Uses config to make decisions
- Mutable state (context, results)
- Example: `PandasEngine.read(path="./data/input.csv", format="csv")` → returns DataFrame

**Why separate?**
- **Validation happens early** (before execution)
- **Config is reusable** (can run same config multiple times)
- **Easier testing** (mock runtime, test config separately)

### 2. Connection: Config vs Object

**Connection Config (YAML):**
```yaml
connections:
  local:
    type: local
    base_path: ./data
```

**Connection Object (Python):**
```python
connections = {
    "local": LocalConnection(base_path="./data")
}
```

**What's the difference?**
- Config is **declarative** (YAML dict)
- Object is **executable** (Python class with methods like `.get_path()`)

**Why both?**
- YAML is **portable** (version controlled, shareable)
- Objects are **functional** (can call methods, maintain state)

### 3. Pipeline vs PipelineManager

**Pipeline:**
- Represents **one** pipeline
- Has nodes, dependencies, execution logic
- Example: `bronze_to_silver` pipeline

**PipelineManager:**
- Manages **multiple** pipelines
- Loads YAML, creates connections, instantiates Pipelines
- Provides unified API: `manager.run()` runs all, `manager.run('bronze_to_silver')` runs one

**Why `Pipeline.from_yaml()` returns PipelineManager?**
- **Convenience:** Most YAMLs have multiple pipelines
- **Backward compatible:** Users can still call `Pipeline.from_yaml()`
- **Unified API:** `manager.run()` works for 1 or 10 pipelines

### 4. `from_yaml()` - The Boilerplate Eliminator

**Before (manual setup):**
```python
import yaml
from odibi.pipeline import Pipeline
from odibi.config import PipelineConfig
from odibi.connections import LocalConnection

with open("config.yaml") as f:
    config = yaml.safe_load(f)

pipeline_config = PipelineConfig(**config['pipelines'][0])
connections = {
    'local': LocalConnection(base_path=config['connections']['local']['base_path'])
}
pipeline = Pipeline(
    pipeline_config=pipeline_config,
    engine="pandas",
    connections=connections
)
results = pipeline.run()
```

**After (`from_yaml()`):**
```python
from odibi.pipeline import Pipeline

manager = Pipeline.from_yaml("config.yaml")
results = manager.run()
```

**What `from_yaml()` does:**
1. Load YAML
2. Validate with Pydantic
3. Create connection objects
4. Instantiate PipelineManager
5. Return ready-to-run manager

**Result:** 2 lines instead of 15!

### 5. Context - The Data Bus

**Purpose:** Pass data between nodes without explicit function calls

**How it works:**
```python
# Node 1: load_data
data = engine.read(...)
context.register("load_data", data)  # Store DataFrame

# Node 2: clean_data (depends_on: [load_data])
data = context.get("load_data")  # Retrieve DataFrame
cleaned = engine.execute_sql("SELECT * FROM load_data WHERE ...", context)
context.register("clean_data", cleaned)

# Node 3: save_data (depends_on: [clean_data])
data = context.get("clean_data")
engine.write(data, ...)
```

**Why not return values?**
- Nodes execute **sequentially** but independently
- SQL transforms reference DataFrames **by name** (not variable)
- Context provides **unified API** across Pandas and Spark

---

## Common Confusion Points

### Confusion #1: "Why do I see both `pipeline` and `name`?"

**Answer:** Different levels of abstraction!

```yaml
pipelines:                   # List of pipelines
  - pipeline: bronze_to_silver  # ← Pipeline NAME (identifies the pipeline)
    nodes:                     # List of nodes in THIS pipeline
      - name: load_data        # ← Node NAME (identifies the node)
```

**Analogy:**
- `pipeline` is like a **book title** ("Harry Potter")
- `name` is like a **chapter name** ("The Boy Who Lived")

**In code:**
- `PipelineConfig.pipeline` → pipeline name
- `NodeConfig.name` → node name

### Confusion #2: "What's the difference between `connection: local` and `type: local`?"

**Answer:** Different contexts!

**In `connections` section (defining connections):**
```yaml
connections:
  local:           # ← Connection NAME (you choose this)
    type: local    # ← Connection TYPE (system type: local, azure_adls, etc.)
    base_path: ./data
```

**In `read`/`write` section (using connections):**
```yaml
nodes:
  - name: load_data
    read:
      connection: local  # ← References the CONNECTION NAME from above
      path: input.csv
```

**Analogy:**
- `connections` section: "Define a car named 'my_car' of type 'sedan'"
- `read.connection`: "Use the car named 'my_car' to drive somewhere"

### Confusion #3: "Why does `from_yaml()` return a manager instead of a pipeline?"

**Answer:** YAML files typically have **multiple** pipelines!

```yaml
pipelines:
  - pipeline: bronze_to_silver  # Pipeline 1
    nodes: [...]
  - pipeline: silver_to_gold    # Pipeline 2
    nodes: [...]
```

**If it returned a single Pipeline:**
- Which one? The first? All?
- How to run specific pipelines?

**By returning PipelineManager:**
- Access all pipelines
- Run all: `manager.run()`
- Run one: `manager.run('bronze_to_silver')`
- Run some: `manager.run(['bronze_to_silver', 'silver_to_gold'])`

**For single pipeline YAMLs:**
```python
result = manager.run()  # If only 1 pipeline, returns PipelineResults (not dict)
```

### Confusion #4: "What's the difference between `options` and `params`?"

**Answer:** Different operation types!

**`options` (in read/write):**
```yaml
read:
  connection: local
  path: data.csv
  format: csv
  options:           # ← Format-specific options (passed to pandas.read_csv())
    header: 0
    dtype:
      id: str
```
**Maps to:** `pandas.read_csv(path, header=0, dtype={"id": str})`

**`params` (in transform):**
```yaml
transform:
  steps:
    - function: my_custom_function
      params:        # ← Function arguments
        threshold: 0.5
        mode: strict
```
**Maps to:** `my_custom_function(context, threshold=0.5, mode='strict')`

**Key difference:**
- `options` → passed to **engine** (Pandas/Spark I/O functions)
- `params` → passed to **your function**

### Confusion #5: "Where do stories get written?"

**Answer:** Stories use the connection pattern, just like data!

**Story configuration (required):**
```yaml
connections:
  outputs:
    type: local
    base_path: ./outputs

story:
  connection: outputs  # ← References connection name
  path: stories/       # ← Path within connection
  enabled: true
```

**Resolved path:** `./outputs/stories/pipeline_name_20251107_143025.md`

**Why this pattern?**
- **Explicit:** Clear where stories are written (no hidden defaults)
- **Traceable:** Connection-based paths preserve truth
- **Consistent:** Same pattern as `read.connection` and `write.connection`
- **Flexible:** Stories can go to ADLS, DBFS, or local storage

**Before v1.1 (confusing):**
```yaml
# Story path was implicit - where is "stories/" relative to?
connections:
  local:
    type: local
    base_path: ./data
```

**After v1.1 (explicit):**
```yaml
connections:
  outputs:
    type: local
    base_path: ./outputs

story:
  connection: outputs  # Required - must exist in connections
  path: stories/
```

### Confusion #6: "How does SQL find the DataFrames?"

**Answer:** The engine looks them up in the context!

**Your SQL:**
```sql
SELECT * FROM load_data WHERE amount > 0
```

**What the engine does (simplified):**
```python
# PandasEngine.execute_sql()
def execute_sql(self, sql: str, context: Context):
    # 1. Find all table references in SQL
    tables = extract_table_names(sql)  # ["load_data"]

    # 2. Get DataFrames from context
    load_data = context.get("load_data")  # The DataFrame from earlier node

    # 3. Execute SQL using pandasql or duckdb
    result = duckdb.query(sql).to_df()

    return result
```

**Key insight:** Table names in SQL **must match** node names in the pipeline!

---

## Decision Trees

### "Which class do I use?"

```
┌─ Need to load and run a YAML file?
│  ├─ YES → Use `Pipeline.from_yaml("config.yaml")`
│  │         Returns PipelineManager
│  │
│  └─ NO → Are you building custom integrations?
│     ├─ YES → Use `PipelineManager(...)` or `Pipeline(...)` directly
│     └─ NO → Use `Pipeline.from_yaml()` (recommended)
```

### "How do I run my pipelines?"

```
┌─ How many pipelines in YAML?
│  ├─ ONE → `manager.run()` returns PipelineResults
│  ├─ MANY → `manager.run()` returns Dict[name -> PipelineResults]
│  │
│  └─ Want to run specific pipeline(s)?
│     ├─ ONE → `manager.run('pipeline_name')` returns PipelineResults
│     └─ MULTIPLE → `manager.run(['pipe1', 'pipe2'])` returns Dict
```

### "Where does my configuration live?"

```
┌─ Is it about the OVERALL project?
│  ├─ YES → Top level (project, engine, connections, story)
│  │
│  └─ NO → Is it about a PIPELINE?
│     ├─ YES → Under `pipelines:` (pipeline, layer, nodes)
│     │
│     └─ NO → Is it about a NODE?
│        ├─ YES → Under `nodes:` (name, read, transform, write)
│        │
│        └─ NO → Is it about an OPERATION?
│           ├─ READ → Under `read:` (connection, path, format, options)
│           ├─ TRANSFORM → Under `transform:` (steps)
│           └─ WRITE → Under `write:` (connection, path, format, mode, options)
```

---

## Quick Reference

### YAML Structure

```yaml
# PROJECT LEVEL (required)
project: string               # Project name
engine: pandas|spark          # Execution engine

# GLOBAL SETTINGS (optional)
retry:
  enabled: bool
  max_attempts: int
  backoff: exponential|linear|constant
logging:
  level: DEBUG|INFO|WARNING|ERROR
  structured: bool
  metadata: dict

# CONNECTIONS (required, at least one)
connections:
  <connection_name>:          # Your choice of name
    type: local|azure_blob|delta|sql_server|http
    validation_mode: lazy|eager   # optional, defaults to 'lazy'
    <type-specific-config>

# ENVIRONMENTS (optional)
environments:
  <env_name>:
    <overrides>: ...
    # Or use external file: env.<env_name>.yaml

# STORY (required)
story:
  connection: string        # Name of connection to write stories
  path: string              # Relative path under that connection
  auto_generate: bool
  max_sample_rows: int
  retention_days: int (optional)
  retention_count: int (optional)

# PIPELINES (required, at least one)
pipelines:
  - pipeline: string          # Pipeline name
    layer: string (optional)
    description: string (optional)
    nodes:                    # At least one node
      - name: string          # Unique node name
        depends_on: [string]  # List of node names (optional)
        cache: bool (optional)

        # At least ONE of these:
        read:
          connection: string  # Connection name
          path: string        # Relative to connection base_path (Required unless 'query' used)
          table: string       # Table name (alternative to path)
          format: csv|parquet|json|excel|avro|sql_server
          options: dict       # Format-specific (optional)
            query: string     # SQL query (substitutes for path/table in sql_server)

        transform:
          steps:              # List of SQL strings or function calls
            - string (SQL)
            - function: string
              params: dict

        write:
          connection: string
          path: string
          table: string       # Table name (alternative to path)
          register_table: string # Register file output as external table (Spark/Delta only)
          format: csv|parquet|json|excel|avro|delta
          mode: overwrite|append
          options: dict (optional)
```

### Python API Quick Reference

```python
# === RECOMMENDED: Simple Usage ===
from odibi.pipeline import Pipeline

# Load and run all pipelines
manager = Pipeline.from_yaml("examples/templates/template_full.yaml")
results = manager.run()  # Dict[name -> PipelineResults] or single PipelineResults

# Run specific pipeline
result = manager.run('bronze_to_silver')

# List available pipelines
print(manager.list_pipelines())  # ['bronze_to_silver', 'silver_to_gold']

# === ADVANCED: Direct PipelineManager ===
from odibi.pipeline import PipelineManager

manager = PipelineManager.from_yaml("config.yaml")
results = manager.run()

# Access specific pipeline
pipeline = manager.get_pipeline('bronze_to_silver')
result = pipeline.run()

# === ADVANCED: Manual Construction ===
from odibi.pipeline import Pipeline
from odibi.config import PipelineConfig
from odibi.connections import LocalConnection

pipeline_config = PipelineConfig(
    pipeline="my_pipeline",
    nodes=[...]
)
connections = {
    "local": LocalConnection(base_path="./data")
}
pipeline = Pipeline(
    pipeline_config=pipeline_config,
    engine="pandas",
    connections=connections
)
result = pipeline.run()
```

### Common Patterns

**Pattern 1: Single pipeline in YAML**
```python
manager = Pipeline.from_yaml("simple.yaml")
result = manager.run()  # Returns PipelineResults (not dict)
print(f"Completed: {result.completed}")
```

**Pattern 2: Multiple pipelines, run all**
```python
manager = Pipeline.from_yaml("multi.yaml")
results = manager.run()  # Returns Dict[name -> PipelineResults]
for name, result in results.items():
    print(f"{name}: {len(result.completed)} nodes")
```

**Pattern 3: Multiple pipelines, run specific**
```python
manager = Pipeline.from_yaml("multi.yaml")
result = manager.run('bronze_to_silver')  # Returns PipelineResults
print(result.to_dict())
```

---

## Summary

**The Three Layers:**
1. **YAML (Layer 1):** What you write (declarative)
2. **Pydantic Models (Layer 2):** Validation (automatic)
3. **Runtime Classes (Layer 3):** Execution (automatic)

**The Flow:**
```
YAML file
  → yaml.safe_load()
  → Pydantic validation
  → PipelineManager/Pipeline creation
  → manager.run()
  → Node execution
  → Results
```

**Key Takeaways:**
- `Pipeline.from_yaml()` returns `PipelineManager` (not `Pipeline`)
- `manager.run()` runs all pipelines (or specific ones by name)
- Configs are **validated** before execution (fail fast)
- Context passes data between nodes (SQL references node names)
- Connections are defined **once**, referenced **many times**

---

**Questions? Confusion?**  
Open an issue on GitHub or check:
- [examples/templates/template_full.yaml](../examples/templates/template_full.yaml) - Complete YAML reference
- [docs/tutorials/walkthroughs/01_local_pipeline_pandas.ipynb](docs/tutorials/walkthroughs/01_local_pipeline_pandas.ipynb) - Interactive tutorial

---

*This document evolves with the framework. Last updated: 2025-11-20*
