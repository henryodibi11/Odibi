# Architecture Guide - Odibi System Design

**Visual guide to how Odibi works. See the big picture!**

---

## System Overview

```
┌─────────────────────────────────────────────────────────────┐
│                        USER                                  │
│                          │                                   │
│                          ▼                                   │
│                   config.yaml                                │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   CONFIG LAYER                               │
│                                                               │
│  config.yaml → Pydantic Models → ProjectConfig               │
│                     ↓                                         │
│              Validation happens here                          │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                  PIPELINE LAYER                              │
│                                                               │
│  Pipeline → DependencyGraph → Execution Order                │
│                     ↓                                         │
│              [Node A, Node B, Node C]                         │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   NODE LAYER                                 │
│                                                               │
│  Node → Read/Transform/Write → Engine                        │
│           ↓                      ↓                            │
│    Transformation           PandasEngine                      │
│      Registry               or SparkEngine                    │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   STORAGE LAYER                              │
│                                                               │
│  Connections → Local / Azure / SQL                           │
│                     ↓                                         │
│               Actual Data                                     │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   STORY LAYER                                │
│                                                               │
│  Metadata → Renderers → HTML/MD/JSON                         │
│      ↓                                                        │
│  Automatic audit trail                                        │
└─────────────────────────────────────────────────────────────┘
```

---

## Pipeline Execution Flow

### Step-by-Step: What Happens When You Run `odibi run config.yaml`

```
1. CLI Entry Point (cli/main.py)
   │
   ├─→ Parse arguments
   └─→ Call run_command(args)

2. Load Configuration (cli/run.py)
   │
   ├─→ Read YAML file
   ├─→ Parse to ProjectConfig (Pydantic validation)
   └─→ Create PipelineManager

3. Build Dependency Graph (graph.py)
   │
   ├─→ Extract all nodes
   ├─→ Build dependency edges
   ├─→ Check for cycles
   └─→ Topological sort → execution order

4. Execute Nodes (pipeline.py)
   │
   ├─→ For each node in order:
   │   │
   │   ├─→ Create Node instance (node.py)
   │   ├─→ Execute read/transform/write
   │   │   │
   │   │   ├─→ Read: Engine.read() → DataFrame
   │   │   ├─→ Transform: Registry.get(operation) → transformed DataFrame  
   │   │   └─→ Write: Engine.write(DataFrame)
   │   │
   │   ├─→ Store result in Context
   │   └─→ Track metadata (timing, rows, schema)
   │
   └─→ All nodes complete

5. Generate Story (story/generator.py)
   │
   ├─→ Collect all node metadata
   ├─→ Calculate aggregates (success rate, total rows)
   ├─→ Render to HTML/MD/JSON
   └─→ Save to stories/runs/

6. Return to User
   │
   └─→ "Pipeline completed successfully" ✅
```

---

## Module Dependencies

### Core Dependencies

```
config.py (no dependencies - pure Pydantic models)
    ↓
context.py (stores DataFrames)
    ↓
transformations/ (registry + decorators)
    ↓
operations/ (uses transformations)
    ↓
engine/ (executes operations)
    ↓
node.py (uses engine + context)
    ↓
graph.py (orders nodes)
    ↓
pipeline.py (orchestrates everything)
    ↓
story/ (documents execution)
    ↓
cli/ (user interface)
```

### Module Relationships

```
transformations/
    ├─→ Used by: operations/, node.py, story/doc_story.py
    └─→ Uses: Nothing (core module)

operations/
    ├─→ Used by: Pipelines (via registry lookup)
    └─→ Uses: transformations/

story/
    ├─→ Used by: pipeline.py, cli/story.py
    └─→ Uses: config.py, validation/, transformations/

connections/
    ├─→ Used by: engine/
    └─→ Uses: Nothing (independent connectors)

engine/
    ├─→ Used by: node.py
    └─→ Uses: connections/

cli/
    ├─→ Used by: Users!
    └─→ Uses: Everything
```

**Key insight:** `transformations/` is at the core. Everything builds on it.

---

## Data Flow

### How Data Moves Through a Pipeline

```
1. User YAML Config
   ↓
2. Parsed to ProjectConfig (in-memory objects)
   ↓
3. Pipeline.run() starts execution
   ↓
4. For each node:

   ┌─────────────────────────────────────────┐
   │ Node Execution                           │
   │                                          │
   │  1. Read Phase (if configured)           │
   │     └─→ Engine.read() → DataFrame        │
   │           └─→ Connection.get_path()      │
   │                 └─→ Actual file/DB       │
   │                                          │
   │  2. Transform Phase (if configured)      │
   │     ├─→ Get DataFrame from context       │
   │     ├─→ Registry.get(operation)          │
   │     └─→ func(df, **params) → DataFrame   │
   │                                          │
   │  3. Write Phase (if configured)          │
   │     └─→ Engine.write(DataFrame)          │
   │           └─→ Connection + format        │
   │                                          │
   │  4. Store Result                         │
   │     └─→ Context.set(node_name, df)       │
   │                                          │
   │  5. Track Metadata                       │
   │     └─→ NodeExecutionMetadata            │
   │           ├─→ Row counts                 │
   │           ├─→ Schema                     │
   │           └─→ Timing                     │
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
# When Python imports odibi/operations/unpivot.py:

@transformation("unpivot", category="reshaping")  # ← This runs immediately!
def unpivot(df, ...):
    ...

# What happens:
# 1. transformation("unpivot", ...) returns a decorator
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
┌───┴────┬──────────┬─────────────┐
│        │          │             │
Local   ADLS    AzureSQL      (more...)
│        │          │
↓        ↓          ↓
./data  Azure Blob  SQL Database
```

**All connections implement:**
- `get_path(relative_path)` - Resolve full path
- `validate()` - Check configuration

**Storage-specific methods:**
- ADLS: `pandas_storage_options()`, `configure_spark()`
- AzureSQL: `read_sql()`, `write_table()`, `get_engine()`
- Local: (just path manipulation)

### Engine Abstraction

```
Engine (interface)
    ↓
┌───┴────────────┐
│                │
PandasEngine  SparkEngine
│                │
↓                ↓
DataFrame    pyspark.DataFrame
```

**All engines implement:**
- `read(connection, path, format, options)`
- `write(df, connection, path, format, mode, options)`
- `execute_sql(df, query)`

**Why?** Swap Pandas ↔ Spark without changing config!

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

### Story Generation Pipeline

```
Execution → Metadata Collection → Rendering → Output
   ↓              ↓                   ↓          ↓
Nodes run    NodeExecution      Renderer    HTML file
             Metadata          (HTML/MD/JSON)
             tracked
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
@transformation("pivot")
def pivot(...): ...

# Look up by name:
func = registry.get("pivot")

# Easy! Scalable! Type-safe!
```

### Registry Singleton Pattern

**One registry for entire process:**

```python
# odibi/transformations/registry.py

# Create once at module level
_global_registry = TransformationRegistry()

def get_registry():
    return _global_registry  # Always same instance
```

**Benefits:**
- ✅ Single source of truth
- ✅ Operations registered once
- ✅ Available everywhere
- ✅ Easy to test (registry.clear() in tests)

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
Layer 4: Runtime (execution errors)
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

**1. Parallel Execution** (future)
```
Current: A → B → C → D (sequential)
Future:  A → B ┐
         A → C ┴→ D (parallel)
```

**2. Lazy Evaluation** (future)
```
Current: Execute all nodes
Future:  Only execute nodes needed for requested output
```

**3. Incremental Processing** (future)
```
Current: Reprocess all data every time
Future:  Only process new/changed data
```

---

## Testing Architecture

### Test Pyramid

```
        /\
       /E2E\          ← 10 tests (slow, comprehensive)
      /──────\
     /Integration\    ← 30 tests (medium speed)
    /────────────\
   /   Unit Tests  \  ← 380+ tests (fast, focused)
  /────────────────\
```

**Unit Tests** (380+)
- Test individual functions/classes
- Use mocks for external dependencies
- Run in <5 seconds
- Cover edge cases

**Integration Tests** (30)
- Test components working together
- Use real files (temp directories)
- Run in <10 seconds
- Cover common scenarios

**E2E Tests** (10)
- Test complete pipelines
- Real configs, real data
- Run in <30 seconds
- Cover critical paths

### Mocking Strategy

**Mock external dependencies, not internal ones:**

```python
# ✅ Good: Mock external SQLAlchemy
@patch('sqlalchemy.create_engine')
def test_azure_sql(mock_engine):
    conn = AzureSQL(...)
    # Test connection logic without real DB

# ❌ Bad: Mock internal functions
@patch('odibi.operations.pivot')
def test_something(mock_pivot):
    # Doesn't test real code!
```

---

## Design Patterns Used

### 1. Registry Pattern

**Where:** `transformations/registry.py`

**Purpose:** Centralized operation lookup

**Example:** All operations register themselves globally

### 2. Factory Pattern

**Where:** `story/renderers.py` (`get_renderer()`)

**Purpose:** Create renderers by format name

```python
renderer = get_renderer("html")  # Returns HTMLStoryRenderer
renderer = get_renderer("md")    # Returns MarkdownStoryRenderer
```

### 3. Decorator Pattern

**Where:** `transformations/decorators.py`, `explanation.py`

**Purpose:** Enhance functions with metadata

```python
@transformation("op")  # Adds registration
@op.explain           # Adds explanation
```

### 4. Strategy Pattern

**Where:** `engine/` (PandasEngine vs SparkEngine)

**Purpose:** Swap execution strategies

```python
# Same interface, different implementation:
engine = PandasEngine()  # or SparkEngine()
df = engine.read(...)    # Works with either!
```

### 5. Builder Pattern

**Where:** `story/doc_story.py`

**Purpose:** Construct complex documentation

```python
generator = DocStoryGenerator(config)
generator.generate(
    output_path="doc.html",
    format="html",
    theme=CORPORATE_THEME
)
```

### 6. Template Method Pattern

**Where:** `story/renderers.py`

**Purpose:** Define rendering algorithm skeleton

```python
class BaseRenderer:
    def render_to_file(self, metadata, path):
        content = self.render(metadata)  # Subclass implements
        self._save(content, path)        # Common logic
```

---

## Key Abstractions

### 1. Engine Abstraction

**Why?** Support multiple execution backends

```python
# User doesn't care if Pandas or Spark:
df = engine.read(connection, "data.parquet", "parquet")

# PandasEngine: uses pd.read_parquet()
# SparkEngine: uses spark.read.parquet()
# Same interface!
```

### 2. Connection Abstraction

**Why?** Support multiple storage systems

```python
# User writes: path: "data.csv"
# Connection resolves to:
# - Local: ./data/data.csv
# - ADLS: abfss://container@account.dfs.core.windows.net/data.csv
# - SQL: Table reference

# Same code, different storage!
```

### 3. Transformation Abstraction

**Why?** User-defined operations work same as built-in

```python
# Built-in:
@transformation("pivot")
def pivot(...): ...

# User-defined:
@transformation("my_custom_op")
def my_custom_op(...): ...

# Both registered the same way!
# Both available in YAML!
```

---

## Extensibility Points

### Where You Can Extend Odibi

**1. Add New Operations**
```
Location: odibi/operations/
Pattern: Use @transformation decorator
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

**4. Add New Renderers**
```
Location: odibi/story/renderers.py
Pattern: Implement .render() method
Impact: New story output formats
```

**5. Add New Themes**
```
Location: odibi/story/themes.py
Pattern: Create StoryTheme instance
Impact: Custom branding
```

**6. Add New Validators**
```
Location: odibi/validation/
Pattern: Create validator class
Impact: Quality enforcement
```

---

## Configuration Model

### Pydantic Model Hierarchy

```
ProjectConfig (root)
    ├── connections: Dict[str, ConnectionConfig]
    ├── story: StoryConfig
    └── pipelines: List[PipelineConfig]
            └── nodes: List[NodeConfig]
                    ├── read: ReadConfig (optional)
                    ├── transform: TransformConfig (optional)
                    └── write: WriteConfig (optional)
```

### Validation Flow

```
YAML file
    ↓
yaml.safe_load() → dict
    ↓
ProjectConfig(**dict)  ← Pydantic validation happens here!
    ↓
If valid: ProjectConfig instance
If invalid: ValidationError with helpful message
```

**Example error:**
```
ValidationError: 1 validation error for NodeConfig
name
  Field required [type=missing]
```

---

## Thread Safety

### Current State: Single-Threaded

**Registry:** Thread-safe (read-only after startup)
**Context:** NOT thread-safe (single pipeline execution)
**Pipeline:** NOT thread-safe (sequential execution)

### Future: Parallel Execution

**Possible:**
```python
# Execute independent nodes in parallel
Layer 0: [A]
Layer 1: [B, C]  ← Can run in parallel!
Layer 2: [D]
```

**Required changes:**
- Thread-safe Context
- Parallel node execution
- Coordinated metadata collection

---

## Memory Management

### DataFrame Lifecycle

```
1. Read → DataFrame created (stored in memory)
   ↓
2. Transform → New DataFrame (old one can be GC'd if not reused)
   ↓
3. Write → DataFrame written to disk
   ↓
4. Context stores DataFrame for downstream nodes
   ↓
5. Pipeline completes → Context cleared → memory freed
```

### Large Dataset Strategies

**Option 1: Don't store in context**
```python
# Future: Streaming mode
# Don't keep DataFrames in memory
# Process and write immediately
```

**Option 2: Use Spark**
```yaml
engine: spark
# Spark handles large data with partitioning
```

**Option 3: Chunk processing**
```python
# Write in chunks
chunksize: 10000  # Process 10K rows at a time
```

---

## Security Considerations

### Credential Handling

**✅ Good:**
```yaml
connections:
  azure:
    auth_mode: key_vault  # Credentials in Key Vault
    key_vault_name: myvault
    secret_name: storage-key
```

**❌ Bad:**
```yaml
connections:
  azure:
    account_key: "hardcoded_key_here"  # DON'T!
```

### SQL Injection Protection

**Odibi uses DuckDB** which executes on DataFrames (not databases):
- No SQL injection risk
- DataFrames are local
- Safe execution

For **Azure SQL**, use parameterized queries:

```python
# ✅ Safe
conn.read_sql(
    "SELECT * FROM users WHERE id = :user_id",
    params={"user_id": 123}
)

# ❌ Unsafe
conn.read_sql(f"SELECT * FROM users WHERE id = {user_id}")
```

---

## Next Steps

**You now understand the architecture!**

Learn how to build on it:
- **[Transformation Guide](05_TRANSFORMATION_GUIDE.md)** - Create custom operations
- **[Troubleshooting](06_TROUBLESHOOTING.md)** - Debug issues
- **Read the code!** Start with `operations/` directory

---

**Remember:** The tests are comprehensive examples. Use them! 🧪
