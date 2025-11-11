# Developer Guide - Understanding Odibi Internals

**Learn how Odibi works under the hood so you can extend and debug it.**

---

## Table of Contents

1. [Core Architecture](#core-architecture)
2. [Module Overview](#module-overview)
3. [Transformation System](#transformation-system)
4. [Story Generation](#story-generation)
5. [How to Learn the Codebase](#how-to-learn-the-codebase)
6. [Extension Points](#extension-points)

---

## Core Architecture

### The Big Picture

```
Config YAML → ProjectConfig → Pipeline → Nodes → Engine → Stories
                                  ↓
                            Dependencies → Execution Order
                                  ↓
                            Transformations Registry
```

**Flow:**
1. User writes YAML config
2. Odibi parses to `ProjectConfig` (Pydantic models)
3. Pipeline builds dependency graph
4. Nodes execute in topological order
5. Each node uses transformations from registry
6. Stories auto-generated during execution

---

## Module Overview

### Directory Structure

```
odibi/
├── config.py              # Pydantic models for YAML configs
├── pipeline.py            # Pipeline orchestrator
├── node.py                # Node executor
├── graph.py               # Dependency graph builder
├── context.py             # Data context (DataFrame storage)
├── engine/                # Execution engines
│   ├── base.py           # Engine interface
│   ├── pandas_engine.py  # Pandas implementation
│   └── spark_engine.py   # Spark implementation
├── connections/           # Storage connectors
│   ├── base.py
│   ├── local.py
│   ├── azure_adls.py
│   └── azure_sql.py
├── transformations/       # Transformation system
│   ├── registry.py       # Global registry
│   ├── decorators.py     # @transformation decorator
│   ├── explanation.py    # @func.explain decorator
│   ├── context.py        # Context passing
│   └── templates.py      # Explanation helpers
├── operations/            # Built-in operations
│   ├── pivot.py
│   ├── unpivot.py
│   ├── join.py
│   └── sql.py
├── story/                 # Story generation
│   ├── metadata.py       # Metadata tracking
│   ├── generator.py      # Legacy generator
│   ├── renderers.py      # HTML/MD/JSON renderers
│   ├── doc_story.py      # Doc story generator
│   ├── themes.py         # Theme system
│   └── templates/        # HTML templates
├── validation/            # Quality enforcement
│   └── explanation_linter.py
├── cli/                   # Command-line interface
│   ├── main.py
│   ├── run.py
│   ├── validate.py
│   └── story.py
└── utils/                 # Utilities
    └── setup_helpers.py
```

### Key Files to Understand

**Start with these 5 files:**

1. **`operations/unpivot.py`** (75 lines)
   - Shows how transformations work
   - Has explain() method
   - Very readable

2. **`transformations/registry.py`** (150 lines)
   - The heart of the system
   - How operations register
   - How lookups work

3. **`transformations/decorators.py`** (60 lines)
   - The `@transformation` decorator
   - How registration happens

4. **`story/metadata.py`** (175 lines)
   - What gets tracked during runs
   - How metrics are calculated

5. **`pipeline.py`** (300 lines)
   - How pipelines execute
   - Dependency resolution
   - Error handling

---

## Transformation System

### How It Works

**1. Registration (at import time)**

```python
# odibi/operations/unpivot.py

from odibi.transformations import transformation

@transformation("unpivot", category="reshaping")
def unpivot(df, id_vars, ...):
    """Unpivot operation."""
    return df.melt(...)

# When this file is imported:
# - @transformation decorator runs
# - Calls registry.register("unpivot", unpivot_func)
# - Function is now globally available
```

**2. Lookup (at runtime)**

```python
# odibi/node.py (simplified)

from odibi.transformations import get_registry

registry = get_registry()
func = registry.get("unpivot")  # Gets the function!
result = func(df, id_vars="ID")  # Calls it!
```

**3. Explanation (for stories)**

```python
# Getting explanation:
func = registry.get("unpivot")
explanation = func.get_explanation(
    id_vars="ID",
    plant="NKC"  # Context
)
# Returns formatted markdown explanation
```

### The Registry Pattern

**Why use a registry?**
- ✅ Centralized lookup
- ✅ Easy to list all operations
- ✅ Metadata storage
- ✅ Validation enforcement

**Global singleton:**
```python
# There's ONE registry for the entire process
_global_registry = TransformationRegistry()

def get_registry():
    return _global_registry
```

**All operations register themselves:**
```python
# When you import odibi.operations.pivot:
@transformation("pivot")  # ← Registers immediately
def pivot(...): ...
```

---

## Story Generation

### How Stories Work

**During Pipeline Execution:**

```python
# In pipeline.py (simplified)

story_metadata = PipelineStoryMetadata(pipeline_name="my_pipeline")

for node in execution_order:
    start = time.time()

    # Execute node
    result = node.execute()

    duration = time.time() - start

    # Track metadata
    node_meta = NodeExecutionMetadata(
        node_name=node.name,
        operation=node.operation,
        duration=duration,
        rows_out=len(result_df),
        status="success" if result.success else "failed"
    )

    # Calculate changes
    node_meta.calculate_row_change()
    node_meta.calculate_schema_changes()

    story_metadata.add_node(node_meta)

# At the end, render story
renderer = get_renderer("html")
renderer.render_to_file(story_metadata, "stories/runs/pipeline_123.html")
```

**No user intervention required!** Automatic.

### Renderers

Three output formats:

**HTMLStoryRenderer:**
```python
from odibi.story import HTMLStoryRenderer

renderer = HTMLStoryRenderer(theme=DARK_THEME)
html = renderer.render(metadata)
# Beautiful interactive HTML
```

**MarkdownStoryRenderer:**
```python
from odibi.story import MarkdownStoryRenderer

renderer = MarkdownStoryRenderer()
markdown = renderer.render(metadata)
# GitHub-flavored markdown
```

**JSONStoryRenderer:**
```python
from odibi.story import JSONStoryRenderer

renderer = JSONStoryRenderer()
json_str = renderer.render(metadata)
# Machine-readable JSON
```

---

## How to Learn the Codebase

### Strategy: Tests Are Your Teacher

**Step 1: Pick something you want to understand**

Example: "How does unpivot work?"

**Step 2: Find its test**

```bash
# Search for tests
grep -r "test.*unpivot" tests/
# Result: tests/unit/test_operations.py
```

**Step 3: Read the test**

```python
# tests/unit/test_operations.py

def test_unpivot_basic(self):
    """Should unpivot DataFrame from wide to long format."""
    df = pd.DataFrame({"ID": ["A"], "X": [10], "Y": [20]})

    unpivot_func = registry.get("unpivot")
    result = unpivot_func(df, id_vars="ID")

    assert len(result) == 2
    assert result["variable"].tolist() == ["X", "Y"]
```

**This tells you:**
- ✅ How to get the function (`registry.get()`)
- ✅ How to call it (`id_vars="ID"`)
- ✅ What it returns (long-format DataFrame)

**Step 4: Look at implementation**

```python
# odibi/operations/unpivot.py

@transformation("unpivot", category="reshaping")
def unpivot(df, id_vars, value_vars=None, var_name="variable", value_name="value"):
    if isinstance(id_vars, str):
        id_vars = [id_vars]

    return df.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name=var_name,
        value_name=value_name,
    )
```

**Aha!** It's just pandas `.melt()`! Simple.

### Learning Path

**Week 1: Operations (Easy)**
1. Read tests in `tests/unit/test_operations.py`
2. Read implementations in `odibi/operations/`
3. Try creating your own operation

**Week 2: Transformations (Core concept)**
1. Read `tests/unit/test_registry.py`
2. Read `odibi/transformations/registry.py`
3. Read `odibi/transformations/decorators.py`
4. Understand how `@transformation` works

**Week 3: Stories (Powerful feature)**
1. Read `tests/unit/test_story_metadata.py`
2. Read `odibi/story/metadata.py`
3. Read `tests/unit/test_story_renderers.py`
4. Read `odibi/story/renderers.py`

**Week 4: Pipeline Execution**
1. Read `tests/test_pipeline.py`
2. Read `odibi/pipeline.py`
3. Read `odibi/graph.py` (dependency resolution)
4. Trace a pipeline run end-to-end

---

## Extension Points

### 1. Create Custom Operations

```python
# my_operations.py

from odibi import transformation
from odibi.transformations.templates import purpose_detail_result

@transformation("deduplicate", category="cleaning")
def deduplicate(df, subset=None):
    """Remove duplicate rows."""
    return df.drop_duplicates(subset=subset)

@deduplicate.explain
def explain(subset=None, **context):
    plant = context.get('plant', 'Unknown')

    details = ["Removes duplicate rows"]
    if subset:
        details.append(f"Based on columns: {', '.join(subset)}")
    else:
        details.append("Based on all columns")

    return purpose_detail_result(
        purpose=f"Remove duplicate records for {plant}",
        details=details,
        result="Deduplicated dataset"
    )

# To use: Import before running pipeline
import my_operations
# Now "deduplicate" is available in your YAML!
```

### 2. Create Custom Themes

```yaml
# my_theme.yaml

name: company_theme
primary_color: "#8B0000"
success_color: "#006400"
font_family: "Calibri, Arial, sans-serif"
company_name: "My Company Inc."
logo_url: "https://example.com/logo.png"
footer_text: "© 2025 My Company"
```

**Use it:**
```bash
odibi story generate config.yaml --theme my_theme.yaml
```

### 3. Create Custom Renderers

```python
# my_renderer.py

from odibi.story.metadata import PipelineStoryMetadata

class PDFStoryRenderer:
    def render(self, metadata: PipelineStoryMetadata) -> bytes:
        # Your PDF generation logic
        pass
```

### 4. Add Custom Connections

```python
# my_connection.py

from odibi.connections.base import BaseConnection

class S3Connection(BaseConnection):
    def __init__(self, bucket, region):
        self.bucket = bucket
        self.region = region

    def get_path(self, relative_path):
        return f"s3://{self.bucket}/{relative_path}"

    def validate(self):
        if not self.bucket:
            raise ValueError("S3 requires bucket name")
```

---

## Common Patterns

### Pattern 1: Registry Lookup

**Everywhere in the codebase:**
```python
from odibi.transformations import get_registry

registry = get_registry()
func = registry.get("operation_name")
```

### Pattern 2: Pydantic Validation

**All configs use Pydantic:**
```python
from odibi.config import NodeConfig

# Pydantic validates automatically:
node = NodeConfig(
    name="test",
    read=ReadConfig(...)  # Validated!
)
```

### Pattern 3: Context Passing

**Context flows through everything:**
```python
# In transformation
@transformation("my_op")
def my_op(df, threshold, **context):
    plant = context.get('plant')  # From YAML!
    # Use plant in logic...

@my_op.explain
def explain(threshold, **context):
    plant = context.get('plant')  # Same context!
    return f"Filter for {plant}..."
```

### Pattern 4: Dependency Injection

**Connections injected into engines:**
```python
# pipeline.py
engine = PandasEngine()

for node in nodes:
    result = node.execute(
        engine=engine,
        connections=connections  # Injected!
    )
```

---

## Understanding Tests

### Test Structure

```
tests/
├── unit/                  # Test individual components
│   ├── test_operations.py      # Operation tests
│   ├── test_registry.py        # Registry tests
│   ├── test_story_metadata.py  # Metadata tests
│   └── ...
├── integration/           # Test components together
│   └── test_cli_integration.py
└── test_pipeline.py       # End-to-end pipeline tests
```

### Test Patterns

**Pattern 1: Setup with Fixtures**

```python
@pytest.fixture
def sample_data():
    return pd.DataFrame({"ID": [1, 2], "Value": [100, 200]})

def test_something(sample_data):
    # Use sample_data
    assert len(sample_data) == 2
```

**Pattern 2: Mock External Dependencies**

```python
@patch('sqlalchemy.create_engine')
def test_azure_sql(mock_create_engine):
    mock_engine = Mock()
    mock_create_engine.return_value = mock_engine

    # Now test without real database!
```

**Pattern 3: Test Setup/Teardown**

```python
class TestOperations:
    def setup_method(self):
        """Run before each test."""
        get_registry().clear()  # Clean state

    def test_something(self):
        # Test runs with clean registry
```

### How to Run Specific Tests

```bash
# Run all tests
pytest

# Run specific file
pytest tests/unit/test_operations.py

# Run specific test
pytest tests/unit/test_operations.py::TestPivotOperation::test_pivot_basic

# Run with verbose output
pytest -v

# Run with coverage
pytest --cov=odibi

# Stop on first failure
pytest -x
```

---

## Debugging Techniques

### Technique 1: Add Print Statements to Tests

```python
def test_pivot_basic(self):
    df = pd.DataFrame(...)
    result = pivot_func(df, ...)

    print("\n=== DEBUG ===")
    print(f"Input:\n{df}")
    print(f"Output:\n{result}")
    print(f"Columns: {result.columns.tolist()}")
    print("=============\n")

    assert ...
```

Run with `-s` to see prints:
```bash
pytest tests/unit/test_operations.py::test_pivot_basic -v -s
```

### Technique 2: Use Python Debugger

```python
def test_something(self):
    import pdb; pdb.set_trace()  # Breakpoint!

    result = func(...)
    # Debugger stops here
```

### Technique 3: Interactive Testing

```python
# Start Python REPL
python

>>> from odibi.transformations import get_registry
>>> import odibi.operations.unpivot  # Registers operation
>>>
>>> registry = get_registry()
>>> unpivot = registry.get("unpivot")
>>>
>>> import pandas as pd
>>> df = pd.DataFrame({"ID": ["A"], "X": [10], "Y": [20]})
>>> result = unpivot(df, id_vars="ID")
>>> print(result)
  ID variable  value
0  A        X     10
1  A        Y     20
```

### Technique 4: Trace Execution

Add logging:

```python
# In any file
import logging
logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)

def my_function():
    logger.debug("Entering my_function")
    logger.debug(f"Parameters: {locals()}")
    # ...
```

---

## How Each System Works

### The Transformation Registry

**Location:** `odibi/transformations/registry.py`

**Core concept:** Global dictionary

```python
class TransformationRegistry:
    def __init__(self):
        self._transformations = {}  # name -> function
        self._metadata = {}         # name -> metadata

    def register(self, name, func, **metadata):
        self._transformations[name] = func
        self._metadata[name] = metadata

    def get(self, name):
        return self._transformations.get(name)
```

**That's it!** Just a dict with some validation.

### The @transformation Decorator

**Location:** `odibi/transformations/decorators.py`

```python
def transformation(name, version="1.0.0", category=None, tags=None):
    def decorator(func):
        registry = get_registry()
        registry.register(
            name=name,
            func=func,
            version=version,
            category=category,
            tags=tags
        )
        return func  # Return original function

    return decorator
```

**Usage:**
```python
@transformation("my_op")  # decorator(func) is called
def my_op(df):
    return df

# Equivalent to:
# my_op = transformation("my_op")(my_op)
# Which calls: registry.register("my_op", my_op)
```

### The @func.explain Decorator

**Location:** `odibi/transformations/explanation.py`

**How it works:**

```python
class ExplainableFunction:
    def __init__(self, func):
        self.func = func
        self.explain_func = None

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)  # Normal call

    def explain(self, explain_func):
        """Decorator for adding explain method."""
        self.explain_func = explain_func
        return self

    def get_explanation(self, **kwargs):
        if self.explain_func:
            return self.explain_func(**kwargs)
        return "No explanation available"
```

**Usage:**
```python
@transformation("my_op")
def my_op(df):
    return df

@my_op.explain  # ← Attaches to my_op
def explain(**context):
    return "Explanation here"

# Now you can call:
my_op.get_explanation(plant="NKC")
```

### The Dependency Graph

**Location:** `odibi/graph.py`

**Algorithm:** Topological sort

```python
class DependencyGraph:
    def __init__(self, nodes):
        self.nodes = {node.name: node for node in nodes}
        self._build_graph()

    def _build_graph(self):
        # For each node, track what depends on it
        for node in self.nodes.values():
            for dep in node.depends_on:
                # Record dependency

    def get_execution_order(self):
        # Topological sort
        # Returns: [node1, node2, node3, ...]
        # Guarantees: dependencies execute first
```

**Why it matters:**
```yaml
nodes:
  - name: C
    depends_on: [A, B]  # C needs A and B
  - name: B
    depends_on: [A]      # B needs A  
  - name: A              # A first

# Execution order: A → B → C
```

---

## Key Design Decisions

### 1. Why Global Registry?

**Alternative:** Pass registry everywhere

```python
# BAD: Pass registry to everything
def execute(registry, node, ...):
    func = registry.get(node.operation)
```

**Good:** Global singleton

```python
# GOOD: Just import and use
from odibi.transformations import get_registry
func = get_registry().get("operation")
```

### 2. Why Pydantic for Config?

**Benefits:**
- ✅ Automatic validation
- ✅ Type checking
- ✅ Great error messages
- ✅ JSON schema generation

```python
class NodeConfig(BaseModel):
    name: str  # ← Must be string, validated automatically!
    description: Optional[str]  # ← Optional
```

### 3. Why Separate Metadata from Generation?

**Metadata** (data):
```python
metadata = PipelineStoryMetadata(...)
metadata.add_node(...)
```

**Renderers** (presentation):
```python
HTMLStoryRenderer().render(metadata)
MarkdownStoryRenderer().render(metadata)
```

**Why?** One metadata source, multiple output formats!

### 4. Why Context Passing with **kwargs?

**Flexible:**
```python
@my_op.explain
def explain(threshold, **context):
    plant = context.get('plant')  # Use if available
    asset = context.get('asset')  # Ignore if missing
```

Forward-compatible - add new context fields without breaking old code!

---

## Code Reading Tips

### Tip 1: Start Small

**Don't read `pipeline.py` first** (300 lines, complex)

**Do read `operations/unpivot.py` first** (75 lines, simple)

### Tip 2: Follow the Imports

```python
from odibi.transformations import transformation

# Where is this?
# 1. Check odibi/transformations/__init__.py
# 2. See: from .decorators import transformation
# 3. Go to: odibi/transformations/decorators.py
# 4. Read the function!
```

### Tip 3: Use IDE "Go to Definition"

- Click `transformation` → jumps to definition
- Click `get_registry()` → jumps to implementation
- Trace the entire call stack!

### Tip 4: Run Tests While Reading

```bash
# Terminal 1: Read the code
vim odibi/operations/pivot.py

# Terminal 2: Run its tests
pytest tests/unit/test_operations.py::TestPivotOperation -v

# See it in action!
```

---

## Common Questions

### Q: Where do operations get registered?

**A:** During import.

```python
import odibi.operations.pivot  # ← @transformation runs now!
registry = get_registry()
func = registry.get("pivot")   # ← It's there!
```

### Q: How does the pipeline know which nodes to run first?

**A:** `DependencyGraph` in `odibi/graph.py`

It builds a directed graph and does topological sort.

### Q: Where are DataFrames stored between nodes?

**A:** `Context` object in `odibi/context.py`

```python
# After node1 executes:
context.set("node1", df_result)

# When node2 runs:
df_input = context.get("node1")
```

### Q: How do explain() methods get context?

**A:** Built in `pipeline.py` and passed via `**kwargs`

```python
context = {
    'plant': config.plant,
    'asset': config.asset,
    # ... more fields ...
}

explanation = func.get_explanation(**node.params, **context)
```

### Q: What happens when a test fails?

**A:** Read the error message carefully:

```
AssertionError: assert 'expected' in 'actual result here'
```

The actual result is shown! Use it to understand what's happening.

---

## Development Workflow

### Making Changes

```bash
# 1. Create a branch
git checkout -b feature/my-feature

# 2. Make changes
edit odibi/operations/my_new_operation.py

# 3. Write tests FIRST (TDD)
edit tests/unit/test_my_operation.py

# 4. Run tests
pytest tests/unit/test_my_operation.py -v

# 5. Run ALL tests
pytest

# 6. Format code
black odibi/

# 7. Commit
git add .
git commit -m "Add my_new_operation"
```

### Test-Driven Development

**Write test first:**
```python
# tests/unit/test_my_operation.py

def test_my_operation():
    df = pd.DataFrame({"value": [1, 2, 3]})
    result = my_operation(df, threshold=2)
    assert len(result) == 1  # Only value=3
```

**Then implement:**
```python
# odibi/operations/my_operation.py

@transformation("my_operation")
def my_operation(df, threshold):
    """Filter values above threshold."""
    return df[df.value > threshold]
```

**Test passes!** ✅

---

## Next Steps

You now understand how Odibi works internally!

**Continue learning:**
- **[Architecture Guide](04_ARCHITECTURE_GUIDE.md)** - Visual diagrams of the system
- **[Transformation Guide](05_TRANSFORMATION_GUIDE.md)** - Write your own operations
- **[Troubleshooting](06_TROUBLESHOOTING.md)** - Debug common issues

**Want to contribute?**
- Read `CONTRIBUTING.md`
- Check open issues
- All tests must pass before PR!

---

**You've got this!** The tests are your teacher. 🎓
