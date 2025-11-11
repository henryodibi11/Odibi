# Odibi Phase 3 Internals: A Guided Walkthrough

**Goal:** Master Odibi's internals (Phases 1-3) through hands-on building  
**Duration:** 2-3 hours  
**Approach:** Learn by doing - build, break, understand, repeat

---

## 🎯 Learning Strategy

**The Oracle's Wisdom:**
> "Teach by building one tiny pipeline end-to-end on Pandas, then spiral inward: transformation → registry/decorator → node/graph → explanations → stories/CLI → Azure SQL."

**What You'll Build:**
- A custom transformation from scratch
- A multi-node pipeline with dependencies
- Self-documenting stories
- An Azure SQL connector pipeline

**Why This Works:**
- ✅ Short read-then-do loops (10-30 min each)
- ✅ Hands-on with immediate feedback
- ✅ Focus on 8-10 core files (not the whole codebase)
- ✅ Checkpoints to verify understanding

---

## Table of Contents

- [Stage 0: Orientation & First Run](#stage-0-orientation--first-run-10-15-min) (10-15 min)
- [Stage 1: Transformations 101](#stage-1-transformations-101-15-20-min) (15-20 min)
- [Stage 2: Execution Path](#stage-2-execution-path-node--graph--pipeline-20-25-min) (20-25 min)
- [Stage 3: Explanation System](#stage-3-explanation-system-15-20-min) (15-20 min)
- [Stage 4: Story Generation](#stage-4-story-generation-25-30-min) (25-30 min)
- [Stage 5: SQL Transformation](#stage-5-sql-transformation--engine-boundary-15-20-min) (15-20 min)
- [Stage 6: Azure SQL Connector](#stage-6-azure-sql-connector-20-25-min) (20-25 min)
- [Stage 7: Quality Enforcement](#stage-7-quality--guardrails-10-15-min) (10-15 min)

---

## Stage 0: Orientation & First Run (10-15 min)

### 🎯 Goal
Get a working mental model and one successful local run.

### 📖 Read (5 min)

**The Big Picture Flow:**
```
YAML Config → ProjectConfig → Pipeline → Nodes → Engine → Stories
                                   ↓
                             Dependencies → Execution Order
                                   ↓
                             Transformations Registry
```

**Files to Peek:**
1. `odibi/config.py` - Look at `ProjectConfig` class (lines 1-50)
2. `examples/example_local.yaml` - See a real config

### 🛠️ Do (10 min)

**Run your first pipeline:**

```python
# Create: scratch/stage0_first_run.py

from odibi.config import ProjectConfig
import pandas as pd

# Load example config
config = ProjectConfig.from_yaml("examples/example_local.yaml")

# Print structure
print("Pipeline name:", config.pipelines[0].name)
print("Number of nodes:", len(config.pipelines[0].nodes))
print("Connections:", list(config.connections.keys()))

# Run the pipeline
from odibi.pipeline import Pipeline

pipeline = Pipeline(config.pipelines[0], config)
result = pipeline.run()

print(f"\n✅ Success! Pipeline completed in {result.duration:.2f}s")
print(f"📊 Nodes executed: {len(result.node_results)}")
```

Run it:
```bash
cd /d:/odibi
python scratch/stage0_first_run.py
```

**Observe:**
- The pipeline executes in order
- Check `stories/runs/` for auto-generated story
- Check `data/` for output files

### ✅ Checkpoint

Can you answer these?
- [ ] What's the flow from YAML to execution?
- [ ] Where do transformations come from? (Registry!)
- [ ] Where are DataFrames stored between nodes? (Context!)
- [ ] What gets auto-generated? (Stories!)

**If yes, move to Stage 1. If no, re-run and trace the output.**

---

## Stage 1: Transformations 101 (15-20 min)

### 🎯 Goal
Internalize how a transformation is implemented and registered.

### 📖 Read (8 min)

**Read these 3 files in order:**

1. **`odibi/operations/unpivot.py`** (75 lines) - The simplest transformation
   - See the `@transformation` decorator
   - See the function implementation
   - See the `@unpivot.explain` decorator

2. **`odibi/transformations/decorators.py`** (60 lines) - The magic decorator
   - How `@transformation` works
   - When registration happens (import time!)

3. **`odibi/transformations/registry.py`** (150 lines) - The global registry
   - How functions are stored
   - How lookups work
   - The singleton pattern

**Key Insight:**
```python
# When this runs:
import odibi.operations.unpivot

# This happens:
# 1. @transformation("unpivot") decorator runs
# 2. Calls registry.register("unpivot", unpivot_func)
# 3. Function is now globally available!
```

### 🛠️ Do (12 min)

**Build your first transformation:**

```python
# Create: scratch/stage1_my_transformation.py

from odibi.transformations import transformation, get_registry
from odibi.transformations.templates import purpose_detail_result
import pandas as pd

# 1. DEFINE YOUR TRANSFORMATION
@transformation("filter_threshold", category="filtering", version="1.0.0")
def filter_threshold(df, column, threshold, operation="greater_than"):
    """Filter rows based on a threshold value."""
    if operation == "greater_than":
        return df[df[column] > threshold]
    elif operation == "less_than":
        return df[df[column] < threshold]
    elif operation == "equals":
        return df[df[column] == threshold]
    else:
        raise ValueError(f"Unknown operation: {operation}")

# 2. ADD EXPLANATION
@filter_threshold.explain
def explain(column, threshold, operation="greater_than", **context):
    """Explain what this transformation does."""
    plant = context.get('plant', 'Unknown Plant')

    op_text = {
        "greater_than": f"greater than {threshold}",
        "less_than": f"less than {threshold}",
        "equals": f"equal to {threshold}"
    }

    return purpose_detail_result(
        purpose=f"Filter {plant} data based on {column}",
        details=[
            f"Keep only rows where **{column}** is {op_text.get(operation, operation)}",
            f"Threshold value: `{threshold}`",
            "Rows not meeting criteria are removed"
        ],
        result=f"Filtered dataset with {column} {op_text.get(operation)}"
    )

# 3. TEST IT
if __name__ == "__main__":
    # Get from registry
    registry = get_registry()
    func = registry.get("filter_threshold")

    print("✅ Found in registry:", func is not None)

    # Create test data
    df = pd.DataFrame({
        "plant": ["NKC", "NKC", "BWA", "BWA"],
        "value": [100, 50, 200, 25],
        "status": ["OK", "OK", "ALERT", "OK"]
    })

    print("\n📊 Original data:")
    print(df)

    # Apply transformation
    result = func(df, column="value", threshold=75, operation="greater_than")

    print("\n📊 Filtered data (value > 75):")
    print(result)

    # Get explanation
    explanation = func.get_explanation(
        column="value",
        threshold=75,
        operation="greater_than",
        plant="NKC Power Station"
    )

    print("\n📝 Explanation:")
    print(explanation)
```

Run it:
```bash
python scratch/stage1_my_transformation.py
```

**Experiment:**
- Change threshold to 150
- Try operation="less_than"
- Add a new operation type
- Change the explanation text

### ✅ Checkpoint

Can you answer these?
- [ ] When does registration happen? (Import time!)
- [ ] How do you get a function from the registry?
- [ ] What's the purpose of `@func.explain`?
- [ ] What does `**context` do?

**Try this:** Without looking, explain the flow from `@transformation` to `registry.get()`.

---

## Stage 2: Execution Path: Node → Graph → Pipeline (20-25 min)

### 🎯 Goal
Understand how config becomes ordered execution.

### 📖 Read (10 min)

**Read these files:**

1. **`odibi/context.py`** (100 lines) - Where DataFrames live
   - See `set()` and `get()` methods
   - This is just a dict wrapper with validation

2. **`odibi/node.py`** (200 lines) - How nodes execute
   - Focus on `execute()` method (lines 100-200)
   - See how it gets input from context
   - See how it calls registry.get()
   - See how it stores output in context

3. **`odibi/graph.py`** (150 lines) - Dependency resolution
   - See `_build_graph()` method
   - See topological sort in `get_execution_order()`

4. **`odibi/pipeline.py`** (300 lines) - SKIM ONLY
   - Just read the `run()` method loop (lines 150-250)
   - Don't deep dive - we'll come back later

**Key Insight:**
```python
# Pipeline execution flow:
# 1. Build dependency graph from nodes
# 2. Get topological order (A → B → C)
# 3. For each node:
#    - Get input from context
#    - Get transformation from registry
#    - Execute transformation
#    - Store result in context
#    - Track metadata for story
```

### 🛠️ Do (15 min)

**Build a multi-node pipeline:**

```yaml
# Create: scratch/stage2_dependency_pipeline.yaml

story:
  connection: local_data
  format: markdown

connections:
  local_data:
    type: local
    base_path: ./scratch/stage2_output

pipelines:
  - name: dependency_demo
    plant: "Learning Lab"
    asset: "Tutorial"

    nodes:
      # Node 1: Load data
      - name: load_raw
        read:
          connection: local_data
          path: input/raw_data.csv
          format: csv

      # Node 2: Filter (depends on load_raw)
      - name: filter_high_values
        depends_on: [load_raw]
        transform:
          operation: filter_threshold  # YOUR custom operation!
          params:
            column: value
            threshold: 100
            operation: greater_than

      # Node 3: Another filter (depends on load_raw - parallel with Node 2!)
      - name: filter_critical
        depends_on: [load_raw]
        transform:
          operation: filter_threshold
          params:
            column: status
            threshold: "CRITICAL"
            operation: equals

      # Node 4: Combine (depends on both filters)
      - name: combine_results
        depends_on: [filter_high_values, filter_critical]
        transform:
          operation: sql
          params:
            query: |
              SELECT DISTINCT * FROM (
                SELECT * FROM filter_high_values
                UNION ALL
                SELECT * FROM filter_critical
              )
        write:
          connection: local_data
          path: output/final_results.csv
          format: csv
```

**Setup and run:**

```python
# Create: scratch/stage2_run_pipeline.py

import pandas as pd
import os
from odibi.config import ProjectConfig
from odibi.pipeline import Pipeline

# Import your custom transformation
import sys
sys.path.insert(0, '/d:/odibi/scratch')
from stage1_my_transformation import filter_threshold  # Registers automatically!

# Create input data
os.makedirs('scratch/stage2_output/input', exist_ok=True)

df = pd.DataFrame({
    "plant": ["NKC"]*5 + ["BWA"]*5,
    "value": [50, 150, 200, 75, 300, 25, 400, 10, 125, 80],
    "status": ["OK", "WARNING", "CRITICAL", "OK", "CRITICAL",
               "OK", "CRITICAL", "OK", "WARNING", "OK"]
})
df.to_csv('scratch/stage2_output/input/raw_data.csv', index=False)

print("📊 Created input data:")
print(df)

# Load and run pipeline
config = ProjectConfig.from_yaml("scratch/stage2_dependency_pipeline.yaml")
pipeline = Pipeline(config.pipelines[0], config)

# ANALYZE EXECUTION ORDER
from odibi.graph import DependencyGraph
graph = DependencyGraph(config.pipelines[0].nodes)
order = graph.get_execution_order()

print("\n🔍 Execution order:")
for i, node in enumerate(order, 1):
    deps = node.depends_on or []
    print(f"  {i}. {node.name} (depends on: {deps or 'nothing'})")

# RUN IT
result = pipeline.run()

print(f"\n✅ Pipeline completed in {result.duration:.2f}s")

# Check output
output = pd.read_csv('scratch/stage2_output/output/final_results.csv')
print(f"\n📊 Final output ({len(output)} rows):")
print(output)
```

Run it:
```bash
python scratch/stage2_run_pipeline.py
```

**Observe:**
- Execution order: load_raw → (filter_high_values + filter_critical in parallel) → combine_results
- Context stores intermediate DataFrames
- Your custom transformation works in a real pipeline!

### ✅ Checkpoint

Can you answer these?
- [ ] How does the graph determine execution order?
- [ ] Where are DataFrames stored between nodes?
- [ ] Can nodes run in parallel if they don't depend on each other?
- [ ] How does a node get its input DataFrame?

**Challenge:** Add a 5th node that depends on combine_results. Predict the new execution order.

---

## Stage 3: Explanation System (15-20 min)

### 🎯 Goal
Attach human explanations to transformations and pass runtime context safely.

### 📖 Read (7 min)

**Read these files:**

1. **`odibi/transformations/explanation.py`** (120 lines)
   - See `ExplainableFunction` class
   - See how `.explain` decorator works
   - See `get_explanation()` method

2. **`odibi/transformations/templates.py`** (80 lines)
   - See helper functions: `purpose_detail_result`, `bold`, `code`
   - These make explanations consistent

3. **`odibi/transformations/context.py`** (50 lines)
   - See how context is built and passed

**Key Insight:**
```python
# The explain pattern:
@transformation("my_op")
def my_op(df, threshold, **context):
    plant = context.get('plant')  # Runtime context from YAML
    return df[df.value > threshold]

@my_op.explain
def explain(threshold, **context):
    plant = context.get('plant')  # Same context!
    return f"Filter {plant} data where value > {threshold}"
```

### 🛠️ Do (13 min)

**Enhance your transformation with rich explanations:**

```python
# Create: scratch/stage3_rich_explanation.py

from odibi.transformations import transformation
from odibi.transformations.templates import (
    purpose_detail_result,
    bold,
    code,
    section
)
import pandas as pd

@transformation("smart_filter", category="advanced_filtering", version="1.0.0")
def smart_filter(df, column, threshold, include_equals=False, **context):
    """Smart filtering with context awareness."""
    if include_equals:
        return df[df[column] >= threshold]
    else:
        return df[df[column] > threshold]

@smart_filter.explain
def explain(column, threshold, include_equals=False, **context):
    """Rich explanation with context."""

    # Extract context
    plant = context.get('plant', 'Unknown Plant')
    asset = context.get('asset', 'Unknown Asset')
    pipeline_name = context.get('pipeline_name', 'Pipeline')

    # Build comparison text
    comparison = f"≥ {threshold}" if include_equals else f"> {threshold}"

    # Build explanation sections
    purpose = f"Filter {plant} {asset} data based on {bold(column)} threshold"

    details = [
        f"Target column: {code(column)}",
        f"Threshold: {code(threshold)}",
        f"Condition: Keep rows where {code(column)} {comparison}",
        f"Include boundary: {code(include_equals)}",
    ]

    # Add context-aware details
    if threshold > 1000:
        details.append("⚠️ High threshold - expect significant data reduction")

    if include_equals:
        details.append("✅ Including boundary values (threshold included)")
    else:
        details.append("🚫 Excluding boundary values (threshold excluded)")

    # Calculate expected impact (if input data available in context)
    result_text = f"Filtered dataset for {pipeline_name}"
    if 'input_rows' in context:
        result_text += f" (from {context['input_rows']} input rows)"

    # Use template
    return purpose_detail_result(
        purpose=purpose,
        details=details,
        result=result_text
    )

# TEST IT
if __name__ == "__main__":
    from odibi.transformations import get_registry

    func = get_registry().get("smart_filter")

    # Test with minimal context
    print("📝 Explanation (minimal context):")
    print(func.get_explanation(
        column="temperature",
        threshold=100,
        include_equals=True
    ))

    print("\n" + "="*60 + "\n")

    # Test with rich context
    print("📝 Explanation (rich context):")
    print(func.get_explanation(
        column="temperature",
        threshold=2500,
        include_equals=False,
        plant="Northridge Power Station",
        asset="Turbine 3",
        pipeline_name="Safety Monitoring",
        input_rows=15000
    ))

    # Test execution
    df = pd.DataFrame({
        "temperature": [500, 1500, 2500, 3500, 4500],
        "pressure": [10, 20, 30, 40, 50]
    })

    print("\n📊 Original data:")
    print(df)

    result = func(df, column="temperature", threshold=2500, include_equals=True)
    print(f"\n📊 Filtered data (>= 2500): {len(result)} rows")
    print(result)
```

Run it:
```bash
python scratch/stage3_rich_explanation.py
```

**Experiment:**
- Add more template helpers (section, italic, etc.)
- Add conditional explanations based on context
- Try `get_explanation()` with different context values

### ✅ Checkpoint

Can you answer these?
- [ ] Why use `**context` instead of explicit parameters?
- [ ] What's the benefit of template helpers?
- [ ] Where does context come from at runtime?
- [ ] How do explanations become part of stories?

**Test:** Can you add a context-aware warning to your explanation?

---

## Stage 4: Story Generation (25-30 min)

### 🎯 Goal
See how runs self-document and produce HTML/MD/JSON.

### 📖 Read (12 min)

**Read these files:**

1. **`odibi/story/metadata.py`** (175 lines)
   - See `PipelineStoryMetadata` class
   - See `NodeExecutionMetadata` class
   - See what gets tracked: duration, rows, schema, status

2. **`odibi/story/renderers.py`** (200 lines)
   - See `HTMLStoryRenderer`, `MarkdownStoryRenderer`, `JSONStoryRenderer`
   - See how they convert metadata to output

3. **`odibi/cli/story.py`** (150 lines) - SKIM
   - See CLI commands: generate, list, diff

**Key Insight:**
```python
# During pipeline execution:
# 1. Track start time
# 2. Execute node
# 3. Track duration, rows, schema
# 4. Store in NodeExecutionMetadata
# 5. Add to PipelineStoryMetadata
# 6. After all nodes, render to HTML/MD/JSON
```

### 🛠️ Do (18 min)

**Generate and explore stories:**

```python
# Create: scratch/stage4_story_generation.py

import pandas as pd
import os
from odibi.config import ProjectConfig
from odibi.pipeline import Pipeline
from odibi.story import HTMLStoryRenderer, MarkdownStoryRenderer, JSONStoryRenderer
from odibi.story.themes import DARK_THEME, LIGHT_THEME, CORPORATE_THEME

# Import custom transformation
import sys
sys.path.insert(0, '/d:/odibi/scratch')
from stage3_rich_explanation import smart_filter

# Create test data
os.makedirs('scratch/stage4_output/input', exist_ok=True)
os.makedirs('scratch/stage4_output/stories', exist_ok=True)

df = pd.DataFrame({
    "timestamp": pd.date_range("2025-01-01", periods=1000, freq="h"),
    "temperature": [1000 + i*2 + (i % 100)*10 for i in range(1000)],
    "pressure": [50 + i*0.5 for i in range(1000)],
    "status": ["OK"]*800 + ["WARNING"]*150 + ["CRITICAL"]*50
})
df.to_csv('scratch/stage4_output/input/sensor_data.csv', index=False)

# Create pipeline YAML
yaml_content = """
story:
  connection: local_data
  format: html
  theme: dark

connections:
  local_data:
    type: local
    base_path: ./scratch/stage4_output

pipelines:
  - name: story_demo_pipeline
    plant: "Demo Power Station"
    asset: "Sensor Array A1"
    description: "Demonstrate story generation with rich metadata"

    nodes:
      - name: load_sensors
        description: "Load raw sensor readings from CSV"
        read:
          connection: local_data
          path: input/sensor_data.csv
          format: csv

      - name: filter_high_temp
        description: "Identify high temperature events"
        depends_on: [load_sensors]
        transform:
          operation: smart_filter
          params:
            column: temperature
            threshold: 2000
            include_equals: false

      - name: filter_critical
        description: "Isolate critical status events"
        depends_on: [load_sensors]
        transform:
          operation: smart_filter
          params:
            column: status
            threshold: "CRITICAL"
            include_equals: true
        transform_type: python

      - name: pivot_summary
        description: "Pivot data by status"
        depends_on: [load_sensors]
        transform:
          operation: pivot
          params:
            index: status
            columns: []
            values: temperature
            aggfunc: mean

      - name: unpivot_for_export
        description: "Reshape to long format for export"
        depends_on: [pivot_summary]
        transform:
          operation: unpivot
          params:
            id_vars: status
        write:
          connection: local_data
          path: output/summary_long.csv
          format: csv
"""

with open('scratch/stage4_pipeline.yaml', 'w') as f:
    f.write(yaml_content)

# Run pipeline
print("🚀 Running pipeline with story generation...")
config = ProjectConfig.from_yaml("scratch/stage4_pipeline.yaml")
pipeline = Pipeline(config.pipelines[0], config)
result = pipeline.run()

print(f"✅ Pipeline completed in {result.duration:.2f}s")
print(f"📊 Nodes executed: {len(result.node_results)}")

# The story is auto-generated! But let's also generate custom formats

# Get the story metadata
metadata = result.metadata

print("\n📝 Story Metadata Summary:")
print(f"  Pipeline: {metadata.pipeline_name}")
print(f"  Duration: {metadata.duration:.2f}s")
print(f"  Nodes: {len(metadata.nodes)}")
print(f"  Status: {metadata.status}")

for node_meta in metadata.nodes:
    print(f"\n  Node: {node_meta.node_name}")
    print(f"    Duration: {node_meta.duration:.2f}s")
    print(f"    Rows: {node_meta.rows_in} → {node_meta.rows_out} ({node_meta.row_change:+d})")
    print(f"    Cols: {node_meta.cols_in} → {node_meta.cols_out}")

# Generate multiple formats
print("\n📄 Generating stories in different formats...")

# HTML with DARK theme
html_renderer = HTMLStoryRenderer(theme=DARK_THEME)
html_output = html_renderer.render(metadata)
with open('scratch/stage4_output/stories/story_dark.html', 'w', encoding='utf-8') as f:
    f.write(html_output)
print("  ✅ HTML (Dark theme): scratch/stage4_output/stories/story_dark.html")

# HTML with LIGHT theme
html_renderer_light = HTMLStoryRenderer(theme=LIGHT_THEME)
html_output_light = html_renderer_light.render(metadata)
with open('scratch/stage4_output/stories/story_light.html', 'w', encoding='utf-8') as f:
    f.write(html_output_light)
print("  ✅ HTML (Light theme): scratch/stage4_output/stories/story_light.html")

# Markdown
md_renderer = MarkdownStoryRenderer()
md_output = md_renderer.render(metadata)
with open('scratch/stage4_output/stories/story.md', 'w', encoding='utf-8') as f:
    f.write(md_output)
print("  ✅ Markdown: scratch/stage4_output/stories/story.md")

# JSON
json_renderer = JSONStoryRenderer()
json_output = json_renderer.render(metadata)
with open('scratch/stage4_output/stories/story.json', 'w', encoding='utf-8') as f:
    f.write(json_output)
print("  ✅ JSON: scratch/stage4_output/stories/story.json")

print("\n🎨 Open the HTML files in your browser to see the stories!")
print("📖 Check the Markdown for a text-based view")
print("🔧 Inspect the JSON for programmatic access")
```

Run it:
```bash
python scratch/stage4_story_generation.py
```

**Explore:**
- Open `scratch/stage4_output/stories/story_dark.html` in browser
- Open `scratch/stage4_output/stories/story_light.html` in browser  
- Read `scratch/stage4_output/stories/story.md`
- Parse `scratch/stage4_output/stories/story.json`

**Try CLI commands (if available):**
```bash
# List all stories
odibi story list scratch/stage4_output/

# Generate story from config
odibi story generate scratch/stage4_pipeline.yaml

# Make a change to the YAML (e.g., change threshold to 2500)
# Then run again and diff
odibi story diff scratch/stage4_output/stories/run_1.json scratch/stage4_output/stories/run_2.json
```

### ✅ Checkpoint

Can you answer these?
- [ ] What metadata is captured during execution?
- [ ] How are durations measured?
- [ ] What's the difference between renderers?
- [ ] When are stories generated?

**Explore:** Can you find where in `pipeline.py` the story metadata is built?

---

## Stage 5: SQL Transformation & Engine Boundary (15-20 min)

### 🎯 Goal
Cement the "SQL-over-Pandas" idea and engine-agnostic boundary.

### 📖 Read (8 min)

**Read these files:**

1. **`odibi/engine/base.py`** (100 lines)
   - See the `Engine` interface
   - See abstract methods: `read()`, `write()`, `execute_sql()`

2. **`odibi/engine/pandas_engine.py`** (250 lines)
   - See how Pandas implements the Engine interface
   - Focus on `execute_sql()` - it uses DuckDB!

3. **`odibi/operations/sql.py`** (60 lines)
   - See how SQL operation calls `engine.execute_sql()`
   - Note: It doesn't care which engine!

**Key Insight:**
```python
# Engine-agnostic design:
#
# Operation doesn't know about Pandas or Spark:
def sql_operation(df, query, engine):
    return engine.execute_sql(query, {"input": df})
#
# Engine abstraction:
# - PandasEngine: Uses DuckDB for SQL
# - SparkEngine: Uses Spark SQL
# - Same operation, different backends!
```

### 🛠️ Do (12 min)

**Test SQL operations:**

```python
# Create: scratch/stage5_sql_demo.py

import pandas as pd
from odibi.config import ProjectConfig
from odibi.pipeline import Pipeline
import os

# Create test data
os.makedirs('scratch/stage5_output/input', exist_ok=True)

# Sales data
sales = pd.DataFrame({
    "product": ["Widget", "Gadget", "Widget", "Gadget", "Widget", "Gizmo"],
    "region": ["North", "North", "South", "South", "East", "East"],
    "revenue": [1000, 1500, 1200, 1800, 900, 2000],
    "units": [10, 15, 12, 18, 9, 20]
})
sales.to_csv('scratch/stage5_output/input/sales.csv', index=False)

# Customer data
customers = pd.DataFrame({
    "region": ["North", "South", "East", "West"],
    "population": [500000, 750000, 600000, 450000],
    "stores": [5, 8, 6, 4]
})
customers.to_csv('scratch/stage5_output/input/customers.csv', index=False)

# Create pipeline with SQL
yaml_content = """
story:
  connection: local_data
  format: markdown

connections:
  local_data:
    type: local
    base_path: ./scratch/stage5_output

pipelines:
  - name: sql_demo
    plant: "Analytics Lab"
    asset: "Sales Data"

    nodes:
      - name: load_sales
        read:
          connection: local_data
          path: input/sales.csv
          format: csv

      - name: load_customers
        read:
          connection: local_data
          path: input/customers.csv
          format: csv

      # SQL: Aggregate sales by region
      - name: sales_summary
        depends_on: [load_sales]
        transform:
          operation: sql
          params:
            query: |
              SELECT
                region,
                COUNT(*) as transactions,
                SUM(revenue) as total_revenue,
                SUM(units) as total_units,
                ROUND(AVG(revenue), 2) as avg_revenue,
                ROUND(SUM(revenue) * 1.0 / SUM(units), 2) as revenue_per_unit
              FROM load_sales
              GROUP BY region
              ORDER BY total_revenue DESC

      # SQL: Join sales with customers
      - name: regional_analysis
        depends_on: [sales_summary, load_customers]
        transform:
          operation: sql
          params:
            query: |
              SELECT
                s.region,
                s.total_revenue,
                s.total_units,
                c.population,
                c.stores,
                ROUND(s.total_revenue * 1.0 / c.population, 2) as revenue_per_capita,
                ROUND(s.total_revenue * 1.0 / c.stores, 2) as revenue_per_store
              FROM sales_summary s
              LEFT JOIN load_customers c ON s.region = c.region
              ORDER BY revenue_per_capita DESC
        write:
          connection: local_data
          path: output/regional_analysis.csv
          format: csv

      # SQL: Advanced window functions
      - name: product_rankings
        depends_on: [load_sales]
        transform:
          operation: sql
          params:
            query: |
              SELECT
                product,
                region,
                revenue,
                units,
                ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue DESC) as rank_in_region,
                ROUND(revenue * 100.0 / SUM(revenue) OVER (PARTITION BY region), 2) as pct_of_regional_revenue,
                ROUND(AVG(revenue) OVER (PARTITION BY product), 2) as avg_product_revenue
              FROM load_sales
              ORDER BY region, rank_in_region
        write:
          connection: local_data
          path: output/product_rankings.csv
          format: csv
"""

with open('scratch/stage5_sql_pipeline.yaml', 'w') as f:
    f.write(yaml_content)

# Run pipeline
print("🚀 Running SQL pipeline...")
config = ProjectConfig.from_yaml("scratch/stage5_sql_pipeline.yaml")
pipeline = Pipeline(config.pipelines[0], config)
result = pipeline.run()

print(f"\n✅ Pipeline completed in {result.duration:.2f}s")

# Show results
print("\n📊 Regional Analysis:")
analysis = pd.read_csv('scratch/stage5_output/output/regional_analysis.csv')
print(analysis)

print("\n📊 Product Rankings:")
rankings = pd.read_csv('scratch/stage5_output/output/product_rankings.csv')
print(rankings)

print("\n🔍 Key Insight:")
print("  - SQL runs on Pandas DataFrames via DuckDB")
print("  - Same SQL would work with SparkEngine via Spark SQL")
print("  - Operations are engine-agnostic!")
```

Run it:
```bash
python scratch/stage5_sql_demo.py
```

**Observe:**
- Complex SQL (JOINs, window functions) work on Pandas DataFrames
- Check the story - SQL queries are included in explanations
- Try running with `engine: spark` (if you have Spark installed)

### ✅ Checkpoint

Can you answer these?
- [ ] What engine does PandasEngine use for SQL? (DuckDB!)
- [ ] What engine does SparkEngine use for SQL? (Spark SQL!)
- [ ] Why do operations receive `engine` as a parameter?
- [ ] How does this enable portability?

**Test:** Can you write a SQL query with a CTE (Common Table Expression)?

---

## Stage 6: Azure SQL Connector (20-25 min)

### 🎯 Goal
Know how to read/write to Azure SQL from a pipeline with safe auth patterns.

### 📖 Read (10 min)

**Read these files:**

1. **`odibi/connections/azure_sql.py`** (200 lines)
   - See `AzureSQLConnection` class
   - See two auth modes: `azure_ad` and `sql_auth`
   - See connection string building
   - See validation logic

2. **`odibi/config.py`** - Search for "AzureSQLConnectionConfig"
   - See how connections are configured in YAML

3. **`docs/setup_azure.md`** (if exists) - SKIM
   - See environment variable patterns
   - See local development guidance

**Key Insight:**
```python
# Two authentication modes:
#
# 1. Azure AD (recommended for production):
#    Uses DefaultAzureCredential (managed identity)
#
# 2. SQL Auth (local development):
#    Uses username/password (not recommended for prod)
#
# Connection is validated eagerly (fail fast)
```

### 🛠️ Do (15 min)

**Configure Azure SQL (mock for local testing):**

```python
# Create: scratch/stage6_azure_sql_demo.py

import pandas as pd
import os
from odibi.config import ProjectConfig
from odibi.pipeline import Pipeline

# Create test data
os.makedirs('scratch/stage6_output/input', exist_ok=True)

df = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
    "department": ["Engineering", "Sales", "Engineering", "Marketing", "Sales"],
    "salary": [95000, 65000, 87000, 72000, 68000]
})
df.to_csv('scratch/stage6_output/input/employees.csv', index=False)

# Create pipeline YAML with Azure SQL connection
# Note: This is a TEMPLATE - adjust for your environment
yaml_content = """
story:
  connection: local_data
  format: markdown

connections:
  local_data:
    type: local
    base_path: ./scratch/stage6_output

  # Azure SQL connection (configure for your environment)
  azure_db:
    type: azure_sql
    server: your-server.database.windows.net  # Change this
    database: your-database                     # Change this
    auth_mode: sql_auth                         # or azure_ad for production
    # For sql_auth mode:
    username: ${AZURE_SQL_USER}                # Use env vars for secrets!
    password: ${AZURE_SQL_PASSWORD}
    # For azure_ad mode: no username/password needed

pipelines:
  - name: azure_sql_demo
    plant: "Data Warehouse"
    asset: "Employee Analytics"

    nodes:
      # Load from local
      - name: load_employees
        read:
          connection: local_data
          path: input/employees.csv
          format: csv

      # Transform
      - name: calculate_metrics
        depends_on: [load_employees]
        transform:
          operation: sql
          params:
            query: |
              SELECT
                department,
                COUNT(*) as employee_count,
                ROUND(AVG(salary), 2) as avg_salary,
                MIN(salary) as min_salary,
                MAX(salary) as max_salary
              FROM load_employees
              GROUP BY department
              ORDER BY avg_salary DESC

      # Write to Azure SQL
      - name: export_to_azure
        depends_on: [calculate_metrics]
        write:
          connection: azure_db
          path: department_metrics  # Table name
          format: sql
          mode: replace            # or append
"""

with open('scratch/stage6_azure_sql.yaml', 'w') as f:
    f.write(yaml_content)

print("📝 Created Azure SQL pipeline template")
print("\n⚠️  Configuration needed:")
print("  1. Update server and database in YAML")
print("  2. Set environment variables:")
print("     export AZURE_SQL_USER='your_username'")
print("     export AZURE_SQL_PASSWORD='your_password'")
print("  3. Or switch to auth_mode: azure_ad for production")

# For DEMO purposes, let's validate the config structure
print("\n🔍 Validating configuration structure...")

try:
    config = ProjectConfig.from_yaml("scratch/stage6_azure_sql.yaml")
    print("  ✅ Config structure is valid!")
    print(f"  ✅ Found {len(config.connections)} connections:")
    for name, conn in config.connections.items():
        print(f"     - {name}: {conn.type}")

    # Don't run pipeline without real Azure SQL credentials
    print("\n⏸️  Skipping execution (no Azure SQL credentials)")
    print("   To actually run: python scratch/stage6_azure_sql_demo.py --run")

except Exception as e:
    print(f"  ❌ Config error: {e}")

# Show how to use Azure AD auth (recommended for production)
print("\n📚 Production Best Practice - Azure AD Authentication:")
print("""
connections:
  azure_db_prod:
    type: azure_sql
    server: prod-server.database.windows.net
    database: prod-database
    auth_mode: azure_ad  # Uses managed identity in Azure

This approach:
  ✅ No passwords in config or env vars
  ✅ Uses Azure managed identity
  ✅ Supports DefaultAzureCredential chain
  ✅ Audit logs track actual user identity
""")

print("\n🔐 Local Development - SQL Auth:")
print("""
connections:
  azure_db_dev:
    type: azure_sql
    server: dev-server.database.windows.net
    database: dev-database
    auth_mode: sql_auth
    username: ${AZURE_SQL_USER}
    password: ${AZURE_SQL_PASSWORD}

This approach:
  ⚠️  Only for local development
  ⚠️  Never commit credentials
  ⚠️  Use env vars or Key Vault
""")

# If --run flag provided and credentials available
import sys
if '--run' in sys.argv:
    print("\n🚀 Running pipeline with Azure SQL...")
    try:
        pipeline = Pipeline(config.pipelines[0], config)
        result = pipeline.run()
        print(f"✅ Pipeline completed in {result.duration:.2f}s")
        print("📊 Data written to Azure SQL!")
    except Exception as e:
        print(f"❌ Execution error: {e}")
        print("   Check your Azure SQL credentials and connection")
```

Run it:
```bash
# Validate config structure
python scratch/stage6_azure_sql_demo.py

# Actually run (if you have Azure SQL setup)
export AZURE_SQL_USER='your_username'
export AZURE_SQL_PASSWORD='your_password'
python scratch/stage6_azure_sql_demo.py --run
```

**Key Takeaways:**
- Never commit credentials to YAML
- Use environment variables or Key Vault
- Prefer Azure AD auth in production
- Connection validation happens early (fail fast)

### ✅ Checkpoint

Can you answer these?
- [ ] What are the two authentication modes?
- [ ] Which mode is recommended for production?
- [ ] How do you avoid hardcoding passwords?
- [ ] When is the connection validated?

**Security check:** Can you spot the security issue if you hardcode a password in YAML?

---

## Stage 7: Quality & Guardrails (10-15 min)

### 🎯 Goal
Enforce explanation quality; get familiar with failure modes.

### 📖 Read (5 min)

**Read these files:**

1. **`odibi/validation/explanation_linter.py`** (100 lines)
   - See what gets validated
   - See quality rules

2. **`docs/guides/06_TROUBLESHOOTING.md`** - SKIM
   - See common errors
   - See where failures happen (config vs runtime)

**Key Insight:**
```python
# Quality enforcement:
# 1. Every transformation should have explain()
# 2. Explanations should be informative (not just "does stuff")
# 3. Should use templates for consistency
# 4. Linter catches violations before production
```

### 🛠️ Do (10 min)

**Run the linter:**

```python
# Create: scratch/stage7_quality_check.py

from odibi.transformations import transformation, get_registry
from odibi.validation.explanation_linter import lint_transformation
import pandas as pd

# BAD EXAMPLE - No explanation
@transformation("bad_op", category="test")
def bad_operation(df, threshold):
    """Bad operation with no explanation."""
    return df[df.value > threshold]

# GOOD EXAMPLE - Has explanation  
@transformation("good_op", category="test")
def good_operation(df, threshold):
    """Good operation with explanation."""
    return df[df.value > threshold]

@good_operation.explain
def explain(threshold, **context):
    """Proper explanation."""
    from odibi.transformations.templates import purpose_detail_result
    return purpose_detail_result(
        purpose=f"Filter data based on threshold",
        details=[f"Threshold: {threshold}", "Removes low values"],
        result="Filtered dataset"
    )

# Run linter
print("🔍 Linting transformations...\n")

print("1. Bad operation (missing explanation):")
issues_bad = lint_transformation("bad_op")
if issues_bad:
    for issue in issues_bad:
        print(f"   ❌ {issue}")
else:
    print("   ✅ No issues")

print("\n2. Good operation (has explanation):")
issues_good = lint_transformation("good_op")
if issues_good:
    for issue in issues_good:
        print(f"   ❌ {issue}")
else:
    print("   ✅ No issues")

# Lint all your custom operations
print("\n3. Your custom operations:")
for name in ["filter_threshold", "smart_filter"]:
    print(f"\n   Checking '{name}':")
    try:
        issues = lint_transformation(name)
        if issues:
            for issue in issues:
                print(f"     ❌ {issue}")
        else:
            print(f"     ✅ No issues")
    except Exception as e:
        print(f"     ⚠️  Not found (may need to import): {e}")

print("\n" + "="*60)
print("📚 Quality Rules:")
print("  ✅ Every transformation must have explain()")
print("  ✅ Explanations should be informative")
print("  ✅ Use templates for consistency")
print("  ✅ Include context-aware details")
print("  ✅ Test explanations with different context values")
```

Run it:
```bash
python scratch/stage7_quality_check.py
```

**Common failure modes:**

```python
# Create: scratch/stage7_troubleshooting.py

from odibi.config import ProjectConfig
from odibi.pipeline import Pipeline
import pandas as pd

print("📚 Common Failure Modes & Where They Occur:\n")

# 1. Config validation error
print("1. Config Validation (ProjectConfig instantiation):")
print("   When: Loading YAML")
print("   Example: Missing required field, invalid type")
print("   Fix: Check YAML structure, use VSCode YAML extension\n")

try:
    bad_yaml = """
    story:
      connection: missing_connection  # This connection doesn't exist!

    connections: {}

    pipelines: []
    """
    with open('scratch/bad_config.yaml', 'w') as f:
        f.write(bad_yaml)

    config = ProjectConfig.from_yaml('scratch/bad_config.yaml')
except Exception as e:
    print(f"   ❌ Caught: {type(e).__name__}: {e}\n")

# 2. Graph validation error
print("2. Dependency Graph (DependencyGraph.validate()):")
print("   When: Building execution order")
print("   Example: Circular dependency, missing node")
print("   Fix: Check depends_on references\n")

try:
    bad_yaml = """
    story:
      connection: local
      format: markdown

    connections:
      local:
        type: local
        base_path: ./scratch

    pipelines:
      - name: bad_deps
        nodes:
          - name: node_a
            depends_on: [node_b]  # Circular!
          - name: node_b
            depends_on: [node_a]  # Circular!
    """
    with open('scratch/bad_deps.yaml', 'w') as f:
        f.write(bad_yaml)

    config = ProjectConfig.from_yaml('scratch/bad_deps.yaml')
    pipeline = Pipeline(config.pipelines[0], config)
    # Graph validation happens here
except Exception as e:
    print(f"   ❌ Caught: {type(e).__name__}: {e}\n")

# 3. Runtime execution error
print("3. Node Execution (Node.execute()):")
print("   When: Running transformation")
print("   Example: Missing column, type error, file not found")
print("   Fix: Check data, operation parameters\n")

# 4. Connection error
print("4. Connection Error (Connection.validate()):")
print("   When: Accessing storage")
print("   Example: Invalid credentials, network timeout")
print("   Fix: Check connection config, credentials\n")

print("="*60)
print("🔧 Debugging Tips:")
print("  1. Read error messages carefully (they show exact location)")
print("  2. Check YAML syntax first (most common issue)")
print("  3. Validate config structure before running")
print("  4. Use --log-level DEBUG for detailed output")
print("  5. Check stories for node-level details")
print("  6. Run linter before production deployment")
```

Run it:
```bash
python scratch/stage7_troubleshooting.py
```

### ✅ Checkpoint

Can you answer these?
- [ ] What does the linter check for?
- [ ] Where do config errors occur?
- [ ] Where do runtime errors occur?
- [ ] How do you debug a failed pipeline?

**Final test:** Run the linter on all your custom operations. Fix any issues.

---

## 🎓 Graduation Challenge

You've completed all 7 stages! Time to put it all together.

### Challenge: Build a Complete Analytics Pipeline

**Requirements:**
1. Create a custom transformation with rich explanations
2. Build a 5+ node pipeline with dependencies
3. Use SQL for at least one transformation
4. Generate stories in all 3 formats (HTML, MD, JSON)
5. Pass the linter with zero issues
6. Include at least one write operation

**Bonus points:**
- Use multiple connections
- Include error handling
- Add data quality checks
- Use window functions in SQL
- Customize story theme

**Solution template:**

```python
# scratch/graduation_challenge.py

# 1. Define custom transformation
# 2. Create comprehensive pipeline YAML
# 3. Generate test data
# 4. Run pipeline
# 5. Generate stories
# 6. Run linter
# 7. Analyze results

# YOUR CODE HERE
```

---

## 📚 Next Steps

You now understand Odibi's internals up to Phase 3!

**Keep learning:**
- Read `docs/guides/04_ARCHITECTURE_GUIDE.md` - Visual diagrams
- Read `docs/guides/05_TRANSFORMATION_GUIDE.md` - Advanced patterns
- Explore Spark engine: `odibi/engine/spark_engine.py`
- Explore Azure ADLS: `walkthroughs/phase2a_adls_test.ipynb`
- Explore Delta Lake: `walkthroughs/phase2b_delta_lake.ipynb`

**Build something real:**
- Migrate an existing ETL script to Odibi
- Build a medallion architecture (Bronze/Silver/Gold)
- Create custom connectors
- Contribute back to the project!

**Get help:**
- Check `docs/guides/06_TROUBLESHOOTING.md`
- Read `CONTRIBUTING.md`
- Open GitHub issues
- Review existing walkthroughs

---

## 📊 Learning Progress Tracker

Track your progress through the stages:

- [ ] Stage 0: First successful pipeline run
- [ ] Stage 1: Built custom transformation
- [ ] Stage 2: Understand dependency resolution
- [ ] Stage 3: Added rich explanations
- [ ] Stage 4: Generated stories in all formats
- [ ] Stage 5: Used SQL transformations
- [ ] Stage 6: Configured Azure SQL (or understood how)
- [ ] Stage 7: Passed linter checks
- [ ] 🎓 Completed graduation challenge

**Estimated completion time:** 2-3 hours  
**Your actual time:** _____ hours

**What was hardest?** _____________________

**What clicked fastest?** _____________________

**Next topic to explore:** _____________________

---

## 🔑 Key Concepts Summary

### Transformation System
- `@transformation` decorator registers functions at import time
- Global registry stores all transformations
- `@func.explain` adds human explanations
- `**context` enables forward-compatible context passing

### Execution Flow
- YAML → ProjectConfig → Pipeline → Nodes → Engine → Stories
- DependencyGraph sorts nodes topologically
- Context stores DataFrames between nodes
- Metadata tracks everything for stories

### Stories
- Auto-generated during execution
- Metadata: duration, rows, schema, explanations
- Renderers: HTML, Markdown, JSON
- Themes: Dark, Light, Corporate, Minimal

### Engine Abstraction
- Operations are engine-agnostic
- PandasEngine: Uses DuckDB for SQL
- SparkEngine: Uses Spark SQL
- Same operations work on both engines

### Quality
- Explanation linter enforces standards
- Fail-fast validation (config, graph, connections)
- Rich error messages with context
- Stories provide execution transparency

---

**You're now ready to build production pipelines with Odibi!** 🚀
