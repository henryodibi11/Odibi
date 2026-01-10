# Lineage Stitcher

The Lineage Stitcher combines lineage data from multiple pipeline stories into a unified view, showing how data flows from raw sources through your entire data warehouse to the semantic layer.

## What is Lineage?

**Lineage** answers the question: "Where did this data come from?"

In a data warehouse, data flows through multiple stages:

```
Raw Sources → Bronze → Silver → Gold → Semantic Views → BI Dashboards
```

Each stage transforms the data. Lineage tracks these transformations so you can:

- **Trace origins** - Find where a metric's data comes from
- **Assess impact** - Know what breaks if a source changes
- **Debug issues** - Track down where bad data entered the pipeline
- **Document compliance** - Prove data provenance for audits

---

## What is the Lineage Stitcher?

Each Odibi pipeline generates its own **story** with a local lineage graph showing inputs and outputs. The **Lineage Stitcher** combines these individual graphs into one comprehensive view.

### Without the Stitcher

You have separate lineage graphs:

```
Bronze Story:
  raw_orders → bronze_orders
  raw_customers → bronze_customers

Silver Story:
  bronze_orders → silver_orders
  bronze_customers → dim_customers

Gold Story:
  silver_orders + dim_customers → fact_orders

Semantic Story:
  fact_orders → vw_daily_orders
  fact_orders → vw_monthly_orders
```

### With the Stitcher

One unified graph:

```
raw_orders → bronze_orders → silver_orders ─┐
                                             ├→ fact_orders → vw_daily_orders
raw_customers → bronze_customers → dim_customers ─┘          → vw_monthly_orders
```

---

## How It Works

The Lineage Stitcher:

1. **Reads all story JSON files** for a given date
2. **Extracts the `graph_data`** from each story
3. **Deduplicates nodes** (same table appearing in multiple stories)
4. **Merges edges** to create connected paths
5. **Generates combined output** as JSON and interactive HTML

### Story Structure

Each pipeline story contains a `graph_data` section:

```json
{
  "name": "gold_pipeline",
  "graph_data": {
    "nodes": [
      {"id": "silver_orders", "type": "table", "layer": "silver"},
      {"id": "dim_customers", "type": "table", "layer": "silver"},
      {"id": "fact_orders", "type": "table", "layer": "gold"}
    ],
    "edges": [
      {"from": "silver_orders", "to": "fact_orders"},
      {"from": "dim_customers", "to": "fact_orders"}
    ]
  }
}
```

The stitcher reads these from all stories and combines them.

---

## Configuration

Lineage stitching is configured in your `odibi.yaml`:

```yaml
story:
  connection: adls_stories      # Where stories are saved
  path: stories                  # Base path for stories
  auto_generate: true            # Generate stories after each run
  generate_lineage: true         # Generate combined lineage
```

### Key Settings

| Setting | Required | Default | Description |
|---------|----------|---------|-------------|
| `story.connection` | Yes | - | Storage connection for stories |
| `story.path` | Yes | - | Base path for story files |
| `story.auto_generate` | No | `true` | Generate stories after pipeline runs |
| `story.generate_lineage` | No | `false` | Generate combined lineage |

---

## Using the Lineage Generator

### Automatic Generation

When `generate_lineage: true` is set, lineage is automatically generated after running the semantic layer:

```python
from odibi import Project

project = Project.load("odibi.yaml")

# Run semantic layer - lineage generated automatically
result = project.run_semantic_layer()

# Lineage paths are in the result
if result['lineage_paths']:
    print(f"Lineage JSON: {result['lineage_paths']['json']}")
    print(f"Lineage HTML: {result['lineage_paths']['html']}")
```

### Manual Generation

For more control, use the `LineageGenerator` class directly:

```python
from odibi.story.lineage import LineageGenerator

# Create generator
generator = LineageGenerator(
    stories_path="abfs://datalake@myaccount.dfs.core.windows.net/stories",
    storage_options={"account_key": "..."}
)

# Generate lineage for today
result = generator.generate()

# Or for a specific date
result = generator.generate(date="2026-01-02")

# Print summary
print(f"Date: {result.date}")
print(f"Layers found: {len(result.layers)}")
print(f"Nodes: {len(result.nodes)}")
print(f"Edges: {len(result.edges)}")

# Save to files
paths = generator.save(result)
print(f"Saved to: {paths['html']}")
```

### Local File System

For local development:

```python
from odibi.story.lineage import LineageGenerator

# Local stories directory
generator = LineageGenerator(stories_path="./stories")

# Generate and save
result = generator.generate()
paths = generator.save(result)

# Open HTML in browser
import webbrowser
webbrowser.open(paths['html'])
```

---

## Output Files

### File Location

Lineage files are saved to:

```
{stories_path}/lineage/{date}/run_{time}.json
{stories_path}/lineage/{date}/run_{time}.html
```

Example:
```
stories/lineage/2026-01-02/run_10-45-30.json
stories/lineage/2026-01-02/run_10-45-30.html
```

### JSON Output

The JSON file contains the complete lineage data:

```json
{
  "generated_at": "2026-01-02T10:45:30",
  "date": "2026-01-02",
  "layers": [
    {
      "name": "bronze",
      "story_path": "stories/bronze/2026-01-02/run_08-00-15.json",
      "status": "success",
      "duration": 62.5,
      "pipeline_layer": "bronze"
    },
    {
      "name": "silver",
      "story_path": "stories/silver/2026-01-02/run_08-05-30.json",
      "status": "success",
      "duration": 132.8,
      "pipeline_layer": "silver"
    },
    {
      "name": "gold",
      "story_path": "stories/gold/2026-01-02/run_08-10-45.json",
      "status": "success",
      "duration": 156.2,
      "pipeline_layer": "gold"
    },
    {
      "name": "Sales_semantic",
      "story_path": "stories/Sales_semantic/2026-01-02/run_08-15-00.json",
      "status": "success",
      "duration": 27.3,
      "pipeline_layer": "semantic"
    }
  ],
  "nodes": [
    {"id": "raw_sales_data", "type": "source", "layer": "raw"},
    {"id": "bronze_sales", "type": "table", "layer": "bronze"},
    {"id": "cleaned_sales", "type": "table", "layer": "silver"},
    {"id": "fact_orders", "type": "table", "layer": "gold"},
    {"id": "vw_sales_daily", "type": "view", "layer": "semantic"},
    {"id": "vw_sales_monthly", "type": "view", "layer": "semantic"}
  ],
  "edges": [
    {"from": "raw_sales_data", "to": "bronze_sales"},
    {"from": "bronze_sales", "to": "cleaned_sales"},
    {"from": "cleaned_sales", "to": "fact_orders"},
    {"from": "fact_orders", "to": "vw_sales_daily"},
    {"from": "fact_orders", "to": "vw_sales_monthly"}
  ]
}
```

### HTML Output

The HTML file provides an interactive visualization:

- **Summary panel** - Date, layer count, node/edge counts
- **Layer cards** - Each pipeline with status and duration
- **Mermaid diagram** - Interactive lineage graph
- **Click-to-expand** - Details for each layer

The diagram is rendered using [Mermaid](https://mermaid.js.org/), showing:

- **Nodes** colored by layer (bronze, silver, gold, semantic)
- **Edges** showing data flow direction
- **Grouping** by layer for clarity

---

## Understanding the Output

### Layers

Each layer represents a pipeline run found for the date:

```json
{
  "name": "bronze",           // Pipeline name
  "story_path": "...",        // Path to the story file
  "status": "success",        // Pipeline status
  "duration": 62.5,           // Execution time in seconds
  "pipeline_layer": "bronze"  // Inferred layer type
}
```

The `pipeline_layer` is inferred from the pipeline name:
- Contains "bronze" → bronze layer
- Contains "silver" → silver layer
- Contains "gold" → gold layer
- Contains "semantic" → semantic layer
- Otherwise → unknown

### Nodes

Each node represents a data asset:

```json
{
  "id": "fact_orders",    // Unique identifier
  "type": "table",        // "source", "table", or "view"
  "layer": "gold"         // Which layer it belongs to
}
```

### Edges

Each edge represents data flow:

```json
{
  "from": "cleaned_sales", // Source node ID
  "to": "fact_orders"      // Target node ID
}
```

---

## Practical Examples

### Example 1: Generate Lineage After Full Pipeline Run

```python
from odibi import Project

project = Project.load("odibi.yaml")

# Run all pipelines
for pipeline in ["bronze", "silver", "gold"]:
    result = project.run(pipeline)
    print(f"{pipeline}: {result.status}")

# Run semantic layer with lineage
result = project.run_semantic_layer()

# View combined lineage
if result['lineage_paths']:
    print(f"\nView lineage at: {result['lineage_paths']['html']}")
```

### Example 2: Generate Lineage for a Past Date

```python
from odibi.story.lineage import LineageGenerator

generator = LineageGenerator(
    stories_path="abfs://datalake@myaccount.dfs.core.windows.net/stories",
    storage_options={"account_key": "..."}
)

# Generate for specific date
result = generator.generate(date="2025-12-15")

if result.nodes:
    print(f"Found {len(result.nodes)} nodes for 2025-12-15")
    generator.save(result)
else:
    print("No stories found for that date")
```

### Example 3: Analyze Lineage Programmatically

```python
from odibi.story.lineage import LineageGenerator

generator = LineageGenerator(stories_path="./stories")
result = generator.generate()

# Find all sources for a specific table
def find_sources(target_id, edges, visited=None):
    """Recursively find all upstream sources."""
    if visited is None:
        visited = set()
    
    sources = []
    for edge in edges:
        if edge.to_node == target_id and edge.from_node not in visited:
            visited.add(edge.from_node)
            sources.append(edge.from_node)
            sources.extend(find_sources(edge.from_node, edges, visited))
    
    return sources

# What feeds into vw_sales_daily?
sources = find_sources("vw_sales_daily", result.edges)
print(f"vw_sales_daily depends on: {sources}")
# Output: ['fact_orders', 'cleaned_sales', 'bronze_sales', 'raw_sales_data']
```

### Example 4: Custom Write Location

```python
from odibi.story.lineage import LineageGenerator

generator = LineageGenerator(stories_path="./stories")
result = generator.generate()

# Custom write function
def write_to_custom_location(path: str, content: str):
    """Write to a custom location."""
    # Could be S3, GCS, a database, etc.
    custom_path = path.replace("stories/", "custom_lineage/")
    with open(custom_path, "w") as f:
        f.write(content)
    print(f"Wrote to: {custom_path}")

generator.save(result, write_file=write_to_custom_location)
```

---

## Troubleshooting

### No Lineage Generated

**Symptom:** Lineage files are empty or missing.

**Causes and Solutions:**

1. **No stories for the date**
   - Check that pipelines ran and generated stories
   - Verify `story.auto_generate: true` is set
   
2. **Stories don't have graph_data**
   - Older story format may lack lineage data
   - Re-run pipelines to generate new stories

3. **Wrong storage path**
   - Verify `stories_path` matches where stories are saved
   - Check storage credentials

### Missing Connections

**Symptom:** Lineage shows disconnected nodes.

**Cause:** Node IDs don't match across stories.

**Solution:** Ensure consistent table naming:
```yaml
# In bronze
write:
  path: bronze_orders  # Use this exact name

# In silver
read:
  path: bronze_orders  # Must match exactly
```

### Duplicate Nodes

**Symptom:** Same table appears multiple times.

**Cause:** Different naming in different stories.

**Solution:** The stitcher deduplicates by `id`. Ensure IDs are consistent:
- `bronze/orders` vs `bronze_orders` → different IDs
- Use consistent naming convention

---

## Best Practices

### 1. Run All Pipelines Before Generating Lineage

For complete lineage, run all pipelines for the date:

```python
project.run("bronze")
project.run("silver")
project.run("gold")
project.run_semantic_layer()  # Generates combined lineage
```

### 2. Use Consistent Naming

Table names should be consistent across pipelines:

```yaml
# Bronze output
write:
  path: customers

# Silver input (must match)
read:
  path: customers
```

### 3. Review Lineage Regularly

Make lineage review part of your data governance:

- Check after major changes
- Include in documentation
- Share with stakeholders

### 4. Archive Historical Lineage

Lineage shows the state at a point in time. Keep historical files:

```
stories/lineage/2026-01-01/run_10-00-00.json
stories/lineage/2026-01-02/run_10-00-00.json
stories/lineage/2026-01-03/run_10-00-00.json
```

### 5. Use Lineage for Impact Analysis

Before making changes, check what depends on a table:

```python
# What views depend on fact_orders?
downstream = [e.to_node for e in result.edges if e.from_node == "fact_orders"]
print(f"Changing fact_orders will affect: {downstream}")
```

---

## Integration with Other Features

### Stories

The Lineage Stitcher reads from [Pipeline Stories](../features/stories.md). Each story contains local lineage that gets combined.

### Cross-Pipeline Lineage

For catalog-based lineage tracking (stored in Delta tables), see [Cross-Pipeline Lineage](../features/lineage.md). The stitcher is complementary - it works from story files rather than the catalog.

### Semantic Layer Runner

When you run the [Semantic Layer Runner](./runner.md) with `generate_lineage: true`, it automatically calls the Lineage Stitcher after view creation.

---

## API Reference

### LineageGenerator

```python
class LineageGenerator:
    def __init__(
        self,
        stories_path: str,              # Base path to stories
        storage_options: dict = None,   # Credentials for remote storage
        date: str = None                # Default date (today if not specified)
    ):
        ...
    
    def generate(
        self,
        date: str = None                # Date to generate for (YYYY-MM-DD)
    ) -> LineageResult:
        """Generate combined lineage from all stories for the date."""
        ...
    
    def save(
        self,
        result: LineageResult = None,   # Result to save (uses last if None)
        write_file: Callable = None     # Custom write function
    ) -> dict:
        """Save lineage as JSON and HTML. Returns paths."""
        ...
    
    def render_json(self, result: LineageResult = None) -> str:
        """Render lineage as JSON string."""
        ...
    
    def render_html(self, result: LineageResult = None) -> str:
        """Render lineage as HTML string with Mermaid diagram."""
        ...
```

### LineageResult

```python
@dataclass
class LineageResult:
    generated_at: str           # ISO timestamp
    date: str                   # Date string (YYYY-MM-DD)
    layers: List[LayerInfo]     # Pipeline layers found
    nodes: List[LineageNode]    # All nodes
    edges: List[LineageEdge]    # All edges
    json_path: str = None       # Path after save
    html_path: str = None       # Path after save
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        ...
```

### LayerInfo

```python
@dataclass
class LayerInfo:
    name: str                   # Pipeline name
    story_path: str             # Path to story file
    status: str                 # "success" or "failed"
    duration: float             # Execution time in seconds
    pipeline_layer: str = None  # Inferred layer (bronze/silver/gold/semantic)
```

### LineageNode

```python
@dataclass
class LineageNode:
    id: str                     # Unique identifier
    type: str                   # "source", "table", or "view"
    layer: str                  # bronze, silver, gold, semantic, etc.
```

### LineageEdge

```python
@dataclass
class LineageEdge:
    from_node: str              # Source node ID
    to_node: str                # Target node ID
```

---

## See Also

- [Semantic Layer Runner](./runner.md) - Create SQL views from metrics
- [Pipeline Stories](../features/stories.md) - Execution documentation
- [Cross-Pipeline Lineage](../features/lineage.md) - Catalog-based lineage tracking
