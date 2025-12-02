# Execution Stories

Auto-generated pipeline execution documentation with rich metadata, sample data, and multiple output formats.

## Overview

Odibi's Story system provides:
- **Execution timeline**: Complete record of pipeline runs with timestamps
- **Node-level metrics**: Duration, row counts, schema changes per node
- **Sample data capture**: Input/output samples with automatic redaction
- **Multiple renderers**: HTML, Markdown, JSON output formats
- **Themes**: Customizable styling for HTML reports
- **Retention policies**: Automatic cleanup of old stories

## Configuration

### Basic Story Setup

```yaml
story:
  connection: "local_data"
  path: "stories/"
  max_sample_rows: 10
  retention_days: 30
  retention_count: 100
  failure_sample_size: 100
  max_failure_samples: 500
  max_sampled_validations: 5
```

### Story Config Options

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `connection` | string | Yes | - | Connection name for story output |
| `path` | string | Yes | - | Path for stories (relative to connection base_path) |
| `max_sample_rows` | int | No | `10` | Maximum rows to include in samples |
| `retention_days` | int | No | `30` | Days to keep stories before cleanup |
| `retention_count` | int | No | `100` | Maximum number of stories to retain |
| `failure_sample_size` | int | No | `100` | Rows to capture per validation failure |
| `max_failure_samples` | int | No | `500` | Total failed rows across all validations |
| `max_sampled_validations` | int | No | `5` | After this many validations, show only counts |
| `theme` | string | No | `default` | Theme name or path to YAML theme file |
| `include_samples` | bool | No | `true` | Whether to include data samples |

### Remote Storage

Stories can be written to remote storage (ADLS, S3) using fsspec:

```yaml
story:
  output_path: abfss://container@account.dfs.core.windows.net/stories/
  storage_options:
    account_key: "${STORAGE_ACCOUNT_KEY}"
```

## Story Contents

Each story captures comprehensive execution metadata:

### Execution Timeline

| Metric | Description |
|--------|-------------|
| `started_at` | ISO timestamp when pipeline started |
| `completed_at` | ISO timestamp when pipeline finished |
| `duration` | Total execution time in seconds |
| `run_id` | Unique identifier for the run |

### Node Results

For each node in the pipeline:

| Metric | Description |
|--------|-------------|
| `node_name` | Name of the node |
| `operation` | Operation type (read, transform, write) |
| `status` | Execution status: `success`, `failed`, `skipped` |
| `duration` | Node execution time in seconds |
| `rows_in` | Input row count |
| `rows_out` | Output row count |
| `rows_change` | Row count difference |
| `rows_change_pct` | Percentage change in row count |

### Sample Data

Sample data is captured with automatic redaction of sensitive values:

```yaml
sample_data:
  - order_id: 12345
    customer_email: "[REDACTED]"
    amount: 99.99
  - order_id: 12346
    customer_email: "[REDACTED]"
    amount: 149.99
```

Configure sample capture:

```yaml
story:
  max_sample_rows: 5      # Limit sample size
  include_samples: true   # Enable/disable samples
```

### Schema Changes

Stories track schema evolution:

| Field | Description |
|-------|-------------|
| `schema_in` | Input column names |
| `schema_out` | Output column names |
| `columns_added` | New columns added |
| `columns_removed` | Columns removed |
| `columns_renamed` | Renamed columns |

### Validation Results

Validation warnings and errors are captured:

```yaml
validation_warnings:
  - "Column 'email' has 5% null values"
  - "Date range extends beyond expected bounds"
```

Error details for failed nodes:

```yaml
error_type: ValueError
error_message: "Column 'order_id' contains duplicate values"
error_traceback: "Full Python traceback..."
error_traceback_cleaned: "Cleaned traceback (Spark/Java noise removed)"
```

### Execution Steps

Stories capture the execution steps taken during node processing for debugging:

```yaml
execution_steps:
  - "Read from bronze_db"
  - "Applied pattern 'deduplicate'"
  - "Executed 2 pre-SQL statement(s)"
  - "Passed 3 contract checks"
```

### Failed Rows Samples

When validations fail, stories capture sample rows that failed each validation:

```yaml
failed_rows_samples:
  not_null_customer_id:
    - { order_id: 123, customer_id: null, amount: 50.00 }
    - { order_id: 456, customer_id: null, amount: 75.00 }
  positive_amount:
    - { order_id: 789, customer_id: "C001", amount: -10.00 }

failed_rows_counts:
  not_null_customer_id: 150
  positive_amount: 25
```

Configure failure sample limits:

```yaml
story:
  failure_sample_size: 100        # Max rows per validation
  max_failure_samples: 500        # Total rows across all validations
  max_sampled_validations: 5      # After 5 validations, show only counts
```

### Retry History

When retries occur, the full history is captured:

```yaml
retry_history:
  - attempt: 1
    success: false
    error: "Connection timeout"
    error_type: "TimeoutError"
    duration: 1.2
  - attempt: 2
    success: false
    error: "Connection timeout"
    error_type: "TimeoutError"
    duration: 2.4
  - attempt: 3
    success: true
    duration: 0.8
```

### Delta Lake Info

For Delta Lake writes, version and operation metrics are captured:

```yaml
delta_info:
  version: 42
  operation: MERGE
  operation_metrics:
    numTargetRowsInserted: 150
    numTargetRowsUpdated: 25
```

## Themes

Customize HTML story appearance with built-in or custom themes.

### Built-in Themes

| Theme | Description |
|-------|-------------|
| `default` | Clean, professional blue theme |
| `corporate` | Traditional business styling with serif headings |
| `dark` | Dark mode with high-contrast colors |
| `minimal` | Simple black and white, compact layout |

### Using Themes

```yaml
story:
  theme: dark
```

### Custom Theme File

Create a custom theme YAML file:

```yaml
# my_theme.yaml
name: company_brand
primary_color: "#003366"
success_color: "#2e7d32"
error_color: "#c62828"
warning_color: "#ff9900"
bg_color: "#ffffff"
text_color: "#333333"
font_family: "Arial, sans-serif"
heading_font: "Georgia, serif"
logo_url: "https://example.com/logo.png"
company_name: "Acme Corp"
footer_text: "Confidential - Internal Use Only"
```

Reference in config:

```yaml
story:
  theme: path/to/my_theme.yaml
```

### Theme Options

| Option | Type | Description |
|--------|------|-------------|
| `name` | string | Theme identifier |
| `primary_color` | hex | Main accent color |
| `success_color` | hex | Success status color |
| `error_color` | hex | Error status color |
| `warning_color` | hex | Warning status color |
| `bg_color` | hex | Background color |
| `text_color` | hex | Primary text color |
| `border_color` | hex | Border color |
| `code_bg` | hex | Code block background |
| `font_family` | string | Body font stack |
| `heading_font` | string | Heading font stack |
| `code_font` | string | Monospace font stack |
| `font_size` | string | Base font size |
| `max_width` | string | Container max width |
| `logo_url` | string | URL to company logo |
| `company_name` | string | Company name for branding |
| `footer_text` | string | Custom footer text |
| `custom_css` | string | Additional CSS rules |

## Renderers

Stories can be rendered in multiple formats.

### HTML Renderer

Default format with interactive, responsive design:

```python
from odibi.story.renderers import HTMLStoryRenderer, get_renderer
from odibi.story.themes import get_theme

# Using the factory
renderer = get_renderer("html")
html = renderer.render(metadata)

# With custom theme
theme = get_theme("dark")
renderer = HTMLStoryRenderer(theme=theme)
html = renderer.render(metadata)
```

Features:
- Collapsible node sections
- Status indicators with color coding
- Summary statistics dashboard
- Responsive layout

### JSON Renderer

Machine-readable format for API integration:

```python
from odibi.story.renderers import JSONStoryRenderer

renderer = JSONStoryRenderer()
json_str = renderer.render(metadata)
```

Output structure:
```json
{
  "pipeline_name": "process_orders",
  "run_id": "20240130_101500",
  "started_at": "2024-01-30T10:15:00",
  "completed_at": "2024-01-30T10:15:45",
  "duration": 45.23,
  "total_nodes": 5,
  "completed_nodes": 4,
  "failed_nodes": 1,
  "skipped_nodes": 0,
  "success_rate": 80.0,
  "total_rows_processed": 15000,
  "nodes": [...]
}
```

### Markdown Renderer

GitHub-flavored markdown for documentation:

```python
from odibi.story.renderers import MarkdownStoryRenderer

renderer = MarkdownStoryRenderer()
md = renderer.render(metadata)
```

### Renderer Factory

Use the factory function to get a renderer by format:

```python
from odibi.story.renderers import get_renderer

# Supported formats: "html", "markdown", "md", "json"
renderer = get_renderer("json")
output = renderer.render(metadata)
```

## Retention

Stories are automatically cleaned up based on retention policies.

### Retention Configuration

```yaml
story:
  retention_days: 30    # Delete stories older than 30 days
  retention_count: 100  # Keep maximum 100 stories per pipeline
```

### How Retention Works

1. **Count-based**: When story count exceeds `retention_count`, oldest stories are deleted first
2. **Time-based**: Stories older than `retention_days` are deleted
3. **Both apply**: A story is deleted if it exceeds either limit

### Storage Structure

Stories are organized by pipeline and date:

```
stories/
├── process_orders/
│   ├── 2024-01-30/
│   │   ├── run_10-15-00.html
│   │   ├── run_10-15-00.json
│   │   ├── run_14-30-00.html
│   │   └── run_14-30-00.json
│   └── 2024-01-31/
│       └── ...
└── process_customers/
    └── ...
```

### Remote Storage Cleanup

> **Note**: Automatic cleanup for remote storage (ADLS, S3) is not yet implemented. Monitor storage usage manually.

## Examples

### Complete Story Configuration

```yaml
project: DataPipeline
engine: spark

story:
  output_path: stories/
  max_sample_rows: 10
  retention_days: 30
  retention_count: 100
  theme: corporate
  include_samples: true

pipelines:
  - pipeline: process_orders
    nodes:
      - name: read_orders
        read:
          connection: bronze
          path: orders/

      - name: transform_orders
        transform:
          operation: sql
          query: |
            SELECT order_id, customer_id, amount
            FROM {read_orders}
            WHERE amount > 0

      - name: write_orders
        write:
          connection: silver
          path: orders/
          mode: merge
```

### Generated Story Output (JSON)

```json
{
  "pipeline_name": "process_orders",
  "pipeline_layer": "silver",
  "run_id": "20240130_101500",
  "started_at": "2024-01-30T10:15:00",
  "completed_at": "2024-01-30T10:15:45",
  "duration": 45.23,
  "total_nodes": 3,
  "completed_nodes": 3,
  "failed_nodes": 0,
  "skipped_nodes": 0,
  "success_rate": 100.0,
  "total_rows_processed": 15000,
  "project": "DataPipeline",
  "nodes": [
    {
      "node_name": "read_orders",
      "operation": "read",
      "status": "success",
      "duration": 5.12,
      "rows_out": 15500,
      "schema_out": ["order_id", "customer_id", "amount", "created_at"]
    },
    {
      "node_name": "transform_orders",
      "operation": "transform",
      "status": "success",
      "duration": 2.34,
      "rows_in": 15500,
      "rows_out": 15000,
      "rows_change": -500,
      "rows_change_pct": -3.2,
      "columns_removed": ["created_at"]
    },
    {
      "node_name": "write_orders",
      "operation": "write",
      "status": "success",
      "duration": 37.77,
      "rows_out": 15000,
      "delta_info": {
        "version": 42,
        "operation": "MERGE",
        "operation_metrics": {
          "numTargetRowsInserted": 500,
          "numTargetRowsUpdated": 14500
        }
      }
    }
  ]
}
```

### Programmatic Story Generation

```python
from odibi.story.generator import StoryGenerator
from odibi.story.metadata import PipelineStoryMetadata
from odibi.story.themes import get_theme

# Create generator
generator = StoryGenerator(
    pipeline_name="process_orders",
    max_sample_rows=10,
    output_path="stories/",
    retention_days=30,
    retention_count=100,
)

# Generate story after pipeline execution
story_path = generator.generate(
    node_results=node_results,
    completed=["read_orders", "transform_orders", "write_orders"],
    failed=[],
    skipped=[],
    duration=45.23,
    start_time="2024-01-30T10:15:00",
    end_time="2024-01-30T10:15:45",
)

# Get summary for alerts
alert_summary = generator.get_alert_summary()
```

### Documentation Stories

Generate stakeholder-ready documentation from pipeline config:

```python
from odibi.story.doc_story import DocStoryGenerator
from odibi.config import PipelineConfig

# Load pipeline config
pipeline_config = PipelineConfig.from_yaml("pipeline.yaml")

# Generate documentation
doc_generator = DocStoryGenerator(pipeline_config)
doc_path = doc_generator.generate(
    output_path="docs/pipeline_doc.html",
    format="html",
    include_flow_diagram=True,
)
```

## Related

- [Alerting](alerting.md) - Stories linked in alert payloads
- [Quality Gates](quality_gates.md) - Gate results captured in stories
- [Schema Tracking](schema_tracking.md) - Schema changes in stories
- [YAML Schema Reference](../reference/yaml_schema.md#storyconfig)
