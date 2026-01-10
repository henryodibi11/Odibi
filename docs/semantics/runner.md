# Semantic Layer Runner

The Semantic Layer Runner executes your metric definitions as SQL Server views and generates execution stories for observability. This guide walks you through everything from basic concepts to production deployment.

## What is the Semantic Layer Runner?

In a typical data warehouse, you have:

1. **Bronze layer** - Raw data ingested from sources
2. **Silver layer** - Cleaned and transformed data
3. **Gold layer** - Business-ready fact and dimension tables
4. **Semantic layer** - Pre-aggregated views for analytics and BI tools

The **Semantic Layer Runner** bridges the gap between your Gold layer tables and your BI tools (Power BI, Tableau, etc.) by:

- **Generating SQL views** from your metric definitions
- **Executing those views** against SQL Server (or Azure SQL)
- **Creating execution stories** that document what happened
- **Generating lineage** showing how data flows from sources to views

Think of it as an automated "view factory" that turns your YAML metric definitions into real database views.

---

## Why Use the Semantic Layer Runner?

### Without the Runner

You would manually:
1. Write SQL view definitions by hand
2. Execute them against SQL Server
3. Track which views succeeded or failed
4. Document the SQL for auditing
5. Update views when metrics change

### With the Runner

You:
1. Define metrics in YAML (human-readable)
2. Run one command
3. Get views created, documented, and tracked automatically

**Benefits:**
- **Consistency** - All views follow the same pattern
- **Auditability** - Every execution is documented in stories
- **Maintainability** - Change YAML, re-run, views update
- **Observability** - Know exactly what happened and when

---

## Prerequisites

Before using the Semantic Layer Runner, you need:

1. **Gold layer tables** - Your fact tables with calculated metrics (e.g., `fact_orders`)
2. **A SQL Server connection** - Where views will be created
3. **A storage connection** - Where stories and SQL files will be saved (e.g., Azure Data Lake)

---

## Configuration

The Semantic Layer Runner is configured in your `odibi.yaml` project file. Here's a complete example:

```yaml
project: SalesAnalytics
engine: spark

# Define your connections
connections:
  # Where your gold layer data lives
  gold:
    type: delta
    account_name: mydatalake
    container: datalake
    base_path: gold/sales
    auth:
      mode: account_key
      account_key: ${AZURE_STORAGE_KEY}

  # Where SQL views will be created
  sql_server:
    type: azure_sql
    host: myserver.database.windows.net
    database: analytics_db
    port: 1433
    auth:
      mode: sql_login
      username: ${SQL_USER}
      password: ${SQL_PASSWORD}

  # Where stories will be saved
  stories:
    type: azure_blob
    account_name: mydatalake
    container: datalake
    base_path: stories
    auth:
      mode: account_key
      account_key: ${AZURE_STORAGE_KEY}

# Story configuration
story:
  connection: stories
  path: stories
  auto_generate: true
  generate_lineage: true

# Semantic layer configuration
semantic:
  # Which SQL Server connection to use for view creation
  connection: sql_server
  
  # Where to save generated SQL files (optional but recommended)
  sql_output_path: gold/sales/views
  
  # Define your views
  views:
    - name: vw_sales_daily
      description: "Daily sales metrics by store"
      source: fact_orders
      db_schema: semantic
      metrics:
        - name: revenue
          expr: "SUM(revenue)"
          description: "Total revenue"
        - name: order_count
          expr: "COUNT(*)"
          description: "Total number of orders"
        - name: avg_order_value
          expr: "AVG(order_value)"
          description: "Average order value"
        - name: total_sales
          expr: "SUM(total_sales)"
          description: "Total sales amount"
      dimensions:
        - name: date
          column: Date
        - name: store
          column: store_id
      grain: day

    - name: vw_sales_monthly
      description: "Monthly sales metrics by store"
      source: fact_orders
      db_schema: semantic
      metrics:
        - name: revenue
          expr: "SUM(revenue)"
        - name: order_count
          expr: "COUNT(*)"
        - name: total_sales
          expr: "SUM(total_sales)"
      dimensions:
        - name: year_month
          column: "FORMAT(Date, 'yyyy-MM')"
        - name: store
          column: store_id
      grain: month
```

### Configuration Reference

#### `semantic.connection`

The name of the SQL Server connection where views will be created.

```yaml
semantic:
  connection: sql_server  # Must match a connection name above
```

#### `semantic.sql_output_path`

Optional path where generated SQL files will be saved. Useful for:
- Version control of SQL definitions
- Auditing what SQL was executed
- Debugging view creation issues

```yaml
semantic:
  sql_output_path: gold/sales/views
```

#### `semantic.views`

A list of view definitions. Each view becomes a SQL Server view.

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | View name (will be created as `db_schema.name`) |
| `description` | No | Human-readable description |
| `source` | Yes | Source table name (your gold layer fact table) |
| `db_schema` | No | SQL Server schema (default: `semantic`) |
| `ensure_schema` | No | Auto-create schema if missing (default: `true`) |
| `metrics` | Yes | List of metrics to include |
| `dimensions` | Yes | List of dimensions to group by |
| `grain` | No | Aggregation grain: `day`, `week`, `month`, `quarter`, `year` |
| `filters` | No | WHERE clause filters |

#### Metric Definition

Each metric defines an aggregation:

```yaml
metrics:
  - name: revenue                # Column name in the view
    expr: "SUM(revenue)"         # SQL aggregation expression
    description: "Total revenue" # Optional documentation
```

**Common expressions:**
- `SUM(column)` - Total
- `AVG(column)` - Average
- `COUNT(*)` - Row count
- `COUNT(DISTINCT column)` - Unique count
- `MAX(column)` / `MIN(column)` - Maximum/Minimum

#### Dimension Definition

Each dimension defines a grouping column:

```yaml
dimensions:
  - name: date           # Column name in the view
    column: Date         # Source column (can be an expression)
```

**Examples:**
```yaml
dimensions:
  # Simple column reference
  - name: store_id
    column: store_id

  # Date formatting
  - name: year_month
    column: "FORMAT(Date, 'yyyy-MM')"

  # Derived column
  - name: is_weekend
    column: "CASE WHEN DATEPART(dw, Date) IN (1,7) THEN 1 ELSE 0 END"
```

---

## Running the Semantic Layer

### Using Python

```python
from odibi import Project

# Load your project configuration
project = Project.load("odibi.yaml")

# Run the semantic layer
# This will:
# 1. Generate SQL for each view
# 2. Execute the SQL against SQL Server
# 3. Save the SQL files
# 4. Generate an execution story
result = project.run_semantic_layer()

# Check results
print(f"Views created: {result['views_created']}")
print(f"Views failed: {result['views_failed']}")
print(f"Duration: {result['duration']:.2f}s")

# Story paths (if auto_generate is true)
if result['story_paths']:
    print(f"Story JSON: {result['story_paths']['json']}")
    print(f"Story HTML: {result['story_paths']['html']}")
```

### Using SemanticLayerRunner Directly

For more control, use the `SemanticLayerRunner` class:

```python
from odibi.semantics.runner import SemanticLayerRunner
from odibi.config import ProjectConfig

# Load configuration
config = ProjectConfig.from_yaml("odibi.yaml")

# Create runner
runner = SemanticLayerRunner(config)

# Run with options
result = runner.run(
    generate_story=True,      # Create execution story
    generate_lineage=True,    # Create combined lineage
)

# Access metadata
if runner.metadata:
    for view in runner.metadata.views:
        status = "✓" if view.status == "success" else "✗"
        print(f"{status} {view.view_name}: {view.duration:.2f}s")
        if view.error_message:
            print(f"  Error: {view.error_message}")
```

### Custom SQL Executor

If you need custom SQL execution logic:

```python
import pyodbc

def my_sql_executor(sql: str) -> None:
    """Custom SQL executor with logging."""
    conn = pyodbc.connect(my_connection_string)
    cursor = conn.cursor()
    print(f"Executing: {sql[:100]}...")
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()

# Run with custom executor
result = runner.run(
    execute_sql=my_sql_executor,
    generate_story=True,
)
```

### Custom File Writer

For custom storage backends:

```python
def write_to_s3(path: str, content: str) -> None:
    """Write files to S3 instead of Azure."""
    import boto3
    s3 = boto3.client('s3')
    s3.put_object(Bucket='my-bucket', Key=path, Body=content)

result = runner.run(
    write_file=write_to_s3,
    generate_story=True,
)
```

---

## Generated SQL

The runner generates standard SQL Server view definitions. Here's an example of what gets created:

### Input (YAML)

```yaml
views:
  - name: vw_sales_monthly
    source: fact_orders
    db_schema: semantic
    metrics:
      - name: revenue
        expr: "SUM(revenue)"
      - name: total_sales
        expr: "SUM(total_sales)"
    dimensions:
      - name: year_month
        column: "FORMAT(Date, 'yyyy-MM')"
      - name: store_id
        column: store_id
    filters:
      - "revenue > 0"
```

### Output (SQL)

```sql
-- View: semantic.vw_sales_monthly
-- Generated by Odibi Semantic Layer Runner
-- Source: fact_orders
-- Generated at: 2026-01-02T10:30:00

-- Ensure schema exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'semantic')
BEGIN
    EXEC('CREATE SCHEMA semantic')
END
GO

-- Create or replace view
CREATE OR ALTER VIEW semantic.vw_sales_monthly AS
SELECT
    FORMAT(Date, 'yyyy-MM') AS year_month,
    store_id AS store_id,
    SUM(revenue) AS revenue,
    SUM(total_sales) AS total_sales
FROM fact_orders
WHERE revenue > 0
GROUP BY FORMAT(Date, 'yyyy-MM'), store_id
GO
```

---

## Execution Stories

Every run generates an **execution story** - a detailed record of what happened. Stories are saved as both JSON (for programmatic access) and HTML (for human viewing).

### Story Location

Stories are saved to:
```
{story.path}/{semantic_name}/{date}/run_{time}.json
{story.path}/{semantic_name}/{date}/run_{time}.html
```

Example:
```
stories/Sales_semantic/2026-01-02/run_10-30-45.json
stories/Sales_semantic/2026-01-02/run_10-30-45.html
```

### Story Contents

The JSON story includes:

```json
{
  "name": "Sales_semantic",
  "started_at": "2026-01-02T10:30:45",
  "completed_at": "2026-01-02T10:31:12",
  "duration": 27.3,
  "views_created": 5,
  "views_failed": 0,
  "views": [
    {
      "view_name": "vw_sales_daily",
      "source_table": "fact_orders",
      "status": "success",
      "duration": 2.1,
      "sql_generated": "CREATE OR ALTER VIEW semantic.vw_sales_daily AS ...",
      "sql_file_path": "gold/sales/views/vw_sales_daily.sql",
      "metrics_included": ["revenue", "order_count", "avg_order_value", "total_sales"],
      "dimensions_included": ["date", "store"]
    },
    {
      "view_name": "vw_sales_monthly",
      "source_table": "fact_orders",
      "status": "success",
      "duration": 1.8,
      "sql_generated": "...",
      "sql_file_path": "gold/sales/views/vw_sales_monthly.sql",
      "metrics_included": ["revenue", "order_count", "total_sales"],
      "dimensions_included": ["year_month", "store"]
    }
  ],
  "sql_files_saved": [
    "gold/sales/views/vw_sales_daily.sql",
    "gold/sales/views/vw_sales_monthly.sql"
  ],
  "graph_data": {
    "nodes": [
      {"id": "fact_orders", "type": "table", "layer": "gold"},
      {"id": "vw_sales_daily", "type": "view", "layer": "semantic"},
      {"id": "vw_sales_monthly", "type": "view", "layer": "semantic"}
    ],
    "edges": [
      {"from": "fact_orders", "to": "vw_sales_daily"},
      {"from": "fact_orders", "to": "vw_sales_monthly"}
    ]
  }
}
```

### HTML Story

The HTML story provides a visual representation with:

- **Summary** - Overall status, duration, counts
- **View Cards** - Each view with status, metrics, dimensions
- **SQL Details** - Expandable section showing generated SQL
- **Lineage Diagram** - Visual graph of source → view relationships

---

## Error Handling

When a view fails to create, the runner:

1. **Continues processing** other views (doesn't stop on first error)
2. **Records the error** in the story
3. **Returns failed views** in the result

### Handling Failures

```python
result = runner.run()

if result['views_failed']:
    print("Some views failed:")
    for view in runner.metadata.views:
        if view.status == "failed":
            print(f"  {view.view_name}: {view.error_message}")
```

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `Invalid column name` | Source column doesn't exist | Check `source` table schema |
| `Invalid object name` | Source table doesn't exist | Ensure gold layer table exists |
| `Cannot create schema` | Permission denied | Grant schema creation rights |
| `Login failed` | Authentication error | Check connection credentials |

---

## Best Practices

### 1. Use Descriptive Names

```yaml
views:
  # Good - clear purpose
  - name: vw_sales_daily_by_store
  
  # Bad - unclear
  - name: v1
```

### 2. Document Your Metrics

```yaml
metrics:
  - name: revenue
    expr: "SUM(revenue)"
    description: "Total revenue - sum of all order values"
```

### 3. Separate Schemas by Purpose

```yaml
views:
  # Operational views for daily dashboards
  - name: vw_sales_daily
    db_schema: operational
  
  # Executive views for monthly reports
  - name: vw_sales_monthly
    db_schema: executive
```

### 4. Save SQL Files for Auditing

```yaml
semantic:
  sql_output_path: gold/views  # Always save SQL
```

### 5. Check Stories After Runs

Always review execution stories, especially in production:

```python
result = runner.run()
if result['story_paths']:
    print(f"Review story at: {result['story_paths']['html']}")
```

---

## Integration with Pipelines

The typical workflow is:

1. **Bronze pipeline** - Ingest raw data
2. **Silver pipeline** - Clean and transform
3. **Gold pipeline** - Build fact tables (e.g., `fact_orders`)
4. **Semantic layer** - Create views from facts

```python
from odibi import Project

project = Project.load("odibi.yaml")

# Run all pipelines in order
project.run("bronze")
project.run("silver")
project.run("gold")

# Then create semantic views
project.run_semantic_layer()
```

### Scheduling

For production, schedule the semantic layer after your gold pipeline:

```python
# In Databricks or Airflow
def daily_etl():
    project = Project.load("odibi.yaml")
    
    # Run ETL
    project.run("bronze")
    project.run("silver")
    project.run("gold")
    
    # Update semantic views
    result = project.run_semantic_layer()
    
    # Alert on failures
    if result['views_failed']:
        send_alert(f"Semantic layer had {len(result['views_failed'])} failures")
```

---

## Troubleshooting

### Views Not Updating

**Symptom:** You changed the YAML but views show old data.

**Cause:** `CREATE OR ALTER VIEW` should update, but check:
1. Are you running against the right SQL Server?
2. Check the story for errors
3. Verify the SQL file content

### Permission Denied

**Symptom:** `Cannot create schema` or `Cannot create view`

**Solution:**
```sql
-- Grant permissions to your service account
GRANT CREATE SCHEMA TO [your_user];
GRANT CREATE VIEW TO [your_user];
GRANT SELECT ON SCHEMA::dbo TO [your_user];  -- For source tables
```

### Stories Not Saving

**Symptom:** Run completes but no story files

**Check:**
1. Is `story.auto_generate: true` set?
2. Does the `story.connection` have write access?
3. Check logs for storage errors

---

## See Also

- [Defining Metrics](./metrics.md) - Metric and dimension definitions
- [Querying](./query.md) - Ad-hoc metric queries
- [Materializing](./materialize.md) - Pre-computing aggregates
- [Lineage Stitcher](./lineage_stitcher.md) - Combined lineage generation
- [Stories](../features/stories.md) - Pipeline execution stories
