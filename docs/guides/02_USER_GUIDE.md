# User Guide - Odibi Framework

**Complete guide for using Odibi in your daily work.**

---

## Table of Contents

1. [Configuration Files](#configuration-files)
2. [Built-in Operations](#built-in-operations)
3. [Running Pipelines](#running-pipelines)
4. [Working with Stories](#working-with-stories)
5. [Connections & Storage](#connections--storage)
6. [Best Practices](#best-practices)

---

## Configuration Files

### Basic Structure

Every Odibi config has 3 main sections:

```yaml
# 1. Project Metadata
project: Manufacturing Analytics
plant: NKC Plant
asset: Germ Dryer 1
business_unit: Operations

# 2. Connections
connections:
  local:
    type: local
    base_path: ./data

  azure_storage:
    type: azure_blob
    account_name: mystorageaccount
    container: data

# 3. Stories (where execution reports go)
story:
  connection: local
  path: stories/

# 4. Pipelines (your actual workflows)
pipelines:
  - pipeline: bronze_layer
    description: Ingest raw data
    layer: bronze
    nodes:
      - name: load_csv
        # ... node config ...
```

### Node Structure

Each node has:
- **name**: Unique identifier
- **description**: Human-readable explanation (optional but recommended)
- **depends_on**: List of nodes this depends on
- **One operation**: read, transform, or write

```yaml
- name: my_node
  description: What this node does
  depends_on: [previous_node]  # Optional
  read:        # OR transform OR write (pick one)
    # ...
```

---

## Built-in Operations

### 1. SQL Transformation

Most flexible - use DuckDB SQL on your data:

```yaml
- name: calculate_efficiency
  description: Calculate thermal efficiency metric
  depends_on: [load_data]
  transform:
    operation: sql
    query: |
      SELECT
        *,
        Output / Fuel AS Efficiency,
        CASE
          WHEN Efficiency > 0.8 THEN 'High'
          WHEN Efficiency > 0.6 THEN 'Medium'
          ELSE 'Low'
        END AS Performance
      FROM df
      WHERE Plant = 'NKC'
```

**When to use:**
- Filtering rows
- Creating calculated columns
- Aggregations
- Complex logic

### 2. Unpivot (Wide → Long)

Convert wide data to long format:

```yaml
# Input:
# | ID | Sales | Revenue |
# |----|-------|---------|
# | A  | 100   | 200     |

- name: unpivot_metrics
  transform:
    operation: unpivot
    id_vars: ID              # Columns to keep
    var_name: metric         # Name for metric column
    value_name: value        # Name for value column

# Output:
# | ID | metric  | value |
# |----|---------|-------|
# | A  | Sales   | 100   |
# | A  | Revenue | 200   |
```

**When to use:**
- Normalizing data
- Preparing for time-series analysis
- Converting Excel-style reports to database format

### 3. Pivot (Long → Wide)

Convert long data to wide format:

```yaml
# Input:
# | ID | metric  | value |
# |----|---------|-------|
# | A  | Temp    | 100   |
# | A  | Pressure| 200   |

- name: pivot_readings
  transform:
    operation: pivot
    group_by: ID                # Group by columns
    pivot_column: metric        # Column to spread
    value_column: value         # Values to fill
    agg_func: first             # Aggregation if needed

# Output:
# | ID | Temp | Pressure |
# |----|------|----------|
# | A  | 100  | 200      |
```

**When to use:**
- Creating summary reports
- Preparing data for Excel
- Dashboards and visualizations

### 4. Join (Combine Data)

Merge datasets together:

```yaml
- name: enrich_with_metadata
  depends_on: [load_data, load_lookup]
  transform:
    operation: join
    right_df: load_lookup      # Other dataset (from node name)
    on: ID                     # Column to join on
    how: left                  # inner, left, right, outer
```

**Join Types:**
- `inner`: Only matching records
- `left`: All left + matching right
- `right`: All right + matching left
- `outer`: All records from both

---

## Running Pipelines

### Basic Run

```bash
odibi run config.yaml
```

This will:
1. ✅ Load configuration
2. ✅ Validate all nodes
3. ✅ Build dependency graph
4. ✅ Execute nodes in order
5. ✅ Generate execution story
6. ✅ Save story to `stories/runs/`

### Environment Control

```bash
# Development (default)
odibi run config.yaml --env development

# Production
odibi run config.yaml --env production
```

### Validate Before Running

```bash
# Check config is valid without running
odibi validate config.yaml
```

---

## Working with Stories

### Automatic Run Stories

Every pipeline run creates a story in `stories/runs/`:

```
stories/
└── runs/
    ├── my_pipeline_20250110_143022.json   # Machine-readable
    ├── my_pipeline_20250110_143022.html   # Human-readable
    └── my_pipeline_20250110_143022.md     # Markdown version
```

**Open the HTML file** in your browser to see:
- ✅ Execution timeline
- 📊 Row count changes
- 🔄 Schema modifications
- ❌ Error details (if any)

### Generate Documentation Stories

Create stakeholder-ready documentation:

```bash
# Generate HTML documentation
odibi story generate config.yaml

# Outputs to: docs/my_pipeline_documentation.html
```

**Customize output:**

```bash
# Choose format
odibi story generate config.yaml --format markdown
odibi story generate config.yaml --format json

# Choose theme
odibi story generate config.yaml --theme corporate
odibi story generate config.yaml --theme dark

# Specify output path
odibi story generate config.yaml --output reports/pipeline_doc.html

# Skip validation (for draft docs)
odibi story generate config.yaml --no-validate
```

### Compare Pipeline Runs

See what changed between runs:

```bash
# Compare two runs
odibi story diff \
  stories/runs/pipeline_20250110_140000.json \
  stories/runs/pipeline_20250110_150000.json

# Get detailed node-level comparison
odibi story diff story1.json story2.json --detailed
```

**Output shows:**
- ⏱️ Execution time differences
- 📊 Row count changes
- ✅ Success rate comparison
- 🔍 Node-level details

### List Available Stories

```bash
# List recent stories
odibi story list

# List stories in custom directory
odibi story list --directory custom/path

# Show more results
odibi story list --limit 20
```

---

## Connections & Storage

### Local Files

```yaml
connections:
  local:
    type: local
    base_path: ./data
```

**Use in nodes:**
```yaml
read:
  connection: local
  path: input.csv        # Reads from ./data/input.csv
  format: csv
```

### Azure ADLS

```yaml
connections:
  bronze_storage:
    type: azure_adls
    account: mystorageaccount
    container: bronze
    auth_mode: key_vault      # Or 'direct_key'
    key_vault_name: myvault
    secret_name: storage-key
```

**Use in nodes:**
```yaml
read:
  connection: bronze_storage
  path: raw/data.parquet
  format: parquet
```

### Azure SQL

```yaml
connections:
  sql_db:
    type: azure_sql
    server: myserver.database.windows.net
    database: analytics
    auth_mode: aad_msi   # Or 'sql' with username/password
```

**Read from SQL:**
```yaml
read:
  connection: sql_db
  table: dbo.users
  query: "SELECT * FROM dbo.users WHERE active = 1"  # Optional
```

**Write to SQL:**
```yaml
write:
  connection: sql_db
  table: dbo.processed_data
  if_exists: append  # Or 'replace', 'fail'
```

---

## Best Practices

### 1. Always Add Descriptions

```yaml
# ❌ Bad
- name: node1
  read: ...

# ✅ Good
- name: load_customer_data
  description: Load customer records from CRM export
  read: ...
```

**Why?** Stories use descriptions to explain what happened.

### 2. Use Meaningful Node Names

```yaml
# ❌ Bad
- name: transform1
- name: transform2

# ✅ Good
- name: calculate_efficiency
- name: filter_outliers
- name: aggregate_by_plant
```

### 3. Keep Dependencies Clear

```yaml
# ✅ Explicit dependencies
- name: load_data
  read: ...

- name: clean_data
  depends_on: [load_data]  # Clear!
  transform: ...

- name: save_results
  depends_on: [clean_data]  # Clear chain
  write: ...
```

### 4. Use Layers for Organization

```yaml
pipelines:
  - pipeline: bronze_ingest
    layer: bronze  # Raw data

  - pipeline: silver_clean
    layer: silver  # Cleaned data

  - pipeline: gold_aggregate
    layer: gold  # Business-ready
```

### 5. Validate Often

```bash
# Before running
odibi validate config.yaml

# Catches:
# - Missing files
# - Invalid connections
# - Circular dependencies
# - Configuration errors
```

---

## Common Workflows

### ETL Pipeline (Extract-Transform-Load)

```yaml
nodes:
  # Extract
  - name: extract_from_source
    read:
      connection: source_db
      table: raw_data

  # Transform
  - name: clean_data
    depends_on: [extract_from_source]
    transform:
      operation: sql
      query: "SELECT * FROM df WHERE quality_flag = 'GOOD'"

  - name: calculate_metrics
    depends_on: [clean_data]
    transform:
      operation: sql
      query: |
        SELECT *,
          value / baseline AS performance_index
        FROM df

  # Load
  - name: load_to_warehouse
    depends_on: [calculate_metrics]
    write:
      connection: warehouse
      table: analytics.metrics
      if_exists: append
```

### Multi-Source Merge

```yaml
nodes:
  - name: load_sales
    read:
      connection: local
      path: sales.csv

  - name: load_customers
    read:
      connection: local
      path: customers.csv

  - name: merge_datasets
    depends_on: [load_sales, load_customers]
    transform:
      operation: join
      right_df: load_customers
      on: customer_id
      how: inner

  - name: save_enriched
    depends_on: [merge_datasets]
    write:
      connection: local
      path: sales_with_customer_info.parquet
```

### Data Quality Pipeline

```yaml
nodes:
  - name: load_raw
    read:
      connection: bronze
      path: raw_data.csv

  - name: remove_nulls
    depends_on: [load_raw]
    transform:
      operation: sql
      query: |
        SELECT * FROM df
        WHERE value IS NOT NULL
        AND timestamp IS NOT NULL

  - name: remove_duplicates
    depends_on: [remove_nulls]
    transform:
      operation: sql
      query: "SELECT DISTINCT * FROM df"

  - name: flag_outliers
    depends_on: [remove_duplicates]
    transform:
      operation: sql
      query: |
        SELECT *,
          CASE
            WHEN value > (SELECT AVG(value) + 3*STDDEV(value) FROM df) THEN 'outlier'
            WHEN value < (SELECT AVG(value) - 3*STDDEV(value) FROM df) THEN 'outlier'
            ELSE 'normal'
          END AS quality_flag
        FROM df

  - name: save_clean
    depends_on: [flag_outliers]
    write:
      connection: silver
      path: clean_data.parquet
```

---

## Tips & Tricks

### Tip 1: Use SQL for Everything Complex

Don't fight with Pandas syntax. Use SQL:

```yaml
# Instead of complex Pandas code, use SQL:
transform:
  operation: sql
  query: |
    SELECT
      plant,
      asset,
      DATE_TRUNC('day', timestamp) as date,
      AVG(temperature) as avg_temp,
      MAX(pressure) as max_pressure
    FROM df
    WHERE quality = 'good'
    GROUP BY plant, asset, date
    HAVING avg_temp > 100
```

### Tip 2: Check Stories After Each Run

Stories show you:
- What actually happened
- How many rows at each step
- Where errors occurred
- Performance metrics

### Tip 3: Use Themes for Different Audiences

```bash
# Technical team
odibi story generate config.yaml --theme default

# Executive presentation
odibi story generate config.yaml --theme corporate

# Dark mode for late-night debugging
odibi story generate config.yaml --theme dark
```

### Tip 4: Name Things Descriptively

Good names make stories self-explanatory:

```yaml
# ❌ Unclear
- name: step1
- name: step2

# ✅ Clear
- name: ingest_sensor_readings
- name: calculate_hourly_averages
- name: detect_anomalies
- name: export_to_dashboard
```

---

## File Formats

Odibi supports all common formats:

### Reading

```yaml
read:
  connection: local
  path: data.{format}
  format: csv|parquet|json|excel|avro|delta

  # Format-specific options:
  options:
    # CSV
    sep: ","
    encoding: utf-8

    # Excel
    sheet_name: Sheet1

    # Parquet
    columns: [ID, Name, Value]  # Read subset
```

### Writing

```yaml
write:
  connection: local
  path: output.{format}
  format: csv|parquet|json|excel|delta
  mode: overwrite|append  # Default: overwrite

  options:
    # CSV
    index: false

    # Parquet
    compression: snappy
```

---

## Advanced Features

### Conditional Execution

Use SQL to branch logic:

```yaml
- name: check_data_quality
  transform:
    operation: sql
    query: |
      SELECT
        COUNT(*) as total_rows,
        COUNT(*) FILTER (WHERE value IS NULL) as null_count
      FROM df

- name: process_if_good_quality
  depends_on: [check_data_quality]
  # Only processes if quality check passed
```

### Parameterized Queries

Use parameters for reusability:

```yaml
- name: filter_by_threshold
  transform:
    operation: sql
    query: "SELECT * FROM df WHERE value > 100"  # Threshold could be param
```

### Schema Evolution

Odibi tracks schema changes automatically:

```yaml
- name: add_calculated_fields
  transform:
    operation: sql
    query: |
      SELECT
        *,
        value * 1.1 AS adjusted_value,
        CURRENT_TIMESTAMP AS processed_at
      FROM df
```

**Story shows:**
- ➕ Added: `adjusted_value`
- ➕ Added: `processed_at`

---

## Working with Multiple Pipelines

```yaml
pipelines:
  # Bronze: Raw data ingestion
  - pipeline: bronze_ingest
    layer: bronze
    nodes:
      - name: load_from_source
        # ... extract raw data ...

  # Silver: Cleaned data
  - pipeline: silver_clean
    layer: silver
    nodes:
      - name: load_bronze
        read:
          connection: bronze_storage
          path: raw_data.parquet

      - name: clean_and_standardize
        # ... cleaning logic ...

  # Gold: Business-ready analytics
  - pipeline: gold_analytics
    layer: gold
    nodes:
      - name: load_silver
        read:
          connection: silver_storage
          path: clean_data.parquet

      - name: calculate_kpis
        # ... business logic ...
```

**Run specific pipeline:**
```bash
# Odibi runs first pipeline by default
odibi run config.yaml

# To run specific pipeline (future feature):
# odibi run config.yaml --pipeline gold_analytics
```

---

## Debugging Tips

### Use Stories to Debug

1. **Run pipeline**
2. **Open story HTML**
3. **Look for:**
   - ❌ Failed nodes
   - 📊 Unexpected row counts
   - 🔄 Missing columns
   - ⏱️ Slow nodes

### Check Row Counts

Stories show row counts at each step:

```
load_data:     1000 rows
filter_valid:   850 rows  (-150, -15%)
aggregate:       50 rows  (-800, -94%)
```

If unexpected, check the SQL query!

### Validate Configuration

```bash
# Always validate first
odibi validate config.yaml

# Common errors it catches:
# - Missing connections
# - Circular dependencies
# - Invalid file paths
# - Typos in operation names
```

### Use Story Diff

```bash
# Compare working version vs broken version
odibi story diff working_run.json broken_run.json --detailed
```

Shows exactly which node changed behavior!

---

## Performance Tips

### 1. Use Parquet for Large Data

```yaml
# Slow (CSV)
path: large_data.csv

# Fast (Parquet)
path: large_data.parquet
```

Parquet is:
- 5-10x smaller
- 10-100x faster to read
- Preserves types

### 2. Filter Early

```yaml
# ❌ Bad: Filter after loading everything
- name: load_all
  read: # Loads 10M rows

- name: filter
  transform:
    query: "SELECT * FROM df WHERE plant = 'NKC'"  # Down to 100K rows

# ✅ Good: Filter during read (if possible)
- name: load_filtered
  read:
    query: "SELECT * FROM table WHERE plant = 'NKC'"  # Only loads 100K
```

### 3. Use Chunks for SQL Writes

```yaml
write:
  connection: sql_db
  table: large_output
  options:
    chunksize: 1000  # Write 1000 rows at a time
```

---

## Next Steps

**You now know how to use Odibi!**

Continue learning:
- **[Transformation Guide](05_TRANSFORMATION_GUIDE.md)** - Write custom transformations
- **[Developer Guide](03_DEVELOPER_GUIDE.md)** - Understand internals
- **[Architecture Guide](04_ARCHITECTURE_GUIDE.md)** - See how it all works
- **[Troubleshooting](06_TROUBLESHOOTING.md)** - Common issues & solutions

---

**Questions?** Check the other guides or look at examples in `examples/` directory.
