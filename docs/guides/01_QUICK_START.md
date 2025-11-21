# Quick Start Guide - Odibi Framework

**Get up and running in 5 minutes!**

---

## Installation

```bash
# Basic installation (Pandas only)
pip install odibi

# With Azure + Spark support
pip install "odibi[azure,spark]"
```

---

## Your First Pipeline (3 steps)

### Step 1: Create a Config File

Create `config.yaml`:

```yaml
project: My First Project
plant: Local Testing

connections:
  local:
    type: local
    base_path: ./data

story:
  connection: local
  path: stories/

pipelines:
  - pipeline: simple_etl
    description: My first Odibi pipeline
    nodes:
      - name: load_data
        description: Load CSV file
        read:
          connection: local
          path: input.csv
          format: csv

      - name: save_data
        description: Save as Parquet
        depends_on: [load_data]
        write:
          connection: local
          path: output.parquet
          format: parquet
```

### Step 2: Create Sample Data

Create `data/input.csv`:

```csv
ID,Name,Value
1,Alice,100
2,Bob,200
3,Charlie,150
```

### Step 3: Run It!

```bash
# Run the pipeline
odibi run config.yaml

# Check your results
ls data/output.parquet
ls stories/runs/  # Auto-generated story!
```

**That's it!** Your first pipeline is complete. ✅

---

## What Just Happened?

1. **Odibi loaded** your config
2. **Read** `input.csv`
3. **Wrote** `output.parquet`
4. **Generated** a markdown story in `stories/` (see `path` in config)

Check the story to see:
- Execution timeline
- Row counts
- Schema changes
- Success/failure status

---

## Next: Add a Transformation

Let's add data filtering. Update your config:

```yaml
pipelines:
  - pipeline: simple_etl
    nodes:
      - name: load_data
        read:
          connection: local
          path: input.csv
          format: csv

      - name: filter_high_values  # NEW!
        description: Keep only high-value records
        depends_on: [load_data]
        transform:
          steps:
            - "SELECT * FROM load_data WHERE Value > 150"

      - name: save_data
        depends_on: [filter_high_values]  # Changed dependency
        write:
          connection: local
          path: output.parquet
          format: parquet
```

Run again:

```bash
odibi run config.yaml
```

Check the story - you'll see:
- **3 nodes** executed
- **Row count change** (3 rows → 1 row)
- **SQL query** in the details

---

## Using Built-in Operations

Odibi includes 4 powerful reshaping operations:

### 1. **Unpivot** (Wide → Long Format)

```yaml
- name: unpivot_metrics
  transform:
    operation: unpivot
    id_vars: ID
    var_name: metric
    value_name: reading
```

### 2. **Pivot** (Long → Wide Format)

```yaml
- name: pivot_metrics
  transform:
    operation: pivot
    group_by: ID
    pivot_column: metric
    value_column: reading
```

### 3. **Join** (Combine Datasets)

```yaml
- name: enrich_data
  transform:
    operation: join
    right_df: lookup_table  # From another node
    on: ID
    how: left
```

### 4. **SQL** (Any Transformation)

```yaml
- name: calculate
  transform:
    operation: sql
    query: |
      SELECT
        *,
        Output / Fuel AS Efficiency
      FROM df
      WHERE Plant = 'NKC'
```

---

## Generate Documentation

Create stakeholder-ready docs:

```bash
# Generate HTML documentation
odibi story generate config.yaml

# Use dark theme
odibi story generate config.yaml --theme dark

# Output as Markdown
odibi story generate config.yaml --format markdown
```

Open `docs/simple_etl_documentation.html` in your browser. Beautiful! 🎨

---

## Compare Pipeline Runs

Made changes? Compare before/after:

```bash
# Run pipeline twice (edit config between runs)
odibi run config.yaml  # Creates stories/runs/simple_etl_20250110_143022.json
# ... make changes ...
odibi run config.yaml  # Creates stories/runs/simple_etl_20250110_144530.json

# Compare them
odibi story diff \
  stories/runs/simple_etl_20250110_143022.json \
  stories/runs/simple_etl_20250110_144530.json \
  --detailed
```

See exactly what changed:
- Execution time differences
- Row count changes
- Node-level comparisons

---

## Next Steps

**You now know the basics!** Here's what to explore next:

1. **[User Guide](02_USER_GUIDE.md)** - Learn all features in detail
2. **[Transformation Guide](05_TRANSFORMATION_GUIDE.md)** - Write custom transformations
3. **[Developer Guide](03_DEVELOPER_GUIDE.md)** - Understand how Odibi works internally
4. **[Architecture Guide](04_ARCHITECTURE_GUIDE.md)** - See the big picture

---

## Common Commands Cheat Sheet

```bash
# Pipelines
odibi run config.yaml                    # Run pipeline
odibi validate config.yaml               # Validate config

# Stories
odibi story generate config.yaml        # Create docs
odibi story list                         # List all stories
odibi story diff story1.json story2.json # Compare runs

# Themes
odibi story generate config.yaml --theme corporate
odibi story generate config.yaml --theme dark
odibi story generate config.yaml --theme minimal
```

---

## Help & Support

- **Stuck?** Check [06_TROUBLESHOOTING.md](06_TROUBLESHOOTING.md)
- **Want examples?** See `examples/` directory
- **Need API details?** Run `python -c "from odibi import transformation; help(transformation)"`

---

**Welcome to Odibi!** 🚀

You're ready to build self-documenting, transparent data pipelines.
