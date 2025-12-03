# ⚡ Odibi Cheatsheet

## CLI Commands

| Command | Description |
| :--- | :--- |
| `odibi run odibi.yaml` | Run the pipeline. |
| `odibi run odibi.yaml --dry-run` | Validate connections without moving data. |
| `odibi doctor odibi.yaml` | Check environment and config health. |
| `odibi stress odibi.yaml` | Run fuzz tests (random data) to find bugs. |
| `odibi story view --latest` | Open the latest run report. |
| `odibi secrets init odibi.yaml` | Create .env template for secrets. |
| `odibi graph odibi.yaml` | Visualize pipeline dependencies. |
| `odibi generate-project` | Scaffold a new project from files. |

---

## `odibi.yaml` Structure

```yaml
version: 1
project: My Project
engine: pandas          # or 'spark'

connections:
  raw_data:
    type: local
    base_path: ./data

story:
  connection: raw_data
  path: stories/

pipelines:
  - pipeline: main_etl
    nodes:
      # 1. Read
      - name: load_csv
        read:
          connection: raw_data
          path: input.csv
          format: csv

      # 2. Transform (SQL)
      - name: clean_data
        depends_on: [load_csv]
        transform:
          steps:
            - "SELECT * FROM load_csv WHERE id IS NOT NULL"

      # 3. Transform (Python)
      - name: advanced_clean
        depends_on: [clean_data]
        transform:
          steps:
            - operation: my_custom_func  # defined in python
              params:
                threshold: 10

      # 4. Write
      - name: save_parquet
        depends_on: [advanced_clean]
        write:
          connection: raw_data
          path: output.parquet
          format: parquet
          mode: overwrite
```

---

## Python Transformation

```python
from odibi.transformations import transformation

@transformation("my_custom_func")
def my_func(df, threshold=10):
    """Docstrings are required!"""
    return df[df['val'] > threshold]
```

---

## Cross-Pipeline Dependencies

Reference outputs from other pipelines using `$pipeline.node` syntax:

```yaml
nodes:
  - name: enriched_data
    inputs:
      # Cross-pipeline reference
      events: $read_bronze.shift_events

      # Explicit read config
      calendar:
        connection: prod
        path: "bronze/calendar"
        format: delta
    transform:
      steps:
        - operation: join
          left: events
          right: calendar
          on: [date_id]
```

| Syntax | Example | Description |
| :--- | :--- | :--- |
| `$pipeline.node` | `$read_bronze.orders` | Reference node output from another pipeline |

**Requirements:**
- Referenced pipeline must have run first
- Referenced node must have a `write` block
- Cannot use both `read` and `inputs` in same node

---

## SQL Templates

| Variable | Value |
| :--- | :--- |
| `${source}` | The path of the source file (if reading). |
| `${SELF}` | The name of the current node. |
