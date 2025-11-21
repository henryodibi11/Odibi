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

## SQL Templates

| Variable | Value |
| :--- | :--- |
| `${source}` | The path of the source file (if reading). |
| `${SELF}` | The name of the current node. |
