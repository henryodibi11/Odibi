# ODIBI Quick Reference

**Cheat sheet for common patterns.**

---

## Basic Pipeline Structure

```yaml
pipeline: my_pipeline
description: "What this pipeline does"

nodes:
  - name: node1
    read: ...
  
  - name: node2
    depends_on: [node1]
    transform: ...
  
  - name: node3
    depends_on: [node2]
    write: ...
```

---

## Read Operations

### CSV
```yaml
read:
  connection: local_data
  format: csv
  path: data.csv
  options:
    delimiter: ","
    encoding: utf-8
```

### Parquet
```yaml
read:
  connection: local_data
  format: parquet
  path: data.parquet
```

### JSON
```yaml
read:
  connection: local_data
  format: json
  path: data.json
  options:
    orient: records
```

---

## Write Operations

### Overwrite
```yaml
write:
  connection: local_output
  format: csv
  path: output.csv
  mode: overwrite
```

### Append
```yaml
write:
  connection: local_output
  format: csv
  path: output.csv
  mode: append
```

---

## Transform Operations

### SQL Transform
```yaml
transform:
  steps:
    - "SELECT * FROM my_node WHERE amount > 100"
    - "SELECT id, SUM(amount) as total FROM __previous__ GROUP BY id"
```

### Python Function Transform
```yaml
transform:
  steps:
    - function: my_transform
      params:
        source: my_node
        threshold: 100
```

### Mixed Transforms
```yaml
transform:
  steps:
    - "SELECT * FROM raw_data"
    - function: clean_data
      params:
        source: raw_data
    - "SELECT * FROM clean_data WHERE valid = true"
```

---

## Transform Functions

### Basic Pattern
```python
from odibi import transform

@transform
def my_transform(context, source: str, param: int = 10):
    \"\"\"Transform description.\"\"\"
    df = context.get(source)
    # ... your logic
    return result_df
```

### With Multiple Sources
```python
@transform
def join_data(context, left_table: str, right_table: str, on: str):
    \"\"\"Join two tables.\"\"\"
    left = context.get(left_table)
    right = context.get(right_table)
    return left.merge(right, on=on)
```

### With Filtering
```python
@transform
def filter_data(context, source: str, column: str, min_value: float):
    \"\"\"Filter rows by threshold.\"\"\"
    df = context.get(source)
    return df[df[column] >= min_value]
```

---

## Dependencies

### Linear
```yaml
nodes:
  - name: A
  - name: B
    depends_on: [A]
  - name: C
    depends_on: [B]

# Execution order: A → B → C
```

### Parallel
```yaml
nodes:
  - name: A
  - name: B  # No dependencies
  - name: C
    depends_on: [A, B]  # Waits for both

# Layer 1: A, B (parallel)
# Layer 2: C
```

### Diamond
```yaml
nodes:
  - name: root
  - name: branch1
    depends_on: [root]
  - name: branch2
    depends_on: [root]
  - name: merge
    depends_on: [branch1, branch2]

# Layer 1: root
# Layer 2: branch1, branch2 (parallel)
# Layer 3: merge
```

---

## Python API

### Load and Run
```python
from odibi.pipeline import Pipeline
from odibi.config import PipelineConfig
from odibi.connections import LocalConnection
import yaml

# Load config
with open('pipelines/my_pipeline.yaml') as f:
    config_data = yaml.safe_load(f)

pipeline_config = PipelineConfig(**config_data)

# Setup connections
connections = {
    "local": LocalConnection(base_path="./data")
}

# Create pipeline
pipeline = Pipeline(pipeline_config, connections=connections)

# Run
results = pipeline.run()
```

### Check Results
```python
print(f"Completed: {results.completed}")
print(f"Failed: {results.failed}")
print(f"Skipped: {results.skipped}")
print(f"Duration: {results.duration}s")
```

### Debug Single Node
```python
import pandas as pd

result = pipeline.run_node(
    "my_node",
    mock_data={
        "input": pd.DataFrame({"a": [1, 2, 3]})
    }
)

print(result.success)
output = pipeline.context.get("my_node")
```

### Validate Without Running
```python
validation = pipeline.validate()

if validation['valid']:
    print("✅ Config is valid")
    print(f"Execution order: {validation['execution_order']}")
else:
    print("❌ Errors found:")
    for error in validation['errors']:
        print(f"  - {error}")
```

---

## Connections

### Local Filesystem
```yaml
connections:
  local:
    type: local
    base_path: ./data
```

```python
LocalConnection(base_path="./data")
```

### Multiple Connections
```yaml
connections:
  input_data:
    type: local
    base_path: ./input
  
  output_data:
    type: local
    base_path: ./output
```

---

## Common Patterns

### ETL Pattern
```yaml
nodes:
  - name: extract
    read: {connection: source, format: csv, path: data.csv}
  
  - name: transform
    depends_on: [extract]
    transform:
      steps:
        - function: clean_data
          params: {source: extract}
  
  - name: load
    depends_on: [transform]
    write: {connection: target, format: parquet, path: output.parquet}
```

### Multi-Source Join Pattern
```yaml
nodes:
  - name: load_fact
    read: ...
  
  - name: load_dim
    read: ...
  
  - name: join
    depends_on: [load_fact, load_dim]
    transform:
      steps:
        - function: join_tables
          params:
            fact: load_fact
            dimension: load_dim
```

### Split Pattern
```yaml
nodes:
  - name: load_all
    read: ...
  
  - name: filter_electronics
    depends_on: [load_all]
    transform:
      steps:
        - function: filter_by_category
          params: {source: load_all, category: "Electronics"}
  
  - name: filter_home
    depends_on: [load_all]
    transform:
      steps:
        - function: filter_by_category
          params: {source: load_all, category: "Home"}
```

---

## Error Handling

### Config Validation Errors
```python
try:
    pipeline_config = PipelineConfig(**yaml_data)
except ValidationError as e:
    print(f"Config error: {e}")
    # Shows exactly what's wrong in YAML
```

### Runtime Errors
```python
results = pipeline.run()

if results.failed:
    for node_name in results.failed:
        node_result = results.get_node_result(node_name)
        print(f"Failed: {node_name}")
        print(f"Error: {node_result.error}")
```

### Debugging Failed Nodes
```python
# Run the failed node in isolation with mock data
result = pipeline.run_node(
    "failed_node_name",
    mock_data={"dependency": test_df}
)
```

---

## Tips & Tricks

### 1. Cache Expensive Reads
```yaml
- name: reference_data
  read: ...
  cache: true  # Used by many downstream nodes
```

### 2. Use SQL for Simple Transforms
```yaml
transform:
  steps:
    - "SELECT * FROM sales WHERE amount > 100"
```

Faster than writing a Python function!

### 3. Name Nodes Descriptively
```yaml
# ❌ Bad
- name: node1

# ✅ Good
- name: load_sales_2024
- name: enrich_with_customer_data
- name: save_to_warehouse
```

### 4. Test Transforms Separately
```python
# In a notebook
from transforms import my_transform
from odibi.context import PandasContext

ctx = PandasContext()
ctx.register("test_data", df)

result = my_transform(ctx, source="test_data", param=10)
```

### 5. Validate Before Running
```python
validation = pipeline.validate()

if not validation['valid']:
    print("Fix these errors first:")
    for error in validation['errors']:
        print(f"  - {error}")
else:
    results = pipeline.run()
```

---

## Need More Help?

1. **Check the examples** - `examples/getting_started/`
2. **Read the docs** - `docs/ODIBI_FRAMEWORK_PLAN.md`
3. **Explore tests** - `test_exploration_phase2.ipynb`
4. **Review code** - Start with `context.py`

---

**Happy pipelining!** 🚀
