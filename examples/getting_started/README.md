# ODIBI Getting Started Example

**Learn ODIBI in 30 minutes!**

This example teaches you everything you need to build data pipelines with ODIBI.

---

## 📁 What's Included

```
getting_started/
├── walkthrough.ipynb          # 👈 START HERE - Interactive tutorial
├── transforms.py              # Sample transform functions
├── project.yaml               # Project configuration
├── pipelines/
│   ├── simple.yaml           # Basic read → write
│   ├── transform.yaml        # With transforms
│   └── advanced.yaml         # SQL, joins, aggregation
├── data/
│   ├── sales.csv             # Sample sales data
│   └── customers.csv         # Sample customer data
└── output/                   # Generated outputs (created when you run)
```

---

## 🚀 Quick Start

### 1. Install ODIBI

```bash
cd d:/odibi
pip install -e .
```

### 2. Open the Walkthrough

```bash
cd examples/getting_started
jupyter notebook walkthrough.ipynb
```

### 3. Run All Cells!

The notebook will teach you:
- ✅ Basic pipelines (read/write)
- ✅ Transform functions
- ✅ SQL transforms
- ✅ Multi-source joins
- ✅ Debugging techniques
- ✅ Error handling

---

## 📚 What You'll Learn

### Pipeline 1: Simple ETL
**File:** `pipelines/simple.yaml`

Load CSV → Save Parquet

```yaml
nodes:
  - name: load_sales
    read:
      connection: local_data
      format: csv
      path: sales.csv
  
  - name: save_parquet
    depends_on: [load_sales]
    write:
      connection: local_output
      format: parquet
      path: sales.parquet
```

---

### Pipeline 2: With Transforms
**File:** `pipelines/transform.yaml`

Load → Calculate Revenue → Filter → Save

Uses custom Python functions:
```python
@transform
def calculate_revenue(context, source: str):
    df = context.get(source)
    df['revenue'] = df['quantity'] * df['price']
    return df
```

---

### Pipeline 3: Advanced
**File:** `pipelines/advanced.yaml`

Features:
- Multiple data sources (sales + customers)
- SQL transforms
- Joins
- Aggregation
- Parallel outputs

---

## 🎯 Key Concepts

### 1. YAML Configs
Define pipelines declaratively:
```yaml
pipeline: my_etl
nodes:
  - name: step1
    read: ...
  - name: step2
    depends_on: [step1]
    transform: ...
```

### 2. Dependencies
Explicit `depends_on`:
```yaml
- name: join_data
  depends_on: [load_sales, load_customers]  # Wait for both
```

### 3. Transform Functions
Type-safe Python functions:
```python
@transform
def my_transform(context, param: str, threshold: float = 0.5):
    df = context.get(param)
    # ... transform logic
    return result
```

### 4. Context
Data passing between nodes:
```python
# Node 1 registers
context.register("sales", df)

# Node 2 retrieves
df = context.get("sales")
```

---

## 📖 Example Outputs

After running the pipelines, you'll have:

```
output/
├── sales.parquet              # From simple.yaml
├── electronics_sales.csv      # From transform.yaml
├── enriched_sales.csv         # From advanced.yaml
└── product_summary.csv        # From advanced.yaml
```

---

## 🐛 Debugging Tips

### Run Single Node
```python
result = pipeline.run_node(
    "my_node",
    mock_data={"input": test_df}
)
```

### Validate Before Running
```python
validation = pipeline.validate()
print(validation['errors'])
```

### Inspect Context
```python
# See what's registered
pipeline.context.list_names()

# Get intermediate result
df = pipeline.context.get("node_name")
```

---

## 🎓 Next Steps

After completing this tutorial:

1. **Build your own pipeline**
   - Use your own data
   - Write custom transforms
   - Try different formats

2. **Explore the framework**
   - Read: `docs/ODIBI_FRAMEWORK_PLAN.md`
   - Tests: `test_exploration_phase2.ipynb`
   - Code: Start with `context.py`, then `node.py`

3. **Contribute**
   - Fix issues in `docs/IMPROVEMENTS.md`
   - Add new features
   - Write more examples!

---

## ❓ Common Issues

### Import Error
```
ModuleNotFoundError: No module named 'odibi'
```
**Fix:** Install in editable mode: `pip install -e d:/odibi`

### DuckDB Not Found
```
SQL execution requires 'duckdb'
```
**Fix:** Install DuckDB: `pip install duckdb`

### Path Issues
Make sure you're in the `examples/getting_started` directory when running the notebook.

---

## 📝 Files Explained

### `transforms.py`
Sample transform functions. Shows how to:
- Calculate derived columns
- Filter data
- Join DataFrames
- Aggregate

### `project.yaml`
Project-level config. Defines:
- Connections (where data lives)
- Global settings
- Defaults

### `pipelines/*.yaml`
Pipeline definitions. Each shows different features.

### `data/*.csv`
Sample data for learning. Replace with your own!

---

**Happy learning!** 🎉

Open `walkthrough.ipynb` and start coding!
