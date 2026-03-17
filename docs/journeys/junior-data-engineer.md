# Junior Data Engineer Learning Journey

---

## 📌 Who Is This For?

**New grads, bootcamp graduates, and career switchers** who need to:
- Run and debug data pipelines
- Copy and adapt working configurations
- Choose the right engine (Pandas vs Spark)
- Fix common errors independently

**Prerequisites: Basic Python and SQL knowledge**

---

## ⏱️ Time to Complete

**4-6 hours** (can be broken into 1-hour sessions)

---

## 🎯 Learning Outcomes

By the end of this journey, you will be able to:

✅ Run a complete pipeline from CSV to Parquet to Delta  
✅ Adapt canonical examples for your own data  
✅ Choose between Pandas, Polars, and Spark engines  
✅ Debug pipelines using `odibi doctor`, `validate`, and `graph`  
✅ Add data quality validation gates  
✅ Understand incremental loading patterns  
✅ Read and modify YAML configs confidently  

---

## 📋 Prerequisites

- **Python 3.9+** installed
- **Terminal/command line** basics (cd, ls, pip)
- **SQL** familiarity (SELECT, WHERE, JOIN)
- **Git** (optional but helpful)

---

## 📚 Learning Modules

### Module 1: Your First Pipeline (45 min)

#### 📖 Read
- [Installation Guide](../guides/installation.md) - Install Odibi
- [Golden Path](../golden_path.md) - Read Steps 1-5

#### 👀 Watch
- Video: "Zero to First Story in 7 Minutes" ← **Coming soon**

#### ✋ Do
Run the Golden Path end-to-end:

```bash
# 1. Install
pip install odibi

# 2. Verify installation
odibi --version
odibi list transformers

# 3. Create project (Option A - recommended)
odibi init my_first_project --template star-schema
cd my_first_project

# 4. Run the pipeline
odibi run odibi.yaml

# 5. View the Data Story
odibi story list
odibi story last
```

#### ✅ Verify
- [ ] Pipeline ran without errors
- [ ] You see `dim_customer`, `dim_product`, `dim_date`, `fact_sales` in `data/gold/`
- [ ] Data Story HTML opened in your browser
- [ ] You can find the row counts in the Story

**Troubleshooting:**
- If `odibi init` fails, see [Installation Guide](../guides/installation.md#troubleshooting-installation)
- If pipeline fails, run `odibi doctor` for diagnostics

---

### Module 2: Engine Decision Tree (30 min)

#### 📖 Read
- [Decision Guide](../guides/decision_guide.md) - "Choose Your Engine" section
- [Engine Parity Rule](../philosophy.md#engine-parity-rule)

#### 👀 Visual
```
Data size?
├─► < 1GB        → engine: pandas (default)
├─► 1-10GB       → engine: polars (fast local)
└─► > 10GB       → engine: spark (distributed)
    └─► Delta Lake → engine: spark (required)
```

#### ✋ Do
Edit `my_first_project/odibi.yaml` and add this line at the top:

```yaml
version: 1
engine: pandas  # Try: pandas, polars, spark
project: my_first_project
```

Run with different engines:

```bash
# 1. Pandas (default - already ran this)
odibi run odibi.yaml

# 2. Polars (install first: pip install "odibi[polars]")
# Edit odibi.yaml: engine: polars
odibi run odibi.yaml

# 3. Compare Stories
odibi story list
```

Questions:
1. Did both engines produce the same row counts?
2. Which was faster for this small dataset?
3. Which Story shows more detailed execution logs?

#### ✅ Verify
- [ ] You understand when to use Pandas vs Spark
- [ ] You successfully changed the `engine` setting
- [ ] You know that Pandas is best for local development

---

### Module 3: Canonical Examples Deep Dive (60 min)

#### 📖 Read
- [Canonical Examples Overview](../examples/canonical/README.md)
- [Example 1: Hello World](../examples/canonical/01_hello_world.md)
- [Example 2: Incremental SQL](../examples/canonical/02_incremental_sql.md)

#### ✋ Do
**Exercise 1: Run Hello World**

```bash
cd docs/examples/canonical/runnable
odibi run 01_hello_world.yaml
ls data/output/  # See the parquet file
```

**Exercise 2: Modify Hello World for Your Data**

1. Create `my_data.csv`:
```csv
id,product,price,category
1,Widget,10.99,Tools
2,Gadget,25.50,Electronics
3,Doohickey,5.00,Tools
```

2. Copy and modify:
```bash
cp 01_hello_world.yaml my_hello_world.yaml
```

3. Edit `my_hello_world.yaml`:
```yaml
# Change path to your CSV
read:
  path: my_data.csv  # Instead of sample.csv

# Change output path
write:
  path: my_output  # Instead of output
```

4. Run it:
```bash
odibi run my_hello_world.yaml
```

**Exercise 3: Add a Transformation**

Add a filter step to keep only Tools:

```yaml
transform:
  steps:
    - "SELECT * FROM load_data WHERE category = 'Tools'"
```

Re-run and verify only 2 rows in output.

#### ✅ Verify
- [ ] You adapted Example 1 for your own CSV
- [ ] You added a SQL transformation successfully
- [ ] Output parquet file has the filtered data

---

### Module 4: Debugging Toolkit (45 min)

#### 📖 Read
- [CLI Master Guide](../guides/cli_master_guide.md) - Focus on `doctor`, `validate`, `graph`
- [Troubleshooting Guide](../troubleshooting.md)

#### 👀 Watch
- Video: "Debug with doctor, validate, graph" ← **Coming soon**

#### ✋ Do
**Exercise 1: Validate a Config**

```bash
# Validate before running
odibi validate my_hello_world.yaml

# Look for:
# - ✅ YAML syntax is valid
# - ✅ All connections are defined
# - ✅ No circular dependencies
```

**Exercise 2: Visualize the DAG**

```bash
odibi graph my_hello_world.yaml
# Opens a visual dependency graph
```

**Exercise 3: Diagnose Issues**

```bash
odibi doctor

# Checks:
# - Python version
# - Required packages
# - Connection accessibility
# - File permissions
```

**Exercise 4: Intentionally Break Something**

Break your config and practice debugging:

```yaml
# Break 1: Typo in connection name
read:
  connection: raw_dataa  # Extra 'a'
```

Run `odibi validate` - what error do you get?

```yaml
# Break 2: Circular dependency
nodes:
  - name: node_a
    depends_on: [node_b]
  - name: node_b
    depends_on: [node_a]
```

Run `odibi validate` - what error do you get?

#### ✅ Verify
- [ ] You can validate configs before running
- [ ] You can visualize pipeline DAGs
- [ ] You can diagnose errors using `doctor`
- [ ] You understand common error messages

---

### Module 5: Data Quality Gates (60 min)

#### 📖 Read
- [Quality Gates](../features/quality_gates.md)
- [Validation Tests](../validation/tests.md)
- [Contracts](../validation/contracts.md)

#### 👀 Visual
```
Read → [Contract Check] → Transform → [Validation Check] → Write
         ↓ fail early         ↓ verify output
```

#### ✋ Do
**Exercise 1: Add Validation Tests**

Edit `my_hello_world.yaml` and add validation:

```yaml
nodes:
  - name: load_data
    read:
      connection: local
      path: my_data.csv
      format: csv
    
    # Add validation BEFORE write
    validation:
      tests:
        - type: not_null
          columns: [id, product]
        - type: unique
          columns: [id]
        - type: row_count
          min: 1
      gate:
        on_fail: abort  # Stop if validation fails
    
    write:
      connection: local
      path: my_output
      format: parquet
```

Run and check the Story for validation results.

**Exercise 2: Test Failure Behavior**

1. Change `on_fail: warn_and_write`
2. Break the data (add duplicate IDs to `my_data.csv`)
3. Re-run - pipeline completes but logs warnings

**Exercise 3: Add Contracts**

```yaml
nodes:
  - name: load_data
    # Add contracts BEFORE read
    contracts:
      - type: not_null
        columns: [id, product, price]
      - type: accepted_values
        column: category
        values: [Tools, Electronics, Home]
    
    read:
      ...
```

Add a row with `category: BadValue` and watch the contract fail immediately.

#### ✅ Verify
- [ ] You added validation tests successfully
- [ ] You understand `fail` vs `warn` behavior
- [ ] You know the difference between contracts (input) and validation (output)
- [ ] Validation results appear in the Data Story

---

### Module 6: Patterns Overview (30 min)

#### 📖 Read
- [Patterns Overview](../patterns/README.md)
- [Merge/Upsert Pattern](../patterns/merge_upsert.md)
- [Incremental Stateful Pattern](../patterns/incremental_stateful.md)

#### ✋ Do
Pick ONE pattern that interests you and wire it into Example 5:

**Option A: Add Merge Logic**

```yaml
transformer: merge
params:
  target:
    connection: gold
    path: existing_table
  keys: [id]
  strategy: upsert  # insert new, update existing
```

**Option B: Add Incremental Loading**

```yaml
read:
  connection: source
  path: transactions.csv
  incremental:
    mode: stateful
    column: updated_at
```

Run the pipeline twice and verify:
- First run: loads all data
- Second run: skips unchanged data (check Story for "rows skipped")

#### ✅ Verify
- [ ] You understand what problems patterns solve
- [ ] You successfully applied one pattern
- [ ] You can explain merge vs append vs overwrite

---

## 🏆 Capstone Project

**Build a Complete Pipeline from Scratch**

### Requirements

Create a pipeline that:
1. Reads data from a CSV
2. Filters bad rows
3. Adds validation gates
4. Handles failures gracefully (quarantine or warn)
5. Writes to Parquet
6. Generates a Data Story

### Starter Data

Create `sales_data.csv`:
```csv
order_id,customer_id,product_id,amount,order_date
1,101,501,150.00,2025-01-01
2,102,502,25.50,2025-01-02
3,103,,99.99,2025-01-03
4,104,504,-10.00,invalid-date
5,105,505,1000.00,2025-01-05
```

(Notice: Row 3 has NULL `product_id`, Row 4 has invalid date and negative amount)

### Your Pipeline Must:

1. **Filter out** negative amounts
2. **Validate**:
   - `order_id` is unique
   - `customer_id` is not null
   - `amount > 0`
3. **Handle failures**: Use gate `on_fail: warn_and_write` or test `on_fail: warn` and document WHY in YAML comments
4. **Generate a Story** with explanation

### Verification Script

Run this after your pipeline:

```bash
python examples/verify/verify_capstone_jr_de.py
```

Expected output:
```
✅ Output file exists
✅ Row count correct (3 valid rows)
✅ No negative amounts
✅ All customer_ids are populated
✅ Story generated
✅ Validation section exists in Story
```

#### ✅ Verify
- [ ] Your pipeline runs without errors
- [ ] Verify script passes all checks
- [ ] Data Story clearly shows validation results
- [ ] You added comments explaining your choices

---

## ➡️ Next Steps

You've completed the Junior DE journey! Here's where to go next:

### Deepen Your Skills
- [CLI Master Guide](../guides/cli_master_guide.md) - Learn advanced CLI commands and debugging
- [Writing Transformations](../guides/writing_transformations.md) - Custom Python functions
- [Dimensional Modeling Tutorial](../tutorials/dimensional_modeling/01_introduction.md) - Star schemas

### Production Readiness
- [Spark Engine Tutorial](../tutorials/spark_engine.md) - Scale to big data
- [Azure Connections](../tutorials/azure_connections.md) - Connect to cloud storage
- [Performance Tuning](../guides/performance_tuning.md) - Optimize pipelines

### Level Up
- [Sr Data Engineer Journey](senior-data-engineer.md) - Design production systems

---

## 📣 Share Your Success

Completed the capstone? **We want to celebrate with you!**

- Share your Data Story in [GitHub Discussions](https://github.com/henryodibi11/Odibi/discussions)
- Tag us on LinkedIn with #OdibiJourney
- Add "Odibi Junior Data Engineer" to your resume!

---

**Questions?** [Troubleshooting Guide](../troubleshooting.md) | [FAQ](../guides/faq.md)
