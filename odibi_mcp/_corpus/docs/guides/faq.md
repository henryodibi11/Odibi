# Frequently Asked Questions (FAQ)

> Quick answers to common questions. Organized by topic.

---

## 📋 Table of Contents

- [Getting Started](#getting-started)
- [Engines & Performance](#engines-performance)
- [Patterns & Transformers](#patterns-transformers)
- [Data Quality & Validation](#data-quality-validation)
- [Incremental Loading](#incremental-loading)
- [Production & Deployment](#production-deployment)
- [Troubleshooting](#troubleshooting)

---

## Getting Started

### Q: What is Odibi?

**A:** Odibi is a declarative data pipeline framework. You define **what** you want (in YAML), and Odibi handles **how** to execute it on Pandas, Polars, or Spark.

**Not a scheduler** (use Airflow). **Not a BI tool** (use Tableau). It's the layer in between: data transformation and quality.

---

### Q: Do I need to know Spark to use Odibi?

**A:** No. Start with the default Pandas engine locally. When your data grows (> 1GB), switch to `engine: spark` in one line. Same YAML, zero code changes.

---

### Q: How is Odibi different from dbt?

| Feature | Odibi | dbt |
|---------|-------|-----|
| **Language** | YAML + Python | SQL + Jinja |
| **Engines** | Pandas, Polars, Spark | SQL warehouses (Snowflake, BigQuery, etc.) |
| **Patterns** | Built-in (SCD2, fact, merge) | Custom macros |
| **Incremental** | Stateful HWM, rolling window | dbt incremental models |
| **Best For** | Lakehouse (Parquet, Delta, files) | Cloud warehouses (Snowflake, BigQuery) |

**Use both:** dbt for warehouse transformations, Odibi for file-based lakehouses and complex Python logic.

---

### Q: Can I use Odibi with Databricks?

**A:** Yes! Odibi was designed for Databricks. Set `engine: spark` and use Delta Lake connections.

```yaml
engine: spark

connections:
  datalake:
    type: delta
    catalog: main
    schema: silver
```

---

## Engines & Performance

### Q: When should I use Pandas vs Spark?

```
Data size?
├─► < 1GB        → engine: pandas (default, fast iteration)
├─► 1-10GB       → engine: polars (high-performance local)
└─► > 10GB       → engine: spark (distributed)
    └─► Delta Lake → engine: spark (required)
```

**Rule of thumb:** Develop with Pandas, deploy with Spark.

---

### Q: Why is my pipeline slow?

**Common causes:**

1. **Reading full tables every time** → Use incremental loading
2. **No partitioning** (Spark/Delta) → Add `partitionBy: [date]`
3. **Small files** (Spark) → Enable `optimize_write: true`
4. **Unnecessary transforms** → Profile with `log_level: DEBUG`

**Quick wins:**

```yaml
# Partition large tables
write:
  options:
    partitionBy: [year, month]
    optimize_write: true

# Cache shared dimensions
nodes:
  - name: dim_customer
    cache: true  # Reused by multiple fact tables
```

See [Performance Tuning Guide](performance_tuning.md).

---

### Q: Can I mix engines in one pipeline?

**A:** No. One pipeline = one engine. But you can have multiple YAML files with different engines and orchestrate them externally (Airflow).

---

## Patterns & Transformers

### Q: SCD2 vs snapshots — when to use which?

| Approach | Use When | Storage | Query Complexity |
|----------|----------|---------|------------------|
| **SCD2** | Need exact change history | Efficient (only changed rows) | Easy (current: `WHERE is_current = TRUE`) |
| **Snapshots** | Need daily/weekly point-in-time | Large (full copy each period) | Moderate (join on snapshot_date) |

**Recommendation:** SCD2 for slowly changing dimensions (customer address changes). Snapshots for daily balances (account snapshots).

---

### Q: How do I choose between `merge` and `scd2` transformers?

| Feature | `merge` | `scd2` |
|---------|---------|--------|
| **History** | No (upsert only) | Yes (versioned rows) |
| **Use Case** | Latest state (product catalog) | Track changes (customer address) |
| **Columns Added** | None | `is_current`, `valid_from`, `valid_to`, `is_deleted` |
| **Complexity** | Simple | Moderate |

**Example:**

```yaml
# Merge (no history)
transformer:
  transformer: merge
  params:
    target: silver.products
    keys: [product_id]
    strategy: upsert

# SCD2 (with history)
pattern:
  type: dimension
  params:
    natural_key: customer_id
    scd_type: 2
    track_cols: [name, email, city]
```

---

### Q: What's the difference between a `transformer` and a `pattern`?

- **Transformer:** Low-level operation (deduplicate, filter, join, hash)
- **Pattern:** High-level workflow (dimension, fact, merge, aggregation)

**Patterns** often use multiple **transformers** internally.

```yaml
# Transformer (explicit steps)
transformer:
  transformer: deduplicate
  params:
    keys: [id]

# Pattern (handles entire workflow)
pattern:
  type: fact
  params:
    grain: [order_id]
    dimensions: [...]
```

---

## Data Quality & Validation

### Q: Contracts vs Validation Tests — what's the difference?

```
Contracts (Before)           Validation (After)
      ↓                             ↓
   [Source] → [Transform] → [Validate] → [Write]
      ↓                             ↓
   "Is this safe       "Is the output
    to process?"        what we expect?"
```

**Contracts:**
- Check **input** data before processing
- Fail fast (save compute)
- Example: "Is source data fresh? Are required columns present?"

**Validation Tests:**
- Check **output** data after transformation
- Verify transformations worked correctly
- Example: "Are all IDs unique? Are amounts positive?"

---

### Q: When should I use `fail` vs `warn` vs `quarantine`?

| Mode | Behavior | Use When |
|------|----------|----------|
| `fail` | Stop pipeline immediately | Critical data (financial transactions, compliance) |
| `warn` | Log warning, continue | Nice-to-have quality (optional fields) |
| `quarantine` | Route bad rows to separate path, continue | Dirty data expected (user input, external APIs) |

**Example:**

```yaml
validation:
  tests:
    - type: not_null
      columns: [transaction_id]  # Critical
  gate:
    on_fail: abort  # Must stop

# vs

validation:
  tests:
    - type: not_null
      columns: [middle_name]  # Optional
  gate:
    on_fail: warn_and_write  # Log but continue

# vs

validation:
  tests:
    - type: not_null
      columns: [transaction_id]
      on_fail: quarantine
  quarantine:
    connection: silver
    path: quarantine/transactions
```

---

### Q: How do I validate foreign keys between fact and dimension?

Use the `fact` pattern with FK validation:

```yaml
pattern:
  type: fact
  params:
    grain: [order_id]
    dimensions:
      - source_column: customer_id
        dimension_table: dim_customer
        dimension_key: customer_id
        surrogate_key: customer_sk
    orphan_handling: unknown  # orphans → SK=0
```

Or use the FK validation module for post-pipeline auditing:

```python
from odibi.validation.fk import FKValidator, RelationshipConfig, RelationshipRegistry

registry = RelationshipRegistry(relationships=[
    RelationshipConfig(
        name="orders_to_customers",
        fact="fact_orders",
        dimension="dim_customer",
        fact_key="customer_sk",
        dimension_key="customer_sk",
        on_violation="error"
    )
])
validator = FKValidator(registry)
report = validator.validate_fact(fact_df, "fact_orders", context)
```

See [FK Validation Guide](../tutorials/dimensional_modeling/13_fk_validation.md).

---

## Incremental Loading

### Q: What's a "high-water mark" (HWM)?

**A:** The last timestamp/value successfully loaded. On the next run, Odibi reads only rows **after** the HWM.

**Example:**

```
Run 1: Load all data up to 2025-01-10 14:30:00
       HWM = 2025-01-10 14:30:00

Run 2: Load WHERE timestamp > '2025-01-10 14:30:00'
       (Only new data since last run)
```

Stored in the System Catalog.

---

### Q: My incremental load is missing data. Why?

**Common causes:**

1. **Late-arriving data:** Data with old timestamps arrives after HWM is set
   - **Fix:** Use `rolling_window` with lookback instead of `stateful`

2. **Timezone issues:** Timestamps in different zones
   - **Fix:** Normalize to UTC before comparing

3. **HWM not updating:** State not persisted
   - **Fix:** Ensure `system:` is configured in YAML

4. **Source has no timestamps:** Can't track HWM
   - **Fix:** Use `rolling_window` or `skip_if_unchanged`

---

### Q: Should I use `mode: overwrite` or `mode: append` for incremental?

| Mode | Use When | Behavior |
|------|----------|----------|
| `overwrite` | Silver layer (full refresh) | Replaces entire table |
| `append` | Bronze layer (immutable raw) | Adds new rows only |
| `upsert` | Silver/Gold (merge) | Insert new, update existing |

**Incremental pattern:**

```
Bronze (append) → Silver (upsert) → Gold (overwrite aggregations)
```

---

## Production & Deployment

### Q: How do I manage secrets (passwords, API keys)?

Use environment variables + `.env` file:

**1. Create `.env`:**
```bash
DB_PASSWORD=super_secret
API_KEY=abc123
SLACK_WEBHOOK=https://hooks.slack.com/...
```

**2. Reference in YAML:**
```yaml
connections:
  warehouse:
    type: sql_server
    host: "${DB_HOST}"
    auth:
      mode: sql_login
      password: "${DB_PASSWORD}"  # Auto-redacted in logs

alerts:
  - type: slack
    url: "${SLACK_WEBHOOK}"
```

**3. Generate template:**
```bash
odibi secrets init odibi.yaml
# Creates .env.template with all required vars
```

**4. Add `.env` to `.gitignore`:**
```
.env
.env.local
```

See [Secrets Management Guide](secrets.md).

---

### Q: How do I run Odibi in Airflow?

**Option 1: BashOperator**

```python
from airflow.operators.bash import BashOperator

run_odibi = BashOperator(
    task_id="run_odibi_pipeline",
    bash_command="odibi run /path/to/odibi.yaml",
    env={"ENV": "production"}
)
```

**Option 2: PythonOperator**

```python
from airflow.operators.python import PythonOperator
from odibi.cli import run_pipeline

def run_odibi():
    run_pipeline(config_path="odibi.yaml", env="production")

task = PythonOperator(
    task_id="run_odibi",
    python_callable=run_odibi
)
```

**Option 3: Databricks Job** (if using Spark)

```python
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

odibi_job = DatabricksSubmitRunOperator(
    task_id="odibi_pipeline",
    json={
        "spark_python_task": {
            "python_file": "dbfs:/pipelines/run_odibi.py",
            "parameters": ["--config", "odibi.yaml"]
        }
    }
)
```

---

### Q: How do I handle multiple environments (dev, staging, prod)?

**Approach 1: Environment Variables**

```yaml
connections:
  warehouse:
    host: "${DB_HOST}"  # dev: localhost, prod: prod-db.example.com
    database: "${DB_NAME}"  # dev: dev_db, prod: prod_db
```

Run with:
```bash
odibi run odibi.yaml --env dev
odibi run odibi.yaml --env prod
```

**Approach 2: Multiple YAML Files**

```
configs/
├── odibi.yaml              # Shared base
├── odibi.dev.yaml          # Dev overrides
├── odibi.prod.yaml         # Prod overrides
```

```bash
odibi run configs/odibi.dev.yaml
odibi run configs/odibi.prod.yaml
```

See [Environments Guide](environments.md).

---

## Troubleshooting

### Q: `ModuleNotFoundError: No module named 'odibi'`

**A:** Odibi not installed or virtual environment not activated.

```bash
# Activate venv
source .venv/bin/activate  # Mac/Linux
.venv\Scripts\activate     # Windows

# Install
pip install odibi

# Verify
odibi --version
```

---

### Q: `ConnectionNotFoundError: 'my_connection' not defined`

**A:** Connection referenced in `read`/`write` but not defined in `connections:` section.

**Fix:**

```yaml
connections:
  my_connection:  # Must match name in read/write
    type: local
    base_path: ./data
```

---

### Q: Pipeline runs but no output files

**Common causes:**

1. **Dry-run mode enabled:**
   ```bash
   # Remove --dry-run
   odibi run odibi.yaml
   ```

2. **Write path doesn't exist:**
   ```bash
   mkdir -p data/output
   ```

3. **Permissions denied:**
   ```bash
   chmod 755 data/output
   ```

---

### Q: `CyclicDependencyError: Circular dependency detected`

**A:** Node A depends on B, which depends on A (directly or indirectly).

**Example of circular dependency:**

```yaml
nodes:
  - name: node_a
    depends_on: [node_b]
  
  - name: node_b
    depends_on: [node_a]  # ❌ Circular!
```

**Fix:** Remove or rearrange dependencies. Use `odibi graph odibi.yaml` to visualize.

---

### Q: Validation passes locally but fails in production

**Common causes:**

1. **Data drift:** Production data has different characteristics
   - **Fix:** Review Data Story, adjust validation thresholds

2. **Timezone differences:** Local is PST, prod is UTC
   - **Fix:** Normalize timestamps to UTC

3. **Missing `.env` in production:**
   - **Fix:** Ensure all environment variables are set

---

### Q: How do I reset incremental state (force full reload)?

To force a full reload, manually clear the stored state:

1. **Delete the node's state entry** from the state JSON file (or delete the entire state file to reset all nodes).
2. **Optionally**, update the `initial_value` in your YAML config to set a new starting point.
3. **Re-run the pipeline:**

```bash
odibi run odibi.yaml
```

---

## Still Have Questions?

- **Troubleshooting Guide:** [troubleshooting.md](../troubleshooting.md)
- **GitHub Issues:** [Open an issue](https://github.com/henryodibi11/Odibi/issues)
- **Discussions:** [Ask the community](https://github.com/henryodibi11/Odibi/discussions)
- **Office Hours:** [Join monthly Q&A](https://github.com/henryodibi11/Odibi/discussions/categories/office-hours)

---

[← Back to Guides](../README.md)
