# Senior Data Engineer Learning Journey

---

## 📌 Who Is This For?

**Experienced data engineers and analytics engineers** who need to:
- Design medallion architecture (Bronze → Silver → Gold)
- Choose optimal incremental loading strategies
- Enforce data contracts and quality gates
- Productionize pipelines with alerts and monitoring
- Write custom transformations and patterns
- Optimize performance for production workloads

**Prerequisites: Strong SQL, Python, and Spark/Polars experience**

---

## ⏱️ Time to Complete

**1-2 days** (can be broken into 2-4 hour sessions)

---

## 🎯 Learning Outcomes

By the end of this journey, you will be able to:

✅ Design end-to-end medallion pipelines (Bronze → Silver → Gold)  
✅ Choose between stateful HWM, rolling window, and CDC patterns  
✅ Implement SCD2, merge, and fact patterns correctly  
✅ Enforce data contracts and validation gates  
✅ Write production-grade custom transformations  
✅ Deploy with alerts, monitoring, and error handling  
✅ Optimize pipeline performance (partitioning, Z-ordering, caching)  
✅ Debug complex issues using Odibi's introspection tools  

---

## 📋 Prerequisites

- **SQL** mastery (window functions, CTEs, joins)
- **Python** proficiency (classes, decorators, type hints)
- **Spark or Polars** hands-on experience
- **Data warehousing** concepts (dimensions, facts, SCD)
- **Git** and **CI/CD** familiarity

---

## 📚 Learning Modules

### Module 1: Odibi Architecture Mental Model (45 min)

#### 📖 Read
- [The Definitive Guide](../guides/the_definitive_guide.md) - Sections 1-2
- [Architecture Deep Dive](../explanation/architecture.md)
- [Engine Parity Rule](../philosophy.md#engine-parity-rule)

#### 👀 Visual
Open [Odibi in One Picture](../visuals/odibi_architecture.md) - System diagram

#### ✋ Do
**Exercise: Trace a Request**

Given this YAML:
```yaml
pipelines:
  - pipeline: example
    nodes:
      - name: raw_orders
        read: {connection: source, path: orders.csv}
        write: {connection: bronze, path: orders}
      
      - name: clean_orders
        depends_on: [raw_orders]
        transform:
          steps:
            - "SELECT * FROM raw_orders WHERE amount > 0"
        write: {connection: silver, path: orders}
```

Trace the execution flow:
1. YAML → which Pydantic model?
2. Dependency resolution → what's the order?
3. `raw_orders` → which engine method?
4. `clean_orders` → where does it get the DataFrame?
5. Story generation → when does it happen?

#### ✅ Verify
- [ ] You understand the 3-layer architecture (YAML → Object Model → Engine)
- [ ] You know Pydantic validates before execution
- [ ] You can explain the Global Context and temp view pattern
- [ ] You understand why pandas and spark produce identical results

**Deep Dive:**
Read the source code:
- `odibi/config.py` - See Pydantic models
- `odibi/engine/pandas_engine.py` - See engine implementation
- `odibi/pipeline.py` - See orchestration logic

---

### Module 2: Incremental Loading Strategy (90 min)

#### 📖 Read
- [Incremental Stateful Pattern](../patterns/incremental_stateful.md)
- [Windowed Reprocess Pattern](../patterns/windowed_reprocess.md)
- [Smart Read Pattern](../patterns/smart_read.md)
- [Decision Guide - Incremental Modes](../guides/decision_guide.md#choose-your-incremental-mode)

#### 👀 Visual
[Incremental Decision Tree](../visuals/incremental_decision_tree.md)

#### ✋ Do
**Exercise 1: Stateful High-Water Mark**

Create `stateful_example.yaml`:

```yaml
project: stateful_demo
engine: pandas

connections:
  source:
    type: local
    base_path: ./data/source
  bronze:
    type: local
    base_path: ./data/bronze

system:
  connection: bronze
  path: _system

pipelines:
  - pipeline: ingest_events
    nodes:
      - name: load_events
        read:
          connection: source
          path: events.csv
          format: csv
          options:
            incremental:
              mode: stateful
              column: event_timestamp
              initial_value: "1970-01-01"
        write:
          connection: bronze
          format: parquet
          path: events
          mode: append
```

Create sample data with timestamps:
```csv
event_id,event_timestamp,value
1,2025-01-01 10:00:00,100
2,2025-01-02 11:00:00,200
3,2025-01-03 12:00:00,300
```

Run twice:
1. First run → loads all 3 rows
2. Add row 4 with `2025-01-04`
3. Second run → loads only row 4

Verify by reading the state:
```bash
odibi catalog state stateful_example.yaml
```

**Exercise 2: Rolling Window (No Timestamps)**

What if your source has no timestamps? Use rolling window:

```yaml
read:
  options:
    incremental:
      mode: rolling_window
      lookback_days: 7
```

**Exercise 3: Skip If Unchanged**

For static files that rarely change:

```yaml
write:
  skip_if_unchanged: true  # Compare hash, skip if identical
```

Run twice with same data - second run skips write.

#### ✅ Verify
- [ ] You implemented stateful HWM successfully
- [ ] You understand when to use rolling window vs stateful
- [ ] You can read and interpret the system catalog state
- [ ] You know `skip_if_unchanged` uses content hashing

**Decision Framework:**
```
Source has reliable timestamps?
├─► Yes → mode: stateful (exact HWM)
└─► No
    ├─► Immutable files → skip_if_unchanged: true
    └─► Daily batches → mode: rolling_window (lookback_days: 1)
```

---

### Module 3: SCD2, Merge, and Fact Patterns Deep Dive (2 hours)

#### 📖 Read
- [SCD2 Pattern](../patterns/scd2.md) - Complete guide
- [Merge/Upsert Pattern](../patterns/merge_upsert.md)
- [Fact Pattern](../patterns/fact.md)
- [Anti-Patterns](../patterns/anti_patterns.md) - Learn what NOT to do

#### 👀 Watch
- Video: "SCD2 Pitfalls in 10 Minutes" ← **Coming soon**

#### ✋ Do
**Exercise 1: Build a Proper SCD2 Dimension**

```yaml
nodes:
  - name: dim_customer
    read:
      connection: bronze
      table: raw_customers
    
    pattern:
      type: dimension
      params:
        natural_key: customer_id
        surrogate_key: customer_sk
        scd_type: 2
        track_cols: [name, email, city, tier]
        effective_from_col: valid_from
        effective_to_col: valid_to
        current_flag_col: is_current
        deleted_flag_col: is_deleted
        unknown_member: true  # Creates SK=0 for orphans
    
    write:
      connection: silver
      format: delta  # Spark engine required
      table: dim_customer
      mode: overwrite
```

Test SCD2 behavior:
1. Run with initial customer data
2. Change `city` for customer_id=1
3. Re-run and verify:
   - Old row: `is_current=False`, `valid_to` is set
   - New row: `is_current=True`, `valid_from` is today

**Exercise 2: Fact Table with SK Lookups**

```yaml
nodes:
  - name: fact_orders
    depends_on: [dim_customer, dim_product, dim_date]
    
    read:
      connection: bronze
      table: raw_orders
    
    pattern:
      type: fact
      params:
        grain: [order_id, line_item_id]
        dimensions:
          - source_column: customer_id
            dimension_table: dim_customer
            dimension_key: customer_id
            surrogate_key: customer_sk
          
          - source_column: product_id
            dimension_table: dim_product
            dimension_key: product_id
            surrogate_key: product_sk
          
          - source_column: order_date
            dimension_table: dim_date
            dimension_key: date_id
            surrogate_key: date_sk
        
        orphan_handling: unknown  # orphans get SK=0
        measures: [quantity, amount, discount]
        degenerate_dimensions: [order_number, invoice_id]
    
    write:
      connection: gold
      table: fact_orders
      mode: append
```

**Exercise 3: Merge Pattern**

For slowly changing tables without full history:

```yaml
transformer:
  transformer: merge
  params:
    target:
      connection: silver
      table: products
    keys: [product_id]
    strategy: upsert  # or: append, delete
    when_matched: update_all  # or: update_changed_only
    when_not_matched: insert
```

#### ✅ Verify
- [ ] SCD2 creates versioned rows correctly
- [ ] Fact pattern performs SK lookups
- [ ] Orphan records get `unknown_member_sk = 0`
- [ ] Merge performs upserts without duplicates
- [ ] You understand grain validation in fact tables

**Common Pitfalls:**
- ❌ Forgetting `unknown_member: true` → orphans cause NULLs
- ❌ Wrong `grain` definition → duplicate facts
- ❌ Running fact before dimensions → SK lookups fail

---

### Module 4: Production Deployment (90 min)

#### 📖 Read
- [Production Deployment Guide](../guides/production_deployment.md)
- [Alerting](../features/alerting.md)
- [Secrets Management](../guides/secrets.md)
- [Performance Tuning](../guides/performance_tuning.md)

#### ✋ Do
**Exercise 1: Add Slack Alerts**

```yaml
alerts:
  - type: slack
    url: "${SLACK_WEBHOOK_URL}"  # From .env
    on_events: [on_failure, on_success]
    metadata:
      environment: production
      owner: data-team
```

**Exercise 2: Secrets Management**

Create `.env`:
```
DB_HOST=prod-db.example.com
DB_PASSWORD=super_secret_password
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/XXX
```

Reference in YAML:
```yaml
connections:
  warehouse:
    type: sql_server
    host: "${DB_HOST}"
    auth:
      mode: sql_login
      password: "${DB_PASSWORD}"  # Auto-redacted in logs
```

Run:
```bash
odibi secrets init odibi.yaml  # Generates .env template
odibi run odibi.yaml --env prod
```

**Exercise 3: Performance Tuning**

For Spark engine with Delta Lake:

```yaml
write:
  format: delta
  options:
    partitionBy: [year, month]
    optimize_write: true
    zorder_by: [customer_id]
    vacuum_hours: 168  # Retain 7 days of history
```

#### ✅ Verify
- [ ] Alerts fire on success and failure
- [ ] Secrets are redacted in logs and Stories
- [ ] You understand Delta Lake optimization (Z-order, partitioning)
- [ ] You can manage multiple environments (dev, prod)

---

### Module 5: Custom Transformations (90 min)

#### 📖 Read
- [Writing Transformations](../guides/writing_transformations.md)
- [Custom Functions Reference](../guides/custom_functions.md)
- [The Definitive Guide - Section 5](../guides/the_definitive_guide.md#5-transformation-engineering)

#### ✋ Do
**Exercise 1: Write a Custom Transformer**

Create `transforms.py`:

```python
from odibi.registry import transform
from odibi.context import EngineContext

@transform
def calculate_customer_lifetime_value(context: EngineContext, params: dict):
    """
    Calculates customer lifetime value (CLV) using RFM analysis.
    
    Args:
        params:
            reference_date: Date to calculate from (default: today)
            min_purchase_count: Minimum purchases to be considered (default: 2)
    
    Returns:
        DataFrame with CLV metrics
    """
    reference_date = params.get("reference_date", "CURRENT_DATE")
    min_purchases = params.get("min_purchase_count", 2)
    
    # Use SQL for engine-agnostic logic
    query = f"""
    WITH customer_metrics AS (
        SELECT
            customer_id,
            COUNT(*) as purchase_count,
            SUM(amount) as total_spent,
            MAX(order_date) as last_order_date,
            DATEDIFF(day, MAX(order_date), {reference_date}) as recency_days,
            AVG(amount) as avg_order_value
        FROM df
        GROUP BY customer_id
        HAVING COUNT(*) >= {min_purchases}
    )
    SELECT
        *,
        total_spent / purchase_count as clv,
        CASE
            WHEN recency_days <= 30 THEN 'Active'
            WHEN recency_days <= 90 THEN 'At Risk'
            ELSE 'Churned'
        END as customer_status
    FROM customer_metrics
    """
    
    return context.sql(query).df
```

Use in YAML:

```yaml
transform:
  steps:
    - operation: calculate_customer_lifetime_value
      params:
        reference_date: "2025-12-31"
        min_purchase_count: 3
```

**Exercise 2: Add Docstring Documentation**

Your docstring automatically appears in:
- `odibi explain calculate_customer_lifetime_value`
- Data Stories (when function is used)
- Auto-generated API docs

**Exercise 3: Engine-Specific Optimization**

```python
@transform
def complex_aggregation(context: EngineContext, params: dict):
    """Optimizes differently for Pandas vs Spark."""
    
    if context.engine_type == "pandas":
        # Use pandas-native for small data
        df = context.df
        return df.groupby("category").agg({"value": ["sum", "mean", "std"]})
    
    else:  # spark or polars
        # Use SQL for distributed processing
        return context.sql("""
            SELECT
                category,
                SUM(value) as total,
                AVG(value) as mean,
                STDDEV(value) as std
            FROM df
            GROUP BY category
        """).df
```

#### ✅ Verify
- [ ] Custom transformer runs successfully
- [ ] Docstring appears in `odibi explain`
- [ ] SQL-based logic works on both Pandas and Spark
- [ ] You understand when to use engine-specific optimization

---

### Module 6: Comprehensive Debugging (60 min)

#### 📖 Read
- [Troubleshooting Guide](../troubleshooting.md)
- [CLI Master Guide - Diagnostics](../guides/cli_master_guide.md#diagnostics)
- [Features: Diagnostics](../features/diagnostics.md)

#### ✋ Do
**Exercise 1: Pre-flight Checks**

```bash
# Validate YAML schema and logic
odibi validate odibi.yaml

# Check environment health
odibi doctor

# Visualize dependency graph
odibi graph odibi.yaml

# Dry-run (no writes)
odibi run odibi.yaml --dry-run
```

**Exercise 2: Debugging Failed Pipelines**

Intentionally break a pipeline:

```yaml
nodes:
  - name: broken_node
    read:
      connection: missing_connection  # Doesn't exist
      path: nonexistent.csv
    transform:
      steps:
        - "SELECT * FROM undefined_table"  # Wrong table name
```

Run and observe errors:
```bash
odibi run broken.yaml 2>&1 | tee error.log
```

Use diagnostics:
```bash
odibi doctor
# Shows: missing_connection is not defined
```

**Exercise 3: Performance Profiling**

Enable detailed timing:

```yaml
system:
  connection: local
  path: _system
  log_level: DEBUG  # Detailed execution logs
```

Read execution times in Story or logs.

#### ✅ Verify
- [ ] You can validate configs before running
- [ ] `doctor` catches missing connections/dependencies
- [ ] `graph` helps visualize complex DAGs
- [ ] You understand how to read error traces

---

## 🏆 Capstone Project

**Build a Production-Grade Medallion Pipeline**

### Requirements

Design and implement a complete Bronze → Silver → Gold pipeline with:

1. **Bronze Layer:**
   - Ingest from 2+ sources (CSV + API or SQL)
   - Stateful incremental loading (HWM)
   - Append-only writes (immutable)
   - Contracts for freshness and completeness

2. **Silver Layer:**
   - SCD2 dimension with history tracking
   - Deduplicated and cleaned data
   - Merge/upsert pattern for updates
   - Validation gates with quarantine

3. **Gold Layer:**
   - Fact table with SK lookups
   - Pre-aggregated metrics (daily/monthly rollups)
   - FK validation between fact and dimensions
   - Unknown member handling

4. **Production Readiness:**
   - Slack alerts on failure
   - Secrets in .env
   - Comprehensive Data Story with explanations
   - Custom transformation with business logic

5. **Performance:**
   - Partitioning strategy (if using Spark)
   - Z-ordering on high-cardinality columns
   - Caching for shared dimensions

### Starter Scenario: E-Commerce Pipeline

**Sources:**
- `customers.csv` - Customer master data (SCD2)
- `products.json` - Product catalog (dimension)
- Orders API - `/api/orders` (incremental)

**Outputs:**
- `silver.dim_customer` - SCD2 dimension
- `silver.dim_product` - Type 1 dimension
- `silver.dim_date` - Generated dimension
- `gold.fact_orders` - Order line items with SK lookups
- `gold.agg_daily_sales` - Daily revenue rollups

### Verification

Run the verification script:

```bash
python examples/verify/verify_capstone_sr_de.py
```

Checks:
- ✅ All 5 tables exist
- ✅ SCD2 has version history
- ✅ Fact table has valid SK references
- ✅ Aggregation matches fact grain
- ✅ Story includes explanations
- ✅ Alerts configured
- ✅ Incremental state persisted

### Submission

Share your:
1. Full YAML config (with comments)
2. Custom transformation code
3. Data Story HTML
4. Brief architecture doc (why you chose each pattern)

#### ✅ Verify
- [ ] All verification checks pass
- [ ] Story clearly documents pipeline logic
- [ ] Code is production-ready (error handling, logging)
- [ ] You can explain design decisions (SCD2 vs SCD1, stateful vs rolling window)

---

## ➡️ Next Steps

You've completed the Senior DE journey! Here's where to level up further:

### Advanced Topics
- [Semantic Layer Guide](../semantics/index.md) - Build metrics on top of your warehouse
- [Lineage Tracking](../features/lineage.md) - OpenLineage integration
- [Schema Evolution](../features/schema_tracking.md) - Handle breaking changes
- [Testing Pipelines](../guides/testing.md) - Unit and integration tests

### Platform Engineering
- [Orchestration](../features/orchestration.md) - Airflow/Prefect integration
- [CI/CD for Data Pipelines](../guides/production_deployment.md)
- [Databricks Best Practices](../guides/production_deployment.md)

### Contribute Back
- [CONTRIBUTING.md](https://github.com/henryodibi11/odibi/blob/main/CONTRIBUTING.md) - Help improve Odibi
- Share your patterns in GitHub Discussions
- Write a case study of your production pipeline

### Mentor Others
- Help juniors through their journey
- Review pull requests
- Share your Data Stories as teaching examples

---

## 🎓 Certification (Coming Soon)

**Odibi Professional Certification**

- Peer-reviewed capstone project
- Design review with core maintainers
- Public badge and certificate
- Listed in Certified Professionals directory

*Coming soon*

---

## 📣 Share Your Success

Completed the capstone? **You're now an Odibi expert!**

- Share your architecture in [GitHub Discussions](https://github.com/henryodibi11/Odibi/discussions)
- Write a blog post about your journey
- Tag us on LinkedIn with #OdibiExpert
- Add "Odibi Senior Data Engineer - Certified" to your resume!

---

**Questions?** [Troubleshooting Guide](../troubleshooting.md) | [FAQ](../guides/faq.md) | [Office Hours](https://github.com/henryodibi11/Odibi/discussions/categories/office-hours)
