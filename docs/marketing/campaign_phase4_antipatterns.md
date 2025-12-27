# LinkedIn Campaign - Phase 4: Anti-Patterns & Troubleshooting

**Duration:** Weeks 17-22 (18 posts)
**Goal:** Build credibility by sharing mistakes and solutions
**Tone:** Humble, "learned the hard way," practical fixes

---

## Week 17: Bronze Layer Anti-Patterns

### Post 17.1 - Don't Transform in Bronze (Monday)

**Hook:**
I lost 6 months of data because I "cleaned" it during ingest.

**Body:**
The mistake:
```yaml
# BAD - transforming in Bronze
- name: bronze_orders
  read:
    path: orders.csv
  transform:
    steps:
      - sql: "SELECT * WHERE status != 'canceled'"
  write:
    connection: bronze
```

I filtered out canceled orders. "We don't need those."

6 months later: "Can you analyze canceled orders?"

Me: "They're... gone."

The lesson:
Bronze layer has ONE job: preserve raw data exactly as received.

```yaml
# GOOD - no transformation
- name: bronze_orders
  read:
    path: orders.csv
  write:
    connection: bronze
    mode: append
```

Transform in Silver. Filter in Silver. Clean in Silver.

Bronze is your undo button. Don't break it.

**CTA:** Have you made this mistake?

---

### Post 17.2 - Missing Extraction Timestamps (Wednesday)

**Hook:**
"When did this data arrive?" "I... don't know."

**Body:**
The mistake:
```yaml
# BAD - no metadata
- name: bronze_orders
  read:
    path: orders.csv
  write:
    connection: bronze
```

The problem:
- Can't tell when rows were loaded
- Can't debug timing issues
- Can't identify duplicate loads

The fix:
```yaml
# GOOD - add metadata
- name: bronze_orders
  read:
    path: orders.csv
  transform:
    steps:
      - function: derive_columns
        params:
          columns:
            _extracted_at: "current_timestamp()"
            _source_file: "'orders.csv'"
  write:
    connection: bronze
```

Now every row has:
- When it arrived
- Where it came from

Debugging time: minutes instead of hours.

I wrote about the 3 Bronze layer mistakes that will haunt you on Medium. Link in comments.

**CTA:** Link to Article 17

---

### Post 17.3 - Append Without Deduplication (Friday)

**Hook:**
Pipeline failed. I reran it. Revenue doubled overnight.

**Body:**
The mistake:
```yaml
# BAD - blind append
mode: append
```

What happened:
1. Pipeline loaded 100k rows
2. Failed at a later step
3. I reran the whole pipeline
4. Another 100k rows appended
5. Now I have duplicates

Dashboard showed 2x revenue. Finance panicked.

The fix:
```yaml
# GOOD - deduplicate downstream
# Bronze: append (keep everything)
bronze:
  mode: append

# Silver: deduplicate
silver:
  transformer: deduplicate
  params:
    keys: [order_id]
    order_by: "_extracted_at DESC"
```

Or use merge mode for idempotency:
```yaml
# Alternative - merge is idempotent
transformer: merge
params:
  keys: [order_id]
```

Rerunning a merge gives the same result.

**CTA:** Make pipelines rerunnable

---

## Week 18: Silver Layer Anti-Patterns

### Post 18.1 - Business Logic Everywhere (Monday)

**Hook:**
"Why is revenue $1,100?" "Check Bronze. No wait, check Silver. Actually..."

**Body:**
The mistake: Business logic scattered across layers.

Bronze:
```yaml
# Applied 8% discount here
transform:
  - sql: "SELECT *, amount * 0.92 as net"
```

Silver:
```yaml
# Added 10% markup here
transform:
  - sql: "SELECT *, net * 1.1 as projected"
```

Gold:
```yaml
# More calculations here
transform:
  - sql: "SELECT *, projected * tax_rate as final"
```

Debugging = archaeology.

The fix: Centralize logic in ONE place.

```yaml
# GOOD - all business logic in Silver
silver_orders:
  transform:
    steps:
      - sql: |
          SELECT *,
            amount * 0.92 as net_amount,
            amount * 0.92 * 1.1 as projected,
            amount * 0.92 * 1.1 * 1.08 as final_with_tax
          FROM df
```

One place to check. One place to fix.

**CTA:** Centralize business logic

---

### Post 18.2 - Skipping Silver Layer (Wednesday)

**Hook:**
"Let's just go Bronze → Gold. Fewer steps."

**Body:**
The temptation:
```yaml
# BAD - skipping Silver
gold_report:
  read:
    connection: bronze
    path: raw_orders
  transform:
    - deduplicate
    - clean
    - validate
    - aggregate
```

The problems:
1. Every Gold table repeats cleaning logic
2. Cleaning done 5 different ways
3. No single source of truth
4. Reports don't match

The fix:
```yaml
# GOOD - Silver is the source of truth
silver_orders:
  read:
    connection: bronze
  transform:
    - deduplicate
    - clean
    - validate
  write:
    connection: silver

gold_report_a:
  read:
    connection: silver
    path: orders
  # Already clean!

gold_report_b:
  read:
    connection: silver
    path: orders
  # Same source, same numbers
```

Silver = cleaned once, used everywhere.

Full guide on Silver layer best practices on Medium. Link in comments.

**CTA:** Link to Article 18

---

### Post 18.3 - Ignoring NULL Keys (Friday)

**Hook:**
1000 orders. 900 in the report. "Where did 100 go?"

**Body:**
The mistake:
```yaml
# Orders with NULL customer_id
# Joined to dim_customer on customer_id
# NULL != NULL in SQL
# 100 orders dropped silently
```

NULL values in join keys = silent data loss.

The fix:
```yaml
# GOOD - handle NULLs explicitly
transform:
  steps:
    - function: fill_nulls
      params:
        columns: [customer_id]
        value: "UNKNOWN"
```

Or use the fact pattern with unknown handling:
```yaml
pattern:
  type: fact
  params:
    orphan_handling: unknown
```

Rows with NULL/invalid keys → customer_sk = 0 (unknown member).

All 1000 orders in the report. 100 flagged as "unknown customer."

Visible problems are fixable. Silent problems are dangerous.

**CTA:** NULL = silent killer

---

## Week 19: SCD2 Anti-Patterns

### Post 19.1 - SCD2 Without Deduplication (Monday)

**Hook:**
dim_customer: 10,000 rows. After SCD2: 10,000,000 rows.

**Body:**
The mistake:
Source had duplicates:
```
customer_id | name  | updated_at
101         | Alice | 2024-01-01 08:00:00
101         | Alice | 2024-01-01 08:00:01
101         | Alice | 2024-01-01 08:00:02
```

SCD2 saw 3 "versions" → created 3 history rows.

Every customer × every duplicate = explosion.

The fix:
```yaml
# GOOD - dedupe before SCD2
- name: prep_customers
  transformer: deduplicate
  params:
    keys: [customer_id]
    order_by: "updated_at DESC"

- name: dim_customer
  depends_on: [prep_customers]
  transformer: scd2
  params:
    keys: [customer_id]
```

Deduplicate FIRST. Then SCD2.

**CTA:** Dedupe before SCD2, always

---

### Post 19.2 - Tracking Volatile Columns (Wednesday)

**Hook:**
Tracking last_login in SCD2. Customer logs in 50 times. 50 history rows created.

**Body:**
The mistake:
```yaml
# BAD - tracking rapidly changing column
track_cols:
  - name
  - email
  - last_login_timestamp  # Changes constantly!
```

Result:
- Customer logs in Monday → new version
- Logs in Tuesday → new version
- Logs in 50 times → 50 versions

Dimension explodes. Storage costs spike. Queries slow.

The fix: Only track SLOWLY changing attributes.

```yaml
# GOOD - track stable attributes only
track_cols:
  - name
  - email
  - address
  - tier
  # NOT: last_login, last_order, session_count
```

SCD2 is for slowly changing dimensions. Not rapidly changing ones.

If you need to track logins, use a fact table.

**CTA:** Slowly. Changing. Dimensions.

---

### Post 19.3 - SCD2 on Fact Tables (Friday)

**Hook:**
Using SCD2 on an orders table. Don't.

**Body:**
The mistake:
```yaml
# BAD - SCD2 on facts
- name: fact_orders
  transformer: scd2
  params:
    keys: [order_id]
    track_cols: [status, amount]
```

Why it's wrong:
Facts are events. Events happened. They don't "change."

Order #1001 was placed on Jan 15 for $99. That's a historical fact.

If the status changes (shipped → delivered), that's a NEW event, not a change to the old one.

The fix:
```yaml
# GOOD - append facts, track status changes separately
mode: append

# Or use a status history table
- name: order_status_events
  mode: append
  # order_id, old_status, new_status, changed_at
```

SCD2 = dimensions only.
Facts = append or merge.

I wrote about SCD2 done wrong and how to prevent history explosion on Medium. Link in comments.

**CTA:** Link to Article 19

---

## Week 20: Performance Anti-Patterns

### Post 20.1 - Reading Too Much Data (Monday)

**Hook:**
5 years of data. I only need today. Still loading all 5 years.

**Body:**
The mistake:
```yaml
# BAD - full table scan
read:
  connection: source
  table: orders
  # No filter = load everything
```

Every run: 500GB loaded. 499GB ignored. $$$$ wasted.

The fix:
```yaml
# GOOD - incremental with watermark
read:
  connection: source
  table: orders
  incremental:
    mode: stateful
    column: updated_at
```

Only load rows newer than last run.

Or explicit filter:
```yaml
read:
  connection: source
  table: orders
  filter: "order_date >= '2024-01-01'"
```

5 years of history matters for initial load.
Daily runs should be incremental.

**CTA:** Don't reload the world

---

### Post 20.2 - No Partition Strategy (Wednesday)

**Hook:**
Query scans 100 million rows. Answer requires 10,000.

**Body:**
The mistake:
```yaml
# BAD - no partitioning
write:
  table: fact_orders
  format: delta
```

Every query scans entire table. Slow and expensive.

The fix:
```yaml
# GOOD - partition by date
write:
  table: fact_orders
  format: delta
  partition_by: [order_date]
```

Now:
```sql
SELECT * FROM fact_orders 
WHERE order_date = '2024-12-01'
```

Only scans one partition. 1000x faster.

Partition by:
- Date (most common)
- Region
- Customer segment
- Whatever you filter by most

Full guide on performance anti-patterns and why your pipeline takes hours on Medium. Link in comments.

**CTA:** Link to Article 20

---

### Post 20.3 - Joining Large Tables in Memory (Friday)

**Hook:**
Two 10GB tables. Join them. Computer crashes.

**Body:**
The mistake:
Using Pandas for data that doesn't fit in memory.

```python
# BAD - Pandas with 10GB tables
df1 = pd.read_parquet("orders.parquet")  # 10GB
df2 = pd.read_parquet("customers.parquet")  # 5GB
result = df1.merge(df2)  # Memory error
```

The fix: Use the right engine.

```yaml
# GOOD - Spark for large data
engine: spark

# Or Polars for medium data
engine: polars
```

Guidelines:
- Pandas: < 1GB (fits in memory with room to spare)
- Polars: 1-10GB (efficient single-machine)
- Spark: > 10GB (distributed processing)

Don't force a kitchen blender to do industrial work.

**CTA:** Right tool, right job

---

## Week 21: Configuration Anti-Patterns

### Post 21.1 - Hardcoded Paths (Monday)

**Hook:**
Pipeline works in dev. Breaks in prod. Paths are different.

**Body:**
The mistake:
```yaml
# BAD - hardcoded paths
read:
  path: "abfss://raw@devaccount.dfs.core.windows.net/orders"
write:
  path: "abfss://bronze@devaccount.dfs.core.windows.net/orders"
```

Moving to prod requires editing every path.

The fix:
```yaml
# GOOD - use connections
connections:
  landing:
    type: azure_blob
    account: ${STORAGE_ACCOUNT}  # From environment
    container: raw
  bronze:
    type: azure_blob
    account: ${STORAGE_ACCOUNT}
    container: bronze

# In pipeline
read:
  connection: landing
  path: orders
write:
  connection: bronze
  path: orders
```

Same YAML. Different environments. Just change env vars.

**CTA:** Connections = portability

---

### Post 21.2 - No Error Handling (Wednesday)

**Hook:**
Pipeline failed at step 47. You have to rerun all 47 steps.

**Body:**
The mistake:
```yaml
# BAD - no checkpointing, no retry
nodes:
  - name: step_1
  - name: step_2
  # ... 47 more steps
```

One failure = start from scratch.

The fix:
```yaml
# GOOD - retry with backoff
retry:
  enabled: true
  max_attempts: 3
  backoff: exponential

# GOOD - graceful failure handling
nodes:
  - name: critical_step
    on_error: fail  # Stop everything
  
  - name: optional_step
    on_error: warn  # Log and continue
```

And design for restart:
- Idempotent writes (merge, not append)
- Checkpoints between major stages
- State tracking

**CTA:** Plan for failure

---

### Post 21.3 - Ignoring Schema Evolution (Friday)

**Hook:**
Source added a column. Pipeline broke at 2am.

**Body:**
The mistake:
```yaml
# BAD - rigid schema expectations
write:
  format: delta
  # No schema handling
```

Source adds `phone_number` column. Delta rejects it. Pipeline fails.

The fix:
```yaml
# GOOD - allow schema evolution
write:
  format: delta
  delta_options:
    mergeSchema: true

# Or explicit policy
schema_policy:
  on_new_column: add
  on_missing_column: warn
  on_type_mismatch: error
```

Sources change. Plan for it.

Full guide on configuration patterns for multi-environment pipelines on Medium. Link in comments.

**CTA:** Link to Article 21

---

## Week 22: Debugging & Troubleshooting

### Post 22.1 - Column Name Case Sensitivity (Monday)

**Hook:**
Error: "Column 'customer_id' not found"
Me: *looks at table* "It's right there!"

**Body:**
The problem:
```
Table columns: CustomerID, Name, Email
Config: customer_id
```

Case doesn't match. Column "not found."

The fix: Normalize early.
```yaml
# In Bronze or Silver
transform:
  steps:
    - function: rename_columns
      params:
        lowercase: true
```

Now everything is lowercase. Forever. No more case bugs.

Or match exact case in config:
```yaml
keys: [CustomerID]  # Match exactly
```

I normalize in Silver. Saves headaches forever.

**CTA:** Normalize column names

---

### Post 22.2 - Debugging Pipeline Failures (Wednesday)

**Hook:**
"Pipeline failed" tells you nothing. Here's how to actually debug.

**Body:**
Step 1: **Check the error message**
```
KeyError: 'updated_at'
```
Translation: Column doesn't exist.

Step 2: **Print columns at failure point**
```yaml
- name: debug_node
  transform:
    steps:
      - sql: "SELECT * FROM df LIMIT 1"
  # Check logs for columns
```

Step 3: **Trace column through pipeline**
- Where was it created?
- Was it renamed?
- Was it dropped?

Step 4: **Check data, not just schema**
```sql
SELECT DISTINCT column_name 
FROM df 
LIMIT 10
```
Maybe it exists but is NULL everywhere.

Step 5: **Run with verbose logging**
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

Most errors are:
- Column name mismatch (case, spelling)
- Column dropped upstream
- Type mismatch

Full guide on debugging data pipelines with a systematic approach on Medium. Link in comments.

**CTA:** Link to Article 22

---

### Post 22.3 - When Row Counts Explode (Friday)

**Hook:**
Before: 100,000 rows. After: 10,000,000 rows. What happened?

**Body:**
Common causes:

**1. Bad join**
```sql
-- Accidental cross join
SELECT * FROM orders, customers
-- Should be
SELECT * FROM orders JOIN customers ON ...
```
1M × 1M = 1 trillion rows.

**2. Duplicate keys in join**
```
orders.customer_id | customers.customer_id
101                | 101
101                | 101  ← duplicate!
```
Each duplicate multiplies rows.

**3. SCD2 without dedup**
Covered earlier-every duplicate creates a version.

How to debug:
```sql
-- Check for duplicates on join keys
SELECT customer_id, COUNT(*) 
FROM customers 
GROUP BY customer_id 
HAVING COUNT(*) > 1
```

If duplicates exist, dedupe before join.

**CTA:** Row explosion = join or dupe issue

---

## Medium Articles for Phase 4

| Week | Article |
|------|---------|
| 17 | "Bronze Layer Anti-Patterns: 3 Mistakes That Will Haunt You" |
| 18 | "Silver Layer Best Practices: Centralize, Validate, Deduplicate" |
| 19 | "SCD2 Done Wrong: History Explosion and How to Prevent It" |
| 20 | "Performance Anti-Patterns: Why Your Pipeline Takes Hours" |
| 21 | "Configuration Patterns for Multi-Environment Pipelines" |
| 22 | "Debugging Data Pipelines: A Systematic Approach" |
