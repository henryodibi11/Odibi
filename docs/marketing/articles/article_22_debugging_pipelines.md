# Debugging Data Pipelines: A Systematic Approach

*When your pipeline fails at 3 AM*

---

## TL;DR

Data pipeline debugging is systematic, not random. This article covers a repeatable process: identify the failure point, check the logs, reproduce the issue, find the root cause, and fix it. We'll cover common error patterns, log analysis, and debugging techniques for Bronze, Silver, and Gold layers.

---

## The Debugging Process

```
1. IDENTIFY    → Which node failed?
     ↓
2. LOGS        → What does the error say?
     ↓
3. DATA        → What data caused the problem?
     ↓
4. REPRODUCE   → Can you recreate it locally?
     ↓
5. ROOT CAUSE  → Why did it happen?
     ↓
6. FIX         → Implement and test solution
     ↓
7. PREVENT     → Add tests/contracts to prevent recurrence
```

---

## Step 1: Identify the Failure Point

### Check Pipeline Status

```bash
# View recent runs
odibi status --last 10

# Output:
# Run ID          Status    Started              Duration   Failed Node
# run_20231215    FAILED    2023-12-15 09:30:00  15m        silver_orders
# run_20231214    SUCCESS   2023-12-14 09:30:00  12m        -
# run_20231213    SUCCESS   2023-12-13 09:30:00  11m        -
```

### Identify Failed Node

```bash
# Details for specific run
odibi status run_20231215

# Output:
# Pipeline: silver_ecommerce
# Status: FAILED
# 
# Nodes:
# ✓ silver_customers    SUCCESS   2m 15s
# ✓ silver_products     SUCCESS   1m 45s  
# ✗ silver_orders       FAILED    5m 30s    ← HERE
# ○ silver_payments     SKIPPED   (depends on silver_orders)
```

---

## Step 2: Check the Logs

### View Node Logs

```bash
# Full logs for failed node
odibi logs run_20231215 --node silver_orders
```

### Common Error Patterns

**Pattern 1: Contract Failure**

```
[ERROR] Contract failed: not_null on customer_id
[ERROR]   Found 47 null values in 99,441 rows
[ERROR]   Sample rows with null customer_id:
[ERROR]     Row 4521: order_id=abc123, customer_id=NULL
[ERROR] Pipeline halted due to contract failure
```

**Pattern 2: Schema Mismatch**

```
[ERROR] Column 'order_total' not found in source
[ERROR] Expected columns: [order_id, customer_id, order_total]
[ERROR] Found columns: [order_id, customer_id, total_amount]
[ERROR] Source schema may have changed
```

**Pattern 3: Data Type Error**

```
[ERROR] Cannot cast 'TBD' to DOUBLE for column 'price'
[ERROR] Row 8932: order_id=xyz789, price='TBD'
[ERROR] Consider using TRY_CAST or filtering invalid values
```

**Pattern 4: Join Failure**

```
[ERROR] Join produced 0 rows
[ERROR] Left table: 50,000 rows
[ERROR] Right table: 10,000 rows
[ERROR] Join condition: left.customer_id = right.customer_id
[ERROR] Possible cause: No matching keys between tables
```

**Pattern 5: Out of Memory**

```
[ERROR] java.lang.OutOfMemoryError: Java heap space
[ERROR] Task failed while processing partition 47 of 100
[ERROR] Consider increasing executor memory or reducing partition size
```

---

## Step 3: Examine the Data

### Find Problem Records

```sql
-- Find rows that would fail the contract
SELECT *
FROM bronze.orders
WHERE customer_id IS NULL
ORDER BY _extracted_at DESC
LIMIT 100
```

### Check for Pattern

```sql
-- Are nulls from a specific batch?
SELECT 
  _batch_id,
  COUNT(*) as total_rows,
  SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customers
FROM bronze.orders
GROUP BY _batch_id
ORDER BY _batch_id DESC
```

### Compare to Previous Run

```sql
-- What changed since last successful run?
SELECT 
  DATE(_extracted_at) as extract_date,
  COUNT(*) as rows,
  SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as nulls
FROM bronze.orders
GROUP BY DATE(_extracted_at)
ORDER BY extract_date DESC
```

---

## Step 4: Reproduce Locally

### Extract Failing Data

```bash
# Save problem records
odibi sample run_20231215 --node silver_orders --filter "customer_id IS NULL" --output failing_data.parquet
```

### Run Locally

```bash
# Run with sample data
odibi run odibi.yaml --node silver_orders --input failing_data.parquet --dry-run
```

### Debug Interactively

```python
# Python debugging
from odibi import Pipeline

pipeline = Pipeline.load("odibi.yaml")
node = pipeline.get_node("silver_orders")

# Read input
df = node.read_input()

# Check data
print(df.filter(df.customer_id.isNull()).count())

# Run transformations step by step
df_step1 = node.run_step(0, df)
df_step2 = node.run_step(1, df_step1)
# ...
```

---

## Step 5: Root Cause Analysis

### Common Root Causes

| Symptom | Likely Cause | Investigation |
|---------|--------------|---------------|
| Null values | Source system bug | Check source system logs |
| Schema change | Upstream deployment | Check schema history |
| Missing data | Extraction failure | Check Bronze for gaps |
| Duplicates | Retry without dedup | Check for duplicate batches |
| Wrong values | Business logic change | Check with data owners |

### The 5 Whys

```
Problem: customer_id is NULL

Why? Source file has NULL customer_id
Why? CRM export job had partial failure
Why? CRM database timeout during export
Why? Large query without pagination
Why? Export job was never optimized

Root Cause: CRM export job needs pagination
```

---

## Step 6: Fix the Issue

### Immediate Fix (Stop the Bleeding)

```yaml
# Add quarantine for immediate recovery
contracts:
  - type: not_null
    column: customer_id
    severity: quarantine  # Changed from error
```

### Proper Fix

```yaml
# After source is fixed, validate it doesn't recur
contracts:
  - type: not_null
    column: customer_id
    severity: error
    description: "Fixed upstream in ticket DATA-1234"
```

### Reprocess Failed Data

```bash
# Rerun failed node
odibi run odibi.yaml --node silver_orders --reprocess run_20231215

# Or reprocess specific batches
odibi run odibi.yaml --node silver_orders --filter "_batch_id = 'batch_20231215'"
```

---

## Step 7: Prevent Recurrence

### Add Contracts

```yaml
# New contract based on failure
contracts:
  - type: not_null
    column: customer_id
    severity: error
    description: "Added after incident INC-2023-1215"
```

### Add Monitoring

```yaml
alerts:
  - type: slack
    url: ${SLACK_WEBHOOK}
    on_events: [on_failure, on_warning]
    message: "Pipeline ${pipeline} node ${node} failed: ${error}"
```

### Add Tests

```python
# test_silver_orders.py
def test_null_customer_rejected():
    """Null customer_id should fail contract"""
    bad_data = pd.DataFrame({
        'order_id': ['ORD-001'],
        'customer_id': [None]
    })
    
    result = run_node('silver_orders', input_df=bad_data, dry_run=True)
    assert result.status == 'failed'
```

---

## Layer-Specific Debugging

### Bronze Debugging

Common issues:
- Empty source files
- Schema drift
- Encoding problems

```bash
# Check source file
head -n 5 data/landing/orders.csv

# Verify Bronze landed correctly
odibi query "SELECT COUNT(*), MIN(_extracted_at), MAX(_extracted_at) FROM bronze.orders"
```

### Silver Debugging

Common issues:
- Contract failures
- Transform errors
- Deduplication issues

```bash
# Check pre/post row counts
odibi query "
  SELECT 
    (SELECT COUNT(*) FROM bronze.orders) as bronze,
    (SELECT COUNT(*) FROM silver.orders) as silver,
    (SELECT COUNT(*) FROM silver.orders_quarantine) as quarantine
"
```

### Gold Debugging

Common issues:
- Dimension lookup failures
- Grain violations
- Aggregation errors

```bash
# Check for orphans
odibi query "
  SELECT customer_sk, COUNT(*) 
  FROM gold.fact_orders 
  WHERE customer_sk = 0  -- Unknown member
  GROUP BY customer_sk
"
```

---

## Debugging Toolkit

### Useful Queries

```sql
-- Data freshness
SELECT MAX(_extracted_at) as latest FROM bronze.orders;

-- Row counts by batch
SELECT _batch_id, COUNT(*) FROM bronze.orders GROUP BY _batch_id;

-- Contract violations
SELECT _rejection_reason, COUNT(*) 
FROM silver.orders_quarantine 
GROUP BY _rejection_reason;

-- Duplicate check
SELECT order_id, COUNT(*) 
FROM silver.orders 
GROUP BY order_id 
HAVING COUNT(*) > 1;

-- Schema comparison
DESCRIBE bronze.orders;
DESCRIBE silver.orders;
```

### Log Analysis Commands

```bash
# Search logs for errors
odibi logs run_20231215 | grep -i error

# Count errors by type
odibi logs run_20231215 | grep ERROR | cut -d: -f3 | sort | uniq -c

# Find slow operations
odibi logs run_20231215 | grep "completed in" | sort -t' ' -k4 -rn | head
```

---

## Emergency Playbook

When the pipeline fails at 3 AM:

### 1. Quick Assessment (5 min)

```bash
odibi status --last 1
odibi logs $RUN_ID | tail -50
```

### 2. Decide: Fix or Skip (5 min)

- **Data issue**: Quarantine and continue
- **Code issue**: Rollback last change
- **Infrastructure**: Alert on-call

### 3. Quick Fix (10 min)

```yaml
# Emergency quarantine
contracts:
  - severity: quarantine  # Was: error
```

```bash
odibi run --node failed_node
```

### 4. Document (5 min)

```
Incident: Pipeline failed 2023-12-15 03:00
Cause: NULL customer_id in batch_20231215
Fix: Quarantined 47 records, pipeline completed
Follow-up: Ticket DATA-1234 for source fix
```

---

## Key Takeaways

| Step | Action |
|------|--------|
| Identify | Which node, which run |
| Logs | Read the error message carefully |
| Data | Find the specific bad records |
| Reproduce | Test locally with sample data |
| Root cause | 5 Whys, find upstream issue |
| Fix | Immediate + proper solution |
| Prevent | Contracts, monitoring, tests |

---

## Next Steps

We've covered the core patterns. Now for advanced topics:

- Semantic layer for metrics
- Production monitoring
- Reflections on open source

Next article: **Building a Semantic Layer: Metrics Everyone Can Trust**.

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]
- **LinkedIn:** Follow for more articles in this series
