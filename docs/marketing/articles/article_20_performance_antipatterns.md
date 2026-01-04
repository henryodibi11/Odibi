# Performance Anti-Patterns: Why Your Pipeline Takes Hours

*Common mistakes that kill data pipeline performance*

---

## TL;DR

Data pipelines slow down for predictable reasons: unnecessary shuffles, wrong join strategies, unpartitioned data, and collect operations that bring everything to a single node. This article covers the top performance killers and how to fix them.

---

## Anti-Pattern 1: Shuffle Everything

### The Problem

Every `GROUP BY`, `JOIN`, or `DISTINCT` operation can trigger a shuffle-redistributing data across the cluster. Unnecessary shuffles kill performance.

```yaml
# ❌ Multiple shuffles
transform:
  steps:
    - sql: "SELECT DISTINCT customer_id FROM df"  # Shuffle 1
    - sql: "SELECT * FROM df GROUP BY region"     # Shuffle 2
    - sql: "SELECT * FROM df ORDER BY date"       # Shuffle 3
```

Three shuffles when you might need zero.

### The Fix

Combine operations to minimize shuffles:

```yaml
# ✅ Single shuffle
transform:
  steps:
    - sql: |
        SELECT region, COUNT(DISTINCT customer_id) as customers
        FROM df
        GROUP BY region
        ORDER BY region
```

And pre-partition data:

```yaml
write:
  partition_by: [region]  # Future reads filter efficiently
```

---

## Anti-Pattern 2: Cartesian Joins

### The Problem

A join without a proper key creates a Cartesian product:

```yaml
# ❌ Missing join key
transform:
  steps:
    - sql: |
        SELECT *
        FROM orders o, customers c  # Cartesian product!
        WHERE o.region = c.region   # This is a filter, not a join key!
```

10,000 orders × 10,000 customers = 100,000,000 row intermediate result.

### The Fix

Always use explicit join syntax with proper keys:

```yaml
# ✅ Proper join
transform:
  steps:
    - sql: |
        SELECT *
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
```

---

## Anti-Pattern 3: Skewed Joins

### The Problem

When one join key has many more values than others:

```
Customer A: 10,000,000 orders
Customer B: 100 orders
Customer C: 50 orders
```

All Customer A data goes to one partition. That partition takes forever while others finish instantly.

### The Fix: Salting

Add a random component to spread the hot key:

```yaml
transform:
  steps:
    - sql: |
        -- Add salt to fact table
        SELECT 
          *,
          CONCAT(customer_id, '_', FLOOR(RAND() * 10)) as customer_id_salted
        FROM orders
    
    - sql: |
        -- Explode dimension to match salted keys
        SELECT 
          c.*,
          EXPLODE(ARRAY(0,1,2,3,4,5,6,7,8,9)) as salt
        FROM customers c
    
    - sql: |
        -- Join on salted keys
        SELECT o.*, c.*
        FROM orders_salted o
        JOIN customers_exploded c 
          ON o.customer_id_salted = CONCAT(c.customer_id, '_', c.salt)
```

Or use Spark's broadcast hint for small dimensions:

```yaml
transform:
  steps:
    - sql: |
        SELECT /*+ BROADCAST(c) */ *
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
```

---

## Anti-Pattern 4: Collect to Driver

### The Problem

Bringing all data to a single node:

```python
# ❌ Collects entire dataset to memory
df = spark.read.parquet("data/")
all_data = df.collect()  # BOOM: OutOfMemory
for row in all_data:
    process(row)
```

### The Fix

Keep data distributed:

```python
# ✅ Process in parallel
df = spark.read.parquet("data/")
df.foreach(process_row)  # Distributed

# Or write results back to storage
df.write.parquet("output/")
```

In Odibi, avoid transforms that require collection:

```yaml
# ✅ All operations stay distributed
transform:
  steps:
    - sql: "SELECT * FROM df WHERE ..."  # Distributed
    - function: aggregate                   # Distributed
```

---

## Anti-Pattern 5: Reading Unpartitioned Data

### The Problem

```yaml
read:
  path: "data/orders"  # 10 billion rows, not partitioned
  filter: "WHERE order_date = '2023-12-15'"
```

Spark reads ALL 10 billion rows, then filters. Slow.

### The Fix

Partition on common filter columns:

```yaml
write:
  path: "data/orders"
  partition_by: [order_year, order_month, order_day]
```

Now:

```yaml
read:
  path: "data/orders"
  filter: "WHERE order_year = 2023 AND order_month = 12 AND order_day = 15"
```

Reads only the relevant partition.

---

## Anti-Pattern 6: Too Many Small Files

### The Problem

1,000,000 tiny files (1MB each) instead of 1,000 reasonable files (1GB each).

Each file = overhead for:
- File listing
- Opening/closing
- Metadata reads

### The Fix

Coalesce on write:

```yaml
write:
  coalesce: 100  # Reduce to 100 files
```

Or repartition:

```yaml
transform:
  steps:
    - sql: "SELECT /*+ REPARTITION(100) */ * FROM df"
```

For Delta Lake, use OPTIMIZE:

```yaml
post_sql:
  - "OPTIMIZE gold.fact_orders"
```

---

## Anti-Pattern 7: Full Table Scans in Lookups

### The Problem

SCD2 dimension lookup scans entire dimension for every fact row:

```yaml
# Implicit nested loop join
pattern:
  type: fact
  params:
    dimensions:
      - dimension_table: dim_customer  # 10M rows
        # Scanned for every fact row!
```

### The Fix

Broadcast small dimensions:

```yaml
pattern:
  type: fact
  params:
    dimensions:
      - dimension_table: dim_customer
        broadcast: true  # If < 1GB
```

Or pre-filter dimensions:

```yaml
transform:
  steps:
    - sql: |
        -- Only current records for lookup
        SELECT * FROM dim_customer WHERE is_current = true
```

---

## Anti-Pattern 8: Wrong File Format

### The Problem

Using row-oriented formats for analytical queries:

| Format | Good For | Bad For |
|--------|----------|---------|
| CSV | Human reading | Everything else |
| JSON | Semi-structured | Analytical queries |
| Parquet | Analytics | Row-level updates |
| Delta | Analytics + Updates | Simple use cases |

```yaml
# ❌ CSV for analytics
read:
  format: csv  # No columnar pushdown, no compression
```

### The Fix

```yaml
# ✅ Delta for analytics
write:
  format: delta
  
post_sql:
  - "OPTIMIZE table ZORDER BY (customer_id)"
```

---

## Anti-Pattern 9: Processing All History Every Run

### The Problem

```yaml
# ❌ Full load every day
read:
  path: "bronze/orders"  # All history

write:
  mode: overwrite  # Rewrite everything
```

Processing 5 years of history when you only need today's data.

### The Fix

Incremental loading:

```yaml
read:
  path: "bronze/orders"
  incremental:
    mode: stateful
    column: _extracted_at
    lookback: "3 days"

write:
  mode: append  # Or merge
```

---

## Anti-Pattern 10: N+1 Query Pattern

### The Problem

Processing each record with a separate query:

```python
# ❌ N+1 queries
for order_id in order_ids:  # 1M iterations
    customer = spark.sql(f"SELECT * FROM customers WHERE id = {order_id}")
    # 1M separate queries!
```

### The Fix

Batch operations:

```python
# ✅ Single join
orders_with_customers = orders.join(customers, "customer_id")
```

In YAML:

```yaml
transform:
  steps:
    - sql: |
        SELECT o.*, c.*
        FROM orders o
        LEFT JOIN customers c ON o.customer_id = c.customer_id
```

---

## Quick Performance Checklist

Before running a slow pipeline, check:

| Check | Fix |
|-------|-----|
| Is data partitioned? | Add partition_by |
| Using columnar format? | Switch to Parquet/Delta |
| Broadcasting small tables? | Add broadcast hint |
| Processing incrementally? | Add watermark |
| Too many small files? | Coalesce or OPTIMIZE |
| Shuffling unnecessarily? | Combine operations |

---

## Monitoring Performance

Add timing to your nodes:

```yaml
- name: silver_orders
  metrics:
    enabled: true
    include:
      - execution_time
      - rows_processed
      - rows_per_second
```

Query metrics:

```sql
SELECT 
  node_name,
  AVG(execution_seconds) as avg_time,
  AVG(rows_processed) as avg_rows,
  AVG(rows_processed / execution_seconds) as rows_per_sec
FROM system.metrics
WHERE run_date >= CURRENT_DATE - 7
GROUP BY node_name
ORDER BY avg_time DESC
```

---

## Key Takeaways

| Anti-Pattern | Impact | Fix |
|--------------|--------|-----|
| Unnecessary shuffles | 10x slowdown | Combine operations |
| Cartesian joins | Exponential explosion | Use proper join keys |
| Skewed data | One partition takes forever | Salting or broadcast |
| Collect to driver | OutOfMemory | Keep distributed |
| Unpartitioned data | Full scans | Partition on filter columns |
| Too many small files | File overhead | Coalesce/OPTIMIZE |
| Wrong file format | No pushdown | Use Parquet/Delta |
| Full loads | Wasted reprocessing | Incremental loading |

---

## Next Steps

Beyond performance, configuration management matters for production:

- Environment-specific settings
- Secret management
- Config validation

Next article: **Configuration Patterns for Multi-Environment Pipelines**.

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]
- **LinkedIn:** Follow for more articles in this series
