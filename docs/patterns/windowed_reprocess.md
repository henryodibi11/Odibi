# Pattern: Windowed Reprocess (Silver → Gold Aggregates)

**Status:** Core Pattern  
**Layer:** Gold (aggregated/BI-ready)  
**Engine:** Spark Batch  
**Write Mode:** `overwrite` (partition-specific)  
**Idempotent:** Yes (recalculated)  

---

## Problem

You have a Gold aggregate table (e.g., daily sales summary). Late-arriving data in Silver invalidates yesterday's numbers. You need to:
- **Fix aggregates when new data arrives**
- **Avoid double-counting** (can't just add new rows)
- **Keep calculations simple** (always correct, never patched)

How do you maintain accurate aggregates without complex update logic?

## Solution

Instead of **patching** aggregates with updates (error-prone), **recalculate the entire time window** and replace it.

**Principle:** "Rebuild the Bucket, Don't Patch the Hole"

---

## How It Works

### The Reprocess Pattern

1. **Identify the window** (e.g., "Last 7 days", "This month")
2. **Read Silver filtered to that window**
3. **Recalculate aggregate** (SUM, COUNT, AVG, etc.)
4. **Write to Gold with Dynamic Partition Overwrite**

If late data arrives in the last 7 days, next run recalculates those days—automatically fixing aggregates.

---

## Step-by-Step Example

### Scenario: Daily Sales Summary

**Silver Table (Orders, with timestamps):**

**Day 1 (Initial load on 2025-11-01 at 10:00):**
```
order_id | order_date | amount | created_at
---------|------------|--------|-------------------
1        | 2025-11-01 | 100    | 2025-11-01 10:00
2        | 2025-11-01 | 50     | 2025-11-01 10:30
3        | 2025-10-31 | 200    | 2025-11-01 10:45
```

**Run 1 (Calculate last 7 days: 2025-10-25 to 2025-11-01):**

```sql
SELECT 
  DATE(order_date) as order_date,
  SUM(amount) as total_sales,
  COUNT(*) as order_count
FROM silver.orders
WHERE DATE(order_date) >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY DATE(order_date)
```

**Gold Result (Days 25-Oct to 1-Nov):**
```
order_date | total_sales | order_count
-----------|-------------|-------------
2025-10-31 | 200         | 1
2025-11-01 | 150         | 2
```

**Partition written:** `order_date=2025-10-31`, `order_date=2025-11-01`

---

### Day 2: Late Data Arrives

**Silver Table (New data arrived at 14:00):**
```
order_id | order_date | amount | created_at
---------|------------|--------|-------------------
1        | 2025-11-01 | 100    | 2025-11-01 10:00
2        | 2025-11-01 | 50     | 2025-11-01 10:30
3        | 2025-10-31 | 200    | 2025-11-01 10:45
4        | 2025-11-01 | 75     | 2025-11-02 14:00  ← LATE DATA (same day, arrived late)
```

**Run 2 (Recalculate last 7 days: 2025-10-25 to 2025-11-02):**

```sql
SELECT 
  DATE(order_date) as order_date,
  SUM(amount) as total_sales,
  COUNT(*) as order_count
FROM silver.orders
WHERE DATE(order_date) >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY DATE(order_date)
```

**Gold Result (Days 25-Oct to 2-Nov):**
```
order_date | total_sales | order_count
-----------|-------------|-------------
2025-10-31 | 200         | 1
2025-11-01 | 225         | 3              ← UPDATED (was 150, now 225)
```

**Write Mode: Dynamic Partition Overwrite**
- Existing partition `order_date=2025-10-31` is untouched
- Partition `order_date=2025-11-01` is **replaced entirely** (was 2 rows, now 3 rows)

**No double-counting:** The aggregate is **recalculated from scratch**, not patched.

---

## Why This Works

### Without Windowed Reprocess (WRONG)

```sql
-- Don't do this
UPDATE gold.sales
SET total_sales = total_sales + 75,
    order_count = order_count + 1
WHERE order_date = '2025-11-01'
```

**Problems:**
- If this query runs twice, you add 75 twice (double-counting)
- If you run it out-of-order, you corrupt data
- Requires tracking "what did I update?"

### With Windowed Reprocess (RIGHT)

```sql
-- Recalculate the entire 7-day window
SELECT 
  DATE(order_date),
  SUM(amount),
  COUNT(*)
FROM silver.orders
WHERE DATE(order_date) >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY DATE(order_date)

-- Write with Dynamic Partition Overwrite
-- Entire partition is replaced
```

**Advantages:**
- Idempotent (run 10 times = same result)
- No double-counting (always fresh calculation)
- Simple logic (standard SQL aggregate)

---

## Odibi YAML

### Simple Daily Aggregate

```yaml
- id: gold_daily_sales
  name: "Daily Sales Summary (Gold)"
  depends_on: [merge_orders_silver]
  read:
    connection: adls_prod
    format: delta
    table: silver.orders
  transform:
    steps:
      - sql: |
          SELECT 
            DATE(order_date) as order_date,
            SUM(amount) as total_sales,
            COUNT(*) as order_count,
            MIN(order_date) as first_order_ts,
            MAX(order_date) as last_order_ts
          FROM silver.orders
          WHERE DATE(order_date) >= DATE_SUB(CURRENT_DATE(), 7)
          GROUP BY DATE(order_date)
  write:
    connection: adls_prod
    format: delta
    table: gold.daily_sales
    mode: overwrite
    options:
      partitionOverwriteMode: dynamic
```

### Monthly Aggregate (Wider Window)

```yaml
- id: gold_monthly_sales
  name: "Monthly Sales Summary (Gold)"
  depends_on: [merge_orders_silver]
  read:
    connection: adls_prod
    format: delta
    table: silver.orders
  transform:
    steps:
      - sql: |
          SELECT 
            DATE_TRUNC('month', order_date) as month,
            SUM(amount) as total_sales,
            COUNT(*) as order_count,
            COUNT(DISTINCT customer_id) as unique_customers
          FROM silver.orders
          WHERE DATE_TRUNC('month', order_date) >= DATE_TRUNC('month', DATE_SUB(CURRENT_DATE(), 90))
          GROUP BY DATE_TRUNC('month', order_date)
  write:
    connection: adls_prod
    format: delta
    table: gold.monthly_sales
    mode: overwrite
    options:
      partitionOverwriteMode: dynamic
```

### Multi-Grain Aggregates

```yaml
- id: gold_sales_by_region_day
  name: "Sales by Region & Day (Gold)"
  depends_on: [merge_orders_silver]
  read:
    connection: adls_prod
    format: delta
    table: silver.orders
  transform:
    steps:
      - sql: |
          SELECT 
            region,
            DATE(order_date) as order_date,
            SUM(amount) as total_sales,
            COUNT(*) as order_count,
            AVG(amount) as avg_order_value
          FROM silver.orders
          WHERE DATE(order_date) >= DATE_SUB(CURRENT_DATE(), 30)
          GROUP BY region, DATE(order_date)
  write:
    connection: adls_prod
    format: delta
    table: gold.sales_by_region_day
    mode: overwrite
    options:
      partitionOverwriteMode: dynamic
```

---

## Window Size Strategy

### How Far Back Should the Window Be?

**Rule of Thumb:** 2-3x your SLA for late data.

| SLA | Window | Example |
|-----|--------|---------|
| Same-day delivery (next day processed) | 3-7 days | Daily aggregate |
| 1-week SLA | 14-21 days | Weekly aggregate |
| End-of-month close (3-5 days) | 30-45 days | Monthly aggregate |

**Conservative approach:** Recalculate 30 days back, even if only aggregating daily. It costs minimal compute.

---

## Dynamic Partition Overwrite

### Why It Matters

**Scenario:** Your table is partitioned by `order_date`:

```
gold/sales/
├── order_date=2025-11-01/
├── order_date=2025-10-31/
├── order_date=2025-10-30/
└── ... (30 days of data)
```

If you use **full overwrite** (default):
- Entire table is replaced
- All 30 days are rewritten (slow)
- Other columns lose their data

If you use **dynamic partition overwrite**:
- Only `order_date=2025-11-01` (and other affected dates) are replaced
- Unaffected dates remain untouched
- Much faster

### Enabling in Odibi

```yaml
write:
  connection: adls_prod
  format: delta
  table: gold.daily_sales
  mode: overwrite
  options:
    partitionOverwriteMode: dynamic
```

This is **automatically enabled** by Odibi's safe defaults (per Architecture Manifesto).

---

## Troubleshooting

### Problem: Aggregate is Still Wrong

**Causes:**
1. **Window is too short** → Late data arriving outside window. Increase window size.
2. **Wrong grouping** → Missing a dimension (e.g., region). Check Silver data.
3. **Stale Silver data** → No new orders merged in. Check merge pipeline.

**Debug:**
```sql
-- Check what's in Silver for the window
SELECT DATE(order_date), COUNT(*) 
FROM silver.orders
WHERE DATE(order_date) >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY DATE(order_date)
ORDER BY order_date DESC;

-- Compare to Gold
SELECT order_date, COUNT(*) as count
FROM gold.daily_sales
WHERE order_date >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY order_date
ORDER BY order_date DESC;
```

### Problem: Slow Rewrites

**Causes:**
1. **Window too large** → Recalculating 365 days every run. Reduce window or run less frequently.
2. **No partitioning** → Entire table is scanned. Add `partition by order_date` to Silver.

**Solution:**
```yaml
# Smaller window for frequent runs
WHERE DATE(order_date) >= DATE_SUB(CURRENT_DATE(), 3)

# Larger window for nightly runs
WHERE DATE(order_date) >= DATE_SUB(CURRENT_DATE(), 30)
```

---

## Trade-Offs

### Advantages
✓ **Always correct** (fresh calculation, not patched)  
✓ **Idempotent** (run multiple times = same result)  
✓ **Self-healing** (late data automatically fixes aggregates)  
✓ **Simple logic** (standard SQL, no complex update logic)  
✓ **Fast** (recalculate 7 days vs. maintain entire history)  

### Disadvantages
✗ **Requires recomputation** (slower than patches, but worth it)  
✗ **Assumes partitioning** (without partitioning, rewrites entire table)  
✗ **Assumes stateless logic** (can't use row-level updates)  

---

## When to Use

- **Always** for Gold aggregates (KPIs, fact tables, summaries)
- Late-arriving data possible
- Queries can be re-executed without side effects
- Need guaranteed correctness over minimal compute

## When NOT to Use

- Audit tables (use append)
- Streaming aggregates with sub-second latency (use Structured Streaming)
- Data with complex, stateful dependencies

---

## Related Patterns

- **[Merge/Upsert](./merge_upsert.md)** → Maintains clean Silver data that aggregates read from
- **[Append-Only Raw](./append_only_raw.md)** → Source of truth if aggregates need replay

---

## References

- [Odibi Architecture Manifesto: Pattern C - Aggregation](../design/02_architecture_manifesto.md#pattern-c-aggregation-silver--gold)
- [Databricks: Dynamic Partition Overwrite](https://docs.databricks.com/en/sql/language-manual/sql-conf-spark-sql-sources-partitionOverwriteMode.html)
- [Fundamentals of Data Engineering: Chapter on Aggregation](https://www.fundamentalsofdataengineering.com/)
