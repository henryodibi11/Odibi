# Bronze Layer Anti-Patterns: 3 Mistakes That Will Haunt You

*How to ruin your data lake before it starts*

---

## TL;DR

The Bronze layer is your safety net-the one place you can always go back to. But three common mistakes destroy this safety: transforming data before landing, overwriting instead of appending, and skipping metadata. This article shows what goes wrong and how to fix it.

---

## What Bronze Is Supposed to Do

Bronze has one job: **preserve the source data exactly as received**.

That's it. No cleaning. No transforming. No filtering. Just land it and add metadata.

When you mess this up, you lose your ability to recover from mistakes made in Silver and Gold.

---

## Anti-Pattern #1: Transforming in Bronze

### The Mistake

"Let's just clean up the dates while we're loading..."

```yaml
# ❌ WRONG: Transforming in Bronze
- name: bronze_orders
  read:
    connection: landing
    path: orders.csv
  
  transform:
    steps:
      # NO! This is Silver's job!
      - function: derive_columns
        params:
          columns:
            order_date: "TO_DATE(order_timestamp)"
      - sql: "SELECT * FROM df WHERE order_status != 'test'"
  
  write:
    connection: bronze
    path: orders
```

### Why It's Wrong

You discarded test orders. Six months later, you discover "test" orders were actually legitimate orders from a test environment that went to production.

Those orders are gone. Forever.

You converted timestamps to dates. Later you realize you needed the time component for delivery SLA calculations.

That precision is gone. Forever.

### The Correct Pattern

```yaml
# ✅ RIGHT: Bronze preserves everything
- name: bronze_orders
  read:
    connection: landing
    path: orders.csv
  
  transform:
    steps:
      # ONLY metadata - nothing from source changes
      - function: derive_columns
        params:
          columns:
            _extracted_at: "current_timestamp()"
            _source_file: "'orders.csv'"
            _batch_id: "'run_20231215_0900'"
  
  write:
    connection: bronze
    path: orders
    mode: append
```

### The Rule

In Bronze, you may only **add** columns. Never **change** or **remove** source columns.

| Allowed in Bronze | Not Allowed in Bronze |
|-------------------|----------------------|
| `_extracted_at` | `UPPER(customer_name)` |
| `_source_file` | `TO_DATE(timestamp)` |
| `_batch_id` | `WHERE status != 'test'` |

---

## Anti-Pattern #2: Overwriting Instead of Appending

### The Mistake

"We don't need history, just keep the latest data..."

```yaml
# ❌ WRONG: Overwriting Bronze
- name: bronze_orders
  read:
    connection: landing
    path: orders.csv
  
  write:
    connection: bronze
    path: orders
    mode: overwrite  # DANGER!
```

### Why It's Wrong

Monday: Source sends 100,000 orders
Tuesday: Source sends 95,000 orders (5,000 missing due to source bug)
Wednesday: You notice revenue is down

With `overwrite`, Monday's data is gone. You can't:
- Identify the 5,000 missing orders
- Compare Tuesday to Monday
- Recover the lost data

### The Correct Pattern

```yaml
# ✅ RIGHT: Always append in Bronze
- name: bronze_orders
  write:
    connection: bronze
    path: orders
    mode: append  # Always append!
```

Now you have:
- Monday's full load (100K rows, batch_1)
- Tuesday's load (95K rows, batch_2)
- Evidence of the problem
- Ability to recover

### But What About Disk Space?

"Appending forever will use too much storage!"

True. But storage is cheap. Lost data is expensive.

Solutions:
1. **Retention policy**: Keep 90 days in Bronze, archive older
2. **Compression**: Delta Lake compresses well
3. **Tiered storage**: Cold storage for older Bronze data

```yaml
# Retention policy
system:
  bronze_retention:
    enabled: true
    days: 90
    archive_connection: cold_storage
```

### When Overwrite Is Okay

Never in production Bronze. Maybe for:
- Local development testing
- Scratch/experimental pipelines
- Non-production environments

Even then, think twice.

---

## Anti-Pattern #3: Skipping Metadata

### The Mistake

"We'll just load the raw files, we can figure out where they came from later..."

```yaml
# ❌ WRONG: No metadata
- name: bronze_orders
  read:
    connection: landing
    path: orders.csv
  
  write:
    connection: bronze
    path: orders
    mode: append
    # No metadata columns added
```

### Why It's Wrong

You have 10 million rows in Bronze. Something is wrong with orders from December 13.

Without metadata:
- Which file did those rows come from?
- When were they loaded?
- Which pipeline run loaded them?

You're stuck.

### The Correct Pattern

```yaml
# ✅ RIGHT: Always add metadata
- name: bronze_orders
  read:
    connection: landing
    path: orders.csv
  
  transform:
    steps:
      - function: derive_columns
        params:
          columns:
            _extracted_at: "current_timestamp()"
            _source_file: "'orders_20231215.csv'"
            _batch_id: "${BATCH_ID}"
            _pipeline_version: "'1.2.3'"
  
  write:
    connection: bronze
    path: orders
    mode: append
```

### Essential Metadata Columns

| Column | Purpose | Example |
|--------|---------|---------|
| `_extracted_at` | When loaded | 2023-12-15 09:30:00 |
| `_source_file` | Which file | orders_20231215.csv |
| `_batch_id` | Which run | run_20231215_0930 |
| `_source_system` | Which system | salesforce_prod |

### Debugging With Metadata

Now you can:

```sql
-- Find December 13 data
SELECT * FROM bronze.orders
WHERE DATE(_extracted_at) = '2023-12-13'

-- Find data from specific file
SELECT * FROM bronze.orders
WHERE _source_file = 'orders_20231213.csv'

-- Compare batches
SELECT _batch_id, COUNT(*) as row_count
FROM bronze.orders
GROUP BY _batch_id
ORDER BY _batch_id DESC
```

---

## The Complete Bronze Pattern

Here's the right way to do Bronze:

```yaml
- name: bronze_orders
  description: "Raw orders - NO transformations"
  
  read:
    connection: landing
    path: orders_*.csv  # Support wildcards
    format: csv
    options:
      header: true
      inferSchema: true
  
  # Minimal contract - just verify we have data
  contracts:
    - type: row_count
      min: 1
      severity: error
  
  # ONLY metadata columns
  transform:
    steps:
      - function: derive_columns
        params:
          columns:
            _extracted_at: "current_timestamp()"
            _source_file: "input_file_name()"  # Gets actual filename
            _batch_id: "'${RUN_ID}'"
  
  write:
    connection: bronze
    path: orders
    format: delta
    mode: append  # Always append!
```

---

## How to Audit Your Bronze Layer

Check for these problems:

### 1. Are You Transforming?

```sql
-- Compare Bronze to source
-- If columns are different (except _metadata), you're transforming
SELECT column_name 
FROM information_schema.columns
WHERE table_name = 'bronze_orders'
  AND column_name NOT LIKE '_%'  -- Exclude metadata

-- Should exactly match source file columns
```

### 2. Are You Appending?

```sql
-- Check for multiple batches
SELECT _batch_id, COUNT(*) as rows, MIN(_extracted_at), MAX(_extracted_at)
FROM bronze.orders
GROUP BY _batch_id
ORDER BY MIN(_extracted_at)

-- If only one batch, you might be overwriting
```

### 3. Do You Have Metadata?

```sql
-- Check for metadata columns
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'bronze_orders'
  AND column_name LIKE '_%'

-- Should have _extracted_at, _source_file, _batch_id at minimum
```

---

## Recovery Stories

### Story 1: The Lost Orders

**Problem**: Source system bug sent empty file on December 14.

**Without proper Bronze**: 
- Overwrite replaced December 13 data with empty
- December 13 orders lost
- Revenue reports wrong

**With proper Bronze**:
- Append added empty batch
- December 13 data still there
- Quick fix: exclude empty batch in Silver

```sql
-- Recovery query
SELECT * FROM bronze.orders
WHERE _batch_id != 'batch_20231214_empty'
```

### Story 2: The Bad Transform

**Problem**: Developer accidentally filtered out orders < $10.

**Without proper Bronze**:
- Bronze already filtered
- Small orders lost forever
- No way to recover

**With proper Bronze**:
- Bronze has all orders
- Fix Silver query
- Reload from Bronze

### Story 3: The Missing Precision

**Problem**: Timestamp truncated to date in Bronze.

**Without proper Bronze**:
- Time component lost
- Can't calculate delivery SLA to the hour
- Months of data affected

**With proper Bronze**:
- Full timestamp preserved
- Create new Silver column with hour precision
- No data loss

---

## Key Takeaways

| Anti-Pattern | Problem | Fix |
|--------------|---------|-----|
| Transforming in Bronze | Lose original data | Only add metadata columns |
| Overwriting | Lose history | Always append |
| Skipping metadata | Can't debug | Add _extracted_at, _source_file, _batch_id |

### The Bronze Mantra

**Bronze is your undo button. Don't break it.**

---

## Next Steps

With Bronze protected, let's look at Silver layer best practices:

- Centralization strategies
- Validation patterns
- Deduplication approaches

Next article: **Silver Layer Best Practices: Centralize, Validate, Deduplicate**.

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]
- **Medium:** Follow for more articles in this series
