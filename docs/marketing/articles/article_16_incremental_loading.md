# Incremental Loading: Watermarks, Merge, and Append

*Stop reprocessing your entire history every day*

---

## TL;DR

Full loads reprocess everything. Incremental loads process only what's new or changed. This article covers the three incremental patterns: append (add new rows), watermark (filter by timestamp), and merge (upsert changed records). You'll learn when to use each, how to configure them in Odibi, and how to handle late-arriving data.

---

## The Problem: Full Loads Don't Scale

You have 1 billion rows in your fact table. Every day, 1 million new records arrive.

**Full load approach**:
- Read 1 billion rows
- Transform 1 billion rows
- Write 1 billion rows
- Time: 4 hours

**Incremental approach**:
- Read 1 million new rows
- Transform 1 million new rows
- Append 1 million rows
- Time: 10 minutes

Same result, 24x faster.

---

## Three Incremental Patterns

| Pattern | How It Works | Use Case |
|---------|--------------|----------|
| **Append** | Add new rows only | Immutable event data |
| **Watermark** | Filter by timestamp | New records with monotonic timestamp |
| **Merge (Upsert)** | Update existing + insert new | Changing records with business key |

---

## Pattern 1: Append

The simplest pattern. Just add new rows.

### When to Use

- Event logs (clicks, transactions, sensor readings)
- Facts that never change
- Source provides only new records

### Configuration

```yaml
- name: bronze_events
  description: "Append new events"
  
  read:
    connection: landing
    path: events/*.parquet
    format: parquet
  
  write:
    connection: bronze
    path: events
    format: delta
    mode: append  # Key setting
```

### How It Works

```
Before:
| event_id | timestamp           | value |
|----------|---------------------|-------|
| 1        | 2023-12-14 10:00:00 | 100   |
| 2        | 2023-12-14 11:00:00 | 200   |

New Data:
| event_id | timestamp           | value |
|----------|---------------------|-------|
| 3        | 2023-12-15 10:00:00 | 150   |

After (Append):
| event_id | timestamp           | value |
|----------|---------------------|-------|
| 1        | 2023-12-14 10:00:00 | 100   |
| 2        | 2023-12-14 11:00:00 | 200   |
| 3        | 2023-12-15 10:00:00 | 150   |  ← Added
```

### Gotcha: Duplicate Prevention

If the source resends data, append creates duplicates.

**Solution 1**: Deduplicate after append

```yaml
- name: bronze_events
  write:
    mode: append

- name: silver_events
  depends_on: [bronze_events]
  transformer: deduplicate
  params:
    keys: [event_id]
    order_by: _extracted_at DESC
```

**Solution 2**: Merge instead of append (if source resends frequently)

---

## Pattern 2: Watermark (High-Water Mark)

Track the maximum timestamp processed, only read newer records.

### When to Use

- Source has monotonically increasing timestamp
- You want to filter at read time (efficiency)
- Source is large, but new data is small

### Configuration

```yaml
- name: silver_orders
  description: "Incremental load using watermark"
  
  read:
    connection: bronze
    path: orders
    format: delta
    incremental:
      mode: stateful       # Remember last high-water mark
      column: order_date   # Timestamp column to track
      lookback: "2 days"   # Safety buffer for late data
```

### How It Works

```
Run 1 (First Load):
- Target doesn't exist
- Read ALL data (ignore watermark)
- Write all rows
- Save HWM = MAX(order_date) = 2023-12-14

Run 2 (Incremental):
- Read HWM = 2023-12-14
- Filter: order_date > 2023-12-14 - 2 days
- Process only filtered rows
- Update HWM = MAX(order_date) from new data
```

### The Lookback Buffer

Why `lookback: "2 days"`?

Data can arrive late. An order from December 13 might arrive on December 15.

Without lookback:
- HWM = 2023-12-14
- Filter: order_date > 2023-12-14
- Late order (2023-12-13) is missed!

With `lookback: "2 days"`:
- Filter: order_date > 2023-12-12
- Late order is captured
- Merge handles duplicates

### Stateful vs Rolling

| Mode | Behavior | Use Case |
|------|----------|----------|
| **Stateful** | Track HWM in system table | Production pipelines |
| **Rolling** | Always use NOW() - lookback | Simple, no state |

```yaml
# Stateful (recommended)
incremental:
  mode: stateful
  column: updated_at

# Rolling (simpler)
incremental:
  mode: rolling
  column: updated_at
  lookback: "7 days"  # Always process last 7 days
```

---

## Pattern 3: Merge (Upsert)

Update existing rows, insert new ones. The most flexible pattern.

### When to Use

- Source data can change (updates, corrections)
- You need exactly one row per business key
- SCD1 dimensions

### Configuration

```yaml
- name: silver_customers
  description: "Merge customer updates"
  
  read:
    connection: bronze
    path: customers
    format: delta
  
  transformer: merge
  params:
    target: silver.customers
    keys: [customer_id]  # Match on business key
    
    # Optional: Only merge if something changed
    condition: |
      source.updated_at > target.updated_at
    
    # What to do on match
    when_matched: update
    
    # What to do on no match
    when_not_matched: insert
```

### How It Works

```
Target (Before):
| customer_id | name      | city       | updated_at |
|-------------|-----------|------------|------------|
| CUST-001    | John      | São Paulo  | 2023-12-01 |
| CUST-002    | Maria     | Rio        | 2023-12-01 |

Source (New Data):
| customer_id | name      | city       | updated_at |
|-------------|-----------|------------|------------|
| CUST-001    | John      | Brasília   | 2023-12-15 |  ← Changed
| CUST-003    | Pedro     | Salvador   | 2023-12-15 |  ← New

Target (After Merge):
| customer_id | name      | city       | updated_at |
|-------------|-----------|------------|------------|
| CUST-001    | John      | Brasília   | 2023-12-15 |  ← Updated
| CUST-002    | Maria     | Rio        | 2023-12-01 |  ← Unchanged
| CUST-003    | Pedro     | Salvador   | 2023-12-15 |  ← Inserted
```

### Merge Strategies

```yaml
# Update all columns on match
when_matched: update

# Update only specific columns
when_matched:
  update:
    columns: [name, city, updated_at]

# Update only if condition met
when_matched:
  update:
    condition: "source.updated_at > target.updated_at"

# Delete matching rows
when_matched: delete

# Do nothing (keep target)
when_matched: ignore
```

### Soft Deletes

Detect and mark deleted records:

```yaml
transformer: merge
params:
  target: silver.customers
  keys: [customer_id]
  
  when_matched: update
  when_not_matched: insert
  
  # Mark records in target that aren't in source
  when_not_matched_by_source:
    update:
      columns:
        is_deleted: true
        deleted_at: "current_timestamp()"
```

---

## Combining Patterns

Often you need multiple patterns together:

### Watermark + Merge

```yaml
- name: silver_orders
  description: "Incremental watermark with merge for updates"
  
  read:
    connection: bronze
    path: orders
    format: delta
    incremental:
      mode: stateful
      column: updated_at
      lookback: "3 days"
  
  transformer: merge
  params:
    target: silver.orders
    keys: [order_id]
    when_matched:
      update:
        condition: "source.updated_at > target.updated_at"
    when_not_matched: insert
```

This:
1. Reads only recent records (watermark)
2. Updates existing orders that changed (merge)
3. Inserts new orders (merge)

### Watermark + Append + Dedupe

```yaml
# Bronze: Append everything
- name: bronze_events
  read:
    connection: landing
    path: events/
  write:
    mode: append

# Silver: Deduplicate
- name: silver_events
  read:
    connection: bronze
    path: events
    incremental:
      mode: stateful
      column: _extracted_at
      lookback: "1 day"
  
  transformer: deduplicate
  params:
    keys: [event_id]
    order_by: event_timestamp DESC
  
  transformer: merge
  params:
    target: silver.events
    keys: [event_id]
```

---

## First Run vs Subsequent Runs

Odibi handles the first run automatically:

### First Run (Target Doesn't Exist)

```yaml
read:
  incremental:
    mode: stateful
    column: order_date
    lookback: "2 days"
```

Behavior:
1. Target table doesn't exist
2. Ignore incremental filter
3. Read ALL source data
4. Create target table
5. Initialize HWM

### First Run Override

For large historical data, limit the first run:

```yaml
read:
  incremental:
    mode: stateful
    column: order_date
    lookback: "2 days"
    first_run_query: |
      SELECT * FROM source
      WHERE order_date >= '2023-01-01'  # Only load 1 year
```

---

## Late-Arriving Data

Data arrives late when:
- Batch files are delayed
- Source systems have sync lag
- Time zone issues

### Strategy 1: Lookback Window

```yaml
incremental:
  lookback: "3 days"  # Reprocess last 3 days every run
```

Pros: Simple, catches late data
Cons: Reprocesses data unnecessarily

### Strategy 2: Restatement Table

Track late arrivals explicitly:

```yaml
- name: detect_late_arrivals
  description: "Find records that arrived late"
  
  transform:
    steps:
      - sql: |
          SELECT *
          FROM bronze.orders
          WHERE _extracted_at > order_date + INTERVAL '2 days'
  
  write:
    connection: silver
    path: late_arrivals_log
    mode: append
```

### Strategy 3: Partition-Based Reprocessing

```yaml
read:
  incremental:
    mode: stateful
    column: order_date
    partition_column: order_date  # Reprocess entire partition
```

When late data arrives for Dec 13, reprocess the entire Dec 13 partition.

---

## State Management

Watermark state is stored in the system catalog:

```yaml
system:
  connection: bronze
  path: _system
```

State table structure:

| pipeline | node | state_key | state_value | updated_at |
|----------|------|-----------|-------------|------------|
| silver | orders | high_water_mark | 2023-12-15 | 2023-12-15 09:30:00 |
| silver | customers | high_water_mark | 2023-12-15 | 2023-12-15 09:35:00 |

### Resetting State

Force a full reload by resetting the watermark:

```python
# Reset watermark
odibi reset-state --pipeline silver --node orders

# Or delete from state table
spark.sql("""
  DELETE FROM system._state
  WHERE pipeline = 'silver' AND node = 'orders'
""")
```

---

## Performance Optimization

### Partition Pruning

Structure data to enable partition pruning:

```yaml
write:
  connection: silver
  path: orders
  format: delta
  partition_by: [order_year, order_month]
```

Incremental reads then only scan relevant partitions.

### Z-Ordering (Delta Lake)

Optimize for common filter columns:

```yaml
write:
  connection: silver
  path: orders
  format: delta
  z_order_by: [customer_id, order_date]
```

### Skip Full Scans

Use file statistics to skip unnecessary files:

```yaml
read:
  connection: bronze
  path: orders
  format: delta
  incremental:
    use_file_stats: true  # Skip files without matching data
```

---

## Complete Example

Here's a production-ready incremental pipeline:

```yaml
pipelines:
  - pipeline: silver_incremental
    layer: silver
    description: "Incremental silver layer processing"
    
    nodes:
      # Append-only events
      - name: silver_page_views
        description: "Append new page view events"
        read:
          connection: bronze
          path: page_views
          format: delta
          incremental:
            mode: stateful
            column: event_timestamp
            lookback: "1 hour"
        write:
          connection: silver
          path: page_views
          format: delta
          mode: append
          partition_by: [event_date]
      
      # Merge with updates
      - name: silver_customers
        description: "Merge customer updates"
        read:
          connection: bronze
          path: customers
          format: delta
          incremental:
            mode: stateful
            column: updated_at
            lookback: "2 days"
        transformer: merge
        params:
          target: silver.customers
          keys: [customer_id]
          when_matched:
            update:
              condition: "source.updated_at > target.updated_at"
          when_not_matched: insert
        write:
          connection: silver
          path: customers
          format: delta
      
      # Watermark with dedup
      - name: silver_orders
        description: "Incremental orders with deduplication"
        read:
          connection: bronze
          path: orders
          format: delta
          incremental:
            mode: stateful
            column: _extracted_at
            lookback: "3 days"
        transformer: deduplicate
        params:
          keys: [order_id]
          order_by: _extracted_at DESC
        transformer: merge
        params:
          target: silver.orders
          keys: [order_id]
          when_matched:
            update:
              condition: "source._extracted_at > target._extracted_at"
          when_not_matched: insert
        write:
          connection: silver
          path: orders
          format: delta
          partition_by: [order_year, order_month]
```

---

## Key Takeaways

1. **Append for immutable data** - Events, logs, transactions
2. **Watermark for efficient filtering** - Read only new data
3. **Merge for changing data** - Update + insert in one operation
4. **Lookback handles late data** - Safety buffer for stragglers
5. **Combine patterns as needed** - Watermark + Merge is common
6. **Monitor state** - Know your high-water marks

---

## Next Steps

We've covered the core patterns. Now we'll explore anti-patterns and troubleshooting:

- Common mistakes in Bronze, Silver, Gold layers
- Performance anti-patterns
- Debugging strategies

Next article: **Bronze Layer Anti-Patterns: 3 Mistakes That Will Haunt You**.

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]
- **Medium:** Follow for more articles in this series
