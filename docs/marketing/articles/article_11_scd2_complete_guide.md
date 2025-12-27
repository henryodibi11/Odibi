# SCD2 Complete Guide: When and How to Track History

*The most powerful (and misused) pattern in dimensional modeling*

---

## TL;DR

SCD Type 2 tracks the complete history of dimension changes by creating new rows with effective dates. It's essential for "as-was" reporting but dangerous when overused-leading to history explosion and performance issues. This article covers when to use SCD2, how it works internally, proper configuration, and common gotchas including volatile columns, deduplication, and performance tuning.

---

## What is SCD2?

SCD stands for **Slowly Changing Dimension**. Type 2 is one of several strategies for handling changes to dimension data:

| Type | Strategy | Use Case |
|------|----------|----------|
| Type 0 | Never update | Static reference data |
| Type 1 | Overwrite | Current state only, no history |
| **Type 2** | Add new row | Full history tracking |
| Type 3 | Add column | Previous + current only |
| Type 6 | Hybrid (1+2+3) | Complex scenarios |

SCD2 creates a new row every time a tracked attribute changes, preserving the complete history.

---

## How SCD2 Works

Let's say a customer moves from São Paulo to Rio de Janeiro:

### Before (One Row)

| customer_sk | customer_id | city | is_current |
|-------------|-------------|------|------------|
| 42 | CUST-123 | São Paulo | true |

### After SCD2 Update (Two Rows)

| customer_sk | customer_id | city | is_current | valid_from | valid_to |
|-------------|-------------|------|------------|------------|----------|
| 42 | CUST-123 | São Paulo | false | 2018-01-15 | 2023-06-01 |
| 89 | CUST-123 | Rio de Janeiro | true | 2023-06-01 | 9999-12-31 |

Key points:
- **Old row closed**: `is_current = false`, `valid_to = 2023-06-01`
- **New row created**: `is_current = true`, new `customer_sk`
- **Natural key unchanged**: `customer_id = CUST-123` for both rows
- **Surrogate key changes**: New row gets new `customer_sk`

---

## When to Use SCD2

Use SCD2 when you need to answer questions about **historical state**:

✅ **Good Use Cases**

| Question | Why SCD2 Helps |
|----------|----------------|
| "What was the customer's address when they placed this order?" | Point-in-time lookup |
| "How long was this product in the 'Electronics' category?" | Duration analysis |
| "What was the sales rep assigned to this territory in Q3 2022?" | Historical attribution |

❌ **Bad Use Cases**

| Scenario | Problem | Better Approach |
|----------|---------|-----------------|
| Tracking every login timestamp | Creates billions of rows | Event log / fact table |
| Recording stock price changes | 1000+ changes per day | Time series, not SCD2 |
| System timestamp fields | Changes on every load | Exclude from tracking |

### The Rule of Thumb

**SCD2 is for slowly changing data**-things that change infrequently but meaningfully:
- Customer address changes: Maybe once a year
- Product category: Maybe never
- Employee department: A few times in a career

If it changes frequently (daily, hourly), it's not "slowly changing"-use a different pattern.

---

## Configuration Deep Dive

### Basic SCD2 Configuration

```yaml
- name: dim_customer
  description: "Customer dimension with history tracking"
  
  read:
    connection: silver
    path: customers
    format: delta
  
  pattern:
    type: dimension
    params:
      natural_key: customer_id
      surrogate_key: customer_sk
      scd_type: 2
      track_cols:
        - customer_city
        - customer_state
        - customer_segment
      target: gold.dim_customer  # Existing table to compare against
      unknown_member: true
      audit:
        load_timestamp: true
        source_system: "crm"
  
  write:
    connection: gold
    path: dim_customer
    format: delta
```

### Configuration Options

| Parameter | Required | Description |
|-----------|----------|-------------|
| `natural_key` | Yes | Business key for matching (e.g., customer_id) |
| `surrogate_key` | Yes | Name of SK column to generate |
| `scd_type` | Yes | Set to `2` for history tracking |
| `track_cols` | Yes (for SCD2) | Columns to monitor for changes |
| `target` | Yes (for SCD2) | Path to existing dimension table |
| `unknown_member` | No | Add SK=0 row for orphan handling |
| `hash_column` | No | Column name for change hash (default: `_row_hash`) |

### Generated Columns

SCD2 automatically adds these columns:

| Column | Type | Description |
|--------|------|-------------|
| `{surrogate_key}` | INT | Auto-generated surrogate key |
| `is_current` | BOOLEAN | True for current version |
| `valid_from` | TIMESTAMP | When this version became active |
| `valid_to` | TIMESTAMP | When this version was superseded |
| `_row_hash` | STRING | Hash of tracked columns (for change detection) |

---

## The SCD2 Algorithm

Here's what happens during an SCD2 load:

```
1. READ new data from source
2. HASH tracked columns for each row
3. COMPARE hashes with existing current records
4. For UNCHANGED records: Keep as-is
5. For NEW records: Insert with new SK, is_current=true
6. For CHANGED records:
   a. Close old row: is_current=false, valid_to=now
   b. Insert new row: new SK, is_current=true, valid_from=now
7. For DELETED records (optional): Close row, valid_to=now
```

### Visual Example

```
SOURCE DATA:                      EXISTING DIMENSION:
┌─────────────┬─────────┐        ┌────┬─────────────┬─────────┬─────────┐
│ customer_id │ city    │        │ sk │ customer_id │ city    │ current │
├─────────────┼─────────┤        ├────┼─────────────┼─────────┼─────────┤
│ CUST-001    │ NYC     │  -->   │ 1  │ CUST-001    │ NYC     │ true    │ (unchanged)
│ CUST-002    │ LA      │  -->   │ 2  │ CUST-002    │ SF      │ true    │ (changed!)
│ CUST-003    │ Chicago │  -->   │    │             │         │         │ (new!)
└─────────────┴─────────┘        └────┴─────────────┴─────────┴─────────┘

RESULT:
┌────┬─────────────┬─────────┬─────────┬────────────┬────────────┐
│ sk │ customer_id │ city    │ current │ valid_from │ valid_to   │
├────┼─────────────┼─────────┼─────────┼────────────┼────────────┤
│ 1  │ CUST-001    │ NYC     │ true    │ 2023-01-01 │ 9999-12-31 │ (kept)
│ 2  │ CUST-002    │ SF      │ false   │ 2023-01-01 │ 2023-06-15 │ (closed)
│ 3  │ CUST-002    │ LA      │ true    │ 2023-06-15 │ 9999-12-31 │ (new version)
│ 4  │ CUST-003    │ Chicago │ true    │ 2023-06-15 │ 9999-12-31 │ (new record)
└────┴─────────────┴─────────┴─────────┴────────────┴────────────┘
```

---

## Common Gotchas

### 1. Volatile Columns (History Explosion)

**Problem**: Including columns that change frequently

```yaml
# ❌ WRONG - updated_at changes on every load!
track_cols:
  - customer_name
  - customer_city
  - updated_at  # This creates a new row every time!
```

**Result**: New SCD2 row every load = millions of rows for no business value

**Solution**: Only track columns with business meaning

```yaml
# ✅ RIGHT - only track meaningful changes
track_cols:
  - customer_name
  - customer_city
  - customer_segment
# updated_at is NOT tracked
```

### 2. Source Duplicates

**Problem**: Source data has duplicate records for same natural key

```
SOURCE:
CUST-001, New York
CUST-001, New York  (duplicate!)
```

If not handled, SCD2 creates multiple "current" rows.

**Solution**: Deduplicate before SCD2

```yaml
- name: dim_customer
  read:
    connection: silver
    path: customers
  
  # Deduplicate FIRST
  transformer: deduplicate
  params:
    keys: [customer_id]
    order_by: _extracted_at DESC
  
  # Then apply SCD2
  pattern:
    type: dimension
    params:
      scd_type: 2
      # ...
```

### 3. Performance on Large Tables

**Problem**: SCD2 compares every incoming row against existing table

For a 10M row dimension with 100K daily updates:
- Hash 100K incoming rows
- Lookup 100K hashes in 10M row table
- Update/Insert operations

**Solution**: Partition and filter

```yaml
pattern:
  type: dimension
  params:
    scd_type: 2
    partition_by: customer_region  # Partition for performance
    incremental:
      column: _extracted_at
      lookback: "7 days"  # Only process recent records
```

### 4. NULL Handling in Tracked Columns

**Problem**: NULL → "value" or "value" → NULL triggers false changes

```
Day 1: customer_segment = NULL
Day 2: customer_segment = NULL  (NULLs are "different" in hashing)
```

**Solution**: Fill NULLs with known values before SCD2

```yaml
transform:
  steps:
    - function: fill_nulls
      params:
        columns:
          customer_segment: "Unknown"
          customer_tier: "Standard"

pattern:
  type: dimension
  params:
    scd_type: 2
    track_cols: [customer_segment, customer_tier]
```

### 5. Case Sensitivity

**Problem**: "new york" vs "New York" creates new history

```
Day 1: city = "New York"
Day 2: city = "NEW YORK"  (uppercase in source)
```

**Solution**: Standardize case before SCD2

```yaml
transform:
  steps:
    - function: clean_text
      params:
        columns: [customer_city]
        case: upper
        trim: true
```

---

## Debugging SCD2 Issues

### Check for Multiple Current Rows

```sql
-- Should return 0 rows
SELECT customer_id, COUNT(*) as current_count
FROM gold.dim_customer
WHERE is_current = true
GROUP BY customer_id
HAVING COUNT(*) > 1
```

### Check History Growth Rate

```sql
-- History versions per natural key
SELECT 
  customer_id,
  COUNT(*) as version_count,
  MIN(valid_from) as first_version,
  MAX(valid_from) as latest_version
FROM gold.dim_customer
GROUP BY customer_id
HAVING COUNT(*) > 5  -- Suspiciously many versions
ORDER BY version_count DESC
```

### Audit Column Changes

```sql
-- What changed?
SELECT 
  a.customer_id,
  a.customer_city as old_city,
  b.customer_city as new_city,
  a.valid_to as change_date
FROM gold.dim_customer a
JOIN gold.dim_customer b 
  ON a.customer_id = b.customer_id 
  AND a.valid_to = b.valid_from
WHERE a.customer_city != b.customer_city
ORDER BY a.valid_to DESC
LIMIT 100
```

---

## SCD2 in Fact Tables: Don't Do It

A common mistake: applying SCD2 to fact tables.

```yaml
# ❌ NEVER DO THIS
pattern:
  type: fact
  params:
    scd_type: 2  # Facts don't need SCD2!
```

Facts are **immutable events**. Once an order is placed, it doesn't change. If you need to track order updates:

1. Create an `order_events` fact table (append-only)
2. Use SCD2 on dimensions only
3. The latest `order_status` lives in a dimension, not the fact

---

## Full Example

Here's a complete SCD2 dimension configuration:

```yaml
- name: dim_customer
  description: "Customer dimension with SCD2 history tracking"
  
  read:
    connection: silver
    path: customers
    format: delta
  
  # Pre-processing: clean and deduplicate
  transformer: deduplicate
  params:
    keys: [customer_id]
    order_by: _extracted_at DESC
  
  transform:
    steps:
      # Standardize text to prevent false changes
      - function: clean_text
        params:
          columns: [customer_city, customer_state]
          case: upper
          trim: true
      
      # Fill NULLs to prevent hash issues
      - function: fill_nulls
        params:
          columns:
            customer_segment: "Unknown"
            customer_tier: "Standard"
      
      # Add derived attributes
      - function: derive_columns
        params:
          columns:
            customer_region: |
              CASE 
                WHEN customer_state IN ('SP', 'RJ', 'MG', 'ES') THEN 'Southeast'
                WHEN customer_state IN ('PR', 'SC', 'RS') THEN 'South'
                ELSE 'Other'
              END
  
  # Apply SCD2 pattern
  pattern:
    type: dimension
    params:
      natural_key: customer_id
      surrogate_key: customer_sk
      scd_type: 2
      track_cols:
        - customer_city
        - customer_state
        - customer_region
        - customer_segment
        - customer_tier
      target: gold.dim_customer
      unknown_member: true
      audit:
        load_timestamp: true
        source_system: "ecommerce"
  
  write:
    connection: gold
    path: dim_customer
    format: delta
```

---

## Key Takeaways

1. **SCD2 is for slowly changing data** - Not for high-frequency changes
2. **Choose tracked columns carefully** - Exclude volatile fields
3. **Deduplicate before SCD2** - Prevent multiple current rows
4. **Standardize data first** - Case, NULLs, whitespace
5. **Monitor history growth** - Excessive versions indicate problems
6. **Never use SCD2 on facts** - Facts are immutable events

---

## Next Steps

With SCD2 understood, we'll explore more dimension patterns:

- Unknown members and orphan handling
- Dimension keys and auditing
- Role-playing dimensions

Next article: **Dimension Table Patterns: Unknown Members, Keys, and Auditing**.

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]
- **Medium:** Follow for more articles in this series
