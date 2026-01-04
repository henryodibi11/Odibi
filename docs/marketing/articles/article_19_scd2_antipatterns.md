# SCD2 Done Wrong: History Explosion and How to Prevent It

*When change tracking becomes a disaster*

---

## TL;DR

SCD2 is powerful but dangerous when misused. The most common mistake is tracking volatile columns that change frequently, causing "history explosion"-millions of unnecessary rows. This article covers the warning signs, common causes, and how to fix an exploding dimension.

---

## The History Explosion Problem

You implement SCD2 on your customer dimension. Day 1: 100,000 rows.

Six months later: 50,000,000 rows.

What happened?

```
Day 1:    100,000 customers × 1 version = 100,000 rows
Month 1:  100,000 customers × 3 versions = 300,000 rows
Month 6:  100,000 customers × 500 versions = 50,000,000 rows
```

You're creating new versions so fast that your dimension is growing exponentially.

---

## Warning Signs

### 1. Dimension Growing Faster Than Source

```sql
-- Check growth rate
SELECT 
  DATE(load_timestamp) as load_date,
  COUNT(*) as rows_added,
  SUM(COUNT(*)) OVER (ORDER BY DATE(load_timestamp)) as cumulative
FROM dim_customer
GROUP BY DATE(load_timestamp)
ORDER BY load_date
```

If `rows_added` is consistently high, something is wrong.

### 2. Many Versions Per Entity

```sql
-- Average versions per customer
SELECT 
  AVG(version_count) as avg_versions,
  MAX(version_count) as max_versions
FROM (
  SELECT customer_id, COUNT(*) as version_count
  FROM dim_customer
  GROUP BY customer_id
)
```

Healthy: 1-5 versions average
Problematic: 50+ versions average

### 3. Same-Day Version Changes

```sql
-- Multiple versions on same day
SELECT customer_id, DATE(valid_from), COUNT(*)
FROM dim_customer
GROUP BY customer_id, DATE(valid_from)
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC
```

Multiple versions per day is a red flag.

---

## Common Causes

### Cause 1: Tracking Volatile Columns

The #1 cause. You track columns that change frequently but have no business value.

```yaml
# ❌ WRONG: Tracking updated_at
pattern:
  type: dimension
  params:
    scd_type: 2
    track_cols:
      - customer_name
      - customer_city
      - updated_at  # EXPLODES! Changes every load!
```

`updated_at` changes on every source system update. That triggers a new SCD2 version every time.

**Fix**: Never track system timestamps.

```yaml
# ✅ RIGHT: Only track business columns
pattern:
  type: dimension
  params:
    scd_type: 2
    track_cols:
      - customer_name
      - customer_city
      # updated_at excluded!
```

### Cause 2: Tracking Derived/Calculated Columns

```yaml
# ❌ WRONG: Tracking calculated fields
track_cols:
  - customer_name
  - customer_lifetime_value  # Recalculated daily!
  - days_since_last_order    # Changes daily!
```

Calculated fields change by definition. Don't track them.

**Fix**: Keep calculated metrics in facts or separate tables.

### Cause 3: Case/Whitespace Sensitivity

```
Day 1: customer_name = "John Smith"
Day 2: customer_name = "John Smith "  # Trailing space!
Day 3: customer_name = "JOHN SMITH"   # Different case!
```

Each creates a new SCD2 version.

**Fix**: Standardize before SCD2.

```yaml
- name: dim_customer
  # Clean BEFORE SCD2
  transform:
    steps:
      - function: clean_text
        params:
          columns: [customer_name]
          case: upper
          trim: true
  
  pattern:
    type: dimension
    params:
      scd_type: 2
      track_cols: [customer_name]
```

### Cause 4: NULL Variations

```
Day 1: segment = NULL
Day 2: segment = NULL  # NULLs hash differently!
```

NULL handling in hashing can cause false changes.

**Fix**: Fill NULLs before SCD2.

```yaml
transform:
  steps:
    - function: fill_nulls
      params:
        columns:
          segment: "Unknown"
          tier: "Standard"
```

### Cause 5: Floating Point Precision

```
Day 1: latitude = 40.7127999999
Day 2: latitude = 40.7128000001  # Precision change!
```

Floating point representation can vary.

**Fix**: Round numeric columns.

```yaml
transform:
  steps:
    - sql: |
        SELECT 
          *,
          ROUND(latitude, 4) as latitude,
          ROUND(longitude, 4) as longitude
        FROM df
```

---

## Diagnosis: Which Column Is Exploding?

Find the culprit:

```sql
-- Compare consecutive versions
WITH versions AS (
  SELECT 
    customer_id,
    customer_name,
    customer_city,
    customer_segment,
    load_timestamp,
    LAG(customer_name) OVER (PARTITION BY customer_id ORDER BY valid_from) as prev_name,
    LAG(customer_city) OVER (PARTITION BY customer_id ORDER BY valid_from) as prev_city,
    LAG(customer_segment) OVER (PARTITION BY customer_id ORDER BY valid_from) as prev_segment
  FROM dim_customer
)
SELECT 
  SUM(CASE WHEN customer_name != prev_name THEN 1 ELSE 0 END) as name_changes,
  SUM(CASE WHEN customer_city != prev_city THEN 1 ELSE 0 END) as city_changes,
  SUM(CASE WHEN customer_segment != prev_segment THEN 1 ELSE 0 END) as segment_changes
FROM versions
WHERE prev_name IS NOT NULL
```

The column with the most changes is your problem.

---

## Fixing an Exploded Dimension

### Step 1: Identify True Changes

Rebuild with only meaningful columns:

```yaml
- name: dim_customer_rebuild
  read:
    connection: gold
    path: dim_customer
  
  transform:
    steps:
      # Dedupe to first occurrence of each meaningful state
      - sql: |
          SELECT *
          FROM (
            SELECT *,
              ROW_NUMBER() OVER (
                PARTITION BY customer_id, customer_name, customer_city
                ORDER BY valid_from
              ) as rn
            FROM df
          )
          WHERE rn = 1
```

### Step 2: Recalculate Valid Ranges

```sql
WITH deduped AS (
  -- Your deduped data
),
with_next AS (
  SELECT 
    *,
    LEAD(valid_from) OVER (
      PARTITION BY customer_id 
      ORDER BY valid_from
    ) as next_valid_from
  FROM deduped
)
SELECT 
  customer_sk,
  customer_id,
  customer_name,
  customer_city,
  valid_from,
  COALESCE(next_valid_from, '9999-12-31') as valid_to,
  CASE WHEN next_valid_from IS NULL THEN true ELSE false END as is_current
FROM with_next
```

### Step 3: Replace Production Table

```yaml
- name: dim_customer_fixed
  # ... rebuild logic ...
  
  write:
    connection: gold
    path: dim_customer_new  # Write to new table first
    
# Then swap:
# 1. Rename dim_customer to dim_customer_backup
# 2. Rename dim_customer_new to dim_customer
```

---

## Prevention: The SCD2 Checklist

Before enabling SCD2, ask:

### ✅ Does this column change slowly?

- Customer address: Yes (maybe once a year)
- Last login timestamp: No (changes constantly)

### ✅ Is the change business-meaningful?

- Customer segment change: Yes (affects pricing)
- Record update timestamp: No (just bookkeeping)

### ✅ Will users query by this column?

- "Show me this customer's address history": Yes
- "Show me timestamp changes": No

### ✅ Is the data already clean?

- Standardized case? ✓
- Trimmed whitespace? ✓
- NULLs handled? ✓
- Precision controlled? ✓

---

## When NOT to Use SCD2

| Scenario | Use Instead |
|----------|-------------|
| Column changes daily | SCD1 (overwrite) |
| Column changes hourly | Event log / fact table |
| Column is calculated | Store in separate table |
| Column is a timestamp | Don't track at all |
| No one needs history | SCD1 |

### Example: Use SCD1 Instead

```yaml
# Customer loyalty points - changes too often for SCD2
pattern:
  type: dimension
  params:
    scd_type: 1  # Just overwrite
    track_cols: []  # No history tracking
```

### Example: Use Event Log Instead

```yaml
# Status changes - every change matters
- name: fact_customer_status_changes
  pattern:
    type: fact
    params:
      grain: [customer_id, change_timestamp]
      measures:
        - status_before
        - status_after
        - change_reason
```

---

## The Right Way to Configure SCD2

```yaml
- name: dim_customer
  description: "Customer dimension with controlled SCD2"
  
  # Clean data first
  transform:
    steps:
      # Standardize case
      - function: clean_text
        params:
          columns: [customer_name, customer_city]
          case: upper
          trim: true
      
      # Fill NULLs
      - function: fill_nulls
        params:
          columns:
            customer_segment: "Unknown"
      
      # Round floats
      - sql: |
          SELECT *, ROUND(latitude, 4) as latitude
          FROM df
  
  # Then apply SCD2 with minimal tracked columns
  pattern:
    type: dimension
    params:
      natural_key: customer_id
      surrogate_key: customer_sk
      scd_type: 2
      track_cols:
        - customer_name       # Rarely changes
        - customer_city       # Rarely changes
        - customer_segment    # Changes quarterly
        # NOT: updated_at, login_count, points_balance
      target: gold.dim_customer
```

---

## Monitoring SCD2 Health

Set up alerts:

```yaml
# In your pipeline
post_sql:
  - |
    INSERT INTO system.metrics
    SELECT 
      'dim_customer' as table_name,
      'avg_versions' as metric,
      AVG(cnt) as value,
      CURRENT_TIMESTAMP() as measured_at
    FROM (
      SELECT customer_id, COUNT(*) as cnt
      FROM dim_customer
      GROUP BY customer_id
    )
```

Alert if `avg_versions > 10`.

---

## Key Takeaways

| Problem | Cause | Fix |
|---------|-------|-----|
| Dimension exploding | Tracking volatile columns | Remove from track_cols |
| Daily version changes | Tracking timestamps | Never track system fields |
| Same-row re-versioning | Case/whitespace differences | Standardize before SCD2 |
| NULL changes | Inconsistent NULL handling | Fill NULLs before SCD2 |

### The SCD2 Rule

**Only track columns that:**
1. Change slowly (yearly, not daily)
2. Have business meaning
3. Users actually query historically

---

## Next Steps

Beyond SCD2, there are other performance killers. Let's cover:

- Query anti-patterns
- Shuffle explosions
- Join disasters

Next article: **Performance Anti-Patterns: Why Your Pipeline Takes Hours**.

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]
- **LinkedIn:** Follow for more articles in this series
