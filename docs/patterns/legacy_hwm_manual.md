# [Legacy] Manual High Water Mark (HWM)

> **⚠️ Deprecated Pattern**
>
> This manual pattern is no longer recommended.
> Please use the new [Stateful Incremental Loading](./incremental_stateful.md) feature which handles this automatically.

## What Is HWM?

A pattern for **incremental data loading**: load all data once on the first run, then load only new/changed data on each subsequent run.

**Day 1**: Load 10 years of history  
**Day 2+**: Load only today's new records

---

## The Pattern

### Configuration

```yaml
nodes:
  load_orders:
    read:
      connection: my_sql_server
      format: sql_server
      query: |
        SELECT
          order_id,
          customer_id,
          amount,
          created_at,
          updated_at
        FROM dbo.orders
        WHERE COALESCE(updated_at, created_at) >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE))

    write:
      connection: adls
      format: delta
      path: bronze/orders
      mode: append
      first_run_query: |
        SELECT
          order_id,
          customer_id,
          amount,
          created_at,
          updated_at
        FROM dbo.orders
      options:
        cluster_by: [created_at]
```

### How It Works

**Day 1** (table doesn't exist):
- Odibi runs `first_run_query`
- Loads ALL orders from source
- Creates table with clustering
- Write mode: OVERWRITE

**Day 2+** (table exists):
- Odibi runs regular `query`
- Loads only orders modified in last 1 day
- Appends to table
- Write mode: APPEND

---

## The Two Queries

### `first_run_query`
```sql
SELECT * FROM dbo.orders
```
- Loads everything
- Runs once on day 1
- Takes longer, but happens only once

### `query` (incremental)
```sql
SELECT * FROM dbo.orders
WHERE COALESCE(updated_at, created_at) >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE))
```
- Loads only new/updated records
- Runs every day from day 2 onward
- Takes seconds

---

## Key Concepts

### 1. Track Changes

Use `updated_at` if available. If not, use `created_at`.

```sql
-- Prefer updated_at (catches changes)
WHERE updated_at >= DATEADD(DAY, -1, GETDATE())

-- Fallback to created_at (new records only)
WHERE created_at >= DATEADD(DAY, -1, GETDATE())

-- Use both (catches new + modified)
WHERE COALESCE(updated_at, created_at) >= DATEADD(DAY, -1, GETDATE())
```

### 2. Time Ranges

Use overlapping time ranges (2 days instead of 1) to catch late-arriving data:

```sql
-- 1 day: misses records that arrived late
WHERE created_at >= DATEADD(DAY, -1, GETDATE())

-- 2 days: safer, catches late arrivals
WHERE created_at >= DATEADD(DAY, -2, GETDATE())
```

### 3. Clustering

On first run, apply clustering for query performance:

```yaml
options:
  cluster_by: [created_at]  # Applied day 1, speeds up incremental queries
```

---

## Setup Steps

1. **Identify HWM column** (`updated_at` or `created_at`)
2. **Write first_run_query** (`SELECT * FROM table`)
3. **Write incremental query** (`SELECT * WHERE timestamp >= date_function`)
4. **Add options** (`cluster_by` for performance)
5. **Deploy** and let Odibi handle the rest

---

## Example: Orders Table

### Source Data (dbo.orders)

```
order_id | customer_id | amount | created_at          | updated_at
1        | 100         | 99.99  | 2025-01-20 10:00:00 | NULL
2        | 101         | 49.99  | 2025-01-21 14:30:00 | 2025-01-21 15:00:00
3        | 102         | 199.99 | 2025-01-23 09:00:00 | NULL
```

### Odibi Configuration

```yaml
nodes:
  load_orders:
    read:
      connection: sql_server
      format: sql_server
      query: |
        SELECT order_id, customer_id, amount, created_at, updated_at
        FROM dbo.orders
        WHERE COALESCE(updated_at, created_at) >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE))

    write:
      connection: adls
      format: delta
      path: bronze/orders
      mode: append
      first_run_query: |
        SELECT order_id, customer_id, amount, created_at, updated_at
        FROM dbo.orders
      options:
        cluster_by: [created_at]
```

### Execution Timeline

**Day 1** (2025-01-20):
```
Run: first_run_query
Loads: All 3 orders
Write: OVERWRITE (creates table)
Bronze table: 3 rows
```

**Day 2** (2025-01-21):
```
Run: Regular query (WHERE COALESCE(...) >= 2025-01-20)
Loads: Order 1 (updated), Order 2 (new)
Write: APPEND
Bronze table: 5 rows (with duplicates of 1, 2)
```

**Day 3** (2025-01-22):
```
Run: Regular query (WHERE COALESCE(...) >= 2025-01-21)
Loads: Order 2 (updated timestamp)
Write: APPEND
Bronze table: 6 rows
```

**Day 4** (2025-01-23):
```
Run: Regular query (WHERE COALESCE(...) >= 2025-01-22)
Loads: Order 3 (new)
Write: APPEND
Bronze table: 7 rows
```

---

## Handling Duplicates

Since we append each day, you'll get duplicates in Bronze (which is fine—that's what Raw/Bronze is for):

```
order_id | created_at          | load_date
1        | 2025-01-20 10:00:00 | 2025-01-20  ← Day 1
1        | 2025-01-20 10:00:00 | 2025-01-21  ← Day 2 (duplicate)
2        | 2025-01-21 14:30:00 | 2025-01-21  ← Day 2
2        | 2025-01-21 14:30:00 | 2025-01-22  ← Day 3 (duplicate)
3        | 2025-01-23 09:00:00 | 2025-01-23  ← Day 4
```

**Silver layer** (merge/upsert) deduplicates later using the merge transformer.

---

## SQL Date Functions (by Database)

| Database | Syntax |
|----------|--------|
| SQL Server | `DATEADD(DAY, -1, CAST(GETDATE() AS DATE))` |
| PostgreSQL | `CURRENT_DATE - INTERVAL '1 day'` |
| MySQL | `DATE_SUB(CURDATE(), INTERVAL 1 DAY)` |
| Snowflake | `DATEADD(day, -1, CURRENT_DATE())` |
| BigQuery | `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` |

---

## Common Mistakes

### ❌ Single query for everything

```yaml
# Wrong: Won't capture first-run history
query: WHERE created_at >= DATEADD(DAY, -1, GETDATE())
```

### ✅ Two queries

```yaml
first_run_query: SELECT * FROM table
query: WHERE created_at >= DATEADD(DAY, -1, GETDATE())
```

---

### ❌ Using `>` instead of `>=`

```sql
-- Wrong: filters out today's data
WHERE created_at > DATEADD(DAY, -1, GETDATE())
```

### ✅ Using `>=` for inclusive range

```sql
-- Right: includes today
WHERE created_at >= DATEADD(DAY, -1, GETDATE())
```

---

### ❌ Only using created_at (misses updates)

```sql
-- Wrong: updated records not captured
WHERE created_at >= DATEADD(DAY, -1, GETDATE())
```

### ✅ Using COALESCE(updated_at, created_at)

```sql
-- Right: captures new AND updated
WHERE COALESCE(updated_at, created_at) >= DATEADD(DAY, -1, GETDATE())
```

---

## That's It

HWM is simple: **two queries, Odibi chooses which one runs**.

- First run: Odibi detects table doesn't exist, runs `first_run_query`
- Subsequent runs: Odibi detects table exists, runs regular `query`
- Automatic mode override: First run uses OVERWRITE, subsequent runs use APPEND

Works great for loading from any SQL database into Bronze.
