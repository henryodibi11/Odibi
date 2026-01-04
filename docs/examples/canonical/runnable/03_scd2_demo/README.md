# SCD2 History Tracking Demo

This example demonstrates **Slowly Changing Dimension Type 2 (SCD2)** history tracking.

SCD2 maintains a complete history of changes by:
- Closing the old row (setting `valid_to` and `is_current=false`)
- Creating a new row with the updated values (setting `is_current=true`)

## Prerequisites

```bash
pip install odibi
```

## Step-by-Step Walkthrough

### Step 1: Initial Load

Run the initial load with original customer data:

```bash
cd docs/examples/canonical/runnable/03_scd2_demo
odibi run step1_initial_load.yaml
```

**Source Data (customers_v1.csv):**

| customer_id | name  | tier   | updated_at |
|-------------|-------|--------|------------|
| 1           | Alice | Gold   | 2025-01-01 |
| 2           | Bob   | Silver | 2025-01-01 |

**Expected Output (dim_customer):**

| customer_sk | customer_id | name    | tier    | valid_from | valid_to | is_current |
|-------------|-------------|---------|---------|------------|----------|------------|
| 0           | _UNKNOWN    | Unknown | Unknown | 1900-01-01 | NULL     | true       |
| 1           | 1           | Alice   | Gold    | 2025-01-01 | NULL     | true       |
| 2           | 2           | Bob     | Silver  | 2025-01-01 | NULL     | true       |

All rows have `is_current=true` and `valid_to=NULL` (open-ended).

### Step 2: Process Changes

Alice's tier changed from **Gold** to **Platinum**. Run the second config:

```bash
odibi run step2_with_changes.yaml
```

**Source Data (customers_v2.csv):**

| customer_id | name  | tier       | updated_at |
|-------------|-------|------------|------------|
| 1           | Alice | **Platinum** | 2025-01-15 |
| 2           | Bob   | Silver     | 2025-01-01 |

**Expected Output (dim_customer):**

| customer_sk | customer_id | name    | tier       | valid_from | valid_to   | is_current |
|-------------|-------------|---------|------------|------------|------------|------------|
| 0           | _UNKNOWN    | Unknown | Unknown    | 1900-01-01 | NULL       | true       |
| 1           | 1           | Alice   | Gold       | 2025-01-01 | 2025-01-15 | **false**  |
| 2           | 2           | Bob     | Silver     | 2025-01-01 | NULL       | true       |
| 3           | 1           | Alice   | **Platinum** | 2025-01-15 | NULL       | **true**   |

Notice:
- **Row 1 (Alice - Gold):** `valid_to` set to 2025-01-15, `is_current=false`
- **Row 3 (Alice - Platinum):** New row created with new surrogate key, `is_current=true`
- **Row 2 (Bob):** Unchanged, still `is_current=true`

## Key Concepts

### Tracked Columns

The `track_cols` parameter specifies which columns trigger a new version:

```yaml
track_cols:
  - name
  - email
  - tier
  - city
```

If any of these columns change, a new SCD2 version is created.

### Natural Key vs Surrogate Key

- **Natural Key** (`customer_id`): Business identifier that stays constant
- **Surrogate Key** (`customer_sk`): System-generated key, unique per version

### Query Patterns

**Current state only:**
```sql
SELECT * FROM dim_customer WHERE is_current = true
```

**Point-in-time lookup:**
```sql
SELECT * FROM dim_customer 
WHERE customer_id = 1 
  AND '2025-01-10' BETWEEN valid_from AND COALESCE(valid_to, '9999-12-31')
```

**Full history:**
```sql
SELECT * FROM dim_customer WHERE customer_id = 1 ORDER BY valid_from
```

## Files in This Demo

| File | Description |
|------|-------------|
| `customers_v1.csv` | Initial customer data |
| `customers_v2.csv` | Changed data (Alice: Gold → Platinum) |
| `step1_initial_load.yaml` | Creates initial dimension |
| `step2_with_changes.yaml` | Processes changes, creates history |
