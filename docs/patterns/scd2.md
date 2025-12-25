# SCD Type 2 (Slowly Changing Dimensions)

The **SCD Type 2** pattern allows you to track the full history of changes for a record over time. Unlike a simple update (which overwrites the old value), SCD2 keeps the old version and adds a new version, managing effective dates for you.

## The "Time Machine" Concept

**Business Problem:**
"I need to know what the customer's address was *last month*, not just where they live now."

**The Solution:**
Each record has an "effective window" (`effective_time` to `end_time`) and a flag (`is_current`) indicating if it is the latest version.

### Visual Example

**Input (Source Update):**
*Customer 101 moved to NY on Feb 1st.*

| customer_id | address | tier | txn_date   |
|-------------|---------|------|------------|
| 101         | NY      | Gold | 2024-02-01 |

**Target Table (Before):**
*Customer 101 lived in CA since Jan 1st.*

| customer_id | address | tier | txn_date   | valid_to | is_active |
|-------------|---------|------|------------|----------|-----------|
| 101         | CA      | Gold | 2024-01-01 | NULL     | true      |

**Target Table (After SCD2):**
*Old record CLOSED (valid_to set). New record OPEN (is_active=true).*

| customer_id | address | tier | txn_date   | valid_to   | is_active |
|-------------|---------|------|------------|------------|-----------|
| 101         | CA      | Gold | 2024-01-01 | 2024-02-01 | false     |
| 101         | NY      | Gold | 2024-02-01 | NULL       | true      |

---

## Configuration

Use the `scd2` transformer in your pipeline node.

### Option 1: Using Table Name

```yaml
nodes:
  - name: "dim_customers"
    # ... (read from source) ...

    transformer: "scd2"
    params:
      target: "silver.dim_customers"   # Registered table name
      keys: ["customer_id"]            # Unique ID
      track_cols: ["address", "tier"]  # Changes here trigger a new version
      effective_time_col: "txn_date"   # When the change happened

    write:
      table: "silver.dim_customers"
      format: "delta"
      mode: "overwrite"                # Important: SCD2 returns FULL history
```

### Option 2: Using Connection + Path (ADLS)

```yaml
nodes:
  - name: "dim_customers"
    # ... (read from source) ...

    transformer: "scd2"
    params:
      connection: adls_prod            # READ existing history from here
      path: OEE/silver/dim_customers
      keys: ["customer_id"]
      track_cols: ["address", "tier"]
      effective_time_col: "txn_date"

    write:
      connection: adls_prod            # WRITE result back (same location)
      path: OEE/silver/dim_customers
      format: "delta"
      mode: "overwrite"
```

> **Why specify the path twice?**
> - `params.connection/path` → Where to **read** existing history (to detect changes)
> - `write.connection/path` → Where to **write** the full result
>
> They should typically match. SCD2 returns a DataFrame (doesn't write directly),
> so the write phase handles persistence separately.

### Full Configuration

```yaml
transformer: "scd2"
params:
  target: "silver.dim_customers"       # OR use connection + path
  keys: ["customer_id"]
  track_cols: ["address", "tier", "email"]

  # Source column for start date
  effective_time_col: "updated_at"

  # Target columns to manage (optional defaults shown)
  end_time_col: "valid_to"
  current_flag_col: "is_current"
```

---

## How It Works

The `scd2` transformer performs a complex set of operations automatically:

1.  **Match**: Finds existing records in the `target` table using `keys`.
2.  **Compare**: Checks `track_cols` to see if any data has changed.
3.  **Close**: If a record changed, it updates the *old* record's `end_time_col` to equal the new record's `effective_time_col`, and sets `is_current = false`.
4.  **Insert**: It adds the *new* record with `effective_time_col` as the start date, `NULL` as the end date, and `is_current = true`.
5.  **Preserve**: It keeps all unchanged history records as they are.

## Important Notes

*   **Write Mode**: You must use `mode: overwrite` for the write operation following this transformer. The transformer constructs the *complete* new state of the history table (including old closed records and new open records).
*   **Target Existence**: If the target table doesn't exist (first run), the transformer simply prepares the source data (adds valid_to/is_current columns) and returns it.
*   **Engine Support**: Works on both Spark (Delta Lake) and Pandas (Parquet/CSV).

## When to Use

*   **Dimension Tables**: Customer dimensions, Product dimensions where attributes change slowly over time.
*   **Audit Trails**: When you need exact historical state reconstruction.

## When NOT to Use

*   **Fact Tables**: Events (Transactions, Logs) are immutable; they don't change state, they just occur. Use `append` instead.
*   **Rapidly Changing Data**: If a record changes 100 times a day, SCD2 will explode your storage size. Use a snapshot or aggregate approach instead.

---

## Common Errors and Debugging

This section covers the most common SCD2 errors and how to fix them.

### Error: `effective_time_col` Not Found

**Error Message:**
```
KeyError: 'updated_at'
# or
AnalysisException: Column 'updated_at' does not exist
```

**What It Means (Plain English):**
The `effective_time_col` you specified doesn't exist in your **source** DataFrame.

**Why It Happens:**
- The column name is misspelled
- The column was renamed or dropped upstream
- Case sensitivity mismatch (`Updated_At` vs `updated_at`)

**Step-by-Step Fix:**

1. **Check your source data columns:**
   ```python
   # Add this before the SCD2 node to debug
   df = spark.read.format("delta").load("bronze/customers")
   print(df.columns)
   # Output: ['customer_id', 'name', 'UpdatedAt', 'address']
   # Aha! It's 'UpdatedAt', not 'updated_at'
   ```

2. **Fix the YAML:**
   ```yaml
   # BEFORE (wrong)
   params:
     effective_time_col: "updated_at"  # ❌ Doesn't exist
   
   # AFTER (correct)
   params:
     effective_time_col: "UpdatedAt"  # ✅ Matches actual column
   ```

**Important:** The `effective_time_col` must exist in the **SOURCE** data, not the target. After SCD2 processing, this column gets used to populate the history columns.

---

### Error: `track_cols` Column Mismatch

**Error Message:**
```
KeyError: 'email'
# or
Column 'Email' not found in schema
```

**What It Means:**
One of the columns in `track_cols` doesn't exist in your source data, or there's a case mismatch.

**Why It Happens:**
- Column names are **case-sensitive**
- A column was renamed in the source system
- You're tracking a column that doesn't exist yet

**Step-by-Step Fix:**

1. **List actual columns:**
   ```python
   df = spark.read.format("delta").load("bronze/customers")
   print(df.columns)
   # ['customer_id', 'Name', 'Email', 'Address']
   ```

2. **Match case exactly:**
   ```yaml
   # BEFORE (wrong - case doesn't match)
   params:
     track_cols: ["name", "email", "address"]
   
   # AFTER (correct - matches actual columns)
   params:
     track_cols: ["Name", "Email", "Address"]
   ```

💡 **Pro Tip:** Consider normalizing column names to lowercase in Bronze/Silver to avoid case issues:
```yaml
transform:
  steps:
    - function: "rename_columns"
      params:
        lowercase: true
```

---

### Error: Schema Evolution Issues

**Error Message:**
```
AnalysisException: A]chema mismatch detected:
- Expected: customer_id: string, name: string, address: string, ...
- Actual:   customer_id: string, name: string, phone: string, ...
```

**What It Means:**
Your target table (from a previous run) has a different schema than the new data.

**Why It Happens:**
- Source added new columns (e.g., `phone`)
- Source removed columns (e.g., dropped `address`)
- Column types changed (e.g., `int` → `string`)

**Step-by-Step Fix:**

**Option 1: Allow Schema Merging**
```yaml
write:
  connection: silver
  table: dim_customers
  format: delta
  mode: overwrite
  delta_options:
    mergeSchema: true  # ✅ Allows new columns
```

**Option 2: Handle in Transform**
```yaml
# Add missing columns with defaults before SCD2
transform:
  steps:
    - function: "derive_columns"
      params:
        columns:
          phone: "COALESCE(phone, 'unknown')"
```

**Option 3: Full Schema Reset (Nuclear Option)**
```python
# Delete target table and rerun from scratch
# WARNING: Loses all history!
spark.sql("DROP TABLE IF EXISTS silver.dim_customers")
```

---

### "Why Did My Row Count Explode?"

**Symptom:**
```
Before: dim_customers had 10,000 rows
After:  dim_customers has 50,000 rows
```

**What's Happening:**
SCD2 is working correctly! Every time a tracked column changes, it creates a new version. If you ran it multiple times or have duplicates, you get multiple versions per record.

**Common Causes:**

1. **Duplicate source data:**
   ```
   customer_id | name  | updated_at
   101         | Alice | 2024-01-01
   101         | Alice | 2024-01-01  <- Duplicate!
   101         | Alice | 2024-01-01  <- Another duplicate!
   ```
   **Fix:** Deduplicate before SCD2 (see [Anti-Patterns](./anti_patterns.md#2-not-deduplicating-before-scd2))

2. **Running SCD2 on append-mode source:**
   Each run sees ALL historical source data, creating versions for old changes again.
   **Fix:** Use incremental loading or filter source to only new records.

3. **Tracking too many columns:**
   ```yaml
   # Tracking every column = version explosion
   track_cols: ["*"]  # ❌ Don't do this!
   
   # Track only meaningful business changes
   track_cols: ["tier", "status", "region"]  # ✅ Selective
   ```

**Debugging Query:**
```sql
-- Find customers with excessive versions
SELECT customer_id, COUNT(*) as version_count
FROM dim_customers
GROUP BY customer_id
HAVING COUNT(*) > 10
ORDER BY version_count DESC
```

---

### Debugging Checklist

Before running your SCD2 pipeline, verify these items:

```yaml
# ✅ DEBUGGING CHECKLIST
# Print this and check each box:

# [ ] 1. Source Data Check
#     - effective_time_col exists in source
#     - All track_cols exist in source
#     - Column names match case exactly

# [ ] 2. Key Column Check  
#     - keys columns exist in source
#     - keys columns have no NULLs
#     - keys columns uniquely identify records

# [ ] 3. Target Table Check
#     - Target exists (or this is first run)
#     - Target schema is compatible
#     - Target has end_time_col and current_flag_col

# [ ] 4. Deduplication Check
#     - Source has no duplicate keys
#     - If duplicates exist, deduplicate BEFORE SCD2

# [ ] 5. Write Mode Check
#     - Using mode: overwrite (required for SCD2)
```

**Python Debugging Script:**
```python
# Add this before your SCD2 node to validate
def validate_scd2_input(df, config):
    """Validate data before SCD2 processing."""
    errors = []
    
    # Check effective_time_col exists
    if config['effective_time_col'] not in df.columns:
        errors.append(f"effective_time_col '{config['effective_time_col']}' not in columns: {df.columns}")
    
    # Check all track_cols exist
    for col in config['track_cols']:
        if col not in df.columns:
            errors.append(f"track_col '{col}' not in columns: {df.columns}")
    
    # Check for duplicate keys
    key_cols = config['keys']
    dup_count = df.groupBy(key_cols).count().filter("count > 1").count()
    if dup_count > 0:
        errors.append(f"Found {dup_count} duplicate keys! Deduplicate first.")
    
    # Check for NULL keys
    for key in key_cols:
        null_count = df.filter(df[key].isNull()).count()
        if null_count > 0:
            errors.append(f"Found {null_count} NULL values in key column '{key}'")
    
    if errors:
        for e in errors:
            print(f"❌ {e}")
        raise ValueError("SCD2 validation failed. Fix errors above.")
    else:
        print("✅ SCD2 input validation passed")
```

---

### Quick Reference: SCD2 Error Cheat Sheet

| Error | Likely Cause | Quick Fix |
|-------|--------------|-----------|
| `effective_time_col not found` | Column doesn't exist or wrong name | Check source columns, fix spelling/case |
| `track_col X not found` | Column name mismatch | Match exact column names including case |
| Schema mismatch | Target has different columns | Use `mergeSchema: true` or reset target |
| Row count explosion | Duplicates or too many runs | Deduplicate source first |
| `merge_key not found` | Key column missing | Verify keys exist in both source and target |
| NULL in key columns | Missing business keys | Handle NULLs before SCD2 |

---

## Next Steps

- [Anti-Patterns Guide](./anti_patterns.md) - What NOT to do with SCD2
- [Dimension Pattern](./dimension.md) - Full dimension table management
- [Troubleshooting Guide](../troubleshooting.md) - General debugging
