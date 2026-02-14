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

### Two Ways to Build SCD2 Dimensions

**Option A: Raw `scd2` Transformer** (shown below)
- Lower-level transformer for SCD2 logic only
- You manage surrogate keys, audit columns, unknown members separately

**Option B: [Dimension Pattern](./dimension.md)** (recommended)
- Higher-level pattern that includes SCD2 + surrogate keys + audit + unknown member
- More comprehensive for building production dimensions
- See the sections on [Audit Columns](#audit-columns), [Unknown Member](#unknown-member-row-sk0), and [Grain Validation](#grain-validation) in this doc

This section covers the **raw `scd2` transformer** configuration.

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
      path: sales/silver/dim_customers
      keys: ["customer_id"]
      track_cols: ["address", "tier"]
      effective_time_col: "txn_date"

    write:
      connection: adls_prod            # WRITE result back (same location)
      path: sales/silver/dim_customers
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

  # Target columns to manage (optional - customize names if needed)
  end_time_col: "valid_to"             # Default: valid_to
  current_flag_col: "is_current"       # Default: is_current
  
  # Optional: soft delete support
  delete_col: "is_deleted"             # If source has soft delete flag
```

### Customizing Column Names

If your organization uses different naming conventions, you can customize the SCD2 metadata column names:

```yaml
# Example: Using company-specific naming standards
transformer: "scd2"
params:
  target: "silver.dim_customers"
  keys: ["customer_id"]
  track_cols: ["address", "tier"]
  effective_time_col: "last_modified_date"
  
  # Custom column names
  end_time_col: "effective_end_date"      # Instead of valid_to
  current_flag_col: "active_flag"         # Instead of is_current
```

**Result columns:**
- `effective_end_date` instead of `valid_to`
- `active_flag` instead of `is_current`

---

## Complete Parameter Reference

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `target` | string | One of target/connection+path | - | Target table name or full path |
| `connection` | string | One of target/connection+path | - | Connection name to resolve path |
| `path` | string | One of target/connection+path | - | Relative path within connection |
| `keys` | list[string] | Yes | - | Natural keys to identify unique entities |
| `track_cols` | list[string] | Yes | - | Columns to monitor for changes |
| `effective_time_col` | string | Yes | - | Source column indicating when change occurred |
| `end_time_col` | string | No | "valid_to" | Name of the end timestamp column |
| `current_flag_col` | string | No | "is_current" | Name of the current record flag column |
| `delete_col` | string | No | None | Column indicating soft deletion (boolean) |

**Validation Rules:**
- Must provide **either** `target` **OR** both `connection` + `path`
  - Providing both will raise a validation error
  - Providing neither will also raise a validation error
- `keys` and `track_cols` must exist in source data
- `effective_time_col` must exist in source data

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

## Audit Columns

SCD2 dimensions work seamlessly with audit columns to track data lineage and load metadata. When building SCD2 dimensions via the [Dimension Pattern](./dimension.md), you can automatically add:

- **`load_timestamp`**: When the record was loaded into the data warehouse
- **`source_system`**: Source system identifier (e.g., "crm", "erp", "pos")

### Configuration

Use the `audit` parameter in the **Dimension Pattern** (not the raw `scd2` transformer):

```yaml
nodes:
  - name: "dim_customers"
    read:
      connection: staging
      path: customers

    pattern:
      type: dimension
      params:
        natural_key: customer_id
        surrogate_key: customer_sk
        scd_type: 2
        track_cols: ["address", "tier", "email"]
        target: "warehouse.dim_customers"
        audit:
          load_timestamp: true       # Adds load_timestamp column
          source_system: "crm"       # Adds source_system='crm' column

    write:
      connection: warehouse
      path: dim_customers
      format: delta
      mode: overwrite
```

### Result

Your SCD2 dimension will include these columns:

| customer_sk | customer_id | address | tier | valid_from | valid_to | is_current | load_timestamp | source_system |
|-------------|-------------|---------|------|------------|----------|------------|----------------|---------------|
| 1 | 101 | CA | Gold | 2024-01-01 | 2024-02-01 | false | 2024-01-01 10:00 | crm |
| 2 | 101 | NY | Gold | 2024-02-01 | NULL | true | 2024-02-01 14:30 | crm |

**Use Cases:**
- **Data lineage**: Track when each version was loaded
- **Source identification**: Know which system the data came from
- **Audit trails**: Reconstruct the history of data loading operations

---

## Unknown Member Row (SK=0)

When building star schema fact tables, you often need to handle **orphan records** — fact records that reference dimensions that don't exist yet (or never will).

### The Problem

```yaml
# Fact table has customer_id=999, but dim_customers doesn't have it yet
fact_orders:
  order_id: 12345
  customer_id: 999  # ← Orphan! Not in dim_customers
  amount: 100.00
```

### The Solution

Add an **unknown member row** to your dimension with surrogate key = 0. When fact tables encounter orphans, they can safely reference SK=0 instead of failing or creating NULL foreign keys.

### Configuration

Enable `unknown_member: true` in the **Dimension Pattern**:

```yaml
nodes:
  - name: "dim_customers"
    pattern:
      type: dimension
      params:
        natural_key: customer_id
        surrogate_key: customer_sk
        scd_type: 2
        track_cols: ["address", "tier"]
        target: "warehouse.dim_customers"
        unknown_member: true        # ← Adds SK=0 row
        audit:
          load_timestamp: true
          source_system: "crm"

    write:
      connection: warehouse
      path: dim_customers
      format: delta
      mode: overwrite
```

### Result

The dimension automatically includes an unknown member row:

| customer_sk | customer_id | address | tier | valid_from | valid_to | is_current | load_timestamp | source_system |
|-------------|-------------|---------|------|------------|----------|------------|----------------|---------------|
| **0** | **-1** | **Unknown** | **Unknown** | **1900-01-01** | **NULL** | **true** | *current_time* | **crm** |
| 1 | 101 | CA | Gold | 2024-01-01 | 2024-02-01 | false | 2024-01-01 10:00 | crm |
| 2 | 101 | NY | Gold | 2024-02-01 | NULL | true | 2024-02-01 14:30 | crm |

**Facts can now safely reference SK=0** when the dimension doesn't exist:

```yaml
fact_orders:
  order_id: 12345
  customer_sk: 0      # ← Maps to unknown member
  amount: 100.00
```

### Integration with Fact Pattern

The [Fact Pattern](./fact.md) supports three **orphan handling strategies**:

1. **`unknown`** (default): Map orphans to SK=0
2. **`reject`**: Fail the pipeline if orphans exist
3. **`quarantine`**: Write orphans to a quarantine table for investigation

```yaml
nodes:
  - name: "fact_orders"
    depends_on: [dim_customers, dim_products]
    
    pattern:
      type: fact
      params:
        grain: [order_id]
        dimensions:
          - source_column: customer_id
            dimension_table: dim_customers
            dimension_key: customer_id
            surrogate_key: customer_sk
            scd2: true                    # Filter to is_current=true
        orphan_handling: unknown          # ← Uses SK=0 for orphans
```

**See [Fact Pattern - Orphan Handling](./fact.md#orphan-handling) for complete details.**

---

## Grain Validation

SCD2 dimensions have a specific **grain** (level of uniqueness) that you must maintain:

**Grain Definition:** `(natural_key, is_current=true)` must be unique.

### What This Means

For each natural key, **only one record** can have `is_current=true` at any time. Historical records (closed versions) have `is_current=false`.

**Valid SCD2 State:**

| customer_sk | customer_id | address | valid_from | valid_to | is_current |
|-------------|-------------|---------|------------|----------|------------|
| 1 | 101 | CA | 2024-01-01 | 2024-02-01 | **false** |
| 2 | 101 | NY | 2024-02-01 | NULL | **true** |

_Note: Only one record per customer_id has is_current=true._

**INVALID State (Duplicate Grain):**

| customer_sk | customer_id | address | valid_from | valid_to | is_current |
|-------------|-------------|---------|------------|----------|------------|
| 1 | 101 | CA | 2024-01-01 | NULL | **true** |
| 2 | 101 | NY | 2024-02-01 | NULL | **true** |

_Problem: Two records for customer_id=101 both have is_current=true (grain violation)._

### How Odibi Prevents This

The `scd2` transformer **automatically maintains grain uniqueness** by:
1. Identifying changed records (comparing `track_cols`)
2. Closing old versions (setting `is_current=false`, `valid_to=<new_effective_time>`)
3. Opening new versions (setting `is_current=true`, `valid_to=NULL`)

### Validating Grain in Fact Tables

When using the [Fact Pattern](./fact.md), define the **fact table grain** to detect duplicates:

```yaml
nodes:
  - name: "fact_orders"
    pattern:
      type: fact
      params:
        grain: [order_id, line_item_id]  # ← Validates uniqueness
        dimensions:
          - source_column: customer_id
            dimension_table: dim_customers
            dimension_key: customer_id
            surrogate_key: customer_sk
            scd2: true
```

**The Fact Pattern will raise an error if duplicates exist at the grain level.**

### Manual Validation Query

If you need to check for grain violations in an existing SCD2 dimension:

```sql
-- Find natural keys with multiple current records (grain violation)
SELECT 
  customer_id,
  COUNT(*) as current_count
FROM dim_customers
WHERE is_current = true
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY current_count DESC;
```

**Expected result:** 0 rows (no violations).

---

## Incremental Loading and Merge Pattern

The `scd2` transformer is **NOT** an incremental merge pattern. It is designed for building complete dimension history from source data.

### Key Differences

| Feature | SCD2 Transformer | Merge Pattern |
|---------|------------------|---------------|
| **Purpose** | Track dimension history | Incrementally merge raw → silver |
| **Write Mode** | `overwrite` (returns full history)* | `append` or `merge` |
| **Use Case** | Dimension tables with history | Fact tables, raw data ingestion |
| **Output** | Complete historical dataset | New/changed records only |
| **Idempotency** | Re-runs rebuild full history | Merge by key prevents duplicates |

_* Note: `overwrite` mode is the typical pattern. The SCD2 transformer returns the complete history (all versions), which you then write back to replace the target. Advanced users can implement incremental SCD2 using Delta merge operations, but this is not the default behavior of the `scd2` transformer._

### When to Use Each

**Use SCD2 Transformer:**
- Dimension tables where you need full history
- Customer, product, employee dimensions
- Tracking attribute changes over time

**Use Merge Pattern:**
- Incremental fact table loading
- Deduplicating raw data into silver
- Stateless transformations (no history tracking)

**Example: Building dimensions with SCD2, facts with merge:**

```yaml
pipelines:
  # 1. Build SCD2 dimension
  - pipeline: build_dimensions
    nodes:
      - name: dim_customers
        read:
          connection: staging
          path: customers
        
        pattern:
          type: dimension
          params:
            natural_key: customer_id
            surrogate_key: customer_sk
            scd_type: 2
            track_cols: [name, address, tier]
            target: warehouse.dim_customers
            unknown_member: true
        
        write:
          connection: warehouse
          path: dim_customers
          mode: overwrite          # ← Full history

  # 2. Merge fact table incrementally
  - pipeline: build_facts
    nodes:
      - name: fact_orders
        read:
          connection: raw
          path: orders_incremental  # Only new/changed orders
        
        transformer: merge
        params:
          target: warehouse.fact_orders
          keys: [order_id]
          mode: insert              # Or upsert
        
        write:
          connection: warehouse
          path: fact_orders
          mode: append              # ← Incremental
```

**See [Merge/Upsert Pattern](./merge_upsert.md) for complete details on incremental loading strategies.**

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
        derivations:
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

### Related Documentation
- **[Dimension Pattern](./dimension.md)** - Complete dimension table management with surrogate keys, audit columns, and unknown members
- **[Fact Pattern](./fact.md)** - Build fact tables with automatic SK lookups and orphan handling
- **[Merge/Upsert Pattern](./merge_upsert.md)** - Incremental loading strategies for raw → silver
- **[Anti-Patterns Guide](./anti_patterns.md)** - What NOT to do with SCD2
- **[Troubleshooting Guide](../troubleshooting.md)** - General debugging

### Key Concepts Covered
- ✅ **Audit Columns**: Track load timestamps and source systems
- ✅ **Unknown Members**: Handle orphan FK references with SK=0
- ✅ **Grain Validation**: Ensure uniqueness at (natural_key, is_current) level
- ✅ **Incremental Strategies**: Use merge pattern for facts, SCD2 for dimensions
