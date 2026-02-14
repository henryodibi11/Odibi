# Troubleshooting Guide

Quick reference for diagnosing and fixing common Odibi issues.

**For beginners:** Each error section includes:
- 📋 **Exact error message** - Copy-paste to search
- 💡 **What it means** - Plain English explanation
- 🔍 **Why it happened** - Root cause
- ✅ **Step-by-step fix** - How to resolve it
- 🛡️ **How to prevent it** - Stop it from happening again
- 📝 **YAML before/after** - Broken vs fixed config

---

## Quick Diagnostic Steps

**My pipeline failed, now what?**

1. **Check the error message** - Look for the specific error type (validation, engine, pattern)
2. **Check logs for context** - Use `get_logging_context()` for structured logs
3. **Run with verbose logging:**
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```
4. **Check data quality issues** - Look for null keys, schema mismatches, FK violations

---

## Common Errors and Fixes

### Import and Installation Issues

#### Module Not Found Errors

```
ModuleNotFoundError: No module named 'odibi'
```

**Fix:** Install odibi in your environment:
```bash
pip install -e .  # Development install
# or
pip install odibi
```

#### Python 3.9 Type Hint Compatibility

```
TypeError: unsupported operand type(s) for |: 'type' and 'NoneType'
```

**Cause:** Code uses Python 3.10+ union syntax (`str | None`) on Python 3.9.

**Fix:** Use typing module syntax:
```python
# Python 3.10+ (will fail on 3.9)
def func(param: str | None = None) -> list[str]:
    ...

# Python 3.9 compatible
from typing import Optional, List
def func(param: Optional[str] = None) -> List[str]:
    ...
```

---

### Engine Errors

#### Spark Python Version Mismatch

```
PYTHON_VERSION_MISMATCH: Python in worker has different version (3, 8) than that in driver 3.9
```

**Cause:** Spark workers and driver use different Python versions.

**Fix:** Set environment variables before starting Spark:
```bash
export PYSPARK_PYTHON=python3.9
export PYSPARK_DRIVER_PYTHON=python3.9
```

Or in Python:
```python
import os
os.environ['PYSPARK_PYTHON'] = 'python3.9'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3.9'
```

#### Pandas/Polars Compatibility

**Pandas FutureWarning (fillna downcasting):**
```
FutureWarning: Downcasting object dtype arrays on .fillna is deprecated
```

**Fix:** Chain `.infer_objects(copy=False)` after fillna:
```python
df['column'].fillna(value).infer_objects(copy=False)
```

**Polars API Changes:**
```
DeprecationWarning: `columns` argument renamed to `on`
```

**Fix:** Use the new parameter name:
```python
df.pivot(on="column", ...)  # Not columns="column"
```

---

### Delta Lake Issues

Delta Lake is a powerful format, but it has specific requirements and error modes. This section covers the most common issues you'll encounter.

---

#### 1. Schema Evolution Failures

📋 **Error Message:**
```
DeltaAnalysisException: A schema mismatch detected when writing to the Delta table
```
or
```
Schema of data does not match table schema
```
or
```
AnalysisException: Cannot write incompatible data to table
```

💡 **What It Means:**
You're trying to write data with a schema (columns and/or types) that doesn't match the existing Delta table. Delta Lake enforces schema consistency to prevent data corruption.

🔍 **When It Happens:**
- Adding new columns to an existing table
- Changing column data types (e.g., from integer to string)
- Renaming columns that Delta interprets as removals + additions
- Source data schema evolved without updating the target configuration

✅ **Step-by-Step Fix:**

**Option 1: Enable schema evolution (recommended for most cases):**
```yaml
# In your YAML config
write:
  format: delta
  mode: append
  delta_options:
    schema_mode: "merge"  # Automatically adds new columns
```

**Option 2: Overwrite schema completely (destructive - use with caution):**
```yaml
write:
  format: delta
  mode: overwrite
  delta_options:
    overwrite_schema: true  # Replaces entire table schema
```

**Option 3: Pre-validate and cast columns before writing:**
```python
# Pandas example
existing_schema = pd.read_parquet("path/to/delta")
for col in df.columns:
    if col in existing_schema.columns:
        df[col] = df[col].astype(existing_schema[col].dtype)
```

🛡️ **How to Prevent It:**

1. **Use Delta table contracts (recommended):**
```python
# When creating the table initially
from deltalake import DeltaTable, write_deltalake

write_deltalake(
    "path/to/table",
    df,
    mode="overwrite",
    configuration={
        "delta.constraints.column_name.notNull": "true",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5"
    }
)
```

2. **Set `schema_mode: "merge"` as default in project config:**
```yaml
# In project-level config
engine:
  pandas:
    performance:
      delta_defaults:
        schema_mode: "merge"
```

3. **Add schema validation step before writes:**
```yaml
transform:
  steps:
    - function: "validate_schema"
      params:
        expected_columns: ["id", "name", "created_at"]
        allow_extra: true  # Allow new columns (will merge)
```

📝 **YAML Before/After:**
```yaml
# BEFORE (broken) - Rigid schema
nodes:
  - name: "write_customers"
    write:
      format: delta
      mode: append
      # Missing schema_mode - fails when source adds columns!

# AFTER (fixed) - Schema evolution enabled
nodes:
  - name: "write_customers"
    write:
      format: delta
      mode: append
      delta_options:
        schema_mode: "merge"  # ✅ Safely handles new columns
```

**⚠️ Important Notes:**
- `schema_mode: "merge"` requires the Rust engine (delta-rs). The PyArrow engine doesn't support it.
- Odibi uses `engine="rust"` by default for Delta writes (see `odibi/engine/pandas_engine.py:1603`)
- `overwrite_schema: true` replaces the entire table - use only for full rebuilds

---

#### 2. Null Column Type Errors

📋 **Error Message:**
```
ArrowInvalid: Could not convert <NA> with type NoneType
```
or
```
ArrowInvalid: Could not convert NULL with type NoneType: did not recognize Python value type when inferring an Arrow data type
```
or
```
pyarrow.lib.ArrowTypeError: Expected schema field ... to be not-null
```

💡 **What It Means:**
You're trying to write a column where **all values are NULL** on the first write. Arrow and Delta Lake can't infer the data type from NULL-only columns, causing the write to fail.

🔍 **When It Happens:**
- First write to a new Delta table with columns that have no data yet
- Optional columns that are completely empty in the initial batch
- Late-binding columns (added to source but not yet populated)
- Columns that will be filled by later transformations

✅ **Step-by-Step Fix:**

**Option 1: Odibi handles this automatically (Pandas engine only):**
```python
# The framework already does this for you (odibi/engine/pandas_engine.py:1518-1522)
# All-null columns are automatically cast to string type
for col in df.columns:
    if df[col].isna().all():
        df[col] = df[col].astype("string")
```

**Option 2: Explicitly cast columns in your YAML:**
```yaml
transform:
  steps:
    - function: "cast_columns"
      params:
        columns:
          optional_column: "string"
          future_column: "string"
          nullable_field: "int64"  # Use int64, not int (supports nulls)
```

**Option 3: Declare schema upfront with `columns:` config:**
```yaml
write:
  format: delta
  columns:
    - name: "customer_id"
      type: "int64"
    - name: "optional_field"
      type: "string"  # ✅ Explicit type even if all nulls
    - name: "future_column"
      type: "string"
```

**Option 4: Provide default non-null values temporarily:**
```python
# Pandas example
df['optional_column'] = df['optional_column'].fillna("UNKNOWN")
```

🛡️ **How to Prevent It:**

1. **Always bootstrap tables with representative data (preferred):**
```yaml
# Use a sample dataset that has at least one non-null value per column
bootstrap:
  source: "sample_data.csv"  # Include all columns with example values
```

2. **Use typed schemas in your data contracts:**
```yaml
# Define schema explicitly at table creation
schema:
  columns:
    - name: "id"
      type: "int64"
      nullable: false
    - name: "optional_field"
      type: "string"
      nullable: true  # ✅ Declared as nullable with known type
```

3. **Avoid writing empty DataFrames on first run:**
```python
if not df.empty and df[optional_cols].notna().any().any():
    # Safe to write
    write_deltalake(...)
```

📝 **YAML Before/After:**
```yaml
# BEFORE (broken) - All-null column fails on first write
nodes:
  - name: "create_customer_table"
    read:
      path: "initial_load.csv"
      # CSV has: id, name, email, phone
      # phone column is all null!
    
    write:
      format: delta
      mode: overwrite
      # ❌ Fails: Can't infer type from NULL-only column

# AFTER (fixed) - Explicit type casting
nodes:
  - name: "create_customer_table"
    read:
      path: "initial_load.csv"
    
    transform:
      steps:
        - function: "cast_columns"
          params:
            columns:
              phone: "string"  # ✅ Explicit type for null column
    
    write:
      format: delta
      mode: overwrite
```

**💡 Pro Tip:**
The Pandas engine (`odibi/engine/pandas_engine.py`) automatically handles this issue by casting all-null columns to string type before writing. However, if you're using Spark or need specific types (not string), use explicit casting.

---

#### 3. Concurrent Write Conflicts

📋 **Error Message:**
```
ConcurrentAppendException: Files were added to the root of the table by a concurrent update
```
or
```
ConcurrentDeleteReadException: This transaction attempted to read files that were deleted by a concurrent commit
```
or
```
ConcurrentDeleteDeleteException: Multiple commits deleted the same files
```
or
```
DeltaError: Transaction commit failed - conflict with concurrent operation
```

💡 **What It Means:**
Two or more processes tried to write to the same Delta table at the same time, and their operations conflicted. Delta Lake uses optimistic concurrency control and detects conflicts at commit time.

🔍 **When It Happens:**
- Parallel pipeline runs writing to the same table simultaneously
- Two users running the same notebook/script at once
- Overlapping scheduled jobs (e.g., hourly job starts before previous run completes)
- Streaming and batch jobs writing to the same table
- Using `overwrite` mode while another process is appending

✅ **Step-by-Step Fix:**

**Option 1: Odibi handles this automatically with retries:**
```python
# The framework already does this (odibi/engine/pandas_engine.py:157-169)
# All Delta operations retry up to 5 times with exponential backoff
def _retry_delta_operation(self, func, max_retries=5, base_delay=0.2):
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if "conflict" in str(e).lower() or "concurrent" in str(e).lower():
                # Wait and retry with exponential backoff
                delay = base_delay * (2**attempt) + random.uniform(0, 0.1)
                time.sleep(delay)
```

**Option 2: Use merge instead of append (idempotent):**
```yaml
# Merge is safer for concurrent operations than append
transformer: "merge"
params:
  target: "silver.customers"
  keys: ["customer_id"]
  strategy: "upsert"
```

**Option 3: Configure isolation level for your use case:**
```python
# For Spark users
spark.conf.set("spark.databricks.delta.properties.defaults.isolationLevel", "WriteSerializable")

# For high-concurrency scenarios
spark.conf.set("spark.databricks.delta.properties.defaults.isolationLevel", "Serializable")
```

**Option 4: Partition writes by date or other dimension:**
```yaml
write:
  format: delta
  delta_options:
    partition_by: ["process_date"]  # Different partitions = no conflict
```

🛡️ **How to Prevent It:**

1. **Configure orchestrator to prevent overlaps:**
```yaml
# Airflow example
dag = DAG(
    'customer_pipeline',
    max_active_runs=1,  # ✅ Only one run at a time
    catchup=False
)

# Databricks Workflows example
{
  "max_concurrent_runs": 1  # ✅ Serial execution only
}
```

2. **Use idempotent patterns (merge, upsert) instead of append:**
```yaml
# SAFER: Merge-based patterns handle concurrency better
pattern: "merge"
params:
  target: "silver.orders"
  keys: ["order_id"]
  # Retrying a merge is safe - produces same result
```

3. **Partition by run ID or timestamp:**
```yaml
# Write to unique paths per run
write:
  path: "bronze/orders/${run_id}"  # Each run gets its own path
  format: delta
```

4. **Use Delta Lake's Change Data Feed for streaming:**
```yaml
read:
  format: delta
  delta_options:
    readChangeFeed: true
    startingVersion: 0
```

📝 **YAML Before/After:**
```yaml
# BEFORE (problematic) - Append mode with concurrent runs
schedule:
  interval: "*/5 * * * *"  # Every 5 minutes
  max_concurrent: 5  # ❌ Allows overlaps!

nodes:
  - name: "load_orders"
    write:
      format: delta
      mode: append  # ❌ Conflicts if runs overlap

# AFTER (fixed) - Serial execution + idempotent merge
schedule:
  interval: "*/5 * * * *"
  max_concurrent: 1  # ✅ One run at a time

nodes:
  - name: "load_orders"
    transformer: "merge"  # ✅ Safer than append
    params:
      target: "bronze.orders"
      keys: ["order_id", "timestamp"]
      strategy: "upsert"
```

**⚠️ Important Notes:**
- Concurrent **reads** are always safe - conflicts only happen with writes
- `overwrite` mode is especially prone to conflicts - prefer merge/upsert
- Retries are automatic in Odibi (Pandas engine) - just ensure retries are enabled
- For very high concurrency, consider using Delta Lake's optimistic transaction log on a distributed file system (not local disk)

---

#### 4. Deletion Vector Errors

📋 **Error Message:**
```
DeltaError: Deletion vectors are not supported by the currently used reader/writer
```
or
```
Generic error: Table uses deletion vectors which are not supported by this reader
```
or
```
ReaderFeatureNotSupported: deletion vectors
```

💡 **What It Means:**
The Delta table uses **deletion vectors** (a performance optimization in Delta Lake 2.0+), but your delta-rs version or reader doesn't support this feature yet.

🔍 **When It Happens:**
- Reading a table written by Databricks with deletion vectors enabled (default in DBR 11.3+)
- Using an older version of delta-rs (< 0.15.0) that predates deletion vector support
- Reading a table that had `DELETE` or `MERGE` operations run with deletion vectors
- Using `delta-rs` readers with tables written by `delta-spark` 2.3.0+

✅ **Step-by-Step Fix:**

**Option 1: Update delta-rs (recommended):**
```bash
# Update to the latest version that supports deletion vectors
pip install --upgrade deltalake

# Verify version (need >= 0.15.0 for deletion vector support)
python -c "import deltalake; print(deltalake.__version__)"
```

**Option 2: Disable deletion vectors on the table (if you control it):**
```python
# For Spark users (run once on the table)
spark.sql("""
    ALTER TABLE catalog.schema.table_name
    SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')
""")
```

```python
# For Databricks SQL
ALTER TABLE silver.customers
SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false');
```

**Option 3: Rewrite table without deletion vectors:**
```python
from deltalake import DeltaTable

# Read the table
dt = DeltaTable("path/to/table")
df = dt.to_pandas()

# Overwrite without deletion vectors
from deltalake import write_deltalake
write_deltalake(
    "path/to/table",
    df,
    mode="overwrite",
    overwrite_schema=False,
    configuration={
        "delta.enableDeletionVectors": "false"
    }
)
```

**Option 4: Use a compatibility shim while upgrading:**
```python
# Temporary workaround: read via Spark then pass to Pandas
spark_df = spark.read.format("delta").load("path/to/table")
pandas_df = spark_df.toPandas()
# Now work with pandas_df in your pipeline
```

🛡️ **How to Prevent It:**

1. **Set project-wide deletion vector policy:**
```yaml
# In your project config for new tables
engine:
  pandas:
    performance:
      delta_defaults:
        configuration:
          delta.enableDeletionVectors: "false"  # Disable for compatibility
```

2. **Pin compatible delta-rs version in requirements:**
```txt
# requirements.txt
deltalake>=0.15.0  # ✅ Supports deletion vectors
```

3. **Check table properties before reading:**
```python
from deltalake import DeltaTable

dt = DeltaTable("path/to/table")
props = dt.metadata().configuration

if props.get("delta.enableDeletionVectors") == "true":
    print("⚠️  Table uses deletion vectors - ensure delta-rs >= 0.15.0")
```

4. **For cross-platform compatibility, disable deletion vectors:**
```python
# When creating tables that will be read by multiple tools
write_deltalake(
    "path/to/table",
    df,
    mode="overwrite",
    configuration={
        "delta.enableDeletionVectors": "false",  # Max compatibility
        "delta.minReaderVersion": "1",
        "delta.minWriterVersion": "2"
    }
)
```

📝 **YAML Before/After:**
```yaml
# BEFORE (broken) - Incompatible reader version
dependencies:
  - deltalake==0.10.0  # ❌ Too old for deletion vectors

read:
  connection: databricks_prod
  table: "silver.customers"  # Uses deletion vectors
  format: delta
  # ❌ Fails with "deletion vectors not supported"

# AFTER (fixed) - Updated dependency
dependencies:
  - deltalake>=0.15.0  # ✅ Supports deletion vectors

read:
  connection: databricks_prod
  table: "silver.customers"
  format: delta
  # ✅ Works with deletion vector tables
```

**💡 Pro Tips:**
- Deletion vectors are a **performance optimization**, not a requirement - disabling them is safe
- If you control the table, disabling is easiest (run `ALTER TABLE` once)
- If you don't control the table, upgrade delta-rs
- Databricks enables deletion vectors by default for better `DELETE`/`MERGE` performance
- Check your delta-rs version with: `pip show deltalake`

**📚 Reference:**
- [Delta Lake Deletion Vectors Docs](https://docs.delta.io/latest/delta-deletion-vectors.html)
- [delta-rs Changelog](https://github.com/delta-io/delta-rs/blob/main/CHANGELOG.md)

---

#### PyArrow Engine Limitations

```
schema_mode 'merge' is not supported in pyarrow engine. Use engine=rust
```

**Cause:** The PyArrow engine doesn't support schema evolution with `schema_mode='merge'`.

**Fix:** Odibi automatically uses `engine="rust"` for Delta writes (see `odibi/engine/pandas_engine.py:1603`). This error should not occur in normal usage. If you see it, verify you're using the Pandas engine's Delta writer, not a custom implementation.

#### Catalog log_run Failures

If `log_run` fails with schema errors:
1. Schema is fixed at bootstrap time
2. Use exact column types that match the run log schema
3. Serialize complex types (like lists) to JSON strings

---

### Validation Errors

#### FK Validation Failures

```
Foreign key validation failed: 3 orphan records found
```

**Diagnosis:**
```python
result = validator.validate_foreign_key(df, 'fk_column', ref_df, 'pk_column')
print(result.orphan_records)  # See which records failed
```

**Common causes:**
- Null FK values (decide: allow nulls or require matches)
- Stale reference data
- Case sensitivity mismatches

#### Quality Gate Blocks

```
Quality gate blocked execution: data_quality_score < 0.95
```

**Diagnosis:** Check which rules failed:
```python
result = validator.run_all()
for check in result.failed_checks:
    print(f"{check.rule}: {check.message}")
```

#### Quarantine Issues

If records are unexpectedly quarantined:
1. Check quarantine rules configuration
2. Review quarantine output for specific failures
3. Verify data types match expected patterns

---

### Pattern-Specific Issues

#### Dimension Pattern: Unknown Member Concat Failures

```
ValueError: all the input array dimensions except for the concatenation axis must match exactly
```

**Cause:** Datetime columns have mismatched types when concatenating unknown member row with data.

**Fix (already applied in framework):** Unknown member row columns are cast to match DataFrame dtypes. If you see this on an older version, upgrade.

#### SCD2: Merge Key Issues

```
KeyError: 'merge_key' not found
```

**Checklist:**
1. Verify merge key column exists in source data
2. Check column name spelling/case sensitivity
3. Ensure key isn't being dropped by upstream transforms

#### Aggregation: Null Handling

```
Cannot aggregate on null values
```

**Fix:** Handle nulls before aggregation:
```python
# Option 1: Filter nulls
df = df.dropna(subset=['group_column'])

# Option 2: Replace nulls with placeholder
df['group_column'] = df['group_column'].fillna('UNKNOWN')
```

---

## Pipeline & Configuration Errors

### Cannot Resolve Column Name

📋 **Error Message:**
```
AnalysisException: Cannot resolve column name 'customer_id' among (CustomerID, Name, Email, Address)
```

💡 **What It Means:**
You're trying to use a column that doesn't exist in your DataFrame. The column names you specified don't match the actual column names in your data.

🔍 **Why It Happens:**
- Column names are **case-sensitive** (e.g., `customer_id` ≠ `CustomerID`)
- The source system changed column names
- An upstream transformation renamed columns
- You have a typo in your YAML

✅ **Step-by-Step Fix:**

1. **Print actual column names:**
   ```python
   df = spark.read.format("delta").load("your/path")
   print(df.columns)
   # Output: ['CustomerID', 'Name', 'Email', 'Address']
   ```

2. **Update YAML to match exactly:**
   ```yaml
   # BEFORE (broken)
   params:
     keys: ["customer_id"]  # ❌ Wrong case
   
   # AFTER (fixed)
   params:
     keys: ["CustomerID"]   # ✅ Matches actual column
   ```

🛡️ **Prevention:**
Normalize column names to lowercase in Bronze/Silver:
```yaml
transform:
  steps:
    - function: "rename_columns"
      params:
        lowercase: true
```

---

### Column Not Found

📋 **Error Message:**
```
KeyError: 'effective_date'
# or
Column 'effective_date' does not exist
```

💡 **What It Means:**
A column you referenced in your config doesn't exist in the DataFrame at that point in the pipeline.

🔍 **Why It Happens:**
- Column was dropped by an earlier transformation
- Column was renamed upstream
- Column is in the wrong format (e.g., expecting `effective_date` but source has `EffectiveDate`)
- You're referencing a column before it's created

✅ **Step-by-Step Fix:**

1. **Add a debug step to see columns at each stage:**
   ```yaml
   nodes:
     - name: "debug_columns"
       depends_on: ["previous_node"]
       transform:
         steps:
           - sql: "SELECT *, 'columns:' as debug FROM df LIMIT 1"
       # Check logs for actual columns
   ```

2. **Track column renames through your pipeline:**
   ```yaml
   # Node 1: Rename columns
   - name: "clean_data"
     transform:
       steps:
         - function: "rename_columns"
           params:
             columns:
               EffectiveDate: "effective_date"  # Now it's lowercase
   
   # Node 2: Use the NEW name
   - name: "process_data"
     depends_on: ["clean_data"]
     params:
       effective_time_col: "effective_date"  # ✅ Use renamed column
   ```

📝 **YAML Before/After:**
```yaml
# BEFORE (broken) - Column renamed but old name used
nodes:
  - name: "prep"
    transform:
      steps:
        - function: "rename_columns"
          params: { columns: { OldName: "new_name" } }
  
  - name: "process"
    depends_on: ["prep"]
    params:
      key_col: "OldName"  # ❌ Still using old name!

# AFTER (fixed) - Use the new column name
nodes:
  - name: "prep"
    transform:
      steps:
        - function: "rename_columns"
          params: { columns: { OldName: "new_name" } }
  
  - name: "process"
    depends_on: ["prep"]
    params:
      key_col: "new_name"  # ✅ Use the renamed column
```

---

### unionByName Failures

📋 **Error Message:**
```
AnalysisException: Union can only be performed on tables with compatible column types.
Column customer_id is of type StringType in first table and IntegerType in second.
# or
Cannot resolve column 'new_column' in the right table
```

💡 **What It Means:**
You're trying to combine (union) two DataFrames that have different schemas—either column types don't match, or columns are missing.

🔍 **Why It Happens:**
- Source schema changed but target table has old schema
- SCD2 target has different columns than current source
- Appending data with different types (e.g., source sends `"123"` as string, target expects integer)

✅ **Step-by-Step Fix:**

1. **Compare schemas:**
   ```python
   source_df = spark.read.format("delta").load("source/path")
   target_df = spark.read.format("delta").load("target/path")
   
   print("Source columns:", source_df.dtypes)
   print("Target columns:", target_df.dtypes)
   ```

2. **Cast columns to match:**
   ```yaml
   transform:
     steps:
       - function: "cast_columns"
         params:
           columns:
             customer_id: "integer"  # Match target type
             amount: "double"
   ```

3. **Or enable schema merging:**
   ```yaml
   write:
     format: delta
     delta_options:
       mergeSchema: true
   ```

📝 **YAML Before/After:**
```yaml
# BEFORE (broken) - Mismatched types
nodes:
  - name: "load_new_data"
    read:
      connection: landing
      path: new_customers.csv  # customer_id is STRING in CSV
    
    write:
      connection: silver
      table: dim_customers  # customer_id is INTEGER in target
      mode: append  # ❌ Fails due to type mismatch

# AFTER (fixed) - Cast types before writing
nodes:
  - name: "load_new_data"
    read:
      connection: landing
      path: new_customers.csv
    
    transform:
      steps:
        - function: "cast_columns"
          params:
            columns:
              customer_id: "integer"  # ✅ Match target type
    
    write:
      connection: silver
      table: dim_customers
      mode: append
```

---

### Spaces in Column Names

📋 **Error Message:**
```
AnalysisException: Syntax error in SQL: unexpected token 'Date'
# or
ParseException: mismatched input 'Name' expecting <EOF>
```

💡 **What It Means:**
Your column names have spaces (e.g., `Customer Name`) which breaks SQL parsing.

🔍 **Why It Happens:**
- Source data came from Excel with friendly column headers
- API returned columns with spaces
- Someone created columns with spaces in the source system

✅ **Step-by-Step Fix:**

**Option 1: Use backticks in SQL:**
```yaml
transform:
  steps:
    - sql: "SELECT `Customer Name`, `Order Date`, `Total Amount` FROM df"
```

**Option 2: Rename columns (recommended):**
```yaml
transform:
  steps:
    - function: "rename_columns"
      params:
        columns:
          "Customer Name": "customer_name"
          "Order Date": "order_date"
          "Total Amount": "total_amount"
```

**Option 3: Auto-normalize all columns:**
```yaml
transform:
  steps:
    - function: "rename_columns"
      params:
        snake_case: true  # Converts "Customer Name" → "customer_name"
```

📝 **YAML Before/After:**
```yaml
# BEFORE (broken) - Spaces in column names
nodes:
  - name: "process_data"
    transform:
      steps:
        - sql: "SELECT Customer Name, Order Date FROM df"  # ❌ Syntax error!

# AFTER (fixed) - Rename first
nodes:
  - name: "process_data"
    transform:
      steps:
        - function: "rename_columns"
          params:
            snake_case: true
        - sql: "SELECT customer_name, order_date FROM df"  # ✅ Works now
```

---

### Connection Not Found

📋 **Error Message:**
```
KeyError: Connection 'prod_warehouse' not found
# or
ConnectionError: No connection named 'gold' is defined
```

💡 **What It Means:**
Your pipeline references a connection name that isn't defined in your project config.

🔍 **Why It Happens:**
- Connection name is misspelled
- Connection is defined in a different config file
- Environment-specific connection isn't loaded
- Connection was renamed but references weren't updated

✅ **Step-by-Step Fix:**

1. **Check available connections:**
   ```yaml
   # In your odibi.yaml, list all connections:
   connections:
     bronze_storage:  # ← These are your available names
       type: azure_blob
       ...
     silver_storage:
       type: azure_blob
       ...
   ```

2. **Fix the reference:**
   ```yaml
   # BEFORE (broken)
   nodes:
     - name: "load_data"
       write:
         connection: gold  # ❌ Not defined in connections!
   
   # AFTER (fixed)
   nodes:
     - name: "load_data"
       write:
         connection: silver_storage  # ✅ Matches defined connection
   ```

🛡️ **Prevention:**
Use a consistent naming convention:
```yaml
connections:
  landing:   # Source data
    ...
  bronze:    # Raw layer
    ...
  silver:    # Cleaned layer
    ...
  gold:      # Business layer
    ...
```

---

### Delta Table Version Conflicts

📋 **Error Message:**
```
ConcurrentAppendException: Files were added to the root of the table by a concurrent update.
# or
ConcurrentDeleteReadException: This transaction attempted to read files that were deleted by a concurrent commit.
```

💡 **What It Means:**
Multiple processes tried to write to the same Delta table at the same time, causing a conflict.

🔍 **Why It Happens:**
- Two pipeline runs overlap (same table, same time)
- Parallel nodes trying to write to the same table
- Streaming job and batch job writing to same table
- Previous job didn't complete before retry started

✅ **Step-by-Step Fix:**

**Option 1: Add retry with backoff:**
```yaml
retry:
  enabled: true
  max_attempts: 3
  backoff: exponential
  initial_delay: 5  # seconds
```

**Option 2: Use merge instead of append (for some cases):**
```yaml
# Merge is idempotent - safe to retry
transformer: "merge"
params:
  target: "silver.dim_customers"
  keys: ["customer_id"]
```

**Option 3: Ensure serial execution:**
```yaml
# In your orchestrator (Airflow, Databricks Workflows):
# - Don't allow concurrent runs of same pipeline
# - Or partition writes by date
```

🛡️ **Prevention:**
- Use unique write paths for parallel jobs
- Configure orchestrator to prevent overlapping runs
- Use Delta Lake isolation levels appropriately

---

## Common Odibi Configuration Errors

These are the most frequent errors beginners encounter when configuring Odibi pipelines. Don't panic—each one has a straightforward fix.

---

### Schema Mismatch: Column Not Found in DataFrame

**Error message:**
```
AnalysisException: Cannot resolve column name 'customer_id' among [CustomerID, order_date, amount]
```

**What it means:** You referenced a column name in your YAML config that doesn't exist in the actual DataFrame.

**Why it happened:**
- Typo in the column name
- Wrong case (column names are case-sensitive)
- Column was renamed or dropped in an upstream step

**Step-by-step fix:**

1. Check the exact column names in your DataFrame:
   ```python
   print(df.columns)  # Pandas/Polars
   df.printSchema()   # Spark
   ```
2. Compare with what you have in your YAML
3. Update the YAML to match the exact column name (including case)

**YAML before (broken):**
```yaml
pattern: dimension
config:
  natural_key: customer_id  # Wrong case!
  columns:
    - customer_id
    - name
```

**YAML after (fixed):**
```yaml
pattern: dimension
config:
  natural_key: CustomerID  # Matches DataFrame exactly
  columns:
    - CustomerID
    - name
```

**How to prevent it next time:**
- Always print `df.columns` before writing your YAML
- Use consistent naming conventions (snake_case recommended)
- Add a schema validation step at pipeline start

---

### Column Not Found in Pattern Config

**Error message:**
```
KeyError: 'customer_id'
```
or
```
Column 'customer_id' not found in DataFrame
```

**What it means:** A column specified in your pattern configuration doesn't exist in the data.

**Why it happened:**
- Column name mismatch (typo or case difference)
- Column was renamed in a previous transform
- Column exists in source but not in transformed data

**Step-by-step fix:**

1. Identify which config field is causing the error (the traceback usually shows this)
2. Print your DataFrame columns at the point of failure
3. Match your config to the actual column names

**YAML before (broken):**
```yaml
pattern: fact
config:
  grain:
    - order_id
    - line_item
  measures:
    - qty      # Wrong! Column is actually 'quantity'
    - amount
```

**YAML after (fixed):**
```yaml
pattern: fact
config:
  grain:
    - order_id
    - line_item
  measures:
    - quantity  # Matches DataFrame column
    - amount
```

**How to prevent it next time:**
- Document expected column names in comments
- Use a pre-flight check that validates all columns exist before processing

---

### UnionByName Failures (SCD2 Target/Source Mismatch)

**Error message:**
```
AnalysisException: Cannot resolve column name 'is_current' among [customer_id, name, effective_date]
```
or
```
ValueError: Cannot union DataFrames with different columns
```

**What it means:** When merging source data with an existing target table (common in SCD2), the schemas don't match. The target has columns the source doesn't have.

**Why it happened:**
- Target table has SCD2-specific columns (`is_current`, `effective_from`, `effective_to`, `row_hash`)
- Source data doesn't include these columns (and shouldn't—the pattern adds them)
- Previous schema changes weren't migrated properly

**Step-by-step fix:**

1. Let the SCD2 pattern add the tracking columns—don't add them to source
2. If manually fixing, ensure both schemas match:
   ```python
   # Check target schema
   target_df.printSchema()
   
   # Check what SCD2 expects to add
   # is_current, effective_from, effective_to, row_hash
   ```
3. If target has extra columns, either:
   - Add them to source with null values
   - Rebuild target with correct schema

**YAML before (broken):**
```yaml
pattern: scd2
config:
  natural_key: customer_id
  effective_time_col: load_date
  # Source has: customer_id, name, load_date
  # Target has: customer_id, name, load_date, is_current, effective_from, effective_to, row_hash
  # MISMATCH! But this is expected - SCD2 adds those columns
```

**YAML after (fixed):**
```yaml
pattern: scd2
config:
  natural_key: customer_id
  effective_time_col: load_date
  # Let the pattern handle the SCD2 columns automatically
  # Don't pre-add them to your source data
```

**How to prevent it next time:**
- Never manually add SCD2 tracking columns to source data
- When bootstrapping, let the pattern create the initial schema
- Document which columns are managed by the pattern

---

### Spaces in Column Names

**Error message:**
```
AnalysisException: Column name 'Customer Name' cannot be resolved
```
or
```
KeyError: 'Customer Name'
```

**What it means:** Your column names contain spaces, which cause parsing issues.

**Why it happened:**
- Data imported from Excel with human-readable headers
- Source system uses spaces in column names
- CSV headers weren't cleaned before loading

**Step-by-step fix:**

1. **Option A:** Rename columns in source (recommended):
   ```python
   # Pandas
   df.columns = df.columns.str.replace(' ', '_')
   
   # Spark
   for col in df.columns:
       df = df.withColumnRenamed(col, col.replace(' ', '_'))
   
   # Polars
   df = df.rename({col: col.replace(' ', '_') for col in df.columns})
   ```

2. **Option B:** Use backticks in YAML (Spark only):
   ```yaml
   columns:
     - "`Customer Name`"
   ```

**YAML before (broken):**
```yaml
pattern: dimension
config:
  natural_key: Customer ID  # Space causes issues!
  columns:
    - Customer ID
    - Customer Name
    - Email Address
```

**YAML after (fixed):**
```yaml
pattern: dimension
config:
  natural_key: customer_id  # Clean snake_case
  columns:
    - customer_id
    - customer_name
    - email_address
```

**Pre-processing step to add:**
```python
# Add this before any pattern processing
df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
```

**How to prevent it next time:**
- Always clean column names at the start of your pipeline
- Establish a naming convention (snake_case is standard)
- Add a column name validator to your ingestion layer

---

### SCD2 effective_time_col Errors

**Error message:**
```
KeyError: 'effective_time_col'
```
or
```
Column 'txn_date' not found in DataFrame
```

**What it means:** The column you specified as `effective_time_col` doesn't exist in your source data at the point where SCD2 needs it.

**Why it happened:**
- Column was renamed in an earlier transform step
- Column name has a typo
- Column was dropped before reaching SCD2 pattern
- You're referencing a derived column that doesn't exist yet

**Step-by-step fix:**

1. Verify the column exists in your source data:
   ```python
   print('txn_date' in df.columns)  # Should be True
   ```
2. Check if any transform renamed or dropped it
3. Update the config to use the correct column name

**YAML before (broken):**
```yaml
pattern: scd2
config:
  natural_key: customer_id
  effective_time_col: txn_date  # Oops! Column was renamed to 'transaction_date'
```

**YAML after (fixed):**
```yaml
pattern: scd2
config:
  natural_key: customer_id
  effective_time_col: transaction_date  # Matches actual column name
```

**Common gotcha:** The `effective_time_col` must exist in your **source** DataFrame. It gets used to set `effective_from` dates and may be dropped after processing.

**How to prevent it next time:**
- Print `df.columns` right before the SCD2 pattern runs
- Keep your transformation pipeline documented
- Use meaningful, consistent column names throughout

---

### Connection Not Found

**Error message:**
```
ConnectionError: Connection 'warehouse' not found in project config
```
or
```
KeyError: 'warehouse'
```

**What it means:** You referenced a connection name that isn't defined in your project configuration.

**Why it happened:**
- Connection not defined in project config
- Typo in connection name
- Connection section missing entirely
- Environment-specific config not loaded

**Step-by-step fix:**

1. Check your project config file structure
2. Add or fix the connections section
3. Ensure connection name matches exactly (case-sensitive)

**YAML before (broken):**
```yaml
# pipeline.yaml
sources:
  - name: customers
    connection: Warehouse  # Wrong case!
    table: dim_customer
```

**YAML after (fixed):**
```yaml
# project_config.yaml - Must have connections defined
connections:
  warehouse:  # lowercase to match
    type: databricks
    catalog: main
    schema: gold

# pipeline.yaml
sources:
  - name: customers
    connection: warehouse  # Matches connection name exactly
    table: dim_customer
```

**How to prevent it next time:**
- Use lowercase connection names consistently
- Keep a template project config with all required sections
- Validate config on pipeline startup

---

### Delta Table Version Conflicts

**Error message:**
```
ConcurrentModificationException: Conflicting commits
```
or
```
DeltaTableVersionMismatch: Expected version X but found version Y
```
or
```
ConcurrentAppendException: Files were added by a concurrent update
```

**What it means:** Multiple processes tried to write to the same Delta table simultaneously, or your reference to the table is stale.

**Why it happened:**
- Two pipelines writing to the same table at the same time
- Long-running transaction conflicted with another write
- Cached table reference is outdated
- Overwrite operation conflicted with append

**Step-by-step fix:**

1. **Identify the conflict source:**
   ```python
   # Check Delta table history
   from delta.tables import DeltaTable
   dt = DeltaTable.forPath(spark, "path/to/table")
   dt.history().show()
   ```

2. **Use merge instead of overwrite:**
   ```python
   # Instead of overwrite (can conflict)
   df.write.format("delta").mode("overwrite").save(path)
   
   # Use merge (handles concurrency better)
   delta_table.alias("target").merge(
       df.alias("source"),
       "target.id = source.id"
   ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
   ```

3. **Add retry logic:**
   ```python
   from tenacity import retry, stop_after_attempt, wait_exponential
   
   @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1))
   def safe_write(df, path):
       df.write.format("delta").mode("append").save(path)
   ```

**YAML before (problematic):**
```yaml
pattern: merge
config:
  write_mode: overwrite  # Can cause conflicts with concurrent writes
```

**YAML after (safer):**
```yaml
pattern: merge
config:
  write_mode: merge  # Handles concurrent operations better
  merge_keys:
    - customer_id
```

**How to prevent it next time:**
- Avoid concurrent writes to the same table
- Use merge patterns instead of overwrite when possible
- Implement job orchestration to serialize conflicting writes
- Enable Delta Lake optimistic concurrency settings

---

## Azure-Specific Troubleshooting

### ADLS Authentication

#### Credential Issues

```
AuthenticationError: Invalid credentials
```

**Checklist:**
1. Verify service principal credentials are correct
2. Check tenant ID, client ID, client secret
3. Ensure service principal has Storage Blob Data Contributor role

```python
from azure.identity import DefaultAzureCredential
credential = DefaultAzureCredential()
# If using service principal:
from azure.identity import ClientSecretCredential
credential = ClientSecretCredential(tenant_id, client_id, client_secret)
```

#### Access Token Expiry

```
TokenExpiredError: Token has expired
```

**Fix:** Use `DefaultAzureCredential` which handles token refresh automatically.

### Delta Table Errors

#### Storage Throttling (429 Errors)

```
TooManyRequests: Rate limit exceeded
```

**Fixes:**
1. Implement retry logic with exponential backoff
2. Reduce concurrent operations
3. Batch smaller writes

```python
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, max=60))
def write_with_retry(df, path):
    df.write.format("delta").save(path)
```

#### Concurrent Write Conflicts

```
ConcurrentAppendException: Files were added by a concurrent update
```

**Fixes:**
1. Enable optimistic concurrency: Set `delta.enableChangeDataFeed = true`
2. Use merge instead of overwrite when possible
3. Coordinate write operations to avoid conflicts

#### File Locking Issues

If writes hang or fail with lock errors:
1. Check for stale lock files in `_delta_log/`
2. Wait for other operations to complete
3. Consider using a single writer pattern

### Azure SQL Issues

#### ODBC Driver Setup

```
Error: [unixODBC][Driver Manager]Can't open lib 'ODBC Driver 17 for SQL Server'
```

**Fix (Ubuntu/WSL):**
```bash
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17
```

#### Connection Timeout

```
OperationalError: Connection timed out
```

**Fix:** Increase connection timeout:
```python
from sqlalchemy import create_engine
engine = create_engine(
    connection_string,
    connect_args={"timeout": 60}
)
```

#### SQLAlchemy Configuration

```python
# Full connection string example
connection_string = (
    "mssql+pyodbc://user:password@server.database.windows.net:1433/"
    "database?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no"
)
```

---

## WSL/Linux Setup Issues

### Python Command Not Found

```
FileNotFoundError: [Errno 2] No such file or directory: 'python'
```

**Fix:** Either create a symlink or install the package:
```bash
# Option 1: Install symlink package
sudo apt install python-is-python3

# Option 2: Use python3.9 explicitly
python3.9 -m pytest tests/
```

### Missing Dependencies

```
ModuleNotFoundError: No module named 'sqlalchemy'
```

**Fix:**
```bash
pip3.9 install sqlalchemy pyodbc
```

### Spark Workers Python Version Mismatch

Ensure all workers use the same Python:
```bash
# Add to ~/.bashrc or set before running Spark
export PYSPARK_PYTHON=/usr/bin/python3.9
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.9
```

---

## Getting Help

### How to Report Bugs

1. Check [GitHub Issues](https://github.com/henryodibi11/Odibi/issues) for existing reports
2. Create a new issue with:
   - Odibi version
   - Python version
   - Engine (Pandas/Spark/Polars)
   - Full error message and traceback
   - Minimal reproducible example

### Required Info for Bug Reports

```markdown
**Environment:**
- Odibi version: X.X.X
- Python version: 3.X
- OS: Windows/Linux/WSL
- Engine: Pandas/Spark/Polars

**Error:**
[Paste full traceback]

**Reproduction:**
[Minimal code to reproduce]

**Expected behavior:**
[What you expected to happen]
```

### Links

- [GitHub Issues](https://github.com/henryodibi11/Odibi/issues) - Report bugs and request features
- [Discussions](https://github.com/henryodibi11/Odibi/discussions) - Ask questions and share ideas
- [Roadmap](./ROADMAP.md) - See what's coming next

---

## Learning Resources

New to Odibi or data engineering? Start here:

- [Data Engineering 101](./learning/data_engineering_101.md) - Complete beginner's guide
- [Glossary](./learning/glossary.md) - Every term explained simply
- [4-Week Curriculum](./learning/curriculum.md) - Structured learning path
- [Anti-Patterns Guide](./patterns/anti_patterns.md) - What NOT to do
- [SCD2 Pattern](./patterns/scd2.md) - History tracking with troubleshooting
