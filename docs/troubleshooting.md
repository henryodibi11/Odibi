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

#### Schema Mismatch Errors

```
Schema of data does not match table schema
```

**Cause:** DataFrame columns don't match the Delta table schema.

**Fix:** Ensure column types match exactly:
```python
# Check schemas before writing
print(df.dtypes)
# Cast columns if needed
df['column'] = df['column'].astype('string')
```

#### PyArrow Engine Limitations

```
schema_mode 'merge' is not supported in pyarrow engine. Use engine=rust
```

**Cause:** The PyArrow engine doesn't support schema evolution with `schema_mode='merge'`.

**Fix:** Either:
1. Use the Rust engine: `engine='rust'`
2. Remove `schema_mode='merge'` for append-only operations (schema is fixed at bootstrap)

#### Catalog log_run Failures

If `log_run` fails with schema errors:
1. Schema is fixed at bootstrap time
2. Use exact column types that match the run log schema
3. Serialize complex types (like lists) to JSON strings

#### Timezone Mismatch Errors

```
TypeError: can't subtract offset-naive and offset-aware datetimes
```

**Cause:** Mixing timezone-naive (`datetime.now()`) and timezone-aware (`datetime.now(timezone.utc)`) timestamps in the same DataFrame or comparison.

**Common scenarios:**
- Freshness validation comparing data timestamps against `datetime.now()`
- Unknown member rows with `datetime(1900, 1, 1)` alongside tz-aware `load_timestamp`
- Delta Lake reads returning UTC timestamps compared against naive Python datetimes

**Fix:** Always use timezone-aware timestamps:
```python
from datetime import datetime, timezone
now = datetime.now(timezone.utc)  # Not datetime.now()
```

**Prevention:** Odibi uses `timezone.utc` throughout. If you write custom transformers, always use `datetime.now(timezone.utc)`.

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

**Common gotcha:** The `effective_time_col` must exist in your **source** DataFrame. It gets renamed to `start_time_col` (default: `valid_from`) in the target.

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
