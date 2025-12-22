# Troubleshooting Guide

Quick reference for diagnosing and fixing common Odibi issues.

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

1. Check if it's already documented in [BUGS.md](https://github.com/henryodibi11/Odibi/blob/main/BUGS.md)
2. Check [GAPS.md](https://github.com/henryodibi11/Odibi/blob/main/GAPS.md) for known limitations
3. Create a GitHub issue with:
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

- [BUGS.md](https://github.com/henryodibi11/Odibi/blob/main/BUGS.md) - Fixed bugs and workarounds
- [GAPS.md](https://github.com/henryodibi11/Odibi/blob/main/GAPS.md) - Known gaps and roadmap
- [GitHub Issues](https://github.com/henryodibi11/Odibi/issues) - Report new issues
