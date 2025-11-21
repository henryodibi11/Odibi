# Troubleshooting Guide - Common Issues & Solutions

**Quick solutions to common problems. Get unblocked fast!**

---

## Table of Contents

1. [Installation Issues](#installation-issues)
2. [Configuration Errors](#configuration-errors)
3. [Runtime Errors](#runtime-errors)
4. [Performance Issues](#performance-issues)
5. [Story Generation Issues](#story-generation-issues)
6. [Connection Problems](#connection-problems)

---

## Installation Issues

### Problem: "No module named 'odibi'"

**Symptoms:**
```bash
>>> import odibi
ModuleNotFoundError: No module named 'odibi'
```

**Solution:**
```bash
# Install Odibi
pip install odibi

# Or install from source
cd /path/to/odibi
pip install -e .
```

**Verify:**
```bash
python -c "import odibi; print(odibi.__version__)"
```

---

### Problem: "No module named 'pyspark'"

**Symptoms:**
```bash
odibi run config.yaml
ImportError: pyspark is not installed
```

**Solution:**
```bash
# Install Spark extras
pip install "odibi[spark]"

# Or just Spark
pip install pyspark delta-spark
```

---

### Problem: "DuckDB not found"

**Symptoms:**
```bash
ImportError: DuckDB is required for SQL operations
```

**Solution:**
```bash
# DuckDB is optional but recommended
pip install duckdb
```

**Alternative:** Use pandas operations instead of SQL

---

## Configuration Errors

### Problem: "Validation error for NodeConfig"

**Symptoms:**
```
pydantic_core._pydantic_core.ValidationError: 1 validation error for NodeConfig
name
  Field required [type=missing]
```

**Cause:** Missing required field in YAML

**Solution:**
```yaml
# ❌ Bad
nodes:
  - description: My node
    read: ...

# ✅ Good
nodes:
  - name: my_node  # ← Required!
    description: My node
    read: ...
```

**Common required fields:**
- `name` (every node)
- `connection` (in read/write configs)
- `path` or `table` (in read/write configs)

---

### Problem: "Circular dependency detected"

**Symptoms:**
```
DependencyError: Circular dependency detected: A → B → C → A
```

**Cause:** Nodes depend on each other in a loop

**Example:**
```yaml
nodes:
  - name: A
    depends_on: [C]  # ← A needs C
  - name: B
    depends_on: [A]  # B needs A
  - name: C
    depends_on: [B]  # C needs B → LOOP!
```

**Solution:** Break the cycle

```yaml
nodes:
  - name: A  # First
  - name: B
    depends_on: [A]
  - name: C
    depends_on: [B]
# Linear: A → B → C ✅
```

---

### Problem: "Connection 'xyz' not found"

**Symptoms:**
```
ValueError: Connection 'bronze_storage' not found
```

**Cause:** Typo in connection name or connection not defined

**Solution:**
```yaml
# Define connection first
connections:
  bronze_storage:  # ← Must match exactly
    type: local
    base_path: ./data

# Use in node
nodes:
  - name: load
    read:
      connection: bronze_storage  # ← Same name
      path: data.csv
```

---

### Problem: "Operation 'xyz' not registered"

**Symptoms:**
```
ValueError: Operation 'unpivott' not found
```

**Cause:** Typo in operation name

**Solution:**
```yaml
# ❌ Wrong
transform:
  operation: unpivott  # Typo!

# ✅ Correct
transform:
  operation: unpivot  # Built-in operation
```

**List available operations:**
```python
from odibi.transformations import get_registry
print(list(get_registry().list_all().keys()))
# ['pivot', 'unpivot', 'join', 'sql']
```

---

## Runtime Errors

### Problem: "Column 'xyz' not found"

**Symptoms:**
```
KeyError: 'temperature'
```

**Cause:** SQL query or transformation references non-existent column

**Solution:**

**1. Check column names:**
```yaml
# Before transformation, add debug node:
- name: check_schema
  transform:
    operation: sql
    query: "SELECT * FROM df LIMIT 5"
# Check story to see actual column names
```

**2. Fix query:**
```yaml
# ❌ Wrong column name
query: "SELECT temperature FROM df"

# ✅ Correct column name
query: "SELECT Temperature FROM df"  # Case-sensitive!
```

---

### Problem: "No data in context"

**Symptoms:**
```
ContextError: Node 'transform' has no data
```

**Cause:** Node has transform but no depends_on and no read

**Solution:**
```yaml
# ❌ Bad - where does data come from?
- name: transform
  transform:
    operation: sql
    query: "SELECT * FROM df"

# ✅ Good - reads data first
- name: load
  read:
    connection: local
    path: data.csv

- name: transform
  depends_on: [load]  # ← Gets data from load
  transform:
    operation: sql
    query: "SELECT * FROM df"
```

---

### Problem: "Division by zero"

**Symptoms:**
```
ZeroDivisionError: division by zero
```

**Cause:** Calculation divides by zero

**Solution:** Add null check in SQL

```yaml
# ❌ Bad
query: "SELECT output / input AS ratio FROM df"

# ✅ Good
query: |
  SELECT
    CASE
      WHEN input = 0 OR input IS NULL THEN NULL
      ELSE output / input
    END AS ratio
  FROM df
```

---

### Problem: "Memory error with large files"

**Symptoms:**
```
MemoryError: Unable to allocate array
```

**Cause:** DataFrame too large for available RAM

**Solutions:**

**1. Use Parquet instead of CSV**
```yaml
# Parquet is compressed and faster
format: parquet  # Instead of csv
```

**2. Filter early:**
```yaml
# ❌ Bad - load everything then filter
- name: load_all
  read:
    path: huge_file.csv  # 10GB
- name: filter
  transform:
    query: "SELECT * FROM df WHERE plant = 'NKC'"

# ✅ Good - filter during read (if possible)
read:
  path: huge_file.csv
  query: "SELECT * FROM df WHERE plant = 'NKC'"  # Load less
```

**3. Use Spark engine:**
```yaml
engine: spark  # Handles large data
```

---

## Performance Issues

### Problem: Pipeline runs slowly

**Symptoms:** Pipeline takes minutes instead of seconds

**Debug steps:**

**1. Check story for slow nodes**

Open `stories/runs/pipeline_xxx.html`:
- Look at duration column
- Find slowest node
- Investigate that node specifically

**2. Common slow operations:**

**Slow: Multiple small SQL queries**
```yaml
# ❌ Slow - multiple transforms
- name: filter1
  transform:
    query: "SELECT * FROM df WHERE condition1"
- name: filter2
  transform:
    query: "SELECT * FROM df WHERE condition2"

# ✅ Fast - combine in one query
- name: filter_all
  transform:
    query: |
      SELECT * FROM df
      WHERE condition1 AND condition2
```

**Slow: CSV reads**
```yaml
# ❌ Slow
format: csv

# ✅ Fast
format: parquet
```

**Slow: Cartesian joins**
```yaml
# ❌ Creates 1M × 1M = 1 trillion rows!
operation: join
how: outer  # Dangerous with large datasets
```

**Solutions:**
- Combine SQL operations
- Use Parquet format
- Add WHERE clauses to limit data
- Switch to Spark for large data

---

## Story Generation Issues

### Problem: "Stories not being generated"

**Symptoms:** No files in `stories/runs/`

**Causes & Solutions:**

**1. Story connection not configured:**
```yaml
# Add story config:
story:
  connection: local
  path: stories/
```

**2. Directory permissions:**
```bash
# Ensure directory exists and is writable
mkdir -p stories/runs
chmod 755 stories/runs
```

**3. Check pipeline code:**
```python
# In your code, ensure:
generate_story=True  # Default, but check if overridden
```

---

### Problem: "Story shows ❌ Failed but pipeline reported success"

**Cause:** Node failed but error was caught

**Solution:** Check story for error details

Open HTML story → Look for error boxes → Read error message

---

### Problem: "odibi story generate fails"

**Symptoms:**
```
ValueError: No pipelines found in configuration
```

**Solution:**
```yaml
# Ensure pipelines section exists:
pipelines:  # ← Required
  - pipeline: my_pipeline
    nodes: [...]
```

---

## Connection Problems

### Problem: "Azure ADLS authentication failed"

**Symptoms:**
```
AuthenticationError: Unable to authenticate to Azure storage
```

**Solutions:**

**1. Check auth mode:**
```yaml
connections:
  azure:
    auth_mode: key_vault  # Or 'direct_key'
```

**2. Verify Key Vault access:**
```python
# Test Key Vault access
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

credential = DefaultAzureCredential()
client = SecretClient(
    vault_url="https://myvault.vault.azure.net",
    credential=credential
)
secret = client.get_secret("storage-key")
print("✅ Key Vault access works!")
```

**3. Use direct key for local development:**
```yaml
connections:
  azure:
    auth_mode: direct_key
    account_key: "your_key_here"  # For testing only!
```

---

### Problem: "Azure SQL connection timeout"

**Symptoms:**
```
TimeoutError: Connection timeout after 30s
```

**Solutions:**

**1. Increase timeout:**
```yaml
connections:
  sql_db:
    server: myserver.database.windows.net
    database: mydb
    timeout: 60  # Increase to 60 seconds
```

**2. Check firewall rules:**
- Azure SQL → Firewall settings
- Add your IP address
- Or enable "Allow Azure services"

**3. Check authentication:**
```yaml
# If using SQL auth:
auth_mode: sql
username: myuser
password: mypassword

# If using AAD:
auth_mode: aad_msi
```

---

### Problem: "File not found" with local connection

**Symptoms:**
```
FileNotFoundError: data/input.csv not found
```

**Solutions:**

**1. Check base_path:**
```yaml
connections:
  local:
    base_path: ./data  # Relative to where you run command
```

**2. Use absolute paths:**
```yaml
connections:
  local:
    base_path: /absolute/path/to/data
```

**3. Check current directory:**
```bash
pwd  # Where are you?
ls data/input.csv  # Does file exist here?
```

---

## Debugging Techniques

### Technique 1: Check the Story

**Every run creates a story!**

1. Run pipeline (even if it fails)
2. Open `stories/runs/pipeline_xxx.html`
3. Look for:
   - ❌ Which node failed?
   - 📊 Unexpected row counts?
   - 🔄 Missing columns?
   - ⏱️ Slow nodes?

**Stories tell you EXACTLY what happened.**

### Technique 2: Add Debug Nodes

```yaml
nodes:
  - name: load_data
    read: ...

  - name: debug_check_schema  # ← Debug node
    depends_on: [load_data]
    transform:
      operation: sql
      query: "SELECT * FROM df LIMIT 5"
  # Check story to see what columns actually exist!

  - name: actual_transform
    depends_on: [load_data]
    transform:
      query: "SELECT correct_column FROM df"
```

### Technique 3: Validate Configuration

```bash
# Always validate before running
odibi validate config.yaml

# Catches:
# - Missing fields
# - Invalid connections
# - Circular dependencies
# - Typos
```

### Technique 4: Test Transformations in Python

```python
# Interactive testing
python

>>> from odibi.transformations import get_registry
>>> import odibi.operations.unpivot
>>>
>>> registry = get_registry()
>>> unpivot = registry.get("unpivot")
>>>
>>> import pandas as pd
>>> df = pd.DataFrame({"ID": ["A"], "X": [10], "Y": [20]})
>>> result = unpivot(df, id_vars="ID")
>>> print(result)
# See if it works!
```

### Technique 5: Use Verbose Mode

```bash
# Run with verbose output (future feature)
odibi run config.yaml --verbose

# For now, check stories for detailed execution info
```

---

## Error Messages Decoded

### "Field required [type=missing]"

**Means:** Pydantic validation failed - you forgot a required field

**Fix:** Add the missing field to your YAML

```yaml
# Error says: "name: Field required"
# Fix: Add name field
- name: my_node  # ← Add this
  read: ...
```

---

### "Operation 'xyz' not found in registry"

**Means:** No transformation registered with that name

**Fixes:**
1. Check spelling: `unpivot` not `unpivott`
2. Import your custom operations before running
3. List available: `registry.list_all().keys()`

---

### "Transformation must have a docstring"

**Means:** Your custom transformation has no/short docstring

**Fix:**
```python
# ❌ Bad
@transformation("my_op")
def my_op(df):
    return df

# ✅ Good
@transformation("my_op")
def my_op(df):
    """Detailed description of what this does."""
    return df
```

---

### "Connection validation failed"

**Means:** Connection config is incomplete

**Fix:** Add required fields

```yaml
# Each connection type has required fields:

local:
  type: local
  base_path: ./data  # ← Required

azure_adls:
  type: azure_adls
  account: xxx       # ← Required
  container: xxx     # ← Required

azure_sql:
  type: azure_sql
  server: xxx        # ← Required
  database: xxx      # ← Required
```

---

## Common YAML Mistakes

### Mistake 1: Indentation

```yaml
# ❌ Wrong indentation
nodes:
- name: my_node
description: My description  # ← Wrong level!
  read:
    connection: local

# ✅ Correct indentation
nodes:
  - name: my_node
    description: My description  # ← Correct level
    read:
      connection: local
```

**YAML is whitespace-sensitive!** Use 2 spaces per level.

### Mistake 2: Missing Colons

```yaml
# ❌ Missing colon
nodes
  - name: my_node  # ← YAML error!

# ✅ Has colon
nodes:  # ← Correct
  - name: my_node
```

### Mistake 3: Quotes in Strings

```yaml
# Usually optional
path: data/file.csv

# Required if special characters:
path: "data/file with spaces.csv"
path: "data/file:with:colons.csv"

# Required in SQL with quotes:
query: 'SELECT * FROM df WHERE name = "Alice"'
# Or escape:
query: "SELECT * FROM df WHERE name = 'Alice'"
```

---

## Performance Troubleshooting

### Slow Pipeline? Check This

**1. Open the story HTML**

Look at duration column:
```
load_data:      0.5s   ✅ Fast
transform:     45.2s   ❌ SLOW!  ← Problem here
save_data:      0.8s   ✅ Fast
```

**2. Investigate the slow node**

Common causes:
- Large CSV file → Use Parquet
- Complex SQL → Optimize query
- Cartesian join → Add WHERE clause
- No indexes → Not applicable (DataFrames don't have indexes)

**3. Optimize:**

```yaml
# Before: Slow SQL
query: |
  SELECT a.*, b.*
  FROM df a, df b  # ← Cartesian product! 1M × 1M rows!

# After: Fast SQL  
query: |
  SELECT a.*, b.*
  FROM df a
  JOIN df b ON a.id = b.id  # ← Proper join
```

---

## File Format Issues

### Problem: "Cannot read file xyz.csv"

**Solutions:**

**1. Check encoding:**
```yaml
read:
  path: data.csv
  format: csv
  options:
    encoding: utf-8  # Or cp1252, iso-8859-1, etc.
```

**2. Check delimiter:**
```yaml
options:
  sep: ";"  # Or tab: "\t", pipe: "|"
```

**3. Skip bad rows:**
```yaml
options:
  on_bad_lines: skip  # Skip rows that don't parse
```

---

### Problem: "Parquet file schema mismatch"

**Symptoms:**
```
ArrowInvalid: Schema mismatch
```

**Cause:** Trying to append data with different schema

**Solution:**

**Option 1:** Use overwrite mode
```yaml
write:
  mode: overwrite  # Replace file
```

**Option 2:** Match schema
```yaml
# Ensure columns match before writing:
- name: align_schema
  transform:
    operation: sql
    query: |
      SELECT
        id,
        name,
        value  -- Same columns as existing file
      FROM df
```

---

## Testing Issues

### Problem: "Tests fail in CI but pass locally"

**Causes:**
1. Different Python versions
2. Missing dependencies
3. Path differences (Windows vs Linux)
4. Timezone issues

**Solutions:**

**1. Test multiple Python versions:**
```bash
# Use tox or test manually:
python3.9 -m pytest
python3.10 -m pytest
python3.11 -m pytest
python3.12 -m pytest
```

**2. Pin dependencies:**
```bash
pip freeze > requirements.txt
```

**3. Use Path objects:**
```python
# ❌ Platform-specific
path = "data/file.csv"  # / vs \ issue

# ✅ Cross-platform
from pathlib import Path
path = Path("data") / "file.csv"
```

---

## Story-Specific Issues

### Problem: "HTML story not rendering properly"

**Solutions:**

**1. Check browser:**
- Open in Chrome/Firefox/Edge
- Some old browsers don't support modern CSS

**2. Check file encoding:**
```bash
# Should be UTF-8
file --mime-encoding story.html
```

**3. Try markdown format:**
```bash
odibi story generate config.yaml --format markdown
```

---

### Problem: "Theme not applying"

**Symptoms:** Story looks like default theme

**Solutions:**

**1. Check theme name:**
```bash
# List available themes:
python -c "from odibi.story import list_themes; print(list_themes().keys())"
# dict_keys(['default', 'corporate', 'dark', 'minimal'])
```

**2. Use correct theme:**
```bash
odibi story generate config.yaml --theme dark
```

**3. For custom theme, check YAML:**
```yaml
# custom_theme.yaml
name: my_theme
primary_color: "#ff0000"
# ... other settings ...
```

```bash
odibi story generate config.yaml --theme custom_theme.yaml
```

---

## Getting Help

### Quick Debug Checklist

When something goes wrong:

- [ ] Run `odibi validate config.yaml`
- [ ] Check the error message carefully
- [ ] Look at the story HTML (even if pipeline failed)
- [ ] Test transformation interactively in Python
- [ ] Check this troubleshooting guide
- [ ] Look at existing tests for similar patterns

### Where to Look

**1. Error messages**
- Read carefully - they're usually helpful!
- Pydantic errors tell you exact field
- Stack traces show where it failed

**2. Stories**
- Every run creates a story
- Shows what actually happened
- Error details included

**3. Tests**
- See how operations should be used
- Example: `tests/unit/test_operations.py`

**4. Examples**
- Check `examples/` directory
- Working configurations

**5. Documentation**
- These guides!
- Code docstrings

---

## Still Stuck?

### Create a Minimal Reproduction

```yaml
# minimal_config.yaml - simplest possible version

project: Debug Test

connections:
  local:
    type: local
    base_path: ./data

story:
  connection: local
  path: stories/

pipelines:
  - pipeline: minimal
    nodes:
      - name: test
        read:
          connection: local
          path: test.csv
          format: csv
```

```bash
# Create tiny test file
echo "id,value" > data/test.csv
echo "1,100" >> data/test.csv

# Run minimal config
odibi run minimal_config.yaml
```

**If this works:** Problem is in your complex config. Simplify step by step.

**If this fails:** Problem is with installation/environment.

---

## Pro Tips

### Tip 1: Use Stories for Everything

Stories are your best debugging tool:
- See actual execution
- See actual data shapes
- See actual timings
- See actual errors

**Always check the story first!**

### Tip 2: Test Incrementally

```yaml
# Don't build entire pipeline at once!

# Step 1: Just load
nodes:
  - name: load
    read: ...

# Test: odibi run config.yaml
# Check story: ✅ Works!

# Step 2: Add one transform
nodes:
  - name: load
    read: ...
  - name: transform1  # ← Add one at a time
    transform: ...

# Test: odibi run config.yaml
# Check story: ✅ Works!

# Continue adding nodes one by one...
```

### Tip 3: Use SQL for Complex Logic

Instead of writing Python:
```python
@transformation("complex_calc")
def complex_calc(df, col1, col2, col3):
    # 50 lines of pandas code...
```

Use SQL:
```yaml
transform:
  operation: sql
  query: |
    SELECT *,
      (col1 + col2) / col3 AS result
    FROM df
    WHERE col1 > 0
```

SQL is:
- ✅ Easier to read
- ✅ Easier to debug
- ✅ Easier to modify
- ✅ Works with DuckDB (fast!)

---

## FAQ

**Q: Pipeline worked yesterday, fails today. What changed?**

**A:** Compare stories!
```bash
odibi story diff yesterday.json today.json --detailed
```

Shows exactly what's different.

---

**Q: How do I debug a transformation?**

**A:** Test it interactively:
```python
python
>>> from odibi.transformations import get_registry
>>> func = registry.get("my_operation")
>>> import pandas as pd
>>> df = pd.DataFrame(...)  # Your test data
>>> result = func(df, param1=..., param2=...)
>>> print(result)
```

---

**Q: Can I see intermediate results?**

**A:** Yes! Check the story or add debug nodes:
```yaml
- name: step1
  transform: ...

- name: debug_step1  # ← Check result
  depends_on: [step1]
  write:
    path: debug_step1.csv  # ← Save it

- name: step2
  depends_on: [step1]  # Continue pipeline
  transform: ...
```

---

**Q: Tests pass but pipeline fails?**

**A:** Common causes:
1. Test uses mock data, pipeline uses real data with edge cases
2. Test DataFrame small, real DataFrame triggers memory issues
3. Missing data in real files

**Solution:** Make tests use realistic data

---

## Emergency Procedures

### Pipeline Stuck?

**Ctrl+C** to cancel

### Need to Skip a Node?

```yaml
# Comment it out temporarily:
nodes:
  - name: working_node
    read: ...

  # - name: broken_node  # ← Commented out
  #   transform: ...

  - name: after_node
    depends_on: [working_node]  # Skip broken_node
    write: ...
```

### Data Corrupted?

```yaml
# Rerun from source:
- name: load_from_original_source
  read:
    connection: original_source
    path: backup/data.csv

# Use mode: overwrite to replace bad data
write:
  mode: overwrite  # ← Replaces corrupted file
```

---

## Next Steps

**You now know how to troubleshoot Odibi!**

**Remember:**
1. ✅ Stories are your best friend
2. ✅ Validate often
3. ✅ Test incrementally
4. ✅ Read error messages carefully

**Still have questions?**
- Check other guides
- Read the tests
- Look at working examples

---

**Most issues can be solved by checking the story!** 📊
