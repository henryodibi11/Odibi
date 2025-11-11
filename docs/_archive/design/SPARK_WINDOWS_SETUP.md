# Setting Up PySpark on Windows

**Issue:** PySpark requires Hadoop binaries on Windows, which aren't included by default.

**Error:** `Py4JJavaError: ... HADOOP_HOME or hadoop.home.dir are not set`

---

## Quick Fix (5 minutes)

### Step 1: Download winutils.exe

```powershell
# Create Hadoop directory
mkdir C:\hadoop\bin

# Download winutils.exe for your Hadoop version
# PySpark 3.4+ uses Hadoop 3.3.x
```

**Download link:**
- Go to: https://github.com/cdarlint/winutils
- Navigate to: `hadoop-3.3.1/bin/`
- Download: `winutils.exe`
- Save to: `C:\hadoop\bin\winutils.exe`

### Step 2: Set Environment Variable

**Option A: Temporary (current session only)**

In your notebook, add this cell BEFORE initializing SparkEngine:

```python
import os
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] = os.environ['HADOOP_HOME'] + r'\bin;' + os.environ['PATH']
print("✅ HADOOP_HOME set to C:\\hadoop")
```

**Option B: Permanent (system-wide)**

1. Open System Environment Variables:
   - Press `Win + R`
   - Type `sysdm.cpl`
   - Click "Environment Variables"

2. Add new System Variable:
   - Variable name: `HADOOP_HOME`
   - Variable value: `C:\hadoop`

3. Update PATH:
   - Find "Path" in System Variables
   - Click "Edit"
   - Add: `C:\hadoop\bin`

4. Restart your notebook kernel

### Step 3: Test

```python
import os
print(f"HADOOP_HOME: {os.environ.get('HADOOP_HOME')}")

from odibi.engine.spark_engine import SparkEngine
spark_engine = SparkEngine()
print(f"✅ Spark version: {spark_engine.spark.version}")
```

---

## Alternative: Use Databricks Community Edition

**Easier option:** Test Spark + Delta in Databricks (cloud, free):

1. Sign up: https://databricks.com/try-databricks
2. Create notebook
3. Spark + Delta already configured!
4. Use the same ODIBI code

---

## Troubleshooting

### Error: Access Denied

If you get permission errors:

```powershell
# Run as Administrator
icacls C:\hadoop\bin\winutils.exe /grant Everyone:F
```

### Error: Java Not Found

PySpark needs Java 11 or 17:

```powershell
# Check Java version
java -version

# If not installed, download:
# https://adoptium.net/
```

### Error: Still Not Working

Use the temporary fix in your notebook:

```python
# Add at the TOP of your notebook (before any Spark imports)
import os
import sys

# Set Hadoop home
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] = os.environ['HADOOP_HOME'] + r'\bin;' + os.environ['PATH']

# Suppress Hadoop warnings
import warnings
warnings.filterwarnings('ignore')

print("✅ Environment configured for Windows + Spark")
```

---

## Recommended Approach for Testing

**For Phase 2B walkthrough:**

1. **Parts 1-10:** Test locally with PandasEngine ✅
   - No Spark needed
   - All Delta features work
   - Faster and simpler

2. **Part 11 (Spark):** Two options:
   - **Skip it** (Spark testing is optional)
   - **Use Databricks** (easier than local Windows setup)

**For production:**
- Use Databricks or cloud Spark clusters
- Don't need local Spark setup

---

## Summary

**Quick decision tree:**

```
Do you need to test Spark locally?
│
├─ No → Skip Part 11, you're done! ✅
│
└─ Yes → Do you want to debug Windows setup?
    │
    ├─ No → Use Databricks Community Edition (free, easier)
    │
    └─ Yes → Follow steps above to install winutils.exe
```

**Recommendation:** Skip local Spark for now. The walkthrough's main value is Delta Lake with Pandas (Parts 1-10), which you've already completed successfully!
