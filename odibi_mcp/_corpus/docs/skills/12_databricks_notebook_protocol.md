# Skill 12 — Databricks Notebook Protocol

> **Layer:** Building
> **When:** Creating notebooks, e2e tests, or self-contained projects on Databricks.
> **Rule:** Never use %run. Always use the lib/ pattern.

---

## Purpose

%run hangs and gets stuck on Databricks. This skill encodes the working pattern for all notebook-based projects — the same pattern that shipped validation_e2e (931 files, 99% success) and document_profiler (148 tests, 234 files).

---

## The lib/ Pattern

### Project Structure
```
my_project/
├── lib/
│   ├── __init__.py          # Empty — makes lib/ a package
│   ├── setup.py             # Auto-installs deps, exports public API
│   └── config.py            # Paths, connection strings, project config
├── 01_phase_one.py          # Notebook — self-contained via lib/
├── 02_phase_two.py          # Notebook — self-contained via lib/
├── 03_analyze_results.py    # Notebook — self-contained via lib/
└── README.md                # What this project does
```

### lib/__init__.py
```python
# Empty file — just makes lib/ importable
```

### lib/config.py
```python
"""Project configuration — paths, connections, constants."""

# Paths
ODIBI_ROOT = "<REPLACE_WITH_YOUR_DATABRICKS_CLONE_PATH>"
DATA_PATH = "/Volumes/catalog/schema/data"
OUTPUT_PATH = "/Volumes/catalog/schema/output"

# Unity Catalog
UC_CATALOG = "your_catalog"
UC_SCHEMA = "your_schema"

# Feature flags
INSTALL_POLARS = True
VERBOSE = True
```

### lib/setup.py
```python
"""Project setup — auto-installs deps, exports API."""
import subprocess
import sys

def _install(pkg):
    """Install a package quietly."""
    subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "-q"])

# --- Auto-install dependencies ---
try:
    import polars
except ImportError:
    _install("polars")
    import polars

# --- Import odibi ---
from lib.config import ODIBI_ROOT
sys.path.insert(0, ODIBI_ROOT)
import odibi

# --- Re-export everything notebooks need ---
from lib.config import *

# Public API for notebooks
__all__ = [
    "odibi",
    "polars",
    "ODIBI_ROOT",
    "DATA_PATH",
    "OUTPUT_PATH",
    "UC_CATALOG",
    "UC_SCHEMA",
]
```

### Notebook Template
```python
# Databricks notebook source

# COMMAND ----------
# Setup — every notebook starts with this
import sys, os

PROJECT_ROOT = (
    os.path.dirname(os.path.abspath(__file__))
    if "__file__" in dir()
    else dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
    .rsplit("/", 1)[0]
)
sys.path.insert(0, PROJECT_ROOT)
from lib.setup import *

# COMMAND ----------
# Your notebook code here — odibi and all deps are available
```

---

## Rules

1. **Never use %run** — it hangs. Use lib/ imports.
2. **Each notebook is self-contained** — it imports everything it needs via `from lib.setup import *`
3. **Config lives in lib/config.py** — paths, credentials, feature flags
4. **Dependencies auto-install in lib/setup.py** — Polars, custom packages
5. **Number notebooks** — `01_`, `02_`, etc. for execution order
6. **Include README.md** — what the project does, how to run it

---

## Using Unity Catalog

When testing with Unity Catalog access:

```python
# Read from Unity Catalog table
df = spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.my_table")

# Write to Unity Catalog
df.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.my_output")

# Use odibi with Spark
from odibi.engine.spark_engine import SparkEngine
engine = SparkEngine(config={"spark": spark})
```

---

## Testing on Databricks

### With Real Spark
```python
from odibi.engine.spark_engine import SparkEngine
from odibi.context import SparkContext

engine = SparkEngine(config={"spark": spark})
df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
context = SparkContext(df=df, spark=spark)

# Test a transformer
from odibi.transformers.sql_core import filter_rows, FilterRowsParams
result = filter_rows(context, FilterRowsParams(condition="id > 1"))
result.df.show()  # Verify visually
assert result.df.count() == 1  # Verify programmatically
```

### With Real Polars
```python
import polars as pl
from odibi.engine.polars_engine import PolarsEngine
from odibi.context import PolarsContext

engine = PolarsEngine(config={})
df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
context = PolarsContext(df=df)

# Test
result = filter_rows(context, FilterRowsParams(condition="id > 1"))
assert result.df.shape[0] == 2
```

---

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| Using %run for imports | Use lib/ pattern with sys.path.insert |
| Hardcoding paths in notebooks | Put all paths in lib/config.py |
| Not auto-installing Polars | Add try/except + _install in lib/setup.py |
| Missing __init__.py in lib/ | Required for Python package resolution |
| Not numbering notebooks | Use 01_, 02_ prefix for execution order |
