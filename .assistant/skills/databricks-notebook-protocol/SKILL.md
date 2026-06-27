---
name: databricks-notebook-protocol
description: "Use when running Odibi inside a Databricks notebook — install, set ODIBI_CONFIG, the lib/ import pattern (never %run), running a pipeline against real Spark/Unity Catalog, and Databricks gotchas."
requires: [odibi]
---

# databricks-notebook-protocol Skill

Run Odibi pipelines inside Databricks notebooks. The core rule: **never use `%run`** (it
hangs) — make each notebook self-contained via a `lib/` package, and run with
`engine: spark` against the notebook's real `spark` session.

## When to Load This Skill

- Setting up or running an Odibi pipeline in a Databricks notebook.
- A notebook hangs on import, or can't find `odibi`.
- Reading/writing Unity Catalog tables from an Odibi pipeline.

## Install

```python
# In a notebook cell — install odibi (and Polars only if you actually use it)
%pip install odibi
dbutils.library.restartPython()
```

Or, if running from a repo clone, add the repo root to `sys.path` (see the lib/ pattern).

## The lib/ Pattern (project layout)

```
my_project/
├── lib/
│   ├── __init__.py     # empty — makes lib/ a package
│   ├── config.py       # paths, UC catalog/schema, feature flags
│   └── setup.py        # auto-installs deps, adds repo to sys.path, re-exports API
├── 01_bronze.py        # numbered notebooks, self-contained
├── 02_silver.py
└── README.md
```

`lib/config.py`:
```python
ODIBI_ROOT  = "/Workspace/Repos/you/Odibi"   # repo clone path (if not pip-installed)
UC_CATALOG  = "your_catalog"
UC_SCHEMA   = "your_schema"
DATA_PATH   = "/Volumes/your_catalog/your_schema/data"
```

`lib/setup.py`:
```python
import sys
from lib.config import ODIBI_ROOT
sys.path.insert(0, ODIBI_ROOT)   # skip if odibi is pip-installed
import odibi
from lib.config import *
```

Every notebook starts with:
```python
# COMMAND ----------
import sys, os
PROJECT_ROOT = (
    os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir()
    else dbutils.notebook.entry_point.getDbutils().notebook().getContext()
         .notebookPath().get().rsplit("/", 1)[0]
)
sys.path.insert(0, PROJECT_ROOT)
from lib.setup import *      # odibi + config now available
```

## Set ODIBI_CONFIG and Run a Pipeline

`ODIBI_CONFIG` points to the project YAML. Set it, then run via the CLI or the Python API.

```python
import os
os.environ["ODIBI_CONFIG"] = f"{PROJECT_ROOT}/project.yaml"
```

In the YAML, select Spark and use connections that resolve on Databricks
(MSI auth, dbfs/Volumes/UC paths):
```yaml
project: my_project
engine: spark            # use the cluster's spark session
connections:
  source: { type: local, base_path: "/Volumes/cat/schema/data" }
  gold:   { type: delta, catalog: my_catalog, schema: gold_db }
story:  { connection: gold, path: stories }
system: { connection: gold, path: _system }
```

Run it:
```python
# CLI from a %sh / shell cell:
#   ODIBI_CONFIG=/Workspace/.../project.yaml python -m odibi run project.yaml
# Or Python API, passing the live spark session:
from odibi.engine.spark_engine import SparkEngine
engine = SparkEngine(config={"spark": spark})   # `spark` is the notebook global
```

## Unity Catalog

```python
df = spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.my_table")          # read
df.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.out")  # write
```
For Odibi-managed writes, use a `delta` connection with `catalog:`/`schema:` set to the UC
catalog/schema (see the `add-a-connection` skill).

## Databricks Gotchas

| Gotcha | Fix |
|---|---|
| `%run` hangs / state leaks | Never use it — use the `lib/` + `sys.path.insert` pattern |
| `import odibi` fails | `%pip install odibi` + `dbutils.library.restartPython()`, or add repo to `sys.path` |
| Hardcoded paths drift across notebooks | Put all paths in `lib/config.py`; use `/Volumes/...` or `dbfs:/...` |
| Pipeline can't find config | Set `ODIBI_CONFIG` env var to the absolute `project.yaml` path |
| SQL Server / ADLS auth | Use `auth: { mode: aad_msi }` (passwordless) — see `add-a-connection` |
| Engine surprises | `engine: spark`; remember Spark paths are lightly unit-tested — verify outputs (`engine-parity`) |
| Missing `lib/__init__.py` | Required or `from lib.setup import *` fails |
| Notebooks run out of order | Prefix `01_`, `02_`, ... for execution order |

## Workflow

1. Lay out `lib/` (config + setup); install or path-add odibi.
2. Author `project.yaml` with `engine: spark` and UC/Volumes connections.
3. Set `ODIBI_CONFIG`; `validate_yaml` before running.
4. Run via `python -m odibi run` or the Spark engine; inspect with `story` / `node_sample`.
