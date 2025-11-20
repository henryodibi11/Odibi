# Local Spark Setup Guide (Windows/Linux/Mac)

This guide explains how to run Odibi with the Spark engine locally on your machine.

## 1. Prerequisites

1.  **Java (JDK 8, 11, or 17)**
    *   Verify with `java -version`
    *   Spark 3.3+ supports Java 17.

2.  **Python 3.9+**

## 2. Environment Setup (Recommended)

We recommend creating a virtual environment to keep dependencies clean.

### Create Venv
```bash
python -m venv odibi\.venv
```

### Install Dependencies
```bash
# Windows
odibi\.venv\Scripts\pip install -e ".[spark]" pyarrow duckdb

# Linux/Mac
source odibi/.venv/bin/activate
pip install -e ".[spark]" pyarrow duckdb
```

---

## 3. Windows Specific Setup (Critical)

Spark on Windows requires **Hadoop binaries** (`winutils.exe` and `hadoop.dll`).

### Automated Setup
Run our helper script:
```bash
odibi\.venv\Scripts\python setup/setup_local_spark.py
```
This downloads binaries to `tools/hadoop/bin`.

### Known Issue with Python 3.12 on Windows
Running Spark scripts directly (e.g., `python script.py`) may fail with "Python worker exited unexpectedly" due to incompatibilities between PySpark 3.5, Python 3.12, and Windows process spawning.

**Workaround:** Use the interactive PySpark shell for testing if scripts fail.

```bash
# Start PySpark Shell with environment set
set HADOOP_HOME=%CD%\tools\hadoop
set PATH=%HADOOP_HOME%\bin;%CD%\odibi\.venv\Scripts;%PATH%
set PYSPARK_PYTHON=python
pyspark
```

---

## 4. Playgrounds

We provide playground scripts to verify your setup.

### Pandas Playground (Recommended first step)
Verifies basic installation and DuckDB integration.
```bash
odibi\.venv\Scripts\python examples/playground_pandas.py
```

### Spark Playground
Attempts to run a local Spark job.
```bash
# Use the helper batch file to set up environment
examples\run_local_spark.bat
```
*Note: If this crashes on Windows, use the PySpark shell method above.*
