# Performance Optimization Guide

This guide explains the "First Principles" performance optimizations introduced in Odibi v2.2 and how to leverage them.

## Overview

Odibi v2.2 introduces a high-performance core that optimizes three key layers:
1.  **I/O & Memory** (Pandas Engine) via Apache Arrow.
2.  **Execution Planning** (Spark Engine) via Adaptive Query Execution.
3.  **Orchestration Overhead** (Control Plane) via C-Extension Compilation.

---

## 1. Arrow-Native Pandas Engine

By default, Pandas uses NumPy arrays which are memory-inefficient for strings and nullable data. We now support **Apache Arrow** backends for zero-copy I/O and massive memory reduction (~50%).

### How to Enable
Arrow optimizations are **enabled by default** in Odibi v2.2+. You can check or disable them in your `odibi.yaml`:

```yaml
project: my_project
performance:
  use_arrow: true  # Default: true. Set to false to use legacy NumPy backend.
```

### Benefits
- **Faster Reads:** Parquet and CSV reading is 2-5x faster.
- **Lower Memory:** String columns use significantly less RAM.
- **Delta Lake:** Native Arrow-to-Pandas conversion avoids expensive serialization.

---

## 2. Spark Engine Tuning

The Spark engine has been updated to enforce best practices by default. No configuration is required—these are active automatically for all Spark pipelines.

### Optimizations Applied
- **Adaptive Query Execution (AQE):** Enabled (`spark.sql.adaptive.enabled=true`) to handle skew and optimize shuffle partitions dynamically.
- **Arrow-PySpark Bridge:** Enabled (`spark.sql.execution.arrow.pyspark.enabled=true`) to speed up data transfer between the JVM and Python UDFs/Drivers by 10-100x.

---

## 3. Core Compilation (mypyc)

Odibi's orchestration logic (`graph`, `pipeline`, `config`) is designed to be compiled into C extensions using `mypyc`. This makes the CLI startup faster and the validation loops robust against runtime type errors.

### How it Works (Opt-In)
Odibi supports a dual-mode installation:
1.  **Pure Python (Default):** Safe, works everywhere, no compiler needed.
2.  **Compiled Binary (Fast):** Requires C compiler and `mypy`.

### How to Compile Locally (Windows)
1.  Install **Visual Studio Build Tools** (Workload: "Desktop development with C++").
2.  Install `mypy`:
    ```bash
    pip install mypy
    ```
3.  Re-install Odibi from source:
    ```bash
    pip install --no-binary :all: .
    ```
    *The `--no-binary` flag ensures pip runs the local build process instead of using a cached wheel.*

### How to Compile on Databricks (Linux)
Databricks clusters typically have `gcc` installed. You can force compilation in your init script:

```bash
# init.sh
pip install mypy
pip install git+https://github.com/henryodibi11/Odibi.git
```

Because `mypy` is present, the installer will automatically detect it and compile the core modules into C extensions.

### Verification
To check if you are running the compiled version:
1.  Open Python.
2.  Import a core module:
    ```python
    import odibi.graph
    print(odibi.graph.__file__)
    ```
3.  **Pure Python:** Output ends in `.py`
4.  **Compiled:** Output ends in `.pyd` (Windows) or `.so` (Linux).
