# Installation Guide

Choose an installation for the environment that will run Odibi. A local Python process, a Databricks Connect client and a managed Databricks runtime need different Spark dependencies; `[all]` is not a universal setup command.

## Quick Install

```bash
# Local Pandas processing (use a project virtual environment)
python -m pip install odibi

# Install with specific features
python -m pip install "odibi[polars]"          # Local Polars processing
python -m pip install "odibi[thermodynamics]"  # Steam tables, refrigerants (CoolProp)
python -m pip install "odibi[azure]"           # Azure cloud storage
python -m pip install "odibi[sql]"             # SQL Server support

# Standalone Spark only: not Databricks Connect or a managed Databricks runtime
python -m pip install "odibi[spark]"
```

For remote Spark from a laptop, use [Databricks Connect](#databricks-connect-from-your-laptop) below. Inside Databricks, [reuse the managed runtime](#inside-databricks).

Base Odibi still installs shared dependencies such as Pandas, NumPy, PyArrow, DuckDB and deltalake. Installing into an existing environment can upgrade or downgrade them. Check its constraints before installation; current bounds include `pyarrow>=14,<17`, `pandas>=2,<3`, `duckdb>=0.9,<1.1` and `deltalake>=0.18,<0.30`. A fresh project environment avoids disturbing unrelated applications; it does not guarantee compatibility with every optional package.

---

## Installation Extras Reference

Odibi uses Python's optional dependencies ("extras") to add capabilities to its base dependency set. Select only those needed by your project.

### Core Extras

| Extra | Command | What It Adds | When You Need It |
|-------|---------|--------------|------------------|
| `spark` | `pip install "odibi[spark]"` | PySpark, Delta-Spark | Standalone Spark where those packages are not already supplied; not Databricks Connect |
| `polars` | `pip install odibi[polars]` | Polars DataFrame library | High-performance local processing |
| `pandas` | `pip install odibi[pandas]` | DuckDB, pandasql, fastavro | Enhanced Pandas SQL support |

### Cloud & Database Extras

| Extra | Command | What It Adds | When You Need It |
|-------|---------|--------------|------------------|
| `azure` | `pip install odibi[azure]` | azure-storage-blob, azure-identity, adlfs | Reading/writing to Azure Data Lake, Blob Storage |
| `sql` | `pip install odibi[sql]` | pyodbc, sqlalchemy | SQL Server connections |

### Specialized Extras

| Extra | Command | What It Adds | When You Need It |
|-------|---------|--------------|------------------|
| `thermodynamics` | `pip install odibi[thermodynamics]` | CoolProp, iapws, psychrolib | Steam tables, refrigerants, humid air calculations |
| `lineage` | `pip install odibi[lineage]` | openlineage-python | Data lineage tracking |
| `telemetry` | `pip install odibi[telemetry]` | OpenTelemetry SDK | Distributed tracing and observability |
| `mcp` | `pip install odibi[mcp]` | MCP server dependencies | AI assistant integration |

### Development Extras

| Extra | Command | What It Adds | When You Need It |
|-------|---------|--------------|------------------|
| `dev` | `pip install "odibi[dev]"` | Test/lint/docs tools plus PySpark and Delta-Spark | Framework development in a separate environment; not alongside Connect |
| `all` | `pip install "odibi[all]"` | Broad runtime bundle, including PySpark and Delta-Spark; not every optional extra | Opt-in standalone environment only; not Connect or managed Databricks |

---

## Common Installation Scenarios

### Local Development (Pandas)

For local development and testing with CSV/Parquet files:

```bash
pip install odibi
```

This gives you:
- ✅ Pandas engine with DuckDB SQL
- ✅ Local file reading (CSV, Parquet, JSON, Excel)
- ✅ Delta Lake support (via deltalake)
- ✅ All 54 transformers
- ✅ **Unit conversion** (Pint) - built-in, no extra install needed
- ✅ CLI tools

### Local Development (Polars)

For local Polars pipelines, including reading downloaded Delta snapshots:

```bash
python -m pip install "odibi[polars]"
```

No Spark session or Databricks connection is needed for local snapshot processing.

### Databricks Connect from Your Laptop

Use a dedicated environment whose Python and Connect versions match your compute. For a compatible 15.4 runtime using Python 3.11, install into that environment:

```bash
python -m pip install "odibi[polars]" "databricks-connect==15.4.*" "deltalake>=0.25,<0.30"
python -m pip check
```

This includes the local writer required by [`write.download`](local_spark_download.md). Select the Connect version for your actual runtime; `15.4.*` is an example, not a default for every workspace. See [Databricks Connect setup](../setup_databricks_connect.md) for environment creation, authentication and compute compatibility.

Do not install Odibi's `[spark]`, `[all]` or `[dev]` extras here. Connect supplies its own `pyspark` modules; ordinary PySpark and Delta-Spark can overwrite or conflict with them. `deltalake` is the separate client-local Delta reader/writer, not `delta-spark`, and belongs in the snapshot environment. Add MCP support with `odibi[mcp,polars]` if this environment will also serve Odibi MCP. Install test/lint tools individually if needed.

### Inside Databricks

Reuse the runtime's Spark installation and existing session:

```python
%pip install odibi
```

Do not add Databricks Connect, ordinary PySpark or Delta-Spark just to use Odibi in a notebook. Avoid `[spark]`, `[all]` and `[dev]` in this managed environment. Add only the non-Spark extras your project needs and check them against the runtime's dependency constraints. Base Odibi also has shared dependencies; it is not guaranteed compatible with every Databricks Runtime. Follow your workspace's library installation policy rather than replacing runtime packages to force a successful install.

### Standalone Spark

For your own Spark environment without Databricks Connect or a managed runtime:

```bash
python -m pip install "odibi[spark]"
```

Match Python, Java, PySpark and Delta-Spark to the Spark version you intend to run. The generic `[spark]` extra is not a runtime-specific lockfile.

### Manufacturing / Engineering

For energy efficiency, thermodynamic calculations, and process engineering:

```bash
python -m pip install "odibi[thermodynamics]"
```

Add `[spark]` only for standalone Spark, or use the appropriate Databricks environment above.

This adds:
- Steam property calculations (IAPWS-IF97)
- Refrigerant properties (R134a, Ammonia, etc.)
- Psychrometric (humid air) calculations
- Support for 122+ fluids via CoolProp

### Azure Data Platform

For Azure-based data lakes and SQL Server:

```bash
pip install odibi[azure,sql]
```

This adds:
- Azure Blob Storage connections
- Azure Data Lake Gen2 (ADLS) support
- Azure Key Vault for secrets
- SQL Server bulk copy and merge operations

### Full Production Stack

For a standalone Spark environment that needs these integrations (not Connect or managed Databricks):

```bash
pip install odibi[spark,azure,sql,thermodynamics,lineage,telemetry]
```

---

## Verifying Installation

After installation, verify everything works:

```bash
# Check version
odibi --version

# Check declared package compatibility (not a Spark/runtime compatibility proof)
python -m pip check

# List available transformers
odibi list transformers

# List available patterns
odibi list patterns

# List available connections
odibi list connections
```

### Verify Specific Extras

```python
# Verify Spark
python -c "from pyspark.sql import SparkSession; print('Spark OK')"

# Verify thermodynamics
python -c "import CoolProp; print(f'CoolProp {CoolProp.__version__} OK')"

# Verify Azure
python -c "import azure.storage.blob; print('Azure OK')"

# Verify Polars
python -c "import polars; print(f'Polars {polars.__version__} OK')"
```

---

## Troubleshooting Installation

### CoolProp Installation Issues

CoolProp requires a C++ compiler on some platforms:

**Windows:**
```bash
# Usually works directly
pip install CoolProp
```

**macOS:**
```bash
# Install Xcode command line tools first
xcode-select --install
pip install CoolProp
```

**Linux:**
```bash
# Install build essentials
sudo apt-get install build-essential python3-dev
pip install CoolProp
```

### Databricks Connect Conflicts

If Connect and ordinary PySpark/Delta-Spark have been installed together, use a fresh dedicated environment following the Connect instructions above. They share Python module paths, so a successful install or `pip check` alone does not establish that Connect's files are intact. Do not repair a managed runtime or change global profiles as a workaround. Verify the actual Connect session and any required local snapshot roundtrip before using work data.

### PySpark on Windows

Standalone PySpark requires Java; Databricks Connect does not need a local JVM:

```bash
# Install the Java version required by your chosen standalone Spark version
# Set JAVA_HOME environment variable
# Then install
pip install odibi[spark]
```

### Polars Version Compatibility

Polars 1.37+ dropped Python 3.9 support. If you're on Python 3.9:

```bash
pip install "polars>=0.20.0,<1.37.0"
```

---

## Upgrading

```bash
# Upgrade odibi
pip install --upgrade odibi

# Upgrade with the extras selected for your environment (local engineering example)
python -m pip install --upgrade "odibi[thermodynamics]"
```

---

## Requirements

- **Python**: 3.9, 3.10, 3.11, or 3.12
- **OS**: Windows, macOS, Linux
- **Memory**: 4GB+ recommended for Pandas, 8GB+ for Spark local mode

---

## Next Steps

- [Quickstart Tutorial](../tutorials/getting_started.md) - Build your first pipeline
- [Thermodynamics Guide](thermodynamics.md) - Engineering calculations
- [The Definitive Guide](the_definitive_guide.md) - Complete framework overview
