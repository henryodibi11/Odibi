# Installation Guide

This guide covers all installation options for odibi, from minimal setups to full-featured installations.

## Quick Install

```bash
# Basic installation (Pandas engine, local files, unit conversion)
pip install odibi

# Install with specific features
pip install odibi[spark]           # Apache Spark support
pip install odibi[thermodynamics]  # Steam tables, refrigerants (CoolProp)
pip install odibi[azure]           # Azure cloud storage
pip install odibi[sql]             # SQL Server support

# Combine multiple extras
pip install odibi[spark,thermodynamics,azure]
```

---

## Installation Extras Reference

Odibi uses Python's optional dependencies ("extras") to keep the base install lightweight while making advanced features easy to add.

### Core Extras

| Extra | Command | What It Adds | When You Need It |
|-------|---------|--------------|------------------|
| `spark` | `pip install odibi[spark]` | PySpark, Delta-Spark | Running on Databricks or Spark clusters |
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
| `dev` | `pip install odibi[dev]` | pytest, ruff, mypy, mkdocs | Contributing to odibi |
| `all` | `pip install odibi[all]` | Everything | Kitchen sink installation |

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
- ✅ All 55+ transformers
- ✅ **Unit conversion** (Pint) - built-in, no extra install needed
- ✅ CLI tools

### Databricks / Spark

For production workloads on Databricks or Spark clusters:

```bash
pip install odibi[spark]
```

!!! note "Databricks Tip"
    On Databricks, PySpark is pre-installed. You may only need:
    ```bash
    pip install odibi
    ```
    The Spark engine will detect the existing SparkSession.

### Manufacturing / Engineering

For energy efficiency, thermodynamic calculations, and process engineering:

```bash
pip install odibi[spark,thermodynamics]
```

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

For a complete enterprise installation:

```bash
pip install odibi[spark,azure,sql,thermodynamics,lineage,telemetry]
```

---

## Verifying Installation

After installation, verify everything works:

```bash
# Check version
odibi --version

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

### PySpark on Windows

PySpark requires Java:

```bash
# Install Java 8 or 11
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

# Upgrade with extras
pip install --upgrade odibi[spark,thermodynamics]
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
