# Odibi MCP Installation Guide

## Overview

The Odibi MCP gateway provides AI assistants with access to the Odibi data engineering framework via the Model Context Protocol (MCP). This gateway has minimal dependencies and leverages the main Odibi package from PyPI.

## Prerequisites

- Python 3.10 or higher
- pip package manager

## Installation

### Option 1: Install from PyPI (Recommended)

Once published to PyPI:

```bash
pip install odibi-mcp
```

This will automatically install:
- `odibi>=3.11.0` (main Odibi framework)
- `mcp>=1.0.0` (Model Context Protocol)
- `databricks-sdk>=0.23.0` (for Databricks Apps context)

### Option 2: Install from Source

```bash
# Clone or navigate to the Odibi repository
cd /path/to/Odibi/odibi_mcp

# Install in development mode
pip install -e .
```

### Option 3: Databricks Apps Environment

For deployment as a Databricks App:

```bash
# Install from workspace path
pip install /Workspace/Users/<username>/Odibi/odibi_mcp
```

Or add to app.yaml:

```yaml
command:
  - pip
  - install
  - /Workspace/Users/<username>/Odibi/odibi_mcp
  - "&&"
  - python
  - -m
  - odibi_mcp.databricks_app
```

## Optional Dependencies

### RAG Features (Vector Search)

For semantic search and knowledge retrieval:

```bash
pip install odibi-mcp[rag]
```

This adds:
- `chromadb` - Vector database
- `sentence-transformers` - Embedding models

### Full Odibi Framework

The MCP gateway depends on core Odibi, but if you need optional Odibi features:

```bash
# Spark support
pip install odibi[spark]

# Thermodynamics transformers (ChemE features)
pip install odibi[thermodynamics]

# Everything
pip install odibi[all]
```

See the main [Odibi pyproject.toml](../pyproject.toml) for all optional dependencies.

## Verify Installation

Test that imports work:

```python
# Test core MCP imports
from odibi_mcp.dispatcher import dispatch_action
from odibi_mcp.tools.workflows import WorkflowEngine

# Test Odibi core imports
from odibi.config import PipelineConfig
from odibi.registry import FunctionRegistry

print("✅ All imports successful")
```

## Running the Server

### Stdio Mode (for MCP clients)

```bash
python -m odibi_mcp                       # or: odibi-mcp
# equivalently: fastmcp run odibi_mcp.mcp_server:mcp
```

### Notebook Mode (Databricks Free Edition — no Apps needed)

```python
import sys; sys.path.insert(0, "/Workspace/Users/<you>/Odibi/odibi_mcp")
from odibi_mcp.bootstrap import init
odibi, odibi_help = init()
odibi_help()                              # discover actions
odibi("onboard")                          # orient
odibi("search_docs", query="simulation")  # find capabilities
```

### HTTP Mode (Databricks Apps — paid workspace)

```bash
python -m odibi_mcp.databricks_app        # or: odibi-mcp-http
```

## Troubleshooting

### ModuleNotFoundError: odibi

**Problem:** `from odibi.config import ...` fails

**Solution:** Install the main odibi package:
```bash
pip install odibi>=3.11.0
```

### ModuleNotFoundError: pint

**Problem:** Import fails with missing `pint` module

**Solution:** This is a core Odibi dependency. Reinstall:
```bash
pip install --force-reinstall odibi>=3.11.0
```

### Import errors for CoolProp, polars, etc.

**Problem:** Optional Odibi dependencies missing

**Solution:** These are only needed if using specific transformers:
```bash
# For thermodynamics transformers
pip install odibi[thermodynamics]

# For Polars engine
pip install odibi[polars]
```

The MCP gateway will work without these unless you specifically use those features.

## Dependency Separation

The MCP gateway is designed with minimal dependencies:

- **MCP Gateway**: `odibi-mcp` → depends on `odibi` (core) + `mcp` + `databricks-sdk`
- **Odibi Core**: `odibi` → depends on pandas, pydantic, pyyaml, etc. (see [pyproject.toml](../pyproject.toml))
- **Optional Features**: Install only what you need via optional dependency groups

This separation ensures:
- ✅ Fast installation for basic MCP usage
- ✅ No heavy dependencies (Spark, CoolProp) unless explicitly needed
- ✅ Works in Databricks Apps environment
- ✅ Compatible with pip, conda, poetry, etc.

## Next Steps

After installation, see [README.md](README.md) for:
- Available MCP tools
- Usage examples
- Configuration options
