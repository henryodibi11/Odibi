# ODIBI Framework

[![CI](https://github.com/henryodibi11/Odibi/workflows/CI/badge.svg)](https://github.com/henryodibi11/Odibi/actions)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

**Explicit over implicit. Stories over magic. Simple over clever.**

ODIBI is a declarative data engineering framework that makes data pipelines transparent, traceable, and teachable. Whether you're learning data engineering or building production systems, ODIBI helps you write pipelines that are easy to understand, debug, and evolve.

## Core Philosophy

- **Everything is a Node**: Every operation follows `read → transform → write`
- **Everything Explicit**: If it appears in the story, it must be in the config
- **Connections Centralized**: Define once, reference everywhere
- **Dependencies Clear**: No magic, no hidden flow
- **Stories Automatic**: Every run is documented
- **Engine Agnostic**: Same config works on Spark or Pandas

## Installation

```bash
# Basic installation (Pandas engine only)
pip install odibi

# With Spark support (for Databricks and large-scale processing)
pip install "odibi[spark]"

# With Azure connectors (ADLS Gen2, Azure SQL)
pip install "odibi[azure]"

# All extras (Spark + Azure + advanced features)
pip install "odibi[all]"

# Development installation (includes testing and linting tools)
git clone https://github.com/henryodibi11/Odibi.git
cd Odibi
pip install -e .[dev]
pre-commit install
```

---

## Quick Start

### Option 1: Local Pandas Pipeline (Simplest)

Perfect for learning and local development.

```python
from odibi.pipeline import Pipeline

# Load and run all pipelines from YAML
manager = Pipeline.from_yaml("examples/example_local.yaml")
results = manager.run()  # Runs all defined pipelines

# Or run specific pipeline
result = manager.run('bronze_to_silver')
print(f"✅ {len(result.completed)} nodes completed")
```

**What it does:**
- Loads CSV/Parquet/Avro from configured locations
- Cleans and validates data (SQL transforms)
- Saves output in desired format
- Generates execution story automatically

See [examples/template_full.yaml](examples/template_full.yaml) for all configuration options.

### Option 2: Spark + Azure Pipeline (Production)

For large-scale data processing on Databricks.

1. **Install with extras:**
   ```bash
   pip install "odibi[spark,azure]"
   ```

2. **Configure Azure connections:**
   - See [docs/setup_azure.md](docs/setup_azure.md) for authentication setup
   - See [docs/setup_databricks.md](docs/setup_databricks.md) for cluster configuration

3. **Run Spark pipeline:**
   ```bash
   python -m odibi.cli run examples/example_spark.yaml
   ```

See [examples/example_spark.yaml](examples/example_spark.yaml) for the full configuration.

---

### 🎓 Interactive Tutorial

**Complete walkthrough:** `examples/getting_started/walkthrough.ipynb`

```bash
cd examples/getting_started
jupyter notebook walkthrough.ipynb
```

**Learn in 30 minutes:**
- ✅ Basic pipelines
- ✅ Transform functions
- ✅ SQL transforms  
- ✅ Multi-source joins
- ✅ Debugging techniques

---

### Your First Pipeline (From Scratch)

**1. Create project.yaml**

```yaml
project: My First Pipeline
engine: pandas

connections:
  local:
    type: local
    base_path: ./data
```

**2. Create pipelines/simple.yaml**

```yaml
pipeline: simple_etl

nodes:
  - name: load_data
    read:
      connection: local
      path: input.csv
      format: csv
    cache: true

  - name: clean_data
    depends_on: [load_data]
    transform:
      steps:
        - "SELECT * FROM load_data WHERE amount > 0"

  - name: save_result
    depends_on: [clean_data]
    write:
      connection: local
      path: output.parquet
      format: parquet
      mode: overwrite
```

**3. Run it**

```bash
odibi run project.yaml
```

That's it! ODIBI will:
- Load your CSV
- Apply transformations
- Save to Parquet
- Generate a complete story of what happened

## Features

### Unified Context API
Transform functions work on both Spark and Pandas without changes:

```python
from odibi import transform

@transform
def enrich_data(context, reference_table: str, threshold: float = 0.5):
    """Enrich data with reference information."""
    ref_data = context.get(reference_table)
    # Your logic here - works on both engines!
    return enriched_df
```

### Smart Dependency Management
Nodes execute in the right order, with automatic parallelization:

```yaml
nodes:
  - name: ref_1
    read: {...}

  - name: ref_2
    read: {...}

  - name: combine
    depends_on: [ref_1, ref_2]  # Waits for both
    transform: {...}
```

### Rich Error Messages
When things fail, you know exactly where and why:

```
✗ Node execution failed: clean_data
  Location: pipelines/simple.yaml:15
  Step: 1 of 1

  Error: Column 'amount' not found
  Available columns: ['timestamp', 'value', 'status']

  Suggestions:
    1. Check input data schema: odibi run-node load_data --show-output
    2. Verify column name in SQL query
```

### Automatic Documentation
Every run generates a story showing:
- What was executed and when
- Input/output samples
- Schema changes
- Row counts
- Duration
- Success/failures

## Development Workflow

```bash
# Validate config without running
odibi validate project.yaml

# Run single node with test data
odibi run-node clean_data \
  --mock load_data=test_data.csv \
  --show-output

# View dependency graph
odibi graph --pipeline simple_etl

# Debug mode
odibi run project.yaml --log-level DEBUG
```

## Architecture

```
User Layer (YAML configs + Python transforms)
            ↓
Orchestration Layer (dependency graph, executor)
            ↓
Node Layer (unified read/transform/write)
            ↓
Engine Layer (Spark/Pandas adapters)
            ↓
Connection Layer (Azure/Local/Delta/SQL)
```

## Documentation

**📚 Learning Resources:**
- [Configuration System Explained](docs/CONFIGURATION_EXPLAINED.md) - ⭐ **NEW: Complete config guide** ⭐
- [Getting Started Tutorial](examples/getting_started/walkthrough.ipynb) - Interactive walkthrough
- [Documentation Index](docs/README.md) - All guides and references
- [Quick Reference](examples/getting_started/QUICK_REFERENCE.md) - Common patterns
- [Pydantic Guide](docs/PYDANTIC_CHEATSHEET.md) - Config validation

**📖 Setup Guides:**
- [Databricks Setup](docs/setup_databricks.md) - Community + Azure Databricks configuration
- [Azure Integration](docs/setup_azure.md) - ADLS Gen2 + Azure SQL authentication

**🧪 Examples:**
- [Local Pandas Example](examples/example_local.yaml) - Simple Bronze→Silver→Gold pipeline
- [Spark Azure Example](examples/example_spark.yaml) - Multi-source ETL with Azure
- [Getting Started Tutorial](examples/getting_started/) - Complete walkthrough with sample data

**🔧 Project Info:**
- [PHASES.md](PHASES.md) - Roadmap and feature timeline
- [CONTRIBUTING.md](CONTRIBUTING.md) - How to contribute
- [CHANGELOG.md](CHANGELOG.md) - Version history and release notes
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) - Community guidelines

---

## Roadmap

ODIBI is being developed in deliberate phases:

- ✅ **Phase 1 (Current):** Pandas MVP, Spark scaffolding, Azure connections, CI/CD
- ⏳ **Phase 2 (Q1 2026):** CLI tools, testing utilities, developer experience
- ⏳ **Phase 3 (Q2 2026):** Story generation, Spark engine implementation, AWS/GCP connectors
- ⏳ **Phase 4 (Q3 2026):** Performance optimization, production hardening
- ⏳ **Phase 5 (2026+):** Community ecosystem, plugin system

See [PHASES.md](PHASES.md) for detailed roadmap.

---

## Contributing

We welcome contributions! ODIBI is designed to be community-driven from the start.

**Before contributing:**
1. Read [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines
2. Check [PHASES.md](PHASES.md) for available work
3. Review [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)

**Quick start for contributors:**
```bash
git clone https://github.com/henryodibi11/Odibi.git
cd Odibi
pip install -e .[dev]
pre-commit install
pytest -v  # All 89 tests should pass
```

---

## Contact & Support

- **GitHub Issues:** https://github.com/henryodibi11/Odibi/issues
- **Email:** henryodibi@outlook.com
- **LinkedIn:** [Henry Odibi](https://www.linkedin.com/in/henry-odibi)

---

## License

MIT
