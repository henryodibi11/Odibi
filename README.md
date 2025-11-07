# ODIBI Framework

**Explicit over implicit. Stories over magic. Simple over clever.**

ODIBI is a declarative data engineering framework that makes data pipelines transparent, traceable, and teachable.

## Core Philosophy

- **Everything is a Node**: Every operation follows `read → transform → write`
- **Everything Explicit**: If it appears in the story, it must be in the config
- **Connections Centralized**: Define once, reference everywhere
- **Dependencies Clear**: No magic, no hidden flow
- **Stories Automatic**: Every run is documented
- **Engine Agnostic**: Same config works on Spark or Pandas

## Quick Start

### Installation

```bash
# Install from source (development)
cd d:/odibi
pip install -e .

# Or install from PyPI (when published)
pip install odibi
```

### 🎓 Learn by Example

**👉 START HERE:** Complete interactive tutorial in `examples/getting_started/walkthrough.ipynb`

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

### Your First Pipeline (Quick Example)

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
- [Getting Started Tutorial](examples/getting_started/walkthrough.ipynb) - 👈 **START HERE!**
- [Quick Reference](examples/getting_started/QUICK_REFERENCE.md) - Common patterns
- [Pydantic Guide](docs/PYDANTIC_CHEATSHEET.md) - Config validation

**📖 Technical Docs:**
- [Framework Plan](docs/ODIBI_FRAMEWORK_PLAN.md) - Complete architecture and design
- [Phase 1 Results](docs/TEST_RESULTS.md) - Foundation tests
- [Phase 2 Results](docs/PHASE2_RESULTS.md) - Orchestration tests
- [Improvements](docs/IMPROVEMENTS.md) - Known issues & roadmap

**🧪 Examples:**
- [Examples Overview](examples/README.md) - All examples
- [Getting Started](examples/getting_started/) - Complete tutorial with sample data

## Project Status

**Version:** 1.0.0 (Phase 1 - MVP)  
**Status:** In Development

### Current Implementation
- ⏳ Core config schemas (Pydantic)
- ⏳ Unified Context API
- ⏳ Node execution engine
- ⏳ Function registry
- ⏳ Dependency graph builder
- ⏳ Pipeline executor

### Roadmap
- v1.0: Core engine, Pandas/Spark, basic CLI
- v1.1: Smart format inference, graph visualization
- v1.2: More connections (AWS S3, GCP, SQL Server)
- v2.0: Advanced features, performance optimizations

## Contributing

This is currently a private framework. Contact the maintainers for contribution guidelines.

## License

MIT
