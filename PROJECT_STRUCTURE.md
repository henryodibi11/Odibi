# ODIBI Project Structure

**Complete overview of the codebase.**

---

## 📁 Directory Layout

```
d:/odibi/
├── odibi/                          # Core framework code
│   ├── __init__.py                 # Package exports
│   ├── config.py                   # Pydantic config models (348 lines)
│   ├── context.py                  # Unified Context API (191 lines)
│   ├── registry.py                 # Transform function registry (188 lines)
│   ├── exceptions.py               # Custom exceptions (183 lines)
│   ├── node.py                     # Node execution engine (437 lines)
│   ├── graph.py                    # Dependency graph builder (318 lines)
│   ├── pipeline.py                 # Pipeline orchestrator (222 lines)
│   ├── engine/                     # Execution engines
│   │   ├── __init__.py
│   │   ├── base.py                 # Engine interface (140 lines)
│   │   └── pandas_engine.py        # Pandas implementation (237 lines)
│   └── connections/                # Data connections
│       ├── __init__.py
│       ├── base.py                 # Connection interface (25 lines)
│       └── local.py                # Local filesystem (35 lines)
│
├── tests/                          # Test suite (78 tests)
│   ├── __init__.py
│   ├── test_config.py              # Config validation tests (25 tests)
│   ├── test_context.py             # Context API tests (12 tests)
│   ├── test_registry.py            # Registry tests (18 tests)
│   ├── test_graph.py               # Graph tests (13 tests)
│   └── test_pipeline.py            # Pipeline tests (10 tests)
│
├── examples/                       # Learning examples
│   ├── README.md                   # Examples overview
│   └── getting_started/            # 👈 START HERE
│       ├── README.md               # Tutorial guide
│       ├── QUICK_REFERENCE.md      # Cheat sheet
│       ├── walkthrough.ipynb       # Interactive tutorial
│       ├── transforms.py           # Sample transform functions
│       ├── project.yaml            # Project config
│       ├── pipelines/
│       │   ├── simple.yaml         # Basic read/write
│       │   ├── transform.yaml      # With transforms
│       │   └── advanced.yaml       # SQL, joins, aggregation
│       ├── data/
│       │   ├── sales.csv           # Sample sales data
│       │   └── customers.csv       # Sample customer data
│       └── output/                 # Generated outputs
│
├── docs/                           # Documentation
│   ├── ODIBI_FRAMEWORK_PLAN.md     # Complete design (2,278 lines)
│   ├── PYDANTIC_CHEATSHEET.md      # Pydantic guide
│   ├── PROGRESS.md                 # Implementation progress
│   ├── TEST_RESULTS.md             # Phase 1 test results
│   ├── PHASE2_RESULTS.md           # Phase 2 test results
│   └── IMPROVEMENTS.md             # Known issues & roadmap
│
├── test_exploration.ipynb          # Interactive tests (Phase 1)
├── test_exploration_phase2.ipynb   # Interactive tests (Phase 2)
├── pyproject.toml                  # Package configuration
├── pytest.ini                      # Pytest configuration
├── README.md                       # Main README
├── README_PHASE2.md                # Phase 2 summary
└── PROJECT_STRUCTURE.md            # This file
```

---

## 📊 Code Statistics

**Core Framework:**
- Total lines: ~2,800
- Files: 14
- Languages: Python

**Tests:**
- Total tests: 78 (all passing)
- Test files: 5
- Coverage: Excellent (all core components)

**Documentation:**
- Markdown files: 12
- Notebooks: 3
- Total docs: ~8,000 lines

**Examples:**
- Pipelines: 3 (simple, transform, advanced)
- Transform functions: 5
- Sample datasets: 2

---

## 🗺️ Component Map

### Layer 1: User Interface
```
examples/getting_started/
├── pipelines/*.yaml        # User writes these
└── transforms.py           # User writes these
```

### Layer 2: Configuration
```
odibi/config.py             # Validates YAML → Python objects
```

### Layer 3: Orchestration
```
odibi/pipeline.py           # Coordinates execution
odibi/graph.py              # Analyzes dependencies
```

### Layer 4: Execution
```
odibi/node.py               # Executes read/transform/write
odibi/context.py            # Passes data between nodes
odibi/registry.py           # Manages transform functions
```

### Layer 5: Engines
```
odibi/engine/
├── base.py                 # Engine interface
└── pandas_engine.py        # Pandas implementation
```

### Layer 6: Connections
```
odibi/connections/
├── base.py                 # Connection interface
└── local.py                # Local filesystem
```

---

## 🎯 Entry Points

### For Users:
1. **Start:** `examples/getting_started/walkthrough.ipynb`
2. **Reference:** `examples/getting_started/QUICK_REFERENCE.md`
3. **Build:** Create your own `pipelines/*.yaml`

### For Developers:
1. **Understand:** Start with `context.py`, then `registry.py`
2. **Contribute:** Check `docs/IMPROVEMENTS.md`
3. **Test:** Run `pytest tests/ -v`

### For Documentation:
1. **Architecture:** `docs/ODIBI_FRAMEWORK_PLAN.md`
2. **Results:** `docs/PHASE2_RESULTS.md`
3. **Pydantic:** `docs/PYDANTIC_CHEATSHEET.md`

---

## 🔄 Data Flow

```
1. User writes YAML
   └─> pipelines/my_pipeline.yaml

2. Config validation
   └─> PipelineConfig (config.py validates)

3. Pipeline creation
   └─> Pipeline(config, connections, engine)
       ├─> Creates Context (context.py)
       ├─> Creates Engine (pandas_engine.py)
       └─> Builds Graph (graph.py)

4. Execution
   └─> Pipeline.run()
       ├─> Graph.topological_sort() → execution order
       └─> For each node:
           ├─> Node.execute() (node.py)
           ├─> Engine.read/transform/write
           └─> Context.register() → data for next node

5. Results
   └─> PipelineResults
       ├─> completed: [node1, node2, ...]
       ├─> failed: []
       └─> duration: 1.23s
```

---

## 🧪 Testing Strategy

### Unit Tests (`tests/`)
- **test_config.py** - Pydantic validation
- **test_context.py** - Data passing
- **test_registry.py** - Function registration
- **test_graph.py** - Dependency analysis
- **test_pipeline.py** - End-to-end execution

### Interactive Tests
- **test_exploration.ipynb** - Phase 1 components
- **test_exploration_phase2.ipynb** - Phase 2 components

### Examples
- **walkthrough.ipynb** - Real-world usage

---

## 📈 Project Stats

**Development Time:** ~6 hours  
**Tests Written:** 78 (all passing)  
**Test Execution:** 0.35 seconds  
**Code Quality:** Well-tested, documented  
**Status:** MVP Complete ✅

---

## 🚀 Next Steps

### For Production:
1. CLI tools (`odibi run`, `odibi validate`)
2. More formats (Avro, Delta, SQL)
3. Story generator (documentation)
4. Spark engine implementation

### For Enhancement:
1. Fix critical issues (see `IMPROVEMENTS.md`)
2. Add unpivot operation
3. Better error messages
4. Connection factory (YAML → Connection objects)

---

## 📞 Support

**Learning:**
- Start with `examples/getting_started/walkthrough.ipynb`
- Check `QUICK_REFERENCE.md` for patterns
- Read `PYDANTIC_CHEATSHEET.md` for config help

**Contributing:**
- Review `IMPROVEMENTS.md`
- Write tests for new features
- Follow existing code patterns

**Issues:**
- Check test files for examples
- Review error messages (they're helpful!)
- Run `pipeline.validate()` first

---

**Last Updated:** 2025-11-05  
**Version:** 1.0.0-MVP
