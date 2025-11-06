# ODIBI Examples

**Learn by doing!** Complete examples and tutorials for ODIBI framework.

---

## 📚 Available Examples

### 1. Getting Started (👈 **START HERE**)
**Path:** `getting_started/`  
**Time:** 30 minutes  
**Level:** Beginner

**Learn:**
- Basic pipelines (read/write)
- Transform functions
- SQL transforms
- Multi-source pipelines
- Debugging techniques

**Start:** Open `getting_started/walkthrough.ipynb`

---

## 🗺️ Learning Path

```
1. getting_started/       ← Start here!
   └── Complete walkthrough with sample data

2. (Future) real_world/
   └── Production-ready examples

3. (Future) advanced/
   └── Complex patterns and optimizations
```

---

## 🚀 Quick Start

```bash
# 1. Install ODIBI
cd d:/odibi
pip install -e .

# 2. Go to getting started
cd examples/getting_started

# 3. Open the tutorial
jupyter notebook walkthrough.ipynb

# 4. Run all cells!
```

---

## 📁 Example Structure

Each example includes:
- ✅ **Walkthrough notebook** - Interactive tutorial
- ✅ **Sample data** - Ready to use
- ✅ **YAML configs** - Pipeline definitions
- ✅ **Transform functions** - Python code examples
- ✅ **README** - Quick reference

---

## 🎯 What You'll Build

### Getting Started Example:

**Pipeline 1: Simple**
```
Load CSV → Save Parquet
```

**Pipeline 2: Transform**
```
Load → Calculate Revenue → Filter → Save
```

**Pipeline 3: Advanced**
```
Load Sales + Customers → Join → Aggregate → Save 2 outputs
```

---

## 💡 Tips

### For Learning:
1. **Start with getting_started** - Don't skip it!
2. **Run cells one by one** - Understand each step
3. **Modify the examples** - Break things and fix them
4. **Read the generated files** - See what ODIBI creates

### For Building:
1. **Copy an example** - Use as a template
2. **Replace the data** - Use your own CSV/Parquet files
3. **Write transforms** - Add your business logic
4. **Test incrementally** - Use `run_node()` for debugging

---

## 🔗 Additional Resources

**Documentation:**
- [Framework Plan](../docs/ODIBI_FRAMEWORK_PLAN.md) - Complete design
- [Pydantic Guide](../docs/PYDANTIC_CHEATSHEET.md) - Config validation
- [Improvements](../docs/IMPROVEMENTS.md) - Known issues & enhancements

**Test Examples:**
- [Phase 1 Tests](../test_exploration.ipynb) - Config, Context, Registry
- [Phase 2 Tests](../test_exploration_phase2.ipynb) - Graph, Pipeline, Execution

**Source Code:**
- Start with `context.py` (simplest)
- Then `registry.py`, `config.py`
- Finally `node.py`, `graph.py`, `pipeline.py`

---

## 🤝 Contributing Examples

Have a great example? Add it!

**Structure:**
```
examples/
└── your_example/
    ├── README.md
    ├── walkthrough.ipynb
    ├── project.yaml
    ├── pipelines/
    ├── data/
    └── transforms.py
```

**Guidelines:**
- Include sample data (small files)
- Explain each step
- Show common patterns
- Document gotchas

---

## ❓ Need Help?

**Common Issues:**

1. **Import errors** → Install: `pip install -e d:/odibi`
2. **DuckDB missing** → Install: `pip install duckdb`
3. **File not found** → Check you're in the right directory
4. **Transform not registered** → Import your transforms module

**Getting Help:**
- Check example READMEs
- Review the walkthrough notebook
- Read error messages carefully (they're helpful!)
- Look at test files for patterns

---

**Ready to learn?** Open `getting_started/walkthrough.ipynb`! 🚀
