# Odibi Learning Guides

**Complete documentation to learn and master Odibi.**

---

## 📚 Guide Index

### For New Users

**Start here if you're new to Odibi:**

1. **[Quick Start Guide](01_QUICK_START.md)** ⭐ *Start here!*
   - Get running in 5 minutes
   - Your first pipeline
   - Basic commands

2. **[User Guide](02_USER_GUIDE.md)**
   - Complete feature walkthrough
   - Built-in operations
   - Working with stories
   - Best practices

### For Developers

**Learn how Odibi works internally:**

3. **[Developer Guide](03_DEVELOPER_GUIDE.md)**
   - Understanding internals
   - Code structure
   - How to learn the codebase
   - Extension points

4. **[Architecture Guide](04_ARCHITECTURE_GUIDE.md)**
   - System design
   - Data flow diagrams
   - Module dependencies
   - Design patterns

### For Contributors

**Build on Odibi:**

5. **[Transformation Guide](05_TRANSFORMATION_GUIDE.md)**
   - Write custom operations
   - Add explanations
   - Testing your code
   - Real-world examples

6. **[Troubleshooting Guide](06_TROUBLESHOOTING.md)**
   - Common errors & solutions
   - Debugging techniques
   - Performance tips
   - FAQ

---

## Learning Paths

### Path 1: Quick User (30 minutes)

Just want to use Odibi? Follow this path:

```
1. Quick Start (5 min)
   ↓
2. User Guide - Sections 1-3 (15 min)
   ↓
3. Try an example from examples/ (10 min)
   ↓
✅ You can use Odibi!
```

---

### Path 2: Power User (2 hours)

Want to master all features?

```
1. Quick Start (5 min)
   ↓
2. Full User Guide (45 min)
   ↓
3. Transformation Guide - Quick Example (15 min)
   ↓
4. Try all built-in operations (30 min)
   ↓
5. Experiment with themes and stories (25 min)
   ↓
✅ You're a power user!
```

---

### Path 3: Developer (1 week)

Want to understand and extend Odibi?

```
Day 1: Quick Start + User Guide
       ↓
Day 2: Read Developer Guide
       Run tests: pytest tests/unit/test_operations.py -v
       ↓
Day 3: Read Architecture Guide
       Trace a pipeline execution end-to-end
       ↓
Day 4: Read Transformation Guide
       Create your first custom operation
       ↓
Day 5: Read 5 source files:
       - operations/unpivot.py
       - transformations/registry.py
       - transformations/decorators.py
       - story/metadata.py
       - pipeline.py (skim)
       ↓
Day 6: Write a custom transformation
       Add tests
       Run full test suite
       ↓
Day 7: Read CONTRIBUTING.md
       Pick an issue to work on
       ↓
✅ You can contribute to Odibi!
```

---

### Path 4: Deep Expert (1 month)

Want to know EVERYTHING?

**Week 1:** All guides + examples
**Week 2:** Read all code in `operations/` and `transformations/`
**Week 3:** Read all code in `story/` and `engine/`
**Week 4:** Read all code in `pipeline.py`, `graph.py`, `cli/`

**Goal:** Read and understand all 416+ tests

✅ You ARE Odibi!

---

## Quick Reference

### File Locations

```
Important files to know:

Code:
  odibi/operations/         ← Built-in operations (start here!)
  odibi/transformations/    ← Core transformation system
  odibi/story/              ← Story generation
  odibi/cli/                ← Command-line interface

Tests:
  tests/unit/test_operations.py        ← Learn operations
  tests/unit/test_registry.py          ← Learn registry
  tests/unit/test_story_metadata.py    ← Learn stories

Examples:
  examples/                 ← Working configurations

Documentation:
  docs/guides/              ← These guides!
  README.md                 ← Project overview
```

### Common Commands

```bash
# Running
odibi run config.yaml                    # Execute pipeline
odibi validate config.yaml               # Check config

# Stories
odibi story generate config.yaml        # Create docs
odibi story list                         # List stories
odibi story diff s1.json s2.json        # Compare runs

# Testing (for developers)
pytest                                   # Run all tests
pytest tests/unit/test_operations.py    # Specific tests
pytest -v                                # Verbose
pytest -x                                # Stop on failure
pytest --cov=odibi                       # With coverage
```

### Key Concepts

**Transformation:** A reusable data operation (pivot, unpivot, join, etc.)

**Registry:** Global storage of all transformations

**Node:** A step in your pipeline (read → transform → write)

**Story:** Auto-generated documentation of what happened

**Context:** Data flowing between nodes + metadata (plant, asset, etc.)

**Engine:** Execution backend (Pandas or Spark)

**Connection:** Storage abstraction (local files, Azure, SQL)

---

## Test-Driven Learning

**The tests are comprehensive examples!**

Want to learn how X works?

```bash
# 1. Find the test
grep -r "test.*X" tests/

# 2. Read the test
less tests/unit/test_X.py

# 3. Run the test
pytest tests/unit/test_X.py::specific_test -v -s

# 4. Add print statements
# Modify test to print intermediate values

# 5. Read the source code
# Now you understand the context!
```

**Every test teaches you:**
- ✅ How to import
- ✅ How to call
- ✅ What you get back
- ✅ Edge cases

**With 416 tests, you have 416 examples!**

---

## What You've Built

### Phase 3 Summary

Over the course of Phase 3, you built:

**Phase 3A - Foundation:**
- ✅ Transformation registry system
- ✅ Explanation system with quality enforcement
- ✅ 4 built-in operations (pivot, unpivot, join, sql)
- ✅ Context passing system

**Phase 3B - Stories:**
- ✅ Story metadata tracking
- ✅ HTML/Markdown/JSON renderers
- ✅ Doc story generator
- ✅ Theme system (4 themes)

**Phase 3C - CLI:**
- ✅ `odibi story generate` command
- ✅ `odibi story diff` command
- ✅ `odibi story list` command

**Phase 3D - Azure SQL:**
- ✅ Full Azure SQL connector
- ✅ Read/write operations
- ✅ Two auth modes

**Phase 3E - Documentation:**
- ✅ 6 comprehensive guides
- ✅ This master index

**Total:**
- **416 tests passing**
- **109 tests added in Phase 3**
- **6 learning guides**
- **Production-ready framework**

---

## Congratulations! 🎉

You now have:

✅ A fully-featured data pipeline framework
✅ Comprehensive documentation
✅ 416 tests showing how everything works
✅ Complete learning guides

**You can:**
- Run pipelines
- Create custom operations
- Generate beautiful documentation
- Debug any issue
- Understand the entire codebase

---

## What's Next?

### Use It!

Build real pipelines for your work:
1. Start with examples
2. Adapt to your needs
3. Create custom transformations
4. Generate stakeholder docs

### Extend It!

Ideas for future enhancements:
- Add more operations
- Create custom themes
- Build plugins
- Contribute back!

### Share It!

- Show the HTML stories to stakeholders
- Use doc stories for project documentation  
- Share custom operations with your team

---

**You're ready to build self-documenting, transparent data pipelines!** 🚀

Happy building!
- Henry Odibi
