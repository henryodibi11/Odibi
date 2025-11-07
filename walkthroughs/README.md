# ODIBI Walkthroughs

**Modular, self-contained learning notebooks for ODIBI framework.**

Each notebook is independently runnable from a clean repo clone and includes automated self-checks.

---

## 📚 Walkthrough Series

| # | Notebook | Topic | Time | Status |
|---|----------|-------|------|--------|
| 00 | [Setup Environment](00_setup_environment.ipynb) | Installation, imports, sanity checks | 2 min | ✅ Phase 1 |
| 01 | [Local Pipeline Pandas](01_local_pipeline_pandas.ipynb) | Bronze→Silver→Gold pipeline | 2 min | ✅ Phase 1 |
| 02 | [CLI and Testing](02_cli_and_testing.ipynb) | CLI preview, testing patterns | 2 min | ⏳ Phase 2 |
| 03 | [Spark Preview](03_spark_preview_stub.ipynb) | Spark architecture, Azure config | 3 min | ⏳ Phase 3 |
| 04 | [CI/CD and Pre-Commit](04_ci_cd_and_precommit.ipynb) | Code quality automation | 3 min | ✅ Phase 1 |
| 05 | [Build New Pipeline](05_build_new_pipeline.ipynb) | Create custom pipeline from scratch | 5 min | ✅ Phase 1 |

**Total time:** ~17 minutes for complete series

---

## 🚀 Quick Start

```bash
# Clone the repository
git clone https://github.com/henryodibi11/Odibi.git
cd Odibi

# Start with walkthrough 00
jupyter notebook walkthroughs/00_setup_environment.ipynb
```

Each notebook will:
1. Install dependencies automatically
2. Generate any needed test data
3. Run independently (no external setup needed)
4. Verify itself with automated checks

---

## 📖 Learning Path

### For Beginners
Start here to learn ODIBI basics:
1. **00 - Setup Environment** → Verify installation
2. **01 - Local Pipeline Pandas** → Run your first pipeline
3. **05 - Build New Pipeline** → Create your own

### For Contributors
Learn the development workflow:
1. **00 - Setup Environment** → Development setup
2. **02 - CLI and Testing** → Testing patterns
3. **04 - CI/CD and Pre-Commit** → Quality automation

### For Production Users
Understand enterprise features:
1. **03 - Spark Preview** → Spark architecture
2. **01 - Local Pipeline Pandas** → Pandas patterns
3. **05 - Build New Pipeline** → Design patterns

---

## ✅ Self-Check Feature

Every notebook includes an automated verification cell:

```python
# ✅ Self-Check
try:
    # Verify notebook executed correctly
    assert os.path.exists("expected_file.csv")
    print("🎉 Walkthrough verified successfully")
except Exception as e:
    print(f"❌ Walkthrough failed: {e}")
    raise
```

This ensures:
- Notebooks execute correctly before commit
- Users can verify they followed along correctly
- Maintainers can trust all examples work

---

## 🎯 Design Principles

Each notebook follows these rules:

1. **Self-contained:** No external dependencies (generates test data inline)
2. **< 30 seconds runtime:** Fast feedback for learners
3. **Clear structure:** Uses consistent headings (🧭 Goal, 🔧 Setup, ▶️ Run, etc.)
4. **Reflection section:** Explains concepts and links to next steps
5. **Automated verification:** Self-check cell at the end

---

## 🔄 Updating Walkthroughs

When updating notebooks:

1. Test end-to-end execution
2. Ensure self-check passes
3. Update this README if structure changes
4. Run pre-commit hooks before committing

```bash
# Test a notebook
jupyter nbconvert --to notebook --execute walkthroughs/00_setup_environment.ipynb

# Or run all walkthroughs
for nb in walkthroughs/*.ipynb; do
    jupyter nbconvert --to notebook --execute "$nb"
done
```

---

## 📦 Phase Status

**Phase 1 (Current):**
- ✅ Notebooks 00, 01, 04, 05 fully functional
- ✅ Pandas engine examples working
- ✅ CI/CD infrastructure demonstrated

**Phase 2 (Q1 2026):**
- ⏳ Notebook 02 will show real CLI commands
- ⏳ Testing utilities examples

**Phase 3 (Q2 2026):**
- ⏳ Notebook 03 will execute Spark pipelines
- ⏳ Azure ADLS read/write examples

---

## 🤝 Contributing

Found an issue or want to improve a walkthrough?

1. Read [CONTRIBUTING.md](../CONTRIBUTING.md)
2. Open an issue: https://github.com/henryodibi11/Odibi/issues
3. Submit a PR with your improvements

**Guidelines:**
- Keep notebooks under 5 minutes runtime
- Include self-check verification
- Follow existing structure and style
- Test thoroughly before submitting

---

## 📚 Additional Resources

- **Main Tutorial:** [examples/getting_started/walkthrough.ipynb](../examples/getting_started/walkthrough.ipynb)
- **Quick Reference:** [examples/getting_started/QUICK_REFERENCE.md](../examples/getting_started/QUICK_REFERENCE.md)
- **Setup Guides:** [docs/setup_databricks.md](../docs/setup_databricks.md), [docs/setup_azure.md](../docs/setup_azure.md)
- **Examples:** [examples/example_local.yaml](../examples/example_local.yaml), [examples/example_spark.yaml](../examples/example_spark.yaml)

---

**Questions?** Open an issue: https://github.com/henryodibi11/Odibi/issues
