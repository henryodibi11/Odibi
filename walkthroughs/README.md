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
| 03 | [Spark Preview](03_spark_preview_stub.ipynb) | Spark architecture, Azure config | 3 min | ⏳ Phase 2 |
| 04 | [CI/CD and Pre-Commit](04_ci_cd_and_precommit.ipynb) | Code quality automation | 3 min | ✅ Phase 1 |
| 05 | [Build New Pipeline](05_build_new_pipeline.ipynb) | Create custom pipeline from scratch | 5 min | ✅ Phase 1 |
| **06** | **[Transformation Registry](06_transformation_registry_basics.ipynb)** | **Registry discover/register/list** | **30 sec** | **✅ Phase 3** |
| **07** | **[Transformation Decorator](07_transformation_decorator_and_composition.ipynb)** | **@transformation metadata + composition** | **30 sec** | **✅ Phase 3** |
| **08** | **[Explanation System](08_explanation_system_explain_decorator.ipynb)** | **@func.explain and context** | **30 sec** | **✅ Phase 3** |
| **09** | **[Story Generation](09_story_generation_metadata_renderers.ipynb)** | **Story metadata + renderers** | **30 sec** | **✅ Phase 3** |
| **10** | **[Story Themes](10_story_themes_and_custom_renderers.ipynb)** | **Theming + custom renderers** | **30 sec** | **✅ Phase 3** |
| **11** | **[CLI Story Tools](11_cli_story_tools_generate_diff_list.ipynb)** | **generate, list, diff commands** | **30 sec** | **✅ Phase 3** |
| **12** | **[Azure SQL Connector](12_azure_sql_connector_quickstart.ipynb)** | **Offline-safe connector quickstart** | **30 sec** | **✅ Phase 3** |

**Total time:** ~21 minutes for complete series (Phase 1-3)

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

### For Phase 3 Deep Dive (NEW!)
Master ODIBI internals and advanced features:
1. **06 - Transformation Registry** → How operations are discovered
2. **07 - Transformation Decorator** → Build custom operations
3. **08 - Explanation System** → Self-documenting pipelines
4. **09 - Story Generation** → Automatic reporting
5. **10 - Story Themes** → Customize output
6. **11 - CLI Story Tools** → Generate, diff, and list stories
7. **12 - Azure SQL Connector** → Database integration

**Complete this path to understand everything from registry to stories!**

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

**Phase 1 (Complete):**
- ✅ Notebooks 00, 01, 04, 05 fully functional
- ✅ Pandas engine examples working
- ✅ CI/CD infrastructure demonstrated

**Phase 2 (Complete):**
- ✅ Spark engine scaffolding (Notebook 03)
- ✅ Azure ADLS integration
- ✅ Delta Lake support

**Phase 3 (Complete - November 2025):**
- ✅ **Notebooks 06-12 fully functional** (NEW!)
- ✅ Transformation registry and decorator system
- ✅ Explanation system with context passing
- ✅ Story generation with metadata tracking
- ✅ Multiple renderers (HTML, Markdown, JSON)
- ✅ Theme system with 4 built-in themes
- ✅ CLI story tools (generate, list, diff)
- ✅ Azure SQL connector with offline fallback

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
