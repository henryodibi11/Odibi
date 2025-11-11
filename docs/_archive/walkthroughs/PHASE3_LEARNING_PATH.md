# Phase 3 Learning Path - Complete Walkthrough Guide

**Created:** November 2025  
**Status:** ✅ Complete  
**Duration:** ~3.5 minutes for all 7 notebooks

---

## 🎯 Overview

This learning path teaches you **everything about ODIBI's Phase 3 features** through 7 hands-on notebooks:

- ✅ Transformation registry system
- ✅ Decorator patterns
- ✅ Explanation system
- ✅ Story generation
- ✅ Themes and renderers
- ✅ CLI tools
- ✅ Azure SQL connector

**Each notebook runs in < 30 seconds** and is completely self-contained.

---

## 📚 The 7-Stage Learning Path

### Stage 1: Transformation Registry (30 sec)
**Notebook:** [06_transformation_registry_basics.ipynb](06_transformation_registry_basics.ipynb)

**What you'll learn:**
- How ODIBI's global registry works
- When and how operations are registered
- How to list all available transformations
- How to create and register custom operations

**Hands-on:**
- Explore built-in operations (pivot, unpivot, join, sql)
- Create `filter_threshold` custom transformation
- Export registry to JSON and CSV

**Artifacts:**
```
.artifacts/06_registry/
├── registry.json
└── transformations.csv
```

---

### Stage 2: Transformation Decorator (30 sec)
**Notebook:** [07_transformation_decorator_and_composition.ipynb](07_transformation_decorator_and_composition.ipynb)

**What you'll learn:**
- How `@transformation` decorator works internally
- Function metadata (name, version, category, tags)
- How to compose transformations into pipelines
- Metadata flow through pipeline steps

**Hands-on:**
- Build 3 custom transformations (`clean_names`, `add_total`, `normalize_values`)
- Compose them: `normalize(add_total(clean_names(df)))`
- Track metadata through the pipeline

**Artifacts:**
```
.artifacts/07_decorator/
├── pipeline_output.parquet
├── transformation_metadata.json
└── pipeline_steps.json
```

---

### Stage 3: Explanation System (30 sec)
**Notebook:** [08_explanation_system_explain_decorator.ipynb](08_explanation_system_explain_decorator.ipynb)

**What you'll learn:**
- `@func.explain` decorator pattern
- Template helpers (purpose_detail_result, with_formula, etc.)
- Context passing via `**kwargs`
- How to make transformations self-documenting

**Hands-on:**
- Add explanations with template helpers
- Pass runtime context (plant, asset, pipeline_name)
- Generate dynamic, context-aware explanations
- Create 4 transformations with different templates

**Artifacts:**
```
.artifacts/08_explanation/
├── explanations.json
├── transformation_with_context.json
└── explanation_examples.md
```

---

### Stage 4: Story Generation (30 sec)
**Notebook:** [09_story_generation_metadata_renderers.ipynb](09_story_generation_metadata_renderers.ipynb)

**What you'll learn:**
- How ODIBI tracks pipeline execution metadata
- PipelineStoryMetadata and NodeExecutionMetadata classes
- What gets tracked (duration, rows, schema, status)
- How to render stories in multiple formats

**Hands-on:**
- Run a 3-node pipeline
- Access execution metadata
- Render stories in HTML, Markdown, and JSON
- Inspect captured metadata

**Artifacts:**
```
.artifacts/09_story/
├── story.html
├── story.md
├── story.json
└── pipeline_metadata.json
```

---

### Stage 5: Story Themes (30 sec)
**Notebook:** [10_story_themes_and_custom_renderers.ipynb](10_story_themes_and_custom_renderers.ipynb)

**What you'll learn:**
- ODIBI's 4 built-in themes (Dark, Light, Corporate, Minimal)
- How to customize themes
- How to create custom renderers
- Theme comparison and selection

**Hands-on:**
- Generate stories with all 4 built-in themes
- Create a custom TerminalTextRenderer
- Compare outputs across themes
- Generate 5 different story formats

**Artifacts:**
```
.artifacts/10_themes/
├── story_dark.html
├── story_light.html
├── story_corporate.html
├── story_minimal.html
├── story_custom.txt
└── theme_comparison.md
```

---

### Stage 6: CLI Story Tools (30 sec)
**Notebook:** [11_cli_story_tools_generate_diff_list.ipynb](11_cli_story_tools_generate_diff_list.ipynb)

**What you'll learn:**
- `odibi story generate` command
- `odibi story list` command
- `odibi story diff` command
- How to compare pipeline runs
- Python fallbacks if CLI unavailable

**Hands-on:**
- Generate stories via CLI (or Python fallback)
- Make config changes
- Run pipeline twice
- List all stories
- Diff two runs to see changes

**Artifacts:**
```
.artifacts/11_cli/
├── runs/run_1/story.json
├── runs/run_2/story.json
├── story_list.txt
├── diff_run_1_run_2.txt
├── config_v1.yaml
└── config_v2.yaml
```

---

### Stage 7: Azure SQL Connector (30 sec)
**Notebook:** [12_azure_sql_connector_quickstart.ipynb](12_azure_sql_connector_quickstart.ipynb)

**What you'll learn:**
- Azure SQL connection patterns
- Two authentication modes (azure_ad, sql_auth)
- Safe credential handling
- SQLite fallback for offline execution
- Database read/write operations

**Hands-on:**
- Create connection configs for both auth modes
- Use SQLite fallback for testing
- Write DataFrame to database
- Read back with queries
- Execute parameterized queries

**Artifacts:**
```
.artifacts/12_azure_sql/
├── test_database.db
├── connection_config_azure_ad.yaml
├── connection_config_sql_auth.yaml
├── query_results.csv
└── row_counts.json
```

---

## 🚀 Quick Start

### Run All Notebooks Sequentially

```bash
cd /d:/odibi/walkthroughs

# Run each notebook
jupyter notebook 06_transformation_registry_basics.ipynb
jupyter notebook 07_transformation_decorator_and_composition.ipynb
jupyter notebook 08_explanation_system_explain_decorator.ipynb
jupyter notebook 09_story_generation_metadata_renderers.ipynb
jupyter notebook 10_story_themes_and_custom_renderers.ipynb
jupyter notebook 11_cli_story_tools_generate_diff_list.ipynb
jupyter notebook 12_azure_sql_connector_quickstart.ipynb
```

### Or Run Individually

Each notebook is **completely self-contained**:
- Creates its own test data
- Manages its own artifacts directory
- Includes automated self-checks
- Can run from a clean repo clone

---

## ✅ Learning Outcomes

After completing all 7 notebooks, you will:

✅ **Understand** how the transformation registry works  
✅ **Build** custom transformations with decorators  
✅ **Create** self-documenting pipelines with explanations  
✅ **Generate** stories in multiple formats  
✅ **Customize** story themes and renderers  
✅ **Use** CLI tools for story management  
✅ **Connect** to Azure SQL databases safely  

**You'll have mastered ODIBI's Phase 3 features!**

---

## 📊 Progress Tracker

Track your progress:

- [ ] 06 - Transformation Registry ✅
- [ ] 07 - Transformation Decorator ✅
- [ ] 08 - Explanation System ✅
- [ ] 09 - Story Generation ✅
- [ ] 10 - Story Themes ✅
- [ ] 11 - CLI Story Tools ✅
- [ ] 12 - Azure SQL Connector ✅

**Completion:** 0/7

---

## 🎓 Graduation Challenge

After completing all notebooks, try this:

**Build a complete analytics pipeline that:**
1. Creates 2 custom transformations with rich explanations
2. Composes them in a 5-node pipeline
3. Uses SQL for at least one transformation
4. Generates stories in all 3 formats (HTML, MD, JSON)
5. Uses a custom theme
6. Writes output to a database
7. Passes all linter checks

**Success criteria:**
- All nodes execute successfully
- Stories are generated automatically
- Explanations are context-aware
- Runtime < 1 minute
- Zero linter issues

---

## 🔑 Key Concepts Summary

### Transformation System
- `@transformation` registers functions at import time
- Global registry stores all operations
- `@func.explain` adds human explanations
- `**context` enables flexible context passing

### Story System
- Metadata tracked automatically during execution
- Multiple renderers (HTML, Markdown, JSON)
- Themes customize appearance
- CLI tools manage story artifacts

### Quality
- Self-checks validate every notebook
- Runtime budget < 30s per notebook
- Offline-safe (no external dependencies)
- Comprehensive error handling

---

## 🛠️ Troubleshooting

**Notebook won't run?**
- Ensure you're in the walkthroughs directory
- Check Python environment has odibi installed
- Try running cells individually

**Artifacts not created?**
- Check `.artifacts/` directory permissions
- Ensure self-check cell ran successfully
- Look for error messages in output

**CLI commands not working?**
- Notebooks have Python fallbacks
- No CLI required - fallbacks are automatic
- Check if `odibi` is in your PATH

---

## 📖 Additional Resources

**Documentation:**
- [Quick Start Guide](../docs/guides/01_QUICK_START.md)
- [Developer Guide](../docs/guides/03_DEVELOPER_GUIDE.md)
- [Transformation Guide](../docs/guides/05_TRANSFORMATION_GUIDE.md)

**Code Examples:**
- [operations/unpivot.py](../odibi/operations/unpivot.py) - Simple transformation
- [transformations/registry.py](../odibi/transformations/registry.py) - Registry implementation
- [story/renderers.py](../odibi/story/renderers.py) - Renderer implementations

**Advanced Topics:**
- Spark engine integration
- Azure ADLS with Key Vault
- Delta Lake support
- Performance optimization

---

## 🤝 Contributing

Found an issue or want to improve a notebook?

1. Read [CONTRIBUTING.md](../CONTRIBUTING.md)
2. Open an issue
3. Submit a PR

**Guidelines:**
- Keep notebooks < 30s runtime
- Include self-check validation
- Follow emoji header style
- Test thoroughly

---

**Ready to start?** Begin with [06_transformation_registry_basics.ipynb](06_transformation_registry_basics.ipynb)!
