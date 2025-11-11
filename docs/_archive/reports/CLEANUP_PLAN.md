# ODIBI Cleanup Plan

**Goal:** Declutter the repository to make it easy for new users to get started without being overwhelmed.

---

## 📊 Current State

**Root directory:** 50+ files (20+ are PHASE_*.md planning documents)  
**Docs folder:** 20+ documents with overlap  
**Walkthroughs:** 24 files (12+ notebooks)  
**Examples:** 8 files (some redundant with new templates)

**Problem:** Too much clutter makes it hard to find what matters.

---

## 🎯 Target State

**Root directory:** Core OSS files only (~12 files)  
**Docs folder:** 6-8 essential guides + archive folder  
**Walkthroughs:** 2-3 curated notebooks + archive  
**Examples:** getting_started/ + 3 templates

---

## 📋 Cleanup Actions

### A) ROOT DIRECTORY

#### ✅ KEEP (Essential)
- README.md
- PHASES.md (roadmap)
- CHANGELOG.md
- CONTRIBUTING.md
- CODE_OF_CONDUCT.md
- SECURITY.md
- LICENSE
- CODEOWNERS
- pyproject.toml
- pytest.ini
- .pre-commit-config.yaml
- .gitignore

#### 📦 ARCHIVE → docs/_archive/planning/
- IMPLEMENTATION_ROADMAP.md
- HANDOFF.md
- PHASE3_ROADMAP.md
- PHASE3_HANDOFF.md
- PHASE3_COMPLETION_SUMMARY.md
- PHASE2B_COMPLETION_SUMMARY.md
- PHASE2C_COMPLETION_SUMMARY.md
- PHASE2.5_COMPLETION_SUMMARY.md
- PHASE_3_COMPLETE_PLAN.md
- PHASE_3B_PLAN.md
- PHASE_3A_PLAN.md
- PHASE_2.5_PLAN.md
- PHASE_2.5_DAY1_SUMMARY.md
- PHASE_2.5_DAY2_SUMMARY.md
- PHASE_2.5_DAY3_SUMMARY.md
- PHASE_2.5_DAYS4-5_SUMMARY.md
- PHASE_2.5_DAYS6-7_FINAL_SUMMARY.md
- PHASE_2.5_DAYS8-9_SUMMARY.md

#### 🗑️ DELETE (Ephemeral/Redundant)
- NEXT_SESSION_PROMPT.md (ephemeral)
- PHASE_2.5_DAYS6-7_SUMMARY.md (duplicate of FINAL)
- dependencies_current.txt (scratch file)
- structure_current.txt (scratch file)
- test_baseline.txt (scratch file)

#### 🔀 CONSOLIDATE (Merge then delete)
- STATUS.md → merge into PHASES.md
- PROJECT_STRUCTURE.md → merge into docs/guides/03_DEVELOPER_GUIDE.md

---

### B) DOCS FOLDER

#### ✅ KEEP (Essential)
- README.md (docs index)
- CONFIGURATION_EXPLAINED.md ⭐
- DELTA_LAKE_GUIDE.md ⭐
- PYDANTIC_CHEATSHEET.md
- setup_azure.md
- setup_databricks.md
- LOCAL_DEVELOPMENT.md
- SUPPORTED_FORMATS.md
- guides/01_QUICK_START.md ⭐
- guides/02_USER_GUIDE.md
- guides/03_DEVELOPER_GUIDE.md
- guides/04_ARCHITECTURE_GUIDE.md
- guides/05_TRANSFORMATION_GUIDE.md
- guides/06_TROUBLESHOOTING.md
- guides/README.md

#### 📦 ARCHIVE → docs/_archive/
- PHASE2_ROADMAP.md → docs/_archive/planning/
- PHASE2_DESIGN_DECISIONS.md → docs/_archive/design/
- SESSION_SUMMARY_PHASE2_PLANNING.md → docs/_archive/planning/
- WALKTHROUGH_VERIFICATION_REPORT.md → docs/_archive/reports/
- TEST_RESULTS.md → docs/_archive/reports/
- ODIBI_FRAMEWORK_PLAN.md → docs/_archive/planning/
- IMPROVEMENTS.md → docs/_archive/planning/

#### 🔀 CONSOLIDATE
- context.md → merge into guides/04_ARCHITECTURE_GUIDE.md
- interaction_guide.md → merge into guides/03_DEVELOPER_GUIDE.md
- SPARK_WINDOWS_SETUP.md → merge into setup_databricks.md

---

### C) WALKTHROUGHS FOLDER

#### ✅ KEEP (Curated)
- phase2b_delta_lake.ipynb ⭐
- phase2b_production_pipeline.ipynb ⭐
- phase2a_adls_test.ipynb

#### 📦 ARCHIVE → docs/_archive/walkthroughs/
- 00_setup_environment.ipynb
- 01_local_pipeline_pandas.ipynb
- 02_cli_and_testing.ipynb
- 03_spark_preview_stub.ipynb
- 04_ci_cd_and_precommit.ipynb
- 05_build_new_pipeline.ipynb
- 06_transformation_registry_basics.ipynb
- 07_transformation_decorator_and_composition.ipynb
- 08_explanation_system_explain_decorator.ipynb
- 09_story_generation_metadata_renderers.ipynb
- 10_story_themes_and_custom_renderers.ipynb
- 11_cli_story_tools_generate_diff_list.ipynb
- 12_azure_sql_connector_quickstart.ipynb
- databricks_complete_test.ipynb
- databricks_multiaccount_test.ipynb
- MASTER_PHASE3_WALKTHROUGH.md
- README_PHASE2B.md
- config_production.yaml

#### 🗑️ DELETE or ARCHIVE
- README.md → replace with pointer in main README
- PHASE3_LEARNING_PATH.md → archive

---

### D) EXAMPLES FOLDER

#### ✅ KEEP (Essential)
- getting_started/ (entire folder) ⭐
- template_full.yaml ⭐
- template_full_adls.yaml ⭐
- example_local.yaml ⭐
- example_delta_pipeline.yaml
- example_spark.yaml
- README.md

#### 🗑️ DELETE
- MASTER_TEMPLATE.yaml (redundant with template_full.yaml)
- QUICK_START_GUIDE.md (merge into docs/guides/01_QUICK_START.md)
- TEMPLATE_UPDATES_SUMMARY.md (archive)

---

## 📂 New Archive Structure

```
docs/
├── _archive/
│   ├── planning/          # All PHASE_* plans and summaries
│   ├── design/            # Design decisions and frameworks
│   ├── reports/           # Test results and verification reports
│   ├── walkthroughs/      # Historical learning notebooks
│   └── README.md          # Index of archived content
```

---

## 🔗 Link Updates Required

After cleanup, update these files:
1. **README.md** - remove links to archived/deleted files
2. **docs/README.md** - update to only reference kept docs
3. **PHASES.md** - incorporate STATUS.md content
4. **docs/guides/03_DEVELOPER_GUIDE.md** - incorporate PROJECT_STRUCTURE.md
5. **docs/guides/01_QUICK_START.md** - merge examples/QUICK_START_GUIDE.md

---

## 📊 Before & After

| Category | Before | After | Reduction |
|----------|--------|-------|-----------|
| Root .md files | 27 | 8 | -70% |
| Docs files | 21 | 13 | -38% |
| Walkthroughs | 24 | 4 | -83% |
| Examples | 8 | 7 | -13% |
| **TOTAL** | **80** | **32** | **-60%** |

---

## ⚡ Implementation Steps

1. ✅ Create archive folder structure
2. ✅ Move files to archive
3. ✅ Delete ephemeral files
4. ✅ Merge consolidation targets
5. ✅ Update all links
6. ✅ Run link checker
7. ✅ Update README with simplified structure
8. ✅ Test that key workflows still work

---

## 🎯 Success Criteria

- New user can find getting started guide in < 30 seconds
- Root directory has < 15 files
- Docs folder has clear, non-overlapping guides
- Historical content is preserved but hidden
- All links in README work

---

**Ready to execute?** This will make ODIBI much easier to navigate!
