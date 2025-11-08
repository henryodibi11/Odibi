# ODIBI Documentation

Welcome to the ODIBI documentation! This guide will help you understand and use the framework effectively.

## 📚 Core Guides

### Getting Started
- **[Configuration System Explained](CONFIGURATION_EXPLAINED.md)** ⭐ **START HERE** ⭐
  - Understand how YAML, Pydantic models, and runtime classes work together
  - Complete walkthrough from config to execution
  - Answers common confusion points
  - Decision trees and quick reference

### Setup Guides
- **[Setup Azure Connections](setup_azure.md)**
  - Azure Data Lake Storage Gen2
  - Azure SQL Database
  - Authentication options

- **[Setup Databricks](setup_databricks.md)**
  - Cluster configuration
  - Notebook integration
  - Spark engine setup

## 🎓 Interactive Learning

### Walkthroughs (Jupyter Notebooks)
Located in [`walkthroughs/`](../walkthroughs/)

1. **[00 - Setup & Environment](../walkthroughs/00_setup_environment.ipynb)**
   - Installation and ODIBI mental model
   - First pipeline walkthrough

2. **[01 - Local Pipeline (Pandas)](../walkthroughs/01_local_pipeline_pandas.ipynb)**
   - Complete pipeline with explanations
   - Config vs Runtime concepts
   - SQL-over-Pandas deep dive

3. **[02 - CLI and Testing](../walkthroughs/02_cli_and_testing.ipynb)**
   - Testing patterns
   - CLI preview (Phase 2)

4. **[03 - Spark Preview](../walkthroughs/03_spark_preview_stub.ipynb)**
   - Spark architecture overview
   - Phase 3 roadmap

5. **[04 - CI/CD & Pre-commit](../walkthroughs/04_ci_cd_and_precommit.ipynb)**
   - Code quality automation
   - GitHub Actions setup

6. **[05 - Build New Pipeline](../walkthroughs/05_build_new_pipeline.ipynb)**
   - Build from scratch tutorial
   - Best practices

## 📖 Reference

### Configuration Templates
- **[Complete YAML Template](../examples/template_full.yaml)**
  - All available options documented
  - Usage examples for each method
  - Format reference

### Example Pipelines
- **[Local Pandas Example](../examples/example_local.yaml)**
  - Simple bronze→silver→gold pipeline
  - Perfect for learning

- **[Spark Example](../examples/example_spark.yaml)**
  - Databricks/Spark configuration
  - Azure connections

## 🔧 Development

### Contributing
- **[Contributing Guide](../CONTRIBUTING.md)**
  - Development setup
  - Testing requirements
  - Pull request process

### Project Structure
- **[Phase Roadmap](../PHASES.md)**
  - Current phase: Phase 1 Complete
  - Phase 2-5 planned features

- **[Changelog](../CHANGELOG.md)**
  - Version history
  - Recent changes

## 🎯 Common Tasks

### How do I...?

**Run a pipeline from YAML?**
```python
from odibi.pipeline import Pipeline
manager = Pipeline.from_yaml("config.yaml")
results = manager.run()
```
See: [Configuration Explained](CONFIGURATION_EXPLAINED.md#example-tracing-a-pipeline-from-yaml-to-execution)

**Add a new connection?**
```yaml
connections:
  my_data:
    type: local
    base_path: /path/to/data
```
See: [Configuration Explained - Connections](CONFIGURATION_EXPLAINED.md#layer-1-yaml-files-declarative)

**Transform data with SQL?**
```yaml
nodes:
  - name: clean_data
    transform:
      steps:
        - "SELECT * FROM previous_node WHERE amount > 0"
```
See: [Walkthrough 01](../walkthroughs/01_local_pipeline_pandas.ipynb)

**Debug a failing node?**
```python
# Run single node with mock data
result = pipeline.run_node('node_name', mock_data={'dep': df})
```
See: [Walkthrough 02 - Testing](../walkthroughs/02_cli_and_testing.ipynb)

## 🆘 Troubleshooting

### Configuration Issues
- **"ValidationError: field required"**
  → Check [Configuration Explained](CONFIGURATION_EXPLAINED.md#layer-2-pydantic-models-validation) for required fields

- **"Pipeline 'X' not found"**
  → Verify pipeline names with `manager.list_pipelines()`

- **"Connection 'X' not configured"**
  → Ensure connection is defined in `connections:` section

### Execution Issues
- **SQL references wrong table**
  → SQL table names must match node names (see [Context](CONFIGURATION_EXPLAINED.md#5-context---the-data-bus))

- **Node dependencies circular**
  → Check dependency graph with `pipeline.visualize()`

See walkthroughs for troubleshooting sections with common errors and solutions.

## 📞 Support

- **Issues:** [GitHub Issues](https://github.com/henryodibi11/Odibi/issues)
- **Discussions:** [GitHub Discussions](https://github.com/henryodibi11/Odibi/discussions)
- **Email:** henryodibi@outlook.com

---

**Current Version:** v1.1.0-alpha.2-walkthroughs  
**Status:** Phase 1 Complete, Phase 2 Planned  
**Last Updated:** 2025-11-07
