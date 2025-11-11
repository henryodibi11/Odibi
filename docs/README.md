# ODIBI Documentation

Welcome to the ODIBI documentation! This guide will help you understand and use the framework effectively.

## 📚 Core Guides

### Getting Started
- **[Quick Start Guide](guides/01_QUICK_START.md)** ⭐ **START HERE** ⭐
  - Get up and running in 5 minutes
  - First pipeline example
  - Common patterns

- **[Configuration System Explained](CONFIGURATION_EXPLAINED.md)**
  - Understand how YAML, Pydantic models, and runtime classes work together
  - Complete walkthrough from config to execution
  - Decision trees and quick reference

- **[User Guide](guides/02_USER_GUIDE.md)**
  - Building pipelines
  - Configuration patterns
  - Best practices

### Feature Guides
- **[Delta Lake Guide](DELTA_LAKE_GUIDE.md)** 🆕
  - ACID transactions
  - Time travel
  - VACUUM and maintenance
  - Complete Delta Lake reference

- **[Transformation Guide](guides/05_TRANSFORMATION_GUIDE.md)**
  - Custom Python transforms
  - SQL transformations
  - Context API usage

- **[Supported Formats](SUPPORTED_FORMATS.md)**
  - CSV, Parquet, JSON, Excel, Avro, Delta
  - Format-specific options
  - Cloud storage support

### Setup Guides
- **[Setup Azure Connections](setup_azure.md)**
  - Azure Data Lake Storage Gen2
  - Azure SQL Database
  - Authentication options (Key Vault, Managed Identity)

- **[Setup Databricks](setup_databricks.md)**
  - Cluster configuration
  - Notebook integration
  - Spark engine setup

- **[Local Development](LOCAL_DEVELOPMENT.md)**
  - Local testing setup
  - Development workflow

## 🎓 Interactive Learning

### Current Walkthroughs
Located in [`walkthroughs/`](../walkthroughs/)

1. **[Delta Lake Deep Dive](../walkthroughs/phase2b_delta_lake.ipynb)** 🆕
   - All Delta Lake features
   - Time travel examples
   - Production patterns

2. **[Production Pipeline](../walkthroughs/phase2b_production_pipeline.ipynb)**
   - YAML configuration
   - Key Vault integration
   - Best practices

3. **[ADLS Integration](../walkthroughs/phase2a_adls_test.ipynb)** (if available)
   - Azure storage setup
   - Multi-account patterns

4. **[Performance & Key Vault](../walkthroughs/phase2c_performance_keyvault.ipynb)**
   - Parallel Key Vault fetching
   - Performance optimization

### Archived Walkthroughs
Historical learning materials are available in [`_archive/walkthroughs/`](_archive/walkthroughs/)

## 📖 Reference

### Architecture & Design
- **[Architecture Guide](guides/04_ARCHITECTURE_GUIDE.md)**
  - System design
  - Component overview
  - Data flow patterns

- **[Developer Guide](guides/03_DEVELOPER_GUIDE.md)**
  - Contributing to ODIBI
  - Code structure
  - Testing guidelines

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

### Project Information
- **[Phase Roadmap](../PHASES.md)**
  - Current status: Phase 3 Complete
  - All phases with completion details
  - Future roadmap

- **[Changelog](../CHANGELOG.md)**
  - Version history
  - Recent changes and features

- **[Troubleshooting Guide](guides/06_TROUBLESHOOTING.md)**
  - Common errors and solutions
  - Debugging tips
  - FAQ

### Historical Documents
- **[Archived Materials](_archive/README.md)**
  - Phase planning documents
  - Design decisions
  - Historical walkthroughs
  - Test reports

## 🎯 Common Tasks

### How do I...?

**Run a pipeline from YAML?**
```python
from odibi.pipeline import PipelineManager
manager = PipelineManager.from_yaml("config.yaml")
results = manager.run()
```
See: [Quick Start Guide](guides/01_QUICK_START.md)

**Add a new connection?**
```yaml
connections:
  my_data:
    type: local
    base_path: /path/to/data
```
See: [Configuration Guide](CONFIGURATION_EXPLAINED.md)

**Transform data with SQL?**
```yaml
nodes:
  - name: clean_data
    transform:
      steps:
        - "SELECT * FROM previous_node WHERE amount > 0"
```
See: [User Guide](guides/02_USER_GUIDE.md)

**Use Delta Lake?**
```yaml
write:
  format: delta
  mode: append
  options:
    versionAsOf: 5  # Time travel!
```
See: [Delta Lake Guide](DELTA_LAKE_GUIDE.md)

## 🆘 Troubleshooting

See the complete **[Troubleshooting Guide](guides/06_TROUBLESHOOTING.md)** for:
- Configuration errors
- Execution issues
- Common pitfalls
- Debugging strategies

Quick fixes:
- **"ValidationError: field required"** → Check [Configuration Guide](CONFIGURATION_EXPLAINED.md)
- **"Pipeline 'X' not found"** → Verify pipeline names in YAML
- **"Connection 'X' not configured"** → Add to `connections:` section
- **SQL errors** → Table names must match node names

## 📞 Support

- **Issues:** [GitHub Issues](https://github.com/henryodibi11/Odibi/issues)
- **Email:** henryodibi@outlook.com

---

**Current Version:** v1.3.0-alpha.5-phase3  
**Status:** Phase 3 Complete - Production Ready  
**Last Updated:** November 11, 2025
