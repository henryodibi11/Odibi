# ODIBI Examples & Templates

This directory contains reference implementations and templates for building data pipelines.

## 📂 Structure

### 🏗️ **Templates** (`examples/templates/`)
Copy-pasteable starting points for new projects.
- `simple_local.yaml` - Best for beginners (Pandas + Local FS)
- `azure_spark.yaml` - Production cloud setup (Spark + ADLS + Key Vault)
- `delta_lake.yaml` - Transactional data lake patterns

### 🚀 **Full Examples**
Complete, runnable projects with folder structures.
- `spark_sql_pipeline/` - End-to-end Spark SQL transformation
- `azure_etl/` - ADLS read → transform → Azure SQL write
- `medallion_architecture/` - Bronze/Silver/Gold pattern
- `story_demo/` - Pipeline showcasing story generation

### 🛠️ **Scripts & Plugins**
- `plugins/` - Example custom connectors (e.g., Postgres)
- `scripts/` - Utility scripts (local Spark setup, checkpointing)

### 🏚️ **Legacy** (`examples/legacy/`)
Old scripts and experimental code kept for reference.

---

## 🚀 Quick Start

**1. Run a local example:**
```bash
odibi run examples/templates/simple_local.yaml
```

**2. Start a new project:**
```bash
# Copy a template to your project root
cp examples/templates/azure_spark.yaml my_project.yaml
```
