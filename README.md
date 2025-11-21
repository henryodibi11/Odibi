# ODIBI Framework
**Explicit over implicit. Stories over magic. Simple over clever.**

[![CI](https://github.com/henryodibi11/Odibi/workflows/CI/badge.svg)](https://github.com/henryodibi11/Odibi/actions)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

ODIBI is a declarative data engineering framework that makes pipelines **transparent**, **traceable**, and **teachable**.

Whether you are processing 10MB on your laptop or 10TB on Databricks, Odibi provides a unified "control plane" for your data.

---

## ⚡ The 30-Second Demo

Don't write boilerplate. Generate it.

```bash
# 1. Point Odibi at your messy data
odibi generate-project --input ./raw_csvs --output ./my_project

# 2. Run the generated pipeline
cd my_project
odibi run odibi.yaml

# 3. View the "Data Story" (Audit Report)
odibi story view --latest
```

**What just happened?**
*   Odibi analyzed your CSVs and inferred the schema.
*   It built a **Bronze/Silver** architecture automatically.
*   It ran the pipeline using the **Pandas** engine (or Spark, if configured).
*   It generated an **interactive HTML report** showing exactly what happened to every row.

---

## 📚 Documentation

We have rebuilt our documentation to help you learn fast.

### 🏁 Start Here
*   **[Getting Started Tutorial](docs/tutorials/getting_started.md):** Go from zero to production in 10 minutes.
*   **[Master CLI Guide](docs/guides/cli_master_guide.md):** Learn `run`, `stress`, `doctor`, and more.

### 📘 Guides & How-To
*   **[WSL Setup Guide](docs/guides/wsl_setup.md):** The definitive guide to Windows development.
*   **[Production Deployment](docs/guides/production_deployment.md):** Moving from local to Azure/Databricks.
*   **[Custom Transformations](docs/guides/custom_transformations.md):** Write your own Python logic.

### ⚙️ Reference
*   **[Cheatsheet](docs/reference/cheatsheet.md):** Syntax and commands on one page.
*   **[Configuration](docs/reference/configuration.md):** Every YAML option explained.
*   **[Supported Formats](docs/reference/supported_formats.md):** CSV, Parquet, Delta, JSON, Avro.

### 🧠 Concepts
*   **[Architecture](docs/explanation/architecture.md):** How Odibi works under the hood.
*   **[Case Studies](docs/explanation/case_studies.md):** Learn from reference projects (OdibiFlix, OdibiEats).

---

## 🚀 Key Features

### 1. "Data Stories" (Auto-Documentation)
Every time a pipeline runs, Odibi generates a "Story"—a JSON/HTML artifact capturing:
*   **Row Counts:** "Dropped 50 rows (-5%) in filtering step."
*   **Schema Changes:** "Column `user_id` cast from String to Int64."
*   **Performance:** "Node `clean_data` took 12.4s."

### 2. Stress Testing ("Chaos Monkey")
Proactively find bugs before production.
```bash
# Fuzz test your pipeline with random data
odibi stress odibi.yaml --runs 10
```

### 3. Production-Ready
*   **Secrets:** Built-in redaction for logs and artifacts.
*   **Delta Lake:** Full ACID transaction support (Time Travel, VACUUM).
*   **Azure Native:** Seamless integration with ADLS Gen2 and Key Vault.

---

## 📦 Installation

```bash
# Standard (Pandas Engine)
pip install odibi

# Enterprise (Spark + Azure + Delta)
pip install "odibi[spark,azure]"
```

---

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](docs/guides/contributing.md).

---

**Maintainer:** Henry Odibi (@henryodibi11)
**License:** MIT
