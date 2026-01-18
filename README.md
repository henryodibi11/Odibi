# Odibi

**Declarative data pipelines. YAML in, star schemas out.**

[![CI](https://github.com/henryodibi11/Odibi/workflows/CI/badge.svg)](https://github.com/henryodibi11/Odibi/actions)
[![PyPI](https://img.shields.io/pypi/v/odibi.svg)](https://pypi.org/project/odibi/)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://henryodibi11.github.io/Odibi/)

Odibi is a framework for building data pipelines. You describe *what* you want in YAML; Odibi handles *how*. Every run generates a "Data Story" — an audit report showing exactly what happened to your data.

> 🤖 **AI/LLM Users:** For comprehensive context, see [docs/ODIBI_DEEP_CONTEXT.md](docs/ODIBI_DEEP_CONTEXT.md) — 2,200+ lines covering all patterns, transformers, validation, connections, and runtime behavior.

---

## ⚡ Quick Start

```bash
pip install odibi
```

**Option 1: Start from a template**
```bash
odibi init my_project --template star-schema
cd my_project
odibi run odibi.yaml
odibi story last          # View the audit report
```

**Option 2: Clone the reference example**
```bash
git clone https://github.com/henryodibi11/Odibi.git
cd Odibi/docs/examples/canonical/runnable
odibi run 04_fact_table.yaml
```

This builds a complete **star schema** in seconds:
- 3 dimension tables (customer, product, date)
- 1 fact table with FK lookups and orphan handling
- HTML audit report

**[See the full breakdown →](docs/examples/canonical/THE_REFERENCE.md)**

---

## 📖 The Canonical Example

```yaml
pipelines:
  - pipeline: build_dimensions
    nodes:
      - name: dim_customer
        read:
          connection: source
          format: csv
          path: customers.csv
        pattern:
          type: dimension
          params:
            natural_key: customer_id
            surrogate_key: customer_sk
            scd_type: 1
        write:
          connection: gold
          format: parquet
          path: dim_customer

      - name: dim_date
        pattern:
          type: date_dimension
          params:
            start_date: "2025-01-01"
            end_date: "2025-12-31"
        write:
          connection: gold
          format: parquet
          path: dim_date

  - pipeline: build_facts
    nodes:
      - name: fact_sales
        depends_on: [dim_customer, dim_date]
        read:
          connection: source
          format: csv
          path: orders.csv
        pattern:
          type: fact
          params:
            grain: [order_id, line_item_id]
            dimensions:
              - source_column: customer_id
                dimension_table: dim_customer
                dimension_key: customer_id
                surrogate_key: customer_sk
            orphan_handling: unknown
        write:
          connection: gold
          format: parquet
          path: fact_sales
```

**[Full runnable example →](docs/examples/canonical/runnable/04_fact_table.yaml)**

---

## 🚀 Key Features

| Feature | Description |
|---------|-------------|
| **Data Stories** | Every run generates an HTML audit report |
| **Dimensional Patterns** | SCD1/SCD2, date dimension, fact tables built-in |
| **Validation & Contracts** | Fail-fast checks, quarantine bad rows |
| **Dual Engine** | Pandas locally, Spark in production — same config |
| **Production Ready** | Retry, alerting, secrets, Delta Lake support |

---

## 📚 Documentation

| Goal | Link |
|------|------|
| **Get running in 10 minutes** | [Golden Path](docs/golden_path.md) |
| **Copy THE working example** | [THE_REFERENCE.md](docs/examples/canonical/THE_REFERENCE.md) |
| **Solve a specific problem** | [Playbook](docs/playbook/README.md) |
| **Understand when to use what** | [Decision Guide](docs/guides/decision_guide.md) |
| **See all config options** | [YAML Schema](docs/reference/yaml_schema.md) |

---

## 📦 Installation

```bash
# Standard (Pandas engine)
pip install odibi

# With Spark + Azure support
pip install "odibi[spark,azure]"
```

---

## 🎯 Who is this for?

- **Solo data engineers** building pipelines without a team
- **Analytics engineers** moving from dbt to Python-based pipelines
- **Anyone** tired of writing the same boilerplate for every project

---

## 🤝 Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md).

---

**Maintainer:** Henry Odibi ([@henryodibi11](https://github.com/henryodibi11))  
**License:** Apache 2.0
