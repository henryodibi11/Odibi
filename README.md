# Odibi

**Declarative data pipelines. YAML in, star schemas out.**

> **Note:** Personal open-source project. See [IP_NOTICE.md](IP_NOTICE.md) for details.

[![CI](https://github.com/henryodibi11/Odibi/workflows/CI/badge.svg)](https://github.com/henryodibi11/Odibi/actions)
[![PyPI](https://img.shields.io/pypi/v/odibi.svg)](https://pypi.org/project/odibi/)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://henryodibi11.github.io/Odibi/)

Odibi is a framework for building data pipelines. You describe *what* you want in YAML; Odibi handles *how*. Every run generates a "Data Story" — an audit report showing exactly what happened to your data.

> 🤖 **AI/LLM Users:** For comprehensive context, see [docs/ODIBI_DEEP_CONTEXT.md](docs/ODIBI_DEEP_CONTEXT.md) — 2,200+ lines covering all patterns, transformers, validation, connections, and runtime behavior.

---

## 🎯 Try Odibi in 5 Minutes (No Install Needed)

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/1y00pX3ScH95QSxq6RoKRVxyXB6QPnHLM)

Click the badge above → run 3 cells → see your first simulation. No Python install, no cloning, no setup.

The notebook walks you through:
1. `pip install odibi` (runs in the cloud)
2. Define a simulation in YAML (sensors, sales data, or industrial equipment)
3. Run the pipeline → see the output → chart it with Altair

**When you're ready for more:** 38 simulation configs covering buildings, compressors, reactors, cooling towers, wastewater, production lines, and sales pipelines.

---

## ⚡ Quick Start (Local)

```bash
pip install odibi
```

**Option 1: Simulate data from YAML**

Create `sim.yaml`:
```yaml
project: my_first_sim
engine: pandas
connections:
  output:
    type: local
    base_path: ./data
story:
  connection: output
  path: stories/
system:
  connection: output
pipelines:
  - pipeline: demo
    nodes:
      - name: sensors
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-01-01T00:00:00Z"
                timestep: "5m"
                row_count: 100
                seed: 42
              entities:
                count: 3
                id_prefix: "sensor_"
              columns:
                - name: sensor_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}
                - name: temperature
                  data_type: float
                  generator:
                    type: random_walk
                    start: 22.0
                    min: 16.0
                    max: 30.0
                    volatility: 0.3
                    mean_reversion: 0.15
        write:
          connection: output
          format: parquet
          path: bronze/sensors.parquet
          mode: overwrite
```

Run it:
```bash
python -c "from odibi.pipeline import PipelineManager; PipelineManager.from_yaml('sim.yaml').run()"
```

Output: `data/bronze/sensors.parquet` — 300 rows of realistic sensor data with memory, drift, and mean reversion. No database needed.

**Option 2: Build a star schema from CSV**
```bash
odibi init my_project --template star-schema
cd my_project
odibi run odibi.yaml
odibi story last          # View the audit report
```

**Option 3: Clone the reference example**
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
| **Dimensional Patterns** | 6 built-in patterns: SCD1/SCD2, date dimension, fact tables, merge, aggregation |
| **56 Transformers** | Comprehensive library for data manipulation and quality |
| **Validation & Contracts** | Fail-fast checks, quarantine bad rows |
| **Multi-Engine** | Pandas, Polars, and Spark — same config across all engines |
| **Production Ready** | Retry, alerting, secrets, Delta Lake support |
| **Battle-Tested** | 5500+ tests ensure reliability and correctness |

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

# With Polars engine
pip install "odibi[polars]"

# With Spark + Azure support
pip install "odibi[spark,azure]"

# All engines and features
pip install "odibi[all]"
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
