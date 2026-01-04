# Odibi Documentation

**Declarative data engineering. YAML in, pipelines out.**

---

## Start Here

| Goal | Go to |
|------|-------|
| **Get running in 10 minutes** | [Golden Path](golden_path.md) |
| **Find a solution to a problem** | [Playbook](playbook/README.md) |
| **Copy a working config** | [Canonical Examples](examples/canonical/README.md) |

---

## What is Odibi?

Odibi is a framework for building data pipelines. You describe *what* you want in YAML; Odibi handles *how*.

- **Declarative**: YAML over imperative Python
- **Auditable**: Every run generates a "Data Story" (HTML report)
- **Dual-engine**: Works with Pandas (local) or Spark (production)

---

## Quick Example

```yaml
project: my_project

connections:
  raw:
    type: local
    base_path: ./data

pipelines:
  - pipeline: ingest
    nodes:
      - name: customers
        read:
          connection: raw
          format: csv
          path: customers.csv
        write:
          connection: raw
          format: parquet
          path: silver/customers
```

```bash
pip install odibi
odibi run odibi.yaml
```

---

## Documentation Map

### New to Odibi?

1. [Golden Path](golden_path.md) — Zero to running in 10 minutes
2. [Getting Started Tutorial](tutorials/getting_started.md) — Detailed first steps
3. [Playbook](playbook/README.md) — Find solutions to problems

### Building Pipelines?

- [Canonical Examples](examples/canonical/README.md) — 5 copy-paste configs
- [Patterns](patterns/README.md) — SCD2, Merge, Aggregation, etc.
- [YAML Schema](reference/yaml_schema.md) — Complete configuration reference

### Going to Production?

- [Decision Guide](guides/decision_guide.md) — When to use what
- [Production Deployment](guides/production_deployment.md) — Cloud setup
- [Alerting](features/alerting.md) — Notifications on failure

---

## Quick Links

- **Repository:** [GitHub](https://github.com/henryodibi11/Odibi)
- **Issues:** [Report a Bug](https://github.com/henryodibi11/Odibi/issues)
- **PyPI:** [View Package](https://pypi.org/project/odibi/)

---

*Last updated: January 2025*
