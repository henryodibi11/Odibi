# Odibi Documentation

**Declarative data engineering. YAML in, pipelines out.**

---

## 🎯 Start Here

> **New to Odibi?** Follow the [Golden Path](golden_path.md) → Run [THE Reference Pipeline](examples/canonical/THE_REFERENCE.md) → You're done.

| Goal | Go to |
|------|-------|
| **Get running in 10 minutes** | [Golden Path](golden_path.md) |
| **Copy THE working config** | [THE_REFERENCE.md](examples/canonical/THE_REFERENCE.md) ⭐ |
| **Solve a specific problem** | [Playbook](playbook/README.md) |

---

## What is Odibi?

Odibi is a framework for building data pipelines. You describe *what* you want in YAML; Odibi handles *how*.

- **Declarative**: YAML over imperative Python
- **Auditable**: Every run generates a "Data Story" (HTML report)
- **Dual-engine**: Pandas (local) → Spark (production) with zero config changes

---

## The Canonical Example

Every new user should run this first:

```bash
cd docs/examples/canonical/runnable
odibi run 04_fact_table.yaml
```

This builds a complete star schema with dimensions, facts, FK lookups, and orphan handling. **[See the full breakdown →](examples/canonical/THE_REFERENCE.md)**

---

## Documentation Map

### New to Odibi?

1. [Golden Path](golden_path.md) — Zero to running in 10 minutes
2. [THE_REFERENCE.md](examples/canonical/THE_REFERENCE.md) — The one example to copy

### Building Pipelines?

- [Patterns](patterns/README.md) — SCD2, Merge, Aggregation, etc.
- [YAML Schema](reference/yaml_schema.md) — Complete configuration reference
- [Canonical Examples](examples/canonical/README.md) — More runnable configs

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
