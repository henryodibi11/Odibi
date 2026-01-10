# Odibi Roadmap

This roadmap outlines where Odibi is headed. It's a living document that evolves based on user feedback and real-world usage.

---

## Current Release: v2.6.0

### What's Available Now

| Feature | Status |
|---------|--------|
| **Declarative YAML pipelines** | ✅ Stable |
| **Multi-engine support** (Pandas, Spark, Polars) | ✅ Stable |
| **Dimensional modeling patterns** (Dimension, Fact, SCD2, Aggregation) | ✅ Stable |
| **Data validation & quality gates** | ✅ Stable |
| **Quarantine for bad records** | ✅ Stable |
| **Data Stories** (execution audit reports) | ✅ Stable |
| **System Catalog** (10 Delta meta tables) | ✅ Stable |
| **Incremental loading** (HWM, skip-if-unchanged) | ✅ Stable |
| **Azure connections** (ADLS, SQL Server, Key Vault) | ✅ Stable |
| **Semantic layer** (metrics, materialization) | ✅ Stable |
| **CLI** (`odibi run`, `odibi validate`, etc.) | ✅ Stable |
| **Golden Path Templates** (`odibi init`) | ✅ Stable |
| **Frictionless Debugging** (`debug_summary()`, `odibi story last`) | ✅ Stable |

---

## Short Term (Next 1-2 Releases)

### v2.7.0 - Observability

| Feature | Description | Status |
|---------|-------------|--------|
| **Enhanced lineage** | Cross-pipeline lineage stitching | 🔄 Planned |
| **Metrics export** | Export pipeline metrics to Prometheus/DataDog | 💭 Considering |
| **Slack/Teams alerts** | Richer alert formatting | 💭 Considering |

---

## Medium Term (3-6 Months)

### Performance & Scale

| Feature | Description | Status |
|---------|-------------|--------|
| **Parallel node execution** | Run independent nodes concurrently | 💭 Considering |
| **Lazy evaluation mode** | Defer execution for optimization | 💭 Considering |
| **Memory profiling** | Track memory usage per node | 💭 Considering |

### Ecosystem Integration

| Feature | Description | Status |
|---------|-------------|--------|
| **dbt compatibility** | Import/export dbt models | 💭 Considering |
| **Airflow operator** | Native Airflow integration | 💭 Considering |
| **Databricks Asset Bundles** | First-class DAB support | 💭 Considering |

---

## Long Term Vision

### The Ultimate Goal

> A framework so easy and powerful it gives data engineers time back to focus on what matters.

**Principles guiding development:**
- **Declarative over imperative** — YAML is the source of truth
- **Explicit over implicit** — No magic, no surprises
- **Engine parity** — Same config works on Pandas, Spark, or Polars
- **Stories over logs** — Every run generates human-readable audit docs

### What Odibi Will Never Be

- ❌ A scheduler (use Airflow, Databricks Workflows, etc.)
- ❌ A BI tool (it prepares data for BI tools)
- ❌ A data catalog (it writes to catalogs)
- ❌ An agent/chat framework (use specialized tools for that)

---

## Request a Feature

Have an idea? We'd love to hear it:

1. **GitHub Issues**: [Request a feature](https://github.com/henryodibi11/Odibi/issues/new)
2. **Discussions**: [Share ideas](https://github.com/henryodibi11/Odibi/discussions)

### How We Prioritize

Features are prioritized based on:
1. **Real usage** — Does a story show the need? (See our [Chimera Mindset](./philosophy.md#the-chimera-mindset))
2. **Impact** — How many users benefit?
3. **Complexity** — Can we ship it without breaking things?
4. **Alignment** — Does it fit [Odibi's principles](./philosophy.md)?

---

## Changelog

See [CHANGELOG.md](https://github.com/henryodibi11/Odibi/blob/main/CHANGELOG.md) for detailed release notes.
