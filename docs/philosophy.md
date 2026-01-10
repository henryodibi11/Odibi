# Odibi Philosophy

> The core principles that guide Odibi's design and evolution.

---

## Why Odibi Exists

Odibi was built for data engineers who work alone or in small teams—people who need to move fast without sacrificing quality. It encodes best practices into reusable patterns so you can focus on solving business problems, not reinventing infrastructure.

**Core Belief:** Data pipelines should be *declared*, not *coded*. You describe what you want; the framework handles how.

---

## Non-Negotiable Principles

1. **Declarative over imperative** — YAML is the source of truth
2. **Pydantic models, not dicts** — Type safety at the boundary
3. **Explicit dependencies over implicit behavior** — No magic
4. **Composition over inheritance** — Build from proven patterns
5. **Fail-fast validation** — Catch errors early, with context
6. **Performance awareness over cleverness** — Simple scales

---

## The Five Laws

These laws guide every design decision:

1. **Robots Remember, Humans Forget**  
   → Checkpoint bookkeeping, not manual tracking. State is managed automatically.

2. **Raw is Sacred**  
   → Append-only, immutable. Never destroy original data.

3. **Rebuild the Bucket, Don't Patch the Hole**  
   → Reprocess windows, don't patch aggregates. When something breaks, rebuild cleanly.

4. **One-off logic is a smell**  
   → If you do it twice, canonize it. Patterns emerge from repetition.

5. **Proven patterns should be reused forever**  
   → Don't invent without evidence. Build on what works.

---

## The Chimera Mindset

Odibi evolves through a continuous cycle:

```
Observe real usage → Absorb what works → Discard what doesn't → Evolve the system
```

We don't invent abstractions without proof. Features graduate to the framework only when real pipelines demonstrate the need.

### How Features Get Added

- **Build it** only if 2+ real use cases show the need
- **Reuse it** if a pattern already exists
- **Discard it** if no one has used it in 3+ months
- **Propose it** with evidence (real examples), not speculation

---

## Engine Parity Rule

If Pandas has it, Spark and Polars must too. No exceptions.

All three engines must produce identical results for the same input. You can develop locally with Pandas and deploy to production with Spark—same YAML config, same behavior.

---

## What Odibi Is

| ✅ Odibi Is | Description |
|-------------|-------------|
| A pipeline framework | Declarative YAML → executed pipelines |
| Multi-engine | Pandas, Spark, Polars with identical behavior |
| Pattern-driven | Dimension, Fact, SCD2, Aggregation built-in |
| Self-documenting | Every run generates a "Data Story" audit report |
| Quality-focused | Validation, contracts, quarantine, gates |

## What Odibi Is NOT

| ❌ Odibi Is Not | Use Instead |
|-----------------|-------------|
| A scheduler | Airflow, Databricks Workflows, Prefect |
| A BI tool | PowerBI, Tableau, Looker |
| A data catalog | Unity Catalog, DataHub, Amundsen |
| An agent/chat framework | LangChain, CrewAI, custom agents |

---

## Contributing

Want to contribute? Read [CONTRIBUTING.md](../CONTRIBUTING.md) first.

Remember: Propose features with evidence from real use cases, not speculation. The Chimera Mindset applies to contributions too.
