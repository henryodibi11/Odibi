# Simulation

**Generate realistic synthetic data for any domain — manufacturing, operations, IoT, process control, business — directly in your pipeline YAML. No code needed.**

Simulation is Odibi's built-in data generator. Define what you need in YAML, and the framework produces time-series, categorical, relational, and process data that behaves like the real thing. Swap to real sources later — your downstream pipeline stays unchanged.

```yaml title="A taste of simulation"
read:
  format: simulation
  options:
    simulation:
      scope: { start_time: "2026-01-01", timestep: "5m", row_count: 288, seed: 42 }
      entities: { count: 3, id_prefix: "sensor_" }
      columns:
        - name: temperature
          data_type: float
          generator: { type: random_walk, start: 72, min: 60, max: 90, volatility: 0.5 }
```

---

## Key Capabilities

| Capability | What You Get |
|---|---|
| **12 Generator Types** | `range` · `random_walk` · `categorical` · `boolean` · `timestamp` · `sequential` · `constant` · `derived` · `uuid` · `email` · `ipv4` · `geo` |
| **Stateful Functions** | `prev()`, `ema()`, `pid()` — values that depend on history for dynamic process simulation |
| **Cross-Entity References** | One entity reacts to another: downstream sensor reads upstream output |
| **Entity Overrides** | Per-entity behavior variation — entity A runs hot, entity B runs cold |
| **Scheduled Events** | Maintenance windows, setpoint changes, shutdowns at specific times |
| **Chaos Engineering** | Outliers, duplicates, downtime gaps, null injection — realistic imperfections |
| **Incremental Mode** | Continuous data generation with HWM state — each run picks up where the last left off |
| **Deterministic** | Same seed = same output, every time |
| **Multi-Engine** | Same YAML works on Pandas, Spark, and Polars |

---

## Learning Path

Work through the simulation docs in order, or jump to what you need:

| Page | What You'll Learn |
|---|---|
| :material-rocket-launch: [Getting Started](getting_started.md) | Your first simulation in 5 minutes |
| :material-book-open-variant: [Core Concepts](core_concepts.md) | Scope, entities, and columns — the three building blocks |
| :material-format-list-bulleted-type: [Generators Reference](generators.md) | All 12 generator types with parameters and examples |
| :material-function-variant: [Stateful Functions](stateful_functions.md) | `prev()`, `ema()`, `pid()` — history-dependent values |
| :material-puzzle: [Advanced Features](advanced_features.md) | Cross-entity references, overrides, scheduled events, chaos |
| :material-sync: [Incremental Mode](incremental.md) | Continuous data generation across pipeline runs |
| :material-chef-hat: [Patterns & Recipes](patterns.md) | Real-world scenarios: IoT fleets, batch reactors, order streams |
| :material-flask: [Process Simulation](process_simulation.md) | ChemE and process control: FOPTD, PID loops, reactor dynamics |

---

## When to Use Simulation

- **Build pipelines before source data exists** — design transforms, test patterns, validate schema now
- **Test with safe, reproducible data** — no PII, no compliance headaches
- **Stress test Delta Lake at scale** — 1,000 entities × 10,000 rows = 10M rows from a single YAML node
- **Demo without exposing real data** — realistic enough for stakeholders, safe enough for anywhere
- **Simulate manufacturing, operations, and IoT** — sensors, PLCs, batch processes, alarms for a local data platform
- **Prototype analytics before production data arrives** — build dashboards on synthetic facts and dimensions

---

!!! success "Installation"
    Simulation is built into odibi core. No extra dependencies, no plugins — if you have odibi installed, you have simulation.

    ```bash
    pip install odibi
    ```

---

## Next Steps

Start with [Getting Started](getting_started.md) to generate your first dataset in under 5 minutes, or browse the [Generators Reference](generators.md) to see what's available.
