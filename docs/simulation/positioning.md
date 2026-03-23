---
title: "Why Odibi Simulation"
---

# Why Odibi Simulation

**Odibi is the only data pipeline framework with a built-in physics simulation engine.**

Other tools generate fake data. Odibi generates *realistic* data - time series with autocorrelation, PID controllers that converge, material balances that close, cross-entity dependencies that propagate - and pipes it through transforms, validation, and Delta Lake. All from one YAML file.

---

## What Makes This Different

### 1. Simulation lives inside the pipeline

Most synthetic data tools are standalone: generate a CSV, then figure out how to get it into your pipeline. Odibi's simulation is a first-class read source. It sits in the same YAML config as your transforms, validation, and write logic. When real data arrives, you change one line (`format: simulation` to `format: csv`) and everything downstream stays unchanged.

```yaml
# Before: simulation
read:
  format: simulation
  options:
    simulation:
      scope: { start_time: "2026-01-01", timestep: "5m", row_count: 288, seed: 42 }
      # ...

# After: real data (one line changes)
read:
  format: csv
  path: bronze/sensors.csv
```

### 2. PID control in YAML

Define a working PID controller with anti-windup, output limits, and correct sign convention for reverse-acting loops - in a YAML expression. It converges. It eliminates steady-state offset. And it persists its integral state across pipeline runs.

```yaml
expression: "pid(pv=vessel_temp_c, sp=temp_setpoint_c, Kp=-2.0, Ki=-0.1, Kd=-0.5, dt=60, output_min=0, output_max=100)"
```

No Python. No Simulink. No MATLAB. A declarative PID controller that runs inside a data pipeline.

### 3. Physics, not noise

The `random_walk` generator implements an Ornstein-Uhlenbeck process with 8 tunable parameters: volatility, mean reversion, dynamic setpoint tracking, trend, precision, shock rate, shock magnitude, and shock bias. It produces data that looks like real SCADA output because it *behaves* like real SCADA output - autocorrelated, mean-reverting, subject to process upsets.

Three stateful functions add memory:

- `prev()` - integration, accumulation, level tracking
- `ema()` - exponential smoothing of noisy signals
- `pid()` - feedback control with anti-windup

Combined, these model first-order dynamics, material balances, energy balances, and closed-loop control - the building blocks of any process system.

### 4. 38 tested patterns across 8 industries

Not toy examples. Complete, copy-paste-ready YAML configs that teach both the framework AND the domain:

| Category | Patterns | Industries |
|----------|----------|------------|
| Foundations | 1-8 | General, manufacturing, IoT, e-commerce |
| Process Engineering | 9-15 | Environmental, oil & gas, chemical, pharma |
| Energy & Utilities | 16-20 | Renewables, storage, EV, smart grid |
| Manufacturing | 21-25 | Food & bev, CNC, logistics, automotive |
| Environmental | 26-28 | Meteorology, air quality, agriculture |
| Healthcare | 29-30 | ICU monitoring, pharma batch records |
| Business & IT | 31-35 | Retail, call center, IT ops, SaaS, supply chain |
| Data Engineering | 36-38 | Late data, schema evolution, multi-source merge |

Every pattern includes narrative context, entity breakdowns, mermaid diagrams, parameter justifications, sample output, exercises, and content extraction guides.

### 5. State persists across runs

Incremental simulation with `mode: stateful` preserves everything between pipeline runs:

- `prev()` last values
- `ema()` smoothed values
- `pid()` integral sums and last errors
- `random_walk` last positions

Run it on a schedule and the data accumulates continuously. Backfill a year of history on the first run, then add a day at a time. Your dashboard never knows it's synthetic.

### 6. Cross-entity dependencies

Real factories have upstream and downstream. A separator's feed is a reactor's output. A cooling tower responds to a compressor's heat load. Odibi models this with `EntityName.column_name` syntax in derived expressions, automatically resolving the dependency DAG across entities.

```yaml
# Separator receives 40% of reactor's outlet flow
expression: "reactor_01.outlet_flow * 0.4"
```

---

## Who Built This

Odibi was built by a chemical engineer turned data engineer - the only DE on an analytics team in operations, not IT. I know the gaps and pain points of doing data work alone while spearheading change in a company without dedicated data engineering support.

Simulation exists because I needed realistic process data and couldn't wait for IT to provision it. I know what a time constant is, what a PID controller does, and why your test data needs to pass the squint test. Every pattern in this library comes from a real problem I've solved.

The unique angle: **I'm a chemical engineer who built this. Let me teach you both the framework AND the engineering.**

---

## The Claims, Plainly Stated

1. **Odibi is the only data pipeline framework with a built-in physics simulation engine.** Faker generates fake names. dbt transforms real data. Odibi generates physically plausible process data, transforms it, validates it, and writes it to Delta Lake - all from one YAML file.

2. **You can define a PID controller in YAML.** It converges, it anti-winds up, and it persists integral state across pipeline runs. No other declarative framework does this.

3. **One line changes to go from simulation to production.** Change `format: simulation` to `format: csv` and delete the simulation block. Silver and gold layers stay unchanged. Your pipeline was tested against realistic data before the real source existed.

4. **38 patterns across 8 industries, all tested.** Copy-paste any of them and run `odibi run`. They work. They produce realistic data. And they teach you the domain as well as the framework.

---

## Start Here

- **[Getting Started](getting_started.md)** - Your first simulation in 30 minutes
- **[Patterns & Recipes](patterns/index.md)** - 38 production-ready patterns
- **[Process Simulation](process_simulation.md)** - ChemE and process control deep dive
- **[Generators Reference](generators.md)** - All 12 generator types
