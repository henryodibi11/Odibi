---
title: "Getting Started: Your First Simulation"
roles: [ba, jr-de, cheme]
tags: [tutorial:beginner, topic:simulation, topic:first-pipeline]
prereqs: [../guides/installation.md]
next: [core_concepts.md, generators.md, patterns.md]
related: [../tutorials/getting_started.md]
time: 30m
---

# Getting Started with Simulation

This tutorial takes you from zero to a working simulation in under 30 minutes. You'll generate realistic data, add chaos, and run transformations — all without a single real data source.

**Prerequisites:**

- Odibi installed (`pip install odibi`)
- Basic familiarity with YAML
- A terminal open in your project folder

---

## Your First Simulation

Let's start with the absolute simplest simulation: **1 entity, 3 columns, no chaos**. Just clean, predictable data.

Create a file called `my_first_simulation.yaml`:

```yaml
project: my_first_simulation
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
  - pipeline: first_sim
    nodes:
      - name: sensor_data
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
                count: 1
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
                  generator: {type: range, min: 20.0, max: 35.0}
        write:
          connection: output
          format: parquet
          path: bronze/sensors.parquet
          mode: overwrite
```

### What Every Line Does

**Pipeline wrapper:**

| Line | Purpose |
|------|---------|
| `project: my_first_simulation` | A human-readable name for this project |
| `engine: pandas` | Use the Pandas engine (good for < 1M rows) |
| `connections: output:` | Defines where output files go |
| `type: local` | Write to the local filesystem |
| `base_path: ./data` | Root folder for all output paths |

**Node definition:**

| Line | Purpose |
|------|---------|
| `name: sensor_data` | Name of this pipeline step |
| `connection: null` | Simulation doesn't read from an external source |
| `format: simulation` | Tells Odibi to generate data instead of reading it |

**Scope — when and how much:**

| Line | Purpose |
|------|---------|
| `start_time: "2026-01-01T00:00:00Z"` | First timestamp in the data |
| `timestep: "5m"` | 5 minutes between each row |
| `row_count: 100` | Generate exactly 100 rows per entity |
| `seed: 42` | Makes output reproducible — same seed, same data every time |

**Entities — who generates data:**

| Line | Purpose |
|------|---------|
| `count: 1` | One entity (one sensor) |
| `id_prefix: "sensor_"` | Entity will be named `sensor_01` |

**Columns — what data gets generated:**

| Line | Purpose |
|------|---------|
| `sensor_id` with `{entity_id}` | Every row gets the entity's name (`sensor_01`) |
| `timestamp` with `type: timestamp` | Auto-incrementing time, using `start_time` + `timestep` |
| `temperature` with `type: range` | Random float between 20.0 and 35.0 |

**Write block:**

| Line | Purpose |
|------|---------|
| `connection: output` | Use the `output` connection defined above |
| `format: parquet` | Write as Parquet (efficient columnar format) |
| `path: bronze/sensors.parquet` | File path relative to `base_path` |
| `mode: overwrite` | Replace the file on each run |

### Run It

```bash
odibi run my_first_simulation.yaml
```

You'll see output confirming **100 rows** written to `./data/bronze/sensors.parquet`.

**Here's what your data looks like:**

| sensor_id | timestamp | temperature |
|-----------|-----------|-------------|
| sensor_01 | 2026-01-01 00:00:00 | 27.3 |
| sensor_01 | 2026-01-01 00:05:00 | 22.8 |
| sensor_01 | 2026-01-01 00:10:00 | 31.1 |
| sensor_01 | 2026-01-01 00:15:00 | 24.6 |
| sensor_01 | 2026-01-01 00:20:00 | 29.4 |
| ... | ... | ... |
| sensor_01 | 2026-01-01 08:15:00 | 33.2 |

100 rows of realistic sensor data - from 20 lines of YAML. No Python, no Faker, no hand-crafted CSV. The same config works on Pandas, Spark, and Polars.

!!! tip "connection: null"
    Simulation doesn't read from any external source, so `connection: null` is all you need. No connection definition required for the read side.

!!! note "Reproducibility"
    The `seed: 42` setting guarantees identical output every time you run this config. Change the seed, get different data. Remove it, get random data on each run.

---

## Adding Multiple Entities

What if you have 5 sensors instead of 1? Change one number:

```yaml
entities:
  count: 5
  id_prefix: "sensor_"
```

That's it. Odibi generates **5 entities × 100 rows = 500 total rows**.

Each entity gets its own name: `sensor_01`, `sensor_02`, `sensor_03`, `sensor_04`, `sensor_05`. The `{entity_id}` placeholder in the `sensor_id` column resolves to the current entity's name, so every row knows which sensor it belongs to.

```
sensor_id   | timestamp              | temperature
------------|------------------------|------------
sensor_01   | 2026-01-01T00:00:00Z   | 27.3
sensor_01   | 2026-01-01T00:05:00Z   | 22.8
...         | ...                    | ...
sensor_05   | 2026-01-01T08:15:00Z   | 31.4
```

Each entity gets its own independent copy of the 100 rows. The timestamps are identical across entities (they all start at the same `start_time`), but the random values differ because each entity has its own RNG stream derived from the seed.

!!! tip "Quick math"
    `entities × row_count = total rows`. Planning for a load test? 100 entities × 10,000 rows = **1,000,000 rows**.

---

## Adding Variety

Sensors don't just report temperature. Let's add a categorical **status** column and a boolean **is_online** column.

Update the `columns` section:

```yaml
columns:
  - name: sensor_id
    data_type: string
    generator: {type: constant, value: "{entity_id}"}

  - name: timestamp
    data_type: timestamp
    generator: {type: timestamp}

  - name: temperature
    data_type: float
    generator: {type: range, min: 20.0, max: 35.0}

  - name: status
    data_type: string
    generator:
      type: categorical
      values: [Running, Idle, Error]
      weights: [0.8, 0.15, 0.05]

  - name: is_online
    data_type: boolean
    generator:
      type: boolean
      true_probability: 0.95
```

### What the new columns do

**`status`** — picks from a list with weighted probabilities:

- 80% of rows → `Running`
- 15% of rows → `Idle`
- 5% of rows → `Error`

**`is_online`** — true/false with a 95% chance of being `True`.

Now each row of output looks like:

```
sensor_id   | timestamp              | temperature | status  | is_online
------------|------------------------|-------------|---------|----------
sensor_01   | 2026-01-01T00:00:00Z   | 27.3        | Running | True
sensor_01   | 2026-01-01T00:05:00Z   | 22.8        | Running | True
sensor_01   | 2026-01-01T00:10:00Z   | 31.1        | Idle    | True
sensor_01   | 2026-01-01T00:15:00Z   | 24.6        | Error   | False
```

!!! note "Weights must sum to 1.0"
    If you use `weights` with `categorical`, they must add up to `1.0`. Omit `weights` entirely for equal probability across all values.

---

## Making It Realistic

Real sensor data isn't clean. Sensors spike, duplicate readings, and go offline. The **chaos** section simulates all of this.

Add a `chaos` block alongside `scope`, `entities`, and `columns`:

```yaml
options:
  simulation:
    scope:
      start_time: "2026-01-01T00:00:00Z"
      timestep: "5m"
      row_count: 100
      seed: 42
    entities:
      count: 5
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
        generator: {type: range, min: 20.0, max: 35.0}
      - name: status
        data_type: string
        generator:
          type: categorical
          values: [Running, Idle, Error]
          weights: [0.8, 0.15, 0.05]
      - name: is_online
        data_type: boolean
        generator:
          type: boolean
          true_probability: 0.95
    chaos:
      outlier_rate: 0.02
      outlier_factor: 3.0
      duplicate_rate: 0.01
      downtime_events:
        - entity: sensor_03
          start_time: "2026-01-01T02:00:00Z"
          end_time: "2026-01-01T04:00:00Z"
```

### What chaos does to your data

| Setting | Effect |
|---------|--------|
| `outlier_rate: 0.02` | 2% of numeric values become outliers |
| `outlier_factor: 3.0` | Outliers are 3× the normal value (a 30°C reading becomes 90°C) |
| `duplicate_rate: 0.01` | 1% of rows are duplicated (same timestamp, same values) |
| `downtime_events` | `sensor_03` produces **no data** between 02:00 and 04:00 |

**Expected output characteristics with 5 entities × 100 rows:**

- ~490 base rows (sensor_03 loses ~24 rows during downtime)
- ~5 duplicate rows scattered across all entities
- ~10 temperature values spiked to 60–105°C range
- Clean, predictable data everywhere else

!!! tip "Why chaos matters"
    Chaos lets you test that your pipeline handles real-world problems — outlier detection catches the spikes, deduplication removes the copies, and gap-filling handles the downtime. Build the pipeline once, test it with chaos, then swap to real data with confidence.

---

## Adding Transformations

Simulated data is regular data. Every transformer, validation test, and pattern that works with real sources works identically with simulation output.

Let's add a `transform` step to classify temperatures and a `validation` block to enforce data quality:

```yaml
nodes:
  - name: sensor_data
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
            count: 5
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
              generator: {type: range, min: 20.0, max: 35.0}

    transform:
      steps:
        - operation: derive_columns
          params:
            columns:
              temp_category: >
                CASE
                  WHEN temperature < 25 THEN 'cold'
                  WHEN temperature < 30 THEN 'normal'
                  ELSE 'hot'
                END

    validation:
      mode: warn
      tests:
        - type: not_null
          columns: [sensor_id, timestamp, temperature]
        - type: range
          column: temperature
          min: 0.0
          max: 150.0

    write:
      connection: output
      format: parquet
      path: bronze/sensors.parquet
      mode: overwrite
```

### What this adds

**`derive_columns`** creates a new `temp_category` column using SQL CASE logic:

- Below 25°C → `cold`
- 25–30°C → `normal`
- Above 30°C → `hot`

**`validation`** runs data quality checks after the transform:

- `not_null` — ensures no NULL values in critical columns
- `range` — flags any temperature outside 0–150°C (catches extreme outliers)

With `mode: warn`, the pipeline continues even if tests fail — you'll see warnings in the console and the Data Story. Switch to `mode: fail` to stop the pipeline on any violation.

!!! note "Build → Test → Swap"
    This is the power of simulation: build your full pipeline — transforms, validations, write logic — using generated data. When the real source is ready, change `format: simulation` to `format: csv` (or `delta`, or `sql`) and point it at the real connection. Everything downstream stays exactly the same.

---

## What's Next

You've built a simulation from scratch, scaled it to multiple entities, added realistic chaos, and wired up transforms and validation. Here's where to go deeper:

- **[Core Concepts](core_concepts.md)** — understand scope, entities, and columns in depth
- **[Generators Reference](generators.md)** — all 13 generator types with full parameter docs
- **[Stateful Functions](stateful_functions.md)** — `prev()`, `ema()`, `pid()`, `delay()` for dynamic, time-dependent data
- **[Safe Functions Reference](../reference/simulation_generators.md#derived)** — complete list of all functions available in derived expressions
- **[Advanced Features](advanced_features.md)** — cross-entity references, scheduled events (recurring, condition-based, ramp transitions), entity overrides
- **[Patterns & Recipes](patterns.md)** — real-world simulation scenarios (IoT fleets, process control, daily feeds)

!!! tip "Explore from the CLI"
    Use `odibi list transformers` to see all 54 available transformers, or `odibi explain derive_columns` to get detailed docs for any specific feature — without leaving your terminal.
