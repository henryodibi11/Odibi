# Synthetic Data Generation (Simulation)

Generate realistic data directly in your pipelines — no external tools, no real data needed.

---

## Why Simulation?

You need data but don't have it yet. Maybe the source system isn't ready, maybe you can't use production data, or maybe you just want to test your pipeline logic without waiting on IT.

Simulation lets you:

- **Build pipelines before sources exist** — swap to real data later, no code changes
- **Test with safe, reproducible data** — same seed = same output, every time
- **Stress test at scale** — generate millions of rows to test Delta Lake performance
- **Demo without risk** — realistic data that isn't anyone's real data

---

## Your First Simulation

Here's the simplest possible simulation — 3 sensors generating 24 hours of readings:

```yaml
nodes:
  - name: sensor_data
    read:
      connection: null          # No connection needed for simulation
      format: simulation
      options:
        simulation:
          scope:
            start_time: "2026-01-01T00:00:00Z"
            timestep: "5m"
            row_count: 288      # 288 × 5min = 24 hours
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
              generator: {type: range, min: 20, max: 35}
    write:
      connection: my_lake
      format: delta
      table: bronze_sensors
      mode: overwrite
```

**What happens:** 3 entities × 288 rows = **864 rows** of sensor data, with temperatures between 20–35°C at 5-minute intervals.

**Run it:**
```bash
odibi run my_pipeline.yaml
```

!!! tip "connection: null"
    Simulation doesn't read from any external source, so set `connection: null`. No connection definition needed in your project.yaml.

---

## How Simulation Works

Every simulation has three parts:

```
┌─────────┐     ┌──────────┐     ┌─────────┐
│  Scope  │  →  │ Entities │  →  │ Columns │
│ (when)  │     │  (who)   │     │ (what)  │
└─────────┘     └──────────┘     └─────────┘
```

1. **Scope** — *When* and *how much* data: start time, interval, row count
2. **Entities** — *Who* generates data: sensors, users, machines, etc.
3. **Columns** — *What* data each entity produces: temperatures, IDs, statuses

Each entity gets its own copy of `row_count` rows. So 5 entities × 100 rows = 500 total rows.

---

## Scope: When and How Much

```yaml
scope:
  start_time: "2026-01-01T00:00:00Z"   # When data starts
  timestep: "5m"                        # Interval between rows
  row_count: 288                        # Rows per entity
  seed: 42                              # Makes output reproducible
```

**Timestep formats:** `5s` (seconds), `10m` (minutes), `1h` (hours), `2d` (days)

**How many rows?** Use either `row_count` OR `end_time` — not both:

```yaml
# Option A: Fixed count
scope:
  row_count: 288        # Exactly 288 rows per entity

# Option B: Time range
scope:
  end_time: "2026-01-02T00:00:00Z"   # Generate until this time
```

!!! tip "Quick math"
    `row_count × timestep = duration`. So 288 rows × 5 minutes = 1,440 minutes = **1 day**.

---

## Entities: Who Generates Data

Entities are the "things" producing data — sensors, users, machines, production lines, etc.

```yaml
# Auto-generate names
entities:
  count: 10
  id_prefix: "sensor_"       # → sensor_01, sensor_02, ... sensor_10

# OR name them explicitly
entities:
  names: [pump_01, pump_02, reactor_01]
```

Each entity generates its own set of `row_count` rows independently.

---

## Columns: What Gets Generated

Each column needs a `name`, `data_type`, and `generator`. Here are all the generator types:

### range — Numbers

```yaml
- name: temperature
  data_type: float
  generator:
    type: range
    min: 60.0
    max: 100.0
```

Optional: add `distribution: normal` with `mean` and `std_dev` for bell-curve values.

### random_walk — Realistic process data

Unlike `range` which picks each value independently, `random_walk` makes each value depend on the previous one — producing smooth, realistic time-series data.

```yaml
- name: reactor_temp
  data_type: float
  generator:
    type: random_walk
    start: 350.0          # Setpoint / initial value
    min: 300.0             # Physical lower bound
    max: 400.0             # Physical upper bound
    volatility: 0.5        # How much it can change per step
    mean_reversion: 0.1    # Pull back toward setpoint (like a PID controller)
    trend: 0.001           # Slow drift per step (fouling, degradation)
    precision: 1           # Round to 1 decimal (like a real sensor)
    shock_rate: 0.02       # 2% chance of sudden process upset per step
    shock_magnitude: 30.0  # Shocks up to ±30 degrees
    shock_bias: 1.0        # Shocks always go up (exothermic runaway)
```

**When to use:** Temperatures, pressures, flow rates, pH, levels — any process variable that changes gradually, not randomly.

**`mean_reversion`** controls how strongly values pull back to the `start` value. Think of it as the tightness of your control loop:
- `0.0` = pure random walk (no control, drifts freely)
- `0.1` = loose control (slow correction)
- `0.5` = tight control (snaps back quickly)

**`trend`** adds gradual drift — simulates fouling, catalyst deactivation, or filter clogging over time.

**`shock_rate`**, **`shock_magnitude`**, and **`shock_bias`** simulate sudden process upsets. Unlike chaos outliers (which modify output after generation), shocks perturb the walk's internal state — so `mean_reversion` naturally pulls the value back over subsequent rows, just like a real PID-controlled process recovering from an upset:
- `shock_rate: 0.02` = 2% chance per step (~1 shock every 50 readings)
- `shock_magnitude: 30.0` = shock jumps up to ±30 from current value
- `shock_bias: 1.0` = shocks always push upward (+1=up, -1=down, 0=random direction)

!!! tip "Incremental mode"
    With `incremental.mode: stateful`, the random walk remembers each entity's last value between runs. Run 2 picks up exactly where run 1 left off — no discontinuities.

### categorical — Pick from a list

```yaml
- name: status
  data_type: string
  generator:
    type: categorical
    values: [Running, Idle, Error]
    weights: [0.8, 0.15, 0.05]     # Optional — default is equal probability
```

### boolean — True/False

```yaml
- name: is_active
  data_type: boolean
  generator:
    type: boolean
    true_probability: 0.95          # 95% chance of True
```

### timestamp — Auto-incrementing time

```yaml
- name: event_time
  data_type: timestamp
  generator:
    type: timestamp                 # Uses scope.timestep automatically
```

### sequential — Auto-incrementing numbers

```yaml
- name: record_id
  data_type: int
  generator:
    type: sequential
    start: 1
    step: 1                         # → 1, 2, 3, 4, ...
```

### constant — Fixed value

```yaml
- name: source_system
  data_type: string
  generator:
    type: constant
    value: "simulation"
```

**Magic variables** you can use in constant values:

| Variable | Description | Example output |
|----------|-------------|----------------|
| `{entity_id}` | Current entity name | `sensor_03` |
| `{entity_index}` | Entity index (0-based) | `2` |
| `{timestamp}` | Current row timestamp | `2026-01-01T00:15:00` |
| `{row_number}` | Row index | `3` |

### uuid — Unique identifiers

```yaml
- name: transaction_id
  data_type: string
  generator:
    type: uuid
```

### email — Email addresses

```yaml
- name: contact
  data_type: string
  generator:
    type: email
```

### ipv4 — IP addresses

```yaml
- name: source_ip
  data_type: string
  generator:
    type: ipv4
```

### geo — Coordinates

```yaml
- name: location
  data_type: string
  generator:
    type: geo
```

### derived — Calculated from other columns

```yaml
- name: temp_fahrenheit
  data_type: float
  generator:
    type: derived
    expression: "temp_celsius * 1.8 + 32"
```

Derived columns can reference any column defined above them. Odibi automatically resolves the dependency order.

**More examples:**

```yaml
# Conditional logic
- name: alert_level
  data_type: string
  generator:
    type: derived
    expression: "'CRITICAL' if temperature > 90 else 'NORMAL'"

# Safe division
- name: efficiency
  data_type: float
  generator:
    type: derived
    expression: "(output / input * 100) if input > 0 else 0"
```

!!! warning "Expression Safety"
    Derived expressions run in a sandboxed namespace. Math, logic, and safe functions (`abs`, `round`, `min`, `max`) are allowed. Imports, file I/O, and system calls are blocked.

---

## Adding Realism

### Null Values

Make some values randomly missing:

```yaml
- name: optional_reading
  data_type: float
  generator: {type: range, min: 0, max: 100}
  null_rate: 0.1                    # 10% of values will be NULL
```

### Entity Overrides

Give specific entities different behavior:

```yaml
- name: pressure
  data_type: float
  generator:
    type: range
    min: 50
    max: 100
  entity_overrides:
    heavy_duty_pump:                # This entity uses a higher range
      type: range
      min: 100
      max: 200
```

### Chaos Engineering

Add realistic imperfections to your data:

```yaml
chaos:
  outlier_rate: 0.01                # 1% of numeric values become outliers
  outlier_factor: 3.0               # Outliers are 3× normal value
  duplicate_rate: 0.005             # 0.5% of rows are duplicated
  downtime_events:                  # Gaps where entities produce no data
    - entity: sensor_05
      start_time: "2026-01-01T12:00:00Z"
      end_time: "2026-01-01T14:00:00Z"
```

This is great for testing whether your pipeline handles bad data gracefully.

---

## Incremental Mode: Continuous Data Generation

Want your simulation to generate **new data on each run**, like a real streaming source? Use incremental mode.

!!! note "Same config as all other sources"
    The `incremental:` block is the **exact same** config used for SQL, Delta, CSV, and every other read source. The simulator simply respects the High Water Mark (HWM) to know where to start. See [Stateful Incremental Loading](../patterns/incremental_stateful.md) for full details.

### How it works

```
Run 1: Generates Jan 1 00:00 → 23:00  →  Saves HWM: Jan 1 23:00
Run 2: Generates Jan 2 00:00 → 23:00  →  Saves HWM: Jan 2 23:00
Run 3: Generates Jan 3 00:00 → 23:00  →  Saves HWM: Jan 3 23:00
...
```

Each run picks up exactly where the last one left off.

### Configuration

Just add `incremental:` to your read block:

```yaml
- name: sensor_stream
  read:
    connection: null
    format: simulation
    options:
      simulation:
        scope:
          start_time: "2026-01-01T00:00:00Z"
          timestep: "5m"
          row_count: 288            # 1 day of data per run
          seed: 42
        entities:
          count: 10
          id_prefix: "sensor_"
        columns:
          - name: sensor_id
            data_type: string
            generator: {type: constant, value: "{entity_id}"}
          - name: timestamp
            data_type: timestamp
            generator: {type: timestamp}
          - name: value
            data_type: float
            generator: {type: range, min: 0, max: 100}

    incremental:
      mode: stateful
      column: timestamp             # Must match a timestamp column

  write:
    connection: my_lake
    format: delta
    table: sensor_stream
    mode: append                    # Append each day's data
```

### Determinism

Incremental simulation stays deterministic — same seed + same HWM = identical output. Each run advances the RNG state based on the HWM, so results are reproducible across environments.

---

## Common Patterns

### Pattern 1: Build Before Sources Exist

```yaml
# Bronze: simulate what the upstream source will look like
- name: bronze_orders
  read:
    connection: null
    format: simulation
    options:
      simulation:
        scope: {start_time: "2026-01-01T00:00:00Z", timestep: "1h", row_count: 100, seed: 42}
        entities: {count: 1, id_prefix: ""}
        columns:
          - name: order_id
            data_type: int
            generator: {type: sequential, start: 1}
          - name: amount
            data_type: float
            generator: {type: range, min: 10, max: 500}
          - name: created_at
            data_type: timestamp
            generator: {type: timestamp}

# Silver: process as if real — this code never changes
- name: silver_orders
  depends_on: [bronze_orders]
  transform:
    - operation: derive_columns
      params:
        columns:
          order_tier: "CASE WHEN amount > 200 THEN 'high' ELSE 'standard' END"
```

When the real source is ready, just swap `bronze_orders` to read from it. Silver stays untouched.

### Pattern 2: Stress Test Delta Lake

```yaml
scope:
  row_count: 10000
entities:
  count: 1000                      # 1,000 entities × 10K rows = 10M rows
```

Test write performance, partitioning strategies, and compaction.

### Pattern 3: Daily Data Feed for Dashboards

```yaml
read:
  connection: null
  format: simulation
  options:
    simulation:
      scope: {start_time: "2026-01-01T00:00:00Z", timestep: "1h", row_count: 24, seed: 42}
      # ...columns...
  incremental:
    mode: stateful
    column: timestamp
write:
  mode: append
```

Schedule with a cron job — each run adds one more day of demo data.

---

## Integration with Pipelines

Simulated data is regular data. Everything that works with real sources works with simulation:

```yaml
- name: sim_bronze
  read:
    connection: null
    format: simulation
    options: {simulation: {...}}
  transform:
    - operation: derive_columns
      params: {...}
  validate:
    tests:
      - type: not_null
        columns: [id, timestamp]
  write:
    connection: lake
    format: delta
    table: bronze.sim_data
    mode: overwrite
```

Transforms, validation, quality gates, quarantine, stories — all work exactly the same.

---

## Performance

| Scenario | Guidance |
|----------|----------|
| < 1M rows | Pandas engine is fine |
| 1M–10M rows | Consider Spark engine |
| > 10M rows | Use Spark with partitioning |

Generation speed is roughly **100K rows/second** on Pandas. Chaos effects (outliers, duplicates) add minimal overhead.

---

## Engine Support

All three engines are fully supported. Same YAML, same output:

| Engine | Status | Notes |
|--------|--------|-------|
| Pandas | ✅ Native | Primary implementation |
| Spark | ✅ Supported | Generates via Pandas, converts to Spark DataFrame |
| Polars | ✅ Supported | Generates via Pandas, converts to Polars LazyFrame |

---

## Troubleshooting

### "Specify either row_count or end_time"

You set both. Pick one:

```yaml
scope:
  row_count: 100          # ✅ Use this
  # end_time: "..."       # ❌ Remove this
```

### "Weights must sum to 1.0"

Categorical weights are probabilities:

```yaml
values: [A, B, C]
weights: [0.5, 0.3, 0.2]   # ✅ Sums to 1.0
```

### "Column override references undefined entity"

Entity overrides must match defined entity names:

```yaml
entities:
  names: [pump_01, pump_02]

entity_overrides:
  pump_01: {...}              # ✅ Exists
  pump_03: {...}              # ❌ Not defined
```

### No data generated

Check chaos `downtime_events` — your entities might be "offline" for the entire simulation window.

---

## What's Next

- [Generators Reference](../reference/simulation_generators.md) — quick parameter lookup for all 13 generators
- [Incremental Loading](../patterns/incremental_stateful.md) — deep dive into stateful vs rolling window modes
- [Example YAML](https://github.com/henryodibi11/Odibi/blob/main/examples/simulation_example.yaml) — full working example
- [Validation](../features/quality_gates.md) — add quality checks to your simulated data
