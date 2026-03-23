# Incremental (Continuous) Simulation

**Generate new data on every pipeline run, picking up exactly where the last run left off - using the same High Water Mark (HWM) system as all other Odibi read sources.**

!!! example "Why this matters"
    A simulation that runs once is a demo. A simulation that runs continuously on a schedule, with PID controllers that remember their integral state and random walks that continue from their last position, is a data platform. Incremental mode turns simulation into a continuous data feed - indistinguishable from a real source to everything downstream. Backfill a year of history on the first run, then add a day at a time. Your dashboard never knows the difference.

---

## Overview

Incremental simulation turns a one-shot data generator into a **continuous data feed**. Instead of regenerating the same dataset every time, each pipeline run produces a fresh batch of rows that starts where the previous batch ended.

This works because simulation plugs into Odibi's standard **incremental loading** infrastructure — the same `incremental:` block you use for SQL, Delta, CSV, and every other read source. The simulator simply respects the High Water Mark to know where to start.

```
Run 1: Generates Jan 1 00:00 → 23:55  →  Saves HWM: Jan 1 23:55
Run 2: Generates Jan 2 00:00 → 23:55  →  Saves HWM: Jan 2 23:55
Run 3: Generates Jan 3 00:00 → 23:55  →  Saves HWM: Jan 3 23:55
...
```

Each run picks up exactly where the last one left off. No gaps, no overlaps, no manual bookkeeping.

!!! note "Same config as all other sources"
    The `incremental:` block is the **exact same** config used for SQL, Delta, CSV, and every other read source. See [Stateful Incremental Loading](../patterns/incremental_stateful.md) for full details on the incremental system.

---

## Configuration

To enable incremental mode, add an `incremental:` block to your read node. That's it — no special simulation-specific flags.

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
          row_count: 288    # 1 day of data per run
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
      column: timestamp     # Must match a timestamp column

  write:
    connection: my_lake
    format: delta
    table: sensor_stream
    mode: append            # Append each run's data
```

### Key configuration points

| Field | Purpose |
|-------|---------|
| `incremental.mode` | Must be `stateful` for simulation |
| `incremental.column` | Must reference a `timestamp` column defined in your simulation |
| `scope.row_count` | Controls how many rows each run generates (per entity) |
| `scope.start_time` | Only used on the **first** run; subsequent runs start from the HWM |
| `write.mode` | Use `append` to accumulate data across runs |

!!! tip "row_count vs end_time"
    For incremental simulation, prefer `row_count` over `end_time`. With `row_count`, each run always produces a fixed batch regardless of where the HWM sits. With `end_time`, the run will stop generating once the end is reached — see [Troubleshooting](#troubleshooting) for the "exhausted incremental window" error.

---

## How It Works

Here's the step-by-step lifecycle of an incremental simulation:

### First run (no existing HWM)

1. Odibi checks the state backend for an existing HWM for this node — finds nothing
2. Simulation starts at `scope.start_time` (`2026-01-01T00:00:00Z`)
3. Generates `row_count` rows per entity (288 rows × 10 entities = 2,880 total)
4. Timestamps span `00:00` to `23:55` (288 × 5min = 1 day)
5. After the write step, Odibi saves the maximum timestamp (`Jan 1 23:55`) as the HWM

### Second run (HWM exists)

1. Odibi reads the HWM: `2026-01-01T23:55:00Z`
2. Simulation starts at **HWM + one timestep** = `2026-01-02T00:00:00Z`
3. Generates another 288 rows per entity
4. Timestamps span `Jan 2 00:00` to `Jan 2 23:55`
5. Saves new HWM: `2026-01-02T23:55:00Z`

### Subsequent runs

The pattern repeats indefinitely. Each run:

1. Reads previous HWM
2. Starts at HWM + timestep
3. Generates `row_count` rows
4. Saves new HWM

```
                    ┌──────────────────┐
                    │  State Backend   │
                    │  (HWM storage)   │
                    └────────┬─────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
         ┌─────────┐   ┌─────────┐   ┌─────────┐
         │  Run 1  │   │  Run 2  │   │  Run 3  │
         │ Jan 1   │──▶│ Jan 2   │──▶│ Jan 3   │──▶ ...
         │ 288 rows│   │ 288 rows│   │ 288 rows│
         └─────────┘   └─────────┘   └─────────┘
```

---

## Stateful Function Persistence

When using `incremental.mode: stateful`, all stateful functions preserve their internal state between pipeline runs. This is the key difference between `stateful` mode and a hypothetical "restart-from-scratch" approach.

### What gets preserved

| Function | What's Preserved | Why It Matters |
|----------|-----------------|----------------|
| `prev()` | Last value per entity | Running totals, level tracking, and lag responses continue seamlessly |
| `ema()` | Last smoothed value per entity | Exponential moving average doesn't reset to the default |
| `pid()` | Integral sum and last error per entity | PID controllers maintain their accumulated correction |
| `random_walk` | Last walk value per entity | The walk continues from its last position, no jump to `start` |

### What this means in practice

**Without stateful mode:** Each run restarts all state from defaults. A tank level that ended at 63.2 m³ would jump back to the initial default (e.g., 50.0 m³) at the start of the next run.

**With stateful mode:** Run 2 picks up exactly where Run 1 left off — no discontinuities, no resets.

```
Run 1: level starts at 50.0, ends at 63.2  →  state saved
Run 2: level starts at 63.2, ends at 58.7  →  state saved
Run 3: level starts at 58.7, ...            →  continuous
```

### Example: Continuous random walk across runs

The `random_walk` generator's last value per entity is persisted. This means the walk is smooth and continuous across run boundaries — no jumps back to `start`.

```yaml
- name: continuous_pressure
  read:
    connection: null
    format: simulation
    options:
      simulation:
        scope:
          start_time: "2026-01-01T00:00:00Z"
          timestep: "5m"
          row_count: 288
          seed: 42
        entities:
          count: 5
          id_prefix: "pump_"
        columns:
          - name: pump_id
            data_type: string
            generator: {type: constant, value: "{entity_id}"}
          - name: timestamp
            data_type: timestamp
            generator: {type: timestamp}
          - name: pressure_psi
            data_type: float
            generator:
              type: random_walk
              start: 150.0
              min: 100.0
              max: 200.0
              volatility: 0.5
              mean_reversion: 0.05

    incremental:
      mode: stateful
      column: timestamp

  write:
    connection: my_lake
    format: delta
    table: pump_pressure
    mode: append
```

**Run 1:** `pump_01`'s `pressure_psi` ends at 147.3.
**Run 2:** `pump_01`'s `pressure_psi` starts at 147.3 (not 150.0), and continues its walk naturally.

Without `stateful` mode, Run 2 would reset `pump_01` to `start: 150.0` — creating an unrealistic jump in the time series.

---

## Determinism

Incremental simulation is **fully deterministic**. Same seed + same HWM = identical output.

The RNG state for each run is derived from:

1. **The base seed** (`scope.seed`)
2. **The HWM timestamp** (or `start_time` for the first run)
3. **The entity index** (each entity has its own independent RNG stream)

This means:

- **Reproducibility** — given the same config and the same HWM, any machine will produce identical data
- **Debugging** — you can replay any specific run by manually setting the HWM to the value it had before that run
- **CI/CD** — pipeline tests produce deterministic results even in incremental mode
- **Environment parity** — development, staging, and production generate the same data from the same state

!!! info "Seed advancement"
    The seed doesn't just increment by 1 per run. It's derived deterministically from the HWM timestamp, so even if you skip a run or adjust the HWM manually, the output is still reproducible.

---

## Write Modes

Incremental simulation should be paired with the right write mode:

### `mode: append` — Recommended

```yaml
write:
  mode: append
```

Each run's data is added to the existing table. This is the natural choice for incremental simulation — it mirrors how real streaming sources accumulate data.

### `mode: overwrite` — Development/Testing

```yaml
write:
  mode: overwrite
```

Replaces all existing data with the current run's output. Useful during development when you're iterating on the simulation config and don't want old data accumulating. **Not recommended for production** — you lose all historical data on each run.

### `mode: merge` — Not recommended

The `merge` write mode is designed for SQL Server upsert operations and is not a natural fit for simulation data. Simulation generates new rows with new timestamps — there's nothing to merge against.

---

## Common Patterns

### Daily data feed

Generate one day of data per run, schedule with cron (or Databricks Jobs) to run daily:

```yaml
scope:
  start_time: "2026-01-01T00:00:00Z"
  timestep: "5m"
  row_count: 288   # 288 × 5min = 1,440 min = 1 day
  seed: 42
```

Schedule the pipeline to run once per day. Each execution adds exactly one day of simulated data.

### Hourly micro-batches

Generate one hour of data per run for higher-frequency processing:

```yaml
scope:
  start_time: "2026-01-01T00:00:00Z"
  timestep: "5m"
  row_count: 12    # 12 × 5min = 60 min = 1 hour
  seed: 42
```

Schedule hourly. Each run adds one hour of data — useful for near-real-time dashboards or testing hourly aggregation patterns.

### Backfill then switch to increments

Start with a large historical dataset on the first run, then switch to smaller increments:

```yaml
scope:
  start_time: "2025-01-01T00:00:00Z"   # Start a year ago
  timestep: "5m"
  row_count: 105120                      # 365 days of 5-min data
  seed: 42
```

1. **First run:** Generates a full year of historical data (105,120 rows per entity)
2. **After first run:** Edit `row_count` to `288` (1 day)
3. **Subsequent runs:** Each adds one day, continuing from the HWM

This is a common pattern for populating dashboards with historical context before switching to daily increments.

### High-frequency sensor simulation

Simulate 1-second sensor data in 1-minute batches:

```yaml
scope:
  start_time: "2026-01-01T00:00:00Z"
  timestep: "1s"
  row_count: 60    # 60 × 1s = 1 minute
  seed: 42
```

Schedule every minute for continuous high-frequency simulation.

---

## Troubleshooting

### "Exhausted incremental window"

**Symptom:** The pipeline runs but produces zero rows.

**Cause:** You used `end_time` instead of `row_count`, and the HWM has advanced past `end_time`. There's no more data to generate.

**Solution:** Switch from `end_time` to `row_count` for ongoing incremental simulation:

```yaml
# ❌ Will exhaust after the time range is covered
scope:
  start_time: "2026-01-01T00:00:00Z"
  end_time: "2026-01-31T00:00:00Z"
  timestep: "5m"

# ✅ Generates a fixed batch on every run, indefinitely
scope:
  start_time: "2026-01-01T00:00:00Z"
  row_count: 288
  timestep: "5m"
```

### Duplicate data between runs

**Symptom:** The same timestamps appear in multiple runs' output.

**Possible causes:**

1. **Write mode is `overwrite` instead of `append`** — each run replaces previous data, so it looks like duplicates if you inspect after re-running
2. **HWM not being saved** — check that the state backend is configured and writable
3. **Manual HWM tampering** — if the HWM was reset or set to an earlier value, data will be regenerated for that period

**Solution:** Verify write mode is `append` and check the state file:

```bash
# Local development — inspect the state file
cat .odibi/state.json
```

Look for your node's entry and verify the HWM timestamp matches the last run's maximum timestamp.

### State file location

Odibi stores HWM state in different backends depending on your environment:

| Environment | Backend | Location |
|-------------|---------|----------|
| **Pandas / Local** | Local JSON | `.odibi/state.json` |
| **Spark / Databricks** | Delta table | `odibi_meta.state` table |

**Local JSON example:**

```json
{
  "sensor_stream": {
    "hwm": "2026-01-03T23:55:00Z",
    "updated_at": "2026-01-04T08:00:12Z"
  }
}
```

**To reset a node's HWM** (force a full reload from `start_time`):

- **Local:** Delete the node's entry from `.odibi/state.json`, or delete the file entirely to reset all nodes
- **Databricks:** Remove the row from the `odibi_meta.state` Delta table

!!! warning "Resetting HWM with stateful functions"
    If you reset the HWM, stateful function state (prev, ema, pid, random_walk) is also reset. This means Run N+1 after a reset will start from defaults — expect discontinuities in the generated data.

### Stateful function state not persisting

**Symptom:** `prev()` resets to its default value on each run. Tank levels jump back to 50.0 instead of continuing.

**Cause:** Incremental mode is not set to `stateful`, or the state backend is not configured.

**Solution:** Ensure your config uses `mode: stateful`:

```yaml
incremental:
  mode: stateful        # ← Must be "stateful", not just present
  column: timestamp
```

---

## Related Documentation

- **[Core Concepts](core_concepts.md)** — Scope, entities, and columns — the three building blocks
- **[Stateful Functions](stateful_functions.md)** — `prev()`, `ema()`, `pid()` — history-dependent values and their persistence
- **[Patterns & Recipes](patterns.md)** — Real-world simulation scenarios
- **[Stateful Incremental Loading](../patterns/incremental_stateful.md)** — Full documentation of Odibi's incremental loading system (shared by all read sources)
