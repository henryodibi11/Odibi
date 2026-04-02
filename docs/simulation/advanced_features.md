# Advanced Features

**Cross-entity references, entity overrides, scheduled events, and chaos engineering - the tools that turn simple simulations into realistic multi-system scenarios.**

!!! example "Why this matters"
    Real systems don't operate in isolation, don't behave identically, don't run 24/7, and don't produce clean data. These four features model exactly that: cross-entity references connect dependent systems, entity overrides create behavioral variation, scheduled events inject operational realism (maintenance, setpoint changes, shutdowns), and chaos engineering adds the data quality problems your pipeline needs to survive. If your test data assumes everything works perfectly, the first real maintenance window will break your dashboard.

These features build on the [Core Concepts](core_concepts.md) (scope, entities, columns) and [Generators](generators.md). If you haven't read those yet, start there.

---

## Cross-Entity References

Entities can reference columns from other entities at the same timestamp. This enables multi-unit/multi-system simulations where one system's output feeds into another.

### Syntax

In a `derived` expression, use `EntityName.column_name`:

```yaml
# Entity "separator_01" referencing "reactor_01"
expression: "reactor_01.outlet_flow * 0.4"
```

### How It Works

1. Odibi scans all `derived` expressions for cross-entity references (the `EntityName.column` pattern)
2. It builds a **dependency DAG** across entities — not just across columns
3. Referenced entities are generated **before** the entities that depend on them
4. Values are matched by **timestamp** (same row index)
5. `EntityProxy` objects provide access to the referenced entity's current row values

```
┌──────────────┐       ┌──────────────┐       ┌──────────────┐
│  mixer_01    │  →    │  reactor_01  │  →    │ separator_01 │
│  (upstream)  │       │  (midstream) │       │ (downstream) │
└──────────────┘       └──────────────┘       └──────────────┘
    Generated 1st          Generated 2nd          Generated 3rd
```

### Requirements

- Referenced entity must be defined using `names:` (not `count:`) so expressions can reference them by name
- Referenced column must exist in the referenced entity's column definitions
- **No circular entity dependencies** — if A references B, B cannot reference A
- Cross-entity `prev()` is **NOT** supported — you cannot do `prev('reactor_01.temp', 0)`

### Example: Production Line with Upstream/Downstream Dependencies

```yaml
options:
  simulation:
    scope:
      start_time: "2026-01-01T00:00:00Z"
      timestep: "5m"
      row_count: 288
      seed: 42
    entities:
      names: [mixer_01, reactor_01, separator_01]
    columns:
      - name: entity_id
        data_type: string
        generator: {type: constant, value: "{entity_id}"}
      - name: timestamp
        data_type: timestamp
        generator: {type: timestamp}

      # Base flow — generated independently per entity
      - name: feed_flow
        data_type: float
        generator:
          type: random_walk
          start: 100.0
          min: 80.0
          max: 120.0
          volatility: 2.0
          mean_reversion: 0.1

      # Outlet flow — derived from entity's own feed_flow
      - name: outlet_flow
        data_type: float
        generator:
          type: derived
          expression: "feed_flow * 0.95"

      # Downstream feed — reactor_01 takes mixer_01's outlet
      - name: downstream_feed
        data_type: float
        generator:
          type: derived
          expression: "mixer_01.outlet_flow * 0.9"
        entity_overrides:
          mixer_01:
            type: derived
            expression: "0"
          reactor_01:
            type: derived
            expression: "mixer_01.outlet_flow * 0.9"
          separator_01:
            type: derived
            expression: "reactor_01.outlet_flow * 0.4"
```

In this example:

- `mixer_01` has no upstream — its `downstream_feed` is `0`
- `reactor_01` reads `mixer_01.outlet_flow` — Odibi generates `mixer_01` first
- `separator_01` reads `reactor_01.outlet_flow` — Odibi generates `reactor_01` second, `separator_01` third

!!! tip "Named entities required"
    When using cross-entity references, always use `names:` instead of `count:` + `id_prefix:`. Auto-generated names like `sensor_01` work, but explicit names make expressions readable and maintainable.

---

## Entity Overrides

Entity overrides allow specific entities to use different generator configurations for the same column. Defined per-column via the `entity_overrides` key.

### Syntax

```yaml
- name: temperature
  data_type: float
  generator:
    type: range
    min: 20.0
    max: 30.0
  entity_overrides:
    hot_zone_sensor:
      type: range
      min: 40.0
      max: 60.0
    cold_storage_sensor:
      type: range
      min: -10.0
      max: 5.0
```

### Rules

- Override entity names **must match** defined entity names — validated at config time
- Override replaces the **entire generator** for that entity+column combination
- You can override to a completely different generator type (e.g., `range` → `random_walk`)
- `null_rate` is applied **after** the override generator runs

### Use Cases

| Scenario | What You Override |
|----------|-------------------|
| Different operating ranges | Equipment running at different capacities |
| Faulty sensors | Different noise profiles, higher variance |
| Regional variation | IoT sensors in different climates/environments |
| Product types | Different products on the same production line |

### Example: Manufacturing Line with Three Machines

One machine runs hot due to heavy-duty operation, one is a new install running cooler, and the rest use the default range.

```yaml
options:
  simulation:
    scope:
      start_time: "2026-01-01T00:00:00Z"
      timestep: "1m"
      row_count: 480
      seed: 42
    entities:
      names: [machine_A, machine_B, machine_C]
    columns:
      - name: machine_id
        data_type: string
        generator: {type: constant, value: "{entity_id}"}
      - name: timestamp
        data_type: timestamp
        generator: {type: timestamp}

      - name: temperature_c
        data_type: float
        generator:
          type: random_walk
          start: 65.0
          min: 55.0
          max: 75.0
          volatility: 0.5
          mean_reversion: 0.05
        entity_overrides:
          machine_A:                       # Heavy-duty — runs hot
            type: random_walk
            start: 90.0
            min: 80.0
            max: 105.0
            volatility: 1.0
            mean_reversion: 0.03
          machine_C:                       # New install — runs cool
            type: random_walk
            start: 45.0
            min: 38.0
            max: 55.0
            volatility: 0.3
            mean_reversion: 0.08

      - name: vibration_mm_s
        data_type: float
        generator:
          type: range
          min: 0.5
          max: 3.0
        entity_overrides:
          machine_A:                       # Heavy-duty — more vibration
            type: range
            min: 2.0
            max: 8.0

      - name: status
        data_type: string
        generator:
          type: categorical
          values: [Running, Idle, Maintenance]
          weights: [0.85, 0.10, 0.05]
        entity_overrides:
          machine_A:                       # Older machine — more maintenance
            type: categorical
            values: [Running, Idle, Maintenance]
            weights: [0.70, 0.15, 0.15]
```

**Expected output:**

| machine_id | temperature_c | vibration_mm_s | Notes |
|------------|---------------|----------------|-------|
| machine_A | 80–105°C | 2.0–8.0 | Hot, high vibration, frequent maintenance |
| machine_B | 55–75°C | 0.5–3.0 | Default ranges |
| machine_C | 38–55°C | 0.5–3.0 | Cool, low vibration |

---

## Scheduled Events

Scheduled events modify simulation behavior at specific times. They let you inject maintenance shutdowns, setpoint changes, and capacity restrictions into your generated data.

!!! info "Which event type should I use?"

    | I want to... | Use this | Example |
    |---|---|---|
    | Set a column to an exact value during a time window | `forced_value` | Shut down a pump: set `power_kw` to 0 during maintenance |
    | Change a column's value permanently from a point in time | `setpoint_change` | Increase reactor temperature target from 350 to 370 at 13:00 |
    | Temporarily change a generator's operating range | `parameter_override` | Cap all power output at 180 kW during grid curtailment |

    **forced_value vs parameter_override - what's the difference?**

    - `forced_value` = "this column is EXACTLY this value." No randomness, no generation - just the value you specify. Every row during the window gets the same value.
    - `parameter_override` = "change how the generator works." The generator still runs, but with a modified parameter (like a new max value). You still get random variation, just within different bounds.

    Use `forced_value` for shutdowns and hard overrides. Use `parameter_override` for "degraded mode" where the process still runs but differently.

Three event types are available:

### forced_value

Force a column to a specific value during a time window.

```yaml
scheduled_events:
  - type: forced_value
    entity: pump_01            # null = all entities
    column: power_kw
    value: 0.0
    start_time: "2026-01-01T14:00:00Z"
    end_time: "2026-01-01T18:00:00Z"
```

**Use cases:** Maintenance shutdowns, safety trips, power outages, planned downtime.

### setpoint_change

Change a column's value permanently from a point in time.

```yaml
scheduled_events:
  - type: setpoint_change
    entity: reactor_01
    column: temp_setpoint_c
    value: 370.0
    start_time: "2026-01-01T12:00:00Z"
    # No end_time = permanent change
```

**Use cases:** Process optimization changes, recipe changes, shift transitions.

### parameter_override

Override a generator parameter during a time window.

```yaml
scheduled_events:
  - type: parameter_override
    entity: null               # All entities
    column: max_output_pct
    value: 80.0
    start_time: "2026-01-01T16:00:00Z"
    end_time: "2026-01-01T19:00:00Z"
```

**Use cases:** Grid curtailment, capacity restrictions, degraded mode operation.

### Event Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `type` | string | Yes | — | `forced_value`, `setpoint_change`, or `parameter_override` |
| `entity` | string | No | `null` (all) | Entity name (must match a name from `entities.names`) or `null` to apply to all entities |
| `column` | string | Yes | — | Column to modify |
| `value` | any | Yes | — | Value to apply |
| `start_time` | string | Conditional | — | ISO8601 start time in Zulu format (e.g., `"2026-01-15T06:00:00Z"`). Required unless `condition` is set |
| `end_time` | string | No | `null` (permanent) | ISO8601 end time in Zulu format (e.g., `"2026-01-15T18:00:00Z"`); omit for permanent changes. Cannot combine with `duration` |
| `priority` | int | No | `0` | For overlapping events — higher priority is applied last (wins) |
| `recurrence` | string | No | — | Repeat interval. Format: number + unit where unit is `s`/`m`/`h`/`d` (e.g., `"30d"`, `"7d"`, `"4h"`). Requires `start_time` |
| `duration` | string | No | — | Duration of each occurrence. Format: number + unit where unit is `s`/`m`/`h`/`d` (e.g., `"4h"`, `"30m"`). Alternative to `end_time` |
| `jitter` | string | No | — | Random offset ± applied to each recurrence start. Format: number + unit where unit is `s`/`m`/`h`/`d` (e.g., `"2d"`, `"6h"`). Requires `recurrence` |
| `max_occurrences` | int | No | — | Stop repeating after N occurrences. Requires `recurrence` |
| `condition` | string | No | — | Sandboxed Python expression evaluated against current row columns. Supports comparison operators, compound logic (`and`, `or`, `not`), and safe functions (`abs`, `round`, `min`, `max`). E.g., `"efficiency < 70 and pressure > 50"` |
| `cooldown` | string | No | — | Minimum gap between condition triggers. Format: number + unit where unit is `s`/`m`/`h`/`d` (e.g., `"7d"`, `"12h"`). Requires `condition` |
| `sustain` | string | No | — | Condition must be continuously true for this duration before triggering. Format: number + unit where unit is `s`/`m`/`h`/`d` (e.g., `"24h"`, `"30m"`). Requires `condition` |
| `transition` | string | No | `"instant"` | How value is applied: `"instant"` (jump) or `"ramp"` (linear interpolation over duration) |

### Recurring Events

Instead of manually specifying start/end times for repeated events, use `recurrence` and `duration`:

```yaml
scheduled_events:
  # CIP cleaning every 15 days, 4 hours each
  - type: parameter_override
    entity: HX_01
    column: actual_efficiency_pct
    value: 94.0
    start_time: "2026-01-15T08:00:00Z"
    recurrence: "15d"
    duration: "4h"
    max_occurrences: 12
    jitter: "2d"          # ±2 day random scheduling variation
```

The event repeats every `recurrence` interval from `start_time`. Each occurrence lasts `duration`. Add `jitter` for realistic scheduling variation (deterministic per seed). Use `max_occurrences` to cap the number of repeats.

### Condition-Based Events

Trigger events based on current data values instead of fixed times:

```yaml
scheduled_events:
  # Override when efficiency drops below 70%
  - type: parameter_override
    column: flow_rate_lpm
    value: 50.0
    condition: "actual_efficiency_pct < 70"
    cooldown: "7d"         # Don't retrigger within 7 days
    sustain: "24h"         # Must be true for 24h before triggering
    duration: "4h"         # Hold override for 4 hours after triggering
```

Condition expressions use the same safe namespace as derived expressions (`abs`, `round`, `min`, `max`, etc.) and can reference any column in the current row. Compound logic works: `"a < 70 and b > 50"`.

- **`sustain`**: Condition must be continuously true for this duration before the event triggers. Prevents spurious triggers from momentary spikes.
- **`cooldown`**: Minimum gap between triggers. Prevents rapid re-triggering.
- **`duration`**: Once triggered, the override holds for this duration even if the condition becomes false. Without `duration`, the event stays active only while the condition is true.

### Ramp Transitions

Gradually transition to the target value instead of jumping instantly:

```yaml
scheduled_events:
  - type: setpoint_change
    entity: Reactor_01
    column: temp_setpoint_c
    value: 370.0
    start_time: "2026-03-11T12:00:00Z"
    duration: "2h"
    transition: ramp       # Linear interpolation over 2 hours
```

With `transition: ramp`, the value changes linearly from the current value to the target over the duration window. At the midpoint of a 2-hour ramp from 350 to 370, the value is 360. After the ramp completes, the target value holds.

Ramp transitions work with `parameter_override` and `setpoint_change` event types. Requires `duration` or both `start_time` and `end_time` to define the ramp window.

!!! tip "How priority works with overlapping events"

    When two events target the same entity and column at the same time, `priority` decides which one wins. Higher priority is applied last, so it overwrites the lower-priority event.

    **Example:** At 14:00, pump_01 has two events:

    - Event A (priority 0): `forced_value`, power_kw = 0 (maintenance shutdown)
    - Event B (priority 1): `parameter_override`, power_kw = 180 (grid curtailment)

    Result: Event A is applied first (power = 0), then Event B overwrites it (power = 180). The pump runs at reduced power instead of shutting down.

    If you flip the priorities (A=1, B=0), the pump shuts down because the shutdown event wins.

    **Default priority is 0.** If you don't set priorities and events overlap, the behavior is undefined - always set explicit priorities for overlapping events.

### Example: 10-Hour Plant Operation

A realistic day shift with normal operations, a maintenance shutdown, startup recovery, and an optimization change.

```yaml
options:
  simulation:
    scope:
      start_time: "2026-01-01T06:00:00Z"    # Shift starts at 06:00
      timestep: "5m"
      end_time: "2026-01-01T16:00:00Z"       # Shift ends at 16:00
      seed: 42
    entities:
      names: [pump_01, pump_02, reactor_01]
    columns:
      - name: entity_id
        data_type: string
        generator: {type: constant, value: "{entity_id}"}
      - name: timestamp
        data_type: timestamp
        generator: {type: timestamp}
      - name: power_kw
        data_type: float
        generator:
          type: random_walk
          start: 250.0
          min: 200.0
          max: 300.0
          volatility: 5.0
          mean_reversion: 0.1
      - name: temp_setpoint_c
        data_type: float
        generator: {type: constant, value: 350.0}
      - name: flow_rate_lpm
        data_type: float
        generator:
          type: random_walk
          start: 500.0
          min: 400.0
          max: 600.0
          volatility: 10.0
          mean_reversion: 0.05

    scheduled_events:
      # 1. Planned maintenance — pump_01 shuts down 09:00–11:00
      - type: forced_value
        entity: pump_01
        column: power_kw
        value: 0.0
        start_time: "2026-01-01T09:00:00Z"
        end_time: "2026-01-01T11:00:00Z"

      - type: forced_value
        entity: pump_01
        column: flow_rate_lpm
        value: 0.0
        start_time: "2026-01-01T09:00:00Z"
        end_time: "2026-01-01T11:00:00Z"

      # 2. Startup recovery — pump_01 ramps up at reduced capacity 11:00–12:00
      - type: parameter_override
        entity: pump_01
        column: power_kw
        value: 150.0
        start_time: "2026-01-01T11:00:00Z"
        end_time: "2026-01-01T12:00:00Z"

      # 3. Process optimization — reactor setpoint increases at 13:00 (permanent)
      - type: setpoint_change
        entity: reactor_01
        column: temp_setpoint_c
        value: 370.0
        start_time: "2026-01-01T13:00:00Z"

      # 4. Grid curtailment — all entities reduce power 14:00–15:00
      - type: parameter_override
        entity: null
        column: power_kw
        value: 180.0
        start_time: "2026-01-01T14:00:00Z"
        end_time: "2026-01-01T15:00:00Z"
```

**Timeline:**

```
06:00  ─── Normal operations ──────────────────────────────────
09:00  ─── pump_01 shutdown (maintenance) ─────────────────────
11:00  ─── pump_01 startup at reduced power ───────────────────
12:00  ─── pump_01 back to normal ─────────────────────────────
13:00  ─── Reactor setpoint change 350→370°C (permanent) ─────
14:00  ─── Grid curtailment: all power capped at 180 kW ──────
15:00  ─── Normal operations resume ───────────────────────────
16:00  ─── Shift ends ─────────────────────────────────────────
```

!!! note "Overlapping events"
    When events overlap, `priority` determines which one wins. If two events target the same entity+column at the same time, the one with the higher priority is applied last (overwriting the lower-priority event).

---

## Chaos Engineering

Chaos engineering adds realistic data imperfections to your simulation. Chaos is applied **after** all generation (including derived columns and cross-entity references), so it simulates real-world data quality issues without affecting the generation logic.

### Outliers

Inject random value spikes into numeric columns.

```yaml
chaos:
  outlier_rate: 0.01         # 1% of numeric values become outliers
  outlier_factor: 3.0        # Outlier values = normal value × this factor
```

- Affects **all numeric columns** (`int` and `float`)
- Simulates sensor spikes, measurement errors, transmission corruption
- A temperature reading of 30°C becomes 90°C at `outlier_factor: 3.0`

### Duplicates

Inject duplicate rows to simulate network retransmits or duplicate event delivery.

```yaml
chaos:
  duplicate_rate: 0.005      # 0.5% of rows are duplicated
```

- Duplicated rows are exact copies (same timestamp, same values)
- Simulates network retransmits, message queue redelivery, double-writes

### Downtime Events

Remove rows entirely during specified time windows.

```yaml
chaos:
  downtime_events:
    - entity: sensor_03
      start_time: "2026-01-01T10:00:00Z"
      end_time: "2026-01-01T12:00:00Z"
    - entity: null                       # All entities
      start_time: "2026-01-01T22:00:00Z"
      end_time: "2026-01-01T23:00:00Z"
```

- Rows during the downtime window are **removed entirely** — not nulled, not zeroed, gone
- `entity: null` applies to all entities (simulates plant-wide network outage)
- Simulates network outages, equipment offline, data collection failures

### Per-Column Null Injection

Inject NULLs into specific columns at a configurable rate.

```yaml
- name: humidity
  data_type: float
  generator: {type: range, min: 30, max: 70}
  null_rate: 0.05            # 5% of values will be NULL
```

- Configured **per column**, not globally — different columns can have different null rates
- Simulates sensor dropouts, missing readings, intermittent connectivity
- Applied independently of other chaos features

### Complete Chaos Example

A realistic IoT monitoring scenario combining all four chaos types:

```yaml
options:
  simulation:
    scope:
      start_time: "2026-01-01T00:00:00Z"
      timestep: "1m"
      row_count: 1440             # 24 hours of minute-level data
      seed: 42
    entities:
      names: [sensor_01, sensor_02, sensor_03, sensor_04, sensor_05]
    columns:
      - name: sensor_id
        data_type: string
        generator: {type: constant, value: "{entity_id}"}
      - name: timestamp
        data_type: timestamp
        generator: {type: timestamp}
      - name: temperature_c
        data_type: float
        generator:
          type: random_walk
          start: 22.0
          min: 18.0
          max: 30.0
          volatility: 0.3
          mean_reversion: 0.05
      - name: humidity_pct
        data_type: float
        generator: {type: range, min: 30.0, max: 70.0}
        null_rate: 0.05              # Humidity sensor drops out 5% of the time
      - name: pressure_hpa
        data_type: float
        generator:
          type: random_walk
          start: 1013.0
          min: 990.0
          max: 1040.0
          volatility: 0.5
          mean_reversion: 0.02
        null_rate: 0.02              # Pressure sensor more reliable — 2% dropout
      - name: battery_pct
        data_type: float
        generator:
          type: derived
          expression: "max(0, 100 - (_row_index * 0.07))"

    chaos:
      # Sensor spikes — 1% of numeric values become outliers
      outlier_rate: 0.01
      outlier_factor: 3.0

      # Network retransmits — 0.5% of rows duplicated
      duplicate_rate: 0.005

      # Equipment offline — sensor_03 loses connectivity for 2 hours
      # Plant-wide outage at 22:00–23:00
      downtime_events:
        - entity: sensor_03
          start_time: "2026-01-01T10:00:00Z"
          end_time: "2026-01-01T12:00:00Z"
        - entity: null
          start_time: "2026-01-01T22:00:00Z"
          end_time: "2026-01-01T23:00:00Z"
```

**What this produces (5 sensors × 1,440 rows = 7,200 base rows):**

| Chaos Feature | Expected Impact |
|---------------|-----------------|
| Outliers (1%) | ~72 numeric values spiked to 3× normal |
| Duplicates (0.5%) | ~36 duplicate rows scattered across all sensors |
| sensor_03 downtime | ~120 rows removed (2 hours × 1 row/minute) |
| Plant-wide outage | ~300 rows removed (5 sensors × 60 minutes) |
| Humidity nulls (5%) | ~360 NULL values in `humidity_pct` |
| Pressure nulls (2%) | ~144 NULL values in `pressure_hpa` |

### Best Practices

- **Start with low rates** (0.5–2%) — real production data typically isn't that messy. Higher rates make the data unrealistically noisy.
- **Use chaos to test your pipeline's validation and quarantine logic** — if your `range` validation doesn't catch outliers at 3×, your thresholds may be too loose.
- **Downtime events pair well with incremental mode** — they create realistic gaps that test your pipeline's ability to handle missing time windows across runs.
- **Layer chaos types for realism** — real systems experience multiple failure modes simultaneously, not just one at a time.
- **Check your math** — `outlier_rate × total_numeric_cells` tells you how many spikes to expect. Run with a fixed seed and validate the output before building pipeline tests around it.

---

## What's Next

- **[Getting Started](getting_started.md)** — build your first simulation from scratch
- **[Core Concepts](core_concepts.md)** — scope, entities, and columns in depth
- **[Generators Reference](generators.md)** — all 13 generator types with parameters
- **[Stateful Functions](stateful_functions.md)** — `prev()`, `ema()`, `pid()`, `delay()` for history-dependent values
