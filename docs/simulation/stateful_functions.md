# Stateful Functions Reference

**Complete reference for `prev()`, `ema()`, `pid()`, and `delay()` - the four stateful functions available in `derived` generator expressions.**

!!! example "Why this matters"
    Four functions turn a row-by-row data generator into a dynamic system simulator. `prev()` gives you memory (tanks fill up, inventories accumulate). `ema()` gives you smoothing (noisy sensors become clean signals). `pid()` gives you feedback control (outputs adjust to hit targets). `delay()` gives you transport lag (pipeline delays, conveyor times). Without these, you get snapshots. With them, you get time series that behave like real processes - and they persist state across pipeline runs.

---

## Overview

Stateful functions remember values between rows for each entity independently. They are used inside `derived` column expressions and enable:

- **Integration / accumulation** — running totals, level tracking, inventory counts
- **Smoothing** — filtering noisy sensor readings into clean signals
- **Feedback control** — PID controllers that adjust outputs to hit targets

**Key behavior:**

- State is tracked **per entity**. If you have 5 sensors, each sensor has its own independent state.
- State flows **forward through rows** — each row can see the previous row's value, but not future rows.
- State can be **persisted between pipeline runs** with `incremental.mode: stateful` (see [Incremental State Persistence](#incremental-state-persistence)).

All stateful functions are used inside `derived` generator expressions:

```yaml
- name: my_column
  data_type: float
  generator:
    type: derived
    expression: "prev('my_column', 0) + new_value"
```

---

## prev(column, default) — Previous Row Value

**Full signature:** `prev('column_name', default_value)`

**What it does:** Returns the value of `column_name` from the previous row for the same entity. On the first row, returns `default_value`.

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `column` | string | Yes | Column name to look back at (must be quoted) |
| `default` | any | Yes | Value to use for the first row |

### Use Cases (General)

- **Running totals:** `prev('total', 0) + new_value`
- **Level tracking:** `prev('level', 100) + inflow - outflow`
- **First-order response:** `prev('temp', 25) + 0.1 * (target - prev('temp', 25))`
- **Cumulative counts:** `prev('count', 0) + 1`

### Use Cases (Operations / Manufacturing)

- **Tank level integration** — track fill level from inflow and outflow rates
- **Inventory tracking** — running stock balance from receipts and issues
- **Equipment runtime hours** — accumulate operating time per asset
- **Batch progress tracking** — cumulative weight or volume processed

### Example 1: Simple Running Total

Accumulate a counter that increments by 1 each row:

```yaml
columns:
  - name: timestamp
    data_type: timestamp
    generator:
      type: timestamp

  - name: row_count
    data_type: int
    generator:
      type: derived
      expression: "prev('row_count', 0) + 1"
```

**Row-by-row:** `1, 2, 3, 4, 5, ...` — each row adds 1 to the previous value.

### Example 2: Tank Level with Inflow / Outflow

Track a tank's fill level as material flows in and drains out:

```yaml
columns:
  - name: timestamp
    data_type: timestamp
    generator:
      type: timestamp

  # Inflow varies between 2-8 m³/hr
  - name: inflow_m3_hr
    data_type: float
    generator:
      type: random_walk
      start: 5.0
      min: 2.0
      max: 8.0

  # Outflow varies between 3-7 m³/hr
  - name: outflow_m3_hr
    data_type: float
    generator:
      type: random_walk
      start: 5.0
      min: 3.0
      max: 7.0

  # Tank level (m³) — integrates inflow minus outflow
  # Timestep = 5 min = 5/60 hours
  - name: level_m3
    data_type: float
    generator:
      type: derived
      expression: "max(0, min(100, prev('level_m3', 50.0) + (inflow_m3_hr - outflow_m3_hr) * (5/60)))"
```

**How it works:**

- `prev('level_m3', 50.0)` — starts at 50 m³, then uses last row's level
- `(inflow - outflow) * (5/60)` — net volume change over 5 minutes
- `max(0, min(100, ...))` — clamps to physical tank limits (0–100 m³)

### Example 3: First-Order Lag Response

Simulate a system that gradually responds to a changing input (e.g., a room heating up toward a thermostat setting):

```yaml
columns:
  - name: timestamp
    data_type: timestamp
    generator:
      type: timestamp

  # Target temperature wanders between 20-30°C
  - name: target_temp_c
    data_type: float
    generator:
      type: random_walk
      start: 25.0
      min: 20.0
      max: 30.0

  # Actual temperature chases the target with a lag
  # Gain = dt/tau = 60/300 = 0.2 (20% correction per timestep)
  - name: actual_temp_c
    data_type: float
    generator:
      type: derived
      expression: "prev('actual_temp_c', 25.0) + 0.2 * (target_temp_c - prev('actual_temp_c', 25.0))"
```

**How it works:**

- Each row, the actual temperature moves 20% of the way toward the target
- A gain of `0.2` means `dt/τ = 60s / 300s` — a 5-minute time constant with 1-minute steps
- Lower gain → slower response; higher gain → faster response

---

## ema(column, alpha, default) — Exponential Moving Average

**Full signature:** `ema('column_name', alpha, default_value)`

**What it does:** Applies exponential smoothing to a noisy signal. Each output value blends the current reading with the previous smoothed value:

```
smoothed = alpha × current + (1 - alpha) × previous_smoothed
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `column` | string | Yes | Column to smooth (must be quoted) |
| `alpha` | float | Yes | Smoothing factor (0.0–1.0) |
| `default` | any | Yes | Initial smoothed value |

### Alpha Guide

| Alpha | Smoothing | History Weight | Best For |
|-------|-----------|----------------|----------|
| 0.1 | Heavy | 90% history | Slow sensors, long-term averaging |
| 0.3 | Moderate-heavy | 70% history | General-purpose filtering |
| 0.5 | Moderate | 50/50 | Balanced noise reduction |
| 0.7 | Moderate-light | 30% history | Responsive filtering |
| 0.9 | Light | 10% history | Fast response, minimal smoothing |

**Rule of thumb:** Lower alpha = smoother output but slower to react. Higher alpha = noisier output but faster to react.

!!! info "What alpha actually looks like in the output"

    Imagine a raw temperature signal bouncing between 70 and 80 degrees C, averaging around 75:

    - **alpha = 0.1:** The smoothed signal barely moves. It drifts slowly between 73-77, ignoring most of the bouncing. Great for filtering out noise, but it reacts very slowly to real changes.
    - **alpha = 0.3:** The smoothed signal follows the general trend but flattens out the spikes. Moves between 72-78. A good default for most sensor filtering.
    - **alpha = 0.5:** Half current reading, half history. The smoothed signal still tracks the raw signal but with softer edges. You can see the pattern but not the noise.
    - **alpha = 0.9:** Almost no smoothing. The output looks nearly identical to the raw signal, just slightly less jagged. Only useful when you want minimal filtering.

    **Bottom line:** Start at 0.2-0.3 for most sensor filtering. Only go higher if you need fast reaction to real changes and can tolerate more noise in the output.

### Example 1: Smoothing Noisy Temperature Readings

A thermocouple reports noisy readings. Use EMA to get a clean signal:

```yaml
columns:
  - name: timestamp
    data_type: timestamp
    generator:
      type: timestamp

  # Raw thermocouple reading (noisy)
  - name: raw_temp_c
    data_type: float
    generator:
      type: random_walk
      start: 75.0
      min: 60.0
      max: 90.0

  # Smoothed temperature — filters out noise
  - name: smooth_temp_c
    data_type: float
    generator:
      type: derived
      expression: "ema('raw_temp_c', 0.2, 75.0)"
```

**Result:** `smooth_temp_c` tracks the trend of `raw_temp_c` but ignores short-term noise spikes. With `alpha=0.2`, each output is 20% current reading + 80% previous smoothed value.

### Example 2: Filtering Production Rate Measurements

A production line's output rate (units/hour) fluctuates due to measurement noise:

```yaml
columns:
  - name: timestamp
    data_type: timestamp
    generator:
      type: timestamp

  # Raw production rate (noisy)
  - name: raw_rate_uph
    data_type: float
    generator:
      type: random_walk
      start: 500.0
      min: 400.0
      max: 600.0

  # Smoothed rate for reporting
  - name: smooth_rate_uph
    data_type: float
    generator:
      type: derived
      expression: "ema('raw_rate_uph', 0.3, 500.0)"

  # Deviation from smoothed rate (quality signal)
  - name: rate_deviation
    data_type: float
    generator:
      type: derived
      expression: "raw_rate_uph - smooth_rate_uph"
```

**Bonus:** The `rate_deviation` column shows how far the raw reading is from the smoothed trend — useful for detecting real process changes vs. noise.

---

## pid(pv, sp, Kp, Ki, Kd, dt, ...) — PID Controller

**Full signature:**

```
pid(pv=column, sp=column_or_value, Kp=float, Ki=float, Kd=float, dt=float,
    output_min=float, output_max=float, anti_windup=bool)
```

**What it does:** Calculates a control output that drives a process variable (`pv`) toward a setpoint (`sp`). It uses three terms based on the error (`sp - pv`):

- **P (Proportional):** Reacts to *current* error — bigger error → bigger correction
- **I (Integral):** Reacts to *accumulated* error — eliminates persistent offset over time
- **D (Derivative):** Reacts to *rate of change* of error — dampens oscillations

The controller maintains integral and derivative state per entity across rows.

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `pv` | column ref | Yes | — | Process variable (the measurement) |
| `sp` | column ref or float | Yes | — | Setpoint (the target value) |
| `Kp` | float | Yes | — | Proportional gain |
| `Ki` | float | No | 0.0 | Integral gain |
| `Kd` | float | No | 0.0 | Derivative gain |
| `dt` | float | Yes | — | Timestep in seconds |
| `output_min` | float | No | 0 | Minimum output value |
| `output_max` | float | No | 100 | Maximum output value |
| `anti_windup` | bool | No | True | Prevent integral windup when output saturates |

!!! warning "The #2 PID mistake: wrong sign (direct vs reverse acting)"

    Odibi's `pid()` calculates error as `setpoint - process_variable`. This means:

    - When the process variable is **above** setpoint, error is **negative**
    - When the process variable is **below** setpoint, error is **positive**

    For **cooling controllers** (more output = lower temperature), you need **negative gains**. Otherwise the PID output goes to zero exactly when you need maximum cooling:

    | Controller type | When PV > SP, I need... | Gains should be... |
    |-----------------|-------------------------|-------------------|
    | Cooling (valve, fan, vent) | More output (more cooling) | **Negative** Kp, Ki, Kd |
    | Heating (heater, steam) | Less output (less heating) | **Positive** Kp, Ki, Kd |
    | Pump draining a tank | More output (more pumping) | **Negative** Kp, Ki, Kd |
    | Fill valve into a tank | Less output (less filling) | **Positive** Kp, Ki, Kd |

    **The rule:** If more output should DECREASE the process variable, use negative gains. If more output should INCREASE the process variable, use positive gains.

!!! warning "The #1 PID mistake: wrong dt"

    The `dt` parameter must match your simulation's `scope.timestep` **in seconds**:

    | scope.timestep | dt value |
    |----------------|----------|
    | `1m` | `60` |
    | `5m` | `300` |
    | `1h` | `3600` |
    | `10s` | `10` |

    If your timestep is `5m` but you set `dt=60`, the PID thinks each step is 1 minute and will over-correct by 5x. If you set `dt=5`, it thinks each step is 5 seconds and will barely respond. Always convert your timestep to seconds.

!!! tip "New to PID? Start here"

    Think of a PID controller as a thermostat with three knobs. The P/I/D terms above describe the math - here's what they mean in practice:

    - **Kp too low?** The process barely responds. Temperature sits above setpoint and nothing happens.
    - **Kp too high?** The process overshoots and oscillates - valve slams open, then shut, then open.
    - **Ki too low (or zero)?** The process gets close to setpoint but never quite reaches it. There's always a small offset.
    - **Ki too high?** The process overshoots badly and takes a long time to settle. The integral "winds up" from accumulated error.
    - **Kd useful?** Only when you see oscillation that P and I alone can't fix. Skip it for noisy measurements - it amplifies noise.

    **Start with this recipe:**

    1. Set `Ki=0, Kd=0`. Increase `Kp` until the process responds without wild oscillation.
    2. Add a small `Ki` (try `Kp / 10`). This eliminates any remaining offset from setpoint.
    3. Only add `Kd` if the process oscillates. Try `Kp / 4`. Skip it entirely for noisy measurements.

    Most simulations work fine with just P and I (set `Kd=0`).

### Practical Tuning Guide

You don't need control theory to tune a PID. Follow these steps:

**Step 1 — Start with P-only:**

Set `Kp` to a small value (e.g., 1.0), `Ki=0`, `Kd=0`. Run the simulation.

- If the output barely moves → increase `Kp`
- If the output oscillates wildly → decrease `Kp`
- If it settles near (but not exactly at) the target → that's normal, move to Step 2

**Step 2 — Add I to eliminate offset:**

Set `Ki = Kp / 10` (start conservative). The integral term slowly nudges the output until the error is zero.

- If it oscillates more → reduce `Ki`
- If the offset goes away but takes too long → increase `Ki`

**Step 3 — Add D (optional) to dampen oscillations:**

Set `Kd = Kp / 4`. The derivative term resists rapid changes, smoothing out oscillations.

- Only needed if Steps 1-2 still oscillate
- Skip D for noisy measurements (it amplifies noise)

### Example 1: Temperature Control with Cooling Valve

A cooling valve controls the temperature of a vessel. The PID adjusts valve position (0–100%) to hit the temperature setpoint:

```yaml
columns:
  - name: timestamp
    data_type: timestamp
    generator:
      type: timestamp

  # Temperature setpoint (target)
  - name: temp_setpoint_c
    data_type: float
    generator:
      type: constant
      value: 75.0

  # Vessel temperature — responds to cooling valve (first-order lag)
  # Process gain: valve% → temperature change
  - name: vessel_temp_c
    data_type: float
    generator:
      type: derived
      expression: "prev('vessel_temp_c', 80.0) + 0.05 * (90.0 - prev('cooling_valve_pct', 50) * 0.3 - prev('vessel_temp_c', 80.0))"
      # 0.05 = dt/tau, 90.0 = heat source, 0.3 = cooling gain per % valve

  # PID controller output → cooling valve position
  - name: cooling_valve_pct
    data_type: float
    generator:
      type: derived
      expression: "pid(pv=vessel_temp_c, sp=temp_setpoint_c, Kp=-2.0, Ki=-0.1, Kd=-0.5, dt=60, output_min=0, output_max=100)"
```

**What happens:**

1. Vessel starts at 80°C (above 75°C setpoint)
2. PID detects temp above setpoint - increases cooling output (reverse-acting)
3. Temperature drops toward 75°C
4. Integral term eliminates any remaining offset
5. Derivative term prevents overshoot

### Example 2: Level Control with Pump Speed

A pump drains a tank to maintain a target level. The PID adjusts pump speed (0–100%) based on the current level:

```yaml
columns:
  - name: timestamp
    data_type: timestamp
    generator:
      type: timestamp

  # Level setpoint (target)
  - name: level_setpoint_m3
    data_type: float
    generator:
      type: constant
      value: 50.0

  # Inflow disturbance — varies unpredictably
  - name: inflow_m3_hr
    data_type: float
    generator:
      type: random_walk
      start: 10.0
      min: 5.0
      max: 15.0

  # Tank level — integrates inflow minus pump outflow
  # Pump outflow = pump_speed_pct/100 * max_pump_rate(20 m³/hr)
  - name: tank_level_m3
    data_type: float
    generator:
      type: derived
      expression: "max(0, min(100, prev('tank_level_m3', 50.0) + (inflow_m3_hr - prev('pump_speed_pct', 50) / 100 * 20) * (5/60)))"

  # PID controller → pump speed
  - name: pump_speed_pct
    data_type: float
    generator:
      type: derived
      expression: "pid(pv=tank_level_m3, sp=level_setpoint_m3, Kp=-3.0, Ki=-0.2, Kd=0.0, dt=300, output_min=0, output_max=100)"
```

**Why Kd=0 here:** Level measurements are often noisy, and derivative action amplifies noise. For level control, PI (without D) is the standard approach.

---

## delay(column, steps, default) — Transport Delay

**Full signature:** `delay('column_name', steps, default_value)`

**What it does:** Returns the value of `column_name` from `steps` rows ago for the same entity. Maintains an internal ring buffer of past values. During the first `steps` rows (before the buffer is full), returns `default_value`.

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `column` | string | Yes | Column name to delay (must be quoted) |
| `steps` | int | Yes | Number of timesteps to look back (must be ≥ 1) |
| `default` | any | Yes | Value to return during the initial fill period |

### Use Cases (General)

- **Pipeline transport:** `delay('pump_flow', 10, 50.0)` — flow arrives at the other end 10 steps later
- **Conveyor belt:** `delay('weight_at_entry', 20, 0.0)` — weight at exit = weight at entry, 20 steps ago
- **Batch queue:** `delay('job_submitted', 5, 0)` — result available 5 steps after submission
- **Measurement lag:** `delay('lab_sample', 30, 0.0)` — lab result arrives 30 minutes after sample taken

### Use Cases (Operations / Manufacturing)

- **Pipeline transport delay** — fluid takes time to travel through a pipe
- **Conveyor belt** — material placed at one end arrives at the other end after a fixed time
- **Sample analysis turnaround** — take a sample now, get results N minutes later
- **Oven/kiln transit time** — product enters, exits after a fixed residence time
- **Paint drying / curing** — apply coating, inspect after fixed drying time

### Steps Guide

| steps | At 1-min timestep | At 5-min timestep | Feels like |
|-------|-------------------|-------------------|------------|
| 1 | 1 minute | 5 minutes | Barely noticeable lag |
| 5 | 5 minutes | 25 minutes | Short pipeline, nearby conveyor |
| 10 | 10 minutes | 50 minutes | Medium pipeline, process queue |
| 30 | 30 minutes | 2.5 hours | Long pipeline, lab analysis |
| 60 | 1 hour | 5 hours | Cross-facility transport |

**How to calculate steps from physical delay:**

```
steps = physical_delay_time / scope.timestep

Example: 500m pipe, flow velocity 1 m/s → transit = 500s ≈ 8.3 min
  At timestep "1m": steps = 8 (round to nearest integer)
  At timestep "5m": steps = 2
```

!!! info "What delay() actually looks like in the output"

    Imagine pump flow bouncing between 8 and 12 m³/hr. With `delay('pump_flow', 10, 10.0)` at a 1-minute timestep:

    - **First 10 rows:** Delivery flow sits flat at 10.0 (the default). The pump is running, but nothing has arrived yet.
    - **From row 11 onwards:** Delivery flow starts bouncing between 8 and 12 — but it's showing what the pump was doing 10 minutes ago. If you overlay the two signals on a chart, the delivery trace is an exact copy of the pump trace, just shifted 10 steps to the right.
    - **If the pump stops at row 50:** Delivery flow keeps going for 10 more rows (rows 50-60), showing what was already in the pipe. Then it drops to whatever the pump was at row 50.

    **Bottom line:** The output is a time-shifted photocopy of the input. No smoothing, no lag curve — just a pure shift. Combine with first-order dynamics if you want a more gradual arrival.

### Example 1: Pipeline Transport Delay

A pump station sends flow through a 500m pipe. At the delivery point, flow appears 10 minutes later:

```yaml
columns:
  - name: timestamp
    data_type: timestamp
    generator:
      type: timestamp

  # Pump output at the source
  - name: pump_flow_m3_hr
    data_type: float
    generator:
      type: random_walk
      start: 10.0
      min: 5.0
      max: 15.0

  # Flow at the delivery point — delayed by 10 timesteps
  - name: delivery_flow_m3_hr
    data_type: float
    generator:
      type: derived
      expression: "delay('pump_flow_m3_hr', 10, 10.0)"
```

**How it works:**

- For the first 10 rows, `delivery_flow_m3_hr` returns the default (10.0)
- From row 11 onwards, it returns whatever `pump_flow_m3_hr` was 10 rows ago
- If pump flow changes at row 20, delivery flow changes at row 30

**Row-by-row (simplified, 3-step delay for clarity):**

With `delay('pump_flow', 3, 10.0)`:

| Row | pump_flow | delay() returns | Why |
|-----|-----------|-----------------|-----|
| 1 | 8.5 | 10.0 | Default — only 1 value stored, need 4 |
| 2 | 11.2 | 10.0 | Default — 2 values stored |
| 3 | 9.8 | 10.0 | Default — 3 values stored |
| 4 | 12.1 | **8.5** | Buffer full — returns row 1's value |
| 5 | 7.9 | **11.2** | Returns row 2's value |
| 6 | 10.3 | **9.8** | Returns row 3's value |

After the buffer fills (row 4 onwards), every output is exactly the input from 3 steps ago.

### Example 2: Dead Time + First-Order Response

Real transport delays are often followed by first-order dynamics (the pipe has some mixing). Combine `delay()` with `prev()`:

```yaml
columns:
  - name: timestamp
    data_type: timestamp
    generator:
      type: timestamp

  - name: input_signal
    data_type: float
    generator:
      type: random_walk
      start: 50.0
      min: 20.0
      max: 80.0

  # Pure delay
  - name: delayed_input
    data_type: float
    generator:
      type: derived
      expression: "delay('input_signal', 5, 50.0)"

  # First-order response to the delayed signal
  - name: output_signal
    data_type: float
    generator:
      type: derived
      expression: "prev('output_signal', 50.0) + 0.15 * (delayed_input - prev('output_signal', 50.0))"
```

**Result:** Output is flat for 5 steps (dead time), then gradually approaches the delayed input (first-order dynamics).

### delay() vs prev()

| Function | Looks Back | State | Best For |
|---|---|---|---|
| `prev()` | Exactly 1 row | Single value | Dynamic state (integrators, first-order, feedback) |
| `delay()` | N rows | Ring buffer of N+1 values | Transport delays (fixed time shift) |

`delay('col', 1, default)` produces the same result as `prev('col', default)`.

---

## Combining Stateful Functions

The real power emerges when you use all four together. Here's a complete example simulating a controlled process with noisy measurements:

```yaml
scope:
  start_time: "2026-01-01T00:00:00Z"
  timestep: "1m"
  row_count: 120
  seed: 42

entities:
  names: [reactor_01]

columns:
  - name: timestamp
    data_type: timestamp
    generator:
      type: timestamp

  # --- Setpoint ---
  - name: temp_setpoint_c
    data_type: float
    generator:
      type: constant
      value: 80.0

  # --- Noisy sensor reading (raw measurement) ---
  - name: raw_temp_c
    data_type: float
    generator:
      type: derived
      expression: "prev('actual_temp_c', 80.0) + (random() - 0.5) * 4"
      # Actual temp + noise band of ±2°C

  # --- Smoothed measurement (EMA filter) ---
  - name: filtered_temp_c
    data_type: float
    generator:
      type: derived
      expression: "ema('raw_temp_c', 0.3, 80.0)"

  # --- PID controller (acts on filtered measurement) ---
  - name: cooling_pct
    data_type: float
    generator:
      type: derived
      expression: "pid(pv=filtered_temp_c, sp=temp_setpoint_c, Kp=-2.0, Ki=-0.1, Kd=-0.5, dt=60, output_min=0, output_max=100)"

  # --- Actual process temperature (first-order response to cooling) ---
  - name: actual_temp_c
    data_type: float
    generator:
      type: derived
      expression: "prev('actual_temp_c', 85.0) + 0.05 * (95.0 - prev('cooling_pct', 50) * 0.3 - prev('actual_temp_c', 85.0))"
      # Starts above setpoint; 95.0 = heat source equilibrium without cooling
```

**What's happening:**

| Column | Function | Role |
|--------|----------|------|
| `raw_temp_c` | `prev()` | Adds noise to the actual temperature |
| `filtered_temp_c` | `ema()` | Smooths noisy reading for the controller |
| `cooling_pct` | `pid()` | Calculates valve position from filtered temp |
| `actual_temp_c` | `prev()` | Process dynamics — responds to cooling valve |

**Flow:** actual temp → noisy sensor → EMA filter → PID controller → cooling valve → actual temp (loop)

---

## Incremental State Persistence

When using `incremental.mode: stateful`, all stateful functions preserve their internal state between pipeline runs:

| Function | What's Preserved |
|----------|-----------------|
| `prev()` | Last value per entity |
| `ema()` | Last smoothed value per entity |
| `pid()` | Integral sum and last error per entity |
| `delay()` | Ring buffer of last N values per entity |
| `random_walk` | Last walk value per entity |

**What this means:** Run 2 picks up exactly where Run 1 left off — no discontinuities, no resets.

```yaml
read:
  connection: null
  format: simulation
  options:
    simulation:
      scope:
        start_time: "2026-01-01T00:00:00Z"
        timestep: "1m"
        row_count: 60    # 1 hour per run
        seed: 42
      columns:
        - name: level_m3
          data_type: float
          generator:
            type: derived
            expression: "prev('level_m3', 50.0) + inflow - outflow"
  incremental:
    mode: stateful
    column: timestamp
```

```
Run 1: level starts at 50.0, ends at 63.2  →  state saved
Run 2: level starts at 63.2, ends at 58.7  →  state saved
Run 3: level starts at 58.7, ...            →  continuous
```

Without `stateful` mode, each run would restart `prev('level_m3', 50.0)` from 50.0, creating a jump.

See [Stateful Incremental Loading](../patterns/incremental_stateful.md) for full configuration details.

---

## Tips and Pitfalls

### ✅ Do

- **Quote column names in `prev()` and `delay()`:** `prev('level', 0)` ✅ — `delay('flow', 5, 0)` ✅
- **Define columns in dependency order:** If column B uses `prev('A', 0)`, define A before B (or use self-reference like `prev('B', 0)` within B's own expression)
- **Match `dt` to your actual timestep:** If `scope.timestep: "5m"`, use `dt=300` (5 × 60 seconds) in `pid()`
- **Use `anti_windup=True`** (the default) to prevent integral windup when the PID output hits min/max limits
- **Clamp integrated values:** Use `max(0, min(100, ...))` to keep levels, percentages, etc. within physical bounds
- **Choose a sensible default for `delay()`:** The default is returned during the first N rows while the buffer fills. Use a realistic steady-state value (e.g., `delay('flow', 10, 10.0)` if flow is normally around 10)
- **Use `delay()` for transport, `prev()` for dynamics:** `delay()` gives a pure time shift (ring buffer). `prev()` gives single-step memory for feedback and integration. Don't use chains of `prev()` columns when `delay()` does the job in one call

### ❌ Don't

- **Unquoted column names:** `prev(level, 0)` ❌ or `delay(flow, 5, 0)` ❌ — will be interpreted as a variable, not a column name
- **Circular dependencies:** Column A depends on current B, and B depends on current A. Use `prev()` to break the cycle:
  ```yaml
  # ❌ Circular
  - name: level
    expression: "prev('level', 5) + flow_in - flow_out"
  - name: flow_out
    expression: "level * 0.1"        # Uses CURRENT level → circular

  # ✅ Fixed
  - name: level
    expression: "prev('level', 5) + flow_in - flow_out"
  - name: flow_out
    expression: "prev('level', 5) * 0.1"  # Uses PREVIOUS level → OK
  ```
- **Mismatched dt:** If your timestep is 5 minutes but you set `dt=60` (1 minute), the PID will over-correct by 5×
- **Derivative on noisy signals:** If your measurement is noisy, set `Kd=0` or apply `ema()` first — derivative amplifies noise

---

## Related Documentation

- [Generators Reference](generators.md) — all 13 generator types with parameters and examples
- [Advanced Features](advanced_features.md) — cross-entity references, scheduled events, chaos engineering
- [Process Simulation](process_simulation.md) — chemical engineering and process control scenarios
- [Incremental Mode](incremental.md) — continuous data generation with HWM state persistence
