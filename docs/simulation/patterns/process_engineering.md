---
title: "Process & Chemical Engineering (9-15)"
---

# Process & Chemical Engineering (9–15)

These patterns model real-world process systems — treatment plants, compressor stations, reactors, distillation columns, cooling towers, batch operations, and tank farms. They use odibi's most powerful simulation features: `pid()` control, `ema()` smoothing, `shock` events, cross-entity cascades, and scheduled operations.

!!! info "Prerequisites"
    These patterns build on [Foundational Patterns 1–8](foundations.md). You should also be familiar with [Stateful Functions](../stateful_functions.md) (`prev`, `ema`, `pid`) and [Advanced Features](../advanced_features.md) (cross-entity references, scheduled events).

---

## Pattern 9: Wastewater Treatment Plant {#pattern-9}

**Industry:** Environmental Engineering | **Difficulty:** Intermediate

!!! tip "What you'll learn"
    - **Cross-entity cascade** — each treatment stage references the upstream stage's output using `Entity.column` syntax, so flow and concentrations propagate realistically through the plant
    - **`entity_overrides`** — give each entity (stage) a completely different generator for the same column. Influent uses `random_walk`, downstream stages use `derived` expressions that multiply upstream values by removal fractions

A wastewater treatment plant is a series of stages: Influent → Primary Clarifier → Aeration → Secondary Clarifier → Effluent. Each stage removes a fraction of contaminants. Flow decreases slightly at each stage (sludge removal), and BOD/TSS concentrations drop dramatically.

```yaml
project: wastewater_treatment
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
  - pipeline: wwtp
    nodes:
      - name: treatment_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-01T00:00:00Z"
                timestep: "15m"
                row_count: 672            # 7 days
                seed: 42
              entities:
                names: [Influent, Primary, Aeration, Secondary, Effluent]
              columns:
                - name: stage_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                # Influent flow — diurnal variation via random walk
                - name: flow_mgd
                  data_type: float
                  generator:
                    type: random_walk
                    start: 12.0
                    min: 6.0
                    max: 20.0
                    volatility: 0.3
                    mean_reversion: 0.05
                    precision: 2
                  entity_overrides:
                    Primary:
                      type: derived
                      expression: "Influent.flow_mgd * 0.98"
                    Aeration:
                      type: derived
                      expression: "Primary.flow_mgd * 0.95"
                    Secondary:
                      type: derived
                      expression: "Aeration.flow_mgd * 0.97"
                    Effluent:
                      type: derived
                      expression: "Secondary.flow_mgd * 0.99"

                # BOD concentration — decreases through treatment
                - name: bod_mg_l
                  data_type: float
                  generator:
                    type: range
                    min: 180.0
                    max: 280.0
                    distribution: normal
                    mean: 220.0
                    std_dev: 25.0
                  entity_overrides:
                    Primary:
                      type: derived
                      expression: "Influent.bod_mg_l * 0.65"
                    Aeration:
                      type: derived
                      expression: "Primary.bod_mg_l * 0.15"
                    Secondary:
                      type: derived
                      expression: "Aeration.bod_mg_l * 0.60"
                    Effluent:
                      type: derived
                      expression: "Secondary.bod_mg_l * 0.85"

                # TSS — total suspended solids
                - name: tss_mg_l
                  data_type: float
                  generator:
                    type: range
                    min: 150.0
                    max: 300.0
                    distribution: normal
                    mean: 200.0
                    std_dev: 30.0
                  entity_overrides:
                    Primary:
                      type: derived
                      expression: "Influent.tss_mg_l * 0.50"
                    Aeration:
                      type: derived
                      expression: "Primary.tss_mg_l * 0.30"
                    Secondary:
                      type: derived
                      expression: "Aeration.tss_mg_l * 0.25"
                    Effluent:
                      type: derived
                      expression: "Secondary.tss_mg_l * 0.80"

                # DO only matters in aeration basin
                - name: do_mg_l
                  data_type: float
                  generator:
                    type: constant
                    value: 0.0
                  entity_overrides:
                    Aeration:
                      type: random_walk
                      start: 2.0
                      min: 0.5
                      max: 4.0
                      volatility: 0.1
                      mean_reversion: 0.2
                      precision: 2

                # pH — relatively stable through treatment
                - name: ph
                  data_type: float
                  generator:
                    type: range
                    min: 6.8
                    max: 7.5
                    distribution: normal
                    mean: 7.1
                    std_dev: 0.15

              # Storm event — surge flow for 6 hours
              scheduled_events:
                - type: forced_value
                  entity: Influent
                  column: flow_mgd
                  value: 18.5
                  start_time: "2026-03-04T02:00:00Z"
                  end_time: "2026-03-04T08:00:00Z"

              chaos:
                outlier_rate: 0.003
                outlier_factor: 2.0
        write:
          connection: output
          format: parquet
          path: bronze/wwtp_data.parquet
          mode: overwrite
```

**What makes this realistic:**

- Cross-entity references cascade flow and concentrations downstream — each stage multiplies upstream values by a realistic removal fraction
- BOD goes from ~220 mg/L influent → ~5 mg/L effluent (typical 97% removal for a well-run plant)
- Storm event at 2 AM forces high influent flow, which propagates through all downstream stages
- Dissolved oxygen only has a meaningful value in the aeration basin (zero elsewhere)
- pH stays in a narrow band with normal distribution — matches real WWTP behavior

!!! example "Try this"
    - Add an `ammonia_mg_l` column with nitrification happening in the Aeration stage (multiply by 0.10 — 90% removal)
    - Add a `chlorine_residual` column on Effluent only (use `entity_overrides` with `range` generator 0.5–2.0 mg/L)
    - Increase the storm duration to 12 hours and watch how it affects downstream BOD removal

> 📖 **Learn more:** [Advanced Features](../advanced_features.md) — Cross-entity references and how entity generation order determines data availability

---

## Pattern 10: Compressor Station Monitoring {#pattern-10}

**Industry:** Oil & Gas | **Difficulty:** Intermediate

!!! tip "What you'll learn"
    - **`shock_rate` and `shock_bias`** — add sudden process upsets to a random walk. `shock_rate` controls how often shocks happen; `shock_bias` controls direction (+1.0 = always up, -1.0 = always down, 0.0 = either direction)
    - **`trend` for gradual wear** — a small positive trend on vibration simulates bearing degradation over time

A natural gas compressor station has 3 units running 24/7. Discharge pressure occasionally spikes (compressor surge — always upward), and vibration slowly trends upward as bearings wear. One unit has a worse bearing.

```yaml
project: compressor_station
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
  - pipeline: compressors
    nodes:
      - name: compressor_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-10T00:00:00Z"
                timestep: "5m"
                row_count: 288            # 24 hours
                seed: 42
              entities:
                names: [Compressor_01, Compressor_02, Compressor_03]
              columns:
                - name: unit_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                - name: suction_pressure_psig
                  data_type: float
                  generator:
                    type: random_walk
                    start: 450.0
                    min: 400.0
                    max: 500.0
                    volatility: 1.5
                    mean_reversion: 0.1
                    precision: 1

                # Discharge pressure — occasional upward spikes (surge)
                - name: discharge_pressure_psig
                  data_type: float
                  generator:
                    type: random_walk
                    start: 1200.0
                    min: 1100.0
                    max: 1400.0
                    volatility: 3.0
                    mean_reversion: 0.08
                    precision: 1
                    shock_rate: 0.03        # 3% chance of surge per timestep
                    shock_magnitude: 50.0   # Up to 50 PSI spike
                    shock_bias: 1.0         # Always upward — surge never goes down

                # Compression ratio = discharge / suction
                - name: compression_ratio
                  data_type: float
                  generator:
                    type: derived
                    expression: "round(discharge_pressure_psig / suction_pressure_psig, 2)"

                # Vibration — slow upward trend (bearing wear)
                - name: vibration_mils
                  data_type: float
                  generator:
                    type: random_walk
                    start: 2.0
                    min: 0.5
                    max: 8.0
                    volatility: 0.15
                    mean_reversion: 0.05
                    trend: 0.002           # Gradual bearing wear
                    precision: 2
                  entity_overrides:
                    Compressor_03:          # Worse bearing
                      type: random_walk
                      start: 3.5
                      min: 1.0
                      max: 8.0
                      volatility: 0.25
                      mean_reversion: 0.03
                      trend: 0.005         # Faster degradation
                      precision: 2

                - name: bearing_temp_f
                  data_type: float
                  generator:
                    type: random_walk
                    start: 180.0
                    min: 160.0
                    max: 250.0
                    volatility: 1.0
                    mean_reversion: 0.1
                    precision: 1

                - name: flow_mmscfd
                  data_type: float
                  generator:
                    type: random_walk
                    start: 25.0
                    min: 15.0
                    max: 35.0
                    volatility: 0.5
                    mean_reversion: 0.1
                    precision: 2

                # Vibration alarm threshold
                - name: vibration_alarm
                  data_type: boolean
                  generator:
                    type: derived
                    expression: "vibration_mils > 5.0"

              chaos:
                outlier_rate: 0.008
                outlier_factor: 2.5
        write:
          connection: output
          format: parquet
          path: bronze/compressor_data.parquet
          mode: overwrite
```

**What makes this realistic:**

- `shock_bias: 1.0` on discharge pressure means compressor surges always spike **upward** — that's how real surge works (back-flow causes momentary pressure spike)
- `trend: 0.002` on vibration simulates gradual bearing wear over the 24-hour window
- `Compressor_03` has a worse bearing (higher start, faster trend) via `entity_overrides`
- Compression ratio is derived from actual pressures, not generated independently
- Vibration alarm at 5.0 mils matches industry standards for centrifugal compressors

!!! example "Try this"
    - Add `shock_rate: 0.02` to `Compressor_03`'s vibration to simulate intermittent bearing impacts
    - Add a `maintenance_needed` derived column: `"vibration_mils > 4.0 and bearing_temp_f > 200"`
    - Change `shock_bias` to `0.0` for bidirectional pressure upsets and compare the data

> 📖 **Learn more:** [Generators Reference](../generators.md) — `random_walk` shock parameters (`shock_rate`, `shock_magnitude`, `shock_bias`)

---

## Pattern 11: CSTR with PID Control {#pattern-11}

**Industry:** Chemical Engineering | **Difficulty:** Advanced

!!! tip "What you'll learn"
    - **`pid()` function** — a built-in PID controller that calculates control output from process variable and setpoint, with proportional, integral, and derivative terms
    - **First-order dynamics with `prev()`** — model how a process variable (temperature) responds to inputs with realistic lag, not instant jumps
    - **Combining `pid()` and `prev()`** — the controller reads the previous temperature, calculates a new cooling output, and the process responds with first-order dynamics

A continuous stirred-tank reactor (CSTR) runs an exothermic reaction. More feed means more heat generation. A PID controller adjusts the cooling water valve to keep reactor temperature at 85°C. At hour 4, a feed disturbance tests the controller's response.

```yaml
project: cstr_pid_control
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
  - pipeline: reactor
    nodes:
      - name: cstr_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-10T06:00:00Z"
                timestep: "1m"
                row_count: 480            # 8 hours
                seed: 42
              entities:
                names: [CSTR_01]
              columns:
                - name: reactor_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                # Setpoint — target temperature
                - name: temp_setpoint_c
                  data_type: float
                  generator: {type: constant, value: 85.0}

                # Feed flow — the disturbance variable
                - name: feed_flow_m3_hr
                  data_type: float
                  generator:
                    type: random_walk
                    start: 5.0
                    min: 4.0
                    max: 6.0
                    volatility: 0.1
                    mean_reversion: 0.1
                    precision: 2

                # Heat generated by reaction (more feed = more heat)
                - name: heat_generation_kw
                  data_type: float
                  generator:
                    type: derived
                    expression: "feed_flow_m3_hr * 10.0"

                # PID controller output — cooling water valve position
                # Uses prev() to read previous temperature (avoids circular dependency)
                - name: cooling_pct
                  data_type: float
                  generator:
                    type: derived
                    expression: >
                      pid(pv=prev('reactor_temp_c', 85.0), sp=temp_setpoint_c,
                      Kp=2.5, Ki=0.08, Kd=0.5, dt=60,
                      output_min=0, output_max=100)

                # Reactor temperature — first-order response
                # τ = 300s (5-min time constant), Δt = 60s
                # Heat input from reaction, cooling from jacket, ambient loss
                - name: reactor_temp_c
                  data_type: float
                  generator:
                    type: derived
                    expression: >
                      prev('reactor_temp_c', 85.0)
                      + (60.0/300.0) * (heat_generation_kw * 0.5
                      - cooling_pct * 0.3
                      - (prev('reactor_temp_c', 85.0) - 25.0) * 0.1)

                # Error for trending
                - name: temp_error_c
                  data_type: float
                  generator:
                    type: derived
                    expression: "temp_setpoint_c - reactor_temp_c"

                # High temperature alarm
                - name: high_temp_alarm
                  data_type: boolean
                  generator:
                    type: derived
                    expression: "reactor_temp_c > 90.0"

              # Feed disturbance at hour 4
              scheduled_events:
                - type: forced_value
                  entity: CSTR_01
                  column: feed_flow_m3_hr
                  value: 5.8
                  start_time: "2026-03-10T10:00:00Z"
                  end_time: "2026-03-10T11:00:00Z"
        write:
          connection: output
          format: parquet
          path: bronze/cstr_data.parquet
          mode: overwrite
```

**What makes this realistic:**

- The PID controller reads **previous** temperature (`prev('reactor_temp_c', 85.0)`) — just like a real controller that samples, calculates, then acts
- Reactor temperature responds with first-order dynamics (τ=5 min) — it doesn't jump to the new value instantly
- More feed → more heat generation → temperature rises → PID opens cooling valve → temperature comes back
- The feed disturbance at hour 4 forces a step change that tests the controller's ability to recover
- `output_min=0, output_max=100` constrains the valve to 0–100% — a real valve can't go beyond fully open or fully closed

!!! example "Try this"
    - Change `Kp` to `5.0` and watch the temperature oscillate (too aggressive)
    - Remove `Ki` (set to `0`) and notice a steady-state offset — the temperature never quite reaches 85°C
    - Add a second reactor (`CSTR_02`) with different tuning parameters and compare their responses to the same disturbance

> 📖 **Learn more:** [Stateful Functions](../stateful_functions.md) — `pid()` parameters and anti-windup | [Process Simulation](../process_simulation.md) — PID control theory and tuning guidelines

---

## Pattern 12: Distillation Column {#pattern-12}

**Industry:** Chemical Engineering | **Difficulty:** Advanced

!!! tip "What you'll learn"
    - **`mean_reversion_to` with a dynamic column** — make a random walk track another *changing* column, not just a fixed start value. Here, the mid-column temperature tracks the feed temperature as it varies.
    - **Multiple correlated process variables** — feed conditions affect overhead purity, condenser duty tracks reboiler duty, and column ΔP indicates flooding

A binary distillation column separates a light/heavy mixture. Feed conditions vary throughout the day. The mid-column temperature profile tracks feed temperature, overhead purity depends on reflux ratio, and column pressure drop signals flooding risk.

```yaml
project: distillation_column
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
  - pipeline: distillation
    nodes:
      - name: column_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-10T00:00:00Z"
                timestep: "5m"
                row_count: 288            # 24 hours
                seed: 42
              entities:
                names: [Column_01]
              columns:
                - name: column_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                # Feed conditions — these are the independent variables
                - name: feed_temp_c
                  data_type: float
                  generator:
                    type: random_walk
                    start: 75.0
                    min: 65.0
                    max: 85.0
                    volatility: 0.5
                    mean_reversion: 0.08
                    precision: 1

                - name: feed_flow_kg_hr
                  data_type: float
                  generator:
                    type: random_walk
                    start: 5000.0
                    min: 4000.0
                    max: 6000.0
                    volatility: 20.0
                    mean_reversion: 0.1
                    precision: 0

                - name: feed_composition
                  data_type: float
                  generator:
                    type: random_walk
                    start: 0.45
                    min: 0.35
                    max: 0.55
                    volatility: 0.005
                    mean_reversion: 0.05
                    precision: 3

                # Operating parameters
                - name: reflux_ratio
                  data_type: float
                  generator:
                    type: random_walk
                    start: 2.5
                    min: 1.5
                    max: 4.0
                    volatility: 0.05
                    mean_reversion: 0.1
                    precision: 2

                - name: reboiler_duty_kw
                  data_type: float
                  generator:
                    type: random_walk
                    start: 800.0
                    min: 600.0
                    max: 1000.0
                    volatility: 5.0
                    mean_reversion: 0.1
                    precision: 0

                # Mid-column temp tracks feed temp (dynamic mean_reversion_to)
                - name: tray_10_temp_c
                  data_type: float
                  generator:
                    type: random_walk
                    start: 82.0
                    min: 70.0
                    max: 95.0
                    volatility: 0.3
                    mean_reversion: 0.15
                    mean_reversion_to: feed_temp_c
                    precision: 1

                - name: overhead_temp_c
                  data_type: float
                  generator:
                    type: random_walk
                    start: 65.0
                    min: 55.0
                    max: 75.0
                    volatility: 0.2
                    mean_reversion: 0.12
                    precision: 1

                - name: bottoms_temp_c
                  data_type: float
                  generator:
                    type: random_walk
                    start: 105.0
                    min: 95.0
                    max: 115.0
                    volatility: 0.3
                    mean_reversion: 0.1
                    precision: 1

                # Overhead purity depends on reflux and feed composition
                - name: overhead_purity
                  data_type: float
                  generator:
                    type: derived
                    expression: >
                      min(0.99, max(0.85,
                      0.95 + (reflux_ratio - 2.5) * 0.02
                      - (feed_composition - 0.45) * 0.1))

                # Energy balance: condenser duty tracks reboiler
                - name: condenser_duty_kw
                  data_type: float
                  generator:
                    type: derived
                    expression: "reboiler_duty_kw * 0.85 + feed_flow_kg_hr * 0.02"

                # Column pressure drop — indicates flooding
                - name: column_dp_kpa
                  data_type: float
                  generator:
                    type: random_walk
                    start: 12.0
                    min: 8.0
                    max: 20.0
                    volatility: 0.2
                    mean_reversion: 0.1
                    precision: 1

                - name: flooding_risk
                  data_type: boolean
                  generator:
                    type: derived
                    expression: "column_dp_kpa > 16.0"

              # Feed composition upset at hour 12
              scheduled_events:
                - type: forced_value
                  entity: Column_01
                  column: feed_composition
                  value: 0.55
                  start_time: "2026-03-10T12:00:00Z"
                  end_time: "2026-03-10T14:00:00Z"
        write:
          connection: output
          format: parquet
          path: bronze/distillation_data.parquet
          mode: overwrite
```

**What makes this realistic:**

- `tray_10_temp_c` uses `mean_reversion_to: feed_temp_c` — the mid-column temperature profile tracks feed temperature as it varies, which is exactly what happens in a real column
- Overhead purity depends on reflux ratio (higher reflux = better separation — McCabe-Thiele relationship) and feed composition
- Condenser duty tracks reboiler duty because energy in ≈ energy out (first law of thermodynamics)
- Column ΔP > 16 kPa signals flooding risk — a real-world operational limit
- Feed composition upset at noon tests the column's response to feed quality changes

!!! example "Try this"
    - Increase `reflux_ratio` start to `3.5` and watch `overhead_purity` improve
    - Add a `column_dp` alarm at 18 kPa (closer to the flood point) and a `reboiler_temp_c` column
    - Add a second column (`Column_02`) processing the bottoms product from `Column_01` — a two-column train

> 📖 **Learn more:** [Generators Reference](../generators.md) — `mean_reversion_to` for dynamic setpoint tracking | [Process Simulation](../process_simulation.md) — Energy balance examples

---

## Pattern 13: Cooling Tower {#pattern-13}

**Industry:** Utilities | **Difficulty:** Intermediate

!!! tip "What you'll learn"
    - **`ema()` for signal smoothing** — Exponential Moving Average filters noisy sensor readings to produce a clean trending signal, exactly like a DCS trending pen. The `alpha` parameter controls how much smoothing: lower alpha = smoother but slower to respond
    - **`safe_div()` for safe division** — avoid division-by-zero errors in derived expressions when denominators could be zero
    - **`mean_reversion_to` for temperature tracking** — cold water supply temperature tracks ambient temperature because that's the thermodynamic limit

Two cooling towers serve a plant's heat rejection needs. Raw sensor readings are noisy; EMA smoothing provides clean signals for control and trending decisions.

```yaml
project: cooling_tower
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
  - pipeline: cooling
    nodes:
      - name: tower_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-10T00:00:00Z"
                timestep: "1m"
                row_count: 1440          # 24 hours
                seed: 42
              entities:
                names: [CT_01, CT_02]
              columns:
                - name: tower_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                # Ambient temp — slow diurnal drift
                - name: ambient_temp_c
                  data_type: float
                  generator:
                    type: random_walk
                    start: 30.0
                    min: 20.0
                    max: 40.0
                    volatility: 0.2
                    mean_reversion: 0.02
                    precision: 1

                # Cold water out — tracks ambient (thermodynamic limit)
                - name: supply_temp_c
                  data_type: float
                  generator:
                    type: random_walk
                    start: 28.0
                    min: 22.0
                    max: 35.0
                    volatility: 0.4
                    mean_reversion: 0.1
                    mean_reversion_to: ambient_temp_c
                    precision: 1

                # Hot water return from process
                - name: return_temp_c
                  data_type: float
                  generator:
                    type: random_walk
                    start: 38.0
                    min: 30.0
                    max: 45.0
                    volatility: 0.5
                    mean_reversion: 0.08
                    precision: 1
                  entity_overrides:
                    CT_02:                 # Heavier process load
                      type: random_walk
                      start: 42.0
                      min: 33.0
                      max: 48.0
                      volatility: 0.5
                      mean_reversion: 0.08
                      precision: 1

                # Raw approach temperature (noisy)
                - name: raw_approach_c
                  data_type: float
                  generator:
                    type: derived
                    expression: "supply_temp_c - ambient_temp_c"

                # EMA-smoothed approach for trending
                - name: smooth_approach_c
                  data_type: float
                  generator:
                    type: derived
                    expression: "ema('raw_approach_c', alpha=0.1, default=raw_approach_c)"

                # Cooling range
                - name: range_c
                  data_type: float
                  generator:
                    type: derived
                    expression: "return_temp_c - supply_temp_c"

                - name: fan_speed_pct
                  data_type: float
                  generator:
                    type: random_walk
                    start: 60.0
                    min: 0.0
                    max: 100.0
                    volatility: 2.0
                    mean_reversion: 0.15
                    precision: 0

                # Makeup water — evaporation drives it
                - name: makeup_water_gpm
                  data_type: float
                  generator:
                    type: derived
                    expression: "range_c * 0.5 + fan_speed_pct * 0.1"

                - name: cycles_of_concentration
                  data_type: float
                  generator:
                    type: random_walk
                    start: 4.0
                    min: 2.0
                    max: 8.0
                    volatility: 0.05
                    mean_reversion: 0.1
                    precision: 1

                # Blowdown — safe_div avoids division by zero
                - name: blowdown_gpm
                  data_type: float
                  generator:
                    type: derived
                    expression: "safe_div(makeup_water_gpm, cycles_of_concentration - 1, 0)"

                # Cooling efficiency
                - name: efficiency_pct
                  data_type: float
                  generator:
                    type: derived
                    expression: >
                      min(100, max(0,
                      safe_div(range_c, max(return_temp_c - ambient_temp_c, 0.1), 0) * 100))
        write:
          connection: output
          format: parquet
          path: bronze/cooling_tower.parquet
          mode: overwrite
```

**What makes this realistic:**

- `supply_temp_c` tracks `ambient_temp_c` via `mean_reversion_to` — a cooling tower can't cool water below ambient (thermodynamic limit)
- `ema('raw_approach_c', alpha=0.1)` smooths noisy approach temperature — exactly like a DCS trending pen with a filter constant
- `safe_div()` in blowdown calculation prevents divide-by-zero when cycles of concentration = 1
- CT_02 has a heavier process load (higher return water temperature) via entity overrides
- Makeup water is derived from evaporation rate (range × flow factor) — real cooling tower water balance

!!! example "Try this"
    - Change `alpha` to `0.3` (less smoothing) and `0.03` (more smoothing) — plot `raw_approach_c` vs `smooth_approach_c` to see the difference
    - Add a `conductivity_us_cm` column using `random_walk` with `mean_reversion_to: cycles_of_concentration` — conductivity tracks concentration
    - Add a `basin_level_pct` column using `prev()`: level rises with makeup, drops with blowdown

> 📖 **Learn more:** [Stateful Functions](../stateful_functions.md) — `ema()` parameters and how smoothing works

---

## Pattern 14: Batch Reactor with Recipe {#pattern-14}

**Industry:** Pharma / Chemical Engineering | **Difficulty:** Advanced

!!! tip "What you'll learn"
    - **Scheduled events for recipe phases** — use multiple `forced_value` events to change setpoints at specific times, simulating a multi-phase batch recipe (Charge → React → Cool Down)
    - **First-order dynamics tracking setpoints** — `prev()` creates a realistic temperature ramp (the reactor doesn't jump from 25°C to 85°C instantly — it ramps up over minutes)

A batch reactor runs a 3-phase recipe: Charging (2h at 25°C, fill reactor), Reaction (4h at 85°C, high agitation), Cooldown (2h at 30°C, slow agitation). Temperature setpoints, agitator speed, and phase labels all change at phase boundaries using scheduled events.

```yaml
project: batch_reactor
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
  - pipeline: batch
    nodes:
      - name: batch_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-10T06:00:00Z"
                timestep: "1m"
                row_count: 480            # 8 hours
                seed: 42
              entities:
                names: [Reactor_01]
              columns:
                - name: reactor_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                # These constants get overridden by scheduled events
                - name: temp_setpoint_c
                  data_type: float
                  generator: {type: constant, value: 25.0}

                - name: agitator_rpm
                  data_type: int
                  generator: {type: constant, value: 120}

                - name: batch_phase
                  data_type: string
                  generator: {type: constant, value: "charging"}

                # Reactor temperature — first-order response to setpoint
                # τ = 600s (10-min time constant), Δt = 60s
                - name: reactor_temp_c
                  data_type: float
                  generator:
                    type: derived
                    expression: >
                      prev('reactor_temp_c', 25.0)
                      + (60.0/600.0) * (temp_setpoint_c - prev('reactor_temp_c', 25.0))

                # Pressure rises with temperature (ideal gas behavior)
                - name: pressure_bar
                  data_type: float
                  generator:
                    type: derived
                    expression: "1.0 + max(0, (reactor_temp_c - 60.0)) * 0.02"

                # Jacket temp leads/lags reactor temp
                - name: jacket_temp_c
                  data_type: float
                  generator:
                    type: derived
                    expression: >
                      reactor_temp_c + 5.0
                      if reactor_temp_c < temp_setpoint_c
                      else reactor_temp_c - 5.0

              # ── Recipe Phases via Scheduled Events ──
              scheduled_events:
                # Phase 2: Reaction (08:00–12:00) — heat up, high agitation
                - type: forced_value
                  entity: Reactor_01
                  column: temp_setpoint_c
                  value: 85.0
                  start_time: "2026-03-10T08:00:00Z"
                  end_time: "2026-03-10T12:00:00Z"
                - type: forced_value
                  entity: Reactor_01
                  column: agitator_rpm
                  value: 200
                  start_time: "2026-03-10T08:00:00Z"
                  end_time: "2026-03-10T12:00:00Z"
                - type: forced_value
                  entity: Reactor_01
                  column: batch_phase
                  value: "reaction"
                  start_time: "2026-03-10T08:00:00Z"
                  end_time: "2026-03-10T12:00:00Z"

                # Phase 3: Cooldown (12:00–14:00) — cool down, slow agitation
                - type: forced_value
                  entity: Reactor_01
                  column: temp_setpoint_c
                  value: 30.0
                  start_time: "2026-03-10T12:00:00Z"
                  end_time: "2026-03-10T14:00:00Z"
                - type: forced_value
                  entity: Reactor_01
                  column: agitator_rpm
                  value: 80
                  start_time: "2026-03-10T12:00:00Z"
                  end_time: "2026-03-10T14:00:00Z"
                - type: forced_value
                  entity: Reactor_01
                  column: batch_phase
                  value: "cooldown"
                  start_time: "2026-03-10T12:00:00Z"
                  end_time: "2026-03-10T14:00:00Z"
        write:
          connection: output
          format: parquet
          path: bronze/batch_reactor.parquet
          mode: overwrite
```

**What makes this realistic:**

- Temperature doesn't jump from 25°C to 85°C instantly — `prev()` with `(Δt/τ) = 60/600 = 0.1` means 10% of the error is corrected each minute, so it takes ~30 minutes to ramp up (just like a real jacketed reactor)
- Pressure rises only above 60°C (reaction temperature threshold) — simulates gas evolution from the reaction
- Jacket temperature leads reactor temp during heat-up and lags during cool-down — exactly how a real heating/cooling jacket works
- Agitator speed changes with phase — higher RPM during reaction for mass transfer, lower during cooldown
- Phase labels (`charging`, `reaction`, `cooldown`) let you filter data by recipe phase in your analysis

!!! example "Try this"
    - Add a second reactor (`Reactor_02`) with recipe phases staggered by 2 hours to simulate two reactors running in sequence
    - Change the time constant from `600` to `300` (5 minutes) for a faster-responding reactor and compare the temperature ramp
    - Add a `conversion_pct` column using `prev()`: `"min(100, prev('conversion_pct', 0) + 0.5)"` during the reaction phase

> 📖 **Learn more:** [Advanced Features](../advanced_features.md) — Scheduled events with `forced_value` | [Process Simulation](../process_simulation.md) — First-order system dynamics

---

## Pattern 15: Tank Farm Inventory {#pattern-15}

**Industry:** Oil & Gas / Logistics | **Difficulty:** Intermediate

!!! tip "What you'll learn"
    - **`prev()` for material balance integration** — the tank level at time *t* equals the level at time *t-1* plus inflow minus outflow. This is the discrete form of the mass balance equation: `dV/dt = Q_in - Q_out`
    - **Physical clamping with `min()` and `max()`** — tank level can't go below 0 or above capacity, so we clamp the derived expression

A tank farm has 4 storage tanks receiving product from a pipeline and shipping out via truck and rail. Level changes based on the difference between inflow and outflow, integrated over time using `prev()`. One tank is smaller, and one has its outflow shut off for maintenance (level rises).

```yaml
project: tank_farm
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
  - pipeline: inventory
    nodes:
      - name: tank_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-01T00:00:00Z"
                timestep: "15m"
                row_count: 672            # 7 days
                seed: 42
              entities:
                names: [Tank_A, Tank_B, Tank_C, Tank_D]
              columns:
                - name: tank_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                # Tank capacity (barrels)
                - name: capacity_bbl
                  data_type: float
                  generator: {type: constant, value: 10000.0}
                  entity_overrides:
                    Tank_D:               # Smaller tank
                      type: constant
                      value: 5000.0

                # Inflow from pipeline
                - name: inflow_bbl_hr
                  data_type: float
                  generator:
                    type: random_walk
                    start: 50.0
                    min: 0.0
                    max: 120.0
                    volatility: 3.0
                    mean_reversion: 0.1
                    precision: 1
                  entity_overrides:
                    Tank_C:               # Busier receiving tank
                      type: random_walk
                      start: 80.0
                      min: 20.0
                      max: 140.0
                      volatility: 4.0
                      mean_reversion: 0.08
                      precision: 1

                # Outflow to truck/rail
                - name: outflow_bbl_hr
                  data_type: float
                  generator:
                    type: random_walk
                    start: 45.0
                    min: 0.0
                    max: 100.0
                    volatility: 4.0
                    mean_reversion: 0.08
                    precision: 1

                # Tank level — material balance integration
                # Δt = 15 min = 0.25 hr (flows in bbl/hr)
                - name: level_bbl
                  data_type: float
                  generator:
                    type: derived
                    expression: >
                      max(0, min(capacity_bbl,
                      prev('level_bbl', 5000)
                      + (inflow_bbl_hr - outflow_bbl_hr) * 0.25))

                - name: level_pct
                  data_type: float
                  generator:
                    type: derived
                    expression: "round(level_bbl / capacity_bbl * 100, 1)"

                # Alarms at industry-standard setpoints
                - name: high_level_alarm
                  data_type: boolean
                  generator:
                    type: derived
                    expression: "level_pct > 85.0"

                - name: low_level_alarm
                  data_type: boolean
                  generator:
                    type: derived
                    expression: "level_pct < 15.0"

                - name: temperature_f
                  data_type: float
                  generator:
                    type: random_walk
                    start: 75.0
                    min: 60.0
                    max: 95.0
                    volatility: 0.5
                    mean_reversion: 0.05
                    precision: 1

              # Tank_A outflow stops for maintenance — level will rise
              scheduled_events:
                - type: forced_value
                  entity: Tank_A
                  column: outflow_bbl_hr
                  value: 0
                  start_time: "2026-03-03T06:00:00Z"
                  end_time: "2026-03-03T18:00:00Z"
        write:
          connection: output
          format: parquet
          path: bronze/tank_farm.parquet
          mode: overwrite
```

**What makes this realistic:**

- Material balance integration with `prev()`: level[t] = level[t-1] + (inflow - outflow) × Δt
- Physical clamping: `max(0, min(capacity_bbl, ...))` prevents level from going negative or exceeding tank capacity
- 15-minute timestep matches typical tank gauging intervals in the field
- Hi/lo alarms at 85% and 15% match standard industry setpoints (API 2350)
- Tank_D is smaller (5,000 bbl vs 10,000) — its alarms trigger faster
- Tank_A outflow shutdown causes level to rise — you can see the high-level alarm trigger

!!! example "Try this"
    - Add a `net_flow_bbl_hr` derived column: `"inflow_bbl_hr - outflow_bbl_hr"` to see the instantaneous rate of change
    - Add a `days_to_full` column: `"safe_div((capacity_bbl - level_bbl), max(inflow_bbl_hr - outflow_bbl_hr, 0.1), 999) / 24"` to estimate time until the tank is full
    - Change Tank_D capacity to `3000` and watch how quickly alarms trigger on a smaller tank

> 📖 **Learn more:** [Stateful Functions](../stateful_functions.md) — `prev()` for integration and accumulation patterns | [Process Simulation](../process_simulation.md) — Material balance theory and examples

---

← [Foundational Patterns (1–8)](foundations.md) | [Energy & Utilities Patterns (16–20)](energy_utilities.md) →
