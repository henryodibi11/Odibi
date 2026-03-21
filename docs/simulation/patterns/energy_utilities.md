---
title: "Energy & Utilities (16-20)"
---

# Energy & Utilities (16–20)

Renewable energy, grid storage, and utility network patterns. These patterns model solar farms, wind turbines, battery systems, smart meters, and EV charging — showcasing the `boolean`, `geo`, `ipv4`, `uuid`, and `email` generators alongside energy-specific physics.

!!! info "Prerequisites"
    These patterns build on [Foundational Patterns 1–8](foundations.md). You should be familiar with [Generators Reference](../generators.md) and [Stateful Functions](../stateful_functions.md).

---

## Pattern 16: Solar Farm {#pattern-16}

**Industry:** Renewables | **Difficulty:** Intermediate

!!! tip "What you'll learn"
    - **`boolean` generator for cloud cover** — `is_cloudy` with `true_probability: 0.3` creates realistic intermittent shading
    - **Weather-to-power coupling** — derived expressions chain irradiance → panel temperature → efficiency → power output
    - **`prev()` for daily energy integration** — cumulative kWh accumulates over 24 hours, just like a real inverter's energy counter

A solar farm has four arrays generating power based on sunlight. Cloud cover randomly blocks irradiance, panel temperature derates efficiency (real panels lose ~0.4%/°C above 25°C), and one array has dirty panels that reduce output. The `prev()` function integrates power over 5-minute intervals to track daily energy production.

```yaml
project: solar_farm
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
  - pipeline: solar
    nodes:
      - name: solar_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-01T00:00:00Z"
                timestep: "5m"
                row_count: 288            # 24 hours
                seed: 42
              entities:
                names: [Array_01, Array_02, Array_03, Array_04]
              columns:
                - name: array_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                # Cloud cover — 30% chance of clouds at any timestep
                - name: is_cloudy
                  data_type: boolean
                  generator:
                    type: boolean
                    true_probability: 0.3

                - name: ambient_temp_c
                  data_type: float
                  generator:
                    type: random_walk
                    start: 22.0
                    min: 15.0
                    max: 38.0
                    volatility: 0.5
                    mean_reversion: 0.1
                    precision: 1

                # Solar irradiance — Array_03 has dirty panels (soiling)
                - name: irradiance_w_m2
                  data_type: float
                  generator:
                    type: random_walk
                    start: 600.0
                    min: 0.0
                    max: 1100.0
                    volatility: 30.0
                    mean_reversion: 0.05
                    precision: 0
                  entity_overrides:
                    Array_03:
                      type: random_walk
                      start: 450.0
                      min: 0.0
                      max: 850.0
                      volatility: 30.0
                      mean_reversion: 0.05
                      precision: 0

                # Panel temp rises with irradiance (NOCT relationship)
                - name: panel_temp_c
                  data_type: float
                  generator:
                    type: derived
                    expression: "ambient_temp_c + irradiance_w_m2 * 0.03"

                # Efficiency derates above 25°C (temp coefficient ~-0.4%/°C)
                - name: efficiency_pct
                  data_type: float
                  generator:
                    type: derived
                    expression: "max(0, 20.0 - 0.05 * max(0, panel_temp_c - 25.0))"

                # Power output — 2.0 is array area factor, cloud impact
                - name: power_kw
                  data_type: float
                  generator:
                    type: derived
                    expression: >
                      round(irradiance_w_m2 * efficiency_pct / 100.0
                      * 2.0 * (0.3 if is_cloudy else 1.0), 2)

                # Daily energy — 5-min integration via prev()
                - name: daily_energy_kwh
                  data_type: float
                  generator:
                    type: derived
                    expression: "round(prev('daily_energy_kwh', 0) + power_kw * 5.0 / 60.0, 2)"

              # Grid curtailment — Array_01 forced to 0 kW at midday
              scheduled_events:
                - type: forced_value
                  entity: Array_01
                  column: power_kw
                  value: 0
                  start_time: "2026-03-01T12:00:00Z"
                  end_time: "2026-03-01T13:00:00Z"

        write:
          connection: output
          format: parquet
          path: bronze/solar_farm.parquet
          mode: overwrite
```

**What makes this realistic:**

- Boolean cloud cover creates intermittency — power drops to 30% when `is_cloudy` is true, mimicking real cloud transients
- Panel temperature derates efficiency (real temp coefficient ~-0.4%/°C above 25°C) — hotter panels produce less power
- `prev()` integrates energy over the day: `daily_energy_kwh[t] = daily_energy_kwh[t-1] + power_kw × 5/60` — exactly how real inverter energy counters work
- Array_03 soiling reduces peak irradiance from 1100 to 850 W/m² — dirty panels lose 20-25% of output
- Grid curtailment event forces Array_01 to zero during peak production (12:00–13:00) — a common real-world constraint

!!! example "Try this"
    - Change `true_probability` to `0.6` for an overcast day and compare daily energy totals
    - Add a `wind_speed_mps` column (random_walk, start 3, min 0, max 15) and modify `panel_temp_c` to subtract wind cooling: `"ambient_temp_c + irradiance_w_m2 * 0.03 - wind_speed_mps * 0.5"`
    - Double to 576 rows (48 hours) and add a second curtailment event on day 2

> 📖 **Learn more:** [Generators Reference](../generators.md) — Boolean generator

---

## Pattern 17: Wind Turbine Fleet {#pattern-17}

**Industry:** Renewables | **Difficulty:** Intermediate

!!! tip "What you'll learn"
    - **`geo` generator for physical locations** — place turbines on a real coordinate grid using `bbox` format
    - **Entity overrides at scale** — differentiate geographic clusters (north vs south ridge) with different wind profiles

A wind turbine fleet has five 2MW turbines split between a northern and southern cluster. The southern ridge is windier, so those turbines get entity overrides with higher wind speeds. Power follows the cubic wind law (P ∝ v³) with real cut-in (3 m/s) and rated speed (12 m/s) thresholds.

```yaml
project: wind_turbine_fleet
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
  - pipeline: wind
    nodes:
      - name: turbine_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-01T00:00:00Z"
                timestep: "10m"
                row_count: 144            # 24 hours
                seed: 42
              entities:
                names: [Turbine_N01, Turbine_N02, Turbine_S01, Turbine_S02, Turbine_S03]
              columns:
                - name: turbine_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                # Physical location — Southern California wind farm
                - name: location
                  data_type: string
                  generator:
                    type: geo
                    bbox: [34.0, -118.5, 34.1, -118.4]
                    format: tuple

                # Wind speed — southern cluster is windier (ridge effect)
                - name: wind_speed_mps
                  data_type: float
                  generator:
                    type: random_walk
                    start: 8.0
                    min: 0.0
                    max: 30.0
                    volatility: 0.8
                    mean_reversion: 0.08
                    precision: 1
                  entity_overrides:
                    Turbine_S01:
                      type: random_walk
                      start: 12.0
                      min: 2.0
                      max: 35.0
                      volatility: 1.0
                      mean_reversion: 0.06
                      precision: 1
                    Turbine_S02:
                      type: random_walk
                      start: 12.0
                      min: 2.0
                      max: 35.0
                      volatility: 1.0
                      mean_reversion: 0.06
                      precision: 1
                    Turbine_S03:
                      type: random_walk
                      start: 12.0
                      min: 2.0
                      max: 35.0
                      volatility: 1.0
                      mean_reversion: 0.06
                      precision: 1

                - name: wind_direction_deg
                  data_type: float
                  generator:
                    type: range
                    min: 0.0
                    max: 360.0

                # Cubic power curve: cut-in 3 m/s, rated at 12 m/s (2MW)
                - name: power_kw
                  data_type: float
                  generator:
                    type: derived
                    expression: >
                      round(0 if wind_speed_mps < 3
                      else (2000 if wind_speed_mps > 25
                      else 2000 * min(1.0, (wind_speed_mps / 12.0) ** 3)), 1)

                - name: rotor_rpm
                  data_type: float
                  generator:
                    type: derived
                    expression: >
                      round(0 if wind_speed_mps < 3
                      else min(15.0, wind_speed_mps * 1.2), 1)

                - name: nacelle_temp_c
                  data_type: float
                  generator:
                    type: random_walk
                    start: 35.0
                    min: 20.0
                    max: 65.0
                    volatility: 0.5
                    mean_reversion: 0.1
                    precision: 1

        write:
          connection: output
          format: parquet
          path: bronze/wind_turbines.parquet
          mode: overwrite
```

**What makes this realistic:**

- Cubic power curve (P ∝ v³) is the real physics of wind energy — power scales with the cube of wind speed
- Cut-in at 3 m/s and rated at 12 m/s match typical 2MW turbine specifications
- `geo` generator places turbines on a real coordinate grid (Southern California wind corridor)
- Southern cluster is windier (start 12 m/s vs 8 m/s, higher max) — ridge effect via entity overrides creates realistic geographic variation
- Nacelle temperature tracks independently — real turbines monitor gearbox/generator housing temperature for overheating

!!! example "Try this"
    - Add a `gearbox_vibration_mm_s` column using `random_walk` with `trend: 0.02` to simulate bearing degradation over time
    - Add a `hub_height_m` constant (80 for north turbines, 100 for south) using entity overrides
    - Lower the cut-in speed to 2.5 m/s to model newer turbine technology and compare power output

> 📖 **Learn more:** [Advanced Features](../advanced_features.md) — Entity overrides | [Generators Reference](../generators.md) — Geo generator

---

## Pattern 18: Battery Energy Storage System (BESS) {#pattern-18}

**Industry:** Energy Storage | **Difficulty:** Intermediate

!!! tip "What you'll learn"
    - **`prev()` for state-of-charge integration** — Coulomb counting, the same algorithm used in real battery management systems. SOC[t] = SOC[t-1] + current × Δt / capacity

A battery energy storage system has two modules that charge and discharge throughout the day. State-of-charge (SOC) is tracked using `prev()` to implement Coulomb counting — the standard BMS algorithm. During peak demand (4–8 PM), a scheduled event forces both modules into discharge mode for peak shaving.

```yaml
project: bess_storage
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
  - pipeline: bess
    nodes:
      - name: bess_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-01T00:00:00Z"
                timestep: "5m"
                row_count: 288            # 24 hours
                seed: 42
              entities:
                names: [BESS_Module_01, BESS_Module_02]
              columns:
                - name: module_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                # Current: positive = charge, negative = discharge
                - name: current_a
                  data_type: float
                  generator:
                    type: random_walk
                    start: 0.0
                    min: -30.0
                    max: 30.0
                    volatility: 2.0
                    mean_reversion: 0.05
                    precision: 1

                - name: voltage_v
                  data_type: float
                  generator:
                    type: random_walk
                    start: 400.0
                    min: 360.0
                    max: 420.0
                    volatility: 1.0
                    mean_reversion: 0.1
                    precision: 1

                # SOC via Coulomb counting — 100Ah capacity, 5min interval
                # SOC[t] = SOC[t-1] + I × (5/60) / 100 × 100%
                # Clamped to 5–95% (battery protection)
                - name: soc_pct
                  data_type: float
                  generator:
                    type: derived
                    expression: >
                      round(max(5, min(95,
                      prev('soc_pct', 50)
                      + current_a * 5.0 / 60.0 / 100.0 * 100)), 1)

                - name: power_kw
                  data_type: float
                  generator:
                    type: derived
                    expression: "round(current_a * voltage_v / 1000.0, 2)"

                - name: cycle_state
                  data_type: string
                  generator:
                    type: derived
                    expression: "'charging' if current_a > 1 else 'discharging' if current_a < -1 else 'idle'"

                - name: cell_temp_c
                  data_type: float
                  generator:
                    type: random_walk
                    start: 25.0
                    min: 15.0
                    max: 45.0
                    volatility: 0.3
                    mean_reversion: 0.1
                    precision: 1

              # Peak shaving — force discharge during utility peak (4–8 PM)
              scheduled_events:
                - type: forced_value
                  entity: null
                  column: current_a
                  value: -25
                  start_time: "2026-03-01T16:00:00Z"
                  end_time: "2026-03-01T20:00:00Z"

        write:
          connection: output
          format: parquet
          path: bronze/bess_storage.parquet
          mode: overwrite
```

**What makes this realistic:**

- Coulomb counting via `prev()` is the real BMS algorithm: SOC[t] = SOC[t-1] + I × Δt / C
- SOC clamped to 5–95% — real batteries protect against deep discharge (below 5%) and overcharge (above 95%) to preserve cell life
- Peak shaving event at 4–8 PM forces discharge at -25A, matching utility peak demand windows
- `entity: null` applies the peak shaving to both modules simultaneously — plant-wide dispatch command
- Power computed from I × V matches real electrical measurement (P = IV)

!!! example "Try this"
    - Change capacity from 100Ah to 200Ah by halving the current factor: replace `/ 100.0` with `/ 200.0` in the SOC expression
    - Add a `degradation_pct` column using `random_walk` with `trend: 0.001` to track long-term capacity fade
    - Increase the discharge current to -30A during peak shaving and watch the SOC drop faster

> 📖 **Learn more:** [Stateful Functions](../stateful_functions.md) — `prev()` for integration patterns

---

## Pattern 19: Smart Meter Network {#pattern-19}

**Industry:** Utilities | **Difficulty:** Intermediate

!!! tip "What you'll learn"
    - **`ipv4` generator for network addresses** — generate realistic IP addresses within a subnet using CIDR notation
    - **High entity count with `count:` and `id_prefix:`** — simulate 50 meters at once without listing each name

A utility deploys 50 smart meters across a distribution network. Each meter has a network IP address on a private subnet, reports consumption every 15 minutes, and occasionally retransmits data (duplicates). The `count: 50` entity shorthand generates all 50 meters with sequential IDs — no need to list each name.

```yaml
project: smart_meter_network
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
  - pipeline: meters
    nodes:
      - name: meter_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-01T00:00:00Z"
                timestep: "15m"
                row_count: 96             # 24 hours
                seed: 42
              entities:
                count: 50
                id_prefix: "meter_"
              columns:
                - name: meter_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                # Network address — /16 subnet accommodates large populations
                - name: ip_address
                  data_type: string
                  generator:
                    type: ipv4
                    subnet: "10.100.0.0/16"

                - name: consumption_kwh
                  data_type: float
                  generator:
                    type: random_walk
                    start: 0.5
                    min: 0.0
                    max: 5.0
                    volatility: 0.2
                    mean_reversion: 0.1
                    precision: 3

                # Voltage clustered around 240V (utility grid spec)
                - name: voltage_v
                  data_type: float
                  generator:
                    type: range
                    min: 228.0
                    max: 252.0
                    distribution: normal
                    mean: 240.0
                    std_dev: 3.0

                - name: power_factor
                  data_type: float
                  generator:
                    type: range
                    min: 0.85
                    max: 1.0
                    distribution: normal
                    mean: 0.95
                    std_dev: 0.03

                - name: signal_strength_dbm
                  data_type: float
                  generator:
                    type: range
                    min: -90.0
                    max: -30.0
                    distribution: normal
                    mean: -60.0
                    std_dev: 10.0

                - name: meter_status
                  data_type: string
                  generator:
                    type: categorical
                    values: [active, warning, offline]
                    weights: [0.95, 0.04, 0.01]

              chaos:
                outlier_rate: 0.01
                outlier_factor: 3.0
                duplicate_rate: 0.005     # AMI network retransmissions

        write:
          connection: output
          format: parquet
          path: bronze/smart_meters.parquet
          mode: overwrite
```

**What makes this realistic:**

- `ipv4` on a `/16` subnet (65,534 hosts) accommodates large meter populations — real AMI networks use private IP ranges
- 50 entities at once stress-tests your pipeline — `count: 50` generates 4,800 rows (50 × 96) in one simulation
- Voltage clustered around 240V with normal distribution (σ=3V) matches utility grid specifications (±5% tolerance)
- `duplicate_rate: 0.005` simulates real AMI network retransmissions — meters occasionally resend readings over lossy RF networks
- Signal strength in dBm with normal distribution around -60 dBm matches typical RF mesh network behavior

!!! example "Try this"
    - Increase to `count: 500` for a real-scale deployment and measure pipeline performance
    - Add a `daily_total_kwh` derived column using `prev()`: `"round(prev('daily_total_kwh', 0) + consumption_kwh, 3)"`
    - Add a `tamper_flag` boolean column with `true_probability: 0.001` for theft detection analytics

> 📖 **Learn more:** [Generators Reference](../generators.md) — IPv4 generator and subnet notation

---

## Pattern 20: EV Charging Stations {#pattern-20}

**Industry:** Transportation | **Difficulty:** Intermediate

!!! tip "What you'll learn"
    - **`uuid` v5 for deterministic session IDs** — same seed produces the same IDs every run, making test assertions reproducible
    - **`email` generator for user contact** — generates realistic email addresses tied to entity names

An EV charging network has four stations — two DC fast chargers (150kW) and two AC Level 2 chargers (22kW). Each charging session gets a deterministic UUID (reproducible for testing) and a user contact email. The charger type and power level are derived from the entity name, so adding a new station automatically gets the right power profile.

```yaml
project: ev_charging
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
  - pipeline: charging
    nodes:
      - name: charging_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-01T00:00:00Z"
                timestep: "10m"
                row_count: 144            # 24 hours
                seed: 42
              entities:
                names: [Station_A_Fast, Station_A_Slow, Station_B_Fast, Station_B_Slow]
              columns:
                - name: station_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                # Deterministic session IDs — same seed = same UUIDs
                - name: session_id
                  data_type: string
                  generator:
                    type: uuid
                    version: 5

                # User contact email tied to entity name
                - name: user_email
                  data_type: string
                  generator:
                    type: email
                    domain: "evcharge.example.com"
                    pattern: "{entity}_{index}"

                # Charger type derived from entity name
                - name: charger_type
                  data_type: string
                  generator:
                    type: derived
                    expression: "'DC_Fast' if 'Fast' in entity_id else 'AC_Level2'"

                # Power: DC Fast ~150kW, AC Level 2 ~22kW
                - name: power_kw
                  data_type: float
                  generator:
                    type: derived
                    expression: >
                      150.0 + random() * 10
                      if 'Fast' in entity_id
                      else 22.0 + random() * 3

                # Energy delivered per 10-min interval
                - name: energy_delivered_kwh
                  data_type: float
                  generator:
                    type: derived
                    expression: "round(power_kw * 10.0 / 60.0, 2)"

                # Session active 60% of the time
                - name: session_active
                  data_type: boolean
                  generator:
                    type: boolean
                    true_probability: 0.6

                - name: payment_status
                  data_type: string
                  generator:
                    type: categorical
                    values: [completed, pending, failed]
                    weights: [0.92, 0.06, 0.02]

        write:
          connection: output
          format: parquet
          path: bronze/ev_charging.parquet
          mode: overwrite
```

**What makes this realistic:**

- UUID v5 gives deterministic session IDs — same seed produces the same IDs every run, making test assertions stable and reproducible
- Email addresses tied to entity names create realistic user contact records for each station
- Power levels match real charger specs: DC fast chargers deliver ~150kW, AC Level 2 chargers deliver ~22kW
- `charger_type` derived from `entity_id` pattern — adding a new "Fast" station automatically gets the DC Fast profile
- Energy delivered per interval (`power_kw × 10/60`) converts power to energy correctly — the same kW-to-kWh conversion used by real charge point operators

!!! example "Try this"
    - Add `soc_start_pct` and `soc_end_pct` columns using `range` (start: 10–40%, end: 80–100%) to model battery state before and after charging
    - Add a `pricing_tier` categorical column with values `[peak, off_peak, super_off_peak]` and weights `[0.3, 0.5, 0.2]`
    - Add chaos with `outlier_rate: 0.01` to simulate communication glitches between charger and backend

> 📖 **Learn more:** [Generators Reference](../generators.md) — UUID generator (v4 vs v5) and email generator

---

← [Process Engineering (9–15)](process_engineering.md) | [Manufacturing & Operations (21–25)](manufacturing.md) →
