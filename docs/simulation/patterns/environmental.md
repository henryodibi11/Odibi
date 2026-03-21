---
title: "Environmental & Agriculture (26-28)"
---

# Environmental & Agriculture (26–28)

Weather monitoring, air quality, and precision agriculture patterns. These patterns show multi-sensor networks with geographic positioning, seasonal trends, and PID control for environmental management.

!!! info "Prerequisites"
    These patterns build on [Foundational Patterns 1–8](foundations.md). Pattern 28 requires familiarity with [Stateful Functions](../stateful_functions.md) (`pid()`).

---

## Pattern 26: Weather Station Network {#pattern-26}

**Industry:** Meteorology | **Difficulty:** Intermediate

!!! tip "What you'll learn"
    - **`geo` generator with bbox** — place stations across a geographic bounding box for realistic spatial spread
    - **Multi-sensor entity** — one entity generates temperature, humidity, wind, pressure, and precipitation simultaneously

A network of 10 weather stations reports conditions every 15 minutes across the NYC metro area. Each station is a single entity that produces multiple sensor readings per timestep — temperature, humidity, wind, pressure, and precipitation. The `geo` generator with a bounding box places stations at realistic geographic coordinates.

```yaml
project: weather_stations
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
  - pipeline: weather
    nodes:
      - name: wx_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-10T00:00:00Z"
                timestep: "15m"
                row_count: 96             # 24 hours
                seed: 42
              entities:
                count: 10
                id_prefix: "wx_"
              columns:
                - name: station_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                # Station placement across NYC metro area
                - name: location
                  data_type: string
                  generator:
                    type: geo
                    bbox: [40.0, -74.5, 41.0, -73.5]
                    format: lat_lon_separate

                - name: temperature_c
                  data_type: float
                  generator:
                    type: random_walk
                    start: 18.0
                    min: 5.0
                    max: 35.0
                    volatility: 0.3
                    mean_reversion: 0.08
                    precision: 1

                - name: humidity_pct
                  data_type: float
                  generator:
                    type: range
                    min: 30
                    max: 95
                    distribution: normal
                    mean: 65
                    std_dev: 12

                - name: wind_speed_mps
                  data_type: float
                  generator:
                    type: random_walk
                    start: 3.0
                    min: 0.0
                    max: 20.0
                    volatility: 0.5
                    mean_reversion: 0.1
                    precision: 1

                - name: wind_direction_deg
                  data_type: float
                  generator:
                    type: range
                    min: 0
                    max: 360

                - name: barometric_pressure_hpa
                  data_type: float
                  generator:
                    type: random_walk
                    start: 1013.25
                    min: 990
                    max: 1040
                    volatility: 0.3
                    mean_reversion: 0.15
                    precision: 1

                - name: is_precipitating
                  data_type: boolean
                  generator: {type: boolean, true_probability: 0.15}

                # Simplified heat index combining temperature and humidity
                - name: heat_index_c
                  data_type: float
                  generator:
                    type: derived
                    expression: "round(temperature_c + 0.5 * (humidity_pct / 100.0) * temperature_c, 1)"

              chaos:
                outlier_rate: 0.005       # Sensor glitches
                outlier_factor: 2.5

        write:
          connection: output
          format: parquet
          path: bronze/weather_stations.parquet
          mode: overwrite
```

**What makes this realistic:**

- 10 stations across a geo bbox gives geographic spread — each station gets unique coordinates within the NYC metro area
- Barometric pressure uses `random_walk` with tight `mean_reversion: 0.15` — weather changes slowly, not randomly
- Precipitation as a boolean with 15% probability matches a temperate climate's rain frequency
- Heat index combines temperature and humidity — the derived column uses both upstream values in a single formula

!!! example "Try this"
    - Change the bbox to your city's coordinates (e.g., `[34.0, -118.5, 34.2, -118.0]` for Los Angeles)
    - Add a `dew_point_c` derived column: `"round(temperature_c - (100 - humidity_pct) / 5.0, 1)"`
    - Add a `wind_chill_c` column for cold weather: `"round(13.12 + 0.6215 * temperature_c - 11.37 * wind_speed_mps ** 0.16 + 0.3965 * temperature_c * wind_speed_mps ** 0.16, 1)"`

> 📖 **Learn more:** [Generators Reference](../generators.md) — Geo generator bbox and format options

---

## Pattern 27: Air Quality Monitoring {#pattern-27}

**Industry:** Environmental | **Difficulty:** Intermediate

!!! tip "What you'll learn"
    - **`trend` parameter on random_walk** — gradual seasonal or pollution drift that accumulates over time
    - **`entity_overrides`** — make specific monitoring sites worse than others to model geographic pollution variation

Four air quality monitoring stations track PM2.5, PM10, ozone, CO, and NO₂ across different land-use zones. The `trend` parameter on `random_walk` causes PM2.5 to drift upward over the week, simulating accumulating pollution. Entity overrides make the industrial and highway sites significantly worse than suburban and downtown stations.

```yaml
project: air_quality
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
  - pipeline: aq_monitoring
    nodes:
      - name: aq_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-10T00:00:00Z"
                timestep: "1h"
                row_count: 168            # 7 days
                seed: 42
              entities:
                names: [AQM_Downtown, AQM_Industrial, AQM_Suburban, AQM_Highway]
              columns:
                - name: station_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                # PM2.5 with upward trend — accumulating pollution
                - name: pm25_ug_m3
                  data_type: float
                  generator:
                    type: random_walk
                    start: 12.0
                    min: 0.0
                    max: 200.0
                    volatility: 2.0
                    mean_reversion: 0.05
                    trend: 0.03
                    precision: 1
                  entity_overrides:
                    AQM_Industrial:        # Worse air quality near factories
                      type: random_walk
                      start: 35.0
                      min: 5.0
                      max: 300.0
                      volatility: 4.0
                      mean_reversion: 0.03
                      trend: 0.05
                      precision: 1

                # PM10 typically ~1.5-2x PM2.5
                - name: pm10_ug_m3
                  data_type: float
                  generator:
                    type: derived
                    expression: "round(pm25_ug_m3 * 1.8 + random() * 5, 1)"

                - name: ozone_ppb
                  data_type: float
                  generator:
                    type: random_walk
                    start: 30.0
                    min: 0.0
                    max: 120.0
                    volatility: 3.0
                    mean_reversion: 0.08
                    precision: 1

                - name: co_ppm
                  data_type: float
                  generator:
                    type: range
                    min: 0.1
                    max: 4.0
                    distribution: normal
                    mean: 0.8
                    std_dev: 0.5
                  entity_overrides:
                    AQM_Highway:            # Highway traffic — elevated CO
                      type: range
                      min: 0.5
                      max: 8.0
                      distribution: normal
                      mean: 3.0
                      std_dev: 1.0

                - name: no2_ppb
                  data_type: float
                  generator:
                    type: range
                    min: 5
                    max: 60
                    distribution: normal
                    mean: 25
                    std_dev: 10

                # Simplified AQI — worst pollutant determines score
                - name: aqi
                  data_type: int
                  generator:
                    type: derived
                    expression: "int(max(pm25_ug_m3 * 2.0, ozone_ppb * 1.5, co_ppm * 25))"

                # Health advisory mapped to EPA breakpoints
                - name: health_advisory
                  data_type: string
                  generator:
                    type: derived
                    expression: "'hazardous' if aqi > 300 else 'very_unhealthy' if aqi > 200 else 'unhealthy' if aqi > 150 else 'unhealthy_sensitive' if aqi > 100 else 'moderate' if aqi > 50 else 'good'"

        write:
          connection: output
          format: parquet
          path: bronze/air_quality.parquet
          mode: overwrite
```

**What makes this realistic:**

- The `trend` parameter causes PM2.5 to drift upward over the week — accumulating pollution that doesn't fully dissipate
- Industrial site has a higher baseline (`start: 35.0`) and faster drift (`trend: 0.05`) — factories emit more particulate matter
- Highway site has elevated CO (`mean: 3.0` vs `0.8`) — vehicle exhaust is the primary source of carbon monoxide
- AQI is calculated from the worst pollutant using `max()` — this matches the real EPA methodology
- Health advisory maps to EPA breakpoints — good/moderate/unhealthy/hazardous categories

!!! example "Try this"
    - Add a wildfire event using `scheduled_events` — force `pm25_ug_m3` to 150+ for `AQM_Suburban` for 48 hours
    - Add a `wind_speed` column and make PM2.5 inversely affected: lower wind = higher pollution concentration
    - Extend to 30 days (`row_count: 720`) to see how the trend parameter causes pollution to accumulate over time

> 📖 **Learn more:** [Generators Reference](../generators.md) — Random walk trend parameter

---

## Pattern 28: Greenhouse / Indoor Farm {#pattern-28}

**Industry:** Agriculture | **Difficulty:** Advanced

!!! tip "What you'll learn"
    - **`pid()` for closed-loop environmental control** — the greenhouse controller adjusts vent position to maintain target temperature
    - **`prev()` for first-order thermal dynamics** — temperature can't change instantly; it follows a time-constant model
    - **Scheduled setpoint changes** — night mode lowers the temperature target, modeling real DIF strategy

Two greenhouse zones maintain optimal growing conditions using PID-controlled ventilation. The `pid()` function adjusts vent position to keep actual temperature near the setpoint, while `prev()` models first-order thermal dynamics — the greenhouse has thermal mass, so temperature responds gradually. At night, scheduled events lower both the temperature setpoint and solar load, triggering the controller to close vents.

```yaml
project: greenhouse_control
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
  - pipeline: greenhouse
    nodes:
      - name: gh_data
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
                names: [GH_Zone_01, GH_Zone_02]
              columns:
                - name: zone_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                # Daytime temperature target
                - name: temp_setpoint_c
                  data_type: float
                  generator: {type: constant, value: 25.0}

                - name: ambient_temp_c
                  data_type: float
                  generator:
                    type: random_walk
                    start: 18.0
                    min: 5.0
                    max: 35.0
                    volatility: 0.3
                    mean_reversion: 0.05
                    precision: 1

                - name: solar_load_w
                  data_type: float
                  generator:
                    type: random_walk
                    start: 300
                    min: 0
                    max: 800
                    volatility: 20
                    mean_reversion: 0.08
                    precision: 0

                # First-order thermal model (tau ~50min)
                # Temperature responds gradually to ambient + solar heat gain
                - name: actual_temp_c
                  data_type: float
                  generator:
                    type: derived
                    expression: "round(prev('actual_temp_c', 22.0) + (ambient_temp_c - prev('actual_temp_c', 22.0) + solar_load_w * 0.005) * 0.1, 1)"

                # PID controller: Kp=5, Ki=0.5, Kd=1.0, dt=300s, output 0-100%
                - name: vent_position_pct
                  data_type: float
                  generator:
                    type: derived
                    expression: "round(max(0, min(100, pid(actual_temp_c, temp_setpoint_c, 5.0, 0.5, 1.0, 300, 0, 100, True))), 1)"

                - name: humidity_pct
                  data_type: float
                  generator:
                    type: random_walk
                    start: 70
                    min: 40
                    max: 95
                    volatility: 1.0
                    mean_reversion: 0.1
                    precision: 1

                - name: soil_moisture_pct
                  data_type: float
                  generator:
                    type: random_walk
                    start: 60
                    min: 20
                    max: 90
                    volatility: 0.5
                    mean_reversion: 0.08
                    precision: 1

                - name: light_ppfd
                  data_type: float
                  generator:
                    type: random_walk
                    start: 400
                    min: 0
                    max: 1200
                    volatility: 30
                    mean_reversion: 0.1
                    precision: 0

                # Vapor pressure deficit — key metric for plant transpiration
                - name: vpd_kpa
                  data_type: float
                  generator:
                    type: derived
                    expression: "round(0.6108 * 2.718 ** (17.27 * actual_temp_c / (actual_temp_c + 237.3)) * (1 - humidity_pct / 100.0), 2)"

              # Night mode: lower setpoint and zero solar load
              scheduled_events:
                - type: forced_value
                  entity: null
                  column: temp_setpoint_c
                  value: 18.0
                  start_time: "2026-03-10T20:00:00Z"
                  end_time: "2026-03-11T06:00:00Z"
                - type: forced_value
                  entity: null
                  column: solar_load_w
                  value: 0
                  start_time: "2026-03-10T20:00:00Z"
                  end_time: "2026-03-11T06:00:00Z"

        write:
          connection: output
          format: parquet
          path: bronze/greenhouse_control.parquet
          mode: overwrite
```

**What makes this realistic:**

- `pid()` adjusts vent position to maintain the temperature setpoint — this is how real greenhouse HVAC controllers work
- First-order thermal model via `prev()` means temperature can't change instantly — the greenhouse has thermal mass with a ~50-minute time constant
- Solar load adds heat gain during the day — the derived expression accounts for both ambient temperature and solar radiation
- VPD (vapor pressure deficit) is the key metric for plant transpiration — growers use it to optimize watering and ventilation
- Night mode lowers the setpoint from 25°C to 18°C — this is a real DIF strategy (temperature differential) that promotes stem growth in many crops

!!! example "Try this"
    - Change `Kp` from `5.0` to `15.0` to see more aggressive control response — watch for oscillation in `vent_position_pct`
    - Add an `irrigation_valve` derived column: `"100.0 if soil_moisture_pct < 40.0 else 0.0"` — open valve when soil is dry
    - Add a `co2_ppm` column with `random_walk` (start 400, min 350, max 1200) for CO₂ enrichment monitoring

> 📖 **Learn more:** [Stateful Functions](../stateful_functions.md) — `pid()` for closed-loop control | [Process Simulation](../process_simulation.md) — PID tuning

---

← [Manufacturing & Operations (21–25)](manufacturing.md) | [Healthcare & Life Sciences (29–30)](healthcare.md) →
