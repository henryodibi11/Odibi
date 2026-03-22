---
title: "Healthcare & Life Sciences (29-30)"
---

# Healthcare & Life Sciences (29–30)

Clinical monitoring and pharmaceutical manufacturing patterns. These patterns show high-frequency data collection, alarm threshold logic, and batch process recipes with phase transitions.

!!! info "Prerequisites"
    These patterns build on [Foundational Patterns 1–8](foundations.md). Pattern 29 uses high-frequency timesteps; Pattern 30 uses `prev()` and `scheduled_events` extensively — see [Stateful Functions](../stateful_functions.md) and [Advanced Features](../advanced_features.md).

---

## Pattern 29: ICU Patient Vitals {#pattern-29}

**Industry:** Healthcare | **Difficulty:** Advanced

!!! tip "What you'll learn"
    - **High-frequency data** — 30-second timestep with 2880 rows (24 hours) tests pipeline performance with dense time series. Also demonstrates deriving multi-condition clinical alerts from vital sign thresholds.

Intensive care monitors sample patient vitals every 15–30 seconds, generating massive time series. This pattern simulates 4 ICU beds over 24 hours at 30-second resolution — 11,520 total rows. Patient_Bed_03 is a deteriorating patient with rising heart rate and falling SpO2, culminating in a cardiac episode at 14:00. The `critical_alert` column applies Modified Early Warning Score (MEWS) thresholds to flag clinical interventions.

```yaml
project: icu_monitoring
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
  - pipeline: icu_vitals
    nodes:
      - name: vitals_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-10T00:00:00Z"
                timestep: "30s"
                row_count: 2880           # 24 hours
                seed: 42
              entities:
                names: [Patient_Bed_01, Patient_Bed_02, Patient_Bed_03, Patient_Bed_04]
              columns:
                - name: bed_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                # Heart rate — normal ~75 bpm
                - name: heart_rate_bpm
                  data_type: float
                  generator:
                    type: random_walk
                    start: 75
                    min: 40
                    max: 180
                    volatility: 1.5
                    mean_reversion: 0.1
                    precision: 0
                  entity_overrides:
                    Patient_Bed_03:        # Deteriorating — HR trending up
                      type: random_walk
                      start: 85
                      min: 40
                      max: 180
                      volatility: 2.0
                      mean_reversion: 0.05
                      trend: 0.02
                      precision: 0

                # Systolic blood pressure
                - name: blood_pressure_sys
                  data_type: float
                  generator:
                    type: random_walk
                    start: 120
                    min: 70
                    max: 200
                    volatility: 2.0
                    mean_reversion: 0.1
                    precision: 0

                # Diastolic ~65% of systolic
                - name: blood_pressure_dia
                  data_type: float
                  generator:
                    type: derived
                    expression: "round(blood_pressure_sys * 0.65 + random() * 5, 0)"

                # SpO2 — normal ~97%
                - name: spo2_pct
                  data_type: float
                  generator:
                    type: random_walk
                    start: 97
                    min: 80
                    max: 100
                    volatility: 0.3
                    mean_reversion: 0.2
                    precision: 0
                  entity_overrides:
                    Patient_Bed_03:        # Deteriorating — SpO2 drifting down
                      type: random_walk
                      start: 95
                      min: 80
                      max: 100
                      volatility: 0.5
                      mean_reversion: 0.2
                      trend: -0.005
                      precision: 0

                # Respiratory rate — normal ~16 breaths/min
                - name: respiratory_rate
                  data_type: float
                  generator:
                    type: range
                    min: 12
                    max: 24
                    distribution: normal
                    mean: 16
                    std_dev: 2

                # Body temperature
                - name: temperature_c
                  data_type: float
                  generator:
                    type: random_walk
                    start: 37.0
                    min: 35.5
                    max: 40.0
                    volatility: 0.05
                    mean_reversion: 0.15
                    precision: 1

                # Clinical alert based on MEWS-style thresholds
                - name: critical_alert
                  data_type: string
                  generator:
                    type: derived
                    expression: "'CRITICAL' if heart_rate_bpm > 150 or heart_rate_bpm < 45 or spo2_pct < 88 or blood_pressure_sys > 180 or blood_pressure_sys < 80 else 'WARNING' if heart_rate_bpm > 120 or spo2_pct < 92 or blood_pressure_sys > 160 else 'NORMAL'"

              # Cardiac episode — Patient_Bed_03 HR spikes to 155 for 15 minutes
              scheduled_events:
                - type: forced_value
                  entity: Patient_Bed_03
                  column: heart_rate_bpm
                  value: 155
                  start_time: "2026-03-10T14:00:00Z"
                  end_time: "2026-03-10T14:15:00Z"

              chaos:
                outlier_rate: 0.002       # Sensor artifacts
                outlier_factor: 1.5

        write:
          connection: output
          format: parquet
          path: bronze/icu_vitals.parquet
          mode: overwrite
```

**What makes this realistic:**

- 30-second sampling matches real ICU monitor rates (many monitors sample every 15–30s)
- Mean reversion keeps vitals in physiological range — heart rate reverts to its `start` value (75 bpm), SpO2 to its `start` value (97%)
- Patient_Bed_03 has deteriorating trends (rising HR, falling SpO2) — this is what clinical early warning scores detect
- Critical alert thresholds match Modified Early Warning Score (MEWS) criteria
- Blood pressure diastolic derived from systolic maintains realistic pulse pressure
- 2880 rows × 4 patients = 11,520 total rows — stress-tests your pipeline

!!! example "Try this"
    - Add an `ews_score` derived column: `"int(heart_rate_bpm > 120) + int(spo2_pct < 92) + int(blood_pressure_sys > 160 or blood_pressure_sys < 90) + int(respiratory_rate > 20 or respiratory_rate < 10) + int(temperature_c > 38.5)"`
    - Change Patient_Bed_03's trend to `-0.01` on `spo2_pct` for faster deterioration
    - Add a `nurse_call` boolean derived from `critical_alert == 'CRITICAL'`

> 📖 **Learn more:** [Generators Reference](../generators.md) — Random walk with mean_reversion_to and trend parameters

---

## Pattern 30: Pharmaceutical Batch Records {#pattern-30}

**Industry:** Pharmaceutical | **Difficulty:** Advanced

!!! tip "What you'll learn"
    - **`sequential` generator** — for batch numbering. **`prev()` for growth curve modeling** — viable cell count doubles over time. **Scheduled events for recipe phase transitions** — inoculation → growth → harvest.

A biopharmaceutical batch runs through three phases: inoculation (seed the reactor), growth (maximize cell density), and harvest (cool down and collect product). This pattern uses `prev()` to model exponential cell growth, `scheduled_events` to transition between recipe phases, and forced values to change agitation speed and temperature at each phase boundary — exactly how real bioreactor recipe managers work.

```yaml
project: pharma_batch
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
  - pipeline: batch_records
    nodes:
      - name: batch_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-10T00:00:00Z"
                timestep: "5m"
                row_count: 96             # 8 hours
                seed: 42
              entities:
                names: [Batch_2026_001, Batch_2026_002]
              columns:
                - name: batch_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                # Sequential batch record number
                - name: batch_number
                  data_type: int
                  generator:
                    type: sequential
                    start: 1
                    step: 1

                # Phase — overridden by scheduled events
                - name: phase
                  data_type: string
                  generator: {type: constant, value: "inoculation"}

                # Reactor temperature — tightly controlled
                - name: reactor_temp_c
                  data_type: float
                  generator:
                    type: random_walk
                    start: 37.0
                    min: 35.0
                    max: 39.0
                    volatility: 0.1
                    mean_reversion: 0.2
                    precision: 1

                # pH — tightly controlled around 7.2
                - name: ph
                  data_type: float
                  generator:
                    type: random_walk
                    start: 7.2
                    min: 6.8
                    max: 7.6
                    volatility: 0.02
                    mean_reversion: 0.15
                    precision: 2

                # Dissolved oxygen
                - name: dissolved_o2_pct
                  data_type: float
                  generator:
                    type: range
                    min: 30
                    max: 80
                    distribution: normal
                    mean: 55
                    std_dev: 8

                # Agitation speed — overridden by scheduled events
                - name: agitation_rpm
                  data_type: int
                  generator: {type: constant, value: 150}

                # Viable cell count — exponential growth via prev()
                - name: viable_cells_million
                  data_type: float
                  generator:
                    type: derived
                    expression: "round(max(0.1, prev('viable_cells_million', 0.5) * (1.0 + 0.008 + random() * 0.004)), 2)"

                # Yield against target cell density (50M cells/mL = 100%)
                - name: yield_pct
                  data_type: float
                  generator:
                    type: derived
                    expression: "round(min(100, viable_cells_million / 50.0 * 100), 1)"

                # GMP quality flag — process parameter limits
                - name: quality_flag
                  data_type: string
                  generator:
                    type: derived
                    expression: "'out_of_spec' if ph < 6.9 or ph > 7.5 or reactor_temp_c < 35.5 or reactor_temp_c > 38.5 else 'in_spec'"

              # Recipe phase transitions via scheduled events
              scheduled_events:
                # Phase 2: Growth (hours 2–6) — increase agitation for O2 transfer
                - type: forced_value
                  entity: null
                  column: phase
                  value: "growth"
                  start_time: "2026-03-10T02:00:00Z"
                  end_time: "2026-03-10T06:00:00Z"
                - type: forced_value
                  entity: null
                  column: agitation_rpm
                  value: 250
                  start_time: "2026-03-10T02:00:00Z"
                  end_time: "2026-03-10T06:00:00Z"

                # Phase 3: Harvest (hours 6–8) — cool down, reduce agitation
                - type: forced_value
                  entity: null
                  column: phase
                  value: "harvest"
                  start_time: "2026-03-10T06:00:00Z"
                  end_time: "2026-03-10T08:00:00Z"
                - type: forced_value
                  entity: null
                  column: agitation_rpm
                  value: 80
                  start_time: "2026-03-10T06:00:00Z"
                  end_time: "2026-03-10T08:00:00Z"
                - type: forced_value
                  entity: null
                  column: reactor_temp_c
                  value: 4.0
                  start_time: "2026-03-10T06:00:00Z"
                  end_time: "2026-03-10T08:00:00Z"

        write:
          connection: output
          format: parquet
          path: bronze/pharma_batch.parquet
          mode: overwrite
```

**What makes this realistic:**

- Cell growth via `prev()` follows exponential kinetics — viable_cells doubles roughly every 7 hours (realistic for CHO cells)
- Recipe phases change via scheduled events — inoculation (gentle start), growth (high agitation for O2 transfer), harvest (cool down to 4°C to preserve product)
- pH and temperature tightly controlled with mean_reversion — matches real bioreactor PID control behavior
- Quality flag checks process parameter limits (35.5–38.5°C, pH 6.9–7.5) — real GMP batch record requirements
- Yield calculated against a target cell density (50M cells/mL) — standard biopharma KPI
- Harvest phase forces reactor_temp to 4°C — real cold harvest preserves cell viability and product quality

!!! example "Try this"
    - Add a `nutrient_feed_rate` column that increases during the growth phase (use `scheduled_events`)
    - Add `co2_pct` that correlates with viable_cells (metabolic byproduct): `"viable_cells_million * 0.1 + random() * 0.5"`
    - Add validation to catch `quality_flag == 'out_of_spec'` rows

> 📖 **Learn more:** [Stateful Functions](../stateful_functions.md) — `prev()` for growth curves | [Advanced Features](../advanced_features.md) — Scheduled events for recipe phases

---

← [Environmental & Agriculture (26–28)](environmental.md) | [Business & IT (31–35)](business_it.md) →
