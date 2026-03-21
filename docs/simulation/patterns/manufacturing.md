---
title: "Manufacturing & Operations (21-25)"
---

# Manufacturing & Operations (21–25)

Discrete manufacturing, logistics, quality control, and supply chain patterns. These patterns show how to simulate production data with validation gates, multi-pipeline projects, and cross-station flows.

!!! info "Prerequisites"
    These patterns build on [Foundational Patterns 1–8](foundations.md). Pattern 2 (Manufacturing Production Line) is the starting point for this category.

---

## Pattern 21: Packaging Line with SPC {#pattern-21}

**Industry:** Food & Beverage | **Difficulty:** Intermediate

!!! tip "What you'll learn"
    - **Validation on simulated data** — run the same validation tests on simulated data that you'd run on production data. This closes the loop: simulate → validate → fix your validation rules → go live with confidence.

A packaging line fills, seals, and labels products at ~60 packages per minute. Fill weight is the critical SPC parameter — it must stay within a tight band around 500g. By adding a `validation` section to the write node, you test your quality gates *before* real data ever flows through the pipeline.

```yaml
project: packaging_spc
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
  - pipeline: packaging
    nodes:
      - name: spc_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-10T06:00:00Z"
                timestep: "1m"
                row_count: 480            # 8-hour shift
                seed: 42
              entities:
                names: [Line_01]
              columns:
                - name: line_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                # Fill weight clustered around 500g (SPC target)
                - name: fill_weight_g
                  data_type: float
                  generator:
                    type: range
                    min: 495.0
                    max: 505.0
                    distribution: normal
                    mean: 500.0
                    std_dev: 1.2

                - name: seal_pressure_bar
                  data_type: float
                  generator:
                    type: random_walk
                    start: 3.0
                    min: 2.0
                    max: 4.5
                    volatility: 0.1
                    mean_reversion: 0.15
                    precision: 2

                # Label deviation from center
                - name: label_position_mm
                  data_type: float
                  generator:
                    type: range
                    min: -1.0
                    max: 1.0
                    distribution: normal
                    mean: 0.0
                    std_dev: 0.3

                - name: packages_per_min
                  data_type: float
                  generator:
                    type: range
                    min: 55.0
                    max: 65.0
                    distribution: normal
                    mean: 60.0
                    std_dev: 2.0

                # Reject if fill weight out of spec or label misaligned
                - name: reject_flag
                  data_type: boolean
                  generator:
                    type: derived
                    expression: "fill_weight_g < 497 or fill_weight_g > 503 or abs(label_position_mm) > 0.8"

              chaos:
                outlier_rate: 0.008       # ~4 outliers per shift
                outlier_factor: 2.5

        validation:
          mode: quarantine
          tests:
            - type: range
              column: fill_weight_g
              min: 496.0
              max: 504.0
              on_fail: quarantine
            - type: not_null
              columns: [fill_weight_g, seal_pressure_bar]
              on_fail: quarantine
          quarantine:
            connection: output
            path: quarantine/packaging_rejects
            add_columns:
              rejection_reason: true
              rejected_at: true

        write:
          connection: output
          format: parquet
          path: bronze/packaging_spc.parquet
          mode: overwrite
```

**What makes this realistic:**

- SPC-style fill weight with tight normal distribution (σ=1.2g) around a 500g target — matches real filling equipment capability
- Validation tests catch the same issues you'd catch in production — out-of-range fills and null sensor readings
- Quarantine mode isolates bad rows into a separate table instead of dropping them — you can investigate rejects later
- Chaos `outlier_rate: 0.008` generates occasional fill weight spikes that trigger the validation rules

!!! example "Try this"
    - Tighten the validation range to `min: 498, max: 502` and see more rows quarantined
    - Add a UCL/LCL derived column: `"500 + 3 * 1.2"` and `"500 - 3 * 1.2"` for ±3σ control limits
    - Change `outlier_rate` to `0.02` for a "bad batch" scenario and watch the quarantine table fill up

> 📖 **Learn more:** [Validation Tests](../../validation/tests.md) — All 11 validation test types and quarantine configuration

---

## Pattern 22: CNC Machine Shop {#pattern-22}

**Industry:** Discrete Manufacturing | **Difficulty:** Intermediate

!!! tip "What you'll learn"
    - **`downtime_events` in chaos config** — unlike `scheduled_events` (which you plan), downtime_events simulate *unplanned* outages where no data is generated at all (missing rows, not null values)

A CNC machine shop runs 3 machines cutting parts all shift. Tools wear gradually, degrading surface finish quality. Unlike scheduled maintenance (which you plan), unplanned downtime means the machine stops reporting entirely — the rows simply don't exist. That's what `downtime_events` models.

```yaml
project: cnc_machine_shop
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
  - pipeline: cnc
    nodes:
      - name: cnc_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-10T06:00:00Z"
                timestep: "1m"
                row_count: 480            # 8-hour shift
                seed: 42
              entities:
                names: [CNC_Lathe_01, CNC_Mill_01, CNC_Mill_02]
              columns:
                - name: machine_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                # Spindle speed — lathe runs slower than mills
                - name: spindle_speed_rpm
                  data_type: float
                  generator:
                    type: random_walk
                    start: 3000.0
                    min: 1000.0
                    max: 6000.0
                    volatility: 50.0
                    mean_reversion: 0.1
                    precision: 0
                  entity_overrides:
                    CNC_Lathe_01:
                      type: random_walk
                      start: 1500.0
                      min: 800.0
                      max: 3000.0
                      volatility: 50.0
                      mean_reversion: 0.1
                      precision: 0

                - name: feed_rate_mm_min
                  data_type: float
                  generator:
                    type: random_walk
                    start: 200.0
                    min: 50.0
                    max: 500.0
                    volatility: 5.0
                    mean_reversion: 0.1
                    precision: 0

                # Tool wear trends upward — real degradation
                - name: tool_wear_pct
                  data_type: float
                  generator:
                    type: random_walk
                    start: 5.0
                    min: 0.0
                    max: 100.0
                    volatility: 0.3
                    mean_reversion: 0.01
                    trend: 0.05
                    precision: 1

                - name: part_count
                  data_type: int
                  generator: {type: sequential, start: 1, step: 1}

                # Surface finish degrades with tool wear
                - name: surface_finish_um
                  data_type: float
                  generator:
                    type: derived
                    expression: "0.5 + tool_wear_pct * 0.02 + random() * 0.1"

                - name: tool_change_needed
                  data_type: boolean
                  generator:
                    type: derived
                    expression: "tool_wear_pct > 80"

              chaos:
                outlier_rate: 0.005
                outlier_factor: 2.0
                # Unplanned downtime — CNC_Mill_02 goes offline for 45 min
                downtime_events:
                  - entity: CNC_Mill_02
                    start_time: "2026-03-10T09:30:00Z"
                    end_time: "2026-03-10T10:15:00Z"

        write:
          connection: output
          format: parquet
          path: bronze/cnc_shop.parquet
          mode: overwrite
```

**What makes this realistic:**

- Tool wear trends upward (`trend: 0.05`) — real cutting tools degrade with every part
- Surface finish degrades with wear (`0.5 + tool_wear_pct * 0.02`) — the correlation between wear and quality is real
- `downtime_events` create missing rows, not null values — the machine literally stops reporting data when it's offline
- Sequential `part_count` provides traceability — each part has a unique number per machine
- Lathe runs at lower spindle speeds than mills (entity override) — matches real machining physics

!!! example "Try this"
    - Add a `coolant_temp_c` column using `random_walk` (start=22, min=20, max=45, volatility=0.3) that rises during cuts
    - Change `trend` to `0.1` for faster tool wear and watch `tool_change_needed` trigger earlier
    - Add a second downtime event for `CNC_Lathe_01` at a different time window

> 📖 **Learn more:** [Advanced Features](../advanced_features.md) — Chaos config and downtime events

---

## Pattern 23: Warehouse Inventory Tracking {#pattern-23}

**Industry:** Logistics | **Difficulty:** Intermediate

!!! tip "What you'll learn"
    - **Multi-pipeline project** — one project config with multiple pipelines (receiving, picking, shipping). Each pipeline generates different data, showing how a real warehouse has multiple data streams.

A warehouse has three separate data streams running at different granularities: receiving docks scan inbound POs every 15 minutes, pickers scan picks every 5 minutes, and shipping bays log shipments hourly. This is one project with three pipelines — exactly how real warehouse management systems work.

```yaml
project: warehouse_ops
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
  # ── Pipeline 1: Receiving Dock ──────────────────────────
  - pipeline: receiving
    nodes:
      - name: receiving_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-10T06:00:00Z"
                timestep: "15m"
                row_count: 64             # 16 hours (6am–10pm)
                seed: 42
              entities:
                count: 1
                id_prefix: "dock_"
              columns:
                - name: dock_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}
                - name: po_number
                  data_type: int
                  generator: {type: sequential, start: 50001}
                - name: sku
                  data_type: string
                  generator:
                    type: categorical
                    values: [SKU_A, SKU_B, SKU_C, SKU_D, SKU_E]
                    weights: [0.30, 0.25, 0.20, 0.15, 0.10]
                - name: quantity_received
                  data_type: int
                  generator:
                    type: range
                    min: 10
                    max: 500
                    distribution: normal
                    mean: 100
                    std_dev: 50
                - name: condition
                  data_type: string
                  generator:
                    type: categorical
                    values: [good, damaged, partial]
                    weights: [0.92, 0.05, 0.03]
        write:
          connection: output
          format: parquet
          path: bronze/warehouse_receiving.parquet
          mode: overwrite

  # ── Pipeline 2: Order Picking ──────────────────────────
  - pipeline: picks
    nodes:
      - name: pick_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-10T06:00:00Z"
                timestep: "5m"
                row_count: 192            # 16 hours
                seed: 43
              entities:
                names: [picker_A, picker_B, picker_C]
              columns:
                - name: picker_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}
                - name: order_number
                  data_type: int
                  generator: {type: sequential, start: 80001}
                - name: sku
                  data_type: string
                  generator:
                    type: categorical
                    values: [SKU_A, SKU_B, SKU_C, SKU_D, SKU_E]
                    weights: [0.30, 0.25, 0.20, 0.15, 0.10]
                - name: quantity_picked
                  data_type: int
                  generator: {type: range, min: 1, max: 50}
                - name: pick_time_sec
                  data_type: float
                  generator:
                    type: range
                    min: 15.0
                    max: 120.0
                    distribution: normal
                    mean: 45.0
                    std_dev: 15.0
                - name: pick_accuracy
                  data_type: boolean
                  generator: {type: boolean, true_probability: 0.98}
        write:
          connection: output
          format: parquet
          path: bronze/warehouse_picks.parquet
          mode: overwrite

  # ── Pipeline 3: Shipping ──────────────────────────
  - pipeline: shipping
    nodes:
      - name: shipping_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-10T06:00:00Z"
                timestep: "1h"
                row_count: 16             # 16 hours
                seed: 44
              entities:
                count: 1
                id_prefix: "bay_"
              columns:
                - name: bay_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}
                - name: shipment_id
                  data_type: string
                  generator: {type: uuid, version: 4}
                - name: carrier
                  data_type: string
                  generator:
                    type: categorical
                    values: [FedEx, UPS, USPS, LTL_Freight]
                    weights: [0.35, 0.30, 0.20, 0.15]
                - name: packages
                  data_type: int
                  generator: {type: range, min: 1, max: 50}
                - name: weight_kg
                  data_type: float
                  generator:
                    type: range
                    min: 2.0
                    max: 200.0
                    distribution: normal
                    mean: 25.0
                    std_dev: 20.0
        write:
          connection: output
          format: parquet
          path: bronze/warehouse_shipping.parquet
          mode: overwrite
```

**What makes this realistic:**

- Three separate data streams at different granularities (15m, 5m, 1h) — real warehouses have multiple systems running at different cadences
- Pick accuracy uses boolean generator with 98% true probability — matches industry pick accuracy benchmarks
- SKU distribution is weighted (Pareto-style) — top SKU accounts for 30% of volume, matching the 80/20 rule
- Each pipeline writes to a separate output — just like real WMS data flows to separate tables

!!! example "Try this"
    - Add more pickers (`picker_D`, `picker_E`) and compare pick times across workers
    - Add a `returns` pipeline with a 30m timestep for processing customer returns
    - Add validation to the receiving pipeline: `type: range, column: quantity_received, min: 1`

> 📖 **Learn more:** [Core Concepts](../core_concepts.md) — Scope, entities, and how multiple pipelines share a project

---

## Pattern 24: Food Safety / Cold Chain Monitoring {#pattern-24}

**Industry:** Food & Beverage | **Difficulty:** Intermediate

!!! tip "What you'll learn"
    - **`email` generator** for alert recipients — generates realistic email addresses tied to entity names
    - **Deriving alarm escalation** from temperature thresholds — a single column drives `normal`, `warning`, and `critical` alert levels

Cold chain monitoring tracks temperature and humidity across trucks and warehouse zones. Temperature must stay below 5°C for food safety — any excursion triggers alerts routed to generated email recipients. Trucks have worse insulation than warehouse zones, and loading dock events cause temperature spikes.

```yaml
project: cold_chain
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
  - pipeline: cold_chain
    nodes:
      - name: cold_chain_data
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
                names: [Truck_01, Truck_02, Warehouse_Zone_A, Warehouse_Zone_B]
              columns:
                - name: unit_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                # Cold chain target ~2°C
                - name: temperature_c
                  data_type: float
                  generator:
                    type: random_walk
                    start: 2.0
                    min: -5.0
                    max: 15.0
                    volatility: 0.3
                    mean_reversion: 0.15
                    precision: 1
                  entity_overrides:
                    Truck_01:             # Worse insulation
                      type: random_walk
                      start: 2.0
                      min: -3.0
                      max: 18.0
                      volatility: 0.6
                      mean_reversion: 0.15
                      precision: 1

                - name: humidity_pct
                  data_type: float
                  generator:
                    type: range
                    min: 80.0
                    max: 95.0
                    distribution: normal
                    mean: 88.0
                    std_dev: 3.0

                # Doors open 5% of the time
                - name: door_open
                  data_type: boolean
                  generator: {type: boolean, true_probability: 0.05}

                # Food safety limit: >5°C is an excursion
                - name: temp_excursion
                  data_type: boolean
                  generator:
                    type: derived
                    expression: "temperature_c > 5.0"

                # Alert routing — realistic emails per entity
                - name: alert_recipient
                  data_type: string
                  generator:
                    type: email
                    domain: "coldfresh.example.com"
                    pattern: "{entity}_{index}"

                # Escalation levels from temperature
                - name: alert_level
                  data_type: string
                  generator:
                    type: derived
                    expression: "'critical' if temperature_c > 8.0 else 'warning' if temperature_c > 5.0 else 'normal'"

              # Truck_02 loading dock stop — doors forced open, temp spikes
              scheduled_events:
                - type: forced_value
                  entity: Truck_02
                  column: door_open
                  value: true
                  start_time: "2026-03-10T14:00:00Z"
                  end_time: "2026-03-10T14:30:00Z"

              chaos:
                outlier_rate: 0.005
                outlier_factor: 2.0

        write:
          connection: output
          format: parquet
          path: bronze/cold_chain.parquet
          mode: overwrite
```

**What makes this realistic:**

- Cold chain has tight temperature bands (target ~2°C, excursion at 5°C) — matches HACCP food safety requirements
- Door open events cause temperature spikes — the loading dock stop forces `door_open: true` for 30 minutes
- `email` generator creates realistic alert routing addresses tied to each entity
- Truck vs. warehouse have different thermal profiles — `Truck_01` has higher volatility (worse insulation) and wider temperature range

!!! example "Try this"
    - Add a `time_out_of_range` column using `prev()`: `"prev('time_out_of_range', 0) + 5 if temperature_c > 5.0 else 0"` to track accumulated minutes above threshold
    - Add a `product_zone` categorical column (`[frozen, chilled, ambient]` with weights `[0.5, 0.4, 0.1]`)
    - Tighten the excursion threshold to 4°C and see how many more alerts fire

> 📖 **Learn more:** [Generators Reference](../generators.md) — Email generator and boolean generator parameters

---

## Pattern 25: Assembly Line Stations {#pattern-25}

**Industry:** Automotive | **Difficulty:** Advanced

!!! tip "What you'll learn"
    - **Cross-entity for station-to-station flow** — parts flow from Station_1 → Station_2 → Station_3 → QC. Each station's output becomes the next station's input using `Entity.column` references. This models a real assembly line where bottlenecks propagate downstream.

An automotive assembly line has four stations: weld, paint, assembly, and QC. Raw material enters Station 1, and each station's output feeds the next station's input via cross-entity references. The paint station is the bottleneck (slower cycle time), and its reduced throughput propagates to every downstream station — just like a real production line.

```yaml
project: assembly_line
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
  - pipeline: assembly
    nodes:
      - name: station_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-10T06:00:00Z"
                timestep: "5m"
                row_count: 96             # 8-hour shift
                seed: 42
              entities:
                names: [Station_1_Weld, Station_2_Paint, Station_3_Assembly, Station_4_QC]
              columns:
                - name: station_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                # Parts entering the station
                # Station_1 gets raw material; downstream stations pull from upstream
                - name: parts_in
                  data_type: int
                  generator:
                    type: range
                    min: 8
                    max: 15
                  entity_overrides:
                    Station_2_Paint:
                      type: derived
                      expression: "Station_1_Weld.parts_out"
                    Station_3_Assembly:
                      type: derived
                      expression: "Station_2_Paint.parts_out"
                    Station_4_QC:
                      type: derived
                      expression: "Station_3_Assembly.parts_out"

                # Cycle time — paint station is the bottleneck
                - name: cycle_time_sec
                  data_type: float
                  generator:
                    type: range
                    min: 45.0
                    max: 75.0
                    distribution: normal
                    mean: 55.0
                    std_dev: 5.0
                  entity_overrides:
                    Station_2_Paint:       # Bottleneck — slower cycle
                      type: range
                      min: 50.0
                      max: 90.0
                      distribution: normal
                      mean: 70.0
                      std_dev: 8.0

                # Some scrap at each station
                - name: parts_out
                  data_type: int
                  generator:
                    type: derived
                    expression: "max(0, parts_in - int(random() * 2))"

                # Running total of output per station
                - name: cumulative_output
                  data_type: int
                  generator:
                    type: derived
                    expression: "prev('cumulative_output', 0) + parts_out"

                # Scrap rate from actual in/out
                - name: scrap_rate
                  data_type: float
                  generator:
                    type: derived
                    expression: "safe_div(parts_in - parts_out, parts_in, 0)"

              chaos:
                outlier_rate: 0.003
                outlier_factor: 1.5

        write:
          connection: output
          format: parquet
          path: bronze/assembly_line.parquet
          mode: overwrite
```

**What makes this realistic:**

- Parts flow downstream — Station 1's `parts_out` becomes Station 2's `parts_in` via cross-entity references
- Bottleneck at the paint station (mean cycle time 70s vs 55s) constrains all downstream stations — fewer parts in means fewer parts out
- Scrap rate is calculated from actual `parts_in - parts_out`, not generated randomly
- `cumulative_output` uses `prev()` for a running total — you can see exactly how many parts each station has produced over the shift
- Entity generation order (Station_1 → Station_2 → Station_3 → Station_4) ensures upstream data exists before downstream stations reference it

!!! example "Try this"
    - Add a `Station_5_Pack` station that pulls from `Station_4_QC.parts_out`
    - Add a `bottleneck_flag` derived column: `"cycle_time_sec > 65"` to flag slow cycles
    - Make the paint station even worse by reducing `parts_out` further: `"max(0, parts_in - int(random() * 3))"`

> 📖 **Learn more:** [Advanced Features](../advanced_features.md) — Cross-entity references and entity generation order

---

← [Energy & Utilities Patterns (16-20)](energy_utilities.md) | [Environmental Patterns (26-28)](environmental.md) →
