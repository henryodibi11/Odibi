---
title: "Patterns & Recipes"
roles: [ba, jr-de, cheme]
tags: [reference, topic:simulation, topic:patterns, topic:recipes]
prereqs: [getting_started.md, core_concepts.md, generators.md]
next: [process_simulation.md]
related: [advanced_features.md, stateful_functions.md]
time: 45m
---

# Patterns & Recipes

Real-world simulation patterns across manufacturing, operations, IoT, business, and data engineering. Each pattern is a complete, copy-paste-ready YAML config.

---

## Pattern 1: Build Before Sources Exist

**The most common pattern.** Simulate what your upstream source will look like, build the full pipeline (transforms, validation, write), then swap bronze to real data later — silver and gold stay unchanged.

This is the entire point of simulation: **decouple pipeline development from data availability.**

```yaml
name: sales_pipeline
engine: pandas

connections:
  output:
    type: local
    base_path: ./data

pipelines:
  - name: sales
    nodes:
      # ── Bronze: Simulated source ──────────────────────────
      - name: raw_orders
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-01-01T00:00:00Z"
                timestep: "1h"
                row_count: 720        # 30 days of hourly data
                seed: 42
              entities:
                count: 1
                id_prefix: "source_"
              columns:
                - name: order_id
                  data_type: int
                  generator: {type: sequential, start: 10001}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}
                - name: customer_id
                  data_type: string
                  generator: {type: uuid, version: 4}
                - name: product
                  data_type: string
                  generator:
                    type: categorical
                    values: [Widget_A, Widget_B, Gadget_X, Premium_Z]
                    weights: [0.40, 0.30, 0.20, 0.10]
                - name: quantity
                  data_type: int
                  generator: {type: range, min: 1, max: 20}
                - name: unit_price
                  data_type: float
                  generator:
                    type: range
                    min: 9.99
                    max: 149.99
                    distribution: normal
                    mean: 45.00
                    std_dev: 25.00
        write:
          connection: output
          format: parquet
          path: bronze/orders.parquet
          mode: overwrite

      # ── Silver: Transform (unchanged when bronze goes live) ──
      - name: clean_orders
        read:
          connection: output
          format: parquet
          path: bronze/orders.parquet
        transform:
          - operation: derive_columns
            params:
              columns:
                line_total: "quantity * unit_price"
                order_tier: >
                  CASE
                    WHEN quantity * unit_price > 500 THEN 'high'
                    WHEN quantity * unit_price > 100 THEN 'medium'
                    ELSE 'low'
                  END
        validation:
          mode: warn
          tests:
            - type: not_null
              columns: [order_id, timestamp, customer_id]
            - type: range
              column: unit_price
              min: 0.0
              max: 1000.0
        write:
          connection: output
          format: parquet
          path: silver/orders.parquet
          mode: overwrite

      # ── Gold: Aggregation (unchanged when bronze goes live) ──
      - name: daily_summary
        read:
          connection: output
          format: parquet
          path: silver/orders.parquet
        transform:
          - operation: aggregate
            params:
              group_by: [product]
              aggregations:
                total_revenue: "SUM(line_total)"
                order_count: "COUNT(order_id)"
                avg_order_value: "AVG(line_total)"
        write:
          connection: output
          format: parquet
          path: gold/daily_product_summary.parquet
          mode: overwrite
```

**When real data arrives:** change the bronze node's `format: simulation` to `format: csv` (or `delta`, or `sql`), point it at your real connection, and delete the `simulation` block. Silver and gold nodes don't change at all.

---

## Pattern 2: Manufacturing Production Line

Simulate a production line with 5 machines, realistic cycle times, defect rates, and operational events.

```yaml
name: production_line
engine: pandas

connections:
  output:
    type: local
    base_path: ./data

pipelines:
  - name: production
    nodes:
      - name: machine_telemetry
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-10T06:00:00Z"
                end_time: "2026-03-10T22:00:00Z"    # One shift: 6am–10pm
                timestep: "5m"
                seed: 42
              entities:
                count: 5
                id_prefix: "machine_"
              columns:
                - name: machine_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                - name: cycle_time_sec
                  data_type: float
                  generator:
                    type: range
                    min: 28.0
                    max: 35.0
                    distribution: normal
                    mean: 31.0
                    std_dev: 1.5
                  entity_overrides:
                    machine_03:             # Older machine — slower
                      type: range
                      min: 34.0
                      max: 45.0
                      distribution: normal
                      mean: 38.0
                      std_dev: 3.0

                - name: units_produced
                  data_type: int
                  generator: {type: range, min: 8, max: 15}
                  entity_overrides:
                    machine_03:
                      type: range
                      min: 5
                      max: 10

                - name: defect_count
                  data_type: int
                  generator: {type: range, min: 0, max: 2}
                  entity_overrides:
                    machine_03:             # Higher defect rate
                      type: range
                      min: 0
                      max: 5

                - name: status
                  data_type: string
                  generator:
                    type: categorical
                    values: [Running, Idle, Changeover, Error]
                    weights: [0.80, 0.10, 0.07, 0.03]

              # machine_02 maintenance shutdown 14:00–16:00
              scheduled_events:
                - type: forced_value
                  entity: machine_02
                  column: status
                  value: Maintenance
                  start_time: "2026-03-10T14:00:00Z"
                  end_time: "2026-03-10T16:00:00Z"
                - type: forced_value
                  entity: machine_02
                  column: units_produced
                  value: 0
                  start_time: "2026-03-10T14:00:00Z"
                  end_time: "2026-03-10T16:00:00Z"

              chaos:
                outlier_rate: 0.01
                outlier_factor: 3.0
                duplicate_rate: 0.005
        write:
          connection: output
          format: parquet
          path: bronze/production_telemetry.parquet
          mode: overwrite
```

**What makes this realistic:**

- `machine_03` is the "problem child" — slower cycle times, lower output, more defects
- `machine_02` goes down for scheduled maintenance mid-shift
- 1% outliers catch extreme cycle times; 0.5% duplicates simulate PLC retransmission
- Normal distribution on cycle times clusters values around the expected mean

---

## Pattern 3: IoT Sensor Network

Simulate a building management system with 20 sensors across 4 floors measuring temperature, humidity, CO₂, and occupancy.

```yaml
name: building_sensors
engine: pandas

connections:
  output:
    type: local
    base_path: ./data

pipelines:
  - name: bms
    nodes:
      - name: sensor_readings
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-10T00:00:00Z"
                end_time: "2026-03-11T00:00:00Z"    # 24 hours
                timestep: "5m"
                seed: 42
              entities:
                count: 20
                id_prefix: "sensor_"
              columns:
                - name: sensor_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                - name: floor
                  data_type: string
                  generator:
                    type: categorical
                    values: [Floor_1, Floor_2, Floor_3, Floor_4]
                    weights: [0.25, 0.25, 0.25, 0.25]

                # Temperature with mean reversion — HVAC keeps it controlled
                - name: temperature_c
                  data_type: float
                  generator:
                    type: random_walk
                    start: 22.0
                    min: 16.0
                    max: 30.0
                    volatility: 0.3
                    mean_reversion: 0.15     # HVAC pulls back to setpoint
                    precision: 1

                # Humidity — some sensors don't have this capability
                - name: humidity_pct
                  data_type: float
                  generator:
                    type: range
                    min: 30.0
                    max: 70.0
                    distribution: normal
                    mean: 45.0
                    std_dev: 8.0
                  null_rate: 0.15            # 15% of sensors lack humidity

                - name: co2_ppm
                  data_type: float
                  generator:
                    type: random_walk
                    start: 420.0
                    min: 350.0
                    max: 1200.0
                    volatility: 5.0
                    mean_reversion: 0.05
                    precision: 0

                - name: occupancy
                  data_type: int
                  generator: {type: range, min: 0, max: 25}

              # sensor_15 battery died — no data after 14:00
              scheduled_events:
                - type: forced_value
                  entity: sensor_15
                  column: temperature_c
                  value: null
                  start_time: "2026-03-10T14:00:00Z"
                - type: forced_value
                  entity: sensor_15
                  column: humidity_pct
                  value: null
                  start_time: "2026-03-10T14:00:00Z"
                - type: forced_value
                  entity: sensor_15
                  column: co2_ppm
                  value: null
                  start_time: "2026-03-10T14:00:00Z"

              chaos:
                outlier_rate: 0.005
                outlier_factor: 2.5
                duplicate_rate: 0.003
        write:
          connection: output
          format: parquet
          path: bronze/building_sensors.parquet
          mode: overwrite
```

**What makes this realistic:**

- `random_walk` with `mean_reversion` simulates HVAC-controlled temperature — it drifts but gets pulled back
- CO₂ uses a random walk because occupancy changes gradually (not randomly)
- `null_rate: 0.15` on humidity simulates sensors without that capability
- `sensor_15` dies at 14:00 (battery failure) — permanent null via scheduled events with no `end_time`
- Low chaos rates (0.5% outliers, 0.3% duplicates) keep data realistic without being noisy

---

## Pattern 4: Order / Transaction Data

Simulate an e-commerce or ERP order stream with multiple channels, realistic distributions, and incremental daily feeds.

```yaml
name: order_stream
engine: pandas

connections:
  output:
    type: local
    base_path: ./data

pipelines:
  - name: orders
    nodes:
      - name: daily_orders
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-01T00:00:00Z"
                end_time: "2026-03-02T00:00:00Z"    # 1 day per run
                timestep: "2m"
                seed: 42
              entities:
                names: [web, mobile, store, partner]
              columns:
                - name: order_id
                  data_type: int
                  generator: {type: sequential, start: 100001}
                - name: source
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}
                - name: customer_id
                  data_type: string
                  generator: {type: uuid, version: 4}

                - name: amount
                  data_type: float
                  generator:
                    type: range
                    min: 5.00
                    max: 500.00
                    distribution: normal
                    mean: 65.00
                    std_dev: 45.00

                - name: status
                  data_type: string
                  generator:
                    type: categorical
                    values: [completed, pending, cancelled, refunded]
                    weights: [0.82, 0.10, 0.05, 0.03]

                - name: payment_method
                  data_type: string
                  generator:
                    type: categorical
                    values: [credit_card, debit_card, paypal, apple_pay, bank_transfer]
                    weights: [0.40, 0.25, 0.15, 0.12, 0.08]

                # Derived tier based on amount thresholds
                - name: order_tier
                  data_type: string
                  generator:
                    type: derived
                    expression: >
                      'platinum' if amount > 300
                      else 'gold' if amount > 150
                      else 'silver' if amount > 50
                      else 'bronze'

          incremental:
            mode: stateful
            column: timestamp
        write:
          connection: output
          format: parquet
          path: bronze/orders.parquet
          mode: append
```

**What makes this realistic:**

- Named entities (`web`, `mobile`, `store`, `partner`) map to real order channels
- Normal distribution on `amount` clusters most orders around $65 with a tail of high-value orders
- Categorical `status` and `payment_method` match real-world ratios
- `derived` expression creates `order_tier` from amount thresholds — no transform step needed
- `incremental: stateful` + `mode: append` means each run generates the next day, appending to the existing file

---

## Pattern 5: Equipment Degradation and Maintenance

Simulate long-running equipment that degrades over time, with periodic cleaning cycles that restore performance.

```yaml
name: heat_exchanger_monitoring
engine: pandas

connections:
  output:
    type: local
    base_path: ./data

pipelines:
  - name: hx_monitoring
    nodes:
      - name: hx_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-01-01T00:00:00Z"
                timestep: "1h"
                row_count: 720            # 30 days
                seed: 42
              entities:
                names: [HX_01, HX_02, HX_03]
              columns:
                - name: equipment_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                # Design efficiency — the target we're degrading from
                - name: design_efficiency_pct
                  data_type: float
                  generator: {type: constant, value: 95.0}

                # Actual efficiency — random walk with negative trend (fouling)
                - name: actual_efficiency_pct
                  data_type: float
                  generator:
                    type: random_walk
                    start: 94.0
                    min: 60.0
                    max: 96.0
                    volatility: 0.2
                    trend: -0.01           # Gradual fouling
                    mean_reversion: 0.02
                    mean_reversion_to: design_efficiency_pct
                    precision: 1

                # Derived: energy loss from degradation
                - name: energy_loss_kw
                  data_type: float
                  generator:
                    type: derived
                    expression: "max(0, (design_efficiency_pct - actual_efficiency_pct) * 2.5)"

                # Derived: maintenance flag
                - name: needs_cleaning
                  data_type: boolean
                  generator:
                    type: derived
                    expression: "actual_efficiency_pct < 80.0"

              # Cleaning cycle resets efficiency on day 15
              scheduled_events:
                - type: parameter_override
                  entity: HX_01
                  column: actual_efficiency_pct
                  value: 94.0
                  start_time: "2026-01-15T08:00:00Z"
                  end_time: "2026-01-15T12:00:00Z"
                - type: parameter_override
                  entity: HX_02
                  column: actual_efficiency_pct
                  value: 94.0
                  start_time: "2026-01-16T08:00:00Z"
                  end_time: "2026-01-16T12:00:00Z"
        write:
          connection: output
          format: parquet
          path: bronze/heat_exchangers.parquet
          mode: overwrite
```

**What makes this realistic:**

- `trend: -0.01` causes efficiency to slowly degrade (fouling buildup)
- `mean_reversion_to: design_efficiency_pct` creates a tug-of-war between degradation and the design spec
- Scheduled events simulate cleaning cycles that reset efficiency back to 94%
- `HX_03` never gets cleaned — its efficiency degrades further than the others
- `energy_loss_kw` is derived in real-time from the gap between design and actual

---

## Pattern 6: Stress Test at Scale

Quick recipe for high-volume testing. Perfect for Delta Lake compaction, partition testing, and engine benchmarking.

```yaml
scope:
  start_time: "2026-01-01T00:00:00Z"
  timestep: "1s"
  row_count: 10000
  seed: 42
entities:
  count: 1000               # 1,000 entities × 10,000 rows = 10M rows
  id_prefix: "device_"
columns:
  - name: device_id
    data_type: string
    generator: {type: constant, value: "{entity_id}"}
  - name: timestamp
    data_type: timestamp
    generator: {type: timestamp}
  - name: value
    data_type: float
    generator: {type: range, min: 0, max: 100}
  - name: status
    data_type: string
    generator:
      type: categorical
      values: [ok, warn, error]
      weights: [0.90, 0.08, 0.02]
```

**Tips for scale testing:**

- **Use Spark engine for >1M rows** — set `engine: spark` in your pipeline config
- **Add partitioning** for Delta Lake writes: `partition_by: [date_column]`
- **Test compaction** — write 10M rows in small batches, then run `OPTIMIZE`
- **Z-ordering** — test query performance with `z_order_by: [device_id]`
- **Memory planning** — 10M rows × 4 columns ≈ 400MB in memory (Pandas) or distributed (Spark)
- **Incremental stress** — run 10 times with `row_count: 1000` to test append performance

---

## Pattern 7: Daily Dashboard Feed

Generate continuous demo data that looks like real streaming data. Each pipeline run produces one day of data and appends it to a Delta table. Connect a dashboard and it updates automatically.

```yaml
name: dashboard_feed
engine: pandas

connections:
  output:
    type: local
    base_path: ./data

pipelines:
  - name: daily_feed
    nodes:
      - name: kpi_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-01T00:00:00Z"
                timestep: "15m"
                row_count: 96             # 24 hours at 15-min intervals
                seed: 42
              entities:
                names: [plant_north, plant_south, plant_east]
              columns:
                - name: plant_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                - name: throughput_tons
                  data_type: float
                  generator:
                    type: random_walk
                    start: 120.0
                    min: 80.0
                    max: 160.0
                    volatility: 2.0
                    mean_reversion: 0.1
                    precision: 1

                - name: quality_pct
                  data_type: float
                  generator:
                    type: range
                    min: 92.0
                    max: 99.5
                    distribution: normal
                    mean: 97.0
                    std_dev: 1.5

                - name: energy_kwh
                  data_type: float
                  generator:
                    type: derived
                    expression: "throughput_tons * 8.5 + (random() - 0.5) * 20"

                - name: downtime_min
                  data_type: int
                  generator: {type: range, min: 0, max: 15}

          incremental:
            mode: stateful
            column: timestamp

        write:
          connection: output
          format: delta
          path: gold/plant_kpis
          mode: append
```

**How to use this as a dashboard feed:**

1. Schedule the pipeline with cron: `odibi run dashboard_feed.yaml` daily
2. Each run generates the next 24 hours (incremental mode picks up from the last timestamp)
3. Dashboard connects to `./data/gold/plant_kpis` (Delta table)
4. New data appears daily — looks like real streaming data to stakeholders

---

## Pattern 8: Multi-System Integration Test

Simulate multiple interconnected systems where downstream systems consume upstream outputs. Uses cross-entity references to create realistic data flow.

```yaml
name: integration_test
engine: pandas

connections:
  output:
    type: local
    base_path: ./data

pipelines:
  - name: multi_system
    nodes:
      - name: system_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-10T00:00:00Z"
                timestep: "10m"
                row_count: 144            # 24 hours
                seed: 42
              entities:
                names: [SystemA_Producer, SystemB_Processor, SystemC_Storage]
              columns:
                - name: system_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                # System A: produces raw events
                - name: events_produced
                  data_type: int
                  generator: {type: range, min: 50, max: 200}

                - name: output_rate
                  data_type: float
                  generator:
                    type: random_walk
                    start: 100.0
                    min: 40.0
                    max: 180.0
                    volatility: 3.0
                    mean_reversion: 0.08

                # System B: processes System A's output with conversion factor
                - name: processed_count
                  data_type: float
                  generator:
                    type: derived
                    expression: >
                      SystemA_Producer.output_rate * 0.85
                      if entity_id == 'SystemB_Processor'
                      else 0

                # System B: processing latency depends on load
                - name: latency_ms
                  data_type: float
                  generator:
                    type: derived
                    expression: >
                      50 + (SystemA_Producer.output_rate * 0.3)
                      if entity_id == 'SystemB_Processor'
                      else 0

                # System C: stores what System B processed
                - name: records_stored
                  data_type: float
                  generator:
                    type: derived
                    expression: >
                      SystemB_Processor.processed_count * 0.98
                      if entity_id == 'SystemC_Storage'
                      else 0

                # System C: storage utilization
                - name: storage_utilization_pct
                  data_type: float
                  generator:
                    type: derived
                    expression: >
                      min(100, prev('storage_utilization_pct', 10.0) + SystemC_Storage.records_stored * 0.001)
                      if entity_id == 'SystemC_Storage'
                      else 0
        write:
          connection: output
          format: parquet
          path: bronze/integration_test.parquet
          mode: overwrite
```

**What makes this realistic:**

- `SystemB_Processor.processed_count` = 85% of `SystemA_Producer.output_rate` — realistic throughput loss
- `latency_ms` increases with `SystemA`'s load — simulates backpressure
- `SystemC_Storage.records_stored` = 98% of what `SystemB` processed — 2% dropped
- `storage_utilization_pct` accumulates over time using `prev()` — storage fills up

---

## Tips for Realistic Simulation

- **Match real-world distributions.** Use `normal` for measurements (temperature, weight, cycle time). Use `categorical` with weighted probabilities for statuses and categories. Use `uniform` only when values are truly random.

- **Use `random_walk` for anything that changes gradually.** Temperature, pressure, stock levels, efficiency — these don't jump randomly between values. Random walk with `mean_reversion` produces the smooth, correlated time series you see in real SCADA/IoT data.

- **Add chaos conservatively.** Real data isn't random noise. Use 0.5–2% outlier rates and 0.3–1% duplicate rates. If your chaos settings produce data that looks obviously fake, dial them back.

- **Use entity overrides to create "problem" entities.** Old equipment runs slower, bad sensors report wider ranges, overloaded channels drop more data. One or two problem entities make the dataset much more realistic than uniform behavior.

- **Use scheduled events for operational realism.** Shifts, maintenance windows, outages, setpoint changes — these create the time-based patterns that real operations data has. A simulation without events is a simulation without a story.

- **Use incremental mode for anything time-based.** Daily feeds, streaming data, dashboard demos — `incremental: stateful` ensures each run picks up where the last one left off with no discontinuities.

- **Validate simulated data the same way you'd validate real data.** Run the same `validation` tests on simulated data that you'll run on production data. If your validation catches problems in simulation, it will catch them in production too.

---

## See Also

- **[Getting Started](getting_started.md)** — Your first simulation in 5 minutes
- **[Generators Reference](generators.md)** — All 12 generator types with parameters and examples
- **[Advanced Features](advanced_features.md)** — Cross-entity references, scheduled events, entity overrides, chaos
- **[Process Simulation](process_simulation.md)** — Chemical engineering and process control scenarios
