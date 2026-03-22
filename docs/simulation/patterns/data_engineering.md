---
title: "Data Engineering Meta-Patterns (36-38)"
---

# Data Engineering Meta-Patterns (36–38)

Patterns for testing your data platform itself. These aren't about simulating a specific industry — they're about generating problematic data on purpose to validate that your pipelines handle edge cases correctly.

!!! info "Prerequisites"
    These patterns build on [Foundational Patterns 1–8](foundations.md), especially Pattern 1 (simulation as a bronze source) and Pattern 6 (stress testing). Pattern 37 requires familiarity with [Validation Tests](../../validation/tests.md).

---

## Pattern 36: Late-Arriving & Duplicate Data {#pattern-36}

**Industry:** Data Engineering | **Difficulty:** Intermediate

!!! tip "What you'll learn"
    - **`chaos` config for messy data** — using high `duplicate_rate` for retransmissions and `outlier_rate` for corrupt values. This is how you test that your downstream deduplication and data quality rules actually work.

Simulated sensor data with deliberately aggressive chaos settings. A 5% outlier rate and 3% duplicate rate generate the kind of worst-case data your bronze-to-silver pipeline must handle — far beyond the ~0.1–1% bad data rate of a real historian, but exactly what you need to stress-test your dedup and quality logic.

```yaml
project: late_arriving_data
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
  - pipeline: late_data
    nodes:
      - name: messy_sensor_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-01T00:00:00Z"
                timestep: "1m"
                row_count: 1440            # 24 hours
                seed: 42
              entities:
                count: 1
                id_prefix: "source_"
              columns:
                - name: source_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                - name: record_id
                  data_type: int
                  generator:
                    type: sequential
                    start: 1
                    step: 1

                - name: sensor_value
                  data_type: float
                  generator:
                    type: random_walk
                    start: 50
                    min: 0
                    max: 100
                    volatility: 3.0
                    mean_reversion: 0.1
                    precision: 2

                - name: quality_code
                  data_type: string
                  generator:
                    type: categorical
                    values: [good, suspect, bad, missing]
                    weights: [0.85, 0.08, 0.05, 0.02]

                - name: source_system
                  data_type: string
                  generator: {type: constant, value: "historian_01"}

                - name: batch_label
                  data_type: string
                  generator:
                    type: categorical
                    values: [batch_a, batch_b, batch_c]
                    weights: [0.5, 0.3, 0.2]

              chaos:
                outlier_rate: 0.05          # 5% — deliberately aggressive
                outlier_factor: 5.0         # Extreme outliers
                duplicate_rate: 0.03        # 3% — simulates network retransmission
        write:
          connection: output
          format: parquet
          path: bronze/late_arriving_data.parquet
          mode: overwrite
```

**What makes this realistic:**

- 5% outlier rate is deliberately aggressive — real historians have ~0.1–1% bad data, but you want your pipeline to handle worst-case
- 3% duplicate rate simulates what happens when an IoT gateway retransmits on network timeout
- `quality_code` column lets your transforms filter on data quality — downstream logic can drop `bad` and `missing` before aggregation
- This pattern is the "crash test dummy" for your bronze-to-silver layer

!!! example "Try this"
    - Add validation to the write node testing `type: range, column: sensor_value, min: 0, max: 100` with `mode: quarantine` to catch outliers
    - Add a `type: unique` test on `record_id` to catch duplicates
    - Increase `duplicate_rate` to `0.10` and watch your dedup logic handle it

> 📖 **Learn more:** [Advanced Features](../advanced_features.md) — Chaos configuration | [Validation Tests](../../validation/tests.md) — Quarantine mode for bad data

---

## Pattern 37: Schema Evolution Test {#pattern-37}

**Industry:** Data Engineering | **Difficulty:** Advanced

!!! tip "What you'll learn"
    - **Simulate → transform → validate loop** — the simulation node generates bronze data, a transform node reshapes it (rename columns, add derived fields), and validation catches schema issues. This is how you test that your silver layer handles schema changes gracefully.

A two-node pipeline implementing the standard medallion architecture: bronze simulation → silver transform. The first node generates raw sensor data, the second reads it back, renames columns, adds derived fields, and validates the result. If you add a new column to bronze that silver doesn't expect, you'll see it pass through (schema evolution). If you remove a column that silver depends on, validation will catch it.

```yaml
project: schema_evolution
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
  - pipeline: schema_test
    nodes:
      # ── Node 1: Simulate bronze data ──
      - name: bronze_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-01T00:00:00Z"
                timestep: "5m"
                row_count: 288             # 24 hours
                seed: 42
              entities:
                count: 1
                id_prefix: "source_"
              columns:
                - name: source_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                - name: measurement_a
                  data_type: float
                  generator:
                    type: random_walk
                    start: 50
                    min: 0
                    max: 100
                    volatility: 2.0
                    mean_reversion: 0.1
                    precision: 2

                - name: measurement_b
                  data_type: float
                  generator:
                    type: range
                    min: 10
                    max: 90
                    distribution: normal
                    mean: 50
                    std_dev: 15

                - name: status_code
                  data_type: int
                  generator:
                    type: categorical
                    values: [0, 1, 2, 3]
                    weights: [0.80, 0.10, 0.07, 0.03]

                - name: category
                  data_type: string
                  generator:
                    type: categorical
                    values: [type_a, type_b, type_c]
                    weights: [0.5, 0.3, 0.2]
        write:
          connection: output
          format: parquet
          path: bronze/schema_test.parquet
          mode: overwrite

      # ── Node 2: Transform bronze → silver ──
      - name: silver_data
        read:
          connection: output
          format: parquet
          path: bronze/schema_test.parquet
        transform:
          steps:
            - operation: rename_columns
              params:
                mapping:
                  measurement_a: sensor_reading
                  measurement_b: reference_value
            - operation: add_column
              params:
                name: delta
                expression: "sensor_reading - reference_value"
            - operation: add_column
              params:
                name: status_label
                expression: "'ok' if status_code == 0 else 'warn' if status_code == 1 else 'error' if status_code == 2 else 'critical'"
        validation:
          mode: warn
          tests:
            - type: not_null
              columns: [sensor_reading, reference_value, delta]
              on_fail: quarantine
            - type: range
              column: sensor_reading
              min: 0
              max: 100
              on_fail: quarantine
          quarantine:
            connection: output
            path: quarantine/schema_test
            add_columns:
              rejection_reason: true
              rejected_at: true
        write:
          connection: output
          format: parquet
          path: silver/schema_test.parquet
          mode: overwrite
```

**What makes this realistic:**

- This is the pattern you use when testing schema changes — add a new column to the simulation, see if your transform handles it
- The two-node pipeline (bronze → silver) is the standard medallion architecture
- Validation on the silver node catches any data quality issues introduced by the transform
- If you add a new column to bronze that silver doesn't expect, you'll see it pass through (schema evolution). If you remove a column that silver depends on, validation will catch it

!!! example "Try this"
    - Add a `measurement_c` column to the bronze simulation and see if silver handles the extra column
    - Remove `measurement_b` from bronze and watch the transform fail (testing schema breakage)
    - Add a `type: column_presence` validation test to ensure required columns exist

> 📖 **Learn more:** [Validation Tests](../../validation/tests.md) — Validation modes and quarantine | [Core Concepts](../core_concepts.md) — Multi-node pipelines

---

## Pattern 38: Multi-Source Bronze Merge {#pattern-38}

**Industry:** Data Engineering | **Difficulty:** Advanced

!!! tip "What you'll learn"
    - **Multiple simulation read nodes in one pipeline** — three separate source systems generate data at different cadences (15m ERP, 5m MES, 1m SCADA) into three bronze tables. This is the pattern for testing multi-source integration where your silver layer must merge data from different schemas and cadences.

Three source systems at different cadences mirror real factory data integration challenges. ERP has business objects (orders), MES has production events, SCADA has continuous process tags — each with completely different schemas. The bronze layer keeps them separate; your silver layer must figure out how to join them by timestamp alignment, product-to-machine mapping, and more.

```yaml
project: multi_source_merge
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
  - pipeline: bronze_sources
    nodes:
      # ── Source 1: ERP system (orders, 15-minute cadence) ──
      - name: source_erp
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-01T00:00:00Z"
                timestep: "15m"
                row_count: 96              # 24 hours
                seed: 42
              entities:
                count: 1
                id_prefix: "erp_"
              columns:
                - name: source_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                - name: order_id
                  data_type: int
                  generator:
                    type: sequential
                    start: 5001
                    step: 1

                - name: product_code
                  data_type: string
                  generator:
                    type: categorical
                    values: [PROD_A, PROD_B, PROD_C]
                    weights: [0.5, 0.3, 0.2]

                - name: quantity
                  data_type: int
                  generator:
                    type: range
                    min: 1
                    max: 100

                - name: unit_cost
                  data_type: float
                  generator:
                    type: range
                    min: 5.0
                    max: 50.0
                    distribution: normal
                    mean: 20.0
                    std_dev: 8.0
        write:
          connection: output
          format: parquet
          path: bronze/erp_orders.parquet
          mode: overwrite

      # ── Source 2: MES system (production events, 5-minute cadence) ──
      - name: source_mes
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-01T00:00:00Z"
                timestep: "5m"
                row_count: 288             # 24 hours
                seed: 43
              entities:
                count: 1
                id_prefix: "mes_"
              columns:
                - name: source_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                - name: work_order
                  data_type: int
                  generator:
                    type: sequential
                    start: 9001
                    step: 1

                - name: machine_id
                  data_type: string
                  generator:
                    type: categorical
                    values: [M01, M02, M03, M04]
                    weights: [0.30, 0.25, 0.25, 0.20]

                - name: cycle_time_sec
                  data_type: float
                  generator:
                    type: range
                    min: 30
                    max: 120
                    distribution: normal
                    mean: 60
                    std_dev: 15

                - name: pass_fail
                  data_type: string
                  generator:
                    type: categorical
                    values: [pass, fail]
                    weights: [0.95, 0.05]
        write:
          connection: output
          format: parquet
          path: bronze/mes_production.parquet
          mode: overwrite

      # ── Source 3: SCADA system (process tags, 1-minute cadence) ──
      - name: source_scada
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-01T00:00:00Z"
                timestep: "1m"
                row_count: 1440            # 24 hours
                seed: 44
              entities:
                count: 1
                id_prefix: "scada_"
              columns:
                - name: source_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                - name: tag_name
                  data_type: string
                  generator:
                    type: categorical
                    values: [TT_101, PT_201, FT_301, LT_401]
                    weights: [0.25, 0.25, 0.25, 0.25]

                - name: tag_value
                  data_type: float
                  generator:
                    type: random_walk
                    start: 50
                    min: 0
                    max: 100
                    volatility: 2.0
                    mean_reversion: 0.1
                    precision: 2

                - name: quality
                  data_type: string
                  generator:
                    type: categorical
                    values: [good, bad, uncertain]
                    weights: [0.95, 0.03, 0.02]
        write:
          connection: output
          format: parquet
          path: bronze/scada_readings.parquet
          mode: overwrite
```

**What makes this realistic:**

- Three source systems at different cadences (15m ERP, 5m MES, 1m SCADA) mirror real factory data integration challenges
- ERP has business objects (orders), MES has production events, SCADA has continuous process tags — each with completely different schemas
- The bronze layer keeps them separate; your silver layer must figure out how to join them (by timestamp alignment, product_code → machine_id mapping, etc.)
- Different seeds (42, 43, 44) ensure each source generates independent data — no accidental correlation
- This is the "hard part" of data engineering that simulation lets you test before go-live

!!! example "Try this"
    - Add chaos to `source_scada` (`outlier_rate: 0.01`) to test silver-layer quality gates
    - Change `source_erp` to hourly (`timestep: "1h"`, `row_count: 24`) and see how sparse data affects joins
    - Add a fourth source (quality inspection system) with its own schema

> 📖 **Learn more:** [Core Concepts](../core_concepts.md) — Multi-node pipelines and data flow | [Validation Tests](../../validation/tests.md) — Testing merged data quality

---

← [Business & IT (31–35)](business_it.md)
