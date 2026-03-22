---
title: "Business & IT (31-35)"
---

# Business & IT (31–35)

Retail, customer service, IT operations, and supply chain patterns. These patterns show business data simulation with weighted categoricals, queue dynamics, server monitoring, API telemetry, and multi-leg shipment tracking.

!!! info "Prerequisites"
    These patterns build on [Foundational Patterns 1–8](foundations.md). Pattern 32 uses `prev()` for queue depth — see [Stateful Functions](../stateful_functions.md).

---

## Pattern 31: Retail POS Transactions {#pattern-31}

**Industry:** Retail | **Difficulty:** Beginner

!!! tip "What you'll learn"
    - **Weighted `categorical` generator** — realistic product mix with weighted SKU distribution
    - **`derived` expressions for tax and total** — business logic in simulation

A point-of-sale system records every transaction across three grocery stores. SKU frequencies follow a Pareto-like distribution (milk is the most purchased item), and tax and total amounts are derived from unit price and quantity — exactly how a real POS register calculates them.

```yaml
project: retail_pos
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
  - pipeline: pos_transactions
    nodes:
      - name: pos_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-10T00:00:00Z"
                timestep: "2m"
                row_count: 720            # 24 hours
                seed: 42
              entities:
                names: [Store_001, Store_002, Store_003]
              columns:
                - name: store_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                - name: transaction_id
                  data_type: int
                  generator:
                    type: sequential
                    start: 100001
                    step: 1

                # Weighted SKU mix — milk most frequent (Pareto-like)
                - name: sku
                  data_type: string
                  generator:
                    type: categorical
                    values: [SKU_MILK, SKU_BREAD, SKU_EGGS, SKU_COFFEE, SKU_SNACK]
                    weights: [0.25, 0.22, 0.18, 0.20, 0.15]

                - name: quantity
                  data_type: int
                  generator:
                    type: range
                    min: 1
                    max: 10

                - name: unit_price
                  data_type: float
                  generator:
                    type: range
                    min: 1.50
                    max: 25.00
                    distribution: normal
                    mean: 8.00
                    std_dev: 4.00

                # 8% sales tax
                - name: tax_amount
                  data_type: float
                  generator:
                    type: derived
                    expression: "round(unit_price * quantity * 0.08, 2)"

                - name: total
                  data_type: float
                  generator:
                    type: derived
                    expression: "round(unit_price * quantity + tax_amount, 2)"

                - name: payment_method
                  data_type: string
                  generator:
                    type: categorical
                    values: [credit_card, debit_card, cash, mobile_pay]
                    weights: [0.40, 0.25, 0.20, 0.15]

                - name: loyalty_member
                  data_type: boolean
                  generator: {type: boolean, true_probability: 0.35}

        write:
          connection: output
          format: parquet
          path: bronze/retail_pos.parquet
          mode: overwrite
```

**What makes this realistic:**

- Weighted SKU distribution follows a Pareto-like pattern — milk is the most frequent item, snacks the least
- Derived tax and total calculations mirror real POS register logic (subtotal + 8% tax)
- Payment method weights match industry averages (credit card dominant at 40%)
- Loyalty membership penetration at 35% is typical for grocery chains

!!! example "Try this"
    - Add a `discount_pct` categorical column (`[0, 5, 10, 15]` with weights `[0.70, 0.15, 0.10, 0.05]`) and adjust `total` to include the discount
    - Add `entity_overrides` for `Store_003` with higher `unit_price` range (premium location)
    - Add a `cashier_id` sequential column to track individual register operators

> 📖 **Learn more:** [Generators Reference](../generators.md) — Categorical generator with weights

---

## Pattern 32: Call Center / Ticket Queue {#pattern-32}

**Industry:** Customer Service | **Difficulty:** Intermediate

!!! tip "What you'll learn"
    - **`prev()` for queue depth accumulation** — the queue at time *t* equals previous depth + incoming calls − resolved calls. This is Little's Law in action: L = λ × W.

A call center runs three queues (support, billing, technical) over a 16-hour operating day. Queue depth accumulates over time — if inflow exceeds resolution rate, the queue grows and wait times increase with it. A lunch rush event forces maximum call volume into the support queue, creating a buildup that takes time to clear.

```yaml
project: call_center
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
  - pipeline: ticket_queue
    nodes:
      - name: queue_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-10T07:00:00Z"
                timestep: "5m"
                row_count: 192            # 16 hours
                seed: 42
              entities:
                names: [Queue_Support, Queue_Billing, Queue_Technical]
              columns:
                - name: queue_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                - name: calls_in
                  data_type: int
                  generator:
                    type: range
                    min: 0
                    max: 8
                    distribution: normal
                    mean: 3
                    std_dev: 1.5

                - name: calls_resolved
                  data_type: int
                  generator:
                    type: range
                    min: 0
                    max: 6
                    distribution: normal
                    mean: 3
                    std_dev: 1.0

                # Queue accumulation — clamped at zero
                - name: queue_depth
                  data_type: int
                  generator:
                    type: derived
                    expression: "max(0, int(prev('queue_depth', 5) + calls_in - calls_resolved))"

                # Wait time proportional to queue depth
                - name: avg_wait_min
                  data_type: float
                  generator:
                    type: derived
                    expression: "round(max(0.5, queue_depth * 2.5 + random() * 3), 1)"

                - name: satisfaction_score
                  data_type: float
                  generator:
                    type: range
                    min: 1.0
                    max: 5.0
                    distribution: normal
                    mean: 3.8
                    std_dev: 0.6

                - name: escalated
                  data_type: boolean
                  generator: {type: boolean, true_probability: 0.08}

                - name: call_type
                  data_type: string
                  generator:
                    type: categorical
                    values: [inquiry, complaint, change_request, cancellation]
                    weights: [0.45, 0.25, 0.20, 0.10]

              # Lunch rush — max calls into support queue
              scheduled_events:
                - type: forced_value
                  entity: Queue_Support
                  column: calls_in
                  value: 8
                  start_time: "2026-03-10T12:00:00Z"
                  end_time: "2026-03-10T13:00:00Z"

        write:
          connection: output
          format: parquet
          path: bronze/call_center.parquet
          mode: overwrite
```

**What makes this realistic:**

- Queue depth accumulates via `prev()` — if inflow exceeds resolution rate, the queue grows (and wait times with it)
- `calls_in` and `calls_resolved` are independent random variables so the queue can grow or shrink naturally
- Lunch rush event creates a queue buildup in support that takes time to resolve after the rush ends
- Satisfaction scores use a normal distribution centered at 3.8/5 (industry average for call centers)

!!! example "Try this"
    - Add a `staffing_level` constant and derive `calls_resolved` from it: `"min(calls_in, staffing_level + int(random() * 2))"`
    - Add a `sla_breach` derived column: `"avg_wait_min > 10"`
    - Increase the lunch rush to 2 hours and watch queue depth spiral

> 📖 **Learn more:** [Stateful Functions](../stateful_functions.md) — `prev()` for accumulation

---

## Pattern 33: Server Monitoring {#pattern-33}

**Industry:** IT Operations | **Difficulty:** Intermediate

!!! tip "What you'll learn"
    - **`ipv4` generator** — realistic server IP addresses on a /24 subnet
    - **`email` generator** — admin contact addresses tied to server names
    - **`trend` on memory usage** — simulates a memory leak that slowly consumes resources over 24 hours

A fleet of 8 servers is monitored for CPU, memory, disk I/O, and network throughput. Memory usage trends upward (a slow memory leak at ~0.3% per row), so by the end of a 24-hour window some servers will cross the critical alert threshold. Each server gets a realistic IP address on the `10.0.1.0/24` subnet and an ops contact email.

```yaml
project: server_monitoring
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
  - pipeline: server_metrics
    nodes:
      - name: metric_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-10T00:00:00Z"
                timestep: "1m"
                row_count: 1440            # 24 hours
                seed: 42
              entities:
                count: 8
                id_prefix: "srv_"
              columns:
                - name: server_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                - name: ip_address
                  data_type: string
                  generator:
                    type: ipv4
                    subnet: "10.0.1.0/24"

                - name: admin_email
                  data_type: string
                  generator:
                    type: email
                    domain: "ops.example.com"
                    pattern: "{entity}_{index}"

                - name: cpu_pct
                  data_type: float
                  generator:
                    type: random_walk
                    start: 30
                    min: 0
                    max: 100
                    volatility: 3.0
                    mean_reversion: 0.08
                    precision: 1

                # Memory leak — 0.3% per row, ~4.3% per hour
                - name: memory_pct
                  data_type: float
                  generator:
                    type: random_walk
                    start: 40
                    min: 0
                    max: 100
                    volatility: 1.0
                    mean_reversion: 0.02
                    trend: 0.003
                    precision: 1

                - name: disk_io_mbps
                  data_type: float
                  generator:
                    type: range
                    min: 0
                    max: 500
                    distribution: normal
                    mean: 80
                    std_dev: 50

                - name: network_mbps
                  data_type: float
                  generator:
                    type: random_walk
                    start: 100
                    min: 0
                    max: 1000
                    volatility: 10
                    mean_reversion: 0.1
                    precision: 0

                - name: error_count
                  data_type: int
                  generator:
                    type: range
                    min: 0
                    max: 5

                - name: alert_level
                  data_type: string
                  generator:
                    type: derived
                    expression: "'critical' if cpu_pct > 90 or memory_pct > 90 else 'warning' if cpu_pct > 75 or memory_pct > 75 else 'normal'"

              chaos:
                outlier_rate: 0.005
                outlier_factor: 2.0

        write:
          connection: output
          format: parquet
          path: bronze/server_monitoring.parquet
          mode: overwrite
```

**What makes this realistic:**

- `memory_pct` trends upward (memory leak) — by hour 24 some servers will hit the critical threshold
- `ipv4` on a /24 subnet gives realistic server rack addressing
- `email` contacts tied to server names for alert routing
- Alert thresholds at 75%/90% match standard monitoring tool defaults (Datadog, Prometheus)

!!! example "Try this"
    - Add a scheduled deployment event that spikes `cpu_pct` to 85 for 2 servers from 02:00–02:30 (maintenance window)
    - Add a `disk_usage_pct` with `trend: 0.001` for slow disk fill
    - Add a `process_count` range column (50–300)

> 📖 **Learn more:** [Generators Reference](../generators.md) — IPv4 and email generators

---

## Pattern 34: API Performance Logs {#pattern-34}

**Industry:** SaaS / Platform | **Difficulty:** Intermediate

!!! tip "What you'll learn"
    - **Latency distributions with `range`** — normal distribution for p50 latency, then deriving p99 from p50 to mimic the long-tail latency pattern seen in real API telemetry
    - **HTTP status code breakdown** — derived from error rate so status counts always sum to `request_count`

A platform monitors four API endpoints over 24 hours. Latency follows a normal distribution for p50, and p99 is derived as ~3× p50 plus noise — matching the long-tail pattern in real telemetry. The payment endpoint is inherently slower (external gateway latency), and a deployment at 03:00 causes a 15-minute latency spike on the orders endpoint.

```yaml
project: api_performance
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
  - pipeline: api_metrics
    nodes:
      - name: api_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-10T00:00:00Z"
                timestep: "1m"
                row_count: 1440            # 24 hours
                seed: 42
              entities:
                names: [api_users, api_orders, api_payments, api_inventory]
              columns:
                - name: endpoint
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                - name: request_count
                  data_type: int
                  generator:
                    type: range
                    min: 10
                    max: 500
                    distribution: normal
                    mean: 150
                    std_dev: 60

                # Payment endpoint is slower (external gateway)
                - name: latency_p50_ms
                  data_type: float
                  generator:
                    type: range
                    min: 5
                    max: 200
                    distribution: normal
                    mean: 45
                    std_dev: 20
                  entity_overrides:
                    api_payments:
                      type: range
                      min: 20
                      max: 500
                      distribution: normal
                      mean: 120
                      std_dev: 40

                # p99 typically 3-5x p50
                - name: latency_p99_ms
                  data_type: float
                  generator:
                    type: derived
                    expression: "round(latency_p50_ms * 3.0 + random() * 50, 1)"

                - name: error_rate_pct
                  data_type: float
                  generator:
                    type: range
                    min: 0
                    max: 5.0
                    distribution: normal
                    mean: 0.5
                    std_dev: 0.5

                - name: status_2xx
                  data_type: int
                  generator:
                    type: derived
                    expression: "int(request_count * (1.0 - error_rate_pct / 100.0))"

                # 70% of errors are client errors
                - name: status_4xx
                  data_type: int
                  generator:
                    type: derived
                    expression: "int(request_count * error_rate_pct / 100.0 * 0.7)"

                # Remainder are server errors
                - name: status_5xx
                  data_type: int
                  generator:
                    type: derived
                    expression: "max(0, request_count - status_2xx - status_4xx)"

                # Requests per second over 1-min window
                - name: throughput_rps
                  data_type: float
                  generator:
                    type: derived
                    expression: "round(request_count / 60.0, 2)"

              # Deployment spike on orders endpoint
              scheduled_events:
                - type: forced_value
                  entity: api_orders
                  column: latency_p50_ms
                  value: 180
                  start_time: "2026-03-10T03:00:00Z"
                  end_time: "2026-03-10T03:15:00Z"

        write:
          connection: output
          format: parquet
          path: bronze/api_performance.parquet
          mode: overwrite
```

**What makes this realistic:**

- p99 latency derived as 3× p50 + noise matches real long-tail distributions observed in production APIs
- Payment endpoint is slower (external payment gateway latency) via entity override
- HTTP status breakdown (`2xx + 4xx + 5xx`) sums to `request_count` — no made-up numbers
- Deployment event causes a latency spike that propagates to p99
- `error_rate_pct` uses a normal distribution centered at 0.5% (common SLO target)

!!! example "Try this"
    - Add an `slo_breach` derived column: `"latency_p99_ms > 500 or error_rate_pct > 2.0"`
    - Add a `cache_hit_rate` range column (0.80–0.99 with normal distribution)
    - Add chaos to `api_inventory` for database connection issues

> 📖 **Learn more:** [Generators Reference](../generators.md) — Range generator with normal distribution

---

## Pattern 35: Supply Chain Shipments {#pattern-35}

**Industry:** Logistics | **Difficulty:** Advanced

!!! tip "What you'll learn"
    - **`geo` generator with different bbox per entity** — multi-leg shipment tracking where each leg has a different geographic bounding box (Houston → Memphis → NYC)
    - **`uuid` v4** — unique shipment identifiers
    - **Cross-entity temperature monitoring** — cold chain tracking across transit legs

A supply chain ships goods across three legs: origin (Houston), transit hub (Memphis), and destination (NYC). Each leg generates GPS coordinates within its geographic region, shipment IDs are globally unique UUIDs, and temperature is tracked for cold-chain compliance. ETA counts down with realistic jitter as delays and early arrivals shift the expected time.

```yaml
project: supply_chain
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
  - pipeline: shipments
    nodes:
      - name: shipment_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-03-10T00:00:00Z"
                timestep: "1h"
                row_count: 72             # 3 days
                seed: 42
              entities:
                names: [Leg_Origin, Leg_Transit, Leg_Destination]
              columns:
                - name: leg_id
                  data_type: string
                  generator: {type: constant, value: "{entity_id}"}
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}

                - name: shipment_id
                  data_type: string
                  generator:
                    type: uuid
                    version: 4

                # Houston → Memphis → NYC via entity overrides
                - name: location
                  data_type: string
                  generator:
                    type: geo
                    bbox: [30.0, -95.5, 30.5, -95.0]
                    format: "tuple"
                  entity_overrides:
                    Leg_Transit:
                      type: geo
                      bbox: [35.0, -90.0, 35.5, -89.5]
                      format: "tuple"
                    Leg_Destination:
                      type: geo
                      bbox: [40.5, -74.5, 41.0, -74.0]
                      format: "tuple"

                - name: status
                  data_type: string
                  generator:
                    type: categorical
                    values: [in_transit, at_warehouse, loading, customs_hold]
                    weights: [0.60, 0.20, 0.12, 0.08]

                - name: weight_kg
                  data_type: float
                  generator: {type: constant, value: 2500.0}

                # Cold chain — target 2°C
                - name: temperature_c
                  data_type: float
                  generator:
                    type: random_walk
                    start: 2.0
                    min: -5.0
                    max: 15.0
                    volatility: 0.5
                    mean_reversion: 0.15
                    precision: 1

                - name: humidity_pct
                  data_type: float
                  generator:
                    type: range
                    min: 60
                    max: 90
                    distribution: normal
                    mean: 75
                    std_dev: 5

                # ETA countdown with jitter
                - name: eta_hours
                  data_type: float
                  generator:
                    type: derived
                    expression: "round(max(0, prev('eta_hours', 72) - 1.0 + random() * 0.3 - 0.15), 1)"

                # Cold chain excursion detection
                - name: temp_excursion
                  data_type: boolean
                  generator:
                    type: derived
                    expression: "temperature_c > 5.0 or temperature_c < -2.0"

              chaos:
                outlier_rate: 0.01
                outlier_factor: 2.0

        write:
          connection: output
          format: parquet
          path: bronze/supply_chain.parquet
          mode: overwrite
```

**What makes this realistic:**

- Different geo bbox per entity leg traces a real shipping route (Houston → Memphis → NYC)
- `uuid` v4 gives globally unique shipment IDs — just like real logistics systems
- Temperature tracks cold chain with excursion detection at ±thresholds (>5°C or <−2°C)
- ETA counts down with realistic jitter (delays and early arrivals)
- Status weights reflect real logistics operations (60% in transit, 8% customs hold)

!!! example "Try this"
    - Add a `customs_delay` scheduled event forcing `status` to `"customs_hold"` for `Leg_Transit` for 12 hours
    - Add a `fuel_consumption_l` column derived from `weight_kg` and a speed factor
    - Add a `delay_flag` derived from eta_hours: `"eta_hours > prev('eta_hours', 72)"`

> 📖 **Learn more:** [Generators Reference](../generators.md) — Geo generator and UUID generator | [Advanced Features](../advanced_features.md) — Entity overrides

---

← [Healthcare & Life Sciences (29–30)](healthcare.md) | [Data Engineering Meta-Patterns (36–38)](data_engineering.md) →
