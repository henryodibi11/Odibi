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

Real-world simulation patterns across manufacturing, operations, IoT, business, and data engineering — **38 patterns** organized by industry and complexity.

Each pattern is a **complete, copy-paste-ready YAML config** that also teaches you an odibi feature progressively. Start with the foundational patterns and work your way through the categories that match your domain.

---

!!! info "How to Use This Section"

    Every pattern follows the same format:

    1. **What you'll learn** — the key odibi features the pattern demonstrates
    2. **Full YAML config** — copy-paste it, run it, see it work
    3. **What makes this realistic** — why the config choices produce believable data
    4. **Try this** — suggested modifications to deepen your understanding
    5. **Learn more** — links to relevant reference docs

    Patterns are numbered **1–38** and build on each other. If a pattern uses a feature you haven't seen, check the earlier patterns or the linked reference docs.

---

## Foundational Patterns (1–8)

Start here. These patterns cover the core simulation features you'll use in every project.

👉 **[View all foundational patterns](foundations.md)**

| # | Pattern | Key Feature | Industry |
|---|---------|-------------|----------|
| 1 | Build Before Sources Exist | Core simulation concept | General |
| 2 | Manufacturing Production Line | `entity_overrides`, `scheduled_events`, `chaos` | Manufacturing |
| 3 | IoT Sensor Network | `daily_profile`, `derived` from occupancy, `random_walk` | Building Mgmt |
| 3b | HVAC Feedback Loop | Derived column chaining, feedback modeling | Building Mgmt |
| 4 | Order / Transaction Data | `incremental: stateful`, `derived` expressions | E-commerce |
| 5 | Equipment Degradation | `trend`, `recurrence`, cleaning cycles | Maintenance |
| 6 | Stress Test at Scale | High-volume config | Data Engineering |
| 7 | Daily Dashboard Feed | Incremental + Delta Lake append | Analytics |
| 8 | Multi-System Integration | Cross-entity refs, `prev()` | Data Engineering |

---

## Process & Chemical Engineering (9–15)

Advanced process control patterns for continuous and batch operations.

👉 **[View process engineering patterns](process_engineering.md)**

| # | Pattern | Key Feature | Industry |
|---|---------|-------------|----------|
| 9 | Wastewater Treatment Plant | Cross-entity cascade (stage→stage) | Environmental |
| 10 | Compressor Station | `shock_rate` / `shock_bias` | Oil & Gas |
| 11 | CSTR with PID Control | `pid()` in full pipeline | Chemical Engineering |
| 12 | Distillation Column | `mean_reversion_to` dynamic column | Chemical Engineering |
| 13 | Cooling Tower | `ema()` for signal smoothing | Utilities |
| 14 | Batch Reactor with Recipe | Scheduled setpoint changes | Pharma / ChemE |
| 15 | Tank Farm Inventory | `prev()` for level integration | Oil & Gas / Logistics |

---

## Energy & Utilities (16–20)

Renewable energy, grid storage, and utility network patterns.

👉 **[View energy & utilities patterns](energy_utilities.md)**

| # | Pattern | Key Feature | Industry |
|---|---------|-------------|----------|
| 16 | Solar Farm | `boolean` generator, weather coupling | Renewables |
| 17 | Wind Turbine Fleet | `geo` generator, entity overrides at scale | Renewables |
| 18 | Battery Storage (BESS) | State of charge with `prev()` | Energy Storage |
| 19 | Smart Meter Network | `ipv4` generator, high entity count | Utilities |
| 20 | EV Charging Stations | `uuid` v5 deterministic IDs | Transportation |

---

## Manufacturing & Operations (21–25)

Discrete manufacturing, logistics, and quality control patterns.

👉 **[View manufacturing patterns](manufacturing.md)**

| # | Pattern | Key Feature | Industry |
|---|---------|-------------|----------|
| 21 | Packaging Line with SPC | Validation on simulated data | Food & Bev |
| 22 | CNC Machine Shop | `downtime_events` in chaos | Discrete Manufacturing |
| 23 | Warehouse Inventory | Multi-pipeline project | Logistics |
| 24 | Food Safety / Cold Chain | `email` generator, alert thresholds | Food & Bev |
| 25 | Assembly Line Stations | Cross-entity station-to-station flow | Automotive |

---

## Environmental & Agriculture (26–28)

Weather, air quality, and precision agriculture patterns.

👉 **[View environmental patterns](environmental.md)**

| # | Pattern | Key Feature | Industry |
|---|---------|-------------|----------|
| 26 | Weather Station Network | `geo` bbox, multi-sensor | Meteorology |
| 27 | Air Quality Monitoring | `trend` for seasonal drift | Environmental |
| 28 | Greenhouse / Indoor Farm | PID + dynamic setpoint tracking | Agriculture |

---

## Healthcare & Life Sciences (29–30)

Clinical monitoring and pharmaceutical batch records.

👉 **[View healthcare patterns](healthcare.md)**

| # | Pattern | Key Feature | Industry |
|---|---------|-------------|----------|
| 29 | ICU Patient Vitals | High-frequency data, alarm thresholds | Healthcare |
| 30 | Pharma Batch Records | Sequential batch IDs, recipe phases | Pharma |

---

## Business & IT (31–35)

Retail, customer service, IT operations, and SaaS patterns.

👉 **[View business & IT patterns](business_it.md)**

| # | Pattern | Key Feature | Industry |
|---|---------|-------------|----------|
| 31 | Retail POS Transactions | Weighted categoricals, derived totals | Retail |
| 32 | Call Center / Ticket Queue | `prev()` for queue depth | Customer Service |
| 33 | Server Monitoring | `ipv4` + `email`, CPU/memory walks | IT Ops |
| 34 | API Performance Logs | Latency distributions, error rates | SaaS |
| 35 | Supply Chain Shipments | `geo`, `uuid`, multi-leg tracking | Logistics |

---

## Data Engineering Meta-Patterns (36–38)

Patterns for testing your data platform itself — schema changes, late data, multi-source merges.

👉 **[View data engineering patterns](data_engineering.md)**

| # | Pattern | Key Feature | Industry |
|---|---------|-------------|----------|
| 36 | Late-Arriving Data | Chaos + out-of-order timestamps | Testing |
| 37 | Schema Evolution Test | Simulation → transform → validate | Testing |
| 38 | Multi-Source Bronze Merge | Multiple sim nodes → one silver | Architecture |

---

## Feature Heat Map

Which features does each pattern use? Denser rows mean more complex simulations. Use this to find patterns that teach a specific feature, or to find the most feature-rich patterns for deep learning.

| # | Pattern | range | walk | d.prof | cat | seq | derived | const | ts | uuid | bool | geo | ip | email | prev | ema | pid | overrides | events | chaos | trend | m.r.to | shock | incr | x-entity | valid |
|---|---------|:-----:|:----:|:------:|:---:|:---:|:-------:|:-----:|:--:|:----:|:----:|:---:|:--:|:-----:|:----:|:---:|:---:|:---------:|:------:|:-----:|:-----:|:------:|:-----:|:----:|:--------:|:-----:|
| 1 | Build Before Sources | **X** | | | **X** | **X** | | | **X** | **X** | | | | | | | | | | | | | | | | **X** |
| 2 | Production Line | **X** | | | **X** | | | **X** | **X** | | | | | | | | | **X** | **X** | **X** | | | | | | |
| 3 | IoT Sensors | | **X** | **X** | | | **X** | | **X** | | | | | | | | | **X** | **X** | **X** | | | | | | |
| 3b | HVAC Feedback | | **X** | **X** | | | **X** | | **X** | | | | | | | | | **X** | **X** | **X** | | | | | | |
| 4 | Order Data | **X** | | | **X** | **X** | **X** | | **X** | **X** | | | | | | | | | | | | | | **X** | | |
| 5 | Degradation | **X** | **X** | | | | **X** | **X** | **X** | | | | | | | | | | **X** | | **X** | **X** | | | | |
| 6 | Stress Test | **X** | | | **X** | | | | **X** | | | | | | | | | | | | | | | | | |
| 7 | Dashboard Feed | | **X** | | | | **X** | | **X** | | | | | | | | | | | | | | | **X** | | |
| 8 | Multi-System | | **X** | | | | **X** | **X** | **X** | | | | | | **X** | | | | | | | | | | **X** | |
| 9 | Wastewater | **X** | **X** | | | | **X** | | **X** | | | | | | | | | **X** | **X** | | | | | | **X** | |
| 10 | Compressor | | **X** | | **X** | | **X** | | **X** | | | | | | | | | | | **X** | | | **X** | | | |
| 11 | CSTR + PID | | | | | | **X** | **X** | **X** | | | | | | **X** | | **X** | | | | | | | | | |
| 12 | Distillation | | **X** | | | | **X** | | **X** | | | | | | | | | | | | | **X** | | | | |
| 13 | Cooling Tower | | **X** | | | | **X** | | **X** | | | | | | | **X** | | | | | | | | | | |
| 14 | Batch Reactor | | | | | | **X** | **X** | **X** | | | | | | **X** | | | | **X** | | | | | | | |
| 15 | Tank Farm | | **X** | | | | **X** | **X** | **X** | | | | | | **X** | | | | | | | | | | | |
| 16 | Solar Farm | **X** | **X** | | **X** | | **X** | | **X** | | **X** | | | | | | | | | | | | | | | |
| 17 | Wind Turbines | | **X** | | | | **X** | | **X** | | | **X** | | | | | | **X** | | | | | | | | |
| 18 | BESS | | **X** | | | | **X** | **X** | **X** | | | | | | **X** | | | | | | | | | | | |
| 19 | Smart Meters | **X** | **X** | | | | **X** | | **X** | | | | **X** | | | | | | | | | | | | | |
| 20 | EV Charging | **X** | **X** | | **X** | | **X** | | **X** | **X** | | **X** | | | | | | | | | | | | | | |
| 21 | Packaging SPC | **X** | **X** | | | | **X** | | **X** | | | | | | | | | | | **X** | | | | | | **X** |
| 22 | CNC Shop | **X** | **X** | | **X** | | **X** | | **X** | | | | | | | | | | | **X** | | | | | | |
| 23 | Warehouse | **X** | | | **X** | **X** | **X** | | **X** | | | | | | **X** | | | | | | | | | | | |
| 24 | Cold Chain | **X** | **X** | | | | **X** | | **X** | | | | | **X** | | | | | | | | | | | | |
| 25 | Assembly Line | | **X** | | **X** | | **X** | | **X** | | | | | | | | | | | | | | | | **X** | |
| 26 | Weather Stations | **X** | **X** | | | | **X** | | **X** | | **X** | **X** | | | | | | | | **X** | | | | | | |
| 27 | Air Quality | | **X** | | **X** | | **X** | | **X** | | | | | | | | | | | | **X** | | | | | |
| 28 | Greenhouse | | **X** | | | | **X** | **X** | **X** | | | | | | **X** | | **X** | | | | | **X** | | | | |
| 29 | ICU Vitals | **X** | **X** | | **X** | | **X** | | **X** | | | | | | | | | | | | | | | | | |
| 30 | Pharma Batch | **X** | **X** | | **X** | **X** | **X** | **X** | **X** | | | | | | **X** | | | | **X** | | | | | | | |
| 31 | Retail POS | **X** | | | **X** | | **X** | | **X** | | | | | | | | | | | | | | | | | |
| 32 | Call Center | **X** | | | **X** | | **X** | | **X** | | | | | | **X** | | | | | | | | | | | |
| 33 | Server Monitor | **X** | **X** | | **X** | | **X** | | **X** | | | | **X** | **X** | | | | | | | | | | | | |
| 34 | API Perf Logs | **X** | | | **X** | | **X** | | **X** | **X** | | | **X** | | | | | | | | | | | | | |
| 35 | Supply Chain | **X** | | | **X** | | **X** | | **X** | **X** | | **X** | | | | | | | | | | | | | | |
| 36 | Late Data | | **X** | | **X** | **X** | | | **X** | | | | | | | | | | | **X** | | | | | | **X** |
| 37 | Schema Evolution | **X** | | | **X** | **X** | **X** | | **X** | | **X** | | | | | | | | | | | | | | | **X** |
| 38 | Multi-Source Merge | **X** | **X** | | **X** | **X** | **X** | | **X** | **X** | | | | | | | | | | **X** | | | | | | |

**Legend:** range = `range`, walk = `random_walk`, d.prof = `daily_profile`, cat = `categorical`, seq = `sequential`, const = `constant`, ts = `timestamp`, m.r.to = `mean_reversion_to`, incr = `incremental: stateful`, x-entity = cross-entity references, valid = validation on simulated data

---

## Find Your Pattern

!!! tip "🎯 Start Here — Pick Your Path"

    === "Chemical / Process Engineer"

        **Your journey:** Pattern 9 (Wastewater) → 11 (CSTR + PID) → 12 (Distillation) → 13 (Cooling Tower EMA) → 14 (Batch Reactor) → 15 (Tank Farm) → 28 (Greenhouse PID)

        You already think in terms of mass balances, PID loops, and process dynamics. Start with Pattern 9 to see how cross-entity cascades model stage-to-stage flow. Then Pattern 11 introduces `pid()` — pay attention to the negative gains for reverse-acting cooling. Pattern 13 shows `ema()` for noisy sensor smoothing. By Pattern 15, you'll be integrating tank levels with `prev()`.

        **Key gotcha:** PID sign convention. Cooling loops need negative Kp. If your controller output is stuck at 0 or 100, check your signs first. See Pattern 11's "Why are the PID gains negative?" callout.

    === "Data Engineer / Analytics Engineer"

        **Your journey:** Pattern 1 (Build Before Sources) → 4 (Incremental) → 6 (Stress Test) → 36 (Late Data) → 37 (Schema Evolution) → 38 (Multi-Source Merge) → 7 (Dashboard Feed)

        You care about pipeline architecture, not domain physics. Start with Pattern 1 to see the medallion architecture (bronze → silver → gold) with simulation as the bronze source. Pattern 4 adds `incremental: stateful` for continuous feeds. Pattern 36 is your crash test dummy — 5% outliers and 3% duplicates deliberately break things. Pattern 38 is the real challenge: merging ERP + MES + SCADA with different cadences.

        **Key gotcha:** `prev()` column ordering. The column using `prev('column_x')` must appear AFTER `column_x` in the YAML. The simulator evaluates columns top-to-bottom. If you reference a column that hasn't been generated yet, you'll get the default value every time.

    === "Business Analyst / Junior DE"

        **Your journey:** Pattern 1 (Build Before Sources) → 2 (Production Line) → 3 (IoT Sensors) → 4 (Orders) → 31 (Retail POS) → 32 (Call Center)

        Start simple. Pattern 1 teaches the core concept: simulate data, build your pipeline, swap to real data later. Pattern 2 adds entity_overrides and scheduled_events. Pattern 3 shows daily_profile for realistic time-of-day behavior. By Pattern 4, you're running incremental pipelines. Patterns 31-32 are business-domain patterns you can show to stakeholders.

        **Key gotcha:** `seed` makes simulation reproducible. Always set a seed when debugging — run it twice, get the same data. Remove the seed (or change it) when you want variety.

    === "IoT / Embedded Engineer"

        **Your journey:** Pattern 3 (Building Sensors) → 5 (Equipment Degradation) → 19 (Smart Meters) → 24 (Cold Chain) → 26 (Weather Stations) → 29 (ICU Vitals)

        You live in the world of sensors, telemetry, and signal noise. Pattern 3 models a 20-sensor building network with daily_profile occupancy, null_rate for sensor dropouts, and derived CO2. Pattern 5 adds trend for degradation curves. Pattern 19 shows ipv4 generator for network addresses at scale. Pattern 29 is high-frequency (30-second) clinical monitoring.

        **Key gotcha:** `null_rate` vs `downtime_events` vs `forced_value: null`. They produce different data shapes: null_rate creates random NULLs, downtime_events create missing rows (no row at all), and forced_value null creates a continuous block of NULLs. Choose based on what failure mode you're modeling.

    === "Energy / Renewables Engineer"

        **Your journey:** Pattern 16 (Solar Farm) → 17 (Wind Turbines) → 18 (BESS) → 20 (EV Charging)

        Solar irradiance, wind speed, battery state-of-charge — these patterns model the physics of renewable energy. Pattern 16 chains weather → panel temp → efficiency → power using derived expressions. Pattern 18 implements Coulomb counting for battery SOC via `prev()`. Note that Pattern 16's random walk irradiance doesn't model day/night — see the "Try this" section for how to add a `daily_profile` solar curve.

        **Key gotcha:** `prev()` for energy integration requires the right units. `power_kw * 5.0 / 60.0` converts 5-minute power to hourly energy (kWh). Get the time conversion wrong and your cumulative energy will be off by 12x or 60x.

    === "Manufacturing / Quality Engineer"

        **Your journey:** Pattern 2 (Production Line) → 21 (Packaging SPC) → 22 (CNC Shop) → 23 (Warehouse) → 24 (Cold Chain) → 25 (Assembly Line)

        OEE, SPC, control charts — these are your daily tools. Pattern 2 is a quick win: 5 machines, one shift, entity overrides for the problem machine. Pattern 21 adds validation rules that work exactly like SPC limits. Pattern 22 introduces downtime_events (missing rows, not null values). Pattern 25 models Theory of Constraints with cross-entity station flow.

        **Key gotcha:** Chaos `outlier_rate` and `outlier_factor` interact. A 0.008 rate with 2.5x factor gives ~4 outliers per shift at 2.5x the normal range. Increase the factor for more dramatic spikes; increase the rate for more frequent ones.

---

## Feature Coverage Matrix

Use this table to find which pattern teaches a specific feature.

### Generators

| Feature | Patterns |
|---------|----------|
| `range` (uniform / normal) | 1, 2, 3, 4, 5, 6 |
| `random_walk` | 3, 5, 7, 8, 10, 17 |
| `categorical` (weighted) | 2, 4, 6, 31 |
| `sequential` | 1, 4, 30 |
| `derived` | 4, 5, 8, 11, 31 |
| `constant` | 2, 5 |
| `timestamp` | 1, 2, 3, 4 |
| `uuid` | 1, 4, 20, 35 |
| `boolean` | 16 |
| `geo` | 17, 26, 35 |
| `ipv4` | 19, 33 |
| `email` | 24, 33 |

### Stateful Functions

| Feature | Patterns |
|---------|----------|
| `prev()` | 8, 15, 18, 32 |
| `ema()` | 13 |
| `pid()` | 11, 28 |

### Advanced Features

| Feature | Patterns |
|---------|----------|
| `entity_overrides` | 2, 17 |
| `scheduled_events` | 2, 3, 5, 12, 14, 18, 22 |
| `recurrence` | 5, 18 |
| `condition` | 22 |
| `transition: ramp` | 12 |
| `chaos` (outliers, duplicates) | 2, 3, 10, 36 |
| `null_rate` | 3 |
| `trend` | 5, 27 |
| `mean_reversion_to` | 5, 12 |
| `shock_rate` / `shock_bias` | 10 |
| `incremental: stateful` | 4, 7 |
| Cross-entity references | 8, 9, 25 |
| Multi-pipeline project | 23 |
| Validation on simulated data | 1, 21, 37 |

---

## Troubleshooting Simulation Patterns {#troubleshooting}

Common pitfalls when building simulation configs. If your simulation output doesn't look right, check here first.

!!! warning "Column Ordering for `prev()` and `pid()`"
    **Problem:** Your `prev('column_x')` always returns the default value, never the actual previous row's value.

    **Cause:** The column using `prev('column_x')` is defined BEFORE `column_x` in the YAML. The simulator evaluates columns top-to-bottom within each timestep. If `column_x` hasn't been calculated yet when `prev('column_x')` runs, it sees the default value.

    **Fix:** Move the column that *uses* `prev()` to appear AFTER the column it references. For example, `cooling_pct` (which reads `prev('reactor_temp_c')`) must come BEFORE `reactor_temp_c` in Pattern 11 — because cooling is computed first, then temperature responds. But the key rule is: the column being *read by prev()* must have been defined in a previous timestep.

    ```yaml
    # ✅ CORRECT — cooling reads prev temp, then temp uses current cooling
    - name: cooling_pct
      generator:
        type: derived
        expression: "pid(pv=prev('reactor_temp_c', 85.0), sp=temp_setpoint_c, ...)"
    - name: reactor_temp_c
      generator:
        type: derived
        expression: "prev('reactor_temp_c', 85.0) + ..."
    ```

!!! warning "PID Sign Convention (Direct vs. Reverse Acting)"
    **Problem:** Your PID controller output is stuck at 0 or 100 and the process variable is running away from setpoint.

    **Cause:** Wrong sign on Kp/Ki/Kd. Odibi's `pid()` calculates `error = setpoint - process_variable`.

    **Fix:** Use this table:

    | Controller type | When PV > SP, you need... | Kp sign |
    |-----------------|---------------------------|---------|
    | Cooling valve, fan, vent, drain | MORE output | **Negative** |
    | Heater, steam valve, fill valve | LESS output | **Positive** |

    If temperature is above setpoint and you need MORE cooling → Kp must be negative.
    If level is above setpoint and you need MORE draining → Kp must be negative.
    If temperature is below setpoint and you need MORE heating → Kp must be positive.

!!! warning "Cross-Entity References Not Resolving"
    **Problem:** `Entity.column` expression returns 0 or NaN instead of the expected upstream value.

    **Cause:** The entity name in the expression doesn't exactly match the entity name in the `names` list (case-sensitive). Or the entity_overrides block is on the wrong column.

    **Fix:** Entity names are case-sensitive. `Influent.flow_mgd` works if the entity is named `Influent`, but NOT if it's named `influent` or `INFLUENT`. Double-check spelling.

    ```yaml
    entities:
      names: [Influent, Primary, Aeration]   # These exact names...
    # ...
    expression: "Influent.flow_mgd * 0.98"    # ...must match here
    ```

!!! warning "Random Walk Producing Unrealistic Values"
    **Problem:** A random walk variable (like temperature or pressure) drifts to extreme values and stays there.

    **Cause:** `mean_reversion` is too low or missing. Without mean reversion, a random walk is a pure Brownian motion that will eventually hit the min or max bounds and stay there.

    **Fix:** Add or increase `mean_reversion`. Values of 0.05-0.2 are typical. Higher values (0.15-0.2) for tightly controlled variables like pressure; lower values (0.05-0.08) for naturally drifting variables like temperature.

    ```yaml
    generator:
      type: random_walk
      start: 50.0
      min: 0.0
      max: 100.0
      volatility: 1.0
      mean_reversion: 0.1    # Pull back toward start value
    ```

!!! warning "Scheduled Events Not Appearing in Output"
    **Problem:** You defined a `scheduled_event` but the forced value doesn't show up in the output.

    **Cause 1:** `start_time` is outside the simulation window. Check that your event times fall within the `start_time` + (`timestep` × `row_count`) window.

    **Cause 2:** The `entity` name doesn't match. Scheduled events use exact entity name matching (case-sensitive).

    **Cause 3:** Using `end_time` vs `duration` — you need one or the other, not both. `end_time` is an absolute timestamp; `duration` is relative to `start_time`.

!!! warning "Derived Expression Errors"
    **Problem:** A derived column produces unexpected values or errors.

    **Common causes:**

    - **Division by zero:** Use `max(denominator, 0.001)` to guard against zero division
    - **None propagation:** If any input column has null values, the derived expression may produce None. Use `0 if column is None else expression` to handle nulls
    - **Operator precedence:** Python `and`/`or` vs `&`/`|` — use Python-style `and`/`or` in derived expressions, not bitwise operators
    - **String comparisons:** Use `==` not `is` for string comparison in expressions

!!! warning "Incremental Mode Producing Duplicate Data"
    **Problem:** Running the pipeline twice produces overlapping timestamps.

    **Cause:** The system catalog wasn't configured, or the `incremental` block is missing from the read node.

    **Fix:** Ensure you have both: (1) a `system:` connection block in the project config, and (2) an `incremental: { mode: stateful, column: timestamp }` block on the read node. The system catalog stores the last-generated timestamp so the next run picks up where it left off.

!!! warning "Entity Overrides Not Working"
    **Problem:** An entity_override is defined but the entity uses the default generator instead.

    **Cause:** The entity name in `entity_overrides` doesn't match the entity's actual name. With `count: 5, id_prefix: "machine_"`, entities are named `machine_00` through `machine_04` (zero-indexed). With `names: [A, B, C]`, entities are named exactly `A`, `B`, `C`.

    **Fix:** Check your entity naming. Use `names:` for explicit control, or remember that `count:`-based entities are zero-indexed with the prefix.

!!! tip "Quick Debugging Checklist"
    1. **Set `seed: 42`** — makes output reproducible for debugging
    2. **Start with `row_count: 10`** — verify the structure before generating thousands of rows
    3. **Check column order** — `prev()` and `pid()` depend on evaluation order
    4. **Check entity names** — case-sensitive everywhere (entity_overrides, scheduled_events, cross-entity refs)
    5. **Check time windows** — scheduled events must fall within the simulation's start/end range
    6. **Read the story** — use the `story:` block to auto-generate a narrative of what happened in the simulation

---

## Standalone YAML Files

All 38 patterns are available as standalone, copy-paste-ready YAML configs in the [`examples/simulation_patterns/`](https://github.com/henryodibi11/Odibi/blob/main/examples/simulation_patterns/) directory.

Two variants are provided for each pattern:

- **`oneshot/`** — Single-run configs using Parquet format with overwrite mode. Run once, get data.
- **`datalake/`** — Incremental configs using Delta format with append mode. Run daily for a growing dataset.

```bash
# Run a oneshot pattern
odibi run examples/simulation_patterns/oneshot/01_sales_pipeline.yaml

# Run an incremental datalake pattern
odibi run examples/simulation_patterns/datalake/01_sales_pipeline.yaml
```

---

## See Also

- **[Getting Started](../getting_started.md)** - Your first simulation in 5 minutes
- **[Generators Reference](../generators.md)** - All generator types with parameters and examples
- **[Advanced Features](../advanced_features.md)** - Cross-entity references, scheduled events (recurring, condition-based, ramp), entity overrides, chaos
- **[Process Simulation](../process_simulation.md)** - Chemical engineering and process control deep dive
