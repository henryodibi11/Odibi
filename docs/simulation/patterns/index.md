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
| 3 | IoT Sensor Network | `null_rate`, `random_walk` + `mean_reversion` | Building Mgmt |
| 4 | Order / Transaction Data | `incremental: stateful`, `derived` expressions | E-commerce |
| 5 | Equipment Degradation | `trend`, `mean_reversion_to`, cleaning cycles | Maintenance |
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

| # | Pattern | range | walk | cat | seq | derived | const | ts | uuid | bool | geo | ip | email | prev | ema | pid | overrides | events | chaos | trend | m.r.to | shock | incr | x-entity | valid |
|---|---------|:-----:|:----:|:---:|:---:|:-------:|:-----:|:--:|:----:|:----:|:---:|:--:|:-----:|:----:|:---:|:---:|:---------:|:------:|:-----:|:-----:|:------:|:-----:|:----:|:--------:|:-----:|
| 1 | Build Before Sources | **X** | | **X** | **X** | | | **X** | **X** | | | | | | | | | | | | | | | | **X** |
| 2 | Production Line | **X** | | **X** | | | **X** | **X** | | | | | | | | | **X** | **X** | **X** | | | | | | |
| 3 | IoT Sensors | **X** | **X** | | | | | **X** | | | | | | | | | | **X** | **X** | | | | | | |
| 4 | Order Data | **X** | | **X** | **X** | **X** | | **X** | **X** | | | | | | | | | | | | | | **X** | | |
| 5 | Degradation | **X** | **X** | | | **X** | **X** | **X** | | | | | | | | | | **X** | | **X** | **X** | | | | |
| 6 | Stress Test | **X** | | **X** | | | | **X** | | | | | | | | | | | | | | | | | |
| 7 | Dashboard Feed | | **X** | | | **X** | | **X** | | | | | | | | | | | | | | | **X** | | |
| 8 | Multi-System | | **X** | | | **X** | **X** | **X** | | | | | | **X** | | | | | | | | | | **X** | |
| 9 | Wastewater | **X** | **X** | | | **X** | | **X** | | | | | | | | | **X** | **X** | | | | | | **X** | |
| 10 | Compressor | | **X** | **X** | | **X** | | **X** | | | | | | | | | | | **X** | | | **X** | | | |
| 11 | CSTR + PID | | | | | **X** | **X** | **X** | | | | | | **X** | | **X** | | | | | | | | | |
| 12 | Distillation | | **X** | | | **X** | | **X** | | | | | | | | | | | | | **X** | | | | |
| 13 | Cooling Tower | | **X** | | | **X** | | **X** | | | | | | | **X** | | | | | | | | | | |
| 14 | Batch Reactor | | | | | **X** | **X** | **X** | | | | | | **X** | | | | **X** | | | | | | | |
| 15 | Tank Farm | | **X** | | | **X** | **X** | **X** | | | | | | **X** | | | | | | | | | | | |
| 16 | Solar Farm | **X** | **X** | **X** | | **X** | | **X** | | **X** | | | | | | | | | | | | | | | |
| 17 | Wind Turbines | | **X** | | | **X** | | **X** | | | **X** | | | | | | **X** | | | | | | | | |
| 18 | BESS | | **X** | | | **X** | **X** | **X** | | | | | | **X** | | | | | | | | | | | |
| 19 | Smart Meters | **X** | **X** | | | **X** | | **X** | | | | **X** | | | | | | | | | | | | | |
| 20 | EV Charging | **X** | **X** | **X** | | **X** | | **X** | **X** | | **X** | | | | | | | | | | | | | | |
| 21 | Packaging SPC | **X** | **X** | | | **X** | | **X** | | | | | | | | | | | **X** | | | | | | **X** |
| 22 | CNC Shop | **X** | **X** | **X** | | **X** | | **X** | | | | | | | | | | | **X** | | | | | | |
| 23 | Warehouse | **X** | | **X** | **X** | **X** | | **X** | | | | | | **X** | | | | | | | | | | | |
| 24 | Cold Chain | **X** | **X** | | | **X** | | **X** | | | | | **X** | | | | | | | | | | | | |
| 25 | Assembly Line | | **X** | **X** | | **X** | | **X** | | | | | | | | | | | | | | | | **X** | |
| 26 | Weather Stations | **X** | **X** | | | **X** | | **X** | | **X** | **X** | | | | | | | | **X** | | | | | | |
| 27 | Air Quality | | **X** | **X** | | **X** | | **X** | | | | | | | | | | | | **X** | | | | | |
| 28 | Greenhouse | | **X** | | | **X** | **X** | **X** | | | | | | **X** | | **X** | | | | | **X** | | | | |
| 29 | ICU Vitals | **X** | **X** | **X** | | **X** | | **X** | | | | | | | | | | | | | | | | | |
| 30 | Pharma Batch | **X** | **X** | **X** | **X** | **X** | **X** | **X** | | | | | | **X** | | | | **X** | | | | | | | |
| 31 | Retail POS | **X** | | **X** | | **X** | | **X** | | | | | | | | | | | | | | | | | |
| 32 | Call Center | **X** | | **X** | | **X** | | **X** | | | | | | **X** | | | | | | | | | | | |
| 33 | Server Monitor | **X** | **X** | **X** | | **X** | | **X** | | | | **X** | **X** | | | | | | | | | | | | |
| 34 | API Perf Logs | **X** | | **X** | | **X** | | **X** | **X** | | | **X** | | | | | | | | | | | | | |
| 35 | Supply Chain | **X** | | **X** | | **X** | | **X** | **X** | | **X** | | | | | | | | | | | | | | |
| 36 | Late Data | | **X** | **X** | **X** | | | **X** | | | | | | | | | | | **X** | | | | | | **X** |
| 37 | Schema Evolution | **X** | | **X** | **X** | **X** | | **X** | | **X** | | | | | | | | | | | | | | | **X** |
| 38 | Multi-Source Merge | **X** | **X** | **X** | **X** | **X** | | **X** | **X** | | | | | | | | | | **X** | | | | | | |

**Legend:** range = `range`, walk = `random_walk`, cat = `categorical`, seq = `sequential`, const = `constant`, ts = `timestamp`, m.r.to = `mean_reversion_to`, incr = `incremental: stateful`, x-entity = cross-entity references, valid = validation on simulated data

---

## Find Your Pattern

!!! tip "Start here based on your role"

    **Business analyst or junior DE?** Start with Pattern 1 (Build Before Sources Exist), then work through Foundations 2-8. You'll learn every core feature progressively.

    **Chemical or process engineer?** Jump to Patterns 9-15 (Process Engineering). These use PID control, material balances, and cross-entity cascades that map directly to real plant systems.

    **Data platform engineer?** Start with Pattern 36 (Late Data), 37 (Schema Evolution), and 38 (Multi-Source Merge). These test your platform, not a domain.

    **Looking for a specific feature?** Use the heat map above to find which patterns teach it.

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
| `scheduled_events` | 2, 3, 5, 14 |
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

## See Also

- **[Getting Started](../getting_started.md)** - Your first simulation in 5 minutes
- **[Generators Reference](../generators.md)** - All generator types with parameters and examples
- **[Advanced Features](../advanced_features.md)** - Cross-entity references, scheduled events, entity overrides, chaos
- **[Process Simulation](../process_simulation.md)** - Chemical engineering and process control deep dive
