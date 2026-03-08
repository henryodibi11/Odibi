# Simulation

## Overview

Generate synthetic data directly in your pipelines without external dependencies.

!!! tip "When to Use"
    - Building pipelines before source data exists
    - Testing with reproducible datasets
    - Creating demos without exposing real data
    - Stress testing Delta Lake at scale

## Quick Example

```yaml title="Generate 300 rows of sensor telemetry"
nodes:
  - name: sensor_data
    read:
      connection: null
      format: simulation
      options:
        simulation:
          scope:
            start_time: "2026-01-01T00:00:00Z"
            timestep: "5m"
            row_count: 100
            seed: 42
          entities:
            count: 3
            id_prefix: "sensor_"
          columns:
            - name: sensor_id
              data_type: string
              generator: {type: constant, value: "{entity_id}"}
            - name: timestamp
              data_type: timestamp
              generator: {type: timestamp}
            - name: temperature
              data_type: float
              generator: {type: range, min: 20, max: 30}
```

**Output:** 3 sensors × 100 rows = 300 rows of time-series data

## Key Features

### 🎲 11 Generator Types
**Basic:** range, categorical, boolean, timestamp, sequential, constant  
**Advanced:** derived, uuid, email, ipv4, geo

### ⚡ Incremental Mode
Generate continuous data streams with HWM-based state tracking.

### 🔧 Chaos Engineering
Add realistic imperfections: outliers, duplicates, null values, downtime events.

### 🚀 Multi-Engine
Same YAML works on Pandas, Spark, and Polars.

### 🔒 Safe Expressions
Derived columns use sandboxed evaluation—no code injection.

### 📊 Deterministic
Same seed produces identical data every time.

## Capabilities Matrix

| Generator | Use Case | Example |
|-----------|----------|---------|
| range | Metrics, measurements | Temperature: 60-100°C |
| categorical | Status, enums | [Running, Idle, Error] |
| boolean | Flags | is_active: 95% true |
| timestamp | Event times | Auto 5-minute intervals |
| sequential | Auto-increment IDs | 1, 2, 3, ... |
| constant | Fixed values | source: "simulation" |
| uuid | Unique keys | a4f2e8d1-... |
| email | Contact info | user@example.com |
| ipv4 | IP addresses | 192.168.1.42 |
| geo | Coordinates | (37.7749, -122.4194) |
| derived | Calculated fields | temp_f = temp_c * 1.8 + 32 |

## Learn More

- **[Detailed Guide](../guides/simulation.md)** - Complete documentation with all generators
- **[Generators Reference](../reference/simulation_generators.md)** - Quick parameter lookup
- **[Example YAML](https://github.com/henryodibi11/Odibi/blob/main/examples/simulation_example.yaml)** - Working example

## Common Patterns

### Development Before Sources Exist

```yaml
# Bronze: Simulate upstream source
- name: bronze_orders
  read:
    format: simulation
    options: {...}

# Silver: Process as if real
- name: silver_orders
  depends_on: [bronze_orders]
  transform: [...]
```

Later swap bronze to real source—silver unchanged.

### Continuous Test Data

```yaml
read:
  format: simulation
  options: {...}
  incremental:
    mode: stateful
    column: timestamp
```

Each run generates new data after previous run's max timestamp.

### Stress Testing

```yaml
entities:
  count: 1000  # 1000 entities
scope:
  row_count: 10000  # 10K rows each = 10M total
```

Test Delta write performance, partitioning, compaction strategies.

## Next Steps

1. Read the [complete guide](../guides/simulation.md)
2. Try the [example YAML](https://github.com/henryodibi11/Odibi/blob/main/examples/simulation_example.yaml)
3. Check [generators reference](../reference/simulation_generators.md) for all options
