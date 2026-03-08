# Synthetic Data Generation (Simulation)

## Overview

Generate realistic synthetic data directly in your Odibi pipelines using declarative YAML configuration.

**Use cases:**

- Build pipelines before source data exists
- Create reproducible test datasets
- Generate realistic demos without exposing customer data
- Stress test Delta Lake at scale
- Provide safe training environments

## Quick Start

```yaml title="Generate 300 rows of device telemetry"
nodes:
  - name: demo_data
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
```

**Output:** 300 rows (3 devices × 100 rows each) with 5-minute intervals

**Run it:**
```bash
odibi run my_pipeline.yaml
```

## Core Concepts

### 1. Simulation Scope

Defines when and how much data to generate:

```yaml
scope:
  start_time: "2026-01-01T00:00:00Z"  # ISO8601 timestamp
  timestep: "5m"                       # 5s, 10m, 1h, 2d supported
  row_count: 1000                      # Generate this many rows per entity
  # OR
  end_time: "2026-01-02T00:00:00Z"    # Generate until this time
  seed: 42                             # Random seed for determinism
```

!!! note "Scope Constraint"
    Choose **exactly one** of `row_count` or `end_time`, not both.

!!! tip "Determinism"
    Set a fixed `seed` value to make simulations reproducible. Same seed + same config = identical data every time.

**Timestep format**: `<number><unit>` where unit is `s` (seconds), `m` (minutes), `h` (hours), or `d` (days).

### 2. Entities

Entities represent the "things" generating data (sensors, users, machines, etc.):

```yaml
# Auto-generate entity names
entities:
  count: 100
  id_prefix: "sensor_"      # → sensor_01, sensor_02, ..., sensor_100

# OR explicit names
entities:
  names: [pump_01, pump_02, reactor_01]
```

Each entity generates `row_count` (or time-based count) rows.

### 3. Column Generators

Each column has a generator that defines how values are created:

#### Range Generator (Numeric)

```yaml
- name: temperature
  data_type: float
  generator:
    type: range
    min: 60.0
    max: 100.0
    distribution: uniform    # or: normal
    # For normal distribution:
    mean: 80.0              # defaults to midpoint
    std_dev: 10.0           # defaults to (max-min)/6
```

#### Categorical Generator

```yaml
- name: status
  data_type: categorical
  generator:
    type: categorical
    values: [Running, Idle, Error]
    weights: [0.8, 0.15, 0.05]  # optional, defaults to uniform
```

#### Boolean Generator

```yaml
- name: is_active
  data_type: boolean
  generator:
    type: boolean
    true_probability: 0.95    # P(True) = 95%
```

#### Timestamp Generator

```yaml
- name: event_time
  data_type: timestamp
  generator:
    type: timestamp           # Uses scope.timestep automatically
```

#### Sequential Generator

```yaml
- name: record_id
  data_type: int
  generator:
    type: sequential
    start: 1
    step: 1                   # → 1, 2, 3, 4, ...
```

#### Constant Generator

```yaml
- name: source
  data_type: string
  generator:
    type: constant
    value: "simulation"
```

**Magic Variables** in constant values:
- `{entity_id}`: Current entity name
- `{entity_index}`: Entity index (0-based)
- `{timestamp}`: Current row timestamp
- `{row_number}`: Row index

#### Derived Generator (v3)

Calculate columns from other columns using Python expressions:

```yaml
- name: temperature_fahrenheit
  data_type: float
  generator:
    type: derived
    expression: "temperature_celsius * 1.8 + 32"
```

**Supported operations:**
- Arithmetic: `+`, `-`, `*`, `/`, `//`, `%`, `**`
- Comparison: `==`, `!=`, `<`, `<=`, `>`, `>=`
- Logical: `and`, `or`, `not`
- Conditionals: `value if condition else other_value`
- Functions: `abs()`, `round()`, `min()`, `max()`, `int()`, `float()`, `str()`, `bool()`

**Examples:**

```yaml
# Conditional logic
- name: status
  data_type: string
  generator:
    type: derived
    expression: "'HOT' if temperature > 80 else 'NORMAL'"

# Multiple dependencies
- name: efficiency
  data_type: float
  generator:
    type: derived
    expression: "(output / input * 100) if input > 0 else 0"

# Chained derivations (b depends on a, c depends on b)
- name: a
  data_type: int
  generator: {type: sequential, start: 1}

- name: b
  data_type: int
  generator: {type: derived, expression: "a * 10"}

- name: c
  data_type: int
  generator: {type: derived, expression: "b + 5"}
```

**Dependency Resolution:**
- Automatic topological sort
- Circular dependencies detected and rejected
- Columns generated in correct order

!!! warning "Expression Safety"
    Derived expressions run in a **restricted namespace**:

    **Allowed:** Math/logic operators, safe functions (abs, round, safe_div, coalesce)  
    **Blocked:** Imports, file I/O, network, system calls, exec, eval

    **Example safe:**
    ```python
    "temperature * 1.8 + 32"
    "safe_div(output, input, 0)"
    "'HOT' if temp > 80 else 'NORMAL'"
    ```

    **Example unsafe (rejected):**
    ```python
    "import os; os.system('rm -rf /')"  # Blocked
    "open('/etc/passwd').read()"        # Blocked
    ```

### 4. Null Rate

Add realistic missing data:

```yaml
- name: optional_field
  data_type: float
  generator: {type: range, min: 0, max: 100}
  null_rate: 0.1              # 10% of values will be NULL
```

### 5. Entity Overrides

Override generator for specific entities:

```yaml
- name: pressure
  data_type: float
  generator:
    type: range
    min: 50
    max: 100
  entity_overrides:
    pump_heavy_duty:          # This entity uses different range
      type: range
      min: 100
      max: 200
```

### 6. Chaos Parameters

Add realistic imperfections:

```yaml
chaos:
  outlier_rate: 0.01          # 1% of numeric values become outliers
  outlier_factor: 3.0         # Outliers are 3x normal value
  duplicate_rate: 0.005       # 0.5% of rows are duplicated
  downtime_events:            # Specific periods with no data
    - entity: sensor_05       # If null, affects all entities
      start_time: "2026-01-01T12:00:00Z"
      end_time: "2026-01-01T14:00:00Z"
```

## Complete Example

See [simulation_example.yaml on GitHub](https://github.com/henryodibi11/Odibi/blob/main/examples/simulation_example.yaml) for a full working example with:
- 10 sensors generating 24 hours of telemetry
- Multiple generator types
- Entity overrides
- Chaos parameters
- Downstream transformations and validation

## Determinism

Simulations are **fully deterministic** when using the same seed:

```yaml
scope:
  seed: 42
```

Same seed → identical data on every run. This enables:
- Reproducible tests
- Consistent demos
- Debugging with fixed datasets

## Integration with Odibi Pipelines

Simulated sources work exactly like real sources:

```yaml
- name: simulated_bronze
  read:
    format: simulation
    options: {...}
  transform:
    - operation: derive_columns
      params: {...}
  validate:
    tests:
      - type: not_null
        columns: [id, timestamp]
  write:
    connection: lake
    table: bronze.sim_data
    mode: overwrite
```

Downstream nodes cannot distinguish simulation from real data.

## Performance Considerations

- **Memory**: All rows generated in-memory. For large datasets (>1M rows), use Spark engine
- **Speed**: Generation is fast (~100K rows/second typical)
- **Chaos**: Outliers and duplicates applied after generation, adds minimal overhead

## Limitations

**Not supported yet:**
- Correlation between columns (planned for v4)
- Time-series patterns (seasonality, trends, planned for v4)
- Custom function libraries (planned for v4)

**Engine Support:**
- ✅ Pandas: Fully supported (v1)
- ✅ Spark: Fully supported (v2, delegates to Pandas)
- ✅ Polars: Fully supported (v2, delegates to Pandas)
- ✅ Incremental mode: Fully supported (v2)
- ✅ Derived columns: Fully supported (v3)

## Use Cases

### 1. Develop Without Source Data

```yaml
# Bronze node simulates upstream source
- name: bronze_orders
  read: {format: simulation, options: {...}}

# Silver node processes as if real data
- name: silver_orders
  depends_on: [bronze_orders]
  inputs: {source: bronze_orders}
  transform: [...]
```

Later, swap bronze node to real source—silver logic unchanged.

### 2. Generate Test Fixtures

```python
# Generate 1000 test records
scope: {row_count: 1000, seed: 42}

# Run tests
pytest tests/test_transform.py
```

### 3. Demo Analytics

Generate realistic data for dashboards without exposing real data.

### 4. Stress Test Pipelines

```yaml
# Generate 10M rows
scope:
  row_count: 10000
entities:
  count: 1000  # 1000 entities × 10K rows = 10M
```

Test Delta write performance, partition strategies, etc.

## Incremental Simulation (v2)

Simulation sources support **stateful incremental mode** for continuous data generation.

### How It Works

With incremental mode enabled, the simulator:
1. Tracks the maximum timestamp from each run in StateManager
2. On subsequent runs, starts generating data after the last timestamp
3. Persists the new maximum timestamp for the next run

This creates a **continuously evolving data source** that behaves like real streaming data.

### Configuration

```yaml
read:
  connection: null
  format: simulation
  options:
    simulation:
      scope:
        start_time: "2026-01-01T00:00:00Z"
        timestep: "5m"
        row_count: 288  # 24 hours of data
        seed: 42
      # ... entities and columns ...

  incremental:
    mode: stateful
    column: timestamp        # Must match a timestamp column
    state_key: my_sim_hwm    # Optional: defaults to <node_name>_hwm
```

### Example: Continuous Telemetry

```yaml
name: continuous_iot
engine: pandas

connections:
  local:
    type: local
    base_path: ./data

system:
  connection: local
  path: .odibi/catalog  # Stores HWM state

pipelines:
  - name: sensor_ingestion
    nodes:
      - name: sensor_data
        read:
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-01-01T00:00:00Z"
                timestep: "1h"
                row_count: 24  # 24 hours per run
                seed: 42
              entities:
                count: 10
                id_prefix: sensor_
              columns:
                - name: timestamp
                  data_type: timestamp
                  generator: {type: timestamp}
                - name: value
                  data_type: float
                  generator: {type: range, min: 0, max: 100}
          incremental:
            mode: stateful
            column: timestamp

        write:
          connection: local
          table: sensor_stream.parquet
          mode: append
```

**Run 1:**
- Generates data from 2026-01-01 00:00 → 23:00 (24 rows per sensor)
- Stores HWM: 2026-01-01T23:00:00Z

**Run 2:**
- Starts from 2026-01-02 00:00 (after HWM)
- Generates data from 00:00 → 23:00 (next 24 rows)
- Updates HWM: 2026-01-02T23:00:00Z

**Run 3:**
- Continues from 2026-01-03 00:00...

### State Storage

HWM state is stored in the System Catalog configured in `project.yaml`:

```yaml
system:
  connection: <connection_name>
  path: <catalog_path>
```

Supported backends:
- **LocalJSONStateBackend**: `.odibi/state.json` (development)
- **DeltaStateBackend**: Delta table in lake (production)
- **AzureSQLStateBackend**: Azure SQL table (enterprise)

### Determinism

Incremental simulation remains **deterministic**:
- Same seed + same HWM → identical data
- Each run advances the RNG state based on HWM
- Reproducible across environments

### Use Cases

1. **Long-running test datasets**: Simulate weeks/months of data incrementally
2. **Continuous integration tests**: Generate fresh data on each CI run
3. **Demo environments**: Auto-refresh dashboards with new simulated data
4. **Performance testing**: Gradually increase dataset size to test scalability

## Configuration Reference

See the [Config API reference](../reference/api/config.md) or [config.py on GitHub](https://github.com/henryodibi11/Odibi/blob/main/odibi/config.py) for complete Pydantic models:

- `SimulationConfig`: Top-level config
- `SimulationScope`: Time/count boundaries
- `EntityConfig`: Entity generation
- `ColumnGeneratorConfig`: Column definition
- `ChaosConfig`: Chaos parameters

All configs are validated at parse time with clear error messages.

## Troubleshooting

### Error: "Specify either row_count or end_time"

Provide exactly one:

```yaml
scope:
  row_count: 100  # ✅
  # end_time: ...  # ❌ Don't specify both
```

### Error: "Weights must sum to 1.0"

Categorical weights must be probabilities:

```yaml
generator:
  type: categorical
  values: [A, B, C]
  weights: [0.5, 0.3, 0.2]  # ✅ Sum = 1.0
```

### Error: "Column override references undefined entity"

Entity overrides must reference entities that exist:

```yaml
entities:
  names: [pump_01, pump_02]  # ✅ Defined

entity_overrides:
  pump_03: {...}  # ❌ Not defined
```

### No data generated

Check downtime events—entities may be offline during entire simulation window.

## Future Enhancements (Roadmap)

- **v2.0**: Incremental mode with HWM persistence
- **v2.1**: Derived columns with dependencies
- **v2.2**: Correlation modeling between columns
- **v2.3**: Time-series patterns (seasonality, trends)
- **v3.0**: Spark and Polars engine support
- **v3.1**: Simulation presets (IoT, retail, manufacturing)
- **v3.2**: Event-driven simulation (Poisson processes)

## Contributing

See [CONTRIBUTING.md on GitHub](https://github.com/henryodibi11/Odibi/blob/main/CONTRIBUTING.md) for how to extend the simulation feature.

## License

Part of the Odibi framework. See [LICENSE on GitHub](https://github.com/henryodibi11/Odibi/blob/main/LICENSE).
