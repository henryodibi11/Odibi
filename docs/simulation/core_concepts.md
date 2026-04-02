# Core Concepts: Scope, Entities, and Columns

Every simulation is built from three concepts. Understand these and you can generate any dataset.

---

## How Simulation Works

```
┌─────────┐     ┌──────────┐     ┌─────────┐
│  Scope  │  →  │ Entities │  →  │ Columns │
│ (when)  │     │  (who)   │     │ (what)  │
└─────────┘     └──────────┘     └─────────┘
```

1. **Scope** — *When* and *how much* data: start time, interval, row count
2. **Entities** — *Who* generates data: sensors, users, machines, production lines
3. **Columns** — *What* data each entity produces: temperatures, IDs, statuses

Each entity gets its own independent copy of `row_count` rows. So 5 entities × 100 rows = **500 total rows**.

The simulation engine evaluates these in order: scope sets the time axis, entities multiply the dataset, and columns fill it with values.

---

## Scope: When and How Much

Scope defines the time boundaries and size of your simulation. It answers: *"When does the data start, how often are readings taken, and how many rows do I get?"*

```yaml
scope:
  start_time: "2026-01-01T00:00:00Z"   # When data starts
  timestep: "5m"                        # Interval between rows
  row_count: 288                        # Rows per entity
  seed: 42                              # Makes output reproducible
```

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `start_time` | string | Yes | — | ISO8601 timestamp in Zulu format (e.g., `"2026-01-01T00:00:00Z"`) |
| `timestep` | string | Yes | — | Time between rows. Format: number + unit where unit is `s` (seconds), `m` (minutes), `h` (hours), or `d` (days). E.g., `5s`, `10m`, `1h`, `2d` |
| `row_count` | int | Either this or `end_time` | — | Exact number of rows to generate per entity |
| `end_time` | string | Either this or `row_count` | — | ISO8601 end timestamp in Zulu format (e.g., `"2026-01-02T00:00:00Z"`); Odibi calculates row count automatically |
| `seed` | int | No | `42` | Random seed for deterministic output |

### start_time

The timestamp of the first row. Use ISO8601 format with a `Z` suffix (UTC):

```yaml
start_time: "2026-01-01T00:00:00Z"
```

Every subsequent row's timestamp increments by `timestep` from this starting point.

### timestep

The time interval between consecutive rows. Supported formats:

| Format | Meaning | Example |
|--------|---------|---------|
| `Ns` | N seconds | `5s` = 5 seconds |
| `Nm` | N minutes | `10m` = 10 minutes |
| `Nh` | N hours | `1h` = 1 hour |
| `Nd` | N days | `2d` = 2 days |

### row_count vs end_time

You must specify **exactly one** of these — they are mutually exclusive. Odibi validates this at configuration load time and raises an error if you provide both or neither.

**Option A: Fixed count** — You know how many rows you want:

```yaml
scope:
  start_time: "2026-01-01T00:00:00Z"
  timestep: "5m"
  row_count: 288        # Exactly 288 rows per entity
```

**Option B: Time range** — You know the time period you want to cover:

```yaml
scope:
  start_time: "2026-01-01T00:00:00Z"
  timestep: "5m"
  end_time: "2026-01-02T00:00:00Z"   # Generate until this time
```

Odibi calculates the row count by dividing the time range by the timestep. In this example: 24 hours ÷ 5 minutes = 288 rows.

!!! tip "Quick math"
    `row_count × timestep = duration`. So 288 rows × 5 minutes = 1,440 minutes = **1 day**.

### seed

The random seed controls all randomness in the simulation. Same seed = same output, every time, on every machine.

```yaml
seed: 42       # Default
seed: 12345    # Different seed = different data
```

**Why it matters:**

- **Reproducibility** — teammates get identical data from the same config
- **Debugging** — regenerate the exact dataset that caused an issue
- **Testing** — assertions can rely on specific values
- **CI/CD** — pipeline tests produce deterministic results

---

## Entities: Who Generates Data

Entities are the "things" producing data — sensors, users, machines, production lines, pumps, reactors, or anything else in your domain. Each entity generates its own independent stream of data.

### Auto-generated entities

Use `count` and `id_prefix` to auto-generate entity names:

```yaml
entities:
  count: 10
  id_prefix: "sensor_"
```

This produces: `sensor_01`, `sensor_02`, ... `sensor_10`.

### Explicit entity names

Use `names` when you need specific, meaningful identifiers:

```yaml
entities:
  names: [pump_01, pump_02, reactor_01, compressor_A]
```

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `count` | int | Either this or `names` | — | Number of entities to auto-generate |
| `names` | list[str] | Either this or `count` | — | Explicit list of entity names |
| `id_prefix` | string | No | `"entity_"` | Prefix for auto-generated IDs (used with `count`) |
| `id_format` | string | No | `"sequential"` | `"sequential"` (e.g., `sensor_01`) or `"uuid"` (e.g., `sensor_a1b2c3d4`) |

### Validation rules

- You must specify **exactly one** of `count` or `names` — not both, not neither
- `names` list cannot be empty
- `count` must be greater than 0

### How entities multiply your data

Each entity generates its own independent copy of `row_count` rows. The total output is:

```
total_rows = number_of_entities × row_count
```

| Entities | Rows per entity | Total rows |
|----------|-----------------|------------|
| 3 sensors | 288 | 864 |
| 10 pumps | 1,000 | 10,000 |
| 50 machines | 10,000 | 500,000 |

Each entity's data is generated independently with its own random state (derived from the global seed), so entity `sensor_01` always produces the same values regardless of how many other entities exist.

### id_format

Controls how auto-generated IDs are formatted:

- **`"sequential"`** (default) — Zero-padded numbers: `sensor_01`, `sensor_02`, ...
- **`"uuid"`** — UUID-based suffixes: `sensor_a1b2c3d4`, `sensor_e5f6g7h8`, ...

UUID format is useful when you need globally unique identifiers or want to simulate distributed systems where IDs aren't sequential.

---

## Columns: What Gets Generated

Columns define the actual data produced by each entity. Every column has a name, a data type, and a generator that determines how values are created.

### The ColumnGeneratorConfig structure

```yaml
columns:
  - name: temperature           # Column name in the output DataFrame
    data_type: float            # Python/Spark data type
    generator:                  # How values are produced
      type: range
      min: 60.0
      max: 100.0
    null_rate: 0.02             # 2% of values will be NULL
    entity_overrides:           # Per-entity customization
      heavy_duty_pump:
        type: range
        min: 80.0
        max: 120.0
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | — | Column name in the output DataFrame |
| `data_type` | string | Yes | — | One of: `string`, `int`, `float`, `boolean`, `timestamp`, `categorical` |
| `generator` | object | Yes | — | Generator config with `type` and type-specific parameters |
| `null_rate` | float | No | `0.0` | Probability of NULL per value (0.0–1.0) |
| `entity_overrides` | dict | No | `{}` | Map of entity name → alternative generator config |

### Data types

| Type | Python Type | Example Value | Use Case |
|------|-------------|---------------|----------|
| `string` | `str` | `"sensor_01"` | IDs, names, labels, categories |
| `int` | `int` | `42` | Counts, sequence numbers, discrete values |
| `float` | `float` | `98.6` | Measurements, temperatures, percentages |
| `boolean` | `bool` | `True` | Flags, on/off states, binary conditions |
| `timestamp` | `datetime` | `2026-01-01T00:00:00Z` | Event times, row timestamps |
| `categorical` | `str` | `"Running"` | Status codes, categories (explicit enum) |

### null_rate: Simulating missing data

Real-world data has gaps. Use `null_rate` to inject NULLs randomly:

```yaml
- name: optional_reading
  data_type: float
  generator: {type: range, min: 0, max: 100}
  null_rate: 0.1    # 10% of values will be NULL
```

- `0.0` (default) — no NULLs
- `0.05` — 5% missing (occasional sensor dropout)
- `0.1` — 10% missing (unreliable sensor)
- `0.5` — 50% missing (intermittent connectivity)

### entity_overrides: Per-entity customization

Override the generator for specific entities. The base generator applies to all entities not listed in overrides.

```yaml
- name: pressure
  data_type: float
  generator:
    type: range
    min: 50
    max: 100
  entity_overrides:
    heavy_duty_pump:          # This entity gets a higher range
      type: range
      min: 100
      max: 200
    old_pump:                 # This entity gets more variance
      type: range
      min: 30
      max: 150
```

In this example:
- `heavy_duty_pump` generates pressure between 100–200
- `old_pump` generates pressure between 30–150
- All other entities use the default 50–100 range

### Complete column example

Here's a column using every available option:

```yaml
- name: vibration
  data_type: float
  generator:
    type: range
    min: 0.1
    max: 5.0
    distribution: normal
    mean: 1.2
    std_dev: 0.8
  null_rate: 0.03
  entity_overrides:
    failing_motor:
      type: range
      min: 5.0
      max: 25.0
    new_motor:
      type: range
      min: 0.05
      max: 0.5
```

This generates:
- Most entities: normal distribution centered at 1.2 mm/s, with 3% NULLs
- `failing_motor`: elevated vibration (5–25 mm/s) indicating wear
- `new_motor`: very low vibration (0.05–0.5 mm/s) indicating new equipment

---

## Column Dependency Resolution

Derived columns can reference other columns by name in their expressions. Odibi automatically resolves the correct evaluation order using **topological sort** — you don't need to worry about listing columns in the right order.

### How it works

1. Odibi scans all `derived` column expressions for column name references
2. It builds a dependency graph (column A depends on column B)
3. It performs a topological sort to determine safe evaluation order
4. Columns are evaluated in dependency order, regardless of their position in the YAML

### Example: Temperature conversion

```yaml
columns:
  # This column is listed SECOND but evaluated FIRST
  - name: temp_celsius
    data_type: float
    generator:
      type: range
      min: 20.0
      max: 35.0

  # This column references temp_celsius — Odibi evaluates it after
  - name: temp_fahrenheit
    data_type: float
    generator:
      type: derived
      expression: "temp_celsius * 1.8 + 32"
```

Even if you reversed the order in YAML (put `temp_fahrenheit` first), Odibi would still evaluate `temp_celsius` first because it detects the dependency.

### Chained dependencies

Dependencies can chain through multiple levels:

```yaml
columns:
  - name: raw_output
    data_type: float
    generator: {type: range, min: 100, max: 500}

  - name: raw_input
    data_type: float
    generator: {type: range, min: 80, max: 400}

  - name: efficiency
    data_type: float
    generator:
      type: derived
      expression: "(raw_output / raw_input * 100) if raw_input > 0 else 0"

  - name: efficiency_grade
    data_type: string
    generator:
      type: derived
      expression: "'A' if efficiency > 90 else 'B' if efficiency > 70 else 'C'"
```

Evaluation order: `raw_output` → `raw_input` → `efficiency` → `efficiency_grade`

!!! warning "Circular dependencies"
    If column A depends on B and B depends on A, Odibi raises an error at config validation time. Circular dependencies are detected before any data is generated.

---

## The YAML Structure

Here's how the three building blocks fit into a complete pipeline node:

```yaml
read:
  connection: null               # No connection needed for simulation
  format: simulation
  options:
    simulation:
      scope:                     # WHEN: time boundaries
        start_time: "2026-01-01T00:00:00Z"
        timestep: "5m"
        row_count: 288
        seed: 42
      entities:                  # WHO: data producers
        count: 3
        id_prefix: "sensor_"
      columns:                   # WHAT: data columns
        - name: sensor_id
          data_type: string
          generator: {type: constant, value: "{entity_id}"}
        - name: timestamp
          data_type: timestamp
          generator: {type: timestamp}
        - name: temperature
          data_type: float
          generator: {type: range, min: 20, max: 35}
      chaos:                     # Optional: inject noise
        outlier_rate: 0.01
        outlier_factor: 3.0
      scheduled_events: []       # Optional: time/condition-based behavior changes
```

The `scope`, `entities`, and `columns` keys are required. `chaos` and `scheduled_events` are optional and covered in [Advanced Features](advanced_features.md).

---

## What's Next

Now that you understand the three building blocks, dive deeper:

- **[Generators](generators.md)** — All 13 generator types in detail (range, random_walk, daily_profile, categorical, derived, and more)
- **[Stateful Functions](stateful_functions.md)** — Random walks, incremental mode, and state persistence across runs
- **[Advanced Features](advanced_features.md)** — Chaos engineering, scheduled events, downtime periods, and entity overrides at scale
