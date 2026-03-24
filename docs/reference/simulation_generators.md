# Simulation Generators Reference

Quick reference for all 13 simulation generator types.

## Generator Types

### range

**Purpose:** Numeric values with statistical distributions

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| min | float | Yes | - | Minimum value |
| max | float | Yes | - | Maximum value |
| distribution | string | No | `uniform` | `uniform` or `normal` |
| mean | float | No | (min+max)/2 | Mean for normal distribution |
| std_dev | float | No | (max-min)/6 | Std deviation for normal |

**Data types:** int, float

**Example:**
```yaml
name: temperature
data_type: float
generator:
  type: range
  min: 60.0
  max: 100.0
  distribution: normal
```

---

### random_walk

**Purpose:** Realistic time-series data where each value depends on the previous value. Ideal for simulating controlled process variables (temperatures, pressures, flow rates).

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| start | float | Yes | - | Initial value / setpoint |
| min | float | Yes | - | Hard lower bound (physical limit) |
| max | float | Yes | - | Hard upper bound (physical limit) |
| volatility | float | No | `1.0` | Std deviation of step-to-step noise |
| mean_reversion | float | No | `0.0` | Pull toward start (0=none, 1=snap back) |
| trend | float | No | `0.0` | Drift per timestep (+/- for gradual shift) |
| precision | int | No | None | Round to N decimal places |
| shock_rate | float | No | `0.0` | Probability of sudden shock per timestep (0=never, 1=every step) |
| shock_magnitude | float | No | `10.0` | Maximum absolute size of a shock event |
| shock_bias | float | No | `0.0` | Directional tendency: +1=up only, -1=down only, 0=either direction |

**Data types:** float

**How it works:** Uses an Ornstein-Uhlenbeck process with optional shock events. Each value = previous + noise + mean_reversion pull + trend. Random shocks inject sudden jumps that the mean_reversion naturally recovers from. Values are clamped to [min, max].

**Example:**
```yaml
name: reactor_temp
data_type: float
generator:
  type: random_walk
  start: 350.0
  min: 300.0
  max: 400.0
  volatility: 0.5
  mean_reversion: 0.1
  trend: 0.001
  precision: 1
  shock_rate: 0.02
  shock_magnitude: 30.0
  shock_bias: 1.0
```

**Tips:**

- Use `mean_reversion: 0.1` to simulate a PID-controlled process at steady state
- Use `trend: 0.001` to simulate slow fouling or catalyst deactivation
- Use `precision: 1` to match real instrument resolution (e.g., temperature to 0.1°F)
- Use `shock_rate: 0.02` with `shock_bias: 1.0` to simulate occasional exothermic runaways in a reactor
- Shocks perturb the walk's internal state, so `mean_reversion` naturally pulls values back — producing realistic spike-and-recover patterns
- A warning is issued if `shock_rate > 0` without `mean_reversion` — shocks without recovery aren't realistic
- Works with incremental mode — the last value per entity is saved and restored on the next run

---

### daily_profile

**Purpose:** Time-of-day patterns with day-to-day variation. Values follow a repeating daily curve defined by anchor points.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| profile | dict | Yes | - | Anchor points mapping `HH:MM` to target values |
| min | float | Yes | - | Hard lower bound |
| max | float | Yes | - | Hard upper bound |
| noise | float | No | `0.0` | Per-reading jitter (±noise) |
| volatility | float | No | `0.0` | Day-to-day variation in anchor targets (std_dev) |
| interpolation | string | No | `linear` | `linear` or `step` |
| precision | int | No | None | Round to N decimal places. `0` = integers |
| weekend_scale | float | No | None | Scale factor for weekends (0.0–1.0) |

**Data types:** int, float

**How it works:** Interpolates between anchor points to get a base value. If `volatility > 0`, each day's anchors are independently shifted by a random normal amount. Noise is added, then clamped to [min, max] and rounded.

**Example:**
```yaml
name: occupancy
data_type: int
generator:
  type: daily_profile
  min: 0
  max: 25
  precision: 0
  noise: 1.5
  volatility: 3.0
  profile:
    "00:00": 1
    "08:00": 19
    "12:00": 15
    "13:00": 22
    "17:00": 14
    "22:00": 2
```

**Tips:**

- Use `precision: 0` for headcounts, counters, or anything that must be a whole number
- Use `noise` for reading-to-reading jitter, `volatility` for day-to-day variation in the overall shape
- Use `weekend_scale: 0.15` for office buildings that are nearly empty on weekends
- Use `interpolation: step` for shift-based patterns that change abruptly (e.g., factory power at shift change)
- Pairs well with `derived` columns — e.g., CO2 derived from occupancy

---

### categorical

**Purpose:** Discrete choice from predefined values

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| values | list | Yes | - | List of possible values |
| weights | list[float] | No | uniform | Probability weights (must sum to 1.0) |

**Data types:** string, int, any

**Example:**
```yaml
name: status
data_type: string
generator:
  type: categorical
  values: [Running, Idle, Error]
  weights: [0.8, 0.15, 0.05]
```

---

### boolean

**Purpose:** True/False with configurable probability

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| true_probability | float | No | 0.5 | Probability of True (0.0-1.0) |

**Data types:** boolean

**Example:**
```yaml
name: is_active
data_type: boolean
generator:
  type: boolean
  true_probability: 0.95
```

---

### timestamp

**Purpose:** Auto-stepped timestamp column

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| _(none)_ | - | - | - | Uses scope.timestep automatically |

**Data types:** timestamp

**Format:** ISO8601 Zulu (`2026-01-01T00:00:00Z`)

**Example:**
```yaml
name: event_time
data_type: timestamp
generator:
  type: timestamp
```

---

### sequential

**Purpose:** Auto-incrementing integers

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| start | int | No | 1 | Starting value |
| step | int | No | 1 | Increment step |

**Data types:** int

**Example:**
```yaml
name: record_id
data_type: int
generator:
  type: sequential
  start: 1000
  step: 10
```

---

### constant

**Purpose:** Fixed values with template support

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| value | any | Yes | - | Constant value or template |

**Data types:** any

**Magic variables:**
- `{entity_id}` - Current entity name
- `{entity_index}` - Entity index (0-based)
- `{timestamp}` - Current row timestamp
- `{row_number}` - Row index

**Example:**
```yaml
name: source_system
data_type: string
generator:
  type: constant
  value: "simulation"

# Or with template:
name: entity_ref
data_type: string
generator:
  type: constant
  value: "{entity_id}_record_{row_number}"
```

---

### uuid

**Purpose:** Unique identifiers (UUID4 or UUID5)

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| version | int | No | 4 | 4=random, 5=deterministic |
| namespace | string | No | DNS | Namespace for UUID5 |

**Data types:** string

**Example:**
```yaml
# Random (deterministic with seed):
name: order_id
data_type: string
generator:
  type: uuid
  version: 4

# Fully deterministic:
name: device_id
data_type: string
generator:
  type: uuid
  version: 5
  namespace: "com.example.devices"
```

---

### email

**Purpose:** Email addresses

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| domain | string | No | `example.com` | Email domain |
| pattern | string | No | `{entity}_{index}` | Username pattern |

**Data types:** string

**Example:**
```yaml
name: user_email
data_type: string
generator:
  type: email
  domain: company.com
  pattern: "user_{row}"
```

**Output:** `user.5@company.com`

---

### ipv4

**Purpose:** IPv4 addresses

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| subnet | string | No | None | CIDR subnet (e.g., `192.168.0.0/24`) |

**Data types:** string

**Example:**
```yaml
# Full range:
name: client_ip
data_type: string
generator:
  type: ipv4

# Within subnet:
name: server_ip
data_type: string
generator:
  type: ipv4
  subnet: "10.0.0.0/8"
```

---

### geo

**Purpose:** Geographic coordinates (latitude/longitude)

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| bbox | list[float] | Yes | - | [min_lat, min_lon, max_lat, max_lon] |
| format | string | No | `tuple` | `tuple` or `lat_lon_separate` |

**Data types:** tuple, object

**Example:**
```yaml
name: location
data_type: string
generator:
  type: geo
  bbox: [37.0, -122.5, 37.8, -122.0]  # San Francisco Bay Area
  format: tuple
```

**Output:** `(37.4532, -122.1823)`

---

### derived

**Purpose:** Calculated columns from other columns

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| expression | string | Yes | - | Python expression |

**Data types:** any (depends on expression result)

**Supported operations:**
- Arithmetic: `+`, `-`, `*`, `/`, `**`, `//`, `%`
- Comparison: `==`, `!=`, `<`, `>`, `<=`, `>=`
- Logical: `and`, `or`, `not`
- Conditionals: `value if condition else other`

**Safe functions:**
- Math: `abs()`, `round()`, `min()`, `max()`
- Type: `int()`, `float()`, `str()`, `bool()`
- Null-safe: `coalesce()`, `safe_div()`, `safe_mul()`

**Example:**
```yaml
# Simple calculation:
name: temp_fahrenheit
data_type: float
generator:
  type: derived
  expression: "temp_celsius * 1.8 + 32"

# Conditional:
name: status
data_type: string
generator:
  type: derived
  expression: "'HOT' if temp_celsius > 80 else 'NORMAL'"

# Null-safe:
name: efficiency
data_type: float
generator:
  type: derived
  expression: "safe_div(output, input, 0)"
```

!!! warning "Security"
    Expressions run in a restricted namespace. No imports, file I/O, or system calls allowed.

---

## Compatibility Matrix

| Generator | Pandas | Spark | Polars | Incremental | Null Rate | Overrides |
|-----------|--------|-------|--------|-------------|-----------|-----------|
| range | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| random_walk | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| daily_profile | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| categorical | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| boolean | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| timestamp | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| sequential | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| constant | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| uuid | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| email | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| ipv4 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| geo | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| derived | ✅ | ✅ | ✅ | ✅ | ⚠️ * | ✅ |

**Notes:**
- \* Derived columns: null_rate applied after calculation. Use null-safe functions (coalesce, safe_div) if dependencies have nulls.

## See Also

- **[Complete Guide](../guides/simulation.md)** - Detailed documentation
- **[State Management](../features/state.md)** - HWM persistence for incremental mode
- **[Configuration](configuration.md)** - General config concepts
