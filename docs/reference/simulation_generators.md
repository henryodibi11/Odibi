# Simulation Generators Reference

Quick reference for all 11 simulation generator types.

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
