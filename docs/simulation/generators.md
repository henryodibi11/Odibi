# Simulation Generators Reference

Comprehensive reference for all 12 simulation generator types. Each generator produces
a specific kind of synthetic data for realistic dataset simulation.

---

## Generator Quick Reference

| Generator | Use Case | Data Types |
|-----------|----------|------------|
| [range](#range) | Metrics, measurements, scores | int, float |
| [random_walk](#random_walk) | Process variables, stock prices, sensor drift | float |
| [categorical](#categorical) | Status codes, categories, enums | string, int |
| [boolean](#boolean) | Flags, binary states | boolean |
| [timestamp](#timestamp) | Event times, auto-stepped | timestamp |
| [sequential](#sequential) | Auto-increment IDs, counters | int |
| [constant](#constant) | Fixed values, metadata, templates | any |
| [derived](#derived) | Calculated fields, physics, business logic | any |
| [uuid](#uuid) | Unique identifiers | string |
| [email](#email) | Contact info | string |
| [ipv4](#ipv4) | IP addresses | string |
| [geo](#geo) | Geographic coordinates | string |

---

## range

Generate numeric values with statistical distributions.

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| min | float | Yes | — | Minimum value |
| max | float | Yes | — | Maximum value |
| distribution | string | No | `uniform` | `uniform` or `normal` |
| mean | float | No | (min+max)/2 | Mean for normal distribution |
| std_dev | float | No | (max-min)/6 | Standard deviation for normal distribution |

**Supported data types:** `int`, `float`

### Examples

**Manufacturing — quality score:**

```yaml
name: quality_score
data_type: float
generator:
  type: range
  min: 85.0
  max: 100.0
  distribution: normal
  mean: 96.0
  std_dev: 2.5
```

**IoT — battery percentage:**

```yaml
name: battery_pct
data_type: int
generator:
  type: range
  min: 0
  max: 100
```

!!! tip
    Use `distribution: normal` with a tight `std_dev` for measurements that cluster around a target (e.g., fill weight, thickness). Use `uniform` for values that are equally likely across a range (e.g., random wait times).

---

## random_walk

Generate realistic time-series data where each value depends on the previous value. Uses an Ornstein-Uhlenbeck process with optional shocks. Ideal for simulating controlled process variables, financial data, and drifting sensor readings.

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| start | float | Yes | — | Initial value / static setpoint |
| min | float | Yes | — | Hard lower bound (physical limit) |
| max | float | Yes | — | Hard upper bound (physical limit) |
| volatility | float | No | `1.0` | Std deviation of step-to-step noise. Controls noise magnitude. Must be > 0. |
| mean_reversion | float | No | `0.0` | Pull strength toward setpoint (0 = pure random walk, 1 = snap back immediately). Simulates PID-like control. Range: 0.0–1.0. |
| mean_reversion_to | string | No | `None` | **Dynamic setpoint.** Column name to use as the reversion target instead of the static `start` value. See [Dynamic Setpoint Tracking](#dynamic-setpoint-tracking) below. |
| trend | float | No | `0.0` | Drift per timestep. Positive = gradual increase, negative = decrease. Simulates fouling, degradation, or slow process drift. |
| precision | int | No | `None` | Round values to N decimal places. None = no rounding. Range: 0–10. |
| shock_rate | float | No | `0.0` | Probability of a sudden shock per timestep (0.0 = never, 1.0 = every step). Range: 0.0–1.0. |
| shock_magnitude | float | No | `10.0` | Maximum absolute size of a shock event. The actual shock is drawn uniformly from `[0, shock_magnitude]`. Must be > 0. |
| shock_bias | float | No | `0.0` | Directional tendency for shocks. +1.0 = always up, -1.0 = always down, 0.0 = either direction. Range: -1.0 to 1.0. |

**Supported data types:** `float`

**How it works:** Each value = previous + noise + mean_reversion pull + trend. Values are clamped to [min, max]. Shocks perturb the internal state, so `mean_reversion` naturally pulls values back — producing realistic spike-and-recover patterns.

### Examples

**Manufacturing — reactor temperature with occasional upsets:**

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

**Business — daily stock price:**

```yaml
name: stock_price
data_type: float
generator:
  type: random_walk
  start: 150.0
  min: 50.0
  max: 500.0
  volatility: 2.5
  trend: 0.01
  precision: 2
```

### Dynamic Setpoint Tracking

The `mean_reversion_to` parameter enables a walk to track a **time-varying reference column** instead of reverting to the static `start` value. This is essential for simulating real-world dependencies where one signal follows another.

**How it works:** At each timestep, the reversion target is read from the referenced column's current row value. The walk drifts toward that dynamic target with the configured `mean_reversion` strength. If the referenced column is not yet available (dependency ordering issue), it falls back to `start`.

!!! warning "Dependency Order"
    The referenced column **must** be defined earlier in the column list. Odibi evaluates columns in order — the target column must already have a value for the current row.

**IoT — battery temperature tracking ambient:**

```yaml
columns:
  - name: ambient_temp_c
    data_type: float
    generator:
      type: random_walk
      start: 25.0
      min: 15.0
      max: 35.0
      volatility: 0.3
      mean_reversion: 0.05

  - name: battery_temp_c
    data_type: float
    generator:
      type: random_walk
      start: 28.0
      min: 20.0
      max: 45.0
      volatility: 0.4
      mean_reversion: 0.1
      mean_reversion_to: ambient_temp_c  # Tracks ambient, not static 28.0
```

**Manufacturing — process variable following changing setpoint:**

```yaml
columns:
  - name: temp_setpoint_c
    data_type: float
    generator:
      type: random_walk
      start: 80.0
      min: 60.0
      max: 100.0
      volatility: 0.1
      mean_reversion: 0.02

  - name: actual_temp_c
    data_type: float
    generator:
      type: random_walk
      start: 80.0
      min: 55.0
      max: 105.0
      volatility: 0.5
      mean_reversion: 0.15
      mean_reversion_to: temp_setpoint_c  # PV tracks SP
      precision: 1
```

### Tips

- Use `mean_reversion: 0.1` to simulate a PID-controlled process at steady state.
- Use `trend: 0.001` to simulate slow fouling or catalyst deactivation.
- Use `precision: 1` to match real instrument resolution (e.g., temperature to 0.1°F).
- Use `shock_rate: 0.02` with `shock_bias: 1.0` to simulate occasional exothermic runaways.
- A warning is issued if `shock_rate > 0` without `mean_reversion` — shocks without recovery aren't realistic.
- Works with **incremental mode** — the last value per entity is saved and restored on the next run.

---

## categorical

Generate discrete values chosen from a predefined list.

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| values | list | Yes | — | List of possible values |
| weights | list[float] | No | uniform | Probability weights (must sum to 1.0) |

**Supported data types:** `string`, `int`, any

### Examples

**Manufacturing — machine status:**

```yaml
name: machine_status
data_type: string
generator:
  type: categorical
  values: [Running, Idle, Maintenance, Error]
  weights: [0.75, 0.12, 0.08, 0.05]
```

**Business — customer tier:**

```yaml
name: customer_tier
data_type: string
generator:
  type: categorical
  values: [Bronze, Silver, Gold, Platinum]
  weights: [0.50, 0.30, 0.15, 0.05]
```

---

## boolean

Generate True/False values with configurable probability.

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| true_probability | float | No | `0.5` | Probability of True (0.0–1.0) |

**Supported data types:** `boolean`

### Examples

**IoT — sensor online flag:**

```yaml
name: is_online
data_type: boolean
generator:
  type: boolean
  true_probability: 0.98
```

**Business — email opted-in:**

```yaml
name: opted_in
data_type: boolean
generator:
  type: boolean
  true_probability: 0.65
```

---

## timestamp

Generate auto-stepped timestamp values based on the simulation scope's timestep configuration.

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| _(none)_ | — | — | — | Uses `scope.timestep` automatically |

**Supported data types:** `timestamp`

**Format:** ISO 8601 Zulu — `2026-01-01T00:00:00Z`

### Example

```yaml
name: event_time
data_type: timestamp
generator:
  type: timestamp
```

!!! tip
    The timestamp column advances automatically based on the `timestep` in your simulation scope (e.g., `1m`, `5m`, `1h`). You only need one timestamp column per entity.

---

## sequential

Generate auto-incrementing integer values.

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| start | int | No | `1` | Starting value |
| step | int | No | `1` | Increment per row |

**Supported data types:** `int`

### Examples

**Business — order line numbers:**

```yaml
name: line_number
data_type: int
generator:
  type: sequential
  start: 1
  step: 1
```

**Manufacturing — batch IDs (by 10s):**

```yaml
name: batch_id
data_type: int
generator:
  type: sequential
  start: 1000
  step: 10
```

---

## constant

Generate a fixed value for every row, with optional template variable support.

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| value | any | Yes | — | Constant value or template string |

**Supported data types:** any

### Magic Variables

Templates can reference these runtime variables:

| Variable | Description |
|----------|-------------|
| `{entity_id}` | Current entity name |
| `{entity_index}` | Entity index (0-based) |
| `{timestamp}` | Current row timestamp |
| `{row_number}` | Row index |

### Examples

**Metadata — source system tag:**

```yaml
name: source_system
data_type: string
generator:
  type: constant
  value: "MES_simulation"
```

**Templated — entity-specific reference:**

```yaml
name: record_ref
data_type: string
generator:
  type: constant
  value: "{entity_id}_batch_{row_number}"
```

---

## derived

Generate calculated columns from other columns using sandboxed Python expressions.

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| expression | string | Yes | — | Python expression referencing other column names |

**Supported data types:** any (depends on expression result)

### Expression Syntax

Expressions use Python syntax and can reference any column defined earlier in the column list by name.

**Arithmetic operators:**

| Operator | Description | Example |
|----------|-------------|---------|
| `+` | Addition | `price + tax` |
| `-` | Subtraction | `gross - tare` |
| `*` | Multiplication | `quantity * unit_price` |
| `/` | Division | `total / count` |
| `**` | Exponentiation | `base ** 2` |
| `//` | Floor division | `seconds // 60` |
| `%` | Modulo | `batch_id % 10` |

**Comparison operators:**

| Operator | Description | Example |
|----------|-------------|---------|
| `==` | Equal | `status == 'OK'` |
| `!=` | Not equal | `grade != 'FAIL'` |
| `<` | Less than | `temp < 100` |
| `>` | Greater than | `pressure > 50` |
| `<=` | Less or equal | `score <= 100` |
| `>=` | Greater or equal | `level >= threshold` |

**Logical operators:**

| Operator | Description | Example |
|----------|-------------|---------|
| `and` | Logical AND | `temp > 80 and pressure > 50` |
| `or` | Logical OR | `status == 'ERROR' or status == 'FAULT'` |
| `not` | Logical NOT | `not is_active` |

**Conditionals:**

```python
value_if_true if condition else value_if_false
```

### Safe Functions

| Function | Signature | Description |
|----------|-----------|-------------|
| `abs()` | `abs(x)` | Absolute value |
| `round()` | `round(x, n)` | Round to n decimals |
| `min()` | `min(a, b, ...)` | Minimum value |
| `max()` | `max(a, b, ...)` | Maximum value |
| `int()` | `int(x)` | Convert to integer |
| `float()` | `float(x)` | Convert to float |
| `str()` | `str(x)` | Convert to string |
| `bool()` | `bool(x)` | Convert to boolean |
| `coalesce()` | `coalesce(a, b, ...)` | Return first non-None value |
| `safe_div()` | `safe_div(a, b, default=None)` | Division handling None and zero |
| `safe_mul()` | `safe_mul(a, b, default=None)` | Multiplication handling None |

### Stateful Functions

These functions maintain state across rows within each entity, enabling time-series logic:

| Function | Signature | Description |
|----------|-----------|-------------|
| `prev()` | `prev(column_name, default=None)` | Get previous row's value for a column |
| `ema()` | `ema(column_name, alpha, default=None)` | Exponential moving average (0 < alpha ≤ 1) |
| `pid()` | `pid(pv, sp, Kp, Ki, Kd, dt, output_min, output_max, anti_windup)` | PID controller with anti-windup |

For full documentation, see **[Stateful Functions](stateful_functions.md)**.

### Cross-Entity References

Derived expressions can reference columns from other entities using `EntityName.column_name` syntax:

```yaml
expression: "Furnace01.temperature * 0.9 + ambient_offset"
```

The referenced entity must be defined in the same simulation. Odibi automatically resolves the dependency order. For details, see **[Advanced Features](advanced_features.md)**.

### Security

Expressions are evaluated in a **sandboxed namespace**. The following are explicitly blocked:

- No `import` statements
- No file I/O (`open`, `read`, `write`)
- No network access
- No system calls
- No access to `__builtins__`

### Examples

**Manufacturing — unit conversion:**

```yaml
name: temp_fahrenheit
data_type: float
generator:
  type: derived
  expression: "temp_celsius * 1.8 + 32"
```

**Business — order total with tax:**

```yaml
name: order_total
data_type: float
generator:
  type: derived
  expression: "round(quantity * unit_price * 1.08, 2)"
```

**IoT — alarm classification:**

```yaml
name: alarm_level
data_type: string
generator:
  type: derived
  expression: "'CRITICAL' if temp > 95 else ('WARNING' if temp > 80 else 'NORMAL')"
```

**Manufacturing — efficiency with null safety:**

```yaml
name: oee
data_type: float
generator:
  type: derived
  expression: "safe_div(good_units, total_units, 0) * 100"
```

---

## uuid

Generate unique identifiers.

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| version | int | No | `4` | 4 = random, 5 = deterministic |
| namespace | string | No | `DNS` | Namespace for UUID5 generation |

**Supported data types:** `string`

### Examples

**Random UUID (deterministic with seed):**

```yaml
name: transaction_id
data_type: string
generator:
  type: uuid
  version: 4
```

**Deterministic UUID (same input → same output):**

```yaml
name: device_id
data_type: string
generator:
  type: uuid
  version: 5
  namespace: "com.factory.devices"
```

---

## email

Generate email addresses.

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| domain | string | No | `example.com` | Email domain |
| pattern | string | No | `{entity}_{index}` | Username pattern |

**Supported data types:** `string`

### Examples

**Business — customer emails:**

```yaml
name: customer_email
data_type: string
generator:
  type: email
  domain: acme-corp.com
  pattern: "user_{row}"
```

**Output:** `user.5@acme-corp.com`

---

## ipv4

Generate IPv4 addresses, optionally within a specific subnet.

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| subnet | string | No | `None` | CIDR subnet (e.g., `192.168.0.0/24`) |

**Supported data types:** `string`

### Examples

**IoT — full range:**

```yaml
name: device_ip
data_type: string
generator:
  type: ipv4
```

**Constrained to a private subnet:**

```yaml
name: server_ip
data_type: string
generator:
  type: ipv4
  subnet: "10.0.1.0/24"
```

---

## geo

Generate geographic coordinates (latitude/longitude).

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| bbox | list[float] | Yes | — | Bounding box: `[min_lat, min_lon, max_lat, max_lon]` |
| format | string | No | `tuple` | `tuple` or `lat_lon_separate` |

**Supported data types:** `string`

### Examples

**Manufacturing — factory fleet within a region:**

```yaml
name: truck_location
data_type: string
generator:
  type: geo
  bbox: [33.7, -84.5, 34.0, -84.2]  # Atlanta metro area
  format: tuple
```

**Output:** `(33.8421, -84.3812)`

**IoT — sensor deployment zone:**

```yaml
name: sensor_location
data_type: string
generator:
  type: geo
  bbox: [51.4, -0.2, 51.6, 0.1]  # London area
  format: tuple
```

---

## Compatibility Matrix

All generators work across all engines, incremental mode, null injection, and entity overrides.

| Generator | Pandas | Spark | Polars | Incremental | Null Rate | Overrides |
|-----------|--------|-------|--------|-------------|-----------|-----------|
| range | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| random_walk | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| categorical | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| boolean | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| timestamp | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| sequential | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| constant | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| derived | ✅ | ✅ | ✅ | ✅ | ⚠️ * | ✅ |
| uuid | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| email | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| ipv4 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| geo | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

\* **Derived + null_rate:** `null_rate` is applied *after* the expression is calculated. If upstream columns have nulls, use null-safe functions (`coalesce`, `safe_div`, `safe_mul`) in your expression to avoid errors.

---

## See Also

- **[Stateful Functions](stateful_functions.md)** — `prev()`, `ema()`, `pid()` in depth
- **[Advanced Features](advanced_features.md)** — Cross-entity references, dependency DAGs
- **[Core Concepts](core_concepts.md)** — Entities, scopes, seeds, incremental mode
