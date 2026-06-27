# Stateful Simulation Functions

**New in v3.3**: `prev()`, `ema()`, `pid()`, and `delay()` for time-dependent process simulation

---

## Overview

Stateful functions enable realistic dynamic process simulation by maintaining state across rows within each entity. This unlocks critical capabilities for process engineering, manufacturing, and renewable energy applications.

**Key Use Cases:**
- Battery state-of-charge (SOC) integration
- PID temperature/pressure control loops
- First-order process dynamics (thermal lags, flow responses)
- Sensor smoothing and noise filtering
- Cumulative metrics (energy, material consumed, run time)

---

## Functions

### `prev(column_name, default=None)`

Access the previous row's value for a column within the same entity.

**Parameters:**
- `column_name` (str): Name of column to retrieve
- `default`: Value to return for first row (no previous value exists)

**Returns:** Previous row value or default

**Example: Cumulative Sum**
```yaml
columns:
  - name: counter
    data_type: int
    generator: {type: constant, value: 1}

  - name: cumsum
    data_type: float
    generator:
      type: derived
      expression: "prev('cumsum', 0) + counter"
```

**Example: First-Order Lag**
```yaml
# Temperature sensor with time constant tau=5 timesteps
- name: measured_temp_c
  data_type: float
  generator:
    type: derived
    expression: "prev('measured_temp_c', 70) + 0.2 * (actual_temp_c - prev('measured_temp_c', 70))"
```

---

### `ema(column_name, alpha, default=None)`

Exponential moving average for smoothing noisy signals.

**Parameters:**
- `column_name` (str): Column to smooth
- `alpha` (float): Smoothing factor, 0-1. Higher = more weight to current value
- `default`: Initial EMA value for first row

**Returns:** Smoothed value

**Formula:** `EMA[t] = alpha * value[t] + (1 - alpha) * EMA[t-1]`

**Example: Sensor Noise Filtering**
```yaml
columns:
  - name: raw_pressure_psi
    data_type: float
    generator: {type: range, min: 148, max: 152}  # Noisy sensor

  - name: smoothed_pressure_psi
    data_type: float
    generator:
      type: derived
      expression: "ema('raw_pressure_psi', alpha=0.3, default=150)"
```

**Choosing Alpha:**
- `alpha = 0.1`: Heavy smoothing, slow response (10% current, 90% history)
- `alpha = 0.5`: Balanced smoothing (50/50 split)
- `alpha = 0.9`: Light smoothing, fast response (90% current, 10% history)

---

### `pid(pv, sp, Kp, Ki, Kd, dt, output_min, output_max, anti_windup)`

PID controller with anti-windup for closed-loop control simulation.

**Parameters:**
- `pv` (float): Process variable (current measurement)
- `sp` (float): Setpoint (target value)
- `Kp` (float): Proportional gain (default: 1.0)
- `Ki` (float): Integral gain (default: 0.0)
- `Kd` (float): Derivative gain (default: 0.0)
- `dt` (float): Time step in seconds - **must match simulation timestep** (default: 1.0)
- `output_min` (float): Minimum controller output (default: 0.0)
- `output_max` (float): Maximum controller output (default: 100.0)
- `anti_windup` (bool): Enable anti-windup protection (default: True)

**Returns:** Controller output, clamped to [output_min, output_max]

**Example: Temperature Control**
```yaml
columns:
  - name: reactor_temp_c
    data_type: float
    generator: {type: range, min: 82, max: 88}

  - name: temp_setpoint_c
    data_type: float
    generator: {type: constant, value: 85.0}

  - name: heater_output_pct
    data_type: float
    generator:
      type: derived
      expression: >
        pid(
          pv=reactor_temp_c,
          sp=temp_setpoint_c,
          Kp=3.0,
          Ki=0.2,
          Kd=1.0,
          dt=60,
          output_min=0,
          output_max=100,
          anti_windup=True
        )
```

**PID Tuning Guidelines:**
- **P-only** (Kp>0, Ki=0, Kd=0): Fast but may have steady-state error
- **PI** (Kp>0, Ki>0, Kd=0): Eliminates steady-state error, common in industry
- **PID** (all >0): Best disturbance rejection, but requires tuning

**Anti-Windup:**
- Prevents integral term from growing when output is saturated
- **Always use `anti_windup=True`** for realistic control
- Critical for processes with frequent saturation (heating/cooling limits)

---

## Complete Example: Battery Energy Storage

### Scenario: Battery SOC Tracking with Thermal Management

```yaml
simulation:
  scope:
    start_time: "2026-01-01T00:00:00Z"
    timestep: "5m"
    row_count: 100  # ~8 hours
    seed: 42

  entities:
    count: 1
    id_prefix: "BESS_Module_"

  columns:
    # Time tracking
    - name: timestamp
      data_type: timestamp
      generator: {type: timestamp}

    - name: row_num
      data_type: int
      generator:
        type: derived
        expression: "prev('row_num', -1) + 1"

    # Battery constants
    - name: capacity_ah
      data_type: float
      generator: {type: constant, value: 100.0}

    # Operating cycle (charge → discharge → idle)
    - name: charge_current_a
      data_type: float
      generator:
        type: derived
        expression: "20.0 if row_num < 40 else (-15.0 if row_num < 80 else 0.0)"

    # SOC INTEGRATION (requires prev!)
    - name: soc_pct
      data_type: float
      generator:
        type: derived
        expression: >
          max(0, min(100,
            prev('soc_pct', 50) +
            (charge_current_a * (5/60)) / capacity_ah * 100
          ))

    # Thermal model
    - name: ambient_temp_c
      data_type: float
      generator: {type: range, min: 20, max: 30}

    - name: temp_setpoint_c
      data_type: float
      generator: {type: constant, value: 25.0}

    - name: heat_gen_w
      data_type: float
      generator:
        type: derived
        expression: "0.01 * charge_current_a * charge_current_a"

    - name: module_temp_c
      data_type: float
      generator:
        type: derived
        expression: >
          prev('module_temp_c', 25) +
          0.03 * (heat_gen_w / 100) +
          0.05 * (ambient_temp_c - prev('module_temp_c', 25)) -
          0.04 * (prev('cooling_pct', 0) / 100) * (prev('module_temp_c', 25) - 20)

    # PID COOLING CONTROL
    - name: cooling_pct
      data_type: float
      generator:
        type: derived
        expression: >
          pid(
            pv=module_temp_c,
            sp=temp_setpoint_c,
            Kp=4.0,
            Ki=0.15,
            Kd=0.8,
            dt=300
          )

    # Smoothed temperature for dashboards
    - name: temp_display_c
      data_type: float
      generator:
        type: derived
        expression: "ema('module_temp_c', alpha=0.2, default=25)"
```

**What This Simulates:**
1. **Realistic SOC tracking**: Charge increases SOC, discharge decreases it
2. **Thermal dynamics**: Heat generation from I²R losses, ambient heat transfer
3. **PID cooling control**: HVAC adjusts to maintain setpoint
4. **Sensor smoothing**: EMA filters temperature for display
5. **All per-entity**: Each battery module has independent state

---

## Technical Details

### State Persistence

**Within a Run:**
- State is maintained across rows for each entity
- Each entity has independent state (Module_01 doesn't see Module_02's prev values)
- State is reset at the start of each simulation run

**Across Incremental Runs:**
- Currently: `prev()` and `pid()` state is **NOT** persisted (resets each run)
- `random_walk` state **IS** persisted via StateManager
- Future enhancement: Persist entity_state for true continuous simulation

### Performance

- **Overhead:** Minimal (~5% for typical use cases)
- **Memory:** O(columns × entities) for state storage
- **Determinism:** State is deterministic given same seed + entity order

### Limitations

1. **No cross-entity references** (yet)
   - Can't reference another entity's values
   - Workaround: Use entity_overrides for coupling

2. **No time-based indexing** (yet)
   - Can't do `prev('temp', time_offset='5m')`
   - Only immediate previous row

3. **State not persisted across runs** (yet)
   - Incremental mode starts fresh each time
   - Acceptable for most use cases (daily batch jobs)

---

## Common Patterns

### Pattern 1: Cumulative Metrics

```yaml
# Total runtime hours
- name: runtime_hours
  generator:
    type: derived
    expression: "prev('runtime_hours', 0) + (1/60) if equipment_running else prev('runtime_hours', 0)"

# Total energy consumption
- name: total_energy_kwh
  generator:
    type: derived
    expression: "prev('total_energy_kwh', 0) + power_kw * (timestep_minutes / 60)"
```

### Pattern 2: State Transitions

```yaml
# Equipment state machine
- name: prev_state
  generator:
    type: derived
    expression: "prev('current_state', 'idle')"

- name: current_state
  generator:
    type: derived
    expression: >
      'running' if start_signal and prev_state == 'idle' else
      ('stopping' if stop_signal and prev_state == 'running' else
      prev_state)
```

### Pattern 3: Material Balance Integration

```yaml
# Tank level integration
# Level[t] = Level[t-1] + (Flow_in - Flow_out) * dt / Area
- name: tank_level_m
  generator:
    type: derived
    expression: >
      max(0, min(10,
        prev('tank_level_m', 5) +
        (flow_in_m3_s - flow_out_m3_s) * 60 / tank_area_m2
      ))
```

### Pattern 4: Closed-Loop Control with Process

```yaml
# Process responds to controller
- name: pv
  generator:
    type: derived
    expression: "prev('pv', 50) + 0.1 * (prev('controller_output', 50) - prev('pv', 50))"

# Controller tracks setpoint
- name: controller_output
  generator:
    type: derived
    expression: "pid(pv=pv, sp=setpoint, Kp=2.0, Ki=0.1, dt=60)"
```

---

## Best Practices

### 1. Match `dt` to Timestep

```yaml
scope:
  timestep: "1m"  # 60 seconds

# Later in PID:
pid(..., dt=60)  # MUST match timestep in seconds
```

### 2. Provide Sensible Defaults

```yaml
# BAD: Default might be far from reality
expression: "prev('soc_pct', 0)"  # SOC of 0% is unrealistic

# GOOD: Default near expected initial value
expression: "prev('soc_pct', 50)"  # Start at half charge
```

### 3. Always Clamp Integrations

```yaml
# BAD: SOC could exceed physical bounds
expression: "prev('soc_pct', 50) + delta_soc"

# GOOD: Clamp to [0, 100]
expression: "max(0, min(100, prev('soc_pct', 50) + delta_soc))"
```

### 4. Use Anti-Windup for PID

```yaml
# ALWAYS use anti_windup=True for realistic control
pid(..., anti_windup=True)
```

### 5. Combine with Random Walk

```yaml
# PV has both control response AND slow drift
- name: pv_baseline
  generator: {type: random_walk, start: 50, drift: 0.01, noise: 0.5}

- name: pv
  generator:
    type: derived
    expression: "pv_baseline + 0.1 * (prev('controller_output', 50) - pv_baseline)"
```

---

## Comparison with Random Walk

| Feature | Random Walk | prev() + Derived |
|---------|-------------|------------------|
| **Autocorrelation** | ✓ Built-in | ✓ Custom |
| **Mean reversion** | ✓ To fixed value | ✓ To any column |
| **Control loops** | ✗ | ✓ Full PID |
| **Integrations** | ✗ | ✓ SOC, tank level, etc. |
| **State persisted** | ✓ Across runs | ✗ Per-run only |
| **Complexity** | Low | Medium |

**Recommendation:** Use random_walk for ambient drift, prev() for engineered dynamics.

---

## Renewable Energy Examples

### Battery SOC Integration

```yaml
- name: soc_pct
  generator:
    type: derived
    expression: >
      max(0, min(100,
        prev('soc_pct', 50) +
        (charge_current_a * (timestep_min / 60)) / capacity_ah * 100
      ))
```

### Solar Thermal HTF Temperature Control

```yaml
- name: htf_temp_c
  generator:
    type: derived
    expression: >
      prev('htf_temp_c', 300) +
      0.05 * (prev('pump_speed_pct', 50) / 100) * (solar_irradiance_w_m2 / 1000) -
      0.02 * (prev('htf_temp_c', 300) - ambient_temp_c)

- name: pump_speed_pct
  generator:
    type: derived
    expression: "pid(pv=htf_temp_c, sp=350, Kp=1.5, Ki=0.05, dt=300)"
```

### Wind Turbine Gearbox Oil Temperature

```yaml
- name: oil_temp_c
  generator:
    type: derived
    expression: >
      prev('oil_temp_c', 40) +
      0.01 * (power_kw / 1000) +
      0.03 * (ambient_temp_c - prev('oil_temp_c', 40)) -
      0.02 * (prev('cooling_fan_pct', 0) / 100) * (prev('oil_temp_c', 40) - 30)

- name: cooling_fan_pct
  generator:
    type: derived
    expression: "pid(pv=oil_temp_c, sp=55, Kp=5.0, Ki=0.3, dt=60)"
```

---

## Advanced: Multi-Loop Control

### Cascade Control (Outer + Inner Loop)

```yaml
# Outer loop: Control product composition
- name: composition_sp_inner
  generator:
    type: derived
    expression: "pid(pv=product_composition, sp=composition_sp, Kp=2.0, Ki=0.1, dt=60, output_min=50, output_max=150)"

# Inner loop: Control temperature to track outer loop's setpoint
- name: heater_pct
  generator:
    type: derived
    expression: "pid(pv=reactor_temp_c, sp=composition_sp_inner, Kp=5.0, Ki=0.5, dt=60)"
```

### Feedforward + Feedback

```yaml
# Feedforward: Compensate for known disturbance
- name: ff_output
  generator:
    type: derived
    expression: "feed_flow_rate * 0.5"  # Proportional to load

# Feedback: PID corrects for unmeasured disturbances
- name: fb_output
  generator:
    type: derived
    expression: "pid(pv=pv, sp=setpoint, Kp=1.0, Ki=0.05, dt=60)"

# Combined
- name: total_output
  generator:
    type: derived
    expression: "ff_output + fb_output"
```

---

## Troubleshooting

### Issue: PID Oscillates Wildly

**Cause:** Gains too high or derivative kick

**Fix:**
- Reduce `Kp`, `Ki`, `Kd` by 50% and re-test
- Start with P-only (Ki=0, Kd=0), then add I, then D
- Use `anti_windup=True`

### Issue: PID Doesn't Converge

**Cause:** `dt` doesn't match timestep, or process model wrong

**Fix:**
```yaml
# If timestep is "5m", dt must be 300 seconds
scope:
  timestep: "5m"

# Later:
pid(..., dt=300)  # 5 minutes = 300 seconds
```

### Issue: prev() Returns Unexpected Values

**Cause:** Dependency order, or referencing column not yet generated

**Fix:**
- Ensure column being prev()'d is defined **before** the column using it
- Topological sort handles this automatically for same-row deps
- prev() accesses **previous row**, so order within current row doesn't matter

### Issue: EMA Not Smoothing Enough

**Cause:** `alpha` too high

**Fix:**
- Lower `alpha` (try 0.1 for heavy smoothing)
- Or use multiple EMA stages:
```yaml
- name: ema1
  expression: "ema('raw', alpha=0.3, default=raw)"
- name: ema2
  expression: "ema('ema1', alpha=0.3, default=ema1)"
```

---

## Implementation Notes

**Determinism:**
- Stateful functions are fully deterministic given same seed + entity order
- State is per-entity, so entity order affects results

**Engine Support:**
- Pandas: Full support
- Spark: Full support (state maintained within partition)
- Polars: Full support

**Null Handling:**
- `prev()` skips null updates (returns last non-null value)
- `ema()` propagates nulls gracefully
- `pid()` returns None if pv or sp is None

---

## Future Enhancements

**Planned (Sprint 2):**
- `mean_reversion_to` parameter for random_walk (revert to any column, not just fixed value)
- Cross-entity references (access other entity's values)
- Scheduled events (maintenance windows, grid curtailments)

**Under Consideration:**
- `lag(column, n)` - access n rows back
- `integral(column, dt)` - explicit integration helper
- `derivative(column, dt)` - rate of change
- State persistence across incremental runs

---

## See Also

- [Chemical Engineering Simulation Guide](../guides/chemical_engineering_simulation_guide.md)
- [Odibi for Renewable Energy](../guides/odibi_for_renewable_energy.md)
- [Simulation Overview](./simulation.md)
- Battery SOC Example (`battery_soc_simulation.yaml`)
