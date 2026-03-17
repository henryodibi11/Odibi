# Process Simulation Guide

**Complete guide to simulating chemical processes and control systems with Odibi.**

This guide demonstrates all simulation features using classical chemical engineering examples from process control textbooks (Seborg, Stephanopoulos, Luyben).

---

## Table of Contents

1. [Stateful Functions Overview](#stateful-functions-overview)
2. [First-Order Systems](#first-order-systems)
3. [PID Control Loops](#pid-control-loops)
4. [Material and Energy Balances](#material-and-energy-balances)
5. [Mean-Reverting Processes](#mean-reverting-processes)
6. [Flowsheet Simulation (Cross-Entity)](#flowsheet-simulation-cross-entity)
7. [Scheduled Operations](#scheduled-operations)
8. [Complete Examples](#complete-examples)

---

## Stateful Functions Overview

Odibi provides three stateful functions for dynamic process simulation:

### `prev(column, default)` - Previous Row Value

Access the previous row's value for integration and lag calculations.

**Chemical Engineering Applications:**
- Material balances (level = prev('level') + inflow - outflow)
- Energy balances (temp = prev('temp') + heat_in - heat_out)
- First-order lags (response to control actions)
- Running totals (cumulative production)

### `ema(column, alpha, default)` - Exponential Moving Average

Smooth noisy sensor readings with configurable time constant.

**Chemical Engineering Applications:**
- Temperature sensor filtering (thermocouples with noise)
- Flow measurement smoothing (turbulent flow variations)
- Composition analyzer filtering (GC/MS with analytical noise)
- Pressure transmitter smoothing

**Alpha parameter:**
- `alpha = 0.1` → Heavy smoothing (90% weight to history)
- `alpha = 0.5` → Moderate smoothing
- `alpha = 0.9` → Light smoothing (90% weight to current value)

### `pid(pv, sp, Kp, Ki, Kd, dt, ...)` - PID Controller

Full PID controller with anti-windup for process control.

**Chemical Engineering Applications:**
- Temperature control (reactor heating/cooling)
- Pressure control (compressor speed, valve position)
- Level control (pump speed, valve opening)
- Flow control (control valve manipulation)
- pH control (acid/base dosing)

---

## First-Order Systems

**Reference:** Seborg Chapter 5 - Dynamic Response Characteristics

### Theory

First-order systems respond to changes according to:

```
τ(dy/dt) + y = Kp × u
```

Where:
- τ = time constant (how fast the system responds)
- Kp = process gain (steady-state change in y per unit change in u)
- y = process variable (output)
- u = manipulated variable (input)

**Discrete approximation using prev():**

```
y[t] = y[t-1] + (Δt/τ) × (Kp×u[t] - y[t-1])
```

### Implementation

**Example: Tank Temperature Responding to Heater**

```yaml
columns:
  # Heater output (manipulated variable)
  - name: heater_pct
    data_type: float
    generator:
      type: random_walk
      start: 50.0
      min: 0.0
      max: 100.0

  # Tank temperature (process variable)
  # τ = 300 seconds (5 min time constant)
  # Kp = 0.5 (50% heater → 25°C increase at steady state)
  - name: tank_temp_c
    data_type: float
    generator:
      type: derived
      expression: "prev('tank_temp_c', 25.0) + (60.0/300.0) * (0.5*heater_pct - prev('tank_temp_c', 25.0))"
```

**Explanation:**
- Δt = 60 seconds (1-minute timestep)
- τ = 300 seconds (5-minute time constant)
- Gain = Δt/τ = 60/300 = 0.2 (20% of error corrected each timestep)
- Steady-state: heater=50% → temp ≈ 25°C

**Textbook Problem:** Similar to Seborg Example 5.3 (First-order tank heating)

---

## PID Control Loops

**Reference:** Seborg Chapter 8 - Feedback Controllers

### Theory

PID controllers calculate control output based on error (setpoint - process variable):

```
u(t) = Kp×e(t) + Ki×∫e(τ)dτ + Kd×(de/dt)
```

Where:
- **Kp** (Proportional gain): Immediate response to error
- **Ki** (Integral gain): Eliminates steady-state offset
- **Kd** (Derivative gain): Dampens oscillations, anticipates change

**Tuning Guidelines (Ziegler-Nichols):**
- **P-only:** Kp = 0.5×Ku, Ki=0, Kd=0 (fast but offset)
- **PI:** Kp = 0.45×Ku, Ki=Kp/1.2τu, Kd=0 (no offset, some overshoot)
- **PID:** Kp = 0.6×Ku, Ki=Kp/2τu, Kd=KpτU/8 (fast, minimal overshoot)

### Implementation

**Example: CSTR Temperature Control**

```yaml
columns:
  # Temperature setpoint
  - name: temp_setpoint_c
    data_type: float
    generator:
      type: constant
      value: 85.0

  # Process variable (reactor temperature)
  # Responds to cooling water (first-order dynamics)
  - name: reactor_temp_c
    data_type: float
    generator:
      type: derived
      expression: "prev('reactor_temp_c', 85.0) + 0.1 * (prev('cooling_pct', 50) - prev('reactor_temp_c', 85.0))"

  # PID controller output (cooling water valve)
  - name: cooling_pct
    data_type: float
    generator:
      type: derived
      expression: "pid(pv=reactor_temp_c, sp=temp_setpoint_c, Kp=3.0, Ki=0.15, Kd=1.0, dt=300, output_min=0, output_max=100, anti_windup=True)"
```

**Tuning Rationale:**
- **Kp = 3.0:** Strong proportional action (reactor temp is critical)
- **Ki = 0.15:** Moderate integral action (eliminate offset without excessive overshoot)
- **Kd = 1.0:** Moderate derivative action (dampen temperature swings)
- **dt = 300:** 5-minute timestep in seconds
- **anti_windup = True:** Prevents integral windup when valve saturates

**Textbook Problem:** Similar to Seborg Example 8.2 (CSTR temperature control)

---

## Material and Energy Balances

**Reference:** Seborg Chapter 2 - Theoretical Models

### Material Balance

For a well-mixed tank:

```
V(dc/dt) = F_in×c_in - F_out×c_out + V×r
```

Discrete form using `prev()`:

```
c[t] = c[t-1] + (Δt/V) × (F_in×c_in - F_out×c_out + V×r)
```

### Implementation

**Example: CSTR with Reaction**

```yaml
scope:
  timestep: "5m"  # 300 seconds

columns:
  # Reactor volume (constant)
  - name: volume_m3
    data_type: float
    generator:
      type: constant
      value: 10.0

  # Inlet flow
  - name: feed_flow_m3_hr
    data_type: float
    generator:
      type: random_walk
      start: 5.0
      min: 4.0
      max: 6.0

  # Outlet flow (equal to inlet at steady state)
  - name: effluent_flow_m3_hr
    data_type: float
    generator:
      type: derived
      expression: "feed_flow_m3_hr * (prev('level_m3', 10.0) / volume_m3)"

  # Reactor level (material balance integration)
  - name: level_m3
    data_type: float
    generator:
      type: derived
      expression: "prev('level_m3', 10.0) + (feed_flow_m3_hr - effluent_flow_m3_hr) * (5/60)"

  # Residence time (minutes)
  - name: residence_time_min
    data_type: float
    generator:
      type: derived
      expression: "(level_m3 / feed_flow_m3_hr) * 60.0"
```

**Explanation:**
- Level changes by (inflow - outflow) × Δt
- Δt = 5 min = 5/60 hour (flows in m³/hr)
- Residence time = volume / flow rate
- Effluent uses `prev('level')` to avoid circular dependency

**Textbook Problem:** Similar to Seborg Example 2.1 (Liquid storage tank)

### Energy Balance

For a jacketed reactor:

```
ρVCp(dT/dt) = F_in×ρCp×(T_in - T) + (-ΔHrxn)×r×V + UA(T_jacket - T)
```

### Implementation

**Example: Reactor with Cooling Jacket**

```yaml
columns:
  # Heat of reaction (kW) - exothermic
  - name: heat_generation_kw
    data_type: float
    generator:
      type: constant
      value: 50.0  # Exothermic reaction

  # Jacket temperature
  - name: jacket_temp_c
    data_type: float
    generator:
      type: random_walk
      start: 20.0
      min: 15.0
      max: 25.0

  # Reactor temperature (energy balance)
  - name: reactor_temp_c
    data_type: float
    generator:
      type: derived
      expression: "prev('reactor_temp_c', 85.0) + (heat_generation_kw - 2.0*(prev('reactor_temp_c', 85.0) - jacket_temp_c)) / 100.0"
```

**Explanation:**
- Heat generation from reaction: +50 kW
- Heat loss to jacket: -UA×(T_reactor - T_jacket)
- UA = 2.0 kW/°C (heat transfer coefficient × area)
- Thermal capacitance = 100 kW/(°C/min) → dividing by this gives rate of temperature change

---

## Mean-Reverting Processes

**Reference:** Seborg Chapter 5 - Transfer Function Models

Many process variables naturally revert toward a reference value:

### Static Setpoint

Controlled variables revert to a fixed setpoint via PID control:

```yaml
- name: pressure_bar
  generator:
    type: random_walk
    start: 3.0
    min: 2.5
    max: 3.5
    volatility: 0.1
    mean_reversion: 0.2  # PID pulls toward 3.0
```

### Dynamic Setpoint (NEW: mean_reversion_to)

Variables that track changing boundary conditions:

**Example: Battery Temperature Tracking Ambient**

```yaml
- name: ambient_temp_c
  generator:
    type: random_walk
    start: 25.0
    min: 20.0
    max: 30.0

- name: battery_temp_c
  generator:
    type: random_walk
    start: 28.0
    min: 22.0
    max: 35.0
    mean_reversion: 0.1
    mean_reversion_to: ambient_temp_c  # Drifts toward ambient (heat loss)
```

**Use Cases:**
- Reactor temperature → Cooling water temperature
- Storage tank temperature → Ambient temperature
- Process variable → Load-dependent setpoint
- HTF temperature → Environmental conditions

---

## Flowsheet Simulation (Cross-Entity)

**Reference:** Seborg Chapter 3 - Dynamic Modeling Principles

Real processes consist of multiple interconnected units. Odibi supports cross-entity references for flowsheet simulation.

### Theory

Material and energy streams connect process units:

```
Unit A → Stream → Unit B
```

Unit B's inlet conditions = Unit A's outlet conditions.

### Implementation

**Example: Two Tanks in Series**

```yaml
entities:
  names:
    - Tank_A
    - Tank_B

columns:
  - name: level
    generator:
      type: random_walk
      start: 5.0
      min: 0.0
      max: 10.0

  # Tank_A has external feed
  - name: feed_flow
    generator:
      type: constant
      value: 2.0
    entity_overrides:
      Tank_B:
        type: constant
        value: 0.0  # Tank_B has no external feed

  # Outlet flow (proportional to level)
  - name: outlet_flow
    generator:
      type: derived
      expression: "level * 0.3"

  # Tank_B receives Tank_A's outlet (CROSS-ENTITY!)
  - name: flow_from_tank_a
    generator:
      type: derived
      expression: "Tank_A.outlet_flow if entity_id == 'Tank_B' else 0.0"

  # Tank_B level balance
  - name: level_integrated
    generator:
      type: derived
      expression: "prev('level_integrated', 5.0) + (feed_flow + flow_from_tank_a - outlet_flow) * 0.1 if entity_id == 'Tank_B' else prev('level_integrated', 5.0) + (feed_flow - outlet_flow) * 0.1"
```

**Result:**
- Tank_A receives external feed
- Tank_A drains to Tank_B
- Tank_B level responds to Tank_A's outlet (realistic cascade)

**Textbook Problem:** Similar to Stephanopoulos Example 7.4 (Interacting tanks)

### Advanced: Heat Exchanger Network

**Example: CSTR with Heat Recovery**

```yaml
entities:
  names:
    - Reactor_R101
    - HeatExchanger_E101

columns:
  # Reactor effluent temperature
  - name: effluent_temp_c
    generator:
      type: random_walk
      start: 85.0
      min: 75.0
      max: 95.0
    entity_overrides:
      HeatExchanger_E101:
        type: constant
        value: 0.0

  # Heat exchanger hot side (receives reactor effluent)
  - name: hot_side_temp_c
    generator:
      type: derived
      expression: "Reactor_R101.effluent_temp_c if entity_id == 'HeatExchanger_E101' else 0.0"

  # Heat duty (proportional to ΔT)
  - name: heat_duty_kw
    generator:
      type: derived
      expression: "max(0, (hot_side_temp_c - 25.0) * 10.0) if entity_id == 'HeatExchanger_E101' else 0.0"
```

**Result:**
- Heat exchanger sees actual reactor outlet temperature
- Heat duty calculated from temperature difference
- Realistic thermal integration

---

## Scheduled Operations

**Reference:** Real-world process operations scheduling

### Maintenance Windows

**Example: Planned Turbine Shutdown**

```yaml
scheduled_events:
  - type: forced_value
    entity: Turbine_01
    column: power_kw
    value: 0.0
    start_time: "2026-03-11T14:00:00Z"
    end_time: "2026-03-11T18:00:00Z"  # 4-hour maintenance
```

### Setpoint Changes

**Example: Temperature Ramp for Startup**

```yaml
scheduled_events:
  # Stage 1: Low temp
  - type: forced_value
    entity: Reactor_01
    column: temp_setpoint_c
    value: 70.0
    start_time: "2026-03-11T00:00:00Z"
    end_time: "2026-03-11T02:00:00Z"

  # Stage 2: Medium temp
  - type: forced_value
    entity: Reactor_01
    column: temp_setpoint_c
    value: 80.0
    start_time: "2026-03-11T02:00:00Z"
    end_time: "2026-03-11T04:00:00Z"

  # Stage 3: Operating temp (permanent)
  - type: forced_value
    entity: Reactor_01
    column: temp_setpoint_c
    value: 85.0
    start_time: "2026-03-11T04:00:00Z"
    end_time: null  # Permanent
```

### Grid Curtailment (Renewable Energy)

**Example: Wind Farm Curtailment**

```yaml
scheduled_events:
  - type: forced_value
    entity: null  # All turbines
    column: power_kw
    value: 0.0
    start_time: "2026-03-11T16:00:00Z"
    end_time: "2026-03-11T17:00:00Z"
```

---

## Complete Examples

### Example 1: CSTR with PI Control

**Based on:** Seborg Example 8.2

```yaml
scope:
  start_time: "2026-03-11T00:00:00Z"
  timestep: "1m"
  row_count: 120
  seed: 42

entities:
  names: [Reactor_R101]

columns:
  - name: timestamp
    data_type: timestamp
    generator:
      type: timestamp

  # Setpoint
  - name: temp_setpoint_c
    data_type: float
    generator:
      type: constant
      value: 85.0

  # Process variable (first-order response to cooling)
  - name: reactor_temp_c
    data_type: float
    generator:
      type: derived
      expression: "prev('reactor_temp_c', 90.0) + 0.05 * (prev('cooling_pct', 50) - prev('reactor_temp_c', 90.0)) + 0.2"
      # 0.05 = process gain, 0.2 = disturbance (heat generation)

  # PI controller (no derivative for noisy temperatures)
  - name: cooling_pct
    data_type: float
    generator:
      type: derived
      expression: "pid(pv=reactor_temp_c, sp=temp_setpoint_c, Kp=2.0, Ki=0.1, Kd=0.0, dt=60)"
```

**Expected Behavior:**
- Reactor starts at 90°C (above setpoint)
- PID drives cooling to bring temperature down
- Converges to 85°C ± 2°C within ~20 minutes
- Steady-state offset eliminated by integral action

### Example 2: Battery SOC Integration

**Based on:** Electrochemical energy storage principles

```yaml
scope:
  timestep: "5m"
  row_count: 120

entities:
  names: [BESS_Module_01]

columns:
  - name: timestamp
    data_type: timestamp
    generator:
      type: timestamp

  # Charge/discharge current (A)
  - name: current_a
    data_type: float
    generator:
      type: random_walk
      start: 20.0  # Charging
      min: -30.0  # Max discharge
      max: 30.0  # Max charge

  # Battery capacity
  - name: capacity_ah
    data_type: float
    generator:
      type: constant
      value: 100.0

  # State of charge (integration)
  - name: soc_pct
    data_type: float
    generator:
      type: derived
      expression: "max(0, min(100, prev('soc_pct', 50) + (current_a * (5/60) / capacity_ah * 100)))"
      # SOC = prev SOC + (current × time) / capacity × 100
```

**Explanation:**
- Δt = 5 minutes = 5/60 hours
- Coulomb counting: ΔQ = I × Δt
- ΔSOC = ΔQ / Capacity × 100%
- Clamped to [0, 100] for physical limits

### Example 3: Complete Flowsheet with All Features

See `cstr_flowsheet_complete.yaml`

**This example demonstrates:**
- ✅ prev() for material/energy balances
- ✅ ema() for sensor noise filtering
- ✅ pid() for temperature control
- ✅ mean_reversion_to for thermal losses to ambient
- ✅ Cross-entity references (CSTR → Separator → HX)
- ✅ Scheduled events (maintenance, optimization, feed changes)

**Realistic 10-hour operation with:**
- Normal operation (00:00-05:00)
- Maintenance shutdown (05:00-06:00)
- Startup recovery (06:00-07:00)
- Process optimization (07:00+, higher temp)
- Feed composition change (08:00+, richer feed)

---

## Best Practices

### 1. Avoid Circular Dependencies

**❌ Bad:**
```yaml
- name: level
  expression: "prev('level', 5) + flow_in - flow_out"

- name: flow_out
  expression: "level * 0.1"  # Uses CURRENT level → circular!
```

**✅ Good:**
```yaml
- name: level
  expression: "prev('level', 5) + flow_in - flow_out"

- name: flow_out
  expression: "prev('level', 5) * 0.1"  # Uses PREVIOUS level → OK
```

### 2. Match Timestep to Process Dynamics

- **Fast processes** (pressure, flow): 1-10 second timesteps
- **Medium processes** (temperature, level): 1-5 minute timesteps
- **Slow processes** (composition, pH): 5-30 minute timesteps

### 3. PID Tuning Guidelines

Start conservative and iterate:
1. **P-only:** Set Kp, Ki=0, Kd=0 → Find stable Kp
2. **Add I:** Add Ki = Kp/10 → Eliminate offset
3. **Add D:** Add Kd = Kp/4 → Reduce oscillation

### 4. Use Realistic Time Constants

From textbooks or plant data:
- **Temperature loops:** τ = 5-30 minutes
- **Flow loops:** τ = 10-60 seconds
- **Level loops:** τ = 2-20 minutes
- **Composition loops:** τ = 5-60 minutes

---

## Reference Materials

**Recommended Textbooks:**
1. **Seborg, Edgar, Mellichamp, Doyle** - "Process Dynamics and Control" (3rd/4th Ed)
   - Chapter 5: Dynamic response characteristics
   - Chapter 8: Feedback controllers (PID tuning)
   - Chapter 2: Theoretical modeling (mass/energy balances)

2. **Stephanopoulos** - "Chemical Process Control"
   - Chapter 7: Dynamic behavior of processes
   - Chapter 23: Design of feedback controllers

3. **Luyben** - "Process Modeling, Simulation, and Control"
   - Chapter 3: Mathematical models
   - Chapter 6: Controller tuning

**Online Resources:**
- [Control Tutorials for MATLAB](http://ctms.engin.umich.edu/) - University of Michigan
- [Process Control Lectures](https://apmonitor.com/pdc/) - BYU APMonitor

---

## Troubleshooting

### PID Not Converging

**Symptoms:** Process variable oscillates or drifts away

**Solutions:**
1. Reduce Kp (proportional gain too high)
2. Reduce Ki (integral action too aggressive)
3. Increase dt (timestep might be wrong)
4. Check process dynamics (time constant might be much slower than expected)

### Integration Drifting

**Symptoms:** Level or temperature unbounded increase/decrease

**Solutions:**
1. Check mass/energy balance (inflow should ≈ outflow)
2. Verify units (flow in m³/hr, time in hours, etc.)
3. Add clamping with `max()` and `min()`
4. Check for missing terms in balance equation

### Cross-Entity Reference Not Working

**Symptoms:** `AttributeError: Entity 'Tank_A' row not yet available`

**Solutions:**
1. Check entity dependency order (Tank_A must be generated before Tank_B)
2. Don't use `Entity.prev()` - not supported yet
3. Verify entity names match exactly (case-sensitive)

---

## Next Steps

- **Validate with real data:** Compare simulated dynamics to plant data
- **Parameter estimation:** Fit time constants, gains to actual process
- **Advanced features:** Nonlinear dynamics, multivariable control
- **Model-based control:** MPC using simulated responses

---

**For questions or examples, see:**
- Complete CSTR Flowsheet (`cstr_flowsheet_complete.yaml`)
- Battery SOC Integration (`battery_soc_simulation.yaml`)
- Solar Thermal Tracking (`solar_thermal_tracking.yaml`)
