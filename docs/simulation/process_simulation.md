# Process Control & Chemical Engineering Simulation

## Overview

Odibi's simulation engine supports realistic process simulation using stateful functions (`prev`, `ema`, `pid`), first-order dynamics, material and energy balances, and PID control — all defined in YAML. No custom code required.

Whether you're modeling a single CSTR, a multi-unit flowsheet, or a full plant with PID-controlled loops, scheduled maintenance, and sensor noise, the simulation framework provides the building blocks to generate physically plausible time-series data from configuration alone.

**Key capabilities:**

| Capability | Odibi Feature |
|---|---|
| First-order dynamics | `prev()` in derived expressions |
| Sensor smoothing | `ema()` with configurable alpha |
| PID control | `pid()` with anti-windup, output limits |
| Mean-reverting processes | `random_walk` with `mean_reversion` |
| Dynamic setpoint tracking | `mean_reversion_to` column reference |
| Cross-entity streams | `Entity.column` references |
| Scheduled operations | `scheduled_events` for maintenance, setpoint changes |

---

## First-Order Systems

**Reference:** Seborg Chapter 5 — Dynamic Response Characteristics

### Theory

First-order systems respond to input changes according to:

```
τ(dy/dt) + y = Kp × u
```

Where:

- **τ** = time constant (how fast the system responds)
- **Kp** = process gain (steady-state change in y per unit change in u)
- **y** = process variable (output)
- **u** = manipulated variable (input)

**Discrete approximation using `prev()`:**

```
y[t] = y[t-1] + (Δt/τ) × (Kp × u[t] - y[t-1])
```

The ratio `Δt/τ` controls responsiveness per timestep. Smaller values mean slower response; larger values (approaching 1.0) mean faster convergence.

### Example: Tank Temperature Responding to Heater

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

- **Δt = 60 seconds** (1-minute timestep)
- **τ = 300 seconds** (5-minute time constant)
- **Gain = Δt/τ = 60/300 = 0.2** → 20% of error corrected each timestep
- **Steady-state:** heater=50% → temp ≈ 25°C

**Textbook Problem:** Similar to Seborg Example 5.3 (First-order tank heating)

---

## PID Control Loops

**Reference:** Seborg Chapter 8 — Feedback Controllers

### Theory

PID controllers calculate control output based on error (setpoint − process variable):

```
u(t) = Kp × e(t) + Ki × ∫e(τ)dτ + Kd × (de/dt)
```

Where:

- **Kp** (Proportional gain): Immediate response to error
- **Ki** (Integral gain): Eliminates steady-state offset
- **Kd** (Derivative gain): Dampens oscillations, anticipates change

**Tuning Guidelines (Ziegler-Nichols):**

| Type | Kp | Ki | Kd |
|---|---|---|---|
| P-only | 0.5×Ku | 0 | 0 |
| PI | 0.45×Ku | Kp/(1.2×τu) | 0 |
| PID | 0.6×Ku | Kp/(2×τu) | Kp×τu/8 |

### Example: CSTR Temperature Control

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

**Reference:** Seborg Chapter 2 — Theoretical Models

### Material Balance

For a well-mixed tank:

```
V(dc/dt) = F_in × c_in - F_out × c_out + V × r
```

Discrete form using `prev()`:

```
c[t] = c[t-1] + (Δt/V) × (F_in × c_in - F_out × c_out + V × r)
```

### Example: CSTR with Reaction

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

- Level changes by (inflow − outflow) × Δt
- Δt = 5 min = 5/60 hour (flows in m³/hr)
- Residence time = volume / flow rate
- Effluent uses `prev('level')` to avoid circular dependency

**Textbook Problem:** Similar to Seborg Example 2.1 (Liquid storage tank)

### Energy Balance

For a jacketed reactor:

```
ρVCp(dT/dt) = F_in × ρCp × (T_in - T) + (-ΔHrxn) × r × V + UA(T_jacket - T)
```

**Example: Reactor with Cooling Jacket**

```yaml
columns:
  # Heat of reaction (kW) - exothermic
  - name: heat_generation_kw
    data_type: float
    generator:
      type: constant
      value: 50.0

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
- Heat loss to jacket: −UA × (T_reactor − T_jacket), where UA = 2.0 kW/°C
- Thermal capacitance = 100 kW/(°C/min) → dividing by this gives rate of temperature change

---

## Mean-Reverting Processes

**Reference:** Seborg Chapter 5 — Transfer Function Models

Many process variables naturally revert toward a reference value. The `random_walk` generator provides four parameters for modeling this behavior without explicit PID loops.

### `mean_reversion` — PID-Like Control Without PID

Controlled variables revert toward their starting value (the `start` parameter) with strength proportional to `mean_reversion`:

```yaml
- name: pressure_bar
  generator:
    type: random_walk
    start: 3.0
    min: 2.5
    max: 3.5
    volatility: 0.1
    mean_reversion: 0.2  # Pulls toward start=3.0
```

**Use cases:** Pressure under automatic control, level with gravity drain, temperature with passive heat loss.

### `mean_reversion_to` — Tracking Dynamic Setpoints

Variables that track a changing reference (another column's value):

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
    mean_reversion_to: ambient_temp_c  # Drifts toward ambient
```

**Use cases:** Reactor temperature → cooling water temperature, storage tank → ambient, process variable → load-dependent setpoint.

### `trend` — Slow Degradation

Model fouling, catalyst deactivation, or equipment aging with a persistent drift:

```yaml
- name: heat_transfer_coeff
  generator:
    type: random_walk
    start: 500.0
    min: 200.0
    max: 600.0
    drift: -0.05      # Slow decline per timestep
    noise: 2.0
```

**Use cases:** Heat exchanger fouling (declining UA), catalyst deactivation (declining conversion), tray efficiency degradation.

### `shocks` — Process Upsets with Natural Recovery

Combine `volatility` spikes with `mean_reversion` to model upsets that naturally recover:

```yaml
- name: reactor_pressure_bar
  generator:
    type: random_walk
    start: 5.0
    min: 3.0
    max: 8.0
    volatility: 0.3
    mean_reversion: 0.15  # Natural recovery after upsets
```

The higher the `mean_reversion`, the faster the process recovers from disturbances. This naturally models self-regulating processes.

---

## Flowsheet Simulation (Cross-Entity)

**Reference:** Seborg Chapter 3 — Dynamic Modeling Principles

Real processes consist of multiple interconnected units. Odibi supports cross-entity references for flowsheet simulation.

### Theory

Material and energy streams connect process units:

```
Unit A → Stream → Unit B → Stream → Unit C
```

Unit B's inlet conditions = Unit A's outlet conditions. Odibi resolves the dependency order automatically.

### Dependency DAG

```
┌─────────────┐     effluent     ┌─────────────┐     product     ┌─────────────┐
│  Reactor     │ ──────────────→ │  Separator   │ ──────────────→ │  Heat       │
│  R101        │                 │  S101        │                 │  Exchanger  │
│              │                 │              │                 │  E101       │
│ - PID temp   │                 │ - Split      │                 │ - Cooling   │
│ - Reaction   │                 │ - Recovery   │                 │ - Fouling   │
└─────────────┘                 └─────────────┘                 └─────────────┘
```

### Example: Two Tanks in Series

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

  # Level balance
  - name: level_integrated
    generator:
      type: derived
      expression: "prev('level_integrated', 5.0) + (feed_flow + flow_from_tank_a - outlet_flow) * 0.1 if entity_id == 'Tank_B' else prev('level_integrated', 5.0) + (feed_flow - outlet_flow) * 0.1"
```

**Result:**

- Tank_A receives external feed and drains to Tank_B
- Tank_B level responds to Tank_A's outlet (realistic cascade)
- Entity dependency resolved automatically

**Textbook Problem:** Similar to Stephanopoulos Example 7.4 (Interacting tanks)

### Advanced: CSTR with Heat Recovery

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

---

## Complete Examples

### Example 1: CSTR with PI Control

**Based on:** Seborg Example 8.2 — 120 rows, 1-minute timesteps

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

**Based on:** Electrochemical energy storage principles — Coulomb counting

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
      min: -30.0   # Max discharge
      max: 30.0    # Max charge

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

### Example 3: 10-Step Recipe for Realistic Plant Data

A systematic methodology for building any process simulation, from simple tanks to full flowsheets.

#### Step 1 — Pick Granularity

Choose between **equipment-as-entity** (fewer entities, many tags each) or **tag-as-entity** (many entities, 1–3 tags each). For plant demos, equipment-as-entity is clearer.

#### Step 2 — Define Scope

```yaml
scope:
  start_time: "2026-01-01T00:00:00Z"
  timestep: "1m"       # Match to process dynamics
  row_count: 1440      # One day at 1-min intervals
  seed: 42             # Reproducibility
```

#### Step 3 — Identify Given Inputs

List the "givens" for each unit: feeds, compositions, ambient conditions, setpoints, valve states, equipment constants (volume, UA, efficiency).

#### Step 4 — Choose Base Variability

| Pattern | Generator | Use Case |
|---|---|---|
| Static within range | `range` | Feed composition, ambient temp |
| Slow drift | `random_walk` | PV-like trends, fouling |
| Fixed value | `constant` | Equipment volume, capacity |
| Categorical state | `categorical` | Operating mode, recipe |

#### Step 5 — Compute Physics in Derived Columns

Use topologically sorted derived expressions for:

- **Mass balances:** accumulation = in − out + generation
- **Energy balances:** ΔT from heat duties, UA, Cp
- **Kinetics:** Arrhenius rate constants, conversion
- **Thermo:** Antoine approximations, Raoult's law

#### Step 6 — Add Control Behavior

Layer control on top of physics:

- **Simple:** Proportional control in derived: `clamp(Kp*(SP - PV) + bias, low, high)`
- **Full PID:** Use `pid()` for integral action and anti-windup
- **Passive:** Use `mean_reversion` on `random_walk` for self-regulating processes

#### Step 7 — Add Instrumentation and Events

- **Alarms:** Boolean derived from thresholds (`reactor_temp_c > 95`)
- **Modes:** Categorical generator (`categorical: [normal, startup, shutdown]`)
- **Sensor noise:** Small `range` overlay or `ema()` smoothing

#### Step 8 — Layer Chaos for Realism

Apply after derived calculations:

```yaml
chaos:
  outlier_rate: 0.005     # Sensor spikes
  outlier_factor: 5.0
  duplicate_rate: 0.002   # Timestamp duplicates
  downtime_rate: 0.001    # Missing data windows
```

#### Step 9 — Ensure Determinism and Scale

- Use entity-based seeding for reproducibility
- Use engine-agnostic YAML; generate on Pandas for quick dev
- Scale to production with Spark when needed

#### Step 10 — Validate Physical Plausibility

- Flows ≥ 0, compositions in [0, 1], energy balances within tolerance
- Route bad rows to quarantine if demonstrating data quality gates
- Add validation tests to catch nonsensical outputs

---

## Best Practices

### Avoid Circular Dependencies

Use `prev()` for previous-row values, not current-row references to avoid circularity:

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

### Match Timestep to Process Dynamics

| Process Speed | Examples | Timestep |
|---|---|---|
| Fast | Pressure, flow | 1–10 seconds |
| Medium | Temperature, level | 1–5 minutes |
| Slow | Composition, pH | 5–30 minutes |

### PID Tuning: Start Conservative

1. **P-only:** Set Kp, Ki=0, Kd=0 → Find a stable Kp
2. **Add I:** Set Ki = Kp/10 → Eliminate steady-state offset
3. **Add D:** Set Kd = Kp/4 → Reduce oscillation

### Use Realistic Time Constants

From textbooks or plant data:

| Loop Type | Typical τ |
|---|---|
| Temperature | 5–30 minutes |
| Flow | 10–60 seconds |
| Level | 2–20 minutes |
| Composition | 5–60 minutes |

### Validate Physical Plausibility

- Check mass balance closure at every timestep
- Verify temperatures stay in realistic ranges
- Ensure compositions sum to 1.0 ± ε
- Confirm energy balance residuals are small

---

## Troubleshooting

### PID Not Converging

**Symptoms:** Process variable oscillates or drifts away from setpoint.

**Solutions:**

1. **Reduce Kp** — proportional gain too high causes oscillation
2. **Reduce Ki** — integral action too aggressive causes windup
3. **Increase dt** — timestep parameter might not match actual simulation timestep
4. **Check process dynamics** — time constant might be much slower than expected
5. **Enable anti_windup** — prevents integral term from growing unbounded when output saturates

### Integration Drifting

**Symptoms:** Level or temperature shows unbounded increase/decrease.

**Solutions:**

1. **Check mass/energy balance** — inflow should ≈ outflow at steady state
2. **Verify units** — flow in m³/hr, time in hours, etc. must be consistent
3. **Add clamping** — use `max()` and `min()` to enforce physical limits
4. **Check for missing terms** — ensure all balance equation terms are included

### Cross-Entity Reference Not Working

**Symptoms:** `AttributeError: Entity 'Tank_A' row not yet available`

**Solutions:**

1. **Check entity dependency order** — referenced entity must be generated first
2. **Don't use `Entity.prev()`** — cross-entity prev is not supported; use current-row cross-entity values
3. **Verify entity names** — must match exactly (case-sensitive)

---

## References

### Textbooks

1. **Seborg, Edgar, Mellichamp, Doyle** — *Process Dynamics and Control* (3rd/4th Ed)
   - Chapter 2: Theoretical modeling (mass/energy balances)
   - Chapter 5: Dynamic response characteristics
   - Chapter 8: Feedback controllers (PID tuning)

2. **Stephanopoulos** — *Chemical Process Control*
   - Chapter 7: Dynamic behavior of processes
   - Chapter 23: Design of feedback controllers

3. **Luyben** — *Process Modeling, Simulation, and Control*
   - Chapter 3: Mathematical models
   - Chapter 6: Controller tuning

### Online Resources

- [Control Tutorials for MATLAB](http://ctms.engin.umich.edu/) — University of Michigan
- [Process Control Lectures](https://apmonitor.com/pdc/) — BYU APMonitor

### Related Documentation

- **[Stateful Functions](stateful_functions.md)** — `prev()`, `ema()`, `pid()` reference
- **[Generators](generators.md)** — `random_walk` with mean reversion, derived expressions
- **[Advanced Features](advanced_features.md)** — Cross-entity references, scheduled events, entity overrides
