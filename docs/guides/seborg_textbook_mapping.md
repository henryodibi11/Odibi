# Seborg Textbook → Odibi Simulation Mapping

**From Theory to Realistic Plant Data: A Beginner's Guide**

---

## About This Guide

This guide maps **Process Dynamics and Control** (Seborg et al., 3rd ed.) to practical data simulation in Odibi. If you're a chemical engineer trying to remember how to translate textbook equations into realistic plant data, this is your companion.

**Who this is for:**
- ChemEs in operations/analytics who want to refresh process control concepts
- Data engineers learning process industries
- Anyone building demos that stand out from generic e-commerce examples

**How to use this:**
- Each chapter maps to Odibi features (generators, transformers, patterns)
- Beginner-friendly explanations assume you've forgotten most of your ChemE classes
- YAML examples show how equations become data pipelines
- LinkedIn-worthy content that proves you understand both ChemE AND data engineering

---

## Quick Reference: Textbook Concepts → Odibi Features

| Textbook Topic | Odibi Feature | Use Case |
|----------------|---------------|----------|
| First-order systems (τ, K) | `prev()` stateful function | Tank level/temperature dynamics |
| Second-order systems (ζ, ωn) | `prev()` + derived columns | Underdamped responses, overshoot |
| PID control | `pid()` transformer | Temperature/pressure/level control |
| Time delays (θ) | `random_walk` with lag | Analyzer delays, dead time |
| Step/ramp inputs | `range`, `categorical` | Setpoint changes, disturbances |
| PRBS signals | `random_walk` with constraints | System identification tests |
| Measurement noise | `range` (small) on sensors | Realistic transmitter data |
| Process nonlinearity | `derived` with conditional logic | Gain scheduling scenarios |
| ARX/ARMAX models | `prev()` + weighted sums | Discrete-time identification |
| Transfer functions | Combine `prev()`, `ema()`, `pid()`, `delay()` | Full closed-loop simulations |

---

## Part I: Introduction to Process Control

### Chapter 1: Introduction to Process Control

**What you forgot from school:**
- **Controlled Variable (CV):** What you want to control (temperature, level, composition)
- **Manipulated Variable (MV):** What you adjust to control it (valve position, heater power)
- **Disturbance Variable (DV):** What messes things up (feed composition changes, ambient temperature)

**Why this matters for data:**
You need realistic CV/MV/DV relationships to build convincing demos. A tank level that doesn't respond to inlet flow changes is obviously fake.

**Odibi Implementation:**

```yaml
# Simple mixing tank: CV = outlet composition, MV = acid flow, DV = feed composition
simulation:
  entities: ["tank_001"]
  start_time: "2024-01-01 00:00:00"
  timestep: "1min"
  row_count: 1440  # 1 day
  seed: 42

  columns:
    # Disturbance: Feed composition varying naturally
    - name: feed_composition_pct
      data_type: float
      generator:
        type: random_walk
        start: 5.0
        min: 4.0
        max: 6.0
        step_size: 0.1

    # Manipulated Variable: Operator adjusting acid flow
    - name: acid_flow_gpm
      data_type: float
      generator:
        type: random_walk
        start: 10.0
        min: 5.0
        max: 15.0
        step_size: 0.5

    # Controlled Variable: Tank composition (mass balance)
    - name: tank_composition_pct
      data_type: float
      generator:
        type: derived
        expression: |
          # Simple mass balance (assuming perfect mixing)
          prev('tank_composition_pct', 5.0) +
          0.1 * (feed_composition_pct - prev('tank_composition_pct', 5.0)) +
          0.05 * (acid_flow_gpm - 10.0)
```

**LinkedIn Post Idea:**
"🎯 ChemE 101: Building a mixing tank simulator from scratch. Notice how the CV lags the MV - that's first-order dynamics in action. No Aspen needed, just data pipelines."

---

### Chapter 2: Theoretical Models of Chemical Processes

**What you forgot:**
- **Degrees of Freedom (DoF):** DoF = Number of Variables − Number of Equations
- If DoF > 0, you need to specify inputs
- If DoF = 0, the system is fully determined (steady-state)
- If DoF < 0, you over-specified (inconsistent equations)

**Why this matters for data:**
When simulating, you must decide which variables are "given" (inputs) vs "calculated" (outputs). Getting DoF wrong means your simulation either crashes or produces garbage.

**Odibi Implementation Pattern:**

For a CSTR (Continuous Stirred Tank Reactor):
- **Variables:** T (temperature), C (concentration), F (flow), V (volume), Q (heat input)
- **Equations:** Mass balance, energy balance
- **DoF analysis:** 5 variables − 2 equations = 3 DoF → specify 3 inputs (F, Q, inlet T or C)

```yaml
columns:
  # Given inputs (3 DoF)
  - name: feed_flow_gpm
    data_type: float
    generator:
      type: constant
      value: 100.0

  - name: feed_temp_f
    data_type: float
    generator:
      type: random_walk
      start: 70.0
      min: 65.0
      max: 75.0
      step_size: 0.5

  - name: heat_input_mbtu_hr
    data_type: float
    generator:
      type: range
      min: 5.0
      max: 15.0

  # Calculated from mass/energy balance (0 DoF remaining)
  - name: reactor_temp_f
    data_type: float
    generator:
      type: derived
      expression: |
        # Simplified energy balance (first-order approximation)
        tau = 10.0  # minutes (residence time = V/F)
        prev_temp = prev('reactor_temp_f', 70.0)
        prev_temp + (1.0/tau) * (feed_temp_f - prev_temp + 0.5*heat_input_mbtu_hr)
```

**Key Takeaway:**
Always count your DoF before building a simulation. If you try to "derive" all variables, Odibi will complain about circular dependencies.

---

## Part II: Dynamic Behavior of Processes

### Chapter 4: Transfer Function Models

**What you forgot:**
A **transfer function** relates output to input in the Laplace domain:

```
G(s) = Y(s) / U(s)
```

The most common form in process control is **First-Order Plus Time Delay (FOPTD)**:

```
G(s) = K e^(-θs) / (τs + 1)
```

Where:
- **K** = process gain (steady-state output change per unit input change)
- **τ** = time constant (how fast it responds - larger τ = slower)
- **θ** = time delay (dead time before any response starts)

**Why this matters for data:**
Almost every process unit (tank, heat exchanger, reactor) can be approximated as FOPTD. If you can simulate FOPTD, you can simulate 80% of real plant behavior.

**Odibi Implementation:**

```yaml
# FOPTD: Tank temperature responding to heater power
# K = 2.0 °F/%, τ = 5 min, θ = 1 min

columns:
  # Input: Heater power (step change at t=10)
  - name: heater_pct
    data_type: float
    generator:
      type: derived
      expression: "50.0 if timestamp < timestamp.shift(10, 'min') else 60.0"

  # Delayed input (θ = 1 min)
  - name: heater_delayed
    data_type: float
    generator:
      type: derived
      expression: "prev('heater_pct', 50.0)"

  # Output: Temperature with first-order lag
  - name: tank_temp_f
    data_type: float
    generator:
      type: derived
      expression: |
        # Discrete approximation: y[k] = y[k-1] + (Δt/τ)(K*u[k] - y[k-1])
        K = 2.0
        tau = 5.0
        dt = 1.0  # minutes
        u = heater_delayed - 50.0  # deviation variable
        y_prev = prev('tank_temp_f', 100.0) - 100.0  # deviation from initial
        y_new = y_prev + (dt/tau) * (K*u - y_prev)
        100.0 + y_new  # convert back to absolute
```

**Validation:**
For a 10% step in heater:
- Initial temp: 100°F
- Final temp: 100 + K×10 = 120°F
- Time to 63.2%: τ = 5 min
- Actual response starts at t = θ = 1 min

**LinkedIn Post Idea:**
"📈 Every tank in your plant is secretly a transfer function. Here's how to simulate FOPTD in data pipelines - K, τ, and θ explained with zero Laplace math."

---

### Chapter 5: Dynamic Behavior of First-Order and Second-Order Processes

#### First-Order Systems: The Building Block

**What you forgot:**
The step response of a first-order system is:

```
y(t) = K(1 - e^(-t/τ))    (for unit step from zero)
```

**Key metrics:**
- **Time constant (τ):** Time to reach 63.2% of final value
- **Settling time:** ≈ 4τ (to reach 98% of final value)
- **Rise time:** ≈ 2.2τ (10% to 90%)

**Odibi Implementation:**

```yaml
# Generate step responses for different time constants
simulation:
  entities: ["slow_tank", "medium_tank", "fast_tank"]
  start_time: "2024-01-01 00:00:00"
  timestep: "10sec"
  row_count: 360  # 1 hour
  seed: 42

  columns:
    # Step input at t=5min
    - name: input_step
      data_type: float
      generator:
        type: derived
        expression: "0.0 if timestamp < timestamp.shift(5, 'min') else 1.0"

    # Slow response (τ = 10 min)
    - name: slow_response
      data_type: float
      generator:
        type: derived
        expression: |
          K = 2.0
          tau = 10.0  # minutes
          dt = 10.0/60.0  # 10 seconds in minutes
          u = input_step
          y_prev = prev('slow_response', 0.0)
          y_prev + (dt/tau) * (K*u - y_prev)

    # Fast response (τ = 1 min)
    - name: fast_response
      data_type: float
      generator:
        type: derived
        expression: |
          K = 2.0
          tau = 1.0
          dt = 10.0/60.0
          u = input_step
          y_prev = prev('fast_response', 0.0)
          y_prev + (dt/tau) * (K*u - y_prev)
```

**Validation Test:**
```python
# After running simulation, check metrics
df = pipeline.run()

# Find settling time (time to reach 98% of final value)
final_value = df['slow_response'].iloc[-1]
threshold = 0.98 * final_value
settling_idx = df[df['slow_response'] >= threshold].index[0]
settling_time = (settling_idx - step_idx) * 10/60  # in minutes

assert abs(settling_time - 4*10) < 1.0  # Should be ~40 min (4τ)
```

#### Second-Order Systems: Overshoot and Oscillation

**What you forgot:**
Second-order systems are described by:

```
G(s) = ωn² / (s² + 2ζωn·s + ωn²)
```

Where:
- **ωn** = natural frequency (rad/time)
- **ζ** = damping ratio:
  - ζ > 1: overdamped (slow, no overshoot)
  - ζ = 1: critically damped (fastest without overshoot)
  - 0 < ζ < 1: underdamped (fast, with overshoot)
  - ζ = 0: undamped (pure oscillation)

**Key metrics for underdamped (ζ < 1):**
- **Overshoot:** Mp ≈ exp(-πζ / √(1-ζ²))
- **Peak time:** tp ≈ π / (ωn√(1-ζ²))
- **Settling time:** ts ≈ 4 / (ζωn)

**Odibi Implementation:**

```yaml
# U-tube manometer or pressure gauge with damping
columns:
  # Step pressure change
  - name: pressure_step_psi
    data_type: float
    generator:
      type: derived
      expression: "0.0 if timestamp < timestamp.shift(2, 'min') else 10.0"

  # Second-order response (ζ = 0.3, ωn = 1.0 rad/min)
  # Using state-space: dx1/dt = x2, dx2/dt = -ωn²x1 - 2ζωn·x2 + ωn²u
  - name: velocity
    data_type: float
    generator:
      type: derived
      expression: |
        zeta = 0.3
        wn = 1.0
        dt = 10.0/60.0  # 10 seconds in minutes
        u = pressure_step_psi
        x1 = prev('pressure_reading_psi', 0.0)
        x2 = prev('velocity', 0.0)
        x2 + dt * (-wn*wn*x1 - 2*zeta*wn*x2 + wn*wn*u)

  - name: pressure_reading_psi
    data_type: float
    generator:
      type: derived
      expression: |
        dt = 10.0/60.0
        x2 = velocity
        prev('pressure_reading_psi', 0.0) + dt * x2
```

**Expected behavior:**
- Input: 10 psi step
- Overshoot: ≈ exp(-π×0.3/√(1-0.3²)) ≈ 37%
- Peak: 10 × 1.37 = 13.7 psi at tp ≈ 3.3 min
- Settles to 10 psi after ≈ 13 min

**LinkedIn Post Idea:**
"🎢 Second-order systems explained: Why your pressure gauge overshoots and oscillates. Here's the damping ratio (ζ) cheatsheet every ChemE should remember."

---

### Chapter 7: Development of Empirical Models from Process Data

**What you forgot:**
You don't always have a physics model. Instead, you:
1. Apply test signals (steps, PRBS)
2. Collect input/output data
3. Fit an empirical model (FOPTD, ARX, ARMAX)
4. Validate with cross-validation

**ARX Model (AutoRegressive with eXogenous input):**
```
y[k] = a₁y[k-1] + a₂y[k-2] + ... + b₁u[k-1] + b₂u[k-2] + ... + e[k]
```

**Why this matters:**
This is how real plant identification works. You can simulate "true" plant data, add noise, then test if your ID method recovers the parameters.

**Odibi Implementation:**

```yaml
# Generate FOPTD data, then fit ARX to it
simulation:
  entities: ["tank_001"]
  start_time: "2024-01-01 00:00:00"
  timestep: "1min"
  row_count: 1440
  seed: 42

  columns:
    # PRBS input for persistent excitation
    - name: input_prbs
      data_type: float
      generator:
        type: random_walk
        start: 0.0
        min: -1.0
        max: 1.0
        step_size: 0.5

    # True FOPTD response (K=2, τ=5, θ=1)
    - name: output_true
      data_type: float
      generator:
        type: derived
        expression: |
          K = 2.0
          tau = 5.0
          dt = 1.0
          u = prev('input_prbs', 0.0)  # 1-min delay
          y_prev = prev('output_true', 0.0)
          y_prev + (dt/tau) * (K*u - y_prev)

    # Measured output (with noise)
    - name: output_measured
      data_type: float
      generator:
        type: derived
        expression: "output_true + noise"

    - name: noise
      data_type: float
      generator:
        type: range
        min: -0.1
        max: 0.1

# After simulation, fit ARX in Python:
# from statsmodels.tsa.arima.model import ARIMA
# model = ARIMA(output_measured, exog=input_prbs, order=(1,0,0))
# fitted = model.fit()
# print(fitted.params)  # Should recover a1 ≈ exp(-dt/tau) ≈ 0.82, b1 ≈ K(1-a1) ≈ 0.36
```

**Validation:**
Compare one-step-ahead predictions vs actual. If RMSE is close to noise level (0.1), the model is good.

---

## Part III: Feedback and Feedforward Control

### Chapter 8: Feedback Controllers (PID)

**What you forgot:**
The PID controller is the workhorse of process control:

```
u(t) = Kc[e(t) + (1/τI)∫e dt + τD(de/dt)]
```

Where:
- **Kc** = proportional gain (how aggressive)
- **τI** = integral time (eliminates offset)
- **τD** = derivative time (anticipates changes, fights noise)
- **e(t)** = error = setpoint − measurement

**Odibi has a built-in `pid()` transformer!**

**Implementation:**

```yaml
# Tank level control with PID
columns:
  # Setpoint: Target level
  - name: level_setpoint_ft
    data_type: float
    generator:
      type: derived
      expression: "10.0 if timestamp < timestamp.shift(30, 'min') else 12.0"

  # Process variable: Tank level (integrating process)
  - name: level_pv_ft
    data_type: float
    generator:
      type: derived
      expression: |
        # Integrating process: dL/dt = (inflow - outflow) / Area
        area = 100.0  # ft²
        outflow = 50.0  # gpm (constant)
        inflow = valve_position  # gpm
        dt = 1.0  # minutes
        dL = (inflow - outflow) / area * 7.48  # gpm to ft³/min
        prev('level_pv_ft', 10.0) + dt * dL

  # Controller output: Inlet valve position
  - name: valve_position
    data_type: float
    generator:
      type: derived
      expression: |
        pid(
          pv='level_pv_ft',
          sp='level_setpoint_ft',
          Kp=5.0,
          Ki=0.1,
          Kd=0.0,
          dt=1.0,
          output_min=0.0,
          output_max=100.0
        )
```

**Expected behavior:**
- P-only (Ki=0): offset remains (level settles below setpoint)
- PI (Kd=0): offset eliminated, some overshoot
- PID: faster response, derivative fights overshoot (but amplifies noise)

**Tuning cheatsheet:**
- Too much Kc: oscillation
- Too little Kc: slow response
- Too much Ki: overshoot/oscillation
- Too little Ki: slow offset elimination
- Too much Kd: noise amplification
- Too little Kd: sluggish on setpoint changes

**LinkedIn Post Idea:**
"🎮 PID control demystified: The 3 knobs every plant engineer adjusts. Here's what they actually do, with live simulation data."

---

### Chapter 12: PID Controller Tuning

**What you forgot:**
There are many tuning methods. The most common:

**1. Ziegler-Nichols (Ultimate Gain Method):**
- Increase Kc until sustained oscillation
- Record Ku (ultimate gain) and Pu (period)
- Use tuning rules:
  - P: Kc = 0.5Ku
  - PI: Kc = 0.45Ku, τI = Pu/1.2
  - PID: Kc = 0.6Ku, τI = Pu/2, τD = Pu/8

**2. Direct Synthesis / IMC:**
- Fit FOPTD model (K, τ, θ)
- Choose closed-loop time constant λ (tuning parameter)
- PI tuning: Kc = τ/(K(λ+θ)), τI = τ

**Odibi Implementation:**

```yaml
# Step 1: Open-loop step test to get K, τ, θ
simulation:
  columns:
    - name: input_step
      data_type: float
      generator:
        type: derived
        expression: "0.0 if timestamp < timestamp.shift(5, 'min') else 1.0"

    - name: output_response
      data_type: float
      generator:
        type: derived
        expression: |
          # Your actual process
          K = 2.0
          tau = 10.0
          theta = 2.0
          dt = 1.0
          u = prev('input_step', 0.0, lag=int(theta))  # delay
          y_prev = prev('output_response', 0.0)
          y_prev + (dt/tau) * (K*u - y_prev)

# Step 2: Fit K, τ, θ from step response
# (Use curve_fit or graphical method: K = Δy/Δu, τ from 63.2% time, θ from start)

# Step 3: Calculate IMC tuning
# λ = tau  # aggressive
# Kc = tau / (K * (lambda + theta)) = 10 / (2 * (10 + 2)) = 0.42
# Ti = tau = 10

# Step 4: Implement PID with calculated tuning
columns:
  - name: controller_output
    data_type: float
    generator:
      type: derived
      expression: |
        pid(
          pv='output_response',
          sp='setpoint',
          Kp=0.42,
          Ki=0.042,  # Kp/Ti
          Kd=0.0,
          dt=1.0,
          output_min=0.0,
          output_max=10.0
        )
```

**Validation:**
Compare tuning methods (ZN vs IMC vs manual) by running same disturbance/setpoint changes and measuring:
- IAE (Integral Absolute Error)
- ISE (Integral Square Error)
- Overshoot %
- Settling time

---

### Chapter 15: Feedforward and Ratio Control

**What you forgot:**
- **Feedback (PID):** Reacts to errors after they happen (slow)
- **Feedforward:** Anticipates disturbances before they affect the process (fast)

**Example:** Distillation column
- Disturbance: feed flow rate changes
- Feedback: measure top composition, adjust reflux (slow, 5-10 min lag)
- Feedforward: measure feed flow, immediately adjust reflux proportionally (instant)

**Ideal feedforward:**
```
Gff(s) = -Gd(s) / Gp(s)
```

Where:
- Gd = disturbance-to-output transfer function
- Gp = manipulated-to-output transfer function

**Odibi Implementation:**

```yaml
# Ratio control: Maintain fixed ratio of two flows
columns:
  # Disturbance: Primary flow (wild)
  - name: feed_flow_gpm
    data_type: float
    generator:
      type: random_walk
      start: 100.0
      min: 80.0
      max: 120.0
      step_size: 2.0

  # Feedforward: Adjust secondary flow to maintain ratio
  - name: additive_flow_setpoint_gpm
    data_type: float
    generator:
      type: derived
      expression: "feed_flow_gpm * 0.15"  # 15% ratio

  # Actual secondary flow (with PI trim)
  - name: additive_flow_pv_gpm
    data_type: float
    generator:
      type: derived
      expression: |
        pid(
          pv='additive_flow_pv_gpm',
          sp='additive_flow_setpoint_gpm',
          Kp=0.5,
          Ki=0.05,
          Kd=0.0,
          dt=1.0,
          output_min=0.0,
          output_max=50.0
        )
```

**Key point:**
Feedforward eliminates 80-90% of disturbance immediately. Feedback (PI trim) handles model mismatch and unmeasured disturbances.

---

## Part IV: Advanced Process Control

### Chapter 16: Enhanced Single-Loop Control Strategies

#### Cascade Control: Fast Inner Loop, Slow Outer Loop

**What you forgot:**
Use cascade when:
- Secondary (fast) measurement is available
- Disturbances affect the intermediate variable
- Inner loop can reject disturbances before they affect outer loop

**Example:** Reactor temperature control
- Primary (outer): reactor temperature (slow, 10 min)
- Secondary (inner): jacket temperature (fast, 1 min)
- Disturbance: jacket inlet temperature varies

**Odibi Implementation:**

```yaml
columns:
  # Primary setpoint: Desired reactor temperature
  - name: reactor_temp_sp_f
    data_type: float
    generator:
      type: constant
      value: 180.0

  # Primary PID: Reactor temperature → Jacket setpoint
  - name: jacket_temp_sp_f
    data_type: float
    generator:
      type: derived
      expression: |
        pid(
          pv='reactor_temp_pv_f',
          sp='reactor_temp_sp_f',
          Kp=2.0,
          Ki=0.1,
          Kd=0.0,
          dt=1.0,
          output_min=100.0,
          output_max=200.0
        )

  # Secondary PID: Jacket temperature → Valve position
  - name: steam_valve_pct
    data_type: float
    generator:
      type: derived
      expression: |
        pid(
          pv='jacket_temp_pv_f',
          sp='jacket_temp_sp_f',
          Kp=5.0,
          Ki=0.5,
          Kd=0.0,
          dt=1.0,
          output_min=0.0,
          output_max=100.0
        )

  # Fast process: Jacket temperature
  - name: jacket_temp_pv_f
    data_type: float
    generator:
      type: derived
      expression: |
        # First-order, fast (τ = 1 min)
        K = 1.0
        tau = 1.0
        dt = 1.0
        u = steam_valve_pct
        y_prev = prev('jacket_temp_pv_f', 120.0)
        y_prev + (dt/tau) * (K*u + 100.0 - y_prev)

  # Slow process: Reactor temperature
  - name: reactor_temp_pv_f
    data_type: float
    generator:
      type: derived
      expression: |
        # First-order, slow (τ = 10 min)
        K = 0.5
        tau = 10.0
        dt = 1.0
        u = jacket_temp_pv_f - 150.0  # deviation
        y_prev = prev('reactor_temp_pv_f', 170.0)
        y_prev + (dt/tau) * (K*u - (y_prev - 170.0))
```

**Tuning rule:** Tune inner loop first (aggressive), then outer loop (slower).

---

### Chapter 20: Model Predictive Control (MPC)

**What you forgot:**
MPC is the "fancy" controller that:
- Predicts future behavior using a model
- Handles constraints explicitly (min/max on MVs and CVs)
- Optimizes a cost function over a future horizon
- Used in refineries, polymerization, power plants

**Basic idea:**
```
min J = Σ ||ŷ - r||² + Σ ||Δu||²
subject to: umin ≤ u ≤ umax, ymin ≤ y ≤ ymax
```

**Why this matters:**
You can simulate MPC-controlled processes to show constraint handling and optimal steady-state.

**Odibi Implementation (Simplified MPC-like logic):**

```yaml
# Constrained level control: Keep level between 8-12 ft, minimize flow changes
columns:
  - name: level_setpoint_ft
    data_type: float
    generator:
      type: constant
      value: 10.0

  - name: level_pv_ft
    data_type: float
    generator:
      type: derived
      expression: |
        # Integrating process
        area = 100.0
        inflow = valve_position
        outflow = 50.0
        dt = 1.0
        dL = (inflow - outflow) / area * 7.48
        clamp(prev('level_pv_ft', 10.0) + dt * dL, 0.0, 15.0)

  # MPC-like controller (simplified: PID with move suppression)
  - name: valve_position
    data_type: float
    generator:
      type: derived
      expression: |
        # PID with rate-of-change penalty (move suppression)
        error = level_setpoint_ft - level_pv_ft
        integral = prev('integral', 0.0) + error
        derivative = error - prev('error', 0.0)

        Kp = 3.0
        Ki = 0.05
        Kd = 0.0
        move_penalty = 0.1

        # Raw PID
        u_raw = Kp*error + Ki*integral + Kd*derivative

        # Constrain move size (MPC move suppression)
        u_prev = prev('valve_position', 50.0)
        delta_max = 5.0  # max 5 gpm change per minute
        u_constrained = clamp(u_raw, u_prev - delta_max, u_prev + delta_max)

        # Constrain output
        clamp(u_constrained, 0.0, 100.0)

  - name: error
    data_type: float
    generator:
      type: derived
      expression: "level_setpoint_ft - level_pv_ft"

  - name: integral
    data_type: float
    generator:
      type: derived
      expression: "prev('integral', 0.0) + error"
```

**Key difference from PID:**
MPC explicitly limits rate-of-change (Δu) to avoid aggressive valve movements. PID only limits output magnitude.

---

## Practical Simulation Recipes

### Recipe 1: Full Closed-Loop Temperature Control

**Scenario:** Tank with heating coil, ambient losses, PID controller

```yaml
simulation:
  entities: ["tank_TK101"]
  start_time: "2024-01-01 00:00:00"
  timestep: "1min"
  row_count: 480  # 8 hours
  seed: 42

  columns:
    # Setpoint schedule
    - name: temp_setpoint_f
      data_type: float
      generator:
        type: derived
        expression: |
          if timestamp.hour < 2:
              150.0
          elif timestamp.hour < 4:
              180.0
          else:
              160.0

    # Disturbance: Ambient temperature
    - name: ambient_temp_f
      data_type: float
      generator:
        type: random_walk
        start: 70.0
        min: 65.0
        max: 75.0
        step_size: 0.5

    # Controller
    - name: heater_output_pct
      data_type: float
      generator:
        type: derived
        expression: |
          pid(
            pv='tank_temp_pv_f',
            sp='temp_setpoint_f',
            Kp=2.0,
            Ki=0.1,
            Kd=0.5,
            dt=1.0,
            output_min=0.0,
            output_max=100.0
          )

    # Process: Energy balance
    - name: tank_temp_pv_f
      data_type: float
      generator:
        type: derived
        expression: |
          # dT/dt = (Q_heater - Q_loss) / (m*cp)
          m_cp = 1000.0  # thermal mass (BTU/°F)
          UA = 50.0  # heat loss coefficient (BTU/min/°F)
          Q_max = 500.0  # max heater power (BTU/min)

          T_prev = prev('tank_temp_pv_f', 150.0)
          Q_heater = heater_output_pct / 100.0 * Q_max
          Q_loss = UA * (T_prev - ambient_temp_f)

          dt = 1.0
          dT = (Q_heater - Q_loss) / m_cp
          T_prev + dt * dT

    # Sensor noise
    - name: temp_sensor_noise_f
      data_type: float
      generator:
        type: range
        min: -0.5
        max: 0.5

    - name: tank_temp_measured_f
      data_type: float
      generator:
        type: derived
        expression: "tank_temp_pv_f + temp_sensor_noise_f"
```

**What this demonstrates:**
- Setpoint tracking
- Disturbance rejection (ambient temperature)
- Controller tuning trade-offs (Kp/Ki/Kd)
- Sensor noise effects

---

### Recipe 2: System Identification Test

**Scenario:** PRBS input test to identify unknown process

```yaml
simulation:
  entities: ["mystery_process"]
  start_time: "2024-01-01 00:00:00"
  timestep: "30sec"
  row_count: 1200  # 10 hours
  seed: 42

  columns:
    # PRBS input (Pseudo-Random Binary Sequence)
    - name: input_prbs
      data_type: float
      generator:
        type: random
        distribution: choice
        choices: [-1.0, -0.5, 0.0, 0.5, 1.0]

    # True process (FOPTD with nonlinearity)
    - name: output_true
      data_type: float
      generator:
        type: derived
        expression: |
          # K varies with operating point (gain scheduling)
          u = prev('input_prbs', 0.0)
          if u > 0.5:
              K = 3.0  # high gain region
          else:
              K = 2.0  # low gain region

          tau = 5.0
          dt = 30.0/60.0  # 30 sec in minutes
          y_prev = prev('output_true', 0.0)
          y_prev + (dt/tau) * (K*u - y_prev)

    # Measurement with realistic noise
    - name: output_measured
      data_type: float
      generator:
        type: derived
        expression: "output_true + noise + bias_drift"

    - name: noise
      data_type: float
      generator:
        type: range
        min: -0.2
        max: 0.2

    - name: bias_drift
      data_type: float
      generator:
        type: random_walk
        start: 0.0
        min: -0.1
        max: 0.1
        step_size: 0.01
```

**Analysis in Python:**
```python
# Fit ARX model
from statsmodels.tsa.arima.model import ARIMA

df = pipeline.run()
model = ARIMA(df['output_measured'], exog=df['input_prbs'], order=(2,0,1))
fitted = model.fit()

# Validate with cross-validation
train = df.iloc[:800]
test = df.iloc[800:]
# ... fit on train, predict on test, compute RMSE
```

---

## Beginner Concepts Explained

### What is "Lag" vs "Delay"?

**Lag (τ):** System takes time to respond because of physical capacity (thermal mass, tank volume)
- Example: Tank temperature changes slowly because water has high heat capacity
- Math: First-order differential equation

**Delay (θ):** Signal takes time to travel or process
- Example: Composition analyzer sits 50 feet from reactor → 2-minute dead time
- Math: Pure time shift

**In Odibi:**
```yaml
# Lag: use prev() with exponential weighting
- name: lagged_value
  data_type: float
  generator:
    type: derived
    expression: "ema('raw_value', alpha=0.2, default=0.0)"

# Delay: use prev() with offset
- name: delayed_value
  data_type: float
  generator:
    type: derived
    expression: "prev('raw_value', default=0.0, lag=5)"  # 5 timesteps ago
```

---

### What is "Overshoot"?

**Overshoot:** When a controlled variable exceeds the setpoint before settling

**Causes:**
- High controller gain (Kc too large)
- Underdamped second-order process (ζ < 1)
- Large dead time (θ) relative to time constant (τ)

**How to reduce:**
- Decrease Kc
- Increase τI (slower integral)
- Add derivative action (τD)
- Use 2-degree-of-freedom PID (setpoint weighting)

**In Odibi:**
```yaml
# Standard PID: 30% overshoot
- name: aggressive_control
  data_type: float
  generator:
    type: derived
    expression: "pid(pv='temp', sp='setpoint', Kp=5.0, Ki=0.5, Kd=0.0, dt=1.0)"

# Conservative PID: 5% overshoot
- name: conservative_control
  data_type: float
  generator:
    type: derived
    expression: "pid(pv='temp', sp='setpoint', Kp=2.0, Ki=0.1, Kd=0.5, dt=1.0)"
```

---

### What is "Integral Windup"?

**Problem:** When MV is saturated (0% or 100%), error keeps accumulating in integral term → huge overshoot when constraint releases

**Example:**
1. Setpoint jumps from 100°F to 200°F
2. Heater goes to 100% (saturated)
3. Error is still 50°F, so integral keeps growing
4. When temperature reaches 200°F, integral is huge
5. Heater stays at 100% way too long → overshoot to 220°F

**Solution:** Anti-windup (stop integrating when saturated)

**Odibi's `pid()` has built-in anti-windup!**

```yaml
- name: safe_control
  data_type: float
  generator:
    type: derived
    expression: |
      pid(
        pv='level',
        sp='setpoint',
        Kp=3.0,
        Ki=0.2,
        Kd=0.0,
        dt=1.0,
        output_min=0.0,    # Anti-windup kicks in here
        output_max=100.0   # and here
      )
```

---

## LinkedIn Content Series Outline

### Post Series: "ChemE × Data Engineering"

**Post 1: The Missing Link**
"Why are all data demos about e-commerce? Let me show you process control simulation with real ChemE concepts. Starting with: What is a transfer function, really?"
- Show FOPTD tank simulation
- Link to this guide

**Post 2: First-Order Systems**
"Every tank in your plant is secretly a first-order system. Here's how to simulate realistic level, temperature, and flow data."
- K, τ, θ explained with visuals
- YAML example
- Validation metrics

**Post 3: PID Control Demystified**
"The 3 knobs every plant engineer adjusts: Proportional, Integral, Derivative. Here's what they actually do."
- PID simulation
- Tuning trade-offs
- Overshoot examples

**Post 4: Why Your Controller Oscillates**
"Spoiler: It's either too much gain or too much dead time. Here's how to diagnose with simulated data."
- Stability examples
- Ziegler-Nichols gone wrong
- Bode plots

**Post 5: System Identification**
"You don't have a model? No problem. Here's how to recover transfer functions from PRBS test data."
- ARX fitting
- Cross-validation
- Model mismatch

**Post 6: Cascade Control**
"When one loop isn't enough: Fast inner loop + slow outer loop = disturbance rejection magic."
- Reactor temperature example
- Performance comparison

**Post 7: Feedforward: The Secret Weapon**
"Feedback is reactive. Feedforward is proactive. Here's how to anticipate disturbances before they hit your process."
- Ratio control
- FF+FB combination

**Post 8: MPC for the Masses**
"Model Predictive Control isn't just for refineries anymore. Here's a simplified version you can actually understand."
- Constraint handling
- Move suppression
- Cost function trade-offs

---

## Appendix: Odibi Feature → Textbook Concept Mapping

| Odibi Feature | Textbook Concept | Chapter |
|---------------|------------------|---------|
| `prev()` | Euler integration, discrete-time models | 4, 5, 17 |
| `ema()` | First-order filter, sensor smoothing | 5, 9 |
| `pid()` | PID controller (parallel form) | 8, 12 |
| `random_walk` | PRBS, disturbances, sensor drift | 7, 21 |
| `range` | Measurement noise, uncertainties | 9 |
| `derived` | State-space equations, ODEs | 2, 4 |
| `constant` | Equipment parameters, setpoints | 2 |
| `categorical` | Mode/grade changes, discrete states | 22 |
| Simulation `seed` | Reproducibility for experiments | 7 |
| Incremental loading | Online data collection, streaming | 17 |

---

## Next Steps

1. **Pick a chapter** from the outline above
2. **Build the simulation** using Odibi YAML
3. **Validate** against textbook equations/plots
4. **Write a LinkedIn post** showing your unique ChemE+DE skills
5. **Repeat** for 8-12 chapters → you now have a portfolio

**Remember:**
- Start simple (FOPTD, single-loop PID)
- Add complexity gradually (cascade, MIMO, MPC)
- Always validate against theory
- Show your work on LinkedIn

---

**Questions? Found an error?**
Open an issue or PR: [github.com/henryodibi11/Odibi](https://github.com/henryodibi11/Odibi)

**Want more?**
- [Process Simulation Guide](process_simulation_guide.md) - Deep dive on stateful functions
- [Chemical Engineering Simulation Guide](chemical_engineering_simulation_guide.md) - Advanced topics
- [Thermodynamics Transformers](thermodynamics.md) - Steam tables, psychrometrics
- [Reference: Simulation Generators](../reference/simulation_generators.md) - All generators explained
