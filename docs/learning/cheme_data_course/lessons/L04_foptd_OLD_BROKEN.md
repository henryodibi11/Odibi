# L04: FOPTD Transfer Functions

**Prerequisites:** L00-L03 | **Effort:** 45 min | **Seborg:** Chapter 4

---

## Learning Objectives

By the end of this lesson you will:

1. ✅ Understand FOPTD (First-Order Plus Time Delay) transfer function parameters
2. ✅ Implement time delay using `prev()` with lag parameter
3. ✅ Parameterize YAML configurations for reusability
4. ✅ Validate step response characteristics (gain, time constant, delay)
5. ✅ Use scenario flags for comparing different process configurations

---

## Theory Recap: FOPTD Transfer Functions (Seborg Ch. 4)

**The FOPTD model** is the most important transfer function in process control:

```
G(s) = K × e^(-θs) / (τs + 1)
```

**Parameters:**
- **K** = Process gain (steady-state change in output per unit input)
  - Example: K = 2.0 means 1% heater change → 2°F temperature change
- **τ** = Time constant (how fast the process responds, in minutes)
  - 63.2% of final value reached at t = τ
  - 98% reached at t ≈ 4τ (settling time)
- **θ** = Dead time / time delay (lag before ANY response starts)
  - Example: Analyzer 50 ft from reactor → 2-minute sample transport delay

**Why FOPTD?**
- 80% of process units can be approximated as FOPTD
- Simple to identify from step tests
- Easy to tune controllers with (IMC, Direct Synthesis methods)

**Discrete-time implementation:**
```python
# At each timestep:
u_delayed = prev('input', default, lag=θ_steps)  # Apply delay
y[k] = y[k-1] + (Δt/τ) × (K × u_delayed - y[k-1])  # First-order lag
```

---

## Odibi Hands-On

### **Example 1: Tank Temperature with FOPTD**

**Process:** Tank with heater, first-order thermal response plus transport delay

**Parameters:**
- K = 0.5 °F/% (heater power to temperature gain)
- τ = 10 minutes (thermal time constant)
- θ = 2 minutes (temperature sensor delay)

```yaml
# tank_foptd.yaml
pipeline:
  name: tank_temperature_foptd
  description: FOPTD model - K=0.5, tau=10, theta=2

connections:
  output:
    type: local
    path: ./output/tank_foptd.parquet
    format: parquet

nodes:
  - name: generate_foptd_data
    output: output

    simulation:
      entities: ["TK-301"]
      start_time: "2024-01-01 00:00:00"
      timestep: "1min"
      row_count: 240  # 4 hours
      seed: 42

      columns:
        # ─────────────────────────────────
        # INPUT: Heater power (step change)
        # ─────────────────────────────────
        - name: heater_pct
          data_type: float
          unit: percent
          description: "Heater power (step: 50% → 60% at t=30min)"
          generator:
            type: derived
            expression: |
              # Step change after 30 minutes
              50.0 if timestamp < timestamp.shift(30, 'minutes') else 60.0

        # ─────────────────────────────────
        # TIME DELAY (θ = 2 minutes)
        # ─────────────────────────────────
        - name: heater_delayed
          data_type: float
          unit: percent
          description: "Heater power delayed by 2 minutes"
          generator:
            type: derived
            expression: "prev('heater_pct', 50.0, lag=2)"

        # ─────────────────────────────────
        # FIRST-ORDER LAG (K=0.5, τ=10)
        # ─────────────────────────────────
        - name: tank_temp_true_f
          data_type: float
          unit: degF
          description: "True tank temperature (no noise)"
          generator:
            type: derived
            expression: |
              # FOPTD parameters
              K = 0.5      # degF/%
              tau = 10.0   # minutes
              dt = 1.0     # timestep in minutes

              # Input (delayed)
              u = heater_delayed - 50.0  # Deviation variable

              # Previous output (deviation)
              y_prev = prev('tank_temp_true_f', 100.0) - 100.0

              # First-order update: y[k] = y[k-1] + (dt/tau)(K*u - y[k-1])
              y_new = y_prev + (dt / tau) * (K * u - y_prev)

              # Convert back to absolute temperature
              100.0 + y_new

        # ─────────────────────────────────
        # SENSOR (with noise)
        # ─────────────────────────────────
        - name: tank_temp_measured_f
          data_type: float
          unit: degF
          generator:
            type: derived
            expression: "tank_temp_true_f + sensor_noise"

        - name: sensor_noise
          data_type: float
          unit: degF
          generator:
            type: range
            min: -0.3
            max: 0.3

        # ─────────────────────────────────
        # VALIDATION METRICS
        # ─────────────────────────────────
        - name: steady_state_gain
          data_type: float
          description: "Expected: K = 0.5 degF/%"
          generator:
            type: derived
            expression: |
              # After settling (t > 100 min), measure ΔT / Δu
              if timestamp > timestamp.shift(100, 'minutes'):
                  (tank_temp_true_f - 100.0) / (heater_pct - 50.0)
              else:
                  null

        - name: time_to_63pct
          data_type: float
          unit: minutes
          description: "Time to reach 63.2% of final value (should = τ)"
          generator:
            type: derived
            expression: |
              # Target: 63.2% of 5°F change = 3.16°F above 100°F
              target = 100.0 + 0.632 * 5.0
              if tank_temp_true_f >= target and prev('tank_temp_true_f', 100.0) < target:
                  (timestamp - timestamp.shift(30, 'minutes')).total_seconds() / 60.0 - 2.0  # Subtract delay
              else:
                  null
```

**Run and validate:**

```bash
odibi run tank_foptd.yaml
```

```python
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_parquet('./output/tank_foptd.parquet')

# Find step change time
step_time = df[df['heater_pct'] == 60.0].iloc[0]['timestamp']

# Calculate expected final temperature
T_initial = 100.0
heater_change = 10.0  # 50% → 60%
K = 0.5
T_final = T_initial + K * heater_change  # 105°F

# Plot
fig, ax = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

# Input
ax[0].plot(df['timestamp'], df['heater_pct'], linewidth=2, label='Heater %')
ax[0].axvline(step_time, color='r', linestyle='--', alpha=0.5, label='Step time')
ax[0].set_ylabel('Heater (%)')
ax[0].legend()
ax[0].grid(True)
ax[0].set_title('FOPTD Step Response: K=0.5, τ=10 min, θ=2 min')

# Output
ax[1].plot(df['timestamp'], df['tank_temp_measured_f'], alpha=0.5, label='Measured (noisy)')
ax[1].plot(df['timestamp'], df['tank_temp_true_f'], linewidth=2, label='True temp')
ax[1].axhline(T_initial, color='gray', linestyle='--', label='Initial (100°F)')
ax[1].axhline(T_final, color='gray', linestyle='--', label=f'Final ({T_final}°F)')
ax[1].axhline(T_initial + 0.632*(T_final-T_initial), color='orange', linestyle=':', label='63.2% response')
ax[1].axvline(step_time + pd.Timedelta(minutes=2), color='purple', linestyle=':', alpha=0.7, label='Delay ends (θ)')
ax[1].axvline(step_time + pd.Timedelta(minutes=2+10), color='green', linestyle=':', alpha=0.7, label='τ point')
ax[1].set_ylabel('Temperature (°F)')
ax[1].set_xlabel('Time')
ax[1].legend()
ax[1].grid(True)

plt.tight_layout()
plt.savefig('foptd_step_response.png', dpi=150)
plt.show()

# Validate parameters
print("FOPTD Parameter Validation:")
print(f"Expected gain (K): 0.5 °F/%")
print(f"Measured gain: {df['steady_state_gain'].dropna().mean():.3f} °F/%")
print(f"\nExpected time constant (τ): 10.0 min")
time_to_63 = df['time_to_63pct'].dropna().iloc[0] if len(df['time_to_63pct'].dropna()) > 0 else None
print(f"Measured time to 63.2%: {time_to_63:.1f} min" if time_to_63 else "Not reached yet")
```

---

### **Example 2: Parameterized FOPTD (Config Reuse)**

**Data Engineering Focus:** Use YAML variables for easy parameter changes

```yaml
# foptd_parameterized.yaml
pipeline:
  name: foptd_parameterized
  description: Reusable FOPTD with parameters

# ─────────────────────────────────
# CONFIGURATION (change here!)
# ─────────────────────────────────
parameters:
  K: 0.5        # Process gain
  tau: 10.0     # Time constant (minutes)
  theta: 2      # Dead time (minutes, must be integer)
  step_size: 10.0  # Input step magnitude
  step_time_min: 30  # When step occurs

connections:
  output:
    type: local
    path: ./output/foptd_${parameters.K}_${parameters.tau}_${parameters.theta}.parquet
    format: parquet

nodes:
  - name: generate_data
    output: output

    simulation:
      entities: ["PROCESS-001"]
      start_time: "2024-01-01 00:00:00"
      timestep: "1min"
      row_count: 240
      seed: 42

      columns:
        - name: input
          data_type: float
          generator:
            type: derived
            expression: |
              0.0 if timestamp < timestamp.shift(${parameters.step_time_min}, 'minutes') else ${parameters.step_size}

        - name: input_delayed
          data_type: float
          generator:
            type: derived
            expression: "prev('input', 0.0, lag=${parameters.theta})"

        - name: output
          data_type: float
          generator:
            type: derived
            expression: |
              K = ${parameters.K}
              tau = ${parameters.tau}
              dt = 1.0
              u = input_delayed
              y_prev = prev('output', 0.0)
              y_prev + (dt / tau) * (K * u - y_prev)
```

**Now you can easily test different parameters:**

```bash
# Scenario 1: Fast process (small τ)
odibi run foptd_parameterized.yaml --set parameters.tau=2.0

# Scenario 2: High gain
odibi run foptd_parameterized.yaml --set parameters.K=2.0

# Scenario 3: Large dead time
odibi run foptd_parameterized.yaml --set parameters.theta=5
```

---

## Data Engineering Focus: Configuration Management

**Key concepts:**

**1. Parameterization:**
Define parameters once, use throughout YAML
```yaml
parameters:
  K: 0.5

columns:
  - expression: "value * ${parameters.K}"
```

**2. Scenario flags:**
Toggle behavior with boolean flags
```yaml
parameters:
  add_noise: true
  use_delay: true

columns:
  - expression: |
      value + (noise if ${parameters.add_noise} else 0.0)
```

**3. Output naming:**
Include parameters in filename for traceability
```yaml
path: ./output/foptd_K${parameters.K}_tau${parameters.tau}.parquet
```

**Why this matters:**
- Don't repeat values (DRY principle)
- Easy to run sensitivity studies
- Parameters documented in filename
- One config, many scenarios

---

## Validation: FOPTD Characteristics

Add validators to check FOPTD behavior:

```yaml
validation:
  tests:
    - name: gain_check
      type: range
      columns: ["steady_state_gain"]
      min: 0.45  # K ± 10%
      max: 0.55
      on_fail: warn

    - name: time_constant_check
      type: range
      columns: ["time_to_63pct"]
      min: 9.0   # τ ± 10%
      max: 11.0
      on_fail: warn

    - name: delay_verification
      type: custom
      expression: |
        # Temperature shouldn't change for first θ minutes after step
        t_since_step = (timestamp - step_time).total_seconds() / 60.0
        if 0 < t_since_step < theta:
            abs(tank_temp_true_f - 100.0) < 0.1  # Should be near initial
        else:
            true
      on_fail: warn
```

---

## Exercises

### **E1: Identify FOPTD from Step Response**

You're given plant data with a step input. Estimate K, τ, and θ:

```python
# Hints:
# K = (final_value - initial_value) / (input_step_size)
# θ = time when output first starts changing
# τ = time from (θ) to reach 63.2% of final change
```

Run the parameterized YAML with your estimated values and compare to actual data.

---

### **E2: Effect of Dead Time**

Run the FOPTD model with three scenarios:
- θ = 0 (no delay)
- θ = 2 (moderate delay)
- θ = 10 (large delay)

Keep K=0.5, τ=10 constant. Plot all three on same axes.

**Question:** How does dead time affect:
- Rise time?
- Settling time?
- Difficulty of control?

---

### **E3: Slow vs Fast Processes**

Compare two processes:
- **Slow:** K=1.0, τ=20, θ=1
- **Fast:** K=1.0, τ=2, θ=1

Same gain, same delay, different time constant.

Which would be easier to control with PID? Why?

---

### **E4: Build a Heat Exchanger FOPTD**

Model a heat exchanger as FOPTD:
- **Input:** Coolant flow rate (gpm)
- **Output:** Outlet temperature (°F)
- Estimate reasonable K, τ, θ from your ChemE knowledge

**Parameters to consider:**
- K: negative (more coolant → lower temp)
- τ: depends on heat capacity and flow (5-15 min typical)
- θ: pipe length to temperature sensor (1-3 min)

---

## Solutions

Full solutions: [../solutions/L04.md](../solutions/L04.md)

YAML files: `/examples/cheme_course/L04_foptd/`

---

## Reflection: How This Relates to Real Plants

**In a real plant:**
- Process engineers identify FOPTD models from step tests (bump tests)
- K tells you sensitivity (high K = touchy process, hard to control)
- τ tells you speed (low τ = fast, high τ = slow/sluggish)
- θ is often from measurement delays (analyzers, sample transport)
- Controllers are tuned based on K, τ, θ (IMC, Lambda tuning)

**Common pitfalls:**
- Ignoring dead time (θ) → controller oscillates
- Wrong gain sign (K positive vs negative) → runaway
- Time constant too large (τ >> actual) → sluggish control

**What you just learned:**
- How to implement FOPTD in discrete time (`prev` with lag)
- Parameterize configs for reusability
- Validate process characteristics from data
- Dead time is CRITICAL for control (we'll see this in PID lesson)

**When you see plant data:**
- Look for step changes (operators testing, upsets)
- Measure K, τ, θ from response
- Use these for controller tuning or simulation

---

## Next Steps

**You now know:**
- ✅ FOPTD transfer function (K, τ, θ)
- ✅ Time delay implementation with `prev(..., lag=N)`
- ✅ Parameterized YAML configurations
- ✅ Step response validation
- ✅ Scenario-based testing

**Next lesson:**
👉 [L05: Second-Order Systems + Overshoot](L05_second_order.md)

We'll learn damping ratio (ζ), natural frequency (ωn), and how to model underdamped oscillations.

---

*Lesson L04 complete!*
