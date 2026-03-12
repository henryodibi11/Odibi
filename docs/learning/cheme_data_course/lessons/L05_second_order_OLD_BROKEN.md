# L05: Second-Order Systems + Overshoot

**Prerequisites:** L00-L04 | **Effort:** 60 min | **Seborg:** Chapter 5

---

## Learning Objectives

By the end of this lesson you will:

1. ✅ Understand second-order system parameters (ζ, ωn)
2. ✅ Implement underdamped responses using state-space form
3. ✅ Calculate overshoot, peak time, and settling time from data
4. ✅ Validate transient response metrics
5. ✅ Recognize overdamped vs critically damped vs underdamped behavior

---

## Theory Recap: Second-Order Systems (Seborg Ch. 5)

**Standard second-order transfer function:**

```
G(s) = ωn² / (s² + 2ζωn·s + ωn²)
```

**Parameters:**
- **ωn** = Natural frequency (rad/time) - how fast the system oscillates
- **ζ** = Damping ratio (dimensionless) - determines overshoot

**Damping ratio categories:**
- **ζ > 1:** Overdamped - slow, no overshoot (like opening a heavy door)
- **ζ = 1:** Critically damped - fastest response without overshoot
- **0 < ζ < 1:** Underdamped - fast with overshoot (like a car suspension)
- **ζ = 0:** Undamped - pure oscillation (spring with no friction)

**Key metrics for underdamped (ζ < 1):**

**Overshoot:**
```
Mp = exp(-πζ / √(1-ζ²))
```
Example: ζ = 0.3 → Mp ≈ 37%

**Peak time:**
```
tp = π / (ωn√(1-ζ²))
```

**Settling time (2% criterion):**
```
ts ≈ 4 / (ζωn)
```

**Why second-order matters:**
- Pressure gauges with damping
- U-tube manometers
- Mechanical systems (valves, actuators)
- Nested control loops (cascade)

---

## Odibi Hands-On

### **Example 1: Underdamped Pressure Response (ζ = 0.3)**

**Process:** Pressure transmitter with mechanical damping

**State-space implementation:**
```
dx₁/dt = x₂                           (position)
dx₂/dt = -ωn²x₁ - 2ζωn·x₂ + ωn²u     (velocity)
y = x₁                                 (output)
```

```yaml
# pressure_underdamped.yaml
pipeline:
  name: pressure_second_order
  description: Underdamped pressure response (zeta=0.3, wn=1.0)

connections:
  output:
    type: local
    path: ./output/pressure_underdamped.parquet
    format: parquet

nodes:
  - name: generate_pressure_data
    output: output

    simulation:
      entities: ["PT-401"]
      start_time: "2024-01-01 00:00:00"
      timestep: "10sec"
      row_count: 600  # 100 minutes
      seed: 42

      columns:
        # ─────────────────────────────────
        # PARAMETERS
        # ─────────────────────────────────
        - name: zeta
          data_type: float
          generator:
            type: constant
            value: 0.3  # Damping ratio

        - name: omega_n
          data_type: float
          unit: rad/min
          generator:
            type: constant
            value: 1.0  # Natural frequency

        # ─────────────────────────────────
        # INPUT: Pressure step change
        # ─────────────────────────────────
        - name: true_pressure_psi
          data_type: float
          unit: psi
          description: "Step: 0 → 10 psi at t=10min"
          generator:
            type: derived
            expression: |
              0.0 if timestamp < timestamp.shift(10, 'minutes') else 10.0

        # ─────────────────────────────────
        # STATE VARIABLES (state-space)
        # ─────────────────────────────────

        # Velocity (dx₁/dt)
        - name: velocity
          data_type: float
          generator:
            type: derived
            expression: |
              dt = 10.0 / 60.0  # 10 sec in minutes
              wn = omega_n
              z = zeta
              u = true_pressure_psi
              x1 = prev('pressure_reading_psi', 0.0)
              x2 = prev('velocity', 0.0)

              # dx₂/dt = -ωn²x₁ - 2ζωn·x₂ + ωn²u
              dx2 = -wn*wn*x1 - 2*z*wn*x2 + wn*wn*u
              x2 + dt * dx2

        # Position (output)
        - name: pressure_reading_psi
          data_type: float
          unit: psi
          description: "Measured pressure (2nd order response)"
          generator:
            type: derived
            expression: |
              dt = 10.0 / 60.0
              x2 = velocity
              x1 = prev('pressure_reading_psi', 0.0)

              # dx₁/dt = x₂
              x1 + dt * x2

        # ─────────────────────────────────
        # METRICS
        # ─────────────────────────────────
        - name: overshoot_pct
          data_type: float
          unit: percent
          description: "Maximum overshoot percentage"
          generator:
            type: derived
            expression: |
              # After step, find max value
              if timestamp > timestamp.shift(10, 'minutes'):
                  peak = pressure_reading_psi
                  final = 10.0
                  if peak > final:
                      (peak - final) / final * 100.0
                  else:
                      null
              else:
                  null

        - name: expected_overshoot_pct
          data_type: float
          unit: percent
          description: "Theoretical overshoot from damping ratio"
          generator:
            type: derived
            expression: |
              # Mp = exp(-πζ / √(1-ζ²))
              import math
              z = zeta
              if z < 1.0:
                  Mp = math.exp(-math.pi * z / math.sqrt(1 - z*z))
                  Mp * 100.0
              else:
                  0.0
```

**Run and analyze:**

```python
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_parquet('./output/pressure_underdamped.parquet')

# Find step time
step_time = df[df['true_pressure_psi'] == 10.0].iloc[0]['timestamp']
step_idx = df[df['timestamp'] == step_time].index[0]

# Find peak
after_step = df[df['timestamp'] > step_time]
peak_idx = after_step['pressure_reading_psi'].idxmax()
peak_value = after_step.loc[peak_idx, 'pressure_reading_psi']
peak_time = after_step.loc[peak_idx, 'timestamp']

# Calculate overshoot
overshoot_pct = (peak_value - 10.0) / 10.0 * 100.0
expected_overshoot = 37.0  # For ζ=0.3

print(f"Overshoot: {overshoot_pct:.1f}% (expected: {expected_overshoot:.1f}%)")
print(f"Peak time: {(peak_time - step_time).total_seconds()/60:.2f} min")

# Plot
fig, ax = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

# Input
ax[0].plot(df['timestamp'], df['true_pressure_psi'], linewidth=2)
ax[0].axvline(step_time, color='r', linestyle='--', alpha=0.5)
ax[0].set_ylabel('True Pressure (psi)')
ax[0].grid(True)
ax[0].set_title('Second-Order System: ζ=0.3, ωn=1.0 rad/min')

# Output
ax[1].plot(df['timestamp'], df['pressure_reading_psi'], linewidth=2, label='Response')
ax[1].axhline(10.0, color='gray', linestyle='--', label='Final value')
ax[1].axhline(peak_value, color='orange', linestyle=':', label=f'Peak ({overshoot_pct:.1f}% overshoot)')
ax[1].axvline(peak_time, color='orange', linestyle=':', alpha=0.5)
ax[1].set_ylabel('Measured Pressure (psi)')
ax[1].set_xlabel('Time')
ax[1].legend()
ax[1].grid(True)

plt.tight_layout()
plt.savefig('second_order_underdamped.png', dpi=150)
plt.show()
```

---

### **Example 2: Damping Ratio Comparison**

Compare underdamped, critically damped, and overdamped:

```yaml
# Compare different damping ratios
simulation:
  entities: ["underdamped", "critical", "overdamped"]

  columns:
    - name: input_step
      data_type: float
      generator:
        type: derived
        expression: "0.0 if timestamp < timestamp.shift(5, 'minutes') else 1.0"

    # Assign damping ratio based on entity
    - name: zeta
      data_type: float
      generator:
        type: derived
        expression: |
          if entity_id == 'underdamped':
              0.3   # Oscillatory
          elif entity_id == 'critical':
              1.0   # Critically damped
          else:
              2.0   # Overdamped

    - name: omega_n
      data_type: float
      generator:
        type: constant
        value: 1.0

    # ... (same state-space implementation as Example 1)
```

**Analysis:**

```python
# Plot all three responses on same plot
fig, ax = plt.subplots(figsize=(12, 6))

for entity in ['underdamped', 'critical', 'overdamped']:
    entity_df = df[df['entity_id'] == entity]
    ax.plot(entity_df['timestamp'], entity_df['output'],
            linewidth=2, label=entity)

ax.axhline(1.0, color='gray', linestyle='--', alpha=0.5, label='Final value')
ax.set_xlabel('Time')
ax.set_ylabel('Response')
ax.set_title('Effect of Damping Ratio on Second-Order Response')
ax.legend()
ax.grid(True)
plt.show()
```

**Observations:**
- **Underdamped (ζ=0.3):** Fastest but overshoots ~37%
- **Critical (ζ=1.0):** No overshoot, reasonably fast
- **Overdamped (ζ=2.0):** Slow, no overshoot

---

## Data Engineering Focus: Computed KPIs & Metrics Tables

**Key concept:** Don't just store raw data - compute and store KPIs!

**KPIs for second-order systems:**
- Overshoot %
- Peak time
- Rise time (10% to 90%)
- Settling time (within 2% of final)
- Steady-state error

**Implementation pattern:**

```yaml
# Separate metrics table (gold layer)
connections:
  output_data:
    type: local
    path: ./output/second_order_data.parquet

  output_metrics:
    type: local
    path: ./output/second_order_metrics.parquet

nodes:
  - name: generate_data
    output: output_data
    # ... simulation config

  - name: compute_metrics
    input: output_data
    output: output_metrics

    transformers:
      - type: aggregate
        columns:
          - name: max_overshoot_pct
            expression: "max(overshoot_pct)"

          - name: peak_time_min
            expression: "time_at_max(pressure_reading_psi) - step_time"

          - name: settling_time_min
            expression: "time_within_band(pressure_reading_psi, 9.8, 10.2)"
```

**Why separate metrics tables?**
- Raw data = time-series (millions of rows)
- Metrics = summary (one row per test/entity)
- Query performance: Don't scan full time-series for KPIs
- Easier reporting and dashboards

---

## Validation: Transient Response Checks

Validate that second-order behavior matches theory:

```yaml
validation:
  tests:
    - name: overshoot_range
      type: range
      columns: ["overshoot_pct"]
      min: 30.0   # For ζ=0.3, expect ~37%
      max: 45.0
      on_fail: warn

    - name: no_oscillation_if_overdamped
      type: custom
      expression: |
        # If ζ ≥ 1, overshoot should be 0
        if zeta >= 1.0:
            overshoot_pct == null or overshoot_pct < 1.0
        else:
            true
      on_fail: quarantine

    - name: settling_time_check
      type: range
      columns: ["settling_time_min"]
      min: 10.0   # Expected: 4/(ζωn) = 4/(0.3*1.0) ≈ 13.3 min
      max: 20.0
      on_fail: warn
```

---

## Exercises

### **E1: Calculate Theoretical Metrics**

For a system with ζ=0.5, ωn=2.0 rad/min:

**Calculate:**
- Expected overshoot %
- Peak time (min)
- Settling time (2% criterion)

**Then simulate** with Odibi and compare actual vs theoretical.

<details>
<summary>Formulas</summary>

```python
import math
zeta = 0.5
omega_n = 2.0

# Overshoot
Mp = math.exp(-math.pi * zeta / math.sqrt(1 - zeta**2))
overshoot_pct = Mp * 100.0

# Peak time
omega_d = omega_n * math.sqrt(1 - zeta**2)
tp = math.pi / omega_d

# Settling time (2%)
ts = 4.0 / (zeta * omega_n)

print(f"Overshoot: {overshoot_pct:.1f}%")
print(f"Peak time: {tp:.2f} min")
print(f"Settling time: {ts:.2f} min")
```
</details>

---

### **E2: Find Critical Damping**

Run simulations with ζ = 0.5, 0.7, 1.0, 1.3, 1.5

**Plot all responses** on same axes.

**Question:** Which ζ gives fastest response with NO overshoot?

---

### **E3: Measure Rise Time**

Add a column to calculate rise time (time from 10% to 90% of final value):

```yaml
- name: rise_time_min
  generator:
    type: derived
    expression: |
      # Time when response crosses 90% minus time when it crossed 10%
      final = 10.0
      if pressure_reading_psi >= 0.9 * final and prev('pressure_reading_psi', 0.0) < 0.9 * final:
          t90 = timestamp
      if pressure_reading_psi >= 0.1 * final and prev('pressure_reading_psi', 0.0) < 0.1 * final:
          t10 = timestamp
      # (calculate difference)
```

**Compare rise time** for underdamped vs critically damped.

---

### **E4: Valve Actuator Model**

Model a control valve actuator as second-order:
- **Input:** Controller signal (4-20 mA)
- **Output:** Valve position (0-100%)
- Typical: ζ ≈ 0.7 (slightly underdamped), ωn ≈ 3 rad/s

Simulate a step change from 50% to 80% valve position.

**Measure:**
- Overshoot (should be small, ~5%)
- Settling time (should be fast, ~1-2 sec)

---

## Solutions

Full solutions: [../solutions/L05.md](../solutions/L05.md)

YAML files: `/examples/cheme_course/L05_second_order/`

---

## Reflection: How This Relates to Real Plants

**In a real plant:**
- Pressure gauges often underdamped (oscillate after disturbances)
- Control valves tuned for critical damping (fast, no overshoot)
- Level controllers can be overdamped (slow but stable)
- Cascade loops create second-order behavior (inner + outer loop)

**Design trade-offs:**
- **Fast response** → small ζ → overshoot (bad for safety-critical)
- **No overshoot** → large ζ → slow response (bad for throughput)
- **Optimal:** ζ ≈ 0.7 (compromise: fast with ~5% overshoot)

**What you just learned:**
- State-space implementation (position + velocity)
- Damping ratio determines overshoot
- KPI calculation from time-series data
- Metrics tables for reporting

**When you see plant data:**
- Oscillation after setpoint change → likely underdamped (ζ < 1)
- Slow, sluggish response → overdamped (ζ > 1) or large τ
- Measure overshoot to estimate ζ: ζ ≈ -ln(Mp) / √(π² + ln²(Mp))

---

## Next Steps

**You now know:**
- ✅ Second-order systems (ζ, ωn)
- ✅ Underdamped vs critically damped vs overdamped
- ✅ State-space implementation in discrete time
- ✅ Computing transient metrics (overshoot, peak time, settling time)
- ✅ Metrics tables for KPI storage

**You've completed Part I: Foundations!** 🎉

**Next lesson:**
👉 [L06: PID Basics + Constraints](L06_pid_basics.md)

Now we start Part II: Feedback Control. You'll implement PID controllers using Odibi's `pid()` transformer.

---

*Lesson L05 complete! Ready for Part II: Control!*
