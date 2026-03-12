# L03: First-Order Dynamics

**Prerequisites:** L02 | **Effort:** 45 min | **Seborg:** Chapter 5

---

## Learning Objectives

By the end of this lesson you will:

1. ✅ Understand first-order system behavior (exponential response)
2. ✅ Implement first-order dynamics using `prev()` and `ema()`
3. ✅ Calculate time constants from data
4. ✅ Use exponential moving averages (EMA) for filtering
5. ✅ Generate KPIs from time-series data (settling time, rise time)

---

## Theory Recap: First-Order Systems (Seborg Ch. 5)

### **What is a First-Order System?**

**Definition:** A system whose behavior is described by a single time constant τ (tau)

**Transfer function:**
```
G(s) = K / (τs + 1)
```

Where:
- **K** = Process gain (steady-state output/input ratio)
- **τ** = Time constant (time to reach 63.2% of final value)

**Step response:**
```
y(t) = K × (1 - e^(-t/τ))
```

**Key properties:**
- At t = τ: reaches 63.2% of final value
- At t = 3τ: reaches 95% of final value
- At t = 5τ: reaches 99.3% of final value (considered "settled")

### **Discrete-Time Implementation:**

**Continuous ODE:**
```
τ dy/dt + y = K×u
```

**Discrete form (Euler):**
```
y[k] = y[k-1] + (Δt/τ) × (K×u[k] - y[k-1])
```

This is what we implement with `prev()` in Odibi!

---

## Common First-Order Processes

**Chemical Engineering Examples:**
- Heated tank temperature (mixing time constant)
- Level in tank with constant area (capacitance)
- Sensor dynamics (thermocouple lag)
- Valve response (actuator lag)
- Filter response (low-pass filtering)

**Characteristic:** Exponential approach to new steady state

---

## Odibi Hands-On

### **Example 1: Tank Temperature (First-Order Response)**

**Process:** Steam-heated tank
- Input: Steam flow rate (step change)
- Output: Tank temperature (first-order response)
- Time constant τ = 10 minutes

```yaml
# tank_temperature_fo.yaml
pipeline:
  name: first_order_tank
  description: Tank temperature with first-order dynamics

connections:
  output:
    type: local
    path: ./output/first_order_tank.parquet
    format: parquet

nodes:
  - name: generate_temperature_response
    output: output

    simulation:
      entities: ["TK-201"]
      start_time: "2024-01-01 00:00:00"
      timestep: "1min"
      row_count: 600  # 10 hours
      seed: 42

      columns:
        # ──────────────────────────────────
        # PROCESS PARAMETERS
        # ──────────────────────────────────
        - name: time_constant_min
          data_type: float
          description: "τ = 10 minutes"
          generator:
            type: constant
            value: 10.0

        - name: process_gain
          data_type: float
          description: "K = 2.0 (°F per lb/hr steam)"
          generator:
            type: constant
            value: 2.0

        # ──────────────────────────────────
        # INPUT (step change at t=60 min)
        # ──────────────────────────────────
        - name: minutes_elapsed
          data_type: float
          generator:
            type: derived
            expression: "row_number() - 1"

        - name: steam_flow_lb_hr
          data_type: float
          description: "Step from 100 to 150 at t=60"
          generator:
            type: derived
            expression: |
              if minutes_elapsed < 60:
                  100.0
              else:
                  150.0

        # ──────────────────────────────────
        # OUTPUT (first-order response)
        # ──────────────────────────────────
        - name: tank_temp_f
          data_type: float
          description: "Temperature with first-order lag"
          generator:
            type: derived
            expression: |
              # Discrete first-order:
              # y[k] = y[k-1] + (Δt/τ) × (K×u - y[k-1])
              tau = time_constant_min
              K = process_gain
              dt = 1.0  # timestep = 1 minute

              u = steam_flow_lb_hr
              y_prev = prev('tank_temp_f', 200.0)  # Initial temp

              y_prev + (dt/tau) * (K * u - y_prev)

        # ──────────────────────────────────
        # SENSOR (with measurement noise)
        # ──────────────────────────────────
        - name: temp_sensor_f
          data_type: float
          generator:
            type: derived
            expression: "tank_temp_f + noise"

        - name: noise
          data_type: float
          generator:
            type: range
            min: -0.5
            max: 0.5

        # ──────────────────────────────────
        # KPIs (calculated)
        # ──────────────────────────────────
        - name: percent_of_final_value
          data_type: float
          description: "% progress to new steady state"
          generator:
            type: derived
            expression: |
              # Final value = K × u_final = 2.0 × 150 = 300°F
              # Initial = K × u_initial = 2.0 × 100 = 200°F
              # Change = 100°F
              if minutes_elapsed < 60:
                  0.0
              else:
                  min(100.0, ((tank_temp_f - 200.0) / 100.0) * 100.0)
```

**Run and analyze:**

```bash
odibi run tank_temperature_fo.yaml
```

```python
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_parquet('./output/first_order_tank.parquet')

fig, ax = plt.subplots(3, 1, figsize=(12, 9), sharex=True)

# Input (step change)
ax[0].plot(df['minutes_elapsed'], df['steam_flow_lb_hr'], linewidth=2, color='blue')
ax[0].axvline(60, color='red', linestyle='--', label='Step at t=60')
ax[0].set_ylabel('Steam Flow (lb/hr)')
ax[0].legend()
ax[0].grid(True)
ax[0].set_title('Input (Step Change)')

# Output (first-order response)
ax[1].plot(df['minutes_elapsed'], df['tank_temp_f'], linewidth=2, label='True Temp', color='green')
ax[1].plot(df['minutes_elapsed'], df['temp_sensor_f'], alpha=0.5, label='Sensor (noisy)')
ax[1].axhline(200, color='gray', linestyle='--', alpha=0.5, label='Initial SS')
ax[1].axhline(300, color='gray', linestyle='--', alpha=0.5, label='Final SS')
ax[1].axvline(60, color='red', linestyle='--')
ax[1].axvline(60 + 10, color='orange', linestyle='--', label='t=τ (63.2%)')
ax[1].axvline(60 + 30, color='purple', linestyle='--', label='t=3τ (95%)')
ax[1].set_ylabel('Temperature (°F)')
ax[1].legend()
ax[1].grid(True)
ax[1].set_title('Output (First-Order Response)')

# Percent of final value
ax[2].plot(df['minutes_elapsed'], df['percent_of_final_value'], linewidth=2, color='purple')
ax[2].axhline(63.2, color='orange', linestyle='--', label='63.2% at t=τ')
ax[2].axhline(95, color='purple', linestyle='--', label='95% at t=3τ')
ax[2].set_ylabel('% of Final Value')
ax[2].set_xlabel('Time (minutes)')
ax[2].legend()
ax[2].grid(True)
ax[2].set_title('Response Progress')

plt.tight_layout()
plt.savefig('first_order_response.png', dpi=150)
plt.show()

# Verify time constant
step_start = df[df['minutes_elapsed'] == 60].index[0]
df_response = df.iloc[step_start:].copy()
df_response['time_from_step'] = df_response['minutes_elapsed'] - 60

# At t=τ, should be 63.2% of change
at_tau = df_response[df_response['time_from_step'].between(9.5, 10.5)]
print(f"\nAt t=τ (10 min): {at_tau['percent_of_final_value'].mean():.1f}% (expect 63.2%)")

at_3tau = df_response[df_response['time_from_step'].between(29, 31)]
print(f"At t=3τ (30 min): {at_3tau['percent_of_final_value'].mean():.1f}% (expect 95%)")
```

**Expected output:**
- At t=60: step input occurs
- At t=70 (τ later): reached 63.2% of new value
- At t=90 (3τ later): reached 95%
- At t=110 (5τ later): essentially settled

---

### **Example 2: Using `ema()` for Filtering**

**Use case:** Noisy sensor data → smooth with exponential moving average

**EMA formula:**
```
y_filtered[k] = α × y_raw[k] + (1-α) × y_filtered[k-1]
```

Where α = smoothing factor (0 to 1):
- α = 1.0: No filtering (raw data)
- α = 0.1: Heavy filtering (slow response)
- α relates to time constant: α ≈ Δt/τ

```yaml
simulation:
  entities: ["SENSOR-101"]
  start_time: "2024-01-01 00:00:00"
  timestep: "1sec"
  row_count: 3600  # 1 hour
  seed: 42

  columns:
    # True process value (slow-varying)
    - name: true_temperature_f
      data_type: float
      generator:
        type: random_walk
        start: 150.0
        min: 145.0
        max: 155.0
        step_size: 0.1

    # Raw sensor (noisy)
    - name: raw_sensor_f
      data_type: float
      generator:
        type: derived
        expression: "true_temperature_f + noise"

    - name: noise
      data_type: float
      generator:
        type: range
        min: -2.0
        max: 2.0

    # Filtered (using ema)
    - name: filtered_sensor_f
      data_type: float
      description: "EMA with α=0.1 (τ≈10 sec)"
      generator:
        type: derived
        expression: "ema('raw_sensor_f', alpha=0.1, default=150.0)"

    # Heavy filtering
    - name: heavily_filtered_f
      data_type: float
      description: "EMA with α=0.05 (τ≈20 sec)"
      generator:
        type: derived
        expression: "ema('raw_sensor_f', alpha=0.05, default=150.0)"
```

**Analysis:**

```python
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_parquet('./output/ema_filtering.parquet')

plt.figure(figsize=(14, 6))
plt.plot(df.index, df['true_temperature_f'], label='True Value', linewidth=2, color='green')
plt.plot(df.index, df['raw_sensor_f'], label='Raw Sensor (noisy)', alpha=0.4, color='gray')
plt.plot(df.index, df['filtered_sensor_f'], label='EMA (α=0.1)', linewidth=1.5, color='blue')
plt.plot(df.index, df['heavily_filtered_f'], label='EMA (α=0.05)', linewidth=1.5, color='red')
plt.xlabel('Sample')
plt.ylabel('Temperature (°F)')
plt.legend()
plt.grid(True)
plt.title('EMA Filtering: Trade-off Between Noise Reduction and Lag')
plt.tight_layout()
plt.savefig('ema_filtering.png', dpi=150)
plt.show()

# Calculate noise reduction
print(f"Raw sensor std dev: {df['raw_sensor_f'].std():.3f}°F")
print(f"Filtered (α=0.1) std dev: {df['filtered_sensor_f'].std():.3f}°F")
print(f"Heavily filtered (α=0.05) std dev: {df['heavily_filtered_f'].std():.3f}°F")
```

**Trade-off:**
- More filtering (lower α) → Less noise BUT more lag
- Less filtering (higher α) → More responsive BUT noisier

---

### **Example 3: Cascade of First-Order Systems**

**Two tanks in series:** Each with its own time constant

```yaml
columns:
  # Tank 1
  - name: inlet_flow_gpm
    data_type: float
    generator:
      type: random_walk
      start: 50.0
      min: 40.0
      max: 60.0
      step_size: 1.0

  - name: tank1_level_ft
    data_type: float
    description: "First-order response: τ1 = 5 min"
    generator:
      type: derived
      expression: |
        tau1 = 5.0
        dt = 1.0
        K1 = 0.1
        prev('tank1_level_ft', 10.0) + (dt/tau1) * (K1 * inlet_flow_gpm - prev('tank1_level_ft', 10.0))

  # Tank 2 (fed by Tank 1 outlet)
  - name: tank1_outlet_flow_gpm
    data_type: float
    generator:
      type: derived
      expression: "tank1_level_ft * 5.0"  # Proportional to level

  - name: tank2_level_ft
    data_type: float
    description: "Second first-order response: τ2 = 8 min"
    generator:
      type: derived
      expression: |
        tau2 = 8.0
        dt = 1.0
        K2 = 0.1
        prev('tank2_level_ft', 8.0) + (dt/tau2) * (K2 * tank1_outlet_flow_gpm - prev('tank2_level_ft', 8.0))
```

**Result:** Overall response is second-order (covered in L05), but built from two first-order stages.

---

## Data Engineering Focus: Stateful Functions and KPIs

### **Stateful Functions:**

**`prev(column, default)`**
- Accesses previous row value
- Essential for discrete-time differential equations
- Default used for first row

**`ema(column, alpha, default)`**
- Exponential moving average
- Equivalent to first-order filter
- Alpha controls smoothing (0-1)

**Use cases:**
- Filtering noisy sensors
- Smoothing KPIs for dashboards
- Rate-of-change calculations (`value - prev(value)`)
- Running balances (accumulation)

### **KPIs from Time-Series:**

**Common KPIs:**
```yaml
# Settling time (time to reach 95% of final)
- name: is_settled
  expression: "percent_of_final >= 95.0"

# Rate of change
- name: temp_rate_f_per_min
  expression: "tank_temp_f - prev('tank_temp_f', 0)"

# Deviation from setpoint
- name: error_f
  expression: "abs(tank_temp_f - setpoint_f)"

# Time in spec
- name: in_spec
  expression: "(tank_temp_f >= 195) & (tank_temp_f <= 205)"
```

**Aggregations (post-processing):**
```python
# Calculate overall KPIs
settling_time = df[df['is_settled']].iloc[0]['minutes_elapsed'] - 60
print(f"Settling time: {settling_time:.1f} minutes")

time_in_spec = (df['in_spec'].sum() / len(df)) * 100
print(f"Time in spec: {time_in_spec:.1f}%")
```

---

## Validation: Time Constant Verification

**Test:** Does the simulated response match expected τ?

```yaml
validation:
  tests:
    # At t=τ after step, should be near 63.2%
    - name: verify_time_constant
      type: expression
      expression: |
        (minutes_elapsed < 69) |
        (minutes_elapsed > 71) |
        (percent_of_final_value >= 60 & percent_of_final_value <= 66)
      on_fail: quarantine
```

**Or in post-processing:**

```python
# Fit exponential to data
from scipy.optimize import curve_fit

def first_order(t, K, tau):
    return 200 + K * (1 - np.exp(-t / tau))

# Fit to response data
df_fit = df[df['minutes_elapsed'] >= 60].copy()
t = df_fit['minutes_elapsed'].values - 60
y = df_fit['tank_temp_f'].values

params, _ = curve_fit(first_order, t, y, p0=[100, 10])
K_fit, tau_fit = params

print(f"Fitted K = {K_fit:.2f} (expected 100)")
print(f"Fitted τ = {tau_fit:.2f} min (expected 10)")
assert abs(tau_fit - 10) < 1.0, "Time constant doesn't match!"
```

---

## Exercises

### **E1: Change Time Constant**

Modify the tank temperature example:
- Change `time_constant_min` from 10 to 5
- Re-run and plot
- **Question:** How does the response curve change?

<details>
<summary>Answer</summary>

Faster response: reaches 63.2% at t=65 (5 min after step), 95% at t=75 (3τ = 15 min).
</details>

---

### **E2: Estimate τ from Data**

Generate first-order data with unknown time constant (don't look at YAML!):

```python
# Given only the data, estimate τ
df = pd.read_parquet('./output/mystery_fo_system.parquet')

# Find when step occurred
step_idx = df[df['input'].diff() != 0].index[0]

# Calculate 63.2% point
initial = df.loc[step_idx - 1, 'output']
final = df.loc[df.index[-1], 'output']
threshold = initial + 0.632 * (final - initial)

# Find when crossed threshold
cross_idx = df[df['output'] >= threshold].index[0]
tau_estimated = (cross_idx - step_idx) * 1.0  # timestep = 1 min

print(f"Estimated τ = {tau_estimated:.1f} minutes")
```

---

### **E3: Thermocouple Lag**

Model a thermocouple with τ = 30 seconds measuring a furnace:

```yaml
- name: furnace_temp_f
  data_type: float
  description: "True furnace temperature"
  generator:
    type: random_walk
    start: 1200.0
    min: 1150.0
    max: 1250.0
    step_size: 5.0

- name: thermocouple_reading_f
  data_type: float
  description: "Thermocouple with 30-sec lag"
  generator:
    type: derived
    expression: |
      tau = 0.5  # 30 sec = 0.5 min
      dt = 0.0167  # 1 sec = 0.0167 min
      prev_tc = prev('thermocouple_reading_f', 1200.0)
      prev_tc + (dt/tau) * (furnace_temp_f - prev_tc)
```

**Challenge:** Compare thermocouple lag to an EMA filter with equivalent time constant.

---

### **E4: Multi-Stage Filtering**

Apply two EMA filters in series:

```yaml
- name: ema_stage1
  expression: "ema('raw_sensor', alpha=0.2)"

- name: ema_stage2
  expression: "ema('ema_stage1', alpha=0.2)"
```

**Question:** How does two-stage filtering compare to single-stage with α=0.1?

---

## Solutions

Full solutions: [../solutions/L03.md](../solutions/L03.md)

YAML files: `/examples/cheme_course/L03_first_order/`

---

## Reflection: How This Relates to Real Plants

**In a real plant:**
- Most processes exhibit first-order dynamics (tanks, heaters, simple reactors)
- Time constants range from seconds (sensors) to hours (large vessels)
- Operators use τ to estimate:
  - How long to wait after a change
  - Appropriate sampling rates (sample faster than τ)
  - Filter settings for noisy sensors
- Control engineers measure τ from step tests

**What you just learned:**
- Implementing first-order dynamics in discrete time
- Using `prev()` for stateful calculations
- EMA filtering for noise reduction
- Extracting KPIs (settling time, time in spec)

**Real example:**
A distillation column's temperature loop has τ ≈ 45 minutes. After changing reflux, operators wait 3τ = 135 minutes (2+ hours) before declaring steady state.

---

## Next Steps

**You now know:**
- ✅ First-order system theory and discrete implementation
- ✅ `prev()` and `ema()` stateful functions
- ✅ Exponential response characteristics (63.2%, 95%, settling)
- ✅ KPI calculations from time-series

**Next lesson:**
👉 [L04: FOPTD Transfer Functions](L04_foptd.md)

We'll add process gain (K) and dead time (θ) to create realistic FOPTD models with parameterized YAML.

---

*Lesson L03 complete!*
