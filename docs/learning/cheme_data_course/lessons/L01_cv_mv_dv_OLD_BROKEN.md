# L01: CV/MV/DV and Time Series Data

**Prerequisites:** L00 | **Effort:** 45 min | **Seborg:** Chapter 1

---

## Learning Objectives

By the end of this lesson you will:

1. ✅ Identify Controlled, Manipulated, and Disturbance variables in a process
2. ✅ Map these variables to data schemas
3. ✅ Understand time-series partitioning strategies
4. ✅ Generate realistic MV/DV/CV relationships
5. ✅ Write partitioned Parquet for efficient queries

---

## Theory Recap: Process Control Variables (Seborg Ch. 1)

### **The Three Variable Types:**

**1. Controlled Variable (CV)** - What you WANT to control
- Examples: Tank level, reactor temperature, product composition
- Goal: Keep CV at setpoint (or within acceptable range)
- In data: This is your "target" or "outcome" column

**2. Manipulated Variable (MV)** - What you ADJUST to control the CV
- Examples: Valve position, heater power, pump speed
- Controller OUTPUT goes here
- In data: This is your "action" or "input" column

**3. Disturbance Variable (DV)** - What MESSES UP your control
- Examples: Feed composition changes, ambient temperature, upstream flow rate
- Unmeasured disturbances are the worst (you can't see them coming)
- In data: These are "external factors" or "uncontrolled inputs"

### **Example: Tank Level Control**

```
Disturbance (DV)    →   Process   →   Controlled (CV)
──────────────          ───────         ──────────────
Feed flow rate          Tank            Tank level

                           ↑
Manipulated (MV)    ───────┘
──────────────
Outlet valve position
```

**Control objective:** Keep tank level at 10 ft despite feed rate changes

**How:** Adjust outlet valve to match inlet flow

---

## Why This Matters for Data

When building datasets (simulation or real plant):

1. **Schema design:** CV/MV/DV become columns in your table
2. **Causality:** MV changes → CV responds (with lag/delay)
3. **Validation:** CV should be "explainable" by MV + DV (if not, something's wrong)
4. **Features:** MV and DV are "features", CV is "label" (ML terminology)

**Bad data example:**
- Level changes but valve position is constant → Not realistic!
- Temperature drops but heater is at 100% → Physics violated!

**Good data:**
- CV lags MV (first-order response)
- DV causes CV to deviate (disturbance rejection)
- Noise is realistic (sensors aren't perfect)

---

## Odibi Hands-On

### **Example 1: Mixing Tank (CV/MV/DV Mapping)**

**Process:** Neutralization tank
- **CV:** pH (target = 7.0)
- **MV:** Acid flow rate (gpm)
- **DV:** Feed pH (varies with upstream batches)

```yaml
# mixing_tank_ph.yaml
pipeline:
  name: mixing_tank_ph_control
  description: pH control with acid addition

connections:
  output:
    type: local
    path: ./output/mixing_tank_ph.parquet
    format: parquet

nodes:
  - name: generate_ph_data
    output: output

    simulation:
      entities: ["TANK-301"]
      start_time: "2024-01-01 00:00:00"
      timestep: "1min"
      row_count: 480  # 8 hours
      seed: 42

      columns:
        # ─────────────────────────────────
        # DISTURBANCE VARIABLE (DV)
        # ─────────────────────────────────
        - name: feed_ph
          data_type: float
          description: "Incoming feed pH (uncontrolled)"
          generator:
            type: random_walk
            start: 8.5
            min: 7.5
            max: 9.5
            step_size: 0.1

        # ─────────────────────────────────
        # MANIPULATED VARIABLE (MV)
        # ─────────────────────────────────
        - name: acid_flow_gpm
          data_type: float
          description: "Acid addition rate (controller output)"
          generator:
            type: random_walk
            start: 2.0
            min: 0.0
            max: 5.0
            step_size: 0.1

        # ─────────────────────────────────
        # CONTROLLED VARIABLE (CV)
        # ─────────────────────────────────
        - name: tank_ph
          data_type: float
          description: "Tank pH (controlled variable)"
          generator:
            type: derived
            expression: |
              # Simplified pH model:
              # pH moves toward feed pH, acid pulls it down
              prev_ph = prev('tank_ph', 7.0)
              disturbance_effect = 0.1 * (feed_ph - prev_ph)
              control_effect = -0.15 * acid_flow_gpm
              prev_ph + disturbance_effect + control_effect

        # ─────────────────────────────────
        # MEASUREMENT (with noise)
        # ─────────────────────────────────
        - name: ph_sensor
          data_type: float
          description: "pH sensor reading (noisy)"
          generator:
            type: derived
            expression: "tank_ph + sensor_noise"

        - name: sensor_noise
          data_type: float
          generator:
            type: range
            min: -0.05
            max: 0.05

        # ─────────────────────────────────
        # METADATA
        # ─────────────────────────────────
        - name: control_mode
          data_type: string
          generator:
            type: categorical
            choices: ["AUTO", "MANUAL"]
            weights: [0.95, 0.05]  # 95% auto, 5% manual
```

**Run it:**

```bash
odibi run mixing_tank_ph.yaml
```

**Analyze:**

```python
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_parquet('./output/mixing_tank_ph.parquet')

fig, ax = plt.subplots(3, 1, figsize=(12, 8), sharex=True)

# Disturbance Variable (DV)
ax[0].plot(df['timestamp'], df['feed_ph'], label='Feed pH (DV)', color='orange')
ax[0].axhline(7.0, color='gray', linestyle='--', label='Target')
ax[0].set_ylabel('pH')
ax[0].legend()
ax[0].grid(True)
ax[0].set_title('Disturbance Variable')

# Manipulated Variable (MV)
ax[1].plot(df['timestamp'], df['acid_flow_gpm'], label='Acid Flow (MV)', color='blue')
ax[1].set_ylabel('Flow (gpm)')
ax[1].legend()
ax[1].grid(True)
ax[1].set_title('Manipulated Variable')

# Controlled Variable (CV)
ax[2].plot(df['timestamp'], df['ph_sensor'], label='pH Sensor (CV)', color='green', alpha=0.7)
ax[2].axhline(7.0, color='gray', linestyle='--', label='Setpoint')
ax[2].fill_between(df['timestamp'], 6.8, 7.2, alpha=0.2, color='green', label='±0.2 tolerance')
ax[2].set_ylabel('pH')
ax[2].set_xlabel('Time')
ax[2].legend()
ax[2].grid(True)
ax[2].set_title('Controlled Variable')

plt.tight_layout()
plt.savefig('cv_mv_dv_example.png')
plt.show()
```

**What you should observe:**
- When feed pH increases (DV ↑) → tank pH rises → acid flow increases (MV ↑) to compensate
- CV has realistic noise (±0.05 pH units)
- 95% of time in AUTO mode, 5% MANUAL (operator intervention)

---

### **Example 2: Heat Exchanger (Multiple CVs)**

**Process:** Shell-and-tube heat exchanger
- **CV1:** Outlet temperature (primary)
- **CV2:** Pressure drop (secondary constraint)
- **MV:** Coolant flow rate
- **DV:** Inlet temperature (from upstream)

```yaml
simulation:
  entities: ["HX-201"]
  start_time: "2024-01-01 00:00:00"
  timestep: "30sec"
  row_count: 2880  # 24 hours
  seed: 42

  columns:
    # DV: Inlet temperature varies
    - name: inlet_temp_f
      data_type: float
      generator:
        type: random_walk
        start: 220.0
        min: 200.0
        max: 240.0
        step_size: 1.0

    # MV: Coolant flow (controlled)
    - name: coolant_flow_gpm
      data_type: float
      generator:
        type: random_walk
        start: 150.0
        min: 100.0
        max: 200.0
        step_size: 2.0

    # CV1: Outlet temperature (first-order lag)
    - name: outlet_temp_f
      data_type: float
      generator:
        type: derived
        expression: |
          # Temperature drops with more coolant
          prev_temp = prev('outlet_temp_f', 180.0)
          tau = 5.0  # time constant = 5 minutes
          dt = 0.5   # timestep = 30 sec = 0.5 min

          # Steady-state: Tout = Tin - K*Flow
          K = 0.3  # cooling effectiveness
          steady_state = inlet_temp_f - K * coolant_flow_gpm

          prev_temp + (dt/tau) * (steady_state - prev_temp)

    # CV2: Pressure drop (increases with flow)
    - name: pressure_drop_psi
      data_type: float
      generator:
        type: derived
        expression: |
          # ΔP ∝ Flow² (turbulent flow)
          0.001 * coolant_flow_gpm ** 2 + noise

    - name: noise
      data_type: float
      generator:
        type: range
        min: -0.5
        max: 0.5
```

**Key insight:** You can have multiple CVs! Often there's a primary CV (temperature) and constraints (pressure drop < 40 psi).

---

## Data Engineering Focus: Time-Series Partitioning

### **Why Partition?**

Imagine you have 1 year of 1-second data:
- 365 days × 24 hours × 3600 seconds = **31.5 million rows**
- Query: "Show me data from March 15, 2024"
- Without partitions: Scan all 31.5M rows (slow!)
- With daily partitions: Scan only 1 day (86,400 rows) → 365x faster!

### **Partitioning Strategies:**

**1. Daily partitions** (most common)
```
output/
  year=2024/
    month=01/
      day=01/
        data.parquet
      day=02/
        data.parquet
```

**2. Hourly partitions** (for high-frequency data)
```
output/
  year=2024/month=01/day=01/
    hour=00/
      data.parquet
    hour=01/
      data.parquet
```

**3. Monthly partitions** (for low-frequency or summaries)
```
output/
  year=2024/
    month=01/
      data.parquet
```

### **Odibi Example: Partitioned Output**

```yaml
connections:
  output:
    type: local
    path: ./output/partitioned_data
    format: parquet
    partition_cols: ["year", "month", "day"]  # Partition by date
```

Then add derived columns for year/month/day:

```yaml
columns:
  # ... (your other columns)

  - name: year
    data_type: int
    generator:
      type: derived
      expression: "timestamp.year"

  - name: month
    data_type: int
    generator:
      type: derived
      expression: "timestamp.month"

  - name: day
    data_type: int
    generator:
      type: derived
      expression: "timestamp.day"
```

**Query partitioned data:**

```python
import pandas as pd

# Read only March 15, 2024
df = pd.read_parquet(
    './output/partitioned_data',
    filters=[
        ('year', '=', 2024),
        ('month', '=', 3),
        ('day', '=', 15)
    ]
)
```

**Performance:**
- Non-partitioned: Reads entire dataset, then filters (slow)
- Partitioned: Only reads relevant partition folders (fast)

---

## Validation: CV/MV Relationships

Add a validation test to check that MV actually affects CV:

```yaml
validation:
  tests:
    - name: cv_responds_to_mv
      type: correlation
      columns: ["acid_flow_gpm", "tank_ph"]
      min_correlation: -0.3  # Negative correlation (more acid → lower pH)
      on_fail: quarantine
```

If correlation is too weak (< -0.3), data goes to quarantine (meaning MV isn't influencing CV → model is broken).

---

## Exercises

### **E1: Identify CV/MV/DV**

For each process, identify CV, MV, and DV:

**a) Distillation column:**
- Reflux flow rate
- Feed composition
- Top product purity

**b) Fired heater:**
- Fuel gas flow
- Ambient temperature
- Outlet temperature

**c) Compressor:**
- Suction pressure
- Motor speed
- Discharge temperature

<details>
<summary>Answers</summary>

a) CV = Top purity, MV = Reflux flow, DV = Feed composition

b) CV = Outlet temp, MV = Fuel flow, DV = Ambient temp

c) CV = Discharge temp, MV = Motor speed, DV = Suction pressure
</details>

---

### **E2: Add a Second Disturbance**

Modify `mixing_tank_ph.yaml` to add a second disturbance:

```yaml
- name: feed_flow_rate_gpm
  data_type: float
  generator:
    type: random_walk
    start: 100.0
    min: 80.0
    max: 120.0
    step_size: 2.0
```

Then update the `tank_ph` calculation to account for dilution effect (higher flow → harder to control pH).

**Hint:** Add a term like `+ 0.01 * (feed_flow_rate_gpm - 100.0)` to the pH expression.

---

### **E3: Add Lag to MV**

Real valves don't respond instantly. Add a first-order lag to `acid_flow_gpm`:

```yaml
- name: acid_flow_setpoint_gpm
  data_type: float
  generator:
    type: random_walk
    start: 2.0
    min: 0.0
    max: 5.0
    step_size: 0.1

- name: acid_flow_gpm
  data_type: float
  description: "Actual flow (lags setpoint)"
  generator:
    type: derived
    expression: |
      # First-order lag: τ = 0.5 min
      tau = 0.5
      dt = 1.0
      setpoint = acid_flow_setpoint_gpm
      prev('acid_flow_gpm', 2.0) + (dt/tau) * (setpoint - prev('acid_flow_gpm', 2.0))
```

Run and plot setpoint vs actual. You should see lag!

---

### **E4: Implement Partitioning**

Modify the output connection to use daily partitions:

```yaml
connections:
  output:
    type: local
    path: ./output/partitioned_ph_data
    format: parquet
    partition_cols: ["year", "month", "day"]
```

Add year/month/day columns (see example above). Run the pipeline.

**Question:** How many partition folders were created for 8 hours of data starting at midnight?

<details>
<summary>Answer</summary>

One folder (year=2024/month=01/day=01/) because all data is on the same day.
</details>

---

## Solutions

Full solutions: [../solutions/L01.md](../solutions/L01.md)

YAML files: `/examples/cheme_course/L01_cv_mv_dv/`

---

## Reflection: How This Relates to Real Plants

**In a real plant:**
- Process engineers identify CV/MV/DV during design
- Control engineers tune loops based on these relationships
- Data historians log all three types of variables
- Troubleshooting often starts with: "What changed? (MV? DV?)"
- Partitioning by date is standard for plant historians (daily rollups)

**What you just learned:**
- Mapping process control concepts → data schemas
- Causality relationships (MV → CV, DV → CV)
- Realistic lag/noise in measurements
- Partitioning strategies for time-series query performance

**Next time you see plant data:**
- Ask: "Which columns are CV? MV? DV?"
- Check: "Does CV respond to MV?" (if not, controller is broken or MV is saturated)
- Look for: "Which DVs are causing upsets?"

---

## Next Steps

**You now know:**
- ✅ CV/MV/DV mapping to data schemas
- ✅ Generating realistic process variable relationships
- ✅ Time-series partitioning for performance
- ✅ Adding metadata columns (year/month/day, control mode)

**Next lesson:**
👉 [L02: Degrees of Freedom + Mass/Energy Balances](L02_dof_balances.md)

We'll learn to count DoF, avoid circular dependencies, and add physical unit columns with validation.

---

*Lesson L01 complete!*
