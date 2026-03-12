# L02: Degrees of Freedom + Mass/Energy Balances

**Prerequisites:** L00, L01 | **Effort:** 60 min | **Seborg:** Chapter 2

---

## Learning Objectives

By the end of this lesson you will:

1. ✅ Count degrees of freedom (DoF) to avoid circular dependencies
2. ✅ Implement mass and energy balances using `derived` columns
3. ✅ Add unit columns and validation tests
4. ✅ Route bad data to quarantine
5. ✅ Understand topological sorting for derived column execution

---

## Theory Recap: Degrees of Freedom (Seborg Ch. 2)

**DoF Analysis:** How many variables can you choose freely?

**Formula:**
```
DoF = Number of Variables - Number of Independent Equations
```

**Rules:**
- **DoF > 0:** Underspecified → you must provide inputs
- **DoF = 0:** Fully specified → system is determined
- **DoF < 0:** Overspecified → inconsistent (can't solve)

**Example: Simple Tank**
- Variables: Level (L), Inlet flow (Fin), Outlet flow (Fout)
- Equation: dL/dt = (Fin - Fout) / Area
- DoF = 3 variables - 1 equation = 2
- **Meaning:** Specify 2 inputs (Fin, Fout), calculate 1 output (L)

**Why this matters for data:**
In Odibi, you must decide which columns are **generators** (inputs) vs **derived** (calculated). If you try to make everything `derived`, you'll get circular dependency errors!

---

## Common Mistake: Circular Dependencies

**BAD (won't work):**
```yaml
columns:
  - name: level
    generator:
      type: derived
      expression: "prev('level', 10.0) + (inlet_flow - outlet_flow) / 100.0"

  - name: inlet_flow
    generator:
      type: derived
      expression: "50.0 if level < 12.0 else 30.0"  # Depends on level!

  - name: outlet_flow
    generator:
      type: derived
      expression: "0.8 * inlet_flow"  # Depends on inlet_flow!
```

**Problem:** level → inlet_flow → outlet_flow → level (circular!)

**GOOD:**
```yaml
columns:
  - name: inlet_flow
    generator:
      type: random_walk  # INPUT (generator)
      start: 50.0

  - name: outlet_flow
    generator:
      type: constant  # INPUT (generator)
      value: 45.0

  - name: level
    generator:
      type: derived  # OUTPUT (calculated from inputs)
      expression: "prev('level', 10.0) + (inlet_flow - outlet_flow) / 100.0"
```

**Odibi automatically sorts derived columns** using topological sorting, but it can't break circular dependencies.

---

## Odibi Hands-On

### **Example 1: Tank Mass Balance**

**Process:** Storage tank with inlet, outlet, and level

**DoF Analysis:**
- Variables: Fin, Fout, L (3)
- Equations: dL/dt = (Fin - Fout)/A (1)
- DoF = 3 - 1 = 2 → Specify Fin and Fout

```yaml
# tank_mass_balance.yaml
pipeline:
  name: tank_mass_balance
  description: Simple mass balance with DoF analysis

connections:
  output:
    type: local
    path: ./output/tank_balance.parquet
    format: parquet

nodes:
  - name: generate_tank_data
    output: output

    simulation:
      entities: ["TK-101"]
      start_time: "2024-01-01 00:00:00"
      timestep: "1min"
      row_count: 480  # 8 hours
      seed: 42

      columns:
        # ─────────────────────────────────
        # INPUTS (2 DoF)
        # ─────────────────────────────────
        - name: inlet_flow_gpm
          data_type: float
          unit: gpm
          generator:
            type: random_walk
            start: 100.0
            min: 80.0
            max: 120.0
            step_size: 2.0

        - name: outlet_flow_gpm
          data_type: float
          unit: gpm
          generator:
            type: random_walk
            start: 95.0
            min: 80.0
            max: 110.0
            step_size: 1.5

        # ─────────────────────────────────
        # CALCULATED (0 DoF remaining)
        # ─────────────────────────────────
        - name: tank_level_ft
          data_type: float
          unit: ft
          generator:
            type: derived
            expression: |
              # Mass balance: dL/dt = (Fin - Fout) / A
              area_ft2 = 100.0
              dt_min = 1.0
              gpm_to_ft3_per_min = 7.48  # conversion factor

              prev_level = prev('tank_level_ft', 10.0)
              net_flow = (inlet_flow_gpm - outlet_flow_gpm) / gpm_to_ft3_per_min
              dL = net_flow / area_ft2 * dt_min

              prev_level + dL

        # ─────────────────────────────────
        # VALIDATION COLUMNS
        # ─────────────────────────────────
        - name: mass_balance_error_pct
          data_type: float
          unit: percent
          generator:
            type: derived
            expression: |
              # Check: mass in = mass out + accumulation
              accumulation = (tank_level_ft - prev('tank_level_ft', 10.0)) * 100.0 * 7.48
              mass_in = inlet_flow_gpm * 1.0
              mass_out = outlet_flow_gpm * 1.0
              expected_accumulation = mass_in - mass_out

              # Error (should be near zero)
              abs(accumulation - expected_accumulation) / (mass_in + 0.001) * 100.0

    # ─────────────────────────────────
    # VALIDATION TESTS
    # ─────────────────────────────────
    validation:
      tests:
        - name: positive_flows
          type: range
          columns: ["inlet_flow_gpm", "outlet_flow_gpm"]
          min: 0.0
          max: 200.0
          on_fail: quarantine

        - name: reasonable_level
          type: range
          columns: ["tank_level_ft"]
          min: 0.0
          max: 20.0
          on_fail: quarantine

        - name: mass_balance_closure
          type: range
          columns: ["mass_balance_error_pct"]
          min: 0.0
          max: 1.0  # Allow 1% error
          on_fail: warn
```

**Run and verify:**

```bash
odibi run tank_mass_balance.yaml
```

```python
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_parquet('./output/tank_balance.parquet')

# Check for quarantined rows
if 'quarantine_reason' in df.columns:
    quarantined = df[df['quarantine_reason'].notna()]
    print(f"⚠️  {len(quarantined)} rows quarantined")
    print(quarantined[['timestamp', 'tank_level_ft', 'quarantine_reason']])

# Plot mass balance
fig, ax = plt.subplots(3, 1, figsize=(12, 8), sharex=True)

ax[0].plot(df['timestamp'], df['inlet_flow_gpm'], label='Inlet')
ax[0].plot(df['timestamp'], df['outlet_flow_gpm'], label='Outlet')
ax[0].set_ylabel('Flow (gpm)')
ax[0].legend()
ax[0].grid(True)

ax[1].plot(df['timestamp'], df['tank_level_ft'])
ax[1].set_ylabel('Level (ft)')
ax[1].grid(True)

ax[2].plot(df['timestamp'], df['mass_balance_error_pct'])
ax[2].set_ylabel('Balance Error (%)')
ax[2].set_xlabel('Time')
ax[2].grid(True)
ax[2].axhline(1.0, color='r', linestyle='--', label='1% threshold')

plt.tight_layout()
plt.show()
```

---

### **Example 2: CSTR Energy Balance**

**Process:** Continuous stirred tank reactor with heating/cooling

**DoF Analysis:**
- Variables: Fin, Tin, Tout, Q, V (5)
- Equations: Mass balance, Energy balance (2)
- DoF = 5 - 2 = 3 → Specify Fin, Tin, Q

```yaml
simulation:
  entities: ["R-201"]
  start_time: "2024-01-01 00:00:00"
  timestep: "1min"
  row_count: 600
  seed: 42

  columns:
    # ─────────────────────────────────
    # INPUTS (3 DoF)
    # ─────────────────────────────────
    - name: feed_flow_gpm
      data_type: float
      unit: gpm
      generator:
        type: constant
        value: 50.0

    - name: feed_temp_f
      data_type: float
      unit: degF
      generator:
        type: random_walk
        start: 70.0
        min: 65.0
        max: 75.0
        step_size: 0.5

    - name: heat_input_btu_min
      data_type: float
      unit: BTU/min
      generator:
        type: random_walk
        start: 1000.0
        min: 0.0
        max: 2000.0
        step_size: 50.0

    # ─────────────────────────────────
    # CALCULATED (2 equations, 0 DoF)
    # ─────────────────────────────────

    # From mass balance: V constant, so Fout = Fin
    - name: outlet_flow_gpm
      data_type: float
      unit: gpm
      generator:
        type: derived
        expression: "feed_flow_gpm"

    # From energy balance: V*ρ*cp*dT/dt = Fin*ρ*cp*(Tin-T) + Q
    - name: reactor_temp_f
      data_type: float
      unit: degF
      generator:
        type: derived
        expression: |
          # Energy balance (simplified: constant density/cp)
          V_gal = 500.0
          rho_lb_per_gal = 8.34  # water
          cp_btu_per_lb_f = 1.0

          prev_temp = prev('reactor_temp_f', 70.0)

          # Accumulation term
          mass = V_gal * rho_lb_per_gal

          # Convection term (inlet - outlet)
          conv_in = feed_flow_gpm * rho_lb_per_gal * cp_btu_per_lb_f * feed_temp_f
          conv_out = outlet_flow_gpm * rho_lb_per_gal * cp_btu_per_lb_f * prev_temp

          # Heat input
          Q = heat_input_btu_min

          # dT/dt = (conv_in - conv_out + Q) / (mass * cp)
          dt_min = 1.0
          dT = (conv_in - conv_out + Q) / (mass * cp_btu_per_lb_f) * dt_min

          prev_temp + dT

    # ─────────────────────────────────
    # METADATA
    # ─────────────────────────────────
    - name: residence_time_min
      data_type: float
      unit: min
      generator:
        type: derived
        expression: "500.0 / feed_flow_gpm"  # V / F
```

---

## Data Engineering Focus: Schema Design with Units

**Key principle:** Always track units explicitly!

**Bad:**
```yaml
- name: temperature
  data_type: float
  # Is this °F? °C? K? Who knows!
```

**Good:**
```yaml
- name: temperature
  data_type: float
  unit: degF
  description: "Reactor outlet temperature"
```

**Why?**
- Prevents unit confusion (°F vs °C cost companies millions)
- Self-documenting data
- Enables automated unit conversion
- Helps validators catch nonsense values

**Validation with units:**
```yaml
validation:
  tests:
    - name: temperature_range_check
      type: range
      columns: ["reactor_temp_f"]
      min: 32.0   # Freezing (degF)
      max: 500.0  # Max operating temp
      on_fail: quarantine
```

---

## Validation: Quarantine Pattern

**What is quarantine?**
When data fails validation, route it to a separate location instead of:
- Silently dropping it (you lose visibility)
- Letting it pollute downstream (causes bad analysis)

**Odibi quarantine:**

```yaml
validation:
  tests:
    - name: level_overflow_check
      type: range
      columns: ["tank_level_ft"]
      min: 0.0
      max: 15.0
      on_fail: quarantine  # Bad rows go to quarantine
```

**Output:**
```
./output/tank_balance.parquet         # Good data
./output/tank_balance_quarantine/     # Bad data + reason
```

**Inspect quarantine:**

```python
import pandas as pd

quarantine = pd.read_parquet('./output/tank_balance_quarantine/')
print(quarantine[['timestamp', 'tank_level_ft', 'quarantine_reason', 'quarantine_test']])
```

**Why this matters:**
- In production pipelines, you NEED to know when data is bad
- Quarantine = audit trail for data quality issues
- You can investigate and fix upstream problems

---

## Exercises

### **E1: Count DoF**

For each system, count DoF and identify which variables to specify:

**a) Heat exchanger:**
- Variables: Tin, Tout, Tcoolant_in, Tcoolant_out, Fhot, Fcool
- Equations: Energy balance hot side, Energy balance cold side

**b) Two tanks in series:**
- Variables: Fin, F1, F2, L1, L2
- Equations: dL1/dt = ..., dL2/dt = ...

<details>
<summary>Answers</summary>

a) DoF = 6 - 2 = 4 → Specify Tin, Tcoolant_in, Fhot, Fcool

b) DoF = 5 - 2 = 3 → Specify Fin, and either (F1, F2) or (L1_setpoint via control)
</details>

---

### **E2: Add Quarantine to Example 1**

The tank can overflow! Modify `tank_mass_balance.yaml` to:

1. Add a validator that quarantines if level > 15 ft or level < 0 ft
2. Run the pipeline
3. Check if any rows were quarantined

**Hint:** Use the `range` validator with `on_fail: quarantine`.

---

### **E3: Implement Pump Power Calculation**

Add a derived column for pump power:

```
Power (hp) = (Flow × Pressure_rise × SG) / (3960 × Efficiency)
```

Where:
- Flow = outlet_flow_gpm
- Pressure_rise = 50 psi (constant)
- SG = 1.0 (water)
- Efficiency = 0.75

Then add a validator: power must be between 0 and 10 hp.

---

### **E4: Break the DoF**

Intentionally create a circular dependency:

```yaml
- name: level
  generator:
    type: derived
    expression: "prev('level', 10.0) + (inlet_flow - 50.0) / 100.0"

- name: inlet_flow
  generator:
    type: derived
    expression: "100.0 if level < 12.0 else 80.0"
```

Try to run it. What error do you get?

**Fix it:** Change `inlet_flow` to use `prev('level', 10.0)` instead of current `level`.

---

## Solutions

Full solutions: [../solutions/L02.md](../solutions/L02.md)

YAML files: `/examples/cheme_course/L02_dof_balances/`

---

## Reflection: How This Relates to Real Plants

**In a real plant:**
- Process engineers do DoF analysis during design (Can this be controlled?)
- Operators balance inputs (flows, heat) to achieve targets (level, temp)
- DCS historians track all variables with units and descriptions
- Bad sensor data (out-of-range) triggers alarms and may be ignored by controllers
- Mass/energy balances are used for reconciliation (detecting leaks, measurement errors)

**What you just learned:**
- DoF analysis prevents simulation errors (and real process design errors)
- Mass/energy balances = how processes actually behave
- Units matter (degF vs degC has caused disasters)
- Quarantine = how production pipelines handle bad data
- Topological sorting = how DAGs (Directed Acyclic Graphs) execute in order

**Red flags in plant data:**
- Temperature in °F but validator expects °C → quarantine
- Level increases but no inlet flow → mass balance violated, sensor broken
- Circular logic in control ("set flow based on level based on flow") → runaway

---

## Next Steps

**You now know:**
- ✅ DoF analysis to avoid circular dependencies
- ✅ Mass and energy balances in discrete time
- ✅ Schema design with units
- ✅ Validation tests and quarantine routing
- ✅ Topological sorting for derived columns

**Next lesson:**
👉 [L03: First-Order Dynamics](L03_first_order.md)

We'll implement first-order lag using `prev()` and `ema()`, understand sampling theory, and compute KPIs like rise time.

---

*Lesson L02 complete!*
