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

## Odibi Hands-On

### **Example 1: Mixing Tank (CV/MV/DV Mapping)**

**Process:** Neutralization tank
- **CV:** pH (target = 7.0)
- **MV:** Acid flow rate (gpm)
- **DV:** Feed pH (varies with upstream batches)

```yaml
# mixing_tank_ph.yaml
name: mixing_tank_ph_control
engine: pandas

connections:
  output:
    type: local
    path: ./output/mixing_tank_ph.parquet
    format: parquet

pipelines:
  - name: mixing_tank_ph_pipeline
    nodes:
      - name: generate_ph_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2024-01-01T00:00:00Z"
                timestep: "1min"
                row_count: 480  # 8 hours
                seed: 42

              entities:
                count: 1
                id_prefix: "TANK-"

              columns:
                - name: entity_id
                  data_type: string
                  generator:
                    type: constant
                    value: "{entity_id}"

                - name: timestamp
                  data_type: timestamp
                  generator:
                    type: timestamp

                # ─────────────────────────────────
                # DISTURBANCE VARIABLE (DV)
                # ─────────────────────────────────
                - name: feed_ph
                  data_type: float
                  generator:
                    type: random_walk
                    start: 8.5
                    drift: 0.0
                    noise: 0.1
                    min: 7.5
                    max: 9.5

                # ─────────────────────────────────
                # MANIPULATED VARIABLE (MV)
                # ─────────────────────────────────
                - name: acid_flow_gpm
                  data_type: float
                  generator:
                    type: random_walk
                    start: 2.0
                    drift: 0.0
                    noise: 0.1
                    min: 0.0
                    max: 5.0

                # ─────────────────────────────────
                # CONTROLLED VARIABLE (CV)
                # ─────────────────────────────────
                - name: tank_ph
                  data_type: float
                  generator:
                    type: derived
                    expression: >
                      prev('tank_ph', 7.0) +
                      0.1 * (feed_ph - prev('tank_ph', 7.0)) -
                      0.15 * acid_flow_gpm

                # ─────────────────────────────────
                # MEASUREMENT (with noise)
                # ─────────────────────────────────
                - name: sensor_noise
                  data_type: float
                  generator:
                    type: range
                    min: -0.05
                    max: 0.05
                    distribution: normal

                - name: ph_sensor
                  data_type: float
                  generator:
                    type: derived
                    expression: "tank_ph + sensor_noise"

                # ─────────────────────────────────
                # METADATA
                # ─────────────────────────────────
                - name: control_mode
                  data_type: string
                  generator:
                    type: categorical
                    values: ["AUTO", "MANUAL"]
                    weights: [0.95, 0.05]

        write:
          connection: output
```

**Working example:** [/examples/cheme_course/L01_cv_mv_dv/mixing_tank_ph.yaml](file:///d:/odibi/examples/cheme_course/L01_cv_mv_dv/mixing_tank_ph.yaml)

### **Run the Pipeline:**

```bash
cd examples/cheme_course/L01_cv_mv_dv
odibi run mixing_tank_ph.yaml
```

**What just happened:**
1. Odibi created 480 rows of data (8 hours at 1-minute intervals)
2. Feed pH varies randomly (DV)
3. Acid flow varies (MV)
4. Tank pH responds to both inputs (CV)
5. 95% AUTO mode, 5% MANUAL (operator intervention)

---

### **Example 2: Heat Exchanger (Multiple CVs)**

**Process:** Shell-and-tube heat exchanger
- **CV1:** Outlet temperature (primary)
- **CV2:** Pressure drop (secondary constraint)
- **MV:** Coolant flow rate
- **DV:** Inlet temperature (from upstream)

**Working example:** [/examples/cheme_course/L01_cv_mv_dv/heat_exchanger.yaml](file:///d:/odibi/examples/cheme_course/L01_cv_mv_dv/heat_exchanger.yaml)

**Key insight:** You can have multiple CVs! Often there's a primary CV (temperature) and constraints (pressure drop < 40 psi).

---

## Exercises

See [../solutions/L01.md](../solutions/L01.md) for full solutions.

All YAML files: [/examples/cheme_course/L01_cv_mv_dv/](file:///d:/odibi/examples/cheme_course/L01_cv_mv_dv/)

---

## Next Steps

**You now know:**
- ✅ CV/MV/DV mapping to data schemas
- ✅ Generating realistic process variable relationships
- ✅ Adding metadata columns (control mode)

**Next lesson:**
👉 [L02: Degrees of Freedom + Mass/Energy Balances](L02_dof_balances.md)

We'll learn to count DoF, avoid circular dependencies, and add physical unit columns with validation.

---

*Lesson L01 complete!*
