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

**GOOD:**
```yaml
columns:
  - name: inlet_flow
    data_type: float
    generator:
      type: random_walk  # INPUT (generator)
      start: 50.0
      drift: 0.0
      noise: 2.0
      min: 40.0
      max: 60.0

  - name: outlet_flow
    data_type: float
    generator:
      type: constant  # INPUT (generator)
      value: 45.0

  - name: level
    data_type: float
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
name: tank_mass_balance
engine: pandas

connections:
  output:
    type: local
    path: ./output/tank_balance.parquet
    format: parquet

pipelines:
  - name: tank_balance_pipeline
    nodes:
      - name: generate_tank_data
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
                id_prefix: "TK-"

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
                # INPUTS (2 DoF)
                # ─────────────────────────────────
                - name: inlet_flow_gpm
                  data_type: float
                  generator:
                    type: random_walk
                    start: 100.0
                    drift: 0.0
                    noise: 2.0
                    min: 80.0
                    max: 120.0

                - name: outlet_flow_gpm
                  data_type: float
                  generator:
                    type: random_walk
                    start: 95.0
                    drift: 0.0
                    noise: 1.5
                    min: 80.0
                    max: 110.0

                # ─────────────────────────────────
                # CALCULATED (0 DoF remaining)
                # ─────────────────────────────────
                - name: tank_level_ft
                  data_type: float
                  generator:
                    type: derived
                    expression: >
                      prev('tank_level_ft', 10.0) +
                      (inlet_flow_gpm - outlet_flow_gpm) / (100.0 * 7.48)

                # ─────────────────────────────────
                # VALIDATION COLUMNS
                # ─────────────────────────────────
                - name: mass_balance_error_pct
                  data_type: float
                  generator:
                    type: derived
                    expression: >
                      abs((tank_level_ft - prev('tank_level_ft', 10.0)) * 100.0 * 7.48 -
                      (inlet_flow_gpm - outlet_flow_gpm)) / (inlet_flow_gpm + 0.001) * 100.0

        validate:
          tests:
            - type: range
              column: inlet_flow_gpm
              min: 0.0
              max: 200.0

            - type: range
              column: outlet_flow_gpm
              min: 0.0
              max: 200.0

            - type: range
              column: tank_level_ft
              min: 0.0
              max: 20.0

        write:
          connection: output
```

**Working example:** [/examples/cheme_course/L02_dof_balances/tank_mass_balance.yaml](file:///d:/odibi/examples/cheme_course/L02_dof_balances/tank_mass_balance.yaml)

**Run and verify:**

```bash
cd examples/cheme_course/L02_dof_balances
odibi run tank_mass_balance.yaml
```

---

### **Example 2: CSTR Energy Balance**

**Process:** Continuous stirred tank reactor with heating/cooling

**DoF Analysis:**
- Variables: Fin, Tin, Tout, Q, V (5)
- Equations: Mass balance, Energy balance (2)
- DoF = 5 - 2 = 3 → Specify Fin, Tin, Q

**Working example:** [/examples/cheme_course/L02_dof_balances/cstr_energy_balance.yaml](file:///d:/odibi/examples/cheme_course/L02_dof_balances/cstr_energy_balance.yaml)

---

## Exercises

See [../solutions/L02.md](../solutions/L02.md) for full solutions.

All YAML files: [/examples/cheme_course/L02_dof_balances/](file:///d:/odibi/examples/cheme_course/L02_dof_balances/)

---

## Next Steps

**You now know:**
- ✅ DoF analysis to avoid circular dependencies
- ✅ Mass and energy balances in discrete time
- ✅ Validation tests and quarantine routing

**Next lesson:**
👉 [L03: First-Order Dynamics](L03_first_order.md)

We'll implement first-order lag using `prev()` and `ema()`, understand sampling theory, and compute KPIs like rise time.

---

*Lesson L02 complete!*
