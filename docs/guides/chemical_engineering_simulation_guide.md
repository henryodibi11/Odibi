# Chemical Engineering Simulation Guide
## Beginner to Advanced: Using Odibi for Process Data

**Author:** AI Assistant + Oracle Analysis  
**Date:** March 11, 2026  
**For:** Chemical Engineers in Operations/Analytics

---

## Executive Summary

This guide shows how to simulate realistic chemical plant data using Odibi's simulation framework. Your unique combination of chemical engineering + data engineering skills enables you to create **genuinely differentiated demo content** that stands out from typical e-commerce/clickstream examples.

**Key Approach:**
- Treat each unit (reactor, column, heat exchanger, pump) or tag as an "entity"
- Use timestamp + entity-based RNG for deterministic, realistic variability
- Compute physics with derived columns (mass/energy balances, kinetics, simple thermo)
- Add slow dynamics via random_walk generator
- Layer chaos (outliers, downtime) to mimic real sensors

---

## Table of Contents

1. [Quick Start Path](#quick-start-path)
2. [Beginner to Advanced Progression](#beginner-to-advanced-progression)
3. [Example Scenarios with YAML](#example-scenarios)
4. [ChemE Concepts → Odibi Features Mapping](#cheme-concepts-mapping)
5. [Simulator Gaps & Missing Features](#simulator-gaps)
6. [Advanced Topics](#advanced-topics)

---

## Quick Start Path

### 10-Step Recipe for Realistic Plant Data

1. **Pick granularity**: Equipment-as-entity (fewer, multi-tag per entity) or tag-as-entity (many entities, 1-3 tags each). For plant demos, equipment-as-entity is clearer.

2. **Define scope**:
   - `start_time`, `timestep` (e.g., 1m/5m), `row_count` (or hours per run), `seed`
   - Use `incremental` + HWM on timestamp for continuous runs

3. **Identify "given" inputs per unit**:
   - Feeds, compositions, ambient, setpoints, valve states
   - Equipment constants (volume, UA, efficiency)

4. **Choose base variability**:
   - Static/random within ranges: `range`/`categorical`/`boolean`
   - Slow time evolution: `random_walk` for PV-like trends/drift/noise

5. **Compute physics in derived** (row-wise, topologically sorted):
   - Mass balances, conversions, splits
   - Energy balances with simple correlations (cp constants, UA effectiveness)
   - Kinetics via Arrhenius and rate laws
   - Simple thermo (Antoine, Raoult approximations)

6. **Add control behavior**:
   - Represent SP and OP
   - Start with proportional control in derived: `OP = clamp(Kp*(SP - PV), low, high)`
   - If using random_walk, let PV drift slowly but bias toward SP for first-order feel

7. **Instrumentation and events**:
   - Status/alarms: booleans derived from thresholds
   - Mode/recipe/grade: categorical
   - Measurement noise: range with small min/max

8. **Realism via chaos** (applied after derived):
   - `outlier_rate` (+factor), `duplicate_rate`, `downtime_rate`
   - Use `null_rate` on columns to emulate dropouts per-signal

9. **Determinism and scale**:
   - Rely on entity-based seeding for reproducibility
   - Use engine-agnostic YAML; generate on Pandas for quick dev

10. **Validate physical plausibility**:
    - Add tests/gates (e.g., flows ≥ 0, compositions in [0,1], energy balances within tolerance)
    - Route bad rows to quarantine if demoing data engineering rigor

---

## Beginner to Advanced Progression

### Beginner: Single-Unit Steady-State Snapshots

**Goal:** Realistic, row-wise snapshot series (each row independent)

**Use:**
- `range` for inputs (flows/temps/compositions)
- `derived` for balances and simple kinetics
- `timestamp` for time series
- `constant` for equipment metadata
- Optional small noise via `range`
- Add chaos outliers at 1-2% to show cleaning/validation later

**Example:** Tank with mass balance, simple overflow logic

---

### Intermediate: Quasi-Dynamic Unit Behavior

**Goal:** Show autocorrelation, drift, and control response

**Add:**
- `random_walk` on main PVs to show autocorrelation and drift
- Setpoint steps (derived using timestamp to switch SP)
- Simple proportional control in derived
- Alarms/limits as booleans
- Use `incremental` with HWM to produce day-by-day runs

**Example:** CSTR with temperature control, reaction kinetics

---

### Advanced: Flowsheet Snippets and Closed-Loop Flavor

**Goal:** Multi-unit interactions, realistic degradation, analytics demos

**Features:**
- Chain units by reusing same "feed" columns across entities
- Add slow degradation (e.g., UA fouling, tray efficiency) via random_walk
- Model cleaning events with downtime or derived step resets
- Showcase analytics tasks:
  - Soft sensors (derived)
  - Anomaly flags
  - SPC metrics
  - Bad-actor chaos (sensor freezing, spikes, upset windows)

**Example:** Distillation train with feed-effluent heat exchanger, product quality control

---

## Example Scenarios

### Scenario A: CSTR with First-Order Reaction and P-Control

**Physics:**
- Residence time: τ = V/F
- Rate constant: k = k₀ × exp(-Ea/RT)
- Outlet concentration: C_out = C_in/(1 + kτ)
- Proportional coolant valve OP tracks C_out setpoint
- PV has slow drift/noise

```yaml
read:
  format: simulation
  options:
    simulation:
      scope:
        start_time: "2026-01-01T00:00:00Z"
        timestep: "1m"
        row_count: 1440  # One day at 1-min intervals
        seed: 42

      entities:
        count: 1
        id_prefix: "R_"

      columns:
        - name: reactor_id
          data_type: string
          generator: {type: constant, value: "{entity_id}"}

        - name: timestamp
          data_type: timestamp
          generator: {type: timestamp}

        # Equipment constants
        - name: V_m3
          data_type: float
          generator: {type: constant, value: 5.0}

        - name: k0
          data_type: float
          generator: {type: constant, value: 2.0e5}

        - name: Ea_over_R
          data_type: float
          generator: {type: constant, value: 8000}  # Ea/R in K

        # Process inputs with variability
        - name: F_in_m3_min
          data_type: float
          generator: {type: range, min: 0.08, max: 0.12}  # residence time ~50-62.5 min

        - name: C_in_mol_m3
          data_type: float
          generator: {type: range, min: 1500, max: 1700}

        - name: T_K
          data_type: float
          generator: {type: range, min: 320, max: 330}

        # Slow PV drift baseline (realistic measurement behavior)
        - name: pv_drift
          data_type: float
          generator: {type: random_walk, start: 1.0, drift: 0.0, noise: 0.01, min: 0.95, max: 1.05}

        # Derived physics calculations
        - name: tau_min
          data_type: float
          generator: {type: derived, expression: "V_m3 / F_in_m3_min"}

        - name: k
          data_type: float
          generator: {type: derived, expression: "k0 * (2.718281828 ** (-Ea_over_R / T_K))"}

        - name: C_out_raw
          data_type: float
          generator: {type: derived, expression: "C_in_mol_m3 / (1 + k * tau_min)"}

        - name: C_out
          data_type: float
          generator: {type: derived, expression: "C_out_raw * pv_drift"}

        # Control system
        - name: C_sp
          data_type: float
          generator: {type: range, min: 800, max: 900}

        - name: Kp
          data_type: float
          generator: {type: constant, value: 0.05}

        - name: valve_op_pct
          data_type: float
          generator: {type: derived, expression: "max(0, min(100, Kp * (C_sp - C_out) + 50))"}

      # Realistic sensor/process issues
      chaos:
        outlier_rate: 0.005
        outlier_factor: 5.0
        duplicate_rate: 0.002
        downtime_rate: 0.001
```

---

### Scenario B: Binary Distillation Column

**Physics:**
- Two-key component split with reflux influence
- Tray efficiency drift over time (fouling)
- Simplified split equations:
  - Distillate A flow = F × z_A × RL
  - Distillate B flow = F × (1-z_A) × (1-RH)
  - Recovery influenced by reflux ratio and tray efficiency

```yaml
read:
  format: simulation
  options:
    simulation:
      scope:
        start_time: "2026-01-01T00:00:00Z"
        timestep: "5m"
        row_count: 1000
        seed: 7

      entities:
        count: 1
        id_prefix: "T_"

      columns:
        - name: column_id
          data_type: string
          generator: {type: constant, value: "{entity_id}"}

        - name: timestamp
          data_type: timestamp
          generator: {type: timestamp}

        # Feed and operating parameters
        - name: F_kmol_h
          data_type: float
          generator: {type: range, min: 90, max: 110}

        - name: zA
          data_type: float
          generator: {type: range, min: 0.35, max: 0.45}  # feed light key fraction

        - name: reflux_ratio
          data_type: float
          generator: {type: range, min: 1.5, max: 2.5}

        - name: RL_base
          data_type: float
          generator: {type: constant, value: 0.94}  # LK recovery in distillate

        - name: RH
          data_type: float
          generator: {type: constant, value: 0.98}  # HK recovery in bottoms

        # Tray efficiency degradation over time (fouling)
        - name: eff_tray
          data_type: float
          generator: {type: random_walk, start: 0.8, drift: -0.00002, noise: 0.002, min: 0.6, max: 0.9}

        # Reflux effect on recovery
        - name: RL
          data_type: float
          generator: {type: derived, expression: "max(0.5, min(0.995, RL_base * eff_tray * (0.8 + 0.1 * reflux_ratio)))"}

        # Component flows and compositions
        - name: D_A
          data_type: float
          generator: {type: derived, expression: "F_kmol_h * zA * RL"}

        - name: D_B
          data_type: float
          generator: {type: derived, expression: "F_kmol_h * (1 - zA) * (1 - RH)"}

        - name: D
          data_type: float
          generator: {type: derived, expression: "D_A + D_B"}

        - name: xD_A
          data_type: float
          generator: {type: derived, expression: "safe_div(D_A, D, 0.0)"}

        - name: B
          data_type: float
          generator: {type: derived, expression: "F_kmol_h - D"}

        # Proportional control on reflux to meet distillate composition
        - name: xD_sp
          data_type: float
          generator: {type: range, min: 0.92, max: 0.96}

        - name: R_Kp
          data_type: float
          generator: {type: constant, value: 5.0}

        - name: reflux_cmd
          data_type: float
          generator: {type: derived, expression: "max(1.0, min(3.0, reflux_ratio + R_Kp * (xD_sp - xD_A)))"}

      chaos:
        outlier_rate: 0.01
        downtime_rate: 0.002
```

---

### Scenario C: Heat Exchanger with Fouling

**Physics:**
- Counter-current heat exchange
- Effectiveness method: T_c_out = T_c_in + eff × (T_h_in - T_c_in)
- T_h_out from energy balance
- Effectiveness drifts down due to fouling
- Periodic cleaning resets effectiveness

```yaml
read:
  format: simulation
  options:
    simulation:
      scope:
        start_time: "2026-01-01T00:00:00Z"
        timestep: "2m"
        row_count: 720
        seed: 21

      entities:
        count: 1
        id_prefix: "E_"

      columns:
        - name: hx_id
          data_type: string
          generator: {type: constant, value: "{entity_id}"}

        - name: timestamp
          data_type: timestamp
          generator: {type: timestamp}

        # Stream properties
        - name: m_dot_h_kg_s
          data_type: float
          generator: {type: range, min: 4.5, max: 5.5}

        - name: m_dot_c_kg_s
          data_type: float
          generator: {type: range, min: 3.0, max: 4.0}

        - name: Th_in_C
          data_type: float
          generator: {type: range, min: 120, max: 140}

        - name: Tc_in_C
          data_type: float
          generator: {type: range, min: 20, max: 30}

        - name: cp_h_kJ_kgK
          data_type: float
          generator: {type: constant, value: 4.0}

        - name: cp_c_kJ_kgK
          data_type: float
          generator: {type: constant, value: 4.2}

        # Effectiveness degradation (fouling) with drift
        - name: eff
          data_type: float
          generator: {type: random_walk, start: 0.7, drift: -0.0001, noise: 0.005, min: 0.4, max: 0.85}

        # Cleaning event logic (simplified - can be enhanced)
        - name: cleaning
          data_type: boolean
          generator: {type: boolean, p_true: 0.0}  # Set explicitly or use chaos downtime

        - name: eff_eff
          data_type: float
          generator: {type: derived, expression: "0.8 if cleaning else eff"}  # reset to 0.8 when cleaned

        # Heat exchange calculations
        - name: Tc_out_C
          data_type: float
          generator: {type: derived, expression: "Tc_in_C + eff_eff * (Th_in_C - Tc_in_C)"}

        - name: Q_kW
          data_type: float
          generator: {type: derived, expression: "m_dot_c_kg_s * cp_c_kJ_kgK * (Tc_out_C - Tc_in_C)"}  # kJ/s == kW

        - name: Th_out_C
          data_type: float
          generator: {type: derived, expression: "Th_in_C - safe_div(Q_kW, m_dot_h_kg_s * cp_h_kJ_kgK, 0.0)"}

      chaos:
        outlier_rate: 0.01
        duplicate_rate: 0.003
        downtime_rate: 0.002
```

---

## ChemE Concepts Mapping

### Mass Balances
- **Odibi:** `derived` columns with algebraic equations
- **Usage:** Use `safe_div` for null/zero handling
- **Chaining:** Topological sort lets you chain balances (feed → unit → product)

### Energy Balances
- **Odibi:** `derived` with cp, UA, simple effectiveness
- **Usage:** Compute duties and outlet temperatures
- **Constants:** Scalar constants via `constant` generator per entity

### Reaction Kinetics
- **Odibi:** `derived` Arrhenius equations
- **Examples:**
  - `k = k0 * exp(-Ea/RT)` (Arrhenius)
  - Rate laws: `k*C^n`, Langmuir-Hinshelwood
  - Steady-state conversions: `C_out = C_in/(1 + k*tau)`

### Thermodynamics (Simplified)
- **Odibi:** `derived` using Antoine and Raoult's law approximations
- **Define:** Component constants per entity
- **Note:** No built-in property package—keep formulas simple

### Control Systems
- **Odibi:** `derived` for SP, PV, OP
- **Proportional:** `OP = clamp(Kp*(SP - PV) + bias, low, high)`
- **Dynamics:** Use `random_walk` for PV with small noise and slow drift
- **Mean reversion:** Adjust drift toward SP if supported

### Instrumentation & Events
- **Modes/grades:** `categorical` generator
- **Statuses/alarms:** `boolean` derived from thresholds
- **Measurement noise:** `range` with small spread
- **Sensor issues:** `chaos` for spikes, duplicates, dropouts
- **Per-signal missingness:** `null_rate` on individual columns

### Plant Variability
- **Entity-based RNG:** Each unit behaves consistently across runs
- **Personality:** Override constants/ranges per entity
- **Reproducibility:** Seed ensures deterministic generation

---

## Simulator Gaps

### High Priority (Implement First)

#### 1. Stateful Dynamics Across Rows [1-2 days]

**Problem:** Can't model true ODE-like updates or autocorrelation without access to previous row values

**Needed:**
- `prev(col, default)` function in derived expressions
- `ema(col, alpha)` for exponential moving average
- Per-entity state memory

**Example Use Case:**
```python
# First-order lag response
PV_t = PV_{t-1} + alpha * (SP - PV_{t-1})

# Derived expression would be:
"prev(PV, 0) + 0.1 * (SP - prev(PV, 0))"
```

**Impact:** Enables realistic controller response, ramp limits, first-order lags

---

#### 2. Built-in Control Helper [1-3 hours]

**Problem:** PI/PID behavior is clunky and not reproducible across rows without state

**Needed:**
```python
pid(pv, sp, Kp, Ki, Kd, dt, anti_windup=True, low=0, high=100)
```

**Impact:** Clean, correct control simulation without manual integral/derivative tracking

---

### Medium Priority

#### 3. Cross-Variable Correlation [1-3 hours]

**Problem:** Can't enforce realistic correlations (e.g., flow and pressure, top and bottom compositions)

**Needed:**
- Multivariate normal sampling with Cholesky decomposition
- Correlation matrix specification for `range` generators at each timestep

**Impact:** More realistic multivariate process data for ML demos

---

#### 4. Mini Thermo/Property Pack [1-3 hours]

**Problem:** No built-in property data; users must hard-code correlations

**Needed:**
- Antoine coefficients library for common components
- Simple Cp(T) polynomials for common fluids
- Ideal gas density
- Raoult's law helper for binary VLE
- Steam tables lite (or approximate water correlations)

**Alternative:** Optional CoolProp plugin (guarded import), but consider dependency philosophy

**Impact:** Faster, more accurate thermodynamic calculations

---

#### 5. Seasonality and Structured Patterns [1-3 hours]

**Problem:** No easy way to add diurnal/weekly patterns, ramps, steps

**Needed:**
- Helpers for sine waves, ramps, step functions based on time
- Useful for utilities (steam pressure, cooling water temp) and ambient effects

---

#### 6. Actuator/Sensor Non-Idealities [Few hours to 1 day]

**Problem:** Chaos is random; real sensors have structured failures

**Needed:**
- Drift (bias ramp over time)
- Hysteresis/deadband
- Saturation
- Resolution/quantization
- Stuck-at values
- Delayed response

**Current Workaround:** Can partially do with derived + chaos, but packaged knobs would help

---

### Low to High Priority

#### 7. Cross-Entity References (Streams Between Units) [1-2 days+]

**Problem:** Can't model streams flowing from one unit to another

**Needed:**
- Allow column in entity B to reference column from entity A at same timestamp
- Optional transport delay parameter
- Start simple: same-pipeline "upstream_of" binding with deterministic ordering
- Later: explicit lag/delay

**Impact:** Essential for flowsheet-level simulation

---

#### 8. Coherent "Upset Windows" [1-3 hours]

**Problem:** `chaos` applies independently to each column; real upsets affect multiple tags coherently

**Needed:**
- Declare time windows where multiple tags coherently deviate
- Example: Trip events where flow, pressure, and temperature all spike together

---

#### 9. Time-Varying/Event-Driven Timesteps [1-2 days]

**Problem:** Fixed timestep limits batch operations and event modeling

**Needed:**
- Poisson/event arrivals
- Scheduled steps at specific times
- Variable timestep support

**Impact:** Better batch reactor, startup/shutdown, upset modeling

---

### Lower Priority

#### 10. Validation Presets for Physics [Few hours]

**Needed:**
- Built-in tests: compositions sum to 1 ± ε
- Energy balance residual < ε
- Temperatures in plausible ranges

**Impact:** Helpful for demos, prevents nonsensical outputs

---

#### 11. Streaming/Large-Scale Generation [P2 Backlog]

**Status:** Currently okay for most demos (<10M rows)

---

## Advanced Topics

### When to Consider Advanced Path

You need the advanced path when:
- Modeling dynamic step responses, controller tuning studies
- Transport delays are critical
- Must conserve mass/energy across multi-unit flowsheet with streams
- Need realistic multivariate correlations for ML demos (soft sensors, PCA/PLS)

### Advanced Path Outline

1. **Add stateful helpers:**
   - `prev()`, `ema()`, `pid()` with per-entity memory
   - Expose `time_step_seconds` to derived expressions

2. **Implement mean-reverting random_walk:**
   - `x_t = x_{t-1} + θ*(μ_t - x_{t-1}) + σ*ε_t`
   - Where μ_t can reference a column (e.g., SP)

3. **Cross-entity "link" binding:**
   - Columns can pull upstream entity values at same timestamp
   - Optional delay parameter for transport lags

4. **Tiny property library module:**
   - Common constants and safe functions in derived namespace
   - Guard against heavy dependencies

### Effort Estimates

- **Short-term demo-ready** (current features): Few hours
  - Create 3-5 compelling unit examples

- **Add stateful helpers + mean-reverting random_walk + basic correlation**: 1-2 days

- **Cross-entity streams with delays + mini-thermo pack**: Multi-day effort

---

## Best Practices

### Configuration

1. **Keep derived expressions readable**
   - Extract constants as separate columns
   - Don't overfit complexity into single expressions

2. **Validate physical ranges**
   - Use tests/gates to prevent nonsensical outputs
   - Check: flows ≥ 0, compositions in [0,1], energy balance closure

3. **Use proper timestamps**
   - Zulu format (already enforced)
   - Positive timesteps (already enforced)
   - Explicit seeds for reproducible demos

4. **Conservative drift/noise**
   - Large chaos factors can break balances
   - Chaos applied after derived (as designed)

### Development Workflow

1. **Start with Pandas** for quick iteration
2. **Use entity-based seeding** for reproducibility
3. **Test incrementally:**
   - Single entity first
   - Add complexity gradually
   - Validate physics at each step

4. **Quarantine workflow:**
   - Route physically impossible rows to quarantine
   - Demonstrates data quality gates

---

## Rationale and Trade-offs

### Why This Path Works

- Odibi already excels at row-wise derived math with deterministic variability
- Can represent most steady-state ChemE relations today
- `random_walk` + `chaos` add lifelike texture
- Keeps config-only, reproducible demos aligned with YAML-first philosophy

### Trade-offs

- **Limited true dynamics:** Without state across rows, closed-loop fidelity is approximate
- **Small feature adds deliver big value:** `prev()`, `pid()`, correlation support would unlock realistic dynamic behavior with modest effort
- **Cross-entity streams:** Needed for true flowsheet simulation but more complex to implement

---

## Next Steps

1. **Try the examples** above (CSTR, distillation, heat exchanger)
2. **Identify your priority gap** (likely stateful dynamics or control helpers)
3. **Build 2-3 compelling demos** showcasing your unique ChemE+DE skills
4. **Share for feedback** - this content will stand out!

---

## Questions?

This guide was generated by AI analysis of your Odibi simulator. For specific implementation questions:
- Check `docs/ODIBI_DEEP_CONTEXT.md` for comprehensive framework docs
- Use `odibi explain simulation` for CLI introspection
- Review `SIMULATION_IMPLEMENTATION_OVERVIEW.md` for technical details
