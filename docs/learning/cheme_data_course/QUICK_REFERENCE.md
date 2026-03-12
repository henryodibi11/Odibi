# ChemE Course Quick Reference

**One-page guide to the complete course**

---

## Course At-A-Glance

| Part | Lessons | Hours | Topics |
|------|---------|-------|--------|
| **I: Foundations** | L00-L05 | ~5.5h | Time-series, CV/MV/DV, mass balances, dynamics |
| **II: Feedback Control** | L06-L09 | ~4.5h | PID, tuning, disturbances, system ID |
| **III: Advanced** | L10-L14 | ~5.5h | Cascade, feedforward, constraints |
| **Capstone** | L15 | ~2.5h | CSTR digital twin (Bronze/Silver/Gold) |
| **TOTAL** | **16 lessons** | **~18h** | **35+ YAML examples** |

---

## Essential Functions

### **pid() - PID Controller**
```yaml
- name: controller_output
  data_type: float
  generator:
    type: derived
    expression: >
      pid(
        pv=measured_value,
        sp=setpoint,
        Kp=2.0,
        Ki=0.1,
        Kd=0.5,
        dt=60,
        output_min=0,
        output_max=100,
        anti_windup=True
      )
```

### **prev() - Previous Row Value**
```yaml
- name: tank_level
  data_type: float
  generator:
    type: derived
    expression: >
      prev('tank_level', 50.0) +
      (inlet_flow - outlet_flow) / 100.0
```

### **ema() - Exponential Moving Average**
```yaml
- name: smoothed_temp
  data_type: float
  generator:
    type: derived
    expression: "ema('raw_temp', alpha=0.2, default=raw_temp)"
```

---

## Controller Tuning Quick Reference

### **Ziegler-Nichols (FOPTD: K, τ, θ)**

| Type | Kp | Ti | Td |
|------|----|----|-----|
| P | τ/(K·θ) | ∞ | 0 |
| PI | 0.9τ/(K·θ) | θ/0.3 | 0 |
| PID | 1.2τ/(K·θ) | 2θ | 0.5θ |

**Convert:** Ki = Kp/Ti, Kd = Kp·Td

### **IMC/Direct Synthesis**
```
Kp = τ / (K·(λ + θ))
Ti = τ
Td = 0

Ki = Kp / Ti
Kd = 0
```

**Tuning knob:** λ
- λ = θ → Balanced
- λ = 3θ → Conservative (recommended)

---

## Common YAML Structure

```yaml
nodes:
  - name: simulation_node
    read:
      format: simulation
      options:
        simulation:
          scope:
            start_time: "2024-01-01T00:00:00Z"
            timestep: "1min"
            row_count: 300
            seed: 42

          entities:
            count: 1
            id_prefix: "ENTITY-"

          columns:
            # 1. Identifiers
            - name: entity_id
              data_type: string
              generator:
                type: constant
                value: "{entity_id}"

            - name: timestamp
              data_type: timestamp
              generator:
                type: timestamp

            # 2. Parameters
            - name: setpoint
              data_type: float
              generator:
                type: constant
                value: 50.0

            # 3. State variables (need prev())
            - name: process_value
              data_type: float
              generator:
                type: derived
                expression: "prev('process_value', 50.0)"

            # 4. Controller output
            - name: control_output
              data_type: float
              generator:
                type: derived
                expression: >
                  pid(
                    pv=process_value,
                    sp=setpoint,
                    Kp=2.0,
                    Ki=0.1,
                    Kd=0.0,
                    dt=60,
                    output_min=0,
                    output_max=100,
                    anti_windup=True
                  )

            # 5. Process dynamics
            - name: process_value_next
              data_type: float
              generator:
                type: derived
                expression: >
                  prev('process_value', 50.0) +
                  0.1 * (control_output - prev('process_value', 50.0))

            # 6. Update state
            - name: process_value
              data_type: float
              generator:
                type: derived
                expression: "process_value_next"

    write:
      connection: local
      format: parquet
      path: data/output/simulation.parquet
      mode: overwrite
```

---

## Process Models

### **First-Order**
```
dY/dt = (K·u - Y) / τ
Discrete: Y[k+1] = Y[k] + (dt/τ)·(K·u - Y[k])
```

### **FOPTD (First-Order Plus Dead Time)**
```
G(s) = K·e^(-θs) / (τs + 1)
Dead time: Use prev() with delay buffer
```

### **Second-Order**
```
d²Y/dt² + 2ζωn·dY/dt + ωn²·Y = ωn²·u
State-space form (use dx1, dx2)
```

### **Mass Balance**
```
dV/dt = F_in - F_out
Discrete: V[k+1] = V[k] + (F_in - F_out)·dt
```

### **Energy Balance**
```
dT/dt = F/V·(T_in - T) + Q/(ρ·Cp·V)
```

---

## Performance Metrics

### **IAE (Integral Absolute Error)**
```yaml
- name: iae
  data_type: float
  generator:
    type: derived
    expression: "prev('iae', 0) + abs(error) * dt"
```

### **Overshoot**
```
Overshoot% = (Peak - Final) / (Final - Initial) × 100
```

### **Settling Time**
```
Time to stay within ±2% of final value
```

### **Rise Time**
```
Time from 10% to 90% of final value
```

---

## Lesson Roadmap

### **Foundations (L00-L05)**
- L00: Setup & data formats
- L01: CV/MV/DV variables
- L02: DOF & mass balances
- L03: First-order systems
- L04: FOPTD + dead time
- L05: Second-order + overshoot

### **Feedback Control (L06-L09)**
- L06: **PID basics** ← Start here for control
- L07: **Tuning methods** (ZN, IMC)
- L08: Disturbances + setpoints
- L09: System identification

### **Advanced (L10-L14)**
- L10: Interacting loops
- L11: **Cascade control**
- L12: Feedforward + ratio
- L13: Nonlinearity + saturation
- L14: MPC-lite (constraints)

### **Capstone (L15)**
- L15: **CSTR Digital Twin** ← Production-ready example

---

## CLI Commands

```bash
# Validate YAML
odibi validate examples/cheme_course/L06_pid_basics/tank_pi.yaml

# Run simulation
odibi run examples/cheme_course/L06_pid_basics/tank_pi.yaml

# List available transformers
odibi list transformers

# Get function help
odibi explain pid

# Validate all course examples
python scripts/validate_cheme_course.py
```

---

## Common Patterns

### **Step Disturbance**
```yaml
- name: disturbance
  data_type: float
  generator:
    type: derived
    expression: "100.0 if minutes_elapsed < 60 else 110.0"
```

### **Setpoint Change**
```yaml
- name: setpoint
  data_type: float
  generator:
    type: derived
    expression: "50.0 if seconds_elapsed < 600 else 55.0"
```

### **Rate Limit**
```yaml
- name: valve_change
  data_type: float
  generator:
    type: derived
    expression: "max(-2.0, min(2.0, requested_change))"
```

### **Dead Time (Simple)**
```yaml
- name: delayed_input
  data_type: float
  generator:
    type: derived
    expression: "prev('input', 0) if seconds_elapsed >= 120 else 0"
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `prev()` returns None | Initialize with default: `prev('col', 50.0)` |
| PID unstable | Reduce Kp, check sign of error (SP - PV) |
| No output file | Check `write: path:` exists, create directory |
| Negative values | Use `max(0, expression)` to clamp |
| Circular dependency | Ensure state updates use `_next` pattern |

---

## Example Snippets

### **Bronze → Silver → Gold Pipeline**
```yaml
# Bronze: Raw data with chaos
nodes:
  - name: bronze
    read:
      format: simulation
      options:
        simulation:
          # ... simulation config ...
          chaos:
            outlier_rate: 0.01
            duplicate_rate: 0.005
    write:
      path: data/output/bronze.parquet

# Silver: Validated data
  - name: silver
    read:
      connection: local
      format: parquet
      path: data/output/bronze.parquet
    validate:
      - name: range_check
        test: range
        column: temperature
        min: 50
        max: 100
        severity: error
        quarantine: true
    write:
      path: data/output/silver.parquet

# Gold: KPIs
  - name: gold
    read:
      connection: local
      format: parquet
      path: data/output/silver.parquet
    transform:
      - sql: >
          SELECT
            DATE_TRUNC('hour', timestamp) as hour,
            AVG(conversion_pct) as avg_conversion,
            SUM(production_kg) as total_production
          FROM __this__
          GROUP BY hour
    write:
      path: data/output/gold.parquet
```

---

## Quick Tips

1. **Always use `prev()` for state variables** - Required for integration, accumulation
2. **Match `dt` to timestep** - If timestep="30sec", use dt=30 in pid()
3. **Start with PI, not PID** - Derivative sensitive to noise, rarely needed
4. **Use anti-windup** - Essential when output can saturate (valves, heaters)
5. **Check your units** - Common mistake: mixing seconds and minutes
6. **Seed for reproducibility** - Same seed → same "random" data
7. **Clamp outputs** - Use `max(0, min(100, value))` for physical limits
8. **Bronze before Silver** - Don't skip validation layer in production

---

## Next Steps

1. **Start:** [Course README](README.md)
2. **Beginner:** Start at L00, work through L00-L05
3. **Know control theory:** Jump to L06 (PID basics)
4. **Need tuning:** Go to L07 (tuning methods)
5. **Advanced:** L10-L14 (cascade, feedforward, MPC)
6. **Build project:** L15 (CSTR digital twin)

---

## Resources

- **Course:** [docs/learning/cheme_data_course/](file:///d:/odibi/docs/learning/cheme_data_course/)
- **Examples:** [examples/cheme_course/](file:///d:/odibi/examples/cheme_course/)
- **Odibi Docs:** [ODIBI_DEEP_CONTEXT.md](file:///d:/odibi/docs/ODIBI_DEEP_CONTEXT.md)
- **Textbook:** Seborg et al. *Process Dynamics and Control* (4th ed.)

**Good luck!** 🎓⚗️
