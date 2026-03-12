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
dx₁/dt = x₂                           (velocity)
dx₂/dt = -ωn²x₁ - 2ζωn·x₂ + ωn²u     (acceleration)
y = x₁                                 (output)
```

```yaml
# pressure_underdamped.yaml
name: pressure_second_order
engine: pandas

connections:
  output:
    type: local
    path: ./output/pressure_underdamped.parquet
    format: parquet

pipelines:
  - name: pressure_pipeline
    nodes:
      - name: generate_pressure_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2024-01-01T00:00:00Z"
                timestep: "10sec"
                row_count: 600  # 100 minutes
                seed: 42

              entities:
                count: 1
                id_prefix: "PT-"

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

                - name: seconds_elapsed
                  data_type: float
                  generator:
                    type: sequential
                    start: 0
                    step: 10

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
                  generator:
                    type: constant
                    value: 0.0167  # rad/sec (1.0 rad/min)

                # ─────────────────────────────────
                # INPUT: Pressure step change
                # ─────────────────────────────────
                - name: true_pressure_psi
                  data_type: float
                  generator:
                    type: derived
                    expression: "10.0 if seconds_elapsed >= 600.0 else 0.0"

                # ─────────────────────────────────
                # STATE VARIABLES (state-space)
                # ─────────────────────────────────

                # Velocity (dx₂/dt)
                - name: velocity
                  data_type: float
                  generator:
                    type: derived
                    expression: >
                      prev('velocity', 0.0) +
                      10.0 *
                      (-omega_n * omega_n * prev('pressure_reading_psi', 0.0) -
                       2.0 * zeta * omega_n * prev('velocity', 0.0) +
                       omega_n * omega_n * true_pressure_psi)

                # Position (output)
                - name: pressure_reading_psi
                  data_type: float
                  generator:
                    type: derived
                    expression: "prev('pressure_reading_psi', 0.0) + 10.0 * velocity"

        write:
          connection: output
```

**Working example:** [/examples/cheme_course/L05_second_order/pressure_underdamped.yaml](file:///d:/odibi/examples/cheme_course/L05_second_order/pressure_underdamped.yaml)

**Expected behavior:**
- Step input at t=600 sec (10 min)
- Response overshoots by ~37% (for ζ=0.3)
- Oscillates before settling

---

### **Example 2: Damping Ratio Comparison**

Compare underdamped, critically damped, and overdamped:

**Working example:** [/examples/cheme_course/L05_second_order/damping_comparison.yaml](file:///d:/odibi/examples/cheme_course/L05_second_order/damping_comparison.yaml)

**Observations:**
- **Underdamped (ζ=0.3):** Fastest but overshoots ~37%
- **Critical (ζ=1.0):** No overshoot, reasonably fast
- **Overdamped (ζ=2.0):** Slow, no overshoot

---

### **Example 3: Valve Actuator Model**

**Process:** Control valve with second-order dynamics (ζ=0.7, fast response)

**Working example:** [/examples/cheme_course/L05_second_order/valve_actuator.yaml](file:///d:/odibi/examples/cheme_course/L05_second_order/valve_actuator.yaml)

**Typical for industrial valves:**
- ζ ≈ 0.7 (slightly underdamped)
- ωn ≈ 3 rad/s (fast response)
- Small overshoot (~5%)
- Settling time ~1-2 seconds

---

## Exercises

See [../solutions/L05.md](../solutions/L05.md) for full solutions.

All YAML files: [/examples/cheme_course/L05_second_order/](file:///d:/odibi/examples/cheme_course/L05_second_order/)

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

**You've completed Part I: Foundations!** 🎉

**Next lesson:**
👉 [L06: PID Basics + Constraints](L06_pid_basics.md)

Now we start Part II: Feedback Control. You'll implement PID controllers using Odibi's `pid()` transformer.

---

*Lesson L05 complete! Ready for Part II: Control!*
