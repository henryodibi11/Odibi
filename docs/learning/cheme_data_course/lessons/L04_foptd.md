# L04: FOPTD Transfer Functions

**Prerequisites:** L00-L03 | **Effort:** 45 min | **Seborg:** Chapter 4

---

## Learning Objectives

By the end of this lesson you will:

1. ✅ Understand FOPTD (First-Order Plus Time Delay) transfer function parameters
2. ✅ Implement time delay using manual shift with multiple `prev()` calls
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
u_delayed = prev(prev(...prev(input)))  # Apply delay with nested prev()
y[k] = y[k-1] + (Δt/τ) × (K × u_delayed - y[k-1])  # First-order lag
```

---

## CRITICAL: Time Delay Implementation

**⚠️ IMPORTANT:** Odibi does NOT support `prev(..., lag=N)` syntax.

**WRONG (does not work):**
```yaml
expression: "prev('input', 0.0, lag=5)"  # ❌ This syntax doesn't exist!
```

**CORRECT (manual shift with nested prev()):**
```yaml
# For 2-timestep delay:
- name: input_delayed_1
  generator:
    type: derived
    expression: "prev('input', 0.0)"

- name: input_delayed
  generator:
    type: derived
    expression: "prev('input_delayed_1', 0.0)"

# Now use input_delayed (2 timesteps behind input)
```

**For larger delays:**
- θ = 1 timestep: Use `prev('input', default)` directly
- θ = 2 timesteps: 2 columns (as shown above)
- θ = 3 timesteps: 3 columns (input → delayed_1 → delayed_2 → delayed)
- θ = 5+ timesteps: Consider reducing timestep or accepting limitation

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
name: tank_temperature_foptd
engine: pandas

connections:
  output:
    type: local
    path: ./output/tank_foptd.parquet
    format: parquet

pipelines:
  - name: foptd_pipeline
    nodes:
      - name: generate_foptd_data
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2024-01-01T00:00:00Z"
                timestep: "1min"
                row_count: 240  # 4 hours
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

                - name: minutes_elapsed
                  data_type: float
                  generator:
                    type: sequential
                    start: 0
                    step: 1

                # ─────────────────────────────────
                # INPUT: Heater power (step change)
                # ─────────────────────────────────
                - name: heater_pct
                  data_type: float
                  generator:
                    type: derived
                    expression: "60.0 if minutes_elapsed >= 30.0 else 50.0"

                # ─────────────────────────────────
                # TIME DELAY (θ = 2 minutes)
                # Manual implementation using nested prev()
                # ─────────────────────────────────
                - name: heater_delayed_1
                  data_type: float
                  generator:
                    type: derived
                    expression: "prev('heater_pct', 50.0)"

                - name: heater_delayed
                  data_type: float
                  generator:
                    type: derived
                    expression: "prev('heater_delayed_1', 50.0)"

                # ─────────────────────────────────
                # FIRST-ORDER LAG (K=0.5, τ=10)
                # ─────────────────────────────────
                - name: tank_temp_true_f
                  data_type: float
                  generator:
                    type: derived
                    expression: >
                      prev('tank_temp_true_f', 100.0) +
                      (1.0 / 10.0) *
                      (0.5 * (heater_delayed - 50.0) -
                       (prev('tank_temp_true_f', 100.0) - 100.0))

                # ─────────────────────────────────
                # SENSOR (with noise)
                # ─────────────────────────────────
                - name: sensor_noise
                  data_type: float
                  generator:
                    type: range
                    min: -0.3
                    max: 0.3
                    distribution: normal

                - name: tank_temp_measured_f
                  data_type: float
                  generator:
                    type: derived
                    expression: "tank_temp_true_f + sensor_noise"

        write:
          connection: output
```

**Working example:** [/examples/cheme_course/L04_foptd/tank_foptd.yaml](file:///d:/odibi/examples/cheme_course/L04_foptd/tank_foptd.yaml)

**Run:**

```bash
cd examples/cheme_course/L04_foptd
odibi run tank_foptd.yaml
```

---

### **Example 2: Parameterized FOPTD**

**Working example:** [/examples/cheme_course/L04_foptd/foptd_parameterized.yaml](file:///d:/odibi/examples/cheme_course/L04_foptd/foptd_parameterized.yaml)

**Note:** Due to manual delay implementation, theta is limited to small values (0-3 timesteps typical).

---

### **Example 3: Heat Exchanger FOPTD**

**Process:** Heat exchanger with negative gain (more coolant → lower temperature)

**Parameters:**
- K = -0.4 °F/gpm (negative gain!)
- τ = 8 minutes
- θ = 3 minutes (sensor location)

**Working example:** [/examples/cheme_course/L04_foptd/heat_exchanger_foptd.yaml](file:///d:/odibi/examples/cheme_course/L04_foptd/heat_exchanger_foptd.yaml)

---

## Exercises

See [../solutions/L04.md](../solutions/L04.md) for full solutions.

All YAML files: [/examples/cheme_course/L04_foptd/](file:///d:/odibi/examples/cheme_course/L04_foptd/)

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
- How to implement FOPTD in discrete time (manual delay with prev())
- Parameterize configs for reusability
- Validate process characteristics from data
- Dead time is CRITICAL for control (we'll see this in PID lesson)

---

## Next Steps

**You now know:**
- ✅ FOPTD transfer function (K, τ, θ)
- ✅ Time delay implementation with manual shift (nested prev())
- ✅ Parameterized YAML configurations
- ✅ Step response validation

**Next lesson:**
👉 [L05: Second-Order Systems + Overshoot](L05_second_order.md)

We'll learn damping ratio (ζ), natural frequency (ωn), and how to model underdamped oscillations.

---

*Lesson L04 complete!*
