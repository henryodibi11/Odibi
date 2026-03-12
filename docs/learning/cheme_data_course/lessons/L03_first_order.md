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

## Odibi Hands-On

### **Example 1: Tank Temperature (First-Order Response)**

**Process:** Steam-heated tank
- Input: Steam flow rate (step change)
- Output: Tank temperature (first-order response)
- Time constant τ = 10 minutes

```yaml
# tank_temperature_fo.yaml
name: first_order_tank
engine: pandas

connections:
  output:
    type: local
    path: ./output/first_order_tank.parquet
    format: parquet

pipelines:
  - name: fo_tank_pipeline
    nodes:
      - name: generate_temperature_response
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2024-01-01T00:00:00Z"
                timestep: "1min"
                row_count: 600  # 10 hours
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

                # ──────────────────────────────────
                # PROCESS PARAMETERS
                # ──────────────────────────────────
                - name: time_constant_min
                  data_type: float
                  generator:
                    type: constant
                    value: 10.0

                - name: process_gain
                  data_type: float
                  generator:
                    type: constant
                    value: 2.0

                # ──────────────────────────────────
                # INPUT (step change at t=60 min)
                # ──────────────────────────────────
                - name: minutes_elapsed
                  data_type: float
                  generator:
                    type: sequential
                    start: 0
                    step: 1

                - name: steam_flow_lb_hr
                  data_type: float
                  generator:
                    type: derived
                    expression: "150.0 if minutes_elapsed >= 60.0 else 100.0"

                # ──────────────────────────────────
                # OUTPUT (first-order response)
                # ──────────────────────────────────
                - name: tank_temp_f
                  data_type: float
                  generator:
                    type: derived
                    expression: >
                      prev('tank_temp_f', 200.0) +
                      (1.0 / time_constant_min) *
                      (process_gain * steam_flow_lb_hr - prev('tank_temp_f', 200.0))

                # ──────────────────────────────────
                # SENSOR (with measurement noise)
                # ──────────────────────────────────
                - name: noise
                  data_type: float
                  generator:
                    type: range
                    min: -0.5
                    max: 0.5
                    distribution: normal

                - name: temp_sensor_f
                  data_type: float
                  generator:
                    type: derived
                    expression: "tank_temp_f + noise"

                # ──────────────────────────────────
                # KPIs (calculated)
                # ──────────────────────────────────
                - name: percent_of_final_value
                  data_type: float
                  generator:
                    type: derived
                    expression: >
                      0.0 if minutes_elapsed < 60.0 else
                      ((tank_temp_f - 200.0) / 100.0) * 100.0

        write:
          connection: output
```

**Working example:** [/examples/cheme_course/L03_first_order/tank_temperature_fo.yaml](file:///d:/odibi/examples/cheme_course/L03_first_order/tank_temperature_fo.yaml)

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

**Working example:** [/examples/cheme_course/L03_first_order/ema_filtering.yaml](file:///d:/odibi/examples/cheme_course/L03_first_order/ema_filtering.yaml)

**Trade-off:**
- More filtering (lower α) → Less noise BUT more lag
- Less filtering (higher α) → More responsive BUT noisier

---

## Exercises

See [../solutions/L03.md](../solutions/L03.md) for full solutions.

All YAML files: [/examples/cheme_course/L03_first_order/](file:///d:/odibi/examples/cheme_course/L03_first_order/)

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
