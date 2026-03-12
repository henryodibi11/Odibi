# L07: PID Tuning Methods

**Prerequisites:** L06 | **Effort:** 60 min | **Seborg:** Chapter 12

---

## Learning Objectives

By the end of this lesson you will:

1. ✅ Apply Ziegler-Nichols tuning rules
2. ✅ Use IMC/Direct Synthesis tuning method
3. ✅ Compare tuning methods for FOPTD processes
4. ✅ Understand tuning tradeoffs (speed vs robustness)
5. ✅ Generate training data for different tuning strategies

---

## Theory Recap: PID Tuning (Seborg Ch. 12)

**The Tuning Problem:**
Given a process model (K, τ, θ), calculate PID parameters (Kp, Ki, Kd) for desired performance.

### **Method 1: Ziegler-Nichols (1942)**

**For FOPTD model:** G(s) = K·e^(-θs) / (τs + 1)

| Controller | Kp | Ti (integral time) | Td (derivative time) |
|------------|----|--------------------|----------------------|
| P | τ/(K·θ) | ∞ | 0 |
| PI | 0.9τ/(K·θ) | θ/0.3 | 0 |
| PID | 1.2τ/(K·θ) | 2θ | 0.5θ |

**Convert to Ki, Kd:**
- Ki = Kp / Ti
- Kd = Kp · Td

**Pros:** Simple, widely used, aggressive (fast response)
**Cons:** Often too aggressive (20-30% overshoot), poor for noisy loops

### **Method 2: IMC/Direct Synthesis**

**Idea:** Design controller to make closed-loop behave like first-order system with time constant λ.

**For FOPTD:**
```
Kp = τ / (K·(λ + θ))
Ti = τ
Td = 0  (PI controller)
```

**Tuning knob:** λ (closed-loop time constant)
- λ small → fast, aggressive (like ZN)
- λ large → slow, conservative (more robust)
- **Rule of thumb:** λ = θ to 3θ

**Pros:** More conservative than ZN, good robustness
**Cons:** Slower response, PI-only (no derivative)

---

## Odibi Hands-On

### **Example 1: FOPTD Process - Ziegler-Nichols Tuning**

**Process:** Temperature control (K=1.5, τ=10 min, θ=2 min)

**ZN Calculations:**
```
Kp = 1.2τ/(K·θ) = 1.2(10)/(1.5·2) = 4.0
Ti = 2θ = 2(2) = 4.0 min
Td = 0.5θ = 0.5(2) = 1.0 min

Ki = Kp/Ti = 4.0/4.0 = 1.0
Kd = Kp·Td = 4.0·1.0 = 4.0
```

```yaml
# temp_control_zn.yaml
nodes:
  - name: temp_control_zn
    read:
      format: simulation
      options:
        simulation:
          scope:
            start_time: "2024-01-01T00:00:00Z"
            timestep: "30sec"
            row_count: 1200  # 10 hours
            seed: 42

          entities:
            count: 1
            id_prefix: "TC-"

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
                step: 30

            # ─────────────────────────────────
            # PROCESS PARAMETERS (FOPTD)
            # ─────────────────────────────────
            - name: K_process
              data_type: float
              generator:
                type: constant
                value: 1.5

            - name: tau_process_min
              data_type: float
              generator:
                type: constant
                value: 10.0

            - name: theta_process_min
              data_type: float
              generator:
                type: constant
                value: 2.0

            # ─────────────────────────────────
            # SETPOINT (step change at t=600 sec)
            # ─────────────────────────────────
            - name: temp_sp_degc
              data_type: float
              generator:
                type: derived
                expression: "25.0 if seconds_elapsed < 600 else 30.0"

            # ─────────────────────────────────
            # CONTROLLER OUTPUT (Ziegler-Nichols tuning)
            # ─────────────────────────────────
            - name: temp_pv_degc
              data_type: float
              generator:
                type: derived
                expression: "prev('temp_pv_degc', 25.0)"

            - name: heater_output_pct
              data_type: float
              generator:
                type: derived
                expression: >
                  pid(
                    pv=temp_pv_degc,
                    sp=temp_sp_degc,
                    Kp=4.0,
                    Ki=1.0,
                    Kd=4.0,
                    dt=30,
                    output_min=0,
                    output_max=100,
                    anti_windup=True
                  )

            # ─────────────────────────────────
            # FOPTD PROCESS DYNAMICS
            # ─────────────────────────────────
            # Dead time buffer (simple delay using modulo)
            - name: delay_steps
              data_type: int
              generator:
                type: constant
                value: 4  # 2 min / 0.5 min = 4 steps

            # Delayed input (simplified - real delay needs buffer)
            - name: heater_delayed_pct
              data_type: float
              generator:
                type: derived
                expression: "prev('heater_output_pct', 0) if seconds_elapsed >= 120 else 0"

            # First-order response
            - name: temp_pv_degc_next
              data_type: float
              generator:
                type: derived
                expression: >
                  prev('temp_pv_degc', 25.0) +
                  (30.0 / 60.0) *
                  (K_process * heater_delayed_pct - prev('temp_pv_degc', 25.0)) / tau_process_min

            # Update state
            - name: temp_pv_degc
              data_type: float
              generator:
                type: derived
                expression: "temp_pv_degc_next"

            # ─────────────────────────────────
            # METRICS
            # ─────────────────────────────────
            - name: error_degc
              data_type: float
              generator:
                type: derived
                expression: "temp_sp_degc - temp_pv_degc"

            - name: iae
              data_type: float
              generator:
                type: derived
                expression: "prev('iae', 0) + abs(error_degc) * 0.5"

    write:
      connection: local
      format: parquet
      path: data/output/temp_control_zn.parquet
      mode: overwrite
```

**Working example:** [/examples/cheme_course/L07_tuning/temp_control_zn.yaml](file:///d:/odibi/examples/cheme_course/L07_tuning/temp_control_zn.yaml)

---

### **Example 2: Same Process - IMC Tuning**

**IMC Calculations (λ = θ):**
```
λ = 2.0 min
Kp = τ/(K·(λ + θ)) = 10/(1.5·(2+2)) = 1.67
Ti = τ = 10 min
Td = 0

Ki = Kp/Ti = 1.67/10 = 0.167
Kd = 0
```

```yaml
# temp_control_imc.yaml (excerpt - full file available)
- name: heater_output_pct
  data_type: float
  generator:
    type: derived
    expression: >
      pid(
        pv=temp_pv_degc,
        sp=temp_sp_degc,
        Kp=1.67,
        Ki=0.167,
        Kd=0.0,
        dt=30,
        output_min=0,
        output_max=100,
        anti_windup=True
      )
```

**Working example:** [/examples/cheme_course/L07_tuning/temp_control_imc.yaml](file:///d:/odibi/examples/cheme_course/L07_tuning/temp_control_imc.yaml)

**Comparison:**

| Tuning Method | Kp | Ki | Kd | Response | Overshoot | Robustness |
|---------------|----|----|----|----|-----------|------------|
| Ziegler-Nichols | 4.0 | 1.0 | 4.0 | Fast | ~20% | Low |
| IMC (λ=θ) | 1.67 | 0.167 | 0 | Moderate | <5% | High |
| IMC (λ=3θ) | 0.83 | 0.083 | 0 | Slow | 0% | Very High |

---

### **Example 3: Tuning Comparison Study**

**Generate side-by-side comparison data:**

```yaml
# tuning_comparison.yaml
# Simulates same process with 3 different tuning methods
# Output: Combined dataset for performance analysis
```

**Working example:** [/examples/cheme_course/L07_tuning/tuning_comparison.yaml](file:///d:/odibi/examples/cheme_course/L07_tuning/tuning_comparison.yaml)

**Analysis metrics:**
- IAE (Integral Absolute Error) - smaller is better
- Rise time (10% → 90%) - faster is better
- Overshoot (%) - smaller is better
- Settling time (2% band) - faster is better
- Total valve movement - less wear is better

---

## Data Engineering Insights

**Why tuning matters for data engineers:**

1. **Detect detuned controllers in historian data**
   - High IAE → controller not optimized
   - Excessive valve movement → Kp too high
   - Slow response → Ki too low

2. **Generate training data for ML-based tuning**
   - Simulate 100s of processes with different tunings
   - Label data with performance metrics
   - Train model to suggest Kp/Ki/Kd from step response

3. **Validate re-tuning before deployment**
   - Simulate proposed tuning on historical disturbances
   - Estimate improvement in IAE, overshoot
   - Prevent production trials

4. **Digital twin for operator training**
   - Let operators "tune" simulated process
   - Learn consequences of aggressive vs conservative tuning
   - No risk to real plant

---

## Exercises

### **Exercise 1: Apply ZN Tuning**
For a process with K=2.0, τ=5 min, θ=1 min:
- Calculate Kp, Ki, Kd using ZN rules
- Modify `temp_control_zn.yaml` with new parameters
- Measure overshoot and settling time

### **Exercise 2: IMC Lambda Sweep**
Test IMC tuning with λ = [θ, 2θ, 3θ, 5θ]:
- How does increasing λ affect settling time?
- What is the tradeoff between speed and robustness?

### **Exercise 3: Comparison Metrics**
Run all 3 tuning methods on same disturbance:
- Calculate IAE for each
- Which method minimizes overshoot?
- Which method has least valve movement?

### **Exercise 4: Noisy Process**
Add measurement noise to PV (±0.5°C random):
- Does ZN's derivative term amplify noise?
- Is IMC (PI-only) more robust to noise?

---

## Reflection: Real Plant Scenarios

**When to use each method:**

| Scenario | Recommended Method |
|----------|-------------------|
| Fast, clean process (flow, pressure) | Ziegler-Nichols |
| Slow, noisy process (temperature) | IMC with λ=2θ or 3θ |
| Critical safety loop (no overshoot) | IMC with λ=5θ |
| Unknown process model | Manual tuning or relay auto-tune |

**Red flags in tuned data:**
- Overshoot > 30% → Too aggressive (reduce Kp)
- Slow settling (>5τ) → Too conservative (increase Kp)
- Oscillations → Kp too high or wrong sign
- Steady offset with PI → Ki=0 (forgot integral!)

---

## Summary

**Key Takeaways:**
- ✅ Ziegler-Nichols: Fast, aggressive (20% overshoot)
- ✅ IMC: Conservative, robust (tune with λ)
- ✅ λ = θ → balanced, λ = 3θ → very robust
- ✅ Always compare methods on same disturbance
- ✅ Simulation prevents risky plant tuning trials

**Next Lesson:** [L08: Disturbances + Setpoints](L08_disturbances.md) - Load rejection vs setpoint tracking

---

## Additional Resources

- **Seborg Chapter 12:** PID Controller Tuning
- **Working examples:** [/examples/cheme_course/L07_tuning/](file:///d:/odibi/examples/cheme_course/L07_tuning/)
- **Calculator:** [Tuning rules spreadsheet](https://github.com/henryodibi11/Odibi/tree/main/examples/cheme_course/L07_tuning/tuning_calculator.xlsx)
