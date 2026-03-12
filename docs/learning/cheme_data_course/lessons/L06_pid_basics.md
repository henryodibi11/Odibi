# L06: PID Basics + Constraints

**Prerequisites:** L00-L05 | **Effort:** 60 min | **Seborg:** Chapter 8

---

## Learning Objectives

By the end of this lesson you will:

1. ✅ Understand P, I, D controller actions and when to use each
2. ✅ Implement PID controllers using Odibi's `pid()` function
3. ✅ Recognize proportional offset and how integral action eliminates it
4. ✅ Configure output limits and anti-windup
5. ✅ Compare P-only vs PI vs PID control performance

---

## Theory Recap: PID Control (Seborg Ch. 8)

**PID Controller Equation:**
```
u(t) = Kp·e(t) + Ki·∫e(t)dt + Kd·de(t)/dt
```

Where:
- **e(t) = SP - PV** (error = setpoint - process variable)
- **Kp** = Proportional gain (immediate response to error)
- **Ki** = Integral gain (eliminates steady-state offset)
- **Kd** = Derivative gain (anticipates future error)

**Controller Actions:**

| Action | Effect | When to Use | Downside |
|--------|--------|-------------|----------|
| **P-only** | Fast response | Simple, stable systems | Offset remains |
| **PI** | Eliminates offset | Most industrial loops (90%) | Slower than P |
| **PID** | Fastest settling | Fast processes, minimal noise | Sensitive to noise |

**Key Concepts:**

**Proportional Offset:**
- P-only control leaves steady-state error
- Offset = Disturbance / Kp
- Higher Kp reduces offset but can cause instability

**Integral Action:**
- Accumulates error over time
- Automatically drives offset to zero
- Can cause "integral windup" when saturated

**Anti-Windup:**
- Stops integral from accumulating when output is saturated
- Essential for real controllers (valves hit 0% or 100%)

**Output Limits:**
- Physical constraints: valves (0-100%), heaters (0-max kW)
- Safety limits: max flow rates, temperatures

---

## Odibi Hands-On

### **Odibi PID Syntax**

```yaml
- name: controller_output
  data_type: float
  generator:
    type: derived
    expression: >
      pid(
        pv=current_measurement,
        sp=setpoint_value,
        Kp=2.0,
        Ki=0.1,
        Kd=0.5,
        dt=60,              # timestep in seconds
        output_min=0,
        output_max=100,
        anti_windup=True
      )
```

**Parameters:**
- `pv`: Process variable (measured value)
- `sp`: Setpoint (target value)
- `Kp`, `Ki`, `Kd`: PID tuning parameters
- `dt`: Timestep in seconds (must match simulation timestep!)
- `output_min/max`: Physical limits (e.g., valve 0-100%)
- `anti_windup`: Enable anti-windup (default True)

---

### **Example 1: P-only Tank Level Control**

**Process:** Tank level control with constant inlet disturbance

```yaml
# tank_p_only.yaml
nodes:
  - name: tank_level_p_only
    read:
      format: simulation
      options:
        simulation:
          scope:
            start_time: "2024-01-01T00:00:00Z"
            timestep: "1min"
            row_count: 300  # 5 hours
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

            # Setpoint: 50%
            - name: level_sp_pct
              data_type: float
              generator:
                type: constant
                value: 50.0

            # Tank parameters
            - name: tank_area_ft2
              data_type: float
              generator:
                type: constant
                value: 100.0

            # Inlet disturbance (step change at t=60)
            - name: inlet_flow_gpm
              data_type: float
              generator:
                type: derived
                expression: "100.0 if minutes_elapsed < 60 else 110.0"

            # P-only controller (Kp=2.0, no integral!)
            - name: level_pv_pct
              data_type: float
              generator:
                type: derived
                expression: "prev('level_pv_pct', 50.0)"

            - name: outlet_valve_pct
              data_type: float
              generator:
                type: derived
                expression: >
                  pid(
                    pv=level_pv_pct,
                    sp=level_sp_pct,
                    Kp=2.0,
                    Ki=0.0,
                    Kd=0.0,
                    dt=60,
                    output_min=0,
                    output_max=100,
                    anti_windup=True
                  )

            # Tank level dynamics (first-order)
            - name: outlet_flow_gpm
              data_type: float
              generator:
                type: derived
                expression: "outlet_valve_pct * 1.5"

            - name: level_pv_pct_updated
              data_type: float
              generator:
                type: derived
                expression: >
                  max(0, min(100,
                    prev('level_pv_pct', 50.0) +
                    (inlet_flow_gpm - outlet_flow_gpm) / tank_area_ft2
                  ))

            # Update state variable
            - name: level_pv_pct
              data_type: float
              generator:
                type: derived
                expression: "level_pv_pct_updated"

            # Calculate error
            - name: error_pct
              data_type: float
              generator:
                type: derived
                expression: "level_sp_pct - level_pv_pct"

    write:
      connection: local
      format: parquet
      path: data/output/tank_p_only.parquet
      mode: overwrite
```

**Working example:** [/examples/cheme_course/L06_pid_basics/tank_p_only.yaml](file:///d:/odibi/examples/cheme_course/L06_pid_basics/tank_p_only.yaml)

**What you'll see:**
- Level starts at 50% (setpoint)
- At t=60 min, inlet flow increases 10%
- P-only controller cannot eliminate offset
- New steady-state level ≈ 55% (5% offset remains)

---

### **Example 2: PI Controller Eliminates Offset**

**Same tank, now with integral action:**

```yaml
# tank_pi.yaml (excerpt - see full file)
- name: outlet_valve_pct
  data_type: float
  generator:
    type: derived
    expression: >
      pid(
        pv=level_pv_pct,
        sp=level_sp_pct,
        Kp=2.0,
        Ki=0.1,        # Add integral action!
        Kd=0.0,
        dt=60,
        output_min=0,
        output_max=100,
        anti_windup=True
      )
```

**Working example:** [/examples/cheme_course/L06_pid_basics/tank_pi.yaml](file:///d:/odibi/examples/cheme_course/L06_pid_basics/tank_pi.yaml)

**What you'll see:**
- Level starts at 50% (setpoint)
- At t=60 min, inlet flow increases 10%
- PI controller drives level back to exactly 50%
- Integral action accumulates error and eliminates offset
- Takes ~100 minutes to settle

---

### **Example 3: PID for Fast Response**

**Add derivative action for faster settling:**

```yaml
# tank_pid.yaml (excerpt - see full file)
- name: outlet_valve_pct
  data_type: float
  generator:
    type: derived
    expression: >
      pid(
        pv=level_pv_pct,
        sp=level_sp_pct,
        Kp=2.0,
        Ki=0.1,
        Kd=0.5,        # Add derivative action!
        dt=60,
        output_min=0,
        output_max=100,
        anti_windup=True
      )
```

**Working example:** [/examples/cheme_course/L06_pid_basics/tank_pid.yaml](file:///d:/odibi/examples/cheme_course/L06_pid_basics/tank_pid.yaml)

**What you'll see:**
- Faster settling time (~60 minutes vs ~100 for PI)
- Less overshoot
- Derivative "anticipates" error trend

---

## Data Engineering Insights

**Why PID simulation matters for data engineers:**

1. **Understand control outputs in historian data**
   - Why does valve position oscillate?
   - Why is there steady-state offset?
   - Is controller poorly tuned or is there a fault?

2. **Validate DCS logic during migrations**
   - Simulate legacy controller before cutover
   - Generate test data for new SCADA system
   - Verify anti-windup logic works

3. **Synthetic data for ML training**
   - Generate realistic controller behavior
   - Include disturbances, setpoint changes
   - Train anomaly detectors on normal operation

4. **Digital twin initialization**
   - Simulate startup sequences
   - Test operator training scenarios
   - Validate control logic before deployment

---

## Exercises

### **Exercise 1: Verify Proportional Offset**
Run `tank_p_only.yaml` with different Kp values (1.0, 2.0, 5.0).
Calculate steady-state offset for each. Does higher Kp reduce offset?

### **Exercise 2: Integral Windup Demo**
Modify `tank_pi.yaml`:
- Set `anti_windup=False`
- Add large disturbance at t=100
- Compare settling time with/without anti-windup

### **Exercise 3: Derivative Kick**
Modify `tank_pid.yaml`:
- Add setpoint step change at t=150 (50% → 60%)
- Does derivative cause large "kick" in valve output?
- How would you filter the setpoint to prevent this?

### **Exercise 4: Output Saturation**
Set inlet flow to 200 gpm (very large disturbance).
Watch valve saturate at 100%. Does integral keep accumulating?

---

## Reflection: Real Plant Scenarios

**When do you see these issues?**

| Issue | Plant Example |
|-------|---------------|
| P-only offset | Old pneumatic controllers (3-15 psi signal) |
| Integral windup | Valve stuck, controller keeps cranking output |
| Derivative noise | Noisy flow sensor causes valve "hunting" |
| Output saturation | Heater maxed out, cannot reach setpoint |

**Red flags in historian data:**
- Controller output stuck at 0% or 100% → Saturation
- PV oscillates around SP → Excessive Kd or noise
- Steady offset from SP → P-only or broken integral

---

## Summary

**Key Takeaways:**
- ✅ P-only is fast but leaves offset
- ✅ PI eliminates offset (use for 90% of loops)
- ✅ PID adds speed but sensitive to noise
- ✅ Always use output limits and anti-windup
- ✅ Odibi's `pid()` function handles state management automatically

**Next Lesson:** [L07: Tuning Methods](L07_tuning.md) - How to calculate Kp, Ki, Kd values

---

## Additional Resources

- **Seborg Chapter 8:** Feedback Controllers
- **Working examples:** [/examples/cheme_course/L06_pid_basics/](file:///d:/odibi/examples/cheme_course/L06_pid_basics/)
- **Odibi docs:** `odibi explain pid`
