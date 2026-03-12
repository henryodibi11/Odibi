# L08: Disturbances + Setpoint Changes

**Prerequisites:** L06-L07 | **Effort:** 60 min | **Seborg:** Chapter 12

---

## Learning Objectives

By the end of this lesson you will:

1. ✅ Distinguish between load rejection and setpoint tracking
2. ✅ Implement step disturbances in simulation
3. ✅ Understand 2-DOF PID and setpoint weighting
4. ✅ Track disturbance metadata in data pipeline
5. ✅ Generate realistic operational scenarios (disturbances + SP changes)

---

## Theory Recap: Disturbances vs Setpoints (Seborg Ch. 12)

**Two fundamental control tasks:**

### **Load Rejection (Disturbance Rejection)**
- **Goal:** Maintain PV at setpoint despite unmeasured disturbances
- **Example:** Keep reactor temp at 80°C when feed temp varies
- **Tuning priority:** Fast integral action (high Ki)
- **Performance metric:** Peak deviation, recovery time

### **Setpoint Tracking**
- **Goal:** Follow changing setpoints smoothly
- **Example:** Ramp reactor temp from 60°C → 80°C during startup
- **Tuning priority:** Avoid derivative kick, smooth response
- **Performance metric:** Overshoot, rise time

**The Problem:**
- Standard PID can't optimize both tasks simultaneously
- Aggressive tuning for load rejection → overshoot on SP changes
- Conservative tuning for SP tracking → slow disturbance recovery

**Solution: 2-Degree-of-Freedom (2-DOF) PID**

**Setpoint weighting:**
```
u(t) = Kp·(β·SP - PV) + Ki·∫(SP - PV)dt + Kd·(-dPV/dt)
```

Where:
- **β** = Setpoint weight (0 to 1)
- **β = 1:** Standard PID (aggressive SP tracking)
- **β = 0:** P-term only responds to disturbances (smooth SP tracking)
- **β = 0.5:** Balanced

**Benefits:**
- Eliminates derivative kick on SP changes (dPV/dt instead of dError/dt)
- Reduces proportional overshoot on SP changes
- Still responds aggressively to disturbances

---

## Odibi Hands-On

### **Example 1: Load Rejection Test**

**Scenario:** Tank level control with inlet flow disturbance

```yaml
# load_rejection.yaml
# Step disturbance at t=60 min: Inlet flow 100 → 120 gpm
# SP constant at 50%
# Measure: Peak deviation, recovery time
```

**Working example:** [/examples/cheme_course/L08_disturbances/load_rejection.yaml](file:///d:/odibi/examples/cheme_course/L08_disturbances/load_rejection.yaml)

**Key metrics:**
- **Peak deviation:** Max |SP - PV| after disturbance
- **Recovery time:** Time to return within ±2% of SP
- **IAE:** Integral of absolute error

---

### **Example 2: Setpoint Tracking Test**

**Scenario:** Tank level SP change with no disturbance

```yaml
# setpoint_tracking.yaml
# SP step at t=60 min: 50% → 60%
# No disturbances
# Measure: Overshoot, rise time, settling time
```

**Working example:** [/examples/cheme_course/L08_disturbances/setpoint_tracking.yaml](file:///d:/odibi/examples/cheme_course/L08_disturbances/setpoint_tracking.yaml)

**Key metrics:**
- **Overshoot:** Max(PV - SP_final) / (SP_final - SP_initial) × 100%
- **Rise time:** Time from 10% to 90% of final value
- **Settling time:** Time to stay within ±2% of final value

---

### **Example 3: Combined Scenario (Realistic Operations)**

**Scenario:** Multiple disturbances + SP changes (like real plant)

```yaml
# operational_scenario.yaml
# t=0-60 min: Steady at 50%
# t=60 min: Disturbance (inlet +10 gpm)
# t=120 min: SP change 50% → 55%
# t=180 min: Another disturbance (inlet -5 gpm)
# t=240 min: SP change 55% → 50%
```

**Working example:** [/examples/cheme_course/L08_disturbances/operational_scenario.yaml](file:///d:/odibi/examples/cheme_course/L08_disturbances/operational_scenario.yaml)

**Metadata tracking:**
```yaml
# Track disturbance events
- name: disturbance_active
  data_type: boolean
  generator:
    type: derived
    expression: "minutes_elapsed >= 60 and minutes_elapsed < 180"

- name: disturbance_type
  data_type: string
  generator:
    type: derived
    expression: >
      'inlet_increase' if minutes_elapsed >= 60 and minutes_elapsed < 120
      else ('inlet_decrease' if minutes_elapsed >= 180 and minutes_elapsed < 240
      else 'none')

- name: setpoint_change_active
  data_type: boolean
  generator:
    type: derived
    expression: "minutes_elapsed in [120, 240]"
```

---

## Data Engineering Insights

**Why disturbance tracking matters:**

1. **Root cause analysis**
   - Tag data with disturbance events (feed pump trip, ambient temp change)
   - Correlate controller performance with disturbance type
   - "Why did level spike at 3am?" → Check disturbance log

2. **Controller performance monitoring**
   - Calculate load rejection metrics from historian data
   - Detect degradation: Recovery time increasing → Fouling? Valve sticking?
   - Compare actual vs simulated response

3. **Synthetic data for ML**
   - Generate 1000s of disturbance scenarios
   - Train anomaly detector: "Normal disturbance response" vs "Abnormal"
   - Augment rare events (pump failures) for training

4. **Operator training scenarios**
   - Realistic disturbance profiles (feed composition swing)
   - Measure operator response time
   - Test new operating procedures

---

## Exercises

### **Exercise 1: Measure Load Rejection**
Run `load_rejection.yaml` with different tunings:
- PI: Kp=2.0, Ki=0.1
- Aggressive: Kp=4.0, Ki=0.5
- Conservative: Kp=1.0, Ki=0.05

Calculate peak deviation and recovery time for each.

### **Exercise 2: Setpoint Overshoot**
Modify `setpoint_tracking.yaml`:
- Add large SP step (50% → 70%)
- Compare overshoot with Kp = [1, 2, 4, 8]
- Plot overshoot vs Kp

### **Exercise 3: Simultaneous Disturbance + SP**
Create scenario where disturbance and SP change occur at same time:
- Can controller handle both?
- Does it prioritize SP or disturbance?

### **Exercise 4: Metadata Enrichment**
Add disturbance classification to `operational_scenario.yaml`:
- "small" (<10 gpm), "medium" (10-20 gpm), "large" (>20 gpm)
- Calculate average recovery time by class

---

## Reflection: Real Plant Scenarios

**Common disturbances in process plants:**

| Process | Typical Disturbances |
|---------|----------------------|
| Heat exchanger | Inlet temp, flow rate, fouling |
| Distillation column | Feed composition, pressure, reflux flow |
| Reactor | Feed rate, catalyst activity, cooling water temp |
| Tank level | Inlet flow, outlet demand, pump speed |

**Red flags in historian data:**
- Large SP changes with no operator annotation → Automatic program?
- Frequent disturbances at same time daily → Upstream schedule?
- Controller struggles with one disturbance type → Poorly tuned?

**Best practices:**
- Log disturbance events (tags, alarms, operator notes)
- Separate SP changes (planned) from disturbances (unplanned)
- Track controller metrics by scenario type

---

## Summary

**Key Takeaways:**
- ✅ Load rejection ≠ setpoint tracking (different tuning priorities)
- ✅ Disturbances are unmeasured, SP changes are planned
- ✅ Track disturbance metadata for analysis
- ✅ Realistic scenarios combine both
- ✅ 2-DOF PID optimizes both tasks (advanced topic)

**Next Lesson:** [L09: System Identification](L09_system_id.md) - PRBS signals and model fitting

---

## Additional Resources

- **Seborg Chapter 12:** Controller Performance
- **Working examples:** [/examples/cheme_course/L08_disturbances/](file:///d:/odibi/examples/cheme_course/L08_disturbances/)
