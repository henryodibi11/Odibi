# L13: Nonlinearity + Saturation

**Prerequisites:** L12 | **Effort:** 60 min | **Seborg:** Chapter 16

---

## Learning Objectives

1. ✅ Handle nonlinear processes (pH, valve saturation)
2. ✅ Implement gain scheduling
3. ✅ Use clamp() and conditional logic
4. ✅ Simulate valve saturation effects

---

## Theory Recap: Nonlinearity (Seborg Ch. 16)

**Common Nonlinearities:**
- **pH control:** Titration curve (steep near equivalence)
- **Valve saturation:** 0-100% physical limits
- **Gain variation:** Process gain changes with operating point

**Solutions:**
1. **Gain scheduling:** Switch controller gains based on PV
2. **Adaptive control:** Continuously adjust gains
3. **Advanced control:** MPC handles constraints natively

---

## Odibi Hands-On

### **Example 1: Valve Saturation**

```yaml
# valve_saturation.yaml
# Large disturbance → valve hits 100%
# Integral windup demonstration
```

**Working example:** [/examples/cheme_course/L13_nonlinearity/valve_saturation.yaml](file:///d:/odibi/examples/cheme_course/L13_nonlinearity/valve_saturation.yaml)

### **Example 2: pH Nonlinearity**

```yaml
# ph_control.yaml
# Gain scheduling: Different Kp for pH 3-5 vs 5-9
```

**Working example:** [/examples/cheme_course/L13_nonlinearity/ph_control.yaml](file:///d:/odibi/examples/cheme_course/L13_nonlinearity/ph_control.yaml)

---

## Summary

- ✅ Saturation and nonlinearity are real-world facts
- ✅ Gain scheduling helps
- ✅ Always use output limits and anti-windup

**Next:** [L14: MPC-lite](L14_mpc_lite.md)

---
