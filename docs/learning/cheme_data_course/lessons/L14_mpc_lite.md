# L14: MPC-lite (Constrained Control)

**Prerequisites:** L13 | **Effort:** 90 min | **Seborg:** Chapter 20

---

## Learning Objectives

1. ✅ Understand constraint handling
2. ✅ Implement rate limits (Δu max)
3. ✅ Simulate move suppression
4. ✅ Approximate MPC behavior with PID + constraints

---

## Theory Recap: MPC Basics (Seborg Ch. 20)

**Model Predictive Control:**
- Optimizes future control trajectory
- Handles constraints explicitly
- Used in refineries, chemicals

**MPC-lite Approximation:**
- PID + rate limits
- Output constraints
- Constraint projection

**Rate Limits:**
```
Δu_max = 5% per minute (slow valve movement)
u[k] = u[k-1] + clamp(Δu, -Δu_max, Δu_max)
```

---

## Odibi Hands-On

### **Example 1: Constrained Level Control**

```yaml
# constrained_level.yaml
# PID with rate limit: max 2% valve change per minute
```

**Working example:** [/examples/cheme_course/L14_mpc_lite/constrained_level.yaml](file:///d:/odibi/examples/cheme_course/L14_mpc_lite/constrained_level.yaml)

---

## Summary

- ✅ Constraints are critical in real plants
- ✅ MPC handles them optimally
- ✅ PID + rate limits approximates MPC

**Next:** [L15: CSTR Digital Twin](L15_cstr_digital_twin.md)

---
