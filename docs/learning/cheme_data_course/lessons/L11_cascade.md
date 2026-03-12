# L11: Cascade Control

**Prerequisites:** L10 | **Effort:** 60 min | **Seborg:** Chapter 16

---

## Learning Objectives

1. ✅ Understand cascade control (inner + outer loops)
2. ✅ Implement fast inner loop + slow outer loop
3. ✅ Compare cascade vs single-loop performance
4. ✅ Apply to reactor temperature control (jacket temp as inner loop)

---

## Theory Recap: Cascade Control (Seborg Ch. 16)

**Cascade Structure:**
- **Outer (Primary) Loop:** Slow process (reactor temp)
- **Inner (Secondary) Loop:** Fast actuator (jacket temp)

**Benefits:**
- Inner loop rejects disturbances before affecting outer
- Faster response to setpoint changes
- Handles actuator nonlinearities

**Tuning Rule:**
- Tune inner loop first (10× faster than outer)
- Then tune outer loop

---

## Odibi Hands-On

### **Example 1: Reactor Cascade Control**

```yaml
# reactor_cascade.yaml
# Outer: Reactor temperature (SP=80°C, τ=20 min)
# Inner: Jacket temperature (SP from outer, τ=2 min)
```

**Working example:** [/examples/cheme_course/L11_cascade/reactor_cascade.yaml](file:///d:/odibi/examples/cheme_course/L11_cascade/reactor_cascade.yaml)

### **Example 2: Cascade vs Single-Loop**

Compare performance against single-loop control.

**Working example:** [/examples/cheme_course/L11_cascade/cascade_vs_single.yaml](file:///d:/odibi/examples/cheme_course/L11_cascade/cascade_vs_single.yaml)

---

## Summary

- ✅ Cascade improves disturbance rejection
- ✅ Inner loop must be 5-10× faster than outer
- ✅ Tune inner first, then outer

**Next:** [L12: Feedforward Control](L12_feedforward.md)

---
