# L12: Feedforward + Ratio Control

**Prerequisites:** L11 | **Effort:** 60 min | **Seborg:** Chapter 15

---

## Learning Objectives

1. ✅ Understand feedforward control (measure disturbance, compensate)
2. ✅ Implement ratio control (maintain 3:1 ratio)
3. ✅ Combine feedforward + feedback (FF+FB)
4. ✅ Compare FF+FB vs feedback-only performance

---

## Theory Recap: Feedforward (Seborg Ch. 15)

**Feedforward Logic:**
- Measure disturbance before it affects process
- Calculate corrective action
- Adjust manipulated variable proactively

**Example:**
- Distillation: Measure feed flow, adjust steam proportionally
- Ratio control: Maintain reactant A:B = 3:1

**FF+FB Structure:**
- FF handles predictable disturbances
- FB handles model mismatch and unknowns

---

## Odibi Hands-On

### **Example 1: Ratio Control (3:1)**

```yaml
# ratio_control.yaml
# Maintain Flow_B = 3.0 × Flow_A
```

**Working example:** [/examples/cheme_course/L12_feedforward/ratio_control.yaml](file:///d:/odibi/examples/cheme_course/L12_feedforward/ratio_control.yaml)

### **Example 2: FF+FB Combination**

**Working example:** [/examples/cheme_course/L12_feedforward/ff_fb_combined.yaml](file:///d:/odibi/examples/cheme_course/L12_feedforward/ff_fb_combined.yaml)

---

## Summary

- ✅ Feedforward measures disturbances, acts proactively
- ✅ Ratio control maintains fixed proportions
- ✅ FF+FB combines best of both

**Next:** [L13: Nonlinearity](L13_nonlinearity.md)

---
