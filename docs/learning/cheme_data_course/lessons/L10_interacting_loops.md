# L10: Interacting Control Loops

**Prerequisites:** L06-L09 | **Effort:** 60 min | **Seborg:** Chapter 16

---

## Learning Objectives

1. ✅ Understand MIMO (Multi-Input Multi-Output) systems
2. ✅ Implement two-tank system with interaction
3. ✅ Simulate coupled loops (one affects the other)
4. ✅ Observe loop interaction effects
5. ✅ Understand when decoupling or advanced control needed

---

## Theory Recap: Interacting Loops (Seborg Ch. 16)

**Loop Interaction:**
When one controller's output affects another loop's PV.

**Example: Two-tank system**
- Tank 1 valve affects Tank 1 level (direct)
- Tank 1 valve also affects Tank 2 level (interaction)
- Controllers "fight" each other

**Solution approaches:**
1. Detune one or both controllers
2. Cascade control
3. Decoupling control
4. Model Predictive Control (MPC)

---

## Odibi Hands-On

### **Example 1: Two-Tank Interaction**

```yaml
# two_tank_interaction.yaml
# Tank 1 feeds Tank 2
# Independent level controllers on each tank
# Observe interaction and oscillations
```

**Working example:** [/examples/cheme_course/L10_interacting_loops/two_tank_interaction.yaml](file:///d:/odibi/examples/cheme_course/L10_interacting_loops/two_tank_interaction.yaml)

---

## Summary

- ✅ Interaction occurs when loops share actuators or flows
- ✅ Can cause oscillations and poor performance
- ✅ Odibi simulates multi-entity interactions

**Next:** [L11: Cascade Control](L11_cascade.md)

---
