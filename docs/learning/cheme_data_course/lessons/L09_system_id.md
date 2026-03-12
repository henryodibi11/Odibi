# L09: System Identification

**Prerequisites:** L00-L08 | **Effort:** 90 min | **Seborg:** Chapter 7

---

## Learning Objectives

1. ✅ Generate PRBS (Pseudo-Random Binary Sequence) excitation signals
2. ✅ Collect input-output data for system identification
3. ✅ Understand train/test data splits for model validation
4. ✅ Export data for ARX/FOPTD model fitting in Python
5. ✅ Validate identified models against test data

---

## Theory Recap: System Identification (Seborg Ch. 7)

**Goal:** Determine process model (K, τ, θ) from input-output data

**Steps:**
1. **Design experiment:** Choose excitation signal (PRBS, steps, ramps)
2. **Collect data:** Run test, record input (u) and output (y)
3. **Fit model:** Estimate parameters using least squares, ARX, etc.
4. **Validate:** Test model on independent data set

**PRBS (Pseudo-Random Binary Sequence):**
- Binary signal that switches between two levels
- "Pseudo-random" pattern excites all frequencies
- Better than single step (more information content)
- Switchtime ≈ 0.5τ to 2τ (need rough τ estimate)

---

## Odibi Hands-On

### **Example 1: Generate PRBS Input Data**

```yaml
# prbs_excitation.yaml
# Generates PRBS input + FOPTD response for system ID
```

**Working example:** [/examples/cheme_course/L09_system_id/prbs_excitation.yaml](file:///d:/odibi/examples/cheme_course/L09_system_id/prbs_excitation.yaml)

### **Example 2: Train/Test Split**

Split data 70/30 for model fitting and validation.

**Working example:** [/examples/cheme_course/L09_system_id/train_test_split.yaml](file:///d:/odibi/examples/cheme_course/L09_system_id/train_test_split.yaml)

### **Example 3: Python Analysis Script**

```python
# fit_arx_model.py
# Loads Odibi-generated PRBS data
# Fits ARX model using scipy or control library
# Validates on test set
```

**Working example:** [/examples/cheme_course/L09_system_id/fit_arx_model.py](file:///d:/odibi/examples/cheme_course/L09_system_id/fit_arx_model.py)

---

## Summary

- ✅ PRBS excites process better than single step
- ✅ Odibi generates identification data
- ✅ Python fits models (ARX, FOPTD)
- ✅ Always validate on test data

**Next:** [L10: Interacting Loops](L10_interacting_loops.md)

---
