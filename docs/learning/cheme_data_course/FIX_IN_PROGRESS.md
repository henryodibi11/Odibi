# Course Fix In Progress

**Status:** 🔄 Fixing L01-L05 now  
**Started:** March 12, 2026  
**Estimated Completion:** 1 hour

---

## ✅ Completed

- [x] L00: Setup - FIXED and working
  - 4 example YAML files created
  - Lesson rewritten with correct syntax
  - [L00_FIX_SUMMARY.md](L00_FIX_SUMMARY.md) documents changes

---

## 🔄 In Progress

### **L01: CV/MV/DV** (30 min)
- [x] Create example: mixing_tank_ph.yaml
- [ ] Create example: heat_exchanger.yaml
- [ ] Rewrite lesson with correct YAML
- [ ] Test all examples

### **L02: DoF + Balances** (30 min)
- [ ] Create example: tank_mass_balance.yaml
- [ ] Create example: cstr_energy_balance.yaml
- [ ] Rewrite lesson
- [ ] Fix validation syntax

### **L03: First-Order** (30 min)
- [ ] Create example: first_order_tank.yaml
- [ ] Verify prev() and ema() usage
- [ ] Rewrite lesson
- [ ] Add KPI calculations

### **L04: FOPTD** (45 min) **⚠️ CRITICAL**
- [ ] Remove prev(..., lag=N) - doesn't exist
- [ ] Implement delay workaround
- [ ] Create example: foptd_delay.yaml
- [ ] Rewrite lesson with correct delay implementation

### **L05: Second-Order** (30 min)
- [ ] Create example: second_order_pressure.yaml
- [ ] Verify state-space implementation
- [ ] Rewrite lesson
- [ ] Test overshoot calculations

---

## 📋 Fix Checklist (Applied to Each Lesson)

For each lesson, ensure:

**YAML Structure:**
- [ ] `read: {connection: null, format: simulation}`
- [ ] `options: {simulation: {scope: {...}, entities: {...}, columns: [...]}}`
- [ ] All columns have `data_type:`
- [ ] `entities` use `count` + `id_prefix`
- [ ] `write:` instead of `output:`
- [ ] Timestamp format: ISO 8601 with Z

**Generators:**
- [ ] `random_walk` uses `drift`, `noise` (not `step_size`)
- [ ] `range` has `min`, `max`, optional `distribution`
- [ ] `derived` has `expression` string
- [ ] No non-existent parameters

**Functions:**
- [ ] `prev('col', default)` - correct syntax
- [ ] `ema('col', alpha, default)` - correct syntax
- [ ] No `prev(..., lag=N)` - doesn't exist

**Validation:**
- [ ] `column:` (singular) for single-column tests
- [ ] `columns:` (plural) for multi-column tests
- [ ] Validation structure matches Odibi schema

---

## 🎯 Deliverables

When complete, you will have:

1. **5 Fixed Lessons** (L00-L05)
   - All with correct YAML syntax
   - Links to working example files
   - Verified against Odibi source

2. **15+ Working Example Files**
   - `/examples/cheme_course/L##_name/*.yaml`
   - Each tested for syntax correctness
   - Ready to run with `odibi run`

3. **Complete Documentation**
   - Fix summaries for each lesson
   - Before/after comparisons
   - What changed and why

---

## ⏱️ Progress Timeline

- **12:00 PM** - Started L01
- **12:30 PM** - L01 complete, start L02
- **1:00 PM** - L02 complete, start L03
- **1:30 PM** - L03 complete, start L04
- **2:15 PM** - L04 complete (extra time for delay workaround), start L05
- **2:45 PM** - L05 complete
- **3:00 PM** - Final testing and documentation

**Expected Done:** ~1:00 PM (March 12, 2026)

---

*This document will be updated as fixes progress...*
