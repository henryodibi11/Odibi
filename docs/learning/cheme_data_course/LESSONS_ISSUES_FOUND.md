# Lessons Issues Found - Testing Report

**Date:** March 12, 2026  
**Tested By:** AI validation against Odibi codebase  
**Status:** 🔴 **CRITICAL ISSUES FOUND - Lessons need fixing**

---

## ❌ Critical Issue: YAML Structure is WRONG

### **Problem:**

All lessons (L00-L05) use an **INCORRECT YAML structure** for simulation that doesn't match Odibi's actual implementation.

### **What the lessons show (WRONG):**

```yaml
pipeline:
  name: my_pipeline

connections:
  output:
    type: local
    path: ./output/data.parquet

nodes:
  - name: generate_data
    output: output

    simulation:              # ❌ WRONG - "simulation" is not a top-level node key
      entities: ["tank_001"]
      start_time: "2024-01-01 00:00:00"
      timestep: "1min"
      row_count: 100
      columns:
        - name: my_column
          generator:
            type: constant
```

### **What Odibi actually expects (CORRECT):**

```yaml
nodes:
  - name: generate_data
    read:                          # ✅ Simulation goes under "read"
      connection: null             # ✅ Required: null connection for simulation
      format: simulation           # ✅ Required: format must be "simulation"
      options:
        simulation:                # ✅ Options > simulation > scope/entities/columns
          scope:
            start_time: "2024-01-01T00:00:00Z"
            timestep: "1min"
            row_count: 100
            seed: 42
          entities:
            count: 1               # ✅ Or list: ["tank_001"]
            id_prefix: "entity_"
          columns:
            - name: my_column
              data_type: float     # ✅ Required: data_type for each column
              generator:
                type: constant
                value: 10.0
```

---

## 🔍 Detailed Issues by Lesson

### **L00: Setup**

**Issues:**
1. ❌ `simulation:` as direct child of node (should be `read: format: simulation`)
2. ❌ `entities: ["TK-101"]` (should be `entities: {count: 1}` or list format)
3. ❌ Missing `connection: null` in read section
4. ❌ Missing `format: simulation` in read section
5. ❌ Missing `data_type` for all columns
6. ❌ `output:` key doesn't exist (should be `write:`)

**Example error locations:**
- All YAML examples in L00
- Tank data examples
- Multi-entity examples

---

### **L01: CV/MV/DV**

**Same issues as L00, plus:**
1. ❌ Partitioning example uses incorrect structure
2. ❌ `partition_cols` should be in `write:` section, not `connection:`

**Example:**
```yaml
# WRONG
connections:
  output:
    partition_cols: ["year", "month", "day"]

# CORRECT
nodes:
  - name: node
    write:
      connection: output
      partition_cols: ["year", "month", "day"]
```

---

### **L02: DoF + Balances**

**Same structural issues, plus:**
1. ❌ `unit:` is not a standard column property (it's just documentation)
2. ❌ Validation section structure incorrect
3. ❌ `on_fail: quarantine` placement wrong

**Validation should be:**
```yaml
nodes:
  - name: node
    read:
      # ... simulation config
    validate:
      tests:
        - type: range
          column: temperature  # singular, not "columns"
          min: 0
          max: 100
      on_fail: quarantine  # At validate level, not test level
```

---

### **L03: First-Order Dynamics**

**Issues:**
1. ❌ All structural issues from L00-L02
2. ✅ `prev()` function usage is CORRECT
3. ✅ `ema()` function usage is CORRECT
4. ❌ Expression syntax may need adjustment

---

### **L04: FOPTD**

**Issues:**
1. ❌ All structural issues
2. ❌ `prev('input', 0.0, lag=2)` - **lag parameter doesn't exist!**
3. ❌ Time delay must be implemented differently

**How to actually implement delay:**
```yaml
# Store historical values manually
- name: input_t_minus_1
  data_type: float
  generator:
    type: derived
    expression: "prev('input', 0.0)"

- name: input_t_minus_2
  data_type: float
  generator:
    type: derived
    expression: "prev('input_t_minus_1', 0.0)"

# Or use array storage (if supported)
```

---

### **L05: Second-Order Systems**

**Issues:**
1. ❌ All structural issues
2. ✅ State-space approach is conceptually correct
3. ❌ YAML structure won't work

---

## 🔧 What Needs to be Fixed

### **Priority 1: Update ALL YAML Examples**

Every lesson needs:
1. Change `simulation:` to `read: {format: simulation, options: {simulation: {...}}}`
2. Add `connection: null` to read section
3. Add `data_type:` to ALL columns
4. Change `entities: ["name"]` to `entities: {count: 1, id_prefix: "name"}`
5. Add `scope:` wrapper around start_time/timestep/row_count
6. Fix `output:` → `write:`
7. Add `seed:` to all simulations

### **Priority 2: Fix Features That Don't Exist**

1. **Remove `prev(..., lag=N)` syntax** - doesn't exist
   - Implement delays with multiple prev() calls
   - Or document limitation

2. **Fix validation structure**
   - `column:` (singular) not `columns:` (plural) for single-column tests
   - `on_fail:` at validate level, not test level

3. **Remove `unit:` field** - it's not validated, just documentation

### **Priority 3: Test All Examples**

Create actual YAML files in `/examples/cheme_course/L##_*/` and run them:

```bash
odibi run examples/cheme_course/L00_setup/tank_data.yaml
odibi run examples/cheme_course/L01_cv_mv_dv/mixing_tank_ph.yaml
# etc...
```

---

## ✅ What's Actually Correct

### **Concepts & Theory:**
- ✅ All process control theory is correct
- ✅ Seborg chapter mapping is accurate
- ✅ ChemE concepts properly explained

### **Functions:**
- ✅ `prev(column, default)` - exists and works
- ✅ `ema(column, alpha, default)` - exists and works
- ✅ `pid(pv, sp, Kp, Ki, Kd, dt, ...)` - exists and works
- ✅ `random_walk` generator - exists
- ✅ `derived` generator - exists
- ✅ `range`, `constant`, `categorical` generators - all exist

### **Learning Path:**
- ✅ Lesson progression is logical
- ✅ Exercises are valuable
- ✅ Data engineering concepts are well-explained

---

## 📋 Action Plan

### **Immediate Actions:**

1. **Create correct YAML template** for all lessons
2. **Test one lesson end-to-end** (L00) with real Odibi
3. **Fix and verify** remaining lessons (L01-L05)
4. **Create working example files** in `/examples/cheme_course/`
5. **Add validation tests** to ensure examples stay working

### **Testing Checklist:**

For each lesson:
- [ ] L00: Tank data YAML runs without errors
- [ ] L01: pH control YAML runs and generates expected data
- [ ] L02: Mass balance YAML passes validation
- [ ] L03: First-order response shows correct time constant
- [ ] L04: FOPTD shows delay and lag (need workaround for delay)
- [ ] L05: Second-order shows overshoot

### **Documentation Updates:**

- [ ] Add "Common Errors" section to each lesson
- [ ] Link to working examples in `/examples/`
- [ ] Add troubleshooting guide
- [ ] Create YAML validator script

---

## 🎯 Estimated Fix Time

- **Per lesson:** 30-45 minutes to fix YAML + test
- **Total for L00-L05:** 3-4 hours
- **Priority:** HIGH - lessons are unusable in current state

---

## 💡 Recommendations

### **Before releasing lessons:**
1. ✅ Run every YAML example through Odibi
2. ✅ Validate all feature references against codebase
3. ✅ Create CI test that runs all lesson examples
4. ✅ Add "Tested on Odibi v0.x.x" badges

### **For future lessons (L06-L15):**
1. ✅ Use correct YAML structure from the start
2. ✅ Reference working examples from /examples/
3. ✅ Test before documenting
4. ✅ Keep examples minimal (10-20 lines max)

---

## 📚 Reference: Correct Simulation Template

```yaml
# Minimal working simulation template
nodes:
  - name: my_simulation
    read:
      connection: null
      format: simulation
      options:
        simulation:
          scope:
            start_time: "2024-01-01T00:00:00Z"
            timestep: "1min"
            row_count: 100
            seed: 42

          entities:
            count: 1
            id_prefix: "entity_"

          columns:
            - name: timestamp
              data_type: timestamp
              generator:
                type: timestamp

            - name: value
              data_type: float
              generator:
                type: range
                min: 0.0
                max: 100.0

    write:
      connection: output
      format: parquet
```

---

## 🔗 Useful References

- ✅ [examples/simulation_example.yaml](../../examples/simulation_example.yaml) - Official example
- ✅ [examples/renewable_energy/battery_soc_simulation.yaml](../../examples/renewable_energy/battery_soc_simulation.yaml) - Stateful example
- ✅ [docs/features/simulation.md](../../features/simulation.md) - Feature docs
- ✅ [docs/features/stateful_simulation.md](../../features/stateful_simulation.md) - prev/ema/pid docs

---

## ⚠️ Bottom Line

**The course content (theory, exercises, progression) is EXCELLENT.**

**The YAML examples are BROKEN and won't run.**

**Fix Priority: CRITICAL**  
**Fix Time: 3-4 hours**  
**Status: Blocking course launch**

---

*Next step: Fix L00, test it end-to-end, then apply fixes to L01-L05.*
