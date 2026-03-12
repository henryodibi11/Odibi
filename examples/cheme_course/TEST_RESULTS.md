# ChemE Course YAML Examples - Test Results

**Date:** March 12, 2026  
**Status:** ✅ ALL 35 EXAMPLES WORKING - COURSE COMPLETE

## Summary

All 35 YAML examples across 16 lessons in the complete ChemE × Data Engineering course have been tested and verified working with `odibi run`.

### Test Results

| Lesson | File | Status |
|--------|------|--------|
| **L00 - Setup** |
| L00 | tank_data.yaml | ✅ Working |
| L00 | tank_data_parquet.yaml | ✅ Working |
| L00 | tank_realistic.yaml | ✅ Working |
| L00 | multi_entity.yaml | ✅ Working |
| **L01 - CV/MV/DV** |
| L01 | mixing_tank_ph.yaml | ✅ Working |
| L01 | heat_exchanger.yaml | ✅ Working |
| **L02 - DoF & Balances** |
| L02 | tank_mass_balance.yaml | ✅ Working |
| L02 | cstr_energy_balance.yaml | ✅ Working |
| **L03 - First Order** |
| L03 | tank_temperature_fo.yaml | ✅ Working |
| L03 | ema_filtering.yaml | ✅ Working (fixed) |
| **L04 - FOPTD** |
| L04 | tank_foptd.yaml | ✅ Working |
| L04 | heat_exchanger_foptd.yaml | ✅ Working |
| L04 | foptd_parameterized.yaml | ✅ Working |
| **L05 - Second Order** |
| L05 | pressure_underdamped.yaml | ✅ Working |
| L05 | valve_actuator.yaml | ✅ Working (fixed) |
| L05 | damping_comparison.yaml | ✅ Working |
| **L06 - PID Basics** |
| L06 | tank_pi.yaml | ✅ Working (fixed) |
| **L07 - Tuning** |
| L07 | ziegler_nichols.yaml | ✅ Working |
| L07 | cohen_coon.yaml | ✅ Working |
| **L08 - Disturbances** |
| L08 | load_disturbance.yaml | ✅ Working |
| L08 | feedwater_disturbance.yaml | ✅ Working |
| **L09 - System ID** |
| L09 | step_response.yaml | ✅ Working |
| L09 | pulse_test.yaml | ✅ Working |
| **L10 - Interacting Loops** |
| L10 | dual_temperature.yaml | ✅ Working |
| L10 | pressure_flow.yaml | ✅ Working |
| **L11 - Cascade Control** |
| L11 | temperature_cascade.yaml | ✅ Working |
| L11 | level_flow_cascade.yaml | ✅ Working |
| **L12 - Feedforward** |
| L12 | simple_feedforward.yaml | ✅ Working |
| L12 | ratio_control.yaml | ✅ Working |
| **L13 - Nonlinearity** |
| L13 | valve_nonlinearity.yaml | ✅ Working |
| L13 | ph_neutralization.yaml | ✅ Working |
| **L14 - MPC Lite** |
| L14 | mpc_basics.yaml | ✅ Working |
| L14 | constrained_control.yaml | ✅ Working |
| **L15 - CSTR Digital Twin** |
| L15 | cstr_full_model.yaml | ✅ Working |
| L15 | optimization.yaml | ✅ Working |

**Total:** 35 examples (16 lessons)  
**Passed:** 35 (100%)  
**Failed:** 0

## Fixes Applied

### Key Pattern: `row_num` Counter

Several files needed the `row_num` pattern for step changes and time-dependent logic:

```yaml
- name: row_num
  data_type: int
  generator:
    type: derived
    expression: "prev('row_num', -1) + 1"

# Then use for step changes:
- name: setpoint
  data_type: float
  generator:
    type: derived
    expression: "10.0 if row_num < 30 else 12.0"
```

### Timestep Format Fixes

Fixed invalid timestep formats:
- ❌ `1sec`, `1min`, `100ms`  
- ✅ `1s`, `1m`, `10s`

### Files That Required Fixes

1. **L03/ema_filtering.yaml**
   - Fixed timestep: `1sec` → `1s`
   - Added `row_num` counter

2. **L05/valve_actuator.yaml**
   - Fixed timestep: `100ms` → `1s`
   - Added `row_num` counter
   - Converted sequential columns to derived

3. **L06/tank_pi.yaml**
   - Added `row_num` counter for step change logic

## Validated Features

These examples demonstrate working:

- ✅ Simulation engine with timestep control
- ✅ Random walk generators
- ✅ Derived expressions with `prev()`
- ✅ EMA smoothing with `ema()`
- ✅ PID controller with `pid()`
- ✅ First-order transfer functions
- ✅ Second-order transfer functions
- ✅ Step changes using `row_num`
- ✅ Multi-entity simulations
- ✅ Parquet output format
- ✅ Data validation (range checks)
- ✅ Mass and energy balances
- ✅ Process control concepts (CV/MV/DV)

## How to Test

Run all tests:
```bash
python scripts/test_all_cheme_examples.py
```

Test individual file:
```bash
D:\odibi\.venv-windows\Scripts\python.exe -m odibi.cli run examples/cheme_course/L00_setup/tank_data.yaml
```

## Course Coverage

The complete ChemE × Data Engineering course includes:

### Fundamentals (L00-L06)
- **L00:** Odibi basics, data generation, multiple entities
- **L01:** Control variables (CV/MV/DV), process diagrams
- **L02:** Degrees of freedom, mass/energy balances
- **L03:** First-order systems, exponential smoothing (EMA)
- **L04:** First-order plus dead time (FOPTD) models
- **L05:** Second-order systems, damping ratios
- **L06:** PI/PID controller basics

### Advanced Control (L07-L12)
- **L07:** PID tuning (Ziegler-Nichols, Cohen-Coon)
- **L08:** Disturbance rejection, load changes
- **L09:** System identification (step test, pulse test)
- **L10:** Interacting control loops
- **L11:** Cascade control (primary/secondary loops)
- **L12:** Feedforward control, ratio control

### Expert Topics (L13-L15)
- **L13:** Nonlinear systems (valve characteristics, pH control)
- **L14:** Model predictive control (MPC) introduction
- **L15:** Complete CSTR digital twin with optimization

## References

- Working pattern: [examples/renewable_energy/battery_soc_simulation.yaml](../renewable_energy/battery_soc_simulation.yaml)
- Official docs: [docs/ODIBI_DEEP_CONTEXT.md](../../docs/ODIBI_DEEP_CONTEXT.md)
- Test script: [scripts/test_all_cheme_examples.py](../../scripts/test_all_cheme_examples.py)
