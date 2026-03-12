# ChemE × Data Engineering Course

**Complete course teaching chemical engineers both process control and data engineering through hands-on Odibi simulations.**

## Overview

This course bridges process control theory (Seborg textbook) with practical data engineering using Odibi. All 35 YAML examples are tested and working.

## Course Structure

### 📚 16 Lessons, 35 Working Examples

| Lesson | Topic | Examples |
|--------|-------|----------|
| **L00** | Setup & Basics | tank_data, tank_data_parquet, tank_realistic, multi_entity |
| **L01** | CV/MV/DV | mixing_tank_ph, heat_exchanger |
| **L02** | DoF & Balances | tank_mass_balance, cstr_energy_balance |
| **L03** | First Order | tank_temperature_fo, ema_filtering |
| **L04** | FOPTD | tank_foptd, heat_exchanger_foptd, foptd_parameterized |
| **L05** | Second Order | pressure_underdamped, valve_actuator, damping_comparison |
| **L06** | PID Basics | tank_pi |
| **L07** | Tuning | ziegler_nichols, cohen_coon |
| **L08** | Disturbances | load_disturbance, feedwater_disturbance |
| **L09** | System ID | step_response, pulse_test |
| **L10** | Interacting Loops | dual_temperature, pressure_flow |
| **L11** | Cascade | temperature_cascade, level_flow_cascade |
| **L12** | Feedforward | simple_feedforward, ratio_control |
| **L13** | Nonlinearity | valve_nonlinearity, ph_neutralization |
| **L14** | MPC Lite | mpc_basics, constrained_control |
| **L15** | Digital Twin | cstr_full_model, optimization |

## Quick Start

### Run Any Example
```bash
# Activate virtual environment
D:\odibi\.venv-windows\Scripts\activate

# Run an example
odibi run examples/cheme_course/L00_setup/tank_data.yaml

# Run all tests
python scripts/test_all_cheme_examples.py
```

### Test Results
✅ **35/35 examples passing (100% success rate)**

See [TEST_RESULTS.md](TEST_RESULTS.md) for detailed test output.

## Learning Path

### Beginner Track (L00-L03)
Start here if you're new to Odibi or process control:
1. **L00/tank_data.yaml** - Learn simulation basics
2. **L01/mixing_tank_ph.yaml** - Understand CV/MV/DV
3. **L02/tank_mass_balance.yaml** - Mass balance fundamentals
4. **L03/tank_temperature_fo.yaml** - First-order dynamics

### Intermediate Track (L04-L08)
Build control systems knowledge:
1. **L04/tank_foptd.yaml** - Dead time effects
2. **L05/pressure_underdamped.yaml** - Second-order response
3. **L06/tank_pi.yaml** - Implement PI control
4. **L07/ziegler_nichols.yaml** - Tune controllers
5. **L08/load_disturbance.yaml** - Handle disturbances

### Advanced Track (L09-L15)
Master advanced control strategies:
1. **L09/step_response.yaml** - System identification
2. **L10/dual_temperature.yaml** - Interacting systems
3. **L11/temperature_cascade.yaml** - Multi-loop control
4. **L12/simple_feedforward.yaml** - Anticipatory control
5. **L13/valve_nonlinearity.yaml** - Nonlinear compensation
6. **L14/mpc_basics.yaml** - Predictive control
7. **L15/cstr_full_model.yaml** - Complete digital twin

## Key Concepts Demonstrated

### Data Engineering with Odibi
- ✅ Simulation engine with timestep control
- ✅ Random walk generators for realistic data
- ✅ Derived expressions with `prev()` for state
- ✅ Multi-entity simulations
- ✅ Parquet output format
- ✅ Data validation and quality checks

### Process Control
- ✅ PID controllers with `pid()` function
- ✅ Transfer functions (first and second order)
- ✅ Cascade control (primary/secondary loops)
- ✅ Feedforward compensation
- ✅ Ratio control
- ✅ Constraint handling
- ✅ System identification
- ✅ Nonlinear process behavior

### Chemical Engineering
- ✅ Mass and energy balances
- ✅ Reaction kinetics (CSTR)
- ✅ Heat exchangers
- ✅ Mixing tanks
- ✅ pH neutralization
- ✅ Operating point optimization

## Common Patterns

### Row Number Counter
Used for step changes and time-dependent logic:
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
    expression: "50.0 if row_num < 100 else 60.0"
```

### PID Controller
```yaml
- name: controller_output
  data_type: float
  generator:
    type: derived
    expression: >
      pid(
        process_variable,
        setpoint,
        Kp,    # Proportional gain
        Ki,    # Integral gain
        Kd,    # Derivative gain
        dt,    # Sample time
        min,   # Output minimum
        max,   # Output maximum
        true   # Anti-windup
      )
```

### EMA Smoothing
```yaml
- name: smoothed_value
  data_type: float
  generator:
    type: derived
    expression: "ema('raw_value', 0.1, 0.0)"
```

## Documentation

- **Lesson Details:** `docs/learning/cheme_data_course/` (16 markdown files)
- **Test Results:** [TEST_RESULTS.md](TEST_RESULTS.md)
- **Test Script:** `scripts/test_all_cheme_examples.py`
- **Odibi Deep Dive:** `docs/ODIBI_DEEP_CONTEXT.md`

## Course Philosophy

**"Can you hand the docs to a business analyst and have them build a working pipeline without your help?"**

This course proves the answer is YES. Every example:
- ✅ Runs successfully with `odibi run`
- ✅ Produces real simulation data
- ✅ Demonstrates practical control concepts
- ✅ Uses beginner-friendly explanations
- ✅ Includes process engineering context

## Success Metrics

| Metric | Target | Actual |
|--------|--------|--------|
| Examples working | 100% | ✅ 100% (35/35) |
| Lessons complete | 16 | ✅ 16/16 |
| Test automation | Yes | ✅ Full test suite |
| Beginner friendly | Yes | ✅ Self-service ready |

## Next Steps

1. **Run the examples** - Start with L00 and progress through
2. **Modify parameters** - Experiment with tuning, setpoints, disturbances
3. **Create your own** - Use these as templates for real processes
4. **Share results** - Generate HTML stories with `odibi story show`

## Credits

Built with ❤️ using Odibi framework by a solo data engineer who needed tools that buy back time and freedom.

**Framework Status:** Production-ready, battle-tested on Databricks
