# 🎓 ChemE × Data Engineering Course - START HERE

**Complete self-paced course teaching Chemical Engineers data engineering through process control**

---

## ✅ **ALL 16 LESSONS COMPLETE AND TESTED** ✅

### **35 Working YAML Examples** (100% tested, 100% passing)

All lessons include:
- ✅ Theory recap from Seborg textbook
- ✅ **Runnable YAML examples** (tested with `odibi run`)
- ✅ Data engineering concepts explained
- ✅ Validation patterns and quarantine
- ✅ Exercises with solution hints
- ✅ Real plant operation connections
- ✅ Progressive difficulty from beginner to expert

**Total course time:** ~20-25 hours  
**Part I time:** ~5 hours (beginner-friendly)

---

## 🚀 Quick Start (5 Minutes)

**Step 1: Install Odibi**
```bash
pip install odibi
odibi --version
```

**Step 2: Run Your First Example**
```bash
# Navigate to course examples
cd examples/cheme_course

# Run first simulation
odibi run L00_setup/tank_data.yaml

# View the generated data
python -c "import pandas as pd; print(pd.read_parquet('data/tank_data.parquet').head())"
```

**Step 3: Start Learning**
Open [L00: Setup](lessons/L00_setup.md) and follow along.

**You're learning!** 🎉

---

## 📚 Your Learning Path

### **Part I: Foundations** ✅ COMPLETE

**[L00: Setup & Basics](lessons/L00_setup.md)** *(45 min)*  
🎯 Install Odibi, run first pipeline, understand data formats  
📂 Examples: `tank_data.yaml`, `tank_data_parquet.yaml`, `tank_realistic.yaml`, `multi_entity.yaml`

**[L01: CV/MV/DV and Time Series](lessons/L01_cv_mv_dv.md)** *(45 min)*  
🎯 Controlled/Manipulated/Disturbance variables  
📂 Examples: `mixing_tank_ph.yaml`, `heat_exchanger.yaml`

**[L02: Degrees of Freedom + Balances](lessons/L02_dof_balances.md)** *(60 min)*  
🎯 DoF analysis, mass/energy balances  
📂 Examples: `tank_mass_balance.yaml`, `cstr_energy_balance.yaml`

**[L03: First-Order Dynamics](lessons/L03_first_order.md)** *(45 min)*  
🎯 `prev()` and `ema()` stateful functions  
📂 Examples: `tank_temperature_fo.yaml`, `ema_filtering.yaml`

**[L04: FOPTD Transfer Functions](lessons/L04_foptd.md)** *(45 min)*  
🎯 First-Order Plus Time Delay modeling  
📂 Examples: `tank_foptd.yaml`, `heat_exchanger_foptd.yaml`, `foptd_parameterized.yaml`

**[L05: Second-Order Systems](lessons/L05_second_order.md)** *(60 min)*  
🎯 Damping ratio, overshoot, settling time  
📂 Examples: `pressure_underdamped.yaml`, `valve_actuator.yaml`, `damping_comparison.yaml`

---

### **Part II: Feedback Control & System ID** ✅ COMPLETE

**[L06: PID Basics](lessons/L06_pid_basics.md)** *(60 min)*  
🎯 P/I/D actions, `pid()` function, anti-windup  
📂 Examples: `tank_pi.yaml`

**[L07: PID Tuning Methods](lessons/L07_tuning.md)** *(60 min)*  
🎯 Ziegler-Nichols, Cohen-Coon tuning  
📂 Examples: `ziegler_nichols.yaml`, `cohen_coon.yaml`

**[L08: Disturbance Rejection](lessons/L08_disturbances.md)** *(60 min)*  
🎯 Load rejection, feedwater disturbances  
📂 Examples: `load_disturbance.yaml`, `feedwater_disturbance.yaml`

**[L09: System Identification](lessons/L09_system_id.md)** *(90 min)*  
🎯 Step test, pulse test for parameter estimation  
📂 Examples: `step_response.yaml`, `pulse_test.yaml`

---

### **Part III: Advanced Control Strategies** ✅ COMPLETE

**[L10: Interacting Control Loops](lessons/L10_interacting_loops.md)** *(60 min)*  
🎯 MIMO systems, loop interactions  
📂 Examples: `dual_temperature.yaml`, `pressure_flow.yaml`

**[L11: Cascade Control](lessons/L11_cascade.md)** *(60 min)*  
🎯 Primary/secondary loops, fast inner loops  
📂 Examples: `temperature_cascade.yaml`, `level_flow_cascade.yaml`

**[L12: Feedforward Control](lessons/L12_feedforward.md)** *(60 min)*  
🎯 Anticipatory control, ratio control  
📂 Examples: `simple_feedforward.yaml`, `ratio_control.yaml`

**[L13: Nonlinear Systems](lessons/L13_nonlinearity.md)** *(60 min)*  
🎯 Valve characteristics, pH neutralization  
📂 Examples: `valve_nonlinearity.yaml`, `ph_neutralization.yaml`

**[L14: Model Predictive Control Intro](lessons/L14_mpc_lite.md)** *(90 min)*  
🎯 Prediction, optimization, constraints  
📂 Examples: `mpc_basics.yaml`, `constrained_control.yaml`

---

### **Capstone: Real-World Digital Twin** ✅ COMPLETE

**[L15: CSTR Digital Twin](lessons/L15_cstr_digital_twin.md)** *(2-3 hours)*  
🎯 Complete reactor model with mass, energy, kinetics  
📂 Examples: `cstr_full_model.yaml`, `optimization.yaml`

---

## 💪 What You'll Learn

### **After completing this course:**

**Data Engineering Skills:**
- ✅ Generate realistic plant time-series data
- ✅ Use CSV, Parquet, and Delta Lake formats
- ✅ Validate data quality with range checks
- ✅ Implement quarantine patterns
- ✅ Build reproducible simulations
- ✅ Handle multi-entity pipelines

**Process Control Skills:**
- ✅ Map process control to data schemas (CV/MV/DV)
- ✅ Implement mass and energy balances
- ✅ Model first and second-order dynamics
- ✅ Design and tune PID controllers
- ✅ Perform system identification
- ✅ Build cascade and feedforward controllers
- ✅ Handle nonlinear processes
- ✅ Apply basic model predictive control

**Chemical Engineering:**
- ✅ CSTR modeling (kinetics, thermodynamics)
- ✅ Heat exchanger dynamics
- ✅ Tank level control
- ✅ pH neutralization
- ✅ Operating point optimization

**Career Skills:**
- ✅ Portfolio of 35+ working examples
- ✅ Production-ready data pipelines
- ✅ Bridge between ChemE and data roles

---

## 🎯 Success Metrics

**After completing this course, you can:**

✅ Build simulations generating millions of rows of realistic process data  
✅ Implement PID controllers from Seborg textbook  
✅ Tune controllers using industry methods (Z-N, Cohen-Coon)  
✅ Model complex systems (CSTR, heat exchangers, cascades)  
✅ Explain to data engineers: "This is a controlled variable"  
✅ Explain to process engineers: "This is a Parquet file"  
✅ Build production-ready data pipelines  
✅ Create digital twins of chemical processes

---

## 📊 Course Statistics

| Metric | Count |
|--------|-------|
| **Total Lessons** | 16 (L00-L15) |
| **YAML Examples** | 35 working examples |
| **Test Pass Rate** | 100% (35/35) |
| **Course Time** | 20-25 hours |
| **Exercises** | 40+ hands-on problems |
| **Seborg Coverage** | Chapters 1-20 |

---

## 🛠️ Course Structure

### Where Everything Lives

**YAML Examples:**
```
examples/cheme_course/
├── L00_setup/           # 4 examples
├── L01_cv_mv_dv/        # 2 examples
├── L02_dof_balances/    # 2 examples
├── L03_first_order/     # 2 examples
├── L04_foptd/           # 3 examples
├── L05_second_order/    # 3 examples
├── L06_pid_basics/      # 1 example
├── L07_tuning/          # 2 examples
├── L08_disturbances/    # 2 examples
├── L09_system_id/       # 2 examples
├── L10_interacting_loops/ # 2 examples
├── L11_cascade/         # 2 examples
├── L12_feedforward/     # 2 examples
├── L13_nonlinearity/    # 2 examples
├── L14_mpc_lite/        # 2 examples
├── L15_cstr_digital_twin/ # 2 examples
├── README.md            # Quick reference
└── TEST_RESULTS.md      # Test validation
```

**Lesson Documentation:**
```
docs/learning/cheme_data_course/
├── START_HERE.md        # This file
├── index.md             # Course overview
├── lessons/
│   ├── L00_setup.md through L15_cstr_digital_twin.md
│   └── (16 lesson files with theory + exercises)
└── solutions/
    └── index.md         # Solutions hub
```

---

## 🧪 Testing & Validation

All 35 examples have been tested and verified:

```bash
# Run the test suite
python scripts/test_all_cheme_examples.py

# Result:
# Total: 35
# Passed: 35 (100%)
# Failed: 0
```

See [TEST_RESULTS.md](../../examples/cheme_course/TEST_RESULTS.md) for detailed validation.

---

## 📖 Key Concepts & Patterns

### Row Number Counter (Essential Pattern)
```yaml
# Use this for step changes and time-dependent logic
- name: row_num
  data_type: int
  generator:
    type: derived
    expression: "prev('row_num', -1) + 1"

# Then create step changes:
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
        dt,    # Sample time (seconds)
        min,   # Output minimum
        max,   # Output maximum
        true   # Anti-windup enabled
      )
```

### EMA Smoothing
```yaml
- name: smoothed_value
  data_type: float
  generator:
    type: derived
    expression: "ema('raw_value', alpha, default)"
```

---

## 🎓 Teaching Philosophy

### **1. Hands-On First**
Don't just read - run code, generate data, see results immediately.

### **2. ChemE Problems → Data Skills**
Learn Parquet by simulating tanks. Learn validation by modeling reactors.

### **3. Incremental Complexity**
L00: Simple CSV files → L15: Production digital twin pipelines

### **4. Real-World Focus**
Every lesson connects to actual plant operations.

### **5. Portfolio Building**
By the end, you have 35+ working examples for LinkedIn/interviews.

---

## 🤔 FAQ

**Q: Is everything really complete and tested?**  
A: Yes! All 35 YAML examples pass `odibi run` with 100% success rate.

**Q: Do I need the Seborg textbook?**  
A: No! Lessons recap key concepts. But it helps for deeper theory.

**Q: Can I skip lessons?**  
A: Start with L00-L03 to learn basics. Then pick topics you need.

**Q: How long does the full course take?**  
A: 20-25 hours total. Part I (L00-L05) takes ~5 hours.

**Q: Can I use this to teach others?**  
A: Absolutely! Share, improve, contribute back.

**Q: What if I get stuck?**  
A: Check solutions, consult docs, or ask questions.

**Q: Can I run these on Databricks?**  
A: Yes! All examples work on local Pandas or Databricks Spark.

---

## 🔗 Additional Resources

**Course Materials:**
- [Course Overview](index.md) - Philosophy and structure
- [Seborg Textbook Mapping](../../guides/seborg_textbook_mapping.md) - All chapters mapped to Odibi
- [Process Simulation Guide](../../guides/process_simulation_guide.md) - Deep dive on stateful functions
- [Solutions Index](solutions/index.md) - Exercise solutions

**Framework Guides:**
- [Chemical Engineering Simulation](../../guides/chemical_engineering_simulation_guide.md)
- [Thermodynamics Transformers](../../guides/thermodynamics.md)
- [Unit Conversion](../../guides/unit_conversion.md)
- [Custom Functions Reference](../../guides/custom_functions.md)

---

## 👉 Get Started Now

### **Recommended Path:**

1. **Read** [Course Overview](index.md) *(10 min)*
2. **Install** Odibi *(5 min)*
3. **Start** [L00: Setup](lessons/L00_setup.md) *(45 min)*
4. **Progress** through L01-L05 at your own pace *(4 hours)*
5. **Advance** to Part II (L06-L09) for control topics *(4 hours)*
6. **Master** Part III (L10-L14) for advanced strategies *(5 hours)*
7. **Build** L15 digital twin capstone project *(2-3 hours)*

### **Alternative Paths:**

**Path A - Just the Basics (5 hours):**
L00 → L01 → L02 → L03 → L04 → L05

**Path B - PID Focus (8 hours):**
L00 → L01 → L03 → L06 → L07 → L08

**Path C - Advanced Only (6 hours):**
L00 (setup) → L10 → L11 → L12 → L13 → L14

**Path D - Digital Twin Sprint (4 hours):**
L00 → L02 → L15

---

## ✨ Bottom Line

**You have a COMPLETE, production-tested course ready to use RIGHT NOW.**

- ✅ All 16 lessons built with theory + exercises
- ✅ All 35 YAML examples tested and working
- ✅ Beginner to expert progression
- ✅ Real process control + data engineering skills
- ✅ Portfolio-ready examples

**Start learning today:**  
👉 **[L00: Setup & Basics](lessons/L00_setup.md)**

---

*Built with ❤️ for Chemical Engineers learning Data Engineering*  
*Part of the Odibi Framework - Explicit over implicit, Stories over magic*
