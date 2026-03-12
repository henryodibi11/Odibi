# Odibi v3.4.0 - ChemE × Data Engineering Course Release

**Release Date:** March 12, 2026

## 🎓 Major Feature: Complete Chemical Engineering Course

This release introduces a **complete 16-lesson course** teaching chemical engineers both process control and data engineering through hands-on Odibi simulations.

### 📊 By The Numbers

- **16 lessons** (L00-L15): From setup to complete CSTR digital twin
- **35 YAML examples**: All tested and working (100% success rate)
- **20-25 hours** of learning content
- **40+ exercises** with solutions
- **Seborg textbook coverage**: Chapters 1-20 mapped to examples

### 🎯 What You Get

#### Part I: Foundations (L00-L05)
Learn Odibi basics while simulating:
- Tank level control with mass balances
- First-order temperature dynamics  
- Second-order pressure systems with damping
- Time-series data generation and validation

#### Part II: Control & System ID (L06-L09)
Master PID controllers and tuning:
- PI/PID implementation with anti-windup
- Ziegler-Nichols and Cohen-Coon tuning methods
- Load disturbance rejection
- Step test and pulse test system identification

#### Part III: Advanced Control (L10-L14)
Implement sophisticated strategies:
- Interacting MIMO control loops
- Cascade control (primary/secondary)
- Feedforward compensation and ratio control
- Nonlinear valve characteristics and pH control
- Model predictive control (MPC) with constraints

#### Capstone: Digital Twin (L15)
Build a complete CSTR simulation:
- Mass, energy, and reaction kinetics
- Operating point optimization
- Full thermodynamic modeling

### 🚀 Quick Start

```bash
# Install/upgrade Odibi
pip install --upgrade odibi

# Navigate to course
cd examples/cheme_course

# Run your first example
odibi run L00_setup/tank_data.yaml

# See all examples
python ../../scripts/test_all_cheme_examples.py
```

### 📚 Documentation

**Entry point:** `docs/learning/cheme_data_course/START_HERE.md`

Or visit the mkdocs site: **Learn → ChemE × Data Engineering Course**

### ✅ Validation

Every example has been tested:

```bash
Testing 35 ChemE course YAML examples...
Total: 35
Passed: 35 (100%)
Failed: 0

*** All tests passed! ***
```

### 🔧 Key Patterns Validated

**Row Number Counter** (for step changes):
```yaml
- name: row_num
  generator:
    type: derived
    expression: "prev('row_num', -1) + 1"
```

**PID Controller**:
```yaml
- name: controller_output
  generator:
    type: derived
    expression: "pid(pv, sp, Kp, Ki, Kd, dt, min, max, true)"
```

**EMA Smoothing**:
```yaml
- name: smoothed_value
  generator:
    type: derived
    expression: "ema('raw_value', 0.1, 0.0)"
```

### 🎓 Who This Is For

- **Chemical engineers** learning data engineering
- **Data engineers** working with process data
- **Students** studying process control
- **Industry professionals** building digital twins

### 🏆 Learning Outcomes

After completing this course, you can:
- ✅ Build realistic process simulations generating millions of rows
- ✅ Implement PID controllers from textbook specifications
- ✅ Tune controllers using industry-standard methods
- ✅ Model complex chemical processes (CSTR, heat exchangers)
- ✅ Create production-ready data pipelines
- ✅ Build digital twins of industrial equipment

### 📖 Course Philosophy

**"Can you hand the docs to a business analyst and have them build a working pipeline without your help?"**

**Answer: YES.** This course proves it.

Every lesson includes:
- Theory recap from Seborg textbook
- Runnable YAML examples
- Data engineering concepts
- Hands-on exercises
- Real plant operation context

### 🔗 Resources

- **Course Docs**: `docs/learning/cheme_data_course/`
- **Examples**: `examples/cheme_course/`
- **Test Suite**: `scripts/test_all_cheme_examples.py`
- **Textbook Mapping**: `docs/guides/seborg_textbook_mapping.md`

### 🐛 Bug Fixes

- Fixed YAML timestep formats (standardized to `1s`, `5s`, `1m`)
- Fixed simulation expression patterns (use `row_num` instead of `timestamp.shift()`)
- Converted sequential columns to derived type

---

## 💡 What's Next

This course is **production-ready** and **self-service**. Future enhancements could include:
- Interactive Jupyter notebooks
- Video walkthroughs
- Real plant data integration
- Full multivariable MPC examples

---

**Full Changelog:** See [CHANGELOG.md](../CHANGELOG.md)

**Installation:** `pip install --upgrade odibi`

**Report Issues:** https://github.com/henryodibi11/Odibi/issues
