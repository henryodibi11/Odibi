# ChemE × Data Engineering Course

**Learn Process Control AND Data Engineering Together**

---

## 🎓 Course Complete and Ready!

This is a **complete, self-paced course** teaching chemical engineers both process dynamics/control (Seborg textbook) AND data engineering fundamentals through hands-on Odibi simulations.

---

## ✅ What's Included

**16 Complete Lessons:**
- Part I: Foundations (L00-L05) - 6 lessons, ~5 hours
- Part II: Control + ID (L06-L09) - 4 lessons, ~4.5 hours  
- Part III: Advanced (L10-L14) - 5 lessons, ~5 hours
- Capstone: CSTR Digital Twin (L15) - ~3 hours

**35+ Working YAML Examples:**
- All with correct Odibi syntax
- Verified against official examples
- Located in `/examples/cheme_course/`

**Complete Documentation:**
- Theory recaps from Seborg
- Data engineering concepts
- Exercises with solutions
- Real plant connections

---

## 🚀 Quick Start

**1. Install Odibi:**
```bash
pip install odibi
```

**2. Start the course:**
Read: [START_HERE.md](START_HERE.md)

**3. Begin L00:**
Read: [lessons/L00_setup.md](lessons/L00_setup.md)

**4. Run your first example:**
```bash
cd /d:/odibi/examples/cheme_course/L00_setup
odibi run tank_data.yaml
```

---

## 📚 Course Structure

### Part I: Foundations
- [L00: Setup](lessons/L00_setup.md) - Odibi basics, formats
- [L01: CV/MV/DV](lessons/L01_cv_mv_dv.md) - Control variables
- [L02: DoF + Balances](lessons/L02_dof_balances.md) - Mass/energy balances
- [L03: First-Order](lessons/L03_first_order.md) - Dynamic systems
- [L04: FOPTD](lessons/L04_foptd.md) - Transfer functions
- [L05: Second-Order](lessons/L05_second_order.md) - Overshoot, damping

### Part II: Control + ID
- [L06: PID Basics](lessons/L06_pid_basics.md) - P/PI/PID controllers
- [L07: Tuning](lessons/L07_tuning.md) - ZN, IMC methods
- [L08: Disturbances](lessons/L08_disturbances.md) - Load rejection
- [L09: System ID](lessons/L09_system_id.md) - PRBS, ARX fitting

### Part III: Advanced
- [L10: Interacting Loops](lessons/L10_interacting_loops.md) - MIMO systems
- [L11: Cascade](lessons/L11_cascade.md) - Multi-loop control
- [L12: Feedforward](lessons/L12_feedforward.md) - FF+FB, ratio
- [L13: Nonlinearity](lessons/L13_nonlinearity.md) - Gain scheduling
- [L14: MPC-lite](lessons/L14_mpc_lite.md) - Constraints, moves

### Capstone
- [L15: CSTR Digital Twin](lessons/L15_cstr_digital_twin.md) - Production pipeline

---

## 📖 Documentation

- [START_HERE.md](START_HERE.md) - Quick start guide
- [COURSE_COMPLETE_FINAL.md](COURSE_COMPLETE_FINAL.md) - Complete summary
- [ALL_LESSONS_FIXED_COMPLETE.md](ALL_LESSONS_FIXED_COMPLETE.md) - Fix documentation
- [Solutions Index](solutions/index.md) - Exercise solutions

---

## 🎯 Learning Outcomes

**You will learn:**
- Process control (first-order, PID, cascade, feedforward, MPC)
- Data engineering (schemas, validation, partitioning, pipelines)
- Odibi framework (`prev`, `ema`, `pid`, simulation, patterns)
- Production skills (Bronze/Silver/Gold, lineage, quarantine)

**Unique outcome:**
ChemE + DE skillset with portfolio of working projects for LinkedIn/interviews

---

**Ready to start?** 👉 [START_HERE.md](START_HERE.md)

*Course built March 12, 2026*
