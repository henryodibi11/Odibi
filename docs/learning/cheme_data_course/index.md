# ChemE × Data Engineering Course

**Learn Process Control AND Data Engineering Together**

---

## What is This Course?

This is a **self-paced, hands-on course** that teaches you:
1. **Process Dynamics & Control** (from Seborg's textbook) - refreshed for working engineers
2. **Data Engineering fundamentals** - CSV, Parquet, time series, validation, pipelines

**The approach:** You already know ChemE. We'll use process control problems you understand to teach data engineering concepts.

**By the end:** You'll confidently model process systems with Odibi AND understand modern data engineering practices.

---

## Who Is This For?

- **Chemical engineers** in operations/analytics who want to learn data engineering
- **ChemEs** who forgot process control and want a practical refresher
- **Data engineers** learning process industries
- **Anyone** tired of generic e-commerce data tutorials

**Prerequisites:**
- Basic ChemE degree (understand tanks, reactors, control loops)
- Python basics (can run scripts, install packages)
- Willingness to learn by doing

---

## Course Structure

### **15 Lessons + 1 Capstone** (~20-30 hours total)

#### Part I: Foundations (Modeling + Data Basics)
**Learn:** First-order systems, transfer functions, time series, schemas, validation

- [L00: Setup - Odibi Basics & Data Formats](lessons/L00_setup.md) *(45 min)*
- [L01: CV/MV/DV and Time Series Data](lessons/L01_cv_mv_dv.md) *(45 min)*
- [L02: Degrees of Freedom + Mass/Energy Balances](lessons/L02_dof_balances.md) *(60 min)*
- [L03: First-Order Dynamics](lessons/L03_first_order.md) *(45 min)*
- [L04: FOPTD Transfer Functions](lessons/L04_foptd.md) *(45 min)*
- [L05: Second-Order Systems + Overshoot](lessons/L05_second_order.md) *(60 min)*

#### Part II: Feedback Control + Identification
**Learn:** PID control, tuning, disturbances, system ID, train/test splits

- [L06: PID Basics + Constraints](lessons/L06_pid_basics.md) *(60 min)*
- [L07: Tuning + Anti-windup](lessons/L07_tuning.md) *(60 min)*
- [L08: Disturbances + Setpoint Strategies](lessons/L08_disturbances.md) *(60 min)*
- [L09: System Identification (PRBS/ARX)](lessons/L09_identification.md) *(90 min)*

#### Part III: Multiloop + Advanced Topics
**Learn:** Cascade, feedforward, interactions, gain scheduling, MPC basics

- [L10: Interacting Loops](lessons/L10_interactions.md) *(60 min)*
- [L11: Cascade Control](lessons/L11_cascade.md) *(60 min)*
- [L12: Feedforward/Ratio Control](lessons/L12_feedforward.md) *(60 min)*
- [L13: Nonlinearity + Saturation](lessons/L13_nonlinearity.md) *(60 min)*
- [L14: MPC-lite (IMC Basics)](lessons/L14_mpc_lite.md) *(90 min)*

#### Capstone: Real-World Pipeline
**Build:** Bronze → Silver → Gold CSTR "digital twin" with lineage and validation

- [L15: Digital Twin Pipeline](lessons/L15_capstone_cstr.md) *(2-3 hours)*

---

## How to Use This Course

### **The Learning Path:**

1. **Start with L00** - Get Odibi installed and working
2. **Do lessons in order** - Each builds on previous concepts
3. **Run the YAML** - Don't just read; actually execute the examples
4. **Do the exercises** - Concepts stick when you struggle a bit
5. **Check solutions** - After attempting exercises, compare your approach
6. **Reflect** - Each lesson ends with "how does this relate to real plants?"

### **Each Lesson Contains:**

- **Prerequisites:** What you need to know first
- **Learning Objectives:** 3-5 specific skills you'll gain
- **Theory Recap:** Seborg concepts translated to discrete-time
- **Odibi Hands-On:** Minimal YAML + realistic YAML you can run
- **Data Engineering Focus:** The new DE concept for this lesson
- **Validation:** How to check your work (tests, quarantine)
- **Exercises:** 2-4 tasks to reinforce learning
- **Solutions:** Full answers + YAML files
- **Reflection:** Connection to real-world plant operations

### **Recommended Pace:**

- **Intensive:** 2-3 lessons per day → Done in 1 week
- **Steady:** 1 lesson per day → Done in 2-3 weeks  
- **Relaxed:** 2-3 lessons per week → Done in 6-8 weeks

**There is no deadline.** Go at your own pace.

---

## What You'll Learn

### **Process Control Skills:**

- First-order and second-order system dynamics
- Transfer function models (FOPTD, SOPTD)
- PID controller design and tuning (ZN, IMC methods)
- Anti-windup and constraint handling
- Cascade and feedforward control strategies
- System identification from test data (PRBS, ARX)
- Multivariable interactions (RGA basics)
- Gain scheduling for nonlinear processes

### **Data Engineering Skills:**

- Time series data modeling (timestamps, sampling, resampling)
- Data formats (CSV vs Parquet vs Delta Lake)
- Schema design and validation
- Data quality checks and quarantine patterns
- Partitioning strategies for time-series data
- Train/test splits for model validation
- Feature materialization and storage
- Pipeline dependency management
- Lineage tracking (where data came from)
- Incremental loading and idempotent runs

### **Odibi Framework Skills:**

- YAML pipeline configuration
- Generators: `random_walk`, `derived`, `range`, `constant`
- Stateful functions: `prev()`, `ema()`, `pid()`
- Validation tests and quality gates
- Quarantine routing for bad data
- Multi-entity simulations and joins
- SCD2 pattern for tracking equipment changes
- Delta Lake integration
- Medallion architecture (Bronze/Silver/Gold)

---

## Course Materials

### **Textbook Reference:**
[Seborg → Odibi Mapping Guide](../guides/seborg_textbook_mapping.md) - Complete chapter mapping

### **Technical References:**
- [Process Simulation Guide](../guides/process_simulation_guide.md) - Stateful functions deep dive
- [YAML Schema Reference](../../reference/yaml_schema.md) - All configuration options
- [Simulation Generators Reference](../../reference/simulation_generators.md) - Generator catalog

### **Example Code:**
All lesson YAML files: `/examples/cheme_course/L##_name/`

### **Solutions:**
Full exercise solutions: [Solutions Index](solutions/index.md)

---

## Teaching Philosophy

### **1. Learn by Doing**
You won't memorize theory. You'll build working simulations.

### **2. Familiar Problems, New Skills**
We use ChemE problems you already understand (tanks, reactors, PID loops) to teach data concepts.

### **3. Mistakes Are Learning**
Exercises are designed to make you struggle a bit. That's where learning happens.

### **4. Real-World Context**
Every lesson connects back to actual plant operations. No academic ivory tower.

### **5. Build Your Portfolio**
By the end, you have 15+ working examples to show in interviews or LinkedIn.

---

## What You'll Build

### **By Lesson 5:**
Realistic FOPTD tank temperature data with noise, delays, and validation

### **By Lesson 9:**
System identification pipeline that fits ARX models from PRBS tests

### **By Lesson 14:**
Multi-loop cascade control with feedforward and constraint handling

### **By Lesson 15 (Capstone):**
Production-grade CSTR "digital twin":
- Bronze: Raw sensor CSV ingestion
- Silver: Filtered, validated, quarantine routing
- Gold: Control outputs, KPIs, features
- Lineage: Track data provenance
- Schedule: Incremental runs
- (Optional) Cloud: Azure Blob + SQL sink

---

## Why This Matters

**Traditional ChemE education:**
- "Here's Laplace transforms and Bode plots"
- Often disconnected from plant reality
- Doesn't teach data engineering

**Traditional data engineering:**
- "Here's e-commerce clickstream data"
- Boring, generic, doesn't leverage your ChemE skills
- Doesn't teach process control

**This course:**
- ✅ Process control concepts you'll actually use
- ✅ Data engineering skills that are immediately practical
- ✅ Builds on knowledge you already have
- ✅ Creates unique portfolio content (ChemE + DE)

---

## Success Stories (What You'll Be Able to Do)

After completing this course:

### **At Work:**
- Simulate process behavior before building expensive pilots
- Generate realistic test data for analytics projects
- Validate control strategies with data pipelines
- Communicate with both process engineers AND data engineers

### **In Interviews:**
- Show working examples (not just theory)
- Demonstrate unique ChemE + data engineering combo
- Have portfolio projects that aren't generic

### **On LinkedIn:**
- Post content that stands out (no boring clickstream demos)
- Showcase process control knowledge + modern data skills
- Attract roles that value both domains

---

## Getting Started

### **Step 1: Installation**
Go to [L00: Setup](lessons/L00_setup.md) and install Odibi

### **Step 2: Run Your First Pipeline**
Complete the L00 exercises (15 minutes)

### **Step 3: Start Learning**
Move to [L01: CV/MV/DV](lessons/L01_cv_mv_dv.md)

---

## Need Help?

- **Questions?** Open an issue: [github.com/henryodibi11/Odibi/issues](https://github.com/henryodibi11/Odibi/issues)
- **Found a bug?** Submit a PR
- **Want to contribute?** Add your own lessons or examples

---

## Course Roadmap

**Current Status:** ✅ Curriculum designed, lessons in progress

**Coming Soon:**
- All 15 lessons with exercises
- Video walkthroughs (optional)
- Advanced Part IV: Industrialization (frequency response, RGA, full MPC)

---

## Let's Begin

**Ready to learn process control AND data engineering?**

👉 **[Start with L00: Setup](lessons/L00_setup.md)**

---

*This course is part of the Odibi Framework - making data engineering accessible to chemical engineers.*
