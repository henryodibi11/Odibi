# ChemE × Data Engineering Course - Status

**Last Updated:** March 12, 2026

---

## ✅ Course Structure Complete!

### **What's Built:**

**Course Foundation:**
- ✅ [Course Overview & Index](index.md) - Complete 15-lesson curriculum
- ✅ [Solutions Hub](solutions/index.md) - Solutions directory structure
- ✅ mkdocs navigation - Course appears under "Learn" section

**Completed Lessons (Part I - Foundations):**
- ✅ [L00: Setup](lessons/L00_setup.md) - Odibi basics, CSV/Parquet/Delta, seeds, reproducibility
- ✅ [L01: CV/MV/DV](lessons/L01_cv_mv_dv.md) - Control variables, time-series partitioning
- ✅ [L02: DoF + Balances](lessons/L02_dof_balances.md) - Schema design, units, validation, quarantine
- ✅ [L03: First-Order Dynamics](lessons/L03_first_order.md) - `prev()`, `ema()`, sampling, KPIs

**Lessons To Be Built (L04-L15):**
The remaining 11 lessons follow the same template and structure. Here's the detailed outline:

---

## Lesson Outline (L04-L15)

### **Part I: Foundations (continued)**

#### L04: FOPTD Transfer Functions (Seborg Ch. 4)
- **Theory:** K, τ, θ parameters; discrete-time approximation
- **Odibi:** Parameterized YAML, time delay with `prev(..., lag=N)`
- **Data Focus:** Config reuse, scenario flags
- **Exercises:** Vary K/τ/θ, validate 63.2% time, step response curves

#### L05: Second-Order + Overshoot (Seborg Ch. 5)
- **Theory:** ζ, ωn, damping ratios; overshoot formula
- **Odibi:** State-space with two `prev()` calls (position, velocity)
- **Data Focus:** Computed KPIs (overshoot %, peak time), metrics tables
- **Exercises:** Underdamped vs overdamped, measure Mp from data

---

### **Part II: Feedback Control + Identification**

#### L06: PID Basics + Constraints (Seborg Ch. 8)
- **Theory:** P/I/D actions, anti-windup, output limits
- **Odibi:** `pid()` transformer with `output_min/max`
- **Data Focus:** Delta Lake write options
- **Exercises:** P-only offset, PI tuning, derivative noise

#### L07: Tuning + Anti-windup (Seborg Ch. 12)
- **Theory:** Ziegler-Nichols, IMC/Direct Synthesis
- **Odibi:** Quality gates for tuning validation
- **Data Focus:** Regression tests for control performance
- **Exercises:** ZN vs IMC comparison, IAE/ISE metrics

#### L08: Disturbances + Setpoint Strategies (Seborg Ch. 8-9)
- **Theory:** Load rejection vs setpoint tracking, 2-DoF PID
- **Odibi:** Scenario branching with metadata
- **Data Focus:** Experiment tracking (run_id, scenario tags)
- **Exercises:** Setpoint weighting (β), disturbance tests

#### L09: System Identification (PRBS/ARX) (Seborg Ch. 7)
- **Theory:** PRBS signals, ARX models, train/test splits
- **Odibi:** Random choice generator for PRBS
- **Data Focus:** Feature materialization, model metrics storage
- **Exercises:** Fit ARX from Odibi data, cross-validation, model order selection

---

### **Part III: Multiloop + Advanced Topics**

#### L10: Interacting Loops (Seborg Ch. 18-19)
- **Theory:** Multi-input/multi-output (MIMO), interaction
- **Odibi:** Multi-entity pipelines, joins, SCD2 equipment dimension
- **Data Focus:** Entity relationships, foreign keys
- **Exercises:** Level-flow interaction, RGA basics

#### L11: Cascade Control (Seborg Ch. 16)
- **Theory:** Inner/outer loops, disturbance rejection
- **Odibi:** Dependency ordering, staged columns
- **Data Focus:** Pipeline caching, idempotent runs
- **Exercises:** Reactor temp cascade, performance comparison

#### L12: Feedforward/Ratio Control (Seborg Ch. 15)
- **Theory:** FF+FB combination, ratio control
- **Odibi:** Combining control signals, threshold validators
- **Data Focus:** Quarantine routing on constraint violations
- **Exercises:** Distillation ratio, FF effectiveness

#### L13: Nonlinearity + Saturation (Seborg Ch. 6)
- **Theory:** Gain scheduling, valve saturation
- **Odibi:** Conditional logic in `derived`, guards
- **Data Focus:** Unit tests for YAML expressions
- **Exercises:** Nonlinear pH control, actuator limits

#### L14: MPC-lite (IMC-inspired) (Seborg Ch. 20 intro)
- **Theory:** Horizon planning, move suppression
- **Odibi:** Rolling windows, constraint handling
- **Data Focus:** Feature stores, metrics tracking
- **Exercises:** Constrained level control, move penalties

---

### **Capstone**

#### L15: CSTR Digital Twin Pipeline
- **Theory:** End-to-end medallion architecture
- **Odibi:** Bronze/Silver/Gold layers, lineage, validation
- **Data Focus:** OpenLineage, Delta Lake, optional cloud (Azure)
- **Deliverable:** Production-grade CSTR pipeline with:
  - Bronze: Raw sensor CSV ingestion
  - Silver: Filtered, validated, SCD2 equipment tracking
  - Gold: Control outputs, KPIs, aggregations
  - Lineage: Full data provenance
  - Schedule: Incremental runs
  - (Optional) Cloud sink: Azure Blob + SQL

---

## How to Continue Building

### **Option 1: Use What You Have (Start Learning Now)**

The first 4 lessons (L00-L03) cover:
- Odibi setup and basics
- Process variables and time series
- Mass/energy balances and DoF
- First-order dynamics and state

**This is enough to:**
- Generate realistic plant data
- Understand time-series data engineering
- Build simple simulations
- Practice validation and quarantine

**Recommended:** Work through L00-L03 exercises, then decide which advanced topics you need most.

---

### **Option 2: Build Specific Lessons On-Demand**

When you need a specific topic:
1. Reference the outline above
2. Ask me to build that lesson (e.g., "Build L06: PID Basics")
3. I'll create it following the same template as L00-L03

**Benefits:**
- Focus on what you need now
- Lessons are fresh and can incorporate your feedback
- Less upfront time investment

---

### **Option 3: Build All Lessons (Complete Course)**

I can build L04-L15 right now as a batch.

**Time estimate:**
- Each lesson: ~15-20 minutes to create
- Total: ~3 hours for all remaining lessons
- Result: Complete, self-contained course

**Trade-off:**
- You get everything at once
- But you haven't tested L00-L03 yet to give feedback
- Might want to iterate on format before building all

---

## Recommended Path Forward

**My suggestion:**

1. **Try L00-L03 yourself** (2-3 hours)
   - Install Odibi
   - Run the YAML examples
   - Do the exercises
   - See what works/doesn't work for your learning style

2. **Give feedback**
   - Too much detail? Not enough?
   - Exercises too hard/easy?
   - Data engineering concepts clear?
   - More visuals needed?

3. **Then build remaining lessons**
   - I'll incorporate your feedback
   - Build L04-L09 first (control fundamentals)
   - L10-L15 can wait until you need advanced topics

---

## What You Can Do Right Now

### **Start the Course:**

1. **Install Odibi:**
   ```bash
   pip install odibi
   ```

2. **Go to L00:**
   Read [docs/learning/cheme_data_course/lessons/L00_setup.md](lessons/L00_setup.md)

3. **Follow along:**
   - Create the YAML files from examples
   - Run `odibi run tank_data.yaml`
   - Inspect outputs
   - Do exercises

4. **Progress through lessons:**
   - L00 → L01 → L02 → L03
   - Each builds on previous

### **Test Your Knowledge:**

After L03, you should be able to:
- Generate time-series process data with Odibi
- Understand CSV vs Parquet vs Delta
- Map CV/MV/DV to schemas
- Count DoF and avoid circular dependencies
- Implement mass/energy balances
- Add validation and quarantine
- Model first-order dynamics

**If yes:** You're ready for PID control (L06)
**If no:** Review the lessons or ask questions

---

## Course Materials Location

```
docs/learning/cheme_data_course/
├── index.md                  # Course overview (START HERE)
├── COURSE_STATUS.md          # This file
├── lessons/
│   ├── L00_setup.md          # ✅ Complete
│   ├── L01_cv_mv_dv.md       # ✅ Complete
│   ├── L02_dof_balances.md   # ✅ Complete
│   ├── L03_first_order.md    # ✅ Complete
│   └── L04_foptd.md          # 📝 To be built (on-demand or batch)
└── solutions/
    └── index.md              # Solutions hub
```

---

## FAQ

### Q: Can I skip lessons?

**A:** Not recommended for Part I (L00-L05). These are foundations.

For Part II/III, you can skip if:
- L09 (System ID): Skip if you don't care about ARX/identification
- L10 (Interactions): Skip if only doing SISO (single loops)
- L14 (MPC-lite): Skip if PID is enough

### Q: Do I need the Seborg textbook?

**A:** No! Lessons recap the key concepts. But if you have it, it provides deeper theory.

### Q: What if I find errors or have suggestions?

**A:** Open an issue on GitHub or ask me directly. This course is iterative!

### Q: Can I use this to teach others?

**A:** Absolutely! That's the point. Share it, improve it, contribute back.

---

## Next Steps Decision Tree

```
Do you have Odibi installed?
├─ No → Go to L00: Setup
└─ Yes
    ├─ Have you done L00-L03?
    │   ├─ No → Start with L00
    │   └─ Yes → Ready for Part II (L06: PID)
    └─ Want all lessons built now?
        ├─ Yes → Ask me to build L04-L15
        └─ No → Tell me which lessons you need next
```

---

## Bottom Line

**You have a complete, working curriculum structure with 4 solid foundational lessons.**

**Next decision:**
- **Start learning** (recommended), or
- **Build remaining lessons** (I can do this now)

What would you like to do?

---

*Course created by: Odibi Framework*  
*For: Chemical Engineers learning Data Engineering*  
*Status: Foundation complete (L00-L03), Advanced lessons on-demand*
