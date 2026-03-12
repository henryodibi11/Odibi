# Course Lessons - Complete Status

**Last Updated:** March 12, 2026

---

## ✅ Lessons Built (L00-L05)

### **Part I: Foundations - COMPLETE**

All foundation lessons are built and ready to use:

1. ✅ **[L00: Setup](lessons/L00_setup.md)** (45 min)
   - Odibi installation
   - CSV vs Parquet vs Delta Lake
   - Seeds and reproducibility
   - Multi-entity simulations

2. ✅ **[L01: CV/MV/DV](lessons/L01_cv_mv_dv.md)** (45 min)
   - Control variable mapping
   - Time-series schemas
   - Partitioning strategies
   - Validation tests

3. ✅ **[L02: DoF + Balances](lessons/L02_dof_balances.md)** (60 min)
   - Degrees of freedom analysis
   - Mass/energy balances
   - Unit tracking
   - Quarantine pattern

4. ✅ **[L03: First-Order Dynamics](lessons/L03_first_order.md)** (45 min)
   - `prev()` and `ema()` functions
   - Sampling theory
   - Rise/settling time KPIs
   - Resampling data

5. ✅ **[L04: FOPTD Transfer Functions](lessons/L04_foptd.md)** (45 min)
   - K, τ, θ parameters
   - Time delay implementation
   - Parameterized YAML
   - Step response validation

6. ✅ **[L05: Second-Order + Overshoot](lessons/L05_second_order.md)** (60 min)
   - Damping ratio (ζ) and natural frequency (ωn)
   - State-space implementation
   - Overshoot calculation
   - Metrics tables

---

## 📝 Lessons L06-L15: Detailed Outlines

The remaining lessons follow the same template and quality as L00-L05. Each outline below is **complete and ready to be built** following the established pattern.

---

### **Part II: Feedback Control + Identification**

#### **L06: PID Basics + Constraints** (60 min)
**Seborg:** Chapter 8

**Learning Objectives:**
- Implement PID control using `pid()` transformer
- Understand P, I, D actions independently
- Configure output limits and anti-windup
- Write to Delta Lake format
- Validate control performance (IAE, ISE metrics)

**Key Content:**
- P-only (offset remains)
- PI (offset eliminated, possible overshoot)
- PID (faster, derivative fights noise)
- Output clamping (`output_min`, `output_max`)
- Anti-windup (built into Odibi `pid()`)

**YAML Examples:**
1. Tank level control with P-only vs PI vs PID
2. Temperature control with output limits
3. Delta Lake output for time-travel

**Exercises:**
- E1: Tune P-only, observe steady-state offset
- E2: Add integral action, eliminate offset
- E3: Add derivative, compare noise sensitivity
- E4: Demonstrate windup without anti-windup protection

---

#### **L07: Tuning + Anti-windup** (60 min)
**Seborg:** Chapter 12

**Learning Objectives:**
- Apply Ziegler-Nichols tuning rules
- Use IMC/Direct Synthesis methods
- Implement validation gates for tuning
- Compare tuning methods (IAE, ISE, overshoot)
- Create regression tests for controller performance

**Key Content:**
- ZN ultimate gain method
- IMC tuning (λ = τ for aggressive, λ = 3τ for conservative)
- 2-degree-of-freedom PID (setpoint weighting)
- Quality gates (reject bad tunings)
- Tuning comparison metrics

**YAML Examples:**
1. FOPTD process with ZN tuning
2. IMC tuning vs ZN comparison
3. Validation gates for IAE thresholds

**Exercises:**
- E1: Find ultimate gain (Ku) from oscillation test
- E2: Apply ZN rules, measure performance
- E3: Compare ZN vs IMC on disturbance rejection
- E4: Implement setpoint weighting (β parameter)

---

#### **L08: Disturbances + Setpoint Strategies** (60 min)
**Seborg:** Chapters 8-9

**Learning Objectives:**
- Distinguish load rejection vs setpoint tracking
- Implement scenario branching with metadata
- Use 2-DoF PID for aggressive tracking
- Track experiment runs with metadata columns
- Compare control strategies with metrics

**Key Content:**
- Disturbance types (step, ramp, sustained)
- Setpoint tracking vs regulation modes
- Setpoint weighting (β) for overshoot reduction
- Scenario flags and branching
- Run metadata (run_id, scenario, timestamp)

**YAML Examples:**
1. Load disturbance test (feed composition change)
2. Setpoint tracking test (temperature ramp)
3. Scenario matrix (4 scenarios, metadata tracking)

**Exercises:**
- E1: Test controller on step disturbance
- E2: Compare β = 0 vs β = 1 for setpoint overshoot
- E3: Run scenario matrix (4 tuning sets × 2 disturbances)
- E4: Generate comparison dashboard data

---

#### **L09: System Identification (PRBS/ARX)** (90 min)
**Seborg:** Chapter 7

**Learning Objectives:**
- Generate PRBS (Pseudo-Random Binary Sequence) signals
- Fit ARX models from time-series data
- Implement train/test splits
- Store model parameters and metrics
- Cross-validate identified models

**Key Content:**
- PRBS for persistent excitation
- ARX model structure: y[k] = Σa_i·y[k-i] + Σb_j·u[k-j]
- Least-squares parameter estimation
- Train/test validation
- One-step-ahead vs simulation predictions

**YAML Examples:**
1. PRBS input generation
2. FOPTD process data for identification
3. Train/test split partitions

**Python Integration:**
```python
# Fit ARX model
from statsmodels.tsa.arima.model import ARIMA
model = ARIMA(y_train, exog=u_train, order=(2,0,1))
fitted = model.fit()

# Validate
y_pred = model.predict(start=0, end=len(y_test), exog=u_test)
rmse = np.sqrt(np.mean((y_test - y_pred)**2))
```

**Exercises:**
- E1: Generate PRBS with varying switch times
- E2: Fit ARX(1,1) and ARX(2,2), compare AIC/BIC
- E3: Cross-validate on hold-out test set
- E4: Recover FOPTD parameters from ARX coefficients

---

### **Part III: Multiloop + Advanced Topics**

#### **L10: Interacting Loops** (60 min)
**Seborg:** Chapters 18-19

**Learning Objectives:**
- Model multi-input/multi-output (MIMO) systems
- Implement multi-entity pipelines with joins
- Understand loop interaction (RGA basics)
- Track equipment history with SCD2 pattern
- Validate cross-entity dependencies

**Key Content:**
- 2×2 system (level/flow, temp/pressure)
- Relative Gain Array (RGA) concept
- Pairing strategies (1-1/2-2 vs 1-2/2-1)
- Multi-entity joins on timestamp
- SCD2 for equipment tracking

**YAML Examples:**
1. Two-tank system (interacting levels)
2. Multi-entity simulation with joins
3. SCD2 dimension for equipment changes

**Exercises:**
- E1: Simulate interacting loops, observe coupling
- E2: Calculate RGA from steady-state gains
- E3: Test both pairings, compare performance
- E4: Implement SCD2 for valve replacements

---

#### **L11: Cascade Control** (60 min)
**Seborg:** Chapter 16

**Learning Objectives:**
- Design cascade control (inner + outer loops)
- Implement staged column dependencies
- Show disturbance rejection improvement
- Manage pipeline execution order
- Compare cascade vs single-loop performance

**Key Content:**
- Primary (slow) vs secondary (fast) loops
- Outer loop → setpoint for inner loop
- Disturbance rejection advantage
- Dependency ordering in YAML
- Tuning: inner loop first, then outer

**YAML Examples:**
1. Reactor temperature cascade (jacket temp inner loop)
2. Level/flow cascade
3. Performance comparison (cascade vs single-loop)

**Exercises:**
- E1: Build cascade for reactor temp control
- E2: Inject disturbance at inner loop, measure rejection
- E3: Compare IAE: cascade vs single-loop
- E4: Detune inner loop, observe degradation

---

#### **L12: Feedforward/Ratio Control** (60 min)
**Seborg:** Chapter 15

**Learning Objectives:**
- Implement feedforward + feedback combination
- Design ratio control schemes
- Validate constraint satisfaction
- Route violations to quarantine
- Measure feedforward effectiveness

**Key Content:**
- Feedforward: anticipates disturbances
- Ratio control: maintain fixed ratio of flows
- FF+FB combination (FF handles measured DV, FB handles mismatch)
- Ideal feedforward: Gff(s) = -Gd(s)/Gp(s)
- Practical FF: static gain + lead/lag

**YAML Examples:**
1. Ratio control (primary/secondary flows)
2. Feedforward + feedback (distillation)
3. Constraint validation (ratio ± 5% tolerance)

**Exercises:**
- E1: Implement 3:1 ratio control
- E2: Add feedforward for feed flow disturbance
- E3: Compare FB-only vs FF+FB on IAE
- E4: Quarantine rows where ratio violates ±10% tolerance

---

#### **L13: Nonlinearity + Saturation** (60 min)
**Seborg:** Chapter 6

**Learning Objectives:**
- Model nonlinear processes (pH, exothermic reactions)
- Implement gain scheduling
- Handle actuator saturation
- Add conditional logic in derived columns
- Unit test YAML expressions

**Key Content:**
- Nonlinear processes (pH curve, valve characteristics)
- Gain scheduling (different gains at different operating points)
- Actuator limits (valve 0-100%, pump min/max speed)
- Conditional expressions in Odibi
- Testing: run multiple operating points

**YAML Examples:**
1. pH control with nonlinear titration curve
2. Gain-scheduled temperature control
3. Valve saturation and split-range control

**Exercises:**
- E1: Model pH neutralization, observe nonlinearity
- E2: Implement gain scheduling (2 operating regions)
- E3: Saturate actuator, observe integrator windup
- E4: Test 3 operating points, validate gain changes

---

#### **L14: MPC-lite (IMC-inspired)** (90 min)
**Seborg:** Chapter 20 (intro)

**Learning Objectives:**
- Understand model predictive control concepts
- Implement constraint handling explicitly
- Add move suppression (limit Δu)
- Use rolling windows for prediction
- Track MV/CV in metrics tables

**Key Content:**
- MPC basics: predict future, optimize over horizon
- Constraints: u_min ≤ u ≤ u_max, y_min ≤ y ≤ y_max
- Move suppression: penalize large Δu
- Simplified MPC: PID + rate limits + constraint projection
- Feature stores for predictions

**YAML Examples:**
1. Constrained level control (MPC-like)
2. Move suppression (max Δu per timestep)
3. Rolling horizon tracking

**Exercises:**
- E1: Implement PID with move limits (Δu ≤ 5 per step)
- E2: Add CV constraints (level ∈ [8, 12] ft)
- E3: Compare MPC-lite vs standard PID
- E4: Measure constraint violations (should be zero)

---

### **Capstone**

#### **L15: CSTR Digital Twin Pipeline** (2-3 hours)
**Seborg:** Chapters 2, 8, 12 (integration)

**Learning Objectives:**
- Build end-to-end medallion pipeline (Bronze/Silver/Gold)
- Implement full validation and quarantine
- Track lineage with OpenLineage
- Use Delta Lake for ACID transactions
- (Optional) Connect to Azure cloud

**Project Deliverable:**

**Bronze Layer:**
- Ingest raw sensor CSV (temperature, flow, composition)
- Minimal transformation (type conversion)
- Write to Parquet partitioned by date

**Silver Layer:**
- Filter outliers (3-sigma rule)
- Validate ranges (temp ∈ [50, 250]°F)
- Quarantine bad data
- Add SCD2 equipment tracking
- Write to Delta Lake

**Gold Layer:**
- Calculate control outputs (PID)
- Compute KPIs (conversion, selectivity, energy efficiency)
- Aggregate hourly metrics
- Materialized features for dashboards

**Lineage:**
- OpenLineage metadata
- Track data provenance (which bronze files → silver tables)

**Schedule:**
- Incremental loading (watermark on timestamp)
- Idempotent runs (same input → same output)

**Optional Cloud:**
- Write to Azure Blob Storage
- Sync metrics to Azure SQL Database

**YAML Structure:**
```yaml
pipeline:
  name: cstr_digital_twin

connections:
  bronze_in:
    type: local
    path: ./data/raw/sensors/*.csv

  silver_out:
    type: delta
    path: ./data/silver/cstr_validated

  gold_out:
    type: delta
    path: ./data/gold/cstr_kpis

nodes:
  - name: bronze_ingest
    # ... raw ingestion

  - name: silver_validation
    # ... validation + quarantine

  - name: gold_features
    # ... KPIs + control outputs
```

**Exercises:**
- E1: Run full pipeline end-to-end
- E2: Inject bad data, verify quarantine works
- E3: Query Delta Lake time-travel (previous versions)
- E4: Add custom KPI (space-time yield)
- E5: (Optional) Deploy to Databricks

---

## How to Get Remaining Lessons

**All L06-L15 outlines are complete.** I can build any of them following the same template as L00-L05.

**Options:**

### **Option 1: On-Demand (Recommended)**
Tell me which lesson you want next:
- "Build L06 (PID Basics)" → I create it
- Work through it
- "Build L07 (Tuning)" → I create next one
- Iterate based on feedback

**Benefits:**
- Lessons incorporate your feedback
- Focus on what you need now
- Less overwhelming

---

### **Option 2: Batch Build**
"Build L06-L15 all at once" → I create all 10 remaining lessons

**Benefits:**
- Complete course immediately
- Can work through at your own pace

**Trade-off:**
- No iteration based on your L00-L05 experience
- Large upfront time investment (~2-3 hours to build all)

---

### **Option 3: Build Specific Topics**
"I need PID (L06), System ID (L09), and Capstone (L15)" → I build just those 3

**Benefits:**
- Skip topics you don't need
- Faster to complete core path

---

## Recommendation

**Start with L00-L05** (you have these complete):
1. Work through them (3-4 hours)
2. Do the exercises
3. See what questions you have

**Then:**
- Tell me which advanced topic you need (PID? Cascade? MPC?)
- I'll build that lesson with your feedback in mind
- Build remaining lessons as you need them

This way, the course evolves based on YOUR learning style and needs.

---

## Current Status Summary

**✅ Complete & Ready:** L00-L05 (Part I: Foundations)  
**📝 Outlined & Ready to Build:** L06-L15 (Parts II-III + Capstone)  
**📖 Reference Docs:** Seborg mapping, process simulation guide, YAML schema

**You have everything you need to start learning right now!**

---

*Ready to start? Go to [L00: Setup](lessons/L00_setup.md)*
