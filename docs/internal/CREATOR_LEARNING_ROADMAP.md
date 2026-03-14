# Creator's Learning Roadmap: Fully Map Odibi

**Goal:** Go through every major capability of your framework, in order.  
**Time:** ~6 hours (split across multiple sessions)  
**Rule:** Follow the docs EXACTLY. Don't use prior knowledge.

---

## Phase 1: Foundation (1 hour)

### ✅ Step 1: Golden Path (10 min)
**Read:** `docs/golden_path.md`  
**Do:** Run every command as written  
**Verify:** Does the Story open? Are outputs where expected?  
**Discovery:** Is `odibi init` documented correctly?

### ✅ Step 2: Your First Project (30 min)
**Read:** `docs/tutorials/getting_started.md`  
**Do:** Create messy CSV, run pipeline, view Story  
**Verify:** Does validation catch the bad data?  
**Discovery:** Does the `init-pipeline` command match reality?

### ✅ Step 3: Installation Extras (10 min)
**Read:** `docs/guides/installation.md`  
**Do:** Run `odibi --version`, `odibi list transformers`, `odibi list patterns`  
**Note:** Count your transformers. Write the number down: _____  
**Discovery:** How many did you expect vs actual?

### ✅ Step 4: CLI Discovery (10 min)
**Run each command:**
```bash
odibi list transformers
odibi list patterns
odibi list connections
odibi templates list
odibi explain scd2
odibi explain detect_deletes
odibi explain sessionize
```
**Note:** Which 3 surprised you?
1. _____
2. _____
3. _____

---

## Phase 2: Medallion Architecture (1.5 hours)

### ✅ Step 5: Bronze Layer (30 min)
**Read:** `docs/tutorials/bronze_layer.md`  
**Do:** Build a bronze pipeline from scratch (not from template)  
**Verify:** `python examples/verify/verify_01_hello_world.py`  
**Discovery:** Does append-only actually work as documented?

### ✅ Step 6: Silver Layer (30 min)
**Read:** `docs/tutorials/silver_layer.md`  
**Do:** Add deduplication and cleaning to your bronze output  
**Verify:** Are duplicates removed? Are types correct?  
**Discovery:** Does the merge pattern work as documented?

### ✅ Step 7: Gold Layer (30 min)
**Read:** `docs/tutorials/gold_layer.md`  
**Do:** Build a fact table from your silver data  
**Verify:** `python examples/verify/verify_04_fact_table.py`  
**Discovery:** Do SK lookups work? Does orphan handling trigger?

---

## Phase 3: Core Patterns (1.5 hours)

### ✅ Step 8: Incremental Loading (20 min)
**Read:** `docs/patterns/incremental_stateful.md`  
**Visual:** `docs/visuals/incremental_decision_tree.md`  
**Do:** Run a pipeline twice, check state catalog  
**Verify:** `python examples/verify/verify_02_incremental_sql.py`  
```bash
odibi catalog state odibi.yaml
```
**Discovery:** Does HWM persist? Does second run skip old data?

### ✅ Step 9: SCD2 Dimensions (30 min)
**Read:** `docs/patterns/scd2.md` then `docs/patterns/dimension.md`  
**Visual:** `docs/visuals/scd2_timeline.md`  
**Do:** Build dim_customer, change a row, re-run  
**Verify:** `python examples/verify/verify_03_scd2_dimension.py`  
**Discovery:** Does version history appear? Is unknown member created?

### ✅ Step 10: Merge/Upsert (15 min)
**Read:** `docs/patterns/merge_upsert.md`  
**Do:** Upsert changed records into silver  
**Discovery:** Does it handle new + changed + unchanged correctly?

### ✅ Step 11: Fact Tables (15 min)
**Read:** `docs/patterns/fact.md`  
**Visual:** `docs/visuals/fact_build_flow.md`  
**Do:** Build fact with 2+ dimension lookups  
**Discovery:** Does grain validation catch duplicates?

### ✅ Step 12: Aggregation (10 min)
**Read:** `docs/patterns/aggregation.md`  
**Do:** Build a daily rollup from your fact table  
**Discovery:** Does incremental aggregation work?

---

## Phase 4: Data Quality (45 min)

### ✅ Step 13: Contracts (15 min)
**Read:** `docs/validation/contracts.md`  
**Do:** Add contracts to a node, break the source data  
**Discovery:** Does it fail fast BEFORE processing?

### ✅ Step 14: Validation Tests (15 min)
**Read:** `docs/validation/tests.md`  
**Do:** Add not_null, unique, row_count, range tests  
**Discovery:** Do all 11 test types work?

### ✅ Step 15: Quality Gates + Quarantine (15 min)
**Read:** `docs/features/quality_gates.md` then `docs/features/quarantine.md`  
**Do:** Set `on_failure: quarantine`, add bad data  
**Discovery:** Do bad rows route to quarantine path?

---

## Phase 5: Transformers Deep Dive (1 hour)

### ✅ Step 16: SQL Core Transformers (15 min)
**Do:** Try each in a pipeline:
```yaml
transform:
  steps:
    - transformer: filter_rows
      params: {condition: "amount > 0"}
    - transformer: derive_columns
      params: {derivations: {total: "price * quantity"}}
    - transformer: cast_columns
      params: {casts: {amount: float}}
```
**Discovery:** Which ones do you use daily vs never?

### ✅ Step 17: Advanced Transformers (15 min)
**Run:** `odibi explain` on each:
```bash
odibi explain deduplicate
odibi explain window_calculation
odibi explain sessionize
odibi explain split_events_by_period
odibi explain hash_columns
odibi explain generate_surrogate_key
```
**Do:** Try `sessionize` on timestamped data  
**Discovery:** Did you know `split_events_by_period` exists?

### ✅ Step 18: Delete Detection (15 min)
**Read:** `odibi explain detect_deletes`  
**Do:** Load data, delete a row from source, re-run  
**Discovery:** Does it detect the deletion? Soft delete or hard?

### ✅ Step 19: Cross-Check Validation (15 min)
**Read:** `odibi explain cross_check`  
**Do:** Compare row counts between two datasets  
**Discovery:** Can you use this to validate pipeline outputs?

---

## Phase 6: Observability & Operations (30 min)

### ✅ Step 20: Data Stories (10 min)
**Read:** `docs/guides/how_to_read_a_story.md`  
**Do:** Generate a Story with explanation sections  
```yaml
explanation: |
  ## Business Logic
  This node does X because Y.
```
**Discovery:** Does the explanation render in the Story HTML?

### ✅ Step 21: System Catalog (10 min)
**Read:** `docs/features/catalog.md`  
**Do:** Check state, lineage, metrics  
```bash
odibi catalog state odibi.yaml
```
**Discovery:** What does "The Brain" actually track?

### ✅ Step 22: Alerting (10 min)
**Read:** `docs/features/alerting.md`  
**Do:** Configure a test alert (webhook or console)  
**Discovery:** Do alerts fire on success AND failure?

---

## Phase 7: Hidden Gems (30 min)

### ✅ Step 23: Simulation / Synthetic Data (10 min)
**Read:** `docs/guides/simulation.md`  
**Do:** Generate synthetic data using the simulation engine  
**Discovery:** Does it support incremental simulation with HWM?

### ✅ Step 24: Variable Substitution (5 min)
**Read:** `docs/guides/variable_substitution.md`  
**Do:** Use `${date:today}`, `${env:VAR}`, `${vars.name}`  
**Discovery:** How many substitution types exist?

### ✅ Step 25: Explanation Feature (5 min)
**Read:** `docs/guides/explanation_feature.md`  
**Do:** Add inline + file-based explanations  
**Discovery:** Do explanations render in Stories with full Markdown?

### ✅ Step 26: API Data Sources (10 min)
**Read:** `docs/guides/api_data_sources.md`  
**Do:** Fetch from a free public API  
**Discovery:** Does pagination work? All 4 types?

---

## Phase 8: Production Readiness (30 min)

### ✅ Step 27: Environments + Secrets (10 min)
**Read:** `docs/guides/environments.md` + `docs/guides/secrets.md`  
**Do:** Create `.env`, reference `${DB_PASSWORD}` in YAML  
**Discovery:** Are secrets redacted in logs and Stories?

### ✅ Step 28: Performance Tuning (10 min)
**Read:** `docs/guides/performance_tuning.md`  
**Do:** Enable `use_arrow: true` and compare speed  
**Discovery:** How much faster is Arrow backend?

### ✅ Step 29: Production Deployment (10 min)
**Read:** `docs/guides/production_deployment.md`  
**Do:** Plan a Databricks deployment using the guide  
**Discovery:** Is the Airflow integration documented correctly?

---

## Phase 9: Final Validation (15 min)

### ✅ Step 30: Run Full Canonical Pipeline
**Do:** Run all 5 canonical examples in order:
```bash
cd docs/examples/canonical/runnable
odibi run 01_hello_world.yaml
odibi run 02_incremental_sql.yaml
odibi run 03_scd2_dimension.yaml
odibi run 04_fact_table.yaml
odibi run 05_full_pipeline.yaml
```

### ✅ Step 31: Run All Verify Scripts
```bash
python examples/verify/verify_01_hello_world.py
python examples/verify/verify_02_incremental_sql.py
python examples/verify/verify_03_scd2_dimension.py
python examples/verify/verify_04_fact_table.py
```

### ✅ Step 32: Run the Capstone Verifiers
```bash
python examples/verify/verify_capstone_jr_de.py
python examples/verify/verify_capstone_sr_de.py
```

---

## 📋 Final Scorecard

After completing all 32 steps, fill this out:

### Features That Work as Documented
- [ ] Golden Path
- [ ] Bronze/Silver/Gold layers
- [ ] SCD2 dimensions
- [ ] Fact tables with SK lookups
- [ ] Incremental loading (stateful)
- [ ] Validation + contracts
- [ ] Quality gates + quarantine
- [ ] Data Stories + explanations
- [ ] System Catalog state
- [ ] Alerting
- [ ] Simulation
- [ ] Variable substitution
- [ ] API data sources

### Features I Didn't Know I Had
1. _____
2. _____
3. _____
4. _____
5. _____

### Docs That Are Wrong
1. _____
2. _____
3. _____

### Things I Need to Fix
1. _____
2. _____
3. _____

### Things I Need to Add
1. _____
2. _____
3. _____

---

## ⏱️ Time Summary

| Phase | Topic | Time |
|-------|-------|------|
| 1 | Foundation | 1h |
| 2 | Medallion Architecture | 1.5h |
| 3 | Core Patterns | 1.5h |
| 4 | Data Quality | 45m |
| 5 | Transformers Deep Dive | 1h |
| 6 | Observability & Operations | 30m |
| 7 | Hidden Gems | 30m |
| 8 | Production Readiness | 30m |
| 9 | Final Validation | 15m |
| **TOTAL** | **Complete Framework Map** | **~7 hours** |

**Recommended:** Split across 3-4 sessions.

- **Session 1:** Phases 1-2 (2.5 hours)
- **Session 2:** Phases 3-4 (2.25 hours)
- **Session 3:** Phases 5-7 (2 hours)
- **Session 4:** Phases 8-9 (45 min)

---

## 🎯 After This Roadmap

You will:
- Know every feature in your framework
- Know which docs are wrong
- Know which features are broken
- Know what's missing
- Have a prioritized fix list
- Truly understand what you built

**Then we fix everything you found.**
