# Chemical Engineer / Domain SME Learning Journey

---

## 📌 Who Is This For?

**Process engineers, plant engineers, domain experts, and technical SMEs** who need to:
- Frame sensor and batch data problems in data engineering terms
- Select appropriate patterns for time-series and batch processes
- Validate business rules and quality thresholds
- Collaborate effectively with data engineering teams
- Understand Data Stories and pipeline reports

**No prior data engineering or coding experience required.**

---

## ⏱️ Time to Complete

**2-3 hours** (can be broken into 30-45 minute sessions)

---

## 🎯 Learning Outcomes

By the end of this journey, you will be able to:

✅ Translate process engineering problems into data pipeline requirements  
✅ Choose patterns for continuous vs batch processes  
✅ Understand incremental loading for sensor/event data  
✅ Set up validation rules for process thresholds and anomalies  
✅ Read Data Stories and identify data quality issues  
✅ Document business logic in YAML using explanation sections  
✅ Communicate effectively with data engineers about pipeline needs  

---

## 📋 Prerequisites

- **Domain expertise** in your field (chemical, mechanical, process engineering)
- **Basic spreadsheet** knowledge (CSV, Excel)
- **Curiosity** about how data flows from sensors/SCADA to analytics

---

## 📚 Learning Modules

### Module 1: From Process Engineering to Data Engineering (30 min)

#### 📖 Read
- [Philosophy](../philosophy.md) - Focus on "The Five Laws"
- [Medallion Architecture](../guides/medallion_architecture.md)

#### 👀 Visual - Process Data Flow

```
Plant Floor              Bronze (Raw)         Silver (Clean)        Gold (Analytics)
-----------              ------------         --------------        ----------------
Sensors/SCADA     →      Immutable          →  Deduplicated    →    KPIs/Dashboards
Batch Systems            Time-stamped          Validated             Aggregated
ERP/MES                  Append-only           Contextualized        Star Schema
```

#### ✋ Think About Your Domain

Map your data sources to Bronze/Silver/Gold:

| My Data Source | Layer | Why? |
|----------------|-------|------|
| SCADA tags (1-min) | Bronze | Raw sensor data, append-only |
| Batch recipe records | Bronze | Immutable historical records |
| Quality lab results | Bronze | As-received from LIMS |
| Cleaned sensor data | Silver | Outliers removed, gaps filled |
| Production batches | Silver | Enriched with equipment/material context |
| Daily OEE metrics | Gold | Aggregated KPIs for dashboards |

**Key Insight:**
- **Bronze** = Truth (never modify)
- **Silver** = Context (add meaning)
- **Gold** = Insights (answer questions)

#### ✅ Verify
- [ ] You can categorize your data sources by layer
- [ ] You understand "Raw is Sacred" (Law #2)
- [ ] You know Bronze data is append-only and immutable

---

### Module 2: Continuous vs Batch Data Patterns (45 min)

#### 📖 Read
- [Incremental Stateful Pattern](../patterns/incremental_stateful.md)
- [Windowed Reprocess Pattern](../patterns/windowed_reprocess.md)
- [Decision Guide](../guides/decision_guide.md#choose-your-incremental-mode)

#### 👀 Visual - Process Data Types

**Continuous Process (Sensors/Tags):**
```
Time:   00:00  00:01  00:02  00:03  00:04  ...
Temp:   145.2  145.8  146.1  145.9  146.3  ...
Flow:   32.1   32.3   32.2   32.5   32.4   ...
```
→ **Pattern:** Stateful (track last timestamp loaded)

**Batch Process (Discrete Events):**
```
Batch_ID  Start_Time         End_Time           Product  Yield
B001      2025-01-01 08:00   2025-01-01 12:00   A        95.2%
B002      2025-01-01 13:00   2025-01-01 17:00   B        97.1%
```
→ **Pattern:** Incremental by batch completion timestamp

#### ✋ Do - Classify Your Data

For each data type below, choose the best pattern:

| Data Type | Pattern | Reasoning |
|-----------|---------|-----------|
| Historian tags (1-sec resolution) | Stateful HWM | Timestamps are reliable |
| Daily production reports (CSV dump) | Rolling window (1 day lookback) | No timestamps in filename |
| Equipment maintenance logs | Stateful HWM | Has `maintenance_date` column |
| Lab results (arrive late) | Rolling window (7 days) | Late-arriving data common |

**Decision Tree:**
```
Does your data have reliable timestamps?
├─► Yes → Use stateful (high-water mark)
│         Example: sensor_timestamp, batch_end_time
└─► No → Use rolling window with lookback
          Example: daily file drops, late-arriving lab results
```

#### ✅ Verify
- [ ] You understand stateful HWM (exact incremental)
- [ ] You know when to use rolling window (safety buffer)
- [ ] You can identify the timestamp column in your data
- [ ] You understand late-arriving data challenges

---

### Module 3: Dimensions and Facts for Process Data (30 min)

#### 📖 Read
- [Dimension Pattern](../patterns/dimension.md)
- [Fact Pattern](../patterns/fact.md)
- [Date Dimension Pattern](../patterns/date_dimension.md)

#### 👀 Visual - Manufacturing Example

**Dimensions (Who/What/Where):**
```
dim_equipment
- equipment_id (natural key)
- equipment_sk (surrogate key)
- equipment_name
- location
- capacity
- install_date

dim_material
- material_id (natural key)
- material_sk (surrogate key)
- material_name
- specification
- supplier

dim_date
- date_id (natural key: YYYYMMDD)
- date_sk (surrogate key)
- date, year, month, day
- is_shift_1, is_shift_2, is_shift_3
```

**Facts (Measurements/Events):**
```
fact_production
- production_sk (surrogate key)
- date_sk (FK to dim_date)
- equipment_sk (FK to dim_equipment)
- material_sk (FK to dim_material)
- batch_id (degenerate dimension)
- quantity_produced
- energy_consumed_kwh
- downtime_minutes
- quality_score
```

#### ✋ Do - Design Your Schema

Pick a process from your domain and sketch a star schema:

**Example: HVAC Chiller Performance**

Dimensions:
- `dim_chiller` (chiller_id, model, capacity_tons, building)
- `dim_time` (hour of day, shift, weekend/weekday)
- `dim_date` (date, month, season)

Fact:
- `fact_chiller_performance`
  - Grain: hourly reading per chiller
  - Measures: `kwh_consumed`, `ton_hours`, `cop` (coefficient of performance)
  - FKs: `chiller_sk`, `date_sk`, `time_sk`

#### ✅ Verify
- [ ] You can distinguish dimensions (descriptive) from facts (measured)
- [ ] You understand grain (what does one row represent?)
- [ ] You know degenerate dimensions (IDs in fact without separate table)

---

### Module 4: Data Quality for Process Data (45 min)

#### 📖 Read
- [Validation Tests](../validation/tests.md)
- [Quality Gates](../features/quality_gates.md)
- [Contracts](../validation/contracts.md)

#### 👀 Visual - Where Quality Checks Happen

```
Source → [Contract] → Transform → [Validation] → Write
           ↓                        ↓
   "Safe to process?"      "Output correct?"
```

#### ✋ Do - Define Your Quality Rules

**Scenario: Sensor Data Pipeline**

Source: `reactor_temp_tags.csv`
```csv
timestamp,reactor_id,temperature_c,pressure_bar
2025-01-01 00:00:00,R01,145.2,3.5
2025-01-01 00:01:00,R01,999.9,3.6  ← Out of range
2025-01-01 00:02:00,R01,146.1,NULL  ← Missing pressure
```

Write validation rules in YAML:

```yaml
nodes:
  - name: load_reactor_data
    read:
      connection: scada
      path: reactor_temp_tags.csv
      format: csv
    
    # Input contracts (fail fast)
    contracts:
      - type: not_null
        columns: [timestamp, reactor_id, temperature_c]
      - type: freshness
        column: timestamp
        max_age: "1h"  # Data must be recent
    
    # Output validation (after cleaning)
    validation:
      tests:
        - type: accepted_range
          column: temperature_c
          min: 100.0
          max: 200.0
        - type: accepted_range
          column: pressure_bar
          min: 0.5
          max: 10.0
        - type: not_null
          columns: [pressure_bar]  # After filling nulls
      gate:
        on_failure: warn  # Log warnings but continue
```

**Exercise: Add Quarantine**

Route bad records to review:

```yaml
validation:
  gate:
    on_failure: quarantine
    quarantine_path: data/quarantine/reactor_data
    quarantine_format: parquet
```

Now bad rows go to `quarantine/` instead of silently passing.

#### ✅ Verify
- [ ] You can write validation rules for your domain thresholds
- [ ] You understand contracts (pre-check) vs validation (post-check)
- [ ] You know when to use `fail` vs `warn` vs `quarantine`
- [ ] You can articulate business impact of failed validations

---

### Module 5: Reading a Data Story (Process Context) (30 min)

#### 📖 Read
- [How to Read a Data Story](../guides/how_to_read_a_story.md)

#### ✋ Do
Run a process-themed example:

```bash
# If you have sample sensor data
odibi run examples/manufacturing/sensor_pipeline.yaml

# Or use the canonical example
odibi run docs/examples/canonical/runnable/04_fact_table.yaml

odibi story view --latest
```

**Inspect the Story and answer:**

1. **Lineage Section:**
   - Which nodes ran in what order?
   - Draw the dependency graph on paper

2. **Data Profile Section:**
   - Row count: How many readings were processed?
   - Schema: Did any columns get added/removed?
   - Sample data: Do values look reasonable?

3. **Validation Section:**
   - Any failed tests?
   - Any warnings about out-of-range values?

4. **Explanation Section:**
   - Is there business logic documented?
   - Can you understand what the pipeline does without asking someone?

**Key Questions for Your Team:**
- If a validation fails at 2 AM, what's the impact?
- Should we stop the pipeline (`fail`) or log and continue (`warn`)?
- How do we route bad sensor readings for manual review?

#### ✅ Verify
- [ ] You can interpret row counts and schema changes
- [ ] You can identify failed validations
- [ ] You understand what information a DE needs from you
- [ ] You can file a ticket with clear business context

---

## 🏆 Capstone Project

**Design a Sensor-to-KPI Pipeline**

### Scenario: Boiler Efficiency Monitoring

**Your Inputs:**
- Steam flow tags (1-min resolution, timestamped)
- Fuel consumption tags (1-min resolution, timestamped)
- Boiler equipment master data (CSV, updated weekly)

**Your Goal:**
Calculate hourly boiler efficiency:
```
Efficiency = (Steam Energy Out) / (Fuel Energy In)
```

### Requirements

1. **Load sensor data incrementally** (stateful HWM on timestamp)
2. **Validate thresholds:**
   - Steam flow: 0-10,000 kg/hr
   - Fuel flow: 0-500 kg/hr
   - Quarantine out-of-range values
3. **Join with equipment dimensions** (boiler capacity, install date)
4. **Aggregate to hourly efficiency**
5. **Document the business logic** in YAML explanation section

### Starter YAML Template

```yaml
project: boiler_efficiency
engine: pandas

connections:
  scada:
    type: local
    base_path: ./data/scada
  silver:
    type: local
    base_path: ./data/silver
  gold:
    type: local
    base_path: ./data/gold

story:
  connection: gold
  path: stories/

system:
  connection: gold
  path: _system

pipelines:
  - pipeline: ingest_tags
    nodes:
      - name: load_steam_flow
        explanation: |
          ## Steam Flow Tags
          Reads 1-minute resolution steam flow from SCADA historian.
          
          **Business Rule:** Flow > 10,000 kg/hr indicates sensor malfunction.
          Quarantine for review by instrumentation team.
        
        read:
          connection: scada
          path: steam_flow_tags.csv
          format: csv
          options:
            incremental:
              mode: stateful
              column: timestamp
              initial_value: "2025-01-01"
        
        # TODO: Add validation for flow range
        # TODO: Add quarantine for out-of-range
        
        write:
          connection: silver
          path: steam_flow
          format: parquet
          mode: append

      # TODO: Add node for fuel_flow (similar pattern)
      
      # TODO: Add node for equipment dimension
      
      # TODO: Add node to calculate hourly efficiency
```

### Deliverables

1. **Complete YAML** with all TODOs filled in
2. **Explanation sections** for each node (business context)
3. **Validation rules** for your thresholds
4. **Data Story** showing:
   - Lineage graph
   - Validation results
   - Sample efficiency calculations

### Verification

Answer these questions:
1. If steam flow goes to 15,000 kg/hr, what happens?
2. If fuel data arrives 2 hours late, does it get picked up?
3. How do you know if efficiency dropped below 80%?
4. Can a junior engineer understand your pipeline from the Story alone?

#### ✅ Verify
- [ ] Your pipeline runs without errors
- [ ] Validation catches out-of-range values
- [ ] Incremental loading works (run twice, second run loads only new data)
- [ ] Explanation sections clearly document business rules
- [ ] Data Story is readable by non-engineers

---

## ➡️ Next Steps

You've completed the ChemE/SME journey! Here's where to go next:

### Deepen Your Skills
- [Glossary](../reference/glossary.md) - Learn data engineering terminology
- [Dimensional Modeling Tutorial](../tutorials/dimensional_modeling/01_introduction.md) - Star schemas in depth
- [Manufacturing Patterns](../patterns/manufacturing.md) - Phase analysis, batch tracking

### Collaborate with DEs
- [Jr Data Engineer Journey](junior-data-engineer.md) - Understand what your DE partners do
- [Filing Issues Guide](../guides/filing_issues.md) ← **New guide**
- [Requesting Metrics Guide](../guides/requesting_metrics.md) ← **New guide**

### Apply to Your Domain
- [Thermodynamics Transformers](../guides/thermodynamics.md) - Steam properties, refrigerants
- [Unit Conversion Guide](../guides/unit_conversion.md) - Pint integration
- [Process Simulation Guide](../guides/process_simulation_guide.md) - Digital twins

### Advanced (Optional)
- [Sr Data Engineer Journey](senior-data-engineer.md) - If you want to build pipelines yourself

---

## 📣 Share Your Success

Completed the capstone? **Your domain expertise + data engineering = powerful insights!**

- Share your pipeline design in [GitHub Discussions](https://github.com/henryodibi11/Odibi/discussions)
- Write a case study for your industry
- Tag us on LinkedIn with #OdibiDomainExpert
- Present your Data Story to your team!

---

## 🌟 Why This Matters

**You bring the domain knowledge.** Data engineers bring the tooling. Together, you:
- Catch process anomalies before they impact production
- Build KPIs that actually reflect plant reality
- Document tribal knowledge in code (explanation sections)
- Enable real-time decision-making

**"We Win When You Win"** — Your success is our success.

---

**Questions?** [Troubleshooting Guide](../troubleshooting.md) | [FAQ](../guides/faq.md)
