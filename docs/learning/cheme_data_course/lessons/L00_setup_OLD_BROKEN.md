# L00: Setup - Odibi Basics & Data Formats

**Prerequisites:** None | **Effort:** 45 min | **Seborg:** N/A (Setup)

---

## Learning Objectives

By the end of this lesson you will:

1. ✅ Install Odibi and verify it works
2. ✅ Understand the difference between CSV, Parquet, and Delta Lake
3. ✅ Run your first pipeline and generate time-series data
4. ✅ Understand seeds, timestamps, and reproducibility
5. ✅ Know where output data lands and how to inspect it

---

## Theory Recap: Time Series Data Basics

**Time series data:** Measurements taken at regular intervals (every 1 minute, 5 seconds, etc.)

**Key concepts:**
- **Timestamp:** When the measurement was taken (must be consistent!)
- **Timestep (Δt):** How often data is collected (e.g., 1 minute intervals)
- **Seed:** Random number generator starting point (same seed → same "random" data)
- **Reproducibility:** Can you run the pipeline twice and get identical results?

**Why this matters:**
In process plants, you have sensors logging data every few seconds. You need to:
- Store it efficiently (Parquet, not CSV for large data)
- Query it quickly (timestamps indexed properly)
- Reproduce analyses (seeds matter for simulation)

---

## Installation

### **Step 1: Install Odibi**

```bash
# Using pip (recommended)
pip install odibi

# Or from source (if you're developing)
git clone https://github.com/henryodibi11/Odibi.git
cd Odibi
pip install -e .
```

### **Step 2: Verify Installation**

```bash
odibi --version
```

You should see something like: `odibi version 0.x.x`

### **Step 3: Check Available Commands**

```bash
odibi list transformers    # See all 56 transformers
odibi list patterns        # See all 6 loading patterns
odibi list connections     # See connection types
```

---

## Odibi Hands-On

### **Example 1: Minimal Pipeline (CSV Output)**

Create a file `tank_data.yaml`:

```yaml
# tank_data.yaml - Your first Odibi pipeline
pipeline:
  name: tank_101
  description: Simple tank level data

connections:
  output_csv:
    type: local
    path: ./output/tank_data.csv

nodes:
  - name: generate_tank_data
    output: output_csv

    simulation:
      entities: ["TK-101"]
      start_time: "2024-01-01 00:00:00"
      timestep: "1min"
      row_count: 100  # 100 minutes of data
      seed: 42        # For reproducibility

      columns:
        # Inlet flow (manipulated variable)
        - name: inlet_flow_gpm
          data_type: float
          generator:
            type: constant
            value: 50.0

        # Outlet flow (constant load)
        - name: outlet_flow_gpm
          data_type: float
          generator:
            type: constant
            value: 45.0

        # Tank level (calculated from balance)
        - name: tank_level_ft
          data_type: float
          generator:
            type: derived
            expression: |
              # Simple mass balance: dL/dt = (inflow - outflow) / Area
              prev('tank_level_ft', 10.0) +
              (inlet_flow_gpm - outlet_flow_gpm) / 100.0
```

### **Run the Pipeline:**

```bash
odibi run tank_data.yaml
```

**What just happened:**
1. Odibi created 100 rows of data (100 minutes)
2. Each row has a timestamp (starting 2024-01-01 00:00:00, incrementing by 1 minute)
3. Inlet flow = 50 gpm (constant)
4. Outlet flow = 45 gpm (constant)
5. Level calculated from prev() function (starts at 10 ft, increases because inflow > outflow)
6. Data saved to `./output/tank_data.csv`

### **Inspect the Output:**

```bash
# Look at first 10 rows
head -10 output/tank_data.csv
```

You should see:
```csv
timestamp,entity_id,inlet_flow_gpm,outlet_flow_gpm,tank_level_ft
2024-01-01 00:00:00,TK-101,50.0,45.0,10.0
2024-01-01 00:01:00,TK-101,50.0,45.0,10.05
2024-01-01 00:02:00,TK-101,50.0,45.0,10.10
...
```

Notice:
- Timestamp increments by 1 minute
- Level increases by 0.05 ft each minute (because net inflow = 5 gpm / 100 ft² area)

---

### **Example 2: Parquet Output (Better for Large Data)**

Modify the YAML:

```yaml
connections:
  output_parquet:
    type: local
    path: ./output/tank_data.parquet
    format: parquet  # Changed from CSV

nodes:
  - name: generate_tank_data
    output: output_parquet
    # ... (rest is the same)
```

**Run again:**

```bash
odibi run tank_data.yaml
```

**Why Parquet?**
- 5-10x smaller file size than CSV
- 10-100x faster to read in Python/Pandas
- Preserves data types (CSV turns everything into strings)
- Industry standard for data engineering

**Inspect Parquet:**

```python
import pandas as pd

df = pd.read_parquet('./output/tank_data.parquet')
print(df.head())
print(df.dtypes)  # Notice: types are preserved (float, datetime)
```

---

### **Example 3: Realistic Data with Noise**

Real sensors have noise. Let's add it:

```yaml
simulation:
  entities: ["TK-101"]
  start_time: "2024-01-01 00:00:00"
  timestep: "1min"
  row_count: 1440  # 24 hours
  seed: 42

  columns:
    # Inlet flow with realistic variation
    - name: inlet_flow_gpm
      data_type: float
      generator:
        type: random_walk
        start: 50.0
        min: 45.0
        max: 55.0
        step_size: 0.5

    # Outlet flow (mostly constant but slight drift)
    - name: outlet_flow_gpm
      data_type: float
      generator:
        type: random_walk
        start: 45.0
        min: 40.0
        max: 50.0
        step_size: 0.2

    # Tank level (from balance)
    - name: tank_level_ft
      data_type: float
      generator:
        type: derived
        expression: |
          prev('tank_level_ft', 10.0) +
          (inlet_flow_gpm - outlet_flow_gpm) / 100.0

    # Level sensor (with measurement noise)
    - name: level_sensor_ft
      data_type: float
      generator:
        type: derived
        expression: "tank_level_ft + sensor_noise"

    # Sensor noise (±0.1 ft)
    - name: sensor_noise
      data_type: float
      generator:
        type: range
        min: -0.1
        max: 0.1
```

**Run and plot:**

```python
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_parquet('./output/tank_data.parquet')

fig, ax = plt.subplots(2, 1, figsize=(10, 6))

# Flows
ax[0].plot(df['timestamp'], df['inlet_flow_gpm'], label='Inlet')
ax[0].plot(df['timestamp'], df['outlet_flow_gpm'], label='Outlet')
ax[0].set_ylabel('Flow (gpm)')
ax[0].legend()
ax[0].grid(True)

# Levels
ax[1].plot(df['timestamp'], df['tank_level_ft'], label='True Level', linewidth=2)
ax[1].plot(df['timestamp'], df['level_sensor_ft'], label='Sensor (noisy)', alpha=0.7)
ax[1].set_ylabel('Level (ft)')
ax[1].set_xlabel('Time')
ax[1].legend()
ax[1].grid(True)

plt.tight_layout()
plt.savefig('tank_level_plot.png')
plt.show()
```

**You should see:**
- Inlet/outlet flows varying realistically
- True level vs noisy sensor reading (±0.1 ft scatter)
- Level trending up/down based on net flow

---

## Data Engineering Focus: File Formats

### **CSV (Comma-Separated Values)**

**Pros:**
- Human-readable (open in Excel/Notepad)
- Universal (every tool can read it)

**Cons:**
- Large file size (everything stored as text)
- Slow to read (must parse every character)
- No data types (everything is a string)
- No compression

**Use when:**
- Sharing data with non-technical users
- Very small datasets (< 10,000 rows)
- Need to manually inspect/edit

### **Parquet (Columnar Binary Format)**

**Pros:**
- 5-10x smaller than CSV (compression + binary)
- 10-100x faster to read (columnar storage)
- Preserves data types (float, int, datetime)
- Industry standard (works with Spark, Pandas, Polars, DuckDB)

**Cons:**
- Not human-readable (binary file)
- Can't edit in text editor

**Use when:**
- Working with > 10,000 rows
- Need performance
- Building data pipelines (99% of the time)

### **Delta Lake (Parquet + Transaction Log)**

**Pros:**
- All benefits of Parquet PLUS:
- ACID transactions (atomic writes)
- Time travel (query historical versions)
- Schema evolution (add columns without breaking)
- MERGE/UPSERT support
- Z-ordering for query performance

**Cons:**
- Requires Delta Lake library
- Slightly more complex setup

**Use when:**
- Production data pipelines
- Need updates/deletes (not just appends)
- Multiple writers (need transaction safety)
- Need audit trail (time travel)

**Rule of thumb:**
- Exploring/prototyping → CSV
- Development/testing → Parquet
- Production → Delta Lake

---

## Validation: Reproducibility Check

**Key concept:** Same seed → Same output (always!)

**Test:**

```bash
# Run 1
odibi run tank_data.yaml
mv output/tank_data.parquet output/run1.parquet

# Run 2 (same YAML, same seed)
odibi run tank_data.yaml
mv output/tank_data.parquet output/run2.parquet

# Compare
```

```python
import pandas as pd

df1 = pd.read_parquet('output/run1.parquet')
df2 = pd.read_parquet('output/run2.parquet')

# Should be EXACTLY identical
assert df1.equals(df2), "Runs are not reproducible!"
print("✅ Reproducibility verified!")
```

If they're different, check:
- Same seed in YAML?
- Same Odibi version?
- Didn't use system time in expressions?

---

## Exercises

### **E1: Change the Timestep**

Modify `tank_data.yaml`:
- Change `timestep: "1min"` to `timestep: "10sec"`
- Keep `row_count: 1440` (now 4 hours instead of 24)
- Run and compare file size to 1-minute data

**Question:** Why is the file larger?

<details>
<summary>Answer</summary>

More rows (1440 rows at 10-sec intervals = 4 hours vs 24 hours at 1-min intervals). Even though time span is shorter, you have more data points, so larger file.
</details>

---

### **E2: Add a Temperature Column**

Add a new column to `tank_data.yaml`:

```yaml
- name: tank_temp_f
  data_type: float
  generator:
    type: random_walk
    start: 75.0
    min: 70.0
    max: 80.0
    step_size: 0.3
```

Run the pipeline. Verify the new column appears in the output.

**Challenge:** Add measurement noise (±0.5°F) to the temperature like we did with level.

---

### **E3: Break Reproducibility**

Modify the YAML to use current system time instead of a fixed seed:

```yaml
- name: random_value
  data_type: float
  generator:
    type: range
    min: 0.0
    max: 10.0
```

Run twice and compare outputs. They should be different!

**Question:** When would you WANT non-reproducible data?

<details>
<summary>Answer</summary>

Monte Carlo simulations or when testing pipeline robustness under varying conditions. But for debugging and validation, reproducibility is critical.
</details>

---

### **E4: Multi-Entity Simulation**

Change `entities: ["TK-101"]` to:

```yaml
entities: ["TK-101", "TK-102", "TK-103"]
```

Run again. How many rows do you get?

**Answer:** 3x as many (3 entities × row_count).

Inspect the `entity_id` column - it cycles through TK-101, TK-102, TK-103.

---

## Solutions

All exercise solutions: [../solutions/L00.md](../solutions/L00.md)

Full YAML files: `/examples/cheme_course/L00_setup/`

---

## Reflection: How This Relates to Real Plants

**In a real plant:**
- Historians (OSIsoft PI, Aveva Wonderware) log sensor data every 1-10 seconds
- Data is stored in time-series databases (optimized like Parquet)
- You query specific time ranges (partitioning by date helps)
- Reproducibility matters for investigations ("what happened during the incident?")

**What you just learned:**
- How to generate realistic time-series data (same format as plant historians)
- File format trade-offs (Parquet is like time-series DB internals)
- Timesteps and sampling (same as configuring historian scan rates)
- Seeds for reproducibility (like snapshot/replay for incident analysis)

---

## Next Steps

**You now know:**
- ✅ How to install and run Odibi
- ✅ CSV vs Parquet vs Delta Lake
- ✅ Basic simulation YAML structure
- ✅ Generating time-series data with timestamps
- ✅ Reproducibility via seeds

**Next lesson:**
👉 [L01: CV/MV/DV and Time Series Data](L01_cv_mv_dv.md)

We'll map process control variables to data schemas and learn partitioning strategies.

---

## Quick Reference

### **Basic YAML Structure:**

```yaml
pipeline:
  name: my_pipeline

connections:
  output:
    type: local
    path: ./output/data.parquet
    format: parquet

nodes:
  - name: generate_data
    output: output

    simulation:
      entities: ["entity_1"]
      start_time: "2024-01-01 00:00:00"
      timestep: "1min"
      row_count: 100
      seed: 42

      columns:
        - name: my_column
          data_type: float
          generator:
            type: constant  # or random_walk, range, derived
            value: 10.0
```

### **Common Generators:**

| Generator | Use Case | Example |
|-----------|----------|---------|
| `constant` | Fixed values (equipment specs, setpoints) | `value: 100.0` |
| `range` | Random uniform (noise, disturbances) | `min: 0.0, max: 10.0` |
| `random_walk` | Slow-varying (temperatures, flows) | `start: 50.0, step_size: 0.5` |
| `derived` | Calculated (balances, control outputs) | `expression: "prev('x', 0) + 1"` |

### **Useful Commands:**

```bash
odibi run pipeline.yaml              # Run pipeline
odibi list transformers              # See all features
odibi explain <transformer_name>     # Get help on specific feature
odibi templates show <pattern>       # Show YAML templates
```

---

*Lesson L00 complete! Ready for L01.*
