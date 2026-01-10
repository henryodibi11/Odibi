# Manufacturing Phase Analysis Pattern

## The Problem

Manufacturing processes generate **time-series sensor data** at high frequency (every second or minute). Raw data looks like this:

```
timestamp           | RunID | SetupTime | ProcessTime | Status | Level | Weight
2024-01-01 10:00:00 | R001  | 0         | 0           | 1      | 10    | 500
2024-01-01 10:01:00 | R001  | 45        | 0           | 2      | 15    | 500
2024-01-01 10:02:00 | R001  | 105       | 0           | 2      | 25    | 502
... (thousands of rows per run)
```

But business users need **one summary row per run**:

```
RunID | ItemCode | SetupTime_start     | SetupTime_end       | SetupTime_max_minutes | SetupTime_active_minutes | ProcessTime_start | ...
R001  | ITEM-A   | 2024-01-01 09:59:15 | 2024-01-01 10:15:00 | 15.75                 | 12.5                     | 2024-01-01 11:00:00 | ...
```

This pattern solves:
- **Phase boundary detection** - When did each phase start and end?
- **Time-in-state analysis** - How long was the equipment idle vs active?
- **Sequential phase chaining** - Each phase starts after the previous ends

---

## How PLC Timers Work

Understanding this is key to using the transformer correctly.

### The Timer Column

In manufacturing systems, a **PLC (Programmable Logic Controller)** tracks phase durations with timer columns. The timer:

1. **Starts at 0** when the phase begins
2. **Increments every second** while the phase is running
3. **Stops (plateaus)** when the phase completes

```
Real timeline:
10:00:15 - Phase STARTS (timer = 0)
10:01:15 - Timer = 60 seconds
10:02:15 - Timer = 120 seconds
10:05:15 - Phase ENDS (timer stops at 300)
10:06:00 - Timer still shows 300 (plateau)
```

### The Polling Problem

Your data system **polls** the PLC at intervals (e.g., every minute). You don't see the exact start - you see:

```
What you capture:
10:00:00 | SetupTime = 0      ← Phase hasn't started yet
10:01:00 | SetupTime = 45     ← Phase started ~45 seconds ago!
10:02:00 | SetupTime = 105    ← Running
10:05:00 | SetupTime = 285    ← Running
10:06:00 | SetupTime = 300    ← Stopped
10:07:00 | SetupTime = 300    ← Still 300 (plateau = phase ended)
```

### Back-Calculating True Start

The transformer detects this pattern and **back-calculates** the true start:

```
First reading: timestamp = 10:01:00, timer = 45 seconds
True start = 10:01:00 - 45 seconds = 10:00:15
```

### Detecting Phase End

The transformer finds the **first repeated value** (plateau):

```
10:05:00 | SetupTime = 285
10:06:00 | SetupTime = 300
10:07:00 | SetupTime = 300  ← REPEAT! Phase ended at 10:06:00
```

---

## Basic Usage

### Minimal Configuration

```yaml
transform:
  - detect_sequential_phases:
      group_by: RunID
      timestamp_col: ts
      phases:
        - SetupTime
        - ProcessTime
        - CycleTime
```

**Output columns:**
- `RunID`
- `SetupTime_start`, `SetupTime_end`, `SetupTime_max_minutes`
- `ProcessTime_start`, `ProcessTime_end`, `ProcessTime_max_minutes`
- `CycleTime_start`, `CycleTime_end`, `CycleTime_max_minutes`

### With Status Tracking

Track how long equipment spent in each state during each phase:

```yaml
transform:
  - detect_sequential_phases:
      group_by: RunID
      timestamp_col: ts
      phases: [SetupTime, ProcessTime, CycleTime]
      status_col: Status
      status_mapping:
        1: idle
        2: active
        3: hold
        4: faulted
```

**Additional output columns:**
- `SetupTime_idle_minutes`, `SetupTime_active_minutes`, `SetupTime_hold_minutes`, `SetupTime_faulted_minutes`
- `ProcessTime_idle_minutes`, `ProcessTime_active_minutes`, ...

### With Phase Metrics

Capture max/min/avg of other columns within each phase window:

```yaml
transform:
  - detect_sequential_phases:
      group_by: RunID
      timestamp_col: ts
      phases: [SetupTime, ProcessTime]
      phase_metrics:
        Level: max
        Pressure: max
        Temperature: mean
```

**Additional output columns:**
- `SetupTime_Level`, `SetupTime_Pressure`, `SetupTime_Temperature`
- `ProcessTime_Level`, `ProcessTime_Pressure`, `ProcessTime_Temperature`

### With Metadata

Include run-level attributes in the output:

```yaml
transform:
  - detect_sequential_phases:
      group_by: RunID
      timestamp_col: ts
      phases: [SetupTime, ProcessTime]
      metadata:
        ItemCode: first_after_start  # First value after phase 1 starts
        Weight: max                  # Max weight across run
        Operator: first              # First value in group
```

**Metadata aggregation options:**
| Method | Description |
|--------|-------------|
| `first` | First value in the group |
| `last` | Last value in the group |
| `first_after_start` | First value after the first phase starts (skips initialization rows) |
| `max`, `min`, `mean`, `sum` | Standard aggregations |

---

## Full Example: Batch Process Analysis

A complete Silver layer transformation for batch processing data:

```yaml
pipelines:
  - pipeline: silver_run_analysis
    layer: silver
    nodes:
      - name: run_phase_summary
        read:
          connection: lakehouse
          format: delta
          path: "bronze/process_telemetry"

        transform:
          # Step 1: Detect all phases and calculate metrics
          - detect_sequential_phases:
              group_by: RunID
              timestamp_col: ts
              phases:
                - SetupTime
                - PrepTime
                - ActiveTime
                - CycleTime
                - CompleteTime
              start_threshold: 240
              status_col: Status
              status_mapping:
                1: idle
                2: active
                3: hold
                4: faulted
              phase_metrics:
                Level: max
                Temperature: max
              metadata:
                ItemCode: first_after_start
                Weight: max

          # Step 2: Filter to complete runs only
          - filter_rows:
              condition: "ActiveTime_Level > 70"

          # Step 3: Sort by run start time
          - sort:
              by: SetupTime_start
              ascending: true

        write:
          connection: lakehouse
          format: delta
          path: "silver/run_phase_summary"
          mode: overwrite
```

---

## Configuration Reference

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `group_by` | `str` or `List[str]` | Yes | - | Column(s) to group by. E.g., `"RunID"` or `["RunID", "AssetID"]` |
| `timestamp_col` | `str` | No | `"ts"` | Timestamp column for ordering |
| `phases` | `List[str]` | Yes | - | Timer columns representing sequential phases |
| `start_threshold` | `int` | No | `240` | Max timer value (seconds) to consider as valid start |
| `status_col` | `str` | No | - | Column containing equipment status codes |
| `status_mapping` | `Dict[int, str]` | No | - | Map status codes to names |
| `phase_metrics` | `Dict[str, str]` | No | - | Columns to aggregate within each phase |
| `metadata` | `Dict[str, str]` | No | - | Run-level columns with aggregation method |
| `fill_null_minutes` | `bool` | No | `False` | If True, fill null numeric columns with 0. Timestamps remain null for skipped phases. |
| `spark_native` | `bool` | No | `False` | If True, use native Spark window functions instead of applyInPandas. See Engine Support section. |

### Output Columns

For each phase (e.g., `SetupTime`):

| Column | Description |
|--------|-------------|
| `{phase}_start` | Phase start timestamp |
| `{phase}_end` | Phase end timestamp |
| `{phase}_max_minutes` | Maximum timer value in minutes |
| `{phase}_{status}_minutes` | Time in each status (if status tracking enabled) |
| `{phase}_{metric}` | Aggregated metrics (if phase_metrics configured) |

---

## Edge Cases Handled

The transformer handles real-world data quality issues:

### Late Readings

If your first reading shows a timer value > `start_threshold`, the transformer skips it and looks for a valid start:

```yaml
start_threshold: 240  # Ignore readings where timer > 4 minutes
```

**Why?** If you see `SetupTime = 500` on the first reading, you missed the start. The transformer finds a valid start point.

### Empty/Skipped Phases

If a phase never ran (timer is always 0), the columns are still generated but with null values. Use `fill_null_minutes: true` to replace null numeric values with 0:

```yaml
transform:
  - detect_sequential_phases:
      group_by: RunID
      phases: [SetupTime, ProcessTime]
      fill_null_minutes: true  # null _max_minutes, _status_minutes → 0
```

Timestamp columns (`_start`, `_end`) remain null for skipped phases.

### NaN Status Values

The transformer filters out rows with null/NaN status values when calculating time-in-state.

### Sequential Chaining

Each phase automatically starts looking for data **after the previous phase ended**. This prevents overlap when phases share the same data window.

### No Data After Start

If the data ends mid-phase (no plateau detected), the transformer uses the last available reading as the end time.

---

## Multi-Column Grouping

Group by multiple columns when the same run operates on different equipment:

```yaml
transform:
  - detect_sequential_phases:
      group_by:
        - RunID
        - AssetID
      timestamp_col: ts
      phases: [SetupTime, ProcessTime]
```

**Output:** One row per unique `(RunID, AssetID)` combination.

---

## Use Cases

| Industry | Phases | Status Codes |
|----------|--------|--------------|
| **Batch Processing** | Setup, Prep, Active, Complete | idle, active, hold, faulted |
| **Assembly Line** | Load, Process, Inspect, Unload | running, stopped, maintenance |
| **CIP Cleaning** | PreRinse, Wash, Rinse, Sanitize | active, draining, idle |
| **Injection Molding** | Clamp, Inject, Cycle, Eject | running, setup, fault |
| **Discrete Manufacturing** | Queue, Setup, Run, Teardown | running, hold, changeover |

---

## Engine Support

| Engine | Implementation | Scalability |
|--------|----------------|-------------|
| **Pandas** | Native loops | Single machine |
| **Spark** | `applyInPandas` (default) | Parallel per group across cluster |
| **Polars** | Pandas fallback | Single machine |

### Spark Performance

By default, Spark uses `applyInPandas` which:
- Groups data once, then processes each run in parallel
- Works well for datasets with **many runs** (e.g., thousands of RunIDs)

For datasets with **few large runs**, you can try native Spark window functions:

```yaml
transform:
  - detect_sequential_phases:
      group_by: RunID
      phases: [SetupTime, ProcessTime]
      spark_native: true  # Use window functions instead of applyInPandas
```

The native implementation uses `lag`, `lead`, and `row_number` window functions but requires multiple joins per phase, which can be slower when there are many phases or shuffles.

---

## Tips

1. **Set `start_threshold` appropriately** - Match your data polling interval. If you poll every minute, 240 seconds (4 minutes) is a safe threshold.

2. **Don't include `status_col` if you don't need it** - The transformer works fine without status tracking; you just won't get time-in-state columns.

3. **Use `phase_metrics` for QA filtering** - Capture `Level: max` to filter out incomplete runs:
   ```yaml
   phase_metrics:
     Level: max
   # Then filter:
   - filter_rows:
       condition: "ProcessTime_Level > 70"
   ```

4. **Chain with other transformers** - The output is a normal DataFrame. Use `filter_rows`, `drop_columns`, `sort`, etc. for post-processing.
