# ChemE Course Fixes - Quick Reference

## Before & After Comparison

### Top-Level Structure

#### ❌ BEFORE (Broken)
```yaml
pipeline:
  name: my_pipeline
  nodes:
    - name: node1
      output: output
      simulation:
        entities: ["TK-001"]
```

#### ✅ AFTER (Fixed)
```yaml
name: my_pipeline
engine: pandas

connections:
  output:
    type: local
    path: ./output/data.parquet
    format: parquet

pipelines:
  - name: my_pipeline
    nodes:
      - name: node1
        read:
          connection: null
          format: simulation
          options:
            simulation:
              # ...

        write:
          connection: output
```

---

### Simulation Block Structure

#### ❌ BEFORE (Broken)
```yaml
simulation:
  entities: ["TK-001"]
  start_time: "2024-01-01 00:00:00"
  timestep: 1min
  row_count: 100

  columns:
    - name: level
      generator:
        type: derived
```

#### ✅ AFTER (Fixed)
```yaml
read:
  connection: null
  format: simulation
  options:
    simulation:
      scope:
        start_time: "2024-01-01T00:00:00Z"
        timestep: "1min"
        row_count: 100
        seed: 42

      entities:
        count: 1
        id_prefix: "TK-"

      columns:
        - name: entity_id
          data_type: string
          generator:
            type: constant
            value: "{entity_id}"

        - name: timestamp
          data_type: timestamp
          generator:
            type: timestamp

        - name: level
          data_type: float
          generator:
            type: derived
            expression: "prev('level', 10.0) + ..."
```

---

### Generator Syntax

#### ❌ BEFORE (Broken)
```yaml
- name: flow
  generator:
    type: random_walk
    start: 50.0
    step_size: 1.0
```

#### ✅ AFTER (Fixed)
```yaml
- name: flow
  data_type: float  # Required!
  generator:
    type: random_walk
    start: 50.0
    drift: 0.0     # Required!
    noise: 1.0     # Use noise, not step_size
    min: 40.0
    max: 60.0
```

---

### Time Delay (CRITICAL L04 Fix)

#### ❌ BEFORE (Doesn't exist!)
```yaml
- name: delayed_input
  generator:
    type: derived
    expression: "prev('input', 0.0, lag=5)"  # ❌ NOT SUPPORTED
```

#### ✅ AFTER (Manual shift)
```yaml
# For 2-timestep delay
- name: input_delayed_1
  data_type: float
  generator:
    type: derived
    expression: "prev('input', 0.0)"

- name: input_delayed
  data_type: float
  generator:
    type: derived
    expression: "prev('input_delayed_1', 0.0)"

# Now use input_delayed (2 timesteps behind input)
```

---

### Validation

#### ❌ BEFORE (Broken)
```yaml
validation:
  tests:
    - name: level_check
      type: range
      columns: ["level"]
```

#### ✅ AFTER (Fixed)
```yaml
validate:
  tests:
    - type: range
      column: level  # Singular, not plural
      min: 0.0
      max: 20.0
```

---

## Common Mistakes & Fixes

### 1. Missing `data_type`
Every column MUST have `data_type` specified.

```yaml
✅ CORRECT:
- name: temperature
  data_type: float
  generator:
    type: constant
    value: 100.0
```

### 2. Missing Required Columns
Every simulation MUST have `entity_id` and `timestamp`:

```yaml
✅ ALWAYS INCLUDE:
columns:
  - name: entity_id
    data_type: string
    generator:
      type: constant
      value: "{entity_id}"

  - name: timestamp
    data_type: timestamp
    generator:
      type: timestamp

  # ... your other columns
```

### 3. Wrong Timestep Format
Must be quoted string:

```yaml
❌ WRONG: timestep: 1min
✅ CORRECT: timestep: "1min"
```

### 4. Missing `drift` in `random_walk`
```yaml
❌ WRONG:
generator:
  type: random_walk
  start: 50.0

✅ CORRECT:
generator:
  type: random_walk
  start: 50.0
  drift: 0.0  # Required!
  noise: 1.0
  min: 40.0
  max: 60.0
```

### 5. Using `step_size` Instead of `noise`
```yaml
❌ WRONG:
generator:
  type: random_walk
  step_size: 1.0

✅ CORRECT:
generator:
  type: random_walk
  noise: 1.0
```

---

## Generators Quick Reference

### `constant`
```yaml
generator:
  type: constant
  value: 50.0
```

### `sequential`
```yaml
generator:
  type: sequential
  start: 0
  step: 1
```

### `random_walk`
```yaml
generator:
  type: random_walk
  start: 50.0
  drift: 0.0     # Required
  noise: 1.0     # Required
  min: 40.0
  max: 60.0
```

### `range` (uniform or normal)
```yaml
generator:
  type: range
  min: -1.0
  max: 1.0
  distribution: normal  # Optional: uniform (default) or normal
```

### `categorical`
```yaml
generator:
  type: categorical
  values: ["AUTO", "MANUAL"]
  weights: [0.95, 0.05]  # Optional
```

### `derived`
```yaml
generator:
  type: derived
  expression: "prev('level', 10.0) + inlet - outlet"
```

### `timestamp`
```yaml
generator:
  type: timestamp  # No params needed
```

---

## Stateful Functions in `derived`

### `prev(column, default)`
```yaml
expression: "prev('level', 10.0) + 0.1"
```

### `ema(column, alpha, default)`
```yaml
expression: "ema('raw_sensor', 0.1, 150.0)"
```

### Math expressions
```yaml
expression: "inlet_flow - outlet_flow"
expression: "tank_temp + sensor_noise"
expression: "0.5 * (heater_pct - 50.0)"
```

### Conditionals
```yaml
expression: "60.0 if minutes_elapsed >= 30.0 else 50.0"
```

---

## File Locations

All working examples:
- `/examples/cheme_course/L00_setup/`
- `/examples/cheme_course/L01_cv_mv_dv/`
- `/examples/cheme_course/L02_dof_balances/`
- `/examples/cheme_course/L03_first_order/`
- `/examples/cheme_course/L04_foptd/`
- `/examples/cheme_course/L05_second_order/`

All fixed lessons:
- `/docs/learning/cheme_data_course/lessons/L01_cv_mv_dv.md`
- `/docs/learning/cheme_data_course/lessons/L02_dof_balances.md`
- `/docs/learning/cheme_data_course/lessons/L03_first_order.md`
- `/docs/learning/cheme_data_course/lessons/L04_foptd.md`
- `/docs/learning/cheme_data_course/lessons/L05_second_order.md`

Old broken backups:
- All saved with `_OLD_BROKEN.md` suffix

---

## Testing Command

```bash
cd examples/cheme_course/L01_cv_mv_dv
odibi run mixing_tank_ph.yaml
```

Should create `./output/mixing_tank_ph.parquet`
