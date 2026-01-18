# PHASE 2D: SCD2 PATTERN ANALYSIS

## Overview
The `SCD2Pattern` within the Odibi framework is a dedicated implementation of Slowly Changing Dimension Type 2 (SCD2). This pattern ensures historical tracking for dimensional data and maintains changes as new versions of rows with `valid_from` and `valid_to` dates, and an `is_current` flag.

---

## Configuration Parameters

### Required Parameters:
1. **`keys`** (list)
   - Business key columns used to uniquely identify records and detect changes.
   - **Required**: Yes.

2. **`target`** (str)
   - Target table or path (Spark/Pandas supported).
   - **Required**: Yes.

---

### Additional Parameters:
1. **`time_col`** (str)
   - Timestamp column for incoming records used for versioning.
   - **Default**: Current time.

2. **`valid_from_col`** (str)
   - Column for start date (when the record became valid).
   - **Default**: `valid_from`.

3. **`valid_to_col`** (str)
   - Column for end date (when the record was invalidated or replaced).
   - **Default**: `valid_to`.

4. **`is_current_col`** (str)
   - Column marking whether a record is the latest version (`True` or `False`).
   - **Default**: `is_current`.

5. **`track_cols`** (_Optional_)
   - Identifies the columns being tracked. Changes in these columns mark a new version.

---

## Key Methods

### 1. `validate(self) -> None`
- Validates the required and optional parameters.
- Validates the following critical points:
  - Presence of business `keys`.
  - `target` parameter (must specify an existing path or table).
  - Correct field keys that correspond to expected configurations, as defined by `SCD2Params`.

### 2. `execute(self, context: EngineContext) -> Any`
- The main execution method for implementing the SCD2 processing.
- Handles:
  - Parameter extraction and validation.
  - Row versioning.
  - Exact tracking based on `track_cols`.
  - Result preparation with updated surrogate keys.
- Returns the transformed DataFrame.

---

## Workflow

1. **SCD2 Logic Pipeline**:
   - The `SCD2Params` configuration integrates closely with the `scd2` transformer to enforce history-tracking.
   - Records with the same business key (`keys`) and changes to `track_cols` are invalidated and new rows are generated.

2. **Integration with Engines**:
   - The pattern distinguishes between Spark and Pandas DataFrame operations, ensuring functionality across both engines.

3. **Performance and Optimization**:
   - Enables manual filtering for current valid rows using `is_current` and related fields.
   - Utilizes efficient data filtering functions specific to Spark or Pandas (`count` or `len`).

---

## Example YAML Configuration

```yaml
pattern:
  type: scd2
  params:
    keys: ["customer_id"]
    target: "dim_customer"
    track_cols: ["email", "address"]
    valid_from_col: "valid_from"
    valid_to_col: "valid_to"
    is_current_col: "is_current"
    time_col: "update_time"
```

---

## Observations and Gotchas

### 1. Target Definition:
   - The `target` parameter must be correctly set to a valid table or path. If the target does not exist, the system raises runtime errors.

### 2. Key Constraints:
   - The presence of `keys` is critical, as these determine the uniqueness and change tracking across records.

### 3. Auto-Filled Columns:
   - The configuration allows defaults for `valid_from_col`, `valid_to_col`, and `is_current_col`, reducing manual overhead when aligning column names across datasets.

---

## Summary

The `SCD2Pattern` is an essential part of the Odibi framework, providing robust support for Slowly Changing Dimension Type 2 implementations. Its ability to handle complex historical data tracking with minimal configuration makes it a critical choice for data warehouse scenarios where data change over time needs to be preserved.

---

## Next Steps
Proceed to analyze and document the `merge.py` file corresponding to Phase 2E: MERGE PATTERN.
