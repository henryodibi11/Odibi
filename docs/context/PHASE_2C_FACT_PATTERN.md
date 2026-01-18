# PHASE 2C: FACT PATTERN ANALYSIS

## Overview
The `FactPattern` is a critical component in the Odibi framework, designed to build and manage fact tables in data warehousing. Its advanced functionality offers features such as surrogate key lookups, orphan handling, grain validation, and audit column tracking. This document provides an in-depth analysis of its configuration, methods, and runtime behaviors.

---

## Configuration Parameters

### Basic Parameters:
1. **`deduplicate`** (bool)
   - Removes duplicates before data insertion.
   - **Default**: False.
   - **Required**: If true, `keys` parameter must be provided.

2. **`keys`** (list)
   - Columns used for deduplication (used only if `deduplicate = true`).
   - Example: `["order_id", "product_id"]`.

3. **`grain`** (list)
   - Defines uniqueness. Used for grain validation to detect duplicate rows.
   - **Required**: No, but critical for validating primary key integrity.

4. **`dimensions`** (list of dict)
   - Defines dimension lookup configurations for surrogate key retrieval:
     - **`source_column`**:
       - The column name in the source fact to look up.
     - **`dimension_table`**:
       - The name of the dimension table to perform lookups.
     - **`dimension_key`**:
       - The column holding the natural key in the dimension.
     - **`surrogate_key`**:
       - The surrogate key to retrieve upon a match.
     - **`scd2`** (bool):
       - Option to filter rows with `is_current = true` for Slowly Changing Dimension Type 2.

5. **`orphan_handling`** (str)
   - Behavior for handling unmatched dimension lookups:
     - `"unknown"`: Maps unknown value to `SK = 0`.
     - `"reject"`: Fails with an error for unmatched rows.
     - `"quarantine"`: Moves unmatched rows to a quarantine location (requires `quarantine` configuration).

6. **`quarantine`** (dict)
   - Configuration for writing quarantined records:
     - **`connection`**: Target connection for quarantine writes.
     - **`path`**: Path for quarantine files (or overwrites table with desired target).
     - **`add_columns`** (dict):
         - **`_rejection_reason`**: Add rejection reasons as metadata (`True` or `False`).
         - **`_rejected_at`**: Include a timestamp recording rejection time.
         - **`_source_dimension`**: Add the name of the dimension source.

7. **`measures`** (list)
   - Data computation and transformations after dimension lookups.
   - Supports three types:
     - As-is Pass-through Columns: `["col1", "col2", ...]`.
     - Rename Columns: `{"new_column_name": "original_column_name"}`.
     - Custom Calculations: `{"new_measure_name": "expression e.g quantity*factor..."}`.

8. **`audit`** (dict)
   - Controls audit metadata:
     - **`load_timestamp`**: Timestamp for data load.
     - **`source_system`**: Identifies the source of the data.

---

### Example YAML
Detailed Complex Measures / Lookup Customizing outlined key-setup inclusion.

---
#### Path:
dimensional YAML FINAL Combined.
</task_progress>
<|vq_4406|></write_to_file>
