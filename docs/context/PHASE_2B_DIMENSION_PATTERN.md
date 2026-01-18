# PHASE 2B: DIMENSION PATTERN ANALYSIS

## Overview
The `DimensionPattern` is a fundamental pattern in Odibi used for building and managing dimension tables in data warehouses. It supports various Slowly Changing Dimension (SCD) types, surrogate key management, and audit column tracking. This document provides an in-depth analysis of its configuration, methods, and runtime behaviors.

---

## Configuration Parameters

### Key Parameters:
1. **`natural_key`** (str)
   - The natural/business key column name. This uniquely identifies records in the source system.
   - **Required**: Yes.

2. **`surrogate_key`** (str)
   - Surrogate key column name, auto-generated as the primary key for the dimension table.
   - **Required**: Yes.

3. **`scd_type`** (int)
   - Supported values:
     - `0`: Static dimension (no updates).
     - `1`: Overwrite changes (no history retention).
     - `2`: Maintain full history (SCD2).
   - **Default**: 1.

4. **`track_cols`** (list)
   - Specifies which columns to monitor for changes (applies to SCD1 and SCD2).
   - **Required for SCD1, SCD2**: Yes.

5. **`target`** (str)
   - Path to the target table, used mainly for SCD2 to detect changes in historical data.
   - **Required for SCD2**: Yes.

6. **`unknown_member`** (bool)
   - If True, inserts a special row (`SK=0`) to handle orphan Foreign Keys.
   - **Default**: False.

7. **`audit`** (dict)
   - Configures audit columns:
     - **`load_timestamp`**: Adds time of data load.
     - **`source_system`**: Source system identifier.

---

## Supported Target Formats

### Spark:
- Catalog Tables: `catalog.schema.table`
- Delta, CSV, JSON, ORC, and Parquet file formats.

### Pandas:
- Supports an extended range:
  - `Parquet`, `CSV`, `JSON`, `Excel`, `Feather`, `Pickle`, and connection paths.

---

## Key Methods

### 1. `validate(self) -> None`
- Performs parameter validation before execution.
- Validates the following critical errors:
  - Missing required configurations such as `natural_key` and `surrogate_key`.
  - Invalid SCD type values (`scd_type`).
  - Missing `target` (for SCD2).

### 2. `execute(self, context: EngineContext) -> Any`
- Central method for executing the dimension pattern.
- Implements the following:
  - Differentiates among SCD0, SCD1, and SCD2.
  - Introduces audit columns (if enabled).
  - Handles unknown member row creation (`unknown_member=True`).

### 3. `_execute_scd0`
- Implements Static Dimension (No updates—only adds new rows).

### 4. `_execute_scd1`
- Applies SCD Type 1 by overwriting updated records.

### 5. `_execute_scd2`
- Leverages the `scd2` transformer to ensure change tracking with history maintenance.

---

## Non-Obvious Behaviors / Gotchas

1. **Audit Columns**:
   - The inclusion of `load_timestamp` and `source_system` directly depends on the `audit` dictionary. A missing `load_timestamp` key in `audit` defaults it to `True`.

2. **Target Loading Failures**:
   - Filesystem paths that are missing or have unsupported formats result in a silent fallback, where the pattern assumes no historical data exists.

3. **Special Handling for Pandas vs. Spark**:
   - Numerous conditional logic checks are included to support both processing engines. Users must ensure suitable data formats based on the target engine.

---

## YAML Example Configuration

```yaml
dimension_pattern:
  natural_key: "customer_id"
  surrogate_key: "customer_sk"
  scd_type: 2
  track_cols:
    - "email"
    - "address"
  target: "warehouse.dim_customer"
  unknown_member: true
  audit:
    load_timestamp: true
    source_system: "CRM"
```

---

## Summary
The `DimensionPattern` offers a robust and flexible way to manage dimension tables. By supporting SCD methodologies, audit tracking, and versatile data sources, it aligns with the needs of modern data warehousing efforts. Its design highlights ease of configuration and multi-engine compatibility but requires careful attention to configuration to avoid common pitfalls.

---

## Next Steps
Continue to analyze and document `fact.py` corresponding to Phase 2C: FACT PATTERN.
