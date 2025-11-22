# Odibi Spec: The Merge Transformer (v1)

**Status:** Draft  
**Phase:** 2.1  
**Target:** Spark (Delta) & Pandas  

---

## 1. Problem Statement
Writing `foreachBatch` logic in PySpark for standard Upsert/Merge patterns is repetitive, error-prone, and requires deep knowledge of the Delta Lake API. Users need a "Configuration-Driven" way to handle stateful merges.

## 2. Solution
Introduce a built-in `transformer: merge` that abstracts the Merge Logic.

## 3. YAML Configuration

### Minimal Config (Upsert)
```yaml
- id: merge_orders
  kind: transform
  transformer: merge
  params:
    target: silver.orders    # Table Name or Path
    keys: [order_id]         # Join Keys
    strategy: upsert         # Default
```

### Full Config
```yaml
- id: merge_orders_complex
  kind: transform
  transformer: merge
  params:
    target: silver.orders
    keys: [order_id, plant_id]
    
    # Strategy Selection
    strategy: upsert         # Options: upsert, append_only, delete_match
    
    # Audit Columns (Auto-Injected)
    audit_cols:
      created_col: created_at
      updated_col: updated_at
      
    # Advanced: Conditional Logic (Overrides 'strategy')
    # matched:
    #   - condition: "source.status = 'CANCELLED'"
    #     action: delete
    #   - action: update
    # not_matched:
    #   - action: insert
```

## 4. Behavior Specification

### 4.1 Engine Agnostic
*   **Streaming Input:** The transformer automatically wraps the logic in `writeStream.foreachBatch(...)`.
*   **Batch Input:** The transformer executes the logic immediately on the DataFrame.

### 4.2 Spark Implementation (Delta)
*   Uses `DeltaTable.forName()` or `forPath()`.
*   Builds condition: `target.key1 = source.key1 AND ...`
*   **Upsert Strategy:**
    *   `whenMatchedUpdateAll()`
    *   `whenNotMatchedInsertAll()`
*   **Append Only:**
    *   `whenNotMatchedInsertAll()`
*   **Delete Match:**
    *   `whenMatchedDelete()`

### 4.3 Pandas Implementation
*   **Upsert:**
    *   Loads Target (Parquet/Delta).
    *   Performs `pd.merge(indicator=True)`.
    *   Updates existing rows.
    *   Appends new rows.
    *   Writes back (Overwrite).
    *   *Note: Not safe for concurrent writes, but acceptable for Local Dev.*

## 5. Audit Column Logic
If `audit_cols` is present:
*   **Insert:** Sets `created_at` and `updated_at` to `current_timestamp()`.
*   **Update:** Sets `updated_at` to `current_timestamp()`. `created_at` is untouched.

## 6. Future Scope (v2)
*   SCD Type 2 Support.
*   Partial Updates (Update specific columns only).
*   Soft Deletes (`is_deleted = true`).
