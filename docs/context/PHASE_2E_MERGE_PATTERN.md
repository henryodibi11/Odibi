# PHASE 2E: MERGE PATTERN ANALYSIS

## Overview
The `MergePattern` in the Odibi framework enables streamlined upsert and merge operations for integrating changes into existing datasets. It supports flexible strategies for upserts, append-only operations, and matching row deletions, ensuring seamless dataset updates across both batch and streaming data sources.

---

## Configuration Parameters

### Required Parameters:
1. **`keys`** (list):
   - List of columns to match between source and target datasets for merge operations.
   - **Required**: Yes.
   - **Example**: `["id", "customer_id"]`.

2. **`target`** (str or `path`):
   - Specifies the target dataset (table name or file path).
   - **Required**: Yes.

---

### Optional Parameters:
1. **`strategy`** (str):
   - Defines the merge strategy.
     - **Available Strategies**:
       - **`upsert`**: Updates existing records and inserts new ones.
       - **`append_only`**: Only appends new records (ignores existing ones).
       - **`delete_match`**: Removes rows present both in the source and target.
   - **Default**: `upsert`.

---

## Supported Engines
The `MergePattern` works with both Spark and Pandas engines, adapting behavior accordingly to make use of respective utilities such as `count` or `len` for source and target data operations.

---

## Key Methods

### 1. `validate(self) -> None`
- Verifies that the required `keys` and `target` parameters are supplied.
- Validates configuration compatibility with the Merge transformer (`merge_transformer.MergeParams`).

### 2. `execute(self, context: EngineContext) -> Any`
- Implements the merge pattern by invoking the `merge` transformer with validated parameters:
   1. Extracts relevant parameters from `params`.
   2. Applies the Merge logic based on the provided strategy (`upsert`, `append_only`, etc.).
   3. Logs detailed execution metrics and errors.
- Returns the resulting DataFrame.

---

## Workflow

1. **Validation**:
   - Parameters such as `target` and `keys` are mandatory and validated before execution.
   - Ensures that configuration adheres to the rules established by the `merge` transformer.

2. **Strategy-Driven Execution**:
   - Depending on the `strategy`, the pattern adapts its behavior:
     - `upsert`: Combines updates and inserts.
     - `append_only`: Appends new records while leaving existing records untouched.
     - `delete_match`: Deletes rows that exist both in the source and target datasets.

3. **Compatibility**:
   - Accommodates both `target` and `path` fields to ensure broader compatibility across different usage scenarios.

---

## Example YAML Configuration

```yaml
pattern:
  type: merge
  params:
    keys: ["id"]
    target: "user_data"
    strategy: "upsert"
```

---

## Observations and Gotchas

1. **Target Configuration Compatibility**:
   - The presence of `target` or `path` is essential. Omitting this parameter results in a validation error.

2. **Invalid Keys**:
   - If the `keys` parameter is missing, validation will fail. Ensure that keys correctly map between source and target schema.

3. **Strategy Specifics**:
   - Each merge strategy must be clearly specified. The default behavior (`upsert`) may create unintended results if the desired strategy is not explicitly defined.

---

## Summary

The `MergePattern` provides a robust mechanism for performing upsert and merge operations in the Odibi framework. Its flexible configuration options and support for multiple engines make it an essential tool for managing dynamic datasets, especially in systems requiring regular updates or synchronization between source and target datasets.

---

## Next Steps
Continue to analyze and document the `date_dimension.py` file as part of Phase 2F: DATE DIMENSION PATTERN.
