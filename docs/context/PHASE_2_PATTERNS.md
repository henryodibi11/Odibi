# PHASE 2: PATTERNS ANALYSIS

## Overview
This phase explores the `patterns` module in the odibi framework. It focuses on the functionality offered by the module, particularly the `AggregationPattern` defined in `aggregation.py`. This pattern provides standardized and efficient aggregation capabilities, including time-grain rollups, incremental merges, and audit column addition, across multiple execution engines (Spark and Pandas).

---

## Key Component and Behavior Observations

### AggregationPattern Class

#### Key Features:
1. **Declarative Aggregation**:
    - Users define `grain` (grouping columns) and `measures` (aggregations).
    - SQL-like expressions (e.g., "SUM(amount)") facilitate seamless aggregation.

2. **Incremental Aggregation**:
    - Handles the merging of new data with existing aggregations, customizable via `merge_strategy` (`replace`, `sum`, `min`, or `max`).

3. **Time Rollups**:
    - Adds support for hierarchical grain levels (e.g., daily, weekly, or monthly summaries derived from timestamp grains).

4. **Audit Columns**:
    - Supports additional metadata columns (e.g., `load_timestamp`, `source_system`).

---

## High-Level Workflow

1. **Validation**:
    - Validates grain, measures, and optional incremental configuration.
    - Ensures required parameters exist and have valid data types.
    - Issues warnings such as undefined merge strategies or missing grain/measure definitions.

2. **Execution**:
    - Steps:
      1. Retrieve source DataFrame.
      2. Perform aggregation using engine-specific logic:
          - **Spark**: Executes aggregation using PySpark with `groupBy` and aggregation expressions.
          - **Pandas**: Executes aggregation via DuckDB SQL queries on in-memory DataFrames.
      3. Apply incremental logic (if configured), merging with existing aggregated results.
      4. Add audit columns like `load_timestamp` or `source_system`.

---

## Engine-Specific Implementations

### Spark Aggregation:
- Uses `PySpark` APIs for loading, grouping, and aggregating data.
- Example:
  ```python
  result = df.groupBy(*grain_cols).agg(*agg_exprs)
  ```
- `HAVING` clauses are applied using Spark's `filter` API.

### Pandas Aggregation:
- Utilizes DuckDB integration through `context.sql()` to run fast in-process SQL queries.
- Example SQL:
  ```sql
  SELECT date_sk, product_sk, SUM(total_amount) AS total_revenue
  FROM df
  GROUP BY date_sk, product_sk
  ```

---

## Non-Obvious Behaviors

1. **Incremental Aggregation Import Paths**:
   - Handles cross-file-target resolution through context connections (`engine.connections`) and supports path resolution for various storage types (e.g., Delta, Parquet, CSV).

2. **Exception-Friendly Row Count**:
   - Fails non-critically when determining row counts for unsupported engines (`try/except` block ensures fallback execution).

3. **Grain-Aware Merge Strategies**:
   - Merge behaviors (replace/sum/min/max) are abstracted with separate implementations for Spark and Pandas.

4. **Dynamic SQL-HAVING Clauses**:
   - Expressions in `having` clauses are crafted dynamically using user configurations.
   - Pandas implementation fallback relies on DuckDB.

---

## Observations

1. **Engine Parity and Time Optimization**:
   - Strong adherence to the "Engine Parity" principle across Pandas/DuckDB and PySpark.
   - Efficient time optimizations include:
     - Hierarchical roll-ups for time-series data.
     - Incremental merges to reuse prior results as base tables.

2. **Complexity in Validation**:
   - Multiple validation rules exist for grain/measure combinations that require careful configuration to avoid runtime failures during incremental merge logic.

3. **Flexible Audit Support**:
   - `audit_config` allows for operational observability with minimal metadata cost (only when needed).

---

## Next Steps
Transition to Phase 3 and analyze the transformations within the `transformers` module to understand intermediate and advanced data manipulation patterns.
