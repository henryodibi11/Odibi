# PHASE 5: RUNTIME ANALYSIS

## Overview
This phase delves into how runtime processes are managed by the Odibi framework. Central to the Odibi design are the `Context` objects which encapsulate execution environments for different data engines like Spark, Pandas, and Polars. These contexts allow seamless integration, ensuring runtime portability and efficient execution across diverse data processing backends.

---

## Key Components and Behavior Observations

### `EngineContext`
- **Role**:
  - Serves as an intermediary between the user’s logic and specific execution engines, such as Spark or Pandas.
  - Maintains local states like the current DataFrame and SQL history while integrating with the global execution context.
- **Key Features**:
  1. **SQL Interface**: Executes SQL queries on a DataFrame by registering it as a temporary view.
      - Spark compatibility enables distributed computation.
      - Thread-safe implementation ensures proper execution in parallel processing scenarios.
  2. **Schema Introspection**:
      - Provides schema insights via global context.
      - Supports issues like PII metadata tracking during runtime.
  3. **Flexibility**:
      - Supports various execution engines as backends (Spark, Pandas, Polars).
      - Permits chaining transformations by returning new contexts, complete with SQL history sharing.

#### Non-Obvious Behaviors:
1. **Temporary View Naming**:
    - A unique naming convention for temporary views is generated using thread and counter information to avoid collisions during parallelism.
2. **Flexible History Management**:
    - While users execute and chain multiple SQL queries (`sql()`), the queried history accumulates in the context instance.

---

### `Context` (Abstract Class)
- Provides a unified interface for managing datasets across transformations.
- Abstracts operations like dataset registration, metadata retrieval, and resource cleanup across diverse engines.
- Important Methods:
  - **register**: Adds a dataset and optional metadata.
  - **get**: Fetches a registered dataset or raises a `KeyError` if the dataset does not exist.
  - **unregister**: Removes datasets and optionally performs cleanup.
  - Implementations must define all abstract methods like `list_names`, `clear`, etc.

#### Observations:
1. By abstracting common, repetitive runtime tasks, `Context` helps reduce duplication across backend-specific implementations.
2. A base validation structure (e.g., `unregister`) improves resource management, avoiding accidental memory overhead.

---

### `PandasContext`, `PolarsContext`, and `SparkContext`
Handles runtime execution for their respective engines:
1. **`PandasContext`**:
    - Designed for lightweight, in-memory processing.
    - Supports native Pandas DataFrames and chunked computation using iterators.
    - Leverages DuckDB where needed for SQL operations.
2. **`PolarsContext`**:
    - Provides similar functionality tailored to Polars DataFrames.
    - Currently lacks extensive parallelism optimizations but maintains parity with PandasContext for functional compatibility.
3. **`SparkContext`**:
    - Built around Spark’s DataFrame API and Delta Lake architecture.
    - Includes strict validation for view names to maintain SQL compliance.
    - Implements thread-safe operations using locks.
    - Offers automatic handling of temporary views with explicit cleanup to avoid memory bloat.
    - Advanced SQL compatibility means more dynamic runtime capabilities compared to Pandas.

---

## Critical Runtime Insights

### Challenges and Considerations:
1. **Engine-Specific Designs**:
    - Different backends have their unique structures (Spark, Pandas, Polars).
    - A transparent `create_context()` ensures the correct context instantiates, but switching engines mid-run requires control to be refined further in client scripts.

2. **State Management**:
    - Purposive separation of runtime state for DataFrames enhances thread safety while maintaining lineage records and debugging transparency.

### Observations:
1. **Shared Abstractions, Divergent Implementations**:
    - The Panda and Polars contexts largely mirror each other, highlighting the community's shift towards supporting Apache Arrow-compatible tech. Polars may become more relevant for users as it scales better in single-node setups.

2. **Advanced Parallelism**:
    - Threading awareness in Spark/Pandas execution patterns ensures that workflows remain performant while maintaining data integrity.

---

## Next Steps
Proceed to Phase 6 to investigate workflows, which govern the orchestration of multiple nodes and how tasks are executed in parallel while ensuring lineage, validation, and error recovery features.
