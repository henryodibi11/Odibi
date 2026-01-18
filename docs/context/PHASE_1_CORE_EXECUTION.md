# PHASE 1: CORE EXECUTION ANALYSIS

## Overview
This phase focuses on understanding the core execution paths in the odibi codebase, covering the interactions between key components: `Pipeline`, `Node/NodeExecutor`, and `DependencyGraph`.

---

## Key Components and Their Execution Roles

### `Pipeline`

The `Pipeline` class acts as an orchestrator and context manager for executing nodes defined in a pipeline configuration. Key responsibilities include:
- **Initialization**: Sets up logging context, initializes the dependency graph via `DependencyGraph`, and prepares the engine.
- **Execution**: The `run()` method drives node execution in either serial or parallel mode using a topological sort of the nodes (produced by the dependency graph).
- **Artifacts**: Generates stories, alerts, intermediate results (`story_path`), and lineage data.
- **State Management**: Implements functions to resume from failure, manage catalog entries, and track metadata across runs.
- **Error Handling**: Adopts configurable error strategies like `FAIL_FAST` or `FAIL_LATER`.
- **Key Methods**:
  - `run()`: The main execution method that invokes nodes using either a serial or parallel strategy.
  - `flush_stories(timeout)`: Handles async story generation for added context visualization.
  - `_send_alerts()`: Sends alerts at the start, success, or failure of the pipeline.

---

### `Node` and `NodeExecutor`

#### Node
- **Role**: High-level container handling the orchestration of `NodeExecutor` and state management.
- **Restore**: Attempts to restore a node's state from previous runs using `restore()`, relying on catalogs and configurations.
- **Caching**: Utilizes results caching for optimized execution.

#### NodeExecutor
- **Role**: Executes individual node logic, breaking down execution into well-defined phases (`read`, `transform`, `validate`, etc.).
- **Execution Steps**:
  1. **Pre-SQL phase**:
     - Executes pre-defined SQL queries on the engine tied to the pipeline context (if any).
  2. **Read phase**:
     - Retrieves data either from the current pipeline or from other pipelines (via catalog references and dependency resolution).
     - Handles partitioned datasets, Delta files, and incremental filtering.
  3. **Transformation phase**:
     - Applies transformation logic, including Python functions and patterns (e.g., `merge`, `scd2`).
     - Patterns are implemented using `EngineContext` and `Pattern` classes.
  4. **Validation phase**:
     - Runs tests to ensure schema compliance and data validity.
  5. **Write phase**:
     - Writes results to specified storage and registers them in catalogs for cross-pipeline dependencies.
- **Feature Highlights**:
  - Incremental Execution: Filters data based on stateful or rolling-window strategies.
  - Metrics Logging: Tracks execution times, schema changes, and row count deltas for insights during debugging.
  - Retry Logic: Executes operations with configurable retry mechanisms and backoff strategies.
  - Quarantine Mechanism: Isolates invalid data during validation and logs warnings or errors.

---

### DependencyGraph

#### Role:
`DependencyGraph` ensures that pipeline nodes are executed in the correct order efficiently while supporting parallelism where possible.

#### Key Features:
1. **Validation**:
   - Detects missing dependencies and ensures all referenced nodes exist in the graph.
   - Identifies and prevents circular dependencies.

2. **Execution Planning**:
   - **Topological Sort**:
     Ensures that all dependencies of a node are executed before the node itself is processed.
   - **Execution Layers**:
     Groups nodes into parallelizable layers based on their dependencies.

3. **Analysis Helpers**:
   - `get_dependencies()`: Lists all direct and transitive dependencies for a node.
   - `get_dependents()`: Lists all direct and transitive dependent nodes.
   - `get_independent_nodes()`: Returns nodes with no dependencies.
   - `visualize()`: Generates a human-readable representation of the graph, helpful for debugging the execution plan.

---

## Non-Obvious Behaviors Identified

### `Pipeline`:
1. `resume_from_failure` relies on both context and the state manager. Missing configurations or corrupted states could cause silent failures or skipped operations.
2. Complex error strategies (`FAIL_FAST`) with potential edge cases when running in parallel mode.

### `Node`:
1. Input data resolution hybridizes current pipeline results and catalog-manager lookups for cross-pipeline inputs, supporting scenarios like result pre-registration or unallocated memory restoration.
2. `dry_run=True` is feature-rich for debugging purposes but does not validate cross-layer dependencies (e.g., incremental filters rely on actual runtime queries).

### `DependencyGraph`:
1. Cross-pipeline dependencies are supported using `$pipeline.node` references in a node's `inputs` block. These are visualized as external references but require catalog data.

---

## Observations
This phase reinforces the tightly coupled system design:
1. Strong guarantees against dependency violations (e.g., cycles, missing nodes).
2. Considerable investment in observability (through alerts, lineage registration, and logging).
3. Configurable failover mechanisms in both `Pipeline` and `NodeExecutor` increase reliability but require precise configurations.

---

## Next Steps
Transition to Phase 2 and analyze the behavioral intricacies within `patterns` to ensure parity between Panda, Spark, and Polars engines.
