# Gap Analysis V7: Performance, Extensibility & Enterprise Readiness

**Date:** 2023-11-26
**Status:** Draft
**Focus:** Day 5 - Performance, Plugins & Governance

## 1. Executive Summary
Odibi has evolved into a robust orchestration framework ("Safe" & "Efficient"). However, it is currently "Closed" and "Local-First".
1.  **Closed:** Users cannot add custom *Connections* or *Engines* without forking the code.
2.  **Local-First:** The Pandas engine is memory-bound. It struggles with datasets larger than RAM.
3.  **Governance:** Lineage exists in "Stories" (HTML), but not in machine-readable standards (OpenLineage).

This phase focuses on **breaking the memory barrier**, **opening the plugin system**, and **standardizing lineage**.

---

## 2. Performance & Scalability (The "Memory" Wall)

### Gap 2.1: The Pandas Memory Limit
*   **Current State:** The `PandasEngine` reads full files into memory. While `chunksize` is supported in `read_csv`, it's not pervasive across transformers. Complex SQL on Pandas (`pandasql` or `duckdb` fallback) often materializes intermediate results.
*   **Problem:** Processing a 10GB CSV on a 8GB laptop crashes `odibi`.
*   **Recommendation:** Integrate **DuckDB** as a first-class citizen within the `PandasEngine` (or a dedicated `DuckDBEngine`). Use DuckDB for SQL transformations on local files *without* loading them entirely into Pandas memory first.

### Gap 2.2: Spark Optimization Exposure
*   **Current State:** Spark sessions are created with defaults. Users cannot tune `spark.sql.shuffle.partitions` or memory settings via `odibi.yaml`.
*   **Problem:** Spark jobs run inefficiently on small data (too many partitions) or crash on large data (OOM).
*   **Recommendation:** Expose a `spark_conf` block in `ProjectConfig` and `NodeConfig` to tune session parameters.

---

## 3. Extensibility (The "Ecosystem" Gap)

### Gap 3.1: Custom Connections
*   **Current State:** Connection types are hardcoded (`local`, `azure_blob`, `sql_server`, etc.) in `odibi/config.py` and `odibi/connections/factory.py`.
*   **Problem:** A user wanting to connect to **Snowflake** or **Postgres** must wait for an Odibi release or hack the core library.
*   **Recommendation:** Implement a **Plugin System** where users can register custom Connection classes in `plugins.py` and use them in `odibi.yaml`.

### Gap 3.2: Custom IO (Readers/Writers)
*   **Current State:** `read` and `write` formats are hardcoded enums.
*   **Problem:** Users cannot read from proprietary formats or specific API endpoints without writing a custom *Transformer* (which breaks the `read:` abstraction).
*   **Recommendation:** Allow registering custom Readers/Writers associated with formats.

---

## 4. Governance & Lineage (The "Enterprise" Gap)

### Gap 4.1: Locked Lineage
*   **Current State:** Lineage is visualized in `odibi graph` (CLI) and `odibi story` (HTML).
*   **Problem:** Enterprise Data Catalogs (DataHub, Amundsen, Marquez) cannot consume Odibi lineage.
*   **Recommendation:** Implement an **OpenLineage** emitter. This standard JSON format allows Odibi to push lineage events to any compatible backend.

---

## 5. Prioritized Roadmap (Day 5)

### P0: Extensibility (Unblocking Users)
1.  **Plugin Registry:** Create `odibi/plugins.py` infrastructure to register external `Connection` classes.
2.  **Dynamic Config:** Relax Pydantic validation to allow "custom" connection types if a plugin is registered.

### P1: Performance (DuckDB)
1.  **DuckDB Integration:** Enhance `PandasEngine` to use DuckDB for SQL steps (`transform: { sql: ... }`) directly on Parquet/CSV files where possible, bypassing Pandas loading.

### P2: Enterprise Integration
1.  **OpenLineage:** Create a basic `OpenLineageClient` that emits `START`, `COMPLETE`, and `FAIL` events with input/output datasets.
