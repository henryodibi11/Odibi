# Odibi Architecture Manifesto & Manual

**Version:** 1.0  
**Status:** Approved  
**Date:** November 22, 2025  
**Target Audience:** Founding Engineer & Future Team Members  

---

# Part 1: The Principles ("The Why")

These principles guide every technical decision we make. They are designed to allow a "One-Man Army" to operate at the scale of a full team.

## 1. Robots Remember, Humans Forget
*   **Principle:** Never write code to track "what files have I processed?".
*   **Implementation:** Use **Spark Structured Streaming** (`availableNow=True`) for ingestion.
*   **Why:** Spark Checkpoints are durable and automatic. Custom "State Tables" are fragile and manual. We use Streaming not for speed, but for **Bookkeeping**.

## 2. Raw is Sacred (The "Time Machine" Rule)
*   **Principle:** Never destroy the original data received from the source.
*   **Implementation:** The **Raw Layer** is Append-Only.
*   **Why:** If we mess up our De-duplication logic in Silver (and we will), we must be able to replay history from Raw. If we de-dup in Raw and delete data, we lose our safety net.
*   **Rule:** "If you break Silver, we rebuild. If you break Raw, we panic."

## 3. Rebuild the Bucket, Don't Patch the Hole
*   **Principle:** Do not try to use row-level updates (`MERGE`) to maintain Aggregates (Gold).
*   **Implementation:** **Dynamic Partition Overwrite**. Reprocess the entire time window (e.g., Month) and replace it.
*   **Why:** Patching aggregates (e.g., `Total + 10`) is prone to "Double Counting" bugs. Re-calculating (`SUM(Total)`) is always correct. Idempotency is king.

## 4. SQL is for Humans, ADLS is for Robots
*   **Principle:** Do not treat Azure SQL as a Data Lake.
*   **Implementation:**
    *   **ADLS:** Stores Everything (Landing, Raw, Silver, Gold).
    *   **SQL:** Stores **Gold Only** (plus select Silver Master Data).
*   **Why:** SQL Server is expensive and doesn't scale to Terabytes easily. ADLS is cheap and scales infinitely. SQL is strictly a "Serving Layer" for Power BI and Business Users.

## 5. No Duplication (The QAT Rule)
*   **Principle:** Do not copy Terabytes of data just to test a pipeline.
*   **Implementation:** QAT reads from **Production Raw** (Read-Only) and writes to **QAT Silver**.
*   **Why:** It saves money, but more importantly, it ensures we test against **Real Data** (with all its messiness), not a sanitized "Test Set" that hides bugs.

---

# Part 2: The Architecture Blueprints ("The How")

## 1. Physical Storage (ADLS Gen2)

We use three strictly isolated Storage Accounts to prevent "Blast Radius" accidents.

| Environment | Account Name | Purpose | Access |
| :--- | :--- | :--- | :--- |
| **Dev** | `sa-goat-dev` | Sandbox | Open |
| **QAT** | `sa-goat-qat` | Testing | Read-Only Access to `sa-goat-prod/raw` |
| **Prod** | `sa-goat-prod` | Truth | Locked Down |

### The Folder Structure (Inside `datalake` container)

| Layer | Format | Retention | Logic |
| :--- | :--- | :--- | :--- |
| **`landing/`** | Parquet / CSV | 30 Days | **Transient.** The loading dock. Data arrives here from ADF (Copy Activity). |
| **`raw/`** | Delta Table | Permanent | **Append-Only.** The history log. One-to-one copy of source, but with history. |
| **`silver/`** | Delta Table | Permanent | **Clean & Unique.** The "Current State". De-duplicated and Type-Cast. |
| **`gold/`** | Delta Table | Permanent | **Aggregated.** Star Schemas and KPIs ready for BI. |

---

## 2. SQL Architecture (Azure SQL)

We use Azure SQL as the high-performance serving layer for Power BI.

### Schemas
*   **`mart`**: **The BI Layer.** Star Schemas (`dim_customer`, `fact_sales`). Power BI connects here.
*   **`core`**: **Master Data.** Cleaned Silver data (`material_master`) used for lookups or ad-hoc queries.
*   **`ref`**: **Reference Data.** Manual mapping tables maintained by the business.
*   **`stg`**: **Loading Bay.** Transient tables used by Spark to push data.

### The High-Performance Sync Pattern
To move data from ADLS Gold -> SQL, we **do not** pull data back into Spark.
1.  **Spark:** Writes new/changed rows to `stg.my_table`. (Fast Bulk Write).
2.  **Spark:** Executes `MERGE INTO mart.my_table USING stg.my_table...` on SQL Server.
3.  **SQL Server:** Performs the heavy diff/join using its own indexes.

---

## 3. Data Flow Patterns

### Pattern A: Ingestion (Landing -> Raw)
*   **Engine:** Spark Structured Streaming (Auto Loader).
*   **Mode:** `trigger(availableNow=True)`.
*   **Logic:** Append-Only.
*   **Why:** Tracks new files automatically. Keeps a perfect history.

### Pattern B: Refinement (Raw -> Silver)
*   **Engine:** Spark Structured Streaming (`foreachBatch`).
*   **Logic:** **The Merge Transformer.**
    1.  Read micro-batch of new Raw data.
    2.  **De-duplicate** (Rank by timestamp).
    3.  **Merge** (Upsert) into Silver.
*   **Result:** Silver is always the "Latest Unique Version".

### Pattern C: Aggregation (Silver -> Gold)
*   **Engine:** Batch.
*   **Logic:** **Windowed Reprocess.**
    1.  Identify Window (e.g., "Last 30 Days").
    2.  Read Silver (filtered by window).
    3.  Aggregate.
    4.  Write to Gold (Mode: **Dynamic Partition Overwrite**).
*   **Result:** Self-healing aggregates. Late data in the last 30 days is automatically fixed next run.

---

# Part 3: The "One-Man Army" Tooling

We use **Odibi** to abstract away the boilerplate.

## The `merge` Transformer (Phase 2.1)
Instead of writing complex PySpark code for every table, we use YAML.

```yaml
- id: raw_to_silver
  kind: transform
  transformer: merge
  params:
    target: silver.orders
    keys: [order_id]
    strategy: upsert
    audit_cols:
      created_col: created_at
      updated_col: updated_at
```

*   **Spark:** Compiles to `foreachBatch` + `DeltaTable.merge()`.
*   **Pandas:** Compiles to `pd.merge()` + `overwrite`.

## The Safe Defaults
Odibi automatically sets `spark.sql.sources.partitionOverwriteMode = dynamic`.
*   **Why:** So you never accidentally wipe your entire history when reprocessing a single month.

---

# Part 4: Decision Log (FAQ)

### Why not duplicate Raw data to QAT?
Because `Raw` is immutable. Reading it from QAT is safe. Duplicating 10TB of data costs money and time, and leads to "Data Drift" where QAT data is stale. Testing against stale data hides bugs.

### Why not use Delta CDF (Change Data Feed)?
For now, it adds complexity (Version Tracking) for marginal gain. The "Windowed Reprocess" pattern (Pattern C) is robust and idempotent. We will revisit CDF if we hit performance bottlenecks on massive tables (>100M rows).

### Why "Append-Only" Raw?
Because storage is cheap, but data loss is expensive. If we Merge in Raw and make a mistake, the original data is gone. If we Append in Raw, we can always replay.
