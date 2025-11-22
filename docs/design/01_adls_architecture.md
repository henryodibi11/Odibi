# Architecture Design: Data Lake (ADLS Gen2)

**Status:** Approved  
**Date:** November 22, 2025  
**Author:** Founding Engineering Lead  

---

## 1. Overview

This document defines the physical and logical architecture for the Data Lake on Azure Data Lake Storage (ADLS) Gen2. The design follows a **"Delta-First"** approach, optimized for a lean engineering team ("One-Man Army") while providing enterprise-grade reliability, history, and isolation.

### Core Principles
1.  **Environment Isolation:** Separate storage accounts for Dev, QAT, and Prod to ensure security and blast radius containment.
2.  **Delta Lake Native:** Use Delta Lake format (`raw`, `silver`, `gold`) to guarantee ACID transactions, schema enforcement, and time travel.
3.  **Single Source of Truth:** Production `Raw` data is the authoritative source. QAT and Dev environments read from Prod Raw (read-only) or use small samples; they do not duplicate full raw datasets.
4.  **Automated Maintenance:** Use Azure Lifecycle Policies to manage transient data cleanup.

---

## 2. Physical Architecture

### 2.1 Storage Accounts
We utilize three distinct storage accounts. Each account contains exactly one container named `datalake`.

| Environment | Storage Account Name (Pattern) | Purpose |
| :--- | :--- | :--- |
| **Development** | `sa-goat-dev` | Sandbox for experimentation and unit testing. |
| **QAT** | `sa-goat-qat` | User Acceptance Testing (UAT) and Integration Testing. |
| **Production** | `sa-goat-prod` | The System of Record. Highly secured. |

### 2.2 Container Structure
Inside the `datalake` container of **each** storage account, the following top-level folders exist:

| Layer | Format | Retention | Purpose |
| :--- | :--- | :--- | :--- |
| **`landing/`** | Parquet / CSV / JSON | **30 Days** (Auto-Delete) | Transient "Loading Dock". Data arrives here "As-Is" from ADF. |
| **`raw/`** | **Delta Table** | **Permanent** | The "Archive". Cleaned, typed, immutable history of all ingestion. |
| **`silver/`** | **Delta Table** | **Permanent** | Enriched data. Joins, de-duplication, and business logic applied. |
| **`gold/`** | **Delta Table** | **Permanent** | Aggregated data. Dimensional models and KPIs ready for SQL/Power BI. |

---

## 3. Naming Convention (Taxonomy)

All paths and filenames must be **lowercase** and use **underscores** (`_`).

### 3.1 Landing Layer
**Path:** `landing/{source_system}/{technical_name}/ingestion_date=YYYY-MM-DD/`

*   `source_system`: Technical name of source (e.g., `sap_ecc`, `plant_x`, `sql_sales`).
*   `technical_name`: Original table or file name (e.g., `mara`, `sensor_log`).
*   `ingestion_date`: Partition key for easy traceability and partial loading.
*   **Files:** `part-*.parquet` (or `.csv` / `.json`). Let the engine name the files.

### 3.2 Raw Layer
**Path:** `raw/{source_system}/{technical_name}/`

*   **Format:** Delta Lake.
*   This folder contains the `_delta_log` and data files. It is queried as a table.
*   **Schema:** Matches source schema but with standardized types (e.g., proper timestamps) and metadata columns (`ingestion_timestamp`).

### 3.3 Silver Layer
**Path:** `silver/{domain}/{business_entity}/`

*   **Switch to Business Naming.**
*   `domain`: High-level business area (e.g., `supply_chain`, `finance`, `manufacturing`).
*   `business_entity`: Clear English name (e.g., `material_master`, `sales_order`).

### 3.4 Gold Layer
**Path:** `gold/{domain}/{report_or_model}/`

*   `report_or_model`: Specific use case (e.g., `daily_yield_kpi`, `dim_plant`).

---

## 4. Data Flows

### 4.1 Production Flow (The "Real" Data)
1.  **Ingest:** ADF Copy Activity moves data from Source -> `sa-goat-prod/.../landing` (Parquet).
2.  **Raw Load:** Odibi (Spark) reads `landing`, performs a **Delta MERGE** (Upsert) into `sa-goat-prod/.../raw`.
3.  **Refine:** Odibi (Spark) reads `raw`, transforms/cleans -> writes to `silver`.
4.  **Aggregate:** Odibi (Spark) reads `silver`, aggregates -> writes to `gold`.

### 4.2 QAT Flow (The "Test" Logic)
*   **Pre-requisite:** `sa-goat-qat` Managed Identity has **Storage Blob Data Reader** role on `sa-goat-prod`.
1.  **Read:** Odibi (Spark) in QAT reads `sa-goat-prod/.../raw` (Production Raw Data).
2.  **Write:** Odibi (Spark) writes to `sa-goat-qat/.../silver` (Test Results).
3.  **Verify:** Users/Power BI validate the data in `sa-goat-qat`.

### 4.3 Dev Flow (The "Sandbox")
1.  Developers upload sample files or run small ADF copies to `sa-goat-dev/.../landing`.
2.  All processing (Raw/Silver/Gold) happens in isolation within `sa-goat-dev`.

---

## 5. Lifecycle Management (Retention)

Lifecycle policies must be configured in the Azure Portal for each storage account.

**Rule Name:** `delete_landing_30_days`
*   **Scope:** Limit with filters.
*   **Prefix Match:** `datalake/landing/`
*   **Action:** Delete blob if base blob was last modified more than **30 days** ago.

---

## 6. Handling Mutating Data

To handle sources that update history (e.g., "Last 30 days of orders"):
1.  **Ingest:** ADF pulls the window (e.g., `WHERE date > TODAY - 30`).
2.  **Raw:** Odibi uses **Delta MERGE** to upsert these records into the Raw Delta table based on Primary Key.
    *   *Result:* Raw always reflects the latest known state of the source, with history preserved via Delta Time Travel.
