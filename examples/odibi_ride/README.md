# OdibiRide: High Volume Urban Mobility

**Codename:** OdibiRide
**Goal:** Stress test the "Big Data" capabilities of Odibi (Partitioning, Chunking, Performance).

## 📂 The Data
**Dataset:** [NYC Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
We will use the "Yellow Taxi" data.

## 🏗 Architecture

### 1. Bronze (Ingestion)
*   **Goal:** Ingest raw Parquet files.
*   **Key Features:** `glob` pattern matching (e.g., `yellow_tripdata_*.parquet`).

### 2. Silver (Cleaning)
*   **Goal:** Filter invalid trips.
*   **Rules:**
    *   `passenger_count` > 0
    *   `trip_distance` > 0
    *   `total_amount` > 0
*   **Enrichment:**
    *   Calculate `trip_duration_minutes`.
    *   Extract `pickup_hour`.

### 3. Gold (Aggregates)
*   **Goal:** Zone Analytics.
*   **Outputs:**
    *   `zone_stats.parquet`: Avg fare & duration by Pickup Zone.
    *   `hourly_demand.parquet`: Trip count by Hour of Day.

## 🚀 Execution
1.  Setup Data: `python examples/odibi_ride/scripts/setup_data.py`
2.  Run Pipeline: `odibi run examples/odibi_ride/config/pipeline.yaml`
