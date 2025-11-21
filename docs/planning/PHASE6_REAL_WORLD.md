# Phase 6: The "Real World" Showcase

**Goal:** Build a LinkedIn-worthy data engineering project using **real, public datasets** to demonstrate Odibi's capabilities in handling CSVs, APIs, and complex transformations.

**Theme:** **Global E-Commerce Analytics** (Simulating a Wayfair/Amazon competitor).
**Codename:** `OdibiStore`

---

## 1. The Real Datasets

We will combine static dumps with live API calls to create a heterogeneous data mesh.

### Source A: The Core Business (CSVs)
**Dataset:** [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
*Note: We will host a subset of this on GitHub/S3 for easy `wget` in scripts, or use the direct Kaggle API.*
*   **Volume:** ~100k Orders, 9 Tables.
*   **Complexity:** Real relational schema (Orders <-> Items <-> Products <-> Sellers <-> Geolocation).
*   **Real Mess:**
    *   Product category names are in Portuguese.
    *   Reviews are unstructured text.
    *   Timestamps (order_purchase, order_approved, order_delivered) have gaps.

### Source B: Macroeconomics (Live API)
**API:** [Frankfurter API](https://www.frankfurter.app/docs/) (Open Source, No Key required).
*   **Goal:** Convert historical BRL (Brazilian Real) order values to USD/EUR based on the *exact date* of the order.
*   **Format:** JSON.
*   **Challenge:** For 100k orders, we can't call the API 100k times. We must implement a **"Smart Fetch"** pipeline to download daily exchange rates for the 2-year period and cache them (Bronze -> Silver).

### Source C: Product Enrichment (Web Scrape / API)
**API:** [Open Food Facts](https://world.openfoodfacts.org/data) or similar product categorization APIs.
*   *Alternative:* Use a **Translation API** (LibreTranslate - Open) to translate category names from PT -> EN dynamically.

---

## 2. The Architecture

### 🥉 Bronze: Ingestion (The "Connector" Showcase)
1.  **`ingest_olist`**: Download/Read raw CSVs.
    *   *Showcase:* `read.format: csv`, `options: {header: true, quoteChar: '"'}`.
2.  **`ingest_rates`**: Python Script Node (or Custom Connector).
    *   *Logic:* Fetch exchange rates for `2016-01-01` to `2018-09-01`.
    *   *Showcase:* Using a custom `python` script within an Odibi pipeline to hit an API and save as JSON.

### 🥈 Silver: Cleaning & Stitching (The "Logic" Showcase)
1.  **`clean_orders`**:
    *   Cast timestamps.
    *   Calculate `delivery_delay_days` (Actual vs Estimated).
2.  **`enrich_currency`**:
    *   Join `orders` (BRL) with `daily_rates` (USD).
    *   Calculate `total_amount_usd`.
3.  **`translate_categories`**:
    *   Map `product_category_name` (PT) -> English.

### 🥇 Gold: Analytics (The "Business" Showcase)
1.  **`global_performance.parquet`**:
    *   Daily Revenue in USD.
    *   Top 10 Categories by Revenue.
2.  **`logistics_health.parquet`**:
    *   Heatmap of delivery delays by State (SP, RJ, etc.).
    *   Correlation between "Freight Value" and "Delivery Time".

---

## 3. The "Frustration Tracker"

We will rigorously document the developer experience in `UX_FEEDBACK.md`.

**Categories:**
*   **Config Friction:** "Why do I have to repeat this config 3 times?"
*   **Debugging Hell:** "The error message said 'Fail' but didn't say which row."
*   **Missing Batteries:** "I had to write custom Python because Odibi didn't have a `HTTP` connector."

---

## 4. Execution Plan

1.  **Setup:** Create `examples/real_world/`.
2.  **Data Fetch:** Write `scripts/download_data.sh` (or `.py`) to get the Olist CSVs.
3.  **Bronze:** Build the pipeline to ingest CSVs + hit the Currency API.
4.  **Silver:** Build the joins (Pandas Engine).
5.  **Gold:** Build the final aggregates.
6.  **Review:** Publish the Story and the UX Feedback.
