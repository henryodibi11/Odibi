# Odibi "Real World" Showcase: Global E-Commerce

**Codename:** OdibiStore

This project demonstrates Odibi's capabilities using a real-world, relational e-commerce dataset. It simulates a data lakehouse architecture for a global retailer.

## 📂 The Data

1.  **Olist E-Commerce:** ~100k orders (expandable to 1M+) relational SQL dump (CSV).
    *   *Orders, Items, Products, Customers, Sellers, Payments, Reviews, Geolocation.*
2.  **Frankfurter API:** Historical currency exchange rates (JSON).
    *   *BRL to USD/EUR conversion.*

## 🏗 Architecture

### 1. Bronze Layer (Ingestion)
*   **Goal:** Ingest raw CSVs and JSON into the data lake (Parquet/Delta).
*   **Key Features:** Schema validation, Format conversion.

### 2. Silver Layer (Enrichment)
*   **Goal:** Clean, Join, and Enrich.
*   **Key Features:**
    *   **Identity Resolution:** Mapping Customers.
    *   **Currency Conversion:** Joining Orders with Daily Exchange Rates.
    *   **Translation:** Translating Product Categories (PT -> EN).

### 3. Gold Layer (Analytics)
*   **Goal:** Business Reporting.
*   **Key Reports:**
    *   `global_revenue`: Revenue in USD.
    *   `logistics_performance`: Delivery delays heatmap.

## 🚀 Getting Started

1.  **Setup Data:**
    ```bash
    python examples/real_world/scripts/setup_data.py
    ```
    *Note: You will need to download the Olist dataset from Kaggle manually if not present.*

2.  **Run Pipeline:**
    ```bash
    odibi run examples/real_world/config/odibi_store.yaml
    ```
