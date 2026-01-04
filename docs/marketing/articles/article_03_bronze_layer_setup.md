# Setting Up a Bronze Layer with Delta Lake

*A complete walkthrough using the Brazilian E-Commerce dataset*

---

## TL;DR

This article walks through setting up a Bronze layer for 8 CSV files using Delta Lake. We'll configure connections, create nodes for each file, add extraction metadata, and run the pipeline. By the end, you'll have a complete Bronze layer ready for Silver processing.

---

## What We're Building

We're using the [Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) from Kaggle. It contains:

| File | Description | Rows |
|------|-------------|------|
| olist_orders_dataset.csv | Order headers | ~100k |
| olist_order_items_dataset.csv | Line items | ~113k |
| olist_customers_dataset.csv | Customer info | ~100k |
| olist_products_dataset.csv | Product catalog | ~33k |
| olist_sellers_dataset.csv | Seller info | ~3k |
| olist_order_payments_dataset.csv | Payment details | ~104k |
| olist_order_reviews_dataset.csv | Customer reviews | ~100k |
| olist_geolocation_dataset.csv | Zip code data | ~1M |

Our goal: Land all of these in Bronze, exactly as-is, with metadata for debugging.

---

## Project Structure

```
ecommerce_warehouse/
├── odibi.yaml              # Main config
├── data/
│   └── landing/            # Raw CSV files go here
├── bronze/                 # Bronze layer output
├── silver/                 # Silver layer output (later)
└── gold/                   # Gold layer output (later)
```

---

## Step 1: Download the Dataset

1. Go to [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
2. Download and extract to `data/landing/`
3. You should have 8 CSV files

---

## Step 2: Configure Connections

First, we define where data lives:

```yaml
# odibi.yaml

project: "ecommerce_warehouse"
engine: "pandas"  # Use pandas for local dev, spark for production

connections:
  # Where raw CSVs are
  landing:
    type: local
    base_path: "./data/landing"
  
  # Where Bronze layer goes
  bronze:
    type: local
    base_path: "./bronze"
  
  # Where Silver layer goes (for later)
  silver:
    type: local
    base_path: "./silver"
  
  # Where Gold layer goes (for later)
  gold:
    type: local
    base_path: "./gold"
```

For production, you'd use cloud connections:

```yaml
connections:
  landing:
    type: azure_blob
    account: ${STORAGE_ACCOUNT}
    container: landing
    credential: ${STORAGE_KEY}
  
  bronze:
    type: azure_blob
    account: ${STORAGE_ACCOUNT}
    container: bronze
    credential: ${STORAGE_KEY}
```

---

## Step 3: Create Bronze Nodes

Each source file gets its own node. The pattern is identical for all:

1. Read from landing
2. Add extraction metadata
3. Write to Bronze as Delta

### Orders Node

```yaml
pipelines:
  - pipeline: bronze_ecommerce
    layer: bronze
    description: "Land raw e-commerce data"
    
    nodes:
      - name: bronze_orders
        description: "Raw order headers"
        
        read:
          connection: landing
          path: olist_orders_dataset.csv
          format: csv
          options:
            header: true
            inferSchema: true
        
        transform:
          steps:
            - function: derive_columns
              params:
                columns:
                  _extracted_at: "current_timestamp()"
                  _source_file: "'olist_orders_dataset.csv'"
        
        write:
          connection: bronze
          path: orders
          format: delta
          mode: append
```

### All Other Nodes

The same pattern repeats. Here's the complete Bronze pipeline:

```yaml
pipelines:
  - pipeline: bronze_ecommerce
    layer: bronze
    description: "Land raw e-commerce data from Kaggle dataset"
    
    nodes:
      # Orders
      - name: bronze_orders
        description: "Raw order headers"
        read:
          connection: landing
          path: olist_orders_dataset.csv
          format: csv
          options:
            header: true
            inferSchema: true
        transform:
          steps:
            - function: derive_columns
              params:
                columns:
                  _extracted_at: "current_timestamp()"
                  _source_file: "'olist_orders_dataset.csv'"
        write:
          connection: bronze
          path: orders
          format: delta
          mode: append

      # Order Items
      - name: bronze_order_items
        description: "Raw order line items"
        read:
          connection: landing
          path: olist_order_items_dataset.csv
          format: csv
          options:
            header: true
            inferSchema: true
        transform:
          steps:
            - function: derive_columns
              params:
                columns:
                  _extracted_at: "current_timestamp()"
                  _source_file: "'olist_order_items_dataset.csv'"
        write:
          connection: bronze
          path: order_items
          format: delta
          mode: append

      # Customers
      - name: bronze_customers
        description: "Raw customer data"
        read:
          connection: landing
          path: olist_customers_dataset.csv
          format: csv
          options:
            header: true
            inferSchema: true
        transform:
          steps:
            - function: derive_columns
              params:
                columns:
                  _extracted_at: "current_timestamp()"
                  _source_file: "'olist_customers_dataset.csv'"
        write:
          connection: bronze
          path: customers
          format: delta
          mode: append

      # Products
      - name: bronze_products
        description: "Raw product catalog"
        read:
          connection: landing
          path: olist_products_dataset.csv
          format: csv
          options:
            header: true
            inferSchema: true
        transform:
          steps:
            - function: derive_columns
              params:
                columns:
                  _extracted_at: "current_timestamp()"
                  _source_file: "'olist_products_dataset.csv'"
        write:
          connection: bronze
          path: products
          format: delta
          mode: append

      # Sellers
      - name: bronze_sellers
        description: "Raw seller data"
        read:
          connection: landing
          path: olist_sellers_dataset.csv
          format: csv
          options:
            header: true
            inferSchema: true
        transform:
          steps:
            - function: derive_columns
              params:
                columns:
                  _extracted_at: "current_timestamp()"
                  _source_file: "'olist_sellers_dataset.csv'"
        write:
          connection: bronze
          path: sellers
          format: delta
          mode: append

      # Payments
      - name: bronze_payments
        description: "Raw payment data"
        read:
          connection: landing
          path: olist_order_payments_dataset.csv
          format: csv
          options:
            header: true
            inferSchema: true
        transform:
          steps:
            - function: derive_columns
              params:
                columns:
                  _extracted_at: "current_timestamp()"
                  _source_file: "'olist_order_payments_dataset.csv'"
        write:
          connection: bronze
          path: payments
          format: delta
          mode: append

      # Reviews
      - name: bronze_reviews
        description: "Raw review data"
        read:
          connection: landing
          path: olist_order_reviews_dataset.csv
          format: csv
          options:
            header: true
            inferSchema: true
        transform:
          steps:
            - function: derive_columns
              params:
                columns:
                  _extracted_at: "current_timestamp()"
                  _source_file: "'olist_order_reviews_dataset.csv'"
        write:
          connection: bronze
          path: reviews
          format: delta
          mode: append

      # Geolocation
      - name: bronze_geolocation
        description: "Raw geolocation data"
        read:
          connection: landing
          path: olist_geolocation_dataset.csv
          format: csv
          options:
            header: true
            inferSchema: true
        transform:
          steps:
            - function: derive_columns
              params:
                columns:
                  _extracted_at: "current_timestamp()"
                  _source_file: "'olist_geolocation_dataset.csv'"
        write:
          connection: bronze
          path: geolocation
          format: delta
          mode: append
```

---

## Step 4: Run the Pipeline

```bash
# Install odibi if you haven't
pip install odibi

# Run the bronze pipeline
odibi run odibi.yaml --pipeline bronze_ecommerce
```

You should see output like:

```
[INFO] Starting pipeline: bronze_ecommerce
[INFO] Executing node: bronze_orders
[INFO]   Read 99,441 rows from olist_orders_dataset.csv
[INFO]   Added metadata columns
[INFO]   Written to bronze/orders (delta format)
[INFO] Executing node: bronze_order_items
[INFO]   Read 112,650 rows from olist_order_items_dataset.csv
...
[INFO] Pipeline bronze_ecommerce completed successfully
```

---

## Step 5: Verify the Results

Check that Delta tables were created:

```python
import pandas as pd
from deltalake import DeltaTable

# Read a Bronze table
dt = DeltaTable("./bronze/orders")
df = dt.to_pandas()

print(f"Rows: {len(df)}")
print(f"Columns: {df.columns.tolist()}")
print(df.head())
```

You should see:
- All original columns from the CSV
- `_extracted_at` with the load timestamp
- `_source_file` with the source filename

---

## Why Delta Lake?

We write to Delta format instead of CSV or Parquet because:

### 1. ACID Transactions
Delta Lake provides atomic writes. Either the whole write succeeds or nothing changes. No partial files.

### 2. Time Travel
Delta keeps history. You can query previous versions:

```python
# Read version from 5 writes ago
dt = DeltaTable("./bronze/orders")
df = dt.load_as_version(5).to_pandas()
```

### 3. Schema Enforcement
Delta validates schema on write. If source columns change, you'll know.

### 4. Efficient Updates
When we get to Silver, Delta enables efficient upserts and deletes.

---

## What We Achieved

After running this pipeline:

| Table | Rows | Columns |
|-------|------|---------|
| bronze.orders | 99,441 | 10 (8 + 2 metadata) |
| bronze.order_items | 112,650 | 9 (7 + 2 metadata) |
| bronze.customers | 99,441 | 7 (5 + 2 metadata) |
| bronze.products | 32,951 | 11 (9 + 2 metadata) |
| bronze.sellers | 3,095 | 6 (4 + 2 metadata) |
| bronze.payments | 103,886 | 7 (5 + 2 metadata) |
| bronze.reviews | 100,000 | 7 (5 + 2 metadata) |
| bronze.geolocation | 1,000,163 | 7 (5 + 2 metadata) |

Total: **~1.5 million rows** landed in Bronze, with full metadata and Delta Lake durability.

---

## Key Principles Followed

1. **No transformations in Bronze** - Data is stored exactly as received
2. **Metadata columns added** - `_extracted_at` and `_source_file` for debugging
3. **Append mode** - Each run adds to history, never overwrites
4. **Delta format** - ACID guarantees, time travel, schema enforcement
5. **One node per source** - Clear lineage and easier debugging

---

## Next Steps

With Bronze complete, we're ready for Silver:

- Deduplicate records
- Cast types
- Validate data quality
- Standardize formats

That's the next article.

---

## Complete Configuration File

Here's the full `odibi.yaml` for reference:

```yaml
project: "ecommerce_warehouse"
engine: "pandas"
version: "1.0.0"
description: "Brazilian E-Commerce Data Warehouse"

connections:
  landing:
    type: local
    base_path: "./data/landing"
  
  bronze:
    type: local
    base_path: "./bronze"
  
  silver:
    type: local
    base_path: "./silver"
  
  gold:
    type: local
    base_path: "./gold"

story:
  connection: bronze
  path: "_stories"
  retention_days: 30

system:
  connection: bronze
  path: "_system"

pipelines:
  - pipeline: bronze_ecommerce
    layer: bronze
    description: "Land raw e-commerce data from Kaggle dataset"
    
    nodes:
      # ... (all 8 nodes from above)
```

---

*Next article: Building the Silver Layer - cleaning, validating, and deduplicating our Bronze data.*

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]
- **LinkedIn:** Follow for more articles in this series
