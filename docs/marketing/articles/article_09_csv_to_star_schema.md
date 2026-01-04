# From CSV to Star Schema: Complete Walkthrough

*Building a complete data warehouse from raw files*

---

## TL;DR

This article ties everything together. We take 8 CSV files from the Brazilian E-Commerce dataset and build a complete star schema: Bronze (raw landing), Silver (cleaned data), and Gold (dimensional model). You'll get the complete configuration files, execution commands, and verification queries. By the end, you'll have a working data warehouse.

---

## What We're Building

Starting point: 8 CSV files from Kaggle

```
data/landing/
├── olist_orders_dataset.csv
├── olist_order_items_dataset.csv
├── olist_customers_dataset.csv
├── olist_products_dataset.csv
├── olist_sellers_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_order_reviews_dataset.csv
└── olist_geolocation_dataset.csv
```

End result: A star schema

```
┌─────────────────────────────────────────────────────────────┐
│                     GOLD LAYER                               │
│                                                              │
│    dim_date ─────┐                                          │
│                  │                                          │
│    dim_customer ─┼──── fact_order_items ──── dim_product   │
│                  │                                          │
│    dim_seller ───┘                                          │
│                                                              │
│    fact_orders ──── fact_payments ──── fact_reviews        │
└─────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
ecommerce_warehouse/
├── odibi.yaml              # Main configuration
├── data/
│   └── landing/            # Raw CSV files
├── bronze/                 # Bronze layer (Delta tables)
├── silver/                 # Silver layer (Delta tables)
├── gold/                   # Gold layer (Star schema)
└── logs/                   # Pipeline logs
```

---

## Architecture Diagram

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   LANDING   │     │   BRONZE    │     │   SILVER    │
│             │     │             │     │             │
│  8 CSV      │────▶│  8 Delta    │────▶│  8 Delta    │
│  files      │     │  tables     │     │  tables     │
│             │     │  (raw +     │     │  (cleaned,  │
│             │     │  metadata)  │     │  validated) │
└─────────────┘     └─────────────┘     └──────┬──────┘
                                               │
                                               ▼
                                        ┌─────────────┐
                                        │    GOLD     │
                                        │             │
                                        │  4 Dims     │
                                        │  4 Facts    │
                                        │  (star      │
                                        │  schema)    │
                                        └─────────────┘
```

---

## Complete Configuration

Here's the full `odibi.yaml`:

```yaml
project: "ecommerce_warehouse"
engine: "pandas"
version: "1.0.0"
description: "Brazilian E-Commerce Data Warehouse"
owner: "data-team"

# ─────────────────────────────────────────────────────────────
# CONNECTIONS
# ─────────────────────────────────────────────────────────────

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

# ─────────────────────────────────────────────────────────────
# SYSTEM CONFIGURATION
# ─────────────────────────────────────────────────────────────

story:
  connection: bronze
  path: "_stories"
  retention_days: 30

system:
  connection: bronze
  path: "_system"

# ─────────────────────────────────────────────────────────────
# PIPELINES
# ─────────────────────────────────────────────────────────────

pipelines:

  # ═══════════════════════════════════════════════════════════
  # BRONZE LAYER - Raw Data Landing
  # ═══════════════════════════════════════════════════════════
  
  - pipeline: bronze_ecommerce
    layer: bronze
    description: "Land raw e-commerce data from CSV files"
    
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

  # ═══════════════════════════════════════════════════════════
  # SILVER LAYER - Cleaned and Validated Data
  # ═══════════════════════════════════════════════════════════
  
  - pipeline: silver_ecommerce
    layer: silver
    description: "Cleaned and validated e-commerce data"
    
    nodes:
      - name: silver_orders
        description: "Validated order data"
        read:
          connection: bronze
          path: orders
          format: delta
        contracts:
          - type: not_null
            column: order_id
            severity: error
          - type: not_null
            column: customer_id
            severity: error
          - type: unique
            columns: [order_id]
            severity: error
        transformer: deduplicate
        params:
          keys: [order_id]
          order_by: _extracted_at DESC
        transform:
          steps:
            - function: clean_text
              params:
                columns: [order_status]
                case: lower
                trim: true
            - function: derive_columns
              params:
                columns:
                  order_purchase_date: "TO_DATE(order_purchase_timestamp)"
                  order_delivered_date: "TO_DATE(order_delivered_customer_date)"
                  order_estimated_date: "TO_DATE(order_estimated_delivery_date)"
            - sql: |
                SELECT
                  order_id,
                  customer_id,
                  order_status,
                  order_purchase_date,
                  order_delivered_date,
                  order_estimated_date,
                  DATEDIFF(order_delivered_date, order_purchase_date) as days_to_delivery,
                  CASE WHEN order_delivered_date > order_estimated_date THEN 1 ELSE 0 END as is_late,
                  _extracted_at
                FROM df
        write:
          connection: silver
          path: orders
          format: delta
          mode: overwrite

      - name: silver_order_items
        description: "Validated order line items"
        read:
          connection: bronze
          path: order_items
          format: delta
        contracts:
          - type: not_null
            column: order_id
            severity: error
          - type: not_null
            column: product_id
            severity: error
          - type: range
            column: price
            min: 0
            severity: error
        transformer: deduplicate
        params:
          keys: [order_id, order_item_id]
          order_by: _extracted_at DESC
        transform:
          steps:
            - function: cast_columns
              params:
                columns:
                  price: double
                  freight_value: double
            - function: derive_columns
              params:
                columns:
                  line_total: "price + freight_value"
            - sql: |
                SELECT
                  order_id,
                  order_item_id,
                  product_id,
                  seller_id,
                  price,
                  freight_value,
                  line_total,
                  _extracted_at
                FROM df
        write:
          connection: silver
          path: order_items
          format: delta
          mode: overwrite

      - name: silver_customers
        description: "Validated customer data"
        read:
          connection: bronze
          path: customers
          format: delta
        contracts:
          - type: not_null
            column: customer_id
            severity: error
          - type: unique
            columns: [customer_id]
            severity: error
        transformer: deduplicate
        params:
          keys: [customer_id]
          order_by: _extracted_at DESC
        transform:
          steps:
            - function: clean_text
              params:
                columns: [customer_city, customer_state]
                case: upper
                trim: true
            - sql: |
                SELECT
                  customer_id,
                  customer_unique_id,
                  customer_zip_code_prefix,
                  customer_city,
                  customer_state,
                  _extracted_at
                FROM df
        write:
          connection: silver
          path: customers
          format: delta
          mode: overwrite

      - name: silver_products
        description: "Validated product catalog"
        read:
          connection: bronze
          path: products
          format: delta
        contracts:
          - type: not_null
            column: product_id
            severity: error
          - type: unique
            columns: [product_id]
            severity: error
        transformer: deduplicate
        params:
          keys: [product_id]
          order_by: _extracted_at DESC
        transform:
          steps:
            - function: fill_nulls
              params:
                columns:
                  product_category_name: "unknown"
                  product_weight_g: 0
            - function: clean_text
              params:
                columns: [product_category_name]
                case: lower
                trim: true
            - sql: |
                SELECT
                  product_id,
                  product_category_name,
                  product_weight_g,
                  product_length_cm,
                  product_height_cm,
                  product_width_cm,
                  _extracted_at
                FROM df
        write:
          connection: silver
          path: products
          format: delta
          mode: overwrite

      - name: silver_sellers
        description: "Validated seller data"
        read:
          connection: bronze
          path: sellers
          format: delta
        contracts:
          - type: not_null
            column: seller_id
            severity: error
          - type: unique
            columns: [seller_id]
            severity: error
        transformer: deduplicate
        params:
          keys: [seller_id]
          order_by: _extracted_at DESC
        transform:
          steps:
            - function: clean_text
              params:
                columns: [seller_city, seller_state]
                case: upper
                trim: true
            - sql: |
                SELECT
                  seller_id,
                  seller_zip_code_prefix,
                  seller_city,
                  seller_state,
                  _extracted_at
                FROM df
        write:
          connection: silver
          path: sellers
          format: delta
          mode: overwrite

      - name: silver_payments
        description: "Validated payment data"
        read:
          connection: bronze
          path: payments
          format: delta
        contracts:
          - type: not_null
            column: order_id
            severity: error
          - type: range
            column: payment_value
            min: 0
            severity: error
        transform:
          steps:
            - function: clean_text
              params:
                columns: [payment_type]
                case: lower
                trim: true
            - sql: |
                SELECT
                  order_id,
                  payment_sequential,
                  payment_type,
                  payment_installments,
                  payment_value,
                  _extracted_at
                FROM df
        write:
          connection: silver
          path: payments
          format: delta
          mode: overwrite

      - name: silver_reviews
        description: "Validated review data"
        read:
          connection: bronze
          path: reviews
          format: delta
        contracts:
          - type: not_null
            column: review_id
            severity: error
          - type: range
            column: review_score
            min: 1
            max: 5
            severity: error
        transformer: deduplicate
        params:
          keys: [review_id]
          order_by: _extracted_at DESC
        transform:
          steps:
            - function: derive_columns
              params:
                columns:
                  review_date: "TO_DATE(review_creation_date)"
            - sql: |
                SELECT
                  review_id,
                  order_id,
                  review_score,
                  review_date,
                  _extracted_at
                FROM df
        write:
          connection: silver
          path: reviews
          format: delta
          mode: overwrite

  # ═══════════════════════════════════════════════════════════
  # GOLD LAYER - Dimensional Model (Star Schema)
  # ═══════════════════════════════════════════════════════════
  
  - pipeline: gold_dimensions
    layer: gold
    description: "Dimension tables for star schema"
    
    nodes:
      # ─────────────────────────────────────────────────────────
      # DATE DIMENSION
      # ─────────────────────────────────────────────────────────
      - name: dim_date
        description: "Date dimension 2016-2025"
        pattern:
          type: date_dimension
          params:
            start_date: "2016-01-01"
            end_date: "2025-12-31"
            date_key_format: "yyyyMMdd"
            fiscal_year_start_month: 1
            unknown_member: true
        write:
          connection: gold
          path: dim_date
          format: delta
          mode: overwrite

      # ─────────────────────────────────────────────────────────
      # CUSTOMER DIMENSION
      # ─────────────────────────────────────────────────────────
      - name: dim_customer
        description: "Customer dimension with surrogate key"
        depends_on: [silver_customers]
        read:
          connection: silver
          path: customers
          format: delta
        pattern:
          type: dimension
          params:
            natural_key: customer_id
            surrogate_key: customer_sk
            scd_type: 1
            unknown_member: true
            audit:
              load_timestamp: true
        transform:
          steps:
            - function: derive_columns
              params:
                columns:
                  customer_region: |
                    CASE 
                      WHEN customer_state IN ('SP', 'RJ', 'MG', 'ES') THEN 'Southeast'
                      WHEN customer_state IN ('PR', 'SC', 'RS') THEN 'South'
                      WHEN customer_state IN ('BA', 'SE', 'AL', 'PE', 'PB', 'RN', 'CE', 'PI', 'MA') THEN 'Northeast'
                      WHEN customer_state IN ('MT', 'MS', 'GO', 'DF') THEN 'Central-West'
                      ELSE 'North'
                    END
        write:
          connection: gold
          path: dim_customer
          format: delta
          mode: overwrite

      # ─────────────────────────────────────────────────────────
      # PRODUCT DIMENSION
      # ─────────────────────────────────────────────────────────
      - name: dim_product
        description: "Product dimension with surrogate key"
        depends_on: [silver_products]
        read:
          connection: silver
          path: products
          format: delta
        pattern:
          type: dimension
          params:
            natural_key: product_id
            surrogate_key: product_sk
            scd_type: 1
            unknown_member: true
            audit:
              load_timestamp: true
        write:
          connection: gold
          path: dim_product
          format: delta
          mode: overwrite

      # ─────────────────────────────────────────────────────────
      # SELLER DIMENSION
      # ─────────────────────────────────────────────────────────
      - name: dim_seller
        description: "Seller dimension with surrogate key"
        depends_on: [silver_sellers]
        read:
          connection: silver
          path: sellers
          format: delta
        pattern:
          type: dimension
          params:
            natural_key: seller_id
            surrogate_key: seller_sk
            scd_type: 1
            unknown_member: true
            audit:
              load_timestamp: true
        transform:
          steps:
            - function: derive_columns
              params:
                columns:
                  seller_region: |
                    CASE 
                      WHEN seller_state IN ('SP', 'RJ', 'MG', 'ES') THEN 'Southeast'
                      WHEN seller_state IN ('PR', 'SC', 'RS') THEN 'South'
                      WHEN seller_state IN ('BA', 'SE', 'AL', 'PE', 'PB', 'RN', 'CE', 'PI', 'MA') THEN 'Northeast'
                      WHEN seller_state IN ('MT', 'MS', 'GO', 'DF') THEN 'Central-West'
                      ELSE 'North'
                    END
        write:
          connection: gold
          path: dim_seller
          format: delta
          mode: overwrite

  - pipeline: gold_facts
    layer: gold
    description: "Fact tables for star schema"
    
    nodes:
      # ─────────────────────────────────────────────────────────
      # FACT ORDER ITEMS
      # ─────────────────────────────────────────────────────────
      - name: fact_order_items
        description: "Order line items fact table"
        depends_on:
          - silver_order_items
          - silver_orders
          - dim_customer
          - dim_product
          - dim_seller
          - dim_date
        inputs:
          order_items: $silver_ecommerce.silver_order_items
          orders: $silver_ecommerce.silver_orders
        transform:
          steps:
            - sql: |
                SELECT 
                  oi.order_id,
                  oi.order_item_id,
                  oi.product_id,
                  oi.seller_id,
                  o.customer_id,
                  o.order_purchase_date,
                  oi.price,
                  oi.freight_value,
                  oi.line_total
                FROM order_items oi
                JOIN orders o ON oi.order_id = o.order_id
        pattern:
          type: fact
          params:
            grain: [order_id, order_item_id]
            dimensions:
              - source_column: customer_id
                dimension_table: dim_customer
                dimension_key: customer_id
                surrogate_key: customer_sk
              - source_column: product_id
                dimension_table: dim_product
                dimension_key: product_id
                surrogate_key: product_sk
              - source_column: seller_id
                dimension_table: dim_seller
                dimension_key: seller_id
                surrogate_key: seller_sk
              - source_column: order_purchase_date
                dimension_table: dim_date
                dimension_key: full_date
                surrogate_key: date_sk
                target_column: order_date_sk
            orphan_handling: unknown
            measures:
              - item_count: "1"
              - price
              - freight_value
              - line_total
            audit:
              load_timestamp: true
        write:
          connection: gold
          path: fact_order_items
          format: delta
          mode: overwrite

      # ─────────────────────────────────────────────────────────
      # FACT ORDERS (Summary)
      # ─────────────────────────────────────────────────────────
      - name: fact_orders
        description: "Order summary fact table"
        depends_on:
          - silver_orders
          - dim_customer
          - dim_date
        read:
          connection: silver
          path: orders
          format: delta
        pattern:
          type: fact
          params:
            grain: [order_id]
            dimensions:
              - source_column: customer_id
                dimension_table: dim_customer
                dimension_key: customer_id
                surrogate_key: customer_sk
              - source_column: order_purchase_date
                dimension_table: dim_date
                dimension_key: full_date
                surrogate_key: date_sk
                target_column: order_date_sk
            orphan_handling: unknown
            measures:
              - order_count: "1"
              - is_late
              - days_to_delivery
            audit:
              load_timestamp: true
        write:
          connection: gold
          path: fact_orders
          format: delta
          mode: overwrite

      # ─────────────────────────────────────────────────────────
      # FACT REVIEWS
      # ─────────────────────────────────────────────────────────
      - name: fact_reviews
        description: "Review fact table"
        depends_on:
          - silver_reviews
          - dim_date
        read:
          connection: silver
          path: reviews
          format: delta
        pattern:
          type: fact
          params:
            grain: [review_id]
            dimensions:
              - source_column: review_date
                dimension_table: dim_date
                dimension_key: full_date
                surrogate_key: date_sk
                target_column: review_date_sk
            orphan_handling: unknown
            measures:
              - review_count: "1"
              - review_score
            audit:
              load_timestamp: true
        write:
          connection: gold
          path: fact_reviews
          format: delta
          mode: overwrite
```

---

## Running the Full Pipeline

Execute in order:

```bash
# 1. Bronze Layer
odibi run odibi.yaml --pipeline bronze_ecommerce

# 2. Silver Layer
odibi run odibi.yaml --pipeline silver_ecommerce

# 3. Gold Dimensions (must run before facts)
odibi run odibi.yaml --pipeline gold_dimensions

# 4. Gold Facts
odibi run odibi.yaml --pipeline gold_facts
```

Or run everything:

```bash
odibi run odibi.yaml
```

Expected output:

```
[INFO] Starting pipeline: bronze_ecommerce
[INFO]   Completed 8 nodes in 12.3s
[INFO] Starting pipeline: silver_ecommerce
[INFO]   Completed 7 nodes in 8.7s
[INFO] Starting pipeline: gold_dimensions
[INFO]   Completed 4 nodes in 3.2s
[INFO] Starting pipeline: gold_facts
[INFO]   Completed 3 nodes in 5.1s
[INFO] All pipelines completed successfully
```

---

## Results

After running all pipelines:

### Bronze Layer

| Table | Rows | Description |
|-------|------|-------------|
| bronze.orders | 99,441 | Raw orders + metadata |
| bronze.order_items | 112,650 | Raw line items + metadata |
| bronze.customers | 99,441 | Raw customers + metadata |
| bronze.products | 32,951 | Raw products + metadata |
| bronze.sellers | 3,095 | Raw sellers + metadata |
| bronze.payments | 103,886 | Raw payments + metadata |
| bronze.reviews | 100,000 | Raw reviews + metadata |
| bronze.geolocation | 1,000,163 | Raw geo data + metadata |

### Silver Layer

| Table | Rows | Key Transformations |
|-------|------|---------------------|
| silver.orders | 99,441 | Dates parsed, delivery metrics calculated |
| silver.order_items | 112,650 | Prices validated, line_total added |
| silver.customers | 99,441 | Text standardized |
| silver.products | 32,951 | Nulls filled, categories cleaned |
| silver.sellers | 3,095 | Text standardized |
| silver.payments | 103,886 | Types standardized |
| silver.reviews | 100,000 | Dates parsed, scores validated |

### Gold Layer

| Table | Rows | Description |
|-------|------|-------------|
| dim_date | 3,653 | 10 years of dates + unknown |
| dim_customer | 99,442 | Customers + unknown member |
| dim_product | 32,952 | Products + unknown member |
| dim_seller | 3,096 | Sellers + unknown member |
| fact_order_items | 112,650 | Line items with SKs |
| fact_orders | 99,441 | Orders with SKs |
| fact_reviews | 100,000 | Reviews with SKs |

---

## Query Examples

Now you can run star schema queries:

### Revenue by Region and Month

```sql
SELECT 
  dc.customer_region,
  dd.month_name,
  dd.year,
  SUM(f.line_total) as revenue,
  COUNT(*) as order_count
FROM gold.fact_order_items f
JOIN gold.dim_customer dc ON f.customer_sk = dc.customer_sk
JOIN gold.dim_date dd ON f.order_date_sk = dd.date_sk
WHERE dd.year = 2018
GROUP BY dc.customer_region, dd.month_name, dd.year
ORDER BY dc.customer_region, dd.year, dd.month
```

### Top Products by Revenue

```sql
SELECT 
  dp.product_category_name,
  SUM(f.line_total) as revenue,
  SUM(f.item_count) as items_sold
FROM gold.fact_order_items f
JOIN gold.dim_product dp ON f.product_sk = dp.product_sk
GROUP BY dp.product_category_name
ORDER BY revenue DESC
LIMIT 10
```

### Late Delivery Analysis

```sql
SELECT 
  dc.customer_region,
  SUM(f.is_late) as late_orders,
  SUM(f.order_count) as total_orders,
  ROUND(100.0 * SUM(f.is_late) / SUM(f.order_count), 2) as late_pct
FROM gold.fact_orders f
JOIN gold.dim_customer dc ON f.customer_sk = dc.customer_sk
GROUP BY dc.customer_region
ORDER BY late_pct DESC
```

### Review Score Distribution

```sql
SELECT 
  review_score,
  SUM(review_count) as count,
  ROUND(100.0 * SUM(review_count) / (SELECT SUM(review_count) FROM gold.fact_reviews), 2) as pct
FROM gold.fact_reviews
GROUP BY review_score
ORDER BY review_score
```

---

## What We Built

```
LANDING (8 CSV files, ~1.5M rows)
    │
    ▼
BRONZE (8 Delta tables, raw + metadata)
    │
    ▼
SILVER (8 Delta tables, cleaned + validated)
    │
    ▼
GOLD (Star Schema)
    ├── dim_date (3,653 rows)
    ├── dim_customer (99,442 rows)
    ├── dim_product (32,952 rows)
    ├── dim_seller (3,096 rows)
    ├── fact_order_items (112,650 rows)
    ├── fact_orders (99,441 rows)
    └── fact_reviews (100,000 rows)
```

---

## Key Takeaways

1. **Layers serve different purposes** - Bronze preserves, Silver cleans, Gold models
2. **Configuration is documentation** - YAML describes the entire pipeline
3. **Patterns reduce code** - Dimension, Fact, Date patterns are declarative
4. **Dependencies matter** - Dimensions before facts
5. **Validation catches problems early** - Contracts stop bad data

---

## Next Steps

With a complete star schema, you can:

1. Connect BI tools (Power BI, Tableau)
2. Build aggregation tables for dashboards
3. Add more facts (payments, inventory snapshots)
4. Implement incremental loading
5. Add a semantic layer for metrics

Next article: **Introducing Odibi** (Article 10, already complete)

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]
- **LinkedIn:** Follow for more articles in this series
