# Complete Silver Layer Configuration Guide

*From raw Bronze to clean, validated, analysis-ready data*

---

## TL;DR

The Silver layer is where data gets cleaned, deduplicated, type-casted, and validated. This article provides complete configurations for all 8 tables in our Brazilian E-Commerce dataset. We cover deduplication, null handling, text cleaning, date parsing, and data contracts. By the end, you'll have a production-ready Silver layer.

---

## What Silver Does

Silver is the Single Source of Truth (SSOT) for your organization. It's where:

| Task | Why |
|------|-----|
| **Deduplication** | Remove duplicate records from source systems |
| **Type casting** | Convert strings to proper dates, numbers, etc. |
| **Null handling** | Replace nulls with defaults or quarantine |
| **Text cleaning** | Trim whitespace, standardize case |
| **Validation** | Ensure data meets business rules |
| **Standardization** | Consistent formats across all tables |

Bronze is your backup. Silver is what everyone queries.

---

## Project Structure

We continue from the Bronze layer setup:

```
ecommerce_warehouse/
├── odibi.yaml              # Main config
├── data/landing/           # Raw CSVs
├── bronze/                 # Bronze layer (done)
├── silver/                 # Silver layer (this article)
└── gold/                   # Gold layer (next)
```

---

## Common Transformation Patterns

Before we configure each table, let's review the patterns we'll use:

### Deduplication

Source systems often send duplicates. Remove them:

```yaml
transformer: deduplicate
params:
  keys: [order_id]
  order_by: _extracted_at DESC  # Keep latest
```

### Type Casting

Bronze infers types. Silver enforces them:

```yaml
transform:
  steps:
    - function: cast_columns
      params:
        columns:
          price: double
          quantity: integer
          order_date: date
```

### Null Handling

Replace nulls with sensible defaults:

```yaml
transform:
  steps:
    - function: fill_nulls
      params:
        columns:
          review_score: 0
          review_comment: ""
```

### Text Cleaning

Standardize text fields:

```yaml
transform:
  steps:
    - function: clean_text
      params:
        columns: [customer_city, customer_state]
        case: upper
        trim: true
```

### Date Parsing

Convert timestamp strings to proper dates:

```yaml
transform:
  steps:
    - function: derive_columns
      params:
        columns:
          order_date: "TO_DATE(order_purchase_timestamp)"
          delivery_date: "TO_DATE(order_delivered_customer_date)"
```

---

## Silver Layer Configuration

Now let's configure each table.

### 1. Silver Orders

The orders table is central to everything. It needs careful cleaning:

```yaml
- name: silver_orders
  description: "Cleaned order headers"
  
  read:
    connection: bronze
    path: orders
    format: delta
  
  # Pre-validation contracts
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
    
    - type: accepted_values
      column: order_status
      values: [created, approved, invoiced, processing, shipped, delivered, canceled, unavailable]
      severity: error
  
  # Deduplicate first
  transformer: deduplicate
  params:
    keys: [order_id]
    order_by: _extracted_at DESC
  
  # Then clean and transform
  transform:
    steps:
      # Standardize status
      - function: clean_text
        params:
          columns: [order_status]
          case: lower
          trim: true
      
      # Parse timestamps to dates
      - function: derive_columns
        params:
          columns:
            order_purchase_date: "TO_DATE(order_purchase_timestamp)"
            order_approved_date: "TO_DATE(order_approved_at)"
            order_delivered_carrier_date: "TO_DATE(order_delivered_carrier_date)"
            order_delivered_customer_date: "TO_DATE(order_delivered_customer_date)"
            order_estimated_delivery_date: "TO_DATE(order_estimated_delivery_date)"
      
      # Calculate derived metrics
      - function: derive_columns
        params:
          columns:
            days_to_delivery: "DATEDIFF(order_delivered_customer_date, order_purchase_date)"
            delivery_delay_days: "DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date)"
            is_late: "CASE WHEN order_delivered_customer_date > order_estimated_delivery_date THEN true ELSE false END"
      
      # Select final columns
      - sql: |
          SELECT
            order_id,
            customer_id,
            order_status,
            order_purchase_date,
            order_approved_date,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date,
            days_to_delivery,
            delivery_delay_days,
            is_late,
            _extracted_at,
            _source_file
          FROM df
  
  write:
    connection: silver
    path: orders
    format: delta
    mode: overwrite
```

### 2. Silver Order Items

Line items need price validation and seller linkage:

```yaml
- name: silver_order_items
  description: "Cleaned order line items"
  
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
    
    - type: not_null
      column: seller_id
      severity: error
    
    - type: range
      column: price
      min: 0
      severity: error
    
    - type: range
      column: freight_value
      min: 0
      severity: error
  
  transformer: deduplicate
  params:
    keys: [order_id, order_item_id]
    order_by: _extracted_at DESC
  
  transform:
    steps:
      # Cast numeric columns
      - function: cast_columns
        params:
          columns:
            price: double
            freight_value: double
            order_item_id: integer
      
      # Calculate line total
      - function: derive_columns
        params:
          columns:
            line_total: "price + freight_value"
      
      # Select final columns
      - sql: |
          SELECT
            order_id,
            order_item_id,
            product_id,
            seller_id,
            shipping_limit_date,
            price,
            freight_value,
            line_total,
            _extracted_at,
            _source_file
          FROM df
  
  write:
    connection: silver
    path: order_items
    format: delta
    mode: overwrite
```

### 3. Silver Customers

Customers need text standardization for geolocation matching:

```yaml
- name: silver_customers
  description: "Cleaned customer data"
  
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
    
    - type: not_null
      column: customer_zip_code_prefix
      severity: warn
  
  transformer: deduplicate
  params:
    keys: [customer_id]
    order_by: _extracted_at DESC
  
  transform:
    steps:
      # Standardize location text
      - function: clean_text
        params:
          columns: [customer_city]
          case: upper
          trim: true
      
      - function: clean_text
        params:
          columns: [customer_state]
          case: upper
          trim: true
      
      # Pad zip code if needed
      - function: derive_columns
        params:
          columns:
            customer_zip_code_prefix: "LPAD(CAST(customer_zip_code_prefix AS STRING), 5, '0')"
      
      # Select final columns
      - sql: |
          SELECT
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state,
            _extracted_at,
            _source_file
          FROM df
  
  write:
    connection: silver
    path: customers
    format: delta
    mode: overwrite
```

### 4. Silver Products

Products need weight and dimension validation:

```yaml
- name: silver_products
  description: "Cleaned product catalog"
  
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
    
    - type: range
      column: product_weight_g
      min: 0
      max: 100000
      severity: warn
  
  transformer: deduplicate
  params:
    keys: [product_id]
    order_by: _extracted_at DESC
  
  transform:
    steps:
      # Cast dimensions
      - function: cast_columns
        params:
          columns:
            product_weight_g: double
            product_length_cm: double
            product_height_cm: double
            product_width_cm: double
            product_photos_qty: integer
      
      # Fill nulls for dimensions
      - function: fill_nulls
        params:
          columns:
            product_weight_g: 0.0
            product_length_cm: 0.0
            product_height_cm: 0.0
            product_width_cm: 0.0
            product_photos_qty: 0
      
      # Calculate volume
      - function: derive_columns
        params:
          columns:
            product_volume_cm3: "product_length_cm * product_height_cm * product_width_cm"
      
      # Clean category name
      - function: clean_text
        params:
          columns: [product_category_name]
          case: lower
          trim: true
      
      # Fill null category
      - function: fill_nulls
        params:
          columns:
            product_category_name: "unknown"
      
      # Select final columns
      - sql: |
          SELECT
            product_id,
            product_category_name,
            product_name_length,
            product_description_length,
            product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm,
            product_volume_cm3,
            _extracted_at,
            _source_file
          FROM df
  
  write:
    connection: silver
    path: products
    format: delta
    mode: overwrite
```

### 5. Silver Sellers

Sellers need location standardization:

```yaml
- name: silver_sellers
  description: "Cleaned seller data"
  
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
      # Standardize location
      - function: clean_text
        params:
          columns: [seller_city]
          case: upper
          trim: true
      
      - function: clean_text
        params:
          columns: [seller_state]
          case: upper
          trim: true
      
      # Pad zip code
      - function: derive_columns
        params:
          columns:
            seller_zip_code_prefix: "LPAD(CAST(seller_zip_code_prefix AS STRING), 5, '0')"
      
      # Select final columns
      - sql: |
          SELECT
            seller_id,
            seller_zip_code_prefix,
            seller_city,
            seller_state,
            _extracted_at,
            _source_file
          FROM df
  
  write:
    connection: silver
    path: sellers
    format: delta
    mode: overwrite
```

### 6. Silver Payments

Payments need type validation and amount checks:

```yaml
- name: silver_payments
  description: "Cleaned payment data"
  
  read:
    connection: bronze
    path: payments
    format: delta
  
  contracts:
    - type: not_null
      column: order_id
      severity: error
    
    - type: accepted_values
      column: payment_type
      values: [credit_card, boleto, voucher, debit_card, not_defined]
      severity: error
    
    - type: range
      column: payment_value
      min: 0
      severity: error
  
  # Note: No dedupe on order_id - orders can have multiple payments
  
  transform:
    steps:
      # Cast amounts
      - function: cast_columns
        params:
          columns:
            payment_sequential: integer
            payment_installments: integer
            payment_value: double
      
      # Standardize payment type
      - function: clean_text
        params:
          columns: [payment_type]
          case: lower
          trim: true
      
      # Select final columns
      - sql: |
          SELECT
            order_id,
            payment_sequential,
            payment_type,
            payment_installments,
            payment_value,
            _extracted_at,
            _source_file
          FROM df
  
  write:
    connection: silver
    path: payments
    format: delta
    mode: overwrite
```

### 7. Silver Reviews

Reviews need score validation and text cleanup:

```yaml
- name: silver_reviews
  description: "Cleaned review data"
  
  read:
    connection: bronze
    path: reviews
    format: delta
  
  contracts:
    - type: not_null
      column: review_id
      severity: error
    
    - type: not_null
      column: order_id
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
      # Cast score
      - function: cast_columns
        params:
          columns:
            review_score: integer
      
      # Parse dates
      - function: derive_columns
        params:
          columns:
            review_creation_date: "TO_DATE(review_creation_date)"
            review_answer_timestamp: "TO_TIMESTAMP(review_answer_timestamp)"
      
      # Clean text fields (but preserve content)
      - function: fill_nulls
        params:
          columns:
            review_comment_title: ""
            review_comment_message: ""
      
      # Flag reviews with comments
      - function: derive_columns
        params:
          columns:
            has_comment: "CASE WHEN LENGTH(review_comment_message) > 0 THEN true ELSE false END"
      
      # Select final columns
      - sql: |
          SELECT
            review_id,
            order_id,
            review_score,
            review_comment_title,
            review_comment_message,
            has_comment,
            review_creation_date,
            review_answer_timestamp,
            _extracted_at,
            _source_file
          FROM df
  
  write:
    connection: silver
    path: reviews
    format: delta
    mode: overwrite
```

### 8. Silver Geolocation

Geolocation needs careful deduplication (multiple entries per zip):

```yaml
- name: silver_geolocation
  description: "Cleaned geolocation data"
  
  read:
    connection: bronze
    path: geolocation
    format: delta
  
  contracts:
    - type: not_null
      column: geolocation_zip_code_prefix
      severity: error
  
  # Aggregate to one row per zip code
  transform:
    steps:
      # Cast coordinates
      - function: cast_columns
        params:
          columns:
            geolocation_lat: double
            geolocation_lng: double
      
      # Standardize location text
      - function: clean_text
        params:
          columns: [geolocation_city]
          case: upper
          trim: true
      
      - function: clean_text
        params:
          columns: [geolocation_state]
          case: upper
          trim: true
      
      # Pad zip code
      - function: derive_columns
        params:
          columns:
            geolocation_zip_code_prefix: "LPAD(CAST(geolocation_zip_code_prefix AS STRING), 5, '0')"
      
      # Aggregate to one record per zip (take average coordinates)
      - sql: |
          SELECT
            geolocation_zip_code_prefix,
            AVG(geolocation_lat) as geolocation_lat,
            AVG(geolocation_lng) as geolocation_lng,
            FIRST(geolocation_city) as geolocation_city,
            FIRST(geolocation_state) as geolocation_state,
            COUNT(*) as source_record_count
          FROM df
          GROUP BY geolocation_zip_code_prefix
  
  write:
    connection: silver
    path: geolocation
    format: delta
    mode: overwrite
```

---

## Complete Pipeline Configuration

Here's the complete Silver pipeline:

```yaml
pipelines:
  - pipeline: silver_ecommerce
    layer: silver
    description: "Cleaned and validated e-commerce data"
    
    nodes:
      # Include all 8 node configurations from above
      - name: silver_orders
        # ... (config from above)
      
      - name: silver_order_items
        # ... (config from above)
      
      - name: silver_customers
        # ... (config from above)
      
      - name: silver_products
        # ... (config from above)
      
      - name: silver_sellers
        # ... (config from above)
      
      - name: silver_payments
        # ... (config from above)
      
      - name: silver_reviews
        # ... (config from above)
      
      - name: silver_geolocation
        # ... (config from above)
```

---

## Running the Pipeline

```bash
# Run Silver pipeline
odibi run odibi.yaml --pipeline silver_ecommerce

# Or run a specific node
odibi run odibi.yaml --node silver_orders
```

Expected output:

```
[INFO] Starting pipeline: silver_ecommerce
[INFO] Executing node: silver_orders
[INFO]   Contracts passed: 4/4
[INFO]   Deduplicated: 99,441 → 99,441 rows (0 duplicates)
[INFO]   Transformed: Added 3 derived columns
[INFO]   Written to silver/orders
[INFO] Executing node: silver_order_items
...
[INFO] Pipeline silver_ecommerce completed successfully
```

---

## Before and After

Let's see what Silver accomplished:

### Orders

| Metric | Bronze | Silver |
|--------|--------|--------|
| Rows | 99,441 | 99,441 |
| Columns | 10 | 13 |
| Duplicates | Unknown | 0 (verified) |
| Date format | String | DATE |
| Derived columns | 0 | 3 (days_to_delivery, etc.) |

### Products

| Metric | Bronze | Silver |
|--------|--------|--------|
| Rows | 32,951 | 32,951 |
| Null weights | 610 | 0 (filled with 0) |
| Category format | Mixed case | Lowercase |
| Volume column | No | Yes (calculated) |

### Geolocation

| Metric | Bronze | Silver |
|--------|--------|--------|
| Rows | 1,000,163 | ~19,000 |
| Duplicates | Many per zip | One per zip |
| Coordinates | Raw | Averaged |

---

## Key Takeaways

1. **Contracts first, transforms second** - Validate before processing
2. **Deduplicate early** - Remove duplicates before calculations
3. **Cast types explicitly** - Don't rely on inference
4. **Standardize text** - Consistent case, trimmed whitespace
5. **Fill nulls intentionally** - Document defaults
6. **Calculate derived columns** - Silver is where business logic lives

---

## Next Steps

With a clean Silver layer, we're ready for the Gold layer:

1. Build dimension tables (Customers, Products, Sellers, Dates)
2. Build fact tables (Orders, Order Items)
3. Create aggregations for reporting

That's the next article: **Facts vs Dimensions: A Practical Guide**.

---

## Connect

- **LinkedIn:** [Your LinkedIn URL]
- **GitHub:** [Odibi Repository URL]
- **Medium:** Follow for more articles in this series
