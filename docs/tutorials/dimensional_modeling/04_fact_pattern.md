# Fact Pattern Tutorial

In this tutorial, you'll learn how to use Odibi's `fact` pattern to build fact tables with automatic surrogate key lookups, orphan handling, grain validation, and measure calculations.

**What You'll Learn:**
- How surrogate key lookups work
- Orphan handling strategies
- Grain validation and deduplication
- Measure calculations
- Joining facts with dimensions

---

## Source Data

We'll use orders data that references customers and products by their natural keys:

**Source Data (orders.csv) - 30 rows:**

| order_id | customer_id | product_id | order_date | quantity | unit_price | status |
|----------|-------------|------------|------------|----------|------------|--------|
| ORD001 | C001 | P001 | 2024-01-15 | 1 | 1299.99 | completed |
| ORD002 | C001 | P002 | 2024-01-15 | 2 | 29.99 | completed |
| ORD003 | C002 | P003 | 2024-01-16 | 1 | 249.99 | completed |
| ORD004 | C003 | P004 | 2024-01-16 | 3 | 49.99 | completed |
| ORD005 | C004 | P005 | 2024-01-17 | 1 | 599.99 | completed |
| ORD006 | C005 | P006 | 2024-01-17 | 1 | 149.99 | completed |
| ORD007 | C006 | P007 | 2024-01-18 | 2 | 399.99 | completed |
| ORD008 | C007 | P008 | 2024-01-18 | 4 | 45.99 | completed |
| ORD009 | C008 | P009 | 2024-01-19 | 1 | 79.99 | completed |
| ORD010 | C009 | P010 | 2024-01-19 | 1 | 189.99 | completed |
| ORD011 | C010 | P001 | 2024-01-20 | 1 | 1299.99 | completed |
| ORD012 | C011 | P002 | 2024-01-20 | 5 | 29.99 | completed |
| ORD013 | C012 | P003 | 2024-01-21 | 2 | 249.99 | completed |
| ORD014 | C001 | P004 | 2024-01-21 | 1 | 49.99 | completed |
| ORD015 | C002 | P005 | 2024-01-22 | 1 | 599.99 | pending |
| ORD016 | C003 | P006 | 2024-01-22 | 2 | 149.99 | completed |
| ORD017 | C004 | P007 | 2024-01-23 | 1 | 399.99 | completed |
| ORD018 | C005 | P008 | 2024-01-23 | 3 | 45.99 | completed |
| ORD019 | C006 | P009 | 2024-01-24 | 2 | 79.99 | completed |
| ORD020 | C007 | P010 | 2024-01-24 | 1 | 189.99 | completed |
| ORD021 | C008 | P001 | 2024-01-25 | 1 | 1299.99 | completed |
| ORD022 | C009 | P002 | 2024-01-25 | 3 | 29.99 | completed |
| ORD023 | C010 | P003 | 2024-01-26 | 1 | 249.99 | cancelled |
| ORD024 | C011 | P004 | 2024-01-26 | 2 | 49.99 | completed |
| ORD025 | C012 | P005 | 2024-01-27 | 1 | 599.99 | completed |
| ORD026 | C001 | P006 | 2024-01-27 | 1 | 149.99 | completed |
| ORD027 | C002 | P007 | 2024-01-28 | 1 | 399.99 | completed |
| ORD028 | C003 | P008 | 2024-01-28 | 2 | 45.99 | completed |
| ORD029 | C004 | P009 | 2024-01-15 | 1 | 79.99 | completed |
| ORD030 | C005 | P010 | 2024-01-16 | 1 | 189.99 | completed |

---

## Dimension Tables (Pre-built)

Before building fact tables, we need dimension tables. Here are the dimensions from previous tutorials:

### dim_customer (13 rows including unknown)

| customer_sk | customer_id | name | region | is_current |
|-------------|-------------|------|--------|------------|
| 0 | -1 | Unknown | Unknown | true |
| 1 | C001 | Alice Johnson | North | true |
| 2 | C002 | Bob Smith | South | true |
| 3 | C003 | Carol White | North | true |
| 4 | C004 | David Brown | East | true |
| 5 | C005 | Emma Davis | West | true |
| 6 | C006 | Frank Miller | South | true |
| 7 | C007 | Grace Lee | East | true |
| 8 | C008 | Henry Wilson | West | true |
| 9 | C009 | Ivy Chen | North | true |
| 10 | C010 | Jack Taylor | South | true |
| 11 | C011 | Karen Martinez | East | true |
| 12 | C012 | Leo Anderson | West | true |

### dim_product (11 rows including unknown)

| product_sk | product_id | name | category |
|------------|------------|------|----------|
| 0 | -1 | Unknown | Unknown |
| 1 | P001 | Laptop Pro 15 | Electronics |
| 2 | P002 | Wireless Mouse | Electronics |
| 3 | P003 | Office Chair | Furniture |
| 4 | P004 | USB-C Hub | Electronics |
| 5 | P005 | Standing Desk | Furniture |
| 6 | P006 | Mechanical Keyboard | Electronics |
| 7 | P007 | Monitor 27" | Electronics |
| 8 | P008 | Desk Lamp | Furniture |
| 9 | P009 | Webcam HD | Electronics |
| 10 | P010 | Filing Cabinet | Furniture |

### dim_date (15 rows for Jan 15-28 + unknown)

| date_sk | full_date | day_of_week | month_name |
|---------|-----------|-------------|------------|
| 0 | 1900-01-01 | Unknown | Unknown |
| 20240115 | 2024-01-15 | Monday | January |
| 20240116 | 2024-01-16 | Tuesday | January |
| 20240117 | 2024-01-17 | Wednesday | January |
| 20240118 | 2024-01-18 | Thursday | January |
| 20240119 | 2024-01-19 | Friday | January |
| 20240120 | 2024-01-20 | Saturday | January |
| 20240121 | 2024-01-21 | Sunday | January |
| 20240122 | 2024-01-22 | Monday | January |
| 20240123 | 2024-01-23 | Tuesday | January |
| 20240124 | 2024-01-24 | Wednesday | January |
| 20240125 | 2024-01-25 | Thursday | January |
| 20240126 | 2024-01-26 | Friday | January |
| 20240127 | 2024-01-27 | Saturday | January |
| 20240128 | 2024-01-28 | Sunday | January |

---

## Step 1: Understanding SK Lookups

The fact pattern's main job is to **replace natural keys with surrogate keys**:

```
Source:     customer_id = "C001"
                    ↓
            Look up in dim_customer where customer_id = "C001"
                    ↓
Fact:       customer_sk = 1
```

This transformation happens for every dimension referenced.

### YAML Configuration

```yaml
project: fact_tutorial
engine: pandas

connections:
  source:
    type: file
    path: ./data
  warehouse:
    type: file
    path: ./warehouse

story:
  connection: warehouse
  path: stories

pipelines:
  - pipeline: build_fact_orders
    nodes:
      # First, load the dimension tables into context
      - name: dim_customer
        read:
          connection: warehouse
          path: dim_customer
          format: parquet
      
      - name: dim_product
        read:
          connection: warehouse
          path: dim_product
          format: parquet
      
      - name: dim_date
        read:
          connection: warehouse
          path: dim_date
          format: parquet
      
      # Then build the fact table
      - name: fact_orders
        depends_on: [dim_customer, dim_product, dim_date]
        read:
          connection: source
          path: orders.csv
          format: csv
        
        transformer: fact
        params:
          grain: [order_id]
          dimensions:
            - source_column: customer_id
              dimension_table: dim_customer
              dimension_key: customer_id
              surrogate_key: customer_sk
              scd2: true
            - source_column: product_id
              dimension_table: dim_product
              dimension_key: product_id
              surrogate_key: product_sk
            - source_column: order_date
              dimension_table: dim_date
              dimension_key: full_date
              surrogate_key: date_sk
          orphan_handling: unknown
          measures:
            - quantity
            - unit_price
            - line_total: "quantity * unit_price"
          audit:
            load_timestamp: true
            source_system: "pos"
        
        write:
          connection: warehouse
          path: fact_orders
          format: parquet
          mode: overwrite
```

### The Transformation

Here's what happens to the first 10 rows:

**Before (Source Data):**

| order_id | customer_id | product_id | order_date | quantity | unit_price |
|----------|-------------|------------|------------|----------|------------|
| ORD001 | C001 | P001 | 2024-01-15 | 1 | 1299.99 |
| ORD002 | C001 | P002 | 2024-01-15 | 2 | 29.99 |
| ORD003 | C002 | P003 | 2024-01-16 | 1 | 249.99 |
| ORD004 | C003 | P004 | 2024-01-16 | 3 | 49.99 |
| ORD005 | C004 | P005 | 2024-01-17 | 1 | 599.99 |
| ORD006 | C005 | P006 | 2024-01-17 | 1 | 149.99 |
| ORD007 | C006 | P007 | 2024-01-18 | 2 | 399.99 |
| ORD008 | C007 | P008 | 2024-01-18 | 4 | 45.99 |
| ORD009 | C008 | P009 | 2024-01-19 | 1 | 79.99 |
| ORD010 | C009 | P010 | 2024-01-19 | 1 | 189.99 |

**After (Fact Table):**

| order_id | customer_sk | product_sk | date_sk | quantity | unit_price | line_total | status | load_timestamp |
|----------|-------------|------------|---------|----------|------------|------------|--------|----------------|
| ORD001 | 1 | 1 | 20240115 | 1 | 1299.99 | 1299.99 | completed | 2024-01-30 10:00:00 |
| ORD002 | 1 | 2 | 20240115 | 2 | 29.99 | 59.98 | completed | 2024-01-30 10:00:00 |
| ORD003 | 2 | 3 | 20240116 | 1 | 249.99 | 249.99 | completed | 2024-01-30 10:00:00 |
| ORD004 | 3 | 4 | 20240116 | 3 | 49.99 | 149.97 | completed | 2024-01-30 10:00:00 |
| ORD005 | 4 | 5 | 20240117 | 1 | 599.99 | 599.99 | completed | 2024-01-30 10:00:00 |
| ORD006 | 5 | 6 | 20240117 | 1 | 149.99 | 149.99 | completed | 2024-01-30 10:00:00 |
| ORD007 | 6 | 7 | 20240118 | 2 | 399.99 | 799.98 | completed | 2024-01-30 10:00:00 |
| ORD008 | 7 | 8 | 20240118 | 4 | 45.99 | 183.96 | completed | 2024-01-30 10:00:00 |
| ORD009 | 8 | 9 | 20240119 | 1 | 79.99 | 79.99 | completed | 2024-01-30 10:00:00 |
| ORD010 | 9 | 10 | 20240119 | 1 | 189.99 | 189.99 | completed | 2024-01-30 10:00:00 |

**Key observations:**
- `customer_id = "C001"` → `customer_sk = 1`
- `product_id = "P001"` → `product_sk = 1`
- `order_date = "2024-01-15"` → `date_sk = 20240115`
- New column `line_total` calculated as `quantity * unit_price`
- Original natural keys can be dropped or kept (configurable)

---

## Step 2: Orphan Handling

What happens when a source record references a dimension value that doesn't exist?

**Source with Orphans (orders_with_orphans.csv) - 5 orphan rows:**

| order_id | customer_id | product_id | order_date | quantity | unit_price |
|----------|-------------|------------|------------|----------|------------|
| ... | ... | ... | ... | ... | ... |
| ORD031 | **C999** | P001 | 2024-01-17 | 1 | 1299.99 |
| ORD032 | **C888** | P002 | 2024-01-18 | 2 | 29.99 |
| ORD033 | **C777** | P003 | 2024-01-19 | 1 | 249.99 |
| ORD034 | **C666** | P004 | 2024-01-20 | 1 | 49.99 |
| ORD035 | **C555** | P005 | 2024-01-21 | 1 | 599.99 |

Customer IDs C999, C888, C777, C666, C555 don't exist in dim_customer.

### Strategy 1: Unknown (Default)

```yaml
params:
  orphan_handling: unknown
```

Orphans are assigned to the unknown member (SK = 0):

| order_id | customer_sk | product_sk | note |
|----------|-------------|------------|------|
| ORD031 | **0** | 1 | C999 not found → SK = 0 |
| ORD032 | **0** | 2 | C888 not found → SK = 0 |
| ORD033 | **0** | 3 | C777 not found → SK = 0 |
| ORD034 | **0** | 4 | C666 not found → SK = 0 |
| ORD035 | **0** | 5 | C555 not found → SK = 0 |

**Pros:** Data isn't lost, can investigate later
**Cons:** Need to ensure unknown member exists in dimension

### Strategy 2: Reject

```yaml
params:
  orphan_handling: reject
```

Pipeline fails with an error listing orphan values:

```
OrphanRecordError: Found 5 orphan records for dimension 'dim_customer':
  - customer_id='C999' (1 record)
  - customer_id='C888' (1 record)
  - customer_id='C777' (1 record)
  - customer_id='C666' (1 record)
  - customer_id='C555' (1 record)
```

**Pros:** Strict data quality enforcement
**Cons:** Entire load fails, need to fix source data

### Strategy 3: Quarantine

```yaml
params:
  orphan_handling: quarantine
```

Orphan records are routed to a separate quarantine table:

**fact_orders (valid records):** 30 rows

**fact_orders_quarantine (orphans):** 5 rows

| order_id | customer_id | orphan_reason |
|----------|-------------|---------------|
| ORD031 | C999 | customer_id not found in dim_customer |
| ORD032 | C888 | customer_id not found in dim_customer |
| ORD033 | C777 | customer_id not found in dim_customer |
| ORD034 | C666 | customer_id not found in dim_customer |
| ORD035 | C555 | customer_id not found in dim_customer |

**Pros:** Valid data loads, orphans are preserved for review
**Cons:** Need to manage quarantine table

---

## Step 3: Grain Validation

The **grain** defines what makes a fact row unique. For orders, it's typically `order_id`.

```yaml
params:
  grain: [order_id]
```

### What Happens with Duplicate Grain?

If your source has duplicate order IDs:

| order_id | customer_id | quantity |
|----------|-------------|----------|
| ORD001 | C001 | 1 |
| **ORD001** | C001 | 2 |  ← Duplicate!

The pattern detects this and raises an error:

```
GrainValidationError: Duplicate grain detected in fact_orders
  Grain columns: ['order_id']
  Duplicate count: 1
  Sample duplicates:
    - order_id='ORD001' (2 occurrences)
```

### Enabling Deduplication

If duplicates are expected and you want to keep the latest:

```yaml
params:
  grain: [order_id]
  deduplicate: true
  keys: [order_id]
```

This keeps only the last occurrence of each order_id.

---

## Step 4: Measure Calculations

Measures are the numeric values in your fact table. You can:
- **Pass through** existing columns
- **Rename** columns
- **Calculate** derived values

```yaml
params:
  measures:
    # Pass through
    - quantity
    - unit_price
    
    # Rename (maps status to order_status)
    - order_status: status
    
    # Calculate
    - line_total: "quantity * unit_price"
    - discount_amount: "unit_price * 0.1"
    - net_total: "quantity * unit_price * 0.9"
```

### Output with Calculated Measures

| order_id | quantity | unit_price | line_total | discount_amount | net_total |
|----------|----------|------------|------------|-----------------|-----------|
| ORD001 | 1 | 1299.99 | 1299.99 | 130.00 | 1169.99 |
| ORD002 | 2 | 29.99 | 59.98 | 3.00 | 53.98 |
| ORD003 | 1 | 249.99 | 249.99 | 25.00 | 224.99 |

---

## Complete Fact Table Output

Here's the complete fact_orders table (30 rows):

| order_id | customer_sk | product_sk | date_sk | quantity | unit_price | line_total | status | load_timestamp |
|----------|-------------|------------|---------|----------|------------|------------|--------|----------------|
| ORD001 | 1 | 1 | 20240115 | 1 | 1299.99 | 1299.99 | completed | 2024-01-30 10:00:00 |
| ORD002 | 1 | 2 | 20240115 | 2 | 29.99 | 59.98 | completed | 2024-01-30 10:00:00 |
| ORD003 | 2 | 3 | 20240116 | 1 | 249.99 | 249.99 | completed | 2024-01-30 10:00:00 |
| ORD004 | 3 | 4 | 20240116 | 3 | 49.99 | 149.97 | completed | 2024-01-30 10:00:00 |
| ORD005 | 4 | 5 | 20240117 | 1 | 599.99 | 599.99 | completed | 2024-01-30 10:00:00 |
| ORD006 | 5 | 6 | 20240117 | 1 | 149.99 | 149.99 | completed | 2024-01-30 10:00:00 |
| ORD007 | 6 | 7 | 20240118 | 2 | 399.99 | 799.98 | completed | 2024-01-30 10:00:00 |
| ORD008 | 7 | 8 | 20240118 | 4 | 45.99 | 183.96 | completed | 2024-01-30 10:00:00 |
| ORD009 | 8 | 9 | 20240119 | 1 | 79.99 | 79.99 | completed | 2024-01-30 10:00:00 |
| ORD010 | 9 | 10 | 20240119 | 1 | 189.99 | 189.99 | completed | 2024-01-30 10:00:00 |
| ORD011 | 10 | 1 | 20240120 | 1 | 1299.99 | 1299.99 | completed | 2024-01-30 10:00:00 |
| ORD012 | 11 | 2 | 20240120 | 5 | 29.99 | 149.95 | completed | 2024-01-30 10:00:00 |
| ORD013 | 12 | 3 | 20240121 | 2 | 249.99 | 499.98 | completed | 2024-01-30 10:00:00 |
| ORD014 | 1 | 4 | 20240121 | 1 | 49.99 | 49.99 | completed | 2024-01-30 10:00:00 |
| ORD015 | 2 | 5 | 20240122 | 1 | 599.99 | 599.99 | pending | 2024-01-30 10:00:00 |
| ORD016 | 3 | 6 | 20240122 | 2 | 149.99 | 299.98 | completed | 2024-01-30 10:00:00 |
| ORD017 | 4 | 7 | 20240123 | 1 | 399.99 | 399.99 | completed | 2024-01-30 10:00:00 |
| ORD018 | 5 | 8 | 20240123 | 3 | 45.99 | 137.97 | completed | 2024-01-30 10:00:00 |
| ORD019 | 6 | 9 | 20240124 | 2 | 79.99 | 159.98 | completed | 2024-01-30 10:00:00 |
| ORD020 | 7 | 10 | 20240124 | 1 | 189.99 | 189.99 | completed | 2024-01-30 10:00:00 |
| ORD021 | 8 | 1 | 20240125 | 1 | 1299.99 | 1299.99 | completed | 2024-01-30 10:00:00 |
| ORD022 | 9 | 2 | 20240125 | 3 | 29.99 | 89.97 | completed | 2024-01-30 10:00:00 |
| ORD023 | 10 | 3 | 20240126 | 1 | 249.99 | 249.99 | cancelled | 2024-01-30 10:00:00 |
| ORD024 | 11 | 4 | 20240126 | 2 | 49.99 | 99.98 | completed | 2024-01-30 10:00:00 |
| ORD025 | 12 | 5 | 20240127 | 1 | 599.99 | 599.99 | completed | 2024-01-30 10:00:00 |
| ORD026 | 1 | 6 | 20240127 | 1 | 149.99 | 149.99 | completed | 2024-01-30 10:00:00 |
| ORD027 | 2 | 7 | 20240128 | 1 | 399.99 | 399.99 | completed | 2024-01-30 10:00:00 |
| ORD028 | 3 | 8 | 20240128 | 2 | 45.99 | 91.98 | completed | 2024-01-30 10:00:00 |
| ORD029 | 4 | 9 | 20240115 | 1 | 79.99 | 79.99 | completed | 2024-01-30 10:00:00 |
| ORD030 | 5 | 10 | 20240116 | 1 | 189.99 | 189.99 | completed | 2024-01-30 10:00:00 |

---

## Complete Runnable Example

```yaml
# File: odibi_fact_tutorial.yaml
project: fact_tutorial
engine: pandas

connections:
  source:
    type: file
    path: ./examples/tutorials/dimensional_modeling/data
  warehouse:
    type: file
    path: ./warehouse

story:
  connection: warehouse
  path: stories

pipelines:
  - pipeline: build_fact_orders
    description: "Build fact table with SK lookups"
    nodes:
      # Load dimension tables
      - name: dim_customer
        read:
          connection: warehouse
          path: dim_customer
          format: parquet
      
      - name: dim_product
        read:
          connection: warehouse
          path: dim_product
          format: parquet
      
      - name: dim_date
        read:
          connection: warehouse
          path: dim_date
          format: parquet
      
      # Build fact table
      - name: fact_orders
        description: "Orders fact table with surrogate key lookups"
        depends_on: [dim_customer, dim_product, dim_date]
        read:
          connection: source
          path: orders.csv
          format: csv
        
        transformer: fact
        params:
          # Define the grain (uniqueness)
          grain: [order_id]
          
          # Define dimension lookups
          dimensions:
            - source_column: customer_id
              dimension_table: dim_customer
              dimension_key: customer_id
              surrogate_key: customer_sk
              scd2: true  # Filter to is_current = true
            
            - source_column: product_id
              dimension_table: dim_product
              dimension_key: product_id
              surrogate_key: product_sk
            
            - source_column: order_date
              dimension_table: dim_date
              dimension_key: full_date
              surrogate_key: date_sk
          
          # Handle missing dimension values
          orphan_handling: unknown
          
          # Define measures
          measures:
            - quantity
            - unit_price
            - line_total: "quantity * unit_price"
          
          # Add audit columns
          audit:
            load_timestamp: true
            source_system: "pos"
        
        write:
          connection: warehouse
          path: fact_orders
          format: parquet
          mode: overwrite
```

---

## Python API Alternative

```python
from odibi.patterns.fact import FactPattern
from odibi.context import EngineContext
from odibi.enums import EngineType
import pandas as pd

# Load source data
orders_df = pd.read_csv("examples/tutorials/dimensional_modeling/data/orders.csv")

# Load dimensions
dim_customer = pd.read_parquet("warehouse/dim_customer")
dim_product = pd.read_parquet("warehouse/dim_product")
dim_date = pd.read_parquet("warehouse/dim_date")

# Create context and register dimensions
context = EngineContext(df=orders_df, engine_type=EngineType.PANDAS)
context.register("dim_customer", dim_customer)
context.register("dim_product", dim_product)
context.register("dim_date", dim_date)

# Create pattern
pattern = FactPattern(params={
    "grain": ["order_id"],
    "dimensions": [
        {
            "source_column": "customer_id",
            "dimension_table": "dim_customer",
            "dimension_key": "customer_id",
            "surrogate_key": "customer_sk",
            "scd2": True
        },
        {
            "source_column": "product_id",
            "dimension_table": "dim_product",
            "dimension_key": "product_id",
            "surrogate_key": "product_sk"
        },
        {
            "source_column": "order_date",
            "dimension_table": "dim_date",
            "dimension_key": "full_date",
            "surrogate_key": "date_sk"
        }
    ],
    "orphan_handling": "unknown",
    "measures": [
        "quantity",
        "unit_price",
        {"line_total": "quantity * unit_price"}
    ],
    "audit": {
        "load_timestamp": True,
        "source_system": "pos"
    }
})

# Execute pattern
result_df = pattern.execute(context)

print(f"Generated {len(result_df)} fact rows")
print(result_df.head(10))
```

---

## Querying the Star Schema

Now you can run powerful analytical queries:

### "Sales by region"

```sql
SELECT 
    c.region,
    COUNT(*) AS order_count,
    SUM(f.line_total) AS total_revenue
FROM fact_orders f
JOIN dim_customer c ON f.customer_sk = c.customer_sk
WHERE c.is_current = true
GROUP BY c.region
ORDER BY total_revenue DESC;
```

| region | order_count | total_revenue |
|--------|-------------|---------------|
| North | 8 | 2,599.87 |
| East | 8 | 2,023.87 |
| South | 7 | 2,449.92 |
| West | 7 | 2,629.88 |

### "Daily sales trend"

```sql
SELECT 
    d.full_date,
    d.day_of_week,
    SUM(f.line_total) AS daily_revenue
FROM fact_orders f
JOIN dim_date d ON f.date_sk = d.date_sk
GROUP BY d.full_date, d.day_of_week
ORDER BY d.full_date;
```

### "Top products by category"

```sql
SELECT 
    p.category,
    p.name,
    SUM(f.quantity) AS units_sold,
    SUM(f.line_total) AS revenue
FROM fact_orders f
JOIN dim_product p ON f.product_sk = p.product_sk
GROUP BY p.category, p.name
ORDER BY revenue DESC;
```

---

## What You Learned

In this tutorial, you learned:

- **Surrogate key lookups** automatically replace natural keys with dimension SKs
- **scd2: true** filters to current dimension rows when looking up SCD2 dimensions
- **Orphan handling** strategies: unknown (default), reject, or quarantine
- **Grain validation** detects duplicate records at the grain level
- **Measures** can be passed through, renamed, or calculated
- **depends_on** ensures dimension tables are loaded before the fact pattern runs
- **Audit columns** track when and where data was loaded

---

## Next Steps

Now that you have a complete star schema, let's build aggregated tables for faster reporting.

**Next:** [Aggregation Pattern Tutorial](./05_aggregation_pattern.md)

---

## Navigation

| Previous | Up | Next |
|----------|----|----|
| [Date Dimension](./03_date_dimension_pattern.md) | [Tutorials](../getting_started.md) | [Aggregation Pattern](./05_aggregation_pattern.md) |

---

## Reference

For complete parameter documentation, see: [Fact Pattern Reference](../../patterns/fact.md)
