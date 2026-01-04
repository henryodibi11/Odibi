# LinkedIn Campaign - Phase 2: Building a Data Warehouse in Public

**Duration:** Weeks 3-10 (24 posts)
**Goal:** Walk through Bronze → Silver → Gold using Brazilian E-Commerce dataset
**Tone:** Teaching, humble, showing real work
**Dataset:** [Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---

## Week 3: Introduction & Bronze Layer

### Post 3.1 - Meet the Dataset (Monday)

**Hook:**
100,000 orders. 8 CSV files. Let's build a data warehouse.

**Body:**
I'm using the Brazilian E-Commerce dataset from Kaggle.

It has everything we need:
- 📦 Orders (100k transactions)
- 👥 Customers (with locations)
- 🛍️ Products (with categories)
- ⭐ Reviews (with scores and comments)
- 💰 Payments (with methods and amounts)

Real data. Real messiness. Real problems to solve.

Here's what the raw files look like:
```
olist_orders_dataset.csv
olist_customers_dataset.csv
olist_products_dataset.csv
olist_order_items_dataset.csv
olist_order_payments_dataset.csv
olist_order_reviews_dataset.csv
olist_sellers_dataset.csv
olist_geolocation_dataset.csv
```

Over the next few weeks, I'll transform this into:
- dim_customer
- dim_product
- dim_seller
- dim_date
- fact_orders

First step? Land everything in Bronze. No transformations. Just store it.

More tomorrow.

**CTA:** Follow along

---

### Post 3.2 - The Bronze Layer Philosophy (Wednesday)

**Hook:**
The Bronze layer has one job: don't lose anything.

**Body:**
When data arrives, I store it exactly as-is:
- Same column names (even if ugly)
- Same data types (even if wrong)
- Same duplicates (yes, really)
- Same nulls (all of them)

Why?

Because I don't know what I don't know.

That "garbage" column I want to delete? Business might need it next month.
That duplicate I want to remove? It might reveal a source system bug.
That null I want to fill? It might mean something specific.

Bronze is my insurance policy.

Here's my Bronze config:
```yaml
nodes:
  - name: bronze_orders
    read:
      connection: landing
      path: olist_orders_dataset.csv
      format: csv
    write:
      connection: bronze
      path: orders
      format: delta
      mode: append
```

No transforms. Just land it.

The only thing I add: `_extracted_at` timestamp so I know when data arrived.

Full article with complete config on LinkedIn → [link]

**CTA:** Link to LinkedIn article

---

### Post 3.3 - Setting Up the Project (Friday)

**Hook:**
Here's the complete Bronze layer config for 8 CSV files.

**Body:**
Every file gets its own node. Every node does the same thing:
1. Read from landing zone
2. Add extraction timestamp
3. Write to Bronze as Delta

```yaml
pipelines:
  - pipeline: bronze_ecommerce
    layer: bronze
    nodes:
      - name: bronze_orders
        read:
          connection: landing
          path: olist_orders_dataset.csv
      
      - name: bronze_customers
        read:
          connection: landing
          path: olist_customers_dataset.csv
      
      - name: bronze_products
        read:
          connection: landing
          path: olist_products_dataset.csv
      
      # ... 5 more files
```

Total setup time: 15 minutes.

Now I have:
- ✅ All raw data preserved
- ✅ Delta format (versioning, time travel)
- ✅ Timestamps for debugging
- ✅ Foundation for Silver layer

Next week: Start cleaning in Silver.

Full YAML on LinkedIn → [link]

**CTA:** Link to LinkedIn article

---

## Week 4: Silver Layer - Cleaning Basics

### Post 4.1 - Welcome to Silver (Monday)

**Hook:**
Bronze stores everything. Silver decides what's worth keeping.

**Body:**
Silver layer is where I:
- Remove true duplicates
- Handle nulls intentionally
- Standardize formats
- Apply business rules
- Validate data quality

But NOT where I:
- Aggregate
- Join across domains
- Create business metrics

Silver = clean, validated, single-source-of-truth

Think of it like a kitchen:
- Bronze = raw ingredients from the store (still in packaging)
- Silver = washed, chopped, prepped (ready to cook)
- Gold = finished dishes (ready to serve)

Today's task: Clean the orders table.

Problems I found:
- 8 orders with null customer IDs
- Dates stored as strings
- Status values with inconsistent casing

Let's fix them.

**CTA:** Continue tomorrow

---

### Post 4.2 - Handling Nulls (Wednesday)

**Hook:**
Nulls aren't errors. They're information.

**Body:**
8 orders have no customer ID.

Options:
1. ❌ Delete them (lose data)
2. ❌ Fill with "UNKNOWN" (hide the problem)
3. ✅ Keep them, flag them, investigate

Here's what I actually do:

```yaml
- name: silver_orders
  read:
    connection: bronze
    path: orders
  
  validation:
    contracts:
      - type: not_null
        columns: [order_id]
        severity: error  # These MUST exist
      
      - type: not_null
        columns: [customer_id]
        severity: warn   # Flag but don't fail
```

Orders without customer_id flow through but get flagged.

I can query later:
```sql
SELECT * FROM silver_orders 
WHERE customer_id IS NULL
```

Now I have visibility without losing data.

Lesson: Not every null is a problem to fix. Some are problems to understand.

I wrote a full guide on data quality contracts as a LinkedIn article. Link in comments.

**CTA:** Link to Article 4 + Engagement question

---

### Post 4.3 - Deduplication (Friday)

**Hook:**
Same order ID appearing twice. Which one do I keep?

**Body:**
Found 23 duplicate order IDs in Bronze.

Possible causes:
- Source sent same record twice
- Extract job ran twice
- Legitimate updates to same order

How do I know which?

I look at the `_extracted_at` timestamp I added in Bronze.

Same timestamp = duplicate extract (keep one)
Different timestamp = update (keep latest)

```yaml
transformer: deduplicate
params:
  keys: [order_id]
  order_by: "_extracted_at DESC"
```

This keeps the most recent version of each order.

Before: 100,023 rows
After: 100,000 rows

23 duplicates removed. Zero data lost-I can always check Bronze if needed.

This is why Bronze exists. It's my safety net.

Full guide on setting up a Bronze layer with Delta Lake as a LinkedIn article. Link in comments.

**CTA:** Link to Article 4 + Follow for more

---

## Week 5: Silver Layer - Data Quality

### Post 5.1 - Data Contracts (Monday)

**Hook:**
A data contract is a promise your pipeline makes.

**Body:**
Before I load data, I check:
- Are required columns present?
- Are values within expected ranges?
- Does the data match expected patterns?

If not, I want to know BEFORE it breaks a dashboard.

```yaml
contracts:
  - type: not_null
    columns: [order_id, customer_id]
  
  - type: accepted_values
    column: order_status
    values: [delivered, shipped, canceled, processing]
  
  - type: row_count
    min: 1000  # Expect at least 1000 orders
```

If any contract fails, the pipeline stops.

No silent failures. No "why is the dashboard empty?" at 9am.

This saved me last month when a source system started sending nulls for a critical column. Contract caught it before it hit production.

**CTA:** Engagement

---

### Post 5.2 - Date Standardization (Wednesday)

**Hook:**
Dates stored as strings are ticking time bombs.

**Body:**
The orders table has dates like:
- "2018-06-04 12:00:00"
- "2018-6-4"
- "06/04/2018"

All valid. All different formats. All will break something eventually.

Silver layer standardizes:

```yaml
transform:
  steps:
    - function: cast_columns
      params:
        columns:
          order_purchase_timestamp: timestamp
          order_delivered_timestamp: timestamp
          order_estimated_delivery: date
```

Now every downstream consumer gets consistent types.

Bonus: timestamps enable time-based queries:
```sql
SELECT * FROM orders 
WHERE order_purchase_timestamp >= '2018-01-01'
```

This fails with string dates. Works perfectly with proper timestamps.

**CTA:** Small thing, big impact

---

### Post 5.3 - Silver Orders Complete (Friday)

**Hook:**
Here's the complete Silver orders config.

**Body:**
Everything together:

```yaml
- name: silver_orders
  read:
    connection: bronze
    path: orders
  
  contracts:
    - type: not_null
      columns: [order_id]
    - type: accepted_values
      column: order_status
      values: [delivered, shipped, canceled, processing, created, approved, invoiced, unavailable]
  
  transformer: deduplicate
  params:
    keys: [order_id]
    order_by: "_extracted_at DESC"
  
  transform:
    steps:
      - function: cast_columns
        params:
          columns:
            order_purchase_timestamp: timestamp
            order_delivered_timestamp: timestamp
      - function: clean_text
        params:
          columns: [order_status]
          lowercase: true
  
  write:
    connection: silver
    path: orders
    format: delta
```

Result:
- ✅ Duplicates removed
- ✅ Dates standardized
- ✅ Text normalized
- ✅ Contracts validated

One table down. Time to do the same for customers, products, and the rest.

Complete Silver layer configuration guide as a LinkedIn article. Link in comments.

**CTA:** Link to Article 5

---

## Week 6: Dimensional Modeling Introduction

### Post 6.1 - Facts vs Dimensions (Monday)

**Hook:**
Every data warehouse has two types of tables: facts and dimensions.

**Body:**
**Facts** = What happened (events, transactions, measurements)
- Orders placed
- Payments made
- Items purchased

**Dimensions** = Context about what happened (who, what, where, when)
- Customers (who bought)
- Products (what was bought)
- Dates (when it happened)
- Locations (where)

Facts have numbers you sum/count/average.
Dimensions have attributes you filter/group by.

In our e-commerce data:
- `fact_orders` = order_id, customer_sk, product_sk, amount, quantity
- `dim_customer` = customer_sk, customer_id, city, state
- `dim_product` = product_sk, product_id, category, weight
- `dim_date` = date_sk, full_date, month, quarter, year

The "sk" = surrogate key. More on that Wednesday.

**CTA:** Facts = numbers, Dimensions = context

---

### Post 6.2 - What's a Surrogate Key? (Wednesday)

**Hook:**
A surrogate key is an ID that means nothing-and that's the point.

**Body:**
Source systems have natural keys:
- customer_id = "abc123xyz"
- product_id = "PROD-7890"

These are fine until:
- The source system changes its ID format
- Two source systems use the same ID for different things
- You need to track history (same customer, different versions)

Surrogate keys solve this:
```
customer_sk = 1, 2, 3, 4...
```

Just integers. Generated by your warehouse. Never change.

Your fact table uses surrogate keys:
```
order_id | customer_sk | product_sk | amount
1001     | 42          | 156        | 99.00
```

To get customer name, join to dim_customer on customer_sk.

Why integers?
- Joins are faster
- Storage is smaller
- No format surprises

I generate them like this:
```yaml
pattern:
  type: dimension
  params:
    natural_key: customer_id
    surrogate_key: customer_sk
```

**CTA:** More on dimension building Friday

---

### Post 6.3 - Building dim_customer (Friday)

**Hook:**
Let's build our first dimension table.

**Body:**
Source: silver_customers (99,441 unique customers)

Target: dim_customer with:
- customer_sk (generated)
- customer_id (from source)
- city
- state
- created_at

```yaml
- name: dim_customer
  read:
    connection: silver
    path: customers
  
  pattern:
    type: dimension
    params:
      natural_key: customer_id
      surrogate_key: customer_sk
      scd_type: 1
      unknown_member: true
      audit:
        load_timestamp: true
  
  write:
    connection: gold
    table: dim_customer
    format: delta
```

What this does:
1. Generates customer_sk (1, 2, 3...)
2. Adds row with customer_sk = 0 for unknown/missing customers
3. Adds load_timestamp for auditing
4. SCD Type 1 = overwrite on change (no history)

Result: 99,442 rows (99,441 customers + 1 unknown member)

Why unknown member? So fact tables can still join even when customer_id is null.

Full guide on facts vs dimensions with code examples as a LinkedIn article. Link in comments.

**CTA:** Link to Article 6

---

## Week 7: More Dimensions

### Post 7.1 - Building dim_product (Monday)

**Hook:**
Products have categories. Categories have hierarchies. Let's model it.

**Body:**
Products table has:
- product_id
- category_name (in Portuguese)
- weight, length, height, width

I want dim_product with:
- product_sk (generated)
- product_id
- category (cleaned up)
- physical attributes

```yaml
- name: dim_product
  read:
    connection: silver
    path: products
  
  pattern:
    type: dimension
    params:
      natural_key: product_id
      surrogate_key: product_sk
      scd_type: 1
      unknown_member: true
```

Cleaning step-category names need work:
```yaml
transform:
  steps:
    - function: clean_text
      params:
        columns: [product_category_name]
        trim: true
        lowercase: true
    - function: fill_nulls
      params:
        columns: [product_category_name]
        value: "unknown"
```

32,951 products → dim_product ready.

**CTA:** Simple pattern, reusable

---

### Post 7.2 - Building dim_date (Wednesday)

**Hook:**
Every data warehouse needs a date dimension. Here's how to generate one.

**Body:**
Date dimensions aren't loaded from source-they're generated.

I need dates from 2016-01-01 to 2025-12-31:

```yaml
- name: dim_date
  pattern:
    type: date_dimension
    params:
      start_date: "2016-01-01"
      end_date: "2025-12-31"
      fiscal_year_start_month: 1
      unknown_member: true
  
  write:
    connection: gold
    table: dim_date
    format: delta
```

This generates:
- date_sk (20160101, 20160102, ...)
- full_date
- day_of_week, day_of_month, day_of_year
- week_of_year
- month, month_name
- quarter, quarter_name
- year
- fiscal_year, fiscal_quarter
- is_weekend, is_month_end

3,653 rows. Zero source data needed.

Now fact tables can join on date_sk for any time-based analysis.

I wrote a complete guide on building a date dimension from scratch as a LinkedIn article. Link in comments.

**CTA:** Link to Article 7

---

### Post 7.3 - Building dim_seller (Friday)

**Hook:**
3,095 sellers. Same pattern. 5 minutes.

**Body:**
```yaml
- name: dim_seller
  read:
    connection: silver
    path: sellers
  
  pattern:
    type: dimension
    params:
      natural_key: seller_id
      surrogate_key: seller_sk
      scd_type: 1
      unknown_member: true
  
  write:
    connection: gold
    table: dim_seller
```

That's it.

Once you understand the pattern, dimensions are fast.

Checklist:
- [x] dim_customer (99,442 rows)
- [x] dim_product (32,952 rows)
- [x] dim_date (3,653 rows)
- [x] dim_seller (3,096 rows)

Next week: The fact table. That's where it gets interesting.

**CTA:** Fact table coming Monday

---

## Week 8: Fact Tables

### Post 8.1 - Anatomy of a Fact Table (Monday)

**Hook:**
Fact tables are where the numbers live.

**Body:**
A fact table has:
1. **Surrogate keys** pointing to dimensions
2. **Degenerate dimensions** (order_id-no separate dim needed)
3. **Measures** (amounts, quantities, counts)

Our fact_orders will have:
```
order_sk          -- Generated PK
order_id          -- Degenerate dimension
customer_sk       -- FK to dim_customer
product_sk        -- FK to dim_product
seller_sk         -- FK to dim_seller
order_date_sk     -- FK to dim_date
delivery_date_sk  -- FK to dim_date
quantity          -- Measure
price             -- Measure
freight_value     -- Measure
```

The pattern:
1. Read order items (the grain-one row per item)
2. Look up surrogate keys from dimensions
3. Handle orphans (what if customer doesn't exist?)
4. Add measures
5. Write

Let's build it.

**CTA:** Tomorrow-the tricky parts

---

### Post 8.2 - Surrogate Key Lookups (Wednesday)

**Hook:**
The hardest part of fact tables: looking up dimension keys.

**Body:**
Source has: customer_id = "abc123"
Fact needs: customer_sk = 42

I need to join to dim_customer and get the surrogate key.

```yaml
pattern:
  type: fact
  params:
    dimensions:
      - source_column: customer_id
        dimension_table: dim_customer
        dimension_key: customer_id
        surrogate_key: customer_sk
      
      - source_column: seller_id
        dimension_table: dim_seller
        dimension_key: seller_id
        surrogate_key: seller_sk
      
      - source_column: product_id
        dimension_table: dim_product
        dimension_key: product_id
        surrogate_key: product_sk
```

This automatically:
1. Joins to each dimension
2. Retrieves the surrogate key
3. Adds it to the fact row

But what about orphans?

**CTA:** Orphans tomorrow

---

### Post 8.3 - Handling Orphan Records (Friday)

**Hook:**
An order references customer_id "xyz"-but that customer doesn't exist in dim_customer. Now what?

**Body:**
Options:
1. **Reject** - Fail the pipeline (strict but painful)
2. **Unknown** - Assign to customer_sk = 0 (the unknown member)
3. **Quarantine** - Route to a holding table for review

I use "unknown" for most cases:

```yaml
pattern:
  type: fact
  params:
    orphan_handling: unknown
    dimensions:
      - source_column: customer_id
        dimension_table: dim_customer
        dimension_key: customer_id
        surrogate_key: customer_sk
```

Now orphan orders get customer_sk = 0.

I can query later:
```sql
SELECT COUNT(*) 
FROM fact_orders 
WHERE customer_sk = 0
```

Found 8 orphans. Now I can investigate without blocking the pipeline.

This is why dim tables have unknown member rows-they catch orphans cleanly.

Full guide on fact table patterns including lookups, orphans, and measures as a LinkedIn article. Link in comments.

**CTA:** Link to Article 8

---

## Week 9: Putting It All Together

### Post 9.1 - The Complete Fact Table (Monday)

**Hook:**
Here's fact_orders-the center of our star schema.

**Body:**
```yaml
- name: fact_orders
  read:
    connection: silver
    path: order_items
  
  pattern:
    type: fact
    params:
      grain: [order_id, product_id, seller_id]
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
      orphan_handling: unknown
      measures:
        - quantity: "order_item_id"
        - price
        - freight_value
        - total_amount: "price + freight_value"
  
  write:
    connection: gold
    table: fact_orders
    format: delta
```

112,650 order items → fact_orders ready for analysis.

**CTA:** Star schema complete

---

### Post 9.2 - The Star Schema (Wednesday)

**Hook:**
Here's what we built:

**Body:**
```
                    dim_customer
                         |
    dim_product ---- fact_orders ---- dim_date
                         |
                    dim_seller
```

From 8 messy CSV files to a clean, queryable warehouse.

Total:
- dim_customer: 99,442 rows
- dim_product: 32,952 rows
- dim_seller: 3,096 rows
- dim_date: 3,653 rows
- fact_orders: 112,650 rows

Now I can answer:
- Revenue by product category?
- Orders by customer state?
- Delivery performance by month?
- Top sellers by quarter?

All with simple SQL joins.

I wrote a complete walkthrough from CSV to Star Schema on Medium. Link in comments.

**CTA:** Link to Article 9 + Sample queries Friday

---

### Post 9.3 - Queries That Just Work (Friday)

**Hook:**
The whole point: analytics that are fast and simple.

**Body:**
**Revenue by product category:**
```sql
SELECT 
  p.product_category_name,
  SUM(f.total_amount) as revenue
FROM fact_orders f
JOIN dim_product p ON f.product_sk = p.product_sk
GROUP BY p.product_category_name
ORDER BY revenue DESC
```

**Orders by customer state:**
```sql
SELECT 
  c.customer_state,
  COUNT(DISTINCT f.order_id) as order_count
FROM fact_orders f
JOIN dim_customer c ON f.customer_sk = c.customer_sk
GROUP BY c.customer_state
```

**Monthly trend:**
```sql
SELECT 
  d.year,
  d.month,
  SUM(f.total_amount) as revenue
FROM fact_orders f
JOIN dim_date d ON f.order_date_sk = d.date_sk
GROUP BY d.year, d.month
ORDER BY d.year, d.month
```

Clean data + good model = simple queries.

**CTA:** Next week-the tool I built to make this easier

---

## Week 10: Introducing Odibi

### Post 10.1 - Why I Built a Framework (Monday)

**Hook:**
I got tired of writing the same pipeline code over and over.

**Body:**
Every project:
- Same Bronze layer patterns
- Same deduplication logic
- Same SCD2 handling
- Same dimension building
- Same fact lookups

Copy-paste. Tweak. Debug. Repeat.

So I built a framework.

Declare WHAT you want. Let the tool handle HOW.

Instead of 200 lines of Python:
```yaml
pattern:
  type: dimension
  params:
    natural_key: customer_id
    surrogate_key: customer_sk
    scd_type: 2
```

Instead of manual joins for SK lookups:
```yaml
pattern:
  type: fact
  params:
    dimensions:
      - source_column: customer_id
        dimension_table: dim_customer
        surrogate_key: customer_sk
```

It's called Odibi.

It's what I've been using for this entire series.

**CTA:** More tomorrow

---

### Post 10.2 - What Odibi Does (Wednesday)

**Hook:**
YAML in. Data warehouse out.

**Body:**
Odibi handles:

**Patterns:**
- Dimensions (SCD0, SCD1, SCD2)
- Facts (with automatic SK lookups)
- Aggregations
- Date dimensions

**Data Quality:**
- Contracts (not_null, accepted_values, row_count)
- Validation before load
- Quarantine for bad records

**Transformations:**
- 30+ built-in functions
- SQL steps
- Custom Python

**Operations:**
- Incremental loading
- Delta Lake support
- Spark, Pandas, or Polars engines

One config file. One command. Full pipeline.

I'm open-sourcing it because I want feedback and collaborators.

I wrote a full introduction to Odibi on Medium explaining the why and how. Link in comments along with GitHub.

**CTA:** Link to Article 10 + GitHub

---

### Post 10.3 - Get Started (Friday)

**Hook:**
Everything from this series-in one repo.

**Body:**
The complete Brazilian E-Commerce warehouse:

📁 **configs/** - All YAML configs
📁 **docs/** - How it works
📁 **examples/** - Ready-to-run pipelines

```bash
pip install odibi
odibi run ecommerce_warehouse.yaml
```

Bronze → Silver → Gold in one command.

What's next:
- More patterns
- Better docs
- Community feedback

If you followed along, thank you. 

If you try it and find bugs-tell me. I'm still learning and building.

GitHub: [link]
Docs: [link]

**CTA:** Links to repo and docs

---

## Medium Articles for Phase 2

| Week | Article |
|------|---------|
| 3 | "Setting Up a Bronze Layer with Delta Lake" |
| 4 | "Data Quality Contracts: Catching Problems Before Production" |
| 5 | "Complete Silver Layer Configuration Guide" |
| 6 | "Facts vs Dimensions: A Practical Guide" |
| 7 | "Building a Date Dimension from Scratch" |
| 8 | "Fact Table Patterns: Lookups, Orphans, and Measures" |
| 9 | "From CSV to Star Schema: Complete Walkthrough" |
| 10 | "Introducing Odibi: Declarative Data Pipelines" |
